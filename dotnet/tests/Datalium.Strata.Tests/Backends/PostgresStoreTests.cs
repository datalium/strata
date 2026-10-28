using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage.Postgres;
using Npgsql;
using NpgsqlTypes;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// Postgres-specific behaviour of <see cref="PostgresCheckpointStore"/> / <see cref="PostgresHistoryStore"/>
/// -- everything <see cref="StoreContractTests"/> can't say because it isn't true of every backend: the
/// on-disk layout the two runtimes share (checked against the same fixtures
/// <c>rust/strata-memory/tests/postgres_stores.rs</c> reads), and safety under concurrent writers.
/// </summary>
public sealed class PostgresStoreTests
{
    private static JsonNode LoadFixture(string relativePath, [CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(testDir, "..", "..", "..", "..", "schema", "fixtures", relativePath));
        return JsonNode.Parse(File.ReadAllText(path))!;
    }

    private static Checkpoint FrozenFixture(string? threadId = null)
    {
        var doc = LoadFixture(Path.Combine("checkpoint-lifecycle", "frozen.json")).AsObject();
        return new Checkpoint
        {
            SchemaVersion = doc["schema_version"]!.GetValue<string>(),
            ThreadId = threadId ?? doc["thread_id"]!.GetValue<string>(),
            Status = CheckpointStatus.Interrupted,
            SuspendedNode = doc["suspended_node"]?.GetValue<string>(),
            NextNode = doc["next_node"]?.GetValue<string>(),
            FrozenState = doc["frozen_state"]?.DeepClone(),
            Metadata = null,
            CreatedAt = doc["created_at"]!.GetValue<string>(),
            ResumedAt = null,
        };
    }

    /// <summary>The <c>information_schema</c> view of a table in the sandbox's schema, in the shape of <c>postgres-columns.json</c>.</summary>
    internal static JsonNode Columns(NpgsqlDataSource dataSource, string table)
    {
        using var command = dataSource.CreateCommand(
            """
            SELECT c.column_name::text, c.data_type::text, c.is_nullable::text = 'NO',
                   COALESCE(k.ordinal_position, 0)::int
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name, kcu.ordinal_position
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON kcu.constraint_name = tc.constraint_name
                 AND kcu.table_schema = tc.table_schema
                 AND kcu.table_name = tc.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = current_schema() AND tc.table_name = @table
            ) k ON k.column_name = c.column_name
            WHERE c.table_schema = current_schema() AND c.table_name = @table
            ORDER BY c.ordinal_position
            """);
        command.Parameters.AddWithValue("table", table);

        var columns = new JsonArray();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            columns.Add(new JsonObject
            {
                ["name"] = reader.GetString(0),
                ["type"] = reader.GetString(1),
                ["notnull"] = reader.GetBoolean(2),
                ["pk"] = reader.GetInt32(3),
            });
        }

        return columns;
    }

    /// <summary>The raw <c>checkpoints</c> row, laid out like <c>frozen.row.json</c> (JSON columns parsed).</summary>
    private static JsonNode RawCheckpointRow(NpgsqlDataSource dataSource, string threadId, long sequence)
    {
        using var command = dataSource.CreateCommand(
            """
            SELECT thread_id, sequence, schema_version, status, suspended_node, next_node,
                   frozen_state::text, metadata::text, created_at, resumed_at, nested::text, decision::text,
                   wait::text, wake_at, graph_id, graph_version
            FROM checkpoints WHERE thread_id = @thread_id AND sequence = @sequence
            """);
        command.Parameters.AddWithValue("thread_id", threadId);
        command.Parameters.AddWithValue("sequence", sequence);
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());

        string? Text(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
        return new JsonObject
        {
            ["thread_id"] = Text(0),
            ["sequence"] = reader.GetInt64(1),
            ["schema_version"] = Text(2),
            ["status"] = Text(3),
            ["suspended_node"] = Text(4),
            ["next_node"] = Text(5),
            ["frozen_state"] = JsonNode.Parse(reader.GetString(6)),
            ["metadata"] = Text(7) is { } metadata ? JsonNode.Parse(metadata) : null,
            ["created_at"] = Text(8),
            ["resumed_at"] = Text(9),
            ["nested"] = Text(10) is { } nested ? JsonNode.Parse(nested) : null,
            ["decision"] = Text(11) is { } decision ? JsonNode.Parse(decision) : null,
            ["wait"] = Text(12) is { } wait ? JsonNode.Parse(wait) : null,
            ["wake_at"] = Text(13),
            ["graph_id"] = Text(14),
            ["graph_version"] = Text(15),
        };
    }

    private static List<long> Sequences(NpgsqlDataSource dataSource, string threadId)
    {
        using var command = dataSource.CreateCommand(
            "SELECT sequence FROM checkpoints WHERE thread_id = @thread_id ORDER BY sequence");
        command.Parameters.AddWithValue("thread_id", threadId);
        using var reader = command.ExecuteReader();
        var sequences = new List<long>();
        while (reader.Read())
        {
            sequences.Add(reader.GetInt64(0));
        }

        return sequences;
    }

    [Fact]
    public async Task AnUnreachableServerIsAnErrorOnFirstUseNotAHang()
    {
        // Port 1 on loopback refuses immediately; the point is that it fails cleanly (and that a bad
        // URL doesn't hang), so this needs no server. Constructing a store connects to nothing
        // (design/34-Storage-Hardening.md): the first call does, repeats the refusal as its policy says, and
        // fails; so does opening one eagerly.
        const string nowhere = "postgres://strata:strata@127.0.0.1:1/strata";
        var once = new PostgresStoreOptions { Retry = Datalium.Strata.Storage.StoreRetryPolicy.None };
        var started = Stopwatch.StartNew();
        using var store = new PostgresCheckpointStore(nowhere, once);
        using var history = new PostgresHistoryStore(nowhere, once);
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(1), "constructing a store waits for nothing");

        await Assert.ThrowsAnyAsync<NpgsqlException>(async () => await store.LoadAsync("t"));
        await Assert.ThrowsAnyAsync<NpgsqlException>(() => PostgresCheckpointStore.OpenAsync(nowhere, once));
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void AUrlBecomesAnNpgsqlConnectionString()
    {
        var builder = new NpgsqlConnectionStringBuilder(
            PostgresConnection.ToConnectionString("postgres://ann:p%40ss:word@db.example:6543/orders?sslmode=require"));

        Assert.Equal("db.example", builder.Host);
        Assert.Equal(6543, builder.Port);
        Assert.Equal("ann", builder.Username);
        Assert.Equal("p@ss:word", builder.Password);
        Assert.Equal("orders", builder.Database);
        Assert.Equal(SslMode.Require, builder.SslMode);

        var defaults = new NpgsqlConnectionStringBuilder(PostgresConnection.ToConnectionString("postgresql://localhost/strata"));
        Assert.Equal("localhost", defaults.Host);
        Assert.Equal(5432, defaults.Port);

        // A native connection string passes through untouched; unknown URL options are refused, not ignored.
        Assert.Equal("Host=h;Database=d", PostgresConnection.ToConnectionString("Host=h;Database=d"));
        Assert.Throws<ArgumentException>(() => PostgresConnection.ToConnectionString("postgres://h/d?options=x"));
    }

    [PostgresFact]
    public void TheCheckpointsTableMatchesTheSharedColumnFixture()
    {
        using var sandbox = new PostgresSandbox("layout");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);

        Assert.True(
            JsonNode.DeepEquals(LoadFixture("checkpoint-log/postgres-columns.json"), Columns(dataSource, "checkpoints")),
            Columns(dataSource, "checkpoints").ToJsonString());
    }

    [PostgresFact]
    public void ACheckpointsTableFromBeforeNestedCheckpointsIsUpgradedInPlace()
    {
        using var sandbox = new PostgresSandbox("pre_nested");
        var dataSource = sandbox.CreateDataSource();
        // The table as it was before nested checkpoints: no `nested` column.
        using (var seed = dataSource.CreateCommand(
            """
            CREATE TABLE checkpoints (
                thread_id      TEXT NOT NULL,
                sequence       BIGINT NOT NULL,
                schema_version TEXT NOT NULL,
                status         TEXT NOT NULL,
                suspended_node TEXT,
                next_node      TEXT,
                frozen_state   JSONB NOT NULL,
                metadata       JSONB,
                created_at     TEXT NOT NULL,
                resumed_at     TEXT,
                PRIMARY KEY (thread_id, sequence)
            );
            INSERT INTO checkpoints VALUES
                ('old', 0, '1.0.0', 'interrupted', 'A', 'B', '{"n": 1}', NULL, '2026-09-16T00:00:00Z', NULL)
            """))
        {
            seed.ExecuteNonQuery();
        }

        using var store = PostgresSandbox.Open(dataSource);

        // The old row reads back as it was, with no child.
        var old = store.Load("old")!;
        Assert.Equal(1, old.FrozenState!["n"]!.GetValue<int>());
        Assert.Null(old.Nested);

        // A checkpoint with a child saves and loads on the upgraded table.
        var nested = old with { ThreadId = "new", SchemaVersion = CheckpointSchema.NestedVersion, Nested = old with { ThreadId = "Work" } };
        store.Save(nested);
        Assert.Equal("Work", store.Load("new")!.Nested!.ThreadId);

        // The upgrade left exactly the shared layout, and opening again is a no-op.
        using var again = PostgresSandbox.Open(dataSource);
        Assert.True(
            JsonNode.DeepEquals(LoadFixture("checkpoint-log/postgres-columns.json"), Columns(dataSource, "checkpoints")),
            Columns(dataSource, "checkpoints").ToJsonString());
    }

    [PostgresFact]
    public void TheHistoryTableMatchesTheSharedColumnFixture()
    {
        using var sandbox = new PostgresSandbox("history_layout");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.OpenHistory(dataSource);

        Assert.True(
            JsonNode.DeepEquals(LoadFixture("history-log/postgres-columns.json"), Columns(dataSource, "history_records")),
            Columns(dataSource, "history_records").ToJsonString());
    }

    [PostgresFact]
    public void TheRowForTheSharedFixtureMatchesTheSharedRowFixture()
    {
        using var sandbox = new PostgresSandbox("row");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);
        var frozen = FrozenFixture();
        store.Save(frozen);

        // The same row fixture the SQLite stores, the CLI and the Rust Postgres store are held to.
        var actual = RawCheckpointRow(dataSource, frozen.ThreadId, 0);
        Assert.True(
            JsonNode.DeepEquals(LoadFixture(Path.Combine("checkpoint-log", "frozen.row.json")), actual),
            actual.ToJsonString());
    }

    [PostgresFact]
    public void TheRowForACancelledCheckpointMatchesTheSharedRowFixture()
    {
        using var sandbox = new PostgresSandbox("cancelled_row");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);
        var cancelled = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "cancelled.json")));
        store.Save(cancelled);

        var actual = RawCheckpointRow(dataSource, cancelled.ThreadId, 0);
        Assert.True(
            JsonNode.DeepEquals(LoadFixture(Path.Combine("checkpoint-log", "cancelled.row.json")), actual),
            actual.ToJsonString());
        StoreContracts.AssertSame(cancelled, store.Load(cancelled.ThreadId));
    }

    [PostgresFact]
    public void TheRowForARunningCheckpointMatchesTheSharedRowFixture()
    {
        // design/33-Durable-Execution.md: a status, and nothing new in the layout.
        using var sandbox = new PostgresSandbox("running_row");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);
        var running = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "running.json")));
        store.Save(running);

        var actual = RawCheckpointRow(dataSource, running.ThreadId, 0);
        Assert.True(
            JsonNode.DeepEquals(LoadFixture(Path.Combine("checkpoint-log", "running.row.json")), actual),
            actual.ToJsonString());
        StoreContracts.AssertSame(running, store.Load(running.ThreadId));
    }

    [PostgresFact]
    public void TheRowForAnEndedCheckpointMatchesTheSharedRowFixture()
    {
        using var sandbox = new PostgresSandbox("ended_row");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);
        var ended = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "ended.json")));
        store.Save(ended);

        var actual = RawCheckpointRow(dataSource, ended.ThreadId, 0);
        Assert.True(
            JsonNode.DeepEquals(LoadFixture(Path.Combine("checkpoint-log", "ended.row.json")), actual),
            actual.ToJsonString());
        StoreContracts.AssertSame(ended, store.Load(ended.ThreadId));
    }

    [PostgresFact]
    public void TheRowForAWaitingCheckpointMatchesTheSharedRowFixture()
    {
        using var sandbox = new PostgresSandbox("waiting_row");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);
        var waiting = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "waiting-event.json")));
        var nested = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "nested-timer.json")));
        store.Save(waiting);
        store.Save(nested);

        var actual = RawCheckpointRow(dataSource, waiting.ThreadId, 0);
        Assert.True(
            JsonNode.DeepEquals(LoadFixture(Path.Combine("checkpoint-log", "waiting.row.json")), actual),
            actual.ToJsonString());
        // A nested wait's derived column is the innermost level's (design/29-Event-Waits-And-Timers.md).
        var row = RawCheckpointRow(dataSource, nested.ThreadId, 0);
        Assert.Null(row["wait"]);
        Assert.Equal("2026-10-06T12:05:00Z", row["wake_at"]!.GetValue<string>());
        StoreContracts.AssertSame(waiting, store.Load(waiting.ThreadId));
        StoreContracts.AssertSame(nested, store.Load(nested.ThreadId));
    }

    [PostgresFact]
    public async Task AHeadTableFromBeforeWaitsIsUpgradedInPlace()
    {
        using var sandbox = new PostgresSandbox("pre_wait");
        var dataSource = sandbox.CreateDataSource();
        // The tables and trigger as they were before waits: no `wait` or `wake_at`.
        using (var seed = dataSource.CreateCommand(
            """
            CREATE TABLE checkpoints (
                thread_id TEXT NOT NULL, sequence BIGINT NOT NULL, schema_version TEXT NOT NULL,
                status TEXT NOT NULL, suspended_node TEXT, next_node TEXT,
                frozen_state JSONB NOT NULL, metadata JSONB, created_at TEXT NOT NULL,
                resumed_at TEXT, nested JSONB, decision JSONB,
                PRIMARY KEY (thread_id, sequence)
            );
            CREATE TABLE checkpoint_threads (
                thread_id TEXT COLLATE "C" NOT NULL PRIMARY KEY, sequence BIGINT NOT NULL,
                state TEXT NOT NULL, updated_at TEXT COLLATE "C" NOT NULL, metadata JSONB
            );
            CREATE OR REPLACE FUNCTION strata_checkpoints_head() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO checkpoint_threads AS h (thread_id, sequence, state, updated_at, metadata)
                VALUES (NEW.thread_id, NEW.sequence,
                        CASE WHEN NEW.resumed_at IS NOT NULL THEN 'resumed' ELSE NEW.status END,
                        COALESCE(NEW.resumed_at, NEW.created_at), NEW.metadata)
                ON CONFLICT (thread_id) DO UPDATE SET
                    sequence = EXCLUDED.sequence, state = EXCLUDED.state,
                    updated_at = EXCLUDED.updated_at, metadata = EXCLUDED.metadata
                WHERE EXCLUDED.sequence > h.sequence;
                RETURN NULL;
            END;
            $$;
            CREATE TRIGGER checkpoints_head AFTER INSERT ON checkpoints
                FOR EACH ROW EXECUTE FUNCTION strata_checkpoints_head();
            INSERT INTO checkpoints (thread_id, sequence, schema_version, status, suspended_node,
                next_node, frozen_state, created_at)
            VALUES ('old', 0, '1.0.0', 'interrupted', 'Approve', 'Execute', '{}', '2026-09-16T00:00:00Z');
            """))
        {
            seed.ExecuteNonQuery();
        }

        using var store = PostgresSandbox.Open(dataSource);
        using var again = PostgresSandbox.Open(dataSource);
        Assert.True(
            JsonNode.DeepEquals(LoadFixture("checkpoint-log/postgres-columns.json"), Columns(dataSource, "checkpoints")),
            Columns(dataSource, "checkpoints").ToJsonString());
        Assert.True(
            JsonNode.DeepEquals(LoadFixture("checkpoint-log/postgres-threads-columns.json"), Columns(dataSource, "checkpoint_threads")),
            Columns(dataSource, "checkpoint_threads").ToJsonString());

        // The new trigger copies wake_at: a waiting thread is due, the old one is still listed.
        store.Save(Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "nested-timer.json"))));
        var due = await store.ListThreadsAsync(new ThreadQuery { DueBy = DateTimeOffset.Parse("2026-10-06T12:05:00Z") });
        Assert.Equal("thread_verify_demo", Assert.Single(due.Threads).ThreadId);
        Assert.Equal(2, (await store.ListThreadsAsync(new ThreadQuery())).Threads.Count);
    }

    [PostgresFact]
    public async Task TheRowForAnIdentifiedCheckpointMatchesTheSharedRowFixture()
    {
        using var sandbox = new PostgresSandbox("identified_row");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);
        var identified = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "identified.json")));
        var nested = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "nested-identified.json")));
        store.Save(identified);
        store.Save(nested);

        var actual = RawCheckpointRow(dataSource, identified.ThreadId, 0);
        Assert.True(
            JsonNode.DeepEquals(LoadFixture(Path.Combine("checkpoint-log", "identified.row.json")), actual),
            actual.ToJsonString());
        // The columns hold the outermost level's identity (design/31-Graph-Identity.md).
        Assert.Equal("incident", RawCheckpointRow(dataSource, nested.ThreadId, 0)["graph_id"]!.GetValue<string>());
        StoreContracts.AssertSame(identified, store.Load(identified.ThreadId));
        StoreContracts.AssertSame(nested, store.Load(nested.ThreadId));
        var listed = await store.ListThreadsAsync(new ThreadQuery { GraphId = "wire-transfer", GraphVersion = "2" });
        Assert.Equal(identified.ThreadId, Assert.Single(listed.Threads).ThreadId);
    }

    [PostgresFact]
    public async Task AHeadTableFromBeforeGraphIdentitiesIsUpgradedInPlace()
    {
        using var sandbox = new PostgresSandbox("pre_identity");
        var dataSource = sandbox.CreateDataSource();
        // The tables and trigger as they were before graph identities: no `graph_id` or `graph_version`.
        using (var seed = dataSource.CreateCommand(
            """
            CREATE TABLE checkpoints (
                thread_id TEXT NOT NULL, sequence BIGINT NOT NULL, schema_version TEXT NOT NULL,
                status TEXT NOT NULL, suspended_node TEXT, next_node TEXT,
                frozen_state JSONB NOT NULL, metadata JSONB, created_at TEXT NOT NULL,
                resumed_at TEXT, nested JSONB, decision JSONB, wait JSONB, wake_at TEXT,
                PRIMARY KEY (thread_id, sequence)
            );
            CREATE TABLE checkpoint_threads (
                thread_id TEXT COLLATE "C" NOT NULL PRIMARY KEY, sequence BIGINT NOT NULL,
                state TEXT NOT NULL, updated_at TEXT COLLATE "C" NOT NULL, metadata JSONB,
                wake_at TEXT COLLATE "C"
            );
            CREATE OR REPLACE FUNCTION strata_checkpoints_head() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO checkpoint_threads AS h (thread_id, sequence, state, updated_at, metadata, wake_at)
                VALUES (NEW.thread_id, NEW.sequence,
                        CASE WHEN NEW.resumed_at IS NOT NULL THEN 'resumed' ELSE NEW.status END,
                        COALESCE(NEW.resumed_at, NEW.created_at), NEW.metadata, NEW.wake_at)
                ON CONFLICT (thread_id) DO UPDATE SET
                    sequence = EXCLUDED.sequence, state = EXCLUDED.state,
                    updated_at = EXCLUDED.updated_at, metadata = EXCLUDED.metadata, wake_at = EXCLUDED.wake_at
                WHERE EXCLUDED.sequence > h.sequence;
                RETURN NULL;
            END;
            $$;
            CREATE TRIGGER checkpoints_head AFTER INSERT ON checkpoints
                FOR EACH ROW EXECUTE FUNCTION strata_checkpoints_head();
            INSERT INTO checkpoints (thread_id, sequence, schema_version, status, suspended_node,
                next_node, frozen_state, created_at)
            VALUES ('old', 0, '1.0.0', 'interrupted', 'Approve', 'Execute', '{}', '2026-09-16T00:00:00Z');
            """))
        {
            seed.ExecuteNonQuery();
        }

        using var store = PostgresSandbox.Open(dataSource);
        using var again = PostgresSandbox.Open(dataSource);
        Assert.True(
            JsonNode.DeepEquals(LoadFixture("checkpoint-log/postgres-columns.json"), Columns(dataSource, "checkpoints")),
            Columns(dataSource, "checkpoints").ToJsonString());
        Assert.True(
            JsonNode.DeepEquals(LoadFixture("checkpoint-log/postgres-threads-columns.json"), Columns(dataSource, "checkpoint_threads")),
            Columns(dataSource, "checkpoint_threads").ToJsonString());

        // The new trigger copies the identity: the identified thread is listed by its graph, the old one still listed.
        store.Save(Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "identified.json"))));
        var byGraph = await store.ListThreadsAsync(new ThreadQuery { GraphId = "wire-transfer" });
        Assert.Equal("thread_hitl_demo", Assert.Single(byGraph.Threads).ThreadId);
        Assert.Equal(2, (await store.ListThreadsAsync(new ThreadQuery())).Threads.Count);
    }

    [PostgresFact]
    public void TheRowForANestedCheckpointHoldsTheChildAsJsonbInTheNestedColumn()
    {
        using var sandbox = new PostgresSandbox("nested_row");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);
        var nested = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "nested.json")));
        store.Save(nested);

        var actual = RawCheckpointRow(dataSource, nested.ThreadId, 0);
        Assert.True(
            JsonNode.DeepEquals(LoadFixture(Path.Combine("checkpoint-log", "nested.row.json")), actual),
            actual.ToJsonString());
        StoreContracts.AssertSame(nested, store.Load(nested.ThreadId));
    }

    [PostgresFact]
    public void TheRowForACheckpointWithADecisionHoldsItAsJsonbInTheDecisionColumn()
    {
        using var sandbox = new PostgresSandbox("decision_row");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);
        var decided = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "decision-failed.json")));
        store.Save(decided);

        var actual = RawCheckpointRow(dataSource, decided.ThreadId, 0);
        Assert.True(
            JsonNode.DeepEquals(LoadFixture(Path.Combine("checkpoint-log", "decision.row.json")), actual),
            actual.ToJsonString());
        StoreContracts.AssertSame(decided, store.Load(decided.ThreadId));
    }

    [PostgresFact]
    public void ARowWrittenAsRawSqlIsLoadable()
    {
        // The reader side of the contract: a row laid out exactly as the fixture says -- which is what
        // the Rust store writes -- is readable through Load.
        using var sandbox = new PostgresSandbox("reader");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);
        var row = LoadFixture(Path.Combine("checkpoint-log", "frozen.row.json")).AsObject();

        using var insert = dataSource.CreateCommand(
            """
            INSERT INTO checkpoints
                (thread_id, sequence, schema_version, status, suspended_node, next_node,
                 frozen_state, metadata, created_at, resumed_at)
            VALUES (@thread_id, @sequence, @schema_version, @status, @suspended_node, @next_node,
                    @frozen_state, NULL, @created_at, NULL)
            """);
        insert.Parameters.AddWithValue("thread_id", row["thread_id"]!.GetValue<string>());
        insert.Parameters.AddWithValue("sequence", row["sequence"]!.GetValue<long>());
        insert.Parameters.AddWithValue("schema_version", row["schema_version"]!.GetValue<string>());
        insert.Parameters.AddWithValue("status", row["status"]!.GetValue<string>());
        insert.Parameters.AddWithValue("suspended_node", row["suspended_node"]!.GetValue<string>());
        insert.Parameters.AddWithValue("next_node", row["next_node"]!.GetValue<string>());
        insert.Parameters.Add(new NpgsqlParameter("frozen_state", NpgsqlDbType.Jsonb) { Value = row["frozen_state"]!.ToJsonString() });
        insert.Parameters.AddWithValue("created_at", row["created_at"]!.GetValue<string>());
        insert.ExecuteNonQuery();

        StoreContracts.AssertSame(FrozenFixture(), store.Load(FrozenFixture().ThreadId));
    }

    [PostgresFact]
    public void SaveAppendsToTheLogAndLoadReturnsTheLatest()
    {
        using var sandbox = new PostgresSandbox("log");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);
        var first = FrozenFixture();
        var resumed = first with { ResumedAt = "2026-09-16T01:00:00Z" };

        store.Save(first);
        store.Save(resumed);
        store.Save(FrozenFixture("other-thread"));

        StoreContracts.AssertSame(resumed, store.Load(first.ThreadId));
        Assert.Equal([0L, 1L], Sequences(dataSource, first.ThreadId)); // the earlier checkpoint stays as history
        Assert.Equal([0L], Sequences(dataSource, "other-thread")); // sequences are per thread
    }

    [PostgresFact]
    public void ACheckpointSurvivesIndependentStoreInstances()
    {
        // The point of a database over InMemoryCheckpointStore: what one connection saved, a brand-new
        // one (a restarted process) reads back.
        using var sandbox = new PostgresSandbox("persist");
        var frozen = FrozenFixture();
        using (var store = PostgresSandbox.Open(sandbox.CreateDataSource()))
        {
            store.Save(frozen);
        }

        using var reopened = PostgresSandbox.Open(sandbox.CreateDataSource());
        StoreContracts.AssertSame(frozen, reopened.Load(frozen.ThreadId));
    }

    /// <summary>Runs <paramref name="work"/> on <paramref name="writers"/> dedicated threads released together.</summary>
    private static async Task RunConcurrentlyAsync(int writers, Func<int, Task> work)
    {
        using var start = new ManualResetEventSlim();
        var tasks = Enumerable.Range(0, writers).Select(writer => Task.Factory.StartNew(
            () =>
            {
                start.Wait();
                return work(writer);
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap()).ToArray();
        start.Set();
        await Task.WhenAll(tasks);
    }

    [PostgresFact]
    public async Task ConcurrentWritersToOneThreadNeverCollideOrLoseACheckpoint()
    {
        // The claim SQLite couldn't make ("single writer assumed"): eight connections, each its own
        // store, hammer the *same* thread. Without the per-thread advisory lock two of them pick the
        // same sequence and one fails on the primary key.
        const int writers = 8;
        const int savesEach = 25;
        using var sandbox = new PostgresSandbox("concurrent_checkpoints");
        var stores = Enumerable.Range(0, writers).Select(_ => PostgresSandbox.Open(sandbox.CreateDataSource())).ToArray();

        await RunConcurrentlyAsync(writers, writer =>
        {
            for (var save = 0; save < savesEach; save++)
            {
                stores[writer].Save(FrozenFixture("contended") with { CreatedAt = $"writer-{writer}-save-{save}" });
            }

            return Task.CompletedTask;
        });

        var dataSource = sandbox.CreateDataSource();
        Assert.Equal(Enumerable.Range(0, writers * savesEach).Select(i => (long)i), Sequences(dataSource, "contended"));

        // Each writer's own saves are still in the order it made them.
        for (var writer = 0; writer < writers; writer++)
        {
            using var command = dataSource.CreateCommand(
                "SELECT created_at FROM checkpoints WHERE thread_id = 'contended' AND created_at LIKE @pattern ORDER BY sequence");
            command.Parameters.AddWithValue("pattern", $"writer-{writer}-save-%");
            using var reader = command.ExecuteReader();
            var saves = new List<int>();
            while (reader.Read())
            {
                saves.Add(int.Parse(reader.GetString(0).Split('-')[^1]));
            }

            Assert.Equal(Enumerable.Range(0, savesEach), saves);
        }

        foreach (var store in stores)
        {
            store.Dispose();
        }
    }

    [PostgresFact]
    public async Task ConcurrentAppendersToOneSessionKeepAContiguousHistory()
    {
        const int writers = 8;
        const int appendsEach = 25;
        using var sandbox = new PostgresSandbox("concurrent_history");
        var stores = Enumerable.Range(0, writers).Select(_ => PostgresSandbox.OpenHistory(sandbox.CreateDataSource())).ToArray();

        await RunConcurrentlyAsync(writers, async writer =>
        {
            for (var n = 0; n < appendsEach; n++)
            {
                await stores[writer].AppendAsync(
                    "contended", new ChatMessage(MessageRole.User, $"{writer}:{n}".AsMemory(), DateTimeOffset.UnixEpoch));
            }
        });

        var history = new List<string>();
        await foreach (var message in stores[0].GetHistoryAsync("contended", writers * appendsEach + 10))
        {
            history.Add(message.Content.ToString());
        }

        Assert.Equal(writers * appendsEach, history.Count); // no append was lost
        for (var writer = 0; writer < writers; writer++)
        {
            var mine = history.Where(c => c.StartsWith($"{writer}:", StringComparison.Ordinal)).Select(c => int.Parse(c.Split(':')[1]));
            Assert.Equal(Enumerable.Range(0, appendsEach), mine);
        }

        foreach (var store in stores)
        {
            store.Dispose();
        }
    }

    [PostgresFact]
    public async Task OpeningStoresAtTheSameMomentOnAFreshDatabaseDoesNotRace()
    {
        // CREATE TABLE IF NOT EXISTS from two sessions at once can fail on pg_type's unique index in
        // Postgres; the DDL advisory lock prevents it. Two runtimes starting against a shared, empty
        // database is exactly this.
        const int openers = 8;
        using var sandbox = new PostgresSandbox("ddl_race");
        var dataSources = Enumerable.Range(0, openers).Select(_ => sandbox.CreateDataSource()).ToArray();

        await RunConcurrentlyAsync(openers, opener =>
        {
            PostgresSandbox.Open(dataSources[opener]).Dispose();
            return Task.CompletedTask;
        });
    }
}
