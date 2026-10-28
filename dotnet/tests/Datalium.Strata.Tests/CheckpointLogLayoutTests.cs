using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage.Memory;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace Datalium.Strata.Tests;

/// <summary>
/// <see cref="EfCoreCheckpointStore"/> writes the shared SQLite checkpoint log (see
/// <c>schema/README.md</c>, "SQLite checkpoint log"), the same table <c>strata-memory</c>'s
/// <c>SqliteCheckpointStore</c> and the <c>strata</c> CLI create. Each language's tests compare
/// its table and its raw row against the same two fixtures
/// (<c>schema/fixtures/checkpoint-log/columns.json</c> and <c>frozen.row.json</c>), so the three
/// can't drift apart silently; <c>rust/strata-memory/src/checkpoint.rs</c> and
/// <c>cli/tests/checkpoint_log_layout.rs</c> are the counterparts.
/// </summary>
public sealed class CheckpointLogLayoutTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"strata-checkpoint-log-{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        // Microsoft.Data.Sqlite pools connections, which keeps the file open after a store is disposed.
        SqliteConnection.ClearAllPools();
        try
        {
            File.Delete(_path);
        }
        catch (IOException)
        {
            // Best-effort cleanup of a temp file.
        }
    }

    private static JsonNode LoadFixture(string relativePath, [CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(testDir, "..", "..", "..", "schema", "fixtures", relativePath));
        return JsonNode.Parse(File.ReadAllText(path))!;
    }

    private static Checkpoint LifecycleFixture()
    {
        var doc = LoadFixture(Path.Combine("checkpoint-lifecycle", "frozen.json")).AsObject();
        return new Checkpoint
        {
            SchemaVersion = doc["schema_version"]!.GetValue<string>(),
            ThreadId = doc["thread_id"]!.GetValue<string>(),
            Status = CheckpointStatus.Interrupted,
            SuspendedNode = doc["suspended_node"]?.GetValue<string>(),
            NextNode = doc["next_node"]?.GetValue<string>(),
            FrozenState = doc["frozen_state"]?.DeepClone(),
            Metadata = null,
            CreatedAt = doc["created_at"]!.GetValue<string>(),
            ResumedAt = null,
        };
    }

    private EfCoreCheckpointStore OpenStore()
    {
        var options = new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={_path}").Options;
        return new EfCoreCheckpointStore(new CheckpointDbContext(options));
    }

    private SqliteConnection OpenRaw()
    {
        var connection = new SqliteConnection($"Data Source={_path}");
        connection.Open();
        return connection;
    }

    /// <summary>The raw <c>checkpoints</c> row, laid out like <c>frozen.row.json</c> (JSON columns parsed).</summary>
    private static JsonNode RawRow(SqliteConnection connection, string threadId, long sequence)
    {
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT thread_id, sequence, schema_version, status, suspended_node, next_node,
                   frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                   graph_id, graph_version
            FROM checkpoints WHERE thread_id = $thread AND sequence = $sequence
            """;
        command.Parameters.AddWithValue("$thread", threadId);
        command.Parameters.AddWithValue("$sequence", sequence);
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read(), $"no row for {threadId} #{sequence}");

        string? Text(int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        JsonNode? Json(int ordinal) => reader.IsDBNull(ordinal) ? null : JsonNode.Parse(reader.GetString(ordinal));

        return new JsonObject
        {
            ["thread_id"] = Text(0),
            ["sequence"] = reader.GetInt64(1),
            ["schema_version"] = Text(2),
            ["status"] = Text(3),
            ["suspended_node"] = Text(4),
            ["next_node"] = Text(5),
            ["frozen_state"] = Json(6),
            ["metadata"] = Json(7),
            ["created_at"] = Text(8),
            ["resumed_at"] = Text(9),
            ["nested"] = Json(10),
            ["decision"] = Json(11),
            ["wait"] = Json(12),
            ["wake_at"] = Text(13),
            ["graph_id"] = Text(14),
            ["graph_version"] = Text(15),
        };
    }

    [Fact]
    public void Table_Layout_Matches_The_Shared_Column_Fixture()
    {
        using (OpenStore())
        {
        }

        using var connection = OpenRaw();
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA table_info(checkpoints)";
        using var reader = command.ExecuteReader();

        var columns = new JsonArray();
        while (reader.Read())
        {
            columns.Add(new JsonObject
            {
                ["name"] = reader.GetString(1),
                ["type"] = reader.GetString(2),
                ["notnull"] = reader.GetInt64(3) != 0,
                ["pk"] = reader.GetInt64(5),
            });
        }

        Assert.Equal(
            LoadFixture(Path.Combine("checkpoint-log", "columns.json")).ToJsonString(),
            columns.ToJsonString());
    }

    [Fact]
    public void Opening_A_Log_From_Before_Nested_Checkpoints_Adds_The_Column_And_Keeps_Its_Rows()
    {
        // The `checkpoints` table as it was before nested checkpoints (design/16-Nested-Interrupts.md):
        // no `nested` column. Seeded directly, as a file an older runtime left behind.
        using (var raw = OpenRaw())
        {
            using var create = raw.CreateCommand();
            create.CommandText = """
                CREATE TABLE checkpoints (
                    thread_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                    schema_version TEXT NOT NULL, status TEXT NOT NULL,
                    suspended_node TEXT, next_node TEXT, frozen_state TEXT NOT NULL,
                    metadata TEXT, created_at TEXT NOT NULL, resumed_at TEXT,
                    PRIMARY KEY (thread_id, sequence));
                INSERT INTO checkpoints VALUES
                    ('old', 0, '1.0.0', 'interrupted', 'A', 'B', '{"n":1}', NULL, '2026-09-16T00:00:00Z', NULL);
                """;
            create.ExecuteNonQuery();
        }

        using (var store = OpenStore())
        {
            // The old row reads back as it was, with no child.
            var old = store.Load("old")!;
            Assert.Equal(1, old.FrozenState!["n"]!.GetValue<int>());
            Assert.Equal("B", old.NextNode);
            Assert.Null(old.Nested);

            // A checkpoint with a child saves and loads on the upgraded file.
            var nested = LifecycleFixture() with
            {
                ThreadId = "new",
                SchemaVersion = CheckpointSchema.NestedVersion,
                Nested = LifecycleFixture() with { ThreadId = "Work" },
            };
            store.Save(nested);
            var loaded = store.Load("new")!;
            Assert.Equal("Work", loaded.Nested!.ThreadId);

            // So does one with a decision (design/21-Typed-Human-Signals.md): its column was added too.
            var decided = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "decision-failed.json")));
            store.Save(decided);
            Assert.Equal("alice@acme.io", store.Load(decided.ThreadId)!.Decision!.Reviewer);
        }

        // The upgrade left exactly the shared layout, and opening again is a no-op.
        using (OpenStore())
        {
        }

        using var connection = OpenRaw();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM pragma_table_info('checkpoints')";
        using var reader = command.ExecuteReader();
        var names = new List<string>();
        while (reader.Read())
        {
            names.Add(reader.GetString(0));
        }

        var expected = LoadFixture(Path.Combine("checkpoint-log", "columns.json")).AsArray()
            .Select(column => column!["name"]!.GetValue<string>());
        Assert.Equal(expected, names);
    }

    [Fact]
    public void Row_For_The_Shared_Fixture_Matches_The_Shared_Row_Fixture()
    {
        var fixture = LifecycleFixture();
        using (var store = OpenStore())
        {
            store.Save(fixture);
        }

        using var connection = OpenRaw();
        Assert.True(JsonNode.DeepEquals(
            LoadFixture(Path.Combine("checkpoint-log", "frozen.row.json")),
            RawRow(connection, fixture.ThreadId, 0)));
    }

    [Fact]
    public void Row_For_A_Cancelled_Checkpoint_Matches_The_Shared_Row_Fixture()
    {
        var fixture = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "cancelled.json")));
        using (var store = OpenStore())
        {
            store.Save(fixture);
        }

        using var connection = OpenRaw();
        Assert.True(JsonNode.DeepEquals(
            LoadFixture(Path.Combine("checkpoint-log", "cancelled.row.json")),
            RawRow(connection, fixture.ThreadId, 0)));

        using var reopened = OpenStore();
        Assert.True(JsonNode.DeepEquals(fixture.ToWireJson(), reopened.Load(fixture.ThreadId)!.ToWireJson()));
    }

    [Fact]
    public void Row_For_A_Running_Checkpoint_Matches_The_Shared_Row_Fixture()
    {
        // design/33-Durable-Execution.md: a status, and nothing new in the layout.
        var fixture = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "running.json")));
        using (var store = OpenStore())
        {
            store.Save(fixture);
        }

        using var connection = OpenRaw();
        Assert.True(JsonNode.DeepEquals(
            LoadFixture(Path.Combine("checkpoint-log", "running.row.json")),
            RawRow(connection, fixture.ThreadId, 0)));

        using var reopened = OpenStore();
        Assert.True(JsonNode.DeepEquals(fixture.ToWireJson(), reopened.Load(fixture.ThreadId)!.ToWireJson()));
    }

    [Fact]
    public void Row_For_An_Ended_Checkpoint_Matches_The_Shared_Row_Fixture()
    {
        var fixture = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "ended.json")));
        using (var store = OpenStore())
        {
            store.Save(fixture);
        }

        using var connection = OpenRaw();
        Assert.True(JsonNode.DeepEquals(
            LoadFixture(Path.Combine("checkpoint-log", "ended.row.json")),
            RawRow(connection, fixture.ThreadId, 0)));

        using var reopened = OpenStore();
        Assert.True(JsonNode.DeepEquals(fixture.ToWireJson(), reopened.Load(fixture.ThreadId)!.ToWireJson()));
    }

    [Fact]
    public void Row_For_A_Waiting_Checkpoint_Holds_Its_Wait_And_Wake_At()
    {
        var fixture = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "waiting-event.json")));
        var nested = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "nested-timer.json")));
        using (var store = OpenStore())
        {
            store.Save(fixture);
            store.Save(nested);
        }

        using var connection = OpenRaw();
        Assert.True(JsonNode.DeepEquals(
            LoadFixture(Path.Combine("checkpoint-log", "waiting.row.json")),
            RawRow(connection, fixture.ThreadId, 0)));

        // The outermost level doesn't wait; the derived column is the child's (design/29-Event-Waits-And-Timers.md).
        var row = RawRow(connection, nested.ThreadId, 0);
        Assert.Null(row["wait"]);
        Assert.Equal("2026-10-06T12:05:00Z", row["wake_at"]!.GetValue<string>());

        using var reopened = OpenStore();
        Assert.True(JsonNode.DeepEquals(fixture.ToWireJson(), reopened.Load(fixture.ThreadId)!.ToWireJson()));
        Assert.True(JsonNode.DeepEquals(nested.ToWireJson(), reopened.Load(nested.ThreadId)!.ToWireJson()));
    }

    [Fact]
    public async Task Opening_A_Log_From_Before_Waits_Adds_The_Columns_And_Replaces_The_Trigger()
    {
        // The tables and trigger as they were before waits (design/29-Event-Waits-And-Timers.md): no `wait`, no
        // `wake_at`, and a trigger that doesn't copy it.
        using (var raw = OpenRaw())
        {
            using var create = raw.CreateCommand();
            create.CommandText = """
                CREATE TABLE checkpoints (
                    thread_id TEXT NOT NULL, sequence INTEGER NOT NULL, schema_version TEXT NOT NULL,
                    status TEXT NOT NULL, suspended_node TEXT, next_node TEXT, frozen_state TEXT NOT NULL,
                    metadata TEXT, created_at TEXT NOT NULL, resumed_at TEXT, nested TEXT, decision TEXT,
                    PRIMARY KEY (thread_id, sequence));
                CREATE TABLE checkpoint_threads (
                    thread_id TEXT NOT NULL PRIMARY KEY, sequence INTEGER NOT NULL, state TEXT NOT NULL,
                    updated_at TEXT NOT NULL, metadata TEXT);
                CREATE TRIGGER checkpoints_head AFTER INSERT ON checkpoints
                BEGIN
                    INSERT INTO checkpoint_threads (thread_id, sequence, state, updated_at, metadata)
                    VALUES (NEW.thread_id, NEW.sequence,
                            CASE WHEN NEW.resumed_at IS NOT NULL THEN 'resumed' ELSE NEW.status END,
                            COALESCE(NEW.resumed_at, NEW.created_at), NEW.metadata)
                    ON CONFLICT (thread_id) DO UPDATE SET
                        sequence = excluded.sequence, state = excluded.state,
                        updated_at = excluded.updated_at, metadata = excluded.metadata
                    WHERE excluded.sequence > checkpoint_threads.sequence;
                END;
                INSERT INTO checkpoints (thread_id, sequence, schema_version, status, suspended_node, next_node,
                    frozen_state, created_at)
                VALUES ('old', 0, '1.0.0', 'interrupted', 'Approve', 'Execute', '{}', '2026-09-16T00:00:00Z');
                """;
            create.ExecuteNonQuery();
        }

        using var store = OpenStore();
        using (OpenStore())
        {
            // Opening again is a no-op.
        }

        using (var connection = OpenRaw())
        {
            List<string> Names(string table)
            {
                using var command = connection.CreateCommand();
                command.CommandText = $"SELECT name FROM pragma_table_info('{table}')";
                using var reader = command.ExecuteReader();
                var names = new List<string>();
                while (reader.Read())
                {
                    names.Add(reader.GetString(0));
                }

                return names;
            }

            IEnumerable<string> Expected(string fixture) =>
                LoadFixture(Path.Combine("checkpoint-log", fixture)).AsArray().Select(c => c!["name"]!.GetValue<string>());

            Assert.Equal(Expected("columns.json"), Names("checkpoints"));
            Assert.Equal(Expected("threads-columns.json"), Names("checkpoint_threads"));
        }

        // The old thread is still listed; a waiting one is due through the new trigger.
        store.Save(Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "nested-timer.json"))));
        var due = await store.ListThreadsAsync(new ThreadQuery { DueBy = DateTimeOffset.Parse("2026-10-06T12:05:00Z") });
        Assert.Equal("thread_verify_demo", Assert.Single(due.Threads).ThreadId);
        Assert.Equal(2, (await store.ListThreadsAsync(new ThreadQuery())).Threads.Count);
    }

    [Fact]
    public async Task Row_For_An_Identified_Checkpoint_Matches_The_Shared_Row_Fixture()
    {
        var fixture = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "identified.json")));
        var nested = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "nested-identified.json")));
        using (var store = OpenStore())
        {
            store.Save(fixture);
            store.Save(nested);
        }

        using var connection = OpenRaw();
        Assert.True(JsonNode.DeepEquals(
            LoadFixture(Path.Combine("checkpoint-log", "identified.row.json")),
            RawRow(connection, fixture.ThreadId, 0)));

        // The columns hold the outermost level's identity; the child's is inside `nested` (design/31-Graph-Identity.md).
        var row = RawRow(connection, nested.ThreadId, 0);
        Assert.Equal("incident", row["graph_id"]!.GetValue<string>());
        Assert.Equal("approval", row["nested"]!["graph"]!["id"]!.GetValue<string>());

        using var reopened = OpenStore();
        Assert.True(JsonNode.DeepEquals(fixture.ToWireJson(), reopened.Load(fixture.ThreadId)!.ToWireJson()));
        Assert.True(JsonNode.DeepEquals(nested.ToWireJson(), reopened.Load(nested.ThreadId)!.ToWireJson()));
        var listed = await reopened.ListThreadsAsync(new ThreadQuery { GraphId = "wire-transfer", GraphVersion = "2" });
        Assert.Equal(fixture.ThreadId, Assert.Single(listed.Threads).ThreadId);
    }

    [Fact]
    public async Task Opening_A_Log_From_Before_Graph_Identities_Adds_The_Columns_And_Replaces_The_Trigger()
    {
        // The tables and trigger as they were before graph identities (design/31-Graph-Identity.md): no
        // `graph_id` or `graph_version`, and a trigger that doesn't copy them.
        using (var raw = OpenRaw())
        {
            using var create = raw.CreateCommand();
            create.CommandText = """
                CREATE TABLE checkpoints (
                    thread_id TEXT NOT NULL, sequence INTEGER NOT NULL, schema_version TEXT NOT NULL,
                    status TEXT NOT NULL, suspended_node TEXT, next_node TEXT, frozen_state TEXT NOT NULL,
                    metadata TEXT, created_at TEXT NOT NULL, resumed_at TEXT, nested TEXT, decision TEXT,
                    wait TEXT, wake_at TEXT,
                    PRIMARY KEY (thread_id, sequence));
                CREATE TABLE checkpoint_threads (
                    thread_id TEXT NOT NULL PRIMARY KEY, sequence INTEGER NOT NULL, state TEXT NOT NULL,
                    updated_at TEXT NOT NULL, metadata TEXT, wake_at TEXT);
                CREATE TRIGGER checkpoints_head AFTER INSERT ON checkpoints
                BEGIN
                    INSERT INTO checkpoint_threads (thread_id, sequence, state, updated_at, metadata, wake_at)
                    VALUES (NEW.thread_id, NEW.sequence,
                            CASE WHEN NEW.resumed_at IS NOT NULL THEN 'resumed' ELSE NEW.status END,
                            COALESCE(NEW.resumed_at, NEW.created_at), NEW.metadata, NEW.wake_at)
                    ON CONFLICT (thread_id) DO UPDATE SET
                        sequence = excluded.sequence, state = excluded.state,
                        updated_at = excluded.updated_at, metadata = excluded.metadata, wake_at = excluded.wake_at
                    WHERE excluded.sequence > checkpoint_threads.sequence;
                END;
                INSERT INTO checkpoints (thread_id, sequence, schema_version, status, suspended_node, next_node,
                    frozen_state, created_at)
                VALUES ('old', 0, '1.0.0', 'interrupted', 'Approve', 'Execute', '{}', '2026-09-16T00:00:00Z');
                """;
            create.ExecuteNonQuery();
        }

        using var store = OpenStore();
        using (OpenStore())
        {
            // Opening again is a no-op.
        }

        using (var connection = OpenRaw())
        {
            List<string> Names(string table)
            {
                using var command = connection.CreateCommand();
                command.CommandText = $"SELECT name FROM pragma_table_info('{table}')";
                using var reader = command.ExecuteReader();
                var names = new List<string>();
                while (reader.Read())
                {
                    names.Add(reader.GetString(0));
                }

                return names;
            }

            IEnumerable<string> Expected(string fixture) =>
                LoadFixture(Path.Combine("checkpoint-log", fixture)).AsArray().Select(c => c!["name"]!.GetValue<string>());

            Assert.Equal(Expected("columns.json"), Names("checkpoints"));
            Assert.Equal(Expected("threads-columns.json"), Names("checkpoint_threads"));
        }

        // The old thread is still listed and has no identity; an identified one is listed by its graph through
        // the new trigger.
        store.Save(Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "identified.json"))));
        var byGraph = await store.ListThreadsAsync(new ThreadQuery { GraphId = "wire-transfer" });
        Assert.Equal("thread_hitl_demo", Assert.Single(byGraph.Threads).ThreadId);
        var all = (await store.ListThreadsAsync(new ThreadQuery())).Threads;
        Assert.Equal(2, all.Count);
        Assert.Null(all.Single(t => t.ThreadId == "old").Graph);
    }

    [Fact]
    public void Row_For_A_Checkpoint_With_A_Decision_Holds_It_As_Json_In_The_Decision_Column()
    {
        var fixture = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "decision-failed.json")));
        using (var store = OpenStore())
        {
            store.Save(fixture);
        }

        using var connection = OpenRaw();
        Assert.True(JsonNode.DeepEquals(
            LoadFixture(Path.Combine("checkpoint-log", "decision.row.json")),
            RawRow(connection, fixture.ThreadId, 0)));

        using var reopened = OpenStore();
        Assert.True(JsonNode.DeepEquals(fixture.ToWireJson(), reopened.Load(fixture.ThreadId)!.ToWireJson()));
    }

    [Fact]
    public void Row_For_A_Nested_Checkpoint_Holds_The_Child_As_Json_In_The_Nested_Column()
    {
        var fixture = Checkpoint.FromWireJson(LoadFixture(Path.Combine("checkpoint-lifecycle", "nested.json")));
        using (var store = OpenStore())
        {
            store.Save(fixture);
        }

        using var connection = OpenRaw();
        Assert.True(JsonNode.DeepEquals(
            LoadFixture(Path.Combine("checkpoint-log", "nested.row.json")),
            RawRow(connection, fixture.ThreadId, 0)));

        // And it reads back whole, child included.
        using var reopened = OpenStore();
        var loaded = reopened.Load(fixture.ThreadId)!;
        Assert.Equal("Approve", loaded.Nested!.SuspendedNode);
        Assert.True(JsonNode.DeepEquals(fixture.ToWireJson(), loaded.ToWireJson()));
    }

    [Fact]
    public void Loads_A_Row_Inserted_As_Raw_Sql_From_The_Shared_Row_Fixture()
    {
        // The reader side of the contract: a row laid out exactly as the fixture says (which is
        // what the Rust store and the CLI write) is readable through Load.
        var row = LoadFixture(Path.Combine("checkpoint-log", "frozen.row.json")).AsObject();
        using var store = OpenStore();

        using (var connection = OpenRaw())
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = """
                INSERT INTO checkpoints
                    (thread_id, sequence, schema_version, status, suspended_node, next_node,
                     frozen_state, metadata, created_at, resumed_at)
                VALUES ($thread, $sequence, $version, $status, $suspended, $next, $state, NULL, $created, NULL)
                """;
            insert.Parameters.AddWithValue("$thread", row["thread_id"]!.GetValue<string>());
            insert.Parameters.AddWithValue("$sequence", row["sequence"]!.GetValue<long>());
            insert.Parameters.AddWithValue("$version", row["schema_version"]!.GetValue<string>());
            insert.Parameters.AddWithValue("$status", row["status"]!.GetValue<string>());
            insert.Parameters.AddWithValue("$suspended", row["suspended_node"]!.GetValue<string>());
            insert.Parameters.AddWithValue("$next", row["next_node"]!.GetValue<string>());
            insert.Parameters.AddWithValue("$state", row["frozen_state"]!.ToJsonString());
            insert.Parameters.AddWithValue("$created", row["created_at"]!.GetValue<string>());
            insert.ExecuteNonQuery();
        }

        var fixture = LifecycleFixture();
        var loaded = store.Load(fixture.ThreadId);

        Assert.NotNull(loaded);
        Assert.Equal(fixture.SchemaVersion, loaded.SchemaVersion);
        Assert.Equal(CheckpointStatus.Interrupted, loaded.Status);
        Assert.Equal(fixture.SuspendedNode, loaded.SuspendedNode);
        Assert.Equal(fixture.NextNode, loaded.NextNode);
        Assert.True(JsonNode.DeepEquals(fixture.FrozenState, loaded.FrozenState));
        Assert.Equal(fixture.CreatedAt, loaded.CreatedAt);
        Assert.Null(loaded.Metadata);
        Assert.Null(loaded.ResumedAt);
    }

    [Fact]
    public void Save_Appends_To_The_Log_And_Load_Returns_The_Latest()
    {
        var first = LifecycleFixture();
        var resumed = first with
        {
            ResumedAt = "2026-09-16T01:00:00Z",
            Metadata = new Dictionary<string, string> { ["approved_by"] = "x" },
        };

        using (var store = OpenStore())
        {
            store.Save(first);
            store.Save(resumed);

            var loaded = store.Load(first.ThreadId);
            Assert.NotNull(loaded);
            Assert.Equal("2026-09-16T01:00:00Z", loaded.ResumedAt);
            Assert.Equal("x", loaded.Metadata!["approved_by"]);
        }

        using var connection = OpenRaw();
        using var count = connection.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM checkpoints WHERE thread_id = $thread";
        count.Parameters.AddWithValue("$thread", first.ThreadId);
        Assert.Equal(2L, count.ExecuteScalar());

        // The earlier checkpoint stays on disk as history; sequences are per thread.
        Assert.Null(RawRow(connection, first.ThreadId, 0)["resumed_at"]);
        Assert.Equal("x", RawRow(connection, first.ThreadId, 1)["metadata"]!["approved_by"]!.GetValue<string>());
    }

    [Fact]
    public void Opening_A_Database_The_Table_Already_Exists_In_Keeps_Its_Rows()
    {
        // CREATE TABLE IF NOT EXISTS is a no-op against a file another runtime (or an earlier
        // instance of this store) already initialised.
        using (var store = OpenStore())
        {
            store.Save(LifecycleFixture());
        }

        using var reopened = OpenStore();
        Assert.NotNull(reopened.Load(LifecycleFixture().ThreadId));
    }
}
