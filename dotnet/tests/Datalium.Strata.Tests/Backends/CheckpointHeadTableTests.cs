using System.Data.Common;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Storage.Redis;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using StackExchange.Redis;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// What makes listing threads cheap (<c>design/19-Async-Queryable-Store.md</c>): the SQL stores'
/// trigger-maintained <c>checkpoint_threads</c> head table -- its layout, the trigger keeping it current
/// whoever writes the log, and the one-time backfill of a log that predates it -- and the Redis index
/// sets. Checked against the fixtures <c>rust/strata-memory</c>'s tests read.
/// </summary>
public sealed class CheckpointHeadTableTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"strata-head-table-{Guid.NewGuid():N}.db");

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

    private static JsonNode Fixture(string name) =>
        CheckpointQueryContractTests.Fixture(Path.Combine("checkpoint-log", name));

    private static void AssertJson(JsonNode? expected, JsonNode? actual) =>
        Assert.True(JsonNode.DeepEquals(expected, actual), $"expected {expected?.ToJsonString()}, got {actual?.ToJsonString()}");

    private EfCoreCheckpointStore OpenSqlite()
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

    /// <summary>Inserts a row in the shape of <c>checkpoint-log/*.row.json</c> as raw SQL, the way any other writer would.</summary>
    private static void InsertRaw(DbConnection connection, JsonNode row, bool jsonb)
    {
        using var command = connection.CreateCommand();
        var cast = jsonb ? "::jsonb" : string.Empty;
        command.CommandText = $"""
            INSERT INTO checkpoints
                (thread_id, sequence, schema_version, status, suspended_node, next_node,
                 frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                 graph_id, graph_version)
            VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7{cast}, @p8{cast}, @p9, @p10, @p11{cast}, @p12{cast},
                    @p13{cast}, @p14, @p15, @p16)
            """;
        object Value(string key, bool json = false) => row[key] switch
        {
            null => DBNull.Value,
            var node when json => node.ToJsonString(),
            var node when node.GetValueKind() == System.Text.Json.JsonValueKind.Number => node.GetValue<long>(),
            var node => node.GetValue<string>(),
        };
        object[] values =
        [
            Value("thread_id"), Value("sequence"), Value("schema_version"), Value("status"), Value("suspended_node"),
            Value("next_node"), Value("frozen_state", json: true), Value("metadata", json: true), Value("created_at"),
            Value("resumed_at"), Value("nested", json: true), Value("decision", json: true), Value("wait", json: true),
            Value("wake_at"), Value("graph_id"), Value("graph_version"),
        ];
        for (var i = 0; i < values.Length; i++)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = $"@p{i + 1}";
            parameter.Value = values[i];
            command.Parameters.Add(parameter);
        }

        command.ExecuteNonQuery();
    }

    /// <summary><c>checkpoint_threads</c>, in the shape of <c>threads.rows.json</c>.</summary>
    private static JsonNode HeadRows(DbConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT thread_id, sequence, state, updated_at, CAST(metadata AS TEXT), wake_at, graph_id, graph_version
            FROM checkpoint_threads ORDER BY thread_id
            """;
        var rows = new JsonArray();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new JsonObject
            {
                ["thread_id"] = reader.GetString(0),
                ["sequence"] = reader.GetInt64(1),
                ["state"] = reader.GetString(2),
                ["updated_at"] = reader.GetString(3),
                ["metadata"] = reader.IsDBNull(4) ? null : JsonNode.Parse(reader.GetString(4)),
                ["wake_at"] = reader.IsDBNull(5) ? null : reader.GetString(5),
                ["graph_id"] = reader.IsDBNull(6) ? null : reader.GetString(6),
                ["graph_version"] = reader.IsDBNull(7) ? null : reader.GetString(7),
            });
        }

        return rows;
    }

    private static JsonNode SqliteColumns(SqliteConnection connection, string table)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA table_info({table})";
        var columns = new JsonArray();
        using var reader = command.ExecuteReader();
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

        return columns;
    }

    // --- SQLite (EF Core) ---

    [Fact]
    public void SqliteHeadTableMatchesTheSharedColumnFixture()
    {
        using (OpenSqlite())
        {
        }

        using var connection = OpenRaw();
        AssertJson(Fixture("threads-columns.json"), SqliteColumns(connection, "checkpoint_threads"));
    }

    [Fact]
    public async Task SqliteTriggerKeepsTheHeadTableCurrentWhoeverWritesTheLog()
    {
        // Raw inserts, not SaveAsync: the head row must come from the database itself.
        using var store = OpenSqlite();
        var rows = Fixture("threads.rows.json");
        using (var connection = OpenRaw())
        {
            foreach (var row in rows["inserts"]!.AsArray())
            {
                InsertRaw(connection, row!, jsonb: false);
            }

            AssertJson(rows["threads"], HeadRows(connection));
        }

        var page = await store.ListThreadsAsync(new ThreadQuery { States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Resumed } });
        Assert.Equal(["t-woken", "t-resumed"], page.Threads.Select(t => t.ThreadId));

        // The trigger copied wake_at, and only the waiting one is due (design/29-Event-Waits-And-Timers.md).
        var due = await store.ListThreadsAsync(new ThreadQuery { DueBy = DateTimeOffset.Parse("2026-09-16T08:00:00Z") });
        Assert.Equal("t-waiting", Assert.Single(due.Threads).ThreadId);
    }

    [Fact]
    public async Task SqliteLogFromBeforeTheHeadTableIsBackfilledOnceWhenOpened()
    {
        var rows = Fixture("threads.rows.json");
        using (var connection = OpenRaw())
        {
            using var create = connection.CreateCommand();
            create.CommandText = EfCoreCheckpointStore.CheckpointLogDdl;
            create.ExecuteNonQuery();
            foreach (var row in rows["inserts"]!.AsArray())
            {
                InsertRaw(connection, row!, jsonb: false);
            }
        }

        using var store = OpenSqlite();
        using (var connection = OpenRaw())
        {
            AssertJson(rows["threads"], HeadRows(connection));
        }

        // Opening again doesn't backfill twice, and the trigger has taken over.
        using (OpenSqlite())
        {
        }

        await store.SaveAsync(StoreContracts.Checkpoint("t-late", "2026-09-17T00:00:00Z"));
        using var check = OpenRaw();
        var head = HeadRows(check).AsArray().Single(r => r!["thread_id"]!.GetValue<string>() == "t-late")!;
        Assert.Equal(2, head["sequence"]!.GetValue<long>());
        Assert.Equal("interrupted", head["state"]!.GetValue<string>());
    }

    // --- Postgres ---

    [PostgresFact]
    public void PostgresHeadTableMatchesTheSharedColumnFixture()
    {
        using var sandbox = new PostgresSandbox("head_layout");
        var dataSource = sandbox.CreateDataSource();
        using (PostgresSandbox.Open(dataSource))
        {
        }

        AssertJson(Fixture("postgres-threads-columns.json"), PostgresStoreTests.Columns(dataSource, "checkpoint_threads"));
    }

    [PostgresFact]
    public async Task PostgresTriggerKeepsTheHeadTableCurrentWhoeverWritesTheLog()
    {
        using var sandbox = new PostgresSandbox("trigger");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);
        var rows = Fixture("threads.rows.json");
        await using (var connection = await dataSource.OpenConnectionAsync())
        {
            foreach (var row in rows["inserts"]!.AsArray())
            {
                InsertRaw(connection, row!, jsonb: true);
            }

            AssertJson(rows["threads"], HeadRows(connection));
        }

        var page = await store.ListThreadsAsync(new ThreadQuery { States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Resumed } });
        Assert.Equal(["t-woken", "t-resumed"], page.Threads.Select(t => t.ThreadId));

        // The trigger copied wake_at, and only the waiting one is due (design/29-Event-Waits-And-Timers.md).
        var due = await store.ListThreadsAsync(new ThreadQuery { DueBy = DateTimeOffset.Parse("2026-09-16T08:00:00Z") });
        Assert.Equal("t-waiting", Assert.Single(due.Threads).ThreadId);
    }

    [PostgresFact]
    public async Task PostgresLogFromBeforeTheHeadTableIsBackfilledOnceEvenWhenOpenedConcurrently()
    {
        using var sandbox = new PostgresSandbox("backfill");
        var dataSource = sandbox.CreateDataSource();
        var rows = Fixture("threads.rows.json");
        await using (var connection = await dataSource.OpenConnectionAsync())
        {
            await using (var create = new NpgsqlCommand(PostgresCheckpointStore.CheckpointLogDdl, connection))
            {
                await create.ExecuteNonQueryAsync();
            }

            foreach (var row in rows["inserts"]!.AsArray())
            {
                InsertRaw(connection, row!, jsonb: true);
            }
        }

        // Several stores open it at once, as several workers starting would.
        var stores = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() => PostgresSandbox.Open(sandbox.CreateDataSource()))));
        await using (var connection = await dataSource.OpenConnectionAsync())
        {
            AssertJson(rows["threads"], HeadRows(connection));
        }

        await stores[0].SaveAsync(StoreContracts.Checkpoint("t-late", "2026-09-17T00:00:00Z"));
        await using (var connection = await dataSource.OpenConnectionAsync())
        {
            var head = HeadRows(connection).AsArray().Single(r => r!["thread_id"]!.GetValue<string>() == "t-late")!;
            Assert.Equal(2, head["sequence"]!.GetValue<long>());
        }

        foreach (var store in stores)
        {
            store.Dispose();
        }
    }

    // --- Redis ---

    /// <summary>The index sets under a prefix, in the shape of <c>redis-index.json</c>.</summary>
    private static JsonNode IndexSets(RedisSandbox sandbox)
    {
        var sets = new JsonObject();
        foreach (var set in new[] { "threads", "threads:waking", "threads:interrupted", "threads:failed", "threads:cancelled", "threads:resumed", "threads:completed", "threads:ended", "threads:running" })
        {
            var members = sandbox.Database.SortedSetRangeByRankWithScores($"{sandbox.Prefix}:{set}", order: Order.Descending);
            sets[set] = new JsonArray([.. members.Select(m => (JsonNode)new JsonArray((string)m.Element!, (long)m.Score))]);
        }

        return sets;
    }

    [RedisFact]
    public async Task SavingTheQuerySeedLeavesTheSharedIndex()
    {
        using var sandbox = new RedisSandbox("index");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await CheckpointQueryContractTests.SaveSeedAsync(store);

        AssertJson(Fixture("redis-index.json")["sets"], IndexSets(sandbox));
    }

    [RedisFact]
    public async Task RebuildingTheIndexFromTheListsAloneGivesTheSameSets()
    {
        using var sandbox = new RedisSandbox("rebuild");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await CheckpointQueryContractTests.SaveSeedAsync(store);
        foreach (var set in new[] { "threads", "threads:waking", "threads:interrupted", "threads:failed", "threads:cancelled", "threads:resumed", "threads:completed", "threads:ended", "threads:running" })
        {
            await sandbox.Database.KeyDeleteAsync($"{sandbox.Prefix}:{set}");
        }

        Assert.Equal(15, await store.RebuildIndexAsync());
        AssertJson(Fixture("redis-index.json")["sets"], IndexSets(sandbox));
    }

    [RedisFact]
    public async Task APrefixWrittenBeforeTheIndexIsIndexedOnFirstConnect()
    {
        using var sandbox = new RedisSandbox("first_connect");
        // Lists only, as a runtime from before SG-11 leaves them.
        foreach (var checkpoint in CheckpointQueryContractTests.Seed())
        {
            await sandbox.Database.ListRightPushAsync($"{sandbox.Prefix}:checkpoints:{checkpoint.ThreadId}", checkpoint.ToWireJson().ToJsonString());
        }

        // Constructing a store sends nothing (design/34-Storage-Hardening.md): its first call indexes the prefix.
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        Assert.Equal(15, (await store.ListThreadsAsync(new ThreadQuery())).Threads.Count);
        AssertJson(Fixture("redis-index.json")["sets"], IndexSets(sandbox));
    }

    [RedisFact]
    public async Task AThreadPushedWithoutTheIndexIsListedAfterARebuild()
    {
        // What an older runtime writing to an indexed prefix does.
        using var sandbox = new RedisSandbox("old_writer");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        Assert.Null(await store.LoadAsync("old"));
        var checkpoint = StoreContracts.Checkpoint("old", "2026-09-16T00:00:00Z");
        await sandbox.Database.ListRightPushAsync($"{sandbox.Prefix}:checkpoints:old", checkpoint.ToWireJson().ToJsonString());

        Assert.Empty((await store.ListThreadsAsync(new ThreadQuery())).Threads);
        await store.RebuildIndexAsync();
        Assert.Single((await store.ListThreadsAsync(new ThreadQuery())).Threads);
    }
}
