using System.Data.Common;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Storage.Redis;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// Where thread leases are kept (<c>design/32-Thread-Leases.md</c>, "Wire format"): the SQL stores'
/// <c>checkpoint_leases</c> table and the Redis lease keys, checked against the fixtures
/// <c>rust/strata-memory</c>'s tests read, so a lease one runtime takes is honoured by the other.
/// </summary>
public sealed class CheckpointLeaseTableTests : IDisposable
{
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(1);

    private readonly string _path = Path.Combine(Path.GetTempPath(), $"strata-leases-{Guid.NewGuid():N}.db");

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

    /// <summary>Inserts <c>leases.rows.json</c>'s rows as raw SQL, the way any other writer would.</summary>
    private static void InsertRows(DbConnection connection, JsonNode fixture, string expiresAt)
    {
        foreach (var row in fixture["rows"]!.AsArray())
        {
            using var command = connection.CreateCommand();
            command.CommandText =
                $"INSERT INTO checkpoint_leases (thread_id, owner, token, expires_at) VALUES (@p1, @p2, @p3, {expiresAt})";
            object?[] values =
            [
                row!["thread_id"]!.GetValue<string>(),
                row["owner"]?.GetValue<string>(),
                row["token"]!.GetValue<long>(),
                row["expires_at"]?.GetValue<long>(),
            ];
            for (var i = 0; i < values.Length; i++)
            {
                var parameter = command.CreateParameter();
                parameter.ParameterName = $"@p{i + 1}";
                parameter.Value = values[i] ?? DBNull.Value;
                if (values[i] is null && i == 3)
                {
                    parameter.DbType = System.Data.DbType.Int64;
                }

                command.Parameters.Add(parameter);
            }

            command.ExecuteNonQuery();
        }
    }

    /// <summary>What <paramref name="store"/> makes of the fixture's rows: each thread's lease, and the token the next acquisition gets.</summary>
    private static async Task AssertReadsTheRows(ICheckpointStore store, JsonNode fixture)
    {
        foreach (var (threadId, expected) in fixture["leases"]!.AsObject())
        {
            var lease = await store.GetLeaseAsync(threadId);
            JsonNode? actual = lease is null
                ? null
                : new JsonObject
                {
                    ["owner"] = lease.Owner,
                    ["token"] = lease.Token,
                    ["expires_at"] = lease.ExpiresAt.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture),
                };
            AssertJson(expected, actual);
        }

        foreach (var (threadId, expected) in fixture["next_tokens"]!.AsObject())
        {
            var attempt = await store.TryAcquireLeaseAsync(threadId, "worker-9", Ttl);
            Assert.Equal(expected?.GetValue<long>(), attempt.Lease?.Token);
            if (expected is null)
            {
                Assert.Equal(fixture["leases"]![threadId]!["owner"]!.GetValue<string>(), attempt.HeldBy?.Owner);
            }
        }
    }

    // --- SQLite (EF Core) ---

    [Fact]
    public void SqliteLeaseTableMatchesTheSharedColumnFixture()
    {
        using (OpenSqlite())
        {
        }

        using var connection = OpenRaw();
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA table_info(checkpoint_leases)";
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

        AssertJson(Fixture("leases-columns.json"), columns);
    }

    [Fact]
    public async Task SqliteReadsLeasesAnotherWriterLeft()
    {
        using var store = OpenSqlite();
        var fixture = Fixture("leases.rows.json");
        using (var connection = OpenRaw())
        {
            InsertRows(connection, fixture, "@p4");
        }

        await AssertReadsTheRows(store, fixture);
    }

    [Fact]
    public async Task SqliteWritesALeaseRowInTheSharedShape()
    {
        using var store = OpenSqlite();
        var lease = (await store.TryAcquireLeaseAsync("t1", "worker-1", Ttl)).Lease!;
        using var connection = OpenRaw();

        JsonNode Row()
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT owner, token, typeof(expires_at), expires_at FROM checkpoint_leases WHERE thread_id = 't1'";
            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());
            return new JsonObject
            {
                ["owner"] = reader.IsDBNull(0) ? null : reader.GetString(0),
                ["token"] = reader.GetInt64(1),
                ["expires_at_type"] = reader.GetString(2),
                ["expires_at"] = reader.IsDBNull(3) ? null : reader.GetInt64(3),
            };
        }

        AssertJson(
            new JsonObject
            {
                ["owner"] = "worker-1",
                ["token"] = 1,
                ["expires_at_type"] = "integer",
                ["expires_at"] = lease.ExpiresAt.ToUnixTimeMilliseconds(),
            },
            Row());

        // A release keeps the row and its token, and clears who held it.
        await store.ReleaseLeaseAsync(lease);
        AssertJson(
            new JsonObject { ["owner"] = null, ["token"] = 1, ["expires_at_type"] = "null", ["expires_at"] = null },
            Row());
    }

    [Fact]
    public async Task OpeningASqliteLogFromBeforeLeasesCreatesTheTable()
    {
        using (var connection = OpenRaw())
        {
            using var create = connection.CreateCommand();
            create.CommandText = EfCoreCheckpointStore.CheckpointLogDdl + ";" + EfCoreCheckpointStore.CheckpointThreadsDdl;
            create.ExecuteNonQuery();
        }

        using var store = OpenSqlite();
        await store.SaveAsync(StoreContracts.Checkpoint("t-old", "2026-09-17T00:00:00Z"));

        Assert.Equal(1, (await store.TryAcquireLeaseAsync("t-old", "worker-1", Ttl)).Lease?.Token);
    }

    // --- Postgres ---

    [PostgresFact]
    public void PostgresLeaseTableMatchesTheSharedColumnFixture()
    {
        using var sandbox = new PostgresSandbox("lease_layout");
        var dataSource = sandbox.CreateDataSource();
        using (PostgresSandbox.Open(dataSource))
        {
        }

        AssertJson(Fixture("postgres-leases-columns.json"), PostgresStoreTests.Columns(dataSource, "checkpoint_leases"));
    }

    [PostgresFact]
    public async Task PostgresReadsLeasesAnotherWriterLeft()
    {
        using var sandbox = new PostgresSandbox("lease_rows");
        var dataSource = sandbox.CreateDataSource();
        using var store = PostgresSandbox.Open(dataSource);
        var fixture = Fixture("leases.rows.json");
        await using (var connection = await dataSource.OpenConnectionAsync())
        {
            InsertRows(connection, fixture, "to_timestamp(@p4 / 1000.0)");
        }

        await AssertReadsTheRows(store, fixture);
    }

    [PostgresFact]
    public async Task OpeningAPostgresLogFromBeforeLeasesCreatesTheTable()
    {
        using var sandbox = new PostgresSandbox("lease_migration");
        var dataSource = sandbox.CreateDataSource();
        using (var old = PostgresSandbox.Open(dataSource))
        {
            await old.SaveAsync(StoreContracts.Checkpoint("t-old", "2026-09-17T00:00:00Z"));
        }

        // A database from before leases has no lease table, and no record of what it holds either.
        await using (var drop = dataSource.CreateCommand("DROP TABLE checkpoint_leases, strata_schema_version"))
        {
            await drop.ExecuteNonQueryAsync();
        }

        using var store = PostgresSandbox.Open(dataSource);
        Assert.Equal(1, (await store.TryAcquireLeaseAsync("t-old", "worker-1", Ttl)).Lease?.Token);
    }

    // --- Redis ---

    /// <summary>The lease keys of thread <c>t1</c> under a prefix, in the shape of <c>redis-leases.json</c>'s steps.</summary>
    private static JsonNode LeaseKeys(RedisSandbox sandbox)
    {
        var hash = sandbox.Database.HashGetAll($"{sandbox.Prefix}:leases:t1");
        var token = sandbox.Database.StringGet($"{sandbox.Prefix}:lease-tokens:t1");
        return new JsonObject
        {
            ["leases:t1"] = hash.Length == 0
                ? null
                : new JsonObject(hash.OrderBy(e => (string)e.Name!).Select(e => KeyValuePair.Create((string)e.Name!, (JsonNode?)(string)e.Value!))),
            ["lease-tokens:t1"] = token.IsNull ? null : (string)token!,
        };
    }

    [RedisFact]
    public async Task ALeaseLeavesTheSharedRedisKeys()
    {
        using var sandbox = new RedisSandbox("lease_keys");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        var steps = Fixture("redis-leases.json")["steps"]!.AsArray();

        var first = (await store.TryAcquireLeaseAsync("t1", "worker-1", Ttl)).Lease!;
        AssertJson(steps[0]!["keys"], LeaseKeys(sandbox));

        // The hash expires with the lease; the counter never does.
        var ttl = await sandbox.Database.KeyTimeToLiveAsync($"{sandbox.Prefix}:leases:t1");
        Assert.InRange(ttl!.Value, TimeSpan.FromSeconds(30), Ttl);
        Assert.Null(await sandbox.Database.KeyTimeToLiveAsync($"{sandbox.Prefix}:lease-tokens:t1"));

        await store.ReleaseLeaseAsync(first);
        AssertJson(steps[1]!["keys"], LeaseKeys(sandbox));

        await store.TryAcquireLeaseAsync("t1", "worker-2", Ttl);
        AssertJson(steps[2]!["keys"], LeaseKeys(sandbox));
    }

    [RedisFact]
    public async Task RedisReadsALeaseAnotherWriterLeft()
    {
        using var sandbox = new RedisSandbox("lease_raw");
        var keys = Fixture("redis-leases.json")["steps"]![2]!["keys"]!;
        var lease = keys["leases:t1"]!.AsObject();
        await sandbox.Database.HashSetAsync(
            $"{sandbox.Prefix}:leases:t1",
            [.. lease.Select(field => new StackExchange.Redis.HashEntry(field.Key, field.Value!.GetValue<string>()))]);
        await sandbox.Database.KeyExpireAsync($"{sandbox.Prefix}:leases:t1", Ttl);
        await sandbox.Database.StringSetAsync($"{sandbox.Prefix}:lease-tokens:t1", keys["lease-tokens:t1"]!.GetValue<string>());

        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        var held = await store.GetLeaseAsync("t1");
        Assert.Equal(("worker-2", 2L), (held?.Owner, held?.Token));
        Assert.Equal("worker-2", (await store.TryAcquireLeaseAsync("t1", "worker-9", Ttl)).HeldBy?.Owner);

        // Once it is gone, the next lease continues the count.
        await sandbox.Database.KeyDeleteAsync($"{sandbox.Prefix}:leases:t1");
        Assert.Equal(3, (await store.TryAcquireLeaseAsync("t1", "worker-9", Ttl)).Lease?.Token);
    }

    /// <summary>The fixtures are JSON the Rust tests read too: keep them parseable as plain data.</summary>
    [Fact]
    public void TheLeaseFixturesAreWellFormed()
    {
        Assert.Equal(JsonValueKind.Array, Fixture("leases-columns.json").GetValueKind());
        Assert.Equal(JsonValueKind.Array, Fixture("postgres-leases-columns.json").GetValueKind());
        Assert.Equal(3, Fixture("leases.rows.json")["rows"]!.AsArray().Count);
        Assert.Equal(3, Fixture("redis-leases.json")["steps"]!.AsArray().Count);
    }
}
