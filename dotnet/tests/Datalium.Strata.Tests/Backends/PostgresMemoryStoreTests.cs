using System.Text.Json.Nodes;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Tests.Memory;
using Microsoft.Extensions.Time.Testing;
using Npgsql;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// <see cref="PostgresMemoryStore"/> against the live Postgres with pgvector (<c>design/45-Long-Term-Memory.md</c>):
/// the shared cases with no index and with HNSW, the layout <c>schema/fixtures/memory/postgres-columns.json</c> pins,
/// the <c>memory</c> schema-version component, and the refusals on open. Each test runs in a sandbox schema whose
/// search path also reaches <c>public</c>, where the extension lives.
/// </summary>
public sealed class PostgresMemoryStoreTests
{
    /// <summary>No waits between attempts: a test clock never advances on its own.</summary>
    private static readonly StoreRetryPolicy NoWait = new() { InitialDelay = TimeSpan.Zero, MaxDelay = TimeSpan.Zero };

    public static IEnumerable<object[]> CaseNames => MemoryCases.Names;

    private static void AssertJson(JsonNode? expected, JsonNode? actual) =>
        Assert.True(JsonNode.DeepEquals(expected, actual), $"expected {expected?.ToJsonString()}, got {actual?.ToJsonString()}");

    private static async Task RunCaseAsync(string name, MemoryIndex index)
    {
        // Labels stay short: the sandbox's schema name must fit Postgres's 63 characters.
        using var sandbox = new PostgresSandbox("mem");
        var dataSource = sandbox.CreateVectorDataSource();
        await MemoryCases.RunAsync(name, (dimensions, clock) =>
        {
            var store = new PostgresMemoryStore(dataSource, new PostgresMemoryOptions
            {
                Dimensions = dimensions,
                Index = index,
                TimeProvider = clock,
                Retry = NoWait,
            });
            return (store, store);
        });
    }

    [PostgresTheory]
    [MemberData(nameof(CaseNames))]
    public Task ExactSearchPassesTheSharedCase(string name) => RunCaseAsync(name, MemoryIndex.None);

    [PostgresTheory]
    [MemberData(nameof(CaseNames))]
    public Task AnHnswIndexPassesTheSharedCase(string name) => RunCaseAsync(name, MemoryIndex.Hnsw());

    private static PostgresMemoryStore Store(NpgsqlDataSource dataSource, int dimensions = 3, MemoryIndex? index = null, bool autoMigrate = true, string table = "memory_records") =>
        new(dataSource, new PostgresMemoryOptions
        {
            Dimensions = dimensions,
            Index = index ?? MemoryIndex.None,
            AutoMigrate = autoMigrate,
            Table = table,
            Retry = NoWait,
        });

    /// <summary>
    /// <see cref="PostgresStoreTests.Columns"/> for the memory table: pgvector's column is <c>USER-DEFINED</c> in
    /// <c>information_schema</c>, so its <c>udt_name</c> (<c>vector</c>) stands for its type, as the fixture says.
    /// </summary>
    internal static JsonNode Columns(NpgsqlDataSource dataSource, string table)
    {
        using var command = dataSource.CreateCommand(
            """
            SELECT c.column_name::text,
                   CASE WHEN c.data_type = 'USER-DEFINED' THEN c.udt_name::text ELSE c.data_type::text END,
                   c.is_nullable::text = 'NO',
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

    private static async Task<Dictionary<string, string>> IndexesAsync(NpgsqlDataSource dataSource, string table)
    {
        await using var command = dataSource.CreateCommand(
            "SELECT indexname::text, indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename = @table");
        command.Parameters.AddWithValue("table", table);
        var indexes = new Dictionary<string, string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            indexes[reader.GetString(0)] = reader.GetString(1);
        }

        return indexes;
    }

    private static async Task<long?> MemoryVersionAsync(NpgsqlDataSource dataSource)
    {
        await using var command = dataSource.CreateCommand(
            "SELECT version FROM strata_schema_version WHERE component = 'memory'");
        return await command.ExecuteScalarAsync() as long?;
    }

    [PostgresFact]
    public async Task TheTableMatchesTheSharedColumnFixture()
    {
        using var sandbox = new PostgresSandbox("memlay");
        var dataSource = sandbox.CreateVectorDataSource();
        await using var store = Store(dataSource);
        await store.MigrateAsync();

        AssertJson(CheckpointQueryContractTests.Fixture(Path.Combine("memory", "postgres-columns.json")), Columns(dataSource, "memory_records"));
        var indexes = await IndexesAsync(dataSource, "memory_records");
        Assert.Contains("(namespace, tenant, model)", indexes["memory_records_scope"], StringComparison.Ordinal);
        Assert.Contains("(updated_at)", indexes["memory_records_updated"], StringComparison.Ordinal);
        Assert.DoesNotContain("memory_records_embedding_hnsw", indexes.Keys);
        await using (var type = dataSource.CreateCommand(
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = to_regclass('memory_records') AND attname = 'embedding'"))
        {
            Assert.Equal("vector(3)", (string)(await type.ExecuteScalarAsync())!);
        }
    }

    [PostgresFact]
    public async Task ARowHoldsTheRecordAsTheLayoutSays()
    {
        using var sandbox = new PostgresSandbox("memrow");
        var dataSource = sandbox.CreateVectorDataSource();
        var clock = new FakeTimeProvider(new DateTimeOffset(2026, 10, 21, 14, 2, 11, 600, TimeSpan.Zero));
        await using var store = new PostgresMemoryStore(dataSource, new PostgresMemoryOptions { Dimensions = 4, TimeProvider = clock, Retry = NoWait });
        var sample = MemoryRecords.FromJson(CheckpointQueryContractTests.Fixture(Path.Combine("memory", "record.sample.json")));

        await store.UpsertAsync(sample);

        await using (var row = dataSource.CreateCommand(
            "SELECT embedding::text, content->>'action', metadata->>'service', created_at, updated_at FROM memory_records"))
        await using (var reader = await row.ExecuteReaderAsync())
        {
            Assert.True(await reader.ReadAsync());
            Assert.Equal("[0.125,-0.5,0.25,0.75]", reader.GetString(0));
            Assert.Equal("kubernetes.rollback", reader.GetString(1));
            Assert.Equal("checkout-api", reader.GetString(2));
            Assert.Equal("2026-10-21T14:02:11Z", reader.GetString(3));
            Assert.Equal("2026-10-21T14:02:11Z", reader.GetString(4));
        }

        var got = (await store.GetAsync(sample.Key))!;
        var expected = MemoryRecords.ToJson(sample with
        {
            CreatedAt = DateTimeOffset.Parse("2026-10-21T14:02:11Z"),
            UpdatedAt = DateTimeOffset.Parse("2026-10-21T14:02:11Z"),
        });
        AssertJson(expected, MemoryRecords.ToJson(got));
    }

    [PostgresFact]
    public async Task AFreshDatabaseHasTheMemoryComponentAtOne()
    {
        using var sandbox = new PostgresSandbox("memver");
        var dataSource = sandbox.CreateVectorDataSource();
        await using var store = Store(dataSource);
        await store.MigrateAsync();

        Assert.Equal(StoreSchemaVersions.MemoryVersion, await MemoryVersionAsync(dataSource));
        Assert.Equal(1L, StoreSchemaVersions.MemoryVersion);
    }

    [PostgresFact]
    public async Task ATableMadeWithOtherDimensionsIsRefused()
    {
        using var sandbox = new PostgresSandbox("memdims");
        var dataSource = sandbox.CreateVectorDataSource();
        await using (var made = Store(dataSource, dimensions: 768))
        {
            await made.MigrateAsync();
        }

        await using var other = Store(dataSource, dimensions: 1536);
        var refused = await Assert.ThrowsAsync<MemoryStoreException>(() => other.MigrateAsync());
        Assert.Equal("memory was created with 768 dimensions, and this store was opened with 1536", refused.Message);

        // On first use as well, and a store that doesn't migrate checks the same.
        var onUse = await Assert.ThrowsAsync<MemoryStoreException>(async () => await other.GetAsync(new MemoryKey("n", "", "a")));
        Assert.Equal(refused.Message, onUse.Message);
        await using var manual = Store(dataSource, dimensions: 1536, autoMigrate: false);
        var checkedOnly = await Assert.ThrowsAsync<MemoryStoreException>(async () => await manual.GetAsync(new MemoryKey("n", "", "a")));
        Assert.Equal(refused.Message, checkedOnly.Message);
    }

    [PostgresFact]
    public async Task ANewerDatabaseIsRefusedAndLeftAlone()
    {
        using var sandbox = new PostgresSandbox("memnew");
        var dataSource = sandbox.CreateVectorDataSource();
        await using (var made = Store(dataSource))
        {
            await made.MigrateAsync();
        }

        await using (var bump = dataSource.CreateCommand("UPDATE strata_schema_version SET version = 2 WHERE component = 'memory'"))
        {
            await bump.ExecuteNonQueryAsync();
        }

        await using var store = Store(dataSource);
        var refused = await Assert.ThrowsAsync<MemoryStoreException>(async () => await store.GetAsync(new MemoryKey("n", "", "a")));
        Assert.Equal("database is at memory schema version 2, and this build knows 1: upgrade the runtime", refused.Message);
        await Assert.ThrowsAsync<MemoryStoreException>(() => store.MigrateAsync());
        Assert.Equal(2L, await MemoryVersionAsync(dataSource));
    }

    [PostgresFact]
    public async Task AStoreThatDoesNotMigrateRefusesADatabaseThatIsBehind()
    {
        using var sandbox = new PostgresSandbox("memman");
        var dataSource = sandbox.CreateVectorDataSource();
        await using var store = Store(dataSource, autoMigrate: false);

        var refused = await Assert.ThrowsAsync<MemoryStoreException>(async () => await store.GetAsync(new MemoryKey("n", "", "a")));
        Assert.Equal("database is at memory schema version 0, and this build needs 1: run its migration", refused.Message);

        // It changed nothing.
        await using (var probe = dataSource.CreateCommand(
            "SELECT to_regclass(format('%I.strata_schema_version', current_schema())) IS NULL " +
            "AND to_regclass(format('%I.memory_records', current_schema())) IS NULL"))
        {
            Assert.True((bool)(await probe.ExecuteScalarAsync())!);
        }

        // A deploy step migrates, and the store then works.
        await store.MigrateAsync();
        Assert.Null(await store.GetAsync(new MemoryKey("n", "", "a")));
        await using var again = Store(dataSource, autoMigrate: false);
        Assert.Null(await again.GetAsync(new MemoryKey("n", "", "a")));

        // The component is at 1, but this store's own table isn't there: still a migration to run.
        await using var otherTable = Store(dataSource, autoMigrate: false, table: "memory_small");
        var missing = await Assert.ThrowsAsync<MemoryStoreException>(async () => await otherTable.GetAsync(new MemoryKey("n", "", "a")));
        Assert.Equal("the memory table 'memory_small' doesn't exist: run its migration", missing.Message);
    }

    [PostgresFact]
    public async Task TwoStoresOnTwoTablesShareTheComponentAndKeepTheirRecordsApart()
    {
        using var sandbox = new PostgresSandbox("memtab");
        var dataSource = sandbox.CreateVectorDataSource();
        await using var large = Store(dataSource, dimensions: 4);
        await using var small = Store(dataSource, dimensions: 2, table: "memory_small");

        await large.UpsertAsync(new MemoryRecord("n", "", "a", new float[] { 1, 0, 0, 0 }, "m"));
        await small.UpsertAsync(new MemoryRecord("n", "", "b", new float[] { 1, 0 }, "m"));

        Assert.Equal(["a"], (await large.ListAsync(new MemoryQuery("n"))).Records.Select(r => r.Id));
        Assert.Equal(["b"], (await small.ListAsync(new MemoryQuery("n"))).Records.Select(r => r.Id));
        Assert.Equal(1L, await MemoryVersionAsync(dataSource));
    }

    [PostgresFact]
    public async Task AnHnswIndexIsMadeWhenAsked()
    {
        using var sandbox = new PostgresSandbox("memhnsw");
        var dataSource = sandbox.CreateVectorDataSource();
        await using (var exact = Store(dataSource))
        {
            await exact.MigrateAsync();
            Assert.DoesNotContain("memory_records_embedding_hnsw", (await IndexesAsync(dataSource, "memory_records")).Keys);
        }

        // On a table that exists already too: the index isn't part of the component's version.
        await using var indexed = Store(dataSource, index: MemoryIndex.Hnsw(m: 8, efConstruction: 32));
        await indexed.UpsertAsync(new MemoryRecord("n", "", "a", new float[] { 1, 0, 0 }, "m"));

        var definition = (await IndexesAsync(dataSource, "memory_records"))["memory_records_embedding_hnsw"];
        Assert.Contains("USING hnsw (embedding vector_cosine_ops)", definition, StringComparison.Ordinal);
        Assert.Contains("m='8'", definition, StringComparison.Ordinal);
        Assert.Contains("ef_construction='32'", definition, StringComparison.Ordinal);
        Assert.Single(await indexed.SearchAsync(new MemorySearch("n", new float[] { 1, 0, 0 }, "m")));
    }

    [PostgresFact]
    public async Task AFilteredHnswSearchIsNotStarvedByTheIndex()
    {
        using var sandbox = new PostgresSandbox("memstarv");
        var dataSource = sandbox.CreateVectorDataSource();
        await using var store = Store(dataSource, index: MemoryIndex.Hnsw());

        // Many near records of another tenant, a few far ones of this one: the filter must not empty the result.
        var random = new Random(32);
        var others = Enumerable.Range(0, 400)
            .Select(i => new MemoryRecord("n", "other", $"o{i:D3}", new float[] { 1, (float)random.NextDouble() * 0.01f, 0.01f }, "m"))
            .ToList();
        await store.UpsertManyAsync(others);
        await store.UpsertManyAsync(
        [
            new MemoryRecord("n", "mine", "far-1", new float[] { 0, 1, 0 }, "m"),
            new MemoryRecord("n", "mine", "far-2", new float[] { 0, 0, 1 }, "m"),
        ]);
        await using (var analyze = dataSource.CreateCommand("ANALYZE memory_records"))
        {
            await analyze.ExecuteNonQueryAsync();
        }

        var matches = await store.SearchAsync(new MemorySearch("n", new float[] { 1, 0, 0 }, "m") { Tenant = "mine", Limit = 2 });

        Assert.Equal(["far-1", "far-2"], matches.Select(m => m.Record.Id));
    }

    [PostgresFact]
    public async Task UpsertManyWritesAllOrNothing()
    {
        using var sandbox = new PostgresSandbox("mematom");
        var dataSource = sandbox.CreateVectorDataSource();
        await using var store = Store(dataSource);
        await store.MigrateAsync();

        // Postgres TEXT can't hold a NUL: the second record fails in the server, after the first was sent.
        var failed = await Assert.ThrowsAsync<MemoryStoreException>(async () => await store.UpsertManyAsync(
        [
            new MemoryRecord("n", "", "good", new float[] { 1, 0, 0 }, "m"),
            new MemoryRecord("n", "", "bad", new float[] { 1, 0, 0 }, "m") { Text = "nul \0 here" },
        ]));

        Assert.StartsWith("memory upsert failed: ", failed.Message, StringComparison.Ordinal);
        Assert.Null(await store.GetAsync(new MemoryKey("n", "", "good")));
    }
}
