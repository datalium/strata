using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Storage.Sqlite;
using Datalium.Strata.Tests.Memory;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// <see cref="SqliteMemoryStore"/> with sqlite-vec (<c>design/45-Long-Term-Memory.md</c>, "SQLite and sqlite-vec"):
/// the shared cases on a file and in memory, the layout <c>schema/fixtures/memory/sqlite-columns.json</c> and
/// <c>sqlite-settings-columns.json</c> pin, the row form, the <c>memory</c> schema-version component, and the
/// refusals on open. The extension comes from the test project's <c>sqlite-vec</c> package reference
/// (<c>runtimes/&lt;rid&gt;/native/vec0.*</c>), found by name as an application's would be.
/// </summary>
public sealed class SqliteMemoryStoreTests : IDisposable
{
    private readonly string _directory = Directory.CreateTempSubdirectory("strata-memory-").FullName;

    public static IEnumerable<object[]> CaseNames => MemoryCases.Names;

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        try
        {
            Directory.Delete(_directory, recursive: true);
        }
        catch (IOException)
        {
            // Best-effort cleanup of temp files.
        }
    }

    private string NewPath() => Path.Combine(_directory, $"{Guid.NewGuid():N}.db");

    private static SqliteMemoryStore Store(string path, int dimensions = 3, string table = "memory_records", TimeProvider? clock = null, string? extension = null) =>
        new(path, new SqliteMemoryOptions
        {
            Dimensions = dimensions,
            Table = table,
            TimeProvider = clock,
            Extension = extension ?? "vec0",
        });

    private static void AssertJson(JsonNode? expected, JsonNode? actual) =>
        Assert.True(JsonNode.DeepEquals(expected, actual), $"expected {expected?.ToJsonString()}, got {actual?.ToJsonString()}");

    /// <summary>One connection to <paramref name="path"/> of the test's own, without pooling, so the file is let go.</summary>
    private static SqliteConnection Open(string path)
    {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Pooling = false }.ToString());
        connection.Open();
        return connection;
    }

    private static object? Scalar(string path, string sql)
    {
        using var connection = Open(path);
        using var command = new SqliteCommand(sql, connection);
        return command.ExecuteScalar();
    }

    [Theory]
    [MemberData(nameof(CaseNames))]
    public Task AFilePassesTheSharedCase(string name)
    {
        var path = NewPath();
        return MemoryCases.RunAsync(name, (dimensions, clock) =>
        {
            var store = Store(path, dimensions, clock: clock);
            return (store, store);
        });
    }

    [Theory]
    [MemberData(nameof(CaseNames))]
    public Task AnInMemoryDatabasePassesTheSharedCase(string name) =>
        MemoryCases.RunAsync(name, (dimensions, clock) =>
        {
            var store = SqliteMemoryStore.CreateInMemory(new SqliteMemoryOptions { Dimensions = dimensions, TimeProvider = clock });
            return (store, store);
        });

    [Fact]
    public async Task TheExtensionIsLoadedByNameAndScoresByCosine()
    {
        // What the store relies on, checked directly: vec0 from the package's native folder, and its distance.
        await using var connection = Open(":memory:");
        connection.LoadExtension("vec0");
        await using var command = new SqliteCommand("SELECT vec_distance_cosine($a, $b)", connection);
        command.Parameters.AddWithValue("$a", MemoryLayout.VectorBytes([1f, 0f, 0f]));
        command.Parameters.AddWithValue("$b", MemoryLayout.VectorBytes([0f, 1f, 0f]));
        Assert.Equal(1.0, (double)(await command.ExecuteScalarAsync())!, 6);
    }

    /// <summary><c>PRAGMA table_info</c> in the fixtures' form.</summary>
    internal static JsonNode Columns(string path, string table)
    {
        using var connection = Open(path);
        using var command = new SqliteCommand($"SELECT name, type, \"notnull\", pk FROM pragma_table_info('{table}') ORDER BY cid", connection);
        using var reader = command.ExecuteReader();
        var columns = new JsonArray();
        while (reader.Read())
        {
            columns.Add(new JsonObject
            {
                ["name"] = reader.GetString(0),
                ["type"] = reader.GetString(1),
                ["notnull"] = reader.GetInt64(2) != 0,
                ["pk"] = reader.GetInt64(3),
            });
        }

        return columns;
    }

    [Fact]
    public async Task TheTablesMatchTheSharedColumnFixtures()
    {
        var path = NewPath();
        await using (var store = Store(path))
        {
            await store.MigrateAsync();
        }

        AssertJson(CheckpointQueryContractTests.Fixture(Path.Combine("memory", "sqlite-columns.json")), Columns(path, "memory_records"));
        AssertJson(
            CheckpointQueryContractTests.Fixture(Path.Combine("memory", "sqlite-settings-columns.json")),
            Columns(path, SqliteMemoryStore.SettingsTable));
        Assert.Equal(3L, Scalar(path, "SELECT dimensions FROM memory_settings WHERE table_name = 'memory_records'"));
        Assert.Equal(
            "memory_records_scope,memory_records_updated",
            Scalar(path, "SELECT group_concat(name, ',') FROM (SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'memory_records' AND sql IS NOT NULL ORDER BY name)"));
        Assert.Equal(StoreSchemaVersions.MemoryVersion, Scalar(path, "SELECT version FROM strata_schema_version WHERE component = 'memory'"));
    }

    [Fact]
    public async Task ARowHoldsTheRecordAsTheLayoutSays()
    {
        var path = NewPath();
        var clock = new FakeTimeProvider(DateTimeOffset.Parse("2026-10-21T14:02:11.600Z"));
        await using var store = Store(path, dimensions: 4, clock: clock);
        var layout = CheckpointQueryContractTests.Fixture(Path.Combine("memory", "redis-record.json"));
        var sample = MemoryRecords.FromJson(CheckpointQueryContractTests.Fixture(Path.Combine("memory", "record.sample.json")));

        await store.UpsertAsync(sample);
        clock.SetUtcNow(DateTimeOffset.Parse("2026-10-21T14:05:47Z"));
        await store.UpsertAsync(sample);

        await using (var connection = Open(path))
        await using (var row = new SqliteCommand(
            "SELECT hex(embedding), content, text, metadata, created_at, updated_at, typeof(embedding) FROM memory_records", connection))
        await using (var reader = await row.ExecuteReaderAsync())
        {
            Assert.True(await reader.ReadAsync());
            var fields = layout["hash"]!["fields"]!;
            Assert.Equal(fields["embedding"]!.GetValue<string>(), reader.GetString(0).ToLowerInvariant());
            Assert.Equal(fields["content"]!.GetValue<string>(), reader.GetString(1));
            Assert.Equal(fields["text"]!.GetValue<string>(), reader.GetString(2));
            Assert.Equal(fields["metadata"]!.GetValue<string>(), reader.GetString(3));
            Assert.Equal("2026-10-21T14:02:11Z", reader.GetString(4));
            Assert.Equal("2026-10-21T14:05:47Z", reader.GetString(5));
            Assert.Equal("blob", reader.GetString(6));
            Assert.False(await reader.ReadAsync());
        }

        var got = (await store.GetAsync(sample.Key))!;
        AssertJson(
            MemoryRecords.ToJson(sample with
            {
                CreatedAt = DateTimeOffset.Parse("2026-10-21T14:02:11Z"),
                UpdatedAt = DateTimeOffset.Parse("2026-10-21T14:05:47Z"),
            }),
            MemoryRecords.ToJson(got));
    }

    [Fact]
    public async Task NothingTouchesTheFileUntilTheStoreIsUsed()
    {
        var path = NewPath();
        await using (Store(path))
        {
            Assert.False(File.Exists(path));
        }

        Assert.False(File.Exists(path));
    }

    [Fact]
    public async Task ATableMadeWithOtherDimensionsIsRefused()
    {
        var path = NewPath();
        await using (var made = Store(path, dimensions: 768))
        {
            await made.MigrateAsync();
        }

        await using var other = Store(path, dimensions: 1536);
        var refused = await Assert.ThrowsAsync<MemoryStoreException>(() => other.MigrateAsync());
        Assert.Equal("memory was created with 768 dimensions, and this store was opened with 1536", refused.Message);
        var onUse = await Assert.ThrowsAsync<MemoryStoreException>(async () => await other.GetAsync(new MemoryKey("n", "", "a")));
        Assert.Equal(refused.Message, onUse.Message);
        Assert.Equal(768L, Scalar(path, "SELECT dimensions FROM memory_settings WHERE table_name = 'memory_records'"));
    }

    [Fact]
    public async Task ANewerDatabaseIsRefusedAndLeftAlone()
    {
        var path = NewPath();
        await using (var made = Store(path))
        {
            await made.MigrateAsync();
        }

        Scalar(path, "UPDATE strata_schema_version SET version = 2 WHERE component = 'memory'");

        // On another table too: the refusal comes before anything is made.
        await using var store = Store(path, table: "memory_small");
        var refused = await Assert.ThrowsAsync<MemoryStoreException>(async () => await store.GetAsync(new MemoryKey("n", "", "a")));
        Assert.Equal("database is at memory schema version 2, and this build knows 1: upgrade the runtime", refused.Message);
        Assert.Equal(2L, Scalar(path, "SELECT version FROM strata_schema_version WHERE component = 'memory'"));
        Assert.Equal(0L, Scalar(path, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'memory_small'"));
    }

    [Fact]
    public async Task AMissingExtensionIsRefusedInWords()
    {
        var path = NewPath();
        await using var store = Store(path, extension: "no_such_sqlite_extension");

        var refused = await Assert.ThrowsAsync<MemoryStoreException>(async () =>
            await store.UpsertAsync(new MemoryRecord("n", "", "a", new float[] { 1, 0, 0 }, "m")));

        Assert.StartsWith("memory on SQLite needs the sqlite-vec extension: ", refused.Message, StringComparison.Ordinal);
        Assert.IsType<SqliteException>(refused.InnerException);

        // Nothing was made: the extension is loaded before the table.
        Assert.Equal(0L, Scalar(path, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'memory_records'"));
    }

    [Fact]
    public async Task TwoStoresOnTwoTablesShareTheFileAndTheComponent()
    {
        var path = NewPath();
        await using var large = Store(path, dimensions: 4);
        await using var small = Store(path, dimensions: 2, table: "memory_small");

        await large.UpsertAsync(new MemoryRecord("n", "", "a", new float[] { 1, 0, 0, 0 }, "m"));
        await small.UpsertAsync(new MemoryRecord("n", "", "b", new float[] { 1, 0 }, "m"));

        Assert.Equal(["a"], (await large.ListAsync(new MemoryQuery("n"))).Records.Select(r => r.Id));
        Assert.Equal(["b"], (await small.SearchAsync(new MemorySearch("n", new float[] { 0, 1 }, "m"))).Select(m => m.Record.Id));
        Assert.Equal(2L, Scalar(path, "SELECT COUNT(*) FROM memory_settings"));
        Assert.Equal(1L, Scalar(path, "SELECT COUNT(*) FROM strata_schema_version WHERE component = 'memory'"));
    }

    [Fact]
    public async Task AFileSharedWithTheCheckpointStoreKeepsBothComponents()
    {
        var path = NewPath();
        await using var memory = Store(path);
        await memory.UpsertAsync(new MemoryRecord("n", "", "a", new float[] { 1, 0, 0 }, "m"));

        var options = new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={path};Pooling=False").Options;
        using (var checkpoints = new EfCoreCheckpointStore(new CheckpointDbContext(options)))
        {
            await checkpoints.SaveAsync(new Checkpoint
            {
                SchemaVersion = "1.0.0",
                ThreadId = "t",
                Status = CheckpointStatus.Interrupted,
                SuspendedNode = "A",
                NextNode = "B",
                FrozenState = JsonNode.Parse("""{"x":1}"""),
                CreatedAt = "2026-10-21T00:00:00Z",
            });
        }

        Assert.Equal(StoreSchemaVersions.MemoryVersion, Scalar(path, "SELECT version FROM strata_schema_version WHERE component = 'memory'"));
        Assert.Equal(StoreSchemaVersions.CheckpointsVersion, Scalar(path, "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'"));
        Assert.Single(await memory.SearchAsync(new MemorySearch("n", new float[] { 1, 0, 0 }, "m")));
    }

    [Fact]
    public async Task UpsertManyWritesAllOrNothing()
    {
        var path = NewPath();
        await using var store = Store(path);
        await store.MigrateAsync();
        Scalar(path, "CREATE TRIGGER refuse_bad BEFORE INSERT ON memory_records WHEN NEW.id = 'bad' BEGIN SELECT RAISE(ABORT, 'refused'); END");

        var failed = await Assert.ThrowsAsync<MemoryStoreException>(async () => await store.UpsertManyAsync(
        [
            new MemoryRecord("n", "", "good", new float[] { 1, 0, 0 }, "m"),
            new MemoryRecord("n", "", "bad", new float[] { 1, 0, 0 }, "m"),
        ]));

        Assert.StartsWith("memory upsert failed: ", failed.Message, StringComparison.Ordinal);
        Assert.Null(await store.GetAsync(new MemoryKey("n", "", "good")));
    }

    [Fact]
    public async Task MetadataKeysWithQuotesAndDotsFilterByEquality()
    {
        await using var store = SqliteMemoryStore.CreateInMemory(new SqliteMemoryOptions { Dimensions = 3 });
        var odd = new Dictionary<string, string> { ["a.b"] = "1", ["say \"hi\""] = "é" };
        await store.UpsertAsync(new MemoryRecord("n", "", "odd", new float[] { 1, 0, 0 }, "m") { Metadata = odd });
        await store.UpsertAsync(new MemoryRecord("n", "", "plain", new float[] { 1, 0, 0 }, "m") { Metadata = new Dictionary<string, string> { ["a"] = "1" } });

        var found = await store.SearchAsync(new MemorySearch("n", new float[] { 1, 0, 0 }, "m") { Metadata = new Dictionary<string, string> { ["say \"hi\""] = "é" } });
        Assert.Equal(["odd"], found.Select(m => m.Record.Id));
        var listed = await store.ListAsync(new MemoryQuery("n") { Metadata = new Dictionary<string, string> { ["a.b"] = "1" } });
        Assert.Equal(["odd"], listed.Records.Select(r => r.Id));
        Assert.Equal(odd, listed.Records[0].Metadata);
    }

    [Fact]
    public async Task IdsAreOrderedByTheirBytes()
    {
        await using var store = SqliteMemoryStore.CreateInMemory(new SqliteMemoryOptions { Dimensions = 3 });

        // UTF-16 order puts the surrogate pair first; byte (code point) order puts U+E000 first.
        foreach (var id in (string[])["\U0001F600", "", "b"])
        {
            await store.UpsertAsync(new MemoryRecord("n", "", id, new float[] { 1, 0, 0 }, "m"));
        }

        Assert.Equal(["b", "", "\U0001F600"], (await store.ListAsync(new MemoryQuery("n"))).Records.Select(r => r.Id));
        Assert.Equal(["b", "", "\U0001F600"], (await store.SearchAsync(new MemorySearch("n", new float[] { 1, 0, 0 }, "m"))).Select(m => m.Record.Id));
    }

    [Fact]
    public void BadOptionsAreRefusedInTheConstructor()
    {
        Assert.Throws<ArgumentException>(() => Store(NewPath(), table: "Memory"));
        Assert.Throws<ArgumentException>(() => Store(NewPath(), table: "memory\n"));
        Assert.Throws<ArgumentException>(() => Store(NewPath(), table: "main.memory"));
        Assert.Throws<ArgumentOutOfRangeException>(() => Store(NewPath(), dimensions: 0));
        Assert.Throws<ArgumentException>(() => Store(NewPath(), extension: ""));
    }

    [Fact]
    public void APathIsOpenedWithoutPoolingAndAConnectionStringAsItIs()
    {
        Assert.Equal("Data Source=x.db;Pooling=False", SqliteMemoryStore.ConnectionString("x.db"));
        Assert.Equal("Data Source=:memory:;Pooling=False", SqliteMemoryStore.ConnectionString(":memory:"));
        Assert.Equal("Data Source=x.db;Cache=Shared", SqliteMemoryStore.ConnectionString("Data Source=x.db;Cache=Shared"));
    }
}
