using System.Text.Json.Nodes;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Tests.Backends;
using Microsoft.Extensions.Time.Testing;
using Npgsql;

namespace Datalium.Strata.Tests.Memory;

/// <summary>
/// The memory store (<c>design/45-Long-Term-Memory.md</c>): the shared cases against <see cref="InMemoryMemoryStore"/>,
/// the refusals the cases don't cover, the record's JSON form, and what needs no database of the Postgres store.
/// </summary>
public sealed class MemoryStoreTests
{
    public static IEnumerable<object[]> CaseNames => MemoryCases.Names;

    [Theory]
    [MemberData(nameof(CaseNames))]
    public Task TheInMemoryStorePassesTheSharedCase(string name) =>
        MemoryCases.RunAsync(name, (dimensions, clock) =>
        {
            var store = new InMemoryMemoryStore(dimensions, clock);
            return (store, store);
        });

    private static MemoryRecord Record(string id, params float[] vector) => new("n", "", id, vector, "m");

    public static TheoryData<MemoryRecord, string> RecordRefusals => new()
    {
        { Record("a", 1, 0, 0) with { Tenant = new string('t', 257) }, "the tenant is longer than 256 bytes" },
        { Record("a", 1, 0, 0) with { Namespace = new string('é', 129) }, "the namespace is longer than 256 bytes" },
        { Record(new string('i', 257), 1, 0, 0), "the id is longer than 256 bytes" },
        { Record("a", 1, float.NaN, 0), "the vector has a value that isn't finite" },
        { Record("a", 1, float.PositiveInfinity, 0), "the vector has a value that isn't finite" },
        {
            Record("a", 1, 0, 0) with { Metadata = new Dictionary<string, string> { [new string('k', 129)] = "v" } },
            $"the metadata key '{new string('k', 129)}' is longer than 128 bytes"
        },
    };

    [Theory]
    [MemberData(nameof(RecordRefusals))]
    public async Task ARecordIsRefusedBeforeAnythingIsWritten(MemoryRecord record, string message)
    {
        var store = new InMemoryMemoryStore(3);

        var refused = await Assert.ThrowsAsync<ArgumentException>(async () => await store.UpsertAsync(record));

        Assert.Equal(message, refused.Message);
        Assert.Null(refused.ParamName);
        Assert.Empty((await store.ListAsync(new MemoryQuery("n"))).Records);
    }

    [Fact]
    public async Task NamesAndKeysAtTheLimitAreAccepted()
    {
        var store = new InMemoryMemoryStore(3);
        var id = new string('i', 256);
        var key = new string('k', 128);

        await store.UpsertAsync(Record(id, 1, 0, 0) with
        {
            Tenant = new string('t', 256),
            Metadata = new Dictionary<string, string> { [key] = "v" },
        });

        Assert.NotNull(await store.GetAsync(new MemoryKey("n", new string('t', 256), id)));
    }

    [Fact]
    public async Task ListingsAndPurgesRefuseTheirLimitsAndCursors()
    {
        var store = new InMemoryMemoryStore(3);

        async Task<string> Refused(Func<Task> act) => (await Assert.ThrowsAsync<ArgumentException>(act)).Message;

        Assert.Equal("the limit is 0; it must be from 1 to 1000", await Refused(async () => await store.ListAsync(new MemoryQuery("n") { Limit = 0 })));
        Assert.Equal("the limit is 1001; it must be from 1 to 1000", await Refused(async () => await store.ListAsync(new MemoryQuery("n") { Limit = 1001 })));
        Assert.Equal("'nope' is not a memory listing cursor", await Refused(async () => await store.ListAsync(new MemoryQuery("n") { Cursor = "nope" })));
        Assert.Equal("the limit is 0; it must be at least 1", await Refused(async () => await store.PurgeAsync(new MemoryPurge(DateTimeOffset.UtcNow) { Limit = 0 })));
        Assert.Equal("the namespace is empty", await Refused(async () => await store.PurgeAsync(new MemoryPurge(DateTimeOffset.UtcNow) { Namespace = "" })));
        Assert.Equal(
            "the minimum similarity is -1.5; it must be from -1 to 1",
            await Refused(async () => await store.SearchAsync(new MemorySearch("n", new float[] { 1, 0, 0 }, "m") { MinSimilarity = -1.5 })));
        Assert.Equal(
            "the vector has 2 dimensions, and this store has 3",
            await Refused(async () => await store.SearchAsync(new MemorySearch("n", new float[] { 1, 0 }, "m"))));
        Assert.Equal(
            "the model is empty",
            await Refused(async () => await store.SearchAsync(new MemorySearch("n", new float[] { 1, 0, 0 }, ""))));
        Assert.Equal(
            "a metadata key is empty",
            await Refused(async () => await store.SearchAsync(new MemorySearch("n", new float[] { 1, 0, 0 }, "m")
            {
                Metadata = new Dictionary<string, string> { [""] = "x" },
            })));
    }

    [Fact]
    public void AStoreHasOneToSixteenThousandDimensions()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new InMemoryMemoryStore(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InMemoryMemoryStore(16_001));
        Assert.Equal(16_000, new InMemoryMemoryStore(16_000).Dimensions);
    }

    [Fact]
    public async Task WhatACallerChangesAfterwardsChangesNothingStored()
    {
        var store = new InMemoryMemoryStore(3);
        var vector = new float[] { 1, 0, 0 };
        var content = new JsonObject { ["cause"] = "pool" };
        var metadata = new Dictionary<string, string> { ["service"] = "checkout" };
        await store.UpsertAsync(new MemoryRecord("n", "", "a", vector, "m") { Content = content, Metadata = metadata });

        vector[0] = 0;
        vector[1] = 1;
        content["cause"] = "deploy";
        metadata["service"] = "payments";
        var got = (await store.GetAsync(new MemoryKey("n", "", "a")))!;
        got.Content!["cause"] = "changed again";

        var again = (await store.GetAsync(new MemoryKey("n", "", "a")))!;
        Assert.Equal([1f, 0f, 0f], again.Vector.ToArray());
        Assert.Equal("pool", again.Content!["cause"]!.GetValue<string>());
        Assert.Equal("checkout", again.Metadata["service"]);
    }

    [Fact]
    public async Task TimestampsAreTheStoresAtWholeSecondsInUtc()
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2026, 10, 21, 16, 2, 11, 750, TimeSpan.FromHours(2)));
        var store = new InMemoryMemoryStore(3, clock);

        await store.UpsertAsync(Record("a", 1, 0, 0) with { CreatedAt = DateTimeOffset.MinValue, UpdatedAt = DateTimeOffset.MinValue });

        var got = (await store.GetAsync(new MemoryKey("n", "", "a")))!;
        Assert.Equal(new DateTimeOffset(2026, 10, 21, 14, 2, 11, TimeSpan.Zero), got.CreatedAt);
        Assert.Equal(TimeSpan.Zero, got.CreatedAt!.Value.Offset);
        Assert.Equal(got.CreatedAt, got.UpdatedAt);
    }

    [Fact]
    public async Task APurgeComparesAtTheSecond()
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2026, 10, 1, 0, 0, 0, 900, TimeSpan.Zero));
        var store = new InMemoryMemoryStore(3, clock);
        await store.UpsertAsync(Record("a", 1, 0, 0));

        // Stored at 00:00:00; a cutoff in that same second keeps it, as stored text compares.
        Assert.Equal(0, await store.PurgeAsync(new MemoryPurge(new DateTimeOffset(2026, 10, 1, 0, 0, 0, 999, TimeSpan.Zero))));
        Assert.Equal(1, await store.PurgeAsync(new MemoryPurge(new DateTimeOffset(2026, 10, 1, 0, 0, 1, TimeSpan.Zero))));
    }

    [Fact]
    public void TheSampleRecordReadsAndWritesBackTheSame()
    {
        var sample = CheckpointQueryContractTests.Fixture(Path.Combine("memory", "record.sample.json"));

        var record = MemoryRecords.FromJson(sample);

        Assert.Equal(new MemoryKey("incidents", "acme", "0199f1c2-7a10-7c3e-9d41-2b6f0e8a5c11"), record.Key);
        Assert.Equal("openai/text-embedding-3-small", record.Model);
        Assert.Equal([0.125f, -0.5f, 0.25f, 0.75f], record.Vector.ToArray());
        Assert.Equal(new DateTimeOffset(2026, 10, 21, 14, 5, 47, TimeSpan.Zero), record.UpdatedAt);
        Assert.True(JsonNode.DeepEquals(sample, MemoryRecords.ToJson(record)), MemoryRecords.ToJson(record).ToJsonString());
    }

    [Fact]
    public void ARecordOfAnotherSchemaVersionIsRefused()
    {
        var sample = CheckpointQueryContractTests.Fixture(Path.Combine("memory", "record.sample.json"));
        sample["schema_version"] = "2.0.0";

        Assert.Throws<FormatException>(() => MemoryRecords.FromJson(sample));
    }

    [Fact]
    public void ACursorIsBase64UrlOfAJsonArrayOfTheId()
    {
        var cursor = MemoryRecords.EncodeCursor("restart-pod");

        Assert.Equal("WyJyZXN0YXJ0LXBvZCJd", cursor);
        Assert.Equal("restart-pod", MemoryRecords.DecodeCursor(cursor));
        Assert.Equal("ü/é", MemoryRecords.DecodeCursor(MemoryRecords.EncodeCursor("ü/é")));
    }

    [Theory]
    [InlineData(new float[] { 1, 0, 0 }, "[1,0,0]")]
    [InlineData(new float[] { -0.5f, 0.25f, 1.5e-7f }, "[-0.5,0.25,1.5E-07]")]
    [InlineData(new float[] { 0.1f }, "[0.1]")]
    public void AVectorIsWrittenAsPgvectorReadsIt(float[] vector, string text)
    {
        Assert.Equal(text, PgVector.Format(vector));
        Assert.Equal(vector, PgVector.Parse(text));
    }

    [Theory]
    [InlineData("[1,-0.5,0.25]", new float[] { 1, -0.5f, 0.25f })]
    [InlineData(" [ 1 , 2e-3 ] ", new float[] { 1, 0.002f })]
    [InlineData("[0.70710677]", new float[] { 0.70710677f })]
    [InlineData("[]", new float[0])]
    public void AVectorIsReadAsPgvectorWritesIt(string text, float[] vector) =>
        Assert.Equal(vector, PgVector.Parse(text));

    [Fact]
    public void EveryFloatRoundTripsThroughTheText()
    {
        var random = new Random(45);
        var vector = Enumerable.Range(0, 1000).Select(_ => (float)(random.NextDouble() * 2 - 1) * MathF.Pow(10, random.Next(-30, 30))).ToArray();
        vector[0] = float.MaxValue;
        vector[1] = float.Epsilon;
        vector[2] = -float.MinValue;

        Assert.Equal(vector, PgVector.Parse(PgVector.Format(vector)));
    }

    [Theory]
    [InlineData("1,2")]
    [InlineData("[1,,2]")]
    [InlineData("[1,x]")]
    public void TextThatIsNotAVectorIsRefused(string text) =>
        Assert.Throws<FormatException>(() => PgVector.Parse(text));

    [Fact]
    public void ThePostgresStoreRefusesBadOptionsWithoutTheDatabase()
    {
        using var dataSource = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1");

        Assert.Throws<ArgumentOutOfRangeException>(() => new PostgresMemoryStore(dataSource, new PostgresMemoryOptions { Dimensions = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PostgresMemoryStore(
            dataSource, new PostgresMemoryOptions { Dimensions = 2001, Index = MemoryIndex.Hnsw() }));
        Assert.Throws<ArgumentException>(() => new PostgresMemoryStore(
            dataSource, new PostgresMemoryOptions { Dimensions = 3, Table = "memory; DROP TABLE checkpoints" }));
        Assert.Throws<ArgumentException>(() => new PostgresMemoryStore(
            dataSource, new PostgresMemoryOptions { Dimensions = 3, Table = "Memory" }));
        Assert.Throws<ArgumentException>(() => new PostgresMemoryStore(
            dataSource, new PostgresMemoryOptions { Dimensions = 3, Table = "1memory" }));
        Assert.Throws<ArgumentException>(() => new PostgresMemoryStore(
            dataSource, new PostgresMemoryOptions { Dimensions = 3, Table = "a.b.c" }));

        // .NET's $ also matches before a final newline: the name must end where the text does.
        Assert.Throws<ArgumentException>(() => new PostgresMemoryStore(
            dataSource, new PostgresMemoryOptions { Dimensions = 3, Table = "memory\n" }));
        Assert.Throws<ArgumentException>(() => new PostgresMemoryStore(
            dataSource, new PostgresMemoryOptions { Dimensions = 3, Table = "ops.memory\n" }));
        Assert.Equal(768, new PostgresMemoryStore(dataSource, new PostgresMemoryOptions { Dimensions = 768, Table = "ops.facts_768" }).Dimensions);
        Assert.Throws<ArgumentOutOfRangeException>(() => MemoryIndex.Hnsw(m: 16, efConstruction: 31));
        Assert.Equal(2000, new PostgresMemoryStore(dataSource, new PostgresMemoryOptions { Dimensions = 2000, Index = MemoryIndex.Hnsw() }).Dimensions);
    }

    [Fact]
    public async Task ThePostgresStoreConstructorTouchesNoDatabase()
    {
        // Nothing listens on port 1: a constructor that connected would throw.
        await using var store = new PostgresMemoryStore(
            "Host=127.0.0.1;Port=1;Timeout=2",
            new PostgresMemoryOptions { Dimensions = 3, Retry = Storage.StoreRetryPolicy.None });

        // Input is refused before any I/O, and a call that needs the database fails as the store's.
        var refused = await Assert.ThrowsAsync<ArgumentException>(async () => await store.GetAsync(new MemoryKey("", "", "a")));
        Assert.Equal("the namespace is empty", refused.Message);
        var failed = await Assert.ThrowsAsync<MemoryStoreException>(async () => await store.GetAsync(new MemoryKey("n", "", "a")));
        Assert.NotNull(failed.InnerException);
    }
}
