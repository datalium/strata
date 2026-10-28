using System.Globalization;
using System.Text.Json.Nodes;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Storage.Redis;
using Datalium.Strata.Tests.Memory;
using Microsoft.Extensions.Time.Testing;
using StackExchange.Redis;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// <see cref="RedisMemoryStore"/> (<c>design/45-Long-Term-Memory.md</c>, "Redis"): without a server, the key
/// escaping, the tags, the hash's fields, the <c>FT.CREATE</c> arguments and the error classification against
/// <c>schema/fixtures/memory/redis-record.json</c>, and both shapes of an <c>FT.SEARCH</c> reply; with the live Redis
/// 8 (<c>STRATA_TEST_REDIS_URL</c>), the shared cases with FLAT and HNSW, the layout the fixture pins, and the
/// refusals on open. Each live test works under a prefix of its own, whose keys and index are removed afterwards.
/// </summary>
public sealed class RedisMemoryStoreTests
{
    /// <summary>No waits between attempts: a test clock never advances on its own.</summary>
    private static readonly StoreRetryPolicy NoWait = new() { InitialDelay = TimeSpan.Zero, MaxDelay = TimeSpan.Zero };

    public static IEnumerable<object[]> CaseNames => MemoryCases.Names;

    private static JsonNode Layout() => CheckpointQueryContractTests.Fixture(Path.Combine("memory", "redis-record.json"));

    private static MemoryRecord Sample(JsonNode layout) => MemoryRecords.FromJson(layout["record"]!);

    private static void AssertJson(JsonNode? expected, JsonNode? actual) =>
        Assert.True(JsonNode.DeepEquals(expected, actual), $"expected {expected?.ToJsonString()}, got {actual?.ToJsonString()}");

    /// <summary>
    /// Hash fields as the fixture gives them: the embedding as hex, <c>content</c> and <c>metadata</c> parsed (they are
    /// compared as JSON), every other value as text.
    /// </summary>
    private static JsonObject Fields(IEnumerable<(string Field, RedisValue Value)> fields)
    {
        var json = new JsonObject();
        foreach (var (field, value) in fields)
        {
            json[field] = field switch
            {
                "embedding" => Convert.ToHexStringLower((byte[])value!),
                "content" or "metadata" => JsonNode.Parse((string)value!),
                _ => (string?)value,
            };
        }

        return json;
    }

    private static JsonObject FixtureFields(JsonNode layout)
    {
        var json = layout["hash"]!["fields"]!.DeepClone().AsObject();
        json["content"] = JsonNode.Parse(json["content"]!.GetValue<string>());
        json["metadata"] = JsonNode.Parse(json["metadata"]!.GetValue<string>());
        return json;
    }

    [Fact]
    public void KeyPartsAreEscapedAsTheFixtureSays()
    {
        var layout = Layout();
        foreach (var pair in layout["escaped_key_parts"]!.AsArray())
        {
            Assert.Equal(pair!["escaped"]!.GetValue<string>(), RedisMemoryStore.Escape(pair["part"]!.GetValue<string>()));
        }

        var sample = Sample(layout);
        Assert.Equal("strata:" + layout["hash"]!["key"]!.GetValue<string>(), RedisMemoryStore.RecordKey("strata", sample.Key));
        Assert.Equal("strata:memory:ids:incidents:acme", RedisMemoryStore.IdsKey("strata", sample.Namespace, sample.Tenant));
        Assert.Equal("strata:memory:records:a%3Ab%25c:%25:x%3A%3A", RedisMemoryStore.RecordKey("strata", new MemoryKey("a:b%c", "%", "x::")));
    }

    [Fact]
    public void TagsAreHexOfTheValuesBytes()
    {
        var layout = Layout();
        var sample = Sample(layout);
        var fields = layout["hash"]!["fields"]!;

        Assert.Equal(fields["_scope"]!.GetValue<string>(), RedisMemoryStore.ScopeTag(sample.Namespace, sample.Tenant, sample.Model));
        Assert.Equal(fields["_id"]!.GetValue<string>(), RedisMemoryStore.Tag(sample.Id));
        Assert.Equal(fields["_meta"]!.GetValue<string>(), RedisMemoryStore.MetaTags(sample.Metadata));
        Assert.Equal("x", RedisMemoryStore.Tag(""));
        Assert.Equal("xc3a9", RedisMemoryStore.Tag("é"));
    }

    [Fact]
    public void AHashHoldsTheFixturesFields()
    {
        var layout = Layout();
        var sample = Sample(layout);
        var updated = layout["updated_at"]!.GetValue<string>();

        // The upsert script adds created_at, which it keeps from the record's first write.
        var fields = RedisMemoryStore.HashFields(sample, updated);
        fields.Add(("created_at", layout["created_at"]!.GetValue<string>()));

        AssertJson(FixtureFields(layout), Fields(fields));

        // A record without content, text or metadata leaves those fields out, and writes metadata as {}.
        var bare = RedisMemoryStore.HashFields(new MemoryRecord("n", "", "a", new float[] { 1, 0 }, "m"), updated)
            .ToDictionary(field => field.Field, field => field.Value);
        Assert.False(bare.ContainsKey("content"));
        Assert.False(bare.ContainsKey("text"));
        Assert.False(bare.ContainsKey("_meta"));
        Assert.Equal("{}", (string?)bare["metadata"]);
    }

    [Fact]
    public void TheUpdatedScoreIsTheTimestampsDigits()
    {
        var layout = Layout();
        var expected = layout["sets"]!["memory:updated"]![0]![1]!.GetValue<long>();
        Assert.Equal(expected, RedisMemoryStore.Score(layout["updated_at"]!.GetValue<string>()));
        Assert.Equal(expected, (long)(double)RedisMemoryStore.UpsertArguments(Sample(layout), layout["updated_at"]!.GetValue<string>())[2]);
    }

    [Fact]
    public void FtCreateTakesTheFixturesArguments()
    {
        var layout = Layout();
        var flat = RedisMemoryStore.FtCreateArguments("strata", 4, MemoryIndex.None).Select(a => (string)a);
        Assert.Equal(layout["ft_create"]!.AsArray().Select(a => a!.GetValue<string>()), flat);

        var hnsw = RedisMemoryStore.FtCreateArguments("strata", 4, MemoryIndex.Hnsw()).Select(a => (string)a).ToList();
        Assert.Equal(
            layout["ft_create_hnsw"]!.AsArray().Select(a => a!.GetValue<string>()),
            hnsw.Skip(hnsw.IndexOf("embedding")));
    }

    private static RedisServerException ServerError(string message) => new(RedisErrorKind.Unknown, CommandFlags.None, message);

    [Fact]
    public void ServerErrorsAreClassified()
    {
        var unknown = ServerError("ERR unknown command 'FT.CREATE', with args beginning with: 'strata:memory:idx' 'ON' ");
        Assert.True(RedisMemoryStore.UnknownCommand(unknown));
        Assert.False(RedisMemoryStore.IndexExists(unknown));

        var exists = ServerError("Index already exists");
        Assert.True(RedisMemoryStore.IndexExists(exists));
        Assert.False(RedisMemoryStore.UnknownCommand(exists));

        var other = ServerError("ERR Invalid field type for field `embedding`");
        Assert.False(RedisMemoryStore.IndexExists(other));
        Assert.False(RedisMemoryStore.UnknownCommand(other));

        Assert.Equal("memory on Redis needs the Query Engine (Redis 8 or Redis Stack): FT.CREATE is unknown", RedisMemoryStore.NoQueryEngine);
    }

    [Fact]
    public void ASearchQueryCarriesEveryFilter()
    {
        var search = new MemorySearch("n", new float[] { 1, 0 }, "m")
        {
            Tenant = "t",
            Exclude = ["a", "b", "a"],
            Metadata = new Dictionary<string, string> { ["z"] = "1", ["k"] = "v" },
        };

        Assert.Equal(
            $"(@_scope:{{{RedisMemoryStore.ScopeTag("n", "t", "m")}}} -@_id:{{x61|x62}} @_meta:{{x6bx76}} @_meta:{{x7ax31}})=>[KNN $k @embedding $vec AS __distance]",
            RedisMemoryStore.SearchQuery(search));
        Assert.Equal(
            "(@_scope:{x6exx6d})=>[KNN $k @embedding $vec AS __distance]",
            RedisMemoryStore.SearchQuery(new MemorySearch("n", new float[] { 1, 0 }, "m")));
    }

    [Fact]
    public void AConfirmationQueryCarriesTheRecordsTagsAndItsOwnVector()
    {
        var record = new MemoryRecord("n", "t", "a", new float[] { 1, 0 }, "m")
        {
            Metadata = new Dictionary<string, string> { ["z"] = "1", ["k"] = "v" },
        };

        Assert.Equal(
            $"(@_scope:{{{RedisMemoryStore.ScopeTag("n", "t", "m")}}} @_id:{{x61}} @_meta:{{x6bx76}} @_meta:{{x7ax31}})=>[KNN 1 @embedding $vec AS __distance]",
            RedisMemoryStore.IndexedQuery(record));
        Assert.Equal(
            "(@_scope:{x6exx6d} @_id:{x62})=>[KNN 1 @embedding $vec AS __distance]",
            RedisMemoryStore.IndexedQuery(new MemoryRecord("n", "", "b", new float[] { 1, 0 }, "m")));

        var vector = MemoryLayout.VectorBytes(record.Vector.Span);
        var arguments = RedisMemoryStore.IndexedArguments("p:memory:idx", "(q)", vector);
        Assert.Same(vector, arguments[5]);
        Assert.Equal(
            ["p:memory:idx", "(q)", "PARAMS", "2", "vec", "<vec>", "RETURN", "1", "__distance", "LIMIT", "0", "1", "TIMEOUT", "0", "DIALECT", "2"],
            arguments.Select(a => a as string ?? "<vec>"));
    }

    [Fact]
    public void ConfirmationRoundsWaitOneMillisecondDoublingToFifty()
    {
        Assert.Equal(
            [1.0, 2, 4, 8, 16, 32, 50, 50, 50],
            Enumerable.Range(1, 9).Select(round => RedisMemoryStore.IndexingWait(round).TotalMilliseconds));
        Assert.Equal(TimeSpan.FromMilliseconds(50), RedisMemoryStore.IndexingWait(1_000));
        Assert.Equal(
            "memory on Redis didn't index record 'r1' within 5 s of writing it",
            RedisMemoryStore.NotIndexed("r1"));
    }

    [Fact]
    public void ARecordIsIndexedWhenTheReplyHasItsKeyAtItsOwnVector()
    {
        static RedisResult Reply(string key, string distance) => RedisResult.Create(
        [
            RedisResult.Create((RedisValue)1),
            Bulk(key), RedisResult.Create([Bulk("__distance"), Bulk(distance)], ResultType.Array),
        ], ResultType.Array);

        Assert.True(RedisMemoryStore.Indexed(Reply("p:memory:records:n::a", "5.96e-08"), "p:memory:records:n::a"));
        Assert.True(RedisMemoryStore.Indexed(Reply("p:memory:records:n::a", "0.0001"), "p:memory:records:n::a"));
        // An older vector the update replaced is still in the index.
        Assert.False(RedisMemoryStore.Indexed(Reply("p:memory:records:n::a", "0.29"), "p:memory:records:n::a"));
        Assert.False(RedisMemoryStore.Indexed(Reply("p:memory:records:n::b", "0"), "p:memory:records:n::a"));
        Assert.False(RedisMemoryStore.Indexed(
            RedisResult.Create([RedisResult.Create((RedisValue)0)], ResultType.Array), "p:memory:records:n::a"));
    }

    private static RedisResult Bulk(string value) => RedisResult.Create((RedisValue)value);

    private static RedisResult[] DocumentFields(float x) =>
    [
        Bulk("id"), Bulk("a"),
        Bulk("embedding"), RedisResult.Create((RedisValue)MemoryLayout.VectorBytes([x, 1f])),
        Bulk("__distance"), Bulk(x.ToString(CultureInfo.InvariantCulture)),
    ];

    [Fact]
    public void AResp2SearchReplyIsParsed()
    {
        var reply = RedisResult.Create(
        [
            RedisResult.Create((RedisValue)2),
            Bulk("p:memory:records:n::a"), RedisResult.Create(DocumentFields(0.25f), ResultType.Array),
            Bulk("p:memory:records:n::b"), RedisResult.Create(DocumentFields(0.5f), ResultType.Array),
        ], ResultType.Array);

        var hits = RedisMemoryStore.ParseSearchReply(reply);

        Assert.Equal(["p:memory:records:n::a", "p:memory:records:n::b"], hits.Select(h => h.Key));
        Assert.Equal([0.25, 0.5], hits.Select(h => h.Distance));
        Assert.Equal(["id", "embedding"], hits[0].Fields.Select(f => (string)f.Name!));
        Assert.Equal(new float[] { 0.5f, 1f }, MemoryLayout.Vector((byte[])hits[1].Fields[1].Value!));
        Assert.Empty(RedisMemoryStore.ParseSearchReply(RedisResult.Create([RedisResult.Create((RedisValue)0)], ResultType.Array)));
    }

    [Fact]
    public void ANilDocumentInAResp2ReplyIsKeptWithoutFieldsAndNotTakenForAKey()
    {
        // A document deleted while the query ran comes back as its key and a nil in place of its fields.
        var reply = RedisResult.Create(
        [
            RedisResult.Create((RedisValue)3),
            Bulk("p:memory:records:n::a"), RedisResult.Create(DocumentFields(0.25f), ResultType.Array),
            Bulk("p:memory:records:n::gone"), RedisResult.Create(RedisValue.Null),
            Bulk("p:memory:records:n::b"), RedisResult.Create(DocumentFields(0.5f), ResultType.Array),
        ], ResultType.Array);

        var hits = RedisMemoryStore.ParseSearchReply(reply);

        Assert.Equal(["p:memory:records:n::a", "p:memory:records:n::gone", "p:memory:records:n::b"], hits.Select(h => h.Key));
        Assert.Empty(hits[1].Fields);
        Assert.Equal(double.PositiveInfinity, hits[1].Distance);
        Assert.Equal(0.5, hits[2].Distance);
    }

    [Fact]
    public void FtSearchAsksForNoTimeoutAndOnlyTheDistance()
    {
        var arguments = RedisMemoryStore.SearchArguments("p:memory:idx", "(q)", [1, 2], 6)
            .Select(a => a as string ?? "<vec>")
            .ToList();

        // Not the documents' fields: a reply with them can leave documents out on Redis 8, so the store reads the
        // hashes itself.
        Assert.Equal(
            [
                "p:memory:idx", "(q)", "PARAMS", "4", "k", "6", "vec", "<vec>", "SORTBY", "__distance", "ASC",
                "RETURN", "1", "__distance",
                "LIMIT", "0", "6", "TIMEOUT", "0", "DIALECT", "2",
            ],
            arguments);
    }

    [Fact]
    public void AResp3SearchReplyIsParsed()
    {
        RedisResult Document(string key, float x) => RedisResult.Create(
        [
            Bulk("id"), Bulk(key),
            Bulk("extra_attributes"), RedisResult.Create(DocumentFields(x), ResultType.Map),
            Bulk("values"), RedisResult.Create(Array.Empty<RedisResult>(), ResultType.Array),
        ], ResultType.Map);

        var reply = RedisResult.Create(
        [
            Bulk("attributes"), RedisResult.Create(Array.Empty<RedisResult>(), ResultType.Array),
            Bulk("format"), Bulk("STRING"),
            Bulk("results"), RedisResult.Create([Document("k1", 0.125f), Document("k2", 0.75f)], ResultType.Array),
            Bulk("total_results"), RedisResult.Create((RedisValue)2),
            Bulk("warning"), RedisResult.Create(Array.Empty<RedisResult>(), ResultType.Array),
        ], ResultType.Map);

        var hits = RedisMemoryStore.ParseSearchReply(reply);

        Assert.Equal(["k1", "k2"], hits.Select(h => h.Key));
        Assert.Equal([0.125, 0.75], hits.Select(h => h.Distance));
        Assert.Equal(2, hits[1].Fields.Length);
    }

    [Fact]
    public void NothingIsSentUntilTheStoreIsUsed()
    {
        // A multiplexer that never connected: a constructor that sent anything would fail here.
        var options = RedisConnection.ToConfigurationOptions("redis://127.0.0.1:1");
        options.AbortOnConnectFail = false;
        options.ConnectTimeout = 100;
        using var connection = ConnectionMultiplexer.Connect(options);
        using var store = new RedisMemoryStore(connection, new RedisMemoryOptions { Dimensions = 3 });
        Assert.Equal(3, store.Dimensions);
        Assert.Throws<ArgumentException>(() => new RedisMemoryStore(connection, new RedisMemoryOptions { Dimensions = 3, Prefix = "" }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RedisMemoryStore(connection, new RedisMemoryOptions { Dimensions = 0 }));
    }

    /// <summary>A prefix of a test's own on the live Redis; its index and keys are removed on dispose.</summary>
    private sealed class MemorySandbox(string label) : IDisposable
    {
        public RedisSandbox Redis { get; } = new(label);

        public string Prefix => Redis.Prefix;

        public RedisMemoryStore Store(int dimensions, MemoryIndex? index = null, TimeProvider? clock = null) =>
            new(Redis.Connection, new RedisMemoryOptions
            {
                Dimensions = dimensions,
                Prefix = Prefix,
                Index = index ?? MemoryIndex.None,
                TimeProvider = clock,
                Retry = NoWait,
            });

        public void Dispose()
        {
            try
            {
                Redis.Database.Execute("FT.DROPINDEX", $"{Prefix}:memory:idx");
            }
            catch (RedisServerException)
            {
                // No index was made.
            }

            Redis.Dispose();
        }
    }

    private static async Task RunCaseAsync(string name, MemoryIndex index)
    {
        using var sandbox = new MemorySandbox("mem");
        await MemoryCases.RunAsync(name, (dimensions, clock) =>
        {
            var store = sandbox.Store(dimensions, index, clock);
            return (store, store);
        });
    }

    [RedisTheory]
    [MemberData(nameof(CaseNames))]
    public Task AFlatIndexPassesTheSharedCase(string name) => RunCaseAsync(name, MemoryIndex.None);

    [RedisTheory]
    [MemberData(nameof(CaseNames))]
    public Task AnHnswIndexPassesTheSharedCase(string name) => RunCaseAsync(name, MemoryIndex.Hnsw());

    /// <summary>Every index and both ways of writing the read-your-writes test runs.</summary>
    public static TheoryData<string, string> ReadYourWritesCases => new()
    {
        { "flat", "many" }, { "flat", "single" },
        { "hnsw", "many" }, { "hnsw", "single" },
    };

    /// <summary>Both indexes.</summary>
    public static TheoryData<string> Indexes => new() { "flat", "hnsw" };

    private static MemoryIndex IndexNamed(string index) => index == "hnsw" ? MemoryIndex.Hnsw() : MemoryIndex.None;

    /// <summary>The six records the first shared case writes.</summary>
    private static List<MemoryRecord> FirstCaseRecords() =>
    [
        .. CheckpointQueryContractTests.Fixture(Path.Combine("memory", "search-cases.json"))["cases"]![0]!["steps"]![0]!["upsert_many"]!
            .AsArray()
            .Select(record => MemoryCases.Record(record!)),
    ];

    private static readonly MemorySearch AlongX = new("n", new float[] { 1, 0, 0 }, "m");

    private static void AssertRanked(string[] expected, IReadOnlyList<MemoryMatch> matches, string where) =>
        Assert.True(
            matches.Select(m => m.Record.Id).SequenceEqual(expected),
            $"{where}: expected [{string.Join(", ", expected)}], got [{string.Join(", ", matches.Select(m => $"{m.Record.Id} {m.Similarity}"))}]");

    /// <summary>
    /// A search right after an upsert sees what it wrote, 50 times on fresh prefixes: the Query Engine indexes after the
    /// write returns, so the upsert waits for it. <c>many</c> writes the first shared case's six records at once,
    /// <c>single</c> one at a time; then 'd', the least similar, is moved to the top and searched for at once.
    /// </summary>
    [RedisTheory]
    [MemberData(nameof(ReadYourWritesCases))]
    public async Task ASearchRightAfterAnUpsertSeesIt(string index, string writes)
    {
        var records = FirstCaseRecords();
        for (var run = 0; run < 50; run++)
        {
            using var sandbox = new MemorySandbox("memryw");
            using var store = sandbox.Store(3, IndexNamed(index));
            if (writes == "single")
            {
                foreach (var record in records)
                {
                    await store.UpsertAsync(record);
                }
            }
            else
            {
                await store.UpsertManyAsync(records);
            }

            AssertRanked(["a", "e", "b", "f", "c", "d"], await store.SearchAsync(AlongX), $"run {run}");

            // 'd' was [-1, 0, 0]; at [1, 0, 0] it ties 'a' and 'e' at the top, ties by id.
            await store.UpsertAsync(records.Single(r => r.Id == "d") with { Vector = new float[] { 1, 0, 0 } });
            AssertRanked(["a", "d", "e", "b", "f", "c"], await store.SearchAsync(AlongX), $"run {run}, after the update");
        }
    }

    /// <summary>
    /// A key written twice by one <see cref="RedisMemoryStore.UpsertManyAsync"/> is confirmed by its last write: the first
    /// write's vector is never in the index once the second replaced it, so waiting for it would always time out.
    /// </summary>
    [RedisTheory]
    [MemberData(nameof(Indexes))]
    public async Task AKeyWrittenTwiceInOneCallIsConfirmedByItsLastWrite(string index)
    {
        using var sandbox = new MemorySandbox("memdup");
        using var store = sandbox.Store(3, IndexNamed(index));
        var records = FirstCaseRecords();
        var moved = records.Single(r => r.Id == "d") with { Vector = new float[] { 1, 0, 0 } };

        await store.UpsertManyAsync([.. records, moved]);

        var matches = await store.SearchAsync(AlongX);
        AssertRanked(["a", "d", "e", "b", "f", "c"], matches, "after the duplicate");
        Assert.Equal(new float[] { 1, 0, 0 }, matches[1].Record.Vector.ToArray());
    }

    [RedisFact]
    public async Task TheKeysHoldTheRecordAsTheFixtureSays()
    {
        using var sandbox = new MemorySandbox("memlay");
        var layout = Layout();
        var clock = new FakeTimeProvider(DateTimeOffset.Parse(layout["created_at"]!.GetValue<string>()));
        using var store = sandbox.Store(layout["dimensions"]!.GetValue<int>(), clock: clock);
        var sample = Sample(layout);

        await store.UpsertAsync(sample);
        clock.SetUtcNow(DateTimeOffset.Parse(layout["updated_at"]!.GetValue<string>()));
        await store.UpsertAsync(sample);

        var database = sandbox.Redis.Database;
        string Key(string relative) => $"{sandbox.Prefix}:{relative}";

        var hash = await database.HashGetAllAsync(Key(layout["hash"]!["key"]!.GetValue<string>()));
        AssertJson(FixtureFields(layout), Fields(hash.Select(entry => ((string)entry.Name!, entry.Value))));

        foreach (var (relative, members) in layout["sets"]!.AsObject())
        {
            var expected = new JsonArray();
            foreach (var member in members!.AsArray())
            {
                var name = member![0]!.GetValue<string>();
                expected.Add(new JsonArray(
                    name.StartsWith("strata:", StringComparison.Ordinal) ? sandbox.Prefix + name["strata".Length..] : name,
                    member[1]!.GetValue<long>()));
            }

            var actual = new JsonArray();
            foreach (var entry in await database.SortedSetRangeByRankWithScoresAsync(Key(relative)))
            {
                actual.Add(new JsonArray((string?)entry.Element, (long)entry.Score));
            }

            AssertJson(expected, actual);
        }

        var schema = new JsonObject();
        foreach (var entry in await database.HashGetAllAsync(Key(layout["schema"]!["key"]!.GetValue<string>())))
        {
            schema[(string)entry.Name!] = (string?)entry.Value;
        }

        AssertJson(layout["schema"]!["fields"], schema);
        Assert.Equal(sample.Key, (await store.GetAsync(sample.Key))!.Key);
    }

    [RedisFact]
    public async Task APrefixMadeWithOtherDimensionsIsRefused()
    {
        using var sandbox = new MemorySandbox("memdims");
        using (var made = sandbox.Store(768))
        {
            await made.MigrateAsync();
        }

        using var other = sandbox.Store(1536);
        var refused = await Assert.ThrowsAsync<MemoryStoreException>(() => other.MigrateAsync());
        Assert.Equal("memory was created with 768 dimensions, and this store was opened with 1536", refused.Message);
        var onUse = await Assert.ThrowsAsync<MemoryStoreException>(async () => await other.GetAsync(new MemoryKey("n", "", "a")));
        Assert.Equal(refused.Message, onUse.Message);
    }

    [RedisFact]
    public async Task ANewerPrefixIsRefusedAndLeftAlone()
    {
        using var sandbox = new MemorySandbox("memnew");
        using (var made = sandbox.Store(3))
        {
            await made.MigrateAsync();
        }

        await sandbox.Redis.Database.HashSetAsync($"{sandbox.Prefix}:memory:schema", "version", 2);

        using var store = sandbox.Store(3);
        var refused = await Assert.ThrowsAsync<MemoryStoreException>(async () => await store.GetAsync(new MemoryKey("n", "", "a")));
        Assert.Equal("database is at memory schema version 2, and this build knows 1: upgrade the runtime", refused.Message);
        Assert.Equal("2", (string?)await sandbox.Redis.Database.HashGetAsync($"{sandbox.Prefix}:memory:schema", "version"));
    }

    [RedisFact]
    public async Task ATieAcrossTheCutIsComplete()
    {
        using var sandbox = new MemorySandbox("memtie");
        using var store = sandbox.Store(2);

        // Twelve records at the same distance: a search for 3 sees more than its first 6 candidates, and ties go by id.
        var ids = Enumerable.Range(0, 12).Select(i => $"r{11 - i:D2}").ToList();
        await store.UpsertManyAsync([.. ids.Select(id => new MemoryRecord("n", "", id, new float[] { 1, 1 }, "m"))]);

        var matches = await store.SearchAsync(new MemorySearch("n", new float[] { 1, 0 }, "m") { Limit = 3 });

        Assert.Equal(["r00", "r01", "r02"], matches.Select(m => m.Record.Id));
    }

    [RedisFact]
    public async Task AListingAndAPurgeSkipEntriesWhoseHashIsGone()
    {
        using var sandbox = new MemorySandbox("memgone");
        var clock = new FakeTimeProvider(DateTimeOffset.Parse("2026-01-01T00:00:00Z"));
        using var store = sandbox.Store(2, clock: clock);
        await store.UpsertManyAsync(
        [
            new MemoryRecord("n", "", "a", new float[] { 1, 0 }, "m"),
            new MemoryRecord("n", "", "b", new float[] { 1, 0 }, "m"),
        ]);
        await sandbox.Redis.Database.KeyDeleteAsync(RedisMemoryStore.RecordKey(sandbox.Prefix, new MemoryKey("n", "", "a")));

        Assert.Equal(["b"], (await store.ListAsync(new MemoryQuery("n"))).Records.Select(r => r.Id));
        Assert.Equal(1, await store.PurgeAsync(new MemoryPurge(DateTimeOffset.Parse("2027-01-01T00:00:00Z"))));
        Assert.Empty((await store.ListAsync(new MemoryQuery("n"))).Records);
    }
}
