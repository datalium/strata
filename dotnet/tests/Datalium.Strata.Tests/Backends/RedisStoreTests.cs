using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage.Redis;
using StackExchange.Redis;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// Redis-specific behaviour of <see cref="RedisCheckpointStore"/> / <see cref="RedisHistoryStore"/>: the
/// key layout and JSON the two runtimes share (the mirror of <c>rust/strata-memory/tests/redis_stores.rs</c>),
/// key-prefix isolation, and concurrent writers.
/// </summary>
public sealed class RedisStoreTests
{
    private static JsonNode FrozenJson([CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(
            testDir, "..", "..", "..", "..", "schema", "fixtures", "checkpoint-lifecycle", "frozen.json"));
        return JsonNode.Parse(File.ReadAllText(path))!;
    }

    private static JsonNode NestedJson([CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(
            testDir, "..", "..", "..", "..", "schema", "fixtures", "checkpoint-lifecycle", "nested.json"));
        return JsonNode.Parse(File.ReadAllText(path))!;
    }

    private static Checkpoint Frozen(string? createdAt = null)
    {
        var doc = FrozenJson().AsObject();
        return new Checkpoint
        {
            SchemaVersion = doc["schema_version"]!.GetValue<string>(),
            ThreadId = doc["thread_id"]!.GetValue<string>(),
            Status = CheckpointStatus.Interrupted,
            SuspendedNode = doc["suspended_node"]?.GetValue<string>(),
            NextNode = doc["next_node"]?.GetValue<string>(),
            FrozenState = doc["frozen_state"]?.DeepClone(),
            Metadata = null,
            CreatedAt = createdAt ?? doc["created_at"]!.GetValue<string>(),
            ResumedAt = null,
        };
    }

    private static ChatMessage Message(string content) =>
        new(MessageRole.Assistant, content.AsMemory(), DateTimeOffset.FromUnixTimeSeconds(1_700_000_000));

    [Fact]
    public void AnUnreachableServerIsAnErrorNotAHang()
    {
        var started = Stopwatch.StartNew();
        Assert.ThrowsAny<Exception>(() => RedisCheckpointStore.Connect("redis://127.0.0.1:1"));
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void AUrlBecomesStackExchangeRedisOptions()
    {
        var options = RedisConnection.ToConfigurationOptions("redis://ann:p%40ss@cache.example:6390/2");
        AssertEndPoint("cache.example", 6390, options);
        Assert.Equal("ann", options.User);
        Assert.Equal("p@ss", options.Password);
        Assert.Equal(2, options.DefaultDatabase);

        var passwordOnly = RedisConnection.ToConfigurationOptions("redis://:secret@localhost");
        AssertEndPoint("localhost", 6379, passwordOnly);
        Assert.Null(passwordOnly.User);
        Assert.Equal("secret", passwordOnly.Password);

        // A native configuration string passes through.
        AssertEndPoint("localhost", 6400, RedisConnection.ToConfigurationOptions("localhost:6400"));
    }

    private static void AssertEndPoint(string host, int port, ConfigurationOptions options)
    {
        var endPoint = Assert.IsType<DnsEndPoint>(options.EndPoints.Single());
        Assert.Equal(host, endPoint.Host);
        Assert.Equal(port, endPoint.Port);
    }

    [RedisFact]
    public void ACheckpointIsOneListElementHoldingTheWireJson()
    {
        using var sandbox = new RedisSandbox("layout");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        var fixture = Frozen();
        store.Save(fixture);

        var key = $"{sandbox.Prefix}:checkpoints:{fixture.ThreadId}";
        var elements = sandbox.Database.ListRange(key);
        Assert.Single(elements);
        // The element is the shared checkpoint wire shape, verbatim -- the same file the Rust store's
        // test compares against.
        Assert.True(JsonNode.DeepEquals(FrozenJson(), JsonNode.Parse((string)elements[0]!)), (string)elements[0]!);

        // Saving again appends -- the list is the log -- and Load reads the tail.
        var resumed = fixture with { ResumedAt = "2026-09-16T01:00:00Z" };
        store.Save(resumed);
        Assert.Equal(2, sandbox.Database.ListLength(key));
        StoreContracts.AssertSame(resumed, store.Load(fixture.ThreadId));
    }

    [RedisFact]
    public void ANestedCheckpointIsOneListElementHoldingTheChildInsideIt()
    {
        using var sandbox = new RedisSandbox("nested");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        var wire = NestedJson();
        var fixture = Checkpoint.FromWireJson(wire);
        store.Save(fixture);

        // Still one element, and it is the nested wire shape verbatim -- the same file the Rust store's
        // test compares against.
        var elements = sandbox.Database.ListRange($"{sandbox.Prefix}:checkpoints:{fixture.ThreadId}");
        Assert.Single(elements);
        Assert.True(JsonNode.DeepEquals(wire, JsonNode.Parse((string)elements[0]!)), (string)elements[0]!);
        StoreContracts.AssertSame(fixture, store.Load(fixture.ThreadId));
    }

    [RedisFact]
    public void ACheckpointWrittenAsRawRedisCommandsIsLoadable()
    {
        // The reader side of the contract: the wire JSON pushed by anything else (the Rust store,
        // redis-cli) is readable through Load.
        using var sandbox = new RedisSandbox("reader");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        var fixture = Frozen();

        sandbox.Database.ListRightPush($"{sandbox.Prefix}:checkpoints:{fixture.ThreadId}", FrozenJson().ToJsonString());

        StoreContracts.AssertSame(fixture, store.Load(fixture.ThreadId));
    }

    [RedisFact]
    public async Task AMessageIsOneListElementWithRoleContentAndTimestamp()
    {
        using var sandbox = new RedisSandbox("history_layout");
        var store = new RedisHistoryStore(sandbox.Database, sandbox.Prefix);
        await store.AppendAsync("s1", Message("hello"));

        var elements = sandbox.Database.ListRange($"{sandbox.Prefix}:history:s1");
        Assert.Single(elements);
        var json = JsonNode.Parse((string)elements[0]!)!.AsObject();
        Assert.Equal(["role", "content", "timestamp"], json.Select(p => p.Key));
        Assert.Equal("assistant", json["role"]!.GetValue<string>());
        Assert.Equal("hello", json["content"]!.GetValue<string>());
        Assert.Equal(
            DateTimeOffset.FromUnixTimeSeconds(1_700_000_000),
            DateTimeOffset.Parse(json["timestamp"]!.GetValue<string>(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind));
    }

    [RedisFact]
    public async Task AMessageWrittenAsRawRedisCommandsIsReadable()
    {
        // The exact element the Rust store writes (`2023-11-14T22:13:20Z`, no fractional seconds).
        using var sandbox = new RedisSandbox("history_reader");
        var store = new RedisHistoryStore(sandbox.Database, sandbox.Prefix);
        sandbox.Database.ListRightPush(
            $"{sandbox.Prefix}:history:s1",
            """{"role":"assistant","content":"from rust","timestamp":"2023-11-14T22:13:20Z"}""");

        var history = new List<ChatMessage>();
        await foreach (var message in store.GetHistoryAsync("s1"))
        {
            history.Add(message);
        }

        var only = Assert.Single(history);
        Assert.Equal(MessageRole.Assistant, only.Role);
        Assert.Equal("from rust", only.Content.ToString());
        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1_700_000_000), only.Timestamp);
    }

    [RedisFact]
    public async Task DifferentPrefixesAreDifferentNamespacesOnOneRedis()
    {
        using var a = new RedisSandbox("ns_a");
        using var b = new RedisSandbox("ns_b");
        var storeA = new RedisCheckpointStore(a.Database, a.Prefix);
        var storeB = new RedisCheckpointStore(b.Database, b.Prefix);
        var historyA = new RedisHistoryStore(a.Database, a.Prefix);
        var historyB = new RedisHistoryStore(b.Database, b.Prefix);

        storeA.Save(Frozen());
        await historyA.AppendAsync("s1", Message("only in a"));

        Assert.Null(storeB.Load(Frozen().ThreadId));
        await foreach (var _ in historyB.GetHistoryAsync("s1"))
        {
            Assert.Fail("prefix b must not see prefix a's history");
        }

        StoreContracts.AssertSame(Frozen(), storeA.Load(Frozen().ThreadId));
        var seen = 0;
        await foreach (var _ in historyA.GetHistoryAsync("s1"))
        {
            seen++;
        }

        Assert.Equal(1, seen);
    }

    [RedisFact]
    public void ACheckpointSurvivesIndependentStoreInstances()
    {
        using var sandbox = new RedisSandbox("persist");
        using (var store = RedisCheckpointStore.Connect(TestBackends.RedisUrl, sandbox.Prefix))
        {
            store.Save(Frozen());
        }

        using var reopened = RedisCheckpointStore.Connect(TestBackends.RedisUrl, sandbox.Prefix);
        StoreContracts.AssertSame(Frozen(), reopened.Load(Frozen().ThreadId));
    }

    [RedisFact]
    public async Task ConcurrentWritersNeverLoseACheckpointOrAMessage()
    {
        // RPUSH is atomic, so -- unlike the SQL stores -- there is no read-then-write to protect; this
        // proves that stays true through the adapter with many connections at once.
        const int writers = 8;
        const int each = 50;
        using var sandbox = new RedisSandbox("concurrent");

        // Each writer stays on its own LongRunning thread for its whole life. An async lambda here
        // would hop to the thread pool at its first await, and the blocking `Save` calls after it would
        // then starve the pool that StackExchange.Redis needs to read replies: on a 2-core CI runner
        // the sync RPUSHes timed out (RedisTimeoutException, "WORKER: Busy=10, Min=2").
        using var start = new ManualResetEventSlim();
        var tasks = Enumerable.Range(0, writers).Select(writer => Task.Factory.StartNew(
            () =>
            {
                using var checkpoints = RedisCheckpointStore.Connect(TestBackends.RedisUrl, sandbox.Prefix);
                using var history = RedisHistoryStore.Connect(TestBackends.RedisUrl, sandbox.Prefix);
                start.Wait();
                for (var n = 0; n < each; n++)
                {
                    checkpoints.Save(Frozen($"{writer}:{n}"));
                    history.AppendAsync("contended", Message($"{writer}:{n}")).GetAwaiter().GetResult();
                }
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default)).ToArray();
        start.Set();
        await Task.WhenAll(tasks);

        Assert.Equal(writers * each, sandbox.Database.ListLength($"{sandbox.Prefix}:checkpoints:{Frozen().ThreadId}"));

        var history = new List<string>();
        await foreach (var message in new RedisHistoryStore(sandbox.Database, sandbox.Prefix).GetHistoryAsync("contended", writers * each + 10))
        {
            history.Add(message.Content.ToString());
        }

        Assert.Equal(writers * each, history.Count);
        for (var writer = 0; writer < writers; writer++)
        {
            var mine = history.Where(c => c.StartsWith($"{writer}:", StringComparison.Ordinal)).Select(c => int.Parse(c.Split(':')[1]));
            Assert.Equal(Enumerable.Range(0, each), mine);
        }
    }
}
