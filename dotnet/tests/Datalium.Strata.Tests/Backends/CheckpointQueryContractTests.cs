using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Storage.Redis;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// The query port (<c>design/19-Async-Queryable-Store.md</c>): every store saves the seed of
/// <c>schema/fixtures/checkpoint-query/cases.json</c> and must answer every case as the fixture says --
/// the same cases <c>rust/strata-memory/tests/checkpoint_store_contract.rs</c> and the CLI run. Redis may
/// page short, so it is held to the threads and their order, not the page boundaries.
/// </summary>
public sealed class CheckpointQueryContractTests
{
    internal static JsonNode Fixture(string relativePath, [CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(testDir, "..", "..", "..", "..", "schema", "fixtures", relativePath));
        return JsonNode.Parse(File.ReadAllText(path))!;
    }

    internal static List<Checkpoint> Seed() =>
        [.. Fixture(Path.Combine("checkpoint-query", "cases.json"))["seed"]!.AsArray().Select(c => Checkpoint.FromWireJson(c!))];

    internal static async Task SaveSeedAsync(ICheckpointStore store)
    {
        foreach (var checkpoint in Seed())
        {
            await store.SaveAsync(checkpoint);
        }
    }

    private static ThreadQuery Query(JsonNode json) => new()
    {
        States = json["states"]?.AsArray().Select(s => CheckpointThreadStates.Parse(s!.GetValue<string>())).ToHashSet(),
        Metadata = json["metadata"]?.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<string>())
            ?? new Dictionary<string, string>(),
        UpdatedAfter = json["updated_after"] is { } after ? CheckpointTimestamps.Parse(after.GetValue<string>()) : null,
        UpdatedBefore = json["updated_before"] is { } before ? CheckpointTimestamps.Parse(before.GetValue<string>()) : null,
        DueBy = json["due_by"] is { } due ? CheckpointTimestamps.Parse(due.GetValue<string>()) : null,
        GraphId = json["graph_id"]?.GetValue<string>(),
        GraphVersion = json["graph_version"]?.GetValue<string>(),
        Limit = json["limit"]?.GetValue<int>() ?? ThreadQuery.DefaultLimit,
    };

    private static string StatusName(CheckpointStatus status) => status.ToString().ToLowerInvariant();

    /// <summary>A summary in the shape of <c>cases.json</c>'s <c>summaries</c>.</summary>
    private static JsonNode SummaryJson(ThreadSummary summary)
    {
        var metadata = new JsonObject();
        foreach (var (key, value) in summary.Metadata)
        {
            metadata[key] = value;
        }

        return new JsonObject
        {
            ["sequence"] = summary.Sequence,
            ["state"] = summary.State.Name(),
            ["status"] = StatusName(summary.Status),
            ["suspended_node"] = summary.SuspendedNode,
            ["next_node"] = summary.NextNode,
            ["waiting_at"] = new JsonArray([.. summary.WaitingAt.Select(n => JsonValue.Create(n))]),
            ["metadata"] = metadata,
            ["updated_at"] = CheckpointTimestamps.Text(summary.UpdatedAt),
            ["schema_version"] = summary.SchemaVersion,
            ["wait"] = summary.Wait?.ToWireJson(),
            ["graph"] = summary.Graph?.ToWireJson(),
        };
    }

    /// <param name="opaqueCursor">
    /// The store's cursor is its own (Strata Cloud's), not the shared encoding: a malformed one is the store's to
    /// refuse, and isn't checked here.
    /// </param>
    internal static async Task RunCasesAsync(ICheckpointQuery store, bool exact, bool opaqueCursor = false)
    {
        var cases = Fixture(Path.Combine("checkpoint-query", "cases.json"));
        var summaries = cases["summaries"]!;

        foreach (var testCase in cases["list"]!.AsArray())
        {
            var name = testCase!["name"]!.GetValue<string>();
            var query = Query(testCase["query"]!);
            var pages = new List<List<string>>();
            while (true)
            {
                var page = await store.ListThreadsAsync(query);
                Assert.True(page.Threads.Count <= query.Limit, $"{name}: page too long");
                foreach (var summary in page.Threads)
                {
                    Assert.True(
                        JsonNode.DeepEquals(SummaryJson(summary), summaries[summary.ThreadId]),
                        $"{name}: summary of {summary.ThreadId} was {SummaryJson(summary).ToJsonString()}");
                }

                pages.Add([.. page.Threads.Select(t => t.ThreadId)]);
                if (page.NextCursor is null)
                {
                    break;
                }

                query = query with { Cursor = page.NextCursor };
                Assert.True(pages.Count < 50, $"{name}: the cursor never ended");
            }

            var expected = testCase["pages"]!.AsArray().Select(p => p!.AsArray().Select(t => t!.GetValue<string>()).ToList()).ToList();
            if (exact)
            {
                Assert.True(expected.Count == pages.Count && expected.Zip(pages).All(p => p.First.SequenceEqual(p.Second)),
                    $"{name}: pages were [{string.Join("] [", pages.Select(p => string.Join(", ", p)))}]");
            }
            else
            {
                Assert.Equal(expected.SelectMany(p => p), pages.SelectMany(p => p));
            }
        }

        var seed = Seed();
        foreach (var testCase in cases["log"]!.AsArray())
        {
            var name = testCase!["name"]!.GetValue<string>();
            var threadId = testCase["thread_id"]!.GetValue<string>();
            var page = await store.GetLogAsync(
                threadId, testCase["after_sequence"]?.GetValue<long>(), testCase["limit"]!.GetValue<int>());
            Assert.Equal(testCase["sequences"]!.AsArray().Select(s => s!.GetValue<long>()), page.Entries.Select(e => e.Sequence));
            Assert.Equal(testCase["next_after_sequence"]?.GetValue<long>(), page.NextAfterSequence);
            var log = seed.Where(c => c.ThreadId == threadId).ToList();
            foreach (var entry in page.Entries)
            {
                StoreContracts.AssertSame(log[(int)entry.Sequence], entry.Checkpoint);
            }
        }

        // Out-of-range page sizes and a malformed cursor are refused.
        foreach (var limit in new[] { 0, 1001 })
        {
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.ListThreadsAsync(new ThreadQuery { Limit = limit }));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.GetLogAsync("failed-acme", null, limit));
        }

        if (!opaqueCursor)
        {
            await Assert.ThrowsAsync<ArgumentException>(async () => await store.ListThreadsAsync(new ThreadQuery { Cursor = "not a cursor" }));
        }
    }

    /// <summary>Every save is kept, in order, and <see cref="ICheckpointQuery.GetLogAsync"/> reads them back.</summary>
    internal static async Task LogContractAsync<TStore>(TStore store)
        where TStore : ICheckpointStore, ICheckpointQuery
    {
        var seed = Seed().Where(c => c.ThreadId == "failed-acme").ToList();
        foreach (var checkpoint in seed)
        {
            await store.SaveAsync(checkpoint);
        }

        var log = await store.GetLogAsync("failed-acme", null, 10);
        Assert.Equal([0L, 1L, 2L], log.Entries.Select(e => e.Sequence));
        for (var i = 0; i < seed.Count; i++)
        {
            StoreContracts.AssertSame(seed[i], log.Entries[i].Checkpoint);
        }

        Assert.Null(log.NextAfterSequence);
        StoreContracts.AssertSame(seed[^1], await store.LoadAsync("failed-acme"));
    }

    [Fact]
    public async Task InMemoryStoreAnswersTheSharedQueryCases()
    {
        var store = new InMemoryCheckpointStore();
        await SaveSeedAsync(store);
        await RunCasesAsync(store, exact: true);
        await LogContractAsync(new InMemoryCheckpointStore());
    }

    [Fact]
    public async Task SqliteStoreAnswersTheSharedQueryCases()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        await SaveSeedAsync(store);
        await RunCasesAsync(store, exact: true);
        using var other = EfCoreCheckpointStore.CreateInMemory();
        await LogContractAsync(other);
    }

    [PostgresFact]
    public async Task PostgresStoreAnswersTheSharedQueryCases()
    {
        using var sandbox = new PostgresSandbox("query");
        using var store = new PostgresCheckpointStore(sandbox.CreateDataSource());
        await SaveSeedAsync(store);
        await RunCasesAsync(store, exact: true);
        using var otherSandbox = new PostgresSandbox("log");
        using var other = new PostgresCheckpointStore(otherSandbox.CreateDataSource());
        await LogContractAsync(other);
    }

    [RedisFact]
    public async Task RedisStoreAnswersTheSharedQueryCases()
    {
        using var sandbox = new RedisSandbox("query");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await SaveSeedAsync(store);
        await RunCasesAsync(store, exact: false);
        using var otherSandbox = new RedisSandbox("log");
        await LogContractAsync(new RedisCheckpointStore(otherSandbox.Database, otherSandbox.Prefix));
    }
}
