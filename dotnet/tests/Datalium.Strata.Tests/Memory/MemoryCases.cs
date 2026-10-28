using System.Text.Json.Nodes;
using Datalium.Strata.Memory;
using Datalium.Strata.Tests.Backends;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.Tests.Memory;

/// <summary>
/// Replays <c>schema/fixtures/memory/search-cases.json</c> (<c>design/45-Long-Term-Memory.md</c>, "Tests") against a
/// memory store: the cases every store runs in both engines. The fixture's own <c>description</c> is the format.
/// </summary>
internal static class MemoryCases
{
    private static readonly Lazy<JsonArray> Cases = new(() =>
        CheckpointQueryContractTests.Fixture(Path.Combine("memory", "search-cases.json"))["cases"]!.AsArray());

    /// <summary>Every case's name, for a theory.</summary>
    public static IEnumerable<object[]> Names => Cases.Value.Select(c => new object[] { c!["name"]!.GetValue<string>() });

    /// <summary>
    /// Runs the case named <paramref name="name"/> against the store <paramref name="open"/> makes, given the case's
    /// dimensions and the clock its steps set.
    /// </summary>
    public static async Task RunAsync(string name, Func<int, TimeProvider, (IMemoryStore Store, IMemoryRetention Retention)> open)
    {
        var testCase = Cases.Value.Single(c => c!["name"]!.GetValue<string>() == name)!;
        var clock = new FakeTimeProvider(DateTimeOffset.Parse("2026-01-01T00:00:00Z"));
        var (store, retention) = open(testCase["dimensions"]!.GetValue<int>(), clock);
        try
        {
            var number = 0;
            foreach (var step in testCase["steps"]!.AsArray())
            {
                number++;
                if (step!["at"] is { } at)
                {
                    clock.SetUtcNow(DateTimeOffset.Parse(at.GetValue<string>()));
                }

                await RunStepAsync(store, retention, step, $"{name}, step {number}");
            }
        }
        finally
        {
            (store as IDisposable)?.Dispose();
        }
    }

    private static async Task RunStepAsync(IMemoryStore store, IMemoryRetention retention, JsonNode step, string where)
    {
        Func<Task> act;
        Func<Task>? check = null;
        if (step["upsert"] is { } upsert)
        {
            act = async () => await store.UpsertAsync(Record(upsert));
        }
        else if (step["upsert_many"] is { } many)
        {
            act = async () => await store.UpsertManyAsync([.. many.AsArray().Select(r => Record(r!))]);
        }
        else if (step["get"] is { } get)
        {
            MemoryRecord? got = null;
            act = async () => got = await store.GetAsync(Key(get));
            check = () =>
            {
                AssertRecord(step["expect"], got, where);
                return Task.CompletedTask;
            };
        }
        else if (step["delete"] is { } delete)
        {
            var deleted = false;
            act = async () => deleted = await store.DeleteAsync(Key(delete));
            check = () =>
            {
                Assert.True(step["expect"]!.GetValue<bool>() == deleted, $"{where}: deleted {deleted}");
                return Task.CompletedTask;
            };
        }
        else if (step["search"] is { } search)
        {
            IReadOnlyList<MemoryMatch> matches = [];
            act = async () => matches = await store.SearchAsync(Search(search));
            check = () =>
            {
                AssertMatches(step["expect"]!.AsArray(), matches, where);
                return Task.CompletedTask;
            };
        }
        else if (step["list"] is { } list)
        {
            var pages = new List<List<string>>();
            act = async () =>
            {
                var query = Query(list);
                while (true)
                {
                    var page = await store.ListAsync(query);
                    pages.Add([.. page.Records.Select(r => r.Id)]);
                    if (page.NextCursor is null)
                    {
                        break;
                    }

                    query = query with { Cursor = page.NextCursor };
                }
            };
            check = () =>
            {
                var expected = step["expect_pages"]!.AsArray()
                    .Select(p => string.Join(",", p!.AsArray().Select(id => id!.GetValue<string>())));
                Assert.True(
                    expected.SequenceEqual(pages.Select(p => string.Join(",", p))),
                    $"{where}: expected pages [{string.Join("] [", expected)}], got [{string.Join("] [", pages.Select(p => string.Join(",", p)))}]");
                return Task.CompletedTask;
            };
        }
        else if (step["purge"] is { } purge)
        {
            var purged = -1;
            act = async () => purged = await retention.PurgeAsync(Purge(purge));
            check = () =>
            {
                Assert.True(step["expect"]!.GetValue<int>() == purged, $"{where}: purged {purged}");
                return Task.CompletedTask;
            };
        }
        else
        {
            throw new InvalidOperationException($"{where}: unknown step {step.ToJsonString()}");
        }

        if (step["error"] is { } error)
        {
            // Exactly an ArgumentException with no parameter name, so its message is the case's word for word.
            var refused = await Assert.ThrowsAsync<ArgumentException>(act);
            Assert.True(error.GetValue<string>() == refused.Message, $"{where}: expected '{error}', got '{refused.Message}'");
            Assert.Null(refused.ParamName);
            return;
        }

        await act();
        if (check is not null)
        {
            await check();
        }
    }

    /// <summary>A record from a step: the record's JSON form without the store's fields.</summary>
    public static MemoryRecord Record(JsonNode json) => MemoryRecords.FromJson(json);

    private static MemoryKey Key(JsonNode json) => new(
        json["namespace"]!.GetValue<string>(), json["tenant"]?.GetValue<string>() ?? "", json["id"]!.GetValue<string>());

    private static MemorySearch Search(JsonNode json) => new(
        json["namespace"]!.GetValue<string>(), Vector(json["vector"]!), json["model"]!.GetValue<string>())
    {
        Tenant = json["tenant"]?.GetValue<string>() ?? "",
        Limit = json["limit"]?.GetValue<int>() ?? 10,
        MinSimilarity = json["min_similarity"]?.GetValue<double>(),
        Exclude = json["exclude"]?.AsArray().Select(id => id!.GetValue<string>()).ToList() ?? [],
        Metadata = Metadata(json["metadata"]),
    };

    private static MemoryQuery Query(JsonNode json) => new(json["namespace"]!.GetValue<string>())
    {
        Tenant = json["tenant"]?.GetValue<string>() ?? "",
        Limit = json["limit"]?.GetValue<int>() ?? 100,
        Metadata = Metadata(json["metadata"]),
    };

    private static MemoryPurge Purge(JsonNode json) => new(DateTimeOffset.Parse(json["older_than"]!.GetValue<string>()))
    {
        // Left out, a purge's namespace and tenant are every one: not "" as a search's tenant is.
        Namespace = json["namespace"]?.GetValue<string>(),
        Tenant = json["tenant"]?.GetValue<string>(),
        Limit = json["limit"]?.GetValue<int>() ?? 1000,
    };

    private static float[] Vector(JsonNode json) => [.. json.AsArray().Select(v => v!.GetValue<float>())];

    private static IReadOnlyDictionary<string, string> Metadata(JsonNode? json) =>
        json?.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<string>()) ?? MemoryRecords.NoMetadata;

    private static void AssertRecord(JsonNode? expected, MemoryRecord? actual, string where)
    {
        if (expected is null)
        {
            Assert.True(actual is null, $"{where}: expected no record, got '{actual?.Id}'");
            return;
        }

        Assert.True(actual is not null, $"{where}: expected a record, got none");

        // The vector by value; the rest as JSON, with the fixture's defaults filled in.
        var want = expected.DeepClone().AsObject();
        Assert.Equal(
            Vector(want["embedding"]!["vector"]!),
            actual!.Vector.ToArray());
        want["embedding"]!.AsObject().Remove("vector");
        want["tenant"] ??= "";
        if (!want.ContainsKey("content"))
        {
            want["content"] = null;
        }

        if (!want.ContainsKey("text"))
        {
            want["text"] = null;
        }

        want["metadata"] ??= new JsonObject();

        var got = MemoryRecords.ToJson(actual);
        got.Remove("schema_version");
        got["embedding"]!.AsObject().Remove("vector");
        Assert.True(JsonNode.DeepEquals(want, got), $"{where}: expected {want.ToJsonString()}, got {got.ToJsonString()}");
    }

    private static void AssertMatches(JsonArray expected, IReadOnlyList<MemoryMatch> actual, string where)
    {
        var describe = string.Join(", ", actual.Select(m => $"{m.Record.Id} {m.Similarity}"));
        Assert.True(expected.Count == actual.Count, $"{where}: expected {expected.Count} matches, got [{describe}]");
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.True(expected[i]!["id"]!.GetValue<string>() == actual[i].Record.Id, $"{where}: match {i}, got [{describe}]");
            Assert.True(
                Math.Abs(expected[i]!["similarity"]!.GetValue<double>() - actual[i].Similarity) <= 1e-5,
                $"{where}: match {i}'s similarity, got [{describe}]");
        }
    }
}
