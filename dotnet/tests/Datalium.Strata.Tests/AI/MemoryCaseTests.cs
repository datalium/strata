using System.Runtime.ExceptionServices;
using System.Text.Json.Nodes;
using Datalium.Strata.AI;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Tests.Scheduling;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.Tests.AI;

/// <summary>
/// The memory helper's shared cases (<c>design/45-Long-Term-Memory.md</c>, "The helper"):
/// <c>schema/fixtures/ai/memory-cases.json</c>, which <c>rust/strata-ai/tests/memory_cases.rs</c> replays too. The
/// fixture's own <c>description</c> is the format. Keep the two in step.
/// </summary>
public sealed class MemoryCaseTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Job(bool Done = false);

    private static JsonNode Fixture() => RunSchedulerTests.Fixture(Path.Combine("ai", "memory-cases.json"));

    public static TheoryData<string> CaseNames()
    {
        var names = new TheoryData<string>();
        foreach (var @case in Fixture()["cases"]!.AsArray())
        {
            names.Add(@case!["name"]!.GetValue<string>());
        }

        return names;
    }

    [Theory]
    [MemberData(nameof(CaseNames))]
    public async Task The_Helper_Behaves_As_The_Shared_Case_Says(string name)
    {
        var fixture = Fixture();
        var @case = fixture["cases"]!.AsArray().Single(c => c!["name"]!.GetValue<string>() == name)!;
        var store = new InMemoryMemoryStore(fixture["dimensions"]!.GetValue<int>());
        var generator = Embedder(fixture["embedder"]!);
        var options = @case["options"];
        var memory = new StrataMemory(store, generator, new StrataMemoryOptions
        {
            Model = options?["model"]?.GetValue<string>(),
            TenantKey = options?["tenant_key"]?.GetValue<string>() ?? "tenant",
        });

        var events = new List<RunEvent>();
        if (@case["context"] is { } context)
        {
            // Every step inside the one node, whose failures are the steps' (asserted after the run).
            ExceptionDispatchInfo? broken = null;
            var graph = new StateGraph<Job>()
                .AddNode("Memory", async (job, ctx, _) =>
                {
                    try
                    {
                        await PlayAsync(@case, memory, store, generator, ctx);
                    }
                    catch (Exception e)
                    {
                        broken = ExceptionDispatchInfo.Capture(e);
                    }

                    return job with { Done = true };
                })
                .Compile();
            var channel = new RunEventChannel();
            var metadata = context["metadata"]!.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<string>(), StringComparer.Ordinal);

            var result = await graph.RunAsync(
                $"memory-case-{Guid.NewGuid():N}", new Job(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel, Metadata = metadata });

            broken?.Throw();
            Assert.IsType<GraphExecutionResult<Job>.Completed>(result);
            await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
            {
                events.Add(e);
            }
        }
        else
        {
            await PlayAsync(@case, memory, store, generator, null);
        }

        if (@case["calls"] is JsonArray calls)
        {
            Assert.Equal(
                calls.Select(call => call!.AsArray().Select(input => input!.GetValue<string>()).ToList()).ToList(),
                generator.Calls.Select(call => call.ToList()).ToList());
        }

        if (@case["events"] is JsonArray expected)
        {
            Assert.Equal(expected.Select(e => e!.GetValue<string>()).ToList(), events.Select(Describe).OfType<string>().ToList());
        }
    }

    private static string? Describe(RunEvent e) => e switch
    {
        ModelCallStarted s => $"ModelCallStarted {s.Operation} {s.Model} {s.Provider}",
        ModelCallCompleted c => $"ModelCallCompleted {c.Operation} {c.Result} {c.Usage.InputTokens?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "-"}",
        _ => null,
    };

    /// <summary>The fixture's scripted embedder: each input's vector, 4 input tokens per input, its failures and omissions.</summary>
    private static ScriptedEmbeddingGenerator Embedder(JsonNode spec)
    {
        var model = spec["model"]!.GetValue<string>();
        var vectors = spec["vectors"]!.AsObject().ToDictionary(
            p => p.Key, p => p.Value!.AsArray().Select(v => (float)v!.GetValue<double>()).ToArray(), StringComparer.Ordinal);
        var fails = spec["fails"]!.AsObject();
        var unanswered = spec["unanswered"]!.AsArray().Select(v => v!.GetValue<string>()).ToHashSet(StringComparer.Ordinal);
        return new ScriptedEmbeddingGenerator(spec["provider"]!.GetValue<string>(), model, inputs =>
        {
            foreach (var input in inputs)
            {
                if (fails[input] is { } failure)
                {
                    // What ModelFaults reads as the status with its hint, and whose message is the provider's.
                    var status = failure["status"]!.GetValue<int>();
                    throw new ModelCallException(
                        failure["message"]!.GetValue<string>(),
                        ModelFaults.IsTransientStatus(status),
                        status,
                        failure["retry_after_ms"] is { } ms ? TimeSpan.FromMilliseconds(ms.GetValue<int>()) : null);
                }
            }

            return ScriptedEmbeddingGenerator.Embeddings(
                model,
                inputs.Where(input => !unanswered.Contains(input)).Select(input =>
                    vectors.TryGetValue(input, out var vector) ? vector : throw new InvalidOperationException($"the case's embedder has no vector for '{input}'")));
        });
    }

    private static async Task PlayAsync(
        JsonNode @case, StrataMemory memory, InMemoryMemoryStore store, ScriptedEmbeddingGenerator generator, NodeContext? context)
    {
        var number = 0;
        foreach (var step in @case["steps"]!.AsArray())
        {
            number++;
            var where = $"{@case["name"]}, step {number}";
            var callsBefore = generator.Calls.Count;
            Func<Task> act;
            Func<Task>? check = null;
            IReadOnlyList<MemoryMatch>? matches = null;
            if (step!["remember"] is { } remember)
            {
                act = () => memory.RememberAsync(Entry(remember), context);
            }
            else if (step["remember_many"] is JsonArray many)
            {
                act = () => memory.RememberManyAsync([.. many.Select(entry => Entry(entry!))], context);
            }
            else if (step["recall"] is { } recall)
            {
                act = async () => matches = await memory.RecallAsync(
                    recall["namespace"]!.GetValue<string>(), recall["text"]!.GetValue<string>(), Recall(recall["options"]), context);
                check = () =>
                {
                    var expected = step["expect"]!.AsArray();
                    Assert.True(expected.Count == matches!.Count, $"{where}: {string.Join(", ", matches.Select(m => $"{m.Record.Id} {m.Similarity}"))}");
                    for (var i = 0; i < expected.Count; i++)
                    {
                        Assert.Equal(expected[i]!["id"]!.GetValue<string>(), matches[i].Record.Id);
                        Assert.True(
                            Math.Abs(expected[i]!["similarity"]!.GetValue<double>() - matches[i].Similarity) < 1e-5,
                            $"{where}: {matches[i].Record.Id} has similarity {matches[i].Similarity}");
                    }

                    return Task.CompletedTask;
                };
            }
            else if (step["get"] is { } get)
            {
                MemoryRecord? record = null;
                act = async () => record = await store.GetAsync(new MemoryKey(
                    get["namespace"]!.GetValue<string>(), get["tenant"]!.GetValue<string>(), get["id"]!.GetValue<string>()));
                check = () =>
                {
                    AssertRecord(step["expect"], record, where);
                    return Task.CompletedTask;
                };
            }
            else
            {
                throw new InvalidOperationException($"{where}: an unknown step");
            }

            if (step["failure"] is { } failure)
            {
                var thrown = await Assert.ThrowsAsync<NodeException>(act);
                var kind = failure["transient"]!.GetValue<bool>() ? NodeFailureKind.Transient : NodeFailureKind.Fatal;
                Assert.Equal(kind, thrown.Kind);
                Assert.Equal(failure["message"]!.GetValue<string>(), thrown.Message);
                Assert.Equal(
                    failure["retry_after_ms"] is { } ms ? TimeSpan.FromMilliseconds(ms.GetValue<int>()) : null,
                    thrown.RetryAfter);
            }
            else if (step["error"] is { } error)
            {
                var thrown = await Assert.ThrowsAsync<ArgumentException>(act);
                Assert.Equal(error.GetValue<string>(), thrown.Message);
                Assert.True(generator.Calls.Count == callsBefore, $"{where}: refused after an embedding call");
            }
            else
            {
                await act();
                if (check is not null)
                {
                    await check();
                }
            }
        }
    }

    private static MemoryEntry Entry(JsonNode entry) =>
        new(entry["namespace"]!.GetValue<string>(), entry["id"]!.GetValue<string>(), entry["text"]!.GetValue<string>())
        {
            Tenant = entry["tenant"]?.GetValue<string>(),
            Content = entry["content"]?.DeepClone().AsObject(),
            Metadata = Pairs(entry["metadata"]),
        };

    private static RecallOptions Recall(JsonNode? options) => options is null
        ? new RecallOptions()
        : new RecallOptions
        {
            Tenant = options["tenant"]?.GetValue<string>(),
            Limit = options["limit"]?.GetValue<int>() ?? 10,
            MinSimilarity = options["min_similarity"]?.GetValue<double>(),
            Exclude = options["exclude"]?.AsArray().Select(id => id!.GetValue<string>()).ToList() ?? [],
            Metadata = Pairs(options["metadata"]),
        };

    private static IReadOnlyDictionary<string, string> Pairs(JsonNode? pairs) =>
        pairs is JsonObject map
            ? map.ToDictionary(p => p.Key, p => p.Value!.GetValue<string>(), StringComparer.Ordinal)
            : MemoryRecords.NoMetadata;

    /// <summary>The record's JSON form without <c>schema_version</c> and timestamps; its vector compared as f32.</summary>
    private static void AssertRecord(JsonNode? expected, MemoryRecord? record, string where)
    {
        if (expected is null)
        {
            Assert.True(record is null, $"{where}: expected no record, found {record}");
            return;
        }

        Assert.True(record is not null, $"{where}: expected a record, found none");
        var actual = MemoryRecords.ToJson(record);
        actual.Remove("schema_version");
        actual.Remove("created_at");
        actual.Remove("updated_at");
        var want = expected.DeepClone().AsObject();
        Assert.Equal(
            want["embedding"]!["vector"]!.AsArray().Select(v => (float)v!.GetValue<double>()).ToArray(),
            record.Vector.ToArray());
        want["embedding"]!.AsObject().Remove("vector");
        actual["embedding"]!.AsObject().Remove("vector");
        Assert.True(JsonNode.DeepEquals(want, actual), $"{where}: expected {want.ToJsonString()}, found {actual.ToJsonString()}");
    }
}
