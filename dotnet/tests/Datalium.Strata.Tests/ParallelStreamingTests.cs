using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/parallel_streaming.rs</c> (<c>design/14-Parallel-Execution.md</c>):
/// the branches of a parallel group can stream tokens concurrently, attributed to
/// <c>"&lt;group&gt;/&lt;branch&gt;"</c>, and a retried fallible node announces the retry with a
/// <see cref="StreamChunkType.Retry"/> chunk. Keep the two in step.
/// </summary>
public class ParallelStreamingTests
{
    public sealed record Report([property: JsonPropertyName("results")] IReadOnlyList<string> Results);

    private static Report NewReport() => new(Results: []);

    private static Report Concat(Report entry, IReadOnlyList<Report> outputs) =>
        entry with { Results = [.. entry.Results, .. outputs.SelectMany(o => o.Results)] };

    /// <summary><c>Gather</c> (the parallel group) -> <c>Finish</c>.</summary>
    private static CompiledGraph<Report> GraphAround(ParallelBranches<Report> branches, RecoveryPolicy? policy = null)
    {
        var graph = new StateGraph<Report>()
            .AddParallelGroup("Gather", branches, Concat)
            .AddNode("Finish", state => state)
            .AddEdge("Gather", "Finish");
        if (policy is not null)
        {
            graph.SetRecoveryPolicy("Gather", policy);
        }

        return graph.Compile();
    }

    private static Report Completed(GraphExecutionResult<Report> result) =>
        Assert.IsType<GraphExecutionResult<Report>.Completed>(result).State;

    /// <summary>Runs <paramref name="graph"/> under <c>RunStreaming</c> and collects every chunk it emits.</summary>
    private static async Task<(List<StreamChunk> Chunks, GraphExecutionResult<Report> Result)> Stream(
        CompiledGraph<Report> graph, TimeSpan? consumerDelay = null)
    {
        var (reader, completion) = graph.RunStreaming("t", NewReport(), new InMemoryCheckpointStore());
        if (consumerDelay is { } delay)
        {
            await Task.Delay(delay);
        }

        var chunks = new List<StreamChunk>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            chunks.Add(chunk);
        }

        return (chunks, await completion);
    }

    /// <summary>The tokens one node (or <c>"&lt;group&gt;/&lt;branch&gt;"</c>) emitted, in arrival order.</summary>
    private static List<string> TokensFrom(IEnumerable<StreamChunk> chunks, string node) =>
        [.. chunks.Where(c => c.Type == StreamChunkType.Token && c.NodeName == node).Select(c => c.Payload.ToString())];

    /// <summary>A streaming branch that emits <paramref name="count"/> numbered tokens, then records <paramref name="label"/>.</summary>
    private static Func<Report, IStreamContext, Report> Talker(string label, int count) =>
        (state, context) =>
        {
            for (var i = 0; i < count; i++)
            {
                context.EmitToken($"{label}{i}");
            }

            return state with { Results = [.. state.Results, label] };
        };

    [Fact]
    public async Task Every_Streaming_Branch_Reaches_The_Consumer_Attributed_To_Group_Slash_Branch()
    {
        var branches = new ParallelBranches<Report>()
            .StreamingBranch("a", Talker("a", 3))
            .StreamingBranch("b", Talker("b", 3));

        var (chunks, result) = await Stream(GraphAround(branches));

        // Each branch's own tokens arrive whole and in order, however the two interleave.
        Assert.Equal(["a0", "a1", "a2"], TokensFrom(chunks, "Gather/a"));
        Assert.Equal(["b0", "b1", "b2"], TokensFrom(chunks, "Gather/b"));
        // The reducer still sees declaration order.
        Assert.Equal(["a", "b"], Completed(result).Results);
    }

    [Fact]
    public async Task Branch_Tokens_Arrive_Between_The_Groups_Own_Start_And_End()
    {
        var branches = new ParallelBranches<Report>()
            .StreamingBranch("a", Talker("a", 2))
            .StreamingBranch("b", Talker("b", 2));

        var (chunks, _) = await Stream(GraphAround(branches));

        var start = chunks.FindIndex(c => c.Type == StreamChunkType.NodeStart && c.NodeName == "Gather");
        var end = chunks.FindIndex(c => c.Type == StreamChunkType.NodeEnd && c.NodeName == "Gather");
        var tokenPositions = chunks
            .Select((chunk, index) => (chunk, index))
            .Where(x => x.chunk.Type == StreamChunkType.Token)
            .Select(x => x.index)
            .ToList();
        Assert.Equal(4, tokenPositions.Count);
        Assert.All(tokenPositions, i => Assert.True(start < i && i < end));
        // Branches are not nodes: the group is still the only lifecycle pair.
        Assert.Equal(
            ["Gather", "Finish"],
            chunks.Where(c => c.Type == StreamChunkType.NodeStart).Select(c => c.NodeName));
    }

    [Fact]
    public async Task A_Plain_Branch_Can_Sit_Beside_A_Streaming_One_And_Says_Nothing()
    {
        var branches = new ParallelBranches<Report>()
            .StreamingBranch("talk", Talker("talk", 2))
            .Branch("quiet", state => state with { Results = [.. state.Results, "quiet"] });

        var (chunks, result) = await Stream(GraphAround(branches));

        Assert.Equal(["talk0", "talk1"], TokensFrom(chunks, "Gather/talk"));
        Assert.Empty(TokensFrom(chunks, "Gather/quiet"));
        Assert.Equal(["talk", "quiet"], Completed(result).Results);
    }

    [Fact]
    public async Task Concurrent_Producers_Overrun_The_Bounded_Channel_And_Nothing_Is_Dropped()
    {
        // 3 producers x 2000 tokens is well past the 1024-chunk channel, and the consumer holds
        // off reading, so every branch ends up blocked in a write. Backpressure must pause them,
        // not drop tokens.
        const int perBranch = 2000;
        var branches = new ParallelBranches<Report>()
            .StreamingBranch("a", Talker("a", perBranch))
            .StreamingBranch("b", Talker("b", perBranch))
            .StreamingBranch("c", Talker("c", perBranch));

        var (chunks, result) = await Stream(GraphAround(branches), consumerDelay: TimeSpan.FromMilliseconds(200));

        Assert.Equal(["a", "b", "c"], Completed(result).Results);
        foreach (var label in new[] { "a", "b", "c" })
        {
            Assert.Equal(
                Enumerable.Range(0, perBranch).Select(i => $"{label}{i}"),
                TokensFrom(chunks, $"Gather/{label}"));
        }
    }

    [Fact]
    public async Task A_Plain_Run_Refuses_A_Group_With_A_Streaming_Branch_Before_Running_Any_Of_It()
    {
        var ran = 0;
        var branches = new ParallelBranches<Report>()
            .Branch("plain", state =>
            {
                Interlocked.Increment(ref ran);
                return state;
            })
            .StreamingBranch("talk", Talker("talk", 1));

        var result = await GraphAround(branches).RunAsync("t", NewReport(), new InMemoryCheckpointStore());

        var failed = Assert.IsType<GraphExecutionResult<Report>.Failed>(result);
        Assert.Contains("'Gather'", failed.Message);
        Assert.Contains("RunStreaming", failed.Message);
        Assert.Equal(0, ran);
    }

    [Fact]
    public async Task A_Retry_Is_Announced_Before_The_Streaming_Branches_Run_Again()
    {
        var attempts = 0;
        var branches = new ParallelBranches<Report>().StreamingBranch("flaky", (state, context) =>
        {
            var attempt = Interlocked.Increment(ref attempts);
            context.EmitToken($"try{attempt}");
            return attempt < 2 ? throw NodeException.Transient("timeout") : state;
        });

        var (chunks, result) = await Stream(GraphAround(branches, RecoveryPolicy.Retry(2)));

        Completed(result);
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Gather", "Gather"),
                (StreamChunkType.Token, "Gather/flaky", "try1"),
                // The group's own name, and the failed attempt's message.
                (StreamChunkType.Retry, "Gather", "branch 'flaky': timeout"),
                (StreamChunkType.Token, "Gather/flaky", "try2"),
                (StreamChunkType.NodeEnd, "Gather", "Gather"),
            ],
            chunks
                .Where(c => c.NodeName.StartsWith("Gather", StringComparison.Ordinal))
                .Select(c => (c.Type, c.NodeName, c.Payload.ToString())));
    }

    [Fact]
    public async Task A_Group_That_Gives_Up_Emits_Error_Not_Another_Retry()
    {
        var branches = new ParallelBranches<Report>().StreamingBranch("down", (state, context) =>
        {
            context.EmitToken("dialling");
            throw NodeException.Transient("outage");
        });

        var (chunks, result) = await Stream(GraphAround(branches, RecoveryPolicy.Retry(1)));

        Assert.Equal(2, Assert.IsType<GraphExecutionResult<Report>.NodeFailed>(result).Attempts);
        // Two attempts: one retry between them, and one Error once the second gave up.
        Assert.Single(chunks, c => c.Type == StreamChunkType.Retry);
        Assert.Single(chunks, c => c.Type == StreamChunkType.Error);
        Assert.Equal(["dialling", "dialling"], TokensFrom(chunks, "Gather/down"));
    }

    [Fact]
    public async Task Any_Retried_Fallible_Node_Emits_A_Retry_Chunk_Not_Only_Groups()
    {
        var attempts = 0;
        var graph = new StateGraph<Report>()
            .AddFallibleNode("Call", state =>
                Interlocked.Increment(ref attempts) <= 2 ? throw NodeException.Transient("rate limited") : state)
            .SetRecoveryPolicy("Call", RecoveryPolicy.Retry(2))
            .Compile();

        var (chunks, result) = await Stream(graph);

        Completed(result);
        Assert.Equal(
            [("Call", "rate limited"), ("Call", "rate limited")],
            chunks.Where(c => c.Type == StreamChunkType.Retry).Select(c => (c.NodeName, c.Payload.ToString())));
    }

    [Fact]
    public async Task A_Retry_That_Is_Never_Needed_Emits_No_Retry_Chunk()
    {
        var branches = new ParallelBranches<Report>().StreamingBranch("a", Talker("a", 1));

        var (chunks, _) = await Stream(GraphAround(branches, RecoveryPolicy.Retry(3)));

        Assert.DoesNotContain(chunks, c => c.Type == StreamChunkType.Retry);
    }

    [Fact]
    public void A_Child_Whose_Group_Has_Only_Plain_Branches_Is_Still_Fine()
    {
        var child = new StateGraph<Report>()
            .AddParallelGroup("Fan", new ParallelBranches<Report>().Branch("a", state => state), Concat)
            .Compile();

        var graph = new StateGraph<Report>().AddSubgraph("Work", child, state => state, (_, result) => result);

        Assert.NotNull(graph.Compile());
    }

    [Fact]
    public async Task Re_Adding_A_Name_Replaces_A_Streaming_Group_And_Its_Stream_Requirement()
    {
        var graph = new StateGraph<Report>()
            .AddParallelGroup("Gather", new ParallelBranches<Report>().StreamingBranch("talk", Talker("talk", 1)), Concat)
            .AddNode("Gather", state => state with { Results = [.. state.Results, "plain"] })
            .Compile();

        // A plain run is fine now: nothing in the graph needs a StreamContext.
        var state = Completed(await graph.RunAsync("t", NewReport(), new InMemoryCheckpointStore()));

        Assert.Equal(["plain"], state.Results);
    }
}
