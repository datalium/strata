using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/subgraph_streaming.rs</c> (<c>design/15-Subgraph-Composition.md</c>):
/// a child graph's streaming nodes, parallel-group branches, lifecycle chunks and
/// <see cref="StreamChunkType.Retry"/>/<see cref="StreamChunkType.Error"/> chunks reach the parent's
/// <c>RunStreaming</c> reader, named <c>"&lt;subgraph&gt;/&lt;child node&gt;"</c>. Keep the two in step.
/// </summary>
public class SubgraphStreamingTests
{
    public sealed record Job([property: JsonPropertyName("log")] IReadOnlyList<string> Log);

    private static Job NewJob() => new(Log: []);

    private static Job Concat(Job entry, IReadOnlyList<Job> outputs) =>
        entry with { Log = [.. entry.Log, .. outputs.SelectMany(o => o.Log)] };

    private static Func<Job, Job> Note(string label) => job => job with { Log = [.. job.Log, label] };

    /// <summary>A streaming node that emits each of <paramref name="tokens"/>, then records <paramref name="label"/>.</summary>
    private static StreamingNodeHandler<Job> Talk(string label, params string[] tokens) =>
        (job, context) =>
        {
            foreach (var token in tokens)
            {
                context.EmitToken(token);
            }

            return job with { Log = [.. job.Log, label] };
        };

    /// <summary><c>Before</c> -> <c>Work</c> (a subgraph running <paramref name="child"/>) -> <c>Finish</c>.</summary>
    private static CompiledGraph<Job> ParentAround(CompiledGraph<Job> child, RecoveryPolicy? policy = null)
    {
        var graph = new StateGraph<Job>()
            .AddNode("Before", Note("before"))
            .AddSubgraph("Work", child, job => job, (_, result) => result)
            .AddNode("Finish", Note("finish"))
            .AddEdge("Before", "Work")
            .AddEdge("Work", "Finish");
        if (policy is not null)
        {
            graph.SetRecoveryPolicy("Work", policy);
        }

        return graph.Compile();
    }

    private static Job Completed(GraphExecutionResult<Job> result) =>
        Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State;

    /// <summary>Runs <paramref name="graph"/> under <c>RunStreaming</c> and collects every chunk it emits.</summary>
    private static async Task<(List<StreamChunk> Chunks, GraphExecutionResult<Job> Result)> Stream(
        CompiledGraph<Job> graph, ICheckpointStore? store = null, TimeSpan? consumerDelay = null)
    {
        var (reader, completion) = graph.RunStreaming("t", NewJob(), store ?? new InMemoryCheckpointStore());
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

    private static List<(StreamChunkType Type, string Node, string Content)> Shape(IEnumerable<StreamChunk> chunks) =>
        [.. chunks.Select(c => (c.Type, c.NodeName, c.Payload.ToString()))];

    /// <summary>The <see cref="Shape"/> of the chunks attributed to <paramref name="node"/> or anything under it.</summary>
    private static List<(StreamChunkType Type, string Node, string Content)> ShapeUnder(
        IEnumerable<StreamChunk> chunks, string node) =>
        Shape(chunks.Where(c => c.NodeName == node || c.NodeName.StartsWith(node + "/", StringComparison.Ordinal)));

    private static List<string> TokensFrom(IEnumerable<StreamChunk> chunks, string node) =>
        [.. chunks.Where(c => c.Type == StreamChunkType.Token && c.NodeName == node).Select(c => c.Payload.ToString())];

    private static int Count(IEnumerable<StreamChunk> chunks, StreamChunkType type, string node) =>
        chunks.Count(c => c.Type == type && c.NodeName == node);

    [Fact]
    public async Task A_Child_Streaming_Node_Reaches_The_Consumer_Named_Subgraph_Slash_Node()
    {
        var child = new StateGraph<Job>()
            .AddStreamingNode("Talk", Talk("talk", "Hel", "lo"))
            .Compile();

        var (chunks, result) = await Stream(ParentAround(child));

        Assert.Equal(["before", "talk", "finish"], Completed(result).Log);
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Before", "Before"),
                (StreamChunkType.NodeEnd, "Before", "Before"),
                (StreamChunkType.NodeStart, "Work", "Work"),
                (StreamChunkType.NodeStart, "Work/Talk", "Talk"),
                (StreamChunkType.Token, "Work/Talk", "Hel"),
                (StreamChunkType.Token, "Work/Talk", "lo"),
                (StreamChunkType.NodeEnd, "Work/Talk", "Talk"),
                (StreamChunkType.NodeEnd, "Work", "Work"),
                (StreamChunkType.NodeStart, "Finish", "Finish"),
                (StreamChunkType.NodeEnd, "Finish", "Finish"),
            ],
            Shape(chunks));
    }

    [Fact]
    public async Task A_Child_Parallel_Groups_Streaming_Branches_Are_Named_Under_The_Subgraph()
    {
        var child = new StateGraph<Job>()
            .AddParallelGroup(
                "Fan",
                new ParallelBranches<Job>()
                    .StreamingBranch("a", (job, context) =>
                    {
                        context.EmitToken("a0");
                        context.EmitToken("a1");
                        return job;
                    })
                    .StreamingBranch("b", (job, context) =>
                    {
                        context.EmitToken("b0");
                        return job;
                    }),
                Concat)
            .Compile();

        var (chunks, result) = await Stream(ParentAround(child));

        Completed(result);
        Assert.Equal(["a0", "a1"], TokensFrom(chunks, "Work/Fan/a"));
        Assert.Equal(["b0"], TokensFrom(chunks, "Work/Fan/b"));
        // The group is a node of the child, so it has its own lifecycle pair.
        Assert.Equal(1, Count(chunks, StreamChunkType.NodeStart, "Work/Fan"));
        Assert.Equal(1, Count(chunks, StreamChunkType.NodeEnd, "Work/Fan"));
    }

    [Fact]
    public async Task Names_Nest_Through_A_Grandchild()
    {
        var grandchild = new StateGraph<Job>().AddStreamingNode("Talk", Talk("talk", "deep")).Compile();
        var child = new StateGraph<Job>()
            .AddSubgraph("Inner", grandchild, job => job, (_, result) => result)
            .Compile();

        var (chunks, result) = await Stream(ParentAround(child));

        Completed(result);
        Assert.Equal(["deep"], TokensFrom(chunks, "Work/Inner/Talk"));
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Work"),
                (StreamChunkType.NodeStart, "Work/Inner"),
                (StreamChunkType.NodeStart, "Work/Inner/Talk"),
                (StreamChunkType.Token, "Work/Inner/Talk"),
                (StreamChunkType.NodeEnd, "Work/Inner/Talk"),
                (StreamChunkType.NodeEnd, "Work/Inner"),
                (StreamChunkType.NodeEnd, "Work"),
            ],
            ShapeUnder(chunks, "Work").Select(s => (s.Type, s.Node)));
    }

    [Fact]
    public async Task One_Child_Graph_Reused_By_Two_Nodes_Names_Each_Run_By_Its_Own_Node()
    {
        var child = new StateGraph<Job>().AddStreamingNode("Talk", Talk("talk", "hi")).Compile();
        var graph = new StateGraph<Job>()
            .AddSubgraph("First", child, job => job, (_, result) => result)
            .AddSubgraph("Second", child, job => job, (_, result) => result)
            .AddEdge("First", "Second")
            .Compile();

        var (chunks, result) = await Stream(graph);

        Completed(result);
        Assert.Equal(["hi"], TokensFrom(chunks, "First/Talk"));
        Assert.Equal(["hi"], TokensFrom(chunks, "Second/Talk"));
    }

    [Fact]
    public async Task A_Plain_Run_Refuses_A_Subgraph_Whose_Child_Streams_Before_Running_Any_Of_It()
    {
        var ran = 0;
        var child = new StateGraph<Job>()
            .AddNode("Setup", job =>
            {
                ran++;
                return job;
            })
            .AddStreamingNode("Talk", Talk("talk", "x"))
            .AddEdge("Setup", "Talk")
            .Compile();

        var result = await ParentAround(child).RunAsync("t", NewJob(), new InMemoryCheckpointStore());

        var failed = Assert.IsType<GraphExecutionResult<Job>.Failed>(result);
        Assert.Contains("'Work'", failed.Message);
        Assert.Contains("RunStreaming", failed.Message);
        Assert.Equal(0, ran);
    }

    [Fact]
    public async Task A_Plain_Run_Refuses_A_Streaming_Grandchild_At_The_Outermost_Subgraph_Node()
    {
        var ran = 0;
        var grandchild = new StateGraph<Job>().AddStreamingNode("Talk", Talk("talk", "x")).Compile();
        var child = new StateGraph<Job>()
            .AddNode("Setup", job =>
            {
                ran++;
                return job;
            })
            .AddSubgraph("Inner", grandchild, job => job, (_, result) => result)
            .AddEdge("Setup", "Inner")
            .Compile();

        var result = await ParentAround(child).RunAsync("t", NewJob(), new InMemoryCheckpointStore());

        var failed = Assert.IsType<GraphExecutionResult<Job>.Failed>(result);
        Assert.Contains("'Work'", failed.Message);
        Assert.Equal(0, ran);
    }

    [Fact]
    public async Task A_Child_Group_With_Only_Plain_Branches_Still_Runs_Under_A_Plain_Run()
    {
        var child = new StateGraph<Job>()
            .AddParallelGroup("Fan", new ParallelBranches<Job>().Branch("a", _ => NewJob()), Concat)
            .Compile();

        var state = Completed(await ParentAround(child).RunAsync("t", NewJob(), new InMemoryCheckpointStore()));

        Assert.Equal(["before", "finish"], state.Log);
    }

    [Fact]
    public async Task Re_Adding_The_Name_Replaces_A_Streaming_Subgraph_And_Its_Stream_Requirement()
    {
        var child = new StateGraph<Job>().AddStreamingNode("Talk", Talk("talk", "x")).Compile();
        var graph = new StateGraph<Job>()
            .AddSubgraph("Work", child, job => job, (_, result) => result)
            .AddNode("Work", Note("plain"))
            .Compile();

        var state = Completed(await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore()));

        Assert.Equal(["plain"], state.Log);
    }

    /// <summary>
    /// A child whose group's one streaming branch emits <c>try&lt;n&gt;</c> and fails transiently on
    /// its first <paramref name="failures"/> attempts.
    /// </summary>
    private static CompiledGraph<Job> FlakyChild(int failures, RecoveryPolicy? childPolicy = null)
    {
        var attempts = 0;
        var child = new StateGraph<Job>().AddParallelGroup(
            "Fan",
            new ParallelBranches<Job>().StreamingBranch("flaky", (job, context) =>
            {
                var attempt = Interlocked.Increment(ref attempts);
                context.EmitToken($"try{attempt}");
                return attempt <= failures ? throw NodeException.Transient("timeout") : job;
            }),
            Concat);
        if (childPolicy is not null)
        {
            child.SetRecoveryPolicy("Fan", childPolicy);
        }

        return child.Compile();
    }

    [Fact]
    public async Task A_Retry_Of_The_Subgraph_Is_Announced_Before_The_Child_Streams_Again()
    {
        var graph = ParentAround(FlakyChild(failures: 1), RecoveryPolicy.Retry(2));

        var (chunks, result) = await Stream(graph);

        Completed(result);
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Work", "Work"),
                (StreamChunkType.NodeStart, "Work/Fan", "Fan"),
                (StreamChunkType.Token, "Work/Fan/flaky", "try1"),
                // The child's group gave up (it has no policy of its own)...
                (StreamChunkType.Error, "Work/Fan", "branch 'flaky': timeout"),
                // ...and the parent's policy re-runs the whole child. The Retry is the parent's own
                // node, carrying the child's failure message.
                (
                    StreamChunkType.Retry,
                    "Work",
                    "subgraph 'Work' failed at 'Fan' after 1 attempt(s): branch 'flaky': timeout"),
                (StreamChunkType.NodeStart, "Work/Fan", "Fan"),
                (StreamChunkType.Token, "Work/Fan/flaky", "try2"),
                (StreamChunkType.NodeEnd, "Work/Fan", "Fan"),
                (StreamChunkType.NodeEnd, "Work", "Work"),
            ],
            ShapeUnder(chunks, "Work"));
    }

    [Fact]
    public async Task A_Childs_Own_Retry_Is_Announced_Under_The_Childs_Node_Not_The_Subgraphs()
    {
        var graph = ParentAround(FlakyChild(failures: 1, RecoveryPolicy.Retry(1)));

        var (chunks, result) = await Stream(graph);

        Completed(result);
        Assert.Equal(1, Count(chunks, StreamChunkType.Retry, "Work/Fan"));
        Assert.Equal(0, Count(chunks, StreamChunkType.Retry, "Work"));
        Assert.Equal(["try1", "try2"], TokensFrom(chunks, "Work/Fan/flaky"));
    }

    [Fact]
    public async Task A_Child_That_Gives_Up_Emits_An_Error_For_Its_Node_Then_One_For_The_Subgraph()
    {
        var graph = ParentAround(FlakyChild(failures: 5));

        var (chunks, result) = await Stream(graph);

        Assert.Equal("Work", Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result).Node);
        Assert.Equal(
            [
                (StreamChunkType.Error, "Work/Fan", "branch 'flaky': timeout"),
                (
                    StreamChunkType.Error,
                    "Work",
                    "subgraph 'Work' failed at 'Fan' after 1 attempt(s): branch 'flaky': timeout"),
            ],
            Shape(chunks).Where(s => s.Type == StreamChunkType.Error));
        // Neither node completed.
        Assert.Equal(0, Count(chunks, StreamChunkType.NodeEnd, "Work/Fan"));
        Assert.Equal(0, Count(chunks, StreamChunkType.NodeEnd, "Work"));
    }

    [Fact]
    public async Task A_Child_Streaming_Past_The_Channel_Bound_Loses_No_Tokens()
    {
        // 3000 tokens is well past the 1024-chunk channel and the consumer holds off reading, so the
        // child is blocked in a send while the subgraph node is mid-run. Backpressure must pause it,
        // not drop tokens.
        const int Tokens = 3000;
        var child = new StateGraph<Job>()
            .AddStreamingNode("Talk", (job, context) =>
            {
                for (var i = 0; i < Tokens; i++)
                {
                    context.EmitToken(i.ToString(System.Globalization.CultureInfo.InvariantCulture));
                }

                return job;
            })
            .Compile();

        var (chunks, result) = await Stream(ParentAround(child), consumerDelay: TimeSpan.FromMilliseconds(200));

        Completed(result);
        Assert.Equal(
            Enumerable.Range(0, Tokens).Select(i => i.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            TokensFrom(chunks, "Work/Talk"));
    }

    // --- Streaming and nested interrupts -----------------------------------

    private static HumanSignal NoSignal() => new(IsApproved: true, ReviewerToken: "x", FieldOverrides: null);

    /// <summary><c>Draft</c> (streams, and is a boundary) -> <c>Publish</c>.</summary>
    private static CompiledGraph<Job> ReviewedChild() =>
        new StateGraph<Job>()
            .AddStreamingNode("Draft", Talk("draft", "v1"))
            .AddNode("Publish", Note("publish"))
            .AddEdge("Draft", "Publish")
            .AddInterruptBoundary("Draft")
            .Compile();

    [Fact]
    public async Task A_Child_That_Freezes_Has_Streamed_Up_To_The_Boundary_And_The_Subgraph_Node_Never_Ends()
    {
        var (chunks, result) = await Stream(ParentAround(ReviewedChild()));

        Assert.Equal("Work", Assert.IsType<GraphExecutionResult<Job>.Interrupted>(result).SuspendedNode);
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Work", "Work"),
                (StreamChunkType.NodeStart, "Work/Draft", "Draft"),
                (StreamChunkType.Token, "Work/Draft", "v1"),
                (StreamChunkType.NodeEnd, "Work/Draft", "Draft"),
            ],
            ShapeUnder(chunks, "Work"));
    }

    [Fact]
    public async Task A_Streamed_Child_That_Froze_After_Its_Streaming_Can_Be_Resumed()
    {
        var graph = ParentAround(ReviewedChild());
        var store = new InMemoryCheckpointStore();
        var (_, frozen) = await Stream(graph, store);
        Assert.IsType<GraphExecutionResult<Job>.Interrupted>(frozen);

        // A plain ResumeAsync is not streamed, but the child's streaming node already ran.
        var state = Completed(await graph.ResumeAsync("t", NoSignal(), store));

        Assert.Equal(["before", "draft", "publish", "finish"], state.Log);
    }

    [Fact]
    public async Task A_Resume_That_Reaches_A_Streaming_Child_Node_Fails_There_Like_A_Flat_Graph()
    {
        // The boundary comes first, so the streaming node is still ahead when the resume (which has
        // no StreamContext) re-enters the child.
        var child = new StateGraph<Job>()
            .AddNode("Review", Note("review"))
            .AddStreamingNode("Talk", Talk("talk", "x"))
            .AddEdge("Review", "Talk")
            .AddInterruptBoundary("Review")
            .Compile();
        var graph = ParentAround(child);
        var store = new InMemoryCheckpointStore();
        var (_, frozen) = await Stream(graph, store);
        Assert.IsType<GraphExecutionResult<Job>.Interrupted>(frozen);

        var result = await graph.ResumeAsync("t", NoSignal(), store);

        var failed = Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result);
        Assert.Equal("Work", failed.Node);
        Assert.Contains("'Talk'", failed.Message);
        Assert.Contains("no StreamContext", failed.Message);
    }
}
