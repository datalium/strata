using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/resume_streaming.rs</c>: <c>ResumeStreaming</c> streams the
/// resumed half of an interrupted run like a fresh <c>RunStreaming</c>, for a flat graph and through
/// nested subgraphs. Keep the two in step.
/// </summary>
public class ResumeStreamingTests
{
    public sealed record Job(
        [property: JsonPropertyName("log")] IReadOnlyList<string> Log,
        [property: JsonPropertyName("who")] string Who);

    private static Job NewJob() => new(Log: [], Who: "");

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

    private static HumanSignal Approve() => new(IsApproved: true, ReviewerToken: "r", FieldOverrides: null);

    private static HumanSignal ApproveAs(string who) =>
        new(IsApproved: true, ReviewerToken: "r", FieldOverrides: new Dictionary<string, string> { ["who"] = who });

    private static async Task<List<StreamChunk>> Drain(System.Threading.Channels.ChannelReader<StreamChunk> reader)
    {
        var chunks = new List<StreamChunk>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            chunks.Add(chunk);
        }

        return chunks;
    }

    /// <summary>Runs <paramref name="graph"/> under <c>RunStreaming</c> up to its first interrupt.</summary>
    private static async Task Freeze(CompiledGraph<Job> graph, ICheckpointStore store)
    {
        var (reader, completion) = graph.RunStreaming("t", NewJob(), store);
        await Drain(reader);
        Assert.IsType<GraphExecutionResult<Job>.Interrupted>(await completion);
    }

    /// <summary>Resumes <c>t</c> under <c>ResumeStreaming</c> and collects every chunk.</summary>
    private static async Task<(List<StreamChunk> Chunks, GraphExecutionResult<Job> Result)> Resume(
        CompiledGraph<Job> graph, ICheckpointStore store, HumanSignal signal, TimeSpan? consumerDelay = null)
    {
        var (reader, completion) = graph.ResumeStreaming("t", signal, store);
        if (consumerDelay is { } delay)
        {
            await Task.Delay(delay);
        }

        return (await Drain(reader), await completion);
    }

    private static Job Completed(GraphExecutionResult<Job> result) =>
        Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State;

    private static List<(StreamChunkType Type, string Node, string Content)> Shape(IEnumerable<StreamChunk> chunks) =>
        [.. chunks.Select(c => (c.Type, c.NodeName, c.Payload.ToString()))];

    private static List<(StreamChunkType Type, string Node, string Content)> ShapeUnder(
        IEnumerable<StreamChunk> chunks, string node) =>
        Shape(chunks.Where(c => c.NodeName == node || c.NodeName.StartsWith(node + "/", StringComparison.Ordinal)));

    private static List<string> TokensFrom(IEnumerable<StreamChunk> chunks, string node) =>
        [.. chunks.Where(c => c.Type == StreamChunkType.Token && c.NodeName == node).Select(c => c.Payload.ToString())];

    // --- A flat graph ------------------------------------------------------

    /// <summary><c>Start</c> -> <c>Review</c> (boundary) -> <c>Talk</c> (streams) -> <c>Finish</c>.</summary>
    private static CompiledGraph<Job> FlatGraph() =>
        new StateGraph<Job>()
            .AddNode("Start", Note("start"))
            .AddNode("Review", Note("review"))
            .AddStreamingNode("Talk", Talk("talk", "Hel", "lo"))
            .AddNode("Finish", Note("finish"))
            .AddEdge("Start", "Review")
            .AddEdge("Review", "Talk")
            .AddEdge("Talk", "Finish")
            .AddInterruptBoundary("Review")
            .Compile();

    [Fact]
    public async Task A_Streaming_Node_After_The_Boundary_Streams_On_Resume()
    {
        var graph = FlatGraph();
        var store = new InMemoryCheckpointStore();
        await Freeze(graph, store);

        var (chunks, result) = await Resume(graph, store, Approve());

        Assert.Equal(["start", "review", "talk", "finish"], Completed(result).Log);
        // The stream starts at the node resumed into; Start and Review ran before the freeze and are
        // not repeated.
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Talk", "Talk"),
                (StreamChunkType.Token, "Talk", "Hel"),
                (StreamChunkType.Token, "Talk", "lo"),
                (StreamChunkType.NodeEnd, "Talk", "Talk"),
                (StreamChunkType.NodeStart, "Finish", "Finish"),
                (StreamChunkType.NodeEnd, "Finish", "Finish"),
            ],
            Shape(chunks));
    }

    [Fact]
    public async Task The_Humans_Field_Overrides_Are_Applied_Before_The_Streaming_Node_Runs()
    {
        var graph = new StateGraph<Job>()
            .AddNode("Review", Note("review"))
            .AddStreamingNode("Talk", (job, context) =>
            {
                context.EmitToken(job.Who);
                return job;
            })
            .AddEdge("Review", "Talk")
            .AddInterruptBoundary("Review")
            .Compile();
        var store = new InMemoryCheckpointStore();
        await Freeze(graph, store);

        var (chunks, result) = await Resume(graph, store, ApproveAs("alice"));

        Assert.Equal("alice", Completed(result).Who);
        Assert.Equal(["alice"], TokensFrom(chunks, "Talk"));
    }

    [Fact]
    public async Task Resuming_A_Thread_With_No_Checkpoint_Fails_And_The_Stream_Ends_Empty()
    {
        var (chunks, result) = await Resume(FlatGraph(), new InMemoryCheckpointStore(), Approve());

        Assert.Empty(chunks);
        Assert.Contains("no checkpoint found", Assert.IsType<GraphExecutionResult<Job>.Failed>(result).Message);
    }

    [Fact]
    public async Task A_Checkpoint_Can_Only_Be_Resumed_Once()
    {
        var graph = FlatGraph();
        var store = new InMemoryCheckpointStore();
        await Freeze(graph, store);
        Completed((await Resume(graph, store, Approve())).Result);

        var (chunks, result) = await Resume(graph, store, Approve());

        Assert.Empty(chunks);
        Assert.Contains("already resumed", Assert.IsType<GraphExecutionResult<Job>.Failed>(result).Message);
    }

    [Fact]
    public async Task A_Streamed_Resume_That_Reaches_A_Second_Boundary_Freezes_And_Can_Be_Resumed_Again()
    {
        // Review -> Draft (streams, boundary) -> Publish (streams).
        var graph = new StateGraph<Job>()
            .AddNode("Review", Note("review"))
            .AddStreamingNode("Draft", Talk("draft", "v1"))
            .AddStreamingNode("Publish", Talk("publish", "final"))
            .AddEdge("Review", "Draft")
            .AddEdge("Draft", "Publish")
            .AddInterruptBoundary("Review")
            .AddInterruptBoundary("Draft")
            .Compile();
        var store = new InMemoryCheckpointStore();
        await Freeze(graph, store);

        var (first, result) = await Resume(graph, store, Approve());
        Assert.Equal("Draft", Assert.IsType<GraphExecutionResult<Job>.Interrupted>(result).SuspendedNode);
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Draft", "Draft"),
                (StreamChunkType.Token, "Draft", "v1"),
                (StreamChunkType.NodeEnd, "Draft", "Draft"),
            ],
            Shape(first));

        var (second, final) = await Resume(graph, store, Approve());
        Assert.Equal(["review", "draft", "publish"], Completed(final).Log);
        Assert.Equal(["final"], TokensFrom(second, "Publish"));
    }

    [Fact]
    public async Task A_Resumed_Streaming_Node_Past_The_Channel_Bound_Loses_No_Tokens()
    {
        // 3000 tokens is well past the 1024-chunk channel and the consumer holds off reading, so the
        // node is blocked in a send. Backpressure must pause it, not drop tokens.
        const int Tokens = 3000;
        var graph = new StateGraph<Job>()
            .AddNode("Review", Note("review"))
            .AddStreamingNode("Talk", (job, context) =>
            {
                for (var i = 0; i < Tokens; i++)
                {
                    context.EmitToken(i.ToString(System.Globalization.CultureInfo.InvariantCulture));
                }

                return job;
            })
            .AddEdge("Review", "Talk")
            .AddInterruptBoundary("Review")
            .Compile();
        var store = new InMemoryCheckpointStore();
        await Freeze(graph, store);

        var (chunks, result) = await Resume(graph, store, Approve(), TimeSpan.FromMilliseconds(200));

        Completed(result);
        Assert.Equal(
            Enumerable.Range(0, Tokens).Select(i => i.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            TokensFrom(chunks, "Talk"));
    }

    // --- Through a subgraph ------------------------------------------------

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

    /// <summary><c>Review</c> (boundary) -> <c>Talk</c> (streams).</summary>
    private static CompiledGraph<Job> ReviewedChild() =>
        new StateGraph<Job>()
            .AddNode("Review", Note("review"))
            .AddStreamingNode("Talk", Talk("talk", "Hel", "lo"))
            .AddEdge("Review", "Talk")
            .AddInterruptBoundary("Review")
            .Compile();

    [Fact]
    public async Task A_Child_Streaming_Node_After_Its_Boundary_Streams_On_Resume_Named_Under_The_Subgraph()
    {
        var graph = ParentAround(ReviewedChild());
        var store = new InMemoryCheckpointStore();
        await Freeze(graph, store);

        var (chunks, result) = await Resume(graph, store, Approve());

        Assert.Equal(["before", "review", "talk", "finish"], Completed(result).Log);
        Assert.Equal(
            [
                // The subgraph node starts again: its first NodeStart, from the run that froze, never
                // got a NodeEnd.
                (StreamChunkType.NodeStart, "Work", "Work"),
                // The child resumes at Talk; Review is not repeated.
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
    public async Task Names_Nest_Through_A_Grandchild_On_Resume()
    {
        var child = new StateGraph<Job>()
            .AddSubgraph("Inner", ReviewedChild(), job => job, (_, result) => result)
            .Compile();
        var graph = ParentAround(child);
        var store = new InMemoryCheckpointStore();
        await Freeze(graph, store);

        var (chunks, result) = await Resume(graph, store, Approve());

        Completed(result);
        Assert.Equal(["Hel", "lo"], TokensFrom(chunks, "Work/Inner/Talk"));
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Work"),
                (StreamChunkType.NodeStart, "Work/Inner"),
                (StreamChunkType.NodeStart, "Work/Inner/Talk"),
                (StreamChunkType.Token, "Work/Inner/Talk"),
                (StreamChunkType.Token, "Work/Inner/Talk"),
                (StreamChunkType.NodeEnd, "Work/Inner/Talk"),
                (StreamChunkType.NodeEnd, "Work/Inner"),
                (StreamChunkType.NodeEnd, "Work"),
            ],
            ShapeUnder(chunks, "Work").Select(s => (s.Type, s.Node)));
    }

    [Fact]
    public async Task The_Humans_Overrides_Reach_The_Childs_State_Before_It_Streams()
    {
        var child = new StateGraph<Job>()
            .AddNode("Review", Note("review"))
            .AddStreamingNode("Talk", (job, context) =>
            {
                context.EmitToken(job.Who);
                return job;
            })
            .AddEdge("Review", "Talk")
            .AddInterruptBoundary("Review")
            .Compile();
        var graph = ParentAround(child);
        var store = new InMemoryCheckpointStore();
        await Freeze(graph, store);

        var (chunks, result) = await Resume(graph, store, ApproveAs("alice"));

        Completed(result);
        Assert.Equal(["alice"], TokensFrom(chunks, "Work/Talk"));
    }

    [Fact]
    public async Task A_Retry_After_A_Streamed_Resume_Reruns_The_Child_From_The_Boundary_Not_The_Start()
    {
        var reviews = 0;
        var attempts = 0;
        // Review (boundary, counted) -> Fan (a streaming branch that fails once).
        var child = new StateGraph<Job>()
            .AddNode("Review", job =>
            {
                Interlocked.Increment(ref reviews);
                return job;
            })
            .AddParallelGroup(
                "Fan",
                new ParallelBranches<Job>().StreamingBranch("flaky", (job, context) =>
                {
                    var attempt = Interlocked.Increment(ref attempts);
                    context.EmitToken($"try{attempt}");
                    return attempt < 2 ? throw NodeException.Transient("timeout") : job;
                }),
                Concat)
            .AddEdge("Review", "Fan")
            .AddInterruptBoundary("Review")
            .Compile();
        var graph = ParentAround(child, RecoveryPolicy.Retry(2));
        var store = new InMemoryCheckpointStore();
        await Freeze(graph, store);

        var (chunks, result) = await Resume(graph, store, Approve());

        Completed(result);
        Assert.Equal(1, reviews);
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Work", "Work"),
                (StreamChunkType.NodeStart, "Work/Fan", "Fan"),
                (StreamChunkType.Token, "Work/Fan/flaky", "try1"),
                (StreamChunkType.Error, "Work/Fan", "branch 'flaky': timeout"),
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
    public async Task A_Child_That_Freezes_Again_After_A_Streamed_Resume_Can_Be_Resumed_Streaming_Again()
    {
        // Review (boundary) -> Draft (streams, boundary) -> Publish (streams).
        var child = new StateGraph<Job>()
            .AddNode("Review", Note("review"))
            .AddStreamingNode("Draft", Talk("draft", "v1"))
            .AddStreamingNode("Publish", Talk("publish", "final"))
            .AddEdge("Review", "Draft")
            .AddEdge("Draft", "Publish")
            .AddInterruptBoundary("Review")
            .AddInterruptBoundary("Draft")
            .Compile();
        var graph = ParentAround(child);
        var store = new InMemoryCheckpointStore();
        await Freeze(graph, store);

        var (first, result) = await Resume(graph, store, Approve());
        Assert.Equal("Work", Assert.IsType<GraphExecutionResult<Job>.Interrupted>(result).SuspendedNode);
        Assert.Equal(["v1"], TokensFrom(first, "Work/Draft"));
        Assert.DoesNotContain(first, c => c.Type == StreamChunkType.NodeEnd && c.NodeName == "Work");

        var (second, final) = await Resume(graph, store, Approve());
        Assert.Equal(["before", "review", "draft", "publish", "finish"], Completed(final).Log);
        Assert.Equal(["final"], TokensFrom(second, "Work/Publish"));
    }
}
