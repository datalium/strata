using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/hop_limit.rs</c> (<c>design/24-Loop-Circuit-Breaker.md</c>): a
/// compiled hop limit stops a thread before the node that would be one hop too many, with a
/// <c>Failed</c> checkpoint whose <c>error_kind</c> is <c>hop_limit</c>, and the count survives
/// interrupts and resumes through <c>strata.hops</c>. Keep the two in step.
/// </summary>
public class HopLimitTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Job(int N, IReadOnlyList<string> Trail);

    private static Job NewJob() => new(0, []);

    private static Func<Job, Job> Step(string label) => job => job with { Trail = [.. job.Trail, label] };

    private static HumanSignal Go() => HumanSignal.Approve("oncall");

    /// <summary>Counts every time a node's handler runs, by node.</summary>
    private sealed class Calls
    {
        private readonly ConcurrentDictionary<string, int> _calls = new();

        public Func<Job, Job> Count(string node, Func<Job, Job> handler) => job =>
        {
            _calls.AddOrUpdate(node, 1, (_, n) => n + 1);
            return handler(job);
        };

        public int this[string node] => _calls.GetValueOrDefault(node);
    }

    /// <summary>
    /// <c>A</c> (n + 1) -> <c>B</c> -> back to <c>A</c> while n &lt; <paramref name="stopAt"/>, else
    /// <c>Done</c>. With <c>stopAt</c> s the run is 2s + 1 hops: A B A B … A B Done.
    /// </summary>
    private static StateGraph<Job> Loop(Calls calls, int stopAt, bool boundaryAtB = false)
    {
        var graph = new StateGraph<Job>()
            .AddNode("A", calls.Count("A", job => job with { N = job.N + 1, Trail = [.. job.Trail, "A"] }))
            .AddNode("B", calls.Count("B", Step("B")))
            .AddNode("Done", calls.Count("Done", Step("done")))
            .AddEdge("A", "B")
            .AddConditionalEdge("B", "A", job => job.N < stopAt)
            .AddEdge("B", "Done");
        return boundaryAtB ? graph.AddInterruptBoundary("B") : graph;
    }

    private static CompilationOptions Limit(int hops) => new() { MaxNodeHops = hops };

    private static GraphExecutionResult<Job>.HopLimitExceeded Tripped(GraphExecutionResult<Job> result) =>
        Assert.IsType<GraphExecutionResult<Job>.HopLimitExceeded>(result);

    private static Job Completed(GraphExecutionResult<Job> result) =>
        Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State;

    private static string? Hops(Checkpoint checkpoint) => checkpoint.Metadata?.GetValueOrDefault("strata.hops");

    [Fact]
    public async Task Without_A_Limit_A_Long_Loop_Completes_And_Checkpoints_Carry_No_Hop_Count()
    {
        var calls = new Calls();
        var graph = Loop(calls, stopAt: 25).Compile();

        var job = Completed(await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore()));

        Assert.Equal(51, job.Trail.Count);
        Assert.Null(graph.MaxNodeHops);

        var store = new InMemoryCheckpointStore();
        await Loop(new Calls(), stopAt: 25, boundaryAtB: true).Compile().RunAsync("t", NewJob(), store);
        Assert.Null(store.Load("t")!.Metadata);
    }

    [Fact]
    public async Task A_Loop_That_Never_Exits_Is_Stopped_Before_Hop_N_Plus_One()
    {
        var calls = new Calls();
        var graph = Loop(calls, stopAt: int.MaxValue).Compile(Limit(5));
        var store = new InMemoryCheckpointStore();

        var result = await graph.RunAsync("t", NewJob(), store);

        var tripped = Tripped(result);
        Assert.Equal(("B", 5, 5), (tripped.Node, tripped.Hops, tripped.Limit));
        Assert.Equal("hop limit of 5 reached: node 'B' would be hop 6", tripped.Message);
        Assert.IsAssignableFrom<GraphExecutionResult<Job>.Failed>(result);
        Assert.Equal(5, graph.MaxNodeHops);

        // Five nodes ran (A B A B A); the sixth never started.
        Assert.Equal((3, 2), (calls["A"], calls["B"]));

        var checkpoint = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Failed, checkpoint.Status);
        Assert.Equal(("B", "B"), (checkpoint.SuspendedNode, checkpoint.NextNode));
        Assert.Equal("hop_limit", checkpoint.Metadata!["error_kind"]);
        Assert.Equal(tripped.Message, checkpoint.Metadata["error"]);
        Assert.Equal("5", Hops(checkpoint));
        Assert.False(checkpoint.Metadata.ContainsKey("attempts"));
        Assert.Equal(3, checkpoint.FrozenState!["N"]!.GetValue<int>());
    }

    [Theory]
    [InlineData(5)]
    [InlineData(6)]
    public async Task A_Loop_That_Ends_At_Or_Before_The_Limit_Is_Untouched(int limit)
    {
        // stopAt 2: A B A B Done, five hops.
        var graph = Loop(new Calls(), stopAt: 2).Compile(Limit(limit));

        var job = Completed(await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore()));

        Assert.Equal(["A", "B", "A", "B", "done"], job.Trail);
    }

    [Fact]
    public async Task Retries_Are_Not_Hops()
    {
        var attempts = 0;
        StateGraph<Job> Flaky() => new StateGraph<Job>()
            .AddFallibleNode("Flaky", job => Interlocked.Increment(ref attempts) % 3 != 0
                ? throw NodeException.Transient("blip")
                : job)
            .AddNode("End", Step("end"))
            .AddEdge("Flaky", "End")
            .SetRecoveryPolicy("Flaky", RecoveryPolicy.Retry(3));

        Completed(await Flaky().Compile(Limit(2)).RunAsync("t", NewJob(), new InMemoryCheckpointStore()));
        Assert.Equal(3, attempts);

        var tripped = Tripped(await Flaky().Compile(Limit(1)).RunAsync("t", NewJob(), new InMemoryCheckpointStore()));
        Assert.Equal(("End", 1), (tripped.Node, tripped.Hops));
    }

    [Fact]
    public async Task A_Fallback_Node_Is_A_Hop()
    {
        static StateGraph<Job> Rescued() => new StateGraph<Job>()
            .AddFallibleNode("Broken", Job (_) => throw NodeException.Fatal("down"))
            .AddNode("Rescue", Step("rescue"))
            .AddNode("End", Step("end"))
            .AddEdge("Broken", "End")
            .AddEdge("Rescue", "End")
            .SetRecoveryPolicy("Broken", RecoveryPolicy.Default.WithFallback("Rescue"));

        Completed(await Rescued().Compile(Limit(3)).RunAsync("t", NewJob(), new InMemoryCheckpointStore()));

        var tripped = Tripped(await Rescued().Compile(Limit(2)).RunAsync("t", NewJob(), new InMemoryCheckpointStore()));
        Assert.Equal(("End", 2), (tripped.Node, tripped.Hops));
    }

    /// <summary>Four nodes in a row, so a child that runs is four hops at its own level.</summary>
    private static StateGraph<Job> FourSteps() => new StateGraph<Job>()
        .AddNode("S1", Step("s1"))
        .AddNode("S2", Step("s2"))
        .AddNode("S3", Step("s3"))
        .AddNode("S4", Step("s4"))
        .AddEdge("S1", "S2")
        .AddEdge("S2", "S3")
        .AddEdge("S3", "S4");

    private static StateGraph<Job> GatherWorkEnd(CompiledGraph<Job> child) => new StateGraph<Job>()
        .AddParallelGroup(
            "Gather",
            new ParallelBranches<Job>().Branch("X", Step("x")).Branch("Y", Step("y")).Branch("Z", Step("z")),
            (entry, results) => entry with { Trail = [.. entry.Trail, .. results.SelectMany(r => r.Trail)] })
        .AddSubgraph("Work", child, job => job, (_, job) => job)
        .AddNode("End", Step("end"))
        .AddEdge("Gather", "Work")
        .AddEdge("Work", "End");

    [Fact]
    public async Task A_Parallel_Group_And_A_Subgraph_Are_One_Hop_Each()
    {
        // Gather, Work and End: three hops, whatever runs inside the first two. The child counts its
        // four nodes against its own limit of 4.
        var child = FourSteps().Compile(Limit(4));
        Completed(await GatherWorkEnd(child).Compile(Limit(3)).RunAsync("t", NewJob(), new InMemoryCheckpointStore()));

        var tripped = Tripped(
            await GatherWorkEnd(child).Compile(Limit(2)).RunAsync("t", NewJob(), new InMemoryCheckpointStore()));
        Assert.Equal(("End", 2), (tripped.Node, tripped.Hops));
    }

    [Fact]
    public async Task The_Count_Survives_An_Interrupt_Boundary_Inside_The_Loop()
    {
        var calls = new Calls();
        var graph = Loop(calls, stopAt: int.MaxValue, boundaryAtB: true).Compile(Limit(5));
        var store = new InMemoryCheckpointStore();

        Assert.IsType<GraphExecutionResult<Job>.Interrupted>(await graph.RunAsync("t", NewJob(), store));
        Assert.Equal("2", Hops(store.Load("t")!));

        Assert.IsType<GraphExecutionResult<Job>.Interrupted>(await graph.ResumeAsync("t", Go(), store));
        Assert.Equal("4", Hops(store.Load("t")!));

        // The same hop a run without interrupts trips at.
        var tripped = Tripped(await graph.ResumeAsync("t", Go(), store));
        Assert.Equal(("B", 5, 5), (tripped.Node, tripped.Hops, tripped.Limit));
        Assert.Equal((3, 2), (calls["A"], calls["B"]));
    }

    /// <summary><c>Plan</c> -> <c>Approve</c> (a boundary) -> <c>Apply</c>: StrataOps' remediation.</summary>
    private static CompiledGraph<Job> Remediation(Calls calls) => new StateGraph<Job>()
        .AddNode("Plan", calls.Count("Plan", Step("plan")))
        .AddNode("Approve", calls.Count("Approve", Step("approve")))
        .AddInterruptBoundary("Approve")
        .AddNode("Apply", calls.Count("Apply", Step("apply")))
        .AddEdge("Plan", "Approve")
        .AddEdge("Approve", "Apply")
        .Compile();

    /// <summary><c>Check</c> (n + 1) -> <c>Remediate</c> (the subgraph) -> back to <c>Check</c>, forever.</summary>
    private static StateGraph<Job> InvestigationLoop(Calls calls, CompiledGraph<Job> remediation) => new StateGraph<Job>()
        .AddNode("Check", calls.Count("Check", job => job with { N = job.N + 1 }))
        .AddSubgraph("Remediate", remediation, job => job, (_, job) => job)
        .AddNode("Done", Step("done"))
        .AddEdge("Check", "Remediate")
        .AddConditionalEdge("Remediate", "Check", _ => true)
        .AddEdge("Remediate", "Done");

    [Fact]
    public async Task The_Count_Survives_An_Interrupt_Nested_In_A_Subgraph_Inside_The_Loop()
    {
        var calls = new Calls();
        var graph = InvestigationLoop(calls, Remediation(calls)).Compile(Limit(5));
        var store = new InMemoryCheckpointStore();

        // Check (1), Remediate (2) freezes inside: the subgraph node is re-entered, so the count is
        // the hops before it.
        var first = Assert.IsType<GraphExecutionResult<Job>.Interrupted>(await graph.RunAsync("t", NewJob(), store));
        Assert.Equal(["Remediate", "Approve"], first.WaitingAt);
        Assert.Equal("1", Hops(store.Load("t")!));
        Assert.Null(store.Load("t")!.Nested!.Metadata);

        // Remediate is hop 2 again, then Check (3), Remediate (4) freezes.
        Assert.IsType<GraphExecutionResult<Job>.Interrupted>(await graph.ResumeAsync("t", Go(), store));
        Assert.Equal("3", Hops(store.Load("t")!));

        // Remediate (4) again, Check (5), and the third Remediate would be hop 6.
        var tripped = Tripped(await graph.ResumeAsync("t", Go(), store));
        Assert.Equal(("Remediate", 5, 5), (tripped.Node, tripped.Hops, tripped.Limit));
        Assert.Equal((3, 2, 2), (calls["Check"], calls["Plan"], calls["Apply"]));
    }

    [Fact]
    public async Task A_Child_That_Hits_Its_Own_Limit_Fails_The_Subgraph_Node_Fatally()
    {
        var calls = new Calls();
        var child = Loop(calls, stopAt: int.MaxValue).Compile(Limit(3));
        var graph = new StateGraph<Job>()
            .AddSubgraph("Work", child, job => job, (_, job) => job)
            .AddNode("End", Step("end"))
            .AddEdge("Work", "End")
            .Compile(Limit(10));
        var store = new InMemoryCheckpointStore();
        var channel = new RunEventChannel();

        var result = await graph.RunAsync("t", NewJob(), store, new RunOptions { Observer = channel });

        var failed = Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result);
        Assert.Equal(("Work", 1), (failed.Node, failed.Attempts));
        Assert.Equal(
            "subgraph 'Work' hit its hop limit: hop limit of 3 reached: node 'B' would be hop 4", failed.Message);

        var checkpoint = store.Load("t")!;
        Assert.Equal(("fatal", "0"), (checkpoint.Metadata!["error_kind"], Hops(checkpoint)));
        var nested = checkpoint.Nested!;
        Assert.Equal((CheckpointStatus.Failed, "B"), (nested.Status, nested.NextNode));
        Assert.Equal(("hop_limit", "3"), (nested.Metadata!["error_kind"], Hops(nested)));

        var events = await Drain(channel);
        var hit = Assert.Single(events.OfType<HopLimitExceeded>());
        Assert.Equal(("Work/B", 3, 3), (hit.NodePath, hit.Hops, hit.Limit));

        // A resume re-enters the child at its hop-limit checkpoint, which trips again.
        Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(await graph.ResumeAsync("t", Go(), store));
        Assert.Equal((2, 1), (calls["A"], calls["B"]));
    }

    [Fact]
    public async Task Resuming_A_Hop_Limit_Checkpoint_Trips_Again_Unless_The_Resume_Raises_The_Limit()
    {
        var calls = new Calls();
        var graph = Loop(calls, stopAt: int.MaxValue).Compile(Limit(5));
        var store = new InMemoryCheckpointStore();
        Tripped(await graph.RunAsync("t", NewJob(), store));

        var again = Tripped(await graph.ResumeAsync("t", Go(), store));
        Assert.Equal(("B", 5, 5), (again.Node, again.Hops, again.Limit));
        Assert.Equal((3, 2), (calls["A"], calls["B"]));
        var latest = store.Load("t")!;
        Assert.Equal(("hop_limit", null), (latest.Metadata!["error_kind"], latest.ResumedAt));

        // Two more hops (B, A), counted on from the stored five.
        var raised = Tripped(await graph.ResumeAsync("t", Go(), store, new RunOptions { MaxNodeHops = 7 }));
        Assert.Equal(("B", 7, 7), (raised.Node, raised.Hops, raised.Limit));
        Assert.Equal((4, 3), (calls["A"], calls["B"]));
    }

    [Fact]
    public async Task Cancellation_Wins_And_A_Cancelled_Checkpoint_Counts_The_Hops_Before_Its_Node()
    {
        var graph = Loop(new Calls(), stopAt: int.MaxValue).Compile(Limit(5));
        var store = new InMemoryCheckpointStore();
        Tripped(await graph.RunAsync("t", NewJob(), store));

        var cancelled = await graph.ResumeAsync("t", Go(), store, new CancellationToken(canceled: true));

        Assert.IsType<GraphExecutionResult<Job>.Cancelled>(cancelled);
        var checkpoint = store.Load("t")!;
        Assert.Equal((CheckpointStatus.Cancelled, "B", "5"), (checkpoint.Status, checkpoint.NextNode, Hops(checkpoint)));
    }

    [Fact]
    public async Task A_Malformed_Hop_Count_Refuses_The_Resume_Before_Anything_Is_Stamped()
    {
        var graph = Loop(new Calls(), stopAt: int.MaxValue, boundaryAtB: true).Compile(Limit(5));
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync("t", NewJob(), store);
        store.Save(store.Load("t")! with { Metadata = new Dictionary<string, string> { ["strata.hops"] = "-1" } });

        var result = await graph.ResumeAsync("t", Go(), store);

        var failed = Assert.IsType<GraphExecutionResult<Job>.Failed>(result);
        Assert.Equal("checkpoint for thread 't' has an invalid 'strata.hops' value '-1'", failed.Message);
        Assert.Null(store.Load("t")!.ResumedAt);
    }

    [Fact]
    public async Task A_Per_Run_Limit_Replaces_The_Compiled_One_At_The_Outermost_Graph_Only()
    {
        var lower = Tripped(await Loop(new Calls(), stopAt: int.MaxValue).Compile(Limit(10))
            .RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { MaxNodeHops = 3 }));
        Assert.Equal(("B", 3, 3), (lower.Node, lower.Hops, lower.Limit));

        var set = Tripped(await Loop(new Calls(), stopAt: int.MaxValue).Compile()
            .RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { MaxNodeHops = 2 }));
        Assert.Equal(("A", 2, 2), (set.Node, set.Hops, set.Limit));

        // The child's four nodes don't count against the run's limit of 3, and it has none of its own.
        Completed(await GatherWorkEnd(FourSteps().Compile()).Compile()
            .RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { MaxNodeHops = 3 }));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task A_Limit_Below_One_Is_Refused(int limit)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Loop(new Calls(), 1).Compile(Limit(limit)));

        await Assert.ThrowsAnyAsync<ArgumentException>(() => Loop(new Calls(), 1).Compile()
            .RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { MaxNodeHops = limit }));
    }

    private static async Task<List<RunEvent>> Drain(RunEventChannel channel)
    {
        var events = new List<RunEvent>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            events.Add(e);
        }

        return events;
    }

    [Fact]
    public async Task The_Trip_Is_A_Run_Event_Between_The_Last_Node_And_The_Checkpoint()
    {
        var graph = Loop(new Calls(), stopAt: int.MaxValue).Compile(Limit(3));
        var channel = new RunEventChannel();

        await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        var events = (await Drain(channel)).Select(e => e switch
        {
            NodeStarted => $"NodeStarted {e.NodePath}",
            NodeCompleted => $"NodeCompleted {e.NodePath}",
            HopLimitExceeded h => $"HopLimitExceeded {e.NodePath} {h.Hops}/{h.Limit} {e.Attempt?.ToString() ?? "-"}",
            CheckpointSaved c => $"CheckpointSaved {c.Status}",
            RunCompleted c => $"RunCompleted {c.Outcome} {c.Error}",
            _ => e.GetType().Name,
        });

        Assert.Equal(
            [
                "RunStarted",
                "NodeStarted A", "NodeCompleted A",
                "NodeStarted B", "NodeCompleted B",
                "NodeStarted A", "NodeCompleted A",
                "HopLimitExceeded B 3/3 -",
                "CheckpointSaved Failed",
                "RunCompleted Failed hop limit of 3 reached: node 'B' would be hop 4",
            ],
            events);
    }

    [Fact]
    public async Task The_Run_Span_Says_Hop_Limit_And_The_Counter_Names_The_Node()
    {
        var spans = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == StrataTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        var counted = new ConcurrentBag<string?>();
        using var meter = new MeterListener();
        meter.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == StrataTelemetry.Name && instrument.Name == "strata.graph.hop_limit_exceeded")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        meter.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "strata.node.path")
                {
                    counted.Add(tag.Value as string);
                }
            }
        });
        meter.Start();

        var graph = new StateGraph<Job>()
            .AddNode("HopSpanA", Step("a"))
            .AddNode("HopSpanB", Step("b"))
            .AddEdge("HopSpanA", "HopSpanB")
            .Compile(Limit(1));
        await graph.RunAsync("hop-span", NewJob(), new InMemoryCheckpointStore());

        var run = spans.Single(a => a.OperationName == "strata.run" && (string?)a.GetTagItem("strata.thread_id") == "hop-span");
        Assert.Equal(ActivityStatusCode.Error, run.Status);
        Assert.Equal("hop_limit", run.GetTagItem("error.type"));
        var trip = Assert.Single(run.Events, e => e.Name == "hop_limit_exceeded");
        Assert.Equal("HopSpanB", trip.Tags.Single(t => t.Key == "strata.node.path").Value);
        Assert.DoesNotContain(spans, a => a.DisplayName == "HopSpanB");
        Assert.Contains("HopSpanB", counted);
    }

    [Fact]
    public async Task A_Streaming_Run_Sends_Nothing_For_The_Refused_Node()
    {
        var graph = Loop(new Calls(), stopAt: int.MaxValue).Compile(Limit(2));

        var (reader, completion) = graph.RunStreaming("t", NewJob(), new InMemoryCheckpointStore());
        var chunks = new List<StreamChunk>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            chunks.Add(chunk);
        }

        Tripped(await completion);
        Assert.Equal(["A", "B"], chunks.Where(c => c.Type == StreamChunkType.NodeStart).Select(c => c.NodeName));
    }
}
