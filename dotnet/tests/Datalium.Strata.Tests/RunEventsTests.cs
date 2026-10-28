using System.Collections.Concurrent;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Tools;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/run_events.rs</c> (<c>design/18-Run-Observability.md</c>, SG-20):
/// the exact event sequence for reference graphs, in every run mode; gapless ordered sequence numbers
/// under concurrent branches; every started node and branch completed however it ends; and an
/// observer that throws or blocks affects nothing. Both suites describe events as the same strings
/// (<see cref="Describe"/>), so the sequences can be compared line for line. Keep the two in step.
/// </summary>
public class RunEventsTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Job(int N, IReadOnlyList<string> Trail);

    private static Job NewJob() => new(1, []);

    private static Func<Job, Job> Step(string label) => job => job with { Trail = [.. job.Trail, label] };

    private static HumanSignal NoSignal() => new(IsApproved: true, ReviewerToken: "x");

    /// <summary>One line per event, the same in both engines' suites.</summary>
    private static string Describe(RunEvent e) => e switch
    {
        RunStarted s => $"RunStarted {s.Mode} {s.Node}",
        Resumed r => $"Resumed {r.From} {string.Join('/', r.WaitingAt)}",
        NodeStarted => $"NodeStarted {e.NodePath}#{e.Attempt}",
        RetryScheduled => $"RetryScheduled {e.NodePath}#{e.Attempt}",
        BranchStarted => $"BranchStarted {e.NodePath}#{e.Attempt}",
        BranchCompleted b => $"BranchCompleted {e.NodePath} {b.Result}",
        NodeCompleted n => $"NodeCompleted {e.NodePath}#{e.Attempt} {n.Result}" + (n.Fallback is { } f ? $" -> {f}" : ""),
        CheckpointSaved c => $"CheckpointSaved {c.Status} {string.Join('/', c.WaitingAt)}" + (c.ResumeStamp ? " (stamp)" : ""),
        Interrupted i => $"Interrupted {string.Join('/', i.WaitingAt)} -> {i.NextNode}",
        RunCompleted c => $"RunCompleted {c.Outcome}",
        ToolCallStarted t => $"ToolCallStarted {e.NodePath}#{e.Attempt} {t.Tool}",
        ToolCallCompleted t => $"ToolCallCompleted {e.NodePath}#{e.Attempt} {t.Tool} {t.Result}",
        _ => e.GetType().Name,
    };

    /// <summary>Every event the channel delivers, until it completes after <see cref="RunCompleted"/>.</summary>
    private static async Task<List<RunEvent>> Drain(RunEventChannel channel)
    {
        var events = new List<RunEvent>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            events.Add(e);
        }

        return events;
    }

    private static async Task<(T Result, List<string> Events)> Observe<T>(Func<RunOptions, Task<T>> run, RunOptions? options = null)
    {
        var channel = new RunEventChannel();
        var result = await run(new RunOptions { Observer = channel, Metadata = options?.Metadata ?? [], RunId = options?.RunId })
            .WaitAsync(Patience);
        return (result, [.. (await Drain(channel)).Select(Describe)]);
    }

    /// <summary>
    /// <c>Classify</c> (fallible, fails transiently once, one retry) -> <c>Approve</c> (a boundary) ->
    /// <c>Execute</c>: the design doc's example.
    /// </summary>
    private static CompiledGraph<Job> ClassifyApproveExecute()
    {
        var calls = 0;
        return new StateGraph<Job>()
            .AddFallibleNode("Classify", job => Interlocked.Increment(ref calls) == 1
                ? throw NodeException.Transient("gateway timeout")
                : job)
            .AddNode("Approve", Step("approve"))
            .AddInterruptBoundary("Approve")
            .AddNode("Execute", Step("execute"))
            .AddEdge("Classify", "Approve")
            .AddEdge("Approve", "Execute")
            .SetRecoveryPolicy("Classify", RecoveryPolicy.Retry(1))
            .Compile();
    }

    private static readonly string[] FlatRun =
    [
        "RunStarted Run Classify",
        "NodeStarted Classify#1",
        "RetryScheduled Classify#1",
        "NodeCompleted Classify#2 Committed",
        "NodeStarted Approve#1",
        "NodeCompleted Approve#1 Committed",
        "CheckpointSaved Interrupted Approve",
        "Interrupted Approve -> Execute",
        "RunCompleted Interrupted",
    ];

    private static readonly string[] FlatResume =
    [
        "RunStarted Resume Execute",
        "CheckpointSaved Interrupted Approve (stamp)",
        "Resumed Interrupted Approve",
        "NodeStarted Execute#1",
        "NodeCompleted Execute#1 Committed",
        "RunCompleted Completed",
    ];

    [Fact]
    public async Task A_Flat_Run_With_A_Retry_And_A_Boundary_Emits_The_Documented_Sequence()
    {
        var graph = ClassifyApproveExecute();
        var store = new InMemoryCheckpointStore();

        var (_, run) = await Observe(o => graph.RunAsync("t", NewJob(), store, o));
        var (_, resume) = await Observe(o => graph.ResumeAsync("t", NoSignal(), store, o));

        Assert.Equal(FlatRun, run);
        Assert.Equal(FlatResume, resume);
    }

    [Fact]
    public async Task Streaming_Runs_Emit_The_Same_Sequence()
    {
        var graph = ClassifyApproveExecute();
        var store = new InMemoryCheckpointStore();

        var (_, run) = await Observe(async o =>
        {
            var (reader, completion) = graph.RunStreaming("t", NewJob(), store, o);
            await foreach (var _ in reader.ReadAllAsync())
            {
            }

            return await completion;
        });
        var (_, resume) = await Observe(async o =>
        {
            var (reader, completion) = graph.ResumeStreaming("t", NoSignal(), store, o);
            await foreach (var _ in reader.ReadAllAsync())
            {
            }

            return await completion;
        });

        Assert.Equal(FlatRun, run);
        Assert.Equal(FlatResume, resume);
    }

    [Fact]
    public async Task A_Failing_Branch_Cancels_Its_Sibling_And_The_Group_Falls_Back()
    {
        var graph = new StateGraph<Job>()
            .AddParallelGroup(
                "Gather",
                new ParallelBranches<Job>()
                    .Branch("Slow", async (job, _, ct) =>
                    {
                        await Task.Delay(Timeout.Infinite, ct);
                        return job;
                    })
                    .Branch("Broken", (_, _, _) => throw NodeException.Fatal("source offline")),
                (state, _) => state)
            .AddNode("Recover", Step("recover"))
            .SetRecoveryPolicy("Gather", RecoveryPolicy.Default.WithFallback("Recover"))
            .Compile();

        var (_, events) = await Observe(o => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));

        Assert.Equal(
            [
                "RunStarted Run Gather",
                "NodeStarted Gather#1",
                "BranchStarted Gather/Slow#1",
                "BranchStarted Gather/Broken#1",
                "BranchCompleted Gather/Broken Failed",
                "BranchCompleted Gather/Slow Cancelled",
                "NodeCompleted Gather#1 Failed -> Recover",
                "NodeStarted Recover#1",
                "NodeCompleted Recover#1 Committed",
                "RunCompleted Completed",
            ],
            events);
    }

    [Fact]
    public async Task A_Subgraph_Boundary_And_Its_Resume_Emit_The_Documented_Sequence()
    {
        var child = new StateGraph<Job>()
            .AddNode("Draft", Step("draft"))
            .AddNode("Approve", Step("approve"))
            .AddInterruptBoundary("Approve")
            .AddNode("Send", Step("send"))
            .AddEdge("Draft", "Approve")
            .AddEdge("Approve", "Send")
            .Compile();
        var graph = new StateGraph<Job>()
            .AddNode("Prepare", Step("prepare"))
            .AddSubgraph("Work", child, job => job, (_, job) => job)
            .AddNode("Finish", Step("finish"))
            .AddEdge("Prepare", "Work")
            .AddEdge("Work", "Finish")
            .Compile();
        var store = new InMemoryCheckpointStore();

        var (_, run) = await Observe(o => graph.RunAsync("t", NewJob(), store, o));
        var (_, resume) = await Observe(o => graph.ResumeAsync("t", NoSignal(), store, o));

        Assert.Equal(
            [
                "RunStarted Run Prepare",
                "NodeStarted Prepare#1",
                "NodeCompleted Prepare#1 Committed",
                "NodeStarted Work#1",
                "NodeStarted Work/Draft#1",
                "NodeCompleted Work/Draft#1 Committed",
                "NodeStarted Work/Approve#1",
                "NodeCompleted Work/Approve#1 Committed",
                "NodeCompleted Work#1 Interrupted",
                "CheckpointSaved Interrupted Work/Approve",
                "Interrupted Work/Approve -> Work",
                "RunCompleted Interrupted",
            ],
            run);
        Assert.Equal(
            [
                "RunStarted Resume Work",
                "CheckpointSaved Interrupted Work/Approve (stamp)",
                "Resumed Interrupted Work/Approve",
                "NodeStarted Work#1",
                "NodeStarted Work/Send#1",
                "NodeCompleted Work/Send#1 Committed",
                "NodeCompleted Work#1 Committed",
                "NodeStarted Finish#1",
                "NodeCompleted Finish#1 Committed",
                "RunCompleted Completed",
            ],
            resume);
    }

    [Fact]
    public async Task Sequence_Numbers_Are_Gapless_And_In_Delivery_Order_Under_Concurrent_Branches()
    {
        var branches = new ParallelBranches<Job>();
        for (var i = 0; i < 16; i++)
        {
            branches.Branch($"B{i}", async (job, _, _) =>
            {
                await Task.Yield();
                return job;
            });
        }

        var graph = new StateGraph<Job>().AddParallelGroup("Fan", branches, (state, _) => state).Compile();
        var channel = new RunEventChannel();

        await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });
        var events = await Drain(channel);

        Assert.Equal(Enumerable.Range(1, events.Count).Select(i => (long)i), events.Select(e => e.Sequence));
        Assert.Equal(16, events.OfType<BranchStarted>().Count());
        Assert.Equal(16, events.OfType<BranchCompleted>().Count(b => b.Result == BranchResult.Succeeded));
        Assert.IsType<RunCompleted>(events[^1]);
        Assert.True(events.Zip(events.Skip(1)).All(p => p.First.Timestamp <= p.Second.Timestamp));
    }

    // --- Pairs always close ------------------------------------------------------------------------

    [Fact]
    public async Task A_Cancelled_Node_Is_Completed_As_Cancelled()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var graph = new StateGraph<Job>()
            .AddNode("Charge", async (Job job, NodeContext _, CancellationToken ct) =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, ct);
                return job;
            })
            .Compile();
        using var cts = new CancellationTokenSource();
        var channel = new RunEventChannel();

        var run = graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel }, cts.Token);
        await started.Task.WaitAsync(Patience);
        await cts.CancelAsync();
        await run.WaitAsync(Patience);
        var events = await Drain(channel);

        Assert.Equal(
            [
                "RunStarted Run Charge",
                "NodeStarted Charge#1",
                "NodeCompleted Charge#1 Cancelled",
                "CheckpointSaved Cancelled Charge",
                "RunCompleted Cancelled",
            ],
            events.Select(Describe));
        Assert.Equal(CancelReason.Requested, Assert.IsType<RunCompleted>(events[^1]).CancelReason);
    }

    [Fact]
    public async Task A_Timed_Out_Node_Is_Retried_Then_Completed_As_Failed()
    {
        var graph = new StateGraph<Job>()
            .AddFallibleNode("Charge", async (job, _, ct) =>
            {
                await Task.Delay(Timeout.Infinite, ct);
                return job;
            })
            .SetTimeout("Charge", TimeSpan.FromMilliseconds(20))
            .SetRecoveryPolicy("Charge", RecoveryPolicy.Retry(1))
            .Compile();
        var channel = new RunEventChannel();

        await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });
        var events = await Drain(channel);

        Assert.Equal(
            [
                "RunStarted Run Charge",
                "NodeStarted Charge#1",
                "RetryScheduled Charge#1",
                "NodeCompleted Charge#2 Failed",
                "CheckpointSaved Failed Charge",
                "RunCompleted Failed",
            ],
            events.Select(Describe));
        Assert.True(events.OfType<RetryScheduled>().Single().TimedOut);
        Assert.Equal("node 'Charge' timed out after 20 ms", events.OfType<NodeCompleted>().Single().Error);
    }

    [Fact]
    public async Task A_Refused_Streaming_Node_Is_Started_And_Completed_As_Failed()
    {
        var graph = new StateGraph<Job>()
            .AddStreamingNode("Draft", async (job, ctx, ct) =>
            {
                await ctx.Stream!.EmitTokenAsync("x", ct);
                return job;
            })
            .Compile();

        var (_, events) = await Observe(o => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));

        Assert.Equal(
            ["RunStarted Run Draft", "NodeStarted Draft#1", "NodeCompleted Draft#1 Failed", "RunCompleted Failed"],
            events);
    }

    [Fact]
    public async Task An_Abandoned_Sync_Branch_Is_Completed_As_Cancelled_While_It_Still_Runs()
    {
        using var release = new ManualResetEventSlim();
        var graph = new StateGraph<Job>()
            .AddParallelGroup(
                "Gather",
                new ParallelBranches<Job>()
                    .Branch("Blocking", job =>
                    {
                        release.Wait(Patience);
                        return job;
                    })
                    .Branch("Broken", (_, _, _) => throw NodeException.Fatal("source offline")),
                (state, _) => state)
            .Compile();

        var (_, events) = await Observe(o => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));
        release.Set();

        Assert.Contains("BranchCompleted Gather/Blocking Cancelled", events);
        Assert.Equal("RunCompleted Failed", events[^1]);
    }

    [Fact]
    public async Task A_Bug_In_A_Node_Still_Completes_The_Node_And_The_Run_Before_It_Propagates()
    {
        var graph = new StateGraph<Job>()
            .AddNode("Broken", (Job _, NodeContext _, CancellationToken _) =>
                ValueTask.FromException<Job>(new InvalidOperationException("bug")))
            .Compile();
        var channel = new RunEventChannel();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel }));
        var events = await Drain(channel);

        Assert.Equal(
            ["RunStarted Run Broken", "NodeStarted Broken#1", "NodeCompleted Broken#1 Failed", "RunCompleted Failed"],
            events.Select(Describe));
        Assert.Equal("bug", Assert.IsType<RunCompleted>(events[^1]).Error);
    }

    // --- The observer can't stall or fail the run --------------------------------------------------

    private sealed class ThrowsOnFirst : IRunObserver
    {
        private int _calls;

        public ConcurrentQueue<RunEvent> Seen { get; } = new();

        public TaskCompletionSource Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void OnEvent(RunEvent runEvent)
        {
            if (Interlocked.Increment(ref _calls) == 1)
            {
                throw new InvalidOperationException("observer bug");
            }

            Seen.Enqueue(runEvent);
            if (runEvent is RunCompleted)
            {
                Done.TrySetResult();
            }
        }
    }

    [Fact]
    public async Task A_Throwing_Observer_Neither_Fails_The_Run_Nor_Stops_The_Events_After()
    {
        var observer = new ThrowsOnFirst();

        var result = await ClassifyApproveExecute().RunAsync(
            "t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = observer });
        await observer.Done.Task.WaitAsync(Patience);

        Assert.IsType<GraphExecutionResult<Job>.Interrupted>(result);
        Assert.Equal(FlatRun.Skip(1), observer.Seen.Select(Describe));
    }

    private sealed class Blocked(ManualResetEventSlim gate) : IRunObserver
    {
        public ConcurrentQueue<RunEvent> Seen { get; } = new();

        public TaskCompletionSource Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void OnEvent(RunEvent runEvent)
        {
            gate.Wait(Patience);
            Seen.Enqueue(runEvent);
            if (runEvent is RunCompleted)
            {
                Done.TrySetResult();
            }
        }
    }

    [Fact]
    public async Task A_Blocked_Observer_Does_Not_Delay_The_Run()
    {
        using var gate = new ManualResetEventSlim();
        var observer = new Blocked(gate);

        var result = await ClassifyApproveExecute()
            .RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = observer })
            .WaitAsync(Patience);

        // The run is over while the observer is still parked on its first event.
        Assert.IsType<GraphExecutionResult<Job>.Interrupted>(result);
        Assert.Empty(observer.Seen);

        gate.Set();
        await observer.Done.Task.WaitAsync(Patience);
        Assert.Equal(FlatRun, observer.Seen.Select(Describe));
    }

    [Fact]
    public async Task A_Channel_Refuses_A_Second_Run()
    {
        var channel = new RunEventChannel();
        var graph = new StateGraph<Job>().AddNode("Only", Step("only")).Compile();

        await graph.RunAsync("t1", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });
        var events = await Drain(channel);
        var second = await graph.RunAsync("t2", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        // The second run is unaffected by its observer refusing it, and the channel stays complete.
        Assert.IsType<GraphExecutionResult<Job>.Completed>(second);
        Assert.All(events, e => Assert.Equal("t1", e.ThreadId));
        Assert.True(channel.Reader.Completion.IsCompleted);
    }

    // --- Run id ------------------------------------------------------------------------------------

    [Fact]
    public async Task A_Caller_Supplied_Run_Id_Reaches_Every_Event_And_Every_Context()
    {
        var seen = new ConcurrentQueue<string>();
        var child = new StateGraph<Job>()
            .AddNode("Inner", (Job job, NodeContext ctx, CancellationToken _) =>
            {
                seen.Enqueue(ctx.RunId);
                return ValueTask.FromResult(job);
            })
            .Compile();
        var graph = new StateGraph<Job>().AddSubgraph("Work", child, job => job, (_, job) => job).Compile();
        var channel = new RunEventChannel();

        await graph.RunAsync(
            "t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel, RunId = "exec-42" });
        var events = await Drain(channel);

        Assert.All(events, e => Assert.Equal("exec-42", e.RunId));
        Assert.Equal(["exec-42"], seen);
    }

    [Fact]
    public async Task The_Default_Run_Id_Is_A_Fresh_Uuid_V7_Per_Call()
    {
        var graph = new StateGraph<Job>().AddNode("Only", Step("only")).Compile();
        var first = new RunEventChannel();
        var second = new RunEventChannel();

        await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = first });
        await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = second });
        var a = (await Drain(first))[0].RunId;
        var b = (await Drain(second))[0].RunId;

        Assert.NotEqual(a, b);
        Assert.Equal(7, Guid.Parse(a).Version);
    }

    [Fact]
    public async Task An_Empty_Run_Id_Is_Refused()
    {
        var graph = new StateGraph<Job>().AddNode("Only", Step("only")).Compile();

        await Assert.ThrowsAsync<ArgumentException>(
            () => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { RunId = "" }));
    }

    // --- Tool calls (design/20-Tool-Dispatch-And-Registry.md) ------------------

    private static ToolRegistry EventRegistry() =>
        new ToolRegistry().Register(new EventTools()).Subset("events.lookup", "events.hang", "events.foreign");

    [Fact]
    public async Task Tool_Calls_Are_Reported_Between_Their_Nodes_Events()
    {
        var tools = EventRegistry();
        var graph = new StateGraph<Job>()
            .AddNode("Investigate", async (job, ctx, ct) =>
            {
                var call = ToolCallContext.From(ctx) with { CallerId = "investigator" };
                await tools.DispatchToolAsync("events.lookup", "{}", call with { CallId = "c1" }, ct);
                await Assert.ThrowsAsync<ToolNotPermittedException>(() => tools.DispatchToolAsync("events.hidden", "{}", call, ct));
                return job;
            })
            .Compile();
        var channel = new RunEventChannel();

        await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel }).WaitAsync(Patience);
        var events = await Drain(channel);

        Assert.Equal(
            [
                "RunStarted Run Investigate",
                "NodeStarted Investigate#1",
                "ToolCallStarted Investigate#1 events.lookup",
                "ToolCallCompleted Investigate#1 events.lookup Succeeded",
                "ToolCallStarted Investigate#1 events.hidden",
                "ToolCallCompleted Investigate#1 events.hidden Refused",
                "NodeCompleted Investigate#1 Committed",
                "RunCompleted Completed",
            ],
            events.Select(Describe));
        var started = Assert.IsType<ToolCallStarted>(events[2]);
        Assert.Equal(("c1", "investigator"), (started.CallId, started.CallerId));
        Assert.Equal("Tool 'events.hidden' is not permitted here.", Assert.IsType<ToolCallCompleted>(events[5]).Error);
    }

    /// <summary>
    /// The stand-in for <c>run_events.rs</c>'s <c>a_tool_call_the_engine_gave_up_on_is_completed_as_cancelled</c>:
    /// Rust drops the call's future when the engine gives up on its node, which records the outcome at
    /// once, but .NET abandons the node and the tool notices its token on another thread, possibly after
    /// <c>RunCompleted</c>. Here the node cancels the call itself, so the order is fixed.
    /// </summary>
    [Fact]
    public async Task A_Cancelled_Tool_Call_Is_Completed_As_Cancelled()
    {
        var tools = EventRegistry();
        var graph = new StateGraph<Job>()
            .AddNode("Investigate", async (job, ctx, ct) =>
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(TimeSpan.FromMilliseconds(20));
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    () => tools.DispatchToolAsync("events.hang", "{}", ToolCallContext.From(ctx), cts.Token));
                return job;
            })
            .Compile();

        var (_, events) = await Observe(o => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));

        Assert.Equal(
            [
                "RunStarted Run Investigate",
                "NodeStarted Investigate#1",
                "ToolCallStarted Investigate#1 events.hang",
                "ToolCallCompleted Investigate#1 events.hang Cancelled",
                "NodeCompleted Investigate#1 Committed",
                "RunCompleted Completed",
            ],
            events);
    }

    [Fact]
    public async Task A_Foreign_OperationCanceledException_From_A_Tool_Is_A_Failure()
    {
        var tools = EventRegistry();
        var graph = new StateGraph<Job>()
            .AddNode("Investigate", async (job, ctx, ct) =>
            {
                await Assert.ThrowsAsync<OperationCanceledException>(
                    () => tools.DispatchToolAsync("events.foreign", "{}", ToolCallContext.From(ctx), ct));
                return job;
            })
            .Compile();

        var (_, events) = await Observe(o => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));

        Assert.Contains("ToolCallCompleted Investigate#1 events.foreign Failed", events);
    }

    [Fact]
    public async Task A_Tool_Call_With_A_Hand_Built_Context_Records_No_Events()
    {
        var tools = EventRegistry();
        var graph = new StateGraph<Job>()
            .AddNode("Investigate", async (job, _, ct) =>
            {
                await tools.DispatchToolAsync("events.lookup", "{}", new ToolCallContext { NodePath = "Investigate" }, ct);
                return job;
            })
            .Compile();

        var (_, events) = await Observe(o => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));

        Assert.DoesNotContain(events, e => e.StartsWith("ToolCall", StringComparison.Ordinal));
    }
}

/// <summary>Tools for <see cref="RunEventsTests"/>.</summary>
public partial class EventTools
{
    [StrataTool(Name = "events.lookup", Description = "Looks.")]
    public static string Lookup() => "found";

    [StrataTool(Name = "events.hang", Description = "Waits for its token.")]
    public static async Task<string> HangAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.Infinite, cancellationToken);
        return "never";
    }

    [StrataTool(Name = "events.hidden", Description = "Left out.")]
    public static string Hidden() => string.Empty;

    [StrataTool(Name = "events.foreign", Description = "Throws an OperationCanceledException nobody asked for.")]
    public static string Foreign() => throw new OperationCanceledException("the HTTP client timed out");
}
