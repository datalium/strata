using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Schema;
using Datalium.Strata.Validation;
using Microsoft.Extensions.Time.Testing;
using Checkpoint = Datalium.Strata.Interruption.Checkpoint;
using CheckpointStatus = Datalium.Strata.Interruption.CheckpointStatus;
using GraphDefinition = Datalium.Strata.Storage.Schema.GraphDefinition;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/event_waits.rs</c> (<c>design/29-Event-Waits-And-Timers.md</c>, SG-16):
/// event waits, durable timers, approval timeouts, the wake driver, through the public API only. Keep the
/// two in step. The clock is a <see cref="FakeTimeProvider"/> starting at the fixtures' time, so a live
/// freeze's <c>wake_at</c> is exactly the fixture's.
/// </summary>
public class EventWaitTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);
    private static readonly DateTimeOffset Noon = DateTimeOffset.Parse("2026-10-06T12:00:00Z");

    public sealed record Deploy(
        [property: JsonPropertyName("release")] string Release,
        [property: JsonPropertyName("started")] bool Started,
        [property: JsonPropertyName("finished")] bool Finished,
        [property: JsonPropertyName("outcome")] string? Outcome,
        [property: JsonPropertyName("rolled_back")] bool RolledBack,
        [property: JsonPropertyName("verified")] bool Verified);

    public sealed record Approval(
        [property: JsonPropertyName("plan")] string Plan,
        [property: JsonPropertyName("presented")] bool Presented,
        [property: JsonPropertyName("executed")] bool Executed,
        [property: JsonPropertyName("escalated")] bool Escalated,
        [property: JsonPropertyName("aborted")] bool Aborted);

    public sealed record Health(
        [property: JsonPropertyName("checks")] int Checks,
        [property: JsonPropertyName("healthy")] bool Healthy);

    public sealed record Remediation(
        [property: JsonPropertyName("step")] string Step,
        [property: JsonPropertyName("verified")] bool Verified,
        [property: JsonPropertyName("checks")] int Checks);

    public sealed record Scheduled(
        [property: JsonPropertyName("due_at")] DateTimeOffset DueAt,
        [property: JsonPropertyName("woke")] bool Woke);

    /// <summary>
    /// An <see cref="InMemoryCheckpointStore"/> whose <c>LoadLatestAsync</c> holds each caller until a second
    /// one has loaded too, once armed: two wake drivers that both decided a thread is due, racing to wake it.
    /// </summary>
    private sealed class RacingStore : ICheckpointStore, ICheckpointQuery
    {
        private readonly TaskCompletionSource _both = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _arrived;

        public InMemoryCheckpointStore Inner { get; } = new();

        public bool Armed { get; set; }

        public void Save(Checkpoint checkpoint) => Inner.Save(checkpoint);

        public Checkpoint? Load(string threadId) => Inner.Load(threadId);

        public async ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default)
        {
            var latest = await Inner.LoadLatestAsync(threadId, cancellationToken);
            if (Armed)
            {
                if (Interlocked.Increment(ref _arrived) == 2)
                {
                    _both.SetResult();
                }

                await _both.Task.WaitAsync(Patience, cancellationToken);
            }

            return latest;
        }

        public ValueTask<bool> SaveIfLatestAsync(Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
            Inner.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken);

        public ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default) =>
            Inner.ListThreadsAsync(query, cancellationToken);

        public ValueTask<CheckpointLogPage> GetLogAsync(
            string threadId, long? afterSequence = null, int limit = ThreadQuery.DefaultLimit, CancellationToken cancellationToken = default) =>
            Inner.GetLogAsync(threadId, afterSequence, limit, cancellationToken);
    }

    /// <summary>Every event of every run, in arrival order.</summary>
    private sealed class Collector : IRunObserver
    {
        private readonly ConcurrentQueue<RunEvent> _events = new();

        public void OnEvent(RunEvent runEvent) => _events.Enqueue(runEvent);

        /// <summary>The events once <paramref name="runs"/> runs have completed (delivery may trail the call).</summary>
        public async Task<List<RunEvent>> After(int runs)
        {
            var deadline = DateTime.UtcNow + Patience;
            while (_events.Count(e => e is RunCompleted) < runs)
            {
                Assert.True(DateTime.UtcNow < deadline, "the runs' events");
                await Task.Delay(5);
            }

            return [.. _events];
        }
    }

    // --- Graphs ---

    private static Deploy NewDeploy() => new("v2.4.1", false, false, null, false, false);

    /// <summary>
    /// <c>waiting-event.json</c>'s graph: <c>Start</c> -> <c>Deploying</c> (waits for <c>deployment.finished</c>,
    /// at most an hour, then <c>Rollback</c>) -> <c>Verify</c>.
    /// </summary>
    private static CompiledGraph<Deploy> DeployGraph(bool route)
    {
        var graph = new StateGraph<Deploy>()
            .AddNode("Start", s => s with { Started = true })
            .AddWaitForEvent("Deploying", "deployment.finished", TimeSpan.FromHours(1), route ? "Rollback" : null)
            .AddNode("Verify", s => s with { Verified = true })
            .AddEdge("Start", "Deploying")
            .AddEdge("Deploying", "Verify");
        if (route)
        {
            graph.AddNode("Rollback", s => s with { RolledBack = true });
        }

        return graph.Compile();
    }

    private static Approval NewApproval() => new("restart api", false, false, false, false);

    /// <summary>
    /// <c>waiting-approval.json</c>'s graph: <c>Approve</c> (a boundary that rejects to <c>Abort</c> and times
    /// out after 30 minutes to <c>Escalate</c>) -> <c>Execute</c>.
    /// </summary>
    private static CompiledGraph<Approval> ApprovalGraph(bool route)
    {
        var graph = new StateGraph<Approval>()
            .AddNode("Approve", s => s with { Presented = true })
            .AddInterruptBoundary("Approve", onReject: "Abort", timeout: TimeSpan.FromMinutes(30), onTimeout: route ? "Escalate" : null)
            .AddNode("Execute", s => s with { Executed = true })
            .AddNode("Abort", s => s with { Aborted = true })
            .AddEdge("Approve", "Execute");
        if (route)
        {
            graph.AddNode("Escalate", s => s with { Escalated = true });
        }

        return graph.Compile();
    }

    /// <summary>The verification window: <c>Check</c> -> <c>Window</c> (a five-minute timer) -> <c>Check</c>, until two checks find it healthy, then <c>Done</c>.</summary>
    private static CompiledGraph<Health> WindowGraph(int healthyAfter = 2, TimeSpan? delay = null) =>
        new StateGraph<Health>()
            .AddNode("Check", s => new Health(s.Checks + 1, s.Checks + 1 >= healthyAfter))
            .AddTimer("Window", delay ?? TimeSpan.FromMinutes(5))
            .AddNode("Done", s => s)
            .AddConditionalEdge("Check", "Window", s => !s.Healthy)
            .AddEdge("Check", "Done")
            .AddEdge("Window", "Check")
            .Compile();

    private static Remediation NewRemediation() => new("start", false, 0);

    /// <summary><c>nested-timer.json</c>'s graph: <c>Remediate</c> -> <c>Verify</c> (a subgraph) -> <c>Close</c>.</summary>
    private static CompiledGraph<Remediation> RemediationGraph<TChild>(
        CompiledGraph<TChild> child, Func<Remediation, TChild> input, Func<Remediation, TChild, Remediation> output) =>
        new StateGraph<Remediation>()
            .AddNode("Remediate", s => s with { Step = "remediated" })
            .AddSubgraph("Verify", child, input, output)
            .AddNode("Close", s => s with { Step = "closed" })
            .AddEdge("Remediate", "Verify")
            .AddEdge("Verify", "Close")
            .Compile();

    private static CompiledGraph<Remediation> VerifyGraph() =>
        RemediationGraph(WindowGraph(), _ => new Health(0, false), (entry, health) => entry with { Verified = health.Healthy, Checks = health.Checks });

    // --- Helpers ---

    private static JsonNode ReadFixture(string relative, [CallerFilePath] string sourceFilePath = "") =>
        JsonNode.Parse(File.ReadAllText(Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(sourceFilePath)!, "..", "..", "..", "schema", "fixtures", relative))))!;

    private static Checkpoint WithoutCreatedAt(Checkpoint checkpoint) =>
        checkpoint with
        {
            CreatedAt = string.Empty,
            Nested = checkpoint.Nested is null ? null : WithoutCreatedAt(checkpoint.Nested),
        };

    /// <summary>The fixture, <c>created_at</c> aside: the fake clock makes <c>wake_at</c> exact.</summary>
    private static void AssertMatchesFixture(string name, Checkpoint actual)
    {
        var expected = WithoutCreatedAt(Checkpoint.FromWireJson(ReadFixture(Path.Combine("checkpoint-lifecycle", name)))).ToWireJson();
        var got = WithoutCreatedAt(actual).ToWireJson();
        Assert.True(JsonNode.DeepEquals(expected, got), got.ToJsonString());
    }

    private static RunOptions At(FakeTimeProvider clock, IRunObserver? observer = null, Dictionary<string, string>? metadata = null) =>
        new() { TimeProvider = clock, Observer = observer, Metadata = metadata ?? [] };

    private static WakeOptions Wake(FakeTimeProvider clock, IRunObserver? observer = null) =>
        new() { Run = At(clock, observer) };

    private static async Task<CheckpointWait?> Freeze<T>(
        CompiledGraph<T> graph, string threadId, T state, ICheckpointStore store, FakeTimeProvider clock)
    {
        var interrupted = Assert.IsType<GraphExecutionResult<T>.Interrupted>(
            await graph.RunAsync(threadId, state, store, At(clock)));
        return interrupted.Wait;
    }

    private static string Refused<T>(GraphExecutionResult<T> result) =>
        Assert.IsAssignableFrom<GraphExecutionResult<T>.Failed>(result).Message;

    private static T Completed<T>(GraphExecutionResult<T> result)
    {
        if (result is GraphExecutionResult<T>.Failed failed)
        {
            Assert.Fail($"expected the run to complete, got: {failed.Message}");
        }

        return Assert.IsType<GraphExecutionResult<T>.Completed>(result).State;
    }

    private static async Task<List<Checkpoint>> LogOf(InMemoryCheckpointStore store, string threadId) =>
        [.. (await store.GetLogAsync(threadId)).Entries.Select(e => e.Checkpoint)];

    private static IEnumerable<(string, WakeReason)> Woken<T>(WakeReport<T> report) =>
        report.Woken.Select(w => (w.ThreadId, w.Reason));

    // --- Timers ---

    [Fact]
    public async Task A_Timer_Freezes_With_Its_Wake_Time_And_Wakes_Only_When_Due()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = WindowGraph();
        var wait = await Freeze(graph, "t", new Health(0, false), store, clock);

        Assert.Equal(new CheckpointWait(WaitKind.Timer, null, "2026-10-06T12:05:00Z"), wait);
        var latest = store.Load("t")!;
        Assert.Equal(wait, latest.Wait);
        Assert.Equal(CheckpointSchema.WaitVersion, latest.SchemaVersion);
        Assert.Equal(("Window", "Check"), (latest.SuspendedNode, latest.NextNode));

        // Not due yet: nothing is woken or written.
        clock.Advance(TimeSpan.FromMinutes(4));
        var report = await graph.WakeDueAsync(store, Wake(clock));
        Assert.Empty(report.Woken);
        Assert.Empty(report.Skipped);
        Assert.Single(await LogOf(store, "t"));

        // Due: the timer continues at the edge out of it.
        clock.Advance(TimeSpan.FromMinutes(1));
        report = await graph.WakeDueAsync(store, Wake(clock));
        Assert.Equal([("t", WakeReason.Timer)], Woken(report));
        Assert.Equal(new Health(2, true), Completed(report.Woken[0].Result));

        // The resume stamp is the only new checkpoint, and it records no decision.
        var log = await LogOf(store, "t");
        Assert.Equal(2, log.Count);
        Assert.NotNull(log[1].ResumedAt);
        Assert.Null(log[1].Decision);

        // Woken, it isn't due any more.
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Empty((await graph.WakeDueAsync(store, Wake(clock))).Woken);
    }

    [Fact]
    public async Task A_Stop_Token_Wakes_No_More_Threads_And_Lets_The_One_In_Flight_Finish()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        using var stop = new CancellationTokenSource();

        // The host is asked to stop while a woken thread's run is in flight (design/35-Hosting-Integration.md).
        var graph = new StateGraph<Health>()
            .AddTimer("Window", TimeSpan.FromMinutes(5))
            .AddNode("Check", async (Health health, NodeContext _, CancellationToken cancellationToken) =>
            {
                await stop.CancelAsync();
                await Task.Yield();
                cancellationToken.ThrowIfCancellationRequested();
                return health with { Checks = health.Checks + 1, Healthy = true };
            })
            .AddEdge("Window", "Check")
            .Compile();
        await Freeze(graph, "a", new Health(0, false), store, clock);
        await Freeze(graph, "b", new Health(0, false), store, clock);
        clock.Advance(TimeSpan.FromMinutes(6));

        var report = await graph.WakeDueAsync(store, Wake(clock) with { Stop = stop.Token });

        // The thread in flight finished, not cancelled, and the other was never started.
        var woken = Assert.Single(report.Woken);
        Assert.True(Completed(woken.Result).Healthy);
        Assert.Empty(report.Skipped);
        var other = woken.ThreadId == "a" ? "b" : "a";
        Assert.Single(await LogOf(store, other));

        // A token that has fired starts nothing.
        var none = await graph.WakeDueAsync(store, Wake(clock) with { Stop = stop.Token });
        Assert.Empty(none.Woken);
        Assert.Empty(none.Skipped);
        Assert.Single(await LogOf(store, other));

        // The thread is still due for the next driver.
        Assert.Equal(other, Assert.Single((await graph.WakeDueAsync(store, Wake(clock))).Woken).ThreadId);
    }

    [Fact]
    public async Task A_Timer_In_A_Loop_Freezes_Again_With_A_New_Wake_Time()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = WindowGraph(healthyAfter: 3, delay: TimeSpan.FromMinutes(1));
        await Freeze(graph, "loop", new Health(0, false), store, clock);

        clock.Advance(TimeSpan.FromMinutes(2));
        var report = await graph.WakeDueAsync(store, Wake(clock));

        Assert.Equal([("loop", WakeReason.Timer)], Woken(report));
        var again = Assert.IsType<GraphExecutionResult<Health>.Interrupted>(report.Woken[0].Result);
        Assert.Equal(2, again.FrozenState.Checks);
        Assert.Equal("2026-10-06T12:03:00Z", again.Wait!.WakeAt);

        // It froze again after the call fixed `now`, so the next call wakes it.
        clock.Advance(TimeSpan.FromMinutes(1));
        report = await graph.WakeDueAsync(store, Wake(clock));
        Assert.Equal(3, Completed(report.Woken[0].Result).Checks);
    }

    [Fact]
    public async Task A_Timer_From_The_State_Wakes_At_That_Time_Rounded_Up()
    {
        var graph = new StateGraph<Scheduled>()
            .AddTimer("Until", s => s.DueAt)
            .AddNode("Woke", s => s with { Woke = true })
            .AddEdge("Until", "Woke")
            .Compile();
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var due = DateTimeOffset.Parse("2030-01-02T03:04:05Z").AddMilliseconds(250);

        var wait = await Freeze(graph, "s", new Scheduled(due, false), store, clock);

        Assert.Equal("2030-01-02T03:04:06Z", wait!.WakeAt);
        // Not at 03:04:05.9, which would be early; at 03:04:06 it is.
        clock.SetUtcNow(DateTimeOffset.Parse("2030-01-02T03:04:05Z").AddMilliseconds(900));
        Assert.Empty((await graph.WakeDueAsync(store, Wake(clock))).Woken);
        clock.SetUtcNow(DateTimeOffset.Parse("2030-01-02T03:04:06Z"));
        Assert.True(Completed((await graph.WakeDueAsync(store, Wake(clock))).Woken[0].Result).Woke);
    }

    // --- Event waits ---

    [Fact]
    public async Task An_Event_Wait_Freezes_To_The_Shared_Fixture()
    {
        var store = new InMemoryCheckpointStore();
        var wait = await Freeze(DeployGraph(route: true), "thread_deploy_demo", NewDeploy(), store, new FakeTimeProvider(Noon));

        AssertMatchesFixture("waiting-event.json", store.Load("thread_deploy_demo")!);
        Assert.Equal(store.Load("thread_deploy_demo")!.Wait, wait);
    }

    [Fact]
    public async Task A_Signal_Resumes_An_Event_Wait_With_Its_Payload()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = DeployGraph(route: true);
        await Freeze(graph, "d", NewDeploy(), store, clock);

        var state = Completed(await graph.SignalAsync(
            "d",
            new EventSignal("deployment.finished").WithOverride("finished", true).WithOverride("outcome", "succeeded"),
            store));

        Assert.True(state.Finished && state.Verified && !state.RolledBack);
        Assert.Equal("succeeded", state.Outcome);
        // An event is not a human's answer: no decision.
        var log = await LogOf(store, "d");
        Assert.Equal(2, log.Count);
        Assert.Null(log[1].Decision);
        // Signalled, it isn't due when its timeout passes.
        clock.Advance(TimeSpan.FromHours(2));
        Assert.Empty((await graph.WakeDueAsync(store, Wake(clock))).Woken);
    }

    [Fact]
    public async Task The_Wrong_Answer_To_A_Wait_Is_Refused_And_Writes_Nothing()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var deploys = DeployGraph(route: true);
        var approvals = ApprovalGraph(route: true);
        var windows = WindowGraph();
        await Freeze(deploys, "event", NewDeploy(), store, clock);
        await Freeze(approvals, "human", NewApproval(), store, clock);
        await Freeze(windows, "timer", new Health(0, false), store, clock);

        Assert.Equal(
            "thread 'event' is not waiting for event 'deployment.started': it is waiting for event 'deployment.finished' at [Deploying]",
            Refused(await deploys.SignalAsync("event", new EventSignal("deployment.started"), store)));
        Assert.Contains(
            "it is waiting for a human at [Approve]",
            Refused(await approvals.SignalAsync("human", new EventSignal("deployment.finished"), store)));
        Assert.Contains(
            "it is waiting for a timer at [Window]",
            Refused(await windows.SignalAsync("timer", new EventSignal("deployment.finished"), store)));
        Assert.Contains(
            "thread 'event' is waiting for event 'deployment.finished' at [Deploying]: only a signal of that event answers it",
            Refused(await deploys.ResumeAsync("event", HumanSignal.Approve("alice"), store)));
        Assert.Contains(
            "thread 'timer' is waiting for a timer at [Window] until 2026-10-06T12:05:00Z: it wakes when due",
            Refused(await windows.ResumeAsync("timer", HumanSignal.Approve("alice"), store)));

        // Nothing was written: each thread still waits as it froze.
        foreach (var thread in new[] { "event", "human", "timer" })
        {
            Assert.Single(await LogOf(store, thread));
        }

        // A failed thread isn't waiting for anything.
        var failing = new StateGraph<Deploy>()
            .AddFallibleNode("Fail", (Deploy _) => throw NodeException.Fatal("boom"))
            .Compile();
        Assert.IsType<GraphExecutionResult<Deploy>.NodeFailed>(await failing.RunAsync("failed", NewDeploy(), store));
        Assert.Contains(
            "thread 'failed' is not waiting for event 'deployment.finished': its latest checkpoint is failed",
            Refused(await failing.SignalAsync("failed", new EventSignal("deployment.finished"), store)));
    }

    // --- Timeouts ---

    [Fact]
    public async Task An_Event_Wait_That_Times_Out_Takes_Its_Timeout_Route()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = DeployGraph(route: true);
        await Freeze(graph, "d", NewDeploy(), store, clock);
        var collector = new Collector();

        clock.Advance(TimeSpan.FromMinutes(61));
        var report = await graph.WakeDueAsync(store, Wake(clock, collector));

        Assert.Equal([("d", WakeReason.Timeout)], Woken(report));
        var state = Completed(report.Woken[0].Result);
        Assert.True(state.RolledBack && !state.Verified);
        var resumed = (await collector.After(1)).OfType<Resumed>().Single();
        Assert.Equal((WakeReason.Timeout, "Rollback", (string?)null, (string?)null), (resumed.Wake, resumed.TimedOutTo, resumed.RejectedTo, resumed.EventType));
    }

    [Fact]
    public async Task A_Wait_That_Times_Out_With_No_Route_Ends_The_Thread()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = DeployGraph(route: false);
        await Freeze(graph, "d", NewDeploy(), store, clock);

        clock.Advance(TimeSpan.FromMinutes(61));
        var report = await graph.WakeDueAsync(store, Wake(clock));

        Assert.Equal([("d", WakeReason.Timeout)], Woken(report));
        var ended = Assert.IsType<GraphExecutionResult<Deploy>.Cancelled>(report.Woken[0].Result);
        Assert.Equal((CancelReason.Ended, "Deploying"), (ended.Reason, ended.Node));
        Assert.True(ended.State.Started && !ended.State.Verified);
        var checkpoint = store.Load("d")!;
        Assert.Equal(CheckpointStatus.Ended, checkpoint.Status);
        Assert.Null(checkpoint.Wait);
        Assert.Equal(ThreadEnd.TimedOut, checkpoint.Metadata![ThreadEnd.ReasonKey]);
        // The query port finds threads that timed out.
        var timedOut = await store.ListThreadsAsync(new ThreadQuery
        {
            Metadata = new Dictionary<string, string> { [ThreadEnd.ReasonKey] = ThreadEnd.TimedOut },
        });
        Assert.Single(timedOut.Threads);
        // Ended, it is neither due nor resumable.
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Empty((await graph.WakeDueAsync(store, Wake(clock))).Woken);
        Assert.Contains("was ended", Refused(await graph.SignalAsync("d", new EventSignal("deployment.finished"), store)));
    }

    [Fact]
    public async Task An_Approval_With_A_Timeout_Freezes_To_The_Shared_Fixture_And_Escalates()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = ApprovalGraph(route: true);
        await Freeze(graph, "thread_approval_demo", NewApproval(), store, clock);
        AssertMatchesFixture("waiting-approval.json", store.Load("thread_approval_demo")!);

        // Not due after 29 minutes; timed out at 30.
        clock.Advance(TimeSpan.FromMinutes(29));
        Assert.Empty((await graph.WakeDueAsync(store, Wake(clock))).Woken);
        clock.Advance(TimeSpan.FromMinutes(1));
        var report = await graph.WakeDueAsync(store, Wake(clock));

        Assert.Equal([("thread_approval_demo", WakeReason.Timeout)], Woken(report));
        var state = Completed(report.Woken[0].Result);
        Assert.True(state.Escalated && !state.Executed && !state.Aborted);
        // A timeout isn't a human's answer: no decision is recorded.
        Assert.All(await LogOf(store, "thread_approval_demo"), c => Assert.Null(c.Decision));
    }

    [Fact]
    public async Task An_Approval_With_No_Timeout_Route_Is_Ended_When_It_Times_Out()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = ApprovalGraph(route: false);
        await Freeze(graph, "a", NewApproval(), store, clock);

        clock.Advance(TimeSpan.FromMinutes(31));
        var report = await graph.WakeDueAsync(store, Wake(clock));

        Assert.Equal(CancelReason.Ended, Assert.IsType<GraphExecutionResult<Approval>.Cancelled>(report.Woken[0].Result).Reason);
        Assert.Equal(CheckpointStatus.Ended, store.Load("a")!.Status);
    }

    [Fact]
    public async Task An_Approval_Answered_Before_Its_Timeout_Is_Not_Due()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = ApprovalGraph(route: true);
        await Freeze(graph, "a", NewApproval(), store, clock);

        Assert.True(Completed(await graph.ResumeAsync("a", HumanSignal.Approve("alice"), store)).Executed);

        clock.Advance(TimeSpan.FromHours(1));
        Assert.Empty((await graph.WakeDueAsync(store, Wake(clock))).Woken);
    }

    // --- Nesting ---

    [Fact]
    public async Task A_Timer_Inside_A_Subgraph_Freezes_To_The_Shared_Fixture_And_Wakes_Into_The_Child()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = VerifyGraph();
        var wait = await Freeze(graph, "thread_verify_demo", NewRemediation(), store, clock);

        var latest = store.Load("thread_verify_demo")!;
        AssertMatchesFixture("nested-timer.json", latest);
        Assert.Equal(latest.InnermostWait(), wait);
        Assert.Null(latest.Wait);

        // The innermost wake time decides.
        clock.Advance(TimeSpan.FromMinutes(5));
        var listed = await store.ListThreadsAsync(new ThreadQuery { DueBy = clock.GetUtcNow() });
        Assert.Equal(latest.InnermostWait(), Assert.Single(listed.Threads).Wait);

        var report = await graph.WakeDueAsync(store, Wake(clock));
        Assert.Equal([("thread_verify_demo", WakeReason.Timer)], Woken(report));
        Assert.Equal(new Remediation("closed", true, 2), Completed(report.Woken[0].Result));
    }

    [Fact]
    public async Task An_Event_Wait_Inside_A_Subgraph_Is_Signalled()
    {
        var graph = RemediationGraph(DeployGraph(route: true), _ => NewDeploy(), (entry, deployed) => entry with { Verified = deployed.Verified });
        var store = new InMemoryCheckpointStore();
        var wait = await Freeze(graph, "n", NewRemediation(), store, new FakeTimeProvider(Noon));
        Assert.Equal("deployment.finished", wait!.EventType);

        var state = Completed(await graph.SignalAsync("n", new EventSignal("deployment.finished").WithOverride("finished", true), store));

        Assert.True(state.Verified);
    }

    [Fact]
    public async Task A_Nested_Wait_That_Times_Out_With_No_Route_Ends_Every_Level()
    {
        var graph = RemediationGraph(DeployGraph(route: false), _ => NewDeploy(), (entry, deployed) => entry with { Verified = deployed.Verified });
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        await Freeze(graph, "n", NewRemediation(), store, clock);

        clock.Advance(TimeSpan.FromMinutes(61));
        var report = await graph.WakeDueAsync(store, Wake(clock));

        var ended = Assert.IsType<GraphExecutionResult<Remediation>.Cancelled>(report.Woken[0].Result);
        Assert.Equal(CancelReason.Ended, ended.Reason);
        Assert.Equal(["Verify", "Deploying"], ended.StoppedAt);
        var checkpoint = store.Load("n")!;
        Assert.Equal(CheckpointStatus.Ended, checkpoint.Status);
        Assert.Equal(CheckpointStatus.Ended, checkpoint.Nested!.Status);
        Assert.Null(checkpoint.Nested.Wait);
    }

    // --- Drivers ---

    [Fact]
    public async Task Two_Drivers_Wake_A_Thread_Once()
    {
        var store = new RacingStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = WindowGraph();
        await Freeze(graph, "t", new Health(0, false), store, clock);
        clock.Advance(TimeSpan.FromMinutes(5));
        store.Armed = true;

        // Both list the thread and load it before either wakes it.
        var reports = await Task.WhenAll(
            graph.WakeDueAsync(store, Wake(clock)),
            graph.WakeDueAsync(store, Wake(clock))).WaitAsync(Patience);

        Assert.Equal(1, reports.Sum(r => r.Woken.Count));
        var skipped = Assert.Single(reports.SelectMany(r => r.Skipped));
        Assert.Equal("another driver woke or ended it first", skipped.Reason);
        Assert.Single(await LogOf(store.Inner, "t"), c => c.ResumedAt is not null);
    }

    /// <summary>
    /// An <see cref="InMemoryCheckpointStore"/> whose listing goes stale at once: another driver resumes every
    /// thread it lists, between the listing and the load.
    /// </summary>
    private sealed class StaleListing : ICheckpointStore, ICheckpointQuery
    {
        public InMemoryCheckpointStore Inner { get; } = new();

        public void Save(Checkpoint checkpoint) => Inner.Save(checkpoint);

        public Checkpoint? Load(string threadId) => Inner.Load(threadId);

        public ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
            Inner.LoadLatestAsync(threadId, cancellationToken);

        public ValueTask<bool> SaveIfLatestAsync(Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
            Inner.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken);

        public async ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default)
        {
            var page = await Inner.ListThreadsAsync(query, cancellationToken);
            foreach (var thread in page.Threads)
            {
                Inner.Save(Inner.Load(thread.ThreadId)! with { ResumedAt = "2026-10-06T12:00:00Z" });
            }

            return page;
        }

        public ValueTask<CheckpointLogPage> GetLogAsync(
            string threadId, long? afterSequence = null, int limit = ThreadQuery.DefaultLimit, CancellationToken cancellationToken = default) =>
            Inner.GetLogAsync(threadId, afterSequence, limit, cancellationToken);
    }

    [Fact]
    public async Task A_Thread_Woken_Between_The_Listing_And_The_Load_Is_Skipped()
    {
        var store = new StaleListing();
        var clock = new FakeTimeProvider(Noon);
        var graph = WindowGraph();
        await Freeze(graph, "t", new Health(0, false), store, clock);

        clock.Advance(TimeSpan.FromMinutes(6));
        var report = await graph.WakeDueAsync(store, Wake(clock));

        Assert.Empty(report.Woken);
        Assert.Equal("it is no longer due", Assert.Single(report.Skipped).Reason);
        // Only the other driver's stamp: this one wrote nothing.
        Assert.Equal(2, (await LogOf(store.Inner, "t")).Count);
    }

    [Fact]
    public async Task A_Driver_Wakes_Only_Its_Own_Graphs_Threads()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var deploys = DeployGraph(route: true);
        var windows = WindowGraph();
        Assert.IsType<GraphExecutionResult<Deploy>.Interrupted>(await deploys.RunAsync(
            "deploy-1", NewDeploy(), store, At(clock, metadata: new Dictionary<string, string> { ["graph"] = "deploy" })));
        clock.Advance(TimeSpan.FromHours(2));

        // The metadata filter leaves the other graph's thread alone entirely.
        var filtered = await windows.WakeDueAsync(store, Wake(clock) with
        {
            Metadata = new Dictionary<string, string> { ["graph"] = "window" },
        });
        Assert.Empty(filtered.Woken);
        Assert.Empty(filtered.Skipped);

        // Without it, the wait isn't one of this graph's: skipped, nothing written.
        var unfiltered = await windows.WakeDueAsync(store, Wake(clock));
        Assert.Empty(unfiltered.Woken);
        Assert.Equal(
            "thread 'deploy-1' waits at [Deploying], which has no timeout in this graph",
            Assert.Single(unfiltered.Skipped).Reason);
        Assert.Single(await LogOf(store, "deploy-1"));

        // Its own driver, with its own filter, times it out.
        var own = await deploys.WakeDueAsync(store, Wake(clock) with
        {
            Metadata = new Dictionary<string, string> { ["graph"] = "deploy" },
            Limit = 10,
        });
        Assert.Equal([("deploy-1", WakeReason.Timeout)], Woken(own));
    }

    // --- Run events ---

    [Fact]
    public async Task Run_Events_Say_What_A_Run_Waits_For_And_What_Woke_It()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = DeployGraph(route: true);
        var collector = new Collector();
        Assert.IsType<GraphExecutionResult<Deploy>.Interrupted>(await graph.RunAsync("d", NewDeploy(), store, At(clock, collector)));
        await graph.SignalAsync("d", new EventSignal("deployment.finished"), store, At(clock, collector));

        var events = await collector.After(2);
        var interrupted = events.OfType<Interrupted>().Single();
        Assert.Equal(new CheckpointWait(WaitKind.Event, "deployment.finished", "2026-10-06T13:00:00Z"), interrupted.Wait);
        var resumed = events.OfType<Resumed>().Single();
        Assert.Equal((WakeReason.Event, "deployment.finished", true, string.Empty), (resumed.Wake, resumed.EventType, resumed.IsApproved, resumed.Reviewer));

        // A human resume says so.
        var approvals = ApprovalGraph(route: true);
        collector = new Collector();
        await approvals.RunAsync("a", NewApproval(), store, At(clock, collector));
        await approvals.ResumeAsync("a", HumanSignal.Approve("alice"), store, At(clock, collector));
        events = await collector.After(2);
        Assert.Equal(WakeReason.Human, events.OfType<Resumed>().Single().Wake);
        Assert.Equal(WaitKind.Human, events.OfType<Interrupted>().Single().Wait!.Kind);
    }

    // --- Definitions and compile-time rules ---

    /// <summary><c>waits.json</c>'s graph.</summary>
    private static StateGraph<int> ReleaseGraph() =>
        new StateGraph<int>()
            .AddNode("Plan", s => s)
            .AddNode("Approve", s => s)
            .AddInterruptBoundary("Approve", onReject: "Abort", timeout: TimeSpan.FromMinutes(30), onTimeout: "Escalate")
            .AddWaitForEvent("Deploying", "deployment.finished", TimeSpan.FromHours(1), "Rollback")
            .AddNode("Check", s => s)
            .AddTimer("Window", TimeSpan.FromMinutes(5), new Dictionary<string, string> { ["purpose"] = "verification window" })
            .AddNode("Close", s => s)
            .AddNode("Abort", s => s)
            .AddNode("Escalate", s => s)
            .AddNode("Rollback", s => s)
            .AddEdge("Plan", "Approve")
            .AddEdge("Approve", "Deploying")
            .AddEdge("Deploying", "Check")
            .AddConditionalEdge("Check", "Window", s => s < 3)
            .AddEdge("Check", "Close")
            .AddEdge("Window", "Check");

    [Fact]
    public void The_Export_Matches_The_Shared_Waits_Fixture()
    {
        var expected = ReadFixture(Path.Combine("graph-export", "waits.json"));
        var graph = ReleaseGraph();
        Assert.True(JsonNode.DeepEquals(expected, JsonSerializer.SerializeToNode(graph.ToDefinition())), JsonSerializer.Serialize(graph.ToDefinition()));
        Assert.True(JsonNode.DeepEquals(expected, JsonSerializer.SerializeToNode(graph.Compile().ToDefinition())));

        // The file round-trips through the interop types and the topology, and is sound.
        var definition = expected.Deserialize<GraphDefinition>()!;
        Assert.True(JsonNode.DeepEquals(expected, JsonSerializer.SerializeToNode(definition.ToTopology().ToDefinition())));
        Assert.True(GraphValidator.Validate(definition.ToTopology()).IsValid);

        // The timeout routes are what keep Escalate and Rollback reachable.
        var without = definition with { Edges = [.. definition.Edges.Where(e => e.Condition != "on_timeout")] };
        Assert.Equal(["Escalate", "Rollback"], GraphValidator.Validate(without.ToTopology()).Errors.Select(e => e.Node));
    }

    [Fact]
    public void A_Wait_In_A_Maps_Template_Is_Reported_At_Any_Depth()
    {
        var definition = ReadFixture(Path.Combine("graph-validation", "wait-in-map.json")).Deserialize<GraphDefinition>()!;

        var errors = GraphValidator.Validate(definition.ToTopology()).Errors;

        Assert.Equal(
            [("STRATA_ERR_WAIT_IN_MAP", "Fan/*/Window"), ("STRATA_ERR_WAIT_IN_MAP", "Review/Batch/*/Inner/Hold")],
            errors.Select(e => (e.Code, e.Node)));
        Assert.All(errors, e => Assert.Equal(ValidationErrorKind.WaitInMap, e.Kind));
    }

    [Fact]
    public void A_Definition_With_A_Wait_Must_Say_So()
    {
        var old = ReadFixture(Path.Combine("graph-export", "waits.json"));
        old["schema_version"] = "1.3.0";
        // 1.3.0 had no boundaries either (design/48-Boundaries-In-Definitions.md).
        old["nodes"]![1]!.AsObject().Remove("interrupt");
        Assert.Equal(
            "node 'Deploying' is a wait, which schema_version 1.3.0 doesn't have",
            Assert.Throws<FormatException>(() => old.Deserialize<GraphDefinition>()!.ToTopology()).Message);

        var noWait = ReadFixture(Path.Combine("graph-export", "waits.json"));
        noWait["nodes"]![2]!.AsObject().Remove("wait");
        Assert.Equal(
            "node 'Deploying' is a wait but has no wait",
            Assert.Throws<FormatException>(() => noWait.Deserialize<GraphDefinition>()!.ToTopology()).Message);

        var stray = ReadFixture(Path.Combine("graph-export", "waits.json"));
        stray["nodes"]![0]!["wait"] = stray["nodes"]![2]!["wait"]!.DeepClone();
        Assert.Equal(
            "node 'Plan' has a wait but is not a wait node",
            Assert.Throws<FormatException>(() => stray.Deserialize<GraphDefinition>()!.ToTopology()).Message);
    }

    [Fact]
    public void Compile_Refuses_A_Wait_A_Map_Item_Would_Hit_And_Other_Misconfigurations()
    {
        var refused = Assert.Throws<InvalidOperationException>(() => new StateGraph<List<int>>()
            .AddMap("Fan", s => s, i => i.ToString(System.Globalization.CultureInfo.InvariantCulture), WindowGraph(), (_, _) => new Health(0, false), (s, _) => s)
            .Compile());
        Assert.Equal("map 'Fan' runs a graph with a wait at 'Window'; a map item can't freeze", refused.Message);

        var nested = RemediationGraph(WindowGraph(), _ => new Health(0, false), (entry, _) => entry);
        refused = Assert.Throws<InvalidOperationException>(() => new StateGraph<List<int>>()
            .AddMap("Fan", s => s, i => i.ToString(System.Globalization.CultureInfo.InvariantCulture), nested, (_, _) => NewRemediation(), (s, _) => s)
            .Compile());
        Assert.Contains("a wait at 'Verify/Window'", refused.Message);

        Assert.Equal(
            "'Wait' is a wait node and can't also be an interrupt boundary",
            Assert.Throws<InvalidOperationException>(() => new StateGraph<int>()
                .AddTimer("Wait", TimeSpan.FromSeconds(1))
                .AddInterruptBoundary("Wait")
                .Compile()).Message);
        Assert.Throws<ArgumentOutOfRangeException>(() => new StateGraph<int>().AddTimer("Wait", TimeSpan.Zero));
        Assert.Throws<ArgumentException>(() => new StateGraph<int>().AddWaitForEvent("Wait", "e", onTimeout: "Wait"));
        Assert.Throws<ArgumentOutOfRangeException>(() => new StateGraph<int>().AddInterruptBoundary("Approve", timeout: TimeSpan.Zero));
        // A timeout route to a node that doesn't exist dangles.
        var dangling = Assert.Throws<GraphCompilationException>(() => new StateGraph<int>()
            .AddWaitForEvent("Wait", "e", TimeSpan.FromSeconds(1), "Nowhere")
            .Compile());
        Assert.Equal(ValidationErrorKind.DanglingEdge, Assert.Single(dangling.Result.Errors).Kind);
    }
}
