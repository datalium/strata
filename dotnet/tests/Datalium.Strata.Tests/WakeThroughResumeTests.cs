using System.Collections.Concurrent;
using System.Threading.Channels;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Scheduling;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;
using Microsoft.Extensions.Time.Testing;
using Checkpoint = Datalium.Strata.Interruption.Checkpoint;
using CheckpointStatus = Datalium.Strata.Interruption.CheckpointStatus;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/wake_through_resume.rs</c> (<c>design/42-Waking-Through-A-Resume.md</c>, SG-35):
/// a due thread woken through the application's own <c>ResumeAsync</c> / <c>ResumeStreaming</c> with
/// <see cref="LevelSignals.Due"/>, and an event signal through them with <see cref="LevelSignals.Event"/>. Keep the two
/// in step. The clock is a <see cref="FakeTimeProvider"/>, the run's <see cref="RunOptions.TimeProvider"/>.
/// </summary>
public class WakeThroughResumeTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);
    private static readonly DateTimeOffset Noon = DateTimeOffset.Parse("2026-10-06T12:00:00Z");

    public sealed record Job(IReadOnlyList<string> Log);

    private static Job NewJob() => new([]);

    private static Func<Job, Job> Note(string label) => job => job with { Log = [.. job.Log, label] };

    private static StreamingNodeHandler<Job> Talk(string label, params string[] tokens) =>
        (job, context) =>
        {
            foreach (var token in tokens)
            {
                context.EmitToken(token);
            }

            return job with { Log = [.. job.Log, label] };
        };

    // --- Graphs ---

    /// <summary><c>Start</c> -> <c>Window</c> (a five-minute timer) -> <c>Talk</c> (streams) -> <c>Finish</c>.</summary>
    private static StateGraph<Job> TimerThenTalk() =>
        new StateGraph<Job>()
            .AddNode("Start", Note("start"))
            .AddTimer("Window", TimeSpan.FromMinutes(5))
            .AddStreamingNode("Talk", Talk("talk", "Hel", "lo"))
            .AddNode("Finish", Note("finish"))
            .AddEdge("Start", "Window")
            .AddEdge("Window", "Talk")
            .AddEdge("Talk", "Finish");

    /// <summary><c>Remediate</c> -> <c>Verify</c> (<see cref="TimerThenTalk"/> as a subgraph) -> <c>Report</c> (streams).</summary>
    private static CompiledGraph<Job> Nested() =>
        new StateGraph<Job>()
            .AddNode("Remediate", Note("remediate"))
            .AddSubgraph("Verify", TimerThenTalk().Compile(), job => job, (_, child) => child)
            .AddStreamingNode("Report", Talk("report", "done"))
            .AddEdge("Remediate", "Verify")
            .AddEdge("Verify", "Report")
            .Compile();

    /// <summary><c>Approve</c> (a boundary that times out after 30 minutes, to <c>Escalate</c> when routed) -> <c>Execute</c>.</summary>
    private static CompiledGraph<Job> Approval(bool route, TimeSpan? timeout = null)
    {
        var graph = new StateGraph<Job>()
            .AddNode("Approve", Note("approve"))
            .AddInterruptBoundary("Approve", timeout: timeout, onTimeout: route ? "Escalate" : null)
            .AddNode("Execute", Note("execute"))
            .AddEdge("Approve", "Execute");
        if (route)
        {
            graph.AddNode("Escalate", Note("escalate"));
        }

        return graph.Compile();
    }

    /// <summary><c>Deploying</c> (waits for <c>deployment.finished</c>) -> <c>Talk</c> (streams).</summary>
    private static CompiledGraph<Job> Deploy() =>
        new StateGraph<Job>()
            .AddWaitForEvent("Deploying", "deployment.finished")
            .AddStreamingNode("Talk", Talk("talk", "deployed"))
            .AddEdge("Deploying", "Talk")
            .Compile();

    // --- Helpers ---

    /// <summary>Every event of every run, in arrival order.</summary>
    private sealed class Collector : IRunObserver
    {
        private readonly ConcurrentQueue<RunEvent> _events = new();

        public void OnEvent(RunEvent runEvent) => _events.Enqueue(runEvent);

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

    /// <summary>
    /// An <see cref="InMemoryCheckpointStore"/> whose <c>LoadLatestAsync</c> holds each caller until a second one
    /// has loaded too, once armed: two wakes that both decided a thread is due, racing to wake it.
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

    private static RunOptions At(FakeTimeProvider clock, IRunObserver? observer = null) =>
        new() { TimeProvider = clock, Observer = observer };

    /// <summary>Runs <paramref name="graph"/> to its first wait, streaming, since some of these graphs' children stream.</summary>
    private static async Task Freeze(CompiledGraph<Job> graph, string threadId, ICheckpointStore store, FakeTimeProvider clock) =>
        Assert.IsType<GraphExecutionResult<Job>.Interrupted>((await Stream(graph.RunStreaming(threadId, NewJob(), store, At(clock)))).Result);

    private static async Task<List<StreamChunk>> Drain(ChannelReader<StreamChunk> reader)
    {
        var chunks = new List<StreamChunk>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            chunks.Add(chunk);
        }

        return chunks;
    }

    private static async Task<(List<StreamChunk> Chunks, GraphExecutionResult<Job> Result)> Stream(
        (ChannelReader<StreamChunk> Reader, Task<GraphExecutionResult<Job>> Completion) run) =>
        (await Drain(run.Reader).WaitAsync(Patience), await run.Completion.WaitAsync(Patience));

    private static List<string> TokensOf(IEnumerable<StreamChunk> chunks, string node) =>
        [.. chunks.Where(c => c.Type == StreamChunkType.Token && c.NodeName == node).Select(c => c.Payload.ToString()!)];

    private static string Refused(GraphExecutionResult<Job> result) =>
        Assert.IsAssignableFrom<GraphExecutionResult<Job>.Failed>(result).Message;

    private static Job Completed(GraphExecutionResult<Job> result)
    {
        if (result is GraphExecutionResult<Job>.Failed failed)
        {
            Assert.Fail($"expected the run to complete, got: {failed.Message}");
        }

        return Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State;
    }

    private static async Task<List<Checkpoint>> LogOf(InMemoryCheckpointStore store, string threadId) =>
        [.. (await store.GetLogAsync(threadId)).Entries.Select(e => e.Checkpoint)];

    // --- Waking ---

    [Fact]
    public async Task A_Due_Timer_Woken_Through_A_Streaming_Resume_Streams_And_Completes()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = TimerThenTalk().Compile();
        await Freeze(graph, "t", store, clock);
        clock.Advance(TimeSpan.FromMinutes(5));
        var collector = new Collector();

        var (chunks, result) = await Stream(graph.ResumeStreaming("t", LevelSignals.Due, store, At(clock, collector)));

        Assert.Equal(["start", "talk", "finish"], Completed(result).Log);
        Assert.Equal(["Hel", "lo"], TokensOf(chunks, "Talk"));
        Assert.Equal("Talk", chunks[0].NodeName);
        // What WakeDueAsync writes: one resume stamp, no decision.
        var log = await LogOf(store, "t");
        Assert.Equal(2, log.Count);
        Assert.NotNull(log[1].ResumedAt);
        Assert.Null(log[1].Decision);
        var events = await collector.After(1);
        Assert.Equal((RunMode.Resume, "Talk"), events.OfType<RunStarted>().Select(e => (e.Mode, e.Node)).Single());
        var resumed = events.OfType<Resumed>().Single();
        Assert.Equal((WakeReason.Timer, (string?)null), (resumed.Wake, resumed.TimedOutTo));
        // Woken, it isn't due any more.
        Assert.Empty((await graph.WakeDueAsync(store, new WakeOptions { Run = At(clock) })).Woken);
    }

    [Fact]
    public async Task A_Due_Timer_Inside_A_Subgraph_Streams_The_Child_And_Then_The_Outer_Level()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = Nested();
        await Freeze(graph, "n", store, clock);
        clock.Advance(TimeSpan.FromMinutes(5));

        var (chunks, result) = await Stream(graph.ResumeStreaming("n", LevelSignals.Due, store, At(clock)));

        Assert.Equal(["remediate", "start", "talk", "finish", "report"], Completed(result).Log);
        Assert.Equal(["Hel", "lo"], TokensOf(chunks, "Verify/Talk"));
        Assert.Equal(["done"], TokensOf(chunks, "Report"));
    }

    [Fact]
    public async Task A_Due_Timeout_Takes_Its_Route()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = Approval(route: true, TimeSpan.FromMinutes(30));
        await Freeze(graph, "a", store, clock);
        clock.Advance(TimeSpan.FromMinutes(30));
        var collector = new Collector();

        var state = Completed(await graph.ResumeAsync("a", LevelSignals.Due, store, At(clock, collector)));

        Assert.Equal(["approve", "escalate"], state.Log);
        var events = await collector.After(1);
        Assert.Equal("Escalate", events.OfType<RunStarted>().Single().Node);
        var resumed = events.OfType<Resumed>().Single();
        Assert.Equal((WakeReason.Timeout, "Escalate", (string?)null), (resumed.Wake, resumed.TimedOutTo, resumed.RejectedTo));
        // A timeout isn't a human's answer: no decision.
        Assert.All(await LogOf(store, "a"), c => Assert.Null(c.Decision));
    }

    [Fact]
    public async Task A_Due_Timeout_With_No_Route_Ends_The_Thread()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = Approval(route: false, TimeSpan.FromMinutes(30));
        await Freeze(graph, "a", store, clock);
        clock.Advance(TimeSpan.FromMinutes(31));
        var collector = new Collector();

        var (chunks, result) = await Stream(graph.ResumeStreaming("a", LevelSignals.Due, store, At(clock, collector)));

        Assert.Empty(chunks);
        var ended = Assert.IsType<GraphExecutionResult<Job>.Cancelled>(result);
        Assert.Equal((CancelReason.Ended, "Approve"), (ended.Reason, ended.Node));
        var checkpoint = store.Load("a")!;
        Assert.Equal(CheckpointStatus.Ended, checkpoint.Status);
        Assert.Equal(ThreadEnd.TimedOut, checkpoint.Metadata![ThreadEnd.ReasonKey]);
        Assert.Equal(2, (await LogOf(store, "a")).Count);
        // The run is the resume it was called as, and says it ended the thread.
        var events = await collector.After(1);
        Assert.Equal((RunMode.Resume, "Approve"), events.OfType<RunStarted>().Select(e => (e.Mode, e.Node)).Single());
        Assert.Equal(CheckpointStatus.Ended, events.OfType<CheckpointSaved>().Single().Status);
        Assert.Empty(events.OfType<Resumed>());
        var completed = events.OfType<RunCompleted>().Single();
        Assert.Equal((RunOutcome.Cancelled, CancelReason.Ended), (completed.Outcome, completed.CancelReason));
        // Ended, it is neither due nor resumable.
        Assert.Contains("was ended", Refused(await graph.ResumeAsync("a", LevelSignals.Due, store, At(clock))));
    }

    [Fact]
    public async Task A_Thread_That_Is_Not_Due_Is_Refused_And_Nothing_Is_Written()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var timers = TimerThenTalk().Compile();
        var approvals = Approval(route: false);
        var deploys = Deploy();
        await Freeze(timers, "timer", store, clock);
        await Freeze(approvals, "human", store, clock);
        await Freeze(deploys, "event", store, clock);
        var failing = new StateGraph<Job>()
            .AddFallibleNode("Fail", (Job _) => throw NodeException.Fatal("boom"))
            .Compile();
        Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(await failing.RunAsync("failed", NewJob(), store));

        // One second early.
        clock.Advance(TimeSpan.FromMinutes(5) - TimeSpan.FromSeconds(1));
        Assert.Equal(
            "thread 'timer' is not due until 2026-10-06T12:05:00Z: it is waiting for a timer at [Window]",
            Refused(await timers.ResumeAsync("timer", LevelSignals.Due, store, At(clock))));
        Assert.Equal(
            "thread 'human' is not due: it is waiting for a human at [Approve], with no timeout",
            Refused(await approvals.ResumeAsync("human", LevelSignals.Due, store, At(clock))));
        Assert.Equal(
            "thread 'event' is not due: it is waiting for event 'deployment.finished' at [Deploying], with no timeout",
            Refused(await deploys.ResumeAsync("event", LevelSignals.Due, store, At(clock))));
        Assert.Equal(
            "thread 'failed' is not due: its latest checkpoint is failed",
            Refused(await failing.ResumeAsync("failed", LevelSignals.Due, store, At(clock))));

        foreach (var thread in new[] { "timer", "human", "event", "failed" })
        {
            Assert.Single(await LogOf(store, thread));
        }

        // A second later it is.
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(["start", "talk", "finish"], Completed((await Stream(timers.ResumeStreaming("timer", LevelSignals.Due, store, At(clock)))).Result).Log);
    }

    [Fact]
    public async Task Another_Graphs_Thread_An_Ended_Thread_And_Outer_Signals_Are_Refused_And_Nothing_Is_Written()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);

        // Another graph's, by identity.
        var named = TimerThenTalk().Compile(new CompilationOptions { GraphId = "verify" });
        var other = TimerThenTalk().Compile(new CompilationOptions { GraphId = "triage" });
        await Freeze(named, "named", store, clock);

        // Another graph's, by its waits: this graph has no timer at [Window].
        var timers = TimerThenTalk().Compile();
        await Freeze(timers, "timer", store, clock);
        var approvals = Approval(route: true, TimeSpan.FromMinutes(30));

        // Ended.
        await Freeze(timers, "ended", store, clock);
        Assert.IsType<GraphExecutionResult<Job>.Cancelled>(await timers.EndAsync("ended", store));

        // Outer-level signals with a wake.
        var nested = Nested();
        await Freeze(nested, "nested", store, clock);

        clock.Advance(TimeSpan.FromHours(1));
        Assert.Contains("was written by graph 'verify'", Refused(await other.ResumeAsync("named", LevelSignals.Due, store, At(clock))));
        Assert.Equal(
            "thread 'timer' waits at [Window], which is not a timer in this graph",
            Refused(await approvals.ResumeAsync("timer", LevelSignals.Due, store, At(clock))));
        Assert.Equal(
            "thread 'ended' was ended: an ended thread can't be resumed",
            Refused(await timers.ResumeAsync("ended", LevelSignals.Due, store, At(clock))));
        Assert.Equal(
            "cannot resume thread 'nested': a wake or an event signal answers the innermost level only, and takes no signals for outer levels",
            Refused(await nested.ResumeAsync("nested", LevelSignals.Due.At([], HumanSignal.Approve("alice")), store, At(clock))));

        foreach (var thread in new[] { "named", "timer", "nested" })
        {
            Assert.Single(await LogOf(store, thread));
        }

        Assert.Equal(2, (await LogOf(store, "ended")).Count);
    }

    [Fact]
    public async Task A_Due_Resume_And_A_Wake_Driver_Wake_A_Thread_Once()
    {
        var store = new RacingStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = TimerThenTalk().Compile();
        await Freeze(graph, "t", store, clock);
        clock.Advance(TimeSpan.FromMinutes(5));
        store.Armed = true;

        // Both load the thread before either wakes it.
        var resume = graph.ResumeStreaming("t", LevelSignals.Due, store, At(clock));
        var driver = graph.WakeDueAsync(store, new WakeOptions { Run = At(clock) });
        var (_, resumed) = await Stream(resume);
        var report = await driver.WaitAsync(Patience);

        if (resumed is GraphExecutionResult<Job>.Completed)
        {
            Assert.Empty(report.Woken);
            Assert.Equal("another driver woke or ended it first", Assert.Single(report.Skipped).Reason);
        }
        else
        {
            Assert.Equal(
                "checkpoint for thread 't' changed before it could be resumed: another resume or end got there first",
                Refused(resumed));
            Assert.Single(report.Woken);
        }

        Assert.Single(await LogOf(store.Inner, "t"), c => c.ResumedAt is not null);
    }

    [Fact]
    public async Task A_Thread_Another_Owner_Holds_Is_Refused_And_A_Woken_Run_Holds_The_Lease()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        ThreadLease? during = null;
        var graph = new StateGraph<Job>()
            .AddTimer("Window", TimeSpan.FromMinutes(5))
            .AddNode("Check", async (Job job, NodeContext context, CancellationToken _) =>
            {
                during = await store.GetLeaseAsync(context.ThreadId);
                return job with { Log = [.. job.Log, "check"] };
            })
            .AddEdge("Window", "Check")
            .Compile();
        await Freeze(graph, "t", store, clock);
        clock.Advance(TimeSpan.FromMinutes(5));
        var leased = new RunOptions { TimeProvider = clock, Lease = new LeaseOptions("worker-1", TimeSpan.FromMinutes(1)) };

        var holder = (await store.TryAcquireLeaseAsync("t", "worker-2", TimeSpan.FromMinutes(10))).Lease!;
        Assert.StartsWith("thread 't' is held by 'worker-2' until ", Refused(await graph.ResumeAsync("t", LevelSignals.Due, store, leased)));
        Assert.Single(await LogOf(store, "t"));

        await store.ReleaseLeaseAsync(holder);
        Assert.Equal(["check"], Completed(await graph.ResumeAsync("t", LevelSignals.Due, store, leased)).Log);
        Assert.Equal("worker-1", during?.Owner);
        Assert.Null(await store.GetLeaseAsync("t"));
    }

    [Fact]
    public async Task A_Due_Resume_Waits_For_A_Slot_At_Its_Priority()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var order = new ConcurrentQueue<string>();
        var graph = new StateGraph<Job>()
            .AddNode("Start", job => job)
            .AddTimer("Window", TimeSpan.FromMinutes(5))
            .AddNode("Work", async (Job job, NodeContext context, CancellationToken _) =>
            {
                order.Enqueue(context.ThreadId);
                await Task.Yield();
                return job;
            })
            .AddEdge("Start", "Window")
            .AddEdge("Window", "Work")
            .Compile();
        await Freeze(graph, "low", store, clock);
        await Freeze(graph, "high", store, clock);
        clock.Advance(TimeSpan.FromMinutes(5));
        var scheduler = new RunScheduler(new RunSchedulerOptions { Name = Guid.NewGuid().ToString(), MaxConcurrent = 1 });
        var holder = await scheduler.AdmitAsync(new AdmissionRequest());

        var low = graph.ResumeAsync("low", LevelSignals.Due, store, new RunOptions { TimeProvider = clock, Scheduler = scheduler, Priority = 0 });
        var high = graph.ResumeAsync("high", LevelSignals.Due, store, new RunOptions { TimeProvider = clock, Scheduler = scheduler, Priority = 5 });
        using (var patience = new CancellationTokenSource(Patience))
        {
            while (scheduler.Snapshot().Queued < 2)
            {
                await Task.Delay(5, patience.Token);
            }
        }

        // Nothing was loaded or written while they waited.
        Assert.Single(await LogOf(store, "low"));
        Assert.Single(await LogOf(store, "high"));
        holder.Dispose();
        await Task.WhenAll(low, high).WaitAsync(Patience);

        Assert.Equal(["high", "low"], order);
    }

    [Fact]
    public async Task A_Durable_Graph_Claims_A_Due_Thread_With_A_Running_Checkpoint()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = TimerThenTalk().Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });
        await Freeze(graph, "t", store, clock);
        clock.Advance(TimeSpan.FromMinutes(5));
        var frozen = (await LogOf(store, "t")).Count;

        Assert.Equal(["start", "talk", "finish"], Completed((await Stream(graph.ResumeStreaming("t", LevelSignals.Due, store, At(clock)))).Result).Log);

        var claim = (await LogOf(store, "t"))[frozen];
        Assert.Equal((CheckpointStatus.Running, "Talk", null), (claim.Status, claim.NextNode, claim.Wait));
        Assert.All(await LogOf(store, "t"), c => Assert.Null(c.ResumedAt));
        Assert.Equal(CheckpointStatus.Completed, store.Load("t")!.Status);
    }

    // --- Event signals ---

    [Fact]
    public async Task An_Event_Signal_Through_A_Streaming_Resume_Streams_The_Signalled_Run()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var graph = Deploy();
        await Freeze(graph, "d", store, clock);
        var collector = new Collector();

        Assert.Equal(
            "thread 'd' is not waiting for event 'deployment.started': it is waiting for event 'deployment.finished' at [Deploying]",
            Refused((await Stream(graph.ResumeStreaming("d", LevelSignals.Event(new EventSignal("deployment.started")), store))).Result));
        var (chunks, result) = await Stream(graph.ResumeStreaming(
            "d",
            LevelSignals.Event(new EventSignal("deployment.finished").WithOverride("Log", new System.Text.Json.Nodes.JsonArray("deployed"))),
            store,
            At(clock, collector)));

        Assert.Equal(["deployed", "talk"], Completed(result).Log);
        Assert.Equal(["deployed"], TokensOf(chunks, "Talk"));
        var resumed = (await collector.After(1)).OfType<Resumed>().Single();
        Assert.Equal((WakeReason.Event, "deployment.finished"), (resumed.Wake, resumed.EventType));
        Assert.Throws<ArgumentException>(() => LevelSignals.Event(new EventSignal(" ")));
    }
}
