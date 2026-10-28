using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;
using Microsoft.Extensions.Time.Testing;
using Xunit.Abstractions;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/durable_execution.rs</c> (<c>design/33-Durable-Execution.md</c>, SG-10): a
/// checkpoint per node, and recovering a thread whose process died, through the public API only. Keep the two
/// in step.
/// </summary>
public class DurableExecutionTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);
    private static readonly DateTimeOffset Noon = new(2026, 10, 10, 12, 0, 0, TimeSpan.Zero);

    private static readonly CompilationOptions Durable = new() { Checkpointing = CheckpointPolicy.EveryNode };

    /// <summary>A state whose <c>trail</c> every node adds its name to.</summary>
    public sealed record Job(
        [property: JsonPropertyName("trail")] string Trail,
        [property: JsonPropertyName("approved")] bool Approved = false,
        [property: JsonPropertyName("limit")] int Limit = 0);

    /// <summary>What a store call does in a process that has died.</summary>
    private sealed class ProcessDied() : Exception("the process died");

    /// <summary>
    /// A store as one process sees it. Once the process has died nothing it does reaches the store: not the
    /// checkpoint after the node in flight, not a <c>failed</c> one, not the release of its lease.
    /// </summary>
    private class Process(InMemoryCheckpointStore inner) : ICheckpointStore, ICheckpointQuery
    {
        private volatile bool _dead;

        public void Die() => _dead = true;

        private T Alive<T>(Func<T> call) => _dead ? throw new ProcessDied() : call();

        public void Save(Checkpoint checkpoint) => Alive(() => { inner.Save(checkpoint); return 0; });

        public Checkpoint? Load(string threadId) => Alive(() => inner.Load(threadId));

        public ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
            Alive(() => ((ICheckpointStore)inner).SaveAsync(checkpoint, cancellationToken));

        public ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
            Alive(() => ((ICheckpointStore)inner).LoadAsync(threadId, cancellationToken));

        public virtual ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
            Alive(() => inner.LoadLatestAsync(threadId, cancellationToken));

        public ValueTask<bool> SaveIfLatestAsync(
            Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
            Alive(() => inner.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken));

        public ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
            string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default) =>
            Alive(() => inner.TryAcquireLeaseAsync(threadId, owner, ttl, cancellationToken));

        public ValueTask<ThreadLease?> RenewLeaseAsync(
            ThreadLease lease, TimeSpan ttl, CancellationToken cancellationToken = default) =>
            Alive(() => inner.RenewLeaseAsync(lease, ttl, cancellationToken));

        public ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken cancellationToken = default) =>
            Alive(() => inner.ReleaseLeaseAsync(lease, cancellationToken));

        public ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken cancellationToken = default) =>
            Alive(() => inner.GetLeaseAsync(threadId, cancellationToken));

        public ValueTask<bool> SaveUnderLeaseAsync(
            Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default) =>
            Alive(() => inner.SaveUnderLeaseAsync(checkpoint, lease, expectedSequence, cancellationToken));

        public ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default) =>
            Alive(() => inner.ListThreadsAsync(query, cancellationToken));

        public ValueTask<CheckpointLogPage> GetLogAsync(
            string threadId, long? afterSequence = null, int limit = ThreadQuery.DefaultLimit, CancellationToken cancellationToken = default) =>
            Alive(() => inner.GetLogAsync(threadId, afterSequence, limit, cancellationToken));
    }

    /// <summary>A process that loaded <paramref name="stale"/> before somebody else moved the thread on.</summary>
    private sealed class Rewound(InMemoryCheckpointStore inner, LatestCheckpoint stale) : Process(inner)
    {
        public override ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
            new(stale);
    }

    /// <summary>One run of a handler: where, under which key, and whether it may have run before.</summary>
    private sealed record Visit(string Path, string Key, bool Recovery)
    {
        public string? Reviewer { get; init; }
    }

    /// <summary>What every handler of a test graph ran as, in order, and what to do when one is reached.</summary>
    private sealed class Journal
    {
        private readonly ConcurrentQueue<Visit> _visits = new();
        private readonly ConcurrentDictionary<string, (Action<NodeContext> Hook, bool Once)> _hooks = new();

        public List<Visit> Visits => [.. _visits];

        public List<string> Paths => [.. _visits.Select(v => v.Path)];

        public int Count(string path) => _visits.Count(v => v.Path == path);

        /// <summary>Runs <paramref name="hook"/> the next time thread <paramref name="threadId"/> reaches <paramref name="path"/>.</summary>
        public void On(string threadId, string path, Action<NodeContext> hook) => _hooks[$"{threadId}:{path}"] = (hook, true);

        /// <summary>The process dies the next time thread <paramref name="threadId"/> reaches <paramref name="path"/>.</summary>
        public void DieAt(string threadId, string path, Process process) => On(threadId, path, _ => process.Die());

        public Job Visit(Job job, NodeContext context)
        {
            _visits.Enqueue(new Visit(context.NodePath, context.IdempotencyKey, context.IsRecovery)
            {
                Reviewer = context.Decision?.Reviewer,
            });
            if (_hooks.TryRemove($"{context.ThreadId}:{context.NodePath}", out var hook))
            {
                hook.Hook(context);
            }

            return job with { Trail = job.Trail + context.Node };
        }
    }

    // --- Graphs ---

    private static Func<Job, NodeContext, CancellationToken, ValueTask<Job>> Node(Journal journal) =>
        (job, context, _) => ValueTask.FromResult(journal.Visit(job, context));

    /// <summary>The nodes in a line, each adding its name to the trail.</summary>
    private static StateGraph<Job> Chain(Journal journal, params string[] nodes)
    {
        var graph = new StateGraph<Job>();
        for (var i = 0; i < nodes.Length; i++)
        {
            graph.AddNode(nodes[i], Node(journal));
            if (i > 0)
            {
                graph.AddEdge(nodes[i - 1], nodes[i]);
            }
        }

        return graph;
    }

    private static CompiledGraph<Job> Line(Journal journal, params string[] nodes) => Chain(journal, nodes).Compile(Durable);

    /// <summary><c>A</c> (a boundary, rejected to <c>Rejected</c>) -> <c>B</c> -> <c>C</c>.</summary>
    private static CompiledGraph<Job> Approval(Journal journal) =>
        Chain(journal, "A", "B", "C")
            .AddInterruptBoundary("A", onReject: "Rejected")
            .AddNode("Rejected", Node(journal))
            .Compile(Durable);

    /// <summary><c>Before</c> -> <c>Work</c> (a subgraph: <c>X</c> -> <c>Y</c> -> <c>Z</c>) -> <c>After</c>.</summary>
    private static CompiledGraph<Job> WithSubgraph(Journal journal) =>
        new StateGraph<Job>()
            .AddNode("Before", Node(journal))
            .AddSubgraph(
                "Work",
                Chain(journal, "X", "Y", "Z").Compile(),
                _ => new Job(string.Empty),
                (job, done) => job with { Trail = $"{job.Trail}[{done.Trail}]" })
            .AddNode("After", Node(journal))
            .AddEdge("Before", "Work")
            .AddEdge("Work", "After")
            .Compile(Durable);

    /// <summary><c>Group</c> (branches <c>b1</c>, <c>b2</c>) -> <c>Fan</c> (a map over <c>k1</c>, <c>k2</c>: <c>Step</c> -> <c>Then</c>) -> <c>Done</c>.</summary>
    private static CompiledGraph<Job> WithFanOuts(Journal journal) =>
        new StateGraph<Job>()
            .AddParallelGroup(
                "Group",
                new ParallelBranches<Job>().Branch("b1", Node(journal)).Branch("b2", Node(journal)),
                (job, _) => job with { Trail = job.Trail + "G" })
            .AddMap(
                "Fan",
                _ => new[] { "k1", "k2" },
                key => key,
                Chain(journal, "Step", "Then").Compile(),
                (_, key) => new Job(key),
                (job, _) => job with { Trail = job.Trail + "M" })
            .AddNode("Done", Node(journal))
            .AddEdge("Group", "Fan")
            .AddEdge("Fan", "Done")
            .Compile(Durable);

    // --- Helpers ---

    private static RunOptions Leased(FakeTimeProvider clock, string owner = "worker-1") =>
        new() { Lease = new LeaseOptions(owner, Ttl), TimeProvider = clock };

    private static Job Completed(GraphExecutionResult<Job> result)
    {
        if (result is GraphExecutionResult<Job>.Failed failed)
        {
            Assert.Fail($"expected a completed run, got: {failed.Message}");
        }

        return Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State;
    }

    private static string Refused(GraphExecutionResult<Job> result) =>
        Assert.IsAssignableFrom<GraphExecutionResult<Job>.Failed>(result).Message;

    private static async Task<List<Checkpoint>> Log(InMemoryCheckpointStore store, string threadId) =>
        [.. (await store.GetLogAsync(threadId)).Entries.Select(e => e.Checkpoint)];

    /// <summary><c>running:A&gt;B</c> for each level, outermost first.</summary>
    private static string Shape(Checkpoint checkpoint)
    {
        var levels = new List<string>();
        for (var level = checkpoint; level is not null; level = level.Nested)
        {
            levels.Add($"{RunTelemetryNames.Status(level.Status)}:{level.SuspendedNode ?? "-"}>{level.NextNode ?? "-"}");
        }

        return string.Join(" / ", levels);
    }

    private static async Task<List<string>> Shapes(InMemoryCheckpointStore store, string threadId) =>
        [.. (await Log(store, threadId)).Select(Shape)];

    private static string Trail(Checkpoint checkpoint) => checkpoint.FrozenState!["trail"]!.GetValue<string>();

    private static string Text(DateTimeOffset time) =>
        time.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Runs thread <paramref name="threadId"/> in a process that dies when it reaches <paramref name="path"/>.</summary>
    private static async Task Crash(
        CompiledGraph<Job> graph, Journal journal, InMemoryCheckpointStore store, string threadId, string path, RunOptions? options = null)
    {
        var process = new Process(store);
        journal.DieAt(threadId, path, process);
        await Assert.ThrowsAsync<ProcessDied>(() => graph.RunAsync(threadId, new Job(string.Empty), process, options ?? new RunOptions()));
    }

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

    private static class RunTelemetryNames
    {
        public static string Status(CheckpointStatus status) => status switch
        {
            CheckpointStatus.Interrupted => "interrupted",
            CheckpointStatus.Completed => "completed",
            CheckpointStatus.Failed => "failed",
            CheckpointStatus.Cancelled => "cancelled",
            CheckpointStatus.Ended => "ended",
            _ => "running",
        };
    }

    // --- What is written ---

    [Fact]
    public async Task A_Durable_Run_Writes_A_Checkpoint_Before_Each_Node_And_Completes()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Line(journal, "A", "B", "C");

        var result = await graph.RunAsync("t", new Job(string.Empty), store);

        Assert.Equal("ABC", Completed(result).Trail);
        var log = await Log(store, "t");
        Assert.Equal(["running:->A", "running:A>B", "running:B>C", "completed:C>-"], log.Select(Shape));
        Assert.Equal([string.Empty, "A", "AB", "ABC"], log.Select(Trail));
        Assert.Equal(["0", "1", "2", "3"], log.Select(c => c.Metadata!["strata.hops"]));
        Assert.Equal(["1.7.0", "1.7.0", "1.7.0", "1.0.0"], log.Select(c => c.SchemaVersion));
        Assert.All(log, c => Assert.Null(c.ResumedAt));

        // A finished thread says so.
        var completed = await store.ListThreadsAsync(
            new ThreadQuery { States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Completed } });
        Assert.Equal(["t"], completed.Threads.Select(t => t.ThreadId));
        Assert.Equal(["t/A#1", "t/B#2", "t/C#3"], journal.Visits.Select(v => v.Key));
        Assert.All(journal.Visits, v => Assert.False(v.Recovery));
    }

    [Fact]
    public async Task A_Run_Without_The_Policy_Writes_Nothing_Between_Nodes()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Chain(journal, "A", "B", "C").Compile();

        var result = await graph.RunAsync("t", new Job(string.Empty), store, new RunOptions { RunId = "run-1" });

        Assert.Equal("ABC", Completed(result).Trail);
        Assert.Empty(await Log(store, "t"));
        Assert.Equal(CheckpointPolicy.Boundaries, graph.Checkpointing);

        // Its hop counts aren't stored, so its keys end with the run id: a later resume can't repeat one.
        Assert.Equal(["t/A#1@run-1", "t/B#2@run-1", "t/C#3@run-1"], journal.Visits.Select(v => v.Key));
    }

    [Fact]
    public async Task A_Parallel_Group_And_A_Map_Are_One_Checkpoint_Each()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();

        var result = await WithFanOuts(journal).RunAsync("t", new Job(string.Empty), store);

        Assert.Equal("GMDone", Completed(result).Trail);
        Assert.Equal(
            ["running:->Group", "running:Group>Fan", "running:Fan>Done", "completed:Done>-"], await Shapes(store, "t"));

        // A branch's key is its group's hop; an item's nodes count their own hops under the map's.
        Assert.Equal(
            [
                "t/Done#3", "t/Fan/k1/Step#2.1", "t/Fan/k1/Then#2.2", "t/Fan/k2/Step#2.1", "t/Fan/k2/Then#2.2",
                "t/Group/b1#1", "t/Group/b2#1",
            ],
            journal.Visits.Select(v => v.Key).Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task A_Subgraph_Nests_Its_Childs_Progress_In_The_Parents_Checkpoint()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();

        var result = await WithSubgraph(journal).RunAsync("t", new Job(string.Empty), store);

        Assert.Equal("Before[XYZ]After", Completed(result).Trail);
        var log = await Log(store, "t");
        Assert.Equal(
            [
                "running:->Before",
                "running:Before>Work",
                "running:Work>Work / running:X>Y",
                "running:Work>Work / running:Y>Z",
                "running:Work>After",
                "completed:After>-",
            ],
            log.Select(Shape));

        // The parent holds the state its subgraph node started with; the child holds its own.
        var inside = log[3];
        Assert.Equal(("Before", "1"), (Trail(inside), inside.Metadata!["strata.hops"]));
        Assert.Equal(("XY", "2", "Work"), (Trail(inside.Nested!), inside.Nested!.Metadata!["strata.hops"], inside.Nested.ThreadId));
        Assert.Equal("1.7.0", inside.SchemaVersion);
        Assert.Equal(
            ["t/Before#1", "t/Work/X#2.1", "t/Work/Y#2.2", "t/Work/Z#2.3", "t/After#3"], journal.Visits.Select(v => v.Key));
    }

    [Fact]
    public async Task Progress_Two_Levels_Down_Is_Nested_At_Every_Level()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var mid = new StateGraph<Job>()
            .AddSubgraph("Stage", Chain(journal, "P", "Q").Compile(), job => job, (_, done) => done)
            .AddNode("Wrap", Node(journal))
            .AddEdge("Stage", "Wrap")
            .Compile();
        var graph = new StateGraph<Job>().AddSubgraph("Work", mid, job => job, (_, done) => done).Compile(Durable);

        var result = await graph.RunAsync("t", new Job(string.Empty), store);

        Assert.Equal("PQWrap", Completed(result).Trail);
        Assert.Equal(
            [
                "running:->Work",
                "running:Work>Work / running:Stage>Stage / running:P>Q",
                "running:Work>Work / running:Stage>Wrap",
                "completed:Work>-",
            ],
            await Shapes(store, "t"));
        Assert.Equal(["t/Work/Stage/P#1.1.1", "t/Work/Stage/Q#1.1.2", "t/Work/Wrap#1.2"], journal.Visits.Select(v => v.Key));
    }

    [Fact]
    public async Task A_Boundary_Writes_An_Interrupted_Checkpoint_And_No_Running_One_After_It()
    {
        var store = new InMemoryCheckpointStore();
        var graph = Approval(new Journal());

        var result = await graph.RunAsync("t", new Job(string.Empty), store);

        Assert.IsType<GraphExecutionResult<Job>.Interrupted>(result);
        var log = await Log(store, "t");
        Assert.Equal(["running:->A", "interrupted:A>B"], log.Select(Shape));
        Assert.Equal("1", log[1].Metadata!["strata.hops"]);
    }

    // --- Never left running ---

    [Fact]
    public async Task A_Failed_Node_A_Cancellation_And_A_Hop_Limit_Leave_Their_Own_Checkpoint()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();

        var failing = new StateGraph<Job>()
            .AddNode("A", Node(journal))
            .AddFallibleNode("B", (Job _) => throw NodeException.Fatal("boom"))
            .AddEdge("A", "B")
            .Compile(Durable);
        Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(await failing.RunAsync("failed", new Job(string.Empty), store));
        Assert.Equal(["running:->A", "running:A>B", "failed:B>B"], await Shapes(store, "failed"));

        using var cancellation = new CancellationTokenSource();
        var stopping = new StateGraph<Job>()
            .AddNode("A", Node(journal))
            .AddNode("B", async (Job job, NodeContext _, CancellationToken token) =>
            {
                await cancellation.CancelAsync();
                await Task.Delay(Timeout.Infinite, token);
                return job;
            })
            .AddEdge("A", "B")
            .Compile(Durable);
        Assert.IsType<GraphExecutionResult<Job>.Cancelled>(
            await stopping.RunAsync("cancelled", new Job(string.Empty), store, cancellation.Token));
        Assert.Equal(["running:->A", "running:A>B", "cancelled:B>B"], await Shapes(store, "cancelled"));

        var limited = Chain(journal, "A", "B").Compile(
            new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode, MaxNodeHops = 1 });
        Assert.IsType<GraphExecutionResult<Job>.HopLimitExceeded>(await limited.RunAsync("limited", new Job(string.Empty), store));
        Assert.Equal(["running:->A", "running:A>B", "failed:B>B"], await Shapes(store, "limited"));
    }

    [Fact]
    public async Task A_Routing_Gap_Leaves_A_Failed_Checkpoint_That_Can_Be_Resumed()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = new StateGraph<Job>()
            .AddNode("A", Node(journal))
            .AddNode("B", Node(journal))
            .AddConditionalEdge("A", "B", job => job.Approved)
            .Compile(Durable);

        var result = await graph.RunAsync("t", new Job(string.Empty), store);

        // The run gave up with nothing to say where; the thread says it failed at the node it was running.
        Assert.Equal("no outgoing edge condition matched for node 'A'", Refused(result));
        var log = await Log(store, "t");
        Assert.Equal(["running:->A", "failed:A>A"], log.Select(Shape));
        Assert.Equal(
            ("no outgoing edge condition matched for node 'A'", "fatal", string.Empty),
            (log[1].Metadata!["error"], log[1].Metadata!["error_kind"], Trail(log[1])));

        var resumed = await graph.ResumeAsync("t", HumanSignal.Approve("ops").WithOverride("approved", true), store);

        Assert.Equal("AB", Completed(resumed).Trail);
        Assert.Equal(
            ["running:->A", "failed:A>A", "running:A>A", "running:A>B", "completed:B>-"], await Shapes(store, "t"));
    }

    [Fact]
    public async Task An_Exception_Out_Of_A_Node_Leaves_A_Failed_Checkpoint_And_Still_Propagates()
    {
        var store = new InMemoryCheckpointStore();
        var graph = new StateGraph<Job>()
            .AddNode("A", Node(new Journal()))
            .AddNode("B", (Job _) => throw new InvalidOperationException("a bug"))
            .AddEdge("A", "B")
            .Compile(Durable);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => graph.RunAsync("t", new Job(string.Empty), store));

        Assert.Equal("a bug", thrown.Message);
        var log = await Log(store, "t");
        Assert.Equal(["running:->A", "running:A>B", "failed:B>B"], log.Select(Shape));
        Assert.Equal(("a bug", "fatal", "A"), (log[2].Metadata!["error"], log[2].Metadata!["error_kind"], Trail(log[2])));
    }

    // --- A resume's claim ---

    [Fact]
    public async Task A_Resume_Claims_The_Thread_With_A_Running_Checkpoint_Holding_Its_Decision()
    {
        var store = new InMemoryCheckpointStore();
        var graph = Approval(new Journal());
        await graph.RunAsync("t", new Job(string.Empty), store);

        var result = await graph.ResumeAsync("t", HumanSignal.Approve("alice").WithOverride("limit", 5), store);

        Assert.Equal(new Job("ABC", Limit: 5), Completed(result));
        var log = await Log(store, "t");
        Assert.Equal(
            ["running:->A", "interrupted:A>B", "running:A>B", "running:B>C", "completed:C>-"], log.Select(Shape));

        // The claim is the resume: the decision, the state with the override, and no stamp anywhere.
        var claim = log[2];
        Assert.Equal(("alice", true, "A"), (claim.Decision!.Reviewer, claim.Decision.IsApproved, claim.Decision.Boundary));
        Assert.Equal(5, claim.FrozenState!["limit"]!.GetValue<int>());
        Assert.Equal("1", claim.Metadata!["strata.hops"]);
        Assert.All(log, c => Assert.Null(c.ResumedAt));

        // A rejection's claim holds the route it took.
        await graph.RunAsync("no", new Job(string.Empty), store);
        var rejected = await graph.ResumeAsync("no", HumanSignal.Reject("bob"), store);
        Assert.Equal("ARejected", Completed(rejected).Trail);
        var rejection = (await Log(store, "no"))[2];
        Assert.Equal(("running:A>Rejected", false), (Shape(rejection), rejection.Decision!.IsApproved));
    }

    [Fact]
    public async Task A_Running_Thread_Refuses_A_Resume_And_A_Signal()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Line(journal, "A", "B", "C");
        await Crash(graph, journal, store, "t", "B");
        var before = await Shapes(store, "t");

        const string refusal = "thread 't' is running: only a waiting, failed or cancelled thread can be resumed";
        Assert.Equal(refusal, Refused(await graph.ResumeAsync("t", HumanSignal.Approve("alice"), store)));
        Assert.Equal(refusal, Refused(await graph.SignalAsync("t", new EventSignal("paid"), store)));

        Assert.Equal(before, await Shapes(store, "t"));
        Assert.Equal(1, journal.Count("B"));
    }

    [Fact]
    public async Task Of_Two_Resumes_That_Loaded_One_Checkpoint_One_Runs()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Approval(journal);
        await graph.RunAsync("t", new Job(string.Empty), store);
        var loaded = (await store.LoadLatestAsync("t"))!;

        Assert.Equal("ABC", Completed(await graph.ResumeAsync("t", HumanSignal.Approve("alice"), store)).Trail);
        var second = await graph.ResumeAsync("t", HumanSignal.Approve("bob"), new Rewound(store, loaded));

        Assert.Equal(
            "checkpoint for thread 't' changed before it could be resumed: another resume or end got there first",
            Refused(second));
        Assert.Equal(1, journal.Count("B"));
    }

    // --- A crash recovered ---

    [Fact]
    public async Task A_Crash_Mid_Run_Is_Recovered_And_The_Node_In_Flight_Runs_Again_With_The_Same_Key()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Line(journal, "A", "B", "C");

        await Crash(graph, journal, store, "t", "B");

        // Nothing after the node in flight reached the store.
        Assert.Equal(["running:->A", "running:A>B"], await Shapes(store, "t"));
        var running = await store.ListThreadsAsync(
            new ThreadQuery { States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Running } });
        Assert.Equal(["t"], running.Threads.Select(t => t.ThreadId));

        // A new process continues the thread where its last checkpoint says.
        var result = await graph.RecoverAsync("t", store);

        Assert.Equal("ABC", Completed(result).Trail);
        Assert.Equal(
            [
                new Visit("A", "t/A#1", false),
                new Visit("B", "t/B#2", false),
                new Visit("B", "t/B#2", true),
                new Visit("C", "t/C#3", false),
            ],
            journal.Visits);
        var log = await Log(store, "t");
        Assert.Equal(
            ["running:->A", "running:A>B", "running:A>B", "running:B>C", "completed:C>-"], log.Select(Shape));

        // Only the claim counts the recovery: a node that commits starts the count again.
        Assert.Equal("1", log[2].Metadata!["strata.recoveries"]);
        Assert.False(log[3].Metadata!.ContainsKey("strata.recoveries"));
    }

    [Fact]
    public async Task A_Crash_After_An_Approval_Is_Recovered_With_The_Decision()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Approval(journal);
        await graph.RunAsync("t", new Job(string.Empty), store);
        var process = new Process(store);
        journal.DieAt("t", "B", process);

        await Assert.ThrowsAsync<ProcessDied>(() =>
            graph.ResumeAsync("t", HumanSignal.Approve("alice").WithOverride("limit", 5), process));

        // The claim is all that was written, and it is enough.
        Assert.Equal(["running:->A", "interrupted:A>B", "running:A>B"], await Shapes(store, "t"));

        var result = await graph.RecoverAsync("t", store);

        Assert.Equal(new Job("ABC", Limit: 5), Completed(result));
        Assert.Equal(["alice", "alice", "alice"], journal.Visits.Where(v => v.Path != "A").Select(v => v.Reviewer));
        Assert.Equal([false, true, false], journal.Visits.Where(v => v.Path != "A").Select(v => v.Recovery));
    }

    [Fact]
    public async Task A_Crash_Inside_A_Subgraph_Re_Enters_The_Child_At_Its_Node_In_Flight()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = WithSubgraph(journal);

        await Crash(graph, journal, store, "t", "Work/Y");
        Assert.Equal("running:Work>Work / running:X>Y", (await Shapes(store, "t"))[^1]);

        var result = await graph.RecoverAsync("t", store);

        // The child's first node doesn't run again; the node in flight does, with the key it had.
        Assert.Equal("Before[XYZ]After", Completed(result).Trail);
        Assert.Equal(
            [
                new Visit("Before", "t/Before#1", false),
                new Visit("Work/X", "t/Work/X#2.1", false),
                new Visit("Work/Y", "t/Work/Y#2.2", false),
                new Visit("Work/Y", "t/Work/Y#2.2", true),
                new Visit("Work/Z", "t/Work/Z#2.3", false),
                new Visit("After", "t/After#3", false),
            ],
            journal.Visits);
    }

    [Fact]
    public async Task A_Crash_In_A_Childs_First_Node_Re_Enters_The_Child_From_Its_Input()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = WithSubgraph(journal);

        await Crash(graph, journal, store, "t", "Work/X");
        Assert.Equal("running:Before>Work", (await Shapes(store, "t"))[^1]);

        var result = await graph.RecoverAsync("t", store);

        Assert.Equal("Before[XYZ]After", Completed(result).Trail);
        Assert.Equal(
            [
                new Visit("Before", "t/Before#1", false),
                new Visit("Work/X", "t/Work/X#2.1", false),
                new Visit("Work/X", "t/Work/X#2.1", true),
                new Visit("Work/Y", "t/Work/Y#2.2", false),
                new Visit("Work/Z", "t/Work/Z#2.3", false),
                new Visit("After", "t/After#3", false),
            ],
            journal.Visits);
    }

    [Fact]
    public async Task A_Crash_Inside_A_Group_Or_A_Map_Runs_Every_Branch_And_Item_Again()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = WithFanOuts(journal);

        await Crash(graph, journal, store, "group", "Group/b2");
        Assert.Equal("GMDone", Completed(await graph.RecoverAsync("group", store)).Trail);
        Assert.Equal(
            ["group/Group/b1#1", "group/Group/b2#1"],
            journal.Visits.Where(v => v.Recovery).Select(v => v.Key).Order(StringComparer.Ordinal));

        // An item isn't checkpointed, so every node of every item may have run before.
        await Crash(graph, journal, store, "map", "Fan/k1/Then");
        Assert.Equal("GMDone", Completed(await graph.RecoverAsync("map", store)).Trail);
        Assert.Equal(
            ["map/Fan/k1/Step#2.1", "map/Fan/k1/Then#2.2", "map/Fan/k2/Step#2.1", "map/Fan/k2/Then#2.2"],
            journal.Visits
                .Where(v => v.Recovery && v.Key.StartsWith("map/", StringComparison.Ordinal))
                .Select(v => v.Key)
                .Order(StringComparer.Ordinal));
        Assert.Equal(2, journal.Count("Done"));
    }

    [Fact]
    public async Task A_Crash_While_Waiting_At_A_Boundary_Leaves_Nothing_To_Recover()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Approval(journal);
        await graph.RunAsync("t", new Job(string.Empty), new Process(store));

        // The process is gone, and the thread holds nothing: it is waiting, not running.
        var recovery = await graph.RecoverAsync("t", store);

        Assert.Equal("thread 't' is not running: its latest checkpoint is interrupted", Refused(recovery));
        Assert.Equal(["running:->A", "interrupted:A>B"], await Shapes(store, "t"));
        Assert.Equal("ABC", Completed(await graph.ResumeAsync("t", HumanSignal.Approve("alice"), store)).Trail);
    }

    [Fact]
    public async Task A_Graph_With_A_Streaming_Node_Is_Recovered_By_RecoverStreaming()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = new StateGraph<Job>()
            .AddNode("A", Node(journal))
            .AddStreamingNode("S", async (Job job, NodeContext context, CancellationToken token) =>
            {
                await context.Stream!.EmitTokenAsync("tok", token);
                return journal.Visit(job, context);
            })
            .AddEdge("A", "S")
            .Compile(Durable);
        var process = new Process(store);
        journal.DieAt("t", "S", process);
        var (dying, died) = graph.RunStreaming("t", new Job(string.Empty), process);
        await foreach (var _ in dying.ReadAllAsync())
        {
        }

        await Assert.ThrowsAsync<ProcessDied>(() => died);

        var (reader, completion) = graph.RecoverStreaming("t", store);
        var chunks = new List<string>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            chunks.Add($"{chunk.Type}:{chunk.NodeName}");
        }

        Assert.Equal("AS", Completed(await completion).Trail);
        Assert.Equal(["NodeStart:S", "Token:S", "NodeEnd:S"], chunks);
    }

    // --- Refusals ---

    [Fact]
    public async Task A_Recovery_Is_Refused_Without_The_Policy_For_Another_Graphs_Thread_And_For_No_Thread()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var options = new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode, GraphId = "billing" };
        var graph = Chain(journal, "A", "B", "C").Compile(options);
        await Crash(graph, journal, store, "t", "B");
        var before = await Shapes(store, "t");

        var plain = Chain(journal, "A", "B", "C").Compile(new CompilationOptions { GraphId = "billing" });
        Assert.Equal(
            "this graph doesn't checkpoint every node: only a graph compiled with CheckpointPolicy.EveryNode can recover a thread",
            Refused(await plain.RecoverAsync("t", store)));

        var other = Chain(journal, "A", "B", "C").Compile(
            new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode, GraphId = "shipping" });
        Assert.StartsWith(
            "thread 't' was written by graph 'billing'", Refused(await other.RecoverAsync("t", store)), StringComparison.Ordinal);

        Assert.Equal("no checkpoint found for thread 'nobody'", Refused(await graph.RecoverAsync("nobody", store)));

        Assert.Equal(before, await Shapes(store, "t"));
        Assert.Equal(1, journal.Count("B"));
    }

    [Fact]
    public async Task A_Live_Lease_Refuses_A_Recovery_With_Or_Without_A_Lease_Of_Its_Own()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var journal = new Journal();
        var graph = Line(journal, "A", "B", "C");

        // The process died holding its lease: nothing released it.
        await Crash(graph, journal, store, "t", "B", Leased(clock));
        var before = await Shapes(store, "t");
        var held = $"thread 't' is held by 'worker-1' until {Text(Noon + Ttl)}";

        Assert.Equal(held, Refused(await graph.RecoverAsync("t", store, Leased(clock, "worker-2"))));
        Assert.Equal(held, Refused(await graph.RecoverAsync("t", store)));
        Assert.Equal(before, await Shapes(store, "t"));
        Assert.Equal(1, journal.Count("B"));

        // The lease runs out, and the thread is nobody's.
        clock.Advance(Ttl + TimeSpan.FromMilliseconds(1));
        var result = await graph.RecoverAsync("t", store, Leased(clock, "worker-2"));

        Assert.Equal("ABC", Completed(result).Trail);
        Assert.Null(await store.GetLeaseAsync("t"));
    }

    [Fact]
    public async Task Of_Two_Recoveries_One_Runs()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Line(journal, "A", "B", "C");
        await Crash(graph, journal, store, "t", "B");
        var loaded = (await store.LoadLatestAsync("t"))!;

        Assert.Equal("ABC", Completed(await graph.RecoverAsync("t", store)).Trail);
        var second = await graph.RecoverAsync("t", new Rewound(store, loaded));

        Assert.Equal(
            "thread 't' changed before it could be recovered: its run went on, or another recovery got there first",
            Refused(second));
        Assert.Equal(2, journal.Count("B"));
        Assert.Equal(1, journal.Count("C"));
    }

    // --- The driver ---

    [Fact]
    public async Task The_Driver_Skips_A_Live_Lease_And_Recovers_A_Thread_Whose_Lease_Expired()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var journal = new Journal();
        var graph = Line(journal, "A", "B", "C");
        await Crash(graph, journal, store, "dead", "B", Leased(clock));
        await Crash(
            graph,
            journal,
            store,
            "live",
            "B",
            new RunOptions { Lease = new LeaseOptions("worker-9", TimeSpan.FromHours(1)), TimeProvider = clock });
        var options = new RecoverOptions { Run = Leased(clock, "worker-2") };

        // Both are held: nothing is recovered, and nothing is written.
        var early = await graph.RecoverStaleAsync(store, options);
        Assert.Empty(early.Recovered);
        Assert.Equal(["dead", "live"], early.Skipped.Select(s => s.ThreadId).Order(StringComparer.Ordinal));

        clock.Advance(Ttl + TimeSpan.FromMilliseconds(1));
        var report = await graph.RecoverStaleAsync(store, options);

        var recovered = Assert.Single(report.Recovered);
        Assert.Equal(("dead", 1), (recovered.ThreadId, recovered.Recoveries));
        Assert.Equal(["B"], recovered.At);
        Assert.Equal("ABC", Completed(recovered.Result).Trail);
        Assert.Equal(
            [new SkippedThread("live", $"it is held by 'worker-9' until {Text(Noon + TimeSpan.FromHours(1))}")],
            report.Skipped);
        Assert.Equal("running:A>B", (await Shapes(store, "live"))[^1]);
    }

    [Fact]
    public async Task Without_Leases_The_Driver_Needs_An_Age_And_Recovers_Only_Threads_Older_Than_It()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Line(journal, "A", "B", "C");
        await Crash(graph, journal, store, "t", "B");

        // Only the caller knows how long its nodes take.
        var refused = await Assert.ThrowsAsync<ArgumentException>(() => graph.RecoverStaleAsync(store));
        Assert.StartsWith("RecoverStaleAsync needs RecoverOptions.StaleAfter", refused.Message, StringComparison.Ordinal);

        var fresh = await graph.RecoverStaleAsync(store, new RecoverOptions { StaleAfter = TimeSpan.FromMinutes(5) });
        Assert.Empty(fresh.Recovered);
        Assert.Equal(1, journal.Count("B"));

        var later = new FakeTimeProvider(DateTimeOffset.UtcNow + TimeSpan.FromMinutes(10));
        var report = await graph.RecoverStaleAsync(
            store, new RecoverOptions { StaleAfter = TimeSpan.FromMinutes(5), Run = new RunOptions { TimeProvider = later } });

        Assert.Equal("ABC", Completed(Assert.Single(report.Recovered).Result).Trail);
    }

    [Fact]
    public async Task A_Stop_Token_Recovers_No_More_Threads_And_Lets_The_One_In_Flight_Finish()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Line(journal, "A", "B", "C");
        await Crash(graph, journal, store, "a", "B");
        await Crash(graph, journal, store, "b", "B");
        var later = new FakeTimeProvider(DateTimeOffset.UtcNow + TimeSpan.FromMinutes(10));
        using var stop = new CancellationTokenSource();
        RecoverOptions Options(CancellationToken stopping = default) => new()
        {
            StaleAfter = TimeSpan.FromMinutes(5),
            Run = new RunOptions { TimeProvider = later },
            Stop = stopping,
        };

        // The host is asked to stop while a recovery is in flight (design/35-Hosting-Integration.md).
        journal.On("a", "B", _ => stop.Cancel());
        journal.On("b", "B", _ => stop.Cancel());
        var report = await graph.RecoverStaleAsync(store, Options(stop.Token));

        // The recovery in flight finished, not cancelled, and the other thread was never started.
        var recovered = Assert.Single(report.Recovered);
        Assert.Equal("ABC", Completed(recovered.Result).Trail);
        var other = recovered.ThreadId == "a" ? "b" : "a";
        Assert.Equal("running:A>B", (await Shapes(store, other))[^1]);

        // A token that has fired starts nothing.
        Assert.Empty((await graph.RecoverStaleAsync(store, Options(stop.Token))).Recovered);
        Assert.Equal("running:A>B", (await Shapes(store, other))[^1]);

        // The thread is still dead for the next driver.
        Assert.Equal(other, Assert.Single((await graph.RecoverStaleAsync(store, Options())).Recovered).ThreadId);
    }

    [Fact]
    public async Task The_Driver_Lists_Its_Own_Graphs_Threads_By_Metadata_And_Counts_Recoveries_In_Its_Limit()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        CompiledGraph<Job> Named(string id) => Chain(journal, "A", "B", "C").Compile(
            new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode, GraphId = id });
        var billing = Named("billing");
        static RunOptions Tenant(string tenant) => new() { Metadata = { ["tenant"] = tenant } };
        await Crash(billing, journal, store, "acme-1", "B", Tenant("acme"));
        await Crash(billing, journal, store, "acme-2", "B", Tenant("acme"));
        await Crash(billing, journal, store, "other", "B", Tenant("other"));
        await Crash(Named("shipping"), journal, store, "shipment", "B", Tenant("acme"));
        var options = new RecoverOptions
        {
            StaleAfter = TimeSpan.Zero,
            Metadata = new Dictionary<string, string> { ["tenant"] = "acme" },
            Limit = 1,
        };

        var first = await billing.RecoverStaleAsync(store, options);
        var second = await billing.RecoverStaleAsync(store, options);
        var third = await billing.RecoverStaleAsync(store, options);

        Assert.Equal(
            ["acme-1", "acme-2"],
            first.Recovered.Concat(second.Recovered).Select(r => r.ThreadId).Order(StringComparer.Ordinal));
        Assert.Empty(third.Recovered);
        Assert.Empty(first.Skipped.Concat(second.Skipped).Concat(third.Skipped));
        Assert.Equal("running:A>B", (await Shapes(store, "other"))[^1]);
        Assert.Equal("running:A>B", (await Shapes(store, "shipment"))[^1]);
    }

    [Fact]
    public async Task The_Driver_Stops_Recovering_A_Thread_That_Keeps_Dying()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Line(journal, "A", "B", "C");
        await Crash(graph, journal, store, "t", "B");
        var options = new RecoverOptions { StaleAfter = TimeSpan.Zero, MaxRecoveries = 2 };

        // Each recovery dies in the same node.
        for (var recovery = 1; recovery <= 2; recovery++)
        {
            var process = new Process(store);
            journal.DieAt("t", "B", process);
            await Assert.ThrowsAsync<ProcessDied>(() => graph.RecoverStaleAsync(process, options));
            Assert.Equal(recovery.ToString(), (await Log(store, "t"))[^1].Metadata!["strata.recoveries"]);
        }

        var report = await graph.RecoverStaleAsync(store, options);

        Assert.Empty(report.Recovered);
        Assert.Equal([new SkippedThread("t", "it was recovered 2 time(s) without a node completing")], report.Skipped);
        Assert.Equal(3, journal.Count("B"));

        // An operator can still recover it by hand, or end it.
        Assert.Equal("ABC", Completed(await graph.RecoverAsync("t", store)).Trail);
    }

    [Fact]
    public async Task An_Application_Limits_Its_Own_Recoveries_By_The_Public_Count()
    {
        // RecoverAsync always recovers: an application that recovers each thread itself (not through the
        // driver) reads the count a claim records and stops at its own limit.
        const int limit = 2;
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Line(journal, "A", "B", "C");
        await Crash(graph, journal, store, "t", "B");
        Assert.Equal(0, RecoveryCount.Of((await Log(store, "t"))[^1]));

        var recoveries = 0;
        while (RecoveryCount.Of((await Log(store, "t"))[^1]) < limit)
        {
            var process = new Process(store);
            journal.DieAt("t", "B", process);
            await Assert.ThrowsAsync<ProcessDied>(() => graph.RecoverAsync("t", process));
            recoveries++;
        }

        var latest = (await Log(store, "t"))[^1];
        Assert.Equal(limit, recoveries);
        Assert.Equal(limit, RecoveryCount.Of(latest));
        Assert.Equal("2", latest.Metadata![RecoveryCount.MetadataKey]);
        Assert.Equal("strata.recoveries", RecoveryCount.MetadataKey);

        // Past its limit it recovers by hand: the claim counts once more, and the node that commits clears it.
        Assert.Equal("ABC", Completed(await graph.RecoverAsync("t", store)).Trail);
        var log = await Log(store, "t");
        Assert.Equal([0, 0, 1, 2, 3, 0, 0], log.Select(RecoveryCount.Of));
    }

    // --- Ending, and leases ---

    [Fact]
    public async Task A_Running_Thread_Is_Ended_Unless_A_Live_Lease_Holds_It()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var journal = new Journal();
        var graph = Line(journal, "A", "B", "C");
        await Crash(graph, journal, store, "t", "B", Leased(clock));

        Assert.Equal(
            $"thread 't' is held by 'worker-1' until {Text(Noon + Ttl)}", Refused(await graph.EndAsync("t", store)));
        Assert.Equal("running:A>B", (await Shapes(store, "t"))[^1]);

        clock.Advance(Ttl + TimeSpan.FromMilliseconds(1));
        var ended = Assert.IsType<GraphExecutionResult<Job>.Cancelled>(await graph.EndAsync("t", store));

        // It stood at the node that was to run.
        Assert.Equal((CancelReason.Ended, "B", "A"), (ended.Reason, ended.Node, ended.State.Trail));
        Assert.Equal(["B"], ended.StoppedAt);
        Assert.Equal("ended:B>-", (await Shapes(store, "t"))[^1]);
        Assert.Equal(
            "thread 't' is not running: its latest checkpoint is ended", Refused(await graph.RecoverAsync("t", store)));
    }

    [Fact]
    public async Task A_Stale_Owners_Completion_Is_Fenced()
    {
        var storeClock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(storeClock);
        var journal = new Journal();
        var graph = Line(journal, "A", "B");

        // While the last node runs, the lease runs out on the store's clock and another worker takes the thread.
        journal.On("t", "B", _ =>
        {
            storeClock.Advance(Ttl + TimeSpan.FromMilliseconds(1));
            Assert.True(store.TryAcquireLeaseAsync("t", "worker-2", Ttl).AsTask().GetAwaiter().GetResult().Acquired);
        });

        var result = await graph.RunAsync("t", new Job(string.Empty), store, Leased(new FakeTimeProvider(Noon)));

        var cancelled = Assert.IsType<GraphExecutionResult<Job>.Cancelled>(result);
        Assert.Equal(CancelReason.LeaseLost, cancelled.Reason);
        Assert.Equal(["running:->A", "running:A>B"], await Shapes(store, "t"));
    }

    // --- Events and telemetry ---

    [Fact]
    public async Task A_Recovery_Says_So_In_Its_Events_And_Telemetry()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var graph = Line(journal, "A", "B", "C");
        await Crash(graph, journal, store, "t", "B");
        var collector = new Collector();
        long recoveries = 0;
        using var meter = new MeterListener();
        meter.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == StrataTelemetry.Name && instrument.Name == "strata.thread.recoveries")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        meter.SetMeasurementEventCallback<long>((_, measurement, _, _) => Interlocked.Add(ref recoveries, measurement));
        meter.Start();

        await graph.RecoverAsync("t", store, new RunOptions { Observer = collector });

        var events = await collector.After(1);
        Assert.Equal(
            [
                "RunStarted", "CheckpointSaved", "Recovered",
                "NodeStarted", "NodeCompleted", "CheckpointSaved",
                "NodeStarted", "NodeCompleted", "CheckpointSaved",
                "RunCompleted",
            ],
            events.Select(e => e.GetType().Name));
        var started = events.OfType<RunStarted>().Single();
        Assert.Equal((RunMode.Recover, "B"), (started.Mode, started.Node));
        var recovered = events.OfType<Recovered>().Single();
        Assert.Equal(["B"], recovered.At);
        Assert.Equal(1, recovered.Recoveries);
        Assert.Equal(
            [CheckpointStatus.Running, CheckpointStatus.Running, CheckpointStatus.Completed],
            events.OfType<CheckpointSaved>().Select(e => e.Status));

        // Other suites recover threads at the same time, so at least this one.
        Assert.True(Interlocked.Read(ref recoveries) >= 1);
    }

    // --- Shared fixtures ---

    private static Checkpoint LifecycleFixture(string name, [CallerFilePath] string sourceFilePath = "")
    {
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(sourceFilePath)!, "..", "..", "..", "schema", "fixtures", "checkpoint-lifecycle", name));
        return Checkpoint.FromWireJson(JsonNode.Parse(File.ReadAllText(path))!);
    }

    /// <summary>Blanks every level's <c>created_at</c> and <c>decided_at</c>, which a live run sets to now.</summary>
    private static Checkpoint WithoutTimestamps(Checkpoint checkpoint) =>
        checkpoint with
        {
            CreatedAt = string.Empty,
            Decision = checkpoint.Decision is null ? null : checkpoint.Decision with { DecidedAt = string.Empty },
            Nested = checkpoint.Nested is null ? null : WithoutTimestamps(checkpoint.Nested),
        };

    private static void AssertMatchesFixture(string name, Checkpoint actual)
    {
        var expected = WithoutTimestamps(LifecycleFixture(name)).ToWireJson();
        var got = WithoutTimestamps(actual).ToWireJson();
        Assert.True(JsonNode.DeepEquals(expected, got), got.ToJsonString());
    }

    [Fact]
    public async Task A_Durable_Run_Writes_What_The_Shared_Fixtures_Hold()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        var line = Line(journal, "A", "B", "C");

        await Crash(line, journal, store, "thread_running_demo", "B");
        AssertMatchesFixture("running.json", store.Load("thread_running_demo")!);

        await line.RunAsync("thread_completed_demo", new Job(string.Empty), store);
        AssertMatchesFixture("completed.json", store.Load("thread_completed_demo")!);

        await Crash(WithSubgraph(journal), journal, store, "thread_nested_running_demo", "Work/Y");
        AssertMatchesFixture("nested-running.json", store.Load("thread_nested_running_demo")!);

        var approval = Approval(journal);
        await approval.RunAsync("thread_claimed_demo", new Job(string.Empty), store);
        var process = new Process(store);
        journal.DieAt("thread_claimed_demo", "B", process);
        await Assert.ThrowsAsync<ProcessDied>(() => approval.ResumeAsync(
            "thread_claimed_demo", HumanSignal.Approve("alice").WithOverride("limit", 5), process));
        AssertMatchesFixture("claimed.json", store.Load("thread_claimed_demo")!);
    }

    [Fact]
    public async Task A_Thread_The_Shared_Fixtures_Hold_Is_Recovered()
    {
        var store = new InMemoryCheckpointStore();
        var journal = new Journal();
        foreach (var name in new[] { "running.json", "nested-running.json", "claimed.json" })
        {
            store.Save(LifecycleFixture(name));
        }

        Assert.Equal("ABC", Completed(await Line(journal, "A", "B", "C").RecoverAsync("thread_running_demo", store)).Trail);
        Assert.Equal(
            "Before[XYZ]After",
            Completed(await WithSubgraph(journal).RecoverAsync("thread_nested_running_demo", store)).Trail);
        Assert.Equal(
            new Job("ABC", Limit: 5), Completed(await Approval(journal).RecoverAsync("thread_claimed_demo", store)));

        // The node in flight ran again with the key the fixture's hop count gives it, and the claim's decision.
        Assert.Contains(new Visit("B", "thread_running_demo/B#2", true), journal.Visits);
        Assert.Contains(new Visit("Work/Y", "thread_nested_running_demo/Work/Y#2.2", true), journal.Visits);
        Assert.Contains(new Visit("B", "thread_claimed_demo/B#2", true) { Reviewer = "alice" }, journal.Visits);
        Assert.Equal(0, journal.Count("Work/X"));
    }

    // --- Cost ---

    [Fact]
    public async Task Fifteen_Nodes_Write_Sixteen_Rows_And_None_Without_The_Policy()
    {
        var names = Enumerable.Range(1, 15).Select(i => $"N{i}").ToArray();
        var durable = Chain(new Journal(), names).Compile(Durable);
        var plain = Chain(new Journal(), names).Compile();
        var store = new InMemoryCheckpointStore();

        await durable.RunAsync("durable", new Job(string.Empty), store);
        await plain.RunAsync("plain", new Job(string.Empty), store);

        Assert.Equal(16, (await Log(store, "durable")).Count);
        Assert.Empty(await Log(store, "plain"));

        // Not asserted: a figure for the design doc's "Cost".
        const int runs = 200;
        async Task<double> MicrosecondsPerNode(CompiledGraph<Job> graph, string prefix)
        {
            var timer = Stopwatch.StartNew();
            for (var run = 0; run < runs; run++)
            {
                await graph.RunAsync($"{prefix}-{run}", new Job(string.Empty), store);
            }

            return timer.Elapsed.TotalMicroseconds / (runs * names.Length);
        }

        await MicrosecondsPerNode(durable, "warm-durable");
        await MicrosecondsPerNode(plain, "warm-plain");
        output.WriteLine(
            $"in-memory store, 15 nodes: {await MicrosecondsPerNode(plain, "p"):F1} us/node with Boundaries, "
            + $"{await MicrosecondsPerNode(durable, "d"):F1} us/node with EveryNode");
    }
}
