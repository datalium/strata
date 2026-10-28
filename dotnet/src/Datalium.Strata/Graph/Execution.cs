using System.Diagnostics;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Observability;
using Datalium.Strata.Scheduling;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Graph;

/// <summary>
/// One node handler as the engine stores it, whatever kind the caller added
/// (<c>design/17-Async-Execution.md</c>): sync handlers are wrapped to run on the thread pool, async
/// ones to report a <see cref="NodeStep{TState}"/>, so the executor has one path for all of them.
/// </summary>
internal delegate ValueTask<NodeStep<TState>> NodeHandler<TState>(TState state, AttemptInputs inputs);

/// <summary>How a node was added, which decides what a <see cref="NodeException"/> from it means.</summary>
internal enum NodeKind
{
    /// <summary><c>AddNode</c>: a <see cref="NodeException"/> is a bug and propagates out of the run.</summary>
    Plain,

    /// <summary><c>AddStreamingNode</c>: as <see cref="Plain"/>, and a plain run refuses it.</summary>
    Streaming,

    /// <summary><c>AddFallibleNode</c>, a parallel group or a subgraph: failures go to its <see cref="RecoveryPolicy"/>.</summary>
    Fallible,
}

/// <summary>
/// A stored node. <paramref name="NeedsStream"/> marks one a plain run must refuse before running any
/// of it: a streaming node, a parallel group with a streaming branch, or a subgraph whose child
/// streams. <paramref name="IsSubgraph"/> marks the only nodes a run can resume <i>into</i> with a
/// nested checkpoint, and the only ones the engine lets wind down on cancellation instead of
/// abandoning. A subgraph also has <paramref name="CheckResume"/>, which checks a resume of its child
/// before anything is stamped (<c>design/21-Typed-Human-Signals.md</c>, "Preflight"), and
/// <paramref name="Boundary"/>, the first interrupt boundary or wait its child could freeze at, qualified
/// under it (<see cref="CompiledGraph{TState}.InterruptBoundary"/>). <paramref name="WindsDown"/> marks a map
/// (<c>design/27-Map-Fan-Out.md</c>): like a subgraph it is waited for when its token fires, so its items
/// stop before the node is reported, but it hands back no checkpoint. A subgraph's <paramref name="Adopt"/>
/// takes its child's level of a stored checkpoint as the child graph does (<c>design/31-Graph-Identity.md</c>),
/// and its <paramref name="Claim"/> makes the child's level of a resume's or a recovery's claim
/// (<c>design/33-Durable-Execution.md</c>).
/// </summary>
internal sealed record NodeEntry<TState>(
    NodeKind Kind,
    NodeHandler<TState> Handler,
    bool NeedsStream,
    bool IsSubgraph,
    ResumeChecker? CheckResume = null,
    bool WindsDown = false,
    Suspension? Boundary = null,
    LevelAdopter? Adopt = null,
    LevelClaimer? Claim = null);

/// <summary>
/// Where a graph can freeze (<see cref="CompiledGraph{TState}.InterruptBoundary"/>): an interrupt boundary,
/// or a wait node (<c>design/29-Event-Waits-And-Timers.md</c>), at a path qualified with the subgraph nodes
/// it is inside.
/// </summary>
internal sealed record Suspension(string Path, bool IsWait)
{
    /// <summary>This suspension as seen from the graph whose subgraph node <paramref name="node"/> holds it.</summary>
    public Suspension Under(string node) => this with { Path = $"{node}/{Path}" };

    /// <summary>For messages: <c>an interrupt boundary at 'Review/Approve'</c>, <c>a wait at 'Window'</c>.</summary>
    public override string ToString() => IsWait ? $"a wait at '{Path}'" : $"an interrupt boundary at '{Path}'";
}

/// <summary>
/// Checks that a resume from <paramref name="checkpoint"/> with <paramref name="signals"/> can start,
/// without changing anything. <paramref name="threadId"/> is the outermost thread and
/// <paramref name="path"/> names the level, for messages (<c>[]</c> at the top).
/// </summary>
internal delegate ResumeCheck ResumeChecker(
    Checkpoint checkpoint, LevelSignals signals, string threadId, IReadOnlyList<string> path);

/// <summary>
/// Takes a level of a stored checkpoint as a graph does (<see cref="CompiledGraph{TState}.Adopt"/>):
/// <paramref name="threadId"/> is the outermost thread and <paramref name="path"/> names the level.
/// </summary>
internal delegate Adopted LevelAdopter(Checkpoint checkpoint, string threadId, IReadOnlyList<string> path);

/// <summary>
/// A level of the <c>running</c> checkpoint a resume or a recovery claims its thread with
/// (<see cref="CompiledGraph{TState}.Claim"/>, <c>design/33-Durable-Execution.md</c>): <paramref name="checkpoint"/>
/// as the run continues it, with <paramref name="signals"/> applied.
/// </summary>
internal delegate Checkpoint LevelClaimer(Checkpoint checkpoint, LevelSignals signals, RunScope run, string now);

/// <summary>
/// Takes the <c>running</c> checkpoint of a subgraph's child, each time a node of the child commits, to nest
/// it in the parent's and save that (<c>design/33-Durable-Execution.md</c>, "Subgraphs, groups and maps").
/// </summary>
internal delegate ValueTask CheckpointSink(Checkpoint child);

/// <summary>
/// A stored checkpoint level as a graph takes it (<c>design/31-Graph-Identity.md</c>): the
/// <paramref name="Checkpoint"/> to go on with (migrated, if another version wrote it), or the
/// <paramref name="Problem"/> that refuses it.
/// </summary>
internal readonly record struct Adopted(Checkpoint? Checkpoint, string? Problem)
{
    public static Adopted Refused(string problem) => new(null, problem);
}

/// <summary>
/// What a resume check found: the <paramref name="Problem"/> that refuses the resume, or, when the
/// innermost level will take a route instead of its next node, the route (<paramref name="Route"/>): its
/// boundary's reject route on a rejection, its wait's timeout route on a timeout
/// (<c>design/29-Event-Waits-And-Timers.md</c>). <paramref name="EndsOnTimeout"/>: the wake is a timeout and
/// the innermost wait has no route, so the thread is ended instead of resumed.
/// </summary>
internal sealed record ResumeCheck(string? Problem, string? Route, bool EndsOnTimeout = false);

/// <summary>
/// What one attempt of a node is handed besides the state: the <see cref="NodeContext"/> its handler
/// sees, the node's concrete <see cref="StreamContext"/> (for a group or subgraph to derive its
/// branches' and child's from), what a subgraph node is resuming from, the level it runs at, and the
/// attempt's token (cancelled with the run, by the node's timeout, or for a branch by a failing sibling).
/// </summary>
internal sealed record AttemptInputs(
    NodeContext Context, StreamContext? Stream, NestedResume? Resume, LevelScope Level, CancellationToken Token);

/// <summary>
/// What every level of one run shares: the caller's thread id, options and root token, and the run's
/// telemetry (<c>design/18-Run-Observability.md</c>), which also holds its id and metadata.
/// </summary>
internal sealed class RunScope(
    string threadId,
    RunOptions options,
    CancellationToken token,
    CancellationToken caller,
    RunTelemetry telemetry,
    RunLease? lease,
    bool durable)
{
    public string ThreadId { get; } = threadId;

    /// <summary>
    /// Whether the run checkpoints every node (<c>design/33-Durable-Execution.md</c>): its outermost graph was
    /// compiled with <see cref="CheckpointPolicy.EveryNode"/>.
    /// </summary>
    public bool Durable { get; } = durable;

    /// <summary>
    /// The <c>running</c> checkpoint this run last saved, while it is still the last thing it saved: what the
    /// thread's log ends with unless someone else wrote. A run that ends with one set has left its thread
    /// <c>running</c>, and marks it failed first (<c>design/33-Durable-Execution.md</c>, "What a durable run
    /// writes").
    /// </summary>
    public Checkpoint? LastRunning { get; set; }

    public IHistoryStore? History { get; } = options.History;

    public IServiceProvider? Services { get; } = options.Services;

    /// <summary>The run's replacement for the outermost graph's compiled hop limit, if any.</summary>
    public int? MaxNodeHops { get; } = options.MaxNodeHops;

    /// <summary>The clock every engine wait of the run uses: backoff, node timeouts, the deadline.</summary>
    public TimeProvider Clock { get; } = options.TimeProvider ?? TimeProvider.System;

    public RunTelemetry Telemetry { get; } = telemetry;

    /// <summary>What a checkpoint at any level of this run holds in <c>metadata</c>: the run's keys and <paramref name="engine"/>'s.</summary>
    public Dictionary<string, string>? CheckpointMetadata(Dictionary<string, string>? engine = null) =>
        RunMetadata.ForCheckpoint(Telemetry.Metadata, engine);

    /// <summary>Cancelled by the caller's token, when the deadline passes, or when the run's lease is lost.</summary>
    public CancellationToken Token { get; } = token;

    /// <summary>The run's lease on its thread, when it runs under one (<c>design/32-Thread-Leases.md</c>).</summary>
    public RunLease? Lease { get; } = lease;

    public CancelReason Reason =>
        Lease is { IsLost: true } ? CancelReason.LeaseLost
        : caller.IsCancellationRequested ? CancelReason.Requested
        : CancelReason.Deadline;

    /// <summary>
    /// Set when the run was refused its thread's lease, to who holds it: <c>held by 'worker-2' until …</c>.
    /// The wake driver then skips the thread instead of reporting it woken.
    /// </summary>
    public string? Held { get; set; }

    /// <summary>
    /// Set when the run wrote nothing because its thread changed after it was loaded: a resume whose
    /// conditional stamp lost, or a timeout's end that did (<c>design/29-Event-Waits-And-Timers.md</c>,
    /// "Waking"). The wake driver then skips the thread instead of reporting it woken.
    /// </summary>
    public bool Superseded { get; set; }

    /// <summary>
    /// The context a handler at <paramref name="path"/> sees: a node's, or a parallel group's branch's, running
    /// at <paramref name="level"/> (for a branch, its group's).
    /// </summary>
    public NodeContext ContextFor(
        string node,
        string path,
        int attempt,
        IStreamContext? stream,
        Activity? activity,
        LevelScope level,
        NodeFailure? lastFailure) =>
        new()
        {
            ThreadId = ThreadId,
            Node = node,
            NodePath = path,
            Attempt = attempt,
            Stream = stream,
            History = History,
            Services = Services,
            RunId = Telemetry.RunId,
            Metadata = Telemetry.Metadata,
            Activity = activity,
            Decision = level.Decision,
            LastFailure = lastFailure,
            IdempotencyKey = level.KeyFor(path),
            IsRecovery = level.Recovering,
            Telemetry = Telemetry,
        };
}

/// <summary>
/// One graph level of a run: the outermost graph, or a subgraph's child. <paramref name="Token"/> is
/// the level's root (the run's, or the subgraph node's attempt token), and <paramref name="Prefix"/>
/// qualifies its node names (<c>null</c> at the top, <c>"Work"</c> inside the subgraph node <c>Work</c>).
/// <paramref name="Decision"/> is the answer to the level's most recent interrupt boundary, if any
/// (<c>design/21-Typed-Human-Signals.md</c>): every node of the level sees it, and every checkpoint
/// the level writes carries it. <paramref name="Hops"/> is set while the level has a hop limit in effect
/// (<c>design/24-Loop-Circuit-Breaker.md</c>): the hops it made before the node a checkpoint written now
/// would resume at, which every such checkpoint records as <c>strata.hops</c>. <paramref name="LastFailure"/>
/// is set only for a fallback node, and says why the node it falls back from failed
/// (<c>design/25-Retry-Backoff.md</c>). A run that checkpoints every node (<c>design/33-Durable-Execution.md</c>)
/// counts hops at every level, limit or no limit.
/// </summary>
internal sealed record LevelScope(
    RunScope Run,
    CancellationToken Token,
    string? Prefix,
    HumanDecision? Decision = null,
    int? Hops = null,
    NodeFailure? LastFailure = null)
{
    public string PathOf(string node) => Prefix is null ? node : $"{Prefix}/{node}";

    /// <summary>The hop number of the node this level is running, from 1: set per node by the executor.</summary>
    public int Hop { get; init; }

    /// <summary>
    /// The hop numbers of the nodes this level is inside, each followed by a dot: empty at the top,
    /// <c>"3."</c> inside the outermost graph's third node.
    /// </summary>
    public string HopChain { get; init; } = string.Empty;

    /// <summary>
    /// Whether the node this level is running may have run before its process died
    /// (<see cref="NodeContext.IsRecovery"/>, <c>design/33-Durable-Execution.md</c>): set by a recovery for the
    /// first node of each level it re-enters, and kept until a node of the level commits.
    /// </summary>
    public bool Recovering { get; init; }

    /// <summary>
    /// Where a subgraph's child hands each <c>running</c> checkpoint of its own, for its parent to nest and
    /// save. <see langword="null"/> at the outermost level, which saves to the store, and for a map's item,
    /// whose progress isn't kept.
    /// </summary>
    public CheckpointSink? Progress { get; init; }

    /// <summary>What <see cref="Child"/> gives the child as <see cref="Progress"/>: set while a subgraph node runs.</summary>
    public CheckpointSink? ChildProgress { get; init; }

    /// <summary>
    /// Whether this level writes a checkpoint as each node commits: the run is durable, and the level is the
    /// outermost or a subgraph's child whose parent does.
    /// </summary>
    public bool Checkpoints => Run.Durable && (IsRoot || Progress is not null);

    /// <summary>
    /// The idempotency key of the handler at <paramref name="path"/> running at this level's current hop
    /// (<see cref="NodeContext.IdempotencyKey"/>): the thread, the path and the hop numbers from the outermost
    /// level in. A durable run stores its hop counts, so a recovery repeats the key; any other run adds its
    /// run id, so a later resume, which counts from wherever its checkpoint says, can't repeat one.
    /// </summary>
    public string KeyFor(string path)
    {
        var key = $"{Run.ThreadId}/{path}#{HopChain}{Hop.ToString(System.Globalization.CultureInfo.InvariantCulture)}";
        return Run.Durable ? key : $"{key}@{Run.Telemetry.RunId}";
    }

    /// <summary>
    /// What a checkpoint at this level holds in <c>metadata</c>: the run's keys, <paramref name="engine"/>'s,
    /// and the hop count when the level has a limit.
    /// </summary>
    public Dictionary<string, string>? CheckpointMetadata(Dictionary<string, string>? engine = null)
    {
        if (Hops is { } hops)
        {
            engine ??= [];
            engine[HopLimit.MetadataKey] = HopLimit.Format(hops);
        }

        return Run.CheckpointMetadata(engine);
    }

    /// <summary>
    /// The <c>cancel_reason</c> a checkpoint at this level records: the run's reason, or
    /// <c>"timeout"</c> when the run is still going and it was the enclosing subgraph node's timeout
    /// that stopped this level.
    /// </summary>
    public string WireReason =>
        !Run.Token.IsCancellationRequested ? "timeout"
        : Run.Reason switch
        {
            CancelReason.Deadline => "deadline",

            // Only ever on a child's level, nested in a checkpoint that is never saved.
            CancelReason.LeaseLost => "lease_lost",
            _ => "requested",
        };

    /// <summary>Whether this is the outermost graph of the run, not a subgraph's child.</summary>
    public bool IsRoot => Prefix is null;

    public NodeContext ContextFor(string node, int attempt, IStreamContext? stream, Activity? activity) =>
        Run.ContextFor(node, PathOf(node), attempt, stream, activity, this, LastFailure);

    /// <summary>
    /// The level a subgraph node's child (or a map's item) runs at: rooted at the node's attempt token, named
    /// under its path, with no decision until the child answers a boundary of its own, counting its own hops
    /// under this node's, and with no failure to report. It hands its progress to <see cref="ChildProgress"/>,
    /// and its first node may have run before if this node may have.
    /// </summary>
    public LevelScope Child(CancellationToken token, string nodePath) =>
        this with
        {
            Token = token,
            Prefix = nodePath,
            Decision = null,
            Hops = null,
            LastFailure = null,
            HopChain = $"{HopChain}{Hop.ToString(System.Globalization.CultureInfo.InvariantCulture)}.",
            Hop = 0,
            Progress = ChildProgress,
            ChildProgress = null,
        };
}

/// <summary>
/// The root of one run's token tree: the caller's token, plus the deadline if there is one, and the loss of
/// the run's lease if it has one.
/// </summary>
internal sealed class RootScope : IDisposable
{
    private static readonly RunOptions NoOptions = new();

    // Longest delay CancelAfter takes; a deadline further away than this can't fire during a run anyway.
    private static readonly TimeSpan MaxDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private readonly CancellationTokenSource _source;

    // Fires at the deadline, on the run's clock.
    private readonly CancellationTokenSource? _deadline;

    // The run's lease on its thread, whose loss cancels the run.
    private readonly RunLease? _lease;

    private RootScope(
        string threadId, RunOptions options, CancellationToken caller, RunMode mode, bool durable, Admission? admission)
    {
        var clock = options.TimeProvider ?? TimeProvider.System;
        if (options.Deadline is { } deadline)
        {
            var delay = deadline - clock.GetUtcNow();
            if (delay <= TimeSpan.Zero)
            {
                _deadline = new CancellationTokenSource();
                _deadline.Cancel();
            }
            else if (delay < MaxDelay)
            {
                _deadline = new CancellationTokenSource(delay, clock);
            }
        }

        var telemetry = new RunTelemetry(threadId, options, mode, admission?.Waited);
        _lease = options.Lease is { } lease ? new RunLease(lease, clock, telemetry) : null;
        _source = CancellationTokenSource.CreateLinkedTokenSource(
            caller, _deadline?.Token ?? CancellationToken.None, _lease?.Lost ?? CancellationToken.None);

        Level = new LevelScope(
            new RunScope(threadId, options, _source.Token, caller, telemetry, _lease, durable), _source.Token, Prefix: null);
    }

    public LevelScope Level { get; }

    public RunTelemetry Telemetry => Level.Run.Telemetry;

    /// <summary>
    /// Starts a run: its token tree, and its telemetry, whose span becomes current. The caller has
    /// validated <paramref name="options"/> (<see cref="RunMetadata.Validate"/>). <paramref name="policy"/> is
    /// the outermost graph's (<c>design/33-Durable-Execution.md</c>). <paramref name="admission"/> is the run's
    /// slot, when it has a scheduler (<c>design/40-Run-Scheduler.md</c>).
    /// </summary>
    public static RootScope Start(
        string threadId,
        RunOptions? options,
        CancellationToken caller,
        RunMode mode,
        CheckpointPolicy policy,
        Admission? admission = null) =>
        new(threadId, options ?? NoOptions, caller, mode, policy == CheckpointPolicy.EveryNode, admission);

    public void Dispose()
    {
        _source.Dispose();
        _deadline?.Dispose();
        _lease?.Dispose();
    }
}

/// <summary>Wraps the handler shapes callers add into a <see cref="NodeHandler{TState}"/>.</summary>
internal static class NodeHandlers
{
    /// <summary>
    /// A sync handler runs on the thread pool, not on the executor, so the engine can stop waiting for
    /// it when its token fires (it can't be stopped, only abandoned).
    /// </summary>
    public static NodeHandler<TState> FromSync<TState>(Func<TState, TState> handler) =>
        (state, _) => new ValueTask<NodeStep<TState>>(
            Task.Run<NodeStep<TState>>(() => new NodeStep<TState>.Done(handler(state))));

    public static NodeHandler<TState> FromAsync<TState>(Func<TState, NodeContext, CancellationToken, ValueTask<TState>> handler) =>
        async (state, inputs) =>
            new NodeStep<TState>.Done(await handler(state, inputs.Context, inputs.Token).ConfigureAwait(false));

    /// <summary>
    /// Makes sure an abandoned task's eventual exception is observed, so it doesn't surface as
    /// <see cref="TaskScheduler.UnobservedTaskException"/>.
    /// </summary>
    public static void Abandon(Task task) =>
        _ = task.ContinueWith(
            static t => _ = t.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
}
