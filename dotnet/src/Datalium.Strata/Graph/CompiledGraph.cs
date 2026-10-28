using Datalium.Strata.Interruption;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Graph;

public sealed partial class CompiledGraph<TState>
{
    public GraphTopology Topology { get; }

    /// <summary>
    /// The loop circuit breaker it was compiled with (<see cref="CompilationOptions.MaxNodeHops"/>), or
    /// <see langword="null"/> for none. <see cref="RunOptions.MaxNodeHops"/> replaces it for one run.
    /// </summary>
    public int? MaxNodeHops { get; }

    /// <summary>
    /// The identity it was compiled with (<see cref="CompilationOptions.GraphId"/> /
    /// <see cref="CompilationOptions.GraphVersion"/>, <c>design/31-Graph-Identity.md</c>), written on every
    /// checkpoint level it writes; <see langword="null"/> for none.
    /// </summary>
    public GraphIdentity? Identity { get; init; }

    /// <summary>
    /// When a run of this graph writes checkpoints (<see cref="CompilationOptions.Checkpointing"/>,
    /// <c>design/33-Durable-Execution.md</c>). As the outermost graph of a run it decides for every level.
    /// </summary>
    public CheckpointPolicy Checkpointing { get; init; }

    /// <summary>What accepts a checkpoint level another version of this graph wrote (<see cref="CompilationOptions.Migrate"/>).</summary>
    internal Func<Checkpoint, Checkpoint?>? Migration { get; init; }

    private readonly IReadOnlyDictionary<string, NodeEntry<TState>> _nodes;
    private readonly IReadOnlyDictionary<string, RecoveryPolicy> _recoveryPolicies;
    private readonly IReadOnlyDictionary<string, TimeSpan> _timeouts;
    private readonly IReadOnlySet<string> _interruptBoundaries;
    private readonly IReadOnlyDictionary<string, string> _rejectRoutes;
    // Same indexing scheme as StateGraph's `_edgeConditions` — parallel to `Topology.Edges`.
    private readonly IReadOnlyList<Func<TState, bool>?> _edgeConditions;
    private static readonly IReadOnlyDictionary<string, WaitSpec<TState>> NoWaits = new Dictionary<string, WaitSpec<TState>>();
    private static readonly IReadOnlyDictionary<string, TimeSpan> NoTimeouts = new Dictionary<string, TimeSpan>();
    private static readonly IReadOnlyDictionary<string, string> NoRoutes = new Dictionary<string, string>();

    /// <summary>Wait nodes and what they wait for (<c>design/29-Event-Waits-And-Timers.md</c>).</summary>
    internal IReadOnlyDictionary<string, WaitSpec<TState>> Waits { get; init; } = NoWaits;

    /// <summary>Interrupt boundaries with a timeout, and how long they wait.</summary>
    internal IReadOnlyDictionary<string, TimeSpan> BoundaryTimeouts { get; init; } = NoTimeouts;

    /// <summary>Boundaries and event waits with a timeout route, and where a timeout continues.</summary>
    internal IReadOnlyDictionary<string, string> TimeoutRoutes { get; init; } = NoRoutes;

    internal CompiledGraph(
        GraphTopology topology,
        IReadOnlyDictionary<string, NodeEntry<TState>> nodes,
        IReadOnlyDictionary<string, RecoveryPolicy> recoveryPolicies,
        IReadOnlyDictionary<string, TimeSpan> timeouts,
        IReadOnlySet<string> interruptBoundaries,
        IReadOnlyDictionary<string, string> rejectRoutes,
        IReadOnlyList<Func<TState, bool>?> edgeConditions,
        int? maxNodeHops)
    {
        Topology = topology;
        MaxNodeHops = maxNodeHops;
        _nodes = nodes;
        _recoveryPolicies = recoveryPolicies;
        _timeouts = timeouts;
        _interruptBoundaries = interruptBoundaries;
        _rejectRoutes = rejectRoutes;
        _edgeConditions = edgeConditions;
    }

    /// <summary>
    /// The graph's structure as <c>strata graph validate</c> sees it — the same output
    /// <see cref="StateGraph{TState}.ToTopology"/> gives for the builder this came from. Prefer this
    /// to <see cref="Topology"/> for export: that holds only the ordinary edges the executor routes
    /// over, so a node reachable only through a recovery policy's fallback, or an interrupt
    /// boundary's reject route, would look like an orphan.
    /// </summary>
    public GraphTopology ToTopology()
    {
        var topology = Topology
            .WithFallbackEdges(_recoveryPolicies)
            .WithRejectEdges(_rejectRoutes)
            .WithTimeoutEdges(TimeoutRoutes);
        return topology with { Identity = Identity };
    }

    /// <summary>
    /// Whether this graph takes a thread whose latest checkpoint is <paramref name="checkpoint"/>
    /// (<c>design/31-Graph-Identity.md</c>): its outermost level has no identity (written before identities, or
    /// by a graph without one), or this graph's id with this graph's version, or with any version when this
    /// graph has a migration (which may still refuse it). What the decision worker asks before resuming.
    /// </summary>
    public bool Owns(Checkpoint checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        return checkpoint.Graph is not { } stored
            || (Identity is { } own && own.Id == stored.Id && (own.Version == stored.Version || Migration is not null));
    }

    /// <summary>
    /// <paramref name="checkpoint"/> as this graph takes it, at this level and, through each subgraph node's
    /// child, every level nested below (<c>design/31-Graph-Identity.md</c>, "The rule"): a level with no identity,
    /// or this graph's, as it is; another version's through the migration, which gets this graph's identity and
    /// keeps its thread id; anything else refused, with why. <paramref name="threadId"/> is the outermost
    /// thread and <paramref name="path"/> this level (<c>[]</c> at the top), for messages. A nested level under a
    /// node that isn't a subgraph is left for the resume check to report.
    /// </summary>
    internal Adopted Adopt(Checkpoint checkpoint, string threadId, IReadOnlyList<string> path)
    {
        var level = checkpoint;
        if (checkpoint.Graph is { } stored && stored != Identity)
        {
            var at = path.Count == 0 ? string.Empty : $" at level [{string.Join('/', path)}]";
            var written = $"thread '{threadId}'{at} was written by graph {stored}";
            if (Identity is not { } own)
            {
                return Adopted.Refused($"{written}, and this graph has no identity");
            }

            if (own.Id != stored.Id)
            {
                return Adopted.Refused($"{written}, not {own}");
            }

            var versions = $"{written}, not {GraphIdentity.VersionText(own.Version)}";
            if (Migration is not { } migrate)
            {
                return Adopted.Refused($"{versions}, and this graph has no migration");
            }

            if (migrate(checkpoint) is not { } migrated)
            {
                return Adopted.Refused($"{versions}, and this graph's migration refused it");
            }

            level = CheckpointSchema.Versioned(migrated with { ThreadId = checkpoint.ThreadId, Graph = own });
        }

        if (level.Nested is not { } child
            || level.NextNode is not { } node
            || !_nodes.TryGetValue(node, out var entry)
            || entry.Adopt is not { } adoptChild)
        {
            return new Adopted(level, Problem: null);
        }

        var inner = adoptChild(child, threadId, [.. path, node]);
        if (inner.Problem is not null)
        {
            return inner;
        }

        return ReferenceEquals(inner.Checkpoint, child)
            ? new Adopted(level, Problem: null)
            : new Adopted(CheckpointSchema.Versioned(level with { Nested = inner.Checkpoint }), Problem: null);
    }

    /// <summary>
    /// Whether a run of this graph needs a <see cref="StreamContext"/>: it has a streaming node, or a
    /// fallible node that does (a parallel group with a streaming branch, or a subgraph whose own
    /// child streams).
    /// </summary>
    internal bool NeedsStream() => _nodes.Values.Any(entry => entry.NeedsStream);

    /// <summary>
    /// The first interrupt boundary or wait node (<c>design/29-Event-Waits-And-Timers.md</c>) a run of this
    /// graph could freeze at, in node order, qualified with the subgraph nodes it is inside
    /// (<c>"Review/Approve"</c>), or <see langword="null"/> for none. A boundary name that isn't a node never
    /// triggers, so it isn't one. A map can't run a graph with one (<c>design/27-Map-Fan-Out.md</c>).
    /// </summary>
    internal Suspension? InterruptBoundary()
    {
        foreach (var node in Topology.Nodes)
        {
            if (_interruptBoundaries.Contains(node))
            {
                return new Suspension(node, IsWait: false);
            }

            if (Waits.ContainsKey(node))
            {
                return new Suspension(node, IsWait: true);
            }

            if (_nodes.TryGetValue(node, out var entry) && entry.Boundary is { } inner)
            {
                return inner.Under(node);
            }
        }

        return null;
    }

    /// <summary>
    /// The <c>wait</c> a checkpoint frozen after <paramref name="node"/> carries
    /// (<c>design/29-Event-Waits-And-Timers.md</c>): a wait node's, or a timed boundary's (kind
    /// <c>human</c>); <see langword="null"/> for a boundary with no timeout.
    /// </summary>
    private CheckpointWait? WaitAfter(string node, TState state, DateTimeOffset now) =>
        Waits.TryGetValue(node, out var wait) ? wait.At(state, now)
        : BoundaryTimeouts.TryGetValue(node, out var timeout)
            ? new CheckpointWait(WaitKind.Human, EventType: null, WakeTimes.Text(now + timeout))
            : null;

    /// <summary>
    /// Runs the graph from its entry point inside another run, as a subgraph does
    /// (<see cref="SubgraphRunner"/>): <paramref name="level"/> is the child's level of that run, and
    /// <paramref name="stream"/> the child's scoped context (<see cref="StreamContext.Scoped"/>) when
    /// the run it is inside is streaming.
    /// </summary>
    internal Task<GraphExecutionResult<TState>> RunInlineAsync(
        string threadId, TState initialState, ICheckpointStore store, StreamContext? stream, LevelScope level) =>
        ExecuteFromAsync(threadId, Topology.EntryPoint, initialState, store, stream, level);

    private enum NextNodeLookup
    {
        /// <summary>No outgoing edges at all — the node is a terminal node.</summary>
        Terminal,

        /// <summary>An edge matched; its target is returned via the <c>out</c> parameter.</summary>
        Resolved,

        /// <summary>Outgoing edges exist but none of their conditions matched the given state.</summary>
        Unmatched,
    }

    /// <summary>
    /// Picks the edge to follow out of <paramref name="node"/>, evaluating conditional edges'
    /// predicates against <paramref name="state"/> in declaration order — the first match wins,
    /// so a plain <see cref="StateGraph{TState}.AddEdge"/> added after one or more
    /// <see cref="StateGraph{TState}.AddConditionalEdge"/> calls acts as a default "else" branch.
    /// <see cref="NextNodeLookup.Unmatched"/> means the node has outgoing edges but none of their
    /// conditions matched — a graph author's routing gap that <see cref="StateGraph{TState}.Compile"/>
    /// can't catch since it depends on runtime state rather than graph shape.
    /// </summary>
    private NextNodeLookup NextNodeAfter(string node, TState state, out string? next)
    {
        var hasOutgoing = false;
        for (var i = 0; i < Topology.Edges.Count; i++)
        {
            var edge = Topology.Edges[i];
            if (edge.From != node)
            {
                continue;
            }

            hasOutgoing = true;
            var condition = _edgeConditions[i];
            if (condition is null || condition(state))
            {
                next = edge.To;
                return NextNodeLookup.Resolved;
            }
        }

        next = null;
        return hasOutgoing ? NextNodeLookup.Unmatched : NextNodeLookup.Terminal;
    }
}
