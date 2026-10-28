using System.Text.Json.Serialization;

namespace Datalium.Strata.Graph;

/// <summary>
/// Lightweight, engine-internal adjacency view of a <see cref="StateGraph{TState}"/> used for
/// compile-time validation. Deliberately distinct from
/// <c>Datalium.Strata.Storage.Schema.GraphDefinition</c> (the cross-language interop shape) —
/// this project has no dependency on Datalium.Strata.Storage, so the mapping between the two is
/// Storage's job (<c>GraphDefinitionExport.ToDefinition</c>, and <c>ToTopology</c> back).
/// <see cref="Details"/> adds node metadata and the inside of parallel groups and subgraphs
/// (<c>design/22-Graph-Structure-In-Definitions.md</c>), which the validator recurses into.
/// </summary>
public sealed record GraphTopology(string EntryPoint, IReadOnlyList<string> Nodes, IReadOnlyList<GraphEdge> Edges)
{
    private static readonly IReadOnlyDictionary<string, GraphNodeDetail> NoDetails =
        new Dictionary<string, GraphNodeDetail>();

    /// <summary>
    /// What the topology knows about a node beyond its name, keyed by node name
    /// (<c>design/22-Graph-Structure-In-Definitions.md</c>): its metadata, and for a parallel group or
    /// subgraph, its branches or child topology. A plain node with no metadata has no entry, and an
    /// entry for a name that isn't in <see cref="Nodes"/> is ignored.
    /// </summary>
    public IReadOnlyDictionary<string, GraphNodeDetail> Details { get; init; } = NoDetails;

    /// <summary>
    /// The identity the graph was compiled with (<c>design/31-Graph-Identity.md</c>), exported as the
    /// definition's <c>identity</c>; <see langword="null"/> for a builder's topology, or a graph without one.
    /// </summary>
    public Interruption.GraphIdentity? Identity { get; init; }

    /// <summary>
    /// The detail for <paramref name="node"/>, or <see langword="null"/> for a plain node with no
    /// metadata.
    /// </summary>
    public GraphNodeDetail? DetailOf(string node) => Details.GetValueOrDefault(node);

    /// <summary>
    /// This topology plus one guarded <c>on_failure</c> edge per recovery-policy fallback, in node
    /// declaration order (not dictionary order, so the export is deterministic). The fallback route
    /// is only taken on a runtime failure, so it is not part of the ordinary edges the executor
    /// routes over, but validation and the exported definition both need it: without it a node
    /// reachable only through a fallback would be reported as an orphan.
    /// </summary>
    internal GraphTopology WithFallbackEdges(IReadOnlyDictionary<string, RecoveryPolicy> policies)
    {
        var edges = new List<GraphEdge>(Edges);
        foreach (var node in Nodes)
        {
            if (policies.TryGetValue(node, out var policy) && policy.Fallback is { } fallback)
            {
                edges.Add(new GraphEdge(node, fallback, Guarded: true, Condition: "on_failure"));
            }
        }

        return this with { Edges = edges };
    }

    /// <summary>
    /// This topology plus one guarded <c>on_reject</c> edge per interrupt boundary with a reject route
    /// (<c>design/21-Typed-Human-Signals.md</c>): in node declaration order, then, for a route on a
    /// boundary name that isn't a node (which validation reports as a dangling edge), in ordinal order.
    /// Like a fallback, the route is taken only by a resume, never by ordinary routing.
    /// </summary>
    internal GraphTopology WithRejectEdges(IReadOnlyDictionary<string, string> routes)
    {
        if (routes.Count == 0)
        {
            return this;
        }

        var edges = new List<GraphEdge>(Edges);
        var sources = Nodes.Where(routes.ContainsKey)
            .Concat(routes.Keys.Where(boundary => !Nodes.Contains(boundary)).Order(StringComparer.Ordinal));
        foreach (var boundary in sources)
        {
            edges.Add(new GraphEdge(boundary, routes[boundary], Guarded: true, Condition: "on_reject"));
        }

        return this with { Edges = edges };
    }

    /// <summary>
    /// This topology plus one guarded <c>on_timeout</c> edge per wait with a timeout route
    /// (<c>design/29-Event-Waits-And-Timers.md</c>): an interrupt boundary's or an event wait's, in node
    /// declaration order, then, for a route on a boundary name that isn't a node, in ordinal order. The
    /// route is taken only when the wait times out, never by ordinary routing.
    /// </summary>
    internal GraphTopology WithTimeoutEdges(IReadOnlyDictionary<string, string> routes)
    {
        if (routes.Count == 0)
        {
            return this;
        }

        var edges = new List<GraphEdge>(Edges);
        var sources = Nodes.Where(routes.ContainsKey)
            .Concat(routes.Keys.Where(boundary => !Nodes.Contains(boundary)).Order(StringComparer.Ordinal));
        foreach (var source in sources)
        {
            edges.Add(new GraphEdge(source, routes[source], Guarded: true, Condition: "on_timeout"));
        }

        return this with { Edges = edges };
    }
}

/// <summary>What a composite node is (<c>design/22-Graph-Structure-In-Definitions.md</c>). A plain node has no kind.</summary>
public enum GraphNodeKind
{
    /// <summary>A parallel group: <see cref="GraphNodeDetail.Branches"/> holds its branches.</summary>
    [JsonStringEnumMemberName("parallel_group")]
    ParallelGroup,

    /// <summary>A subgraph: <see cref="GraphNodeDetail.Graph"/> holds the child's topology.</summary>
    [JsonStringEnumMemberName("subgraph")]
    Subgraph,

    /// <summary>
    /// A map (<c>design/27-Map-Fan-Out.md</c>): <see cref="GraphNodeDetail.Graph"/> holds the topology of
    /// the child each item runs, its template.
    /// </summary>
    [JsonStringEnumMemberName("map")]
    Map,

    /// <summary>
    /// A wait node (<c>design/29-Event-Waits-And-Timers.md</c>): <see cref="GraphNodeDetail.Wait"/> says
    /// what it waits for.
    /// </summary>
    [JsonStringEnumMemberName("wait")]
    Wait,
}

/// <summary>
/// A node's structure beyond its name (<c>design/22-Graph-Structure-In-Definitions.md</c>). A parallel
/// group has <see cref="Kind"/> <see cref="GraphNodeKind.ParallelGroup"/> and <see cref="Branches"/>; a
/// subgraph has <see cref="GraphNodeKind.Subgraph"/> and <see cref="Graph"/>, the child's whole
/// topology (fallback and reject edges included), recursively; a map has <see cref="GraphNodeKind.Map"/>
/// and its items' child as <see cref="Graph"/> (<c>design/27-Map-Fan-Out.md</c>). A plain node has neither.
/// </summary>
public sealed record GraphNodeDetail
{
    /// <summary>Description only, set with <c>SetNodeMetadata</c>; the engine never reads it.</summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    public GraphNodeKind? Kind { get; init; }

    /// <summary>A parallel group's branches, in declaration order.</summary>
    public IReadOnlyList<GraphBranch>? Branches { get; init; }

    /// <summary>A subgraph's child, or a map's template.</summary>
    public GraphTopology? Graph { get; init; }

    /// <summary>
    /// A parallel group's or a map's join, when it isn't the default (<c>All</c>, failing the group)
    /// (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>).
    /// </summary>
    public GraphJoin? Join { get; init; }

    /// <summary>A wait node's wait (<c>design/29-Event-Waits-And-Timers.md</c>).</summary>
    public GraphWait? Wait { get; init; }

    /// <summary>
    /// Set when the node is an interrupt boundary, of any kind but a wait
    /// (<c>design/48-Boundaries-In-Definitions.md</c>): the run freezes after it until a human answers.
    /// </summary>
    public GraphInterrupt? Interrupt { get; init; }
}

/// <summary>
/// An interrupt boundary as the definition exports it (<c>design/48-Boundaries-In-Definitions.md</c>): its
/// timeout in whole milliseconds, or <see langword="null"/> for none. Its reject and timeout routes are
/// edges (<c>on_reject</c>, <c>on_timeout</c>), not repeated here.
/// </summary>
public sealed record GraphInterrupt(long? TimeoutMs);

/// <summary>
/// A wait node as the definition exports it (<c>design/29-Event-Waits-And-Timers.md</c>): what it waits for
/// (<see cref="Interruption.WaitKind.Event"/> or <see cref="Interruption.WaitKind.Timer"/>), an event's type,
/// a timer's fixed delay (<see langword="null"/> when it is computed from the state), and an event wait's
/// timeout, in milliseconds.
/// </summary>
public sealed record GraphWait(Interruption.WaitKind Kind, string? EventType, long? DelayMs, long? TimeoutMs);

/// <summary>
/// A parallel group's join as the definition exports it (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>):
/// the policy, a quorum's <paramref name="Count"/>, a <c>FirstMatching</c> predicate's description as
/// <paramref name="Condition"/>, and what an unmet join does. The predicate itself isn't exported.
/// </summary>
public sealed record GraphJoin(JoinKind Policy, int? Count, string? Condition, PartialResults Partial);

/// <summary>One branch of a parallel group. Branches have no edges: they all start when the group starts.</summary>
public sealed record GraphBranch(string Name, IReadOnlyDictionary<string, string>? Metadata = null);

/// <param name="Condition">
/// Descriptive text for the exported definition only (validation ignores it). The engine records
/// no text for an edge predicate, so this is set only on the synthetic edges: <c>on_failure</c> for a
/// recovery-policy fallback, <c>on_reject</c> for an interrupt boundary's reject route, <c>on_timeout</c> for
/// a wait's timeout route.
/// </param>
public readonly record struct GraphEdge(string From, string To, bool Guarded, string? Condition = null);
