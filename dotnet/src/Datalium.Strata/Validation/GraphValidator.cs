using Datalium.Strata.Graph;

namespace Datalium.Strata.Validation;

/// <summary>
/// Runs Tarjan's Strongly Connected Components algorithm over a <see cref="GraphTopology"/> to
/// catch the structural error classes STRATA-SPEC-004 calls out: a missing entry point,
/// orphan (unreachable) nodes, and unguarded cycles, plus dangling edges (an edge naming something
/// that is not a node) and nodes with no path to an end. Mirrors
/// <c>rust/strata-core/src/validate.rs</c> so a graph accepted/rejected by one engine is
/// accepted/rejected by the other.
///
/// It recurses (<c>design/22-Graph-Structure-In-Definitions.md</c>): each parallel group must have at
/// least one branch, with unique names, no node or branch name may contain <c>/</c> (the separator in a
/// node's path), and each subgraph's child topology is validated as a graph in
/// its own right, its errors naming nodes by their qualified path (<c>Remediate/Abort</c>), the path
/// run events use.
/// </summary>
public static class GraphValidator
{
    /// <summary>
    /// The <c>Condition</c> of the synthetic edges the engines export for a reject route, a timeout route and
    /// a recovery-policy fallback. A node finishes by its other edges, so one whose only edges are these
    /// ends the run when it finishes.
    /// </summary>
    private static readonly HashSet<string> RouteConditions = ["on_reject", "on_timeout", "on_failure"];

    /// <summary>
    /// Every error in <paramref name="topology"/>, at every level, in this order: the level's own (entry
    /// point, names with <c>/</c>, dangling edges, orphans, unguarded cycles, nodes with no path to an end), then its groups' in node order, then each
    /// subgraph child's, in node order. A level whose entry point is missing reports only that, as
    /// before.
    /// </summary>
    public static ValidationResult Validate(GraphTopology topology)
    {
        var result = new ValidationResult();
        ValidateLevel(topology, prefix: string.Empty, inMap: false, result);
        return result;
    }

    /// <summary>
    /// Validates one level. <paramref name="prefix"/> qualifies its node names with the path of the
    /// subgraph node it is the child of (<c>"Remediate/"</c>); the top level's is empty, so its errors
    /// read exactly as they always have. <paramref name="inMap"/>: the level is inside a map's template, at any
    /// depth, where a wait node is an error (<c>design/29-Event-Waits-And-Timers.md</c>).
    /// </summary>
    private static void ValidateLevel(GraphTopology topology, string prefix, bool inMap, ValidationResult result)
    {
        string Q(string name) => prefix + name;

        var nodeNames = new HashSet<string>(topology.Nodes);
        if (!nodeNames.Contains(topology.EntryPoint))
        {
            var entry = Q(topology.EntryPoint);
            result.Errors.Add(new ValidationError(
                ValidationErrorKind.MissingEntryPoint,
                "STRATA_ERR_MISSING_ENTRY",
                entry,
                null,
                $"Entry point node '{entry}' does not exist in the graph definition."));
            return;
        }

        AddNameErrors(topology, prefix, result);
        AddDanglingEdgeErrors(topology, nodeNames, Q, result);

        var adjacency = BuildAdjacency(topology, nodeNames);

        var reachable = AddOrphanErrors(topology, adjacency, Q, result);
        AddUnguardedCycleErrors(topology, adjacency, Q, result);
        AddNoTerminalErrors(topology, adjacency, reachable, Q, result);
        AddGroupErrors(topology, Q, result);
        if (inMap)
        {
            AddWaitInMapErrors(topology, Q, result);
            AddBoundaryInMapErrors(topology, Q, result);
        }

        // A map's template runs once per item, so its nodes are named with a `*` for the item
        // (design/27-Map-Fan-Out.md).
        foreach (var node in topology.Nodes)
        {
            if (topology.DetailOf(node) is { Graph: { } child } detail)
            {
                var map = detail.Kind == GraphNodeKind.Map;
                ValidateLevel(child, Q(node) + (map ? "/*/" : "/"), inMap || map, result);
            }
        }
    }

    /// <summary>
    /// What a name with <c>/</c> in it is told: <c>/</c> separates the names in a node's path, so <c>G/b</c>
    /// could be a node or branch <c>b</c> of group <c>G</c>.
    /// </summary>
    private const string Slash = "'/' separates the names in a node's path";

    /// <summary>
    /// Every node of a level whose name contains <c>/</c>, in node order. A map item's key can't either, but
    /// it comes from the state, so the map checks it when it runs (<c>design/27-Map-Fan-Out.md</c>).
    /// </summary>
    private static void AddNameErrors(GraphTopology topology, string prefix, ValidationResult result)
    {
        var at = prefix.Length == 0 ? string.Empty : $" in '{prefix[..^1]}'";
        foreach (var node in topology.Nodes.Where(node => node.Contains('/', StringComparison.Ordinal)))
        {
            result.Errors.Add(new ValidationError(
                ValidationErrorKind.InvalidName,
                "STRATA_ERR_INVALID_NAME",
                prefix + node,
                null,
                $"Node name '{node}'{at} contains '/'; {Slash}."));
        }
    }

    /// <summary>
    /// Every wait node of a level inside a map's template, in node order: a map item can't freeze
    /// (<c>design/29-Event-Waits-And-Timers.md</c>).
    /// </summary>
    private static void AddWaitInMapErrors(GraphTopology topology, Func<string, string> q, ValidationResult result)
    {
        foreach (var node in topology.Nodes.Where(node => topology.DetailOf(node) is { Kind: GraphNodeKind.Wait }))
        {
            var wait = q(node);
            result.Errors.Add(new ValidationError(
                ValidationErrorKind.WaitInMap,
                "STRATA_ERR_WAIT_IN_MAP",
                wait,
                null,
                $"Wait node '{wait}' is inside a map's template; a map item can't wait."));
        }
    }

    /// <summary>
    /// Every interrupt boundary of a level inside a map's template, in node order: a map item can't freeze
    /// (<c>design/48-Boundaries-In-Definitions.md</c>).
    /// </summary>
    private static void AddBoundaryInMapErrors(GraphTopology topology, Func<string, string> q, ValidationResult result)
    {
        foreach (var node in topology.Nodes.Where(node => topology.DetailOf(node) is { Interrupt: not null }))
        {
            var boundary = q(node);
            result.Errors.Add(new ValidationError(
                ValidationErrorKind.BoundaryInMap,
                "STRATA_ERR_BOUNDARY_IN_MAP",
                boundary,
                null,
                $"Interrupt boundary '{boundary}' is inside a map's template; a map item can't freeze."));
        }
    }

    /// <summary>
    /// For each parallel group, in node order: no branches at all, or one error per branch name used
    /// more than once (in the order each first repeats). For each map, a quorum below 1.
    /// </summary>
    private static void AddGroupErrors(GraphTopology topology, Func<string, string> q, ValidationResult result)
    {
        foreach (var node in topology.Nodes)
        {
            if (topology.DetailOf(node) is { Kind: GraphNodeKind.Map } map)
            {
                AddMapErrors(q(node), map, result);
                continue;
            }

            if (topology.DetailOf(node) is not { Kind: GraphNodeKind.ParallelGroup } detail)
            {
                continue;
            }

            var group = q(node);
            var branches = detail.Branches ?? [];
            if (branches.Count == 0)
            {
                result.Errors.Add(new ValidationError(
                    ValidationErrorKind.EmptyGroup,
                    "STRATA_ERR_EMPTY_GROUP",
                    group,
                    null,
                    $"Parallel group '{group}' has no branches."));
                continue;
            }

            AddBranchErrors(group, branches, result);

            // design/26-Parallel-Group-Controls-And-Joins.md: a quorum must be reachable.
            if (detail.Join is { Policy: JoinKind.Quorum } join
                && (join.Count is not { } count || count < 1 || count > branches.Count))
            {
                var stated = join.Count?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "none";
                result.Errors.Add(new ValidationError(
                    ValidationErrorKind.InvalidQuorum,
                    "STRATA_ERR_INVALID_QUORUM",
                    group,
                    null,
                    $"Parallel group '{group}' has a quorum of {stated}, which isn't between 1 and its {branches.Count} branches."));
            }
        }
    }

    /// <summary>
    /// A group's branches, in order: a name with <c>/</c> where it first appears, and one error per name used
    /// more than once, where it first repeats.
    /// </summary>
    private static void AddBranchErrors(string group, IReadOnlyList<GraphBranch> branches, ValidationResult result)
    {
        var seen = new HashSet<string>();
        var reported = new HashSet<string>();
        foreach (var name in branches.Select(branch => branch.Name))
        {
            var first = seen.Add(name);
            if (first && name.Contains('/', StringComparison.Ordinal))
            {
                result.Errors.Add(new ValidationError(
                    ValidationErrorKind.InvalidName,
                    "STRATA_ERR_INVALID_NAME",
                    group,
                    name,
                    $"Parallel group '{group}' has a branch named '{name}', which contains '/'; {Slash}."));
            }

            if (!first && reported.Add(name))
            {
                result.Errors.Add(new ValidationError(
                    ValidationErrorKind.DuplicateBranch,
                    "STRATA_ERR_DUPLICATE_BRANCH",
                    group,
                    name,
                    $"Parallel group '{group}' has more than one branch named '{name}'."));
            }
        }
    }

    /// <summary>
    /// A map's quorum must be at least 1 (<c>design/27-Map-Fan-Out.md</c>). Its items are counted at run
    /// time, so there is no upper bound here: a quorum above them is a join that can't be met.
    /// </summary>
    private static void AddMapErrors(string map, GraphNodeDetail detail, ValidationResult result)
    {
        if (detail.Join is { Policy: JoinKind.Quorum } join && join.Count is not >= 1)
        {
            var stated = join.Count?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "none";
            result.Errors.Add(new ValidationError(
                ValidationErrorKind.InvalidQuorum,
                "STRATA_ERR_INVALID_QUORUM",
                map,
                null,
                $"Map '{map}' has a quorum of {stated}, which isn't at least 1."));
        }
    }

    /// <summary>
    /// One error per edge with an endpoint that is not a node, in edge order. <c>Node</c> is the
    /// edge's <c>From</c> and <c>RelatedNode</c> its <c>To</c>, as for a cycle.
    /// </summary>
    private static void AddDanglingEdgeErrors(
        GraphTopology topology,
        HashSet<string> nodeNames,
        Func<string, string> q,
        ValidationResult result)
    {
        foreach (var edge in topology.Edges)
        {
            var fromExists = nodeNames.Contains(edge.From);
            var toExists = nodeNames.Contains(edge.To);
            if (fromExists && toExists)
            {
                continue;
            }

            var (from, to) = (q(edge.From), q(edge.To));
            var message = (fromExists, toExists) switch
            {
                (true, false) => $"Edge '{from}' -> '{to}' points at '{to}', which is not a node in the graph.",
                (false, true) => $"Edge '{from}' -> '{to}' starts at '{from}', which is not a node in the graph.",
                _ => $"Edge '{from}' -> '{to}' connects '{from}' and '{to}', neither of which is a node in the graph.",
            };
            result.Errors.Add(new ValidationError(
                ValidationErrorKind.DanglingEdge,
                "STRATA_ERR_DANGLING_EDGE",
                from,
                to,
                message));
        }
    }

    /// <summary>
    /// Only edges between two real nodes are followed by the passes after the dangling-edge check,
    /// so an edge that dangles is reported once and can't also make a name that is not a node look
    /// reachable or part of a cycle.
    /// </summary>
    private static Dictionary<string, List<GraphEdge>> BuildAdjacency(GraphTopology topology, HashSet<string> nodeNames)
    {
        var adjacency = topology.Nodes.ToDictionary(n => n, _ => new List<GraphEdge>());
        foreach (var edge in topology.Edges)
        {
            if (nodeNames.Contains(edge.From) && nodeNames.Contains(edge.To))
            {
                adjacency[edge.From].Add(edge);
            }
        }

        return adjacency;
    }

    /// <summary>One error per node the entry point can't reach, in node order. Returns the nodes it can reach.</summary>
    private static HashSet<string> AddOrphanErrors(
        GraphTopology topology,
        Dictionary<string, List<GraphEdge>> adjacency,
        Func<string, string> q,
        ValidationResult result)
    {
        var visited = new HashSet<string>();
        var stack = new Stack<string>();
        stack.Push(topology.EntryPoint);

        while (stack.Count > 0)
        {
            var node = stack.Pop();
            if (!visited.Add(node))
            {
                continue;
            }

            foreach (var edge in adjacency.GetValueOrDefault(node, []))
            {
                if (!visited.Contains(edge.To))
                {
                    stack.Push(edge.To);
                }
            }
        }

        foreach (var node in topology.Nodes)
        {
            if (!visited.Contains(node))
            {
                var (name, entry) = (q(node), q(topology.EntryPoint));
                result.Errors.Add(new ValidationError(
                    ValidationErrorKind.OrphanNode,
                    "STRATA_ERR_ORPHAN",
                    name,
                    null,
                    $"Node '{name}' is isolated and unreachable from the entry point '{entry}'."));
            }
        }

        return visited;
    }

    /// <summary>
    /// One error per node reachable from the entry point that has no path to an end, in node order
    /// (<c>design/Arch-Specs/STRATA-SPEC-004.md</c>). A run ends at a node with no outgoing edge but routes
    /// (<see cref="RouteConditions"/>), so those are the ends; a walk back from them along every edge,
    /// guarded, plain and routes alike, finds every node that can reach one. An orphan is reported as one
    /// already, so not again here. It's a structural check: it proves a node <i>can</i> reach an end, not
    /// that a run will, so the hop limit stays the guard for a loop whose exit never matches.
    /// </summary>
    private static void AddNoTerminalErrors(
        GraphTopology topology,
        Dictionary<string, List<GraphEdge>> adjacency,
        HashSet<string> reachable,
        Func<string, string> q,
        ValidationResult result)
    {
        var reverse = new Dictionary<string, List<string>>();
        foreach (var edge in adjacency.Values.SelectMany(edges => edges))
        {
            if (!reverse.TryGetValue(edge.To, out var sources))
            {
                reverse[edge.To] = sources = [];
            }

            sources.Add(edge.From);
        }

        var ends = new HashSet<string>();
        var stack = new Stack<string>(topology.Nodes.Where(node =>
            adjacency[node].All(edge => edge.Condition is { } condition && RouteConditions.Contains(condition))));
        while (stack.Count > 0)
        {
            var node = stack.Pop();
            if (!ends.Add(node))
            {
                continue;
            }

            foreach (var source in reverse.GetValueOrDefault(node, []))
            {
                if (!ends.Contains(source))
                {
                    stack.Push(source);
                }
            }
        }

        foreach (var node in topology.Nodes)
        {
            if (reachable.Contains(node) && !ends.Contains(node))
            {
                var name = q(node);
                result.Errors.Add(new ValidationError(
                    ValidationErrorKind.NoTerminal,
                    "STRATA_ERR_NO_TERMINAL",
                    name,
                    null,
                    $"Node '{name}' has no path to an end: every path from it loops back without reaching a node with no outgoing edge."));
            }
        }
    }

    /// <summary>
    /// Tarjan's SCC low-link bookkeeping, run over the <b>unguarded</b> edges only: a cycle is
    /// unguarded exactly when the subgraph of unguarded edges has one, so a guarded edge is not
    /// followed at all. In that subgraph an edge to a node still <c>onStack</c> is inside a
    /// strongly connected component, so it lies on a cycle of unguarded edges, and every such
    /// cycle has at least one such edge; errors are raised the moment one is found rather than by
    /// re-scanning components afterward. (Following guarded edges too would flag a plain edge
    /// that only happens to rejoin a node of a guarded loop.)
    /// </summary>
    private static void AddUnguardedCycleErrors(
        GraphTopology topology,
        Dictionary<string, List<GraphEdge>> adjacency,
        Func<string, string> q,
        ValidationResult result)
    {
        var index = 0;
        var indices = new Dictionary<string, int>();
        var lowLinks = new Dictionary<string, int>();
        var onStack = new HashSet<string>();
        var stack = new Stack<string>();

        void StrongConnect(string node)
        {
            indices[node] = index;
            lowLinks[node] = index;
            index++;
            stack.Push(node);
            onStack.Add(node);

            var unguardedTargets = adjacency.GetValueOrDefault(node, []).Where(e => !e.Guarded).Select(e => e.To);
            foreach (var neighbor in unguardedTargets)
            {
                if (!indices.ContainsKey(neighbor))
                {
                    StrongConnect(neighbor);
                    lowLinks[node] = Math.Min(lowLinks[node], lowLinks[neighbor]);
                }
                else if (onStack.Contains(neighbor))
                {
                    lowLinks[node] = Math.Min(lowLinks[node], indices[neighbor]);

                    var (from, to) = (q(node), q(neighbor));
                    result.Errors.Add(new ValidationError(
                        ValidationErrorKind.UnguardedCycle,
                        "STRATA_ERR_UNGUARDED_CYCLE",
                        from,
                        to,
                        $"Unguarded cycle detected between '{from}' and '{to}'. Add a guarded/conditional edge to protect this loop."));
                }
            }

            if (lowLinks[node] == indices[node])
            {
                string member;
                do
                {
                    member = stack.Pop();
                    onStack.Remove(member);
                } while (member != node);
            }
        }

        foreach (var node in topology.Nodes)
        {
            if (!indices.ContainsKey(node))
            {
                StrongConnect(node);
            }
        }
    }
}
