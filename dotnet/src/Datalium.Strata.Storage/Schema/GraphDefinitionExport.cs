using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Storage.Schema;

/// <summary>
/// Maps an engine graph to <see cref="GraphDefinition"/> (<c>schema/graph-definition.schema.json</c>),
/// the file <c>strata graph validate</c> reads — the .NET twin of Rust's
/// <c>StateGraph::to_definition</c> — and a definition back to a <see cref="GraphTopology"/>, so a
/// .NET program can validate a definition file with the validator <c>Compile()</c> uses. Serialize the
/// result with the default <c>System.Text.Json</c> options and the JSON matches what the Rust engine
/// writes for the same graph (<c>schema/fixtures/graph-export/</c> pins both).
/// </summary>
public static class GraphDefinitionExport
{
    /// <summary>
    /// The builder's graph <b>without validating it</b>, so a graph <c>Compile()</c> would reject
    /// can still be written out and gated in CI. See <see cref="StateGraph{TState}.ToTopology"/>.
    /// </summary>
    public static GraphDefinition ToDefinition<TState>(this StateGraph<TState> graph) =>
        graph.ToTopology().ToDefinition();

    /// <summary>
    /// A compiled graph's structure, fallback routes included. Use this rather than mapping
    /// <see cref="CompiledGraph{TState}.Topology"/>, which leaves them out.
    /// </summary>
    public static GraphDefinition ToDefinition<TState>(this CompiledGraph<TState> graph) =>
        graph.ToTopology().ToDefinition();

    /// <summary>
    /// The interop shape of <paramref name="topology"/>, recursively: <c>schema_version</c>
    /// <see cref="GraphDefinition.BoundaryVersion"/> when a node at this level is an interrupt boundary
    /// (<c>design/48-Boundaries-In-Definitions.md</c>), else <see cref="GraphDefinition.IdentityVersion"/> when the level has an identity
    /// (<c>design/31-Graph-Identity.md</c>), else <see cref="GraphDefinition.WaitVersion"/> when a node at this
    /// level is a wait node
    /// (<c>design/29-Event-Waits-And-Timers.md</c>), else <see cref="GraphDefinition.MapVersion"/> when a node
    /// at this level is a map
    /// (<c>design/27-Map-Fan-Out.md</c>), else <see cref="GraphDefinition.JoinVersion"/> when a parallel
    /// group at this level has a join
    /// (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>), else
    /// <see cref="GraphDefinition.CompositeVersion"/> when a node at this level is a parallel group or
    /// subgraph, else <see cref="GraphDefinition.Version"/> (<c>design/22-Graph-Structure-In-Definitions.md</c>);
    /// metadata keys in ordinal order.
    /// </summary>
    public static GraphDefinition ToDefinition(this GraphTopology topology)
    {
        var nodes = topology.Nodes.Select(name => NodeDefinition(name, topology.DetailOf(name))).ToList();
        return new()
        {
            SchemaVersion = nodes.Any(node => node.Interrupt is not null) ? GraphDefinition.BoundaryVersion
                : topology.Identity is not null ? GraphDefinition.IdentityVersion
                : nodes.Any(node => node.Kind == GraphNodeKind.Wait) ? GraphDefinition.WaitVersion
                : nodes.Any(node => node.Kind == GraphNodeKind.Map) ? GraphDefinition.MapVersion
                : nodes.Any(node => node.Join is not null) ? GraphDefinition.JoinVersion
                : nodes.Any(node => node.Kind is not null) ? GraphDefinition.CompositeVersion
                : GraphDefinition.Version,
            Identity = topology.Identity is { } identity ? new Identity { Id = identity.Id, Version = identity.Version } : null,
            EntryPoint = topology.EntryPoint,
            Nodes = nodes,
            Edges =
            [
                .. topology.Edges.Select(edge => new GraphEdgeDefinition
                {
                    From = edge.From,
                    To = edge.To,
                    Guarded = edge.Guarded,
                    Condition = edge.Condition,
                }),
            ],
        };
    }

    private static GraphNodeDefinition NodeDefinition(string name, GraphNodeDetail? detail) => new()
    {
        Name = name,
        Metadata = Sorted(detail?.Metadata),
        Kind = detail?.Kind,
        Branches = detail?.Kind == GraphNodeKind.ParallelGroup
            ? [.. (detail.Branches ?? []).Select(b => new GraphBranchDefinition { Name = b.Name, Metadata = Sorted(b.Metadata) })]
            : null,
        Graph = detail?.Kind is GraphNodeKind.Subgraph or GraphNodeKind.Map ? detail.Graph?.ToDefinition() : null,
        Join = detail?.Kind is GraphNodeKind.ParallelGroup or GraphNodeKind.Map && detail.Join is { } join
            ? new GraphJoinDefinition
            {
                Policy = join.Policy,
                Count = join.Count,
                Condition = join.Condition,
                Partial = join.Partial,
            }
            : null,
        Wait = detail?.Kind == GraphNodeKind.Wait && detail.Wait is { } wait
            ? new GraphWaitDefinition
            {
                Kind = CheckpointWait.KindName(wait.Kind),
                EventType = wait.EventType,
                DelayMs = wait.DelayMs,
                TimeoutMs = wait.TimeoutMs,
            }
            : null,
        Interrupt = detail?.Kind != GraphNodeKind.Wait && detail?.Interrupt is { } interrupt
            ? new GraphInterruptDefinition { TimeoutMs = interrupt.TimeoutMs }
            : null,
    };

    private static Dictionary<string, string>? Sorted(IReadOnlyDictionary<string, string>? metadata)
    {
        if (metadata is null)
        {
            return null;
        }

        var sorted = new Dictionary<string, string>(metadata.Count);
        foreach (var key in metadata.Keys.Order(StringComparer.Ordinal))
        {
            sorted[key] = metadata[key];
        }

        return sorted;
    }

    /// <summary>
    /// <paramref name="definition"/> as a topology <see cref="Validation.GraphValidator.Validate"/> can
    /// check, recursively: the inverse of <see cref="ToDefinition(GraphTopology)"/>. Throws
    /// <see cref="FormatException"/> for what <c>strata graph validate</c> refuses as not a graph
    /// definition (exit <c>2</c>), at any level: a <c>schema_version</c> this build doesn't know, a
    /// <c>kind</c> in a <c>1.0.0</c> level, a <c>map</c> below <c>1.3.0</c>, a <c>wait</c> node below
    /// <c>1.4.0</c>, an <c>identity</c> below <c>1.5.0</c> or with an empty id, an <c>interrupt</c> below
    /// <c>1.6.0</c>, on a wait node or with a negative <c>timeout_ms</c>, a node whose <c>kind</c> doesn't
    /// match its fields, or a <c>join</c> below <c>1.2.0</c>,
    /// on anything but a parallel group or a map, or with a <c>count</c> or <c>condition</c> its policy
    /// doesn't have; and at a <c>1.6.0</c> level, an <c>on_reject</c> edge from a node that isn't an interrupt
    /// boundary, or an <c>on_timeout</c> edge from a node without a timeout
    /// (<c>design/48-Boundaries-In-Definitions.md</c>). The messages are the CLI's.
    /// </summary>
    public static GraphTopology ToTopology(this GraphDefinition definition) => ToTopology(definition, prefix: string.Empty);

    private static GraphTopology ToTopology(GraphDefinition definition, string prefix)
    {
        if (!GraphDefinition.IsSupportedVersion(definition.SchemaVersion))
        {
            var at = prefix.Length == 0 ? string.Empty : $" (in '{prefix.TrimEnd('/')}')";
            throw new FormatException(
                $"unsupported schema_version '{definition.SchemaVersion}'{at}: this build reads {GraphDefinition.Version}, {GraphDefinition.CompositeVersion}, {GraphDefinition.JoinVersion}, {GraphDefinition.MapVersion}, {GraphDefinition.WaitVersion}, {GraphDefinition.IdentityVersion} and {GraphDefinition.BoundaryVersion}");
        }

        if (definition.Identity is { } identity)
        {
            var at = prefix.Length == 0 ? string.Empty : $" (in '{prefix.TrimEnd('/')}')";
            if (!GraphDefinition.IsAtLeast(definition.SchemaVersion, GraphDefinition.IdentityVersion))
            {
                throw new FormatException(
                    $"the definition{at} has an identity, which schema_version {definition.SchemaVersion} doesn't have");
            }

            if (string.IsNullOrWhiteSpace(identity.Id))
            {
                throw new FormatException($"the definition{at} has an identity with an empty id");
            }
        }

        var details = new Dictionary<string, GraphNodeDetail>();
        foreach (var node in definition.Nodes)
        {
            var path = prefix + node.Name;
            if (node.Kind is not null && definition.SchemaVersion == GraphDefinition.Version)
            {
                throw new FormatException(
                    $"node '{path}' has a kind, which schema_version {GraphDefinition.Version} doesn't have");
            }

            if (node.Kind == GraphNodeKind.Map
                && !GraphDefinition.IsAtLeast(definition.SchemaVersion, GraphDefinition.MapVersion))
            {
                throw new FormatException(
                    $"node '{path}' is a map, which schema_version {definition.SchemaVersion} doesn't have");
            }

            if (node.Kind == GraphNodeKind.Wait
                && !GraphDefinition.IsAtLeast(definition.SchemaVersion, GraphDefinition.WaitVersion))
            {
                throw new FormatException(
                    $"node '{path}' is a wait, which schema_version {definition.SchemaVersion} doesn't have");
            }

            CheckKindFits(node, path);
            CheckInterruptFits(node, path, definition.SchemaVersion);
            CheckJoinFits(node, path, definition.SchemaVersion);
            var detail = new GraphNodeDetail
            {
                Metadata = node.Metadata,
                Kind = node.Kind,
                Branches = node.Branches?.Select(b => new GraphBranch(b.Name, b.Metadata)).ToList(),
                Graph = node.Graph is { } child ? ToTopology(child, path + "/") : null,
                Join = node.Join is { } join ? new GraphJoin(join.Policy, join.Count, join.Condition, join.Partial) : null,
                Wait = node.Wait is { } wait ? WaitOf(wait, path) : null,
                Interrupt = node.Interrupt is { } interrupt ? new GraphInterrupt(interrupt.TimeoutMs) : null,
            };
            if (detail != new GraphNodeDetail())
            {
                details[node.Name] = detail;
            }
        }

        if (GraphDefinition.IsAtLeast(definition.SchemaVersion, GraphDefinition.BoundaryVersion))
        {
            CheckRoutes(definition, prefix);
        }

        var nodes = definition.Nodes.Select(n => n.Name).ToList();
        var edges = definition.Edges.Select(e => new GraphEdge(e.From, e.To, e.Guarded, e.Condition)).ToList();
        return new GraphTopology(definition.EntryPoint, nodes, edges)
        {
            Details = details,
            Identity = definition.Identity is { } own ? new GraphIdentity(own.Id, own.Version) : null,
        };
    }

    /// <summary>
    /// A <c>join</c> needs <c>1.2.0</c> or later, a parallel group or a map, and only the fields its policy
    /// has.
    /// </summary>
    private static void CheckJoinFits(GraphNodeDefinition node, string path, string version)
    {
        if (node.Join is not { } join)
        {
            return;
        }

        if (!GraphDefinition.IsAtLeast(version, GraphDefinition.JoinVersion))
        {
            throw new FormatException($"node '{path}' has a join, which schema_version {version} doesn't have");
        }

        if (node.Kind is not (GraphNodeKind.ParallelGroup or GraphNodeKind.Map))
        {
            throw new FormatException($"node '{path}' has a join but is neither a parallel_group nor a map");
        }

        if (join.Count is not null && join.Policy != JoinKind.Quorum)
        {
            throw new FormatException($"node '{path}' has a join with a count, which only a quorum has");
        }

        if (join.Condition is not null && join.Policy != JoinKind.FirstMatching)
        {
            throw new FormatException($"node '{path}' has a join with a condition, which only first_matching has");
        }
    }

    /// <summary>
    /// An <c>interrupt</c> needs <c>1.6.0</c>, a node that isn't a wait node, and a timeout that isn't negative
    /// (<c>design/48-Boundaries-In-Definitions.md</c>).
    /// </summary>
    private static void CheckInterruptFits(GraphNodeDefinition node, string path, string version)
    {
        if (node.Interrupt is not { } interrupt)
        {
            return;
        }

        if (!GraphDefinition.IsAtLeast(version, GraphDefinition.BoundaryVersion))
        {
            throw new FormatException($"node '{path}' has an interrupt, which schema_version {version} doesn't have");
        }

        if (node.Kind == GraphNodeKind.Wait)
        {
            throw new FormatException($"node '{path}' is a wait and can't also have an interrupt");
        }

        if (interrupt.TimeoutMs < 0)
        {
            throw new FormatException($"node '{path}' has an interrupt with a negative timeout_ms");
        }
    }

    /// <summary>
    /// At a <c>1.6.0</c> level, where a boundary says it is one (<c>design/48-Boundaries-In-Definitions.md</c>):
    /// an <c>on_reject</c> edge comes from an interrupt boundary, and an <c>on_timeout</c> edge from a node with a
    /// timeout (a boundary's or an event wait's). An edge from a name that isn't a node is the validator's (a
    /// dangling edge). Before <c>1.6.0</c> no boundary was marked, so these aren't checked there.
    /// </summary>
    private static void CheckRoutes(GraphDefinition definition, string prefix)
    {
        foreach (var edge in definition.Edges)
        {
            if (definition.Nodes.FirstOrDefault(node => node.Name == edge.From) is not { } node)
            {
                continue;
            }

            var path = prefix + node.Name;
            if (edge.Condition == "on_reject" && node.Interrupt is null)
            {
                throw new FormatException($"node '{path}' has an on_reject edge but is not an interrupt boundary");
            }

            if (edge.Condition == "on_timeout" && node.Interrupt?.TimeoutMs is null && node.Wait?.TimeoutMs is null)
            {
                throw new FormatException($"node '{path}' has an on_timeout edge but no timeout");
            }
        }
    }

    /// <summary>A wait node's wait: an <c>event</c> or a <c>timer</c>.</summary>
    private static GraphWait WaitOf(GraphWaitDefinition wait, string path)
    {
        var kind = wait.Kind switch
        {
            "event" => WaitKind.Event,
            "timer" => WaitKind.Timer,
            _ => throw new FormatException($"node '{path}' has a wait of kind '{wait.Kind}', which is neither event nor timer"),
        };
        return new GraphWait(kind, wait.EventType, wait.DelayMs, wait.TimeoutMs);
    }

    private static void CheckKindFits(GraphNodeDefinition node, string path)
    {
        if (node.Kind == GraphNodeKind.Wait && node.Wait is null)
        {
            throw new FormatException($"node '{path}' is a wait but has no wait");
        }

        if (node.Kind != GraphNodeKind.Wait && node.Wait is not null)
        {
            throw new FormatException($"node '{path}' has a wait but is not a wait node");
        }

        var (branches, graph) = (node.Branches is not null, node.Graph is not null);
        var fits = node.Kind switch
        {
            null or GraphNodeKind.Wait => !branches && !graph,
            GraphNodeKind.ParallelGroup => branches && !graph,
            GraphNodeKind.Subgraph or GraphNodeKind.Map => graph && !branches,
            _ => false,
        };
        if (fits)
        {
            return;
        }

        var kind = node.Kind switch
        {
            null => "a plain node",
            GraphNodeKind.ParallelGroup => "a parallel_group",
            GraphNodeKind.Subgraph => "a subgraph",
            GraphNodeKind.Map => "a map",
            GraphNodeKind.Wait => "a wait",
            _ => $"'{node.Kind}'",
        };
        var fields = (branches, graph) switch
        {
            (false, false) => "neither branches nor graph",
            (true, false) => "branches",
            (false, true) => "graph",
            (true, true) => "both branches and graph",
        };
        throw new FormatException($"node '{path}' is {kind} but has {fields}");
    }
}
