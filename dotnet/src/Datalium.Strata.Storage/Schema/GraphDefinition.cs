using System.Text.Json.Serialization;
using Datalium.Strata.Graph;

namespace Datalium.Strata.Storage.Schema;

/// <summary>
/// Types for <c>schema/graph-definition.schema.json</c>. Must stay in lockstep with the
/// JSON Schema file and with the Rust equivalent in <c>rust/strata-core/src/schema.rs</c>.
/// See <c>schema/README.md</c>.
/// </summary>
public record GraphNodeDefinition
{
    [JsonPropertyName("name")]
    public required string Name { get; init; }

    /// <summary>Description only, set with <c>SetNodeMetadata</c>; written with its keys sorted.</summary>
    [JsonPropertyName("metadata")]
    public Dictionary<string, string>? Metadata { get; init; }

    /// <summary>
    /// Absent for a plain node (<c>design/22-Graph-Structure-In-Definitions.md</c>). A parallel group
    /// has <see cref="Branches"/>, a subgraph and a map <see cref="Graph"/>.
    /// </summary>
    [JsonPropertyName("kind")]
    [JsonConverter(typeof(JsonStringEnumConverter<GraphNodeKind>))]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public GraphNodeKind? Kind { get; init; }

    /// <summary>A parallel group's branches, in declaration order.</summary>
    [JsonPropertyName("branches")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public List<GraphBranchDefinition>? Branches { get; init; }

    /// <summary>
    /// A subgraph's child, or a map's template (the child each item runs, <c>design/27-Map-Fan-Out.md</c>):
    /// its whole definition, with its own <c>schema_version</c>.
    /// </summary>
    [JsonPropertyName("graph")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public GraphDefinition? Graph { get; init; }

    /// <summary>
    /// A parallel group's or a map's join, when it isn't the default
    /// (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>). A level with one is at least
    /// <see cref="GraphDefinition.JoinVersion"/>.
    /// </summary>
    [JsonPropertyName("join")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public GraphJoinDefinition? Join { get; init; }

    /// <summary>
    /// A wait node's wait (<c>design/29-Event-Waits-And-Timers.md</c>). A level with one is
    /// <see cref="GraphDefinition.WaitVersion"/>.
    /// </summary>
    [JsonPropertyName("wait")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public GraphWaitDefinition? Wait { get; init; }

    /// <summary>
    /// Set when the node is an interrupt boundary, of any kind but a wait
    /// (<c>design/48-Boundaries-In-Definitions.md</c>). A level with one is
    /// <see cref="GraphDefinition.BoundaryVersion"/>.
    /// </summary>
    [JsonPropertyName("interrupt")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public GraphInterruptDefinition? Interrupt { get; init; }
}

/// <summary>
/// An interrupt boundary (<c>design/48-Boundaries-In-Definitions.md</c>): the run freezes after the node until a
/// human answers it, and with a <c>timeout_ms</c> it times out, continuing at its guarded <c>on_timeout</c> edge
/// or ending the thread. Its routes are edges, not repeated here. The key is always written.
/// </summary>
public record GraphInterruptDefinition
{
    [JsonPropertyName("timeout_ms")]
    public long? TimeoutMs { get; init; }
}

/// <summary>
/// A wait node's wait (<c>design/29-Event-Waits-And-Timers.md</c>). Every key is always written:
/// <c>event_type</c> for an event wait, <c>delay_ms</c> for a timer with a fixed delay, <c>timeout_ms</c> for
/// an event wait with a timeout.
/// </summary>
public record GraphWaitDefinition
{
    /// <summary><c>event</c> or <c>timer</c>.</summary>
    [JsonPropertyName("kind")]
    public required string Kind { get; init; }

    [JsonPropertyName("event_type")]
    public string? EventType { get; init; }

    [JsonPropertyName("delay_ms")]
    public long? DelayMs { get; init; }

    [JsonPropertyName("timeout_ms")]
    public long? TimeoutMs { get; init; }
}

/// <summary>
/// A parallel group's join (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>). Every key is always
/// written: <c>count</c> is set only for a quorum, <c>condition</c> only for <c>first_matching</c>.
/// </summary>
public record GraphJoinDefinition
{
    [JsonPropertyName("policy")]
    [JsonConverter(typeof(JsonStringEnumConverter<JoinKind>))]
    public required JoinKind Policy { get; init; }

    [JsonPropertyName("count")]
    public int? Count { get; init; }

    [JsonPropertyName("condition")]
    public string? Condition { get; init; }

    [JsonPropertyName("partial")]
    [JsonConverter(typeof(JsonStringEnumConverter<PartialResults>))]
    public required PartialResults Partial { get; init; }
}

/// <summary>One branch of a parallel group (<c>design/22-Graph-Structure-In-Definitions.md</c>).</summary>
public record GraphBranchDefinition
{
    [JsonPropertyName("name")]
    public required string Name { get; init; }

    [JsonPropertyName("metadata")]
    public Dictionary<string, string>? Metadata { get; init; }
}

public record GraphEdgeDefinition
{
    [JsonPropertyName("from")]
    public required string From { get; init; }

    [JsonPropertyName("to")]
    public required string To { get; init; }

    [JsonPropertyName("guarded")]
    public required bool Guarded { get; init; }

    [JsonPropertyName("condition")]
    public string? Condition { get; init; }
}

public record GraphDefinition
{
    /// <summary>
    /// The version a definition with no parallel group or subgraph at its own level is written with,
    /// byte for byte as before <c>design/22-Graph-Structure-In-Definitions.md</c>.
    /// </summary>
    public const string Version = "1.0.0";

    /// <summary>The version of a definition with a parallel group or subgraph node at its own level.</summary>
    public const string CompositeVersion = "1.1.0";

    /// <summary>
    /// The version of a definition with a parallel group at its own level whose join isn't the default
    /// (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>).
    /// </summary>
    public const string JoinVersion = "1.2.0";

    /// <summary>
    /// The version of a definition with a map at its own level (<c>design/27-Map-Fan-Out.md</c>). It is
    /// a superset of <see cref="JoinVersion"/>: a group at that level may have a join too.
    /// </summary>
    public const string MapVersion = "1.3.0";

    /// <summary>
    /// The version of a definition with a wait node at its own level (<c>design/29-Event-Waits-And-Timers.md</c>).
    /// It is a superset of <see cref="MapVersion"/>.
    /// </summary>
    public const string WaitVersion = "1.4.0";

    /// <summary>
    /// The version of a definition whose level has an <see cref="Identity"/> (<c>design/31-Graph-Identity.md</c>).
    /// It is a superset of <see cref="WaitVersion"/>.
    /// </summary>
    public const string IdentityVersion = "1.5.0";

    /// <summary>
    /// The version of a definition with an interrupt boundary at its own level, a node with an
    /// <see cref="GraphNodeDefinition.Interrupt"/> (<c>design/48-Boundaries-In-Definitions.md</c>). It is a
    /// superset of <see cref="IdentityVersion"/>.
    /// </summary>
    public const string BoundaryVersion = "1.6.0";

    private static readonly string[] Versions =
        [Version, CompositeVersion, JoinVersion, MapVersion, WaitVersion, IdentityVersion, BoundaryVersion];

    /// <summary>Whether this build can read a graph definition written with <paramref name="version"/>.</summary>
    public static bool IsSupportedVersion(string version) => Versions.Contains(version);

    /// <summary>
    /// Whether <paramref name="version"/> is <paramref name="least"/> or a later version this build knows: each
    /// version is a superset of every earlier one.
    /// </summary>
    internal static bool IsAtLeast(string version, string least) =>
        IsSupportedVersion(version) && Array.IndexOf(Versions, version) >= Array.IndexOf(Versions, least);

    [JsonPropertyName("schema_version")]
    public required string SchemaVersion { get; init; }

    /// <summary>
    /// The identity the graph was compiled with (<c>design/31-Graph-Identity.md</c>). Absent, not <c>null</c>,
    /// when it has none; a level with one is <see cref="IdentityVersion"/>.
    /// </summary>
    [JsonPropertyName("identity")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Identity? Identity { get; init; }

    [JsonPropertyName("entry_point")]
    public required string EntryPoint { get; init; }

    [JsonPropertyName("nodes")]
    public required List<GraphNodeDefinition> Nodes { get; init; }

    [JsonPropertyName("edges")]
    public required List<GraphEdgeDefinition> Edges { get; init; }
}
