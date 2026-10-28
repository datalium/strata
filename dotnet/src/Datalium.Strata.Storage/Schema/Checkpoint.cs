using System.Text.Json;
using System.Text.Json.Serialization;

namespace Datalium.Strata.Storage.Schema;

/// <summary>
/// Types for <c>schema/checkpoint.schema.json</c>. Must stay in lockstep with the JSON
/// Schema file and with the Rust equivalent in <c>rust/strata-core/src/schema.rs</c>.
/// See <c>schema/README.md</c>.
/// </summary>
public enum CheckpointStatus
{
    [JsonStringEnumMemberName("interrupted")]
    Interrupted,

    [JsonStringEnumMemberName("completed")]
    Completed,

    [JsonStringEnumMemberName("failed")]
    Failed,

    /// <summary><c>design/17-Async-Execution.md</c>: the run was cancelled, or its deadline passed.</summary>
    [JsonStringEnumMemberName("cancelled")]
    Cancelled,

    /// <summary><c>design/28-Ending-A-Thread.md</c>: the thread was ended where it stood, and can't be resumed.</summary>
    [JsonStringEnumMemberName("ended")]
    Ended,

    /// <summary>A run is going on, or its process died (<c>design/33-Durable-Execution.md</c>).</summary>
    [JsonStringEnumMemberName("running")]
    Running,
}

public record Checkpoint
{
    [JsonPropertyName("schema_version")]
    public required string SchemaVersion { get; init; }

    [JsonPropertyName("thread_id")]
    public required string ThreadId { get; init; }

    [JsonPropertyName("status")]
    [JsonConverter(typeof(JsonStringEnumConverter<CheckpointStatus>))]
    public required CheckpointStatus Status { get; init; }

    [JsonPropertyName("suspended_node")]
    public string? SuspendedNode { get; init; }

    [JsonPropertyName("next_node")]
    public string? NextNode { get; init; }

    [JsonPropertyName("frozen_state")]
    public required JsonElement FrozenState { get; init; }

    [JsonPropertyName("metadata")]
    public Dictionary<string, string>? Metadata { get; init; }

    [JsonPropertyName("created_at")]
    public required string CreatedAt { get; init; }

    [JsonPropertyName("resumed_at")]
    public string? ResumedAt { get; init; }

    /// <summary>
    /// The child's own checkpoint, when <see cref="SuspendedNode"/> is a subgraph node whose child is
    /// itself interrupted (or, on a failed or cancelled checkpoint, failed or cancelled) --
    /// <c>design/16-Nested-Interrupts.md</c>, <c>design/17-Async-Execution.md</c>.
    /// Absent, not <c>null</c>, on the wire when there is no child.
    /// </summary>
    [JsonPropertyName("nested")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Checkpoint? Nested { get; init; }

    /// <summary>
    /// The decision in effect at this checkpoint's level when it was written
    /// (<c>design/21-Typed-Human-Signals.md</c>). Absent, not <c>null</c>, on the wire when there is none.
    /// </summary>
    [JsonPropertyName("decision")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Decision? Decision { get; init; }

    /// <summary>
    /// Why this level waits (<c>design/29-Event-Waits-And-Timers.md</c>): an event, a timer, or a human with a
    /// timeout. Absent, not <c>null</c>, on the wire when there is none.
    /// </summary>
    [JsonPropertyName("wait")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Wait? Wait { get; init; }

    /// <summary>
    /// The graph that wrote this level (<c>design/31-Graph-Identity.md</c>). Absent, not <c>null</c>, on the wire
    /// when it had no identity.
    /// </summary>
    [JsonPropertyName("graph")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Identity? Graph { get; init; }
}

/// <summary>
/// A graph's identity (<c>design/31-Graph-Identity.md</c>): a checkpoint level's <c>graph</c>, a definition's
/// <c>identity</c>. Both keys are always written.
/// </summary>
public record Identity
{
    [JsonPropertyName("id")]
    public required string Id { get; init; }

    [JsonPropertyName("version")]
    public string? Version { get; init; }
}

/// <summary>The checkpoint's <c>wait</c> object. Every key is always written.</summary>
public record Wait
{
    /// <summary><c>human</c>, <c>event</c> or <c>timer</c>.</summary>
    [JsonPropertyName("kind")]
    public required string Kind { get; init; }

    [JsonPropertyName("event_type")]
    public string? EventType { get; init; }

    /// <summary>When a timer fires or the wait times out (RFC 3339, whole seconds); <c>null</c> for never.</summary>
    [JsonPropertyName("wake_at")]
    public string? WakeAt { get; init; }
}

/// <summary>The checkpoint's <c>decision</c> object: a human's answer to an interrupt boundary.</summary>
public record Decision
{
    [JsonPropertyName("boundary")]
    public required string Boundary { get; init; }

    [JsonPropertyName("is_approved")]
    public required bool IsApproved { get; init; }

    [JsonPropertyName("reviewer")]
    public required string Reviewer { get; init; }

    /// <summary>Every override applied, with its JSON type, by key.</summary>
    [JsonPropertyName("overrides")]
    public required Dictionary<string, JsonElement> Overrides { get; init; }

    [JsonPropertyName("decided_at")]
    public required string DecidedAt { get; init; }
}
