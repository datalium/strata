using System.Text.Json;
using System.Text.Json.Nodes;

namespace Datalium.Strata.Interruption;

/// <summary>
/// Human-in-the-loop interruption (STRATA-SPEC-002). Mirrors
/// <c>rust/strata-core/src/schema.rs</c>'s <c>CheckpointStatus</c>.
/// </summary>
public enum CheckpointStatus
{
    Interrupted,
    Completed,
    Failed,

    /// <summary>
    /// The run was cancelled, or its deadline passed, while a node was running or about to start
    /// (<c>design/17-Async-Execution.md</c>). Resumable once, like <see cref="Failed"/>: a resume
    /// re-enters that node with the last committed state.
    /// </summary>
    Cancelled,

    /// <summary>
    /// The thread was ended where it stood, by <c>CompiledGraph.EndAsync</c> (<c>design/28-Ending-A-Thread.md</c>).
    /// Terminal: a resume is refused, and ending it again changes nothing. Every level of the checkpoint
    /// is ended, and none has a next node.
    /// </summary>
    Ended,

    /// <summary>
    /// A run is going on, or its process died (<c>design/33-Durable-Execution.md</c>): what a graph compiled
    /// with <c>CheckpointPolicy.EveryNode</c> writes before its first node and after every node that commits,
    /// and what a resume of one claims its thread with. Not resumable: a run that died is recovered
    /// (<c>CompiledGraph.RecoverAsync</c>). Every level of the checkpoint is running, and each has a next node.
    /// </summary>
    Running,
}

/// <summary>
/// The <c>schema_version</c>s of <c>schema/checkpoint.schema.json</c>. Mirrors
/// <c>rust/strata-core/src/schema.rs</c>'s <c>SCHEMA_VERSION</c> / <c>NESTED_CHECKPOINT_SCHEMA_VERSION</c> /
/// <c>CANCELLED_CHECKPOINT_SCHEMA_VERSION</c> / <c>DECISION_CHECKPOINT_SCHEMA_VERSION</c> /
/// <c>ENDED_CHECKPOINT_SCHEMA_VERSION</c> / <c>WAIT_CHECKPOINT_SCHEMA_VERSION</c> /
/// <c>GRAPH_CHECKPOINT_SCHEMA_VERSION</c> / <c>RUNNING_CHECKPOINT_SCHEMA_VERSION</c>.
/// </summary>
public static class CheckpointSchema
{
    /// <summary>What every checkpoint is written with, except one that nests a child's.</summary>
    public const string Version = "1.0.0";

    /// <summary>
    /// What a checkpoint carrying a <see cref="Checkpoint.Nested"/> child is written with
    /// (<c>design/16-Nested-Interrupts.md</c>). Everything that predates nesting is unchanged, and a
    /// reader that doesn't know about nesting can tell it is out of its depth.
    /// </summary>
    public const string NestedVersion = "1.1.0";

    /// <summary>
    /// What a checkpoint is written with when it is <see cref="CheckpointStatus.Cancelled"/>, or nests a
    /// cancelled one at any depth (<c>design/17-Async-Execution.md</c>). Everything else keeps the
    /// version it had before, so a reader that predates cancellation can tell it is out of its depth.
    /// </summary>
    public const string CancelledVersion = "1.2.0";

    /// <summary>
    /// What a checkpoint is written with when it carries a <see cref="Checkpoint.Decision"/> at any
    /// depth (<c>design/21-Typed-Human-Signals.md</c>). A reader that predates decisions would drop it
    /// and could resume a rejection down the approval path, so it must refuse the checkpoint.
    /// </summary>
    public const string DecisionVersion = "1.3.0";

    /// <summary>
    /// What an <see cref="CheckpointStatus.Ended"/> checkpoint is written with
    /// (<c>design/28-Ending-A-Thread.md</c>), a superset of <see cref="DecisionVersion"/>. A reader that
    /// predates it would see a status it can't parse, so it must refuse the checkpoint.
    /// </summary>
    public const string EndedVersion = "1.4.0";

    /// <summary>
    /// What a checkpoint is written with when a level of it carries a <see cref="Checkpoint.Wait"/>
    /// (<c>design/29-Event-Waits-And-Timers.md</c>), a superset of <see cref="EndedVersion"/>. A reader that
    /// predates it would resume an event wait or a timer as if a human had answered it, so it must refuse
    /// the checkpoint.
    /// </summary>
    public const string WaitVersion = "1.5.0";

    /// <summary>
    /// What a checkpoint is written with when a level of it carries a <see cref="Checkpoint.Graph"/>
    /// (<c>design/31-Graph-Identity.md</c>), a superset of <see cref="WaitVersion"/>. A reader that predates
    /// it can't check which graph wrote the thread and would resume it under any, so it must refuse the
    /// checkpoint.
    /// </summary>
    public const string GraphVersion = "1.6.0";

    /// <summary>
    /// What a <see cref="CheckpointStatus.Running"/> checkpoint is written with
    /// (<c>design/33-Durable-Execution.md</c>), a superset of <see cref="GraphVersion"/>. A reader that
    /// predates it would see a status it can't parse, so it must refuse the checkpoint.
    /// </summary>
    public const string RunningVersion = "1.7.0";

    /// <summary>Whether this build can read a checkpoint written with <paramref name="version"/>.</summary>
    public static bool IsSupported(string version) =>
        version is Version or NestedVersion or CancelledVersion or DecisionVersion or EndedVersion or WaitVersion
            or GraphVersion or RunningVersion;

    /// <summary>
    /// The lowest version that can express <paramref name="checkpoint"/>: <see cref="RunningVersion"/> if it is
    /// running, else <see cref="GraphVersion"/> if a
    /// level of it carries a <see cref="Checkpoint.Graph"/>, else <see cref="EndedVersion"/> if
    /// it is ended, else <see cref="WaitVersion"/> if a level of it waits with a <see cref="Checkpoint.Wait"/>,
    /// else <see cref="DecisionVersion"/> if anything in it carries a decision, else
    /// <see cref="CancelledVersion"/> if anything in it is cancelled, else <see cref="NestedVersion"/> if
    /// it nests a child, else <see cref="Version"/>.
    /// </summary>
    internal static string For(Checkpoint checkpoint)
    {
        // Every level of a running checkpoint is running, so the outermost says it.
        if (checkpoint.Status == CheckpointStatus.Running)
        {
            return RunningVersion;
        }

        for (var level = checkpoint; level is not null; level = level.Nested)
        {
            if (level.Graph is not null)
            {
                return GraphVersion;
            }
        }

        if (checkpoint.Status == CheckpointStatus.Ended)
        {
            return EndedVersion;
        }

        if (checkpoint.InnermostWait() is not null)
        {
            return WaitVersion;
        }

        var cancelled = false;
        for (var level = checkpoint; level is not null; level = level.Nested)
        {
            if (level.Decision is not null)
            {
                return DecisionVersion;
            }

            cancelled |= level.Status == CheckpointStatus.Cancelled;
        }

        return cancelled ? CancelledVersion
            : checkpoint.Nested is null ? Version
            : NestedVersion;
    }

    /// <summary><paramref name="checkpoint"/> with the <see cref="Checkpoint.SchemaVersion"/> its content needs.</summary>
    internal static Checkpoint Versioned(Checkpoint checkpoint) => checkpoint with { SchemaVersion = For(checkpoint) };
}

/// <summary>
/// Core-local, JSON-based checkpoint shape. Deliberately not
/// <c>Datalium.Strata.Storage.Schema.Checkpoint</c> (the cross-language interop shape) — this
/// project has no dependency on <c>Datalium.Strata.Storage</c>, only on the BCL's
/// <c>System.Text.Json</c>. <see cref="FrozenState"/> uses the mutable
/// <see cref="JsonNode"/> (not <see cref="System.Text.Json.JsonElement"/>) since resume needs to
/// merge field overrides into it.
/// </summary>
public sealed record Checkpoint
{
    public required string SchemaVersion { get; init; }

    public required string ThreadId { get; init; }

    public required CheckpointStatus Status { get; init; }

    public string? SuspendedNode { get; init; }

    public string? NextNode { get; init; }

    public required JsonNode? FrozenState { get; init; }

    public Dictionary<string, string>? Metadata { get; init; }

    public required string CreatedAt { get; init; }

    public string? ResumedAt { get; init; }

    /// <summary>
    /// The checkpoint of a child graph that is itself interrupted (or, on a <c>Failed</c> or
    /// <c>Cancelled</c> checkpoint, that failed or was cancelled), when <see cref="SuspendedNode"/> is a
    /// subgraph node (<c>design/16-Nested-Interrupts.md</c>). Its <see cref="ThreadId"/> is the name of
    /// that subgraph node, its status is its parent's — except that a <c>Failed</c> parent may nest a
    /// <c>Cancelled</c> child, stopped by the parent node's timeout (<c>design/17-Async-Execution.md</c>)
    /// — and it may nest further. Only present on an interrupted, failed or cancelled checkpoint, which
    /// is then written as <see cref="CheckpointSchema.NestedVersion"/>, or as
    /// <see cref="CheckpointSchema.CancelledVersion"/> if anything in it is cancelled.
    /// </summary>
    public Checkpoint? Nested { get; init; }

    /// <summary>
    /// The decision in effect at this checkpoint's level when it was written: the answer to the
    /// level's most recent interrupt boundary (<c>design/21-Typed-Human-Signals.md</c>), which a
    /// resume restores. <see langword="null"/> when no boundary of the level has been answered. A
    /// checkpoint carrying one at any depth is written as <see cref="CheckpointSchema.DecisionVersion"/>.
    /// </summary>
    public HumanDecision? Decision { get; init; }

    /// <summary>
    /// Why this level waits, when it waits for more than a human with no timeout
    /// (<c>design/29-Event-Waits-And-Timers.md</c>): an event, a timer, or a human with a timeout, and when
    /// it wakes or times out. Only an interrupted level carries one, and only the innermost level of a
    /// checkpoint can. A checkpoint carrying one is written as <see cref="CheckpointSchema.WaitVersion"/>.
    /// </summary>
    public CheckpointWait? Wait { get; init; }

    /// <summary>
    /// The graph that wrote this level (<c>design/31-Graph-Identity.md</c>): the identity it was compiled
    /// with, or <see langword="null"/> when it had none (or the checkpoint predates identities), which any
    /// graph may resume. A nested level carries its own subgraph's. A checkpoint carrying one at any level
    /// is written as <see cref="CheckpointSchema.GraphVersion"/>.
    /// </summary>
    public GraphIdentity? Graph { get; init; }

    /// <summary>The <see cref="Wait"/> of the innermost level, the only one that can have one.</summary>
    public CheckpointWait? InnermostWait()
    {
        var level = this;
        while (level.Nested is not null)
        {
            level = level.Nested;
        }

        return level.Wait;
    }

    /// <summary>
    /// Where this thread is waiting (or, for a failed one, stopped): the <see cref="SuspendedNode"/> of
    /// every level, from the outermost graph in. <c>["Work", "Approve"]</c> for a boundary at
    /// <c>Approve</c> inside the subgraph node <c>Work</c>; just <c>["Approve"]</c> when nothing is
    /// nested. Empty if the checkpoint has no <see cref="SuspendedNode"/>.
    /// </summary>
    public IReadOnlyList<string> WaitingAt()
    {
        var path = new List<string>();
        for (var level = this; level is not null; level = level.Nested)
        {
            if (level.SuspendedNode is { } node)
            {
                path.Add(node);
            }
        }

        return path;
    }

    /// <summary>
    /// This checkpoint as the JSON of <c>schema/checkpoint.schema.json</c>, child included. The
    /// <c>nested</c>, <c>decision</c>, <c>wait</c> and <c>graph</c> keys are absent, not <c>null</c>, when there is none,
    /// so a checkpoint that predates them serializes exactly as before.
    /// </summary>
    public JsonObject ToWireJson()
    {
        var json = new JsonObject
        {
            ["schema_version"] = SchemaVersion,
            ["thread_id"] = ThreadId,
            ["status"] = StatusToString(Status),
            ["suspended_node"] = SuspendedNode,
            ["next_node"] = NextNode,
            ["frozen_state"] = FrozenState?.DeepClone(),
            ["metadata"] = Metadata is null ? null : JsonSerializer.SerializeToNode(Metadata),
            ["created_at"] = CreatedAt,
            ["resumed_at"] = ResumedAt,
        };

        if (Nested is not null)
        {
            json["nested"] = Nested.ToWireJson();
        }

        if (Decision is not null)
        {
            json["decision"] = Decision.ToWireJson();
        }

        if (Wait is not null)
        {
            json["wait"] = Wait.ToWireJson();
        }

        if (Graph is not null)
        {
            json["graph"] = Graph.ToWireJson();
        }

        return json;
    }

    /// <summary>The inverse of <see cref="ToWireJson"/>.</summary>
    public static Checkpoint FromWireJson(JsonNode node)
    {
        var json = node.AsObject();
        return new Checkpoint
        {
            SchemaVersion = json["schema_version"]!.GetValue<string>(),
            ThreadId = json["thread_id"]!.GetValue<string>(),
            Status = StatusFromString(json["status"]!.GetValue<string>()),
            SuspendedNode = json["suspended_node"]?.GetValue<string>(),
            NextNode = json["next_node"]?.GetValue<string>(),
            FrozenState = json["frozen_state"]?.DeepClone(),
            Metadata = json["metadata"] is { } metadata
                ? metadata.Deserialize<Dictionary<string, string>>()
                : null,
            CreatedAt = json["created_at"]!.GetValue<string>(),
            ResumedAt = json["resumed_at"]?.GetValue<string>(),
            Nested = json["nested"] is { } nested ? FromWireJson(nested) : null,
            Decision = json["decision"] is { } decision ? HumanDecision.FromWireJson(decision) : null,
            Wait = json["wait"] is { } wait ? CheckpointWait.FromWireJson(wait) : null,
            Graph = json["graph"] is { } graph ? GraphIdentity.FromWireJson(graph) : null,
        };
    }

    private static string StatusToString(CheckpointStatus status) => status switch
    {
        CheckpointStatus.Interrupted => "interrupted",
        CheckpointStatus.Completed => "completed",
        CheckpointStatus.Failed => "failed",
        CheckpointStatus.Cancelled => "cancelled",
        CheckpointStatus.Ended => "ended",
        CheckpointStatus.Running => "running",
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "unknown checkpoint status"),
    };

    private static CheckpointStatus StatusFromString(string status) => status switch
    {
        "interrupted" => CheckpointStatus.Interrupted,
        "completed" => CheckpointStatus.Completed,
        "failed" => CheckpointStatus.Failed,
        "cancelled" => CheckpointStatus.Cancelled,
        "ended" => CheckpointStatus.Ended,
        "running" => CheckpointStatus.Running,
        _ => throw new InvalidOperationException($"unknown checkpoint status '{status}' in the checkpoint log"),
    };
}
