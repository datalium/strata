namespace Datalium.Strata.Storage.Memory;

/// <summary>
/// One row of the shared SQLite checkpoint log (see <c>schema/README.md</c>, "SQLite checkpoint
/// log"): one <c>Checkpoint</c> field per column, <c>FrozenState</c> and <c>Metadata</c> as JSON
/// text, <see cref="Sequence"/> counting a thread's saves from 0. The same table is written by
/// <c>strata-memory</c>'s <c>SqliteCheckpointStore</c> and the <c>strata</c> CLI, which is what
/// lets each read a database file the others produced. <see cref="CheckpointDbContext"/> maps
/// the properties to the snake_case column names.
/// </summary>
public sealed class CheckpointRecordEntity
{
    public required string ThreadId { get; init; }

    public required long Sequence { get; init; }

    public required string SchemaVersion { get; init; }

    /// <summary>Lowercase, matching the Rust runtime: <c>interrupted</c>, <c>completed</c> or <c>failed</c>.</summary>
    public required string Status { get; init; }

    public string? SuspendedNode { get; init; }

    public string? NextNode { get; init; }

    public required string FrozenState { get; init; }

    public string? Metadata { get; init; }

    public required string CreatedAt { get; init; }

    public string? ResumedAt { get; init; }

    /// <summary>
    /// JSON text of the child's own checkpoint (the wire shape of <c>schema/checkpoint.schema.json</c>)
    /// when the thread froze inside a subgraph (<c>design/16-Nested-Interrupts.md</c>); <c>NULL</c>
    /// otherwise.
    /// </summary>
    public string? Nested { get; init; }

    /// <summary>
    /// JSON text of the decision in effect at the checkpoint's level (<c>design/21-Typed-Human-Signals.md</c>);
    /// <c>NULL</c> when there is none.
    /// </summary>
    public string? Decision { get; init; }

    /// <summary>
    /// JSON text of the checkpoint's own level's wait (<c>design/29-Event-Waits-And-Timers.md</c>);
    /// <c>NULL</c> when there is none.
    /// </summary>
    public string? Wait { get; init; }

    /// <summary>
    /// The innermost level's <c>wait.wake_at</c>, derived when the row is written, for the head table's
    /// trigger; never read back.
    /// </summary>
    public string? WakeAt { get; init; }

    /// <summary>
    /// The outermost level's <c>graph.id</c> (<c>design/31-Graph-Identity.md</c>); <c>NULL</c> when it has no
    /// identity. Nested levels carry theirs inside <see cref="Nested"/>.
    /// </summary>
    public string? GraphId { get; init; }

    /// <summary>The outermost level's <c>graph.version</c>; <c>NULL</c> when it has none.</summary>
    public string? GraphVersion { get; init; }
}
