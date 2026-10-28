using Datalium.Strata.Interruption;

namespace Datalium.Strata.Graph;

/// <summary>
/// Options for <see cref="StateGraph{TState}.Compile(CompilationOptions)"/> (STRATA-SPEC-004). Mirrors
/// the Rust engine's <c>CompileOptions</c>.
/// </summary>
public sealed class CompilationOptions
{
    /// <summary>
    /// The loop circuit breaker (<c>design/24-Loop-Circuit-Breaker.md</c>): the most nodes one thread may
    /// start at this graph's level, across every run and resume of it. A retry is not a new hop; a
    /// fallback node, a parallel group and a subgraph node are one each. Starting the node that would
    /// be one more ends the run with <c>GraphExecutionResult.HopLimitExceeded</c> and a <c>Failed</c>
    /// checkpoint. A subgraph's child counts its own nodes against its own limit.
    ///
    /// <see langword="null"/> (the default) is no limit. Must be at least 1.
    /// </summary>
    public int? MaxNodeHops { get; init; }

    /// <summary>
    /// The graph's identity (<c>design/31-Graph-Identity.md</c>): written on every checkpoint level this graph
    /// writes, and checked on every one it resumes or ends, so a thread another graph wrote is refused.
    /// <see langword="null"/> (the default) is no identity: the graph resumes any thread without one, as
    /// before. Must not be blank.
    /// </summary>
    public string? GraphId { get; init; }

    /// <summary>
    /// The version of <see cref="GraphId"/>, free text compared exactly: a thread written by another version
    /// is refused unless <see cref="Migrate"/> accepts it. Needs <see cref="GraphId"/>; must not be blank.
    /// </summary>
    public string? GraphVersion { get; init; }

    /// <summary>
    /// Accepts a checkpoint level written by another version of this graph (<c>design/31-Graph-Identity.md</c>,
    /// "The rule"): returns the level to resume (or end) instead, with nodes renamed or state reshaped as this
    /// version needs, or <see langword="null"/> to refuse it. It sees the level as stored, its
    /// <see cref="Checkpoint.Graph"/> saying which version wrote it; the engine then gives it this graph's
    /// identity and keeps its thread id. It should change its own level only, not <see cref="Checkpoint.Nested"/>.
    /// It runs before anything is written, and must be pure. Needs <see cref="GraphId"/>.
    /// </summary>
    public Func<Checkpoint, Checkpoint?>? Migrate { get; init; }

    /// <summary>
    /// When a run of this graph writes checkpoints (<c>design/33-Durable-Execution.md</c>).
    /// <see cref="CheckpointPolicy.Boundaries"/> (the default): only where it stops, as before.
    /// <see cref="CheckpointPolicy.EveryNode"/>: also before its first node and after every node that commits,
    /// so a thread whose process died can be recovered (<c>CompiledGraph.RecoverAsync</c>). The outermost
    /// graph's policy is the run's, at every depth: a subgraph's child follows its parent's.
    /// </summary>
    public CheckpointPolicy Checkpointing { get; init; }
}

/// <summary>When a run writes checkpoints (<see cref="CompilationOptions.Checkpointing"/>).</summary>
public enum CheckpointPolicy
{
    /// <summary>
    /// Only where a run stops: an interrupt boundary or a wait, a failure, a cancellation, an end. Nothing
    /// is saved between nodes, and a run that completes writes nothing. A thread whose process dies mid-run
    /// can't be continued.
    /// </summary>
    Boundaries,

    /// <summary>
    /// Also a <c>running</c> checkpoint before the first node and after every node that commits (a parallel
    /// group and a map are one node each), and a <c>completed</c> one when the run finishes
    /// (<c>design/33-Durable-Execution.md</c>). A resume claims its thread with a <c>running</c> checkpoint
    /// instead of stamping <c>resumed_at</c>. One store append and one state serialization per node.
    /// </summary>
    EveryNode,
}
