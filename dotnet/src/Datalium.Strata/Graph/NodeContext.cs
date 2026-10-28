using System.Diagnostics;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Observability;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Graph;

/// <summary>
/// What an async node handler is told about the run it is part of
/// (<c>design/17-Async-Execution.md</c>). The cancellation token is the handler's separate
/// <see cref="CancellationToken"/> parameter. Mirrors the Rust engine's <c>NodeContext</c>.
/// </summary>
/// <remarks>
/// A sealed class with <c>init</c> properties, not a positional record, so fields can be added
/// without breaking anyone. A handler can be unit-tested with a context built by hand:
/// <c>new NodeContext { ThreadId = "t", Node = "Draft", NodePath = "Draft" }</c>.
/// </remarks>
public sealed class NodeContext : IExecutionContext
{
    /// <summary>
    /// The thread id the caller passed to the outermost <c>RunAsync</c> / <c>ResumeAsync</c>, at every
    /// depth. Inside a subgraph it is not the child's own thread id (the subgraph node's name, which
    /// only labels nested checkpoints).
    /// </summary>
    public required string ThreadId { get; init; }

    /// <summary>The node's own name: <c>"Draft"</c>, or a branch's name inside a parallel group.</summary>
    public required string Node { get; init; }

    /// <summary>
    /// The node's qualified name, the same one streaming chunks carry: <c>"&lt;subgraph&gt;/&lt;node&gt;"</c>
    /// to any depth, <c>"&lt;group&gt;/&lt;branch&gt;"</c> for a branch.
    /// </summary>
    public required string NodePath { get; init; }

    /// <summary>
    /// 1-based, counting this node's retries under its <see cref="RecoveryPolicy"/>. A branch reports
    /// its group's attempt. A node inside a subgraph counts its own retries only.
    /// </summary>
    public int Attempt { get; init; } = 1;

    /// <summary>
    /// The node's stream context whenever the run is streaming (<c>RunStreaming</c> /
    /// <c>ResumeStreaming</c>), <see langword="null"/> otherwise. Always present in a streaming node
    /// or a streaming branch, since a plain run refuses those before running them.
    /// </summary>
    public IStreamContext? Stream { get; init; }

    /// <summary>What <see cref="RunOptions.History"/> supplied, if anything.</summary>
    public IHistoryStore? History { get; init; }

    /// <summary>What <see cref="RunOptions.Services"/> supplied, if anything.</summary>
    public IServiceProvider? Services { get; init; }

    /// <summary>
    /// The id of the run this node is part of, at every depth: <see cref="RunOptions.RunId"/>, or the
    /// one the engine generated. A resume is a new run with a new id.
    /// </summary>
    public string RunId { get; init; } = string.Empty;

    /// <summary>
    /// The run's metadata (<see cref="RunOptions.Metadata"/>, merged with a resumed checkpoint's), at
    /// every depth. Empty when there is none.
    /// </summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = Observability.RunMetadata.None;

    /// <summary>
    /// The span this handler runs inside: its attempt's, or for a branch the branch's. It is also
    /// <see cref="Activity.Current"/> while the handler runs. <see langword="null"/> when nothing listens
    /// to <c>Datalium.Strata</c> (<see cref="Observability.StrataTelemetry"/>).
    /// </summary>
    public Activity? Activity { get; init; }

    /// <summary>
    /// The answer to the most recent interrupt boundary of this node's graph level
    /// (<c>design/21-Typed-Human-Signals.md</c>): set from the resume that answered it on, for every
    /// node and branch of the level, until another boundary of the level is answered. Kept across a
    /// failed or cancelled thread's resume, which doesn't replace it. <see langword="null"/> when no
    /// boundary of the level has been answered; a subgraph's child doesn't inherit its parent's.
    /// </summary>
    public HumanDecision? Decision { get; init; }

    /// <summary>
    /// Set only in a fallback node's context, on every attempt of it: why the node whose
    /// <see cref="RecoveryPolicy"/> routed here failed (<c>design/25-Retry-Backoff.md</c>). A parallel
    /// group used as a fallback hands it to its branches. <see langword="null"/> everywhere else,
    /// including the node after the fallback, and in a fallback re-entered by a resume.
    /// </summary>
    public NodeFailure? LastFailure { get; init; }

    /// <summary>
    /// Names this execution of this node in this thread (<c>design/33-Durable-Execution.md</c>, "The crash
    /// contract"): the thread id, the node's path, and its hop number at every level, as
    /// <c>inc-42/Remediate/Execute#3.2</c>. The same on every retry of the node, and another when the thread
    /// reaches the node again. Pass it to whatever the node changes outside its state, so doing it twice does
    /// it once.
    ///
    /// In a graph compiled with <c>CheckpointPolicy.EveryNode</c> it is also the same when the node runs again
    /// in a recovery (<see cref="IsRecovery"/>). In any other run it ends with <c>@</c> and the run id, so a
    /// later resume never repeats one. A branch of a parallel group has its own, under its group's hop. Empty
    /// in a context built by hand, unless set.
    /// </summary>
    public string IdempotencyKey { get; init; } = string.Empty;

    /// <summary>
    /// Whether this node may have run before, in a process that died (<c>design/33-Durable-Execution.md</c>):
    /// it is the node a recovery (<c>CompiledGraph.RecoverAsync</c>) starts with at its level, or its fallback,
    /// or inside a parallel group or a map that a recovery runs again. On every attempt. It doesn't say the
    /// node ran, only that it may have: what it did then may already be done. <see langword="false"/> once a
    /// node of the level has committed, and in every run that isn't a recovery.
    /// </summary>
    public bool IsRecovery { get; init; }

    /// <summary>
    /// The run's hook layer, for the tool calls a handler makes (<see cref="Tools.ToolCallContext.From"/>).
    /// <see langword="null"/> in a context built by hand.
    /// </summary>
    internal RunTelemetry? Telemetry { get; init; }

    /// <summary>For code that takes an <see cref="IExecutionContext"/>; throws if the run has no history store.</summary>
    IHistoryStore IExecutionContext.History =>
        History ?? throw new InvalidOperationException(
            $"node '{NodePath}' has no history store: pass one in RunOptions.History");
}
