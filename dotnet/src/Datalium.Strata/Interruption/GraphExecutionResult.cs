using Datalium.Strata.Scheduling;

namespace Datalium.Strata.Interruption;

/// <summary>
/// Mirrors <c>rust/strata-core/src/checkpoint.rs</c>'s <c>GraphExecutionResult&lt;S&gt;</c> enum
/// as a closed record hierarchy (the private constructor prevents external subclassing, so a
/// <c>switch</c> over the nested records is exhaustive in practice). <see cref="NodeFailed"/>
/// refines <see cref="Failed"/>, so code that only matches <see cref="Failed"/> still catches it.
/// </summary>
public abstract record GraphExecutionResult<TState>
{
    private GraphExecutionResult()
    {
    }

    public sealed record Completed(TState State) : GraphExecutionResult<TState>;

    /// <summary>
    /// The run froze at an interrupt boundary. <paramref name="WaitingAt"/> is where it is actually
    /// waiting, one node per level from the outermost graph in: <c>["Approve"]</c> for a boundary in
    /// this graph, <c>["Work", "Approve"]</c> for one at <c>Approve</c> inside the subgraph node
    /// <c>Work</c> (whose name is then also <paramref name="SuspendedNode"/>).
    /// </summary>
    public sealed record Interrupted(
        string ThreadId,
        string SuspendedNode,
        string NextNode,
        TState FrozenState,
        IReadOnlyList<string> WaitingAt) : GraphExecutionResult<TState>
    {
        /// <summary>
        /// What the innermost level waits for, when it is more than a human with no timeout
        /// (<c>design/29-Event-Waits-And-Timers.md</c>): an event, a timer, or a human with a timeout, and
        /// when it wakes or times out. The checkpoint's innermost <c>wait</c>.
        /// </summary>
        public CheckpointWait? Wait { get; init; }
    }

    public record Failed(string Message) : GraphExecutionResult<TState>;

    /// <summary>
    /// A fallible node failed and its recovery policy had nothing left to try (retries exhausted,
    /// or a fatal error, with no fallback node). A <c>CheckpointStatus.Failed</c> checkpoint
    /// holding the last committed state was persisted before this was returned.
    /// <paramref name="Attempts"/> counts every attempt including the first.
    /// </summary>
    public sealed record NodeFailed(string Node, int Attempts, string Message) : Failed(Message);

    /// <summary>
    /// The loop circuit breaker tripped (<c>design/24-Loop-Circuit-Breaker.md</c>): the graph was about to
    /// start <paramref name="Node"/>, which would have been hop <paramref name="Hops"/> + 1 with a limit of
    /// <paramref name="Limit"/>. The node never started. A <c>CheckpointStatus.Failed</c> checkpoint at
    /// <paramref name="Node"/>, with <c>error_kind</c> <c>hop_limit</c> and the last committed state, was
    /// persisted before this was returned; resuming it trips again unless the resume raises the limit
    /// (<c>RunOptions.MaxNodeHops</c>). No recovery policy sees it.
    /// </summary>
    public sealed record HopLimitExceeded(string Node, int Hops, int Limit)
        : Failed($"hop limit of {Limit} reached: node '{Node}' would be hop {Hops + 1}");

    /// <summary>
    /// The run's <c>RunOptions.Scheduler</c> gave it no slot (<c>design/40-Run-Scheduler.md</c>):
    /// <paramref name="Reason"/> says why, after <paramref name="Waited"/>. The run never started: nothing was
    /// loaded or written and no run event or span was made, so the thread is as it was and the call can be made
    /// again.
    /// </summary>
    public sealed record NotAdmitted(NotAdmittedReason Reason, TimeSpan Waited) : Failed(MessageFor(Reason))
    {
        private static string MessageFor(NotAdmittedReason reason) => "run not admitted: " + reason switch
        {
            NotAdmittedReason.QueueFull => "the scheduler's queue is full",
            NotAdmittedReason.Shed => "shed from the scheduler's queue for a higher-priority run",
            NotAdmittedReason.QueueTimeout => "no slot within the scheduler's longest wait",
            NotAdmittedReason.Cancelled => "cancelled while waiting for a slot",
            NotAdmittedReason.Deadline => "its deadline passed while it waited for a slot",
            _ => "the scheduler is closed",
        };
    }

    /// <summary>
    /// The run was cancelled through its <see cref="CancellationToken"/>, or its
    /// <c>RunOptions.Deadline</c> passed (<c>design/17-Async-Execution.md</c>). A
    /// <c>CheckpointStatus.Cancelled</c> checkpoint was persisted before this was returned:
    /// <paramref name="Node"/> is the node that was running or about to start, and
    /// <paramref name="State"/> the last committed state, the one that node started with. Resuming the
    /// thread re-enters <paramref name="Node"/>. <paramref name="StoppedAt"/> is where the run stopped,
    /// one node per level from the outermost graph in, like <see cref="Interrupted.WaitingAt"/>.
    ///
    /// Deliberately not a <see cref="Failed"/>: a cancellation is not a failure, and no recovery
    /// policy sees it.
    ///
    /// <c>CompiledGraph.EndAsync</c> returns it too, with <see cref="CancelReason.Ended"/>
    /// (<c>design/28-Ending-A-Thread.md</c>): then the checkpoint is <c>ended</c>, <paramref name="Node"/>
    /// is where the thread stood, and the thread can't be resumed.
    ///
    /// With <see cref="CancelReason.LeaseLost"/> (<c>design/32-Thread-Leases.md</c>) no checkpoint was
    /// persisted: the run lost its thread's lease to another owner, and the thread's history is theirs.
    /// </summary>
    public sealed record Cancelled(
        string ThreadId,
        string Node,
        CancelReason Reason,
        TState State,
        IReadOnlyList<string> StoppedAt) : GraphExecutionResult<TState>;
}

/// <summary>Why a run ended <see cref="GraphExecutionResult{TState}.Cancelled"/>.</summary>
public enum CancelReason
{
    /// <summary>The caller's <see cref="CancellationToken"/> was cancelled.</summary>
    Requested,

    /// <summary>The run's <c>RunOptions.Deadline</c> passed.</summary>
    Deadline,

    /// <summary>
    /// The thread was ended by <c>CompiledGraph.EndAsync</c> (<c>design/28-Ending-A-Thread.md</c>): its
    /// latest checkpoint is <see cref="CheckpointStatus.Ended"/>, and it can't be resumed.
    /// </summary>
    Ended,

    /// <summary>
    /// The run lost its thread's lease (<c>RunOptions.Lease</c>, <c>design/32-Thread-Leases.md</c>): another
    /// owner acquired the thread, or no renewal succeeded for a whole ttl. The run stopped and wrote nothing
    /// more, not even a <c>cancelled</c> checkpoint: the thread is the new owner's.
    /// </summary>
    LeaseLost,
}
