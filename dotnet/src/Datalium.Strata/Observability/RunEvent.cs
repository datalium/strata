using Datalium.Strata.Interruption;

namespace Datalium.Strata.Observability;

/// <summary>
/// Something that happened during a run, as an <see cref="IRunObserver"/> sees it
/// (<c>design/18-Run-Observability.md</c>). Mirrors the Rust engine's <c>RunEvent</c>.
/// </summary>
/// <remarks>
/// Only the engine creates events, so a <c>switch</c> over them needs a default arm for the event
/// types later versions add (a map's <c>MapExpanded</c>, for one).
/// </remarks>
public abstract record RunEvent
{
    private protected RunEvent()
    {
    }

    /// <summary>The caller's thread id, at every depth.</summary>
    public string ThreadId { get; internal set; } = string.Empty;

    /// <summary>The id of the <c>RunAsync</c> / <c>ResumeAsync</c> call this event belongs to.</summary>
    public string RunId { get; internal set; } = string.Empty;

    /// <summary>1, 2, 3, … per run, with no gaps. Events are delivered in this order.</summary>
    public long Sequence { get; internal set; }

    /// <summary>When the engine recorded the event.</summary>
    public DateTimeOffset Timestamp { get; internal set; }

    /// <summary>
    /// The node's qualified path (<c>"Work/Send"</c>, <c>"Gather/Acme"</c>), as
    /// <c>NodeContext.NodePath</c> gives it; <see langword="null"/> on run-level events.
    /// </summary>
    public string? NodePath { get; internal set; }

    /// <summary>
    /// The node's attempt at that moment (a branch reports its group's); <see langword="null"/> on
    /// run-level events.
    /// </summary>
    public int? Attempt { get; internal set; }
}

/// <summary>Whether a run started from the entry point, resumed a checkpoint, or ended a thread.</summary>
public enum RunMode
{
    Run,
    Resume,

    /// <summary><c>CompiledGraph.EndAsync</c> (<c>design/28-Ending-A-Thread.md</c>): no node runs.</summary>
    End,

    /// <summary>
    /// <c>CompiledGraph.RecoverAsync</c> / <c>RecoverStaleAsync</c> (<c>design/33-Durable-Execution.md</c>): a
    /// running thread whose run died continues where its last checkpoint says.
    /// </summary>
    Recover,
}

/// <summary>How a run ended: the kind of its <c>GraphExecutionResult</c>.</summary>
public enum RunOutcome
{
    Completed,
    Interrupted,
    Failed,
    Cancelled,
}

/// <summary>How the engine finished with a node.</summary>
public enum NodeResult
{
    /// <summary>The node completed and its state was committed.</summary>
    Committed,

    /// <summary>The node failed: its policy had nothing left to try, it was refused, or it threw a bug.</summary>
    Failed,

    /// <summary>A subgraph node whose child froze at an interrupt boundary.</summary>
    Interrupted,

    /// <summary>The run was cancelled while the node ran.</summary>
    Cancelled,
}

/// <summary>How a parallel group's branch ended.</summary>
public enum BranchResult
{
    Succeeded,
    Failed,

    /// <summary>The group gave up on it (a sibling failed, it lost a race, the run was cancelled, the group timed out).</summary>
    Cancelled,
}

/// <summary>
/// Why a branch ended as it did, when the result alone doesn't say
/// (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>).
/// </summary>
public enum BranchEndReason
{
    /// <summary>On a <see cref="BranchResult.Failed"/> branch: it ran over its group's <c>BranchTimeout</c>.</summary>
    TimedOut,

    /// <summary>On a <see cref="BranchResult.Cancelled"/> branch: its group's join was met without it.</summary>
    LostRace,

    /// <summary>On a <see cref="BranchResult.Cancelled"/> branch: its group failed because of a sibling and gave up on it.</summary>
    SiblingFailed,
}

/// <summary>The first event of every run. For a resume it comes after the checkpoint is loaded.</summary>
/// <param name="Node">
/// The node the run starts at: the entry point, or the checkpoint's next node. When ending a thread,
/// where it stands: its checkpoint's suspended node.
/// </param>
/// <param name="Metadata">The run's metadata (for a resume, the stored metadata merged with the resume's).</param>
public sealed record RunStarted(RunMode Mode, string Node, IReadOnlyDictionary<string, string> Metadata) : RunEvent
{
    /// <summary>
    /// How long the run waited for its scheduler's slot (<c>design/40-Run-Scheduler.md</c>), <see langword="null"/>
    /// when it has no scheduler.
    /// </summary>
    public TimeSpan? QueuedFor { get; init; }

    /// <summary>
    /// The identity of the graph running (<c>design/31-Graph-Identity.md</c>), <see langword="null"/> when it has
    /// none. For a resume, the graph resuming, which after a migration is not the version that wrote the thread.
    /// </summary>
    public GraphIdentity? Graph { get; init; }
}

/// <summary>A resume, after the checkpoint it resumes was stamped <c>resumed_at</c>.</summary>
/// <param name="From">The status of the checkpoint resumed.</param>
/// <param name="WaitingAt">Where that checkpoint was waiting, one node per level.</param>
/// <param name="IsApproved">The innermost signal's answer (<c>design/21-Typed-Human-Signals.md</c>).</param>
/// <param name="Reviewer">
/// The innermost signal's reviewer: who resumed. On a failed or cancelled thread's resume this is the
/// operator, while <c>NodeContext.Decision</c> keeps whoever answered the boundary.
/// </param>
/// <param name="RejectedTo">The reject route this resume took, if it took one; overrides are not included, like state.</param>
public sealed record Resumed(
    CheckpointStatus From, IReadOnlyList<string> WaitingAt, bool IsApproved, string Reviewer, string? RejectedTo) : RunEvent
{
    /// <summary>
    /// What woke the thread (<c>design/29-Event-Waits-And-Timers.md</c>): <see cref="WakeReason.Human"/> for
    /// <c>ResumeAsync</c>, <see cref="WakeReason.Event"/> for a signal, <see cref="WakeReason.Timer"/> or
    /// <see cref="WakeReason.Timeout"/> for <c>WakeDueAsync</c>. For anything but a human, <c>IsApproved</c>
    /// is <see langword="true"/> and <c>Reviewer</c> is empty.
    /// </summary>
    public WakeReason Wake { get; init; }

    /// <summary>The signal's event type, when <see cref="Wake"/> is <see cref="WakeReason.Event"/>.</summary>
    public string? EventType { get; init; }

    /// <summary>The timeout route this resume took, when <see cref="Wake"/> is <see cref="WakeReason.Timeout"/>.</summary>
    public string? TimedOutTo { get; init; }
}

/// <summary>
/// A recovery, after its claim was saved (<c>design/33-Durable-Execution.md</c>): the thread's <c>running</c>
/// checkpoint written again by this run, which nobody else had changed. Not recorded for a recovery that was
/// refused.
/// </summary>
/// <param name="At">
/// Where the thread continues, one node per level from the outermost graph in: each level's next node. The
/// innermost is the node that was in flight when the process died, which runs again.
/// </param>
/// <param name="Recoveries">
/// How many times the thread has been recovered from this checkpoint, this one included: recoveries since a
/// node last committed.
/// </param>
public sealed record Recovered(IReadOnlyList<string> At, int Recoveries) : RunEvent;

/// <summary>A node is about to run its first attempt.</summary>
public sealed record NodeStarted : RunEvent;

/// <summary>An attempt failed transiently and the node will run again. <c>Attempt</c> is the one that failed.</summary>
/// <param name="Delay">
/// The wait before the next attempt: the policy's backoff delay, or the failure's capped retry hint; zero
/// when the policy has no backoff (<c>design/25-Retry-Backoff.md</c>).
/// </param>
public sealed record RetryScheduled(string Error, bool TimedOut, TimeSpan Delay) : RunEvent;

/// <summary>A parallel group launched a branch.</summary>
public sealed record BranchStarted : RunEvent;

/// <summary>A branch settled, or its group gave up on it.</summary>
public sealed record BranchCompleted(BranchResult Result, TimeSpan Duration, string? Error) : RunEvent
{
    /// <summary>
    /// Why, when the result alone doesn't say (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>):
    /// a branch timeout, a lost race, or a sibling's failure. <see langword="null"/> for a success, an
    /// ordinary failure, and a branch stopped by a cancelled run or its group's own timeout.
    /// </summary>
    public BranchEndReason? Reason { get; init; }
}

/// <summary>
/// A parallel group whose join isn't <c>All</c> decided it, once per attempt, at the group's path
/// (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>). It comes after the deciding branch's
/// <see cref="BranchCompleted"/> and before the losers'.
/// </summary>
/// <param name="Policy">The join: <c>first_success</c>, <c>first_matching</c> or <c>quorum</c>.</param>
/// <param name="Met">Whether it was met; false when it could no longer be.</param>
/// <param name="Deciding">The branches that met it, in the order they did; empty when it wasn't met.</param>
public sealed record JoinDecided(string Policy, bool Met, IReadOnlyList<string> Deciding) : RunEvent;

/// <summary>
/// A map read its items, once per attempt, at the map's path, after their keys were checked and before
/// any item started (<c>design/27-Map-Fan-Out.md</c>). Each item then reports as a branch, at
/// <c>"&lt;map&gt;/&lt;key&gt;"</c>, and its child's nodes at <c>"&lt;map&gt;/&lt;key&gt;/&lt;node&gt;"</c>.
/// </summary>
/// <param name="Keys">The items' keys, in list order; empty for an empty list.</param>
public sealed record MapExpanded(IReadOnlyList<string> Keys) : RunEvent;

/// <summary>
/// The engine is finished with a node, however it ended. Every <see cref="NodeStarted"/> gets exactly
/// one. <c>Attempt</c> is the last attempt made.
/// </summary>
/// <param name="Duration">From <see cref="NodeStarted"/>, every attempt included.</param>
/// <param name="Fallback">The node the policy falls back to, when it does.</param>
public sealed record NodeCompleted(NodeResult Result, TimeSpan Duration, string? Error, string? Fallback) : RunEvent;

/// <summary>A checkpoint was written to the caller's store.</summary>
/// <param name="WaitingAt">The checkpoint's suspended node at every level.</param>
/// <param name="ResumeStamp">Whether this was a resume stamping the old checkpoint <c>resumed_at</c>.</param>
public sealed record CheckpointSaved(
    CheckpointStatus Status, IReadOnlyList<string> WaitingAt, bool ResumeStamp, TimeSpan Duration) : RunEvent;

/// <summary>The run froze at an interrupt boundary, and the checkpoint is saved.</summary>
/// <param name="NextNode">The node a resume continues at (for a nested freeze, the subgraph node).</param>
public sealed record Interrupted(IReadOnlyList<string> WaitingAt, string NextNode) : RunEvent
{
    /// <summary>
    /// What the innermost level waits for, when it is more than a human with no timeout
    /// (<c>design/29-Event-Waits-And-Timers.md</c>): an event, a timer, or a human with a timeout, and when it
    /// wakes or times out.
    /// </summary>
    public CheckpointWait? Wait { get; init; }
}

/// <summary>
/// The loop circuit breaker tripped at a level (<c>design/24-Loop-Circuit-Breaker.md</c>): <c>NodePath</c>
/// is the node that would have been hop <paramref name="Hops"/> + 1, which never started, and there is no
/// <c>Attempt</c>. Recorded at whichever level tripped, so a subgraph's child's is visible too.
/// </summary>
/// <param name="Hops">The hops the level had made.</param>
/// <param name="Limit">The limit in effect at that level.</param>
public sealed record HopLimitExceeded(int Hops, int Limit) : RunEvent;

/// <summary>
/// The run acquired its thread's lease (<c>RunOptions.Lease</c>, <c>design/32-Thread-Leases.md</c>), before
/// its first save. Not recorded when the thread is held: that run fails.
/// </summary>
/// <param name="Token">The lease's fencing token: the thread's count of acquisitions.</param>
/// <param name="ExpiresAt">When it expires unless renewed, on the store's clock.</param>
public sealed record LeaseAcquired(string Owner, long Token, DateTimeOffset ExpiresAt) : RunEvent;

/// <summary>
/// The run lost its thread's lease, once, when it found out: a renewal or a save was refused, or no renewal
/// succeeded for a whole ttl. The run stops and writes nothing more; <see cref="RunCompleted"/> follows with
/// <see cref="CancelReason.LeaseLost"/>.
/// </summary>
public sealed record LeaseLost(string Owner, long Token) : RunEvent;

/// <summary>The last event of every run.</summary>
/// <param name="Error">The failure message, when the outcome is <see cref="RunOutcome.Failed"/>.</param>
/// <param name="CancelReason">Why, when the outcome is <see cref="RunOutcome.Cancelled"/>.</param>
public sealed record RunCompleted(RunOutcome Outcome, TimeSpan Duration, string? Error, CancelReason? CancelReason)
    : RunEvent;

/// <summary>How a tool call a <c>ToolRegistry</c> dispatched ended.</summary>
public enum ToolCallResult
{
    Succeeded,

    /// <summary>The tool threw, or its arguments didn't parse.</summary>
    Failed,

    /// <summary>Its token was cancelled before it started, or it threw <see cref="OperationCanceledException"/> for that token.</summary>
    Cancelled,

    /// <summary>No such tool, or one the registry's subset doesn't permit.</summary>
    Refused,
}

/// <summary>
/// A <c>ToolRegistry</c> received a tool call whose context came from a node
/// (<c>ToolCallContext.From</c>), before resolving its name (<c>design/20-Tool-Dispatch-And-Registry.md</c>).
/// <c>NodePath</c> and <c>Attempt</c> are the calling node's. The arguments are not included: they can hold secrets.
/// </summary>
public sealed record ToolCallStarted(string Tool, string CallId, string? CallerId) : RunEvent;

/// <summary>
/// That call ended, however it ended. Every <see cref="ToolCallStarted"/> gets exactly one. The result is not included.
/// </summary>
/// <param name="Error">The message, when the result is <see cref="ToolCallResult.Failed"/> or <see cref="ToolCallResult.Refused"/>.</param>
public sealed record ToolCallCompleted(string Tool, string CallId, ToolCallResult Result, TimeSpan Duration, string? Error)
    : RunEvent;

/// <summary>How a model call (<see cref="ModelCallScope"/>) ended.</summary>
public enum ModelCallResult
{
    Succeeded,

    /// <summary>The provider refused the request or failed, or the call couldn't be made.</summary>
    Failed,

    /// <summary>The caller's token stopped it.</summary>
    Cancelled,
}

/// <summary>
/// The tokens a model call used, as its provider reported them (<c>design/44-LLM-Integration.md</c>); a count the
/// provider didn't report is <see langword="null"/>, never estimated.
/// </summary>
public sealed record ModelUsage(long? InputTokens, long? OutputTokens)
{
    /// <summary>Nothing reported.</summary>
    public static ModelUsage None { get; } = new(null, null);

    /// <summary>Both counts, when either is known.</summary>
    public long? TotalTokens => InputTokens is null && OutputTokens is null ? null : (InputTokens ?? 0) + (OutputTokens ?? 0);
}

/// <summary>
/// A node made a model call (<c>design/44-LLM-Integration.md</c>), recorded through a <see cref="ModelCallScope"/>
/// started with the node's context. <c>NodePath</c> and <c>Attempt</c> are the calling node's. The prompt is not included.
/// </summary>
/// <param name="Model">The model asked for, as the request named it.</param>
/// <param name="CallId">Pairs this event with its <see cref="ModelCallCompleted"/> (a UUID v7).</param>
/// <param name="Provider">The provider (<c>openai</c>, <c>anthropic</c>, …), when the client says.</param>
/// <param name="Route">
/// The model route the call is an attempt of (<c>design/46-Model-Routes.md</c>), <see langword="null"/> outside a route.
/// </param>
/// <param name="Operation">
/// The GenAI operation (<see cref="ModelCallInfo.Operation"/>): <c>chat</c>, or <c>embeddings</c> for an embedding
/// call (<c>design/45-Long-Term-Memory.md</c>).
/// </param>
public sealed record ModelCallStarted(
    string Model, string CallId, string? Provider, string? Route = null, string Operation = "chat")
    : RunEvent;

/// <summary>
/// That call ended, however it ended. Every <see cref="ModelCallStarted"/> gets exactly one. The answer is not included.
/// </summary>
/// <param name="FinishReason">Why the model stopped (<c>stop</c>, <c>length</c>, <c>tool_calls</c>, …), when it succeeded.</param>
/// <param name="Error">The message, when the result is <see cref="ModelCallResult.Failed"/>.</param>
/// <param name="Route">The model route the call is an attempt of, <see langword="null"/> outside a route.</param>
/// <param name="Operation">The GenAI operation, as on <see cref="ModelCallStarted"/>.</param>
public sealed record ModelCallCompleted(
    string Model,
    string CallId,
    ModelCallResult Result,
    ModelUsage Usage,
    string? FinishReason,
    TimeSpan Duration,
    string? Error,
    string? Route = null,
    string Operation = "chat")
    : RunEvent;

/// <summary>What happened to one candidate of a model route in one call through it (<c>design/46-Model-Routes.md</c>).</summary>
public enum ModelRouteOutcome
{
    /// <summary>Its answer is the call's.</summary>
    Answered,

    /// <summary>It failed: a transient fault the route moved on from, or a fatal one that stopped the route.</summary>
    Failed,

    /// <summary>It answered with the finish reason <c>content_filter</c>.</summary>
    Refused,

    /// <summary>It answered, and the route's <c>Accept</c> rejected the answer.</summary>
    Rejected,

    /// <summary>Its eligibility check gave a reason not to ask it. It was never asked.</summary>
    Ineligible,

    /// <summary>Its health key had a provider's <c>Retry-After</c> in force. It was never asked.</summary>
    CoolingDown,

    /// <summary>Its health key's breaker was open, or half-open with its one test already in flight. It was never asked.</summary>
    BreakerOpen,

    /// <summary>It was running when another candidate won, and was cancelled.</summary>
    LostRace,

    /// <summary>The route ended before its turn came.</summary>
    NotTried,

    /// <summary>It was running when the call was cancelled, or when another candidate's fatal fault stopped the route.</summary>
    Cancelled,
}

/// <summary>One candidate's part in a call through a model route.</summary>
/// <param name="Candidate">The candidate's name, unique within its route.</param>
/// <param name="Reason">
/// Why, when the outcome has one: a failure's error type (<c>429</c>, <c>connection</c>, <c>stream_broken</c>), the
/// reason <c>Accept</c> or the eligibility check gave, or how long a cooldown or an open breaker had left
/// (<c>12 s</c>).
/// </param>
/// <param name="CallId">
/// The candidate's model call (<see cref="ModelCallStarted.CallId"/>), when it was asked; <see langword="null"/> for a
/// candidate that never was.
/// </param>
public sealed record ModelRouteAttempt(string Candidate, ModelRouteOutcome Outcome, string? Reason, string? CallId);

/// <summary>
/// A call through a model route ended (<c>design/46-Model-Routes.md</c>), once, after its last attempt: which candidate
/// answered and what happened to every candidate. Each attempt is a model call of its own, with its own
/// <see cref="ModelCallStarted"/> / <see cref="ModelCallCompleted"/> naming the route. <c>NodePath</c> and
/// <c>Attempt</c> are the calling node's.
/// </summary>
/// <param name="Route">The route's name.</param>
/// <param name="CallId">The route call's id (a UUID v7), also on its <c>route &lt;name&gt;</c> span.</param>
/// <param name="Answered">The candidate whose answer is the call's, <see langword="null"/> when the route failed.</param>
/// <param name="Model">The model that answered, <see langword="null"/> when none did.</param>
/// <param name="Attempts">Every candidate, once each, in the route's order.</param>
/// <param name="Hedges">How many candidates started because the one before was slow (<c>HedgeAfter</c>).</param>
/// <param name="Duration">From the call's start to its last attempt's end.</param>
public sealed record ModelRouted(
    string Route,
    string CallId,
    string? Answered,
    string? Model,
    IReadOnlyList<ModelRouteAttempt> Attempts,
    int Hedges,
    TimeSpan Duration)
    : RunEvent;
