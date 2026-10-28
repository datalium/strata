using Datalium.Strata.Memory;
using Datalium.Strata.Observability;
using Datalium.Strata.Scheduling;

namespace Datalium.Strata.Graph;

/// <summary>
/// Run-level inputs to <c>RunAsync</c> / <c>ResumeAsync</c> / <c>RunStreaming</c> /
/// <c>ResumeStreaming</c> (<c>design/17-Async-Execution.md</c>, <c>design/18-Run-Observability.md</c>),
/// so new ones don't keep lengthening those methods' parameter lists. The cancellation token is a
/// separate parameter, as is usual in .NET. Mirrors the Rust engine's <c>RunConfig</c>.
/// </summary>
public sealed class RunOptions
{
    /// <summary>Handed to every async handler as <see cref="NodeContext.History"/>, at every depth.</summary>
    public IHistoryStore? History { get; init; }

    /// <summary>Handed to every async handler as <see cref="NodeContext.Services"/>, at every depth.</summary>
    public IServiceProvider? Services { get; init; }

    /// <summary>
    /// When the run must stop. Once it passes, the run is cancelled exactly as if its token had been:
    /// it ends <c>Cancelled</c> with <see cref="Interruption.CancelReason.Deadline"/>. Absolute, so one
    /// deadline can cover a run, its freeze and its resume.
    /// </summary>
    public DateTimeOffset? Deadline { get; init; }

    /// <summary>
    /// The caller's metadata (<c>tenant</c>, <c>incident</c>, …), stamped on every checkpoint the run
    /// writes, at every nesting level, and handed to every async handler as
    /// <see cref="NodeContext.Metadata"/>. A resume carries forward the metadata of the checkpoint it
    /// resumes, with this laid over it key by key.
    ///
    /// The checkpoint's <c>metadata</c> map also holds the engine's keys, so <c>error</c>,
    /// <c>error_kind</c>, <c>attempts</c>, <c>cancel_reason</c>, every key starting <c>strata.</c>, and
    /// an empty key are refused: the run throws <see cref="ArgumentException"/> before it starts. The
    /// run copies the dictionary when it starts.
    /// </summary>
    public Dictionary<string, string> Metadata { get; init; } = [];

    /// <summary>
    /// The id the run's events, spans and <see cref="NodeContext.RunId"/> carry; a fresh UUID v7 per
    /// call when not set. The engine doesn't check that it is unique.
    /// </summary>
    public string? RunId { get; init; }

    /// <summary>
    /// Receives the run's events (<see cref="RunEvent"/>), in every run mode. It can't stall or fail
    /// the run: see <see cref="IRunObserver"/>.
    /// </summary>
    public IRunObserver? Observer { get; init; }

    /// <summary>
    /// Replaces the graph's compiled <see cref="CompilationOptions.MaxNodeHops"/> for this run or resume,
    /// higher or lower, or sets one on a graph compiled without (<c>design/24-Loop-Circuit-Breaker.md</c>).
    /// It applies to the outermost graph only: a subgraph's child keeps its own. It isn't stored, so the
    /// next resume uses the compiled limit unless it passes this again. Must be at least 1: the run
    /// throws <see cref="ArgumentException"/> before it starts otherwise.
    /// </summary>
    public int? MaxNodeHops { get; init; }

    /// <summary>
    /// The clock for every wait the engine makes in this run (<c>design/25-Retry-Backoff.md</c>): the
    /// backoff between retries, node timeouts, and <see cref="Deadline"/>. <see langword="null"/>:
    /// <see cref="TimeProvider.System"/>. A test passes a fake one to run those waits without waiting.
    /// Timestamps on checkpoints and run events stay wall-clock.
    /// </summary>
    public TimeProvider? TimeProvider { get; init; }

    /// <summary>
    /// Runs the thread under a lease (<c>design/32-Thread-Leases.md</c>): the run, resume, signal, wake or end
    /// acquires the thread's lease from the store before its first save, renews it on
    /// <see cref="TimeProvider"/>, saves under it, and releases it when it returns. A thread another owner
    /// holds is refused with <c>Failed</c>; losing the lease ends the run <c>Cancelled</c> with
    /// <see cref="Interruption.CancelReason.LeaseLost"/>, with nothing more written. <see langword="null"/>:
    /// no lease, as before. A blank owner or a ttl below one millisecond is refused before the run starts
    /// (<see cref="ArgumentException"/>); a store without leases throws <see cref="NotSupportedException"/>.
    /// </summary>
    public LeaseOptions? Lease { get; init; }

    /// <summary>
    /// Waits for a slot of this scheduler before the run loads or writes anything, and holds it until the run
    /// returns (<c>design/40-Run-Scheduler.md</c>). A run it doesn't admit returns
    /// <see cref="Interruption.GraphExecutionResult{TState}.NotAdmitted"/> with nothing loaded or written. The run's
    /// tenant is its <see cref="Metadata"/>'s <see cref="RunSchedulerOptions.TenantKey"/>. Ending a thread takes no
    /// slot. <see langword="null"/>: the run starts at once, as before.
    /// </summary>
    public RunScheduler? Scheduler { get; init; }

    /// <summary>The run's priority for <see cref="Scheduler"/>: higher first. 0 by default; any value, negative too.</summary>
    public int Priority { get; init; }
}
