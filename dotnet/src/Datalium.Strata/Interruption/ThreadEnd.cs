namespace Datalium.Strata.Interruption;

/// <summary>
/// Who ends a thread with <c>CompiledGraph.EndAsync</c> (<c>design/28-Ending-A-Thread.md</c>). Mirrors
/// the Rust engine's <c>ThreadEnd</c>.
/// </summary>
public sealed record ThreadEnd
{
    /// <summary>The checkpoint metadata key the end's reason is recorded under.</summary>
    public const string ReasonKey = "strata.end_reason";

    /// <summary>The checkpoint metadata key <see cref="EndedBy"/> is recorded under.</summary>
    public const string EndedByKey = "strata.ended_by";

    /// <summary>The reason a caller's end records: it was asked for.</summary>
    public const string Requested = "requested";

    /// <summary>
    /// The reason an end records when a wait timed out with no timeout route
    /// (<c>design/29-Event-Waits-And-Timers.md</c>). Only the engine ends a thread with it.
    /// </summary>
    public const string TimedOut = "timed_out";

    /// <summary>
    /// Who ended it, when known: an operator, a service, a reviewer. Recorded as <see cref="EndedByKey"/>.
    /// </summary>
    public string? EndedBy { get; init; }

    /// <summary>Why: <see cref="Requested"/> for every caller's end, <see cref="TimedOut"/> for the engine's.</summary>
    internal string Reason { get; init; } = Requested;

    /// <summary>An end by <paramref name="endedBy"/>.</summary>
    public static ThreadEnd By(string endedBy) => new() { EndedBy = endedBy };
}
