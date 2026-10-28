using Datalium.Strata.Interruption;

namespace Datalium.Strata.Graph;

/// <summary>
/// A wait node as the builder declared it (<c>design/29-Event-Waits-And-Timers.md</c>): an event wait, with
/// its event type and optional timeout, or a timer, with a fixed delay or a time computed from the state.
/// </summary>
internal sealed record WaitSpec<TState>(
    WaitKind Kind,
    string? EventType,
    TimeSpan? Delay,
    Func<TState, DateTimeOffset>? WakeAt,
    TimeSpan? Timeout)
{
    /// <summary>
    /// The checkpoint <c>wait</c> of a run frozen at this node with <paramref name="state"/> at
    /// <paramref name="now"/>.
    /// </summary>
    public CheckpointWait At(TState state, DateTimeOffset now) => Kind switch
    {
        WaitKind.Timer => new CheckpointWait(
            WaitKind.Timer, EventType: null, WakeTimes.Text(WakeAt is { } at ? at(state) : now + Delay!.Value)),
        _ => new CheckpointWait(
            WaitKind.Event, EventType, Timeout is { } timeout ? WakeTimes.Text(now + timeout) : null),
    };

    /// <summary>This wait as the exported definition describes it.</summary>
    public GraphWait Exported() => new(
        Kind,
        EventType,
        Delay is { } delay ? (long)delay.TotalMilliseconds : null,
        Timeout is { } timeout ? (long)timeout.TotalMilliseconds : null);
}

/// <summary>How a <c>wake_at</c> is written (<c>design/29-Event-Waits-And-Timers.md</c>, "Precision").</summary>
internal static class WakeTimes
{
    /// <summary>
    /// <paramref name="time"/> rounded <b>up</b> to a whole second, as stored text, so a timer never fires
    /// early: <c>12:00:01.2</c> is written <c>12:00:02</c>.
    /// </summary>
    public static string Text(DateTimeOffset time)
    {
        var utc = time.ToUniversalTime();
        var floored = new DateTimeOffset(utc.Ticks - (utc.Ticks % TimeSpan.TicksPerSecond), TimeSpan.Zero);
        return CheckpointTimestamps.Text(floored == utc ? floored : floored.AddSeconds(1));
    }
}

/// <summary>
/// What <c>CompiledGraph.WakeDueAsync</c> wakes, and how it runs them
/// (<c>design/29-Event-Waits-And-Timers.md</c>). Mirrors the Rust engine's <c>WakeOptions</c>.
/// </summary>
public sealed record WakeOptions
{
    private static readonly IReadOnlyDictionary<string, string> NoMetadata = new Dictionary<string, string>();

    /// <summary>
    /// Only threads whose latest checkpoint's metadata has every pair. A graph with an identity lists only its
    /// own threads anyway (<see cref="MatchGraph"/>); a graph without one should put its name in every run's
    /// <see cref="RunOptions.Metadata"/> (<c>graph=investigation</c>) and wake with the same pair, since a store
    /// holds many graphs' threads.
    /// </summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = NoMetadata;

    /// <summary>
    /// Whether a graph with an identity lists only the threads it wrote (<c>design/31-Graph-Identity.md</c>): its
    /// id, and its version unless it has a migration. <see langword="true"/> by default. <see langword="false"/>
    /// lists by <see cref="Metadata"/> alone, as before identities, which also wakes the threads written before
    /// the graph had one; another graph's threads are still skipped. A graph without an identity ignores it.
    /// </summary>
    public bool MatchGraph { get; init; } = true;

    /// <summary>The most threads one call wakes or skips: 1 to <see cref="ThreadQuery.MaxLimit"/>.</summary>
    public int Limit { get; init; } = ThreadQuery.DefaultLimit;

    /// <summary>
    /// The options of every run the call starts (metadata, observer, history, services, hop limit). Its
    /// <see cref="RunOptions.TimeProvider"/> is the clock that decides what is due.
    /// </summary>
    public RunOptions Run { get; init; } = new();

    /// <summary>
    /// When it fires, the call wakes no more threads and returns what it did; the thread it is waking finishes.
    /// Unlike the call's own token, which also cancels that run. How a host stops a driver without leaving a
    /// thread cancelled (<c>design/35-Hosting-Integration.md</c>, "Stopping").
    /// </summary>
    public CancellationToken Stop { get; init; }
}

/// <summary>A thread <c>WakeDueAsync</c> woke, and what its run returned.</summary>
/// <param name="Reason"><see cref="WakeReason.Timer"/> or <see cref="WakeReason.Timeout"/>.</param>
/// <param name="Result">
/// The resumed run's result; for a timeout with no route, the end's (<c>Cancelled</c> with
/// <see cref="CancelReason.Ended"/>).
/// </param>
public sealed record WokenThread<TState>(string ThreadId, WakeReason Reason, GraphExecutionResult<TState> Result);

/// <summary>
/// A due thread <c>WakeDueAsync</c> left alone, with why: it stopped being due, another driver got there
/// first, or its wait isn't one of this graph's. Nothing was written to it.
/// </summary>
public sealed record SkippedThread(string ThreadId, string Reason);

/// <summary>What one <c>WakeDueAsync</c> call did, in the order it did it.</summary>
public sealed record WakeReport<TState>(IReadOnlyList<WokenThread<TState>> Woken, IReadOnlyList<SkippedThread> Skipped);
