namespace Datalium.Strata.Interruption;

/// <summary>
/// The human's answers to one <c>ResumeAsync</c>: a <see cref="HumanSignal"/> for the innermost graph
/// (what a bare <see cref="HumanSignal"/> has always meant), plus optionally one for each outer level,
/// when the thread froze inside subgraphs (<c>design/16-Nested-Interrupts.md</c>).
///
/// A level is addressed by the names of the subgraph nodes leading to it from the graph
/// <c>ResumeAsync</c> is called on: <c>[]</c> is that graph itself, <c>["Work"]</c> the child running
/// inside its <c>Work</c> node, <c>["Work", "Inner"]</c> that child's child. A signal for an outer
/// level applies its field overrides to the state the level's subgraph node started with, before the
/// node re-enters its child, so they reach the <c>output</c> mapper that folds the child's result
/// back. Only its overrides are used there: an outer level isn't waiting at a boundary of its own, so
/// its signal records no <see cref="HumanDecision"/> and takes no reject route
/// (<c>design/21-Typed-Human-Signals.md</c>).
///
/// A level the checkpoint doesn't have, or the innermost level addressed twice (it already has the
/// signal passed to the constructor), refuses the resume before anything is changed. Mirrors
/// <c>rust/strata-core/src/checkpoint.rs</c>'s <c>LevelSignals</c>.
/// </summary>
public sealed class LevelSignals
{
    private readonly List<(string[] Path, HumanSignal Signal)> _outer;

    /// <summary>Signals with <paramref name="innermost"/> for the deepest level the thread froze at.</summary>
    public LevelSignals(HumanSignal innermost)
        : this(innermost, [], decidedAt: null)
    {
    }

    private LevelSignals(
        HumanSignal innermost,
        List<(string[] Path, HumanSignal Signal)> outer,
        string? decidedAt,
        WakeReason wake = WakeReason.Human,
        string? eventType = null,
        bool whenDue = false)
    {
        ArgumentNullException.ThrowIfNull(innermost);
        Innermost = innermost;
        _outer = outer;
        DecidedAt = decidedAt;
        Wake = wake;
        EventType = eventType;
        WhenDue = whenDue;
    }

    /// <summary>
    /// Wakes a thread that is due (<c>design/42-Waking-Through-A-Resume.md</c>), through
    /// <c>ResumeAsync</c> or <c>ResumeStreaming</c>: what <c>WakeDueAsync</c> does for one thread. The resume
    /// decides at its own load, on the run's <see cref="Graph.RunOptions.TimeProvider"/>, that the thread is
    /// due: a timer whose time has come continues at the edge out of it, and a wait for a human or an event
    /// whose timeout has passed takes its timeout route or, with none, is ended with <c>strata.end_reason</c>
    /// <c>timed_out</c> (the result is <c>Cancelled</c> with <c>CancelReason.Ended</c>). A thread that isn't
    /// due, that waits with no time, or whose wait isn't one of this graph's is refused with <c>Failed</c>
    /// before anything is written. The stamp and the end are conditional, so this resume and a wake driver
    /// can't both wake one thread.
    /// </summary>
    public static LevelSignals Due { get; } =
        new(new HumanSignal(true, string.Empty), [], decidedAt: null, whenDue: true);

    /// <summary>
    /// Answers a thread waiting for <paramref name="signal"/>'s event, through <c>ResumeAsync</c> or
    /// <c>ResumeStreaming</c>, as <c>SignalAsync</c> does (<c>design/42-Waking-Through-A-Resume.md</c>): its
    /// overrides go to the waiting level's state, and it records no decision.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="signal"/> has no event type.</exception>
    public static LevelSignals Event(EventSignal signal)
    {
        ArgumentNullException.ThrowIfNull(signal);
        ArgumentException.ThrowIfNullOrWhiteSpace(signal.EventType, nameof(signal));
        return ForWake(WakeReason.Event, signal);
    }

    internal HumanSignal Innermost { get; }

    /// <summary>
    /// What answers the innermost level (<c>design/29-Event-Waits-And-Timers.md</c>): a human (the
    /// default), an event signal, a due timer, or a timeout. Only a human's answer records a decision.
    /// </summary>
    internal WakeReason Wake { get; }

    /// <summary>The event type, when <see cref="Wake"/> is <see cref="WakeReason.Event"/>.</summary>
    internal string? EventType { get; }

    /// <summary>
    /// Whether these are <see cref="Due"/>, not yet decided: the outermost resume replaces them with the wake
    /// the thread is due for (<see cref="WhenDueAt"/>), or refuses it.
    /// </summary>
    internal bool WhenDue { get; }

    /// <summary>
    /// What <paramref name="checkpoint"/>, a thread's latest, is due for at <paramref name="due"/> (whole
    /// seconds): a timer's wake or a timeout, by the stored wait; <see langword="null"/> when it isn't due.
    /// </summary>
    internal static WakeReason? DueFor(Checkpoint checkpoint, string due)
    {
        if (!ThreadQuery.IsDue(checkpoint, due))
        {
            return null;
        }

        return checkpoint.InnermostWait()!.Kind == WaitKind.Timer ? WakeReason.Timer : WakeReason.Timeout;
    }

    /// <summary>
    /// For <see cref="Due"/>: the wake <paramref name="checkpoint"/> is due for at <paramref name="due"/>, or
    /// these signals unchanged when it isn't due (the resume then refuses them, after its own refusals).
    /// </summary>
    internal LevelSignals WhenDueAt(Checkpoint checkpoint, string due) =>
        WhenDue && DueFor(checkpoint, due) is { } wake ? new(Innermost, _outer, DecidedAt, wake) : this;

    /// <summary>Why <paramref name="checkpoint"/>, which <see cref="Due"/> found not due, isn't.</summary>
    internal static string NotDue(string threadId, Checkpoint checkpoint)
    {
        if (checkpoint.Status != CheckpointStatus.Interrupted)
        {
            return $"thread '{threadId}' is not due: its latest checkpoint is {CheckpointThreadStates.Of(checkpoint).Name()}";
        }

        var wait = checkpoint.InnermostWait();
        var at = $"[{string.Join('/', checkpoint.WaitingAt())}]";
        return wait?.WakeAt is { } wakeAt
            ? $"thread '{threadId}' is not due until {wakeAt}: it is waiting for {wait.Describe()} at {at}"
            : $"thread '{threadId}' is not due: it is waiting for {wait?.Describe() ?? "a human"} at {at}, with no timeout";
    }

    /// <summary>
    /// Signals for a wake that isn't a human's: the event's overrides (as an innermost signal that approves,
    /// with no reviewer), or none for a timer or a timeout.
    /// </summary>
    internal static LevelSignals ForWake(WakeReason wake, EventSignal? signal = null) =>
        new(
            new HumanSignal(true, string.Empty) { Overrides = signal?.Overrides },
            [],
            decidedAt: null,
            wake,
            signal?.EventType);

    /// <summary>
    /// Signals that answer nothing and override nothing: what a recovery continues a thread with
    /// (<c>design/33-Durable-Execution.md</c>).
    /// </summary>
    internal static LevelSignals None => ForWake(WakeReason.Human);

    /// <summary>
    /// When the resume these signals belong to began: the <c>DecidedAt</c> of the decision it records,
    /// the same at every level and on every retry. Set once by the outermost resume.
    /// </summary>
    internal string? DecidedAt { get; }

    /// <summary>These signals, stamped with the time the resume began.</summary>
    internal LevelSignals At(string decidedAt) => new(Innermost, _outer, decidedAt, Wake, EventType, WhenDue);

    /// <summary>Every signal these carry, the innermost first.</summary>
    internal IEnumerable<HumanSignal> All() => _outer.Select(entry => entry.Signal).Prepend(Innermost);

    /// <summary>The signal addressed to the level being resumed itself, if any.</summary>
    internal HumanSignal? Own => _outer.FirstOrDefault(entry => entry.Path.Length == 0).Signal;

    /// <summary>These signals plus one for the level at <paramref name="path"/> (see the type's docs).</summary>
    public LevelSignals At(IReadOnlyList<string> path, HumanSignal signal)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(signal);
        var key = path.ToArray();
        var outer = _outer.Where(entry => !entry.Path.SequenceEqual(key)).ToList();
        outer.Add((key, signal));
        return new LevelSignals(Innermost, outer, DecidedAt, Wake, EventType, WhenDue);
    }

    /// <summary>The signals for the level inside <paramref name="node"/>, addressed relative to it.</summary>
    internal LevelSignals Descend(string? node) =>
        new(
            Innermost,
            _outer
                .Where(entry => entry.Path.Length > 0 && entry.Path[0] == node)
                .Select(entry => (entry.Path[1..], entry.Signal))
                .ToList(),
            DecidedAt,
            Wake,
            EventType,
            WhenDue);

    /// <summary>What is wrong with these signals for <paramref name="checkpoint"/>, or <see langword="null"/>.</summary>
    internal string? Check(Checkpoint checkpoint)
    {
        // Only a human's resume addresses outer levels (design/42-Waking-Through-A-Resume.md).
        if (_outer.Count > 0 && (Wake != WakeReason.Human || WhenDue))
        {
            return "a wake or an event signal answers the innermost level only, and takes no signals for outer levels";
        }

        var levels = new List<string[]> { Array.Empty<string>() };
        for (var level = checkpoint; level.Nested is not null; level = level.Nested)
        {
            levels.Add([.. levels[^1], level.NextNode ?? level.SuspendedNode ?? string.Empty]);
        }

        var innermost = levels[^1];
        foreach (var (path, _) in _outer.OrderBy(entry => string.Join('', entry.Path), StringComparer.Ordinal))
        {
            if (path.SequenceEqual(innermost))
            {
                return $"a signal was addressed to level {Show(path)}, which is the innermost level and already has the innermost signal";
            }

            if (!levels.Any(level => level.SequenceEqual(path)))
            {
                return $"a signal was addressed to level {Show(path)}, but the checkpoint's levels are {string.Join(", ", levels.Select(Show))}";
            }
        }

        return null;
    }

    private static string Show(string[] path) => path.Length == 0 ? "[]" : $"[{string.Join('/', path)}]";
}
