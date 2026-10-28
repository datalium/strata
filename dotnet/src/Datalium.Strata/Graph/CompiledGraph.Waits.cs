using System.Text.Json;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;

namespace Datalium.Strata.Graph;

/// <summary>Event waits and durable timers (<c>design/29-Event-Waits-And-Timers.md</c>, SG-16).</summary>
public sealed partial class CompiledGraph<TState>
{
    /// <summary>
    /// Answers a thread waiting for <paramref name="signal"/>'s event at a wait node
    /// (<see cref="StateGraph{TState}.AddWaitForEvent"/>), at any depth: applies the signal's overrides to the
    /// waiting level's state and continues at the edge out of the wait node, like a resume. It records no
    /// decision: the level keeps the one it had.
    ///
    /// A thread that isn't waiting for that event type (waiting for another, for a human, for a timer, or not
    /// waiting at all) is refused with <see cref="GraphExecutionResult{TState}.Failed"/> before anything is
    /// written, as is everything a resume refuses. The resume stamp is conditional, so a signal racing a
    /// timeout, an end or another signal can't both win.
    /// </summary>
    public Task<GraphExecutionResult<TState>> SignalAsync(
        string threadId, EventSignal signal, ICheckpointStore store, CancellationToken cancellationToken = default) =>
        SignalAsync(threadId, signal, store, NoOptions, cancellationToken);

    /// <inheritdoc cref="SignalAsync(string, EventSignal, ICheckpointStore, CancellationToken)"/>
    /// <exception cref="ArgumentException"><paramref name="options"/> has a reserved metadata key.</exception>
    public async Task<GraphExecutionResult<TState>> SignalAsync(
        string threadId,
        EventSignal signal,
        ICheckpointStore store,
        RunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(signal);
        ArgumentException.ThrowIfNullOrWhiteSpace(signal.EventType, nameof(signal));
        ArgumentNullException.ThrowIfNull(options);
        RunMetadata.Validate(options);
        using var admission = await AdmitAsync(options, cancellationToken).ConfigureAwait(false);
        if (admission is { IsAdmitted: false })
        {
            return NotAdmitted(admission);
        }

        using var root = RootScope.Start(threadId, options, cancellationToken, RunMode.Resume, Checkpointing, admission);
        return await ObserveAsync(root, store, observed =>
                ResumeFromAsync(threadId, LevelSignals.ForWake(WakeReason.Event, signal), observed, stream: null, root.Level))
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Wakes every thread of <paramref name="store"/> that is due now: a timer whose time has come continues at
    /// the edge out of it, and a wait for a human or an event whose timeout has passed continues at its timeout
    /// route or, with none, is ended with <c>strata.end_reason</c> <c>timed_out</c>
    /// (<c>design/29-Event-Waits-And-Timers.md</c>, "Waking"). "Now" is <see cref="WakeOptions.Run"/>'s
    /// <see cref="RunOptions.TimeProvider"/>, read once.
    ///
    /// Threads are listed with <see cref="ThreadQuery.DueBy"/> and <see cref="WakeOptions.Metadata"/>, and woken
    /// one at a time, each as a run of its own (a resume, or an end) with <see cref="WakeOptions.Run"/>'s
    /// options and <paramref name="cancellationToken"/>, until <see cref="WakeOptions.Limit"/> threads have been
    /// woken or skipped. Each is loaded and decided again first: one that stopped being due, or whose wait isn't
    /// one of this graph's, is skipped with nothing written. The stamp and the end are conditional, so of two
    /// drivers that list the same thread one wakes it and the other skips it.
    ///
    /// A library call: the application calls it on its own cadence (no hosted service). An exception from a
    /// woken run propagates, as from <see cref="ResumeAsync(string, HumanSignal, ICheckpointStore, CancellationToken)"/>.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="WakeOptions.Limit"/> is out of range.</exception>
    /// <exception cref="ArgumentException">The run options have a reserved metadata key.</exception>
    public async Task<WakeReport<TState>> WakeDueAsync<TStore>(
        TStore store, WakeOptions? options = null, CancellationToken cancellationToken = default)
        where TStore : ICheckpointStore, ICheckpointQuery
    {
        ArgumentNullException.ThrowIfNull(store);
        options ??= new WakeOptions();
        ArgumentNullException.ThrowIfNull(options.Run);
        ThreadQuery.CheckLimit(options.Limit);
        RunMetadata.Validate(options.Run);

        var now = (options.Run.TimeProvider ?? TimeProvider.System).GetUtcNow();
        var due = CheckpointTimestamps.Text(now);

        // Only this graph's threads, when it has an identity (design/31-Graph-Identity.md); any version it may
        // migrate.
        var graph = options.MatchGraph ? Identity : null;
        var woken = new List<WokenThread<TState>>();
        var skipped = new List<SkippedThread>();
        bool Stopped() => cancellationToken.IsCancellationRequested || options.Stop.IsCancellationRequested;
        string? cursor = null;
        do
        {
            var page = await store.ListThreadsAsync(
                    new ThreadQuery
                    {
                        DueBy = now,
                        Metadata = options.Metadata,
                        GraphId = graph?.Id,
                        GraphVersion = Migration is null ? graph?.Version : null,
                        Limit = options.Limit,
                        Cursor = cursor,
                    },
                    CancellationToken.None)
                .ConfigureAwait(false);
            foreach (var thread in page.Threads)
            {
                if (woken.Count + skipped.Count >= options.Limit || Stopped())
                {
                    return new WakeReport<TState>(woken, skipped);
                }

                var (wokenThread, skippedThread) =
                    await WakeOneAsync(thread, due, store, options, cancellationToken).ConfigureAwait(false);
                if (wokenThread is not null)
                {
                    woken.Add(wokenThread);
                }
                else
                {
                    skipped.Add(skippedThread!);
                }
            }

            cursor = page.NextCursor;
        }
        while (cursor is not null && woken.Count + skipped.Count < options.Limit && !Stopped());

        return new WakeReport<TState>(woken, skipped);
    }

    /// <summary>
    /// Decides again whether <paramref name="listed"/>'s thread is due at <paramref name="due"/> and one of this
    /// graph's waits, then wakes it: a resume for a timer or a timeout with a route, an end for a timeout with
    /// none. With a scheduler, it waits for a slot first (<c>design/40-Run-Scheduler.md</c>), and an end runs in it.
    /// </summary>
    private async Task<(WokenThread<TState>? Woken, SkippedThread? Skipped)> WakeOneAsync(
        ThreadSummary listed, string due, ICheckpointStore store, WakeOptions wake, CancellationToken cancellationToken)
    {
        var threadId = listed.ThreadId;
        var options = wake.Run;
        using var admission = await AdmitAsync(options, cancellationToken, listed.Metadata, wake.Stop).ConfigureAwait(false);
        if (admission is { IsAdmitted: false })
        {
            return Skip($"it was not admitted: {NotAdmitted(admission).Message}");
        }

        // With its sequence: the stamp, or the end, is conditional on it (design/28-Ending-A-Thread.md).
        var latest = await store.LoadLatestAsync(threadId, CancellationToken.None).ConfigureAwait(false);
        if (latest is null)
        {
            return Skip("it has no checkpoint");
        }

        var checkpoint = latest.Checkpoint;
        if (!CheckpointSchema.IsSupported(checkpoint.SchemaVersion))
        {
            return Skip($"its checkpoint has unsupported schema_version '{checkpoint.SchemaVersion}'");
        }

        if (!ThreadQuery.IsDue(checkpoint, due))
        {
            return Skip("it is no longer due");
        }

        // What is due is the stored wait, whatever a migration makes of it.
        var reason = LevelSignals.DueFor(checkpoint, due)!.Value;

        // Another graph's thread is skipped, and another version's migrated (design/31-Graph-Identity.md).
        var adopted = Adopt(checkpoint, threadId, []);
        if (adopted.Problem is { } foreign)
        {
            return Skip(foreign);
        }

        checkpoint = adopted.Checkpoint!;
        latest = latest with { Checkpoint = checkpoint };

        var signals = LevelSignals.ForWake(reason);
        var check = CheckResume(checkpoint, signals, threadId, []);
        if (check.Problem is { } problem)
        {
            return Skip(problem);
        }

        GraphExecutionResult<TState> result;
        bool superseded;
        string? held;
        if (check.EndsOnTimeout)
        {
            using var root = RootScope.Start(threadId, options, CancellationToken.None, RunMode.End, Checkpointing, admission);
            result = await ObserveAsync(root, store, observed => EndTimedOutAsync(threadId, latest, observed, root.Level))
                .ConfigureAwait(false);
            superseded = root.Level.Run.Superseded;
            held = root.Level.Run.Held;
        }
        else
        {
            using var root = RootScope.Start(threadId, options, cancellationToken, RunMode.Resume, Checkpointing, admission);
            result = await ObserveAsync(root, store, observed =>
                    ResumeLoadedAsync(threadId, latest, signals, observed, stream: null, root.Level))
                .ConfigureAwait(false);
            superseded = root.Level.Run.Superseded;
            held = root.Level.Run.Held;
        }

        // A thread another owner holds is theirs to wake (design/32-Thread-Leases.md).
        if (held is not null)
        {
            return Skip($"it is {held}");
        }

        return superseded
            ? Skip("another driver woke or ended it first")
            : (new WokenThread<TState>(threadId, reason, result), null);

        (WokenThread<TState>?, SkippedThread?) Skip(string why) => (null, new SkippedThread(threadId, why));
    }

    /// <summary>
    /// <see cref="EndOnTimeoutAsync"/> as the wake driver's run of its own (<c>RunMode.End</c>): starts the
    /// run's telemetry first.
    /// </summary>
    private Task<GraphExecutionResult<TState>> EndTimedOutAsync(
        string threadId, LatestCheckpoint latest, ICheckpointStore store, LevelScope level)
    {
        var checkpoint = latest.Checkpoint;
        var telemetry = level.Run.Telemetry;
        telemetry.AdoptStored(checkpoint.Metadata);
        telemetry.RunStarted(checkpoint.SuspendedNode ?? string.Empty, Identity);
        return EndOnTimeoutAsync(threadId, checkpoint, latest.Sequence, store, level);
    }

    /// <summary>
    /// Ends a thread whose wait timed out with no timeout route (<c>design/29-Event-Waits-And-Timers.md</c>):
    /// <see cref="EndAsync(string, ICheckpointStore)"/>'s ended checkpoint with <c>strata.end_reason</c>
    /// <c>timed_out</c>, saved only if <paramref name="checkpoint"/> is still the thread's latest, at
    /// <paramref name="sequence"/>, and counted as a wake when it is. The run has started: the wake driver's
    /// end, or a resume given <see cref="LevelSignals.Due"/> (<c>design/42-Waking-Through-A-Resume.md</c>).
    /// </summary>
    private async Task<GraphExecutionResult<TState>> EndOnTimeoutAsync(
        string threadId, Checkpoint checkpoint, long? sequence, ICheckpointStore store, LevelScope level)
    {
        TState state;
        try
        {
            state = checkpoint.FrozenState.Deserialize<TState>()!;
        }
        catch (JsonException ex)
        {
            return new GraphExecutionResult<TState>.Failed(
                $"failed to rehydrate state for thread '{threadId}': {ex.Message}");
        }

        if (await AcquireLeaseAsync(threadId, store, level).ConfigureAwait(false) is { } held)
        {
            return held;
        }

        var result = new GraphExecutionResult<TState>.Cancelled(
            threadId, checkpoint.SuspendedNode ?? string.Empty, CancelReason.Ended, state, checkpoint.WaitingAt());
        var ended = Ended(checkpoint, level, new ThreadEnd { Reason = ThreadEnd.TimedOut }, NowRfc3339());
        switch (await PersistIfLatestAsync(store, ended, sequence, level).ConfigureAwait(false))
        {
            case LeasedSave.NotLatest:
                level.Run.Superseded = true;
                return new GraphExecutionResult<TState>.Failed(
                    $"thread '{threadId}' changed before its timeout could end it: another driver got there first");
            case LeasedSave.LeaseLost:
                return result with { Reason = CancelReason.LeaseLost };
            default:
                RunTelemetry.Woken(WakeReason.Timeout);
                return result;
        }
    }
}
