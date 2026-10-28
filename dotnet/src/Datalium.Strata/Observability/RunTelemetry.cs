using System.Diagnostics;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Observability;

/// <summary>
/// One run's hook layer (<c>design/18-Run-Observability.md</c>): the executor calls it once at each
/// lifecycle point, and each call records the event (when there is an observer), starts or ends the
/// span, and updates the metrics. Events, spans and metrics come from the same call, so they can't
/// disagree. A subgraph's child shares its parent's, since it is the same run.
/// </summary>
internal sealed class RunTelemetry
{
    private readonly RunEventQueue? _events;
    private readonly Activity? _activity;
    private readonly long _started = Stopwatch.GetTimestamp();
    private readonly string _mode;
    private int _completed;

    // How long the run waited for its scheduler's slot, when it has a scheduler (design/40-Run-Scheduler.md).
    private readonly TimeSpan? _queuedFor;

    /// <summary>
    /// Starts the run's span (a child of whatever is current) and counts it as active. <paramref name="queuedFor"/>
    /// is how long it waited for its scheduler's slot, when it has a scheduler.
    /// </summary>
    public RunTelemetry(string threadId, RunOptions options, RunMode mode, TimeSpan? queuedFor = null)
    {
        _queuedFor = queuedFor;
        ThreadId = threadId;
        RunId = options.RunId ?? Guid.CreateVersion7().ToString();
        Mode = mode;
        Metadata = RunMetadata.Snapshot(options.Metadata);
        _mode = mode switch
        {
            RunMode.Run => "run",
            RunMode.Resume => "resume",
            RunMode.Recover => "recover",
            _ => "end",
        };
        _events = options.Observer is { } observer ? new RunEventQueue(observer, threadId, RunId) : null;

        _activity = StrataTelemetry.Source.StartActivity("strata.run");
        if (_activity is not null)
        {
            _activity.DisplayName = $"strata.{_mode}";
            Tag(_activity);
            _activity.SetTag("strata.run.mode", _mode);
            if (queuedFor is { } waited)
            {
                _activity.SetTag("strata.run.priority", options.Priority);
                _activity.SetTag("strata.scheduler.wait_ms", waited.TotalMilliseconds);
            }
        }

        StrataTelemetry.GraphsStarted.Add(1, new KeyValuePair<string, object?>("strata.run.mode", _mode));
        StrataTelemetry.GraphsActive.Add(1);
    }

    public string ThreadId { get; }

    public string RunId { get; }

    public RunMode Mode { get; }

    /// <summary>The run's metadata. A resume's is settled once its checkpoint is loaded.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; private set; }

    /// <summary>Whether the run has an observer; without one, no event is even built.</summary>
    internal bool Observed => _events is not null;

    /// <summary>Lays the resume's metadata over the caller keys of the checkpoint it resumes.</summary>
    public void AdoptStored(IReadOnlyDictionary<string, string>? stored) =>
        Metadata = RunMetadata.Resumed(stored, Metadata);

    public void RunStarted(string node, GraphIdentity? graph)
    {
        if (_activity is not null)
        {
            foreach (var (key, value) in Metadata)
            {
                _activity.SetTag($"strata.metadata.{key}", value);
            }

            // Which graph runs (design/31-Graph-Identity.md).
            if (graph is not null)
            {
                _activity.SetTag("strata.graph.id", graph.Id);
                _activity.SetTag("strata.graph.version", graph.Version);
            }
        }

        if (Observed)
        {
            Record(new RunStarted(Mode, node, Metadata) { Graph = graph, QueuedFor = _queuedFor }, null, null);
        }
    }

    public void Resumed(Checkpoint checkpoint, LevelSignals signals, string? route)
    {
        _activity?.SetTag("strata.wake.reason", Wire(signals.Wake));
        if (signals.Wake != WakeReason.Human)
        {
            Woken(signals.Wake);
        }

        if (Observed)
        {
            var signal = signals.Innermost;
            Record(
                new Resumed(
                    checkpoint.Status,
                    checkpoint.WaitingAt(),
                    signal.IsApproved,
                    signal.ReviewerToken,
                    signals.Wake == WakeReason.Human ? route : null)
                {
                    Wake = signals.Wake,
                    EventType = signals.EventType,
                    TimedOutTo = signals.Wake == WakeReason.Timeout ? route : null,
                },
                null,
                null);
        }
    }

    /// <summary>
    /// A recovery claimed its thread (<c>design/33-Durable-Execution.md</c>): it continues at
    /// <paramref name="at"/>, and has now been recovered <paramref name="recoveries"/> times from that checkpoint.
    /// </summary>
    public void Recovered(IReadOnlyList<string> at, int recoveries)
    {
        StrataTelemetry.Recoveries.Add(1);
        _activity?.SetTag("strata.recoveries", recoveries);
        if (Observed)
        {
            Record(new Recovered(at, recoveries), null, null);
        }
    }

    /// <summary>
    /// Counts a thread a signal or <c>WakeDueAsync</c> woke or ended (<c>design/29-Event-Waits-And-Timers.md</c>).
    /// </summary>
    public static void Woken(WakeReason reason) =>
        StrataTelemetry.Wakes.Add(1, new KeyValuePair<string, object?>("strata.wake.reason", Wire(reason)));

    public NodeScope NodeStarted(string path) => new(this, path);

    public BranchScope BranchStarted(string path, int attempt) => new(this, path, attempt);

    /// <summary>
    /// A group at <paramref name="path"/> decided its join (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>):
    /// the event, and a span event on <paramref name="activity"/>, the group's attempt span.
    /// </summary>
    public void JoinDecided(string path, int attempt, string policy, bool met, IReadOnlyList<string> deciding, Activity? activity)
    {
        activity?.AddEvent(new ActivityEvent("join_decided", tags: new ActivityTagsCollection
        {
            ["strata.join.policy"] = policy,
            ["strata.join.met"] = met,
            ["strata.join.deciding"] = deciding.ToArray(),
        }));
        if (Observed)
        {
            Record(new JoinDecided(policy, met, deciding), path, attempt);
        }
    }

    /// <summary>
    /// A map at <paramref name="path"/> expanded into <paramref name="keys"/> (<c>design/27-Map-Fan-Out.md</c>):
    /// the event, and a span event on <paramref name="activity"/>, the map's attempt span.
    /// </summary>
    public void MapExpanded(string path, int attempt, IReadOnlyList<string> keys, Activity? activity)
    {
        activity?.AddEvent(new ActivityEvent("map_expanded", tags: new ActivityTagsCollection
        {
            ["strata.map.item_count"] = keys.Count,
            ["strata.map.keys"] = keys.ToArray(),
        }));
        if (Observed)
        {
            Record(new MapExpanded(keys), path, attempt);
        }
    }

    public static void Transition(string from, string to) =>
        StrataTelemetry.Transitions.Add(
            1,
            new KeyValuePair<string, object?>("strata.node.path", from),
            new KeyValuePair<string, object?>("strata.next_node.path", to));

    public void CheckpointSaved(Checkpoint checkpoint, TimeSpan duration)
    {
        var status = Wire(checkpoint.Status);
        var resumeStamp = checkpoint.ResumedAt is not null;
        StrataTelemetry.CheckpointLatency.Record(
            duration.TotalSeconds,
            new KeyValuePair<string, object?>("strata.checkpoint.operation", "save"),
            new KeyValuePair<string, object?>("strata.checkpoint.status", status));
        _activity?.AddEvent(new ActivityEvent("checkpoint.saved", tags: new ActivityTagsCollection
        {
            ["strata.checkpoint.status"] = status,
            ["strata.checkpoint.resume_stamp"] = resumeStamp,
            ["strata.checkpoint.duration_ms"] = duration.TotalMilliseconds,
        }));
        if (Observed)
        {
            Record(new CheckpointSaved(checkpoint.Status, checkpoint.WaitingAt(), resumeStamp, duration), null, null);
        }
    }

    public static void CheckpointLoaded(Checkpoint? checkpoint, TimeSpan duration) =>
        StrataTelemetry.CheckpointLatency.Record(
            duration.TotalSeconds,
            new KeyValuePair<string, object?>("strata.checkpoint.operation", "load"),
            new KeyValuePair<string, object?>(
                "strata.checkpoint.status", checkpoint is null ? "none" : Wire(checkpoint.Status)));

    /// <summary>
    /// A level's hop limit refused the node at <paramref name="path"/> (<c>design/24-Loop-Circuit-Breaker.md</c>),
    /// before its checkpoint is saved.
    /// </summary>
    public void HopLimitExceeded(string path, int hops, int limit)
    {
        StrataTelemetry.HopLimitsExceeded.Add(1, new KeyValuePair<string, object?>("strata.node.path", path));
        _activity?.AddEvent(new ActivityEvent("hop_limit_exceeded", tags: new ActivityTagsCollection
        {
            ["strata.node.path"] = path,
            ["strata.hops"] = hops,
            ["strata.hop_limit"] = limit,
        }));
        if (Observed)
        {
            Record(new HopLimitExceeded(hops, limit), path, null);
        }
    }

    /// <summary>The run acquired its thread's lease (<c>design/32-Thread-Leases.md</c>).</summary>
    public void LeaseAcquired(ThreadLease lease)
    {
        StrataTelemetry.LeaseAcquisitions.Add(1, new KeyValuePair<string, object?>("strata.lease.outcome", "acquired"));
        _activity?.SetTag("strata.lease.owner", lease.Owner);
        _activity?.SetTag("strata.lease.token", lease.Token);
        if (Observed)
        {
            Record(new LeaseAcquired(lease.Owner, lease.Token, lease.ExpiresAt), null, null);
        }
    }

    /// <summary>The run was refused its thread's lease: another owner holds it.</summary>
    public static void LeaseHeld() =>
        StrataTelemetry.LeaseAcquisitions.Add(1, new KeyValuePair<string, object?>("strata.lease.outcome", "held"));

    /// <summary>The run lost its thread's lease, and stops.</summary>
    public void LeaseLost(ThreadLease lease)
    {
        StrataTelemetry.LeaseLosses.Add(1);
        _activity?.AddEvent(new ActivityEvent("lease_lost", tags: new ActivityTagsCollection
        {
            ["strata.lease.owner"] = lease.Owner,
            ["strata.lease.token"] = lease.Token,
        }));
        if (Observed)
        {
            Record(new LeaseLost(lease.Owner, lease.Token), null, null);
        }
    }

    public void Interrupted(IReadOnlyList<string> waitingAt, string nextNode, CheckpointWait? wait)
    {
        if (Observed)
        {
            Record(new Interrupted(waitingAt, nextNode) { Wait = wait }, null, null);
        }
    }

    /// <summary>
    /// Ends the run: the last event, the span, and the run metrics. Only the first call counts.
    /// <paramref name="errorType"/> tags a failed run's span (<c>hop_limit</c>).
    /// </summary>
    public void RunCompleted(RunOutcome outcome, string? error, CancelReason? reason, string? errorType = null)
    {
        if (Interlocked.Exchange(ref _completed, 1) == 1)
        {
            return;
        }

        var elapsed = Stopwatch.GetElapsedTime(_started);
        var outcomeTag = Wire(outcome);
        StrataTelemetry.GraphsActive.Add(-1);
        StrataTelemetry.GraphDuration.Record(
            elapsed.TotalSeconds,
            new KeyValuePair<string, object?>("strata.run.mode", _mode),
            new KeyValuePair<string, object?>("strata.outcome", outcomeTag));

        if (_activity is not null)
        {
            _activity.SetTag("strata.outcome", outcomeTag);
            if (reason is { } cancelReason)
            {
                _activity.SetTag("strata.cancel_reason", Wire(cancelReason));
            }

            if (outcome == RunOutcome.Failed)
            {
                _activity.SetStatus(ActivityStatusCode.Error, error);
                if (errorType is not null)
                {
                    _activity.SetTag("error.type", errorType);
                }
            }

            _activity.Dispose();
        }

        if (_events is { } events)
        {
            events.Record(new RunCompleted(outcome, elapsed, error, reason), null, null);
            events.Complete();
        }
    }

    internal void Record(RunEvent runEvent, string? nodePath, int? attempt) =>
        _events?.Record(runEvent, nodePath, attempt);

    /// <summary>The span attributes every Strata span carries.</summary>
    internal void Tag(Activity activity)
    {
        activity.SetTag("strata.thread_id", ThreadId);
        activity.SetTag("strata.run_id", RunId);
    }

    internal static string Wire(CheckpointStatus status) => status switch
    {
        CheckpointStatus.Interrupted => "interrupted",
        CheckpointStatus.Completed => "completed",
        CheckpointStatus.Failed => "failed",
        CheckpointStatus.Ended => "ended",
        CheckpointStatus.Running => "running",
        _ => "cancelled",
    };

    internal static string Wire(WakeReason reason) => reason switch
    {
        WakeReason.Event => "event",
        WakeReason.Timer => "timer",
        WakeReason.Timeout => "timeout",
        _ => "human",
    };

    internal static string Wire(CancelReason reason) => reason switch
    {
        CancelReason.Deadline => "deadline",
        CancelReason.Ended => "ended",
        CancelReason.LeaseLost => "lease_lost",
        _ => "requested",
    };

    private static string Wire(RunOutcome outcome) => outcome switch
    {
        RunOutcome.Completed => "completed",
        RunOutcome.Interrupted => "interrupted",
        RunOutcome.Failed => "failed",
        _ => "cancelled",
    };
}

/// <summary>One node of a run, from <see cref="NodeStarted"/> to <see cref="NodeCompleted"/>.</summary>
internal sealed class NodeScope
{
    private readonly RunTelemetry _run;
    private readonly long _started = Stopwatch.GetTimestamp();
    private readonly Activity? _activity;
    private int _completed;

    public NodeScope(RunTelemetry run, string path)
    {
        _run = run;
        Path = path;
        _activity = StrataTelemetry.Source.StartActivity("strata.node");
        if (_activity is not null)
        {
            _activity.DisplayName = path;
            run.Tag(_activity);
            _activity.SetTag("strata.node.path", path);
        }

        if (run.Observed)
        {
            run.Record(new NodeStarted(), path, 1);
        }
    }

    public string Path { get; }

    /// <summary>The attempt running, or the last one made.</summary>
    public int Attempt { get; private set; } = 1;

    /// <summary>Starts attempt <paramref name="attempt"/>'s span, the one its handler runs inside.</summary>
    public Activity? StartAttempt(int attempt)
    {
        Attempt = attempt;
        var activity = StrataTelemetry.Source.StartActivity("strata.attempt");
        if (activity is not null)
        {
            activity.DisplayName = $"{Path} #{attempt}";
            _run.Tag(activity);
            activity.SetTag("strata.node.path", Path);
            activity.SetTag("strata.attempt", attempt);
        }

        return activity;
    }

    /// <summary>
    /// The attempt that just ended failed transiently and the node will run again after
    /// <paramref name="delay"/> (<c>design/25-Retry-Backoff.md</c>).
    /// </summary>
    public void Retry(string error, string errorType, bool timedOut, TimeSpan delay)
    {
        StrataTelemetry.NodeRetries.Add(
            1,
            new KeyValuePair<string, object?>("strata.node.path", Path),
            new KeyValuePair<string, object?>("error.type", errorType));
        _activity?.AddEvent(new ActivityEvent("retry", tags: new ActivityTagsCollection
        {
            ["strata.attempt"] = Attempt,
            ["error.type"] = errorType,
            ["message"] = error,
            ["strata.retry.delay_ms"] = delay.TotalMilliseconds,
        }));
        if (_run.Observed)
        {
            _run.Record(new RetryScheduled(error, timedOut, delay), Path, Attempt);
        }
    }

    /// <summary>
    /// Ends the node. Only the first call counts, so a failure path can end it before its checkpoint
    /// is saved and the executor's later call changes nothing.
    /// </summary>
    public void Completed(NodeResult result, string? error = null, string? errorType = null, string? fallback = null)
    {
        if (Interlocked.Exchange(ref _completed, 1) == 1)
        {
            return;
        }

        var elapsed = Stopwatch.GetElapsedTime(_started);
        var resultTag = Wire(result);
        StrataTelemetry.NodeDuration.Record(
            elapsed.TotalSeconds,
            new KeyValuePair<string, object?>("strata.node.path", Path),
            new KeyValuePair<string, object?>("strata.node.result", resultTag));

        if (_activity is not null)
        {
            _activity.SetTag("strata.retry_count", Attempt - 1);
            _activity.SetTag("strata.node.result", resultTag);
            if (result == NodeResult.Failed)
            {
                StrataTelemetry.Fail(_activity, error ?? "failed", errorType);
            }

            _activity.Dispose();
        }

        if (_run.Observed)
        {
            _run.Record(new NodeCompleted(result, elapsed, error, fallback), Path, Attempt);
        }
    }

    /// <summary>An exception that isn't recovery-managed (a bug) is propagating out of the node.</summary>
    public void Faulted(Exception exception) =>
        Completed(NodeResult.Failed, exception.Message, exception.GetType().FullName);

    private static string Wire(NodeResult result) => result switch
    {
        NodeResult.Committed => "committed",
        NodeResult.Failed => "failed",
        NodeResult.Interrupted => "interrupted",
        _ => "cancelled",
    };
}

/// <summary>One branch of a parallel group, from <see cref="BranchStarted"/> to <see cref="BranchCompleted"/>.</summary>
internal sealed class BranchScope
{
    private readonly RunTelemetry _run;
    private readonly long _started = Stopwatch.GetTimestamp();
    private readonly int _attempt;
    private int _completed;

    /// <summary>Starts the branch's span, which becomes current: the caller restores the group's.</summary>
    public BranchScope(RunTelemetry run, string path, int attempt)
    {
        _run = run;
        _attempt = attempt;
        Path = path;
        Activity = StrataTelemetry.Source.StartActivity("strata.branch");
        if (Activity is not null)
        {
            Activity.DisplayName = path;
            run.Tag(Activity);
            Activity.SetTag("strata.node.path", path);
            Activity.SetTag("strata.attempt", attempt);
        }

        if (run.Observed)
        {
            run.Record(new BranchStarted(), path, attempt);
        }
    }

    public string Path { get; }

    public Activity? Activity { get; }

    /// <summary>Ends the branch, saying why when the result alone doesn't. Only the first call counts.</summary>
    public void Completed(BranchResult result, Exception? error, BranchEndReason? reason = null)
    {
        if (Interlocked.Exchange(ref _completed, 1) == 1)
        {
            return;
        }

        var elapsed = Stopwatch.GetElapsedTime(_started);
        if (Activity is not null)
        {
            if (result == BranchResult.Failed && error is not null)
            {
                StrataTelemetry.Fail(Activity, error.Message, ErrorType(error));
            }

            if (reason is { } why)
            {
                Activity.SetTag("strata.branch.reason", Wire(why));
            }

            Activity.Dispose();
        }

        if (_run.Observed)
        {
            _run.Record(new BranchCompleted(result, elapsed, error?.Message) { Reason = reason }, Path, _attempt);
        }
    }

    /// <summary>A reason as span attributes write it.</summary>
    internal static string Wire(BranchEndReason reason) => reason switch
    {
        BranchEndReason.TimedOut => "timed_out",
        BranchEndReason.LostRace => "lost_race",
        _ => "sibling_failed",
    };

    private static string ErrorType(Exception error) => error is NodeException node
        ? node.Kind == NodeFailureKind.Transient ? "transient" : "fatal"
        : error.GetType().FullName ?? "exception";
}

/// <summary>
/// The caller's store as the run sees it: every save and load is timed, and every save reported as
/// <see cref="CheckpointSaved"/>. A subgraph's child saves to its own capturing store instead, so
/// only real writes are reported.
/// </summary>
internal sealed class ObservedStore(ICheckpointStore inner, RunTelemetry telemetry) : ICheckpointStore
{
    public void Save(Checkpoint checkpoint) => throw new NotSupportedException("the engine saves through SaveAsync");

    public Checkpoint? Load(string threadId) => throw new NotSupportedException("the engine loads through LoadAsync");

    public async ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default)
    {
        var started = Stopwatch.GetTimestamp();
        await inner.SaveAsync(checkpoint, cancellationToken).ConfigureAwait(false);
        telemetry.CheckpointSaved(checkpoint, Stopwatch.GetElapsedTime(started));
    }

    public async ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default)
    {
        var started = Stopwatch.GetTimestamp();
        var checkpoint = await inner.LoadAsync(threadId, cancellationToken).ConfigureAwait(false);
        RunTelemetry.CheckpointLoaded(checkpoint, Stopwatch.GetElapsedTime(started));
        return checkpoint;
    }

    public async ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default)
    {
        var started = Stopwatch.GetTimestamp();
        var latest = await inner.LoadLatestAsync(threadId, cancellationToken).ConfigureAwait(false);
        RunTelemetry.CheckpointLoaded(latest?.Checkpoint, Stopwatch.GetElapsedTime(started));
        return latest;
    }

    /// <summary>Reported as <see cref="CheckpointSaved"/> only when it wrote.</summary>
    public async ValueTask<bool> SaveIfLatestAsync(
        Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default)
    {
        var started = Stopwatch.GetTimestamp();
        var saved = await inner.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken).ConfigureAwait(false);
        if (saved)
        {
            telemetry.CheckpointSaved(checkpoint, Stopwatch.GetElapsedTime(started));
        }

        return saved;
    }

    public ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
        string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        inner.TryAcquireLeaseAsync(threadId, owner, ttl, cancellationToken);

    public ValueTask<ThreadLease?> RenewLeaseAsync(
        ThreadLease lease, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        inner.RenewLeaseAsync(lease, ttl, cancellationToken);

    public ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken cancellationToken = default) =>
        inner.ReleaseLeaseAsync(lease, cancellationToken);

    public ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken cancellationToken = default) =>
        inner.GetLeaseAsync(threadId, cancellationToken);

    /// <summary>Reported as <see cref="CheckpointSaved"/> only when it wrote: not when it was fenced or lost a race.</summary>
    public async ValueTask<bool> SaveUnderLeaseAsync(
        Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default)
    {
        var started = Stopwatch.GetTimestamp();
        var saved = await inner.SaveUnderLeaseAsync(checkpoint, lease, expectedSequence, cancellationToken)
            .ConfigureAwait(false);
        if (saved)
        {
            telemetry.CheckpointSaved(checkpoint, Stopwatch.GetElapsedTime(started));
        }

        return saved;
    }
}
