using Datalium.Strata.Observability;

namespace Datalium.Strata.Scheduling;

/// <summary>
/// Admits runs to a fixed number of slots (<c>design/40-Run-Scheduler.md</c>): higher priorities first and in
/// order of arrival within one, with slots kept for higher priorities, at most so many runs of one tenant at once,
/// and a bounded queue that sheds its lowest-ranked run for a newcomer that outranks it. A run gets one through
/// <see cref="Graph.RunOptions.Scheduler"/>; <see cref="AdmitAsync"/> is public so an application can schedule work
/// of its own beside its runs. Thread-safe; one scheduler is shared by every run it bounds.
/// </summary>
public sealed class RunScheduler
{
    // Longest delay a timer takes; a wait further away than this is no limit at all.
    private static readonly TimeSpan MaxDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private readonly Lock _gate = new();
    private readonly SlotReservation[] _reservations;
    private readonly KeyValuePair<string, object?> _name;

    // Waiting runs in rank order: higher priority first, then earlier arrival.
    private readonly List<Waiter> _queue = [];

    // Each tenant's limit of its own, set by the options or SetTenantLimit.
    private readonly Dictionary<string, int> _limits;

    // Tenants with a run active or waiting.
    private readonly Dictionary<string, TenantCount> _tenants = new(StringComparer.Ordinal);

    private int _active;
    private long _arrivals;
    private bool _closed;

    /// <exception cref="ArgumentException"><paramref name="options"/> is invalid.</exception>
    public RunScheduler(RunSchedulerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        Options = options;
        _reservations = [.. options.Reservations];
        _limits = new Dictionary<string, int>(options.TenantLimits, StringComparer.Ordinal);
        _name = new("strata.scheduler.name", options.Name);
    }

    /// <summary>The options it was made with. <see cref="SetTenantLimit"/> changes don't show here.</summary>
    public RunSchedulerOptions Options { get; }

    /// <summary>Whether <see cref="Close"/> was called.</summary>
    public bool IsClosed
    {
        get
        {
            lock (_gate)
            {
                return _closed;
            }
        }
    }

    /// <summary>
    /// Waits for a slot, and returns it, or why there is none. An admitted <see cref="Admission"/> holds its slot
    /// until it is disposed. The wait ends without one when <paramref name="cancellationToken"/> is cancelled, the
    /// request's deadline passes, <see cref="RunSchedulerOptions.MaxQueueTime"/> passes, a run that outranks it
    /// sheds it, or the scheduler is closed. Never throws for any of those.
    /// </summary>
    public async ValueTask<Admission> AdmitAsync(AdmissionRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var waiter = new Waiter(request.Priority, request.Tenant, request.TimeProvider ?? TimeProvider.System);
        lock (_gate)
        {
            if (_closed)
            {
                Refuse(waiter, NotAdmittedReason.Closed);
            }
            else
            {
                waiter.Sequence = _arrivals++;
                Enqueue(waiter);
                Dispatch();
                if (waiter.State == WaiterState.Queued)
                {
                    Overflow(waiter);
                }
            }
        }

        if (waiter.State == WaiterState.Queued)
        {
            await WaitAsync(waiter, request, cancellationToken).ConfigureAwait(false);
        }

        return waiter.State == WaiterState.Admitted
            ? new Admission(this, waiter.Tenant, refusal: null, waiter.Waited)
            : new Admission(scheduler: null, waiter.Tenant, waiter.Refusal, waiter.Waited);
    }

    /// <summary>
    /// Gives <paramref name="tenant"/> a limit of its own (0 pauses it), or with <see langword="null"/> returns it
    /// to <see cref="RunSchedulerOptions.TenantLimit"/>. Takes effect at once: waiting runs a higher limit allows
    /// start, and no running run is stopped by a lower one.
    /// </summary>
    public void SetTenantLimit(string tenant, int? limit)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenant);
        if (limit is { } value)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value, nameof(limit));
        }

        lock (_gate)
        {
            if (limit is { } set)
            {
                _limits[tenant] = set;
            }
            else
            {
                _limits.Remove(tenant);
            }

            Dispatch();
        }
    }

    /// <summary>The scheduler's runs, and each tenant's, now.</summary>
    public SchedulerSnapshot Snapshot()
    {
        lock (_gate)
        {
            var tenants = new Dictionary<string, TenantUsage>(StringComparer.Ordinal);
            foreach (var (tenant, count) in _tenants)
            {
                tenants[tenant] = new TenantUsage(count.Active, count.Queued, LimitOf(tenant));
            }

            foreach (var tenant in _limits.Keys)
            {
                tenants.TryAdd(tenant, new TenantUsage(0, 0, LimitOf(tenant)));
            }

            return new SchedulerSnapshot(
                Options.Name, _active, _queue.Count, Options.MaxConcurrent, Options.MaxQueued, _closed, tenants);
        }
    }

    /// <summary>
    /// Admits nothing more: every waiting run, and every run that asks from now on, is refused with
    /// <see cref="NotAdmittedReason.Closed"/>. Runs holding slots are not touched.
    /// </summary>
    public void Close()
    {
        lock (_gate)
        {
            _closed = true;
            while (_queue.Count > 0)
            {
                var waiter = _queue[^1];
                Dequeue(waiter);
                Refuse(waiter, NotAdmittedReason.Closed);
            }
        }
    }

    /// <summary>Gives a slot back, and admits whoever it lets start.</summary>
    internal void Release(string? tenant)
    {
        lock (_gate)
        {
            _active--;
            StrataTelemetry.SchedulerActive.Add(-1, _name);
            if (tenant is not null && _tenants.TryGetValue(tenant, out var count))
            {
                count.Active--;
                Forget(tenant, count);
            }

            Dispatch();
        }
    }

    private async ValueTask WaitAsync(Waiter waiter, AdmissionRequest request, CancellationToken cancellationToken)
    {
        // The wait's own end: the deadline, or the longest a run may wait, whichever comes first.
        var clock = waiter.Clock;
        TimeSpan? limit = null;
        var reason = NotAdmittedReason.QueueTimeout;
        if (Options.MaxQueueTime is { } queueTime)
        {
            limit = queueTime - clock.GetElapsedTime(waiter.Arrived);
        }

        if (request.Deadline is { } deadline)
        {
            var untilDeadline = deadline - clock.GetUtcNow();
            if (limit is null || untilDeadline <= limit)
            {
                limit = untilDeadline;
                reason = NotAdmittedReason.Deadline;
            }
        }

        using var timer = limit switch
        {
            null => null,
            { } delay when delay <= TimeSpan.Zero => Fired(),
            { } delay when delay < MaxDelay => new CancellationTokenSource(delay, clock),
            _ => null,
        };
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, timer?.Token ?? CancellationToken.None);
        try
        {
            await waiter.Decided.Task.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            lock (_gate)
            {
                // Admitted or refused at the same moment: that stands.
                if (waiter.State == WaiterState.Queued)
                {
                    Dequeue(waiter);
                    Refuse(waiter, cancellationToken.IsCancellationRequested ? NotAdmittedReason.Cancelled : reason);
                }
            }
        }

        static CancellationTokenSource Fired()
        {
            var fired = new CancellationTokenSource();
            fired.Cancel();
            return fired;
        }
    }

    /// <summary>Admits every waiting run that may start now, in rank order. Under the lock.</summary>
    private void Dispatch()
    {
        for (var i = 0; i < _queue.Count;)
        {
            var waiter = _queue[i];

            // No slot for this priority is none for any after it: they have the same headroom or less.
            if (!SlotFree(waiter.Priority))
            {
                return;
            }

            // A tenant at its limit holds back only its own runs.
            if (!TenantFree(waiter.Tenant))
            {
                i++;
                continue;
            }

            Dequeue(waiter);
            _active++;
            StrataTelemetry.SchedulerActive.Add(1, _name);
            if (waiter.Tenant is { } tenant)
            {
                Count(tenant).Active++;
            }

            Decide(waiter, WaiterState.Admitted, refusal: null);
        }
    }

    /// <summary>Makes room in a queue a newcomer made too long. Under the lock.</summary>
    private void Overflow(Waiter newcomer)
    {
        if (Options.TenantMaxQueued is { } tenantMax
            && newcomer.Tenant is { } tenant
            && _tenants.TryGetValue(tenant, out var count)
            && count.Queued > tenantMax)
        {
            Dequeue(newcomer);
            Refuse(newcomer, NotAdmittedReason.QueueFull);
            return;
        }

        if (_queue.Count > Options.MaxQueued)
        {
            // The lowest-ranked run: shed for a newcomer that outranks it, or the newcomer itself.
            var lowest = _queue[^1];
            Dequeue(lowest);
            Refuse(lowest, ReferenceEquals(lowest, newcomer) ? NotAdmittedReason.QueueFull : NotAdmittedReason.Shed);
        }
    }

    private bool SlotFree(int priority)
    {
        var reserved = 0;
        foreach (var reservation in _reservations)
        {
            if (reservation.MinPriority > priority)
            {
                reserved += reservation.Slots;
            }
        }

        return _active < Options.MaxConcurrent - reserved;
    }

    private bool TenantFree(string? tenant) =>
        tenant is null
        || LimitOf(tenant) is not { } limit
        || (_tenants.TryGetValue(tenant, out var count) ? count.Active : 0) < limit;

    private int? LimitOf(string tenant) => _limits.TryGetValue(tenant, out var limit) ? limit : Options.TenantLimit;

    private void Enqueue(Waiter waiter)
    {
        // After every run of its priority or higher: it arrived last.
        var index = _queue.Count;
        while (index > 0 && _queue[index - 1].Priority < waiter.Priority)
        {
            index--;
        }

        _queue.Insert(index, waiter);
        StrataTelemetry.SchedulerQueued.Add(1, _name);
        if (waiter.Tenant is { } tenant)
        {
            Count(tenant).Queued++;
        }
    }

    private void Dequeue(Waiter waiter)
    {
        _queue.Remove(waiter);
        StrataTelemetry.SchedulerQueued.Add(-1, _name);
        if (waiter.Tenant is { } tenant && _tenants.TryGetValue(tenant, out var count))
        {
            count.Queued--;
            Forget(tenant, count);
        }
    }

    private TenantCount Count(string tenant)
    {
        if (!_tenants.TryGetValue(tenant, out var count))
        {
            count = new TenantCount();
            _tenants[tenant] = count;
        }

        return count;
    }

    private void Forget(string tenant, TenantCount count)
    {
        if (count is { Active: 0, Queued: 0 })
        {
            _tenants.Remove(tenant);
        }
    }

    private void Refuse(Waiter waiter, NotAdmittedReason reason) => Decide(waiter, WaiterState.Refused, reason);

    private void Decide(Waiter waiter, WaiterState state, NotAdmittedReason? refusal)
    {
        waiter.State = state;
        waiter.Refusal = refusal;
        waiter.Waited = waiter.Clock.GetElapsedTime(waiter.Arrived);
        var outcome = new KeyValuePair<string, object?>("strata.scheduler.outcome", Outcome(refusal));
        StrataTelemetry.SchedulerAdmissions.Add(
            1, _name, outcome, new KeyValuePair<string, object?>("strata.run.priority", waiter.Priority));
        StrataTelemetry.SchedulerWait.Record(waiter.Waited.TotalSeconds, _name, outcome);
        waiter.Decided.TrySetResult();
    }

    /// <summary>A decision's <c>strata.scheduler.outcome</c>.</summary>
    internal static string Outcome(NotAdmittedReason? refusal) => refusal switch
    {
        null => "admitted",
        NotAdmittedReason.QueueFull => "queue_full",
        NotAdmittedReason.Shed => "shed",
        NotAdmittedReason.QueueTimeout => "queue_timeout",
        NotAdmittedReason.Cancelled => "cancelled",
        NotAdmittedReason.Deadline => "deadline",
        _ => "closed",
    };

    private enum WaiterState
    {
        Queued,
        Admitted,
        Refused,
    }

    private sealed class Waiter(int priority, string? tenant, TimeProvider clock)
    {
        public int Priority { get; } = priority;

        public string? Tenant { get; } = tenant;

        public TimeProvider Clock { get; } = clock;

        public long Arrived { get; } = clock.GetTimestamp();

        public long Sequence { get; set; }

        // Written under the scheduler's lock; read by the waiting caller after Decided completes, or under the lock.
        public WaiterState State { get; set; }

        public NotAdmittedReason? Refusal { get; set; }

        public TimeSpan Waited { get; set; }

        public TaskCompletionSource Decided { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class TenantCount
    {
        public int Active { get; set; }

        public int Queued { get; set; }
    }
}

/// <summary>
/// A slot, or why there is none (<see cref="RunScheduler.AdmitAsync"/>). Disposing an admitted one gives its slot
/// back, once; disposing a refused one does nothing.
/// </summary>
public sealed class Admission : IDisposable
{
    private RunScheduler? _scheduler;
    private readonly string? _tenant;

    internal Admission(RunScheduler? scheduler, string? tenant, NotAdmittedReason? refusal, TimeSpan waited)
    {
        _scheduler = scheduler;
        _tenant = tenant;
        Refusal = refusal;
        Waited = waited;
    }

    /// <summary>Whether the run got a slot.</summary>
    public bool IsAdmitted => Refusal is null;

    /// <summary>Why it didn't, when it didn't.</summary>
    public NotAdmittedReason? Refusal { get; }

    /// <summary>How long it waited for the decision: next to nothing for a run admitted on arrival.</summary>
    public TimeSpan Waited { get; }

    public void Dispose() => Interlocked.Exchange(ref _scheduler, null)?.Release(_tenant);
}
