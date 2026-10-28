namespace Datalium.Strata.Scheduling;

/// <summary>
/// How a <see cref="RunScheduler"/> admits runs (<c>design/40-Run-Scheduler.md</c>): how many at once, how many
/// may wait, for how long, the slots kept for higher priorities, and each tenant's share.
/// </summary>
public sealed class RunSchedulerOptions
{
    /// <summary>Names the scheduler on its metrics (<c>strata.scheduler.name</c>), for a process with several.</summary>
    public string Name { get; init; } = "default";

    /// <summary>How many runs may hold a slot at once. At least 1.</summary>
    public int MaxConcurrent { get; init; } = 16;

    /// <summary>
    /// How many runs may wait for a slot. When a newcomer would make it more, the lowest-ranked waiting run leaves
    /// the queue: shed, if the newcomer outranks it, or else the newcomer, refused. 0: a run starts at once or not
    /// at all.
    /// </summary>
    public int MaxQueued { get; init; } = 1024;

    /// <summary>How long a run may wait for a slot before it is refused. <see langword="null"/>: as long as it takes.</summary>
    public TimeSpan? MaxQueueTime { get; init; }

    /// <summary>
    /// Slots kept for higher priorities: a run at priority <c>p</c> can't take a slot that would leave fewer free
    /// than every reservation whose <see cref="SlotReservation.MinPriority"/> is above <c>p</c> keeps. Together
    /// they must leave at least one slot for everyone.
    /// </summary>
    public IReadOnlyList<SlotReservation> Reservations { get; init; } = [];

    /// <summary>The metadata key that names a run's tenant.</summary>
    public string TenantKey { get; init; } = "tenant";

    /// <summary>How many runs of one tenant may hold slots at once. <see langword="null"/>: no limit.</summary>
    public int? TenantLimit { get; init; }

    /// <summary>Limits of particular tenants, in place of <see cref="TenantLimit"/>. 0 pauses a tenant.</summary>
    public IReadOnlyDictionary<string, int> TenantLimits { get; init; } = new Dictionary<string, int>();

    /// <summary>How many runs of one tenant may wait for a slot. <see langword="null"/>: no limit beyond <see cref="MaxQueued"/>.</summary>
    public int? TenantMaxQueued { get; init; }

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(Name, nameof(Name));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxConcurrent, 1, nameof(MaxConcurrent));
        ArgumentOutOfRangeException.ThrowIfNegative(MaxQueued, nameof(MaxQueued));
        if (MaxQueueTime is { } queueTime)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(queueTime, TimeSpan.Zero, nameof(MaxQueueTime));
        }

        ArgumentNullException.ThrowIfNull(Reservations, nameof(Reservations));
        var reserved = 0;
        var priorities = new HashSet<int>();
        foreach (var reservation in Reservations)
        {
            ArgumentNullException.ThrowIfNull(reservation, nameof(Reservations));
            ArgumentOutOfRangeException.ThrowIfLessThan(reservation.Slots, 1, nameof(Reservations));
            if (!priorities.Add(reservation.MinPriority))
            {
                throw new ArgumentException(
                    $"two reservations are for priority {reservation.MinPriority}: give each priority one", nameof(Reservations));
            }

            reserved += reservation.Slots;
        }

        if (reserved >= MaxConcurrent)
        {
            throw new ArgumentException(
                $"the reservations keep {reserved} of {MaxConcurrent} slots: they must leave at least one for every priority",
                nameof(Reservations));
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(TenantKey, nameof(TenantKey));
        if (TenantLimit is { } limit)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(limit, nameof(TenantLimit));
        }

        ArgumentNullException.ThrowIfNull(TenantLimits, nameof(TenantLimits));
        foreach (var (tenant, tenantLimit) in TenantLimits)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tenant, nameof(TenantLimits));
            ArgumentOutOfRangeException.ThrowIfNegative(tenantLimit, nameof(TenantLimits));
        }

        if (TenantMaxQueued is { } queued)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(queued, nameof(TenantMaxQueued));
        }
    }
}

/// <summary>
/// <paramref name="Slots"/> slots only runs at priority <paramref name="MinPriority"/> or more may take
/// (<c>design/40-Run-Scheduler.md</c>, "Admission"). Headroom, not a pool of their own: those runs use any free
/// slot first, like everyone.
/// </summary>
public sealed record SlotReservation(int MinPriority, int Slots);

/// <summary>A request for a slot (<see cref="RunScheduler.AdmitAsync"/>).</summary>
public sealed record AdmissionRequest
{
    /// <summary>Higher runs first. Runs of one priority run in the order they arrived.</summary>
    public int Priority { get; init; }

    /// <summary>Whose run it is, for the tenant limits. <see langword="null"/>: nobody's, and no tenant limit applies.</summary>
    public string? Tenant { get; init; }

    /// <summary>When the run must stop: a wait still going then ends <see cref="NotAdmittedReason.Deadline"/>.</summary>
    public DateTimeOffset? Deadline { get; init; }

    /// <summary>The clock of <see cref="Deadline"/>, <see cref="RunSchedulerOptions.MaxQueueTime"/> and <see cref="Admission.Waited"/>. <see langword="null"/>: <see cref="TimeProvider.System"/>.</summary>
    public TimeProvider? TimeProvider { get; init; }
}

/// <summary>Why a run got no slot (<c>design/40-Run-Scheduler.md</c>, "Not admitted").</summary>
public enum NotAdmittedReason
{
    /// <summary>The queue (or the tenant's share of it) was full, and the run outranked nobody waiting.</summary>
    QueueFull,

    /// <summary>The run was waiting when a run that outranks it arrived at a full queue, and left to make room.</summary>
    Shed,

    /// <summary>The run waited <see cref="RunSchedulerOptions.MaxQueueTime"/>.</summary>
    QueueTimeout,

    /// <summary>The run's token was cancelled while it waited (or a driver's stop token fired).</summary>
    Cancelled,

    /// <summary>The run's deadline passed while it waited.</summary>
    Deadline,

    /// <summary>The scheduler was closed (<see cref="RunScheduler.Close"/>).</summary>
    Closed,
}

/// <summary>A scheduler at one moment (<see cref="RunScheduler.Snapshot"/>).</summary>
/// <param name="Active">Runs holding a slot.</param>
/// <param name="Queued">Runs waiting for one.</param>
/// <param name="Tenants">Every tenant with a run active or waiting, or a limit of its own.</param>
public sealed record SchedulerSnapshot(
    string Name,
    int Active,
    int Queued,
    int MaxConcurrent,
    int MaxQueued,
    bool IsClosed,
    IReadOnlyDictionary<string, TenantUsage> Tenants);

/// <summary>One tenant's runs. <paramref name="Limit"/> <see langword="null"/>: no limit.</summary>
public sealed record TenantUsage(int Active, int Queued, int? Limit);
