namespace Datalium.Strata.Interruption;

/// <summary>
/// A thread's lease (<c>design/32-Thread-Leases.md</c>): who holds the thread, the token that fences its
/// saves, and when it expires unless renewed. <paramref name="Token"/> is a counter per thread: every
/// acquisition gets the next one, and <see cref="ICheckpointStore.SaveUnderLeaseAsync"/> refuses a save whose
/// token is no longer the thread's latest. <paramref name="Owner"/> is a label for people; it decides nothing.
/// </summary>
public sealed record ThreadLease(string ThreadId, string Owner, long Token, DateTimeOffset ExpiresAt);

/// <summary>
/// What <see cref="ICheckpointStore.TryAcquireLeaseAsync"/> found: the <paramref name="Lease"/> it acquired, or
/// the live lease the thread is <paramref name="HeldBy"/>. Exactly one is set.
/// </summary>
public sealed record LeaseAttempt(ThreadLease? Lease, ThreadLease? HeldBy)
{
    /// <summary>Whether the lease was acquired.</summary>
    public bool Acquired => Lease is not null;
}

/// <summary>
/// A save under a lease whose token is no longer its thread's latest: another owner acquired the thread
/// since. Nothing was written (<c>design/32-Thread-Leases.md</c>, "Fencing").
/// </summary>
public sealed class LeaseLostException(ThreadLease lease)
    : Exception($"the lease '{lease.Owner}' held on thread '{lease.ThreadId}' (token {lease.Token}) was lost: another owner acquired the thread")
{
    /// <summary>The lease that was lost.</summary>
    public ThreadLease Lease { get; } = lease;
}

/// <summary>What every lease member of a store checks before anything else.</summary>
public static class ThreadLeases
{
    /// <summary>The message a store with no leases refuses with.</summary>
    public const string NotSupported = "this checkpoint store has no leases";

    /// <summary>The shortest ttl a lease can have.</summary>
    public static readonly TimeSpan MinimumTtl = TimeSpan.FromMilliseconds(1);

    /// <summary>Refuses a blank owner and a ttl below <see cref="MinimumTtl"/>.</summary>
    public static void Check(string owner, TimeSpan ttl)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(owner);
        CheckTtl(ttl);
    }

    /// <summary>Refuses a ttl below <see cref="MinimumTtl"/>.</summary>
    public static void CheckTtl(TimeSpan ttl) => ArgumentOutOfRangeException.ThrowIfLessThan(ttl, MinimumTtl);

    /// <summary>A ttl in the whole milliseconds a store counts in, rounded up.</summary>
    public static long Milliseconds(TimeSpan ttl) => (long)Math.Ceiling(ttl.TotalMilliseconds);

    /// <summary>Refuses a lease for another thread than <paramref name="checkpoint"/>'s.</summary>
    public static void CheckThread(Checkpoint checkpoint, ThreadLease lease)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        ArgumentNullException.ThrowIfNull(lease);
        if (lease.ThreadId != checkpoint.ThreadId)
        {
            throw new ArgumentException(
                $"the lease is for thread '{lease.ThreadId}', not the checkpoint's thread '{checkpoint.ThreadId}'", nameof(lease));
        }
    }
}
