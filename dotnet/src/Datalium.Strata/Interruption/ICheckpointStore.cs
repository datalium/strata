namespace Datalium.Strata.Interruption;

/// <summary>
/// The persistence port. Concrete adapters (starting with an in-memory store, per
/// <c>design/03-Architecture.md</c>'s ring-buffer-first progression) live in
/// <c>Datalium.Strata.Storage</c>, which depends on this project for the interface and the
/// <see cref="Checkpoint"/> shape — not the other way around.
///
/// The engine only calls <see cref="SaveAsync"/> and <see cref="LoadAsync"/>
/// (<c>design/19-Async-Queryable-Store.md</c>). Their default implementations run <see cref="Save"/>
/// and <see cref="Load"/> inline, which is what the engine did before they existed, so a store that
/// implements only the synchronous members keeps working unchanged. A store with real async I/O
/// overrides them; its synchronous members then only serve direct callers, so an async-first store
/// may make them thin wrappers, or throw <see cref="NotSupportedException"/> if nothing calls them.
///
/// The engine never passes a run's cancellation token to a store call: a cancelled run must still
/// write its <c>cancelled</c> checkpoint (<c>design/17-Async-Execution.md</c>). A store that can hang
/// is bounded by its own timeouts.
///
/// Ending a thread and a resume's <c>resumed_at</c> stamp go through <see cref="LoadLatestAsync"/> and
/// <see cref="SaveIfLatestAsync"/>, a compare-and-set on the thread's log, so an end and a resume
/// (or two resumes) racing on one thread can't both win (<c>design/28-Ending-A-Thread.md</c>, "Races").
/// Their default implementations load with no sequence and save unconditionally, which is what the
/// engine did before they existed, race included. Every adapter in <c>Datalium.Strata.Storage</c>
/// overrides both.
///
/// A run given <c>RunOptions.Lease</c> holds its thread through the five lease members
/// (<c>design/32-Thread-Leases.md</c>): it acquires before its first save, renews, saves through
/// <see cref="SaveUnderLeaseAsync"/>, which refuses a save from an owner another has replaced, and releases
/// when it returns. Their defaults throw <see cref="NotSupportedException"/>: a store without them works as
/// before for every run that has no lease. Every adapter in <c>Datalium.Strata.Storage</c> implements them.
/// </summary>
public interface ICheckpointStore
{
    void Save(Checkpoint checkpoint);

    Checkpoint? Load(string threadId);

    /// <summary>Persists <paramref name="checkpoint"/> as its thread's latest. What the engine calls.</summary>
    ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Save(checkpoint);
        return ValueTask.CompletedTask;
    }

    /// <summary>The thread's latest checkpoint, or <c>null</c> if it has none. What the engine calls.</summary>
    ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(Load(threadId));
    }

    /// <summary>
    /// The thread's latest checkpoint with its sequence in the thread's log (the sequence
    /// <see cref="ICheckpointQuery.GetLogAsync"/> reports), or <c>null</c> if it has none. The default
    /// is <see cref="LoadAsync"/> with no sequence, for a store that can't tell.
    /// </summary>
    async ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
        await LoadAsync(threadId, cancellationToken).ConfigureAwait(false) is { } checkpoint
            ? new LatestCheckpoint(checkpoint, Sequence: null)
            : null;

    /// <summary>
    /// Appends <paramref name="checkpoint"/> only if its thread's latest checkpoint is still the one at
    /// <paramref name="expectedSequence"/>, atomically, and says whether it did: <c>false</c> means
    /// something was appended since, and nothing was written. A <c>null</c> sequence (from a store that
    /// can't tell) saves unconditionally. The default is <see cref="SaveAsync"/>, then <c>true</c>.
    /// </summary>
    async ValueTask<bool> SaveIfLatestAsync(
        Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default)
    {
        await SaveAsync(checkpoint, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Takes <paramref name="threadId"/>'s lease for <paramref name="owner"/>, expiring after
    /// <paramref name="ttl"/> unless renewed (<c>design/32-Thread-Leases.md</c>). A thread whose lease was not
    /// released and has not expired is held, whoever asks: the attempt then carries that lease and nothing
    /// changes. Otherwise it carries a new lease with the thread's next token. The default has no leases.
    /// </summary>
    ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
        string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(ThreadLeases.NotSupported);

    /// <summary>
    /// Extends <paramref name="lease"/> to <paramref name="ttl"/> from now, if its token is still its thread's
    /// latest, and returns it as it now is; <c>null</c> when another owner acquired the thread since. An
    /// expired lease nobody took over can still be renewed.
    /// </summary>
    ValueTask<ThreadLease?> RenewLeaseAsync(
        ThreadLease lease, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(ThreadLeases.NotSupported);

    /// <summary>
    /// Gives <paramref name="lease"/> up, if its token is still its thread's latest, so the thread can be
    /// acquired at once; says whether it did. The token is kept: the next acquisition gets a higher one.
    /// </summary>
    ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(ThreadLeases.NotSupported);

    /// <summary>The lease <paramref name="threadId"/> is held under now, or <c>null</c> when nobody holds it.</summary>
    ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(ThreadLeases.NotSupported);

    /// <summary>
    /// Appends <paramref name="checkpoint"/> only if <paramref name="lease"/>'s token is still its thread's
    /// latest, decided in the same atomic step as the append: otherwise nothing is written and it throws
    /// <see cref="LeaseLostException"/>. Only the token decides, not the expiry. With an
    /// <paramref name="expectedSequence"/> it is also conditional as <see cref="SaveIfLatestAsync"/> is, and
    /// returns <c>false</c> when something was appended since; without one it returns <c>true</c>.
    /// </summary>
    ValueTask<bool> SaveUnderLeaseAsync(
        Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(ThreadLeases.NotSupported);
}

/// <summary>
/// A thread's latest checkpoint, with its sequence in the thread's log: what
/// <see cref="ICheckpointStore.SaveIfLatestAsync"/> compares against. <paramref name="Sequence"/> is
/// <c>null</c> when the store can't tell (it doesn't override <see cref="ICheckpointStore.LoadLatestAsync"/>).
/// </summary>
public sealed record LatestCheckpoint(Checkpoint Checkpoint, long? Sequence);
