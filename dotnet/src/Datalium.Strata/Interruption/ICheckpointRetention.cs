namespace Datalium.Strata.Interruption;

/// <summary>
/// Deletes the logs of threads that are done with (<c>design/34-Storage-Hardening.md</c>, "Retention").
/// Separate from <see cref="ICheckpointStore"/>, like <see cref="ICheckpointQuery"/>, so a store that can
/// only save and load stays complete; every adapter in <c>Datalium.Strata.Storage</c> implements it. Mirrors
/// <c>rust/strata-core/src/store.rs</c>'s <c>CheckpointRetention</c>.
/// </summary>
public interface ICheckpointRetention
{
    /// <summary>
    /// Deletes every checkpoint of each thread whose <b>latest</b> checkpoint is in one of
    /// <paramref name="states"/> and was written before <paramref name="olderThan"/> (at second precision,
    /// exclusive, like <see cref="ThreadQuery.UpdatedBefore"/>), oldest first, up to
    /// <paramref name="limit"/> threads, with the thread's lease record. Returns how many threads it purged:
    /// call it again while it returns <paramref name="limit"/>.
    ///
    /// <paramref name="states"/> defaults to <see cref="CheckpointRetention.Finished"/>: threads that will not
    /// run again. Naming another state purges threads that could still be resumed, recovered or answered: a
    /// waiting approval is then gone. A thread a live lease holds is never purged, and neither is one that
    /// changed while it was being purged.
    ///
    /// Nothing in the engine calls this: retention is the application's to decide and schedule.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="limit"/> is not between 1 and <see cref="ThreadQuery.MaxLimit"/>.</exception>
    ValueTask<int> PurgeAsync(
        DateTimeOffset olderThan,
        IReadOnlySet<CheckpointThreadState>? states = null,
        int limit = ThreadQuery.MaxLimit,
        CancellationToken cancellationToken = default);
}

/// <summary>Helpers for <see cref="ICheckpointRetention"/>, shared by the adapters.</summary>
public static class CheckpointRetention
{
    /// <summary>
    /// The states <see cref="ICheckpointRetention.PurgeAsync"/> purges when none are named:
    /// <see cref="CheckpointThreadState.Completed"/> and <see cref="CheckpointThreadState.Ended"/>.
    /// </summary>
    public static IReadOnlySet<CheckpointThreadState> Finished { get; } =
        new HashSet<CheckpointThreadState> { CheckpointThreadState.Completed, CheckpointThreadState.Ended };

    /// <summary>
    /// Whether a thread whose latest checkpoint is <paramref name="checkpoint"/> is one a purge of
    /// <paramref name="states"/> older than <paramref name="olderThan"/> (stored text) takes. For stores that
    /// decide in code.
    /// </summary>
    public static bool Takes(Checkpoint checkpoint, IReadOnlySet<CheckpointThreadState> states, string olderThan) =>
        states.Contains(CheckpointThreadStates.Of(checkpoint))
        && string.CompareOrdinal(CheckpointTimestamps.UpdatedAt(checkpoint), olderThan) < 0;
}
