using Datalium.Strata.Interruption;

namespace Datalium.Strata.Graph;

/// <summary>Running a thread under a lease (<c>design/32-Thread-Leases.md</c>, SG-12).</summary>
public sealed partial class CompiledGraph<TState>
{
    /// <summary>
    /// Takes the thread's lease for a run that has one (<see cref="RunOptions.Lease"/>), before its first save.
    /// Returns the refusal when another owner holds the thread, <see langword="null"/> when the run may go on:
    /// it holds the lease now, or runs without one. Only the outermost level saves, so only it acquires.
    /// </summary>
    private static async ValueTask<GraphExecutionResult<TState>.Failed?> AcquireLeaseAsync(
        string threadId, ICheckpointStore store, LevelScope level)
    {
        if (!level.IsRoot || level.Run.Lease is not { } lease)
        {
            return null;
        }

        if (await lease.AcquireAsync(store, threadId).ConfigureAwait(false) is not { } holder)
        {
            return null;
        }

        // Rounded up to the second, so the thread isn't said to be free before it is.
        var held = $"held by '{holder.Owner}' until {WakeTimes.Text(holder.ExpiresAt)}";
        level.Run.Held = held;
        return new GraphExecutionResult<TState>.Failed($"thread '{threadId}' is {held}");
    }

    /// <summary>
    /// Saves <paramref name="checkpoint"/> at <paramref name="level"/>: under the run's lease at the outermost
    /// level, when it has one. <see langword="false"/> means the lease is lost and nothing was written; the run
    /// then ends with <see cref="LeaseLostAt"/>.
    /// </summary>
    private static async ValueTask<bool> PersistAsync(ICheckpointStore store, Checkpoint checkpoint, LevelScope level)
    {
        if (level.IsRoot && level.Run.Lease is { } lease)
        {
            if (await lease.SaveAsync(store, checkpoint, expectedSequence: null).ConfigureAwait(false)
                == LeasedSave.LeaseLost)
            {
                return false;
            }
        }
        else
        {
            await store.SaveAsync(checkpoint, CancellationToken.None).ConfigureAwait(false);
        }

        Saved(checkpoint, level);
        return true;
    }

    /// <summary>
    /// Notes what the run just saved at the outermost level: a <c>running</c> checkpoint is what its thread's log
    /// now ends with, and anything else means the thread is no longer running
    /// (<see cref="RunScope.LastRunning"/>, <c>design/33-Durable-Execution.md</c>).
    /// </summary>
    private static void Saved(Checkpoint checkpoint, LevelScope level)
    {
        if (level.IsRoot)
        {
            level.Run.LastRunning = checkpoint.Status == CheckpointStatus.Running ? checkpoint : null;
        }
    }

    /// <summary>
    /// <see cref="PersistAsync"/> for the outermost level's conditional saves (a resume's stamp or claim, a
    /// recovery's claim, an end): only if the thread's latest sequence is still <paramref name="sequence"/>.
    /// </summary>
    private static async ValueTask<LeasedSave> PersistIfLatestAsync(
        ICheckpointStore store, Checkpoint checkpoint, long? sequence, LevelScope level)
    {
        var saved = level.Run.Lease is { } lease
            ? await lease.SaveAsync(store, checkpoint, sequence).ConfigureAwait(false)
            : await store.SaveIfLatestAsync(checkpoint, sequence, CancellationToken.None).ConfigureAwait(false)
                ? LeasedSave.Saved
                : LeasedSave.NotLatest;
        if (saved == LeasedSave.Saved)
        {
            Saved(checkpoint, level);
        }

        return saved;
    }

    /// <summary>
    /// For a run without a lease of its own that is about to take over a <c>running</c> thread (a recovery, an
    /// end): the refusal when a live lease holds the thread, since whoever holds it is running it
    /// (<c>design/33-Durable-Execution.md</c>). <see langword="null"/> when nobody does, when the store has no
    /// leases, and for a run with a lease, which has acquired it by now.
    /// </summary>
    private static async ValueTask<GraphExecutionResult<TState>.Failed?> HeldByAnotherAsync(
        string threadId, ICheckpointStore store, LevelScope level)
    {
        if (level.Run.Lease is not null)
        {
            return null;
        }

        ThreadLease? live;
        try
        {
            live = await store.GetLeaseAsync(threadId, CancellationToken.None).ConfigureAwait(false);
        }
        catch (NotSupportedException)
        {
            return null;
        }

        if (live is null)
        {
            return null;
        }

        var held = $"held by '{live.Owner}' until {WakeTimes.Text(live.ExpiresAt)}";
        level.Run.Held = held;
        return new GraphExecutionResult<TState>.Failed($"thread '{threadId}' is {held}");
    }

    /// <summary>
    /// How a run that lost its lease ends: cancelled at <paramref name="node"/> with <paramref name="state"/>,
    /// as any cancellation reports them, with nothing written.
    /// </summary>
    private static GraphExecutionResult<TState>.Cancelled LeaseLostAt(
        string threadId, string node, TState state, IReadOnlyList<string> stoppedAt) =>
        new(threadId, node, CancelReason.LeaseLost, state, stoppedAt);
}
