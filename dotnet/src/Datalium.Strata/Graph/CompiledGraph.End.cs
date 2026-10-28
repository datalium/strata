using System.Text.Json;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;

namespace Datalium.Strata.Graph;

/// <summary>Ending a thread where it stands (<c>design/28-Ending-A-Thread.md</c>, SG-34).</summary>
public sealed partial class CompiledGraph<TState>
{
    /// <summary>How many times an end reloads a thread that changed under it before giving up.</summary>
    private const int EndAttempts = 3;

    /// <summary>
    /// Ends <paramref name="threadId"/> where it stands: a thread waiting at an interrupt boundary (at any
    /// depth), or stopped by a failure or a cancellation, gets an <c>ended</c> checkpoint and can't be
    /// resumed again. Returns <see cref="GraphExecutionResult{TState}.Cancelled"/> with
    /// <see cref="CancelReason.Ended"/>: where the thread stood, and its last committed state.
    ///
    /// Ending an ended thread returns the same result and writes nothing. A thread with no checkpoint, a
    /// completed one, and one whose resume began (it may still be running) are refused with
    /// <see cref="GraphExecutionResult{TState}.Failed"/>, and nothing is written.
    ///
    /// A <c>running</c> thread (<c>design/33-Durable-Execution.md</c>) is ended at the node that was to run,
    /// unless a live lease holds it: then somebody is running it, and the end is refused. Without leases
    /// nothing stops a run that is alive after all from writing after the end.
    ///
    /// The save is conditional (<see cref="ICheckpointStore.SaveIfLatestAsync"/>): if the thread changes
    /// between the load and the save, the end loads it again and decides again.
    /// </summary>
    public Task<GraphExecutionResult<TState>> EndAsync(string threadId, ICheckpointStore store) =>
        EndAsync(threadId, new ThreadEnd(), store, NoOptions);

    /// <inheritdoc cref="EndAsync(string, ICheckpointStore)"/>
    public Task<GraphExecutionResult<TState>> EndAsync(string threadId, ThreadEnd end, ICheckpointStore store) =>
        EndAsync(threadId, end, store, NoOptions);

    /// <summary>
    /// <see cref="EndAsync(string, ICheckpointStore)"/> with <paramref name="end"/>'s
    /// <see cref="ThreadEnd.EndedBy"/>, and run-level <paramref name="options"/>: metadata for the ended
    /// checkpoint (laid over the stored caller keys, as for a resume), a run id, and an observer. The
    /// deadline, hop limit, history and services don't apply: nothing runs.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="options"/> has a reserved metadata key.</exception>
    public async Task<GraphExecutionResult<TState>> EndAsync(
        string threadId, ThreadEnd end, ICheckpointStore store, RunOptions options)
    {
        ArgumentNullException.ThrowIfNull(end);
        ArgumentNullException.ThrowIfNull(options);
        RunMetadata.Validate(options);
        using var root = RootScope.Start(threadId, options, CancellationToken.None, RunMode.End, Checkpointing);
        return await ObserveAsync(root, store, observed => EndFromAsync(threadId, end, observed, root.Level))
            .ConfigureAwait(false);
    }

    private async Task<GraphExecutionResult<TState>> EndFromAsync(
        string threadId, ThreadEnd end, ICheckpointStore store, LevelScope level)
    {
        var telemetry = level.Run.Telemetry;
        for (var attempt = 1; ; attempt++)
        {
            var latest = await store.LoadLatestAsync(threadId, CancellationToken.None).ConfigureAwait(false);
            var checkpoint = latest?.Checkpoint;
            if (attempt == 1)
            {
                telemetry.AdoptStored(checkpoint?.Metadata);
                telemetry.RunStarted(checkpoint?.SuspendedNode ?? string.Empty, Identity);
            }

            if (checkpoint is null)
            {
                return new GraphExecutionResult<TState>.Failed($"no checkpoint found for thread '{threadId}'");
            }

            if (Refusal(threadId, checkpoint) is { } refused)
            {
                return refused;
            }

            // Another graph's thread is refused, and another version's migrated (design/31-Graph-Identity.md).
            var adopted = Adopt(checkpoint, threadId, []);
            if (adopted.Problem is { } foreign)
            {
                return new GraphExecutionResult<TState>.Failed(foreign);
            }

            // A running thread stands at the node that was to run, at every level (design/33-Durable-Execution.md).
            var running = checkpoint.Status == CheckpointStatus.Running;
            checkpoint = running ? Standing(adopted.Checkpoint!) : adopted.Checkpoint!;

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

            var result = new GraphExecutionResult<TState>.Cancelled(
                threadId, checkpoint.SuspendedNode ?? string.Empty, CancelReason.Ended, state, checkpoint.WaitingAt());

            // Already ended: the same answer, and nothing to write.
            if (checkpoint.Status == CheckpointStatus.Ended)
            {
                return result;
            }

            // A thread another owner holds isn't ended under it (design/32-Thread-Leases.md).
            if (await AcquireLeaseAsync(threadId, store, level).ConfigureAwait(false) is { } held)
            {
                return held;
            }

            // Whoever holds a running thread is running it.
            if (running && await HeldByAnotherAsync(threadId, store, level).ConfigureAwait(false) is { } live)
            {
                return live;
            }

            var ended = Ended(checkpoint, level, end, NowRfc3339());
            switch (await PersistIfLatestAsync(store, ended, latest!.Sequence, level).ConfigureAwait(false))
            {
                case LeasedSave.Saved:
                    return result;
                case LeasedSave.LeaseLost:
                    return result with { Reason = CancelReason.LeaseLost };
            }

            // Something was appended since the load: a resume, another end, a new run. Decide again.
            if (attempt == EndAttempts)
            {
                return new GraphExecutionResult<TState>.Failed(
                    $"thread '{threadId}' kept changing while it was being ended");
            }
        }
    }

    /// <summary>Why <paramref name="checkpoint"/>, a thread's latest, can't be ended; <c>null</c> if it can.</summary>
    private static GraphExecutionResult<TState>.Failed? Refusal(string threadId, Checkpoint checkpoint)
    {
        if (!CheckpointSchema.IsSupported(checkpoint.SchemaVersion))
        {
            return new($"checkpoint for thread '{threadId}' has unsupported schema_version '{checkpoint.SchemaVersion}'");
        }

        if (checkpoint.ResumedAt is { } resumedAt)
        {
            return new(
                $"thread '{threadId}' was resumed at {resumedAt} and may still be running: only a waiting, failed or cancelled thread can be ended");
        }

        return checkpoint.Status == CheckpointStatus.Completed
            ? new($"thread '{threadId}' has completed: only a waiting, failed or cancelled thread can be ended")
            : null;
    }

    /// <summary>
    /// A <c>running</c> checkpoint with each level suspended at the node it was to run, which is where a
    /// running thread stands: what ending it records, as a failed or cancelled thread's checkpoint does.
    /// </summary>
    private static Checkpoint Standing(Checkpoint running) => running with
    {
        SuspendedNode = running.NextNode ?? running.SuspendedNode,
        Nested = running.Nested is { } child ? Standing(child) : null,
    };

    /// <summary>
    /// <paramref name="checkpoint"/> ended where it stands, at every level it nests: each keeps where it
    /// stood, its state and its decision, has no next node or wait, and carries the end's metadata (the
    /// run's, with the reason and who ended it) instead of the engine keys of the stop it ends.
    /// </summary>
    private static Checkpoint Ended(Checkpoint checkpoint, LevelScope level, ThreadEnd end, string now)
    {
        var engine = new Dictionary<string, string> { [ThreadEnd.ReasonKey] = end.Reason };
        if (end.EndedBy is { } endedBy)
        {
            engine[ThreadEnd.EndedByKey] = endedBy;
        }

        return CheckpointSchema.Versioned(checkpoint with
        {
            Status = CheckpointStatus.Ended,
            NextNode = null,
            Metadata = level.Run.CheckpointMetadata(new Dictionary<string, string>(engine)),
            CreatedAt = now,
            ResumedAt = null,
            Nested = checkpoint.Nested is { } child ? Ended(child, level, end, now) : null,

            // An ended thread never wakes (design/29-Event-Waits-And-Timers.md).
            Wait = null,
        });
    }
}
