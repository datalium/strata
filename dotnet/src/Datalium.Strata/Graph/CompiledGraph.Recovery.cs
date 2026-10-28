using System.Threading.Channels;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Graph;

/// <summary>Recovering a thread whose run died (<c>design/33-Durable-Execution.md</c>, SG-10).</summary>
public sealed partial class CompiledGraph<TState>
{
    private static readonly IReadOnlySet<CheckpointThreadState> RunningThreads =
        new HashSet<CheckpointThreadState> { CheckpointThreadState.Running };

    private const string NotDurable =
        "this graph doesn't checkpoint every node: only a graph compiled with CheckpointPolicy.EveryNode can recover a thread";

    /// <summary>
    /// Continues <paramref name="threadId"/>, whose latest checkpoint is <c>running</c>, where that checkpoint
    /// says: at its next node with its state, at every nesting level. For a thread whose process died. The
    /// node that was in flight runs again (<see cref="NodeContext.IsRecovery"/>, with the same
    /// <see cref="NodeContext.IdempotencyKey"/>); the nodes before it don't.
    ///
    /// The recovery claims the thread first, by saving the checkpoint again only if nothing was appended since
    /// it was loaded, so of two recoveries one runs, and a run that turns out to be alive is left alone. A
    /// thread a live lease holds is refused: somebody is running it. So are a thread that isn't
    /// <c>running</c>, another graph's thread, and a graph compiled without
    /// <see cref="CheckpointPolicy.EveryNode"/>, each with <see cref="GraphExecutionResult{TState}.Failed"/>
    /// and nothing written.
    ///
    /// Without leases nothing tells a dead run from a slow one: recover only a thread known to be dead.
    /// </summary>
    public Task<GraphExecutionResult<TState>> RecoverAsync(
        string threadId, ICheckpointStore store, CancellationToken cancellationToken = default) =>
        RecoverAsync(threadId, store, NoOptions, cancellationToken);

    /// <inheritdoc cref="RecoverAsync(string, ICheckpointStore, CancellationToken)"/>
    /// <exception cref="ArgumentException"><paramref name="options"/> has a reserved metadata key.</exception>
    public async Task<GraphExecutionResult<TState>> RecoverAsync(
        string threadId, ICheckpointStore store, RunOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        RunMetadata.Validate(options);
        using var admission = await AdmitAsync(options, cancellationToken).ConfigureAwait(false);
        if (admission is { IsAdmitted: false })
        {
            return NotAdmitted(admission);
        }

        using var root = RootScope.Start(threadId, options, cancellationToken, RunMode.Recover, Checkpointing, admission);
        return await ObserveAsync(root, store, observed =>
            RecoverFromAsync(threadId, observed, stream: null, root.Level)).ConfigureAwait(false);
    }

    /// <summary>
    /// <see cref="RecoverAsync(string, ICheckpointStore, CancellationToken)"/>, streaming the recovered run's
    /// chunks as <see cref="ResumeStreaming(string, HumanSignal, ICheckpointStore, CancellationToken)"/> does a
    /// resumed run's: what a graph with streaming nodes is recovered with. The stream starts at the node the
    /// recovery starts with. A recovery that is refused ends the stream with no chunks.
    /// </summary>
    public (ChannelReader<StreamChunk> Reader, Task<GraphExecutionResult<TState>> Completion) RecoverStreaming(
        string threadId, ICheckpointStore store, CancellationToken cancellationToken = default) =>
        RecoverStreaming(threadId, store, NoOptions, cancellationToken);

    /// <inheritdoc cref="RecoverStreaming(string, ICheckpointStore, CancellationToken)"/>
    public (ChannelReader<StreamChunk> Reader, Task<GraphExecutionResult<TState>> Completion) RecoverStreaming(
        string threadId, ICheckpointStore store, RunOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        RunMetadata.Validate(options);
        return Stream(threadId, threadId, store, options, RunMode.Recover, cancellationToken, (context, level, observed) =>
            RecoverFromAsync(threadId, observed, context, level));
    }

    private async Task<GraphExecutionResult<TState>> RecoverFromAsync(
        string threadId, ICheckpointStore store, StreamContext? stream, LevelScope level)
    {
        // With its sequence: the claim is conditional on it.
        var latest = await store.LoadLatestAsync(threadId, CancellationToken.None).ConfigureAwait(false);
        return await RecoverLoadedAsync(threadId, latest, store, stream, level).ConfigureAwait(false);
    }

    /// <summary>
    /// <see cref="RecoverFromAsync"/> for a latest checkpoint already loaded, with its sequence: what the
    /// driver does after deciding the thread is dead.
    /// </summary>
    private async Task<GraphExecutionResult<TState>> RecoverLoadedAsync(
        string threadId, LatestCheckpoint? latest, ICheckpointStore store, StreamContext? stream, LevelScope level)
    {
        var checkpoint = latest?.Checkpoint;
        level.Run.Telemetry.AdoptStored(checkpoint?.Metadata);
        level.Run.Telemetry.RunStarted(checkpoint?.NextNode ?? string.Empty, Identity);

        // A graph that doesn't checkpoint every node would recover the thread and then leave it running
        // again when it completed.
        if (Checkpointing != CheckpointPolicy.EveryNode)
        {
            return new GraphExecutionResult<TState>.Failed(NotDurable);
        }

        return checkpoint is null
            ? new GraphExecutionResult<TState>.Failed($"no checkpoint found for thread '{threadId}'")
            : await ResumeCheckpointAsync(
                    threadId, checkpoint, LevelSignals.None, store, stream, level, latest!.Sequence, recovering: true)
                .ConfigureAwait(false);
    }

    /// <summary>
    /// A level of the <c>running</c> checkpoint a durable resume or a recovery claims its thread with
    /// (<c>design/33-Durable-Execution.md</c>, "A resume under EveryNode"), and through each subgraph node's
    /// child every level nested below: <paramref name="checkpoint"/> as the run continues it. The level's
    /// overrides are applied to its state; the innermost level takes the route and records the decision a
    /// resume answers its boundary with; nothing waits any more. For a recovery, whose
    /// <paramref name="signals"/> answer nothing, it is the checkpoint again, written by this run. The resume
    /// check has passed, so nothing here can fail.
    /// </summary>
    internal Checkpoint Claim(Checkpoint checkpoint, LevelSignals signals, RunScope run, string now)
    {
        var start = RouteFor(checkpoint, signals) ?? checkpoint.NextNode!;
        var decision = AnswersBoundary(checkpoint) && signals.Wake == WakeReason.Human
            ? HumanDecision.From(signals.Innermost, checkpoint.SuspendedNode!, signals.DecidedAt!)
            : checkpoint.Decision;

        var state = checkpoint.FrozenState?.DeepClone();
        ApplyOverrides(state, checkpoint.Nested is null ? signals.Innermost : signals.Own);

        // Checked in preflight, so it reads.
        HopLimit.TryRead(checkpoint, out var hops, out _);
        var nested = checkpoint.Nested is { } child
            && _nodes.TryGetValue(start, out var entry)
            && entry.Claim is { } claimChild
                ? claimChild(child, signals.Descend(start), run, now)
                : null;

        return CheckpointSchema.Versioned(checkpoint with
        {
            Status = CheckpointStatus.Running,
            NextNode = start,
            FrozenState = state,
            Metadata = run.CheckpointMetadata(new Dictionary<string, string>
            {
                [HopLimit.MetadataKey] = HopLimit.Format(hops),
            }),
            CreatedAt = now,
            ResumedAt = null,
            Nested = nested,
            Decision = decision,
            Wait = null,
            Graph = Identity,
        });
    }

    /// <summary>
    /// Recovers the threads of <paramref name="store"/> whose run died (<c>design/33-Durable-Execution.md</c>,
    /// "Who is dead"): each <c>running</c> thread that no live lease holds and whose latest checkpoint is older
    /// than <see cref="RecoverOptions.StaleAfter"/>, as
    /// <see cref="RecoverAsync(string, ICheckpointStore, CancellationToken)"/> does, one at a time, with
    /// <see cref="RecoverOptions.Run"/>'s options and <paramref name="cancellationToken"/>, until
    /// <see cref="RecoverOptions.Limit"/> threads have been recovered. "Now" is <see cref="RecoverOptions.Run"/>'s
    /// <see cref="RunOptions.TimeProvider"/>, read once.
    ///
    /// Threads are listed by state (this graph's, by identity, and those matching
    /// <see cref="RecoverOptions.Metadata"/>), and each is loaded and decided again first: one that is no longer
    /// running or stale, that a live lease holds, that isn't this graph's, or that was recovered
    /// <see cref="RecoverOptions.MaxRecoveries"/> times already is skipped with nothing written. The claim is
    /// conditional, so of two drivers that list the same thread one recovers it and the other skips it.
    ///
    /// A library call: the application calls it on its own cadence (no hosted service). An exception from a
    /// recovered run propagates, as from <see cref="RecoverAsync(string, ICheckpointStore, CancellationToken)"/>.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The limit, the recovery limit or the age is out of range.</exception>
    /// <exception cref="ArgumentException">
    /// The run options have a reserved metadata key, or neither a lease nor a <see cref="RecoverOptions.StaleAfter"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">The graph wasn't compiled with <see cref="CheckpointPolicy.EveryNode"/>.</exception>
    public async Task<RecoveryReport<TState>> RecoverStaleAsync<TStore>(
        TStore store, RecoverOptions? options = null, CancellationToken cancellationToken = default)
        where TStore : ICheckpointStore, ICheckpointQuery
    {
        ArgumentNullException.ThrowIfNull(store);
        options ??= new RecoverOptions();
        ArgumentNullException.ThrowIfNull(options.Run);
        ThreadQuery.CheckLimit(options.Limit);
        RunMetadata.Validate(options.Run);
        if (options.MaxRecoveries is < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options), options.MaxRecoveries, "MaxRecoveries must be at least 1");
        }

        if (options.StaleAfter < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.StaleAfter, "StaleAfter must not be negative");
        }

        if (Checkpointing != CheckpointPolicy.EveryNode)
        {
            throw new InvalidOperationException(NotDurable);
        }

        // Without leases the age of a running checkpoint is the only sign that its run died, and only the
        // caller knows how long its nodes take.
        var staleAfter = options.StaleAfter
            ?? (options.Run.Lease is not null
                ? TimeSpan.Zero
                : throw new ArgumentException(
                    "RecoverStaleAsync needs RecoverOptions.StaleAfter when its runs have no lease: without leases only the age of a running checkpoint says its run died",
                    nameof(options)));

        var now = (options.Run.TimeProvider ?? TimeProvider.System).GetUtcNow();
        DateTimeOffset? staleBefore = staleAfter > TimeSpan.Zero ? now - staleAfter : null;

        // Only this graph's threads, when it has an identity (design/31-Graph-Identity.md); any version it may
        // migrate.
        var graph = options.MatchGraph ? Identity : null;
        var recovered = new List<RecoveredThread<TState>>();
        var skipped = new List<SkippedThread>();
        bool Stopped() => cancellationToken.IsCancellationRequested || options.Stop.IsCancellationRequested;
        string? cursor = null;
        do
        {
            var page = await store.ListThreadsAsync(
                    new ThreadQuery
                    {
                        States = RunningThreads,
                        Metadata = options.Metadata,
                        UpdatedBefore = staleBefore,
                        GraphId = graph?.Id,
                        GraphVersion = Migration is null ? graph?.Version : null,
                        Cursor = cursor,
                    },
                    CancellationToken.None)
                .ConfigureAwait(false);
            foreach (var thread in page.Threads)
            {
                if (recovered.Count >= options.Limit || Stopped())
                {
                    return new RecoveryReport<TState>(recovered, skipped);
                }

                var (recoveredThread, skippedThread) =
                    await RecoverOneAsync(thread, staleBefore, options, store, cancellationToken)
                        .ConfigureAwait(false);
                if (recoveredThread is not null)
                {
                    recovered.Add(recoveredThread);
                }
                else
                {
                    skipped.Add(skippedThread!);
                }
            }

            cursor = page.NextCursor;
        }
        while (cursor is not null && recovered.Count < options.Limit && !Stopped());

        return new RecoveryReport<TState>(recovered, skipped);
    }

    /// <summary>
    /// Decides again whether <paramref name="listed"/>'s thread is running, stale, this graph's and nobody's, then
    /// recovers it. With a scheduler, it waits for a slot first (<c>design/40-Run-Scheduler.md</c>).
    /// </summary>
    private async Task<(RecoveredThread<TState>? Recovered, SkippedThread? Skipped)> RecoverOneAsync(
        ThreadSummary listed,
        DateTimeOffset? staleBefore,
        RecoverOptions options,
        ICheckpointStore store,
        CancellationToken cancellationToken)
    {
        var threadId = listed.ThreadId;
        using var admission = await AdmitAsync(options.Run, cancellationToken, listed.Metadata, options.Stop)
            .ConfigureAwait(false);
        if (admission is { IsAdmitted: false })
        {
            return Skip($"it was not admitted: {NotAdmitted(admission).Message}");
        }

        // With its sequence: the claim is conditional on it.
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

        if (checkpoint.Status != CheckpointStatus.Running)
        {
            return Skip("it is no longer running");
        }

        if (staleBefore is { } before
            && string.CompareOrdinal(CheckpointTimestamps.UpdatedAt(checkpoint), CheckpointTimestamps.Text(before)) >= 0)
        {
            return Skip("it is not stale yet");
        }

        var recoveries = RecoveryCount.Of(checkpoint);
        if (recoveries >= options.MaxRecoveries)
        {
            return Skip($"it was recovered {recoveries} time(s) without a node completing");
        }

        // Another graph's thread is skipped, and another version's migrated (design/31-Graph-Identity.md).
        var adopted = Adopt(checkpoint, threadId, []);
        if (adopted.Problem is { } foreign)
        {
            return Skip(foreign);
        }

        if (CheckResume(adopted.Checkpoint!, LevelSignals.None, threadId, []).Problem is { } problem)
        {
            return Skip(problem);
        }

        // Whoever holds a running thread is running it. Checked here, before a run is started for it, so a
        // sweep over live threads is one read each and no events; the recovery checks again when it claims.
        try
        {
            if (await store.GetLeaseAsync(threadId, CancellationToken.None).ConfigureAwait(false) is { } live)
            {
                return Skip($"it is held by '{live.Owner}' until {WakeTimes.Text(live.ExpiresAt)}");
            }
        }
        catch (NotSupportedException)
        {
            // A store without leases: nothing can hold the thread.
        }

        using var root = RootScope.Start(threadId, options.Run, cancellationToken, RunMode.Recover, Checkpointing, admission);
        var result = await ObserveAsync(root, store, observed =>
                RecoverLoadedAsync(threadId, latest, observed, stream: null, root.Level))
            .ConfigureAwait(false);
        if (root.Level.Run.Held is { } held)
        {
            return Skip($"it is {held}");
        }

        return root.Level.Run.Superseded
            ? Skip("another run or recovery got there first")
            : (new RecoveredThread<TState>(
                threadId, RecoveryCount.ContinuesAt(adopted.Checkpoint!), recoveries + 1, result), null);

        (RecoveredThread<TState>?, SkippedThread?) Skip(string why) => (null, new SkippedThread(threadId, why));
    }
}
