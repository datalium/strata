using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Graph;

/// <summary>
/// The executor: human-in-the-loop interruption (STRATA-SPEC-002, Freeze/Flush/Evict), streaming
/// (STRATA-SPEC-001), error recovery, and cancellation (<c>design/17-Async-Execution.md</c>).
///
/// <see cref="ICheckpointStore"/> is the port; concrete adapters live in
/// <c>Datalium.Strata.Storage</c>, which depends on this project for the interface and the
/// <see cref="Checkpoint"/> shape — not the other way around. The store is still synchronous (SG-11
/// makes it async), so the executor calls it inline.
///
/// Execution is asynchronous: an async handler is awaited, and a sync one runs on the thread pool, so
/// the executor never blocks a thread on a node. "Evict" means that once
/// <c>GraphExecutionResult.Interrupted</c> (or <c>Cancelled</c>) is returned, nothing of the run is
/// still being waited for; the only thing that survives is whatever the store persisted. A handler the
/// engine gave up on (a timeout, a cancellation) may still be running, but nothing waits for it and its
/// result is discarded.
/// </summary>
public sealed partial class CompiledGraph<TState>
{
    private static readonly RunOptions NoOptions = new();

    /// <summary>
    /// Executes from the entry point. Runs nodes to completion, or until an interrupt boundary is hit,
    /// in which case the state is frozen and flushed to <paramref name="store"/> before returning
    /// <see cref="GraphExecutionResult{TState}.Interrupted"/>.
    ///
    /// <paramref name="cancellationToken"/> reaches every node, branch and subgraph at any depth.
    /// Cancelling it ends the run with <see cref="GraphExecutionResult{TState}.Cancelled"/> and a
    /// <c>cancelled</c> checkpoint at the node that was running, which a resume re-enters; it doesn't
    /// throw <see cref="OperationCanceledException"/>.
    /// </summary>
    public Task<GraphExecutionResult<TState>> RunAsync(
        string threadId, TState initialState, ICheckpointStore store, CancellationToken cancellationToken = default) =>
        RunAsync(threadId, initialState, store, NoOptions, cancellationToken);

    /// <summary>
    /// <see cref="RunAsync(string, TState, ICheckpointStore, CancellationToken)"/> with run-level
    /// <paramref name="options"/>: a history store and services for every async handler's
    /// <see cref="NodeContext"/>, a deadline, metadata for every checkpoint, a run id, and an observer
    /// of the run's events.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="options"/> has a reserved metadata key.</exception>
    public async Task<GraphExecutionResult<TState>> RunAsync(
        string threadId,
        TState initialState,
        ICheckpointStore store,
        RunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        RunMetadata.Validate(options);
        using var admission = await AdmitAsync(options, cancellationToken).ConfigureAwait(false);
        if (admission is { IsAdmitted: false })
        {
            return NotAdmitted(admission);
        }

        using var root = RootScope.Start(threadId, options, cancellationToken, RunMode.Run, Checkpointing, admission);
        return await ObserveAsync(root, store, observed =>
            StartAsync(threadId, initialState, observed, stream: null, root.Level)).ConfigureAwait(false);
    }

    /// <summary>
    /// A fresh run of the outermost graph: takes the thread's lease, when the run has one
    /// (<c>design/32-Thread-Leases.md</c>), before the first node starts, then runs from the entry point. A
    /// durable run first says it is running (<c>design/33-Durable-Execution.md</c>): a process that dies in
    /// the first node then leaves a thread to recover, not nothing.
    /// </summary>
    private async Task<GraphExecutionResult<TState>> StartAsync(
        string threadId, TState initialState, ICheckpointStore store, StreamContext? stream, LevelScope level)
    {
        level.Run.Telemetry.RunStarted(Topology.EntryPoint, Identity);
        if (await AcquireLeaseAsync(threadId, store, level).ConfigureAwait(false) is { } held)
        {
            return held;
        }

        if (level.Run.Durable)
        {
            var running = RunningAt(
                threadId,
                node: null,
                Topology.EntryPoint,
                JsonSerializer.SerializeToNode(initialState),
                level with { Hops = 0 });
            if (!await PersistAsync(store, running, level).ConfigureAwait(false))
            {
                return LeaseLostAt(threadId, Topology.EntryPoint, initialState, [Topology.EntryPoint]);
            }
        }

        return await ExecuteFromAsync(threadId, Topology.EntryPoint, initialState, store, stream, level)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// A level's <c>running</c> checkpoint (<c>design/33-Durable-Execution.md</c>): <paramref name="node"/>
    /// committed (<see langword="null"/> before the first), <paramref name="next"/> is where the level goes on,
    /// and <paramref name="state"/> is what it starts with. With <paramref name="child"/>, the level is inside
    /// the subgraph node <paramref name="next"/>, which a recovery re-enters. <paramref name="level"/> gives
    /// the hops made before <paramref name="next"/>.
    /// </summary>
    private Checkpoint RunningAt(
        string threadId, string? node, string next, JsonNode? state, LevelScope level, Checkpoint? child = null) =>
        CheckpointSchema.Versioned(new Checkpoint
        {
            SchemaVersion = CheckpointSchema.RunningVersion,
            ThreadId = threadId,
            Status = CheckpointStatus.Running,
            SuspendedNode = node,
            NextNode = next,
            FrozenState = state,
            Metadata = level.CheckpointMetadata(),
            CreatedAt = NowRfc3339(),
            ResumedAt = null,
            Nested = child,
            Decision = level.Decision,
            Graph = Identity,
        });

    /// <summary>
    /// Runs <paramref name="run"/> against the caller's store as the run's telemetry sees it
    /// (<see cref="ObservedStore"/>), and ends the run's telemetry with its result, or with the
    /// exception propagating out of it (<c>design/18-Run-Observability.md</c>).
    /// </summary>
    private static async Task<GraphExecutionResult<TState>> ObserveAsync(
        RootScope root, ICheckpointStore store, Func<ICheckpointStore, Task<GraphExecutionResult<TState>>> run)
    {
        var telemetry = root.Telemetry;
        var observed = new ObservedStore(store, telemetry);
        GraphExecutionResult<TState> result;
        try
        {
            result = await run(observed).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await FailRunningAsync(root.Level, observed, ex.Message).ConfigureAwait(false);
            await ReleaseAsync().ConfigureAwait(false);
            telemetry.RunCompleted(RunOutcome.Failed, ex.Message, reason: null);
            throw;
        }

        if (result is GraphExecutionResult<TState>.Failed gaveUp)
        {
            await FailRunningAsync(root.Level, observed, gaveUp.Message).ConfigureAwait(false);
        }

        await ReleaseAsync().ConfigureAwait(false);

        // The run's lease is given up when the run returns, however it ends (design/32-Thread-Leases.md).
        ValueTask ReleaseAsync() => root.Level.Run.Lease?.ReleaseAsync() ?? ValueTask.CompletedTask;

        switch (result)
        {
            case GraphExecutionResult<TState>.Completed:
                telemetry.RunCompleted(RunOutcome.Completed, error: null, reason: null);
                break;
            case GraphExecutionResult<TState>.Interrupted interrupted:
                telemetry.Interrupted(interrupted.WaitingAt, interrupted.NextNode, interrupted.Wait);
                telemetry.RunCompleted(RunOutcome.Interrupted, error: null, reason: null);
                break;
            case GraphExecutionResult<TState>.Cancelled cancelled:
                telemetry.RunCompleted(RunOutcome.Cancelled, error: null, cancelled.Reason);
                break;
            case GraphExecutionResult<TState>.Failed failed:
                telemetry.RunCompleted(
                    RunOutcome.Failed,
                    failed.Message,
                    reason: null,
                    failed is GraphExecutionResult<TState>.HopLimitExceeded ? "hop_limit" : null);
                break;
        }

        return result;
    }

    /// <summary>
    /// For a durable run that is ending without having said how (<c>design/33-Durable-Execution.md</c>, "What a
    /// durable run writes"): it returned <c>Failed</c> for something no checkpoint recorded (an edge with no
    /// matching condition, a node it can't run), or an exception is propagating out of it, and its thread's
    /// log still ends with a <c>running</c> checkpoint of its own. Saves a <c>failed</c> one in its place, so
    /// a process that is alive never leaves a thread <c>running</c>. A run that lost its lease writes nothing,
    /// as always, and a store that fails now leaves the thread running, for a recovery.
    /// </summary>
    private static async ValueTask FailRunningAsync(LevelScope level, ICheckpointStore store, string error)
    {
        if (level.Run.LastRunning is not { } running)
        {
            return;
        }

        try
        {
            await PersistAsync(store, Failing(running, error, NowRfc3339()), level).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // What the run is ending with stands.
        }
    }

    /// <summary>
    /// <paramref name="running"/> as a <c>failed</c> checkpoint, at every level: failed at the node that was
    /// to run, with the state it would have started with, so a resume starts it.
    /// </summary>
    private static Checkpoint Failing(Checkpoint running, string error, string now)
    {
        var node = running.NextNode ?? running.SuspendedNode;
        var metadata = new Dictionary<string, string>(running.Metadata ?? [])
        {
            ["error"] = error,
            ["error_kind"] = "fatal",
        };
        metadata.Remove(RecoveryCount.MetadataKey);
        return CheckpointSchema.Versioned(running with
        {
            Status = CheckpointStatus.Failed,
            SuspendedNode = node,
            NextNode = node,
            Metadata = metadata,
            CreatedAt = now,
            Nested = running.Nested is { } child ? Failing(child, error, now) : null,
        });
    }

    /// <summary>
    /// Rehydrates the frozen state for <paramref name="threadId"/> from <paramref name="store"/>,
    /// merges in <paramref name="signal"/>'s field overrides, and continues execution from the
    /// checkpoint's next node.
    ///
    /// If the checkpoint nests a child's (an interrupt inside a subgraph — see
    /// <see cref="StateGraph{TState}.AddSubgraph{TChild}"/>), the overrides go to the innermost
    /// graph's state instead, which is the one a human reviewed, and execution re-enters the child
    /// where it froze. Pass <see cref="LevelSignals"/> instead of a bare signal to send different
    /// overrides to outer levels too.
    ///
    /// A <c>Failed</c> or <c>Cancelled</c> checkpoint can be resumed as well: execution re-enters the
    /// node that failed or was stopped, with the last committed state, once per checkpoint (stopping
    /// again writes a new one). For a subgraph node it re-enters the child at the node that failed or
    /// was stopped there, so what a human answered before is not asked again.
    /// </summary>
    public Task<GraphExecutionResult<TState>> ResumeAsync(
        string threadId, HumanSignal signal, ICheckpointStore store, CancellationToken cancellationToken = default) =>
        ResumeAsync(threadId, new LevelSignals(signal), store, NoOptions, cancellationToken);

    /// <inheritdoc cref="ResumeAsync(string, HumanSignal, ICheckpointStore, CancellationToken)"/>
    public Task<GraphExecutionResult<TState>> ResumeAsync(
        string threadId, LevelSignals signals, ICheckpointStore store, CancellationToken cancellationToken = default) =>
        ResumeAsync(threadId, signals, store, NoOptions, cancellationToken);

    /// <inheritdoc cref="ResumeAsync(string, HumanSignal, ICheckpointStore, CancellationToken)"/>
    public Task<GraphExecutionResult<TState>> ResumeAsync(
        string threadId,
        HumanSignal signal,
        ICheckpointStore store,
        RunOptions options,
        CancellationToken cancellationToken = default) =>
        ResumeAsync(threadId, new LevelSignals(signal), store, options, cancellationToken);

    /// <inheritdoc cref="ResumeAsync(string, HumanSignal, ICheckpointStore, CancellationToken)"/>
    public async Task<GraphExecutionResult<TState>> ResumeAsync(
        string threadId,
        LevelSignals signals,
        ICheckpointStore store,
        RunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        RunMetadata.Validate(options);
        using var admission = await AdmitAsync(options, cancellationToken).ConfigureAwait(false);
        if (admission is { IsAdmitted: false })
        {
            return NotAdmitted(admission);
        }

        using var root = RootScope.Start(threadId, options, cancellationToken, RunMode.Resume, Checkpointing, admission);
        return await ObserveAsync(root, store, observed =>
            ResumeFromAsync(threadId, signals, observed, stream: null, root.Level)).ConfigureAwait(false);
    }

    /// <summary>
    /// Like <see cref="ResumeAsync(string, HumanSignal, ICheckpointStore, CancellationToken)"/>, but
    /// drives the resumed run on a background <see cref="Task"/> and streams its
    /// <c>StreamChunk</c>s to the returned reader, the way <see cref="RunStreaming(string, TState, ICheckpointStore, CancellationToken)"/>
    /// does for a fresh run — so a streaming node the run reaches <i>after</i> the interrupt boundary
    /// works.
    ///
    /// The stream starts at the node the run resumes into (<c>NodeStart</c> for it) and says nothing
    /// about the nodes that ran before the freeze. When the checkpoint nests a child's, the parent's
    /// subgraph node emits a new <c>NodeStart</c> (its first one, from the run that froze, never got a
    /// <c>NodeEnd</c>) and the child's chunks follow, named <c>"&lt;subgraph&gt;/&lt;child node&gt;"</c>.
    /// A checkpoint that can't be resumed (none for the thread, already resumed, unsupported version)
    /// ends the stream with no chunks and the task yields <c>Failed</c>, exactly as
    /// <see cref="ResumeAsync(string, HumanSignal, ICheckpointStore, CancellationToken)"/> would return it.
    /// </summary>
    public (ChannelReader<StreamChunk> Reader, Task<GraphExecutionResult<TState>> Completion) ResumeStreaming(
        string threadId, HumanSignal signal, ICheckpointStore store, CancellationToken cancellationToken = default) =>
        ResumeStreaming(threadId, new LevelSignals(signal), store, NoOptions, cancellationToken);

    /// <inheritdoc cref="ResumeStreaming(string, HumanSignal, ICheckpointStore, CancellationToken)"/>
    public (ChannelReader<StreamChunk> Reader, Task<GraphExecutionResult<TState>> Completion) ResumeStreaming(
        string threadId, LevelSignals signals, ICheckpointStore store, CancellationToken cancellationToken = default) =>
        ResumeStreaming(threadId, signals, store, NoOptions, cancellationToken);

    /// <inheritdoc cref="ResumeStreaming(string, HumanSignal, ICheckpointStore, CancellationToken)"/>
    public (ChannelReader<StreamChunk> Reader, Task<GraphExecutionResult<TState>> Completion) ResumeStreaming(
        string threadId,
        HumanSignal signal,
        ICheckpointStore store,
        RunOptions options,
        CancellationToken cancellationToken = default) =>
        ResumeStreaming(threadId, new LevelSignals(signal), store, options, cancellationToken);

    /// <inheritdoc cref="ResumeStreaming(string, HumanSignal, ICheckpointStore, CancellationToken)"/>
    public (ChannelReader<StreamChunk> Reader, Task<GraphExecutionResult<TState>> Completion) ResumeStreaming(
        string threadId,
        LevelSignals signals,
        ICheckpointStore store,
        RunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        RunMetadata.Validate(options);
        return Stream(threadId, threadId, store, options, RunMode.Resume, cancellationToken, (context, level, observed) =>
            ResumeFromAsync(threadId, signals, observed, context, level));
    }

    /// <summary>
    /// A resume of the outermost graph: loads its checkpoint, settles the run's metadata (the stored
    /// caller keys with the resume's laid over them), and starts the run.
    /// </summary>
    private async Task<GraphExecutionResult<TState>> ResumeFromAsync(
        string threadId, LevelSignals signals, ICheckpointStore store, StreamContext? stream, LevelScope level)
    {
        // With its sequence, so the resume stamp can't land after another resume's, or an end
        // (design/28-Ending-A-Thread.md, "Races").
        var latest = await store.LoadLatestAsync(threadId, CancellationToken.None).ConfigureAwait(false);
        return await ResumeLoadedAsync(threadId, latest, signals, store, stream, level).ConfigureAwait(false);
    }

    /// <summary>
    /// <see cref="ResumeFromAsync"/> for a latest checkpoint already loaded, with its sequence: what a wake
    /// does after deciding the thread is due (<c>design/29-Event-Waits-And-Timers.md</c>).
    /// </summary>
    private async Task<GraphExecutionResult<TState>> ResumeLoadedAsync(
        string threadId,
        LatestCheckpoint? latest,
        LevelSignals signals,
        ICheckpointStore store,
        StreamContext? stream,
        LevelScope level)
    {
        var checkpoint = latest?.Checkpoint;
        level.Run.Telemetry.AdoptStored(checkpoint?.Metadata);
        if (checkpoint is null)
        {
            level.Run.Telemetry.RunStarted(string.Empty, Identity);
            return new GraphExecutionResult<TState>.Failed($"no checkpoint found for thread '{threadId}'");
        }

        // A wake through a resume is decided at this load, the one the stamp is conditional on, on the run's
        // clock (design/42-Waking-Through-A-Resume.md).
        signals = signals.WhenDueAt(checkpoint, CheckpointTimestamps.Text(level.Run.Clock.GetUtcNow()));
        level.Run.Telemetry.RunStarted(StartsAt(checkpoint, signals) ?? string.Empty, Identity);
        return await ResumeCheckpointAsync(threadId, checkpoint, signals, store, stream, level, latest!.Sequence)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// The node a resume of <paramref name="checkpoint"/> with <paramref name="signals"/> starts at, for its
    /// <c>RunStarted</c>: the route it takes, or its next node, or for a timeout that ends the thread where it
    /// stands, the node it waits at.
    /// </summary>
    private string? StartsAt(Checkpoint checkpoint, LevelSignals signals)
    {
        if (RouteFor(checkpoint, signals) is { } route)
        {
            return route;
        }

        return signals.Wake == WakeReason.Timeout && AnswersBoundary(checkpoint)
            ? checkpoint.SuspendedNode
            : checkpoint.NextNode;
    }

    /// <summary>
    /// <see cref="ResumeAsync(string, HumanSignal, ICheckpointStore, CancellationToken)"/> for a
    /// checkpoint already in hand: what a subgraph node calls to resume its child from the checkpoint
    /// nested in the parent's. <paramref name="stream"/> is the child's scoped context, if the run it
    /// is inside is streaming, and <paramref name="level"/> the child's level of that run.
    /// </summary>
    internal Task<GraphExecutionResult<TState>> ResumeInlineAsync(
        string threadId,
        Checkpoint checkpoint,
        LevelSignals signals,
        ICheckpointStore store,
        StreamContext? stream,
        LevelScope level) =>
        ResumeCheckpointAsync(threadId, checkpoint, signals, store, stream, level);

    /// <summary>
    /// Resumes <paramref name="checkpoint"/>. At the outermost level, <paramref name="sequence"/> is its
    /// sequence in the thread's log (<c>null</c> when the store can't tell), and the resume stamp is only
    /// written if nothing was appended to the thread since.
    ///
    /// With <paramref name="recovering"/> it continues a <c>running</c> thread whose run died instead
    /// (<c>design/33-Durable-Execution.md</c>): <paramref name="signals"/> answer nothing, and only a
    /// <c>running</c> checkpoint is taken. A durable run's claim, for a resume or a recovery, is a
    /// <c>running</c> checkpoint (<see cref="Claim"/>) in place of the stamp.
    /// </summary>
    private async Task<GraphExecutionResult<TState>> ResumeCheckpointAsync(
        string threadId,
        Checkpoint checkpoint,
        LevelSignals signals,
        ICheckpointStore store,
        StreamContext? stream,
        LevelScope level,
        long? sequence = null,
        bool recovering = false)
    {
        if (!CheckpointSchema.IsSupported(checkpoint.SchemaVersion))
        {
            return new GraphExecutionResult<TState>.Failed(
                $"checkpoint for thread '{threadId}' has unsupported schema_version '{checkpoint.SchemaVersion}'");
        }

        // A running thread isn't waiting for anyone: its run is going on, or died and is recovered. And only a
        // running thread is recovered. A nested level is whatever its outermost level is.
        if (level.IsRoot && recovering != (checkpoint.Status == CheckpointStatus.Running))
        {
            return new GraphExecutionResult<TState>.Failed(recovering
                ? $"thread '{threadId}' is not running: its latest checkpoint is {CheckpointThreadStates.Of(checkpoint).Name()}"
                : $"thread '{threadId}' is running: only a waiting, failed or cancelled thread can be resumed");
        }

        if (checkpoint.Status == CheckpointStatus.Ended)
        {
            return new GraphExecutionResult<TState>.Failed(
                $"thread '{threadId}' was ended: an ended thread can't be resumed");
        }

        if (checkpoint.Status == CheckpointStatus.Completed)
        {
            return new GraphExecutionResult<TState>.Failed(
                $"checkpoint for thread '{threadId}' is not resumable (status: {checkpoint.Status}): only an interrupted, failed or cancelled checkpoint can be");
        }

        if (checkpoint.ResumedAt is not null)
        {
            return new GraphExecutionResult<TState>.Failed($"checkpoint for thread '{threadId}' was already resumed");
        }

        // A wake the thread wasn't due for, decided at the load (design/42-Waking-Through-A-Resume.md).
        if (signals.WhenDue)
        {
            return new GraphExecutionResult<TState>.Failed(LevelSignals.NotDue(threadId, checkpoint));
        }

        // Another graph's thread is refused, and another version's migrated, at every level, before anything
        // else is checked against it (design/31-Graph-Identity.md).
        var adopted = Adopt(checkpoint, threadId, []);
        if (adopted.Problem is { } foreign)
        {
            return new GraphExecutionResult<TState>.Failed(foreign);
        }

        checkpoint = adopted.Checkpoint!;
        if (signals.Check(checkpoint) is { } problem)
        {
            return new GraphExecutionResult<TState>.Failed($"cannot resume thread '{threadId}': {problem}");
        }

        // Everything that can refuse the resume is checked before the checkpoint is stamped, at every
        // level the signals reach, so a resume that can't start leaves the thread resumable
        // (design/21-Typed-Human-Signals.md, "Preflight").
        var check = CheckResume(checkpoint, signals, threadId, []);
        if (check.Problem is { } refused)
        {
            return new GraphExecutionResult<TState>.Failed(refused);
        }

        // A timeout with nowhere to go ends the thread instead, where it stands: WakeDueAsync decides that
        // before resuming, and a resume given LevelSignals.Due here (design/42-Waking-Through-A-Resume.md).
        if (check.EndsOnTimeout)
        {
            return level.IsRoot
                ? await EndOnTimeoutAsync(threadId, checkpoint, sequence, store, level).ConfigureAwait(false)
                : new GraphExecutionResult<TState>.Failed($"cannot resume thread '{threadId}': its wait has no timeout route");
        }

        // The outermost resume fixes the time once, so every level's decision, and every retry's,
        // has the same DecidedAt.
        var now = NowRfc3339();
        signals = signals.DecidedAt is null ? signals.At(now) : signals;

        // Only a level waiting at its own boundary or wait is answered by this resume: it may take the
        // boundary's reject route or the wait's timeout route, and a human's answer is recorded as its
        // decision. Every other level, and a level answered by an event, a timer or a timeout, keeps the
        // decision its checkpoint carries (design/29-Event-Waits-And-Timers.md).
        var answers = AnswersBoundary(checkpoint);
        var start = (answers ? check.Route : null) ?? checkpoint.NextNode!;
        var decision = answers && signals.Wake == WakeReason.Human
            ? HumanDecision.From(signals.Innermost, checkpoint.SuspendedNode!, signals.DecidedAt!)
            : checkpoint.Decision;

        var stateNode = checkpoint.FrozenState?.DeepClone();
        NestedResume? nested = null;
        if (checkpoint.Nested is { } child)
        {
            // With a child nested inside, this graph's frozen state is just the state its subgraph
            // node started with. The human reviewed the child's, so the innermost signal travels
            // down to it; a signal addressed to this level applies here, and the rest go on down
            // with the child.
            ApplyOverrides(stateNode, signals.Own);
            nested = new NestedResume(child, signals.Descend(start));
        }
        else
        {
            ApplyOverrides(stateNode, signals.Innermost);
        }

        TState state;
        try
        {
            state = stateNode.Deserialize<TState>()!;
        }
        catch (JsonException ex)
        {
            return new GraphExecutionResult<TState>.Failed(
                $"failed to rehydrate state for thread '{threadId}': {ex.Message}");
        }

        if (level.IsRoot)
        {
            // Nothing can refuse the resume any more, so it takes the thread now, and a refused resume never
            // took it from anyone (design/32-Thread-Leases.md).
            if (await AcquireLeaseAsync(threadId, store, level).ConfigureAwait(false) is { } held)
            {
                return held;
            }

            // Whoever holds a running thread is running it.
            if (recovering && await HeldByAnotherAsync(threadId, store, level).ConfigureAwait(false) is { } live)
            {
                return live;
            }

            // A durable run claims the thread with the checkpoint it continues from, so what the resume
            // decided is stored in the same step that says it began (design/33-Durable-Execution.md). Any
            // other run stamps the checkpoint it resumes.
            var recoveries = RecoveryCount.Of(checkpoint) + 1;
            var claim = !level.Run.Durable ? checkpoint with { ResumedAt = now }
                : recovering ? RecoveryCount.Counted(Claim(checkpoint, signals, level.Run, now), recoveries)
                : Claim(checkpoint, signals, level.Run, now);

            // Conditional, so of two callers that loaded the same checkpoint only one resumes it, and a
            // thread ended meanwhile stays ended (design/28-Ending-A-Thread.md, "Races").
            switch (await PersistIfLatestAsync(store, claim, sequence, level).ConfigureAwait(false))
            {
                case LeasedSave.NotLatest:
                    level.Run.Superseded = true;
                    return new GraphExecutionResult<TState>.Failed(recovering
                        ? $"thread '{threadId}' changed before it could be recovered: its run went on, or another recovery got there first"
                        : $"checkpoint for thread '{threadId}' changed before it could be resumed: another resume or end got there first");
                case LeasedSave.LeaseLost:
                    return LeaseLostAt(threadId, start, state, checkpoint.WaitingAt());
            }

            if (recovering)
            {
                level.Run.Telemetry.Recovered(RecoveryCount.ContinuesAt(claim), recoveries);
            }
            else
            {
                level.Run.Telemetry.Resumed(checkpoint, signals, check.Route);
            }
        }
        else
        {
            await store.SaveAsync(checkpoint with { ResumedAt = now }, CancellationToken.None).ConfigureAwait(false);
        }

        // Checked in preflight, so it reads.
        HopLimit.TryRead(checkpoint, out var hops, out _);
        return await ExecuteFromAsync(
                threadId,
                start,
                state,
                store,
                stream,
                level with { Decision = decision, Recovering = recovering || level.Recovering },
                nested,
                hops)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Whether a resume of <paramref name="checkpoint"/> answers an interrupt boundary of this graph:
    /// it is interrupted at one of its own boundaries, with nothing nested.
    /// </summary>
    private static bool AnswersBoundary(Checkpoint checkpoint) =>
        checkpoint.Status == CheckpointStatus.Interrupted && checkpoint.Nested is null;

    /// <summary>
    /// The route a resume of <paramref name="checkpoint"/> with <paramref name="signals"/> continues at
    /// instead of its next node, if it takes one at this level: the checkpoint answers one of this graph's
    /// boundaries or waits, and either a human rejects at a boundary with a reject route
    /// (<c>design/21-Typed-Human-Signals.md</c>, "Exactly when the reject route applies"), or the wait timed
    /// out and has a timeout route (<c>design/29-Event-Waits-And-Timers.md</c>).
    /// </summary>
    private string? RouteFor(Checkpoint checkpoint, LevelSignals signals)
    {
        if (!AnswersBoundary(checkpoint) || checkpoint.SuspendedNode is not { } node)
        {
            return null;
        }

        return signals.Wake switch
        {
            WakeReason.Human when !signals.Innermost.IsApproved => _rejectRoutes.GetValueOrDefault(node),
            WakeReason.Timeout => TimeoutRoutes.GetValueOrDefault(node),
            _ => null,
        };
    }

    /// <summary>
    /// Why <paramref name="signals"/>' wake can't answer <paramref name="checkpoint"/>, the innermost level, at
    /// <paramref name="path"/> (<c>design/29-Event-Waits-And-Timers.md</c>, "Answering a wait");
    /// <see langword="null"/> if it can. A human resumes anything but an event wait or a timer; a signal only
    /// an event wait for its type; a timer wake only a timer; a timeout only a wait with a timeout. The wait
    /// must be one of this graph's, which catches a thread of another graph.
    /// </summary>
    private string? WakeProblem(Checkpoint checkpoint, LevelSignals signals, string threadId, IReadOnlyList<string> path)
    {
        var waiting = checkpoint.Status == CheckpointStatus.Interrupted;
        var wait = waiting ? checkpoint.Wait : null;
        var node = checkpoint.SuspendedNode ?? string.Empty;
        var at = $"[{string.Join('/', path.Append(node))}]";
        var spec = Waits.GetValueOrDefault(node);
        switch (signals.Wake)
        {
            case WakeReason.Human:
                return wait?.Kind switch
                {
                    WaitKind.Event =>
                        $"thread '{threadId}' is waiting for event '{wait.EventType}' at {at}: only a signal of that event answers it",
                    WaitKind.Timer =>
                        $"thread '{threadId}' is waiting for a timer at {at} until {wait.WakeAt}: it wakes when due",
                    _ => null,
                };
            case WakeReason.Event:
                if (!waiting)
                {
                    return $"thread '{threadId}' is not waiting for event '{signals.EventType}': its latest checkpoint is {RunTelemetry.Wire(checkpoint.Status)}";
                }

                if (wait is not { Kind: WaitKind.Event } || wait.EventType != signals.EventType)
                {
                    return $"thread '{threadId}' is not waiting for event '{signals.EventType}': it is waiting for {wait?.Describe() ?? "a human"} at {at}";
                }

                return spec is { Kind: WaitKind.Event } && spec.EventType == signals.EventType
                    ? null
                    : $"thread '{threadId}' waits at {at}, which is not a wait for event '{signals.EventType}' in this graph";
            case WakeReason.Timer:
                if (wait is not { Kind: WaitKind.Timer })
                {
                    return $"thread '{threadId}' is not waiting for a timer at {at}";
                }

                return spec is { Kind: WaitKind.Timer }
                    ? null
                    : $"thread '{threadId}' waits at {at}, which is not a timer in this graph";
            default:
                if (wait is not { WakeAt: not null } || wait.Kind == WaitKind.Timer)
                {
                    return $"thread '{threadId}' is not waiting with a timeout at {at}";
                }

                var timed = wait.Kind == WaitKind.Human
                    ? BoundaryTimeouts.ContainsKey(node) && _interruptBoundaries.Contains(node)
                    : spec is { Kind: WaitKind.Event, Timeout: not null };
                return timed ? null : $"thread '{threadId}' waits at {at}, which has no timeout in this graph";
        }
    }

    /// <summary>
    /// Checks, without changing anything, that a resume of <paramref name="checkpoint"/> with
    /// <paramref name="signals"/> can start at this level and every level nested below it: there is a
    /// node to continue at, no override is set twice, the overrides fit the level's state type, the hop
    /// count (<c>strata.hops</c>) reads, and a nested child sits under a subgraph node whose own child
    /// passes the same check.
    /// <paramref name="threadId"/> is the outermost thread, and <paramref name="path"/> this level
    /// (<c>[]</c> at the top), for messages. Reports the innermost level's reject route, if it takes one.
    /// </summary>
    internal ResumeCheck CheckResume(
        Checkpoint checkpoint, LevelSignals signals, string threadId, IReadOnlyList<string> path)
    {
        var at = path.Count == 0 ? string.Empty : $" at level [{string.Join('/', path)}]";
        if (path.Count > 0 && !CheckpointSchema.IsSupported(checkpoint.SchemaVersion))
        {
            return Refuse(
                $"checkpoint for thread '{threadId}'{at} has unsupported schema_version '{checkpoint.SchemaVersion}'");
        }

        // Resetting the count of a hand-edited log would defeat the breaker, so refuse it instead.
        if (!HopLimit.TryRead(checkpoint, out _, out var invalidHops))
        {
            return Refuse(
                $"checkpoint for thread '{threadId}'{at} has an invalid '{HopLimit.MetadataKey}' value '{invalidHops}'");
        }

        if (checkpoint.Nested is null && WakeProblem(checkpoint, signals, threadId, path) is { } unanswered)
        {
            return Refuse(unanswered);
        }

        var route = RouteFor(checkpoint, signals);
        if (checkpoint.Nested is null && signals.Wake == WakeReason.Timeout && route is null)
        {
            return new ResumeCheck(Problem: null, Route: null, EndsOnTimeout: true);
        }

        if ((route ?? checkpoint.NextNode) is not { } start)
        {
            return Refuse($"checkpoint for thread '{threadId}'{at} has no next node to resume from");
        }

        var applied = checkpoint.Nested is null ? signals.Innermost : signals.Own;
        string? twice = null;
        _ = applied?.EffectiveOverrides(out twice);
        if (twice is not null)
        {
            return Refuse($"cannot resume thread '{threadId}': {twice}");
        }

        var stateNode = checkpoint.FrozenState?.DeepClone();
        ApplyOverrides(stateNode, applied);
        try
        {
            _ = stateNode.Deserialize<TState>();
        }
        catch (JsonException ex)
        {
            return Refuse($"failed to rehydrate state for thread '{threadId}'{at}: {ex.Message}");
        }

        if (checkpoint.Nested is not { } child)
        {
            return new ResumeCheck(Problem: null, route);
        }

        return _nodes.TryGetValue(start, out var entry) && entry.CheckResume is { } checkChild
            ? checkChild(child, signals.Descend(start), threadId, [.. path, start])
            : Refuse($"checkpoint nests a child graph under '{start}', which is not a subgraph node");

        static ResumeCheck Refuse(string problem) => new(problem, Route: null);
    }

    /// <summary>
    /// Merges <paramref name="signal"/>'s overrides into the object <paramref name="state"/>: string ones
    /// as JSON strings, typed ones with their JSON type. A key set in both maps is refused before this.
    /// </summary>
    private static void ApplyOverrides(JsonNode? state, HumanSignal? signal)
    {
        if (signal is null || state is not JsonObject stateObject)
        {
            return;
        }

        foreach (var (key, value) in signal.EffectiveOverrides(out _))
        {
            stateObject[key] = value;
        }
    }

    /// <summary>
    /// Like <see cref="RunAsync(string, TState, ICheckpointStore, CancellationToken)"/>, but drives the
    /// graph on a background <see cref="Task"/> and streams <c>StreamChunk</c>s to the returned reader
    /// as nodes run, instead of only handing back a final state once the whole graph finishes. This is
    /// the integration point STRATA-SPEC-001 describes, via
    /// <see cref="Streaming.StreamContext.ForNode"/> (one context per node, sharing a single channel).
    ///
    /// Every node — streaming or plain — gets automatic
    /// <see cref="StreamChunkType.NodeStart"/>/<see cref="StreamChunkType.NodeEnd"/> chunks
    /// around its execution; streaming nodes (and an async node's <see cref="NodeContext.Stream"/>)
    /// can also emit their own <see cref="StreamChunkType.Token"/> chunks from inside the handler, as
    /// can the streaming branches of a parallel group (attributed to
    /// <c>"&lt;group&gt;/&lt;branch&gt;"</c>). A fallible node about to be retried emits a
    /// <see cref="StreamChunkType.Retry"/> first.
    ///
    /// A subgraph node (see <see cref="StateGraph{TState}.AddSubgraph{TChild}"/>) streams its child's
    /// chunks too — all of them, lifecycle chunks included — named
    /// <c>"&lt;subgraph&gt;/&lt;child node&gt;"</c>, nested to any depth. They sit between the
    /// subgraph node's own <c>NodeStart</c> and <c>NodeEnd</c>.
    ///
    /// The stream ends when the run ends, whatever the outcome. A node stopped by cancellation gets no
    /// <c>NodeEnd</c>, and the completion says why the stream ended.
    /// </summary>
    public (ChannelReader<StreamChunk> Reader, Task<GraphExecutionResult<TState>> Completion) RunStreaming(
        string threadId, TState initialState, ICheckpointStore store, CancellationToken cancellationToken = default) =>
        RunStreaming(threadId, initialState, store, NoOptions, cancellationToken);

    /// <inheritdoc cref="RunStreaming(string, TState, ICheckpointStore, CancellationToken)"/>
    public (ChannelReader<StreamChunk> Reader, Task<GraphExecutionResult<TState>> Completion) RunStreaming(
        string threadId,
        TState initialState,
        ICheckpointStore store,
        RunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        RunMetadata.Validate(options);
        return Stream(threadId, Topology.EntryPoint, store, options, RunMode.Run, cancellationToken, (context, level, observed) =>
            StartAsync(threadId, initialState, observed, context, level));
    }

    /// <summary>
    /// Runs <paramref name="run"/> on a background task with a fresh channel, completing the channel
    /// however the run ends so the reader never hangs. The task waits for the run's scheduler's slot first
    /// (<c>design/40-Run-Scheduler.md</c>); a run it refuses ends the stream with no chunks. The run's span is a child of whatever is current
    /// here, which <see cref="Task.Run(Func{Task})"/> carries over.
    /// </summary>
    private (ChannelReader<StreamChunk> Reader, Task<GraphExecutionResult<TState>> Completion) Stream(
        string threadId,
        string channelName,
        ICheckpointStore store,
        RunOptions options,
        RunMode mode,
        CancellationToken cancellationToken,
        Func<StreamContext, LevelScope, ICheckpointStore, Task<GraphExecutionResult<TState>>> run)
    {
        var (context, reader) = StreamContext.CreateChannel(channelName);
        var completion = Task.Run(async () =>
        {
            try
            {
                using var admission = await AdmitAsync(options, cancellationToken).ConfigureAwait(false);
                if (admission is { IsAdmitted: false })
                {
                    return NotAdmitted(admission);
                }

                using var root = RootScope.Start(threadId, options, cancellationToken, mode, Checkpointing, admission);
                return await ObserveAsync(root, store, observed => run(context, root.Level, observed))
                    .ConfigureAwait(false);
            }
            finally
            {
                context.Complete();
            }
        });
        return (reader, completion);
    }

    /// <summary>
    /// Refuses a node this run can't execute, otherwise runs it under its recovery policy and timeout
    /// and reports what happened as a <see cref="NodeOutcome"/> — split out so
    /// <see cref="ExecuteFromAsync"/>'s loop stays focused on routing/checkpointing.
    /// </summary>
    private Task<NodeOutcome> ExecuteNodeAsync(
        string threadId,
        string current,
        TState state,
        ICheckpointStore store,
        StreamContext? nodeContext,
        LevelScope level,
        NestedResume? resume,
        NodeScope scope)
    {
        if (!_nodes.TryGetValue(current, out var entry))
        {
            return Terminal($"node '{current}' not found");
        }

        if (nodeContext is null && entry.NeedsStream)
        {
            if (entry.Kind == NodeKind.Streaming)
            {
                return Terminal(
                    $"node '{current}' is a streaming node but this run has no StreamContext — use RunStreaming instead of RunAsync");
            }

            // Not when resuming into a subgraph without a stream (a plain `ResumeAsync`): the child
            // may have done its streaming before it froze. If it reaches a streaming node after all,
            // the child's own run fails there, as a flat graph's would.
            if (resume is null)
            {
                return Terminal(
                    $"node '{current}' is a parallel group with a streaming branch, or a subgraph that streams, but this run has no StreamContext — use RunStreaming instead of RunAsync");
            }
        }

        return RunAttemptsAsync(threadId, current, entry, state, store, nodeContext, level, resume, scope);

        static Task<NodeOutcome> Terminal(string message) =>
            Task.FromResult<NodeOutcome>(new NodeOutcome.Terminal(new GraphExecutionResult<TState>.Failed(message)));
    }

    /// <summary>What running a node resolved to.</summary>
    private abstract record NodeOutcome
    {
        /// <summary>The node completed; this is the new committed state.</summary>
        public sealed record Committed(TState State) : NodeOutcome;

        /// <summary>
        /// The node couldn't be recovered by retrying, and its policy names a fallback: continue
        /// there with the pre-failure state, telling it why (<see cref="NodeContext.LastFailure"/>).
        /// </summary>
        public sealed record Fallback(string Node, NodeFailure Failure) : NodeOutcome;

        /// <summary>
        /// A subgraph node's child froze at an interrupt boundary. Carries the state the node
        /// started with and the child's checkpoint, for the run to freeze around.
        /// </summary>
        public sealed record Interrupted(TState State, Checkpoint Child) : NodeOutcome;

        /// <summary>
        /// The run was cancelled while the node ran (or before it started). Carries the child's
        /// <c>cancelled</c> checkpoint when the node is a subgraph, for the run's to nest.
        /// </summary>
        public sealed record Cancelled(Checkpoint? Child) : NodeOutcome;

        /// <summary>
        /// The node can't be executed, or failed with no fallback (in which case the
        /// <c>Failed</c> checkpoint has already been persisted). Carries the result to return
        /// from the run.
        /// </summary>
        public sealed record Terminal(GraphExecutionResult<TState> Result) : NodeOutcome;
    }

    /// <summary>
    /// Runs a node under its <see cref="RecoveryPolicy"/> (default: no retries, no fallback) and its
    /// timeout, if it has one.
    ///
    /// Every attempt receives the same <paramref name="state"/>, so a failed attempt can't leak
    /// partial changes into the next attempt, the fallback node, or the <c>Failed</c> checkpoint —
    /// the last committed state is what they all see. A transient <see cref="NodeException"/> (or a
    /// timeout, which is one) is retried up to <c>MaxRetries</c> more times, after the policy's backoff
    /// (<c>design/25-Retry-Backoff.md</c>), waited on the run's clock; a fatal one, or a
    /// transient one out of retries, is terminal: route to the policy's fallback if it has one,
    /// otherwise persist a <c>CheckpointStatus.Failed</c> checkpoint and fail the run. Only a
    /// fallible node's <see cref="NodeException"/> is recovery-managed; from any other node it
    /// propagates, like any other exception, but a timeout fails any node.
    ///
    /// A subgraph whose child freezes is not a failure, and neither is a cancelled run: both end the
    /// loop untouched by the policy. <paramref name="resume"/> is what a subgraph node is resuming
    /// from, passed to every attempt alike.
    /// </summary>
    private async Task<NodeOutcome> RunAttemptsAsync(
        string threadId,
        string node,
        NodeEntry<TState> entry,
        TState state,
        ICheckpointStore store,
        StreamContext? nodeContext,
        LevelScope level,
        NestedResume? resume,
        NodeScope scope)
    {
        var policy = _recoveryPolicies.GetValueOrDefault(node) ?? RecoveryPolicy.Default;
        TimeSpan? timeout = _timeouts.TryGetValue(node, out var limit) ? limit : null;

        // A subgraph's child hands up a running checkpoint each time one of its nodes commits. This level
        // nests it in its own, suspended at the subgraph node with the state the node started with, and saves
        // that, or hands it up in turn (design/33-Durable-Execution.md, "Subgraphs, groups and maps"). A lost
        // lease cancels the run's token, which is what stops the child.
        if (entry.IsSubgraph && level.Checkpoints)
        {
            var around = level;
            JsonNode? started = null;
            level = level with
            {
                ChildProgress = async child =>
                {
                    started ??= JsonSerializer.SerializeToNode(state);
                    var running = RunningAt(threadId, node, node, started, around, child);
                    if (around.Progress is { } up)
                    {
                        await up(running).ConfigureAwait(false);
                    }
                    else
                    {
                        await PersistAsync(store, running, around).ConfigureAwait(false);
                    }
                },
            };
        }

        var attempts = 0;
        AttemptResult failed;
        while (true)
        {
            attempts++;

            // A subgraph being resumed into is entered even so: its child stops at its own innermost
            // node, after the human's signal has been applied there, so nothing answered is lost.
            if (resume is null && level.Token.IsCancellationRequested)
            {
                return new NodeOutcome.Cancelled(Child: null);
            }

            // The timeout runs on the run's clock (design/25-Retry-Backoff.md).
            using var expiry = timeout is { } duration ? new CancellationTokenSource(duration, level.Run.Clock) : null;
            using var attempt = expiry is null
                ? CancellationTokenSource.CreateLinkedTokenSource(level.Token)
                : CancellationTokenSource.CreateLinkedTokenSource(level.Token, expiry.Token);

            // The attempt's span is current while its handler runs, so the handler's own spans nest
            // under it (design/18-Run-Observability.md).
            var activity = scope.StartAttempt(attempts);
            var inputs = new AttemptInputs(
                level.ContextFor(node, attempts, nodeContext, activity), nodeContext, resume, level, attempt.Token);
            AttemptResult result;
            try
            {
                result = await AttemptOnceAsync(node, entry, state, inputs, level, attempt, timeout).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                StrataTelemetry.Fail(activity, ex.Message, ex.GetType().FullName);
                activity?.Dispose();
                throw;
            }

            if (result.Failure is { } attemptFailure)
            {
                StrataTelemetry.Fail(activity, attemptFailure.Message, result.ErrorType);
            }

            activity?.Dispose();

            if (result.Outcome is { } outcome)
            {
                return outcome;
            }

            if (result.Failure!.Kind == NodeFailureKind.Transient && attempts <= policy.MaxRetries)
            {
                // Retry from the same committed `state`. Whatever the failed attempt streamed can't
                // be recalled, so tell the consumer.
                var delay = policy.Backoff?.DelayBefore(attempts, result.Failure.RetryAfter, Random.Shared)
                    ?? TimeSpan.Zero;
                scope.Retry(result.Failure.Message, result.ErrorType!, result.TimedOut, delay);
                await Emit(nodeContext, StreamChunkType.Retry, result.Failure.Message, level.Token).ConfigureAwait(false);
                await BackOffAsync(delay, level).ConfigureAwait(false);
                continue;
            }

            failed = result;
            break;
        }

        var failure = failed.Failure!;

        // The node ends here, before its checkpoint is saved, so a run's events read in the order
        // things happened: NodeCompleted, then CheckpointSaved.
        scope.Completed(NodeResult.Failed, failure.Message, failed.ErrorType, policy.Fallback);
        await Emit(nodeContext, StreamChunkType.Error, failure.Message, level.Token).ConfigureAwait(false);

        if (policy.Fallback is { } fallbackNode)
        {
            return new NodeOutcome.Fallback(fallbackNode, new NodeFailure
            {
                Node = node,
                NodePath = level.PathOf(node),
                Kind = failure.Kind,
                Message = failure.Message,
                RetryAfter = HintOf(failure),
                Attempts = attempts,
                TimedOut = failed.TimedOut,
            });
        }

        var engineMetadata = new Dictionary<string, string>
        {
            ["error"] = failure.Message,
            ["error_kind"] = failure.Kind == NodeFailureKind.Transient ? "transient" : "fatal",
            ["attempts"] = attempts.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
        if (HintOf(failure) is { } hint)
        {
            engineMetadata[RetryAfterHint.MetadataKey] = RetryAfterHint.Format(hint);
        }

        // A subgraph whose child failed at one of its own nodes (or was stopped by this node's
        // timeout) keeps the child's checkpoint, so a resume can re-enter it there.
        var checkpoint = CheckpointSchema.Versioned(new Checkpoint
        {
            SchemaVersion = CheckpointSchema.Version,
            ThreadId = threadId,
            Status = CheckpointStatus.Failed,
            SuspendedNode = node,
            // The node a replay would re-run.
            NextNode = node,
            FrozenState = JsonSerializer.SerializeToNode(state),
            Metadata = level.CheckpointMetadata(engineMetadata),
            CreatedAt = NowRfc3339(),
            ResumedAt = null,
            Nested = failure.FailedChild,
            Decision = level.Decision,
            Graph = Identity,
        });

        if (!await PersistAsync(store, checkpoint, level).ConfigureAwait(false))
        {
            // The failure isn't the thread's history: another owner has it (design/32-Thread-Leases.md).
            return new NodeOutcome.Terminal(LeaseLostAt(threadId, node, state, checkpoint.WaitingAt()));
        }

        return new NodeOutcome.Terminal(
            new GraphExecutionResult<TState>.NodeFailed(node, attempts, failure.Message));
    }

    /// <summary>A failure's retry hint, which only a transient failure has.</summary>
    private static TimeSpan? HintOf(NodeException failure) =>
        failure.Kind == NodeFailureKind.Transient ? failure.RetryAfter : null;

    /// <summary>
    /// Waits <paramref name="delay"/> on the run's clock before a retry, or until
    /// <paramref name="level"/>'s token fires (the run was cancelled, its deadline passed, or an
    /// enclosing subgraph timed out), whichever comes first. The retry loop then sees the token as it
    /// would at the top of any attempt. A zero delay doesn't wait at all.
    /// </summary>
    private static async Task BackOffAsync(TimeSpan delay, LevelScope level)
    {
        if (delay <= TimeSpan.Zero)
        {
            return;
        }

        try
        {
            await Task.Delay(delay, level.Run.Clock, level.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (level.Token.IsCancellationRequested)
        {
        }
    }

    /// <summary>
    /// How one attempt ended: an <see cref="Outcome"/> that ends the node (committed, frozen,
    /// cancelled), or a <see cref="Failure"/> for the recovery policy, with its <c>error.type</c>
    /// (<c>transient</c>, <c>fatal</c> or <c>timeout</c>).
    /// </summary>
    private readonly record struct AttemptResult(
        NodeOutcome? Outcome, NodeException? Failure, string? ErrorType, bool TimedOut)
    {
        public static AttemptResult Ended(NodeOutcome outcome) => new(outcome, null, null, false);

        public static AttemptResult Failed(NodeException failure) =>
            new(null, failure, failure.Kind == NodeFailureKind.Transient ? "transient" : "fatal", false);

        public static AttemptResult TimedOutWith(NodeException failure) => new(null, failure, "timeout", true);
    }

    /// <summary>
    /// One attempt, resolved to an <see cref="AttemptResult"/>. Only a fallible node's
    /// <see cref="NodeException"/> is caught; anything else propagates.
    /// </summary>
    private static async Task<AttemptResult> AttemptOnceAsync(
        string node,
        NodeEntry<TState> entry,
        TState state,
        AttemptInputs inputs,
        LevelScope level,
        CancellationTokenSource attempt,
        TimeSpan? timeout)
    {
        try
        {
            switch (await AttemptAsync(entry, state, inputs).ConfigureAwait(false))
            {
                case NodeStep<TState>.Done done:
                    return AttemptResult.Ended(new NodeOutcome.Committed(done.State));
                case NodeStep<TState>.Interrupted interrupted:
                    return AttemptResult.Ended(new NodeOutcome.Interrupted(state, interrupted.Child));
                case NodeStep<TState>.Cancelled cancelled when level.Token.IsCancellationRequested:
                    return AttemptResult.Ended(new NodeOutcome.Cancelled(cancelled.Child));
                case NodeStep<TState>.Cancelled cancelled:
                    // Only this node's timeout cancels a child while the run goes on. Keep where the
                    // child stopped, so a resume re-enters it there.
                    return AttemptResult.TimedOutWith(
                        new NodeException(NodeFailureKind.Transient, TimedOut(node, timeout))
                        {
                            FailedChild = cancelled.Child,
                        });
                default:
                    throw new InvalidOperationException("unreachable: unknown NodeStep");
            }
        }
        catch (OperationCanceledException) when (attempt.IsCancellationRequested)
        {
            return level.Token.IsCancellationRequested
                ? AttemptResult.Ended(new NodeOutcome.Cancelled(Child: null))
                : AttemptResult.TimedOutWith(NodeException.Transient(TimedOut(node, timeout)));
        }
        catch (NodeException ex) when (entry.Kind == NodeKind.Fallible)
        {
            return AttemptResult.Failed(ex);
        }
    }

    /// <summary>
    /// One attempt of a node. The engine stops waiting the moment the attempt's token fires
    /// (<see cref="OperationCanceledException"/>) and abandons the handler: it is never waited for once
    /// given up on. A subgraph is the exception: it is a graph, not a handler, so it is awaited and
    /// winds down by itself, handing back the checkpoint of where its child stopped. A map is awaited
    /// too, while its items wind down (<c>design/27-Map-Fan-Out.md</c>); it then throws
    /// <see cref="OperationCanceledException"/>.
    /// </summary>
    private static async Task<NodeStep<TState>> AttemptAsync(NodeEntry<TState> entry, TState state, AttemptInputs inputs)
    {
        Task<NodeStep<TState>> task;
        try
        {
            task = entry.Handler(state, inputs).AsTask();
        }
        catch (Exception ex)
        {
            task = Task.FromException<NodeStep<TState>>(ex);
        }

        if (entry.IsSubgraph || entry.WindsDown)
        {
            return await task.ConfigureAwait(false);
        }

        try
        {
            return await task.WaitAsync(inputs.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (inputs.Token.IsCancellationRequested && !task.IsCompleted)
        {
            NodeHandlers.Abandon(task);
            throw;
        }
    }

    private static string TimedOut(string node, TimeSpan? timeout) =>
        $"node '{node}' timed out after {(long)(timeout ?? TimeSpan.Zero).TotalMilliseconds} ms";

    private static ValueTask Emit(StreamContext? context, StreamChunkType type, string message, CancellationToken cancellationToken) =>
        context?.EmitLifecycleAsync(type, message, cancellationToken) ?? ValueTask.CompletedTask;

    /// <summary>
    /// Freezes the run around a subgraph node whose child froze at one of its own interrupt
    /// boundaries (<c>design/16-Nested-Interrupts.md</c>).
    ///
    /// The checkpoint is suspended <i>at the subgraph node</i> — the node a resume re-enters, so
    /// <c>NextNode</c> is the node itself — and holds <paramref name="state"/>, the state that node
    /// started with (its <c>output</c> mapper needs it once the child completes), with the child's own
    /// checkpoint nested inside.
    /// </summary>
    private async Task<GraphExecutionResult<TState>> FreezeAroundChildAsync(
        string threadId, string node, TState state, Checkpoint child, ICheckpointStore store, LevelScope level)
    {
        var checkpoint = CheckpointSchema.Versioned(new Checkpoint
        {
            SchemaVersion = CheckpointSchema.NestedVersion,
            ThreadId = threadId,
            Status = CheckpointStatus.Interrupted,
            SuspendedNode = node,
            NextNode = node,
            FrozenState = JsonSerializer.SerializeToNode(state),
            Metadata = level.CheckpointMetadata(),
            CreatedAt = NowRfc3339(),
            ResumedAt = null,
            Nested = child,
            Decision = level.Decision,
            Graph = Identity,
        });
        if (!await PersistAsync(store, checkpoint, level).ConfigureAwait(false))
        {
            return LeaseLostAt(threadId, node, state, checkpoint.WaitingAt());
        }

        return new GraphExecutionResult<TState>.Interrupted(threadId, node, node, state, checkpoint.WaitingAt())
        {
            Wait = checkpoint.InnermostWait(),
        };
    }

    /// <summary>
    /// Ends the run as cancelled at <paramref name="node"/>, the node that was running or about to
    /// start (<c>design/17-Async-Execution.md</c>): persists a <c>cancelled</c> checkpoint holding
    /// <paramref name="state"/>, the last committed state, with <c>NextNode</c> the node itself so a
    /// resume re-enters it. For a subgraph node, <paramref name="child"/> is where its child stopped.
    /// </summary>
    private async Task<GraphExecutionResult<TState>> CancelAtAsync(
        string threadId, string node, TState state, ICheckpointStore store, LevelScope level, Checkpoint? child)
    {
        var checkpoint = CheckpointSchema.Versioned(new Checkpoint
        {
            SchemaVersion = CheckpointSchema.CancelledVersion,
            ThreadId = threadId,
            Status = CheckpointStatus.Cancelled,
            SuspendedNode = node,
            NextNode = node,
            FrozenState = JsonSerializer.SerializeToNode(state),
            Metadata = level.CheckpointMetadata(new Dictionary<string, string> { ["cancel_reason"] = level.WireReason }),
            CreatedAt = NowRfc3339(),
            ResumedAt = null,
            Nested = child,
            Decision = level.Decision,
            Graph = Identity,
        });
        // A run that lost its lease writes nothing, this checkpoint included (design/32-Thread-Leases.md).
        if (!await PersistAsync(store, checkpoint, level).ConfigureAwait(false))
        {
            return LeaseLostAt(threadId, node, state, checkpoint.WaitingAt());
        }

        return new GraphExecutionResult<TState>.Cancelled(threadId, node, level.Run.Reason, state, checkpoint.WaitingAt());
    }

    /// <summary>
    /// Ends the run at <paramref name="node"/>, which <paramref name="level"/>'s hop limit refused to start
    /// (<c>design/24-Loop-Circuit-Breaker.md</c>): persists a <c>Failed</c> checkpoint holding
    /// <paramref name="state"/>, the last committed state, with <c>error_kind</c> <c>hop_limit</c> and
    /// <c>NextNode</c> the node itself, so a resume with a higher limit starts it. No attempt was made,
    /// so there is no <c>attempts</c>. <paramref name="child"/> is the checkpoint a subgraph node was
    /// being resumed into, if it was.
    /// </summary>
    private async Task<GraphExecutionResult<TState>> HopLimitAtAsync(
        string threadId, string node, TState state, ICheckpointStore store, LevelScope level, int limit, Checkpoint? child)
    {
        var hops = level.Hops!.Value;
        var result = new GraphExecutionResult<TState>.HopLimitExceeded(node, hops, limit);
        level.Run.Telemetry.HopLimitExceeded(level.PathOf(node), hops, limit);
        var checkpoint = CheckpointSchema.Versioned(new Checkpoint
        {
            SchemaVersion = CheckpointSchema.Version,
            ThreadId = threadId,
            Status = CheckpointStatus.Failed,
            SuspendedNode = node,
            NextNode = node,
            FrozenState = JsonSerializer.SerializeToNode(state),
            Metadata = level.CheckpointMetadata(new Dictionary<string, string>
            {
                ["error"] = result.Message,
                ["error_kind"] = "hop_limit",
            }),
            CreatedAt = NowRfc3339(),
            ResumedAt = null,
            Nested = child,
            Decision = level.Decision,
            Graph = Identity,
        });
        if (!await PersistAsync(store, checkpoint, level).ConfigureAwait(false))
        {
            return LeaseLostAt(threadId, node, state, checkpoint.WaitingAt());
        }

        return result;
    }

    /// <summary>
    /// The hop limit in effect at <paramref name="level"/> of a run of this graph
    /// (<c>design/24-Loop-Circuit-Breaker.md</c>): the run's own, at the outermost graph, or the compiled one.
    /// </summary>
    private int? HopLimitAt(LevelScope level) =>
        level.IsRoot && level.Run.MaxNodeHops is { } replaced ? replaced : MaxNodeHops;

    /// <summary>
    /// The executor loop, from <paramref name="current"/>: what a run, a resume, and a subgraph's child
    /// all end up in. <paramref name="hops"/> is how many nodes the level already started in earlier runs
    /// of the thread, from the checkpoint resumed (<c>design/24-Loop-Circuit-Breaker.md</c>).
    /// </summary>
    private async Task<GraphExecutionResult<TState>> ExecuteFromAsync(
        string threadId,
        string current,
        TState state,
        ICheckpointStore store,
        StreamContext? streamContext,
        LevelScope level,
        NestedResume? resume = null,
        int hops = 0)
    {
        // Only the first node is resumed into, and only a subgraph node can be.
        if (resume is not null && !(_nodes.TryGetValue(current, out var resumed) && resumed.IsSubgraph))
        {
            return new GraphExecutionResult<TState>.Failed(
                $"checkpoint nests a child graph under '{current}', which is not a subgraph node");
        }

        var limit = HopLimitAt(level);

        // A level that checkpoints every node records its hops whether or not it has a limit: a node's
        // idempotency key is made of them, and a recovery has to count on from them
        // (design/33-Durable-Execution.md).
        var counted = limit is not null || level.Checkpoints;

        // Whether the node about to start may have run before its process died: the first node a recovery
        // starts at this level, and its fallback, until a node commits. A level that isn't checkpointed (a
        // map's item) is run again whole, so every node of it may have.
        var recovering = level.Recovering;

        // Why the node a fallback came from failed, for the fallback node only (design/25-Retry-Backoff.md).
        NodeFailure? lastFailure = null;
        while (true)
        {
            var nodeResume = resume;
            resume = null;

            // What a checkpoint about `current` records: the hops made before it, which a resume
            // re-entering it starts from, so it gets the same hop number again.
            var nodeLevel = (counted ? level with { Hops = hops } : level) with { Hop = hops + 1, Recovering = recovering };
            if (lastFailure is not null)
            {
                nodeLevel = nodeLevel with { LastFailure = lastFailure };
                lastFailure = null;
            }

            // Not for a subgraph being resumed into: see RunAttemptsAsync.
            if (nodeResume is null && level.Token.IsCancellationRequested)
            {
                return await CancelAtAsync(threadId, current, state, store, nodeLevel, child: null).ConfigureAwait(false);
            }

            // The breaker trips before the node that would be one hop too many starts, so nothing of it
            // runs. A subgraph being resumed into keeps the child it was resuming, so nothing answered is lost.
            if (hops >= limit)
            {
                return await HopLimitAtAsync(
                        threadId, current, state, store, nodeLevel, limit.Value, nodeResume?.Checkpoint)
                    .ConfigureAwait(false);
            }

            hops++;

            var nodeContext = streamContext?.ForNode(current);
            if (nodeContext is not null)
            {
                try
                {
                    await nodeContext.EmitDiagnosticAsync(
                        StreamChunkType.NodeStart, current, nodeResume is null ? level.Token : CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (level.Token.IsCancellationRequested)
                {
                    // Cancelled while a full channel held the node back: it never started.
                    return await CancelAtAsync(threadId, current, state, store, nodeLevel, child: null).ConfigureAwait(false);
                }
            }

            // Every NodeStarted gets exactly one NodeCompleted, however the node ends
            // (design/18-Run-Observability.md). A failure path ends it itself, before its checkpoint.
            var scope = level.Run.Telemetry.NodeStarted(level.PathOf(current));
            NodeOutcome outcome;
            try
            {
                outcome = await ExecuteNodeAsync(threadId, current, state, store, nodeContext, nodeLevel, nodeResume, scope)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                scope.Faulted(ex);
                throw;
            }

            switch (outcome)
            {
                case NodeOutcome.Committed committed:
                    scope.Completed(NodeResult.Committed);
                    state = committed.State;
                    recovering &= !level.Checkpoints;
                    break;
                case NodeOutcome.Fallback fallback:
                    // The failed node never completed, so no NodeEnd and no interrupt boundary
                    // check: continue at the fallback with the pre-failure state.
                    RunTelemetry.Transition(scope.Path, level.PathOf(fallback.Node));
                    current = fallback.Node;
                    lastFailure = fallback.Failure;
                    continue;
                case NodeOutcome.Interrupted interrupted:
                    // The child froze, so this node never completed: no NodeEnd, and its own
                    // boundary (if it is one) only applies once the child has finished.
                    scope.Completed(NodeResult.Interrupted);
                    return await FreezeAroundChildAsync(threadId, current, interrupted.State, interrupted.Child, store, nodeLevel)
                        .ConfigureAwait(false);
                case NodeOutcome.Cancelled cancelled:
                    // The node never completed: no NodeEnd. `state` is still what it started with.
                    scope.Completed(NodeResult.Cancelled);
                    return await CancelAtAsync(threadId, current, state, store, nodeLevel, cancelled.Child).ConfigureAwait(false);
                case NodeOutcome.Terminal terminal:
                    // Refused before it ran; a node that failed has already ended its scope.
                    scope.Completed(
                        NodeResult.Failed, (terminal.Result as GraphExecutionResult<TState>.Failed)?.Message, "fatal");
                    return terminal.Result;
            }

            await Emit(nodeContext, StreamChunkType.NodeEnd, current, level.Token).ConfigureAwait(false);

            // A wait node freezes like a boundary, with what it waits for (design/29-Event-Waits-And-Timers.md).
            if (_interruptBoundaries.Contains(current) || Waits.ContainsKey(current))
            {
                var lookup = NextNodeAfter(current, state, out var nextNode);
                if (lookup == NextNodeLookup.Unmatched)
                {
                    return new GraphExecutionResult<TState>.Failed(
                        $"no outgoing edge condition matched for node '{current}'");
                }

                var checkpoint = CheckpointSchema.Versioned(new Checkpoint
                {
                    SchemaVersion = CheckpointSchema.Version,
                    ThreadId = threadId,
                    Status = CheckpointStatus.Interrupted,
                    SuspendedNode = current,
                    NextNode = nextNode,
                    FrozenState = JsonSerializer.SerializeToNode(state),
                    // A resume goes on to the next node, so the boundary node is among the hops made.
                    Metadata = (counted ? level with { Hops = hops } : level).CheckpointMetadata(),
                    CreatedAt = NowRfc3339(),
                    ResumedAt = null,
                    Decision = level.Decision,
                    Wait = WaitAfter(current, state, level.Run.Clock.GetUtcNow()),
                    Graph = Identity,
                });

                if (!await PersistAsync(store, checkpoint, level).ConfigureAwait(false))
                {
                    return LeaseLostAt(threadId, current, state, [current]);
                }

                // FREEZE + FLUSH are done above. EVICT is implicit: nothing of this run is awaited
                // once this returns — the only thing that survives is what the store persisted.
                return new GraphExecutionResult<TState>.Interrupted(
                    threadId, current, nextNode ?? string.Empty, state, [current])
                {
                    Wait = checkpoint.Wait,
                };
            }

            var afterLookup = NextNodeAfter(current, state, out var after);
            if (afterLookup == NextNodeLookup.Terminal)
            {
                // A durable run says it finished, so its thread isn't left running, and a stale owner's
                // completion is fenced like any other save (design/33-Durable-Execution.md). A child's
                // completion is its subgraph node's commit, which its parent records.
                if (level.IsRoot && level.Run.Durable)
                {
                    var completed = CheckpointSchema.Versioned(new Checkpoint
                    {
                        SchemaVersion = CheckpointSchema.Version,
                        ThreadId = threadId,
                        Status = CheckpointStatus.Completed,
                        SuspendedNode = current,
                        NextNode = null,
                        FrozenState = JsonSerializer.SerializeToNode(state),
                        Metadata = (level with { Hops = hops }).CheckpointMetadata(),
                        CreatedAt = NowRfc3339(),
                        ResumedAt = null,
                        Decision = level.Decision,
                        Graph = Identity,
                    });
                    if (!await PersistAsync(store, completed, level).ConfigureAwait(false))
                    {
                        return LeaseLostAt(threadId, current, state, [current]);
                    }
                }

                return new GraphExecutionResult<TState>.Completed(state);
            }
            if (afterLookup == NextNodeLookup.Unmatched)
            {
                return new GraphExecutionResult<TState>.Failed(
                    $"no outgoing edge condition matched for node '{current}'");
            }

            // The node committed and an edge leads on: a durable level says where it is now, before the next
            // node starts (design/33-Durable-Execution.md).
            if (level.Checkpoints)
            {
                var running = RunningAt(
                    threadId, current, after!, JsonSerializer.SerializeToNode(state), level with { Hops = hops });
                if (level.Progress is { } up)
                {
                    await up(running).ConfigureAwait(false);
                }
                else if (!await PersistAsync(store, running, level).ConfigureAwait(false))
                {
                    return LeaseLostAt(threadId, after!, state, [after!]);
                }
            }

            RunTelemetry.Transition(level.PathOf(current), level.PathOf(after!));
            current = after!;
        }
    }

    private static string NowRfc3339() =>
        DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);
}
