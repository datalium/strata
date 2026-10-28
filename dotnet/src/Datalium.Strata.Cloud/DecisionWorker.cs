using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Cloud;

/// <summary>What a <see cref="DecisionWorker{TState}"/> did with a decision.</summary>
public enum DecisionOutcome
{
    /// <summary>The worker resumed the thread, and the resume ran (completed, interrupted again, cancelled).</summary>
    Resumed,

    /// <summary>
    /// The resume returned <c>Failed</c> for a reason of its own: refused before anything was written (an
    /// override that doesn't fit the state, a thread of another graph), or the resumed run failed. The
    /// decision is not handled again.
    /// </summary>
    Failed,

    /// <summary>
    /// Someone else acted on the decision first: the checkpoint was already resumed, or changed before
    /// this worker's resume could stamp it (another worker, an end, a later boundary). Nothing ran here.
    /// </summary>
    AlreadyHandled,

    /// <summary>
    /// The decision is for a thread this worker doesn't own: one <see cref="DecisionWorkerOptions{TState}.ThreadIdPrefix"/>
    /// or <see cref="DecisionWorkerOptions{TState}.Owns"/> leaves out, or one another graph wrote
    /// (<c>CompiledGraph.Owns</c>, <c>design/31-Graph-Identity.md</c>). It was not resumed.
    /// </summary>
    NotMine,
}

/// <summary>A decision a <see cref="DecisionWorker{TState}"/> handled, and what came of it.</summary>
/// <param name="Result">The resume's result; <see langword="null"/> for <see cref="DecisionOutcome.NotMine"/>.</param>
public sealed record HandledDecision<TState>(
    CloudDecision Decision,
    DecisionOutcome Outcome,
    GraphExecutionResult<TState>? Result);

/// <summary>How a <see cref="DecisionWorker{TState}"/> chooses, resumes and reports decisions.</summary>
public sealed record DecisionWorkerOptions<TState>
{
    /// <summary>Wait on this one thread only (Cloud's long-poll takes one). Otherwise the account's whole feed.</summary>
    public string? ThreadId { get; init; }

    /// <summary>A thread is this worker's only if its id starts with this (ordinal).</summary>
    public string? ThreadIdPrefix { get; init; }

    /// <summary>
    /// A thread is this worker's only if this says so: a lookup, checked before the thread is loaded. Combined
    /// with <see cref="ThreadIdPrefix"/> when both are set. Default: every thread. Whatever it says, a thread
    /// another graph wrote is not this worker's (<c>CompiledGraph.Owns</c>, <c>design/31-Graph-Identity.md</c>).
    /// </summary>
    public Func<CloudDecision, CancellationToken, ValueTask<bool>>? Owns { get; init; }

    /// <summary>The run options for each resume (its metadata, observer, deadline). Default: none.</summary>
    public Func<CloudDecision, RunOptions>? RunOptions { get; init; }

    /// <summary>
    /// Called once for every decision handled, in order, before the next one: StrataOps audits here. An
    /// exception from it stops the worker; catch inside it if auditing is best-effort.
    /// </summary>
    public Func<HandledDecision<TState>, CancellationToken, ValueTask>? OnHandled { get; init; }

    /// <summary>How long each long-poll is held. Default 25 s, Cloud's maximum.</summary>
    public TimeSpan Wait { get; init; } = TimeSpan.FromSeconds(25);

    /// <summary>The page size; <see langword="null"/> for Cloud's default (25).</summary>
    public int? Limit { get; init; }

    /// <summary>
    /// The wait after a round that found decisions but nothing new (every one already handled here), since
    /// a long-poll answers at once while any decision is waiting. Default 2 s.
    /// </summary>
    public TimeSpan IdleDelay { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>The wait after the feed was unavailable (a network error, a <c>5xx</c>): 1 s doubling to 30 s, jittered.</summary>
    public RetryBackoff ErrorBackoff { get; init; } = new(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), BackoffJitter.Full);
}

/// <summary>
/// Resumes the threads a reviewer decided in the Strata Cloud console (<c>design/30-Strata-Cloud-Store.md</c>,
/// "The worker"). A library loop, not a hosted service: a host runs <see cref="RunAsync"/> under its own
/// background service, or drives <see cref="PollAsync"/> itself.
///
/// Each round reads the feed from its start (long-polling while it is empty) and handles each decision it
/// hasn't handled before, oldest first, one at a time: a decision for a thread it doesn't own is reported
/// <see cref="DecisionOutcome.NotMine"/>; any other is resumed through the store with the decision's
/// signal. The thread is loaded first, and one another graph wrote is not this worker's either
/// (<c>design/31-Graph-Identity.md</c>); the resume goes on from that load. The resume is pinned to the checkpoint the decision answered, so a decision read before the
/// thread moved on can't approve a later boundary. Every decision is handled once per worker (the last
/// 10,000 are remembered): one Cloud keeps offering because nothing was saved to its thread is paged past.
///
/// One worker resumes one thread at a time. Scale with several workers on disjoint threads.
/// </summary>
public sealed class DecisionWorker<TState>
{
    private const int Remembered = 10_000;

    private readonly CompiledGraph<TState> _graph;
    private readonly ICheckpointStore _store;
    private readonly StrataCloudDecisions _decisions;
    private readonly DecisionWorkerOptions<TState> _options;
    private readonly HashSet<string> _seen = new(StringComparer.Ordinal);
    private readonly Queue<string> _seenOrder = new();
    private bool _lastRoundStale;

    /// <param name="graph">The graph whose threads the worker resumes.</param>
    /// <param name="store">The store the threads live in: a <see cref="StrataCloudCheckpointStore"/>.</param>
    /// <param name="decisions">The feed to read decisions from.</param>
    /// <param name="options">Which threads, the run options, the callback and the waits.</param>
    public DecisionWorker(
        CompiledGraph<TState> graph,
        ICheckpointStore store,
        StrataCloudDecisions decisions,
        DecisionWorkerOptions<TState>? options = null)
    {
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(decisions);
        _graph = graph;
        _store = store;
        _decisions = decisions;
        _options = options ?? new DecisionWorkerOptions<TState>();
    }

    /// <summary>
    /// Polls and resumes until <paramref name="cancellationToken"/> is cancelled, then returns. Waits out an
    /// unavailable feed or store (<c>Retry-After</c> on a <c>429</c>, else <see cref="DecisionWorkerOptions{TState}.ErrorBackoff"/>),
    /// and throws a failure no wait can fix: a <c>401</c>, any other refusal of the feed. Cancelling stops a
    /// long-poll at once; a resume in progress runs to its end (bound it with <c>RunOptions.Deadline</c>).
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var time = _decisions.Options.TimeProvider;
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            TimeSpan delay;
            try
            {
                await PollAsync(_options.Wait, cancellationToken).ConfigureAwait(false);
                failures = 0;
                delay = _lastRoundStale ? _options.IdleDelay : TimeSpan.Zero;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (StrataCloudException ex) when (ex.IsTransient)
            {
                failures++;
                var backoff = Backoff(failures);
                delay = ex.StatusCode == 429
                    ? ex.RetryAfter ?? TimeSpan.FromSeconds(5)
                    : ex.RetryAfter is { } after && after > backoff ? after : backoff;
            }

            if (delay > TimeSpan.Zero)
            {
                try
                {
                    await Task.Delay(delay, time, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
            }
        }
    }

    /// <summary>
    /// One round: reads the feed from its start, holding the first request up to <paramref name="wait"/>
    /// while it is empty, follows every cursor, and handles each decision not handled before. Returns them in
    /// order. Throws a <see cref="StrataCloudException"/> from the feed, or one the store threw that a wait
    /// may fix (the decision is then not remembered, so the next round tries it again).
    /// </summary>
    public async Task<IReadOnlyList<HandledDecision<TState>>> PollAsync(TimeSpan wait, CancellationToken cancellationToken = default)
    {
        var handled = new List<HandledDecision<TState>>();
        var sawItems = false;
        string? cursor = null;
        do
        {
            CloudDecisionPage page;
            try
            {
                page = await _decisions
                    .GetAsync(_options.ThreadId, _options.Limit, cursor, cursor is null ? wait : TimeSpan.Zero, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (StrataCloudException ex) when (cursor is not null && ex.StatusCode == 400)
            {
                // A cursor Cloud no longer takes: the next round starts over.
                break;
            }

            sawItems |= page.Items.Count > 0;
            foreach (var decision in page.Items)
            {
                if (_seen.Contains(decision.ApprovalId))
                {
                    continue;
                }

                if (await HandleAsync(decision, cancellationToken).ConfigureAwait(false) is not { } outcome)
                {
                    // Its thread is held by another owner's lease, or its resume wasn't admitted: left for the next
                    // round, not remembered.
                    continue;
                }

                Remember(decision.ApprovalId);
                handled.Add(outcome);
                if (_options.OnHandled is { } onHandled)
                {
                    await onHandled(outcome, cancellationToken).ConfigureAwait(false);
                }
            }

            cursor = page.NextCursor;
        }
        while (cursor is not null);

        _lastRoundStale = sawItems && handled.Count == 0;
        return handled;
    }

    /// <summary>
    /// Resumes <paramref name="decision"/>'s thread and says how it went; <see langword="null"/> when the thread
    /// is held by another owner's lease (<c>design/32-Thread-Leases.md</c>), which is theirs to finish with, or the
    /// run's scheduler didn't admit the resume (<c>design/40-Run-Scheduler.md</c>).
    /// </summary>
    private async Task<HandledDecision<TState>?> HandleAsync(CloudDecision decision, CancellationToken cancellationToken)
    {
        if (!await OwnsAsync(decision, cancellationToken).ConfigureAwait(false))
        {
            return new HandledDecision<TState>(decision, DecisionOutcome.NotMine, Result: null);
        }

        // Loaded before resuming, so another graph's thread is left to the worker running that graph, with nothing
        // run here (design/31-Graph-Identity.md). The resume goes on from this load, so it costs no request.
        LatestCheckpoint? latest;
        try
        {
            latest = await _store.LoadLatestAsync(decision.ThreadId, CancellationToken.None).ConfigureAwait(false);
        }
        catch (StrataCloudException ex) when (ex.IsTransient || ex.Kind == StrataCloudErrorKind.Unauthorized)
        {
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new HandledDecision<TState>(
                decision, DecisionOutcome.Failed, new GraphExecutionResult<TState>.Failed(ex.Message));
        }

        if (latest is not null && !_graph.Owns(latest.Checkpoint))
        {
            return new HandledDecision<TState>(decision, DecisionOutcome.NotMine, Result: null);
        }

        var runOptions = _options.RunOptions?.Invoke(decision) ?? new RunOptions();
        GraphExecutionResult<TState> result;
        try
        {
            // No token: stopping the worker must not cancel a remediation halfway (RunOptions.Deadline bounds it).
            result = await _graph
                .ResumeAsync(
                    decision.ThreadId, decision.Signal, new PinnedStore(_store, decision.Sequence, latest), runOptions, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (StrataCloudException ex) when (ex.IsTransient || ex.Kind == StrataCloudErrorKind.Unauthorized)
        {
            // The store's problem, not the decision's: the round fails and the decision is tried again.
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            result = new GraphExecutionResult<TState>.Failed(ex.Message);
        }

        if (result is GraphExecutionResult<TState>.Failed held
            && held.Message.StartsWith($"thread '{decision.ThreadId}' is held by '", StringComparison.Ordinal))
        {
            return null;
        }

        // The run's scheduler had no slot for it (design/40-Run-Scheduler.md): nothing was tried, so the decision is
        // left for the next round like a held thread's.
        if (result is GraphExecutionResult<TState>.NotAdmitted)
        {
            return null;
        }

        var outcome = result switch
        {
            GraphExecutionResult<TState>.Failed failed when SomeoneElseActed(decision.ThreadId, failed.Message) => DecisionOutcome.AlreadyHandled,
            GraphExecutionResult<TState>.Failed => DecisionOutcome.Failed,
            _ => DecisionOutcome.Resumed,
        };
        return new HandledDecision<TState>(decision, outcome, result);
    }

    private async ValueTask<bool> OwnsAsync(CloudDecision decision, CancellationToken cancellationToken)
    {
        if (_options.ThreadIdPrefix is { } prefix && !decision.ThreadId.StartsWith(prefix, StringComparison.Ordinal))
        {
            return false;
        }

        return _options.Owns is not { } owns || await owns(decision, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The engine's refusals that mean another resume or an end acted on the thread first
    /// (<c>design/28-Ending-A-Thread.md</c>, "Races"), matched exactly for this thread: the checkpoint was
    /// stamped, it changed before this resume's stamp, or the thread is running, which a durable graph's
    /// resume makes it (<c>design/33-Durable-Execution.md</c>).
    /// </summary>
    private static bool SomeoneElseActed(string threadId, string message) =>
        message == $"checkpoint for thread '{threadId}' was already resumed"
        || message == $"checkpoint for thread '{threadId}' changed before it could be resumed: another resume or end got there first"
        || message == $"thread '{threadId}' is running: only a waiting, failed or cancelled thread can be resumed";

    private void Remember(string approvalId)
    {
        if (!_seen.Add(approvalId))
        {
            return;
        }

        _seenOrder.Enqueue(approvalId);
        if (_seenOrder.Count > Remembered)
        {
            _seen.Remove(_seenOrder.Dequeue());
        }
    }

    private TimeSpan Backoff(int failures)
    {
        var backoff = _options.ErrorBackoff;
        var ceiling = backoff.Initial;
        for (var i = 1; i < failures && ceiling < backoff.Max; i++)
        {
            ceiling = ceiling > backoff.Max / 2 ? backoff.Max : ceiling * 2;
        }

        return backoff.Jitter switch
        {
            BackoffJitter.Full => ceiling * Random.Shared.NextDouble(),
            BackoffJitter.Equal => (ceiling / 2) + ((ceiling / 2) * Random.Shared.NextDouble()),
            _ => ceiling,
        };
    }

    /// <summary>
    /// The store a decision's resume goes through: the latest checkpoint, the one the worker already
    /// <paramref name="loaded"/>, is reported at the decision's sequence, so the resume's conditional stamp writes
    /// only if the thread is still at the checkpoint the reviewer decided (with a store that has the conditional
    /// save).
    /// </summary>
    private sealed class PinnedStore(ICheckpointStore inner, long sequence, LatestCheckpoint? loaded) : ICheckpointStore
    {
        public void Save(Checkpoint checkpoint) => inner.Save(checkpoint);

        public Checkpoint? Load(string threadId) => inner.Load(threadId);

        public ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
            inner.SaveAsync(checkpoint, cancellationToken);

        public ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
            inner.LoadAsync(threadId, cancellationToken);

        public ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(loaded is null ? null : loaded with { Sequence = sequence });

        public ValueTask<bool> SaveIfLatestAsync(
            Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
            inner.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken);

        // A resume given RunOptions.Lease holds the thread through the store (design/32-Thread-Leases.md).
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

        public ValueTask<bool> SaveUnderLeaseAsync(
            Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default) =>
            inner.SaveUnderLeaseAsync(checkpoint, lease, expectedSequence, cancellationToken);
    }
}
