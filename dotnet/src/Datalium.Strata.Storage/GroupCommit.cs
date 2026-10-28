using System.Diagnostics;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Storage;

/// <summary>
/// Group commit (<c>design/41-Checkpoint-Efficiency.md</c>): saves that arrive while flushes are running are
/// flushed together, in one transaction, by the Postgres and SQLite stores. There is no timer: a save that
/// finds fewer than <see cref="MaxConcurrentFlushes"/> flushes running is flushed at once. Each save keeps its own answer, one thread's
/// saves keep their order, and a save is answered only when its flush has committed. Mirrors
/// <c>strata-memory</c>'s <c>GroupCommit</c>.
/// </summary>
public sealed record GroupCommitOptions
{
    /// <summary>The most saves one flush takes. At least 1.</summary>
    public int MaxBatch { get; init; } = 256;

    /// <summary>
    /// How many flushes may run at once, each on a connection of its own. At least 1; the SQLite store, which
    /// has one connection, always uses 1.
    /// </summary>
    public int MaxConcurrentFlushes { get; init; } = 4;

    internal GroupCommitOptions Checked()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxBatch, 1, nameof(MaxBatch));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxConcurrentFlushes, 1, nameof(MaxConcurrentFlushes));
        return this;
    }
}

/// <summary>What became of one save in a flush.</summary>
internal enum SaveVerdict
{
    /// <summary>Appended.</summary>
    Saved,

    /// <summary>A conditional save whose thread had moved past its expected sequence: nothing written.</summary>
    Lost,

    /// <summary>A fenced save whose lease token was no longer the thread's latest: nothing written.</summary>
    LeaseLost,
}

/// <summary>One save waiting for, or in, a flush. A store's flush answers it, or fails it.</summary>
internal sealed class QueuedSave(Checkpoint checkpoint, long? expectedSequence, ThreadLease? lease)
{
    private readonly TaskCompletionSource<SaveVerdict> _done = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Checkpoint Checkpoint { get; } = checkpoint;

    public long? ExpectedSequence { get; } = expectedSequence;

    public ThreadLease? Lease { get; } = lease;

    /// <summary>When it arrived, as a <see cref="Stopwatch"/> timestamp.</summary>
    public long ArrivedAt { get; } = Stopwatch.GetTimestamp();

    public bool IsAnswered => _done.Task.IsCompleted;

    public Task<SaveVerdict> Answer => _done.Task;

    /// <summary>Its place in the queue while it waits; null once a flush has taken it, or it was withdrawn.</summary>
    internal LinkedListNode<QueuedSave>? Node { get; set; }

    public void Settle(SaveVerdict verdict) => _done.TrySetResult(verdict);

    public void Fail(Exception exception) => _done.TrySetException(exception);

    public void Withdraw(CancellationToken cancellationToken) => _done.TrySetCanceled(cancellationToken);
}

/// <summary>
/// The queue of a store with <see cref="GroupCommitOptions"/>: takes saves, hands them to the store's flush in
/// batches, and gives each its answer. The flush must settle or fail every save it is given once its
/// transaction has committed; one that throws fails what it left unanswered, except that a flush of several
/// saves that fails for a reason that isn't transient is tried again save by save, so only a save at fault
/// fails.
/// </summary>
internal sealed class GroupCommitQueue
{
    private readonly GroupCommitOptions _options;
    private readonly int _maxFlushes;
    private readonly string _store;
    private readonly Func<IReadOnlyList<QueuedSave>, ValueTask> _flush;
    private readonly Func<Exception, bool> _isTransient;
    private readonly object _lock = new();
    private readonly LinkedList<QueuedSave> _queue = new();

    // Threads with a save in a running flush, and how many: their later saves wait for a later flush.
    private readonly Dictionary<string, int> _inFlight = new(StringComparer.Ordinal);
    private int _flushing;
    private bool _closed;
    private TaskCompletionSource? _idle;

    public GroupCommitQueue(
        GroupCommitOptions options,
        string store,
        Func<IReadOnlyList<QueuedSave>, ValueTask> flush,
        Func<Exception, bool> isTransient,
        int? maxFlushes = null)
    {
        _options = options.Checked();
        _maxFlushes = maxFlushes ?? _options.MaxConcurrentFlushes;
        _store = store;
        _flush = flush;
        _isTransient = isTransient;
    }

    /// <summary>
    /// Queues a save and waits for its flush: <see langword="true"/> when it was appended,
    /// <see langword="false"/> when a conditional save lost its race; a fenced save whose lease was lost throws
    /// <see cref="LeaseLostException"/>. <paramref name="cancellationToken"/> withdraws the save while it is
    /// still queued; once a flush has taken it, the flush answers it.
    /// </summary>
    public async ValueTask<bool> SaveAsync(
        Checkpoint checkpoint, long? expectedSequence, ThreadLease? lease, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var save = new QueuedSave(checkpoint, expectedSequence, lease);
        List<List<QueuedSave>> batches;
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            save.Node = _queue.AddLast(save);
            batches = TakeBatches();
        }

        Start(batches, inline: true);
        SaveVerdict verdict;
        await using (cancellationToken.CanBeCanceled ? cancellationToken.Register(() => Withdraw(save, cancellationToken)) : default)
        {
            verdict = await save.Answer.ConfigureAwait(false);
        }

        return verdict switch
        {
            SaveVerdict.Saved => true,
            SaveVerdict.Lost => false,
            _ => throw new LeaseLostException(lease!),
        };
    }

    /// <summary>Refuses later saves and waits until every queued save has been flushed.</summary>
    public async ValueTask DrainAsync()
    {
        Task idle;
        lock (_lock)
        {
            _closed = true;
            if (_queue.Count == 0 && _flushing == 0)
            {
                return;
            }

            _idle ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            idle = _idle.Task;
        }

        await idle.ConfigureAwait(false);
    }

    private void Withdraw(QueuedSave save, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (save.Node is not { } node)
            {
                return;
            }

            _queue.Remove(node);
            save.Node = null;
            SignalIdleIfDone();
        }

        save.Withdraw(cancellationToken);
    }

    /// <summary>
    /// Under the lock: the batches to start now, while flushes may start. A batch takes queued saves in arrival
    /// order, up to <see cref="GroupCommitOptions.MaxBatch"/>, skipping every save of a thread already in a
    /// running flush.
    /// </summary>
    private List<List<QueuedSave>> TakeBatches()
    {
        var batches = new List<List<QueuedSave>>();
        while (_flushing < _maxFlushes && _queue.Count > 0)
        {
            var batch = new List<QueuedSave>();
            for (var node = _queue.First; node is not null && batch.Count < _options.MaxBatch;)
            {
                var next = node.Next;
                if (!_inFlight.ContainsKey(node.Value.Checkpoint.ThreadId))
                {
                    _queue.Remove(node);
                    node.Value.Node = null;
                    batch.Add(node.Value);
                }

                node = next;
            }

            if (batch.Count == 0)
            {
                break;
            }

            foreach (var save in batch)
            {
                _inFlight[save.Checkpoint.ThreadId] = _inFlight.GetValueOrDefault(save.Checkpoint.ThreadId) + 1;
            }

            _flushing++;
            batches.Add(batch);
        }

        return batches;
    }

    /// <summary>
    /// Starts <paramref name="batches"/>. The first runs on the caller until its first wait when
    /// <paramref name="inline"/>, so a save alone costs no thread hop; the others go to the thread pool, so a
    /// flush that completes synchronously doesn't start the next one on its own stack.
    /// </summary>
    private void Start(List<List<QueuedSave>> batches, bool inline)
    {
        for (var i = 0; i < batches.Count; i++)
        {
            var batch = batches[i];
            if (inline && i == 0)
            {
                _ = RunAsync(batch);
            }
            else
            {
                ThreadPool.UnsafeQueueUserWorkItem(static state => _ = state.queue.RunAsync(state.batch), (queue: this, batch), preferLocal: false);
            }
        }
    }

    private async Task RunAsync(List<QueuedSave> batch)
    {
        try
        {
            StorageTelemetry.Flushing(_store, batch);
            try
            {
                await _flush(batch).ConfigureAwait(false);
            }
            catch (Exception ex) when (batch.Count > 1 && !_isTransient(ex))
            {
                // One save's data may be what failed the flush: each saves alone, so only that one fails.
                foreach (var save in batch.Where(save => !save.IsAnswered))
                {
                    try
                    {
                        await _flush([save]).ConfigureAwait(false);
                    }
                    catch (Exception one)
                    {
                        save.Fail(one);
                    }
                }
            }
            catch (Exception ex)
            {
                foreach (var save in batch)
                {
                    save.Fail(ex);
                }
            }

            foreach (var save in batch.Where(save => !save.IsAnswered))
            {
                save.Fail(new InvalidOperationException($"the {_store} store's flush left a save of thread '{save.Checkpoint.ThreadId}' unanswered"));
            }
        }
        finally
        {
            List<List<QueuedSave>> next;
            lock (_lock)
            {
                _flushing--;
                foreach (var save in batch)
                {
                    var thread = save.Checkpoint.ThreadId;
                    if (--_inFlight[thread] == 0)
                    {
                        _inFlight.Remove(thread);
                    }
                }

                next = TakeBatches();
                SignalIdleIfDone();
            }

            Start(next, inline: false);
        }
    }

    private void SignalIdleIfDone()
    {
        if (_closed && _queue.Count == 0 && _flushing == 0)
        {
            _idle?.TrySetResult();
        }
    }
}
