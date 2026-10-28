using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;

namespace Datalium.Strata.Graph;

/// <summary>
/// Runs a thread under a lease (<see cref="RunOptions.Lease"/>, <c>design/32-Thread-Leases.md</c>), so one
/// owner at a time works on it and a stale owner's saves are refused. The run acquires the thread's lease
/// before its first save, renews it every third of <paramref name="Ttl"/> on the run's clock, saves under it,
/// and releases it when it returns. A thread another owner holds is refused; a run that loses its lease stops
/// with <see cref="CancelReason.LeaseLost"/> and writes nothing more.
/// </summary>
/// <param name="Owner">
/// Who holds the thread, for people and logs: name the process, not the service, since a thread held under
/// one name is refused to another process using the same name.
/// </param>
/// <param name="Ttl">
/// How long the lease lasts unless renewed: how long a thread stays held after its owner dies. At least one
/// millisecond.
/// </param>
public sealed record LeaseOptions(string Owner, TimeSpan Ttl);

/// <summary>How a save made for a run with a lease ended.</summary>
internal enum LeasedSave
{
    Saved,

    /// <summary>A conditional save found something appended since its expected sequence. Nothing was written.</summary>
    NotLatest,

    /// <summary>The run's lease is lost. Nothing was written, and nothing more will be.</summary>
    LeaseLost,
}

/// <summary>
/// One run's lease on its thread (<c>design/32-Thread-Leases.md</c>, "What a run does"): acquired before the
/// run's first save, renewed in the background, carried by every save, released when the run returns. When it
/// is lost (a renewal says so, a save is refused, or no renewal succeeded for a whole ttl) <see cref="Lost"/>
/// fires, which cancels the run, and every later save writes nothing.
/// </summary>
internal sealed class RunLease(LeaseOptions options, TimeProvider clock, RunTelemetry telemetry) : IDisposable
{
    // The longest delay Task.Delay takes.
    private static readonly TimeSpan MaxDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private readonly CancellationTokenSource _lost = new();
    private readonly CancellationTokenSource _stop = new();
    private volatile ThreadLease? _current;
    private ICheckpointStore? _store;
    private Task? _renewal;
    private int _isLost;

    /// <summary>Cancelled when the lease is lost.</summary>
    public CancellationToken Lost => _lost.Token;

    public bool IsLost => Volatile.Read(ref _isLost) == 1;

    /// <summary>
    /// Takes <paramref name="threadId"/>'s lease from <paramref name="store"/> and starts renewing it. Returns
    /// the lease that holds the thread when it is held, <see langword="null"/> once this run holds it. A second
    /// call changes nothing.
    /// </summary>
    public async ValueTask<ThreadLease?> AcquireAsync(ICheckpointStore store, string threadId)
    {
        if (_current is not null)
        {
            return null;
        }

        var attempt = await store.TryAcquireLeaseAsync(threadId, options.Owner, options.Ttl, CancellationToken.None)
            .ConfigureAwait(false);
        if (attempt.Lease is not { } lease)
        {
            RunTelemetry.LeaseHeld();
            return attempt.HeldBy
                ?? throw new InvalidOperationException("the store neither acquired the lease nor said who holds it");
        }

        _store = store;
        _current = lease;
        telemetry.LeaseAcquired(lease);

        // The first renewal's timer is set here, before the run goes on, so time that passes from now on
        // counts towards it whatever the thread pool is doing.
        _renewal = RenewAsync(store, NextRenewal());
        return null;
    }

    /// <summary>Completes a third of the ttl from now on the run's clock, or is cancelled when the run stops renewing.</summary>
    private Task NextRenewal()
    {
        var interval = options.Ttl / 3;
        interval = interval < ThreadLeases.MinimumTtl ? ThreadLeases.MinimumTtl : interval > MaxDelay ? MaxDelay : interval;
        return Task.Delay(interval, clock, _stop.Token);
    }

    /// <summary>
    /// Saves <paramref name="checkpoint"/> under the lease, conditionally when there is an
    /// <paramref name="expectedSequence"/>. Once the lease is lost nothing is written, by this save or any
    /// later one.
    /// </summary>
    public async ValueTask<LeasedSave> SaveAsync(ICheckpointStore store, Checkpoint checkpoint, long? expectedSequence)
    {
        if (IsLost)
        {
            return LeasedSave.LeaseLost;
        }

        var lease = _current ?? throw new InvalidOperationException("a run with a lease saved before acquiring it");
        try
        {
            return await store.SaveUnderLeaseAsync(checkpoint, lease, expectedSequence, CancellationToken.None)
                .ConfigureAwait(false)
                ? LeasedSave.Saved
                : LeasedSave.NotLatest;
        }
        catch (LeaseLostException)
        {
            Lose(lease);
            return LeasedSave.LeaseLost;
        }
    }

    /// <summary>
    /// Stops renewing and gives the lease up, when the run returns. The renewal in flight, if any, is waited
    /// for first: a renewal landing after the release would hold the thread again with nobody to release it.
    /// A release that fails is ignored: the lease expires by itself.
    /// </summary>
    public async ValueTask ReleaseAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        if (_renewal is { } renewal)
        {
            await renewal.ConfigureAwait(false);
        }

        if (_current is not { } lease || _store is not { } store || IsLost)
        {
            return;
        }

        _current = null;
        try
        {
            await store.ReleaseLeaseAsync(lease, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The run's result stands; the thread is free again once the lease expires.
        }
    }

    /// <summary>
    /// Renews every third of the ttl on the run's clock until stopped, the first time when
    /// <paramref name="due"/> completes. A renewal the store refuses loses the lease; one that throws doesn't,
    /// until a whole ttl has passed since the last that succeeded.
    /// </summary>
    private async Task RenewAsync(ICheckpointStore store, Task due)
    {
        var renewedAt = clock.GetTimestamp();
        while (true)
        {
            try
            {
                await due.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            // The next renewal's timer is set before this one is made: the clock is never without one, so no
            // tick is missed while a renewal is in flight.
            due = NextRenewal();

            var lease = _current!;
            ThreadLease? renewed;
            try
            {
                renewed = await store.RenewLeaseAsync(lease, options.Ttl, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception)
            {
                if (clock.GetElapsedTime(renewedAt) >= options.Ttl)
                {
                    Lose(lease);
                    return;
                }

                continue;
            }

            if (renewed is null)
            {
                Lose(lease);
                return;
            }

            _current = renewed;
            renewedAt = clock.GetTimestamp();
        }
    }

    private void Lose(ThreadLease lease)
    {
        if (Interlocked.Exchange(ref _isLost, 1) == 1)
        {
            return;
        }

        telemetry.LeaseLost(lease);
        _lost.Cancel();
    }

    public void Dispose()
    {
        _stop.Dispose();
        _lost.Dispose();
    }
}
