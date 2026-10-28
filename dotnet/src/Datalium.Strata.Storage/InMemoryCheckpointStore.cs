using System.Collections.Concurrent;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Storage;

/// <summary>
/// In-memory <see cref="ICheckpointStore"/> adapter (STRATA-SPEC-002), the first tier of
/// <c>design/03-Architecture.md</c>'s "in-memory ring buffer → SQLite/RocksDB → Redis/PostgreSQL"
/// progression. Keeps every thread's whole checkpoint log, like the persistent stores keep it on disk,
/// so it answers <see cref="ICheckpointQuery"/> like they do (<c>design/19-Async-Queryable-Store.md</c>).
/// It grows with every save; it is for tests and development.
///
/// Thread leases (<c>design/32-Thread-Leases.md</c>) are kept beside the logs and expire on
/// <paramref name="timeProvider"/> (the system clock when not given), so a test can pass a fake one.
/// </summary>
public sealed class InMemoryCheckpointStore(TimeProvider? timeProvider = null) : ICheckpointStore, ICheckpointQuery, ICheckpointRetention
{
    private readonly ConcurrentDictionary<string, List<Checkpoint>> _logs = new();

    // Every lease member, and every save under a lease, runs under this one lock, so a save's token check
    // and its append can't be split by an acquisition.
    private readonly Dictionary<string, LeaseRow> _leases = [];
    private readonly TimeProvider _clock = timeProvider ?? TimeProvider.System;

    /// <summary>A thread's lease: its latest token, and who holds it until when (<c>null</c> once released).</summary>
    private sealed record LeaseRow(long Token, string? Owner, DateTimeOffset? ExpiresAt)
    {
        public bool HeldAt(DateTimeOffset now) => ExpiresAt is { } expiresAt && expiresAt > now;
    }

    public void Save(Checkpoint checkpoint)
    {
        var log = _logs.GetOrAdd(checkpoint.ThreadId, _ => []);
        lock (log)
        {
            log.Add(checkpoint);
        }
    }

    public Checkpoint? Load(string threadId)
    {
        if (!_logs.TryGetValue(threadId, out var log))
        {
            return null;
        }

        lock (log)
        {
            return log.Count == 0 ? null : log[^1];
        }
    }

    public ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default)
    {
        if (!_logs.TryGetValue(threadId, out var log))
        {
            return ValueTask.FromResult<LatestCheckpoint?>(null);
        }

        lock (log)
        {
            return ValueTask.FromResult(log.Count == 0 ? null : new LatestCheckpoint(log[^1], log.Count - 1));
        }
    }

    /// <summary>Appends under the log's lock if its last sequence is still <paramref name="expectedSequence"/>.</summary>
    public ValueTask<bool> SaveIfLatestAsync(
        Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default)
    {
        var log = _logs.GetOrAdd(checkpoint.ThreadId, _ => []);
        lock (log)
        {
            if (expectedSequence is { } expected && log.Count - 1 != expected)
            {
                return ValueTask.FromResult(false);
            }

            log.Add(checkpoint);
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
        string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ThreadLeases.Check(owner, ttl);
        lock (_leases)
        {
            var now = _clock.GetUtcNow();
            var row = _leases.GetValueOrDefault(threadId);
            if (row is not null && row.HeldAt(now))
            {
                return ValueTask.FromResult(
                    new LeaseAttempt(null, new ThreadLease(threadId, row.Owner!, row.Token, row.ExpiresAt!.Value)));
            }

            var lease = new ThreadLease(threadId, owner, (row?.Token ?? 0) + 1, now + ttl);
            _leases[threadId] = new LeaseRow(lease.Token, owner, lease.ExpiresAt);
            return ValueTask.FromResult(new LeaseAttempt(lease, null));
        }
    }

    public ValueTask<ThreadLease?> RenewLeaseAsync(
        ThreadLease lease, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ThreadLeases.CheckTtl(ttl);
        lock (_leases)
        {
            if (_leases.GetValueOrDefault(lease.ThreadId)?.Token != lease.Token)
            {
                return ValueTask.FromResult<ThreadLease?>(null);
            }

            var renewed = lease with { ExpiresAt = _clock.GetUtcNow() + ttl };
            _leases[lease.ThreadId] = new LeaseRow(lease.Token, lease.Owner, renewed.ExpiresAt);
            return ValueTask.FromResult<ThreadLease?>(renewed);
        }
    }

    public ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        lock (_leases)
        {
            if (_leases.GetValueOrDefault(lease.ThreadId)?.Token != lease.Token)
            {
                return ValueTask.FromResult(false);
            }

            _leases[lease.ThreadId] = new LeaseRow(lease.Token, Owner: null, ExpiresAt: null);
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken cancellationToken = default)
    {
        lock (_leases)
        {
            return ValueTask.FromResult(
                _leases.GetValueOrDefault(threadId) is { } row && row.HeldAt(_clock.GetUtcNow())
                    ? new ThreadLease(threadId, row.Owner!, row.Token, row.ExpiresAt!.Value)
                    : null);
        }
    }

    /// <summary>Checks the token and appends under the lease lock, so no acquisition can come between them.</summary>
    public ValueTask<bool> SaveUnderLeaseAsync(
        Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default)
    {
        ThreadLeases.CheckThread(checkpoint, lease);
        lock (_leases)
        {
            if (_leases.GetValueOrDefault(lease.ThreadId)?.Token != lease.Token)
            {
                throw new LeaseLostException(lease);
            }

            return SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken);
        }
    }

    public ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default)
    {
        var latest = new List<(long, Checkpoint)>();
        foreach (var log in _logs.Values)
        {
            lock (log)
            {
                if (log.Count > 0)
                {
                    latest.Add((log.Count - 1, log[^1]));
                }
            }
        }

        return ValueTask.FromResult(CheckpointListing.PageFromLatest(latest, query));
    }

    public ValueTask<CheckpointLogPage> GetLogAsync(
        string threadId, long? afterSequence = null, int limit = ThreadQuery.DefaultLimit, CancellationToken cancellationToken = default)
    {
        ThreadQuery.CheckLimit(limit);
        var entries = new List<CheckpointLogEntry>();
        if (_logs.TryGetValue(threadId, out var log))
        {
            lock (log)
            {
                var start = (int)Math.Max(0, (afterSequence ?? -1) + 1);
                for (var sequence = start; sequence < log.Count && entries.Count < limit; sequence++)
                {
                    entries.Add(new CheckpointLogEntry(sequence, log[sequence]));
                }
            }
        }

        return ValueTask.FromResult(CheckpointLogPage.For(entries, limit));
    }

    /// <summary>
    /// Empties the logs of threads that are done with (<c>design/34-Storage-Hardening.md</c>, "Retention"),
    /// oldest first, each under the lease lock and its own, and only if its latest checkpoint is still the one
    /// that was picked and no live lease holds it.
    /// </summary>
    public ValueTask<int> PurgeAsync(
        DateTimeOffset olderThan,
        IReadOnlySet<CheckpointThreadState>? states = null,
        int limit = ThreadQuery.MaxLimit,
        CancellationToken cancellationToken = default)
    {
        ThreadQuery.CheckLimit(limit);
        states ??= CheckpointRetention.Finished;
        var before = CheckpointTimestamps.Text(olderThan);
        var now = _clock.GetUtcNow();
        var picked = new List<(string ThreadId, Checkpoint Latest)>();
        foreach (var (threadId, log) in _logs)
        {
            // A held thread isn't a candidate, so it takes no place in the limit.
            lock (_leases)
            {
                if (_leases.GetValueOrDefault(threadId) is { } lease && lease.HeldAt(now))
                {
                    continue;
                }
            }

            lock (log)
            {
                if (log.Count > 0 && CheckpointRetention.Takes(log[^1], states, before))
                {
                    picked.Add((threadId, log[^1]));
                }
            }
        }

        var purged = 0;
        foreach (var (threadId, latest) in picked
            .OrderBy(p => CheckpointTimestamps.UpdatedAt(p.Latest), StringComparer.Ordinal)
            .ThenBy(p => p.ThreadId, StringComparer.Ordinal)
            .Take(limit))
        {
            lock (_leases)
            {
                if (_leases.GetValueOrDefault(threadId) is { } lease && lease.HeldAt(_clock.GetUtcNow()))
                {
                    continue;
                }

                // The entry stays, empty: a save that already has the list in hand must not write into a
                // list nobody reads. An empty log is no thread.
                var log = _logs[threadId];
                lock (log)
                {
                    if (log.Count == 0 || !ReferenceEquals(log[^1], latest))
                    {
                        continue;
                    }

                    log.Clear();
                }

                _leases.Remove(threadId);
                purged++;
            }
        }

        return ValueTask.FromResult(purged);
    }
}
