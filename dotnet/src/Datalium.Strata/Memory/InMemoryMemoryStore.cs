namespace Datalium.Strata.Memory;

/// <summary>
/// The in-memory <see cref="IMemoryStore"/> (<c>design/45-Long-Term-Memory.md</c>): records in a dictionary
/// behind a lock, searched exactly, by cosine similarity in double precision over every record of the namespace,
/// tenant and model. For tests, and for an application whose memories fit in its process and need not outlive it.
/// It records no metrics. Mirrors <c>strata-core</c>'s <c>memory::InMemoryMemoryStore</c>.
///
/// Each record is copied on the way in and out, so a caller that changes its vector or content afterwards changes
/// nothing stored.
/// </summary>
public sealed class InMemoryMemoryStore : IMemoryStore, IMemoryRetention
{
    private readonly Dictionary<MemoryKey, MemoryRecord> _records = [];
    private readonly Lock _gate = new();
    private readonly TimeProvider _clock;

    /// <param name="dimensions">The length of every vector: 1 to <see cref="MemoryRecords.MaxDimensions"/>.</param>
    /// <param name="timeProvider">The clock for the records' timestamps; <see cref="TimeProvider.System"/> when <see langword="null"/>.</param>
    public InMemoryMemoryStore(int dimensions, TimeProvider? timeProvider = null)
    {
        Dimensions = MemoryRecords.CheckDimensions(dimensions);
        _clock = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc/>
    public int Dimensions { get; }

    /// <inheritdoc/>
    public ValueTask UpsertAsync(MemoryRecord record, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(record, Dimensions);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            Write(record, Now());
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public ValueTask UpsertManyAsync(IReadOnlyList<MemoryRecord> records, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(records, Dimensions);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var now = Now();
            foreach (var record in records)
            {
                Write(record, now);
            }
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public ValueTask<MemoryRecord?> GetAsync(MemoryKey key, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(key);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            return ValueTask.FromResult(_records.TryGetValue(key, out var record) ? Copy(record) : null);
        }
    }

    /// <inheritdoc/>
    public ValueTask<bool> DeleteAsync(MemoryKey key, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(key);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            return ValueTask.FromResult(_records.Remove(key));
        }
    }

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<MemoryMatch>> SearchAsync(MemorySearch search, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(search, Dimensions);
        cancellationToken.ThrowIfCancellationRequested();
        var exclude = search.Exclude as IReadOnlySet<string> ?? search.Exclude.ToHashSet(StringComparer.Ordinal);
        List<MemoryMatch> matches;
        lock (_gate)
        {
            // Every filter before ranking: a record that fails one never takes a place in the limit.
            matches =
            [
                .. _records.Values
                    .Where(record => record.Namespace == search.Namespace
                        && record.Tenant == search.Tenant
                        && record.Model == search.Model
                        && !exclude.Contains(record.Id)
                        && MemoryRecords.Matches(record.Metadata, search.Metadata))
                    .Select(record => new MemoryMatch(record, MemoryRecords.CosineSimilarity(record.Vector.Span, search.Vector.Span)))
                    .Where(match => search.MinSimilarity is not { } min || match.Similarity >= min)
                    .OrderByDescending(match => match.Similarity)
                    .ThenBy(match => match.Record.Id, MemoryRecords.ByteOrder)
                    .Take(search.Limit)
                    .Select(match => match with { Record = Copy(match.Record) }),
            ];
        }

        return ValueTask.FromResult<IReadOnlyList<MemoryMatch>>(matches);
    }

    /// <inheritdoc/>
    public ValueTask<MemoryPage> ListAsync(MemoryQuery query, CancellationToken cancellationToken = default)
    {
        var after = MemoryRecords.Check(query);
        cancellationToken.ThrowIfCancellationRequested();
        List<MemoryRecord> page;
        lock (_gate)
        {
            // One more than a page, to know whether another follows.
            page =
            [
                .. _records.Values
                    .Where(record => record.Namespace == query.Namespace
                        && record.Tenant == query.Tenant
                        && (after is null || MemoryRecords.ByteOrder.Compare(record.Id, after) > 0)
                        && MemoryRecords.Matches(record.Metadata, query.Metadata))
                    .OrderBy(record => record.Id, MemoryRecords.ByteOrder)
                    .Take(query.Limit + 1)
                    .Select(Copy),
            ];
        }

        string? next = null;
        if (page.Count > query.Limit)
        {
            page.RemoveAt(page.Count - 1);
            next = MemoryRecords.EncodeCursor(page[^1].Id);
        }

        return ValueTask.FromResult(new MemoryPage(page, next));
    }

    /// <inheritdoc/>
    public ValueTask<int> PurgeAsync(MemoryPurge purge, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(purge);
        cancellationToken.ThrowIfCancellationRequested();

        // At the second, exclusive: as the SQL stores compare their stored text.
        var cutoff = MemoryRecords.Truncate(purge.OlderThan);
        lock (_gate)
        {
            var doomed = _records.Values
                .Where(record => record.UpdatedAt < cutoff
                    && (purge.Namespace is null || record.Namespace == purge.Namespace)
                    && (purge.Tenant is null || record.Tenant == purge.Tenant))
                .OrderBy(record => record.UpdatedAt)
                .ThenBy(record => record.Namespace, MemoryRecords.ByteOrder)
                .ThenBy(record => record.Tenant, MemoryRecords.ByteOrder)
                .ThenBy(record => record.Id, MemoryRecords.ByteOrder)
                .Take(purge.Limit)
                .Select(record => record.Key)
                .ToList();
            foreach (var key in doomed)
            {
                _records.Remove(key);
            }

            return ValueTask.FromResult(doomed.Count);
        }
    }

    private DateTimeOffset Now() => MemoryRecords.Truncate(_clock.GetUtcNow());

    /// <summary>Stores a copy of <paramref name="record"/> at <paramref name="now"/>, keeping a replaced record's creation time. Under the lock.</summary>
    private void Write(MemoryRecord record, DateTimeOffset now)
    {
        var created = _records.TryGetValue(record.Key, out var existing) ? existing.CreatedAt : now;
        _records[record.Key] = Copy(record) with { CreatedAt = created, UpdatedAt = now };
    }

    private static MemoryRecord Copy(MemoryRecord record) => record with
    {
        Vector = record.Vector.ToArray(),
        Content = record.Content?.DeepClone().AsObject(),
        Metadata = record.Metadata.ToDictionary(StringComparer.Ordinal),
    };
}
