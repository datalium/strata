namespace Datalium.Strata.Memory;

/// <summary>
/// The long-term memory port (<c>design/45-Long-Term-Memory.md</c>): records with an embedding, found again by
/// cosine similarity. Separate from <see cref="IHistoryStore"/>, which keeps one conversation: a memory is what an
/// agent recalls from one run to the next by meaning. The store takes vectors and never calls a model, so storage
/// stays apart from providers, retries and cost. Adapters: <see cref="InMemoryMemoryStore"/> here; Postgres with
/// pgvector, SQLite with sqlite-vec and Redis with the Query Engine in <c>Datalium.Strata.Storage</c>. Mirrors
/// <c>strata-core</c>'s <c>memory::MemoryStore</c>.
///
/// Every member refuses bad input before any I/O with an <see cref="ArgumentException"/> whose message the shared
/// cases pin (<see cref="MemoryRecords"/>), and fails with a <see cref="MemoryStoreException"/> when the store
/// can't do what was asked.
/// </summary>
public interface IMemoryStore
{
    /// <summary>The length of every vector this store holds; fixed when the store is first made.</summary>
    int Dimensions { get; }

    /// <summary>
    /// Writes <paramref name="record"/>, replacing every field of one with the same key but its
    /// <see cref="MemoryRecord.CreatedAt"/>. The timestamps are the store's.
    /// </summary>
    ValueTask UpsertAsync(MemoryRecord record, CancellationToken cancellationToken = default);

    /// <summary>
    /// <see cref="UpsertAsync"/> for each of <paramref name="records"/>, in order. Every record is checked before
    /// any is written; the SQL stores write them in one transaction.
    /// </summary>
    ValueTask UpsertManyAsync(IReadOnlyList<MemoryRecord> records, CancellationToken cancellationToken = default);

    /// <summary>The record at <paramref name="key"/>, or <see langword="null"/>.</summary>
    ValueTask<MemoryRecord?> GetAsync(MemoryKey key, CancellationToken cancellationToken = default);

    /// <summary>Deletes the record at <paramref name="key"/>; whether there was one.</summary>
    ValueTask<bool> DeleteAsync(MemoryKey key, CancellationToken cancellationToken = default);

    /// <summary>The records most similar to <paramref name="search"/>'s vector, most similar first, ties by id (ordinal).</summary>
    ValueTask<IReadOnlyList<MemoryMatch>> SearchAsync(MemorySearch search, CancellationToken cancellationToken = default);

    /// <summary>One page of a namespace and tenant's records, by id (ordinal).</summary>
    ValueTask<MemoryPage> ListAsync(MemoryQuery query, CancellationToken cancellationToken = default);
}

/// <summary>
/// Deletes memories by age (<c>design/45-Long-Term-Memory.md</c>, "Retention"). Separate from
/// <see cref="IMemoryStore"/>, as <see cref="Interruption.ICheckpointRetention"/> is from the checkpoint store;
/// every built-in memory store implements it. Nothing in the engine calls it: retention is the application's to
/// decide and schedule. Mirrors <c>strata-core</c>'s <c>memory::MemoryRetention</c>.
/// </summary>
public interface IMemoryRetention
{
    /// <summary>
    /// Deletes the records <paramref name="purge"/> names, oldest first (then by namespace, tenant and id), up
    /// to its limit, and returns how many: call it again while it returns the limit.
    /// </summary>
    ValueTask<int> PurgeAsync(MemoryPurge purge, CancellationToken cancellationToken = default);
}

/// <summary>
/// A memory store couldn't do what was asked: its database refused or couldn't be reached, its layout is another
/// build's or another size's. Bad input is an <see cref="ArgumentException"/> instead, and cancellation stays an
/// <see cref="OperationCanceledException"/>. The cause, when there is one, is the inner exception.
/// </summary>
public sealed class MemoryStoreException : Exception
{
    public MemoryStoreException(string message)
        : base(message)
    {
    }

    public MemoryStoreException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
