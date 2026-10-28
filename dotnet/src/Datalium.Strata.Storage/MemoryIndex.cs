namespace Datalium.Strata.Storage;

/// <summary>
/// A memory store's vector index (<c>design/45-Long-Term-Memory.md</c>, "Index"): what the Postgres and Redis memory
/// stores take (<see cref="Postgres.PostgresMemoryOptions.Index"/>, <see cref="Redis.RedisMemoryOptions.Index"/>), and
/// what hosting reads from <c>Strata:Memory:Index</c>. SQLite's search is always exact. Mirrors
/// <c>strata-memory</c>'s <c>MemoryIndex</c>.
/// </summary>
public abstract record MemoryIndex
{
    /// <summary>pgvector's most dimensions for an HNSW index on a <c>vector</c> column.</summary>
    public const int MaxHnswDimensions = 2000;

    private MemoryIndex()
    {
    }

    /// <summary>No index: every search is exact, over every record of the namespace, tenant and model.</summary>
    public static MemoryIndex None { get; } = new NoIndex();

    /// <summary>
    /// An HNSW index on cosine distance. <paramref name="m"/> (2 to 100) is the links per node and
    /// <paramref name="efConstruction"/> (at least twice <paramref name="m"/>, at most 1000) the candidates kept
    /// while it is built: pgvector's defaults and limits.
    /// </summary>
    public static MemoryIndex Hnsw(int m = 16, int efConstruction = 64)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(m, 2);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(m, 100);
        ArgumentOutOfRangeException.ThrowIfLessThan(efConstruction, 2 * m);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(efConstruction, 1000);
        return new HnswIndex(m, efConstruction);
    }

    /// <summary>No index.</summary>
    public sealed record NoIndex : MemoryIndex;

    /// <summary>An HNSW index: made by <see cref="Hnsw"/>.</summary>
    public sealed record HnswIndex : MemoryIndex
    {
        internal HnswIndex(int m, int efConstruction)
        {
            M = m;
            EfConstruction = efConstruction;
        }

        /// <summary>Links per node.</summary>
        public int M { get; }

        /// <summary>Candidates kept while the index is built.</summary>
        public int EfConstruction { get; }
    }
}
