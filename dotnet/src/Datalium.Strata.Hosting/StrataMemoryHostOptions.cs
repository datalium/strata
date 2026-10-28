using Datalium.Strata.Memory;
using Datalium.Strata.Storage;

namespace Datalium.Strata.Hosting;

/// <summary>
/// The memory store's and the memory helper's settings, from <c>Strata:Memory</c>
/// (<c>design/45-Long-Term-Memory.md</c>, "Hosting"). The connection is the checkpoint store's: <c>Strata:Postgres</c>,
/// <c>Strata:Sqlite:Path</c> or <c>Strata:Redis</c>. Named apart from <c>Datalium.Strata.AI.StrataMemoryOptions</c>,
/// the helper's own options, which <see cref="Model"/> and <see cref="TenantKey"/> become.
/// </summary>
public sealed class StrataMemoryHostOptions
{
    internal const string Section = "Memory";

    /// <summary>
    /// The length of every vector: the embedding model's. Required: a memory store is made with its dimensions, and
    /// the host doesn't start without them, nor on a database whose memory was made with others.
    /// </summary>
    public int Dimensions { get; set; }

    /// <summary>The Postgres and Redis stores' vector index; the SQLite and in-memory stores are always exact.</summary>
    public StoreMemoryIndexOptions Index { get; } = new();

    /// <summary>The Postgres and SQLite stores' table. <see langword="null"/>: the store's default, <c>memory_records</c>.</summary>
    public string? Table { get; set; }

    /// <summary>The Redis store's key prefix. <see langword="null"/> or blank: <c>Strata:Redis:Prefix</c>.</summary>
    public string? Prefix { get; set; }

    /// <summary>The SQLite store's sqlite-vec extension. <see langword="null"/>: the store's default, <c>vec0</c>.</summary>
    public string? Extension { get; set; }

    /// <summary>
    /// The label of the memory helper's vectors (<c>StrataMemoryOptions.Model</c>). <see langword="null"/> or blank:
    /// <c>&lt;provider&gt;/&lt;model&gt;</c> from the registered embedding generator.
    /// </summary>
    public string? Model { get; set; }

    /// <summary>The run metadata key the memory helper reads a tenant from. <c>tenant</c> by default.</summary>
    public string TenantKey { get; set; } = "tenant";
}

/// <summary>Which vector index a memory store makes (<see cref="StoreMemoryIndexOptions"/>).</summary>
public enum MemoryIndexKind
{
    /// <summary>None: every search is exact.</summary>
    None,

    /// <summary>An approximate HNSW index, for tables past tens of thousands of records per namespace, tenant and model.</summary>
    Hnsw,
}

/// <summary>
/// A memory store's vector index as configuration can say it, from <c>Strata:Memory:Index</c>. The defaults are
/// <see cref="MemoryIndex.Hnsw"/>'s; <see cref="M"/> and <see cref="EfConstruction"/> are read only for HNSW.
/// </summary>
public sealed class StoreMemoryIndexOptions
{
    /// <summary>None (the default) or HNSW.</summary>
    public MemoryIndexKind Kind { get; set; }

    /// <summary>HNSW's links per node: 2 to 100.</summary>
    public int M { get; set; } = 16;

    /// <summary>HNSW's candidates kept while the index is built: at least twice <see cref="M"/>, at most 1000.</summary>
    public int EfConstruction { get; set; } = 64;

    /// <summary>The index these settings name.</summary>
    public MemoryIndex ToIndex() => Kind == MemoryIndexKind.Hnsw ? MemoryIndex.Hnsw(M, EfConstruction) : MemoryIndex.None;
}

/// <summary>
/// The memory retention driver's settings, from <c>Strata:Memory:Retention</c> (<c>design/45-Long-Term-Memory.md</c>,
/// "Retention"): a round purges memories last written more than <see cref="OlderThan"/> ago, batch after batch.
/// </summary>
public sealed class MemoryRetentionOptions
{
    internal const string Section = "Memory:Retention";

    /// <summary>The wait between two rounds.</summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>The most records one purge deletes: 1 to <see cref="MemoryRecords.MaxLimit"/>. A round purges until one comes back short.</summary>
    public int BatchSize { get; set; } = MemoryRecords.MaxLimit;

    /// <summary>
    /// How long a memory is kept after it was last written. No default: an application says how long it keeps
    /// its memories, and the host doesn't start without it.
    /// </summary>
    public TimeSpan? OlderThan { get; set; }

    /// <summary>Only this namespace; every namespace when <see langword="null"/>.</summary>
    public string? Namespace { get; set; }

    /// <summary>Only this tenant (<c>""</c>: only records without one); every tenant when <see langword="null"/>.</summary>
    public string? Tenant { get; set; }
}
