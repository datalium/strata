using System.Text.Json.Nodes;

namespace Datalium.Strata.Memory;

/// <summary>
/// One long-term memory (<c>design/45-Long-Term-Memory.md</c>): something an application stores with an
/// embedding and finds again by similarity, from one run to the next. Episodic, semantic and procedural memory
/// are namespaces an application uses (<c>incidents</c>, <c>facts</c>, <c>runbooks</c>), not types Strata
/// knows. Its JSON form is <c>schema/memory-record.schema.json</c>. Mirrors <c>strata-core</c>'s
/// <c>memory::MemoryRecord</c>.
/// </summary>
/// <param name="Namespace">The kind of memory. A search never crosses one. Not empty, at most 256 bytes of UTF-8.</param>
/// <param name="Tenant">Whose memory it is, <c>""</c> when the application has none. A search reads one tenant only. At most 256 bytes of UTF-8.</param>
/// <param name="Id">Unique within the namespace and tenant. Not empty, at most 256 bytes of UTF-8.</param>
/// <param name="Vector">The embedding: as many finite numbers as the store's <see cref="IMemoryStore.Dimensions"/>, not all zero.</param>
/// <param name="Model">
/// What made <paramref name="Vector"/>, as free text compared exactly. A search names a model and reads only its
/// records, so two models' vectors are never compared, even at the same dimensions.
/// </param>
public sealed record MemoryRecord(string Namespace, string Tenant, string Id, ReadOnlyMemory<float> Vector, string Model)
{
    /// <summary>The application's payload, never searched.</summary>
    public JsonObject? Content { get; init; }

    /// <summary>
    /// The text <see cref="Vector"/> was made from, never searched. Kept so an application that changes models
    /// can embed a namespace again, which a vector alone wouldn't allow.
    /// </summary>
    public string? Text { get; init; }

    /// <summary>String pairs a search and a listing filter on by equality. Keys are not empty, at most 128 bytes of UTF-8.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = MemoryRecords.NoMetadata;

    /// <summary>When the record was first written, by the store's clock at whole seconds; kept by a replacing upsert. A caller's is ignored.</summary>
    public DateTimeOffset? CreatedAt { get; init; }

    /// <summary>When the record was last written, by the store's clock at whole seconds. What retention purges by. A caller's is ignored.</summary>
    public DateTimeOffset? UpdatedAt { get; init; }

    /// <summary>The record's key.</summary>
    public MemoryKey Key => new(Namespace, Tenant, Id);
}

/// <summary>A memory's key: unique within a store.</summary>
public sealed record MemoryKey(string Namespace, string Tenant, string Id);

/// <summary>
/// A search by similarity (<c>design/45-Long-Term-Memory.md</c>, "Search"): the records of one namespace, one
/// tenant and one model, most similar to <see cref="Vector"/> first, ties by id. Every filter is applied before
/// ranking, so a record that fails one never takes a place in the limit.
/// </summary>
/// <param name="Model">The model that made <paramref name="Vector"/>: only records of this model are read.</param>
public sealed record MemorySearch(string Namespace, ReadOnlyMemory<float> Vector, string Model)
{
    /// <summary>The tenant searched; <c>""</c> reads only records without one, never every tenant's.</summary>
    public string Tenant { get; init; } = "";

    /// <summary>How many matches at most: 1 to <see cref="MemoryRecords.MaxLimit"/>.</summary>
    public int Limit { get; init; } = 10;

    /// <summary>Drops matches less similar than this, from −1 to 1, so a search may return fewer than <see cref="Limit"/>. None by default.</summary>
    public double? MinSimilarity { get; init; }

    /// <summary>Ids within the namespace and tenant never returned (the record a search was made for, say).</summary>
    public IReadOnlyCollection<string> Exclude { get; init; } = [];

    /// <summary>Only records whose metadata has every one of these pairs.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = MemoryRecords.NoMetadata;
}

/// <summary>A search's result.</summary>
/// <param name="Similarity">1 − cosine distance, from −1 to 1.</param>
public sealed record MemoryMatch(MemoryRecord Record, double Similarity);

/// <summary>
/// A listing of one namespace and tenant by id (ordinal), in pages: for an application that syncs a namespace,
/// or embeds one again with another model.
/// </summary>
public sealed record MemoryQuery(string Namespace)
{
    /// <summary>The tenant listed; <c>""</c> lists only records without one.</summary>
    public string Tenant { get; init; } = "";

    /// <summary>Only records whose metadata has every one of these pairs.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = MemoryRecords.NoMetadata;

    /// <summary>How many records a page holds at most: 1 to <see cref="MemoryRecords.MaxLimit"/>.</summary>
    public int Limit { get; init; } = 100;

    /// <summary>Where to continue: the previous page's <see cref="MemoryPage.NextCursor"/>.</summary>
    public string? Cursor { get; init; }
}

/// <summary>One page of <see cref="IMemoryStore.ListAsync"/>.</summary>
/// <param name="NextCursor">Pass this as <see cref="MemoryQuery.Cursor"/> for the next page; <see langword="null"/> when no more records follow.</param>
public sealed record MemoryPage(IReadOnlyList<MemoryRecord> Records, string? NextCursor);

/// <summary>
/// What <see cref="IMemoryRetention.PurgeAsync"/> deletes: records last written before <see cref="OlderThan"/>
/// (at the second, exclusive), oldest first.
/// </summary>
public sealed record MemoryPurge(DateTimeOffset OlderThan)
{
    /// <summary>Only this namespace; every namespace when <see langword="null"/>.</summary>
    public string? Namespace { get; init; }

    /// <summary>Only this tenant (<c>""</c>: only records without one); every tenant when <see langword="null"/>.</summary>
    public string? Tenant { get; init; }

    /// <summary>How many records at most, at least 1. Call again while a purge returns it.</summary>
    public int Limit { get; init; } = 1000;
}
