using System.Buffers.Text;
using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Memory;

/// <summary>
/// What every memory store shares (<c>design/45-Long-Term-Memory.md</c>): the input checks, made before any I/O
/// with the messages <c>schema/fixtures/memory/search-cases.json</c> pins (an <see cref="ArgumentException"/> with
/// no parameter name, so its message is exactly the case's); the listing cursor; cosine similarity; the record's
/// JSON form. Public so an adapter in another assembly, or an application's own, behaves as the built-in ones do.
/// Mirrors <c>strata-core</c>'s <c>memory</c> module.
/// </summary>
public static class MemoryRecords
{
    /// <summary>The record schema's version: <c>schema/memory-record.schema.json</c>.</summary>
    public const string SchemaVersion = "1.0.0";

    /// <summary>The most bytes of UTF-8 a namespace, a tenant or an id may have.</summary>
    public const int MaxKeyBytes = 256;

    /// <summary>The most bytes of UTF-8 a metadata key may have.</summary>
    public const int MaxMetadataKeyBytes = 128;

    /// <summary>The largest search limit and listing page.</summary>
    public const int MaxLimit = 1000;

    /// <summary>The most dimensions a store may have: pgvector's limit for a column without an index.</summary>
    public const int MaxDimensions = 16_000;

    /// <summary>An empty metadata map, shared.</summary>
    public static IReadOnlyDictionary<string, string> NoMetadata { get; } = new Dictionary<string, string>();

    private static readonly JsonSerializerOptions Compact = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    /// <summary>Refuses dimensions outside 1 to <see cref="MaxDimensions"/>: for a store's constructor.</summary>
    public static int CheckDimensions(int dimensions) =>
        dimensions is >= 1 and <= MaxDimensions
            ? dimensions
            : throw new ArgumentOutOfRangeException(
                nameof(dimensions), dimensions, $"a memory store has 1 to {MaxDimensions} dimensions");

    /// <summary>Refuses a record a store of <paramref name="dimensions"/> can't hold.</summary>
    public static void Check(MemoryRecord record, int dimensions)
    {
        ArgumentNullException.ThrowIfNull(record);
        CheckKey(record.Namespace, record.Tenant, record.Id);
        CheckEmbedding(record.Vector.Span, record.Model, dimensions);
        CheckMetadata(record.Metadata);
    }

    /// <summary>Refuses every record of <paramref name="records"/> as <see cref="Check(MemoryRecord, int)"/> does, before any is written.</summary>
    public static void Check(IReadOnlyList<MemoryRecord> records, int dimensions)
    {
        ArgumentNullException.ThrowIfNull(records);
        foreach (var record in records)
        {
            Check(record, dimensions);
        }
    }

    /// <summary>Refuses a key no record can have.</summary>
    public static void Check(MemoryKey key)
    {
        ArgumentNullException.ThrowIfNull(key);
        CheckKey(key.Namespace, key.Tenant, key.Id);
    }

    /// <summary>Refuses a search a store of <paramref name="dimensions"/> can't run.</summary>
    public static void Check(MemorySearch search, int dimensions)
    {
        ArgumentNullException.ThrowIfNull(search);
        CheckNamespace(search.Namespace);
        CheckTenant(search.Tenant);
        CheckEmbedding(search.Vector.Span, search.Model, dimensions);
        CheckSearchOptions(search);
    }

    /// <summary>
    /// Refuses a search as <see cref="Check(MemorySearch, int)"/> does but for its vector and model, which aren't
    /// read: for a caller that makes the vector afterwards and wants a bad search refused before it pays for one,
    /// as the memory helper does before its embedding call (<c>design/45-Long-Term-Memory.md</c>, "The helper").
    /// </summary>
    public static void CheckFilters(MemorySearch search)
    {
        ArgumentNullException.ThrowIfNull(search);
        CheckNamespace(search.Namespace);
        CheckTenant(search.Tenant);
        CheckSearchOptions(search);
    }

    private static void CheckSearchOptions(MemorySearch search)
    {
        CheckLimit(search.Limit);
        if (search.MinSimilarity is { } min && !(min is >= -1 and <= 1))
        {
            throw new ArgumentException(
                $"the minimum similarity is {min.ToString(CultureInfo.InvariantCulture)}; it must be from -1 to 1");
        }

        ArgumentNullException.ThrowIfNull(search.Exclude);
        CheckMetadata(search.Metadata);
    }

    /// <summary>Refuses a listing a store can't run; returns the id it continues after, if it continues.</summary>
    public static string? Check(MemoryQuery query)
    {
        ArgumentNullException.ThrowIfNull(query);
        CheckNamespace(query.Namespace);
        CheckTenant(query.Tenant);
        CheckMetadata(query.Metadata);
        CheckLimit(query.Limit);
        return query.Cursor is null ? null : DecodeCursor(query.Cursor);
    }

    /// <summary>Refuses a purge a store can't run.</summary>
    public static void Check(MemoryPurge purge)
    {
        ArgumentNullException.ThrowIfNull(purge);
        if (purge.Namespace is not null)
        {
            CheckNamespace(purge.Namespace);
        }

        if (purge.Tenant is not null)
        {
            CheckTenant(purge.Tenant);
        }

        if (purge.Limit < 1)
        {
            throw new ArgumentException(
                $"the limit is {purge.Limit.ToString(CultureInfo.InvariantCulture)}; it must be at least 1");
        }
    }

    private static void CheckKey(string ns, string tenant, string id)
    {
        CheckNamespace(ns);
        CheckTenant(tenant);
        if (string.IsNullOrEmpty(id))
        {
            throw new ArgumentException("the id is empty");
        }

        CheckLength("id", id);
    }

    private static void CheckNamespace(string ns)
    {
        if (string.IsNullOrEmpty(ns))
        {
            throw new ArgumentException("the namespace is empty");
        }

        CheckLength("namespace", ns);
    }

    private static void CheckTenant(string tenant)
    {
        if (tenant is null)
        {
            throw new ArgumentException("the tenant is null; use \"\" for none");
        }

        CheckLength("tenant", tenant);
    }

    private static void CheckLength(string what, string value)
    {
        if (Encoding.UTF8.GetByteCount(value) > MaxKeyBytes)
        {
            throw new ArgumentException($"the {what} is longer than {MaxKeyBytes} bytes");
        }
    }

    private static void CheckEmbedding(ReadOnlySpan<float> vector, string model, int dimensions)
    {
        if (string.IsNullOrEmpty(model))
        {
            throw new ArgumentException("the model is empty");
        }

        if (vector.Length != dimensions)
        {
            throw new ArgumentException(
                $"the vector has {vector.Length.ToString(CultureInfo.InvariantCulture)} dimensions, and this store has {dimensions.ToString(CultureInfo.InvariantCulture)}");
        }

        var zeros = true;
        foreach (var value in vector)
        {
            if (!float.IsFinite(value))
            {
                throw new ArgumentException("the vector has a value that isn't finite");
            }

            zeros &= value == 0;
        }

        if (zeros)
        {
            // Cosine is undefined for it: every similarity would be NaN.
            throw new ArgumentException("the vector is all zeros");
        }
    }

    /// <summary>
    /// Refuses metadata no record can have: an empty key, a key longer than <see cref="MaxMetadataKeyBytes"/>, a
    /// null value. Part of every check that takes metadata; public for a caller that checks a record's key and
    /// metadata before it has its vector (the memory helper).
    /// </summary>
    public static void CheckMetadata(IReadOnlyDictionary<string, string> metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        foreach (var (key, value) in metadata)
        {
            if (string.IsNullOrEmpty(key))
            {
                throw new ArgumentException("a metadata key is empty");
            }

            if (Encoding.UTF8.GetByteCount(key) > MaxMetadataKeyBytes)
            {
                throw new ArgumentException($"the metadata key '{key}' is longer than {MaxMetadataKeyBytes} bytes");
            }

            if (value is null)
            {
                throw new ArgumentException($"the metadata key '{key}' has no value");
            }
        }
    }

    private static void CheckLimit(int limit)
    {
        if (limit is < 1 or > MaxLimit)
        {
            throw new ArgumentException(
                $"the limit is {limit.ToString(CultureInfo.InvariantCulture)}; it must be from 1 to {MaxLimit}");
        }
    }

    /// <summary>
    /// The cursor of a listing that continues after <paramref name="id"/>: base64url (no padding) of the JSON
    /// array <c>[id]</c>, as <see cref="ThreadCursor"/> encodes its keyset, so a cursor from either engine decodes
    /// in the other. Opaque to callers.
    /// </summary>
    public static string EncodeCursor(string id) =>
        Base64Url.EncodeToString(JsonSerializer.SerializeToUtf8Bytes(new[] { id }, Compact));

    /// <summary>The id a cursor continues after.</summary>
    /// <exception cref="ArgumentException">It isn't a memory listing cursor.</exception>
    public static string DecodeCursor(string cursor)
    {
        try
        {
            if (JsonSerializer.Deserialize<string[]>(Base64Url.DecodeFromChars(cursor)) is [{ } id])
            {
                return id;
            }
        }
        catch (Exception e) when (e is FormatException or JsonException)
        {
            // Refused below, as a cursor of the wrong shape is.
        }

        throw new ArgumentException($"'{cursor}' is not a memory listing cursor");
    }

    /// <summary>
    /// Strings in the order of their UTF-8 bytes, which is Unicode code point order: what every memory store orders
    /// ids by (SQLite's <c>BINARY</c> collation, Redis's <c>ZRANGEBYLEX</c>, Postgres's <c>COLLATE "C"</c>, Rust's
    /// <c>str</c>). <see cref="StringComparer.Ordinal"/> differs from it only between a surrogate pair and a
    /// character from U+E000 to U+FFFF.
    /// </summary>
    public static IComparer<string> ByteOrder { get; } = new Utf8ByteOrder();

    /// <summary>
    /// Cosine similarity in double precision: the dot product over the product of the norms, from −1 to 1. Both
    /// vectors are the same length and not all zeros (<see cref="Check(MemoryRecord, int)"/>).
    /// </summary>
    public static double CosineSimilarity(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        if (a.Length != b.Length)
        {
            throw new ArgumentException("the vectors have different lengths");
        }

        double dot = 0, normA = 0, normB = 0;
        for (var i = 0; i < a.Length; i++)
        {
            dot += (double)a[i] * b[i];
            normA += (double)a[i] * a[i];
            normB += (double)b[i] * b[i];
        }

        return dot / (Math.Sqrt(normA) * Math.Sqrt(normB));
    }

    /// <summary>Whether <paramref name="metadata"/> has every pair of <paramref name="filter"/>.</summary>
    public static bool Matches(IReadOnlyDictionary<string, string> metadata, IReadOnlyDictionary<string, string> filter)
    {
        foreach (var (key, value) in filter)
        {
            if (!metadata.TryGetValue(key, out var actual) || !string.Equals(actual, value, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary><paramref name="time"/> as a store writes it: UTC, whole seconds (the fraction dropped).</summary>
    public static DateTimeOffset Truncate(DateTimeOffset time)
    {
        var utc = time.ToUniversalTime();
        return new DateTimeOffset(utc.Ticks - (utc.Ticks % TimeSpan.TicksPerSecond), TimeSpan.Zero);
    }

    /// <summary><paramref name="time"/> as stored text: RFC 3339, UTC, whole seconds (<c>2026-10-21T14:02:11Z</c>), as checkpoints' timestamps.</summary>
    public static string TimestampText(DateTimeOffset time) => CheckpointTimestamps.Text(time);

    /// <summary>Parses a stored timestamp.</summary>
    public static DateTimeOffset ParseTimestamp(string text) =>
        DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var time)
            ? time.ToUniversalTime()
            : throw new FormatException($"bad timestamp '{text}' in a memory record");

    /// <summary>
    /// <paramref name="record"/>'s JSON form (<c>schema/memory-record.schema.json</c>). A timestamp the record
    /// lacks is left out, so a record that was never stored doesn't validate against the schema.
    /// </summary>
    public static JsonObject ToJson(MemoryRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        var vector = new JsonArray();
        foreach (var value in record.Vector.Span)
        {
            vector.Add(JsonValue.Create(value));
        }

        var metadata = new JsonObject();
        foreach (var (key, value) in record.Metadata.OrderBy(pair => pair.Key, ByteOrder))
        {
            metadata[key] = value;
        }

        var json = new JsonObject
        {
            ["schema_version"] = SchemaVersion,
            ["namespace"] = record.Namespace,
            ["tenant"] = record.Tenant,
            ["id"] = record.Id,
            ["embedding"] = new JsonObject { ["model"] = record.Model, ["vector"] = vector },
            ["content"] = record.Content?.DeepClone(),
            ["text"] = record.Text,
            ["metadata"] = metadata,
        };
        if (record.CreatedAt is { } created)
        {
            json["created_at"] = TimestampText(created);
        }

        if (record.UpdatedAt is { } updated)
        {
            json["updated_at"] = TimestampText(updated);
        }

        return json;
    }

    /// <summary>
    /// A record from its JSON form. <c>schema_version</c> and the timestamps may be left out, and
    /// <c>tenant</c>, <c>content</c>, <c>text</c> and <c>metadata</c> default to <c>""</c>, none, none and empty.
    /// </summary>
    /// <exception cref="FormatException">It isn't a memory record, or is of a schema version this build doesn't know.</exception>
    public static MemoryRecord FromJson(JsonNode json)
    {
        ArgumentNullException.ThrowIfNull(json);
        try
        {
            var version = json["schema_version"]?.GetValue<string>();
            if (version is not null && version != SchemaVersion)
            {
                throw new FormatException($"memory record schema version {version} is not one this build knows ({SchemaVersion})");
            }

            var embedding = Required(json, "embedding");
            var vector = Required(embedding, "vector").AsArray().Select(value => Required(value).GetValue<float>()).ToArray();
            return new MemoryRecord(
                Required(json, "namespace").GetValue<string>(),
                json["tenant"]?.GetValue<string>() ?? "",
                Required(json, "id").GetValue<string>(),
                vector,
                Required(embedding, "model").GetValue<string>())
            {
                Content = json["content"] is JsonObject content ? content.DeepClone().AsObject() : null,
                Text = json["text"]?.GetValue<string>(),
                Metadata = json["metadata"] is JsonObject metadata
                    ? metadata.ToDictionary(pair => pair.Key, pair => Required(pair.Value).GetValue<string>(), StringComparer.Ordinal)
                    : NoMetadata,
                CreatedAt = json["created_at"] is { } created ? ParseTimestamp(created.GetValue<string>()) : null,
                UpdatedAt = json["updated_at"] is { } updated ? ParseTimestamp(updated.GetValue<string>()) : null,
            };
        }
        catch (Exception e) when (e is InvalidOperationException or ArgumentException)
        {
            throw new FormatException($"not a memory record: {e.Message}", e);
        }
    }

    private static JsonNode Required(JsonNode parent, string name) =>
        parent[name] ?? throw new FormatException($"a memory record has no '{name}'");

    private static JsonNode Required(JsonNode? value) =>
        value ?? throw new FormatException("a memory record has a null where a value belongs");

    private sealed class Utf8ByteOrder : IComparer<string>
    {
        public int Compare(string? x, string? y)
        {
            if (x is null || y is null)
            {
                return x is null ? (y is null ? 0 : -1) : 1;
            }

            var length = Math.Min(x.Length, y.Length);
            for (var i = 0; i < length; i++)
            {
                if (x[i] != y[i])
                {
                    return Rank(x[i]) - Rank(y[i]);
                }
            }

            return x.Length - y.Length;
        }

        /// <summary>A UTF-16 code unit moved so that surrogates sort after every other character, as their code points do.</summary>
        private static int Rank(char c) => c switch
        {
            >= '\uD800' and <= '\uDFFF' => c + 0x2000,
            >= '\uE000' => c - 0x800,
            _ => c,
        };
    }
}
