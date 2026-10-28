using System.Buffers.Binary;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Memory;

namespace Datalium.Strata.Storage;

/// <summary>
/// The stored forms the SQLite and Redis memory stores share (<c>design/45-Long-Term-Memory.md</c>): the vector as
/// little-endian float32 bytes (sqlite-vec's own format, and the Query Engine's <c>FLOAT32</c>), content and
/// metadata as compact JSON text with metadata keys in byte order, and byte order itself. Mirrors what
/// <c>strata-memory</c> writes, so either runtime reads the other's rows and hashes.
/// </summary>
internal static class MemoryLayout
{
    /// <summary>Compact JSON that leaves non-ASCII text as it is, as <c>serde_json</c> writes it.</summary>
    private static readonly JsonSerializerOptions Compact = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    private static readonly JsonWriterOptions CompactWriter = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    /// <summary><paramref name="vector"/> as little-endian float32 bytes.</summary>
    public static byte[] VectorBytes(ReadOnlySpan<float> vector)
    {
        var bytes = new byte[vector.Length * sizeof(float)];
        for (var i = 0; i < vector.Length; i++)
        {
            BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(i * sizeof(float)), vector[i]);
        }

        return bytes;
    }

    /// <summary>A vector from its little-endian float32 bytes.</summary>
    /// <exception cref="FormatException">The bytes aren't a whole number of floats.</exception>
    public static float[] Vector(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length % sizeof(float) != 0)
        {
            throw new FormatException($"a stored vector of {bytes.Length} bytes isn't float32s");
        }

        var vector = new float[bytes.Length / sizeof(float)];
        for (var i = 0; i < vector.Length; i++)
        {
            vector[i] = BinaryPrimitives.ReadSingleLittleEndian(bytes[(i * sizeof(float))..]);
        }

        return vector;
    }

    /// <summary>Content as stored: compact JSON text, keys in the order given, or <see langword="null"/>.</summary>
    public static string? ContentJson(JsonObject? content) => content?.ToJsonString(Compact);

    /// <summary>Metadata as stored: a compact JSON object, keys in byte order; <c>{}</c> when empty.</summary>
    public static string MetadataJson(IReadOnlyDictionary<string, string> metadata)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer, CompactWriter))
        {
            writer.WriteStartObject();
            foreach (var (key, value) in metadata.OrderBy(pair => pair.Key, MemoryRecords.ByteOrder))
            {
                writer.WriteString(key, value);
            }

            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length);
    }

    /// <summary>Stored content back as an object.</summary>
    public static JsonObject? ParseContent(string? json) => json is null ? null : JsonNode.Parse(json)?.AsObject();

    /// <summary>Stored metadata back as a map.</summary>
    public static IReadOnlyDictionary<string, string> ParseMetadata(string json) =>
        JsonNode.Parse(json)!.AsObject().ToDictionary(pair => pair.Key, pair => pair.Value!.GetValue<string>(), StringComparer.Ordinal);
}
