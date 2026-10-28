using System.Text.Json.Serialization;

namespace Datalium.Strata.Storage.Schema;

/// <summary>
/// Types for <c>schema/history-record.schema.json</c>. Must stay in lockstep with the
/// JSON Schema file and with the Rust equivalent in <c>rust/strata-memory/src/schema.rs</c>.
/// See <c>schema/README.md</c>.
/// </summary>
public enum MessageRole
{
    [JsonStringEnumMemberName("system")]
    System,

    [JsonStringEnumMemberName("user")]
    User,

    [JsonStringEnumMemberName("assistant")]
    Assistant,

    [JsonStringEnumMemberName("tool")]
    Tool,
}

public record HistoryRecord
{
    [JsonPropertyName("schema_version")]
    public required string SchemaVersion { get; init; }

    [JsonPropertyName("session_id")]
    public required string SessionId { get; init; }

    [JsonPropertyName("sequence")]
    public required ulong Sequence { get; init; }

    [JsonPropertyName("role")]
    [JsonConverter(typeof(JsonStringEnumConverter<MessageRole>))]
    public required MessageRole Role { get; init; }

    [JsonPropertyName("content")]
    public required string Content { get; init; }

    [JsonPropertyName("timestamp")]
    public required string Timestamp { get; init; }
}
