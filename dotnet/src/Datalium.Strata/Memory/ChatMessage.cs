namespace Datalium.Strata.Memory;

/// <summary>
/// Zero-copy per design/03-Architecture.md: <see cref="Content"/> is a
/// <see cref="ReadOnlyMemory{T}"/> slice, not a fresh string copy, so an <see cref="IHistoryStore"/>
/// can hand back messages it already holds without allocating (see
/// <c>Datalium.Strata.Tests.Memory.HistoryStoreContractTests</c>'s zero-copy test).
///
/// Uses <see cref="DateTimeOffset"/> rather than the design doc's literal
/// <c>DateTime</c> — <c>DateTime</c>'s ambiguous kind (local/UTC/unspecified) is a well-known
/// footgun for timestamps that cross store boundaries; <c>DateTimeOffset</c> is unambiguous
/// and is the closer match to Rust's <c>DateTime&lt;Utc&gt;</c> anyway.
/// </summary>
public readonly record struct ChatMessage(MessageRole Role, ReadOnlyMemory<char> Content, DateTimeOffset Timestamp);
