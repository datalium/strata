namespace Datalium.Strata.Memory;

/// <summary>
/// The persistence port for chat/session history (design/03-Architecture.md, CLAUDE.md item 6).
/// Concrete adapters (in-memory, then SQLite via EF Core) live in <c>Datalium.Strata.Storage</c>,
/// which depends on this project for the interface — not the other way around, mirroring
/// <see cref="Interruption.ICheckpointStore"/>.
/// </summary>
public interface IHistoryStore
{
    Task AppendAsync(string sessionId, ChatMessage message, CancellationToken ct = default);

    /// <summary>
    /// Streams up to <paramref name="limit"/> of the most recent messages for
    /// <paramref name="sessionId"/>, oldest first — the shape a caller needs to feed straight
    /// into an LLM context window. Returning <see cref="IAsyncEnumerable{T}"/> (matching
    /// design/03-Architecture.md's own interface exactly) rather than a materialized list lets
    /// a store stream sequentially from disk instead of loading everything into memory at once.
    /// </summary>
    IAsyncEnumerable<ChatMessage> GetHistoryAsync(string sessionId, int limit = 50, CancellationToken ct = default);
}
