using System.Runtime.CompilerServices;
using Datalium.Strata.Memory;

namespace Datalium.Strata.Storage.Memory;

/// <summary>
/// In-memory <see cref="IHistoryStore"/> adapter — the "in-memory ring buffer" tier from
/// design/03-Architecture.md's memory blueprint, matching <see cref="InMemoryCheckpointStore"/>'s
/// role for checkpoints. A plain per-session <see cref="List{T}"/> guarded by a lock; returned
/// <see cref="ChatMessage"/> values are value-type copies of the record itself, but
/// <see cref="ChatMessage.Content"/> stays a slice over the original string's backing array —
/// no string is ever recopied (see <c>HistoryStoreContractTests</c>'s zero-copy test).
/// </summary>
public sealed class InMemoryHistoryStore : IHistoryStore
{
    private readonly Dictionary<string, List<ChatMessage>> _sessions = [];
    private readonly object _gate = new();

    public Task AppendAsync(string sessionId, ChatMessage message, CancellationToken ct = default)
    {
        lock (_gate)
        {
            if (!_sessions.TryGetValue(sessionId, out var messages))
            {
                messages = [];
                _sessions[sessionId] = messages;
            }

            messages.Add(message);
        }

        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<ChatMessage> GetHistoryAsync(
        string sessionId,
        int limit = 50,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        List<ChatMessage> snapshot;
        lock (_gate)
        {
            snapshot = _sessions.TryGetValue(sessionId, out var messages)
                ? messages.Skip(Math.Max(0, messages.Count - limit)).ToList()
                : [];
        }

        foreach (var message in snapshot)
        {
            ct.ThrowIfCancellationRequested();
            yield return message;
        }
    }
}
