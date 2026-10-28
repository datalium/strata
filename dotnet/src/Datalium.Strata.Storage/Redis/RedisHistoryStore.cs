using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using Datalium.Strata.Memory;
using StackExchange.Redis;

namespace Datalium.Strata.Storage.Redis;

/// <summary>
/// Redis <see cref="IHistoryStore"/> adapter. Each session is one Redis <c>LIST</c> at
/// <c>{prefix}:history:{session_id}</c>; <see cref="AppendAsync"/> <c>RPUSH</c>es one JSON
/// <c>{"role","content","timestamp"}</c> element and <see cref="GetHistoryAsync"/> is
/// <c>LRANGE key -limit -1</c> -- the "latest N, oldest first" the interface asks for, with no sort. A
/// message's sequence is its position in the list, so it is not stored. Same keys and JSON as
/// <c>strata-memory</c>'s <c>RedisHistoryStore</c>, so either runtime reads what the other wrote.
///
/// A read is repeated after a transient failure (<c>design/34-Storage-Hardening.md</c>); an append only when
/// it never reached the server, since the store can't tell a message that was written from one that wasn't.
///
/// Not built: expiry/eviction (a list grows until you delete it), Redis Cluster/Sentinel, and TLS beyond
/// what StackExchange.Redis offers through your own <see cref="IDatabase"/>.
/// </summary>
public sealed class RedisHistoryStore : IHistoryStore, IDisposable
{
    private readonly IDatabase _database;
    private readonly string _prefix;
    private readonly StoreRetrier _retry;
    private readonly ConnectionMultiplexer? _ownedConnection;

    /// <param name="database">An existing database handle, e.g. from your application's shared multiplexer.</param>
    /// <param name="prefix">Key namespace; keys are <c>{prefix}:history:…</c>.</param>
    public RedisHistoryStore(IDatabase database, string prefix = RedisConnection.DefaultPrefix)
        : this(database, new RedisStoreOptions { Prefix = prefix })
    {
    }

    /// <summary>A store over <paramref name="database"/> with <paramref name="options"/>' prefix and retries.</summary>
    public RedisHistoryStore(IDatabase database, RedisStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(options);
        _database = database;
        _prefix = options.Prefix;
        _retry = new StoreRetrier(options.Retry, options.TimeProvider, "redis", RedisFaults.IsTransient);
    }

    private RedisHistoryStore(ConnectionMultiplexer connection, RedisStoreOptions options)
        : this(connection.GetDatabase(), options)
    {
        _ownedConnection = connection;
    }

    /// <summary>Connects to <paramref name="url"/> (see <see cref="RedisConnection.Connect"/>) and owns the connection.</summary>
    public static RedisHistoryStore Connect(string url, string prefix = RedisConnection.DefaultPrefix) =>
        new(RedisConnection.Connect(url), new RedisStoreOptions { Prefix = prefix });

    /// <summary><see cref="Connect"/> without blocking a thread on the connection, and with <paramref name="options"/>.</summary>
    public static async Task<RedisHistoryStore> ConnectAsync(string url, RedisStoreOptions? options = null) =>
        new(await RedisConnection.ConnectAsync(url).ConfigureAwait(false), options ?? new RedisStoreOptions());

    private RedisKey Key(string sessionId) => $"{_prefix}:history:{sessionId}";

    public async Task AppendAsync(string sessionId, ChatMessage message, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var json = new JsonObject
        {
            ["role"] = RoleToString(message.Role),
            ["content"] = message.Content.ToString(),
            ["timestamp"] = message.Timestamp.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
        }.ToJsonString();

        await _retry.RunAsync(
                "append",
                (_, _) => new ValueTask<long>(_database.ListRightPushAsync(Key(sessionId), json)),
                ct,
                unless: failure => !RedisFaults.NeverSent(failure))
            .ConfigureAwait(false);
    }

    public async IAsyncEnumerable<ChatMessage> GetHistoryAsync(
        string sessionId,
        int limit = 50,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // `LRANGE key -0 -1` would be the whole list, not nothing.
        if (limit <= 0)
        {
            yield break;
        }

        ct.ThrowIfCancellationRequested();
        var elements = await _retry.RunAsync(
                "load", (_, _) => new ValueTask<RedisValue[]>(_database.ListRangeAsync(Key(sessionId), -limit, -1)), ct)
            .ConfigureAwait(false);
        foreach (var element in elements)
        {
            var json = JsonNode.Parse((string)element!)!;
            yield return new ChatMessage(
                RoleFromString(json["role"]!.GetValue<string>()),
                json["content"]!.GetValue<string>().AsMemory(),
                DateTimeOffset.Parse(
                    json["timestamp"]!.GetValue<string>(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind));
        }
    }

    public void Dispose() => _ownedConnection?.Dispose();

    private static string RoleToString(MessageRole role) => role switch
    {
        MessageRole.System => "system",
        MessageRole.User => "user",
        MessageRole.Assistant => "assistant",
        MessageRole.Tool => "tool",
        _ => throw new ArgumentOutOfRangeException(nameof(role), role, "unknown message role"),
    };

    private static MessageRole RoleFromString(string role) => role switch
    {
        "system" => MessageRole.System,
        "user" => MessageRole.User,
        "assistant" => MessageRole.Assistant,
        "tool" => MessageRole.Tool,
        _ => throw new InvalidOperationException($"unknown role '{role}' in history store"),
    };
}
