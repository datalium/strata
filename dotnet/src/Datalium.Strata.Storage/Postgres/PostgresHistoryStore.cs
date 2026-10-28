using System.Globalization;
using System.Runtime.CompilerServices;
using Datalium.Strata.Memory;
using Npgsql;
using NpgsqlTypes;

namespace Datalium.Strata.Storage.Postgres;

/// <summary>
/// Postgres <see cref="IHistoryStore"/> adapter: one <c>history_records</c> row per message, ordered by a
/// per-session <c>sequence</c>, safe for concurrent appenders (a per-session advisory lock serializes the
/// choice of the next sequence). The table is the same one <c>strata-memory</c>'s
/// <c>PostgresHistoryStore</c> creates, with lowercase roles and an RFC 3339 <c>TEXT</c> timestamp, so
/// either runtime reads what the other wrote. (The SQLite history stores are not cross-runtime: the EF
/// Core one stores the role as <c>"User"</c>, not <c>"user"</c>.)
///
/// Postgres <c>TEXT</c> cannot hold a NUL character, so a message containing one is rejected by the
/// server.
///
/// No constructor touches the database (<c>design/34-Storage-Hardening.md</c>): the table is created the
/// first time the store is used, or by <see cref="MigrateAsync"/> or
/// <see cref="OpenAsync(string, PostgresStoreOptions?, CancellationToken)"/>. A read is repeated after a
/// transient failure; an append is repeated only when the failure came before its commit, since the store
/// can't tell a message that was written from one that wasn't.
///
/// Not built: TLS beyond what the connection string asks of Npgsql (pass your own
/// <see cref="NpgsqlDataSource"/>).
/// </summary>
public sealed class PostgresHistoryStore : IHistoryStore, IDisposable, IAsyncDisposable
{
    /// <summary>The Postgres <c>history_records</c> table. Identical to <c>strata-memory</c>'s <c>POSTGRES_HISTORY_DDL</c>.</summary>
    public const string HistoryLogDdl = """
        CREATE TABLE IF NOT EXISTS history_records (
            session_id TEXT NOT NULL,
            sequence   BIGINT NOT NULL,
            role       TEXT NOT NULL,
            content    TEXT NOT NULL,
            timestamp  TEXT NOT NULL,
            PRIMARY KEY (session_id, sequence)
        )
        """;

    private readonly NpgsqlDataSource _dataSource;
    private readonly bool _ownsDataSource;
    private readonly PostgresStoreOptions _options;
    private readonly StoreRetrier _retry;
    private readonly PostgresPreparation _preparation;

    /// <summary>
    /// A store over <paramref name="connectionString"/> (an Npgsql connection string or a <c>postgres://</c>
    /// URL), which owns its connection pool. Nothing is connected until the store is used.
    /// </summary>
    public PostgresHistoryStore(string connectionString, PostgresStoreOptions? options = null)
        : this(PostgresConnection.CreateDataSource(connectionString), options, ownsDataSource: true)
    {
    }

    /// <param name="dataSource">A pooled data source -- the way to configure TLS or a custom <c>Search Path</c>.</param>
    /// <param name="options">Retries and migration; the defaults when <see langword="null"/>.</param>
    /// <param name="ownsDataSource">Whether <see cref="Dispose"/> should dispose <paramref name="dataSource"/>.</param>
    public PostgresHistoryStore(NpgsqlDataSource dataSource, PostgresStoreOptions? options = null, bool ownsDataSource = false)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
        _ownsDataSource = ownsDataSource;
        _options = options ?? new PostgresStoreOptions();
        _retry = new StoreRetrier(_options.Retry, _options.TimeProvider, "postgres", PostgresFaults.IsTransient);
        _preparation = new PostgresPreparation(
            _dataSource,
            _options,
            _retry,
            StoreSchemaVersions.History,
            StoreSchemaVersions.HistoryVersion,
            (connection, transaction, token) => PostgresSchema.ExecuteAsync(connection, transaction, HistoryLogDdl, token),
            steps: []);
    }

    /// <summary>A store over <paramref name="connectionString"/> whose table has been created: the way to find out at startup that the database can't be reached. Owns its pool.</summary>
    public static async Task<PostgresHistoryStore> OpenAsync(
        string connectionString, PostgresStoreOptions? options = null, CancellationToken cancellationToken = default)
    {
        var store = new PostgresHistoryStore(connectionString, options);
        try
        {
            await store.MigrateAsync(cancellationToken).ConfigureAwait(false);
            return store;
        }
        catch
        {
            await store.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Creates the store's table, whatever <see cref="PostgresStoreOptions.AutoMigrate"/> says
    /// (<c>design/34-Storage-Hardening.md</c>, "Migrations"). Safe to call at any time.
    /// </summary>
    /// <exception cref="InvalidOperationException">The database is newer than this build.</exception>
    public async Task MigrateAsync(CancellationToken cancellationToken = default) =>
        await _preparation.EnsureAsync(migrate: true, cancellationToken).ConfigureAwait(false);

    public async Task AppendAsync(string sessionId, ChatMessage message, CancellationToken ct = default)
    {
        await _preparation.EnsureAsync(migrate: false, ct).ConfigureAwait(false);

        // An append whose commit failed may have been written, and a message can't be told from another like
        // it: that failure is the call's. A failure before the commit is repeated.
        var committing = false;
        await _retry.RunAsync(
                "append",
                (_, token) =>
                {
                    committing = false;
                    return AppendOnceAsync(sessionId, message, () => committing = true, token);
                },
                ct,
                unless: _ => committing)
            .ConfigureAwait(false);
    }

    private async ValueTask AppendOnceAsync(string sessionId, ChatMessage message, Action committing, CancellationToken ct)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);

        await PostgresSchema.LockAsync(connection, transaction, "history", sessionId, ct);

        await using var insert = new NpgsqlCommand(
            """
            INSERT INTO history_records (session_id, sequence, role, content, timestamp)
            VALUES (
                @session_id,
                (SELECT COALESCE(MAX(sequence), -1) + 1 FROM history_records WHERE session_id = @session_id),
                @role, @content, @timestamp)
            """,
            connection,
            transaction);
        insert.Parameters.Add(PostgresSchema.Param("session_id", NpgsqlDbType.Text, sessionId));
        insert.Parameters.Add(PostgresSchema.Param("role", NpgsqlDbType.Text, RoleToString(message.Role)));
        insert.Parameters.Add(PostgresSchema.Param("content", NpgsqlDbType.Text, message.Content.ToString()));
        insert.Parameters.Add(PostgresSchema.Param(
            "timestamp", NpgsqlDbType.Text, message.Timestamp.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture)));
        await insert.ExecuteNonQueryAsync(ct);

        committing();
        await transaction.CommitAsync(ct);
        if (_options.Fault is { } fault)
        {
            await fault("append:after-commit").ConfigureAwait(false);
        }
    }

    public async IAsyncEnumerable<ChatMessage> GetHistoryAsync(
        string sessionId,
        int limit = 50,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (limit <= 0)
        {
            yield break;
        }

        await _preparation.EnsureAsync(migrate: false, ct).ConfigureAwait(false);
        var rows = await _retry.RunAsync(
                "load",
                async (_, token) =>
                {
                    var read = new List<(string Role, string Content, string Timestamp)>();
                    await using var connection = await _dataSource.OpenConnectionAsync(token).ConfigureAwait(false);
                    await using var command = new NpgsqlCommand(
                        """
                        SELECT role, content, timestamp FROM history_records
                        WHERE session_id = @session_id ORDER BY sequence DESC LIMIT @limit
                        """,
                        connection);
                    command.Parameters.Add(PostgresSchema.Param("session_id", NpgsqlDbType.Text, sessionId));
                    command.Parameters.Add(PostgresSchema.Param("limit", NpgsqlDbType.Bigint, (long)limit));

                    await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                    while (await reader.ReadAsync(token).ConfigureAwait(false))
                    {
                        read.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
                    }

                    return read;
                },
                ct)
            .ConfigureAwait(false);

        // Queried DESC (to LIMIT to the most recent rows); callers want chronological order back.
        rows.Reverse();
        foreach (var (role, content, timestamp) in rows)
        {
            yield return new ChatMessage(
                RoleFromString(role),
                content.AsMemory(),
                DateTimeOffset.Parse(timestamp, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind));
        }
    }

    public void Dispose()
    {
        _preparation.Dispose();
        if (_ownsDataSource)
        {
            _dataSource.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _preparation.Dispose();
        if (_ownsDataSource)
        {
            await _dataSource.DisposeAsync().ConfigureAwait(false);
        }
    }

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
