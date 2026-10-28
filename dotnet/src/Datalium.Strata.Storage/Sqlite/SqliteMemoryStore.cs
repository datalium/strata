using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Datalium.Strata.Memory;
using Microsoft.Data.Sqlite;

namespace Datalium.Strata.Storage.Sqlite;

/// <summary>
/// Options of <see cref="SqliteMemoryStore"/> (<c>design/45-Long-Term-Memory.md</c>, "SQLite and sqlite-vec").
/// Mirrors <c>strata-memory</c>'s <c>SqliteMemoryOptions</c>.
/// </summary>
public sealed record SqliteMemoryOptions
{
    /// <summary>
    /// The length of every vector: 1 to <see cref="MemoryRecords.MaxDimensions"/>. The first store on a table
    /// records them in <c>memory_settings</c>, and a store opened on a table made with others is refused.
    /// </summary>
    public required int Dimensions { get; init; }

    /// <summary>
    /// The table: lowercase letters, digits and underscores, not starting with a digit, at most 63 characters (no
    /// schema part in SQLite). Its indexes are named after it. An application with two embedding sizes uses two
    /// stores on two tables.
    /// </summary>
    public string Table { get; init; } = "memory_records";

    /// <summary>
    /// The sqlite-vec extension, which the store loads into each connection it opens with
    /// <see cref="SqliteConnection.LoadExtension(string, string?)"/>: a path to the library, or a file name that
    /// SQLite and Microsoft.Data.Sqlite look for in the application's native library directories (its
    /// <c>runtimes/&lt;rid&gt;/native/</c> folders) and the platform's search path. <c>vec0</c> (the default) is
    /// what the <c>sqlite-vec</c> NuGet package ships. <c>Datalium.Strata.Storage</c> doesn't reference that
    /// package: the application does (with <c>ExcludeAssets="contentFiles"</c>, since the package also adds a
    /// public <c>Microsoft.Data.Sqlite.SqliteVectorExtensions</c> class to any project that references it), or it
    /// ships sqlite-vec's library itself and gives its path here. Only <c>vec_distance_cosine</c> is used, and the
    /// rows are readable without it: the vectors are plain little-endian float32 BLOBs.
    /// </summary>
    public string Extension { get; init; } = "vec0";

    /// <summary>The clock for the records' timestamps. <see langword="null"/>: <see cref="System.TimeProvider.System"/>.</summary>
    public TimeProvider? TimeProvider { get; init; }
}

/// <summary>
/// SQLite <see cref="IMemoryStore"/> with sqlite-vec (<c>design/45-Long-Term-Memory.md</c>, "SQLite and
/// sqlite-vec"): one row per record in an ordinary table, the vector a BLOB of little-endian float32, searched
/// exactly with sqlite-vec's scalar <c>vec_distance_cosine</c>, every filter a plain <c>WHERE</c> clause applied
/// before ranking. The layout is the one <c>strata-memory</c>'s <c>SqliteMemoryStore</c> makes, pinned by
/// <c>schema/fixtures/memory/sqlite-columns.json</c> and <c>sqlite-settings-columns.json</c>, so either runtime
/// reads what the other wrote, and a file may be shared with the SQLite checkpoint store.
///
/// The store keeps one connection, opened on first use, and runs its calls on it one at a time. Nothing touches
/// the file in the constructor: the first call (or <see cref="MigrateAsync"/>) opens it, loads the extension
/// (refusing with <c>memory on SQLite needs the sqlite-vec extension: …</c> when it can't), and in one
/// transaction records the <c>memory</c> component at version 1 in <c>strata_schema_version</c>, creates the
/// table, its indexes and <c>memory_settings</c>, and refuses a newer database or a table made with other
/// dimensions. A SQLite store always migrates; it has no retries (<c>design/34-Storage-Hardening.md</c>). A
/// failure is a <see cref="MemoryStoreException"/>.
///
/// Telemetry on the <see cref="StorageTelemetry.Name"/> meter: <c>strata.memory.operations</c>,
/// <c>strata.memory.operation.duration</c> and <c>strata.memory.search.results</c>, with <c>strata.store</c>
/// <c>sqlite</c>.
/// </summary>
public sealed partial class SqliteMemoryStore : IMemoryStore, IMemoryRetention, IDisposable, IAsyncDisposable
{
    private const string StoreName = "sqlite";

    /// <summary>The table that records each memory table's dimensions.</summary>
    internal const string SettingsTable = "memory_settings";

    /// <summary>The columns every read selects, in this order.</summary>
    private const string Columns = "namespace, tenant, id, model, embedding, content, text, metadata, created_at, updated_at";

    private readonly string _connectionString;
    private readonly SqliteMemoryOptions _options;
    private readonly TimeProvider _clock;
    private readonly string _table;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private SqliteConnection? _connection;
    private bool _disposed;

    /// <param name="dataSource">
    /// A file path, <c>:memory:</c> (a database that lives as long as the store), or a Microsoft.Data.Sqlite
    /// connection string (one that names <c>Data Source</c> or <c>Filename</c>). A path is opened without
    /// connection pooling, so disposing the store releases the file.
    /// </param>
    /// <param name="options">The dimensions, the table, the extension and the clock.</param>
    public SqliteMemoryStore(string dataSource, SqliteMemoryOptions options)
    {
        ArgumentException.ThrowIfNullOrEmpty(dataSource);
        ArgumentNullException.ThrowIfNull(options);
        Dimensions = MemoryRecords.CheckDimensions(options.Dimensions);
        if (options.Table is null || !TableName().IsMatch(options.Table))
        {
            throw new ArgumentException($"the memory table '{options.Table}' isn't a lowercase SQL name", nameof(options));
        }

        ArgumentException.ThrowIfNullOrEmpty(options.Extension, nameof(options));
        _connectionString = ConnectionString(dataSource);
        _options = options;
        _table = options.Table;
        _clock = options.TimeProvider ?? TimeProvider.System;
    }

    /// <summary>A store over a database in memory that lives as long as the store: for tests.</summary>
    public static SqliteMemoryStore CreateInMemory(SqliteMemoryOptions options) => new(":memory:", options);

    /// <summary>
    /// A store over <paramref name="dataSource"/> whose table has been made and checked: the way to find out at
    /// startup that the extension is missing, the file is newer than this build or holds vectors of another size.
    /// </summary>
    public static async Task<SqliteMemoryStore> OpenAsync(
        string dataSource, SqliteMemoryOptions options, CancellationToken cancellationToken = default)
    {
        var store = new SqliteMemoryStore(dataSource, options);
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

    /// <inheritdoc/>
    public int Dimensions { get; }

    /// <summary>
    /// Opens the database if the store hasn't yet: loads the extension, creates the table, its indexes and the
    /// <c>memory</c> component's row, and checks the table's dimensions. Every call does this first, so calling it
    /// is only a way to fail early.
    /// </summary>
    /// <exception cref="MemoryStoreException">
    /// The extension couldn't be loaded, the database is newer than this build, or the table was made with other dimensions.
    /// </exception>
    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await ConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask UpsertAsync(MemoryRecord record, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(record, Dimensions);
        await WriteAsync([record], cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask UpsertManyAsync(IReadOnlyList<MemoryRecord> records, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(records, Dimensions);
        if (records.Count > 0)
        {
            await WriteAsync(records, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Upserts <paramref name="records"/> in one transaction, all at one time.</summary>
    private ValueTask<bool> WriteAsync(IReadOnlyList<MemoryRecord> records, CancellationToken cancellationToken)
    {
        var now = MemoryRecords.TimestampText(_clock.GetUtcNow());
        var sql = $"""
            INSERT INTO {_table} (namespace, tenant, id, model, embedding, content, text, metadata, created_at, updated_at)
            VALUES ($namespace, $tenant, $id, $model, $embedding, $content, $text, $metadata, $now, $now)
            ON CONFLICT (namespace, tenant, id) DO UPDATE SET
                model = excluded.model,
                embedding = excluded.embedding,
                content = excluded.content,
                text = excluded.text,
                metadata = excluded.metadata,
                updated_at = excluded.updated_at
            """;

        return RunAsync(
            "upsert",
            async (connection, token) =>
            {
                // Microsoft.Data.Sqlite begins a non-deferred transaction with BEGIN IMMEDIATE.
                await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(token).ConfigureAwait(false);
                await using var command = new SqliteCommand(sql, connection, transaction);
                var parameters = new[] { "$namespace", "$tenant", "$id", "$model", "$embedding", "$content", "$text", "$metadata" }
                    .Select(name => command.Parameters.Add(new SqliteParameter { ParameterName = name }))
                    .ToArray();
                command.Parameters.AddWithValue("$now", now);
                foreach (var record in records)
                {
                    parameters[0].Value = record.Namespace;
                    parameters[1].Value = record.Tenant;
                    parameters[2].Value = record.Id;
                    parameters[3].Value = record.Model;
                    parameters[4].Value = MemoryLayout.VectorBytes(record.Vector.Span);
                    parameters[5].Value = (object?)MemoryLayout.ContentJson(record.Content) ?? DBNull.Value;
                    parameters[6].Value = (object?)record.Text ?? DBNull.Value;
                    parameters[7].Value = MemoryLayout.MetadataJson(record.Metadata);
                    await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                }

                await transaction.CommitAsync(token).ConfigureAwait(false);
                return true;
            },
            cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<MemoryRecord?> GetAsync(MemoryKey key, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(key);
        return RunAsync(
            "get",
            async (connection, token) =>
            {
                await using var command = new SqliteCommand(
                    $"SELECT {Columns} FROM {_table} WHERE namespace = $namespace AND tenant = $tenant AND id = $id", connection);
                AddKey(command, key);
                await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                return await reader.ReadAsync(token).ConfigureAwait(false) ? ReadRecord(reader) : null;
            },
            cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<bool> DeleteAsync(MemoryKey key, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(key);
        return RunAsync(
            "delete",
            async (connection, token) =>
            {
                await using var command = new SqliteCommand(
                    $"DELETE FROM {_table} WHERE namespace = $namespace AND tenant = $tenant AND id = $id", connection);
                AddKey(command, key);
                return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false) > 0;
            },
            cancellationToken);
    }

    /// <summary>
    /// The records most similar to <paramref name="search"/>'s vector, exactly: sqlite-vec's
    /// <c>vec_distance_cosine</c> over every row the filters keep (SQL <c>WHERE</c> clauses, so they apply before
    /// ranking), ordered by distance and then id in byte order. <see cref="MemorySearch.MinSimilarity"/> is applied
    /// as the rows are read, which stops at the first below it.
    /// </summary>
    public async ValueTask<IReadOnlyList<MemoryMatch>> SearchAsync(MemorySearch search, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(search, Dimensions);
        var sql = new StringBuilder($"""
            SELECT {Columns}, vec_distance_cosine(embedding, $query) AS distance
            FROM {_table} AS t
            WHERE namespace = $namespace AND tenant = $tenant AND model = $model
              AND id NOT IN (SELECT value FROM json_each($exclude))
            """);
        var pairs = search.Metadata.ToList();
        AppendMetadataFilter(sql, pairs);
        sql.Append("\nORDER BY distance, id\nLIMIT $limit");

        var matches = await RunAsync(
                "search",
                async (connection, token) =>
                {
                    await using var command = new SqliteCommand(sql.ToString(), connection);
                    command.Parameters.AddWithValue("$query", MemoryLayout.VectorBytes(search.Vector.Span));
                    command.Parameters.AddWithValue("$namespace", search.Namespace);
                    command.Parameters.AddWithValue("$tenant", search.Tenant);
                    command.Parameters.AddWithValue("$model", search.Model);
                    command.Parameters.AddWithValue("$exclude", ExcludeJson(search.Exclude));
                    AddMetadataParameters(command, pairs);
                    command.Parameters.AddWithValue("$limit", search.Limit);

                    var found = new List<MemoryMatch>();
                    await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                    while (await reader.ReadAsync(token).ConfigureAwait(false))
                    {
                        var similarity = 1 - reader.GetDouble(10);
                        if (search.MinSimilarity is { } min && similarity < min)
                        {
                            break;
                        }

                        found.Add(new MemoryMatch(ReadRecord(reader), similarity));
                    }

                    return found;
                },
                cancellationToken)
            .ConfigureAwait(false);

        StorageTelemetry.MemorySearched(StoreName, matches.Count);
        return matches;
    }

    /// <inheritdoc/>
    public ValueTask<MemoryPage> ListAsync(MemoryQuery query, CancellationToken cancellationToken = default)
    {
        var after = MemoryRecords.Check(query);
        var sql = new StringBuilder($"SELECT {Columns} FROM {_table} AS t\nWHERE namespace = $namespace AND tenant = $tenant");
        if (after is not null)
        {
            sql.Append(" AND id > $after");
        }

        var pairs = query.Metadata.ToList();
        AppendMetadataFilter(sql, pairs);
        sql.Append("\nORDER BY id\nLIMIT $limit");

        return RunAsync(
            "list",
            async (connection, token) =>
            {
                await using var command = new SqliteCommand(sql.ToString(), connection);
                command.Parameters.AddWithValue("$namespace", query.Namespace);
                command.Parameters.AddWithValue("$tenant", query.Tenant);
                if (after is not null)
                {
                    command.Parameters.AddWithValue("$after", after);
                }

                AddMetadataParameters(command, pairs);

                // One more than a page, to know whether another follows.
                command.Parameters.AddWithValue("$limit", query.Limit + 1);

                var records = new List<MemoryRecord>();
                await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    records.Add(ReadRecord(reader));
                }

                string? next = null;
                if (records.Count > query.Limit)
                {
                    records.RemoveAt(records.Count - 1);
                    next = MemoryRecords.EncodeCursor(records[^1].Id);
                }

                return new MemoryPage(records, next);
            },
            cancellationToken);
    }

    /// <summary>
    /// Deletes the records <paramref name="purge"/> names in one statement: <c>updated_at</c> before its cutoff (at
    /// the second, exclusive, compared as stored text), oldest first, then by namespace, tenant and id.
    /// </summary>
    public ValueTask<int> PurgeAsync(MemoryPurge purge, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(purge);
        var filter = new StringBuilder("updated_at < $cutoff");
        if (purge.Namespace is not null)
        {
            filter.Append(" AND namespace = $namespace");
        }

        if (purge.Tenant is not null)
        {
            filter.Append(" AND tenant = $tenant");
        }

        var sql = $"""
            DELETE FROM {_table} WHERE rowid IN (
                SELECT rowid FROM {_table}
                WHERE {filter}
                ORDER BY updated_at, namespace, tenant, id
                LIMIT $limit)
            """;
        return RunAsync(
            "purge",
            async (connection, token) =>
            {
                await using var command = new SqliteCommand(sql, connection);
                command.Parameters.AddWithValue("$cutoff", MemoryRecords.TimestampText(purge.OlderThan));
                command.Parameters.AddWithValue("$limit", purge.Limit);
                if (purge.Namespace is not null)
                {
                    command.Parameters.AddWithValue("$namespace", purge.Namespace);
                }

                if (purge.Tenant is not null)
                {
                    command.Parameters.AddWithValue("$tenant", purge.Tenant);
                }

                return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            },
            cancellationToken);
    }

    /// <summary>
    /// Makes one call on the store's connection, one at a time: opens and prepares it first if it isn't; counts the
    /// call, and turns a failure into a <see cref="MemoryStoreException"/>.
    /// </summary>
    private async ValueTask<T> RunAsync<T>(
        string operation, Func<SqliteConnection, CancellationToken, Task<T>> call, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var connection = await ConnectionAsync(cancellationToken).ConfigureAwait(false);
                return await call(connection, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _gate.Release();
            }
        }
        catch (Exception e) when (e is not (OperationCanceledException or MemoryStoreException or ObjectDisposedException))
        {
            throw new MemoryStoreException($"memory {operation} failed: {e.Message}", e);
        }
        finally
        {
            StorageTelemetry.MemoryOperation(StoreName, operation, Stopwatch.GetElapsedTime(started));
        }
    }

    /// <summary>
    /// The store's connection, opened, with the extension loaded and the table prepared. Under the gate. A failure
    /// leaves no connection, so the next call tries again.
    /// </summary>
    private async ValueTask<SqliteConnection> ConnectionAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_connection is not null)
        {
            return _connection;
        }

        var connection = new SqliteConnection(_connectionString);
        try
        {
            try
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (SqliteException e)
            {
                throw new MemoryStoreException($"memory couldn't be prepared: {e.Message}", e);
            }

            LoadExtension(connection, _options.Extension);
            await PrepareAsync(connection, cancellationToken).ConfigureAwait(false);
            _connection = connection;
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Loads sqlite-vec into <paramref name="connection"/>, refusing in words when it can't.</summary>
    internal static void LoadExtension(SqliteConnection connection, string extension)
    {
        try
        {
            connection.LoadExtension(extension);
        }
        catch (Exception e) when (e is SqliteException or InvalidOperationException)
        {
            throw new MemoryStoreException($"memory on SQLite needs the sqlite-vec extension: {e.Message}", e);
        }
    }

    /// <summary>
    /// The memory component, the settings table, this store's table and its indexes, in one immediate transaction so
    /// concurrent first opens can't interleave: refuses a database whose component is newer than this build before
    /// changing anything, records this table's dimensions if no store has, refuses other dimensions, and records the
    /// component at version 1. Every statement is safe to run again.
    /// </summary>
    private async Task PrepareAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        try
        {
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, transaction, StoreSchemaVersions.SqliteDdl, cancellationToken).ConfigureAwait(false);
            long found;
            await using (var read = new SqliteCommand(
                $"SELECT version FROM {StoreSchemaVersions.Table} WHERE component = $component", connection, transaction))
            {
                read.Parameters.AddWithValue("$component", StoreSchemaVersions.Memory);
                found = await read.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is long version ? version : 0;
            }

            if (found > StoreSchemaVersions.MemoryVersion)
            {
                throw new MemoryStoreException(
                    StoreSchemaVersions.Newer(StoreSchemaVersions.Memory, found, StoreSchemaVersions.MemoryVersion));
            }

            await ExecuteAsync(connection, transaction, SettingsDdl, cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, transaction, TableDdl(_table), cancellationToken).ConfigureAwait(false);
            long dimensions;
            await using (var settings = new SqliteCommand(
                $"""
                INSERT INTO {SettingsTable} (table_name, dimensions) VALUES ($table, $dimensions) ON CONFLICT (table_name) DO NOTHING;
                SELECT dimensions FROM {SettingsTable} WHERE table_name = $table;
                """,
                connection,
                transaction))
            {
                settings.Parameters.AddWithValue("$table", _table);
                settings.Parameters.AddWithValue("$dimensions", Dimensions);
                dimensions = (long)(await settings.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
            }

            if (dimensions != Dimensions)
            {
                throw new MemoryStoreException(
                    $"memory was created with {dimensions.ToString(CultureInfo.InvariantCulture)} dimensions, and this store was opened with {Dimensions.ToString(CultureInfo.InvariantCulture)}");
            }

            if (found < StoreSchemaVersions.MemoryVersion)
            {
                await using var record = new SqliteCommand(
                    $"INSERT INTO {StoreSchemaVersions.Table} (component, version) VALUES ($component, $version) " +
                    "ON CONFLICT (component) DO UPDATE SET version = excluded.version",
                    connection,
                    transaction);
                record.Parameters.AddWithValue("$component", StoreSchemaVersions.Memory);
                record.Parameters.AddWithValue("$version", StoreSchemaVersions.MemoryVersion);
                await record.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not (OperationCanceledException or MemoryStoreException))
        {
            throw new MemoryStoreException($"memory couldn't be prepared: {e.Message}", e);
        }
    }

    private static async Task ExecuteAsync(
        SqliteConnection connection, SqliteTransaction transaction, string sql, CancellationToken cancellationToken)
    {
        await using var command = new SqliteCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The settings table, one row per memory table in the file, as <c>strata-memory</c> makes it. Pinned by
    /// <c>schema/fixtures/memory/sqlite-settings-columns.json</c>.
    /// </summary>
    internal const string SettingsDdl = """
        CREATE TABLE IF NOT EXISTS memory_settings (
            table_name TEXT NOT NULL PRIMARY KEY,
            dimensions INTEGER NOT NULL
        )
        """;

    /// <summary>
    /// The table and its indexes, as <c>strata-memory</c>'s <c>SqliteMemoryStore</c> makes them, pinned by
    /// <c>schema/fixtures/memory/sqlite-columns.json</c>. SQLite's default <c>BINARY</c> collation compares bytes, so
    /// every ordering and keyset comparison is in byte order without a <c>COLLATE</c>.
    /// </summary>
    internal static string TableDdl(string table) => $"""
        CREATE TABLE IF NOT EXISTS {table} (
            namespace  TEXT NOT NULL,
            tenant     TEXT NOT NULL,
            id         TEXT NOT NULL,
            model      TEXT NOT NULL,
            embedding  BLOB NOT NULL,
            content    TEXT,
            text       TEXT,
            metadata   TEXT NOT NULL DEFAULT '{"{}"}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (namespace, tenant, id)
        );
        CREATE INDEX IF NOT EXISTS {table}_scope ON {table} (namespace, tenant, model);
        CREATE INDEX IF NOT EXISTS {table}_updated ON {table} (updated_at);
        """;

    /// <summary>
    /// One <c>EXISTS</c> per metadata pair, through <c>json_each</c> rather than a JSON path, since a key may hold
    /// <c>"</c> or <c>.</c>.
    /// </summary>
    private static void AppendMetadataFilter(StringBuilder sql, List<KeyValuePair<string, string>> pairs)
    {
        for (var i = 0; i < pairs.Count; i++)
        {
            sql.Append(CultureInfo.InvariantCulture, $"\n  AND EXISTS (SELECT 1 FROM json_each(t.metadata) WHERE key = $key{i} AND value = $value{i})");
        }
    }

    private static void AddMetadataParameters(SqliteCommand command, List<KeyValuePair<string, string>> pairs)
    {
        for (var i = 0; i < pairs.Count; i++)
        {
            command.Parameters.AddWithValue($"$key{i}", pairs[i].Key);
            command.Parameters.AddWithValue($"$value{i}", pairs[i].Value);
        }
    }

    private static string ExcludeJson(IReadOnlyCollection<string> exclude)
    {
        var array = new JsonArray();
        foreach (var id in exclude)
        {
            array.Add(id);
        }

        return array.ToJsonString();
    }

    private static void AddKey(SqliteCommand command, MemoryKey key)
    {
        command.Parameters.AddWithValue("$namespace", key.Namespace);
        command.Parameters.AddWithValue("$tenant", key.Tenant);
        command.Parameters.AddWithValue("$id", key.Id);
    }

    private static MemoryRecord ReadRecord(SqliteDataReader reader) => new(
        reader.GetString(0),
        reader.GetString(1),
        reader.GetString(2),
        MemoryLayout.Vector((byte[])reader.GetValue(4)),
        reader.GetString(3))
    {
        Content = reader.IsDBNull(5) ? null : MemoryLayout.ParseContent(reader.GetString(5)),
        Text = reader.IsDBNull(6) ? null : reader.GetString(6),
        Metadata = MemoryLayout.ParseMetadata(reader.GetString(7)),
        CreatedAt = MemoryRecords.ParseTimestamp(reader.GetString(8)),
        UpdatedAt = MemoryRecords.ParseTimestamp(reader.GetString(9)),
    };

    /// <summary>
    /// <paramref name="dataSource"/> as a connection string: as it is when it is one, else a file (or
    /// <c>:memory:</c>) opened without pooling, so disposing the store closes the file.
    /// </summary>
    internal static string ConnectionString(string dataSource)
    {
        if (dataSource.Contains("data source=", StringComparison.OrdinalIgnoreCase)
            || dataSource.Contains("datasource=", StringComparison.OrdinalIgnoreCase)
            || dataSource.Contains("filename=", StringComparison.OrdinalIgnoreCase))
        {
            return dataSource;
        }

        return new SqliteConnectionStringBuilder { DataSource = dataSource, Pooling = false }.ToString();
    }

    [GeneratedRegex(@"\A[a-z_][a-z0-9_]{0,62}\z")]
    private static partial Regex TableName();

    public void Dispose()
    {
        _gate.Wait();
        try
        {
            _disposed = true;
            _connection?.Dispose();
            _connection = null;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            _disposed = true;
            if (_connection is not null)
            {
                await _connection.DisposeAsync().ConfigureAwait(false);
                _connection = null;
            }
        }
        finally
        {
            _gate.Release();
        }
    }
}
