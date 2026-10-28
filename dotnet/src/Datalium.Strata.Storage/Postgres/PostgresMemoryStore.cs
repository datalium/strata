using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Datalium.Strata.Memory;
using Npgsql;
using NpgsqlTypes;

namespace Datalium.Strata.Storage.Postgres;

/// <summary>
/// Options of <see cref="PostgresMemoryStore"/> (<c>design/45-Long-Term-Memory.md</c>). Mirrors
/// <c>strata-memory</c>'s <c>PostgresMemoryOptions</c>.
/// </summary>
public sealed record PostgresMemoryOptions
{
    /// <summary>
    /// The length of every vector: 1 to <see cref="MemoryRecords.MaxDimensions"/>, at most
    /// <see cref="MemoryIndex.MaxHnswDimensions"/> with an HNSW index. The table is made with them
    /// (<c>vector(N)</c>), and a store opened on a table made with others is refused.
    /// </summary>
    public required int Dimensions { get; init; }

    /// <summary>
    /// The table, <c>name</c> (in the connection's search path) or <c>schema.name</c>, each part lowercase letters,
    /// digits and underscores, not starting with a digit, at most 63 characters. Its indexes are named after it, in
    /// its schema. An application with two embedding sizes uses two stores on two tables.
    /// </summary>
    public string Table { get; init; } = "memory_records";

    /// <summary>
    /// <see cref="MemoryIndex.None"/> (the default): every search is exact. <see cref="MemoryIndex.Hnsw"/>: an
    /// approximate index, created when the store migrates, for tables past tens of thousands of records per
    /// namespace, tenant and model; it may miss a near neighbour.
    /// </summary>
    public MemoryIndex Index { get; init; } = MemoryIndex.None;

    /// <summary>How a call that failed for a transient reason is repeated. <see cref="StoreRetryPolicy.None"/> turns it off.</summary>
    public StoreRetryPolicy Retry { get; init; } = new();

    /// <summary>
    /// Whether the store creates its table, or brings it up to date, the first time it is used (the default).
    /// When <see langword="false"/> it only checks, and refuses a database that is behind or lacks the table: a
    /// deploy step then runs <see cref="PostgresMemoryStore.MigrateAsync"/>.
    /// </summary>
    public bool AutoMigrate { get; init; } = true;

    /// <summary>
    /// The clock for the records' timestamps and for the waits between attempts.
    /// <see langword="null"/>: <see cref="System.TimeProvider.System"/>.
    /// </summary>
    public TimeProvider? TimeProvider { get; init; }
}

/// <summary>
/// Postgres <see cref="IMemoryStore"/> with pgvector (<c>design/45-Long-Term-Memory.md</c>, "Postgres"): one row
/// per record, the vector in a <c>vector(N)</c> column, searched by cosine distance (<c>&lt;=&gt;</c>), exactly
/// unless <see cref="PostgresMemoryOptions.Index"/> asks for HNSW. Plain Npgsql and SQL: the vector goes in as text
/// cast to <c>vector</c> and comes out as text, so no pgvector client type mapping or EF plugin is needed. The
/// layout is the one <c>strata-memory</c>'s <c>PostgresMemoryStore</c> makes, pinned by
/// <c>schema/fixtures/memory/postgres-columns.json</c>, so either runtime reads what the other wrote.
///
/// No constructor touches the database (<c>design/34-Storage-Hardening.md</c>): the first call, or
/// <see cref="MigrateAsync"/> or <see cref="OpenAsync(string, PostgresMemoryOptions, CancellationToken)"/>, creates
/// the <c>vector</c> extension if the database lacks it (refusing with <c>memory needs pgvector: …</c> when that
/// fails), the table and its indexes, records the <c>memory</c> component at version 1 in
/// <c>strata_schema_version</c>, and refuses a table made with other dimensions. Every call is repeated after a
/// transient failure as <see cref="PostgresMemoryOptions.Retry"/> says (<c>strata.store.operation</c>
/// <c>memory</c>): each is safe to make twice, an upsert included, though a delete repeated after its commit was
/// lost answers <see langword="false"/>. A failure is a <see cref="MemoryStoreException"/>.
///
/// Telemetry on the <see cref="StorageTelemetry.Name"/> meter: <c>strata.memory.operations</c>,
/// <c>strata.memory.operation.duration</c> and <c>strata.memory.search.results</c>, with <c>strata.store</c>
/// <c>postgres</c>.
/// </summary>
public sealed partial class PostgresMemoryStore : IMemoryStore, IMemoryRetention, IDisposable, IAsyncDisposable
{
    private const string StoreName = "postgres";

    /// <summary>What the retry metric calls this store's calls.</summary>
    private const string RetryOperation = "memory";

    /// <summary>The columns every read selects, in this order.</summary>
    private const string Columns =
        "namespace, tenant, id, model, embedding::text, content::text, text, metadata::text, created_at, updated_at";

    private readonly NpgsqlDataSource _dataSource;
    private readonly bool _ownsDataSource;
    private readonly PostgresMemoryOptions _options;
    private readonly TimeProvider _clock;
    private readonly StoreRetrier _retry;
    private readonly PostgresPreparation _preparation;
    private readonly string _table;

    /// <summary>
    /// A store over <paramref name="connectionString"/> (an Npgsql connection string or a <c>postgres://</c>
    /// URL), which owns its connection pool. Nothing is connected until the store is used.
    /// </summary>
    public PostgresMemoryStore(string connectionString, PostgresMemoryOptions options)
        : this(PostgresConnection.CreateDataSource(connectionString), options, ownsDataSource: true)
    {
    }

    /// <param name="dataSource">A pooled data source: the way to configure TLS or a <c>Search Path</c>, which must reach the <c>vector</c> type.</param>
    /// <param name="options">The dimensions, the table, the index, retries and migration.</param>
    /// <param name="ownsDataSource">Whether disposing the store disposes <paramref name="dataSource"/>.</param>
    public PostgresMemoryStore(NpgsqlDataSource dataSource, PostgresMemoryOptions options, bool ownsDataSource = false)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(options);
        Dimensions = MemoryRecords.CheckDimensions(options.Dimensions);
        ArgumentNullException.ThrowIfNull(options.Index);
        if (options.Index is MemoryIndex.HnswIndex && Dimensions > MemoryIndex.MaxHnswDimensions)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options), Dimensions, $"an HNSW index takes at most {MemoryIndex.MaxHnswDimensions} dimensions");
        }

        if (options.Table is null || !TableName().IsMatch(options.Table))
        {
            throw new ArgumentException(
                $"the memory table '{options.Table}' isn't a lowercase SQL name (name or schema.name)", nameof(options));
        }

        _dataSource = dataSource;
        _ownsDataSource = ownsDataSource;
        _options = options;
        _table = options.Table;
        _clock = options.TimeProvider ?? TimeProvider.System;
        _retry = new StoreRetrier(options.Retry, options.TimeProvider, StoreName, PostgresFaults.IsTransient);
        _preparation = new PostgresPreparation(
            _dataSource,
            new PostgresStoreOptions { Retry = options.Retry, AutoMigrate = options.AutoMigrate, TimeProvider = options.TimeProvider },
            _retry,
            StoreSchemaVersions.Memory,
            StoreSchemaVersions.BaselineVersion,
            BaselineAsync,
            StoreSchemaVersions.PostgresMemorySteps,
            RetryOperation,
            PrepareTableAsync);
    }

    /// <summary>
    /// A store over <paramref name="connectionString"/> whose table has been made and checked: the way to find
    /// out at startup that the database can't be reached, lacks pgvector, is newer than this build or holds
    /// vectors of another size. Owns its pool.
    /// </summary>
    public static async Task<PostgresMemoryStore> OpenAsync(
        string connectionString, PostgresMemoryOptions options, CancellationToken cancellationToken = default)
    {
        var store = new PostgresMemoryStore(connectionString, options);
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
    /// Creates the <c>vector</c> extension when the database lacks it, the table, its indexes (the HNSW one when
    /// asked) and the <c>memory</c> component's row, whatever <see cref="PostgresMemoryOptions.AutoMigrate"/> says
    /// (<c>design/34-Storage-Hardening.md</c>, "Migrations"), and checks the table's dimensions. Safe to call at
    /// any time, from any number of processes at once.
    /// </summary>
    /// <exception cref="MemoryStoreException">
    /// pgvector couldn't be created, the database is newer than this build, or the table was made with other dimensions.
    /// </exception>
    public async Task MigrateAsync(CancellationToken cancellationToken = default) =>
        await PrepareAsync(migrate: true, cancellationToken).ConfigureAwait(false);

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

    /// <summary>Upserts <paramref name="records"/>, in one transaction when there are several, all at one time.</summary>
    private ValueTask<bool> WriteAsync(IReadOnlyList<MemoryRecord> records, CancellationToken cancellationToken)
    {
        // One time for every attempt: a repeat writes what the first would have.
        var now = MemoryRecords.TimestampText(_clock.GetUtcNow());
        var sql = $"""
            INSERT INTO {_table} (namespace, tenant, id, model, embedding, content, text, metadata, created_at, updated_at)
            VALUES ($1, $2, $3, $4, CAST($5 AS vector), $6, $7, $8, $9, $9)
            ON CONFLICT (namespace, tenant, id) DO UPDATE SET
                model = EXCLUDED.model,
                embedding = EXCLUDED.embedding,
                content = EXCLUDED.content,
                text = EXCLUDED.text,
                metadata = EXCLUDED.metadata,
                updated_at = EXCLUDED.updated_at
            """;

        return RunAsync(
            "upsert",
            async (connection, token) =>
            {
                await using var transaction = records.Count > 1
                    ? await connection.BeginTransactionAsync(token).ConfigureAwait(false)
                    : null;
                await using var batch = new NpgsqlBatch(connection, transaction);
                foreach (var record in records)
                {
                    var command = new NpgsqlBatchCommand(sql);
                    command.Parameters.Add(Text(record.Namespace));
                    command.Parameters.Add(Text(record.Tenant));
                    command.Parameters.Add(Text(record.Id));
                    command.Parameters.Add(Text(record.Model));
                    command.Parameters.Add(Text(PgVector.Format(record.Vector.Span)));
                    command.Parameters.Add(Jsonb(record.Content?.ToJsonString()));
                    command.Parameters.Add(Text(record.Text));
                    command.Parameters.Add(Jsonb(MetadataJson(record.Metadata)));
                    command.Parameters.Add(Text(now));
                    batch.BatchCommands.Add(command);
                }

                await batch.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                if (transaction is not null)
                {
                    await transaction.CommitAsync(token).ConfigureAwait(false);
                }

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
                await using var command = new NpgsqlCommand(
                    $"SELECT {Columns} FROM {_table} WHERE namespace = $1 AND tenant = $2 AND id = $3", connection);
                command.Parameters.Add(Text(key.Namespace));
                command.Parameters.Add(Text(key.Tenant));
                command.Parameters.Add(Text(key.Id));
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
                await using var command = new NpgsqlCommand(
                    $"DELETE FROM {_table} WHERE namespace = $1 AND tenant = $2 AND id = $3", connection);
                command.Parameters.Add(Text(key.Namespace));
                command.Parameters.Add(Text(key.Tenant));
                command.Parameters.Add(Text(key.Id));
                return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false) > 0;
            },
            cancellationToken);
    }

    /// <summary>
    /// The records most similar to <paramref name="search"/>'s vector. Filters are SQL <c>WHERE</c> clauses, so
    /// they apply before ranking; <see cref="MemorySearch.MinSimilarity"/> is applied as the rows are read, which
    /// stops at the first below it. With an HNSW index the search runs with pgvector's iterative scan
    /// (<c>hnsw.iterative_scan = relaxed_order</c>, pgvector 0.8), so filters don't starve the limit, over twice
    /// the limit's candidates, re-sorted by exact distance and then id.
    /// </summary>
    public async ValueTask<IReadOnlyList<MemoryMatch>> SearchAsync(MemorySearch search, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(search, Dimensions);
        var hnsw = _options.Index is MemoryIndex.HnswIndex;
        const string Filter = """
            WHERE namespace = $2 AND tenant = $3 AND model = $4
              AND NOT (id = ANY($5)) AND metadata @> $6
            """;
        var sql = hnsw
            ? $"""
                WITH near AS MATERIALIZED (
                    SELECT {Columns}, embedding <=> CAST($1 AS vector) AS distance
                    FROM {_table}
                    {Filter}
                    ORDER BY embedding <=> CAST($1 AS vector)
                    LIMIT $8
                )
                SELECT {Columns}, 1 - distance AS similarity
                FROM near
                ORDER BY distance, id COLLATE "C"
                LIMIT $7
                """
            : $"""
                SELECT {Columns}, 1 - (embedding <=> CAST($1 AS vector)) AS similarity
                FROM {_table}
                {Filter}
                ORDER BY embedding <=> CAST($1 AS vector), id COLLATE "C"
                LIMIT $7
                """;

        var matches = await RunAsync(
                "search",
                async (connection, token) =>
                {
                    await using var transaction = hnsw ? await connection.BeginTransactionAsync(token).ConfigureAwait(false) : null;
                    if (transaction is not null)
                    {
                        await using var scan = new NpgsqlCommand("SET LOCAL hnsw.iterative_scan = relaxed_order", connection, transaction);
                        await scan.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                    }

                    await using var command = new NpgsqlCommand(sql, connection, transaction);
                    command.Parameters.Add(Text(PgVector.Format(search.Vector.Span)));
                    command.Parameters.Add(Text(search.Namespace));
                    command.Parameters.Add(Text(search.Tenant));
                    command.Parameters.Add(Text(search.Model));
                    command.Parameters.Add(new NpgsqlParameter
                    {
                        NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
                        Value = search.Exclude.ToArray(),
                    });
                    command.Parameters.Add(Jsonb(MetadataJson(search.Metadata)));
                    command.Parameters.Add(Bigint(search.Limit));
                    if (hnsw)
                    {
                        command.Parameters.Add(Bigint(2 * search.Limit));
                    }

                    var found = new List<MemoryMatch>();
                    await using (var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync(token).ConfigureAwait(false))
                        {
                            var similarity = reader.GetDouble(10);
                            if (search.MinSimilarity is { } min && similarity < min)
                            {
                                break;
                            }

                            found.Add(new MemoryMatch(ReadRecord(reader), similarity));
                        }
                    }

                    if (transaction is not null)
                    {
                        await transaction.CommitAsync(token).ConfigureAwait(false);
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
        var sql = $"""
            SELECT {Columns} FROM {_table}
            WHERE namespace = $1 AND tenant = $2 AND metadata @> $3 {(after is null ? "" : "AND id COLLATE \"C\" > $5")}
            ORDER BY id COLLATE "C"
            LIMIT $4
            """;
        return RunAsync(
            "list",
            async (connection, token) =>
            {
                await using var command = new NpgsqlCommand(sql, connection);
                command.Parameters.Add(Text(query.Namespace));
                command.Parameters.Add(Text(query.Tenant));
                command.Parameters.Add(Jsonb(MetadataJson(query.Metadata)));

                // One more than a page, to know whether another follows.
                command.Parameters.Add(Bigint(query.Limit + 1));
                if (after is not null)
                {
                    command.Parameters.Add(Text(after));
                }

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
    /// Deletes the records <paramref name="purge"/> names in one statement: <c>updated_at</c> before its cutoff
    /// (at the second, exclusive, compared as stored text), oldest first, then by namespace, tenant and id. The
    /// cutoff is checked again on each picked row, so a record written again while the purge waited for it is kept.
    /// </summary>
    public ValueTask<int> PurgeAsync(MemoryPurge purge, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(purge);
        var filter = new StringBuilder("updated_at < $1");
        if (purge.Namespace is not null)
        {
            filter.Append(" AND namespace = $3");
        }

        if (purge.Tenant is not null)
        {
            filter.Append(purge.Namespace is null ? " AND tenant = $3" : " AND tenant = $4");
        }

        var sql = $"""
            DELETE FROM {_table} WHERE updated_at < $1 AND (namespace, tenant, id) IN (
                SELECT namespace, tenant, id FROM {_table}
                WHERE {filter}
                ORDER BY updated_at COLLATE "C", namespace COLLATE "C", tenant COLLATE "C", id COLLATE "C"
                LIMIT $2)
            """;
        return RunAsync(
            "purge",
            async (connection, token) =>
            {
                await using var command = new NpgsqlCommand(sql, connection);
                command.Parameters.Add(Text(MemoryRecords.TimestampText(purge.OlderThan)));
                command.Parameters.Add(Bigint(purge.Limit));
                if (purge.Namespace is not null)
                {
                    command.Parameters.Add(Text(purge.Namespace));
                }

                if (purge.Tenant is not null)
                {
                    command.Parameters.Add(Text(purge.Tenant));
                }

                return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            },
            cancellationToken);
    }

    /// <summary>
    /// Makes one call: prepares the table, then runs <paramref name="attempt"/> on a connection of its own under
    /// the retry policy; counts it, and turns a failure into a <see cref="MemoryStoreException"/>.
    /// </summary>
    private async ValueTask<T> RunAsync<T>(
        string operation, Func<NpgsqlConnection, CancellationToken, Task<T>> attempt, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            await PrepareAsync(migrate: false, cancellationToken).ConfigureAwait(false);
            return await _retry.RunAsync(
                    RetryOperation,
                    async (_, token) =>
                    {
                        await using var connection = await _dataSource.OpenConnectionAsync(token).ConfigureAwait(false);
                        return await attempt(connection, token).ConfigureAwait(false);
                    },
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception e) when (e is not (OperationCanceledException or MemoryStoreException))
        {
            throw new MemoryStoreException($"memory {operation} failed: {e.Message}", e);
        }
        finally
        {
            StorageTelemetry.MemoryOperation(StoreName, operation, Stopwatch.GetElapsedTime(started));
        }
    }

    private async ValueTask PrepareAsync(bool migrate, CancellationToken cancellationToken)
    {
        try
        {
            await _preparation.EnsureAsync(migrate, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException e)
        {
            // The schema-version refusals (a newer database, or one behind a store that doesn't migrate), as they are.
            throw new MemoryStoreException(e.Message, e);
        }
        catch (Exception e) when (e is not (OperationCanceledException or MemoryStoreException))
        {
            throw new MemoryStoreException($"memory couldn't be prepared: {e.Message}", e);
        }
    }

    /// <summary>
    /// The table and its indexes, as <c>strata-memory</c>'s <c>PostgresMemoryStore</c> makes them. Every
    /// statement is safe to run again. <c>created_at</c> and <c>updated_at</c> are RFC 3339 text at whole
    /// seconds, as checkpoints' are, so a purge compares them as text. The key's columns are <c>COLLATE "C"</c>,
    /// and every ordering names it too, so ties and listings go by id in byte order (ordinal, as the in-memory
    /// store's) whatever the database's collation.
    /// </summary>
    internal static string TableDdl(string table, int dimensions) => $$"""
        CREATE TABLE IF NOT EXISTS {{table}} (
            namespace  TEXT COLLATE "C" NOT NULL,
            tenant     TEXT COLLATE "C" NOT NULL,
            id         TEXT COLLATE "C" NOT NULL,
            model      TEXT NOT NULL,
            embedding  vector({{dimensions.ToString(CultureInfo.InvariantCulture)}}) NOT NULL,
            content    JSONB,
            text       TEXT,
            metadata   JSONB NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (namespace, tenant, id)
        );
        CREATE INDEX IF NOT EXISTS {{IndexPrefix(table)}}_scope ON {{table}} (namespace, tenant, model);
        CREATE INDEX IF NOT EXISTS {{IndexPrefix(table)}}_updated ON {{table}} (updated_at);
        """;

    /// <summary>What a table's indexes are named after: its name without the schema, since an index lives in its table's.</summary>
    private static string IndexPrefix(string table) => table[(table.LastIndexOf('.') + 1)..];

    /// <summary>The HNSW index on cosine distance.</summary>
    internal static string HnswDdl(string table, MemoryIndex.HnswIndex index) =>
        $"CREATE INDEX IF NOT EXISTS {IndexPrefix(table)}_embedding_hnsw ON {table} USING hnsw (embedding vector_cosine_ops) " +
        $"WITH (m = {index.M.ToString(CultureInfo.InvariantCulture)}, ef_construction = {index.EfConstruction.ToString(CultureInfo.InvariantCulture)})";

    /// <summary>
    /// Version 1 of the memory component: the <c>vector</c> extension and this store's table. A later store on
    /// another table makes its own in <see cref="PrepareTableAsync"/>, since the component's row is shared.
    /// </summary>
    private async Task BaselineAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await EnsureVectorAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        await PostgresSchema.ExecuteAsync(connection, transaction, TableDdl(_table, Dimensions), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Run on every preparation, once the component's version is settled, under the DDL lock: when it may change
    /// the database, the extension, the table and its indexes; always, the check that the table exists and was made
    /// with this store's dimensions.
    /// </summary>
    private async Task PrepareTableAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, bool apply, CancellationToken cancellationToken)
    {
        if (apply)
        {
            await EnsureVectorAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            await PostgresSchema.ExecuteAsync(connection, transaction, TableDdl(_table, Dimensions), cancellationToken).ConfigureAwait(false);
            if (_options.Index is MemoryIndex.HnswIndex hnsw)
            {
                await PostgresSchema.ExecuteAsync(connection, transaction, HnswDdl(_table, hnsw), cancellationToken).ConfigureAwait(false);
            }
        }

        // pgvector keeps a column's dimensions as its type modifier: vector(768) is 768.
        await using var read = new NpgsqlCommand(
            "SELECT a.atttypmod FROM pg_attribute a WHERE a.attrelid = to_regclass($1) AND a.attname = 'embedding' AND NOT a.attisdropped",
            connection,
            transaction);
        read.Parameters.Add(Text(_table));
        var found = await read.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (found is null)
        {
            // Only a store that doesn't migrate gets here: the component is recorded, by a store on another table.
            throw new MemoryStoreException($"the memory table '{_table}' doesn't exist: run its migration");
        }

        var dimensions = Convert.ToInt32(found, CultureInfo.InvariantCulture);
        if (dimensions != Dimensions)
        {
            throw new MemoryStoreException(
                $"memory was created with {dimensions.ToString(CultureInfo.InvariantCulture)} dimensions, and this store was opened with {Dimensions.ToString(CultureInfo.InvariantCulture)}");
        }
    }

    /// <summary>Creates the <c>vector</c> extension unless the database has it, refusing in words when it can't.</summary>
    private static async Task EnsureVectorAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        if (await PostgresSchema.QueryAsync(
                connection, transaction, "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')", cancellationToken)
            .ConfigureAwait(false))
        {
            return;
        }

        try
        {
            await PostgresSchema.ExecuteAsync(connection, transaction, "CREATE EXTENSION IF NOT EXISTS vector", cancellationToken)
                .ConfigureAwait(false);
        }
        catch (PostgresException e) when (!PostgresFaults.IsTransient(e))
        {
            throw new MemoryStoreException($"memory needs pgvector: CREATE EXTENSION vector failed ({e.MessageText})", e);
        }
    }

    private static MemoryRecord ReadRecord(NpgsqlDataReader reader) => new(
        reader.GetString(0),
        reader.GetString(1),
        reader.GetString(2),
        PgVector.Parse(reader.GetString(4)),
        reader.GetString(3))
    {
        Content = reader.IsDBNull(5) ? null : JsonNode.Parse(reader.GetString(5))?.AsObject(),
        Text = reader.IsDBNull(6) ? null : reader.GetString(6),
        Metadata = JsonNode.Parse(reader.GetString(7))!.AsObject()
            .ToDictionary(pair => pair.Key, pair => pair.Value!.GetValue<string>(), StringComparer.Ordinal),
        CreatedAt = MemoryRecords.ParseTimestamp(reader.GetString(8)),
        UpdatedAt = MemoryRecords.ParseTimestamp(reader.GetString(9)),
    };

    private static string MetadataJson(IReadOnlyDictionary<string, string> metadata)
    {
        var json = new JsonObject();
        foreach (var (key, value) in metadata)
        {
            json[key] = value;
        }

        return json.ToJsonString();
    }

    private static NpgsqlParameter Text(string? value) => new() { NpgsqlDbType = NpgsqlDbType.Text, Value = value ?? (object)DBNull.Value };

    private static NpgsqlParameter Jsonb(string? value) => new() { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = value ?? (object)DBNull.Value };

    private static NpgsqlParameter Bigint(long value) => new() { NpgsqlDbType = NpgsqlDbType.Bigint, Value = value };

    [GeneratedRegex(@"\A[a-z_][a-z0-9_]{0,62}(\.[a-z_][a-z0-9_]{0,62})?\z")]
    private static partial Regex TableName();

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
}

/// <summary>
/// pgvector's text form, the only one the memory store sends or reads: <c>[1,-0.5,0.25]</c>, each element the
/// shortest text that reads back as the same <see cref="float"/>, in the invariant culture.
/// </summary>
internal static class PgVector
{
    public static string Format(ReadOnlySpan<float> vector)
    {
        var text = new StringBuilder(vector.Length * 8 + 2).Append('[');
        for (var i = 0; i < vector.Length; i++)
        {
            if (i > 0)
            {
                text.Append(',');
            }

            text.Append(vector[i].ToString("R", CultureInfo.InvariantCulture));
        }

        return text.Append(']').ToString();
    }

    /// <exception cref="FormatException">It isn't a vector's text.</exception>
    public static float[] Parse(string text)
    {
        var trimmed = text.AsSpan().Trim();
        if (trimmed.Length < 2 || trimmed[0] != '[' || trimmed[^1] != ']')
        {
            throw new FormatException($"'{text}' is not a vector");
        }

        var inner = trimmed[1..^1];
        if (inner.Trim().IsEmpty)
        {
            return [];
        }

        var values = new List<float>();
        foreach (var range in inner.Split(','))
        {
            values.Add(float.Parse(inner[range].Trim(), NumberStyles.Float, CultureInfo.InvariantCulture));
        }

        return [.. values];
    }
}
