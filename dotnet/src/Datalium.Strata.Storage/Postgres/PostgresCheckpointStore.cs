using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;
using Npgsql;
using NpgsqlTypes;

namespace Datalium.Strata.Storage.Postgres;

/// <summary>
/// Postgres <see cref="ICheckpointStore"/> adapter -- the "Redis/PostgreSQL" tier from
/// <c>design/03-Architecture.md</c>'s memory blueprint, and the first backend that more than one process
/// can write to at once. Plain Npgsql, no EF Core, and async throughout
/// (<c>design/19-Async-Queryable-Store.md</c>); the synchronous members wrap the async ones.
///
/// The table is the SQLite checkpoint log with Postgres types (see <c>schema/README.md</c>, "Postgres
/// checkpoint log"): an <b>append-only log</b> where <see cref="SaveAsync"/> adds a row at the next
/// <c>sequence</c> for its thread and <see cref="LoadAsync"/> returns the highest, with <c>sequence</c> as
/// <c>BIGINT</c> and <c>frozen_state</c>/<c>metadata</c> as <c>JSONB</c> (so key order and whitespace are
/// not preserved on the way back out -- harmless, state is read by field name). Beside it, the
/// <c>checkpoint_threads</c> head table, kept current by a trigger, is what <see cref="ListThreadsAsync"/>
/// reads. It is the same layout <c>strata-memory</c>'s <c>PostgresCheckpointStore</c> creates, so a thread
/// frozen by one runtime can be resumed by the other.
///
/// Unlike <see cref="Memory.EfCoreCheckpointStore"/>, more than one writer is supported: a save takes a
/// per-thread transaction-scoped advisory lock before choosing the next sequence, so concurrent writers
/// serialize instead of colliding on the primary key.
///
/// No constructor touches the database (<c>design/34-Storage-Hardening.md</c>): the tables are created or
/// brought up to date the first time the store is used, or by <see cref="MigrateAsync"/> or
/// <see cref="OpenAsync(string, PostgresStoreOptions?, CancellationToken)"/>, and recorded in
/// <c>strata_schema_version</c>. Every call is repeated after a transient failure as
/// <see cref="PostgresStoreOptions.Retry"/> says; a save that may have been committed is looked up before it
/// is made again, so it is never written twice.
///
/// Not built: TLS beyond what the connection string asks of Npgsql (pass your own
/// <see cref="NpgsqlDataSource"/> to configure it).
/// </summary>
public sealed class PostgresCheckpointStore : ICheckpointStore, ICheckpointQuery, ICheckpointRetention, IDisposable, IAsyncDisposable
{
    /// <summary>
    /// The canonical Postgres <c>checkpoints</c> table. Identical to <c>strata-memory</c>'s
    /// <c>POSTGRES_CHECKPOINT_LOG_DDL</c>; each language's tests compare the created table against
    /// <c>schema/fixtures/checkpoint-log/postgres-columns.json</c>.
    /// </summary>
    public const string CheckpointLogDdl = """
        CREATE TABLE IF NOT EXISTS checkpoints (
            thread_id      TEXT NOT NULL,
            sequence       BIGINT NOT NULL,
            schema_version TEXT NOT NULL,
            status         TEXT NOT NULL,
            suspended_node TEXT,
            next_node      TEXT,
            frozen_state   JSONB NOT NULL,
            metadata       JSONB,
            created_at     TEXT NOT NULL,
            resumed_at     TEXT,
            nested         JSONB,
            decision       JSONB,
            wait           JSONB,
            wake_at        TEXT,
            graph_id       TEXT,
            graph_version  TEXT,
            PRIMARY KEY (thread_id, sequence)
        )
        """;

    /// <summary>
    /// Adds the <c>nested</c> column to a <c>checkpoints</c> table created before nested checkpoints
    /// (<c>design/16-Nested-Interrupts.md</c>), and the <c>decision</c> column to one created before
    /// recorded decisions (<c>design/21-Typed-Human-Signals.md</c>), and the <c>wait</c> and <c>wake_at</c>
    /// columns to one created before waits (<c>design/29-Event-Waits-And-Timers.md</c>), and the <c>graph_id</c>
    /// and <c>graph_version</c> columns to one created before graph identities (<c>design/31-Graph-Identity.md</c>),
    /// which <c>CREATE TABLE IF NOT EXISTS</c> leaves as it is. Existing rows keep <c>NULL</c>, which is right:
    /// none has a child, a decision, a wait or an identity. A no-op on a current table. Identical to
    /// <c>strata-memory</c>'s <c>POSTGRES_CHECKPOINT_LOG_MIGRATION</c>.
    /// </summary>
    public const string CheckpointLogMigration =
        "ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS nested JSONB, ADD COLUMN IF NOT EXISTS decision JSONB, " +
        "ADD COLUMN IF NOT EXISTS wait JSONB, ADD COLUMN IF NOT EXISTS wake_at TEXT, " +
        "ADD COLUMN IF NOT EXISTS graph_id TEXT, ADD COLUMN IF NOT EXISTS graph_version TEXT";

    /// <summary>
    /// Adds <c>wake_at</c> to a head table created before waits (<c>design/29-Event-Waits-And-Timers.md</c>), and
    /// <c>graph_id</c> and <c>graph_version</c> to one created before graph identities
    /// (<c>design/31-Graph-Identity.md</c>); <see cref="CheckpointThreadsDdl"/> then replaces its trigger. Identical
    /// to <c>strata-memory</c>'s <c>POSTGRES_CHECKPOINT_THREADS_MIGRATION</c>.
    /// </summary>
    public const string CheckpointThreadsMigration =
        "ALTER TABLE checkpoint_threads ADD COLUMN IF NOT EXISTS wake_at TEXT COLLATE \"C\", " +
        "ADD COLUMN IF NOT EXISTS graph_id TEXT, ADD COLUMN IF NOT EXISTS graph_version TEXT";

    /// <summary>
    /// The head table listing threads reads, its indexes, and the trigger that keeps it current whoever
    /// writes the log (<c>design/19-Async-Queryable-Store.md</c>, "SQL: a trigger-maintained head table").
    /// <c>thread_id</c> and <c>updated_at</c> use the <c>"C"</c> collation, so threads sort byte-wise as in
    /// every other store. Identical to <c>strata-memory</c>'s <c>POSTGRES_CHECKPOINT_THREADS_DDL</c>; pinned
    /// by <c>schema/fixtures/checkpoint-log/postgres-threads-columns.json</c>.
    /// </summary>
    public const string CheckpointThreadsDdl = """
        CREATE TABLE IF NOT EXISTS checkpoint_threads (
            thread_id  TEXT COLLATE "C" NOT NULL PRIMARY KEY,
            sequence   BIGINT NOT NULL,
            state      TEXT NOT NULL,
            updated_at TEXT COLLATE "C" NOT NULL,
            metadata      JSONB,
            wake_at       TEXT COLLATE "C",
            graph_id      TEXT,
            graph_version TEXT
        );
        CREATE INDEX IF NOT EXISTS checkpoint_threads_by_state
            ON checkpoint_threads (state, updated_at, thread_id);
        CREATE INDEX IF NOT EXISTS checkpoint_threads_by_updated
            ON checkpoint_threads (updated_at, thread_id);
        CREATE INDEX IF NOT EXISTS checkpoint_threads_by_metadata
            ON checkpoint_threads USING GIN (metadata jsonb_path_ops);
        CREATE INDEX IF NOT EXISTS checkpoint_threads_by_wake
            ON checkpoint_threads (wake_at);
        CREATE INDEX IF NOT EXISTS checkpoint_threads_by_graph
            ON checkpoint_threads (graph_id, updated_at, thread_id);
        CREATE OR REPLACE FUNCTION strata_checkpoints_head() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO checkpoint_threads AS h (thread_id, sequence, state, updated_at, metadata, wake_at, graph_id, graph_version)
            VALUES (NEW.thread_id, NEW.sequence,
                    CASE WHEN NEW.resumed_at IS NOT NULL THEN 'resumed' ELSE NEW.status END,
                    COALESCE(NEW.resumed_at, NEW.created_at), NEW.metadata, NEW.wake_at, NEW.graph_id, NEW.graph_version)
            ON CONFLICT (thread_id) DO UPDATE SET
                sequence = EXCLUDED.sequence,
                state = EXCLUDED.state,
                updated_at = EXCLUDED.updated_at,
                metadata = EXCLUDED.metadata,
                wake_at = EXCLUDED.wake_at,
                graph_id = EXCLUDED.graph_id,
                graph_version = EXCLUDED.graph_version
            WHERE EXCLUDED.sequence > h.sequence;
            RETURN NULL;
        END;
        $$;
        CREATE OR REPLACE TRIGGER checkpoints_head AFTER INSERT ON checkpoints
            FOR EACH ROW EXECUTE FUNCTION strata_checkpoints_head();
        """;

    /// <summary>Fills a just-created <c>checkpoint_threads</c> from the log it was added to. Identical to <c>POSTGRES_CHECKPOINT_THREADS_BACKFILL</c>.</summary>
    public const string CheckpointThreadsBackfill = """
        INSERT INTO checkpoint_threads (thread_id, sequence, state, updated_at, metadata, wake_at, graph_id, graph_version)
        SELECT c.thread_id, c.sequence,
               CASE WHEN c.resumed_at IS NOT NULL THEN 'resumed' ELSE c.status END,
               COALESCE(c.resumed_at, c.created_at), c.metadata, c.wake_at, c.graph_id, c.graph_version
        FROM checkpoints c
        JOIN (SELECT thread_id, MAX(sequence) AS latest FROM checkpoints GROUP BY thread_id) l
          ON c.thread_id = l.thread_id AND c.sequence = l.latest
        """;

    /// <summary>
    /// The thread leases (<c>design/32-Thread-Leases.md</c>): one row per thread that ever had one, kept
    /// until the thread is purged, so <c>token</c> only grows. A thread is held while <c>expires_at</c> is set and after
    /// <c>clock_timestamp()</c>, the server's clock; a release clears it and <c>owner</c>. Identical to
    /// <c>strata-memory</c>'s <c>POSTGRES_CHECKPOINT_LEASES_DDL</c>; pinned by
    /// <c>schema/fixtures/checkpoint-log/postgres-leases-columns.json</c>.
    /// </summary>
    public const string CheckpointLeasesDdl = """
        CREATE TABLE IF NOT EXISTS checkpoint_leases (
            thread_id  TEXT NOT NULL PRIMARY KEY,
            owner      TEXT,
            token      BIGINT NOT NULL,
            expires_at TIMESTAMPTZ
        )
        """;

    /// <summary>
    /// The blobs of value offload (<c>design/41-Checkpoint-Efficiency.md</c>, "Offload"): each value a store
    /// offloaded to its own database, by thread and <c>sha256:</c> hash, as its JSON text. The checkpoint
    /// component's schema version 2. Identical to <c>strata-memory</c>'s <c>POSTGRES_CHECKPOINT_BLOBS_DDL</c>;
    /// pinned by <c>schema/fixtures/checkpoint-log/postgres-blobs-columns.json</c>.
    /// </summary>
    public const string CheckpointBlobsDdl = """
        CREATE TABLE IF NOT EXISTS checkpoint_blobs (
            thread_id TEXT NOT NULL,
            hash      TEXT NOT NULL,
            content   BYTEA NOT NULL,
            PRIMARY KEY (thread_id, hash)
        )
        """;

    /// <summary>How many times an acquisition looks again at a lease that changed while it was being read.</summary>
    private const int AcquireAttempts = 3;

    /// <summary>The columns every read selects, in this order.</summary>
    private const string CheckpointColumns =
        "c.thread_id, c.sequence, c.schema_version, c.status, c.suspended_node, c.next_node, " +
        "c.frozen_state::text, c.metadata::text, c.created_at, c.resumed_at, c.nested::text, c.decision::text, " +
        "c.wait::text, c.graph_id, c.graph_version";

    private readonly NpgsqlDataSource _dataSource;
    private readonly bool _ownsDataSource;
    private readonly PostgresStoreOptions _options;
    private readonly StoreRetrier _retry;
    private readonly PostgresPreparation _preparation;
    private readonly GroupCommitQueue? _group;
    private readonly CheckpointWriter? _writer;

    /// <summary>Where offloaded values go when not in this database.</summary>
    private readonly ICheckpointBlobStore? _blobs;

    /// <summary>
    /// A store over <paramref name="connectionString"/> (an Npgsql connection string or a <c>postgres://</c>
    /// URL), which owns its connection pool. Nothing is connected until the store is used.
    /// </summary>
    public PostgresCheckpointStore(string connectionString, PostgresStoreOptions? options = null)
        : this(PostgresConnection.CreateDataSource(connectionString), options, ownsDataSource: true)
    {
    }

    /// <param name="dataSource">A pooled data source -- the way to configure TLS or a custom <c>Search Path</c>.</param>
    /// <param name="options">Retries and migration; the defaults when <see langword="null"/>.</param>
    /// <param name="ownsDataSource">Whether <see cref="Dispose"/> should dispose <paramref name="dataSource"/>.</param>
    public PostgresCheckpointStore(NpgsqlDataSource dataSource, PostgresStoreOptions? options = null, bool ownsDataSource = false)
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
            StoreSchemaVersions.Checkpoints,
            StoreSchemaVersions.BaselineVersion,
            BaselineAsync,
            StoreSchemaVersions.PostgresCheckpointSteps);
        if (_options.GroupCommit is { } groupCommit)
        {
            _group = new GroupCommitQueue(groupCommit, "postgres", FlushAsync, _retry.IsTransient);
        }

        _writer = CheckpointWriter.For(_options.Deltas, _options.Offload);
        _blobs = _options.Offload?.Blobs;
    }

    /// <summary>
    /// A store over <paramref name="connectionString"/> whose tables have been brought up to date: the way to
    /// find out at startup that the database can't be reached or is newer than this build. Owns its pool.
    /// </summary>
    public static async Task<PostgresCheckpointStore> OpenAsync(
        string connectionString, PostgresStoreOptions? options = null, CancellationToken cancellationToken = default)
    {
        var store = new PostgresCheckpointStore(connectionString, options);
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
    /// Creates the store's tables, or brings an older database up to date, whatever
    /// <see cref="PostgresStoreOptions.AutoMigrate"/> says (<c>design/34-Storage-Hardening.md</c>,
    /// "Migrations"). Safe to call at any time, from any number of processes at once.
    /// </summary>
    /// <exception cref="InvalidOperationException">The database is newer than this build.</exception>
    public async Task MigrateAsync(CancellationToken cancellationToken = default) =>
        await _preparation.EnsureAsync(migrate: true, cancellationToken).ConfigureAwait(false);

    private ValueTask ReadyAsync(CancellationToken cancellationToken) =>
        _preparation.EnsureAsync(migrate: false, cancellationToken);

    /// <summary>
    /// Version 1 of the checkpoint component (<see cref="StoreSchemaVersions"/>): creates the log, its head
    /// table and the leases, or brings an older database up to date. It adds the <c>nested</c>,
    /// <c>decision</c>, <c>wait</c>, <c>wake_at</c>, <c>graph_id</c> and <c>graph_version</c> columns, creates and
    /// backfills <c>checkpoint_threads</c> when it doesn't exist yet (<c>design/19-Async-Queryable-Store.md</c>,
    /// "Migration"), and adds the columns it lacks and replaces its trigger when it predates waits
    /// (<c>design/29-Event-Waits-And-Timers.md</c>) or graph identities (<c>design/31-Graph-Identity.md</c>).
    /// Creating the trigger locks <c>checkpoints</c> against inserts until the transaction commits, so no save
    /// can land between the backfill and the trigger taking over. Safe on any database, old or current.
    /// </summary>
    private static async Task BaselineAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        Task Execute(string sql) => PostgresSchema.ExecuteAsync(connection, transaction, sql, cancellationToken);
        Task<bool> Query(string sql) => PostgresSchema.QueryAsync(connection, transaction, sql, cancellationToken);

        await Execute(CheckpointLogDdl).ConfigureAwait(false);
        await Execute(CheckpointLogMigration).ConfigureAwait(false);
        if (!await Query("SELECT to_regclass('checkpoint_threads') IS NOT NULL").ConfigureAwait(false))
        {
            await Execute(CheckpointThreadsDdl).ConfigureAwait(false);
            await Execute(CheckpointThreadsBackfill).ConfigureAwait(false);
        }
        else if (!await Query(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() " +
            "AND table_name = 'checkpoint_threads' AND column_name = 'graph_version')").ConfigureAwait(false))
        {
            // A head table from before waits or identities: nothing in it waits with a time or has an
            // identity, so every row's NULL is right.
            await Execute(CheckpointThreadsMigration).ConfigureAwait(false);
            await Execute(CheckpointThreadsDdl).ConfigureAwait(false);
        }

        // Leases (design/32-Thread-Leases.md): nothing to migrate, a database from before them has none.
        await Execute(CheckpointLeasesDdl).ConfigureAwait(false);
    }

    /// <summary>Synchronous <see cref="SaveAsync"/>.</summary>
    public void Save(Checkpoint checkpoint) => SaveAsync(checkpoint).AsTask().GetAwaiter().GetResult();

    /// <summary>Synchronous <see cref="LoadAsync"/>.</summary>
    public Checkpoint? Load(string threadId) => LoadAsync(threadId).AsTask().GetAwaiter().GetResult();

    /// <summary>Appends <paramref name="checkpoint"/> to the log at the next sequence for its thread; <see cref="LoadAsync"/> then returns it as the latest.</summary>
    public async ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
        await InsertAsync(checkpoint, expectedSequence: null, lease: null, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Appends <paramref name="checkpoint"/> only if <paramref name="expectedSequence"/> is still its thread's
    /// latest sequence, decided under the save's per-thread advisory lock (<c>design/28-Ending-A-Thread.md</c>,
    /// "Races").
    /// </summary>
    public async ValueTask<bool> SaveIfLatestAsync(
        Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
        await InsertAsync(checkpoint, expectedSequence, lease: null, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Appends <paramref name="checkpoint"/> only if <paramref name="lease"/>'s token is still its thread's
    /// latest, read under the save's per-thread advisory lock, which an acquisition takes too, so none can come
    /// between the check and the insert (<c>design/32-Thread-Leases.md</c>, "Fencing").
    /// </summary>
    public async ValueTask<bool> SaveUnderLeaseAsync(
        Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default)
    {
        ThreadLeases.CheckThread(checkpoint, lease);
        return await InsertAsync(checkpoint, expectedSequence, lease, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Under the thread's advisory lock: the thread's first lease at token 1, or its next token when the lease
    /// it has isn't held; no row back means it is held, and a second statement reads by whom.
    /// </summary>
    public async ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
        string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ThreadLeases.Check(owner, ttl);
        await ReadyAsync(cancellationToken).ConfigureAwait(false);

        // An acquisition whose commit failed may have taken the lease, and the store can't tell its own from
        // another process's with the same owner name: that failure is the call's, and the lease, if it was
        // taken, runs out by itself. A failure before the commit is repeated.
        var committing = false;
        return await _retry.RunAsync(
                "lease",
                (_, token) =>
                {
                    committing = false;
                    return AcquireOnceAsync(threadId, owner, ttl, () => committing = true, token);
                },
                cancellationToken,
                unless: _ => committing)
            .ConfigureAwait(false);
    }

    private async ValueTask<LeaseAttempt> AcquireOnceAsync(
        string threadId, string owner, TimeSpan ttl, Action committing, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await PostgresSchema.LockAsync(connection, transaction, "checkpoints", threadId, cancellationToken).ConfigureAwait(false);

        for (var attempt = 1; ; attempt++)
        {
            await using var acquire = new NpgsqlCommand(
                """
                INSERT INTO checkpoint_leases (thread_id, owner, token, expires_at)
                VALUES (@thread_id, @owner, 1, clock_timestamp() + @ttl * interval '1 millisecond')
                ON CONFLICT (thread_id) DO UPDATE
                    SET owner = EXCLUDED.owner, token = checkpoint_leases.token + 1, expires_at = EXCLUDED.expires_at
                    WHERE checkpoint_leases.expires_at IS NULL OR checkpoint_leases.expires_at <= clock_timestamp()
                RETURNING owner, token, expires_at
                """,
                connection,
                transaction);
            acquire.Parameters.Add(PostgresSchema.Param("thread_id", NpgsqlDbType.Text, threadId));
            acquire.Parameters.Add(PostgresSchema.Param("owner", NpgsqlDbType.Text, owner));
            acquire.Parameters.Add(PostgresSchema.Param("ttl", NpgsqlDbType.Bigint, ThreadLeases.Milliseconds(ttl)));
            if (await ReadLeaseAsync(acquire, threadId, cancellationToken).ConfigureAwait(false) is { } lease)
            {
                committing();
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                await FaultAsync("lease:after-commit").ConfigureAwait(false);
                return new LeaseAttempt(lease, null);
            }

            await using var holder = new NpgsqlCommand(HeldLeaseSql, connection, transaction);
            holder.Parameters.Add(PostgresSchema.Param("thread_id", NpgsqlDbType.Text, threadId));
            if (await ReadLeaseAsync(holder, threadId, cancellationToken).ConfigureAwait(false) is { } held)
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return new LeaseAttempt(null, held);
            }

            // Released or expired between the two statements (a release takes no lock): look again.
            if (attempt == AcquireAttempts)
            {
                throw new InvalidOperationException($"the lease of thread '{threadId}' kept changing while it was being acquired");
            }
        }
    }

    public async ValueTask<ThreadLease?> RenewLeaseAsync(
        ThreadLease lease, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ThreadLeases.CheckTtl(ttl);
        await ReadyAsync(cancellationToken).ConfigureAwait(false);

        // Renewing twice is renewing once, so it is repeated whatever the failure.
        return await _retry.RunAsync(
                "lease",
                async (_, token) =>
                {
                    await using var renew = _dataSource.CreateCommand(
                        """
                        UPDATE checkpoint_leases
                        SET owner = @owner, expires_at = clock_timestamp() + @ttl * interval '1 millisecond'
                        WHERE thread_id = @thread_id AND token = @token
                        RETURNING owner, token, expires_at
                        """);
                    renew.Parameters.Add(PostgresSchema.Param("owner", NpgsqlDbType.Text, lease.Owner));
                    renew.Parameters.Add(PostgresSchema.Param("ttl", NpgsqlDbType.Bigint, ThreadLeases.Milliseconds(ttl)));
                    renew.Parameters.Add(PostgresSchema.Param("thread_id", NpgsqlDbType.Text, lease.ThreadId));
                    renew.Parameters.Add(PostgresSchema.Param("token", NpgsqlDbType.Bigint, lease.Token));
                    return await ReadLeaseAsync(renew, lease.ThreadId, token).ConfigureAwait(false);
                },
                cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        await ReadyAsync(cancellationToken).ConfigureAwait(false);

        // Releasing twice is releasing once (the row is matched by its token), so it is repeated whatever the
        // failure.
        return await _retry.RunAsync(
                "lease",
                async (_, token) =>
                {
                    await using var release = _dataSource.CreateCommand(
                        "UPDATE checkpoint_leases SET owner = NULL, expires_at = NULL WHERE thread_id = @thread_id AND token = @token");
                    release.Parameters.Add(PostgresSchema.Param("thread_id", NpgsqlDbType.Text, lease.ThreadId));
                    release.Parameters.Add(PostgresSchema.Param("token", NpgsqlDbType.Bigint, lease.Token));
                    return await release.ExecuteNonQueryAsync(token).ConfigureAwait(false) == 1;
                },
                cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken cancellationToken = default)
    {
        await ReadyAsync(cancellationToken).ConfigureAwait(false);
        return await _retry.RunAsync(
                "lease",
                async (_, token) =>
                {
                    await using var held = _dataSource.CreateCommand(HeldLeaseSql);
                    held.Parameters.Add(PostgresSchema.Param("thread_id", NpgsqlDbType.Text, threadId));
                    return await ReadLeaseAsync(held, threadId, token).ConfigureAwait(false);
                },
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>The lease a thread is held under now, on the server's clock.</summary>
    private const string HeldLeaseSql =
        "SELECT owner, token, expires_at FROM checkpoint_leases WHERE thread_id = @thread_id AND expires_at > clock_timestamp()";

    /// <summary>The lease <paramref name="command"/> returns as <c>owner, token, expires_at</c>, if it returns a row.</summary>
    private static async Task<ThreadLease?> ReadLeaseAsync(NpgsqlCommand command, string threadId, CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new ThreadLease(threadId, reader.GetString(0), reader.GetInt64(1), reader.GetFieldValue<DateTimeOffset>(2))
            : null;
    }

    /// <summary>
    /// Appends <paramref name="checkpoint"/> at the next sequence for its thread, or, with an
    /// <paramref name="expectedSequence"/>, only if that is still the thread's latest; says whether it did.
    /// With a <paramref name="lease"/>, only if its token is still the thread's latest, else it throws
    /// <see cref="LeaseLostException"/> having written nothing.
    /// </summary>
    private async ValueTask<bool> InsertAsync(
        Checkpoint checkpoint, long? expectedSequence, ThreadLease? lease, CancellationToken cancellationToken)
    {
        await ReadyAsync(cancellationToken).ConfigureAwait(false);
        if (_group is { } group)
        {
            return await group.SaveAsync(checkpoint, expectedSequence, lease, cancellationToken).ConfigureAwait(false);
        }

        // An attempt that failed while it committed may have been written. The next attempt looks before it
        // writes again, so a checkpoint is never appended twice and a conditional save keeps its answer
        // (design/34-Storage-Hardening.md, "A save that may have happened"). Whatever it finds, the store no
        // longer knows which row is the thread's latest. Blobs elsewhere are written before the row.
        var save = _writer?.Prepare(checkpoint) ?? EncodedSave.Plain(checkpoint);
        if (_blobs is { } blobs)
        {
            await OffloadedBlobs.PutAsync(blobs, checkpoint.ThreadId, save.Blobs, cancellationToken).ConfigureAwait(false);
        }

        var mayBeWritten = false;
        return await _retry.RunAsync(
                "save",
                async (_, token) =>
                {
                    if (mayBeWritten)
                    {
                        await FaultAsync("save:recheck").ConfigureAwait(false);
                        var fate = await FateAsync(checkpoint, expectedSequence, token).ConfigureAwait(false);
                        _writer?.Forget(checkpoint.ThreadId);
                        switch (fate)
                        {
                            case SaveFate.Saved:
                                return true;
                            case SaveFate.Lost:
                                return false;
                        }

                        mayBeWritten = false;
                    }

                    return await InsertOnceAsync(save, expectedSequence, lease, () => mayBeWritten = true, token)
                        .ConfigureAwait(false);
                },
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// What a look at the log says about a save that may have happened: for a conditional one, the entry right
    /// after its expected sequence; for any other, the thread's latest.
    /// </summary>
    private async ValueTask<SaveFate> FateAsync(Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken)
    {
        var next = await RowAsync(checkpoint.ThreadId, expectedSequence + 1, cancellationToken).ConfigureAwait(false);
        return StoredCheckpoints.FateOf(checkpoint, next?.Row.Checkpoint, conditional: expectedSequence is not null);
    }

    /// <summary>
    /// The depth of the row <c>l</c>: how many rows before it a load reads with it
    /// (<c>design/41-Checkpoint-Efficiency.md</c>, "Deltas").
    /// </summary>
    private const string DepthSql =
        $"CASE WHEN l.schema_version IN ({CheckpointEncoding.EncodedVersionsSql}) THEN (l.frozen_state->>'depth')::bigint ELSE 0 END";

    /// <summary>A thread's latest row and the rows before it that it is a delta of, oldest first: one read.</summary>
    private const string LatestChainSql = $"""
        WITH target AS (
            SELECT l.sequence, {DepthSql} AS depth FROM checkpoints l
            WHERE l.thread_id = $1 ORDER BY l.sequence DESC LIMIT 1)
        SELECT {CheckpointColumns} FROM checkpoints c, target
        WHERE c.thread_id = $1 AND c.sequence BETWEEN target.sequence - target.depth AND target.sequence
        ORDER BY c.sequence
        """;

    /// <summary><see cref="LatestChainSql"/> for the row at the sequence <c>$2</c>.</summary>
    private const string RowChainSql = $"""
        WITH target AS (
            SELECT l.sequence, {DepthSql} AS depth FROM checkpoints l
            WHERE l.thread_id = $1 AND l.sequence = $2)
        SELECT {CheckpointColumns} FROM checkpoints c, target
        WHERE c.thread_id = $1 AND c.sequence BETWEEN target.sequence - target.depth AND target.sequence
        ORDER BY c.sequence
        """;

    /// <summary>The rows of a thread from <c>$2</c> to <c>$3</c>, oldest first: what a page of its log is a delta of.</summary>
    private const string RangeSql =
        $"SELECT {CheckpointColumns} FROM checkpoints c WHERE c.thread_id = $1 AND c.sequence BETWEEN $2 AND $3 ORDER BY c.sequence";

    /// <summary>
    /// The row of <paramref name="threadId"/> at <paramref name="sequence"/> (the latest when
    /// <see langword="null"/>), decoded, its offloaded values read, with its sequence and how far it is from a whole
    /// row; one attempt.
    /// </summary>
    private async Task<(long Sequence, DecodedRow Row, int Depth)?> RowAsync(
        string threadId, long? sequence, CancellationToken cancellationToken)
    {
        var rows = sequence is { } at
            ? await QueryAsync(
                RowChainSql,
                [
                    new NpgsqlParameter { Value = threadId, NpgsqlDbType = NpgsqlDbType.Text },
                    new NpgsqlParameter { Value = at, NpgsqlDbType = NpgsqlDbType.Bigint },
                ],
                reader => ReadCheckpoint(reader, withState: true),
                cancellationToken).ConfigureAwait(false)
            : await QueryAsync(
                LatestChainSql,
                [new NpgsqlParameter { Value = threadId, NpgsqlDbType = NpgsqlDbType.Text }],
                reader => ReadCheckpoint(reader, withState: true),
                cancellationToken).ConfigureAwait(false);
        if (rows.Count == 0)
        {
            return null;
        }

        var depth = CheckpointEncoding.DepthOf(rows[^1].Checkpoint, rows[^1].Sequence);
        var latest = (await CheckpointEncoding.DecodeAsync(rows, rows.Count - 1, ReaderFor(threadId, retried: false), cancellationToken)
            .ConfigureAwait(false))[0];
        return (latest.Sequence, latest, depth);
    }

    /// <summary>
    /// How <paramref name="threadId"/>'s offloaded values are read: from the blob store the options name, or from
    /// this database's <c>checkpoint_blobs</c>, in one query, under the retry policy when <paramref name="retried"/>
    /// (and as part of the caller's attempt otherwise).
    /// </summary>
    private BlobReader ReaderFor(string threadId, bool retried) => _blobs is { } blobs
        ? (hashes, cancellationToken) => OffloadedBlobs.ReadAsync(blobs, threadId, hashes, cancellationToken)
        : async (hashes, cancellationToken) =>
        {
            const string sql = "SELECT hash, content FROM checkpoint_blobs WHERE thread_id = $1 AND hash = ANY($2)";
            NpgsqlParameter[] parameters =
            [
                new() { Value = threadId, NpgsqlDbType = NpgsqlDbType.Text },
                new() { Value = hashes.ToArray(), NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text },
            ];
            static (string Hash, byte[] Content) Read(NpgsqlDataReader reader) => (reader.GetString(0), reader.GetFieldValue<byte[]>(1));
            var found = retried
                ? await ReadAsync("load", sql, parameters, Read, cancellationToken).ConfigureAwait(false)
                : await QueryAsync(sql, parameters, Read, cancellationToken).ConfigureAwait(false);
            return found.ToDictionary(blob => blob.Hash, blob => (ReadOnlyMemory<byte>)blob.Content, StringComparer.Ordinal);
        };

    /// <summary>A thread's latest row, decoded, under the retry policy; the store then knows it as the thread's latest.</summary>
    private async ValueTask<(long Sequence, Checkpoint Checkpoint)?> LatestAsync(string threadId, CancellationToken cancellationToken)
    {
        await ReadyAsync(cancellationToken).ConfigureAwait(false);
        var latest = await _retry.RunAsync(
                "load",
                (_, token) => new ValueTask<(long Sequence, DecodedRow Row, int Depth)?>(RowAsync(threadId, null, token)),
                cancellationToken)
            .ConfigureAwait(false);
        if (latest is not { } row)
        {
            return null;
        }

        _writer?.Loaded(row.Sequence, row.Depth, row.Row, row.Row.Checkpoint.CreatedAt);
        return (row.Sequence, row.Row.Checkpoint);
    }

    private ValueTask FaultAsync(string point) => _options.Fault?.Invoke(point) ?? ValueTask.CompletedTask;

    /// <summary>
    /// One attempt of <see cref="InsertAsync"/>. <paramref name="committing"/> is called once the commit is sent. A
    /// save whose base is no longer the thread's latest is written whole instead, under the same lock.
    /// </summary>
    private async ValueTask<bool> InsertOnceAsync(
        EncodedSave save, long? expectedSequence, ThreadLease? lease, Action committing, CancellationToken cancellationToken)
    {
        var checkpoint = save.Checkpoint;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        await PostgresSchema.LockAsync(connection, transaction, "checkpoints", checkpoint.ThreadId, cancellationToken)
            .ConfigureAwait(false);

        if (lease is not null)
        {
            // Under the lock an acquisition takes too, so the token can't move before the insert commits.
            await using var fence = new NpgsqlCommand(
                "SELECT token FROM checkpoint_leases WHERE thread_id = @thread_id", connection, transaction);
            fence.Parameters.Add(PostgresSchema.Param("thread_id", NpgsqlDbType.Text, checkpoint.ThreadId));
            if (await fence.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not long token || token != lease.Token)
            {
                throw new LeaseLostException(lease);
            }
        }

        // The latest sequence is read under the lock, so nothing can be appended between the check and
        // the insert.
        var (sequence, bytes) = await InsertRowAsync(connection, transaction, save, expectedSequence, _blobs is null, cancellationToken)
            .ConfigureAwait(false);
        if (sequence is null && save.Base is not null)
        {
            save = save.Whole();
            if (_blobs is { } blobs)
            {
                await OffloadedBlobs.PutAsync(blobs, checkpoint.ThreadId, save.Blobs, cancellationToken).ConfigureAwait(false);
            }

            (sequence, bytes) = await InsertRowAsync(connection, transaction, save, expectedSequence, _blobs is null, cancellationToken)
                .ConfigureAwait(false);
        }

        await FaultAsync("save:before-commit").ConfigureAwait(false);
        if (sequence is not null)
        {
            // From here on a failure doesn't say whether the row is there.
            committing();
        }

        await FaultAsync("save:commit").ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        await FaultAsync("save:after-commit").ConfigureAwait(false);
        if (sequence is { } saved)
        {
            StorageTelemetry.Stored("postgres", save, bytes);
            _writer?.Saved(save, saved, save.Stored.CreatedAt);
            return true;
        }

        _writer?.Forget(checkpoint.ThreadId);
        return false;
    }

    /// <summary>
    /// The insert of a save, in <paramref name="transaction"/> under the thread's lock: at the next sequence, only
    /// if that follows <paramref name="expectedSequence"/> when there is one, and, for a save that relies on the row
    /// before it, only if it follows that row, by its sequence and its <c>created_at</c>; with
    /// <paramref name="ownBlobs"/>, the blobs it writes in the same statement, only if the row is appended. Returns
    /// the sequence it was appended at, if it was, and the bytes the row holds.
    /// </summary>
    private static async ValueTask<(long? Sequence, long Bytes)> InsertRowAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, EncodedSave save, long? expectedSequence, bool ownBlobs,
        CancellationToken cancellationToken)
    {
        var checkpoint = save.Stored;
        var frozenState = checkpoint.FrozenState?.ToJsonString() ?? "null";
        var metadata = checkpoint.Metadata is null ? null : JsonSerializer.Serialize(checkpoint.Metadata);
        var nested = checkpoint.Nested?.ToWireJson().ToJsonString();
        var decision = checkpoint.Decision?.ToWireJson().ToJsonString();
        var wait = checkpoint.Wait?.ToWireJson().ToJsonString();
        await using var insert = new NpgsqlCommand(InsertSql, connection, transaction);
        insert.Parameters.Add(PostgresSchema.Param("thread_id", NpgsqlDbType.Text, checkpoint.ThreadId));
        insert.Parameters.Add(PostgresSchema.Param("schema_version", NpgsqlDbType.Text, checkpoint.SchemaVersion));
        insert.Parameters.Add(PostgresSchema.Param("status", NpgsqlDbType.Text, CheckpointListing.StatusToString(checkpoint.Status)));
        insert.Parameters.Add(PostgresSchema.Param("suspended_node", NpgsqlDbType.Text, checkpoint.SuspendedNode));
        insert.Parameters.Add(PostgresSchema.Param("next_node", NpgsqlDbType.Text, checkpoint.NextNode));
        insert.Parameters.Add(PostgresSchema.Param("frozen_state", NpgsqlDbType.Jsonb, frozenState));
        insert.Parameters.Add(PostgresSchema.Param("metadata", NpgsqlDbType.Jsonb, metadata));
        insert.Parameters.Add(PostgresSchema.Param("created_at", NpgsqlDbType.Text, checkpoint.CreatedAt));
        insert.Parameters.Add(PostgresSchema.Param("resumed_at", NpgsqlDbType.Text, checkpoint.ResumedAt));
        insert.Parameters.Add(PostgresSchema.Param("nested", NpgsqlDbType.Jsonb, nested));
        insert.Parameters.Add(PostgresSchema.Param("decision", NpgsqlDbType.Jsonb, decision));
        insert.Parameters.Add(PostgresSchema.Param("wait", NpgsqlDbType.Jsonb, wait));
        insert.Parameters.Add(PostgresSchema.Param("wake_at", NpgsqlDbType.Text, checkpoint.InnermostWait()?.WakeAt));
        insert.Parameters.Add(PostgresSchema.Param("graph_id", NpgsqlDbType.Text, checkpoint.Graph?.Id));
        insert.Parameters.Add(PostgresSchema.Param("graph_version", NpgsqlDbType.Text, checkpoint.Graph?.Version));
        insert.Parameters.Add(PostgresSchema.Param("expected", NpgsqlDbType.Bigint, expectedSequence));
        insert.Parameters.Add(PostgresSchema.Param("base", NpgsqlDbType.Bigint, save.Base?.Sequence));
        insert.Parameters.Add(PostgresSchema.Param("base_created", NpgsqlDbType.Text, save.Base?.Token));
        var blobs = ownBlobs ? save.Blobs : [];
        insert.Parameters.Add(PostgresSchema.Param("hashes", NpgsqlDbType.Array | NpgsqlDbType.Text, blobs.Select(blob => blob.Hash).ToArray()));
        insert.Parameters.Add(PostgresSchema.Param(
            "contents", NpgsqlDbType.Array | NpgsqlDbType.Bytea, blobs.Select(blob => blob.Content.ToArray()).ToArray()));
        var sequence = await insert.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as long?;
        return (sequence, StoredBytes(frozenState, metadata, nested, decision, wait));
    }

    /// <summary>
    /// A single save's statement: the row at the next sequence under <see cref="InsertRowAsync"/>'s conditions,
    /// and, only if it was appended, its blobs (<c>@hashes</c>, <c>@contents</c>), a blob the thread has already
    /// left as it is.
    /// </summary>
    private const string InsertSql = """
        WITH appended AS (
            INSERT INTO checkpoints
                (thread_id, sequence, schema_version, status, suspended_node, next_node,
                 frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                 graph_id, graph_version)
            SELECT
                @thread_id, latest.sequence + 1,
                @schema_version, @status, @suspended_node, @next_node,
                @frozen_state, @metadata, @created_at, @resumed_at, @nested, @decision, @wait, @wake_at,
                @graph_id, @graph_version
            FROM (SELECT COALESCE(MAX(sequence), -1) AS sequence FROM checkpoints WHERE thread_id = @thread_id) latest
            WHERE (@expected::bigint IS NULL OR latest.sequence = @expected::bigint)
              AND (@base::bigint IS NULL OR (latest.sequence = @base::bigint AND EXISTS (
                  SELECT 1 FROM checkpoints b
                  WHERE b.thread_id = @thread_id AND b.sequence = @base::bigint AND b.created_at = @base_created::text)))
            RETURNING sequence
        ), blobs AS (
            INSERT INTO checkpoint_blobs (thread_id, hash, content)
            SELECT @thread_id, b.hash, b.content FROM unnest(@hashes::text[], @contents::bytea[]) AS b(hash, content)
            WHERE EXISTS (SELECT 1 FROM appended)
            ON CONFLICT DO NOTHING
        )
        SELECT sequence FROM appended
        """;

    /// <summary>The bytes of the JSON a row holds: what <c>strata.store.checkpoint.size</c> records.</summary>
    private static long StoredBytes(params string?[] parts) =>
        parts.Sum(part => part is null ? 0L : Encoding.UTF8.GetByteCount(part));

    /// <summary>
    /// One row of a flush: the checkpoint's fields as the insert binds them, made once for every attempt, in
    /// <see cref="FlushWaveSql"/>'s order from <c>thread_id</c> to <c>base_created</c>, and how it is encoded.
    /// </summary>
    private sealed record FlushRow(QueuedSave Save, EncodedSave Encoded, object?[] Values, long Bytes);

    /// <summary>
    /// A row's verdict in a flush attempt, the sequence it was given when it was appended, and, for a delta
    /// whose base had moved, the row it was written as instead.
    /// </summary>
    private readonly record struct RowVerdict(SaveVerdict Verdict, long? Sequence, FlushRow? Whole = null);

    /// <summary>The types of <see cref="FlushWaveSql"/>'s arrays, after <c>ord</c>.</summary>
    private static readonly NpgsqlDbType[] FlushColumnTypes =
    [
        NpgsqlDbType.Text, NpgsqlDbType.Text, NpgsqlDbType.Text, NpgsqlDbType.Text, NpgsqlDbType.Text,
        NpgsqlDbType.Jsonb, NpgsqlDbType.Jsonb, NpgsqlDbType.Text, NpgsqlDbType.Text, NpgsqlDbType.Jsonb,
        NpgsqlDbType.Jsonb, NpgsqlDbType.Jsonb, NpgsqlDbType.Text, NpgsqlDbType.Text, NpgsqlDbType.Text,
        NpgsqlDbType.Bigint, NpgsqlDbType.Bigint, NpgsqlDbType.Bigint, NpgsqlDbType.Text,
    ];

    /// <summary>
    /// One wave of a flush: rows of distinct threads, whose advisory locks are already held, given as one array
    /// per column. Each row reads its thread's latest sequence and lease token, gets the verdict a single save
    /// would (a lost lease first, then a moved sequence), and is appended only when it is saved. A row that relies
    /// on a base that isn't the latest any more (by sequence and <c>created_at</c>) is <c>rebase</c>: not appended,
    /// and written whole by the flush after. The blobs of the rows saved (<c>$21</c> to <c>$23</c>, each with its
    /// row's position) are written with them. Returns each row's position, verdict and the sequence it got. One
    /// statement, so one plan, however many rows; a later wave sees an earlier one's rows. Identical to
    /// <c>strata-memory</c>'s <c>FLUSH_WAVE_SQL</c>.
    /// </summary>
    private const string FlushWaveSql = """
        WITH rows AS (
            SELECT * FROM unnest(
                $1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::jsonb[], $8::jsonb[],
                $9::text[], $10::text[], $11::jsonb[], $12::jsonb[], $13::jsonb[], $14::text[], $15::text[], $16::text[],
                $17::bigint[], $18::bigint[], $19::bigint[], $20::text[])
                AS r(ord, thread_id, schema_version, status, suspended_node, next_node, frozen_state, metadata,
                     created_at, resumed_at, nested, decision, wait, wake_at, graph_id, graph_version, expected, token,
                     base, base_created)
        ), verdict AS (
            SELECT r.*, latest.sequence AS latest,
                   CASE
                       WHEN r.token IS NOT NULL
                            AND (SELECT l.token FROM checkpoint_leases l WHERE l.thread_id = r.thread_id) IS DISTINCT FROM r.token
                           THEN 'lease_lost'
                       WHEN r.expected IS NOT NULL AND latest.sequence <> r.expected THEN 'lost'
                       WHEN r.base IS NOT NULL AND (latest.sequence <> r.base OR NOT EXISTS (
                           SELECT 1 FROM checkpoints b
                           WHERE b.thread_id = r.thread_id AND b.sequence = r.base AND b.created_at = r.base_created))
                           THEN 'rebase'
                       ELSE 'saved'
                   END AS outcome
            FROM rows r
            CROSS JOIN LATERAL (
                SELECT COALESCE(MAX(c.sequence), -1) AS sequence FROM checkpoints c WHERE c.thread_id = r.thread_id
            ) latest
        ), blobs AS (
            INSERT INTO checkpoint_blobs (thread_id, hash, content)
            SELECT v.thread_id, b.hash, b.content
            FROM unnest($21::int[], $22::text[], $23::bytea[]) AS b(ord, hash, content)
            JOIN verdict v ON v.ord = b.ord
            WHERE v.outcome = 'saved'
            ON CONFLICT DO NOTHING
        ), inserted AS (
            INSERT INTO checkpoints
                (thread_id, sequence, schema_version, status, suspended_node, next_node,
                 frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                 graph_id, graph_version)
            SELECT thread_id, latest + 1, schema_version, status, suspended_node, next_node,
                   frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                   graph_id, graph_version
            FROM verdict WHERE outcome = 'saved'
        )
        SELECT ord, outcome, latest + 1 FROM verdict ORDER BY ord
        """;

    /// <summary>
    /// Takes the advisory lock of every thread of a flush, in the order of their keys, so flushes and single
    /// saves never wait on each other in a cycle. The key is <see cref="PostgresSchema.LockAsync"/>'s.
    /// </summary>
    private const string FlushLockSql = """
        SELECT pg_advisory_xact_lock(k)
        FROM (SELECT DISTINCT hashtextextended('checkpoints' || ':' || t, 0) AS k FROM unnest($1::text[]) AS t) keys
        ORDER BY k
        """;

    /// <summary>
    /// Group commit's flush (<c>design/41-Checkpoint-Efficiency.md</c>, "Group commit"): every save of
    /// <paramref name="saves"/> in one transaction, sent as one batch with the threads' locks, under the retry
    /// policy. An attempt whose commit may have happened is looked up row by row before the rest are tried
    /// again: a row appended at the sequence it was given was saved; a row whose place another checkpoint took
    /// lost its race if it was conditional, and is saved again if not.
    /// </summary>
    private async ValueTask FlushAsync(IReadOnlyList<QueuedSave> saves)
    {
        // A thread with two saves in one flush has both written whole: the row before the second isn't known
        // until the first has its verdict. Blobs elsewhere are all written before the flush, so a row written whole
        // in it needs none that isn't there.
        var alone = saves.GroupBy(save => save.Checkpoint.ThreadId, StringComparer.Ordinal)
            .Where(thread => thread.Count() == 1)
            .Select(thread => thread.Key)
            .ToHashSet(StringComparer.Ordinal);
        var rows = saves
            .Select(save => BindRow(
                save,
                _writer?.Prepare(save.Checkpoint, alone.Contains(save.Checkpoint.ThreadId), writeAllBlobs: _blobs is not null)
                    ?? EncodedSave.Plain(save.Checkpoint)))
            .ToList();
        if (_blobs is { } blobs)
        {
            foreach (var row in rows)
            {
                await OffloadedBlobs.PutAsync(blobs, row.Save.Checkpoint.ThreadId, row.Encoded.Blobs, CancellationToken.None)
                    .ConfigureAwait(false);
            }
        }

        Dictionary<FlushRow, RowVerdict>? inDoubt = null;
        await _retry.RunAsync(
                "save",
                async (_, token) =>
                {
                    if (inDoubt is not null)
                    {
                        await FaultAsync("save:recheck").ConfigureAwait(false);
                        rows = await SettleAsync(rows, inDoubt, token).ConfigureAwait(false);
                        inDoubt = null;
                        if (rows.Count == 0)
                        {
                            return;
                        }
                    }

                    var verdicts = await FlushOnceAsync(rows, attempt => inDoubt = attempt, token).ConfigureAwait(false);
                    foreach (var row in rows)
                    {
                        Answer(row, verdicts[row]);
                    }
                },
                CancellationToken.None)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Gives a row its answer, and remembers what it was written as: a row whose sequence isn't known (it was
    /// looked up after a flush in doubt) or that lost its race leaves the thread's latest row unknown.
    /// </summary>
    private void Answer(FlushRow row, RowVerdict verdict)
    {
        var written = verdict.Whole ?? row;
        var threadId = row.Save.Checkpoint.ThreadId;
        if (verdict.Verdict == SaveVerdict.Saved)
        {
            StorageTelemetry.Stored("postgres", written.Encoded, written.Bytes);
            if (verdict.Sequence is { } sequence)
            {
                _writer?.Saved(written.Encoded, sequence, written.Encoded.Stored.CreatedAt);
            }
            else
            {
                _writer?.Forget(threadId);
            }
        }
        else if (verdict.Verdict == SaveVerdict.Lost)
        {
            _writer?.Forget(threadId);
        }

        row.Save.Settle(verdict.Verdict);
    }

    private static FlushRow BindRow(QueuedSave save, EncodedSave encoded)
    {
        var checkpoint = encoded.Stored;
        var frozenState = checkpoint.FrozenState?.ToJsonString() ?? "null";
        var metadata = checkpoint.Metadata is null ? null : JsonSerializer.Serialize(checkpoint.Metadata);
        var nested = checkpoint.Nested?.ToWireJson().ToJsonString();
        var decision = checkpoint.Decision?.ToWireJson().ToJsonString();
        var wait = checkpoint.Wait?.ToWireJson().ToJsonString();
        object?[] values =
        [
            checkpoint.ThreadId,
            checkpoint.SchemaVersion,
            CheckpointListing.StatusToString(checkpoint.Status),
            checkpoint.SuspendedNode,
            checkpoint.NextNode,
            frozenState,
            metadata,
            checkpoint.CreatedAt,
            checkpoint.ResumedAt,
            nested,
            decision,
            wait,
            checkpoint.InnermostWait()?.WakeAt,
            checkpoint.Graph?.Id,
            checkpoint.Graph?.Version,
            save.ExpectedSequence,
            save.Lease?.Token,
            encoded.Base?.Sequence,
            encoded.Base?.Token,
        ];
        return new FlushRow(save, encoded, values, StoredBytes(frozenState, metadata, nested, decision, wait));
    }

    /// <summary>
    /// One attempt of <see cref="FlushAsync"/>: the transaction, its locks and its rows in one batch, then the
    /// commit. The rows go in waves of distinct threads: the first save of each thread, then the second, and so
    /// on, so one thread's saves keep their order. Returns each row's verdict; <paramref name="committing"/> is
    /// given them once the commit is sent, when a failure no longer says whether they were written.
    /// </summary>
    private async ValueTask<Dictionary<FlushRow, RowVerdict>> FlushOnceAsync(
        List<FlushRow> rows, Action<Dictionary<FlushRow, RowVerdict>> committing, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using var batch = new NpgsqlBatch(connection, transaction);

        var locks = new NpgsqlBatchCommand(FlushLockSql);
        locks.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = rows.Select(row => row.Save.Checkpoint.ThreadId).Distinct(StringComparer.Ordinal).ToArray(),
        });
        batch.BatchCommands.Add(locks);

        var waves = new List<List<int>>();
        var seen = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var index = 0; index < rows.Count; index++)
        {
            var thread = rows[index].Save.Checkpoint.ThreadId;
            var wave = seen.GetValueOrDefault(thread);
            seen[thread] = wave + 1;
            if (wave == waves.Count)
            {
                waves.Add([]);
            }

            waves[wave].Add(index);
        }

        foreach (var wave in waves)
        {
            batch.BatchCommands.Add(WaveCommand(rows, wave, _blobs is null));
        }

        // Prepared once per physical connection, which Npgsql keeps across the pool: the large wave statement
        // is planned once, not at every flush.
        await batch.PrepareAsync(cancellationToken).ConfigureAwait(false);
        var verdicts = new Dictionary<FlushRow, RowVerdict>(rows.Count);
        var rebased = new List<FlushRow>();
        await using (var reader = await batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var _ in waves)
            {
                await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = rows[reader.GetInt32(0)];
                    if (reader.GetString(1) == "rebase")
                    {
                        rebased.Add(row);
                    }
                    else
                    {
                        verdicts[row] = ReadVerdict(reader);
                    }
                }
            }
        }

        if (rebased.Count > 0)
        {
            // Deltas whose base another writer moved past: written whole, in the same transaction and under the
            // same locks. Each is its thread's only row in the flush, so one wave holds them all.
            var whole = rebased.Select(row => BindRow(row.Save, row.Encoded.Whole())).ToList();
            await using var again = new NpgsqlBatch(connection, transaction);
            again.BatchCommands.Add(WaveCommand(whole, [.. Enumerable.Range(0, whole.Count)], _blobs is null));
            await again.PrepareAsync(cancellationToken).ConfigureAwait(false);
            await using var reader = await again.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var index = reader.GetInt32(0);
                verdicts[rebased[index]] = ReadVerdict(reader) with { Whole = whole[index] };
            }
        }

        await FaultAsync("save:before-commit").ConfigureAwait(false);
        if (verdicts.Values.Any(verdict => verdict.Verdict == SaveVerdict.Saved))
        {
            // From here on a failure doesn't say whether the rows are there.
            committing(verdicts);
        }

        await FaultAsync("save:commit").ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        await FaultAsync("save:after-commit").ConfigureAwait(false);
        return verdicts;
    }

    /// <summary>The verdict of a row of <see cref="FlushWaveSql"/>'s result.</summary>
    private static RowVerdict ReadVerdict(NpgsqlDataReader reader) => reader.GetString(1) switch
    {
        "saved" => new RowVerdict(SaveVerdict.Saved, reader.GetInt64(2)),
        "lost" => new RowVerdict(SaveVerdict.Lost, null),
        _ => new RowVerdict(SaveVerdict.LeaseLost, null),
    };

    /// <summary>
    /// <see cref="FlushWaveSql"/> for the rows at <paramref name="wave"/> (indexes into <paramref name="rows"/>, each a
    /// distinct thread), with their blobs when <paramref name="ownBlobs"/>.
    /// </summary>
    private static NpgsqlBatchCommand WaveCommand(IReadOnlyList<FlushRow> rows, List<int> wave, bool ownBlobs)
    {
        var insert = new NpgsqlBatchCommand(FlushWaveSql);
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = wave.ToArray() });
        for (var column = 0; column < FlushColumnTypes.Length; column++)
        {
            insert.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Array | FlushColumnTypes[column],
                Value = wave.Select(index => rows[index].Values[column]).ToArray(),
            });
        }

        var blobs = ownBlobs
            ? wave.SelectMany(index => rows[index].Encoded.Blobs.Select(blob => (Ord: index, blob.Hash, Content: blob.Content.ToArray()))).ToList()
            : [];
        insert.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer,
            Value = blobs.Select(blob => blob.Ord).ToArray(),
        });
        insert.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = blobs.Select(blob => blob.Hash).ToArray(),
        });
        insert.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea,
            Value = blobs.Select(blob => blob.Content).ToArray(),
        });
        return insert;
    }

    /// <summary>
    /// After an attempt whose commit may have happened: answers every row whose fate the log tells, and returns
    /// the rows to flush again. A refused row's verdict stands (a log and a lease token only move forward); a row
    /// that was given a sequence is looked up there, all in one query.
    /// </summary>
    private async ValueTask<List<FlushRow>> SettleAsync(
        List<FlushRow> rows, Dictionary<FlushRow, RowVerdict> verdicts, CancellationToken cancellationToken)
    {
        var placed = rows.Where(row => verdicts[row].Sequence is not null).ToList();
        var found = placed.Count == 0
            ? []
            : await QueryAsync(
                $"""
                SELECT {CheckpointColumns}
                FROM checkpoints c
                JOIN unnest($1::text[], $2::bigint[]) AS p(thread_id, sequence)
                  ON c.thread_id = p.thread_id AND c.sequence = p.sequence
                """,
                [
                    new NpgsqlParameter
                    {
                        NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
                        Value = placed.Select(row => row.Save.Checkpoint.ThreadId).ToArray(),
                    },
                    new NpgsqlParameter
                    {
                        NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint,
                        Value = placed.Select(row => verdicts[row].Sequence!.Value).ToArray(),
                    },
                ],
                reader => ReadCheckpoint(reader, withState: true),
                cancellationToken).ConfigureAwait(false);
        var stored = new Dictionary<(string, long), Checkpoint>();
        foreach (var (sequence, checkpoint) in found)
        {
            // A delta is compared as what it decodes to, read again with the rows it needs.
            stored[(checkpoint.ThreadId, sequence)] = CheckpointEncoding.IsEncoded(checkpoint.SchemaVersion)
                && await RowAsync(checkpoint.ThreadId, sequence, cancellationToken).ConfigureAwait(false) is { } decoded
                    ? decoded.Row.Checkpoint
                    : checkpoint;
        }

        var again = new List<FlushRow>();
        foreach (var row in rows)
        {
            var verdict = verdicts[row];
            if (verdict.Sequence is not { } sequence)
            {
                Answer(row, verdict);
                continue;
            }

            var next = stored.GetValueOrDefault((row.Save.Checkpoint.ThreadId, sequence));
            switch (StoredCheckpoints.FateOf(row.Save.Checkpoint, next, conditional: row.Save.ExpectedSequence is not null))
            {
                case SaveFate.Saved:
                    Answer(row, new RowVerdict(SaveVerdict.Saved, null));
                    break;
                case SaveFate.Lost:
                    Answer(row, new RowVerdict(SaveVerdict.Lost, null));
                    break;
                default:
                    again.Add(row);
                    break;
            }
        }

        return again;
    }

    public async ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
        await LatestAsync(threadId, cancellationToken).ConfigureAwait(false) is { } latest
            ? new LatestCheckpoint(latest.Checkpoint, latest.Sequence)
            : null;

    public async ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
        (await LatestAsync(threadId, cancellationToken).ConfigureAwait(false))?.Checkpoint;

    public async ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default)
    {
        var cursor = query.Validate();
        var filters = new List<string>();
        var parameters = new List<NpgsqlParameter>();
        string Bind(object value, NpgsqlDbType? type = null)
        {
            var parameter = new NpgsqlParameter { Value = value };
            if (type is { } dbType)
            {
                parameter.NpgsqlDbType = dbType;
            }

            parameters.Add(parameter);
            return $"${parameters.Count}";
        }

        if (query.States is { Count: > 0 } states)
        {
            filters.Add($"h.state = ANY({Bind(states.Select(s => s.Name()).ToArray())})");
        }

        if (query.Metadata.Count > 0)
        {
            filters.Add($"h.metadata @> {Bind(JsonSerializer.Serialize(query.Metadata), NpgsqlDbType.Jsonb)}");
        }

        if (query.UpdatedAfterText is { } after)
        {
            filters.Add($"h.updated_at > {Bind(after, NpgsqlDbType.Text)}");
        }

        if (query.UpdatedBeforeText is { } before)
        {
            filters.Add($"h.updated_at < {Bind(before, NpgsqlDbType.Text)}");
        }

        if (query.DueByText is { } due)
        {
            filters.Add($"h.state = 'interrupted' AND h.wake_at <= {Bind(due, NpgsqlDbType.Text)}");
        }

        if (query.GraphId is { } graphId)
        {
            filters.Add($"h.graph_id = {Bind(graphId, NpgsqlDbType.Text)}");
        }

        if (query.GraphVersion is { } graphVersion)
        {
            filters.Add($"h.graph_version = {Bind(graphVersion, NpgsqlDbType.Text)}");
        }

        if (cursor is not null)
        {
            filters.Add($"(h.updated_at, h.thread_id) < ({Bind(cursor.UpdatedAt, NpgsqlDbType.Text)}, {Bind(cursor.ThreadId, NpgsqlDbType.Text)})");
        }

        // A summary reads no state: of an encoded row it needs only the version it was saved with.
        var sql = new StringBuilder($"""
            SELECT c.thread_id, c.sequence, c.schema_version, c.status, c.suspended_node, c.next_node,
                   NULL::text, c.metadata::text, c.created_at, c.resumed_at, c.nested::text, c.decision::text,
                   c.wait::text, c.graph_id, c.graph_version, h.updated_at,
                   CASE WHEN c.schema_version IN ({CheckpointEncoding.EncodedVersionsSql}) THEN c.frozen_state->>'checkpoint_version' END
            FROM checkpoint_threads h
            JOIN checkpoints c ON c.thread_id = h.thread_id AND c.sequence = h.sequence
            """);
        if (filters.Count > 0)
        {
            sql.Append(" WHERE ").Append(string.Join(" AND ", filters));
        }

        // One more than a page, to know whether another follows.
        sql.Append($" ORDER BY h.updated_at DESC, h.thread_id DESC LIMIT {Bind((long)query.Limit + 1, NpgsqlDbType.Bigint)}");

        var rows = await ReadAsync(
            "list",
            sql.ToString(),
            parameters,
            reader => (Row: ReadListed(reader), UpdatedAt: reader.GetString(15)),
            cancellationToken).ConfigureAwait(false);
        var page = rows.Take(query.Limit).ToList();
        var next = rows.Count > query.Limit
            ? new ThreadCursor(page[^1].UpdatedAt, page[^1].Row.Checkpoint.ThreadId).Encode()
            : null;
        return new ThreadPage([.. page.Select(r => ThreadSummary.Of(r.Row.Sequence, r.Row.Checkpoint))], next);
    }

    public async ValueTask<CheckpointLogPage> GetLogAsync(
        string threadId, long? afterSequence = null, int limit = ThreadQuery.DefaultLimit, CancellationToken cancellationToken = default)
    {
        ThreadQuery.CheckLimit(limit);
        var rows = await ReadAsync(
            "load",
            $"SELECT {CheckpointColumns} FROM checkpoints c WHERE c.thread_id = $1 AND c.sequence > $2 ORDER BY c.sequence LIMIT $3",
            [
                new NpgsqlParameter { Value = threadId, NpgsqlDbType = NpgsqlDbType.Text },
                new NpgsqlParameter { Value = afterSequence ?? -1, NpgsqlDbType = NpgsqlDbType.Bigint },
                new NpgsqlParameter { Value = (long)limit, NpgsqlDbType = NpgsqlDbType.Bigint },
            ],
            reader => ReadCheckpoint(reader, withState: true),
            cancellationToken).ConfigureAwait(false);
        var decoded = await CheckpointEncoding.DecodePageAsync(
            rows,
            async (from, to) => await ReadAsync(
                "load",
                RangeSql,
                [
                    new NpgsqlParameter { Value = threadId, NpgsqlDbType = NpgsqlDbType.Text },
                    new NpgsqlParameter { Value = from, NpgsqlDbType = NpgsqlDbType.Bigint },
                    new NpgsqlParameter { Value = to, NpgsqlDbType = NpgsqlDbType.Bigint },
                ],
                reader => ReadCheckpoint(reader, withState: true),
                cancellationToken).ConfigureAwait(false),
            ReaderFor(threadId, retried: true),
            cancellationToken).ConfigureAwait(false);
        return CheckpointLogPage.For([.. decoded.Select(r => new CheckpointLogEntry(r.Sequence, r.Checkpoint))], limit);
    }

    /// <summary>Makes sure the store is prepared, then runs a read under the retry policy: a read is repeated freely.</summary>
    private async Task<List<T>> ReadAsync<T>(
        string operation,
        string sql,
        IReadOnlyList<NpgsqlParameter> parameters,
        Func<NpgsqlDataReader, T> read,
        CancellationToken cancellationToken)
    {
        await ReadyAsync(cancellationToken).ConfigureAwait(false);
        return await _retry.RunAsync(
                operation,
                (_, token) => new ValueTask<List<T>>(QueryAsync(sql, parameters, read, token)),
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<List<T>> QueryAsync<T>(
        string sql, IReadOnlyList<NpgsqlParameter> parameters, Func<NpgsqlDataReader, T> read, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(sql);
        foreach (var parameter in parameters)
        {
            // A parameter belongs to one command, and an attempt may be one of several.
            command.Parameters.Add(parameter.Clone());
        }

        var rows = new List<T>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows.Add(read(reader));
        }

        return rows;
    }

    /// <summary>A row of a listing: <see cref="CheckpointColumns"/> without the state, with the version it was saved with.</summary>
    private static (long Sequence, Checkpoint Checkpoint) ReadListed(NpgsqlDataReader reader)
    {
        var (sequence, checkpoint) = ReadCheckpoint(reader, withState: false);
        var version = reader.IsDBNull(16) ? null : reader.GetString(16);
        return (sequence, checkpoint with { SchemaVersion = CheckpointEncoding.ListedVersion(checkpoint.SchemaVersion, version) });
    }

    /// <summary>A row of <see cref="CheckpointColumns"/>; the state is skipped where a summary doesn't need it.</summary>
    private static (long Sequence, Checkpoint Checkpoint) ReadCheckpoint(NpgsqlDataReader reader, bool withState)
    {
        string? Text(int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        var metadata = Text(7);
        var nested = Text(10);
        var decision = Text(11);
        var wait = Text(12);
        var graphId = Text(13);
        return (reader.GetInt64(1), new Checkpoint
        {
            ThreadId = reader.GetString(0),
            SchemaVersion = reader.GetString(2),
            Status = CheckpointListing.StatusFromString(reader.GetString(3)),
            SuspendedNode = Text(4),
            NextNode = Text(5),
            FrozenState = withState ? JsonNode.Parse(reader.GetString(6)) : null,
            Metadata = metadata is null ? null : JsonSerializer.Deserialize<Dictionary<string, string>>(metadata),
            CreatedAt = reader.GetString(8),
            ResumedAt = Text(9),
            Nested = nested is null ? null : Checkpoint.FromWireJson(JsonNode.Parse(nested)!),
            Decision = decision is null ? null : HumanDecision.FromWireJson(JsonNode.Parse(decision)!),
            Wait = wait is null ? null : CheckpointWait.FromWireJson(JsonNode.Parse(wait)!),
            Graph = graphId is null ? null : new GraphIdentity(graphId, Text(14)),
        });
    }

    /// <summary>
    /// Deletes the logs of threads that are done with (<c>design/34-Storage-Hardening.md</c>, "Retention"),
    /// oldest first. Each thread is deleted under its advisory lock, and only if its head is still the one that
    /// was picked and no live lease holds it, so a thread that ran again meanwhile is kept. Its blobs go in the same
    /// transaction, or, in another blob store, after it (<c>design/41-Checkpoint-Efficiency.md</c>, "Retention"); a
    /// failure there is the purge's.
    /// </summary>
    public async ValueTask<int> PurgeAsync(
        DateTimeOffset olderThan,
        IReadOnlySet<CheckpointThreadState>? states = null,
        int limit = ThreadQuery.MaxLimit,
        CancellationToken cancellationToken = default)
    {
        ThreadQuery.CheckLimit(limit);
        states ??= CheckpointRetention.Finished;
        if (states.Count == 0)
        {
            return 0;
        }

        var names = states.Select(state => state.Name()).ToArray();
        var before = CheckpointTimestamps.Text(olderThan);
        var candidates = await ReadAsync(
            "purge",
            """
            SELECT h.thread_id, h.sequence FROM checkpoint_threads h
            WHERE h.state = ANY($1) AND h.updated_at < $2
              AND NOT EXISTS (
                  SELECT 1 FROM checkpoint_leases l WHERE l.thread_id = h.thread_id AND l.expires_at > clock_timestamp())
            ORDER BY h.updated_at, h.thread_id LIMIT $3
            """,
            [
                new NpgsqlParameter { Value = names },
                new NpgsqlParameter { Value = before, NpgsqlDbType = NpgsqlDbType.Text },
                new NpgsqlParameter { Value = (long)limit, NpgsqlDbType = NpgsqlDbType.Bigint },
            ],
            reader => (ThreadId: reader.GetString(0), Sequence: reader.GetInt64(1)),
            cancellationToken).ConfigureAwait(false);

        var purged = 0;
        foreach (var (threadId, sequence) in candidates)
        {
            // Purging twice is purging once: a repeat finds the head gone and deletes nothing.
            if (await _retry.RunAsync(
                    "purge",
                    (_, token) => PurgeOneAsync(threadId, sequence, names, before, token),
                    cancellationToken).ConfigureAwait(false))
            {
                purged++;
                _writer?.Forget(threadId);
                if (_blobs is { } blobs)
                {
                    await blobs.DeleteThreadAsync(threadId, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        return purged;
    }

    private async ValueTask<bool> PurgeOneAsync(
        string threadId, long sequence, string[] states, string before, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await PostgresSchema.LockAsync(connection, transaction, "checkpoints", threadId, cancellationToken).ConfigureAwait(false);

        await using var head = new NpgsqlCommand(
            """
            DELETE FROM checkpoint_threads h
            WHERE h.thread_id = @thread_id AND h.sequence = @sequence AND h.state = ANY(@states) AND h.updated_at < @before
              AND NOT EXISTS (
                  SELECT 1 FROM checkpoint_leases l WHERE l.thread_id = h.thread_id AND l.expires_at > clock_timestamp())
            """,
            connection,
            transaction);
        head.Parameters.Add(PostgresSchema.Param("thread_id", NpgsqlDbType.Text, threadId));
        head.Parameters.Add(PostgresSchema.Param("sequence", NpgsqlDbType.Bigint, sequence));
        head.Parameters.Add(new NpgsqlParameter("states", states));
        head.Parameters.Add(PostgresSchema.Param("before", NpgsqlDbType.Text, before));
        if (await head.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 0)
        {
            return false;
        }

        foreach (var table in (string[])["checkpoints", "checkpoint_leases", "checkpoint_blobs"])
        {
            // Both names are the store's own constants, not input.
            await using var rows = new NpgsqlCommand($"DELETE FROM {table} WHERE thread_id = @thread_id", connection, transaction);
            rows.Parameters.Add(PostgresSchema.Param("thread_id", NpgsqlDbType.Text, threadId));
            await rows.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>Whether saves go through group commit.</summary>
    internal bool GroupCommits => _group is not null;

    /// <summary>Whether running checkpoints are stored as deltas.</summary>
    internal bool WritesDeltas => _writer?.WritesDeltas == true;

    /// <summary>Whether large values are offloaded, and where to when not to this database.</summary>
    internal (bool On, ICheckpointBlobStore? Blobs) Offloads => (_writer?.Offloads == true, _blobs);

    /// <summary>Flushes the saves group commit has queued, then lets the pool go if the store owns it.</summary>
    public void Dispose()
    {
        _group?.DrainAsync().AsTask().GetAwaiter().GetResult();
        _preparation.Dispose();
        if (_ownsDataSource)
        {
            _dataSource.Dispose();
        }
    }

    /// <summary>Flushes the saves group commit has queued, then lets the pool go if the store owns it.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_group is not null)
        {
            await _group.DrainAsync().ConfigureAwait(false);
        }

        _preparation.Dispose();
        if (_ownsDataSource)
        {
            await _dataSource.DisposeAsync().ConfigureAwait(false);
        }
    }
}
