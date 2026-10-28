using System.Data.Common;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace Datalium.Strata.Storage.Memory;

/// <summary>Options of <see cref="EfCoreCheckpointStore"/>. Mirrors <c>strata-memory</c>'s <c>SqliteStoreOptions</c>.</summary>
public sealed record SqliteStoreOptions
{
    /// <summary>
    /// Flushes saves that arrive while another flush runs together, in one transaction and one sync to disk
    /// (<c>design/41-Checkpoint-Efficiency.md</c>). Off when <see langword="null"/>, the default. A SQLite store
    /// runs one flush at a time, whatever <see cref="GroupCommitOptions.MaxConcurrentFlushes"/> says.
    /// </summary>
    public GroupCommitOptions? GroupCommit { get; init; }

    /// <summary>
    /// Stores a <c>running</c> checkpoint as the changes to the thread's previous row, with a whole row every
    /// <see cref="DeltaOptions.FullEvery"/> (<c>design/41-Checkpoint-Efficiency.md</c>, "Deltas"). Off when
    /// <see langword="null"/>, the default. A store reads rows stored this way whatever its own options say.
    /// </summary>
    public DeltaOptions? Deltas { get; init; }

    /// <summary>
    /// Stores each top-level value of a level's state whose JSON is at least <see cref="OffloadOptions.Threshold"/>
    /// bytes once, in a blob store, and refers to it (<c>design/41-Checkpoint-Efficiency.md</c>, "Offload"). Off when
    /// <see langword="null"/>, the default. The blobs are read from where these options say, whatever the threshold:
    /// the store's own database when <see cref="OffloadOptions.Blobs"/> is <see langword="null"/>.
    /// </summary>
    public OffloadOptions? Offload { get; init; }

    /// <summary>For tests: called as a group-commit flush starts (<c>"flush:start"</c>), where it may wait.</summary>
    internal Func<string, ValueTask>? Fault { get; init; }
}

/// <summary>
/// SQLite/EF Core <see cref="ICheckpointStore"/> adapter, the persistent counterpart to
/// <see cref="InMemoryCheckpointStore"/> (per <c>design/03-Architecture.md</c>'s "in-memory ring
/// buffer -&gt; SQLite/RocksDB -&gt; Redis/PostgreSQL" progression). Async with EF Core's async API
/// (<c>design/19-Async-Queryable-Store.md</c>); a <see cref="SemaphoreSlim"/> serializes calls, since a
/// <see cref="DbContext"/> isn't thread-safe and runs sharing one store call it concurrently. The
/// synchronous members wrap the async ones.
///
/// The database is a language-agnostic <b>append-only checkpoint log</b> (see
/// <c>schema/README.md</c>, "SQLite checkpoint log"): <see cref="SaveAsync"/> adds a row at the next
/// <c>sequence</c> for its thread, with each <see cref="Checkpoint"/> field in its own column
/// (<c>frozen_state</c> and <c>metadata</c> as JSON text), and <see cref="LoadAsync"/> returns the
/// highest-sequence row. Beside it, the <c>checkpoint_threads</c> head table, kept current by a
/// trigger, is what <see cref="ListThreadsAsync"/> reads. It's the same layout <c>strata-memory</c>'s
/// <c>SqliteCheckpointStore</c> creates, so a file this store wrote can be resumed by the Rust runtime
/// and inspected by the CLI, and vice versa.
///
/// Several writers: calls through one store are serialized, and SQLite serializes writes to one file (a
/// write waits for the file's lock up to the connection's busy timeout). A save computes the next sequence
/// in the same statement that inserts it, so two writers on one thread can't corrupt its log. The engine's
/// conditional saves (<see cref="SaveIfLatestAsync"/>: a resume's stamp or claim, ending a thread) and leased
/// saves (<see cref="SaveUnderLeaseAsync"/>, <c>design/32-Thread-Leases.md</c>) settle such a race: the
/// loser writes nothing and is told so (<see langword="false"/>, <see cref="LeaseLostException"/>). A plain
/// <see cref="SaveAsync"/> that loses it collides on the primary key and throws
/// <see cref="SqliteException"/>, not retried; run workers on one thread under leases, as on any store.
/// One file suits the processes of one machine; for many writers, use Postgres.
/// </summary>
public sealed class EfCoreCheckpointStore : ICheckpointStore, ICheckpointQuery, ICheckpointRetention, IDisposable
{
    /// <summary>
    /// The canonical <c>checkpoints</c> table. Identical to <c>strata-memory</c>'s
    /// <c>CHECKPOINT_LOG_DDL</c>; each language's tests compare the created table against
    /// <c>schema/fixtures/checkpoint-log/columns.json</c>.
    /// </summary>
    public const string CheckpointLogDdl = """
        CREATE TABLE IF NOT EXISTS checkpoints (
            thread_id      TEXT NOT NULL,
            sequence       INTEGER NOT NULL,
            schema_version TEXT NOT NULL,
            status         TEXT NOT NULL,
            suspended_node TEXT,
            next_node      TEXT,
            frozen_state   TEXT NOT NULL,
            metadata       TEXT,
            created_at     TEXT NOT NULL,
            resumed_at     TEXT,
            nested         TEXT,
            decision       TEXT,
            wait           TEXT,
            wake_at        TEXT,
            graph_id       TEXT,
            graph_version  TEXT,
            PRIMARY KEY (thread_id, sequence)
        )
        """;

    /// <summary>
    /// The head table listing threads reads, its indexes, and the trigger that keeps it current
    /// whoever writes the log (<c>design/19-Async-Queryable-Store.md</c>, "SQL: a trigger-maintained
    /// head table"). Identical to <c>strata-memory</c>'s <c>CHECKPOINT_THREADS_DDL</c>; pinned by
    /// <c>schema/fixtures/checkpoint-log/threads-columns.json</c> and <c>threads.rows.json</c>.
    /// </summary>
    public const string CheckpointThreadsDdl = """
        CREATE TABLE IF NOT EXISTS checkpoint_threads (
            thread_id  TEXT NOT NULL PRIMARY KEY,
            sequence   INTEGER NOT NULL,
            state      TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            metadata      TEXT,
            wake_at       TEXT,
            graph_id      TEXT,
            graph_version TEXT
        );
        CREATE INDEX IF NOT EXISTS checkpoint_threads_by_state
            ON checkpoint_threads (state, updated_at, thread_id);
        CREATE INDEX IF NOT EXISTS checkpoint_threads_by_updated
            ON checkpoint_threads (updated_at, thread_id);
        CREATE INDEX IF NOT EXISTS checkpoint_threads_by_wake
            ON checkpoint_threads (wake_at);
        CREATE INDEX IF NOT EXISTS checkpoint_threads_by_graph
            ON checkpoint_threads (graph_id, updated_at, thread_id);
        CREATE TRIGGER IF NOT EXISTS checkpoints_head AFTER INSERT ON checkpoints
        BEGIN
            INSERT INTO checkpoint_threads (thread_id, sequence, state, updated_at, metadata, wake_at, graph_id, graph_version)
            VALUES (NEW.thread_id, NEW.sequence,
                    CASE WHEN NEW.resumed_at IS NOT NULL THEN 'resumed' ELSE NEW.status END,
                    COALESCE(NEW.resumed_at, NEW.created_at), NEW.metadata, NEW.wake_at, NEW.graph_id, NEW.graph_version)
            ON CONFLICT (thread_id) DO UPDATE SET
                sequence = excluded.sequence,
                state = excluded.state,
                updated_at = excluded.updated_at,
                metadata = excluded.metadata,
                wake_at = excluded.wake_at,
                graph_id = excluded.graph_id,
                graph_version = excluded.graph_version
            WHERE excluded.sequence > checkpoint_threads.sequence;
        END;
        """;

    /// <summary>Fills a just-created <c>checkpoint_threads</c> from the log it was added to. Identical to <c>CHECKPOINT_THREADS_BACKFILL</c>.</summary>
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
    /// until the thread is purged, so <c>token</c> only grows. A thread is held while <c>expires_at</c> (Unix milliseconds on
    /// this store's clock) is set and in the future; a release clears it and <c>owner</c>. Identical to
    /// <c>strata-memory</c>'s <c>CHECKPOINT_LEASES_DDL</c>; pinned by
    /// <c>schema/fixtures/checkpoint-log/leases-columns.json</c>.
    /// </summary>
    public const string CheckpointLeasesDdl = """
        CREATE TABLE IF NOT EXISTS checkpoint_leases (
            thread_id  TEXT NOT NULL PRIMARY KEY,
            owner      TEXT,
            token      INTEGER NOT NULL,
            expires_at INTEGER
        )
        """;

    /// <summary>
    /// The blobs of value offload (<c>design/41-Checkpoint-Efficiency.md</c>, "Offload"): each value a store
    /// offloaded to its own database, by thread and <c>sha256:</c> hash, as its JSON text. The checkpoint
    /// component's schema version 2. Identical to <c>strata-memory</c>'s <c>CHECKPOINT_BLOBS_DDL</c>; pinned by
    /// <c>schema/fixtures/checkpoint-log/blobs-columns.json</c>.
    /// </summary>
    public const string CheckpointBlobsDdl = """
        CREATE TABLE IF NOT EXISTS checkpoint_blobs (
            thread_id TEXT NOT NULL,
            hash      TEXT NOT NULL,
            content   BLOB NOT NULL,
            PRIMARY KEY (thread_id, hash)
        )
        """;

    /// <summary>The columns every read selects, in this order.</summary>
    private const string CheckpointColumns =
        "c.thread_id, c.sequence, c.schema_version, c.status, c.suspended_node, c.next_node, " +
        "c.frozen_state, c.metadata, c.created_at, c.resumed_at, c.nested, c.decision, c.wait, c.graph_id, c.graph_version";

    /// <summary>How many times an acquisition looks again at a lease that changed while it was being read.</summary>
    private const int AcquireAttempts = 3;

    /// <summary><c>SQLITE_CONSTRAINT</c>: another writer took the sequence first.</summary>
    private const int SqlitePrimaryKeyViolation = 19;

    private readonly CheckpointDbContext _db;
    private readonly SqliteConnection? _ownedConnection;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly TimeProvider _clock;
    private readonly GroupCommitQueue? _group;
    private readonly CheckpointWriter? _writer;

    /// <summary>Where offloaded values go when not in this database.</summary>
    private readonly ICheckpointBlobStore? _blobs;
    private readonly Func<string, ValueTask>? _fault;

    /// <param name="timeProvider">
    /// The clock thread leases expire on (<c>design/32-Thread-Leases.md</c>); the system clock when not given.
    /// SQLite has no clock of its own that every process sharing the file would agree on better than this.
    /// </param>
    /// <param name="options">Group commit and deltas (<c>design/41-Checkpoint-Efficiency.md</c>); both off when not given.</param>
    public EfCoreCheckpointStore(
        CheckpointDbContext db,
        SqliteConnection? ownedConnection = null,
        TimeProvider? timeProvider = null,
        SqliteStoreOptions? options = null)
        : this(db, ownedConnection, timeProvider, laterSteps: [], options)
    {
    }

    /// <summary>For tests: a store that also knows <paramref name="laterSteps"/>, as a later build would.</summary>
    internal EfCoreCheckpointStore(
        CheckpointDbContext db,
        SqliteConnection? ownedConnection,
        TimeProvider? timeProvider,
        IReadOnlyList<SchemaStep> laterSteps,
        SqliteStoreOptions? options = null)
    {
        _db = db;
        _ownedConnection = ownedConnection;
        _clock = timeProvider ?? TimeProvider.System;
        _fault = options?.Fault;
        if (options?.GroupCommit is { } groupCommit)
        {
            // One connection, one writer: one flush at a time, whatever the options say.
            _group = new GroupCommitQueue(groupCommit, "sqlite", FlushAsync, isTransient: _ => false, maxFlushes: 1);
        }

        _writer = CheckpointWriter.For(options?.Deltas, options?.Offload);
        _blobs = options?.Offload?.Blobs;
        try
        {
            Migrate(laterSteps);
        }
        catch
        {
            _ownedConnection?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Brings the checkpoint component up to date in one immediate transaction, so concurrent first opens
    /// can't interleave (<c>design/34-Storage-Hardening.md</c>, "Migrations"): reads its version from
    /// <c>strata_schema_version</c> (no row: 0), refuses a file newer than this build, at 0 runs the baseline
    /// (<see cref="PrepareLog"/>), then each later step the file hasn't had (this build's, then
    /// <paramref name="laterSteps"/>), and records the version reached.
    /// </summary>
    private void Migrate(IReadOnlyList<SchemaStep> laterSteps)
    {
        IReadOnlyList<SchemaStep> steps = [.. StoreSchemaVersions.SqliteCheckpointSteps, .. laterSteps];
        var known = steps.Max(step => step.Version);

        // Microsoft.Data.Sqlite begins a non-deferred transaction with BEGIN IMMEDIATE.
        using var transaction = _db.Database.BeginTransaction();
        _db.Database.ExecuteSqlRaw(StoreSchemaVersions.SqliteDdl);
        var found = _db.Database
            .SqlQueryRaw<long>("SELECT version AS Value FROM strata_schema_version WHERE component = {0}", StoreSchemaVersions.Checkpoints)
            .AsEnumerable()
            .FirstOrDefault();
        if (found > known)
        {
            throw new InvalidOperationException(StoreSchemaVersions.Newer(StoreSchemaVersions.Checkpoints, found, known));
        }

        if (found == known)
        {
            transaction.Commit();
            return;
        }

        if (found == 0)
        {
            PrepareLog();
            found = StoreSchemaVersions.BaselineVersion;
        }

        foreach (var step in steps.Where(step => step.Version > found).OrderBy(step => step.Version))
        {
            foreach (var statement in step.Statements)
            {
                _db.Database.ExecuteSqlRaw(statement);
            }
        }

        _db.Database.ExecuteSqlRaw(
            "INSERT INTO strata_schema_version (component, version) VALUES ({0}, {1}) " +
            "ON CONFLICT (component) DO UPDATE SET version = excluded.version",
            StoreSchemaVersions.Checkpoints,
            known);
        transaction.Commit();
    }

    /// <summary>
    /// Version 1 of the checkpoint component (<see cref="StoreSchemaVersions"/>), inside <see cref="Migrate"/>'s
    /// transaction: creates the log and its head table, or brings an older file up to date. It adds the <c>nested</c> column a log from
    /// before nested checkpoints lacks (<c>design/16-Nested-Interrupts.md</c>), and the <c>decision</c>
    /// column one from before recorded decisions lacks (<c>design/21-Typed-Human-Signals.md</c>), and the
    /// <c>wait</c> and <c>wake_at</c> columns one from before waits lacks (<c>design/29-Event-Waits-And-Timers.md</c>),
    /// and the <c>graph_id</c> and <c>graph_version</c> columns one from before graph identities lacks
    /// (<c>design/31-Graph-Identity.md</c>); creates and backfills <c>checkpoint_threads</c> on a log that predates it
    /// (<c>design/19-Async-Queryable-Store.md</c>, "Migration"), or adds the columns it lacks and replaces
    /// its trigger on one from before waits or identities; and creates <c>checkpoint_leases</c> on one from
    /// before leases (<c>design/32-Thread-Leases.md</c>). A no-op on a current file. Not <c>EnsureCreated()</c>: that skips table creation
    /// whenever the database already has <i>any</i> table, and would emit its own DDL rather than the
    /// shared layout.
    /// </summary>
    private void PrepareLog()
    {
        _db.Database.ExecuteSqlRaw(CheckpointLogDdl);
        foreach (var column in (string[])["nested", "decision", "wait", "wake_at", "graph_id", "graph_version"])
        {
            AddMissingColumn("checkpoints", column);
        }

        if (Count("SELECT COUNT(*) AS Value FROM sqlite_master WHERE type = 'table' AND name = 'checkpoint_threads'") == 0)
        {
            _db.Database.ExecuteSqlRaw(CheckpointThreadsDdl);
            _db.Database.ExecuteSqlRaw(CheckpointThreadsBackfill);
        }
        else
        {
            var added = false;
            foreach (var column in (string[])["wake_at", "graph_id", "graph_version"])
            {
                added |= AddMissingColumn("checkpoint_threads", column);
            }

            if (added)
            {
                // A head table from before waits or identities: its trigger doesn't copy the new columns.
                // Nothing in it waits with a time or has an identity, so every row's NULL is right.
                _db.Database.ExecuteSqlRaw("DROP TRIGGER IF EXISTS checkpoints_head");
                _db.Database.ExecuteSqlRaw(CheckpointThreadsDdl);
            }
        }

        _db.Database.ExecuteSqlRaw(CheckpointLeasesDdl);
    }

    /// <summary>Adds <paramref name="column"/> to <paramref name="table"/> as <c>TEXT</c> if it lacks it; says whether it did.</summary>
    private bool AddMissingColumn(string table, string column)
    {
        if (Count($"SELECT COUNT(*) AS Value FROM pragma_table_info('{table}') WHERE name = '{column}'") > 0)
        {
            return false;
        }

        // Both names are the store's own constants, not input.
        var alter = $"ALTER TABLE {table} ADD COLUMN {column} TEXT";
        _db.Database.ExecuteSqlRaw(alter);
        return true;
    }

    private int Count(string sql) => _db.Database.SqlQueryRaw<int>(sql).AsEnumerable().Single();

    /// <summary>
    /// A self-contained in-memory SQLite database for tests: owns and keeps open the
    /// <see cref="SqliteConnection"/> a SQLite in-memory database needs to stay alive for
    /// (closing the last connection to a <c>:memory:</c> database deletes it), and disposes it
    /// alongside the <see cref="CheckpointDbContext"/> in <see cref="Dispose"/>.
    /// </summary>
    public static EfCoreCheckpointStore CreateInMemory(TimeProvider? timeProvider = null, SqliteStoreOptions? options = null)
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var contextOptions = new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite(connection).Options;
        return new EfCoreCheckpointStore(new CheckpointDbContext(contextOptions), connection, timeProvider, options);
    }

    /// <summary>Synchronous <see cref="SaveAsync"/>.</summary>
    public void Save(Checkpoint checkpoint) => SaveAsync(checkpoint).AsTask().GetAwaiter().GetResult();

    /// <summary>Synchronous <see cref="LoadAsync"/>.</summary>
    public Checkpoint? Load(string threadId) => LoadAsync(threadId).AsTask().GetAwaiter().GetResult();

    /// <summary>Appends <paramref name="checkpoint"/> to the log at the next sequence for its thread; <see cref="LoadAsync"/> then returns it as the latest.</summary>
    public async ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default)
    {
        if (_group is { } group)
        {
            await group.SaveAsync(checkpoint, expectedSequence: null, lease: null, cancellationToken).ConfigureAwait(false);
            return;
        }

        await InsertAsync(checkpoint, expectedSequence: null, token: null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// A save outside group commit, in one statement: <paramref name="checkpoint"/> at the next sequence for its
    /// thread, only if that follows <paramref name="expectedSequence"/> and the lease token is still
    /// <paramref name="token"/>, when given; stored as a delta of the thread's latest row, or with its large values
    /// offloaded, when the store can, and whole, with every blob written again, when the row it relied on is no
    /// longer the latest. Blobs in this database are written in the row's transaction; blobs elsewhere, before it.
    /// Returns the sequence it was appended at, or <see langword="null"/>. Another process writing the same row
    /// first counts as losing a conditional save.
    /// </summary>
    private async ValueTask<long?> InsertAsync(
        Checkpoint checkpoint, long? expectedSequence, long? token, CancellationToken cancellationToken)
    {
        var save = _writer?.Prepare(checkpoint) ?? EncodedSave.Plain(checkpoint);
        if (_blobs is { } blobs)
        {
            await OffloadedBlobs.PutAsync(blobs, checkpoint.ThreadId, save.Blobs, cancellationToken).ConfigureAwait(false);
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _db.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var connection = (SqliteConnection)_db.Database.GetDbConnection();
                var (sequence, bytes) = await WriteRowAsync(connection, null, save, expectedSequence, token, cancellationToken)
                    .ConfigureAwait(false);
                if (sequence is null && save.Base is not null)
                {
                    save = save.Whole();
                    if (_blobs is { } elsewhere)
                    {
                        await OffloadedBlobs.PutAsync(elsewhere, checkpoint.ThreadId, save.Blobs, cancellationToken).ConfigureAwait(false);
                    }

                    (sequence, bytes) = await WriteRowAsync(connection, null, save, expectedSequence, token, cancellationToken)
                        .ConfigureAwait(false);
                }

                if (sequence is { } saved)
                {
                    StorageTelemetry.Stored("sqlite", save, bytes);
                    _writer?.Saved(save, saved, save.Stored.CreatedAt);
                }
                else
                {
                    _writer?.Forget(checkpoint.ThreadId);
                }

                return sequence;
            }
            catch (SqliteException ex) when (expectedSequence is not null && ex.SqliteErrorCode == SqlitePrimaryKeyViolation)
            {
                _writer?.Forget(checkpoint.ThreadId);
                return null;
            }
            finally
            {
                await _db.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// <see cref="InsertRowAsync"/>, then, when the row was appended and its blobs are in this database, the blobs it
    /// writes, in one immediate transaction (<paramref name="transaction"/>'s, when given).
    /// </summary>
    private async ValueTask<(long? Sequence, long Bytes)> WriteRowAsync(
        SqliteConnection connection, SqliteTransaction? transaction, EncodedSave save, long? expectedSequence, long? token,
        CancellationToken cancellationToken)
    {
        if (_blobs is not null || save.Blobs.Count == 0)
        {
            return await InsertRowAsync(connection, transaction, save, expectedSequence, token, cancellationToken)
                .ConfigureAwait(false);
        }

        using var own = transaction is null ? connection.BeginTransaction(deferred: false) : null;
        var inserted = await InsertRowAsync(connection, transaction ?? own, save, expectedSequence, token, cancellationToken)
            .ConfigureAwait(false);
        if (inserted.Sequence is not null)
        {
            await InsertBlobsAsync(connection, (transaction ?? own)!, save, cancellationToken).ConfigureAwait(false);
        }

        own?.Commit();
        return inserted;
    }

    /// <summary>The blobs <paramref name="save"/> writes, into <c>checkpoint_blobs</c>; one the thread has already is left as it is.</summary>
    private static async ValueTask InsertBlobsAsync(
        SqliteConnection connection, SqliteTransaction transaction, EncodedSave save, CancellationToken cancellationToken)
    {
        foreach (var blob in save.Blobs)
        {
            await using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = "INSERT OR IGNORE INTO checkpoint_blobs (thread_id, hash, content) VALUES ($thread, $hash, $content)";
            insert.Parameters.AddWithValue("$thread", save.Checkpoint.ThreadId);
            insert.Parameters.AddWithValue("$hash", blob.Hash);
            insert.Parameters.AddWithValue("$content", blob.Content.ToArray());
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The insert of a save: at the next sequence, only if that follows <paramref name="expectedSequence"/>, the
    /// lease token is still <paramref name="token"/> and, for a save that relies on the row before it, that row is
    /// still the latest, by its sequence and its <c>created_at</c>, as far as each is given. Returns the sequence it
    /// was appended at, if it was, and the bytes it holds.
    /// </summary>
    private static async ValueTask<(long? Sequence, long Bytes)> InsertRowAsync(
        SqliteConnection connection, SqliteTransaction? transaction, EncodedSave save, long? expectedSequence, long? token,
        CancellationToken cancellationToken)
    {
        var checkpoint = save.Stored;
        var frozenState = checkpoint.FrozenState?.ToJsonString() ?? "null";
        var metadata = checkpoint.Metadata is null ? null : JsonSerializer.Serialize(checkpoint.Metadata);
        var nested = checkpoint.Nested?.ToWireJson().ToJsonString();
        var decision = checkpoint.Decision?.ToWireJson().ToJsonString();
        var wait = checkpoint.Wait?.ToWireJson().ToJsonString();
        await using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = """
            INSERT INTO checkpoints
                (thread_id, sequence, schema_version, status, suspended_node, next_node,
                 frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                 graph_id, graph_version)
            SELECT $thread, latest.sequence + 1, $schema_version, $status, $suspended_node, $next_node,
                   $frozen_state, $metadata, $created_at, $resumed_at, $nested, $decision, $wait, $wake_at,
                   $graph_id, $graph_version
            FROM (SELECT COALESCE(MAX(sequence), -1) AS sequence FROM checkpoints WHERE thread_id = $thread) latest
            WHERE ($expected IS NULL OR latest.sequence = $expected)
              AND ($token IS NULL OR (SELECT token FROM checkpoint_leases WHERE thread_id = $thread) = $token)
              AND ($base IS NULL OR (latest.sequence = $base AND EXISTS (
                  SELECT 1 FROM checkpoints b WHERE b.thread_id = $thread AND b.sequence = $base AND b.created_at = $base_created)))
            RETURNING sequence
            """;
        void Bind(string name, object? value) => insert.Parameters.AddWithValue(name, value ?? DBNull.Value);
        Bind("$thread", checkpoint.ThreadId);
        Bind("$schema_version", checkpoint.SchemaVersion);
        Bind("$status", CheckpointListing.StatusToString(checkpoint.Status));
        Bind("$suspended_node", checkpoint.SuspendedNode);
        Bind("$next_node", checkpoint.NextNode);
        Bind("$frozen_state", frozenState);
        Bind("$metadata", metadata);
        Bind("$created_at", checkpoint.CreatedAt);
        Bind("$resumed_at", checkpoint.ResumedAt);
        Bind("$nested", nested);
        Bind("$decision", decision);
        Bind("$wait", wait);
        Bind("$wake_at", checkpoint.InnermostWait()?.WakeAt);
        Bind("$graph_id", checkpoint.Graph?.Id);
        Bind("$graph_version", checkpoint.Graph?.Version);
        Bind("$expected", expectedSequence);
        Bind("$token", token);
        Bind("$base", save.Base?.Sequence);
        Bind("$base_created", save.Base?.Token);
        var sequence = await insert.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as long?;
        return (sequence, StoredBytes(frozenState, metadata, nested, decision, wait));
    }

    /// <summary>The bytes of the JSON a row holds: what <c>strata.store.checkpoint.size</c> records.</summary>
    private static long StoredBytes(params string?[] parts) =>
        parts.Sum(part => part is null ? 0L : Encoding.UTF8.GetByteCount(part));

    /// <summary>
    /// Group commit's flush (<c>design/41-Checkpoint-Efficiency.md</c>): every save of <paramref name="saves"/>
    /// in one immediate transaction, so one commit, and one sync to disk, for all of them. Each row reads its
    /// thread's latest sequence and lease token and gets the verdict a single save would (a lost lease first,
    /// then a moved sequence); the immediate transaction keeps every other writer out until the commit.
    /// </summary>
    private async ValueTask FlushAsync(IReadOnlyList<QueuedSave> saves)
    {
        if (_fault is { } fault)
        {
            await fault("flush:start").ConfigureAwait(false);
        }

        // A thread with two saves in one flush has both written whole, as in the Postgres store. Blobs elsewhere are
        // all written before the transaction, so a row written whole in it needs none that isn't there.
        var alone = saves.GroupBy(save => save.Checkpoint.ThreadId, StringComparer.Ordinal)
            .Where(thread => thread.Count() == 1)
            .Select(thread => thread.Key)
            .ToHashSet(StringComparer.Ordinal);
        var encoded = saves
            .Select(save => _writer?.Prepare(save.Checkpoint, alone.Contains(save.Checkpoint.ThreadId), writeAllBlobs: _blobs is not null)
                ?? EncodedSave.Plain(save.Checkpoint))
            .ToList();
        if (_blobs is { } blobs)
        {
            foreach (var save in encoded)
            {
                await OffloadedBlobs.PutAsync(blobs, save.Checkpoint.ThreadId, save.Blobs, CancellationToken.None).ConfigureAwait(false);
            }
        }

        var verdicts = new List<(QueuedSave Save, SaveVerdict Verdict, long Bytes, EncodedSave Written, long Sequence)>(saves.Count);
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            await _db.Database.OpenConnectionAsync().ConfigureAwait(false);
            try
            {
                var connection = (SqliteConnection)_db.Database.GetDbConnection();
                using var transaction = connection.BeginTransaction(deferred: false);
                for (var i = 0; i < saves.Count; i++)
                {
                    var (verdict, bytes, written, sequence) =
                        await FlushRowAsync(connection, transaction, saves[i], encoded[i]).ConfigureAwait(false);
                    verdicts.Add((saves[i], verdict, bytes, written, sequence));
                }

                transaction.Commit();
            }
            finally
            {
                await _db.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _gate.Release();
        }

        foreach (var (save, verdict, bytes, written, sequence) in verdicts)
        {
            if (verdict == SaveVerdict.Saved)
            {
                StorageTelemetry.Stored("sqlite", written, bytes);
                _writer?.Saved(written, sequence, written.Stored.CreatedAt);
            }
            else if (verdict == SaveVerdict.Lost)
            {
                _writer?.Forget(save.Checkpoint.ThreadId);
            }

            save.Settle(verdict);
        }
    }

    /// <summary>
    /// One row of <see cref="FlushAsync"/>, inside its transaction: its verdict, and, when it was saved, the bytes
    /// it holds, what it was written as (whole when the row it relies on is no longer the latest) and where.
    /// </summary>
    private async ValueTask<(SaveVerdict Verdict, long Bytes, EncodedSave Written, long Sequence)> FlushRowAsync(
        SqliteConnection connection, SqliteTransaction transaction, QueuedSave save, EncodedSave encoded)
    {
        var checkpoint = save.Checkpoint;
        var threadId = checkpoint.ThreadId;
        long latest;
        long? token;
        string? latestCreated;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText =
                "SELECT (SELECT COALESCE(MAX(sequence), -1) FROM checkpoints WHERE thread_id = $thread), " +
                "(SELECT token FROM checkpoint_leases WHERE thread_id = $thread), " +
                "(SELECT created_at FROM checkpoints WHERE thread_id = $thread ORDER BY sequence DESC LIMIT 1)";
            read.Parameters.AddWithValue("$thread", threadId);
            await using var reader = await read.ExecuteReaderAsync().ConfigureAwait(false);
            await reader.ReadAsync().ConfigureAwait(false);
            latest = reader.GetInt64(0);
            token = reader.IsDBNull(1) ? null : reader.GetInt64(1);
            latestCreated = reader.IsDBNull(2) ? null : reader.GetString(2);
        }

        if (save.Lease is { } lease && token != lease.Token)
        {
            return (SaveVerdict.LeaseLost, 0, encoded, -1);
        }

        if (save.ExpectedSequence is { } expected && latest != expected)
        {
            return (SaveVerdict.Lost, 0, encoded, -1);
        }

        // Under the immediate transaction nothing else can append: the insert lands at latest + 1.
        var written = encoded.Base is { } basis && (basis.Sequence != latest || basis.Token != latestCreated)
            ? encoded.Whole()
            : encoded;
        var (sequence, bytes) = await WriteRowAsync(connection, transaction, written, null, null, CancellationToken.None)
            .ConfigureAwait(false);
        return (SaveVerdict.Saved, bytes, written, sequence ?? latest + 1);
    }

    /// <summary>
    /// Appends <paramref name="checkpoint"/> at <paramref name="expectedSequence"/> + 1 only if that is still
    /// its thread's latest sequence, in one statement (<c>design/28-Ending-A-Thread.md</c>, "Races"). Another
    /// process writing the same row first counts as losing.
    /// </summary>
    public async ValueTask<bool> SaveIfLatestAsync(
        Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default)
    {
        if (expectedSequence is not { } expected)
        {
            await SaveAsync(checkpoint, cancellationToken).ConfigureAwait(false);
            return true;
        }

        if (_group is { } group)
        {
            return await group.SaveAsync(checkpoint, expected, lease: null, cancellationToken).ConfigureAwait(false);
        }

        return await InsertAsync(checkpoint, expected, token: null, cancellationToken).ConfigureAwait(false) is not null;
    }

    /// <summary>
    /// Appends <paramref name="checkpoint"/> only if <paramref name="lease"/>'s token is still its thread's
    /// latest and, with an <paramref name="expectedSequence"/>, only if that is still the thread's latest
    /// sequence: one statement, so no acquisition can come between the check and the insert
    /// (<c>design/32-Thread-Leases.md</c>, "Fencing"). When nothing is inserted the token is read again to say
    /// which condition failed: a token never goes back, so one that still matches means the sequence didn't.
    /// </summary>
    public async ValueTask<bool> SaveUnderLeaseAsync(
        Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default)
    {
        ThreadLeases.CheckThread(checkpoint, lease);
        if (_group is { } group)
        {
            return await group.SaveAsync(checkpoint, expectedSequence, lease, cancellationToken).ConfigureAwait(false);
        }

        var token = lease.Token;
        if (await InsertAsync(checkpoint, expectedSequence, token, cancellationToken).ConfigureAwait(false) is not null)
        {
            return true;
        }

        var current = await ReadAsync(
            "SELECT token FROM checkpoint_leases WHERE thread_id = $p1", [checkpoint.ThreadId], reader => reader.GetInt64(0), cancellationToken)
            .ConfigureAwait(false);
        return current.Count == 1 && current[0] == token && expectedSequence is not null
            ? false
            : throw new LeaseLostException(lease);
    }

    /// <summary>
    /// One upsert: the thread's first lease at token 1, or its next token when the lease it has isn't held;
    /// no row back means it is held, and a second statement reads by whom (<c>design/32-Thread-Leases.md</c>).
    /// </summary>
    public async ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
        string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ThreadLeases.Check(owner, ttl);
        for (var attempt = 1; ; attempt++)
        {
            var now = _clock.GetUtcNow().ToUnixTimeMilliseconds();
            var expiresAt = now + ThreadLeases.Milliseconds(ttl);
            var acquired = await ReadAsync(
                """
                INSERT INTO checkpoint_leases (thread_id, owner, token, expires_at) VALUES ($p1, $p2, 1, $p3)
                ON CONFLICT (thread_id) DO UPDATE
                    SET owner = excluded.owner, token = checkpoint_leases.token + 1, expires_at = excluded.expires_at
                    WHERE checkpoint_leases.expires_at IS NULL OR checkpoint_leases.expires_at <= $p4
                RETURNING token
                """,
                [threadId, owner, expiresAt, now],
                reader => reader.GetInt64(0),
                cancellationToken).ConfigureAwait(false);
            if (acquired.Count == 1)
            {
                return new LeaseAttempt(
                    new ThreadLease(threadId, owner, acquired[0], DateTimeOffset.FromUnixTimeMilliseconds(expiresAt)), null);
            }

            if (await GetLeaseAsync(threadId, cancellationToken).ConfigureAwait(false) is { } holder)
            {
                return new LeaseAttempt(null, holder);
            }

            // Released or expired between the two statements: look again.
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
        var expiresAt = _clock.GetUtcNow().ToUnixTimeMilliseconds() + ThreadLeases.Milliseconds(ttl);
        var renewed = await ReadAsync(
            "UPDATE checkpoint_leases SET owner = $p1, expires_at = $p2 WHERE thread_id = $p3 AND token = $p4 RETURNING token",
            [lease.Owner, expiresAt, lease.ThreadId, lease.Token],
            reader => reader.GetInt64(0),
            cancellationToken).ConfigureAwait(false);
        return renewed.Count == 1 ? lease with { ExpiresAt = DateTimeOffset.FromUnixTimeMilliseconds(expiresAt) } : null;
    }

    public async ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        var released = await ReadAsync(
            "UPDATE checkpoint_leases SET owner = NULL, expires_at = NULL WHERE thread_id = $p1 AND token = $p2 RETURNING token",
            [lease.ThreadId, lease.Token],
            reader => reader.GetInt64(0),
            cancellationToken).ConfigureAwait(false);
        return released.Count == 1;
    }

    public async ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken cancellationToken = default)
    {
        var held = await ReadAsync(
            "SELECT owner, token, expires_at FROM checkpoint_leases WHERE thread_id = $p1 AND expires_at > $p2",
            [threadId, _clock.GetUtcNow().ToUnixTimeMilliseconds()],
            reader => new ThreadLease(
                threadId, reader.GetString(0), reader.GetInt64(1), DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(2))),
            cancellationToken).ConfigureAwait(false);
        return held.Count == 1 ? held[0] : null;
    }

    public async ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
        await LatestAsync(threadId, cancellationToken).ConfigureAwait(false) is { } latest
            ? new LatestCheckpoint(latest.Checkpoint, latest.Sequence)
            : null;

    public async ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
        (await LatestAsync(threadId, cancellationToken).ConfigureAwait(false))?.Checkpoint;

    /// <summary>
    /// The depth of the row <c>l</c>: how many rows before it a load reads with it
    /// (<c>design/41-Checkpoint-Efficiency.md</c>, "Deltas").
    /// </summary>
    private const string DepthSql =
        $"CASE WHEN l.schema_version IN ({CheckpointEncoding.EncodedVersionsSql}) THEN CAST(json_extract(l.frozen_state, '$.depth') AS INTEGER) ELSE 0 END";

    /// <summary>A thread's latest row and the rows before it that it is a delta of, oldest first: one read.</summary>
    private const string LatestChainSql = $"""
        WITH target AS (
            SELECT l.sequence, {DepthSql} AS depth FROM checkpoints l
            WHERE l.thread_id = $p1 ORDER BY l.sequence DESC LIMIT 1)
        SELECT {CheckpointColumns} FROM checkpoints c, target
        WHERE c.thread_id = $p1 AND c.sequence BETWEEN target.sequence - target.depth AND target.sequence
        ORDER BY c.sequence
        """;

    /// <summary>The rows of a thread from <c>$p2</c> to <c>$p3</c>, oldest first: what a page of its log is a delta of.</summary>
    private const string RangeSql =
        $"SELECT {CheckpointColumns} FROM checkpoints c WHERE c.thread_id = $p1 AND c.sequence BETWEEN $p2 AND $p3 ORDER BY c.sequence";

    /// <summary>A thread's latest row, decoded; the store then knows it as the thread's latest.</summary>
    private async ValueTask<(long Sequence, Checkpoint Checkpoint)?> LatestAsync(string threadId, CancellationToken cancellationToken)
    {
        var rows = await ReadAsync(LatestChainSql, [threadId], reader => ReadCheckpoint(reader, withState: true), cancellationToken)
            .ConfigureAwait(false);
        if (rows.Count == 0)
        {
            return null;
        }

        var depth = CheckpointEncoding.DepthOf(rows[^1].Checkpoint, rows[^1].Sequence);
        var latest = (await CheckpointEncoding.DecodeAsync(rows, rows.Count - 1, ReaderFor(threadId), cancellationToken)
            .ConfigureAwait(false))[0];
        _writer?.Loaded(latest.Sequence, depth, latest, latest.Checkpoint.CreatedAt);
        return (latest.Sequence, latest.Checkpoint);
    }

    /// <summary>
    /// How <paramref name="threadId"/>'s offloaded values are read: from the blob store the options name, or from
    /// this database's <c>checkpoint_blobs</c>.
    /// </summary>
    private BlobReader ReaderFor(string threadId) => _blobs is { } blobs
        ? (hashes, cancellationToken) => OffloadedBlobs.ReadAsync(blobs, threadId, hashes, cancellationToken)
        : async (hashes, cancellationToken) =>
        {
            var marks = string.Join(", ", hashes.Select((_, i) => $"$p{i + 2}"));
            var found = await ReadAsync(
                $"SELECT hash, content FROM checkpoint_blobs WHERE thread_id = $p1 AND hash IN ({marks})",
                [threadId, .. hashes],
                reader => (Hash: reader.GetString(0), Content: (byte[])reader.GetValue(1)),
                cancellationToken).ConfigureAwait(false);
            return found.ToDictionary(blob => blob.Hash, blob => (ReadOnlyMemory<byte>)blob.Content, StringComparer.Ordinal);
        };

    public async ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default)
    {
        var cursor = query.Validate();
        var filters = new List<string>();
        var parameters = new List<object>();
        string Bind(object value)
        {
            parameters.Add(value);
            return $"$p{parameters.Count}";
        }

        if (query.States is { Count: > 0 } states)
        {
            filters.Add($"h.state IN ({string.Join(", ", states.Select(state => Bind(state.Name())))})");
        }

        foreach (var (key, value) in query.Metadata)
        {
            filters.Add($"EXISTS (SELECT 1 FROM json_each(h.metadata) j WHERE j.key = {Bind(key)} AND j.value = {Bind(value)})");
        }

        if (query.UpdatedAfterText is { } after)
        {
            filters.Add($"h.updated_at > {Bind(after)}");
        }

        if (query.UpdatedBeforeText is { } before)
        {
            filters.Add($"h.updated_at < {Bind(before)}");
        }

        if (query.DueByText is { } due)
        {
            filters.Add($"h.state = 'interrupted' AND h.wake_at <= {Bind(due)}");
        }

        if (query.GraphId is { } graphId)
        {
            filters.Add($"h.graph_id = {Bind(graphId)}");
        }

        if (query.GraphVersion is { } graphVersion)
        {
            filters.Add($"h.graph_version = {Bind(graphVersion)}");
        }

        if (cursor is not null)
        {
            filters.Add($"(h.updated_at, h.thread_id) < ({Bind(cursor.UpdatedAt)}, {Bind(cursor.ThreadId)})");
        }

        // A summary reads no state: of an encoded row it needs only the version it was saved with.
        var sql = new StringBuilder($"""
            SELECT {CheckpointColumns}, h.updated_at,
                   CASE WHEN c.schema_version IN ({CheckpointEncoding.EncodedVersionsSql}) THEN json_extract(c.frozen_state, '$.checkpoint_version') END
            FROM checkpoint_threads h
            JOIN checkpoints c ON c.thread_id = h.thread_id AND c.sequence = h.sequence
            """);
        if (filters.Count > 0)
        {
            sql.Append(" WHERE ").Append(string.Join(" AND ", filters));
        }

        // One more than a page, to know whether another follows.
        sql.Append($" ORDER BY h.updated_at DESC, h.thread_id DESC LIMIT {Bind(query.Limit + 1)}");

        var rows = await ReadAsync(sql.ToString(), parameters, reader => (Row: ReadListed(reader), UpdatedAt: reader.GetString(15)), cancellationToken)
            .ConfigureAwait(false);
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
            $"SELECT {CheckpointColumns} FROM checkpoints c WHERE c.thread_id = $p1 AND c.sequence > $p2 ORDER BY c.sequence LIMIT $p3",
            [threadId, afterSequence ?? -1, limit],
            reader => ReadCheckpoint(reader, withState: true),
            cancellationToken).ConfigureAwait(false);
        var decoded = await CheckpointEncoding.DecodePageAsync(
            rows,
            async (from, to) => await ReadAsync(
                RangeSql, [threadId, from, to], reader => ReadCheckpoint(reader, withState: true), cancellationToken)
                .ConfigureAwait(false),
            ReaderFor(threadId),
            cancellationToken).ConfigureAwait(false);
        return CheckpointLogPage.For([.. decoded.Select(r => new CheckpointLogEntry(r.Sequence, r.Checkpoint))], limit);
    }

    /// <summary>Runs <paramref name="sql"/> with <c>$p1</c>, <c>$p2</c>, … bound to <paramref name="parameters"/>.</summary>
    private async Task<List<T>> ReadAsync<T>(
        string sql, IReadOnlyList<object> parameters, Func<DbDataReader, T> read, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _db.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await using var command = _db.Database.GetDbConnection().CreateCommand();
                command.CommandText = sql;
                for (var i = 0; i < parameters.Count; i++)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = $"$p{i + 1}";
                    parameter.Value = parameters[i];
                    command.Parameters.Add(parameter);
                }

                var rows = new List<T>();
                await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    rows.Add(read(reader));
                }

                return rows;
            }
            finally
            {
                await _db.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>A row of a listing: <see cref="CheckpointColumns"/> without the state, with the version it was saved with.</summary>
    private static (long Sequence, Checkpoint Checkpoint) ReadListed(DbDataReader reader)
    {
        var (sequence, checkpoint) = ReadCheckpoint(reader, withState: false);
        var version = reader.IsDBNull(16) ? null : reader.GetString(16);
        return (sequence, checkpoint with { SchemaVersion = CheckpointEncoding.ListedVersion(checkpoint.SchemaVersion, version) });
    }

    /// <summary>A row of <see cref="CheckpointColumns"/>; the state is skipped where a summary doesn't need it.</summary>
    private static (long Sequence, Checkpoint Checkpoint) ReadCheckpoint(DbDataReader reader, bool withState)
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
    /// oldest first, in one transaction: each thread's checkpoints, its head row, its lease row and its blobs. A
    /// thread a live lease holds (on this store's clock) is kept. Blobs in another blob store are deleted after the
    /// transaction (<c>design/41-Checkpoint-Efficiency.md</c>, "Retention"); a failure there is the purge's.
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

        // The names are the engine's own, not input.
        var named = string.Join(", ", states.Select(state => $"'{state.Name()}'"));
        var pick = $$"""
            SELECT h.thread_id AS Value FROM checkpoint_threads h
            WHERE h.state IN ({{named}}) AND h.updated_at < {0}
              AND NOT EXISTS (SELECT 1 FROM checkpoint_leases l WHERE l.thread_id = h.thread_id AND l.expires_at > {1})
            ORDER BY h.updated_at, h.thread_id LIMIT {2}
            """;

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var transaction = await _db.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            var threads = await _db.Database
                .SqlQueryRaw<string>(pick, CheckpointTimestamps.Text(olderThan), _clock.GetUtcNow().ToUnixTimeMilliseconds(), limit)
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);
            foreach (var threadId in threads)
            {
                foreach (var table in (string[])["checkpoints", "checkpoint_threads", "checkpoint_leases", "checkpoint_blobs"])
                {
                    // The table names are the store's own constants.
                    var delete = $"DELETE FROM {table} WHERE thread_id = {{0}}";
                    await _db.Database.ExecuteSqlRawAsync(delete, [threadId], cancellationToken).ConfigureAwait(false);
                }
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            foreach (var threadId in threads)
            {
                _writer?.Forget(threadId);
                if (_blobs is { } blobs)
                {
                    await blobs.DeleteThreadAsync(threadId, cancellationToken).ConfigureAwait(false);
                }
            }

            return threads.Count;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Whether saves go through group commit.</summary>
    internal bool GroupCommits => _group is not null;

    /// <summary>Whether running checkpoints are stored as deltas.</summary>
    internal bool WritesDeltas => _writer?.WritesDeltas == true;

    /// <summary>Whether large values are offloaded, and where to when not to this database.</summary>
    internal (bool On, ICheckpointBlobStore? Blobs) Offloads => (_writer?.Offloads == true, _blobs);

    /// <summary>Flushes the saves group commit has queued, then closes the database.</summary>
    public void Dispose()
    {
        _group?.DrainAsync().AsTask().GetAwaiter().GetResult();
        _db.Dispose();
        _ownedConnection?.Dispose();
        _gate.Dispose();
    }
}
