using System.Net.Sockets;
using Npgsql;
using NpgsqlTypes;

namespace Datalium.Strata.Storage.Postgres;

/// <summary>
/// Options of the Postgres stores (<c>design/34-Storage-Hardening.md</c>). Mirrors <c>strata-memory</c>'s
/// <c>PostgresStoreOptions</c>.
/// </summary>
public sealed record PostgresStoreOptions
{
    /// <summary>How a call that failed for a transient reason is repeated. <see cref="StoreRetryPolicy.None"/> turns it off.</summary>
    public StoreRetryPolicy Retry { get; init; } = new();

    /// <summary>
    /// Whether the store brings its tables up to date the first time it is used (the default). When
    /// <see langword="false"/> it only checks them, and refuses a database that is behind: a deploy step then
    /// runs <c>MigrateAsync</c>.
    /// </summary>
    public bool AutoMigrate { get; init; } = true;

    /// <summary>The clock for the waits between attempts. <see langword="null"/>: <see cref="System.TimeProvider.System"/>.</summary>
    public TimeProvider? TimeProvider { get; init; }

    /// <summary>
    /// Flushes saves that arrive while another flush runs together, in one transaction
    /// (<c>design/41-Checkpoint-Efficiency.md</c>). Off when <see langword="null"/>, the default. The history
    /// store ignores it.
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
    /// the store's own database when <see cref="OffloadOptions.Blobs"/> is <see langword="null"/>. The history store ignores it.
    /// </summary>
    public OffloadOptions? Offload { get; init; }

    /// <summary>
    /// For tests: called at named points of a call (<c>"save:before-commit"</c>, <c>"save:commit"</c>,
    /// <c>"save:after-commit"</c>, <c>"save:recheck"</c>, <c>"lease:after-commit"</c>,
    /// <c>"append:after-commit"</c>), where it may throw what a lost connection would.
    /// </summary>
    internal Func<string, ValueTask>? Fault { get; init; }

    /// <summary>For tests: schema steps after the ones this build has, as a later build would add them.</summary>
    internal IReadOnlyList<SchemaStep> LaterSteps { get; init; } = [];
}

/// <summary>Which Postgres failures a store repeats a call for (<c>design/34-Storage-Hardening.md</c>, "Retries").</summary>
internal static class PostgresFaults
{
    // Shutdown and startup, serialization failure, deadlock, too many connections. Class 08 is checked by prefix.
    private static readonly HashSet<string> TransientStates = ["57P01", "57P02", "57P03", "40001", "40P01", "53300"];

    public static bool IsTransient(Exception exception) => exception switch
    {
        PostgresException server =>
            server.SqlState.StartsWith("08", StringComparison.Ordinal) || TransientStates.Contains(server.SqlState),

        // A connection that broke under a call: Npgsql wraps the I/O error.
        NpgsqlException client =>
            client.InnerException is IOException or SocketException or TimeoutException,
        IOException or SocketException or TimeoutException => true,
        _ => false,
    };
}

/// <summary>Plumbing shared by <see cref="PostgresCheckpointStore"/> and <see cref="PostgresHistoryStore"/>.</summary>
internal static class PostgresSchema
{
    /// <summary>
    /// Held while a store creates or migrates its tables. <c>CREATE TABLE IF NOT EXISTS</c> is not safe to run
    /// from two sessions at once in Postgres (the loser can fail on <c>pg_type</c>'s unique index instead of
    /// being a no-op), and opening two stores -- or one from each runtime -- at the same moment is
    /// exactly what happens on a shared database. Must equal <c>DDL_LOCK_KEY</c> in
    /// <c>strata-memory</c>'s <c>postgres_store.rs</c> so both runtimes take the same lock.
    /// </summary>
    private const long DdlLockKey = 0x5354_5241_5441; // "STRATA"

    /// <summary>
    /// Brings <paramref name="component"/>'s tables up to date under the DDL lock, in one transaction
    /// (<c>design/34-Storage-Hardening.md</c>, "Migrations"): reads its version from
    /// <c>strata_schema_version</c> (no row: 0); refuses a database newer than this build; at 0 runs
    /// <paramref name="baseline"/>, which creates the tables or adds what an older layout lacks and is safe
    /// on any database; then runs each of <paramref name="steps"/> the database hasn't had, in order; and
    /// records the version it reached. With <paramref name="apply"/> off it changes nothing, and refuses a
    /// database that is behind. <paramref name="afterwards"/>, when given, then runs in the same transaction under
    /// the same lock, whatever the version was, told whether it may change anything: for what a store needs beyond
    /// its component's version (the memory store's own table and the dimensions it was made with).
    /// </summary>
    /// <exception cref="InvalidOperationException">The database is newer than this build, or behind and not to be changed.</exception>
    public static async Task MigrateAsync(
        NpgsqlDataSource dataSource,
        string component,
        long baselineVersion,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task> baseline,
        IReadOnlyList<SchemaStep> steps,
        bool apply,
        CancellationToken cancellationToken,
        Func<NpgsqlConnection, NpgsqlTransaction, bool, CancellationToken, Task>? afterwards = null)
    {
        var known = steps.Count == 0 ? baselineVersion : steps.Max(step => step.Version);

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using (var ddlLock = new NpgsqlCommand("SELECT pg_advisory_xact_lock(@key)", connection, transaction))
        {
            ddlLock.Parameters.Add(new NpgsqlParameter("key", NpgsqlDbType.Bigint) { Value = DdlLockKey });
            await ddlLock.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await ExecuteAsync(connection, transaction, StoreSchemaVersions.PostgresDdl, cancellationToken).ConfigureAwait(false);
        long found;
        await using (var read = new NpgsqlCommand(
            "SELECT version FROM strata_schema_version WHERE component = @component", connection, transaction))
        {
            read.Parameters.Add(Param("component", NpgsqlDbType.Text, component));
            found = await read.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is long version ? version : 0;
        }

        if (found > known)
        {
            throw new InvalidOperationException(StoreSchemaVersions.Newer(component, found, known));
        }

        if (found < known)
        {
            if (!apply)
            {
                // Rolled back with the transaction: a store that doesn't migrate changes nothing.
                throw new InvalidOperationException(StoreSchemaVersions.Behind(component, found, known));
            }

            await UpgradeAsync(connection, transaction, component, found, known, baselineVersion, baseline, steps, cancellationToken)
                .ConfigureAwait(false);
        }

        if (afterwards is not null)
        {
            await afterwards(connection, transaction, apply, cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Runs the baseline (at 0) and the steps a database at <paramref name="found"/> hasn't had, and records <paramref name="known"/>.</summary>
    private static async Task UpgradeAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string component,
        long found,
        long known,
        long baselineVersion,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task> baseline,
        IReadOnlyList<SchemaStep> steps,
        CancellationToken cancellationToken)
    {
        if (found == 0)
        {
            await baseline(connection, transaction, cancellationToken).ConfigureAwait(false);
            found = baselineVersion;
        }

        foreach (var step in steps.Where(step => step.Version > found).OrderBy(step => step.Version))
        {
            foreach (var statement in step.Statements)
            {
                await ExecuteAsync(connection, transaction, statement, cancellationToken).ConfigureAwait(false);
            }
        }

        await using var record = new NpgsqlCommand(
            """
            INSERT INTO strata_schema_version (component, version) VALUES (@component, @version)
            ON CONFLICT (component) DO UPDATE SET version = EXCLUDED.version
            """,
            connection,
            transaction);
        record.Parameters.Add(Param("component", NpgsqlDbType.Text, component));
        record.Parameters.Add(Param("version", NpgsqlDbType.Bigint, known));
        await record.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public static async Task ExecuteAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public static async Task<bool> QueryAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        return (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
    }

    /// <summary>
    /// Takes a transaction-scoped advisory lock keyed by <paramref name="scope"/> and <paramref name="key"/>
    /// (e.g. <c>"checkpoints"</c> and a thread id) so concurrent writers to one thread or session serialize
    /// instead of picking the same next <c>sequence</c>. The hash is computed server-side, so both runtimes
    /// contend on the same lock.
    /// </summary>
    public static async Task LockAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string scope, string key, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(hashtextextended(@scope || ':' || @key, 0))", connection, transaction);
        command.Parameters.Add(new NpgsqlParameter("scope", NpgsqlDbType.Text) { Value = scope });
        command.Parameters.Add(new NpgsqlParameter("key", NpgsqlDbType.Text) { Value = key });
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public static NpgsqlParameter Param(string name, NpgsqlDbType type, object? value) =>
        new(name, type) { Value = value ?? DBNull.Value };
}

/// <summary>
/// A Postgres store's preparation, done once, on first use or when asked
/// (<c>design/34-Storage-Hardening.md</c>, "Async everywhere"): no constructor touches the database. A
/// preparation that fails is tried again by the next call. Its attempts are counted as
/// <paramref name="operation"/> retries; <paramref name="afterwards"/> is
/// <see cref="PostgresSchema.MigrateAsync"/>'s.
/// </summary>
internal sealed class PostgresPreparation(
    NpgsqlDataSource dataSource,
    PostgresStoreOptions options,
    StoreRetrier retry,
    string component,
    long baselineVersion,
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task> baseline,
    IReadOnlyList<SchemaStep> steps,
    string operation = "migrate",
    Func<NpgsqlConnection, NpgsqlTransaction, bool, CancellationToken, Task>? afterwards = null) : IDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private volatile bool _done;

    /// <summary>Makes sure the tables are current: as the options say on first use, or always changing them when <paramref name="migrate"/>.</summary>
    public async ValueTask EnsureAsync(bool migrate, CancellationToken cancellationToken)
    {
        if (_done && !migrate)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_done && !migrate)
            {
                return;
            }

            await retry.RunAsync(
                    operation,
                    (_, token) => new ValueTask(PostgresSchema.MigrateAsync(
                        dataSource,
                        component,
                        baselineVersion,
                        baseline,
                        [.. steps, .. options.LaterSteps],
                        migrate || options.AutoMigrate,
                        token,
                        afterwards)),
                    cancellationToken)
                .ConfigureAwait(false);
            _done = true;
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose() => _gate.Dispose();
}
