using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Storage;

/// <summary>
/// How a Postgres or Redis store repeats a call that failed for a transient reason
/// (<c>design/34-Storage-Hardening.md</c>, "Retries"): a dropped connection, a server shutting down or
/// starting, a deadlock or a serialization failure, a timeout. Each store decides what is transient and what
/// repeating a call means; a save that may have happened is looked up before it is made again. Mirrors
/// <c>strata-memory</c>'s <c>StoreRetry</c>.
/// </summary>
public sealed record StoreRetryPolicy
{
    /// <summary>How many times a call is made at most, the first included. 1 never repeats. At least 1.</summary>
    public int MaxAttempts { get; init; } = 4;

    /// <summary>The wait before the first repeat, doubled before each later one. Not negative.</summary>
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromMilliseconds(50);

    /// <summary>The longest wait between two attempts. Not negative.</summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>No retries: every failure is the call's, as before this policy existed.</summary>
    public static StoreRetryPolicy None { get; } = new() { MaxAttempts = 1 };

    /// <summary>
    /// The wait before repeat number <paramref name="repeat"/> (from 1): <see cref="InitialDelay"/> doubled
    /// <paramref name="repeat"/> − 1 times, capped at <see cref="MaxDelay"/>, then jittered over the whole of
    /// it by <paramref name="sample"/> (0 to 1), so stores that failed together don't come back together.
    /// </summary>
    public TimeSpan DelayBefore(int repeat, double sample)
    {
        var doubled = InitialDelay.TotalMilliseconds * Math.Pow(2, Math.Max(0, repeat - 1));
        var capped = Math.Min(doubled, MaxDelay.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(capped * Math.Clamp(sample, 0, 1));
    }

    internal StoreRetryPolicy Checked()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxAttempts, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(InitialDelay, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxDelay, TimeSpan.Zero);
        return this;
    }
}

/// <summary>
/// Runs a store's calls under its <see cref="StoreRetryPolicy"/>: an attempt that throws something
/// <paramref name="isTransient"/> says is transient is made again after the policy's wait, on
/// <paramref name="clock"/>, until the attempts run out. A cancelled token is never a transient failure, and
/// stops a wait.
/// </summary>
internal sealed class StoreRetrier(
    StoreRetryPolicy policy, TimeProvider? clock, string store, Func<Exception, bool> isTransient)
{
    private readonly StoreRetryPolicy _policy = policy.Checked();
    private readonly TimeProvider _clock = clock ?? TimeProvider.System;

    /// <summary>Whether <paramref name="exception"/> is one this store repeats a call for.</summary>
    public bool IsTransient(Exception exception) => exception is not OperationCanceledException && isTransient(exception);

    /// <summary>
    /// Makes <paramref name="attempt"/> (given its number, from 1) until it returns, fails for good, or the
    /// attempts run out. <paramref name="unless"/> names transient failures that must not be repeated: what
    /// an attempt can't safely do twice.
    /// </summary>
    public async ValueTask<T> RunAsync<T>(
        string operation,
        Func<int, CancellationToken, ValueTask<T>> attempt,
        CancellationToken cancellationToken,
        Func<Exception, bool>? unless = null)
    {
        for (var number = 1; ; number++)
        {
            try
            {
                return await attempt(number, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (
                number < _policy.MaxAttempts
                && !cancellationToken.IsCancellationRequested
                && IsTransient(ex)
                && unless?.Invoke(ex) != true)
            {
                StorageTelemetry.Retried(store, operation, number, ex);
                var delay = _policy.DelayBefore(number, Random.Shared.NextDouble());
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, _clock, cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary><see cref="RunAsync{T}"/> for a call with no result.</summary>
    public async ValueTask RunAsync(
        string operation,
        Func<int, CancellationToken, ValueTask> attempt,
        CancellationToken cancellationToken,
        Func<Exception, bool>? unless = null) =>
        await RunAsync(
            operation,
            async (number, token) =>
            {
                await attempt(number, token).ConfigureAwait(false);
                return true;
            },
            cancellationToken,
            unless).ConfigureAwait(false);
}

/// <summary>
/// What became of a save whose attempt may or may not have been written
/// (<c>design/34-Storage-Hardening.md</c>, "A save that may have happened").
/// </summary>
internal enum SaveFate
{
    /// <summary>The checkpoint is in the log where the save would have put it.</summary>
    Saved,

    /// <summary>A conditional save's place was taken by another checkpoint: it lost the race.</summary>
    Lost,

    /// <summary>Nothing was written: the save is made again.</summary>
    NotWritten,
}

/// <summary>Comparing checkpoints as stored, for the stores' re-check of a save that may have happened.</summary>
internal static class StoredCheckpoints
{
    /// <summary>
    /// Whether <paramref name="stored"/> is <paramref name="saved"/> as a store gives it back: the same wire
    /// JSON, keys in any order (a <c>JSONB</c> column doesn't keep them).
    /// </summary>
    public static bool Same(Checkpoint saved, Checkpoint? stored) =>
        stored is not null && JsonNode.DeepEquals(saved.ToWireJson(), stored.ToWireJson());

    /// <summary>
    /// What a look at the log says about a save of <paramref name="saved"/> that may have happened: for a
    /// conditional one, <paramref name="next"/> is the entry right after its expected sequence; for a plain
    /// one, the thread's latest.
    /// </summary>
    public static SaveFate FateOf(Checkpoint saved, Checkpoint? next, bool conditional) =>
        Same(saved, next) ? SaveFate.Saved
        : conditional && next is not null ? SaveFate.Lost
        : SaveFate.NotWritten;
}

/// <summary>
/// The storage adapters' telemetry (<c>design/34-Storage-Hardening.md</c>, "Telemetry"): a meter named
/// <see cref="Name"/>, apart from the engine's <c>Datalium.Strata</c>.
/// </summary>
public static class StorageTelemetry
{
    /// <summary>The meter's name: <c>Datalium.Strata.Storage</c>.</summary>
    public const string Name = "Datalium.Strata.Storage";

    private static readonly Meter Meter = new(Name);

    private static readonly Counter<long> Retries = Meter.CreateCounter<long>(
        "strata.store.retries", "{attempt}", "Store calls repeated after a transient failure, by store and operation.");

    private static readonly Histogram<int> FlushSize = Meter.CreateHistogram<int>(
        "strata.store.flush.size", "{checkpoint}", "Saves per group-commit flush, by store.");

    private static readonly Histogram<double> FlushWait = Meter.CreateHistogram<double>(
        "strata.store.flush.wait", "s", "How long a save waited for its group-commit flush to start, by store.");

    private static readonly Histogram<long> CheckpointSize = Meter.CreateHistogram<long>(
        "strata.store.checkpoint.size", "By", "The JSON a store wrote for each saved checkpoint, by store and encoding.");

    private static readonly Counter<long> BlobCount = Meter.CreateCounter<long>(
        "strata.store.blobs", "{blob}", "Values a store offloaded to its blob store, by store; a blob it already had isn't counted.");

    private static readonly Histogram<long> BlobSize = Meter.CreateHistogram<long>(
        "strata.store.blob.size", "By", "The JSON of each value a store offloaded, by store.");

    private static readonly Counter<long> MemoryOperations = Meter.CreateCounter<long>(
        "strata.memory.operations", "{operation}", "Calls to a memory store, by store and operation, failed ones included.");

    private static readonly Histogram<double> MemoryOperationDuration = Meter.CreateHistogram<double>(
        "strata.memory.operation.duration", "s", "How long a memory store's calls took, retries included, by store and operation.");

    private static readonly Histogram<int> MemorySearchResults = Meter.CreateHistogram<int>(
        "strata.memory.search.results", "{match}", "The matches each memory search returned, by store.");

    /// <summary>
    /// Records one call to a memory store (<c>design/45-Long-Term-Memory.md</c>, "Telemetry"):
    /// <paramref name="operation"/> is <c>upsert</c>, <c>get</c>, <c>delete</c>, <c>search</c>, <c>list</c> or
    /// <c>purge</c>.
    /// </summary>
    internal static void MemoryOperation(string store, string operation, TimeSpan elapsed)
    {
        var storeTag = new KeyValuePair<string, object?>("strata.store", store);
        var operationTag = new KeyValuePair<string, object?>("strata.memory.operation", operation);
        MemoryOperations.Add(1, storeTag, operationTag);
        MemoryOperationDuration.Record(elapsed.TotalSeconds, storeTag, operationTag);
    }

    /// <summary>Records how many matches a memory search returned.</summary>
    internal static void MemorySearched(string store, int results) =>
        MemorySearchResults.Record(results, new KeyValuePair<string, object?>("strata.store", store));

    /// <summary>Records a flush starting: its size, and how long each of its saves waited for it.</summary>
    internal static void Flushing(string store, IReadOnlyList<QueuedSave> batch)
    {
        var tag = new KeyValuePair<string, object?>("strata.store", store);
        FlushSize.Record(batch.Count, tag);
        var now = Stopwatch.GetTimestamp();
        foreach (var save in batch)
        {
            FlushWait.Record(Stopwatch.GetElapsedTime(save.ArrivedAt, now).TotalSeconds, tag);
        }
    }

    /// <summary>
    /// Records the size of a checkpoint a store wrote, as <paramref name="encoding"/> (<c>plain</c>, <c>delta</c>,
    /// <c>offloaded</c> or <c>delta_offloaded</c>).
    /// </summary>
    internal static void Stored(string store, string encoding, long bytes) =>
        CheckpointSize.Record(
            bytes,
            new KeyValuePair<string, object?>("strata.store", store),
            new KeyValuePair<string, object?>("strata.store.encoding", encoding));

    /// <summary>Records <paramref name="save"/>'s row and the blobs it wrote with it.</summary>
    internal static void Stored(string store, EncodedSave save, long bytes)
    {
        Stored(store, save.Encoding, bytes);
        var tag = new KeyValuePair<string, object?>("strata.store", store);
        foreach (var blob in save.Blobs)
        {
            BlobCount.Add(1, tag);
            BlobSize.Record(blob.Content.Length, tag);
        }
    }

    /// <summary>
    /// Counts a repeat, and notes it on the current activity: the operation, the attempt that failed and the
    /// failure's type, never its message, which can hold a connection string.
    /// </summary>
    internal static void Retried(string store, string operation, int attempt, Exception failure)
    {
        Retries.Add(
            1,
            new KeyValuePair<string, object?>("strata.store", store),
            new KeyValuePair<string, object?>("strata.store.operation", operation));
        Activity.Current?.AddEvent(new ActivityEvent("store_retry", tags: new ActivityTagsCollection
        {
            ["strata.store"] = store,
            ["strata.store.operation"] = operation,
            ["strata.store.attempt"] = attempt,
            ["error.type"] = failure.GetType().FullName,
        }));
    }
}

/// <summary>
/// What each SQL database's <c>strata_schema_version</c> table says
/// (<c>design/34-Storage-Hardening.md</c>, "Migrations"): one row per component with the version its tables
/// are at. Version 1 is the layout every store brought an older database to before the table existed; later
/// changes are numbered steps. Both engines and the CLI read and write the same rows; pinned by
/// <c>schema/fixtures/checkpoint-log/schema-versions.json</c>. Mirrors <c>strata-memory</c>'s
/// <c>schema_versions</c>.
/// </summary>
public static class StoreSchemaVersions
{
    /// <summary>The table's name.</summary>
    public const string Table = "strata_schema_version";

    /// <summary>The component of the checkpoint log, its head table and the leases.</summary>
    public const string Checkpoints = "checkpoints";

    /// <summary>The component of the history log.</summary>
    public const string History = "history";

    /// <summary>
    /// The checkpoint component's version in a database this build has prepared: 2 since value offload's
    /// <c>checkpoint_blobs</c> table (<c>design/41-Checkpoint-Efficiency.md</c>, "Wire format").
    /// </summary>
    public const long CheckpointsVersion = 2;

    /// <summary>The version a component's baseline brings a database to; later versions are steps.</summary>
    internal const long BaselineVersion = 1;

    /// <summary>The checkpoint component's steps after the baseline in SQLite.</summary>
    internal static readonly IReadOnlyList<SchemaStep> SqliteCheckpointSteps =
    [
        new(2, "checkpoint_blobs", [Storage.Memory.EfCoreCheckpointStore.CheckpointBlobsDdl]),
    ];

    /// <summary>The checkpoint component's steps after the baseline in Postgres.</summary>
    internal static readonly IReadOnlyList<SchemaStep> PostgresCheckpointSteps =
    [
        new(2, "checkpoint_blobs", [Postgres.PostgresCheckpointStore.CheckpointBlobsDdl]),
    ];

    /// <summary>The history component's version in a database this build has prepared.</summary>
    public const long HistoryVersion = 1;

    /// <summary>
    /// The component of the long-term memory tables (<c>design/45-Long-Term-Memory.md</c>, "Schema version and
    /// migration"): one row for every memory table in the database, whatever its name.
    /// </summary>
    public const string Memory = "memory";

    /// <summary>The memory component's version in a database this build has prepared.</summary>
    public const long MemoryVersion = 1;

    /// <summary>The memory component's steps after the baseline in Postgres: none yet.</summary>
    internal static readonly IReadOnlyList<SchemaStep> PostgresMemorySteps = [];

    /// <summary>The table in SQLite. Identical to <c>strata-memory</c>'s <c>SCHEMA_VERSION_DDL</c>.</summary>
    public const string SqliteDdl = """
        CREATE TABLE IF NOT EXISTS strata_schema_version (
            component TEXT NOT NULL PRIMARY KEY,
            version   INTEGER NOT NULL
        )
        """;

    /// <summary>The table in Postgres. Identical to <c>strata-memory</c>'s <c>POSTGRES_SCHEMA_VERSION_DDL</c>.</summary>
    public const string PostgresDdl = """
        CREATE TABLE IF NOT EXISTS strata_schema_version (
            component TEXT NOT NULL PRIMARY KEY,
            version   BIGINT NOT NULL
        )
        """;

    /// <summary>Why a database at <paramref name="found"/> can't be used by a build that knows <paramref name="known"/>.</summary>
    public static string Newer(string component, long found, long known) =>
        $"database is at {component} schema version {found}, and this build knows {known}: upgrade the runtime";

    /// <summary>Why a store that doesn't migrate refuses a database at <paramref name="found"/>.</summary>
    public static string Behind(string component, long found, long needed) =>
        $"database is at {component} schema version {found}, and this build needs {needed}: run its migration";
}

/// <summary>
/// A numbered change to a component's tables after version 1 (<see cref="StoreSchemaVersions"/>): the
/// statements that take a database from <c>Version − 1</c> to <see cref="Version"/>. Each must be safe to run
/// again: a step whose version wasn't recorded is run again. The first is version 2, value offload's
/// <c>checkpoint_blobs</c>.
/// </summary>
internal sealed record SchemaStep(long Version, string Name, IReadOnlyList<string> Statements);
