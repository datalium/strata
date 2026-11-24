namespace Datalium.Strata.Storage;

/// <summary>
/// What the read-only checkpoint stores share (<c>design/51-Read-Only-Store-Open.md</c>): the messages they refuse with,
/// pinned by <c>schema/fixtures/checkpoint-log/read-only-errors.json</c> and the same in both engines and the CLI, the
/// versions they read, and the blob directory they need.
/// </summary>
internal static class ReadOnlyStores
{
    /// <summary>A save of any kind: <c>SaveAsync</c>, <c>SaveIfLatestAsync</c> and <c>SaveUnderLeaseAsync</c>.</summary>
    public const string Save = "save";

    public const string AcquireLease = "acquire lease";

    public const string RenewLease = "renew lease";

    public const string ReleaseLease = "release lease";

    public const string Purge = "purge";

    /// <summary>A Postgres store's <c>MigrateAsync</c>.</summary>
    public const string Migrate = "migrate";

    /// <summary>A Redis store's <c>RebuildIndexAsync</c>.</summary>
    public const string RebuildIndex = "rebuild index";

    /// <summary>The refusal of a write on a store opened read-only, before it reaches the database.</summary>
    public static InvalidOperationException Refused(string operation) =>
        new($"the store was opened read-only: {operation} is refused");

    public static InvalidOperationException MissingSqliteFile(string path) =>
        new($"no SQLite store at '{path}': the file doesn't exist");

    /// <param name="store">The SQLite path, or the Postgres database's name.</param>
    public static InvalidOperationException NoStrataLog(string store) => new($"'{store}' has no Strata checkpoint log");

    public static InvalidOperationException HotJournal(string path, Exception? inner = null) =>
        new($"'{path}' has an unfinished write: open it once with the application that writes it", inner);

    public static InvalidOperationException PrefixNotIndexed(string prefix) =>
        new($"no Strata thread index under prefix '{prefix}': nothing was written there, or no current runtime has connected to it");

    /// <summary>
    /// Refuses a <c>checkpoints</c> component version a read-only store can't read: 0 (a log from before the version
    /// table, without the head table listing reads) and any version newer than this build's.
    /// </summary>
    public static void CheckVersion(long found)
    {
        var known = StoreSchemaVersions.CheckpointsVersion;
        if (found > known)
        {
            throw new InvalidOperationException(StoreSchemaVersions.Newer(StoreSchemaVersions.Checkpoints, found, known));
        }

        if (found < StoreSchemaVersions.BaselineVersion)
        {
            throw new InvalidOperationException(
                $"database is at {StoreSchemaVersions.Checkpoints} schema version {found}, and a read-only store reads " +
                $"{StoreSchemaVersions.BaselineVersion} to {known}: open it once with a runtime to migrate it");
        }
    }

    /// <summary>
    /// Whether a database at <paramref name="version"/> has <c>checkpoint_blobs</c>: from version 2, when offload came.
    /// A read-only store at version 1 never queries it, since none of its rows can be offloaded.
    /// </summary>
    public static bool HasOwnBlobs(long version) => version >= 2;

    /// <summary>The blob reader of a read-only store at version 1, which has no blobs of its own: it finds none.</summary>
    public static readonly BlobReader NoBlobs = (_, _) =>
        ValueTask.FromResult<IReadOnlyDictionary<string, ReadOnlyMemory<byte>>>(
            new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal));

    /// <summary>
    /// Refuses a <see cref="FileSystemCheckpointBlobStore"/> whose root doesn't exist: read, it would answer "not
    /// found" for every blob, and the first offloaded value would fail with a misleading missing-blob error.
    /// </summary>
    /// <exception cref="DirectoryNotFoundException">The blob directory doesn't exist.</exception>
    public static void CheckBlobs(OffloadOptions? offload)
    {
        if (offload?.Blobs is FileSystemCheckpointBlobStore files && !Directory.Exists(files.Root))
        {
            throw new DirectoryNotFoundException($"blob directory '{files.Root}' doesn't exist");
        }
    }
}
