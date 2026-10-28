using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Redis;

namespace Datalium.Strata.Hosting;

/// <summary>
/// How a Postgres or Redis store repeats a call that failed for a transient reason, as configuration can say
/// it (<c>design/34-Storage-Hardening.md</c>, "Retries"). The defaults are <see cref="StoreRetryPolicy"/>'s.
/// </summary>
public sealed class StoreRetryOptions
{
    /// <summary>How many times a call is made at most, the first included. 1 never repeats.</summary>
    public int MaxAttempts { get; set; } = 4;

    /// <summary>The wait before the first repeat, doubled before each later one.</summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>The longest wait between two attempts.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(2);

    internal StoreRetryPolicy ToPolicy() =>
        new() { MaxAttempts = MaxAttempts, InitialDelay = InitialDelay, MaxDelay = MaxDelay };
}

/// <summary>
/// Group commit as configuration can say it (<c>design/41-Checkpoint-Efficiency.md</c>), from
/// <c>Strata:Postgres:GroupCommit</c> and <c>Strata:Sqlite:GroupCommit</c>. Off unless <see cref="Enabled"/>; the
/// other defaults are <see cref="GroupCommitOptions"/>'s.
/// </summary>
public sealed class StoreGroupCommitOptions
{
    /// <summary>Whether the checkpoint store flushes concurrent saves together.</summary>
    public bool Enabled { get; set; }

    /// <summary>The most saves one flush takes.</summary>
    public int MaxBatch { get; set; } = 256;

    /// <summary>How many flushes may run at once (the SQLite store always runs one).</summary>
    public int MaxConcurrentFlushes { get; set; } = 4;

    internal GroupCommitOptions? ToOptions() =>
        Enabled ? new GroupCommitOptions { MaxBatch = MaxBatch, MaxConcurrentFlushes = MaxConcurrentFlushes } : null;
}

/// <summary>
/// Delta checkpoints as configuration can say them (<c>design/41-Checkpoint-Efficiency.md</c>, "Deltas"), from
/// <c>Strata:Postgres:Deltas</c>, <c>Strata:Redis:Deltas</c> and <c>Strata:Sqlite:Deltas</c>. Off unless
/// <see cref="Enabled"/>; the other defaults are <see cref="DeltaOptions"/>'s.
/// </summary>
public sealed class StoreDeltaOptions
{
    /// <summary>Whether the checkpoint store stores running checkpoints as the changes to the row before.</summary>
    public bool Enabled { get; set; }

    /// <summary>Every this many rows of a thread one is stored whole.</summary>
    public int FullEvery { get; set; } = 16;

    /// <summary>How many threads' latest rows the store remembers.</summary>
    public int CachedThreads { get; set; } = 10_000;

    internal DeltaOptions? ToOptions() =>
        Enabled ? new DeltaOptions { FullEvery = FullEvery, CachedThreads = CachedThreads } : null;
}

/// <summary>
/// Value offload as configuration can say it (<c>design/41-Checkpoint-Efficiency.md</c>, "Offload"), from
/// <c>Strata:Postgres:Offload</c>, <c>Strata:Redis:Offload</c> and <c>Strata:Sqlite:Offload</c>. Off unless
/// <see cref="Enabled"/>. The blobs go to the <see cref="ICheckpointBlobStore"/> the application registered when there
/// is one, else to a <see cref="FileSystemCheckpointBlobStore"/> at <see cref="Path"/>, else to the store's own
/// database; a registered blob store and a <see cref="Path"/> together are refused.
/// </summary>
public sealed class StoreOffloadOptions
{
    /// <summary>Whether the checkpoint store stores large values once, in a blob store, and refers to them.</summary>
    public bool Enabled { get; set; }

    /// <summary>The bytes of a value's JSON from which it is offloaded.</summary>
    public int Threshold { get; set; } = 64 * 1024;

    /// <summary>The directory of a filesystem blob store, instead of the store's own database.</summary>
    public string? Path { get; set; }

    internal OffloadOptions? ToOptions(IServiceProvider services)
    {
        if (!Enabled)
        {
            return null;
        }

        var registered = services.GetService(typeof(ICheckpointBlobStore)) as ICheckpointBlobStore;
        if (registered is not null && !string.IsNullOrWhiteSpace(Path))
        {
            throw new InvalidOperationException(
                "Strata's offload has two blob stores: set Offload:Path or register an ICheckpointBlobStore, not both");
        }

        return new OffloadOptions
        {
            Threshold = Threshold,
            Blobs = registered ?? (string.IsNullOrWhiteSpace(Path) ? null : new FileSystemCheckpointBlobStore(Path)),
        };
    }
}

/// <summary>
/// The Postgres stores' settings, from <c>Strata:Postgres</c> (<c>design/35-Hosting-Integration.md</c>,
/// "Stores"). The connection is the first of: a keyed <c>NpgsqlDataSource</c> from the container
/// (<see cref="DataSourceKey"/>), the unkeyed one (<see cref="UseRegisteredDataSource"/>),
/// <see cref="ConnectionString"/>, the connection string named <see cref="ConnectionStringName"/>.
/// </summary>
public sealed class StrataPostgresOptions
{
    internal const string Section = "Postgres";

    /// <summary>A connection string, or a <c>postgres://</c> URL.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>The name of a connection string in <c>ConnectionStrings</c>. <c>strata</c> by default.</summary>
    public string ConnectionStringName { get; set; } = "strata";

    /// <summary>The key of an <c>NpgsqlDataSource</c> the application registered. The store doesn't own it.</summary>
    public string? DataSourceKey { get; set; }

    /// <summary>Use the unkeyed <c>NpgsqlDataSource</c> the application registered. The store doesn't own it.</summary>
    public bool UseRegisteredDataSource { get; set; }

    /// <summary>
    /// Whether the tables are brought up to date when the host starts (the default). When
    /// <see langword="false"/> they are only checked, and a database that is behind stops the host: a deploy
    /// step runs the migration.
    /// </summary>
    public bool AutoMigrate { get; set; } = true;

    /// <summary>How a call that failed for a transient reason is repeated.</summary>
    public StoreRetryOptions Retry { get; } = new();

    /// <summary>Whether the checkpoint store flushes concurrent saves together, and how.</summary>
    public StoreGroupCommitOptions GroupCommit { get; } = new();

    /// <summary>Whether the checkpoint store stores running checkpoints as deltas, and how.</summary>
    public StoreDeltaOptions Deltas { get; } = new();

    /// <summary>Whether the checkpoint store offloads large values to a blob store, and where.</summary>
    public StoreOffloadOptions Offload { get; } = new();
}

/// <summary>
/// The Redis stores' settings, from <c>Strata:Redis</c>. The connection is the first of: the
/// <c>IConnectionMultiplexer</c> the application registered (<see cref="UseRegisteredMultiplexer"/>),
/// <see cref="ConnectionString"/>, the connection string named <see cref="ConnectionStringName"/>.
/// </summary>
public sealed class StrataRedisOptions
{
    internal const string Section = "Redis";

    /// <summary>A <c>redis://</c> URL, or a StackExchange.Redis configuration string.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>The name of a connection string in <c>ConnectionStrings</c>. <c>strata-redis</c> by default.</summary>
    public string ConnectionStringName { get; set; } = "strata-redis";

    /// <summary>Use the <c>IConnectionMultiplexer</c> the application registered. The stores don't own it.</summary>
    public bool UseRegisteredMultiplexer { get; set; }

    /// <summary>The key namespace: keys are <c>{prefix}:checkpoints:…</c>.</summary>
    public string Prefix { get; set; } = RedisConnection.DefaultPrefix;

    /// <summary>
    /// Lets a thread's keys expire this long after it completes or ends
    /// (<c>design/34-Storage-Hardening.md</c>, "Retention"). Off unless set.
    /// </summary>
    public TimeSpan? FinishedThreadTtl { get; set; }

    /// <summary>How a call that failed for a transient reason is repeated.</summary>
    public StoreRetryOptions Retry { get; } = new();

    /// <summary>Whether the checkpoint store stores running checkpoints as deltas, and how.</summary>
    public StoreDeltaOptions Deltas { get; } = new();

    /// <summary>Whether the checkpoint store offloads large values to a blob store, and where.</summary>
    public StoreOffloadOptions Offload { get; } = new();
}

/// <summary>The SQLite stores' settings, from <c>Strata:Sqlite</c>.</summary>
public sealed class StrataSqliteOptions
{
    internal const string Section = "Sqlite";

    /// <summary>The database file. The checkpoint and the history store share it.</summary>
    public string? Path { get; set; }

    /// <summary>Whether the checkpoint store flushes concurrent saves together, and how.</summary>
    public StoreGroupCommitOptions GroupCommit { get; } = new();

    /// <summary>Whether the checkpoint store stores running checkpoints as deltas, and how.</summary>
    public StoreDeltaOptions Deltas { get; } = new();

    /// <summary>Whether the checkpoint store offloads large values to a blob store, and where.</summary>
    public StoreOffloadOptions Offload { get; } = new();
}

/// <summary>
/// Strata Cloud's settings, from <c>Strata:Cloud</c> (<c>design/30-Strata-Cloud-Store.md</c>). The API key
/// belongs in a secret store, not in a settings file.
/// </summary>
public sealed class StrataCloudHostOptions
{
    internal const string Section = "Cloud";

    /// <summary>Cloud's address, without <c>/v1</c>.</summary>
    public Uri? BaseUrl { get; set; }

    /// <summary>The SDK API key.</summary>
    public string? ApiKey { get; set; }

    /// <summary>How long a connection attempt may take.</summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>How long a request may take.</summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
