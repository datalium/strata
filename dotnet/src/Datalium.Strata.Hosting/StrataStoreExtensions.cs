using Datalium.Strata.Cloud;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Storage.Redis;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Npgsql;
using StackExchange.Redis;

namespace Datalium.Strata.Hosting;

/// <summary>
/// Registers a checkpoint store and a history store (<c>design/35-Hosting-Integration.md</c>, "Stores"). Each
/// is one singleton, registered under its own type and under every port it implements. A container has one
/// checkpoint store and one history store: registering another replaces the first. Nothing is opened at
/// registration; the host's start-up prepares what was registered.
/// </summary>
public static class StrataStoreExtensions
{
    /// <summary>
    /// The thread id the start-up check and the health check load. Nobody's thread: the answer is "none",
    /// and getting it shows the store is reachable and its tables are current.
    /// </summary>
    internal const string ProbeThreadId = "strata.health";

    private const string CheckpointsStep = "checkpoints";
    private const string HistoryStep = "history";

    // --- Checkpoint stores ---

    /// <summary>An in-memory store, for tests and development: it is gone with the process.</summary>
    public static IStrataBuilder AddInMemoryCheckpoints(this IStrataBuilder builder) =>
        Checkpoints(builder, sp => new InMemoryCheckpointStore(sp.GetRequiredService<TimeProvider>()));

    /// <summary>A SQLite store on the file <c>Strata:Sqlite:Path</c> names.</summary>
    public static IStrataBuilder AddSqliteCheckpoints(this IStrataBuilder builder, Action<StrataSqliteOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind(builder.Services, StrataSqliteOptions.Section, configure);
        return Checkpoints(builder, sp =>
        {
            var options = new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite(SqliteFile(sp)).Options;
            return new EfCoreCheckpointStore(
                new CheckpointDbContext(options),
                ownedConnection: null,
                sp.GetRequiredService<TimeProvider>(),
                new SqliteStoreOptions
                {
                    GroupCommit = Settings<StrataSqliteOptions>(sp).GroupCommit.ToOptions(),
                    Deltas = Settings<StrataSqliteOptions>(sp).Deltas.ToOptions(),
                    Offload = Settings<StrataSqliteOptions>(sp).Offload.ToOptions(sp),
                });
        });
    }

    /// <summary>
    /// A Postgres store, configured by <c>Strata:Postgres</c> and then <paramref name="configure"/>. When the
    /// host starts its tables are migrated, or with <see cref="StrataPostgresOptions.AutoMigrate"/> off only
    /// checked: a database that is behind, or can't be reached, stops the host from starting.
    /// </summary>
    public static IStrataBuilder AddPostgresCheckpoints(this IStrataBuilder builder, Action<StrataPostgresOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind(builder.Services, StrataPostgresOptions.Section, configure);
        return Checkpoints(
            builder,
            sp =>
            {
                var options = Settings<StrataPostgresOptions>(sp);
                var (dataSource, connectionString) = PostgresConnection(sp, options);
                return dataSource is not null
                    ? new PostgresCheckpointStore(dataSource, PostgresOptions(sp, options), ownsDataSource: false)
                    : new PostgresCheckpointStore(connectionString!, PostgresOptions(sp, options));
            },
            (store, sp, cancellationToken) => Settings<StrataPostgresOptions>(sp).AutoMigrate
                ? store.MigrateAsync(cancellationToken)
                : store.LoadLatestAsync(ProbeThreadId, cancellationToken).AsTask());
    }

    /// <summary>
    /// A Redis store, configured by <c>Strata:Redis</c> and then <paramref name="configure"/>. When the host
    /// starts the store is used once, which indexes a prefix an older runtime wrote to: a Redis that can't be
    /// reached stops the host from starting.
    /// </summary>
    public static IStrataBuilder AddRedisCheckpoints(this IStrataBuilder builder, Action<StrataRedisOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind(builder.Services, StrataRedisOptions.Section, configure);
        builder.Services.TryAddSingleton<StrataRedisConnection>();
        return Checkpoints(
            builder,
            sp => new RedisCheckpointStore(sp.GetRequiredService<StrataRedisConnection>().Database, RedisOptions(sp)),
            (store, _, cancellationToken) => store.LoadLatestAsync(ProbeThreadId, cancellationToken).AsTask());
    }

    /// <summary>
    /// The Strata Cloud store and decisions client (<c>design/30-Strata-Cloud-Store.md</c>), configured by
    /// <c>Strata:Cloud</c> and then <paramref name="configure"/>. The store lists threads and has leases
    /// (<c>design/43-Cloud-Leases-And-Listing.md</c>), so it is registered as <see cref="ICheckpointQuery"/> too and
    /// the wake and recovery drivers run against it; it doesn't purge (retention is Cloud's), so the retention
    /// driver can't. Nothing is sent when the host starts.
    /// </summary>
    public static IStrataBuilder AddCloudCheckpoints(this IStrataBuilder builder, Action<StrataCloudHostOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind(builder.Services, StrataCloudHostOptions.Section, configure);
        builder.Services.TryAddSingleton(sp => new StrataCloudDecisions(CloudOptions(sp)));
        return Checkpoints(builder, sp => new StrataCloudCheckpointStore(CloudOptions(sp)));
    }

    /// <summary>
    /// A store of the application's own. It is registered under <see cref="ICheckpointStore"/> and, when
    /// <typeparamref name="TStore"/> implements them, <see cref="ICheckpointQuery"/> and
    /// <see cref="ICheckpointRetention"/>. The container owns what <paramref name="factory"/> returns.
    /// </summary>
    public static IStrataBuilder AddCheckpointStore<TStore>(this IStrataBuilder builder, Func<IServiceProvider, TStore> factory)
        where TStore : class, ICheckpointStore =>
        Checkpoints(builder, factory);

    // --- History stores ---

    /// <summary>An in-memory history store, for tests and development.</summary>
    public static IStrataBuilder AddInMemoryHistory(this IStrataBuilder builder) =>
        History(builder, _ => new InMemoryHistoryStore());

    /// <summary>A SQLite history store on the file <c>Strata:Sqlite:Path</c> names, which the checkpoint store may share.</summary>
    public static IStrataBuilder AddSqliteHistory(this IStrataBuilder builder, Action<StrataSqliteOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind(builder.Services, StrataSqliteOptions.Section, configure);
        return History(builder, sp =>
        {
            var options = new DbContextOptionsBuilder<HistoryDbContext>().UseSqlite(SqliteFile(sp)).Options;
            return new EfCoreHistoryStore(new HistoryDbContext(options));
        });
    }

    /// <summary>A Postgres history store, on the connection <c>Strata:Postgres</c> names. Migrated or checked at start-up like the checkpoint store.</summary>
    public static IStrataBuilder AddPostgresHistory(this IStrataBuilder builder, Action<StrataPostgresOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind(builder.Services, StrataPostgresOptions.Section, configure);
        return History(
            builder,
            sp =>
            {
                var options = Settings<StrataPostgresOptions>(sp);
                var (dataSource, connectionString) = PostgresConnection(sp, options);
                return dataSource is not null
                    ? new PostgresHistoryStore(dataSource, PostgresOptions(sp, options), ownsDataSource: false)
                    : new PostgresHistoryStore(connectionString!, PostgresOptions(sp, options));
            },
            async (store, sp, cancellationToken) =>
            {
                if (Settings<StrataPostgresOptions>(sp).AutoMigrate)
                {
                    await store.MigrateAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }

                // Any read prepares the store, which with AutoMigrate off is the check.
                await foreach (var unused in store.GetHistoryAsync(ProbeThreadId, 1, cancellationToken).ConfigureAwait(false))
                {
                }
            });
    }

    /// <summary>A Redis history store, on the connection and prefix <c>Strata:Redis</c> names.</summary>
    public static IStrataBuilder AddRedisHistory(this IStrataBuilder builder, Action<StrataRedisOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind(builder.Services, StrataRedisOptions.Section, configure);
        builder.Services.TryAddSingleton<StrataRedisConnection>();
        return History(builder, sp => new RedisHistoryStore(sp.GetRequiredService<StrataRedisConnection>().Database, RedisOptions(sp)));
    }

    /// <summary>A history store of the application's own. The container owns what <paramref name="factory"/> returns.</summary>
    public static IStrataBuilder AddHistoryStore<TStore>(this IStrataBuilder builder, Func<IServiceProvider, TStore> factory)
        where TStore : class, IHistoryStore =>
        History(builder, factory);

    // --- Registration ---

    private static IStrataBuilder Checkpoints<TStore>(
        IStrataBuilder builder,
        Func<IServiceProvider, TStore> factory,
        Func<TStore, IServiceProvider, CancellationToken, Task>? prepare = null)
        where TStore : class, ICheckpointStore
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(factory);
        var services = builder.Services;
        services.RemoveAll<ICheckpointStore>();
        services.RemoveAll<ICheckpointQuery>();
        services.RemoveAll<ICheckpointRetention>();
        services.RemoveAll<TStore>();

        services.AddSingleton(factory);

        // A factory typed as the port itself is already the port's registration: forwarding it to itself would hang.
        if (typeof(TStore) != typeof(ICheckpointStore))
        {
            services.AddSingleton<ICheckpointStore>(sp => sp.GetRequiredService<TStore>());
        }

        if (typeof(ICheckpointQuery).IsAssignableFrom(typeof(TStore)))
        {
            services.AddSingleton(sp => (ICheckpointQuery)sp.GetRequiredService<TStore>());
        }

        if (typeof(ICheckpointRetention).IsAssignableFrom(typeof(TStore)))
        {
            services.AddSingleton(sp => (ICheckpointRetention)sp.GetRequiredService<TStore>());
        }

        // Resolving the store constructs it, which for a file or a connection string is where it is opened.
        StrataStartupStep.Set(services, CheckpointsStep, (sp, cancellationToken) =>
        {
            var store = sp.GetRequiredService<TStore>();
            return prepare?.Invoke(store, sp, cancellationToken) ?? Task.CompletedTask;
        });
        return builder;
    }

    private static IStrataBuilder History<TStore>(
        IStrataBuilder builder,
        Func<IServiceProvider, TStore> factory,
        Func<TStore, IServiceProvider, CancellationToken, Task>? prepare = null)
        where TStore : class, IHistoryStore
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(factory);
        var services = builder.Services;
        services.RemoveAll<IHistoryStore>();
        services.RemoveAll<TStore>();
        services.AddSingleton(factory);
        if (typeof(TStore) != typeof(IHistoryStore))
        {
            services.AddSingleton<IHistoryStore>(sp => sp.GetRequiredService<TStore>());
        }

        StrataStartupStep.Set(services, HistoryStep, (sp, cancellationToken) =>
        {
            var store = sp.GetRequiredService<TStore>();
            return prepare?.Invoke(store, sp, cancellationToken) ?? Task.CompletedTask;
        });
        return builder;
    }

    // --- Settings to store options ---

    internal static TOptions Settings<TOptions>(IServiceProvider services)
        where TOptions : class =>
        services.GetRequiredService<IOptions<TOptions>>().Value;

    private static string SqliteFile(IServiceProvider services) =>
        new SqliteConnectionStringBuilder { DataSource = SqlitePath(services) }.ToString();

    /// <summary>The file <c>Strata:Sqlite:Path</c> names, which every SQLite store of the container shares.</summary>
    internal static string SqlitePath(IServiceProvider services)
    {
        var path = Settings<StrataSqliteOptions>(services).Path;
        return string.IsNullOrWhiteSpace(path)
            ? throw new InvalidOperationException("Strata's SQLite store has no file: set Strata:Sqlite:Path")
            : path;
    }

    private static PostgresStoreOptions PostgresOptions(IServiceProvider services, StrataPostgresOptions options) => new()
    {
        Retry = options.Retry.ToPolicy(),
        AutoMigrate = options.AutoMigrate,
        TimeProvider = services.GetRequiredService<TimeProvider>(),
        GroupCommit = options.GroupCommit.ToOptions(),
        Deltas = options.Deltas.ToOptions(),
        Offload = options.Offload.ToOptions(services),
    };

    internal static (NpgsqlDataSource? DataSource, string? ConnectionString) PostgresConnection(
        IServiceProvider services, StrataPostgresOptions options)
    {
        if (options.DataSourceKey is { } key)
        {
            return (services.GetRequiredKeyedService<NpgsqlDataSource>(key), null);
        }

        if (options.UseRegisteredDataSource)
        {
            return (services.GetRequiredService<NpgsqlDataSource>(), null);
        }

        var connectionString = options.ConnectionString
            ?? services.GetService<IConfiguration>()?.GetConnectionString(options.ConnectionStringName);
        return string.IsNullOrWhiteSpace(connectionString)
            ? throw new InvalidOperationException(
                $"Strata's Postgres store has no connection: set Strata:Postgres:ConnectionString, a connection string named '{options.ConnectionStringName}', or use a registered NpgsqlDataSource")
            : (null, connectionString);
    }

    private static RedisStoreOptions RedisOptions(IServiceProvider services)
    {
        var options = Settings<StrataRedisOptions>(services);
        return new RedisStoreOptions
        {
            Prefix = options.Prefix,
            Retry = options.Retry.ToPolicy(),
            FinishedThreadTtl = options.FinishedThreadTtl,
            TimeProvider = services.GetRequiredService<TimeProvider>(),
            Deltas = options.Deltas.ToOptions(),
            Offload = options.Offload.ToOptions(services),
        };
    }

    private static StrataCloudOptions CloudOptions(IServiceProvider services)
    {
        var options = Settings<StrataCloudHostOptions>(services);
        return new StrataCloudOptions
        {
            BaseUrl = options.BaseUrl ?? throw new InvalidOperationException("Strata Cloud has no address: set Strata:Cloud:BaseUrl"),
            ApiKey = string.IsNullOrWhiteSpace(options.ApiKey)
                ? throw new InvalidOperationException("Strata Cloud has no API key: set Strata:Cloud:ApiKey")
                : options.ApiKey,
            ConnectTimeout = options.ConnectTimeout,
            RequestTimeout = options.RequestTimeout,
            TimeProvider = services.GetRequiredService<TimeProvider>(),
        };
    }
}

/// <summary>
/// The Redis connection the checkpoint and the history store share: the application's own multiplexer, or
/// one made from the connection string the first time a store is resolved and closed with the container.
/// </summary>
internal sealed class StrataRedisConnection(IServiceProvider services, IOptions<StrataRedisOptions> options) : IDisposable
{
    private readonly object _gate = new();
    private IConnectionMultiplexer? _multiplexer;
    private bool _owned;

    public IDatabase Database
    {
        get
        {
            lock (_gate)
            {
                return (_multiplexer ??= Connect()).GetDatabase();
            }
        }
    }

    private IConnectionMultiplexer Connect()
    {
        var settings = options.Value;
        if (settings.UseRegisteredMultiplexer)
        {
            return services.GetRequiredService<IConnectionMultiplexer>();
        }

        var connectionString = settings.ConnectionString
            ?? services.GetService<IConfiguration>()?.GetConnectionString(settings.ConnectionStringName);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                $"Strata's Redis store has no connection: set Strata:Redis:ConnectionString, a connection string named '{settings.ConnectionStringName}', or use a registered IConnectionMultiplexer");
        }

        _owned = true;
        return RedisConnection.Connect(connectionString);
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_owned)
            {
                _multiplexer?.Dispose();
            }

            _multiplexer = null;
        }
    }
}
