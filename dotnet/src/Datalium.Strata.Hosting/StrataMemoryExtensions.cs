using Datalium.Strata.AI;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Storage.Redis;
using Datalium.Strata.Storage.Sqlite;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Datalium.Strata.Hosting;

/// <summary>
/// Registers a long-term memory store, the memory helper and the memory retention driver
/// (<c>design/45-Long-Term-Memory.md</c>, "Hosting"). A store is one singleton, registered under its own type,
/// <see cref="IMemoryStore"/> and, when it purges, <see cref="IMemoryRetention"/>. A container has one memory store:
/// registering another replaces the first. Each reads <c>Strata:Memory</c> (<see cref="StrataMemoryHostOptions"/>),
/// <see cref="StrataMemoryHostOptions.Dimensions"/> required, and its connection from the checkpoint store's section.
/// Nothing is opened at registration: when the host starts, the store is made and prepared (migrated, or checked),
/// so a database that is behind or newer, holds vectors of other dimensions, or lacks pgvector, sqlite-vec or the
/// Query Engine stops the host from starting.
/// </summary>
public static class StrataMemoryExtensions
{
    private const string MemoryStep = "memory";
    private const string HelperStep = "memory:helper";

    /// <summary>The key the start-up check reads when the Postgres store only checks: nobody's record.</summary>
    private static readonly MemoryKey ProbeKey = new(StrataStoreExtensions.ProbeThreadId, "", StrataStoreExtensions.ProbeThreadId);

    /// <summary>An in-memory store, for tests and development: it is gone with the process.</summary>
    public static IStrataBuilder AddInMemoryMemory(this IStrataBuilder builder, Action<StrataMemoryHostOptions>? configure = null) =>
        Memory(builder, configure, sp => new InMemoryMemoryStore(Dimensions(sp), sp.GetRequiredService<TimeProvider>()));

    /// <summary>
    /// A Postgres store with pgvector on the connection <c>Strata:Postgres</c> names (its <c>AutoMigrate</c> and
    /// <c>Retry</c> too), the table and index <c>Strata:Memory</c> names. When the host starts its table is
    /// migrated, or with <c>AutoMigrate</c> off only checked.
    /// </summary>
    public static IStrataBuilder AddPostgresMemory(this IStrataBuilder builder, Action<StrataMemoryHostOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind<StrataPostgresOptions>(builder.Services, StrataPostgresOptions.Section, configure: null);
        return Memory(
            builder,
            configure,
            sp =>
            {
                var postgres = StrataStoreExtensions.Settings<StrataPostgresOptions>(sp);
                var memory = Settings(sp);
                var options = new PostgresMemoryOptions
                {
                    Dimensions = Dimensions(sp),
                    Index = memory.Index.ToIndex(),
                    Retry = postgres.Retry.ToPolicy(),
                    AutoMigrate = postgres.AutoMigrate,
                    TimeProvider = sp.GetRequiredService<TimeProvider>(),
                };
                if (memory.Table is { } table)
                {
                    options = options with { Table = table };
                }

                var (dataSource, connectionString) = StrataStoreExtensions.PostgresConnection(sp, postgres);
                return dataSource is not null
                    ? new PostgresMemoryStore(dataSource, options, ownsDataSource: false)
                    : new PostgresMemoryStore(connectionString!, options);
            },
            async (store, sp, cancellationToken) =>
            {
                if (StrataStoreExtensions.Settings<StrataPostgresOptions>(sp).AutoMigrate)
                {
                    await store.MigrateAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }

                // Any call prepares the store, which with AutoMigrate off is the check.
                await store.GetAsync(ProbeKey, cancellationToken).ConfigureAwait(false);
            });
    }

    /// <summary>
    /// A SQLite store with sqlite-vec on the file <c>Strata:Sqlite:Path</c> names, which the checkpoint and history
    /// stores may share, with the table and extension <c>Strata:Memory</c> names. The application supplies
    /// sqlite-vec (<see cref="SqliteMemoryOptions.Extension"/>). Migrated when the host starts.
    /// </summary>
    public static IStrataBuilder AddSqliteMemory(this IStrataBuilder builder, Action<StrataMemoryHostOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind<StrataSqliteOptions>(builder.Services, StrataSqliteOptions.Section, configure: null);
        return Memory(
            builder,
            configure,
            sp =>
            {
                var memory = Settings(sp);
                var options = new SqliteMemoryOptions
                {
                    Dimensions = Dimensions(sp),
                    TimeProvider = sp.GetRequiredService<TimeProvider>(),
                };
                if (memory.Table is { } table)
                {
                    options = options with { Table = table };
                }

                if (memory.Extension is { } extension)
                {
                    options = options with { Extension = extension };
                }

                // The path, not a connection string: the store opens it unpooled, so the container's end closes the file.
                return new SqliteMemoryStore(StrataStoreExtensions.SqlitePath(sp), options);
            },
            (store, _, cancellationToken) => store.MigrateAsync(cancellationToken));
    }

    /// <summary>
    /// A Redis store on the Query Engine, on the connection <c>Strata:Redis</c> names (its <c>Retry</c> too), under
    /// <c>Strata:Memory:Prefix</c> or else <c>Strata:Redis:Prefix</c>, with the index <c>Strata:Memory</c> names.
    /// Prepared when the host starts.
    /// </summary>
    public static IStrataBuilder AddRedisMemory(this IStrataBuilder builder, Action<StrataMemoryHostOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind<StrataRedisOptions>(builder.Services, StrataRedisOptions.Section, configure: null);
        builder.Services.TryAddSingleton<StrataRedisConnection>();
        return Memory(
            builder,
            configure,
            sp =>
            {
                var redis = StrataStoreExtensions.Settings<StrataRedisOptions>(sp);
                var memory = Settings(sp);
                return new RedisMemoryStore(
                    sp.GetRequiredService<StrataRedisConnection>().Database,
                    new RedisMemoryOptions
                    {
                        Dimensions = Dimensions(sp),
                        Prefix = string.IsNullOrWhiteSpace(memory.Prefix) ? redis.Prefix : memory.Prefix,
                        Index = memory.Index.ToIndex(),
                        Retry = redis.Retry.ToPolicy(),
                        TimeProvider = sp.GetRequiredService<TimeProvider>(),
                    });
            },
            (store, _, cancellationToken) => store.MigrateAsync(cancellationToken));
    }

    /// <summary>
    /// A memory store of the application's own. It is registered under <see cref="IMemoryStore"/> and, when
    /// <typeparamref name="TStore"/> implements it, <see cref="IMemoryRetention"/>. The container owns what
    /// <paramref name="factory"/> returns.
    /// </summary>
    public static IStrataBuilder AddMemoryStore<TStore>(this IStrataBuilder builder, Func<IServiceProvider, TStore> factory)
        where TStore : class, IMemoryStore =>
        Memory(builder, configure: null, factory);

    /// <summary>
    /// The memory helper (<see cref="StrataMemory"/>, a singleton) over the container's <see cref="IMemoryStore"/>
    /// and <see cref="IEmbeddingGenerator{TInput, TEmbedding}"/> of strings and float embeddings, with
    /// <c>Strata:Memory</c>'s <see cref="StrataMemoryHostOptions.Model"/> and
    /// <see cref="StrataMemoryHostOptions.TenantKey"/>. A container without either, or whose generator names no
    /// model when no label is configured, doesn't start.
    /// </summary>
    public static IStrataBuilder AddMemory(this IStrataBuilder builder, Action<StrataMemoryHostOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;
        StrataConfiguration.Bind(services, StrataMemoryHostOptions.Section, configure);
        services.RemoveAll<StrataMemory>();
        services.AddSingleton(sp =>
        {
            var options = Settings(sp);
            return new StrataMemory(
                sp.GetRequiredService<IMemoryStore>(),
                sp.GetRequiredService<IEmbeddingGenerator<string, Embedding<float>>>(),
                new StrataMemoryOptions
                {
                    Model = string.IsNullOrWhiteSpace(options.Model) ? null : options.Model,
                    TenantKey = options.TenantKey,
                });
        });
        StrataStartupStep.Set(services, HelperStep, (sp, _) =>
        {
            if (sp.GetService<IMemoryStore>() is null)
            {
                throw new InvalidOperationException(
                    "AddMemory needs a memory store: AddInMemoryMemory, AddPostgresMemory, AddSqliteMemory, AddRedisMemory or AddMemoryStore");
            }

            if (sp.GetService<IEmbeddingGenerator<string, Embedding<float>>>() is null)
            {
                throw new InvalidOperationException(
                    "AddMemory needs an IEmbeddingGenerator<string, Embedding<float>> in the container");
            }

            // Making it labels its vectors, which refuses a generator that names no model.
            sp.GetRequiredService<StrataMemory>();
            return Task.CompletedTask;
        });
        return builder;
    }

    /// <summary>
    /// Purges memories last written longer ago than <see cref="MemoryRetentionOptions.OlderThan"/>
    /// (<c>design/45-Long-Term-Memory.md</c>, "Retention"), from <c>Strata:Memory:Retention</c> and then
    /// <paramref name="configure"/>: a driver like <c>AddRetention</c>'s, every hour by default. Needs a memory store
    /// with the retention port and an <see cref="MemoryRetentionOptions.OlderThan"/>.
    /// </summary>
    public static IStrataBuilder AddMemoryRetention(this IStrataBuilder builder, Action<MemoryRetentionOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind(builder.Services, MemoryRetentionOptions.Section, configure);
        builder.Services.AddHostedService<MemoryRetentionDriver>();
        StrataStartupStep.Set(builder.Services, $"driver:{MemoryRetentionDriver.Kind}", (sp, _) =>
        {
            MemoryRetentionDriver.Check(sp);
            return Task.CompletedTask;
        });
        return builder;
    }

    // --- Registration ---

    private static IStrataBuilder Memory<TStore>(
        IStrataBuilder builder,
        Action<StrataMemoryHostOptions>? configure,
        Func<IServiceProvider, TStore> factory,
        Func<TStore, IServiceProvider, CancellationToken, Task>? prepare = null)
        where TStore : class, IMemoryStore
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(factory);
        var services = builder.Services;
        StrataConfiguration.Bind(services, StrataMemoryHostOptions.Section, configure);
        services.RemoveAll<IMemoryStore>();
        services.RemoveAll<IMemoryRetention>();
        services.RemoveAll<TStore>();

        services.AddSingleton(factory);

        // A factory typed as the port itself is already the port's registration: forwarding it to itself would hang.
        if (typeof(TStore) != typeof(IMemoryStore))
        {
            services.AddSingleton<IMemoryStore>(sp => sp.GetRequiredService<TStore>());
        }

        if (typeof(IMemoryRetention).IsAssignableFrom(typeof(TStore)))
        {
            services.AddSingleton(sp => (IMemoryRetention)sp.GetRequiredService<TStore>());
        }

        // Resolving the store makes it, which checks its dimensions; preparing it opens its database.
        StrataStartupStep.Set(services, MemoryStep, (sp, cancellationToken) =>
        {
            var store = sp.GetRequiredService<TStore>();
            return prepare?.Invoke(store, sp, cancellationToken) ?? Task.CompletedTask;
        });
        return builder;
    }

    private static StrataMemoryHostOptions Settings(IServiceProvider services) =>
        StrataStoreExtensions.Settings<StrataMemoryHostOptions>(services);

    private static int Dimensions(IServiceProvider services) =>
        Settings(services).Dimensions is > 0 and var dimensions
            ? dimensions
            : throw new InvalidOperationException("Strata's memory store has no dimensions: set Strata:Memory:Dimensions");
}

/// <summary>
/// The memory retention driver (<c>design/45-Long-Term-Memory.md</c>, "Retention"): each round purges what is older
/// than <see cref="MemoryRetentionOptions.OlderThan"/> by the host's clock, a batch at a time, until a batch comes back
/// short or the host stops.
/// </summary>
internal sealed class MemoryRetentionDriver(
    IServiceProvider services,
    IOptions<MemoryRetentionOptions> options,
    StrataDriverStatus status,
    TimeProvider clock,
    ILoggerFactory loggers)
    : StrataDriver(Kind, Kind, status, clock, loggers)
{
    public const string Kind = "memory-retention";

    private readonly MemoryRetentionOptions _options = options.Value;
    private readonly TimeProvider _clock = clock;

    protected override TimeSpan Interval => _options.Interval;

    public static void Check(IServiceProvider services)
    {
        var options = services.GetRequiredService<IOptions<MemoryRetentionOptions>>().Value;
        StrataDriverExtensions.CheckPacing("memory retention", options.Interval, options.BatchSize);
        if (options.OlderThan is not { } olderThan || olderThan <= TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                "the memory retention driver needs Strata:Memory:Retention:OlderThan: how long a memory is kept after it was last written");
        }

        // The namespace and tenant as every purge will check them, so a bad one stops the host with the store's message.
        MemoryRecords.Check(new MemoryPurge(DateTimeOffset.UnixEpoch)
        {
            Namespace = options.Namespace,
            Tenant = options.Tenant,
            Limit = options.BatchSize,
        });

        if (services.GetService<IMemoryRetention>() is null)
        {
            throw new InvalidOperationException(
                "the memory retention driver needs a memory store that purges (IMemoryRetention), and the registered one doesn't");
        }
    }

    protected override async Task<int> RoundAsync(CancellationToken stop)
    {
        var retention = services.GetRequiredService<IMemoryRetention>();
        var purged = 0;
        while (true)
        {
            // No token of the call's own: a purge in flight finishes when the host stops.
            var batch = await retention
                .PurgeAsync(
                    new MemoryPurge(_clock.GetUtcNow() - _options.OlderThan!.Value)
                    {
                        Namespace = _options.Namespace,
                        Tenant = _options.Tenant,
                        Limit = _options.BatchSize,
                    },
                    CancellationToken.None)
                .ConfigureAwait(false);
            purged += batch;
            if (batch < _options.BatchSize || stop.IsCancellationRequested)
            {
                return purged;
            }
        }
    }
}
