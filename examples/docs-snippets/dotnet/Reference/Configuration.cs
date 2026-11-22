// Samples for https://stratamesh.dev/reference/configuration/: the full appsettings.json, a whole-file sample in
// files/reference/configuration/. The tests bind it, then start hosts on it: one on the SQLite stores with the
// scheduler, retention, the memory retention and the wake and recovery drivers, and one each on the Postgres and
// Redis stores when STRATA_TEST_POSTGRES_URL / STRATA_TEST_REDIS_URL are set.

using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Hosting;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage.Postgres;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Datalium.Strata.DocsSnippets.Reference.Configuration;

public class ConfigurationSamples
{
    private static readonly string AppSettings =
        Path.Combine(AppContext.BaseDirectory, "files", "reference", "configuration", "appsettings.json");

    [Fact]
    public void AppSettingsBindsEverySection()
    {
        var builder = Builder();
        builder.Services.AddStrata()
            .AddPostgresCheckpoints()
            .AddRedisHistory()
            .AddSqliteHistory()
            .AddCloudCheckpoints(o => o.ApiKey = "from a secret store")
            .AddScheduler()
            .AddGraph(_ => DurableGraph())
            .AddWakeDriver<Tick>()
            .AddRecoveryDriver<Tick>()
            .AddRetention()
            .AddInMemoryMemory()
            .AddMemoryRetention();
        using var host = builder.Build();
        T Get<T>()
            where T : class => host.Services.GetRequiredService<IOptions<T>>().Value;
        T Named<T>()
            where T : class => host.Services.GetRequiredService<IOptionsMonitor<T>>().Get(typeof(Tick).FullName);

        var sqlite = Get<StrataSqliteOptions>();
        Assert.Equal("strata.db", sqlite.Path);
        Assert.True(sqlite.Deltas.Enabled);
        Assert.Equal((65536, "blobs"), (sqlite.Offload.Threshold, sqlite.Offload.Path));

        var postgres = Get<StrataPostgresOptions>();
        Assert.Equal("strata", postgres.ConnectionStringName);
        Assert.False(postgres.AutoMigrate);
        Assert.Equal(TimeSpan.FromMilliseconds(50), postgres.Retry.InitialDelay);
        Assert.Equal(4, postgres.GroupCommit.MaxConcurrentFlushes);

        var redis = Get<StrataRedisOptions>();
        Assert.Equal(("strata-redis", "orders"), (redis.ConnectionStringName, redis.Prefix));
        Assert.Equal(TimeSpan.FromDays(7), redis.FinishedThreadTtl);

        var cloud = Get<StrataCloudHostOptions>();
        Assert.Equal(new Uri("https://cloud.example.com"), cloud.BaseUrl);
        Assert.Equal(TimeSpan.FromSeconds(30), cloud.RequestTimeout);

        var runs = Get<StrataRunOptions>();
        Assert.Equal(("orders-1", TimeSpan.FromSeconds(30)), (runs.Lease.Owner, runs.Lease.Ttl));

        var scheduler = Get<StrataSchedulerOptions>();
        Assert.Equal((16, 1024, TimeSpan.FromSeconds(10)), (scheduler.MaxConcurrent, scheduler.MaxQueued, scheduler.MaxQueueTime));
        Assert.Equal(4, scheduler.TenantLimit);

        var wake = Named<WakeDriverOptions>();
        Assert.Equal((TimeSpan.FromSeconds(5), 100, true), (wake.Interval, wake.BatchSize, wake.MatchGraph));
        var recovery = Named<RecoveryDriverOptions>();
        Assert.Equal((TimeSpan.FromMinutes(5), 3, 10), (recovery.StaleAfter, recovery.MaxRecoveries, recovery.Priority));

        var retention = Get<RetentionOptions>();
        Assert.Equal(TimeSpan.FromDays(30), retention.OlderThan);
        Assert.Equal(
            [CheckpointThreadState.Completed, CheckpointThreadState.Ended, CheckpointThreadState.Failed],
            retention.States);

        var memory = Get<StrataMemoryHostOptions>();
        Assert.Equal((1536, "tenant"), (memory.Dimensions, memory.TenantKey));
        var memoryRetention = Get<MemoryRetentionOptions>();
        Assert.Equal((TimeSpan.FromDays(365), "incidents"), (memoryRetention.OlderThan, memoryRetention.Namespace));
    }

    [Fact]
    public async Task AppSettingsStartsAHostOnTheSqliteStores()
    {
        // The file's relative paths, in a directory of the test's own.
        var directory = Path.Combine(Path.GetTempPath(), $"strata-docs-configuration-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            var builder = Builder(new()
            {
                ["Strata:Sqlite:Path"] = Path.Combine(directory, "strata.db"),
                ["Strata:Sqlite:Offload:Path"] = Path.Combine(directory, "blobs"),
            });
            builder.Services.AddStrata()
                .AddSqliteCheckpoints()
                .AddSqliteHistory()
                .AddScheduler()
                .AddGraph(_ => DurableGraph())
                .AddWakeDriver<Tick>()
                .AddRecoveryDriver<Tick>()
                .AddRetention()
                .AddInMemoryMemory()
                .AddMemoryRetention();
            using var host = builder.Build();
            await host.StartAsync();
            await host.StopAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (IOException)
            {
                // Left for the OS to clean up.
            }
        }
    }

    [Fact]
    public async Task AppSettingsStartsAHostOnThePostgresStores()
    {
        if (BackendUrl("STRATA_TEST_POSTGRES_URL") is not { } url)
        {
            return;
        }

        var connection = PostgresConnection.ToConnectionString(url);

        // The file turns AutoMigrate off, so bring the tables up to date first, as a deployment step would.
        var migrate = Builder(new() { ["ConnectionStrings:strata"] = connection, ["Strata:Postgres:AutoMigrate"] = "true" });
        migrate.Services.AddStrata().AddPostgresCheckpoints().AddPostgresHistory();
        using (var host = migrate.Build())
        {
            await host.StartAsync();
            await host.StopAsync();
        }

        var builder = Builder(new() { ["ConnectionStrings:strata"] = connection });
        builder.Services.AddStrata()
            .AddPostgresCheckpoints()
            .AddPostgresHistory()
            .AddGraph(_ => DurableGraph())
            .AddWakeDriver<Tick>()
            .AddRecoveryDriver<Tick>();
        using var started = builder.Build();
        await started.StartAsync();
        await started.StopAsync();
    }

    [Fact]
    public async Task AppSettingsStartsAHostOnTheRedisStores()
    {
        if (BackendUrl("STRATA_TEST_REDIS_URL") is not { } url)
        {
            return;
        }

        var builder = Builder(new()
        {
            ["ConnectionStrings:strata-redis"] = new Uri(url).Authority,
            ["Strata:Redis:Prefix"] = $"docs-reference-configuration-{Environment.ProcessId}-{Guid.NewGuid():N}",
        });
        builder.Services.AddStrata()
            .AddRedisCheckpoints()
            .AddRedisHistory()
            .AddGraph(_ => DurableGraph())
            .AddWakeDriver<Tick>()
            .AddRecoveryDriver<Tick>();
        using var host = builder.Build();
        await host.StartAsync();
        await host.StopAsync();
    }

    // appsettings.json, then the test's own settings over it.
    private static HostApplicationBuilder Builder(Dictionary<string, string?>? overrides = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddJsonFile(AppSettings, optional: false);
        if (overrides is not null)
        {
            builder.Configuration.AddInMemoryCollection(overrides);
        }

        return builder;
    }

    private static CompiledGraph<Tick> DurableGraph() =>
        new StateGraph<Tick>()
            .AddNode("Tick", tick => tick with { Done = true })
            .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });

    // The URL in the variable, or null to skip, unless STRATA_REQUIRE_BACKENDS=1 says the test must run.
    private static string? BackendUrl(string variable)
    {
        var url = Environment.GetEnvironmentVariable(variable);
        if (!string.IsNullOrEmpty(url))
        {
            return url;
        }

        return Environment.GetEnvironmentVariable("STRATA_REQUIRE_BACKENDS") == "1"
            ? throw new InvalidOperationException($"{variable} is not set, but STRATA_REQUIRE_BACKENDS=1 says these tests must run")
            : null;
    }
}

public sealed record Tick([property: JsonPropertyName("done")] bool Done);
