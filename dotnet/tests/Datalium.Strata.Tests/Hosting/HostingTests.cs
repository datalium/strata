using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Serialization;
using Datalium.Strata.Cloud;
using Datalium.Strata.Graph;
using Datalium.Strata.Hosting;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Observability;
using Datalium.Strata.Scheduling;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Storage.Redis;
using Datalium.Strata.Tests.Backends;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Npgsql;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using StackExchange.Redis;

namespace Datalium.Strata.Tests.Hosting;

/// <summary>
/// The hosting package (<c>design/35-Hosting-Integration.md</c>, SG-28): registration, settings, start-up,
/// run defaults, the drivers, health checks and instrumentation. The drivers' loops are mirrored by
/// <c>strata-host</c>'s <c>tests/drivers.rs</c>. The Postgres and Redis tests need
/// <c>STRATA_TEST_POSTGRES_URL</c> / <c>STRATA_TEST_REDIS_URL</c>.
/// </summary>
public class HostingTests
{
    private static readonly DateTimeOffset Noon = new(2026, 10, 6, 12, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(15);

    public sealed record Tick(
        [property: JsonPropertyName("ticks")] int Ticks,
        [property: JsonPropertyName("done")] bool Done);

    // --- Helpers ---

    private static HostApplicationBuilder Builder(TimeProvider? clock = null, Dictionary<string, string?>? settings = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        if (settings is not null)
        {
            builder.Configuration.AddInMemoryCollection(settings);
        }

        if (clock is not null)
        {
            builder.Services.AddSingleton(clock);
        }

        return builder;
    }

    /// <summary>Waits for <paramref name="condition"/>, calling <paramref name="nudge"/> (a step of the fake clock) between looks.</summary>
    private static async Task Until(Func<bool> condition, string what, Action? nudge = null)
    {
        var deadline = DateTime.UtcNow + Patience;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, what);
            nudge?.Invoke();
            await Task.Delay(10);
        }
    }

    private static async Task Until(Func<Task<bool>> condition, string what, Action? nudge = null)
    {
        var deadline = DateTime.UtcNow + Patience;
        while (!await condition())
        {
            Assert.True(DateTime.UtcNow < deadline, what);
            nudge?.Invoke();
            await Task.Delay(10);
        }
    }

    /// <summary><c>Wait</c> (a five-minute timer) -> <c>After</c>.</summary>
    private static CompiledGraph<Tick> TimerGraph(Func<Tick, Task>? whileAfter = null) =>
        new StateGraph<Tick>()
            .AddTimer("Wait", TimeSpan.FromMinutes(5))
            .AddNode("After", async (Tick tick, NodeContext _, CancellationToken _) =>
            {
                if (whileAfter is not null)
                {
                    await whileAfter(tick);
                }

                return tick with { Ticks = tick.Ticks + 1, Done = true };
            })
            .AddEdge("Wait", "After")
            .Compile();

    /// <summary><c>First</c> -> <c>Second</c>, with a checkpoint per node.</summary>
    private static CompiledGraph<Tick> DurableGraph() =>
        new StateGraph<Tick>()
            .AddNode("First", tick => tick with { Ticks = tick.Ticks + 1 })
            .AddNode("Second", tick => tick with { Done = true })
            .AddEdge("First", "Second")
            .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });

    private static async Task<List<Checkpoint>> Log(IServiceProvider services, string threadId) =>
        [.. (await services.GetRequiredService<ICheckpointQuery>().GetLogAsync(threadId)).Entries.Select(e => e.Checkpoint)];

    private static Checkpoint Finished(string threadId, string createdAt) =>
        StoreContracts.Checkpoint(threadId, createdAt) with { Status = CheckpointStatus.Completed };

    private sealed class Collector : IRunObserver
    {
        private readonly ConcurrentQueue<RunEvent> _events = new();

        public List<RunEvent> Events => [.. _events];

        public void OnEvent(RunEvent runEvent) => _events.Enqueue(runEvent);
    }

    private sealed class Greeter
    {
        public int Greeted;
    }

    /// <summary>A store with the store port only, as an application's own might be.</summary>
    private sealed class BareStore : ICheckpointStore
    {
        private readonly ConcurrentDictionary<string, Checkpoint> _latest = new();

        public void Save(Checkpoint checkpoint) => _latest[checkpoint.ThreadId] = checkpoint;

        public Checkpoint? Load(string threadId) => _latest.GetValueOrDefault(threadId);
    }

    /// <summary>The in-memory store behind a switch: while <see cref="Failing"/>, listing throws; while <see cref="Hanging"/>, a load never answers.</summary>
    private sealed class FlakyStore(TimeProvider clock) : ICheckpointStore, ICheckpointQuery
    {
        private readonly ICheckpointStore _inner = new InMemoryCheckpointStore(clock);
        private int _listings;

        public volatile bool Failing;

        public volatile bool Hanging;

        public int Listings => Volatile.Read(ref _listings);

        public void Save(Checkpoint checkpoint) => _inner.Save(checkpoint);

        public Checkpoint? Load(string threadId) => _inner.Load(threadId);

        public ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
            _inner.SaveAsync(checkpoint, cancellationToken);

        public ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
            _inner.LoadAsync(threadId, cancellationToken);

        public async ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default)
        {
            if (Hanging)
            {
                // A store that ignores its token.
                await Task.Delay(Timeout.Infinite, CancellationToken.None);
            }

            if (Failing)
            {
                throw new InvalidOperationException("Host=db.internal;Password=hunter2 is unreachable");
            }

            return await _inner.LoadLatestAsync(threadId, cancellationToken);
        }

        public ValueTask<bool> SaveIfLatestAsync(Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
            _inner.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken);

        public ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _listings);
            return Failing
                ? throw new InvalidOperationException("Host=db.internal;Password=hunter2 is unreachable")
                : ((ICheckpointQuery)_inner).ListThreadsAsync(query, cancellationToken);
        }

        public ValueTask<CheckpointLogPage> GetLogAsync(
            string threadId, long? afterSequence = null, int limit = ThreadQuery.DefaultLimit, CancellationToken cancellationToken = default) =>
            ((ICheckpointQuery)_inner).GetLogAsync(threadId, afterSequence, limit, cancellationToken);
    }

    // --- Registration ---

    [Fact]
    public void A_Store_Is_One_Instance_Under_Every_Port_It_Has_And_A_Second_Registration_Replaces_It()
    {
        var builder = Builder();
        builder.Services.AddStrata().AddInMemoryCheckpoints().AddInMemoryHistory();
        using (var host = builder.Build())
        {
            var store = host.Services.GetRequiredService<InMemoryCheckpointStore>();
            Assert.Same(store, host.Services.GetRequiredService<ICheckpointStore>());
            Assert.Same(store, host.Services.GetRequiredService<ICheckpointQuery>());
            Assert.Same(store, host.Services.GetRequiredService<ICheckpointRetention>());
            Assert.IsType<InMemoryHistoryStore>(host.Services.GetRequiredService<IHistoryStore>());
        }

        // A store with fewer ports leaves none of the first one's behind.
        builder = Builder();
        builder.Services.AddStrata().AddInMemoryCheckpoints().AddCheckpointStore(_ => new BareStore());
        using (var host = builder.Build())
        {
            Assert.IsType<BareStore>(host.Services.GetRequiredService<ICheckpointStore>());
            Assert.Null(host.Services.GetService<ICheckpointQuery>());
            Assert.Null(host.Services.GetService<ICheckpointRetention>());
            Assert.Single(host.Services.GetServices<ICheckpointStore>());
        }

        // Strata Cloud has the store port, the query port and the decisions client (design/43), and doesn't purge.
        builder = Builder();
        builder.Services.AddStrata().AddInMemoryCheckpoints().AddCloudCheckpoints(o =>
        {
            o.BaseUrl = new Uri("https://cloud.example");
            o.ApiKey = "sk-test";
        });
        using (var host = builder.Build())
        {
            var store = Assert.IsType<StrataCloudCheckpointStore>(host.Services.GetRequiredService<ICheckpointStore>());
            Assert.NotNull(host.Services.GetRequiredService<StrataCloudDecisions>());
            Assert.Same(store, host.Services.GetRequiredService<ICheckpointQuery>());
            Assert.Null(host.Services.GetService<ICheckpointRetention>());
        }
    }

    [Fact]
    public async Task Stores_Registered_As_The_Ports_Themselves_Resolve()
    {
        var builder = Builder();
        builder.Services.AddStrata()
            .AddCheckpointStore<ICheckpointStore>(sp => new InMemoryCheckpointStore(sp.GetRequiredService<TimeProvider>()))
            .AddHistoryStore<IHistoryStore>(_ => new InMemoryHistoryStore());
        using var host = builder.Build();

        // Forwarded to themselves, the ports would never resolve.
        var (checkpoints, history) = await Task.Run(() =>
                (host.Services.GetRequiredService<ICheckpointStore>(), host.Services.GetRequiredService<IHistoryStore>()))
            .WaitAsync(Patience);
        Assert.IsType<InMemoryCheckpointStore>(checkpoints);
        Assert.IsType<InMemoryHistoryStore>(history);
        Assert.Single(host.Services.GetServices<ICheckpointStore>());
        Assert.Single(host.Services.GetServices<IHistoryStore>());
        await host.StartAsync();
        await host.StopAsync();
    }

    [Fact]
    public void Settings_Come_From_Configuration_And_A_Delegate_Is_Applied_Over_Them()
    {
        var builder = Builder(settings: new()
        {
            ["Strata:Postgres:ConnectionString"] = "Host=db",
            ["Strata:Postgres:ConnectionStringName"] = "checkpoints",
            ["Strata:Postgres:AutoMigrate"] = "false",
            ["Strata:Postgres:Retry:MaxAttempts"] = "6",
            ["Strata:Postgres:Retry:MaxDelay"] = "00:00:05",
            ["Strata:Redis:Prefix"] = "ops",
            ["Strata:Redis:FinishedThreadTtl"] = "30.00:00:00",
            ["Strata:Redis:Retry:InitialDelay"] = "00:00:00.100",
            ["Strata:Sqlite:Path"] = "strata.sqlite",
            ["Strata:Cloud:BaseUrl"] = "https://cloud.example",
            ["Strata:Cloud:ApiKey"] = "sk-test",
            ["Strata:Cloud:RequestTimeout"] = "00:00:10",
            ["Strata:Runs:Lease:Owner"] = "worker-7",
            ["Strata:Runs:Lease:Ttl"] = "00:00:30",
            ["Strata:Wake:Interval"] = "00:00:02",
            ["Strata:Wake:BatchSize"] = "10",
            ["Strata:Wake:Metadata:graph"] = "investigation",
            ["Strata:Wake:MatchGraph"] = "false",
            ["Strata:Recovery:Interval"] = "00:00:15",
            ["Strata:Recovery:StaleAfter"] = "00:10:00",
            ["Strata:Recovery:MaxRecoveries"] = "3",
            ["Strata:Retention:OlderThan"] = "30.00:00:00",
            ["Strata:Retention:States:0"] = "Completed",
            ["Strata:Retention:States:1"] = "Failed",
        });
        builder.Services.AddStrata()
            .AddPostgresCheckpoints(o => o.Retry.MaxAttempts = 9)
            .AddPostgresHistory(o => o.ConnectionStringName = "history")
            .AddRedisHistory()
            .AddSqliteHistory()
            .AddCloudCheckpoints()
            .AddGraph(_ => DurableGraph())
            .AddWakeDriver<Tick>(o => o.BatchSize = 25)
            .AddRecoveryDriver<Tick>()
            .AddRetention(o => o.Interval = TimeSpan.FromMinutes(10));
        using var host = builder.Build();
        T Get<T>()
            where T : class => host.Services.GetRequiredService<IOptions<T>>().Value;
        T Named<T>()
            where T : class => host.Services.GetRequiredService<IOptionsMonitor<T>>().Get(typeof(Tick).FullName);

        var postgres = Get<StrataPostgresOptions>();
        Assert.Equal(("Host=db", false), (postgres.ConnectionString, postgres.AutoMigrate));
        Assert.Equal(TimeSpan.FromSeconds(5), postgres.Retry.MaxDelay);
        Assert.Equal(TimeSpan.FromMilliseconds(50), postgres.Retry.InitialDelay);

        // The delegates are applied over the configuration, each once, in the order they were given.
        Assert.Equal(9, postgres.Retry.MaxAttempts);
        Assert.Equal("history", postgres.ConnectionStringName);

        var redis = Get<StrataRedisOptions>();
        Assert.Equal(("ops", TimeSpan.FromDays(30)), (redis.Prefix, redis.FinishedThreadTtl));
        Assert.Equal(TimeSpan.FromMilliseconds(100), redis.Retry.InitialDelay);
        Assert.Equal(4, redis.Retry.MaxAttempts);
        Assert.Equal("strata.sqlite", Get<StrataSqliteOptions>().Path);

        var cloud = Get<StrataCloudHostOptions>();
        Assert.Equal((new Uri("https://cloud.example"), "sk-test"), (cloud.BaseUrl, cloud.ApiKey));
        Assert.Equal(TimeSpan.FromSeconds(10), cloud.RequestTimeout);

        var runs = Get<StrataRunOptions>();
        Assert.Equal(("worker-7", TimeSpan.FromSeconds(30)), (runs.Lease.Owner, runs.Lease.Ttl));

        var wake = Named<WakeDriverOptions>();
        Assert.Equal((TimeSpan.FromSeconds(2), 25, false), (wake.Interval, wake.BatchSize, wake.MatchGraph));
        Assert.Equal("investigation", wake.Metadata["graph"]);

        var recovery = Named<RecoveryDriverOptions>();
        Assert.Equal(
            (TimeSpan.FromSeconds(15), TimeSpan.FromMinutes(10), 3),
            (recovery.Interval, recovery.StaleAfter, recovery.MaxRecoveries));

        var retention = Get<RetentionOptions>();
        Assert.Equal((TimeSpan.FromMinutes(10), TimeSpan.FromDays(30)), (retention.Interval, retention.OlderThan));
        Assert.Equal([CheckpointThreadState.Completed, CheckpointThreadState.Failed], retention.States);
    }

    [Fact]
    public void Without_Configuration_Every_Setting_Has_Its_Default()
    {
        var services = new ServiceCollection();
        services.AddStrata().AddGraph(_ => TimerGraph()).AddWakeDriver<Tick>().AddRecoveryDriver<Tick>().AddRetention();
        using var provider = services.BuildServiceProvider();

        var runs = provider.GetRequiredService<IOptions<StrataRunOptions>>().Value;
        Assert.Null(runs.Lease.Ttl);
        Assert.Equal($"{Environment.MachineName}/{Environment.ProcessId}", runs.Lease.Owner);

        var wake = provider.GetRequiredService<IOptionsMonitor<WakeDriverOptions>>().Get(typeof(Tick).FullName);
        Assert.Equal((TimeSpan.FromSeconds(5), 100, true), (wake.Interval, wake.BatchSize, wake.MatchGraph));
        var recovery = provider.GetRequiredService<IOptionsMonitor<RecoveryDriverOptions>>().Get(typeof(Tick).FullName);
        Assert.Equal((TimeSpan.FromSeconds(30), 100), (recovery.Interval, recovery.BatchSize));
        Assert.Null(recovery.StaleAfter);
        var retention = provider.GetRequiredService<IOptions<RetentionOptions>>().Value;
        Assert.Equal((TimeSpan.FromHours(1), 1000), (retention.Interval, retention.BatchSize));
        Assert.Null(retention.OlderThan);
        Assert.Empty(retention.States);

        var retry = new StoreRetryOptions();
        Assert.Equal(new StoreRetryPolicy(), new StoreRetryPolicy { MaxAttempts = retry.MaxAttempts, InitialDelay = retry.InitialDelay, MaxDelay = retry.MaxDelay });
    }

    // --- Start-up ---

    [Fact]
    public async Task A_Graph_That_Does_Not_Compile_Or_A_Store_With_No_Connection_Stops_The_Host_From_Starting()
    {
        var orphaned = Builder();
        orphaned.Services.AddStrata().AddInMemoryCheckpoints().AddGraph(_ =>
            new StateGraph<Tick>()
                .AddNode("A", tick => tick)
                .AddNode("B", tick => tick)
                .AddNode("Orphan", tick => tick)
                .AddEdge("A", "B")
                .Compile());
        using (var host = orphaned.Build())
        {
            await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync());
        }

        var unconnected = Builder();
        unconnected.Services.AddStrata().AddPostgresCheckpoints();
        using (var host = unconnected.Build())
        {
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
            Assert.StartsWith("Strata's Postgres store has no connection", refused.Message, StringComparison.Ordinal);
        }

        var fileless = Builder();
        fileless.Services.AddStrata().AddSqliteCheckpoints();
        using (var host = fileless.Build())
        {
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
            Assert.Equal("Strata's SQLite store has no file: set Strata:Sqlite:Path", refused.Message);
        }
    }

    [Fact]
    public async Task A_Driver_Without_What_It_Needs_Stops_The_Host_From_Starting()
    {
        async Task<string> Refusal(Action<IStrataBuilder> register, Dictionary<string, string?>? settings = null)
        {
            var builder = Builder(settings: settings);
            register(builder.Services.AddStrata());
            using var host = builder.Build();
            return (await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync())).Message;
        }

        Assert.Equal(
            "the wake driver for Tick has no graph: register one with AddGraph<Tick>",
            await Refusal(strata => strata.AddInMemoryCheckpoints().AddWakeDriver<Tick>()));
        Assert.Equal(
            "the wake driver needs a checkpoint store that lists threads (ICheckpointQuery), and the registered one doesn't",
            await Refusal(strata => strata.AddCheckpointStore(_ => new BareStore()).AddGraph(_ => TimerGraph()).AddWakeDriver<Tick>()));
        Assert.Equal(
            "the wake driver's Interval must be more than zero",
            await Refusal(strata => strata.AddInMemoryCheckpoints().AddGraph(_ => TimerGraph()).AddWakeDriver<Tick>(o => o.Interval = TimeSpan.Zero)));

        // Only a graph that checkpoints every node leaves a running thread, and without leases only its age
        // says its run died.
        Assert.StartsWith(
            "the recovery driver for Tick needs a graph compiled with CheckpointPolicy.EveryNode",
            await Refusal(strata => strata.AddInMemoryCheckpoints().AddGraph(_ => TimerGraph()).AddRecoveryDriver<Tick>()),
            StringComparison.Ordinal);
        Assert.StartsWith(
            "the recovery driver needs Strata:Runs:Lease:Ttl or Strata:Recovery:StaleAfter",
            await Refusal(strata => strata.AddInMemoryCheckpoints().AddGraph(_ => DurableGraph()).AddRecoveryDriver<Tick>()),
            StringComparison.Ordinal);

        Assert.StartsWith(
            "the retention driver needs Strata:Retention:OlderThan",
            await Refusal(strata => strata.AddInMemoryCheckpoints().AddRetention()),
            StringComparison.Ordinal);
        Assert.StartsWith(
            "the retention driver needs a checkpoint store that purges",
            await Refusal(strata => strata.AddCheckpointStore(_ => new BareStore()).AddRetention(o => o.OlderThan = TimeSpan.FromDays(1))),
            StringComparison.Ordinal);
        // Strata Cloud lists threads (design/43), so the wake and recovery drivers take it; purging is Cloud's own.
        Assert.StartsWith(
            "the retention driver needs a checkpoint store that purges",
            await Refusal(strata => strata
                .AddCloudCheckpoints(o =>
                {
                    o.BaseUrl = new Uri("https://cloud.example");
                    o.ApiKey = "sk-test";
                })
                .AddGraph(_ => TimerGraph())
                .AddWakeDriver<Tick>()
                .AddRetention(o => o.OlderThan = TimeSpan.FromDays(1))),
            StringComparison.Ordinal);
        Assert.StartsWith(
            "the decision worker needs Strata Cloud's decisions client",
            await Refusal(strata => strata.AddInMemoryCheckpoints().AddGraph(_ => TimerGraph()).AddDecisionWorker<Tick>()),
            StringComparison.Ordinal);
    }

    [Fact]
    public void Group_Commit_Is_Off_Unless_Enabled_And_Its_Settings_Reach_The_Stores()
    {
        var path = Path.Combine(Path.GetTempPath(), $"strata_hosting_{Guid.NewGuid():N}.sqlite");
        try
        {
            var builder = Builder(settings: new()
            {
                ["Strata:Sqlite:Path"] = path,
                ["Strata:Sqlite:GroupCommit:Enabled"] = "true",
                ["Strata:Postgres:GroupCommit:Enabled"] = "true",
                ["Strata:Postgres:GroupCommit:MaxBatch"] = "64",
                ["Strata:Postgres:GroupCommit:MaxConcurrentFlushes"] = "2",
            });
            builder.Services.AddStrata().AddSqliteCheckpoints().AddPostgresHistory();
            using (var host = builder.Build())
            {
                Assert.True(((EfCoreCheckpointStore)host.Services.GetRequiredService<ICheckpointStore>()).GroupCommits);
                var postgres = host.Services.GetRequiredService<IOptions<StrataPostgresOptions>>().Value.GroupCommit;
                Assert.Equal(new GroupCommitOptions { MaxBatch = 64, MaxConcurrentFlushes = 2 }, postgres.ToOptions());
            }

            var plain = Builder(settings: new() { ["Strata:Sqlite:Path"] = path });
            plain.Services.AddStrata().AddSqliteCheckpoints();
            using (var host = plain.Build())
            {
                Assert.False(((EfCoreCheckpointStore)host.Services.GetRequiredService<ICheckpointStore>()).GroupCommits);
            }
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Offload_Is_Off_Unless_Enabled_And_Its_Settings_Reach_The_Stores()
    {
        var path = Path.Combine(Path.GetTempPath(), $"strata_hosting_{Guid.NewGuid():N}.sqlite");
        var blobs = Path.Combine(Path.GetTempPath(), $"strata_hosting_blobs_{Guid.NewGuid():N}");
        try
        {
            var builder = Builder(settings: new()
            {
                ["Strata:Sqlite:Path"] = path,
                ["Strata:Sqlite:Offload:Enabled"] = "true",
                ["Strata:Sqlite:Offload:Threshold"] = "1",
                ["Strata:Sqlite:Offload:Path"] = blobs,
                ["Strata:Postgres:Offload:Enabled"] = "true",
                ["Strata:Postgres:Offload:Threshold"] = "4096",
            });
            builder.Services.AddStrata().AddSqliteCheckpoints().AddPostgresHistory();
            using (var host = builder.Build())
            {
                var store = (EfCoreCheckpointStore)host.Services.GetRequiredService<ICheckpointStore>();
                Assert.True(store.Offloads.On);
                Assert.Equal(blobs, Assert.IsType<FileSystemCheckpointBlobStore>(store.Offloads.Blobs).Root);
                await store.SaveAsync(Finished("t", "2026-09-16T00:00:00Z"));
                Assert.NotNull(await store.LoadAsync("t"));
                Assert.NotEmpty(Directory.GetDirectories(blobs));
                var postgres = host.Services.GetRequiredService<IOptions<StrataPostgresOptions>>().Value.Offload.ToOptions(host.Services);
                Assert.Equal((4096, null), (postgres!.Threshold, postgres.Blobs));
                Assert.Null(host.Services.GetRequiredService<IOptions<StrataRedisOptions>>().Value.Offload.ToOptions(host.Services));
            }

            // A blob store the application registered is the one the stores use; one and a path together are refused.
            var registered = Builder(settings: new()
            {
                ["Strata:Redis:Offload:Enabled"] = "true",
                ["Strata:Postgres:Offload:Enabled"] = "true",
                ["Strata:Postgres:Offload:Path"] = blobs,
            });
            var own = new InMemoryCheckpointBlobStore();
            registered.Services.AddSingleton<ICheckpointBlobStore>(own);
            registered.Services.AddStrata().AddRedisHistory().AddPostgresHistory();
            using (var host = registered.Build())
            {
                Assert.Same(own, host.Services.GetRequiredService<IOptions<StrataRedisOptions>>().Value.Offload.ToOptions(host.Services)!.Blobs);
                var refused = Assert.Throws<InvalidOperationException>(() =>
                    host.Services.GetRequiredService<IOptions<StrataPostgresOptions>>().Value.Offload.ToOptions(host.Services));
                Assert.Contains("not both", refused.Message, StringComparison.Ordinal);
            }
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            File.Delete(path);
            if (Directory.Exists(blobs))
            {
                Directory.Delete(blobs, recursive: true);
            }
        }
    }

    [Fact]
    public void Deltas_Are_Off_Unless_Enabled_And_Their_Settings_Reach_The_Stores()
    {
        var path = Path.Combine(Path.GetTempPath(), $"strata_hosting_{Guid.NewGuid():N}.sqlite");
        try
        {
            var builder = Builder(settings: new()
            {
                ["Strata:Sqlite:Path"] = path,
                ["Strata:Sqlite:Deltas:Enabled"] = "true",
                ["Strata:Postgres:Deltas:Enabled"] = "true",
                ["Strata:Postgres:Deltas:FullEvery"] = "8",
                ["Strata:Redis:Deltas:Enabled"] = "true",
                ["Strata:Redis:Deltas:CachedThreads"] = "500",
            });
            builder.Services.AddStrata().AddSqliteCheckpoints().AddPostgresHistory().AddRedisHistory();
            using (var host = builder.Build())
            {
                Assert.True(((EfCoreCheckpointStore)host.Services.GetRequiredService<ICheckpointStore>()).WritesDeltas);
                Assert.Equal(
                    new DeltaOptions { FullEvery = 8 },
                    host.Services.GetRequiredService<IOptions<StrataPostgresOptions>>().Value.Deltas.ToOptions());
                Assert.Equal(
                    new DeltaOptions { CachedThreads = 500 },
                    host.Services.GetRequiredService<IOptions<StrataRedisOptions>>().Value.Deltas.ToOptions());
            }

            var plain = Builder(settings: new() { ["Strata:Sqlite:Path"] = path });
            plain.Services.AddStrata().AddSqliteCheckpoints();
            using (var host = plain.Build())
            {
                Assert.False(((EfCoreCheckpointStore)host.Services.GetRequiredService<ICheckpointStore>()).WritesDeltas);
            }
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            File.Delete(path);
        }
    }

    [Fact]
    public async Task The_Sqlite_Stores_Share_One_File()
    {
        var path = Path.Combine(Path.GetTempPath(), $"strata_hosting_{Guid.NewGuid():N}.sqlite");
        try
        {
            var builder = Builder(settings: new() { ["Strata:Sqlite:Path"] = path });
            builder.Services.AddStrata().AddSqliteCheckpoints().AddSqliteHistory();
            using (var host = builder.Build())
            {
                await host.StartAsync();
                var store = host.Services.GetRequiredService<ICheckpointStore>();
                Assert.IsType<EfCoreCheckpointStore>(store);
                await store.SaveAsync(Finished("t", "2026-09-16T00:00:00Z"));
                await host.Services.GetRequiredService<IHistoryStore>().AppendAsync(
                    "s", new ChatMessage(MessageRole.User, "hello".AsMemory(), Noon));
                Assert.Equal(["t"], (await host.Services.GetRequiredService<ICheckpointQuery>().ListThreadsAsync(new ThreadQuery())).Threads.Select(t => t.ThreadId));
                await host.StopAsync();
            }

            Assert.True(File.Exists(path));
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            File.Delete(path);
        }
    }

    [PostgresFact]
    public async Task A_Postgres_Store_On_The_Applications_Data_Source_Is_Migrated_At_Startup_And_Not_Disposed()
    {
        using var sandbox = new PostgresSandbox("hosting");
        await using var dataSource = sandbox.CreateDataSource();
        async Task<long> Scalar(string sql)
        {
            await using var command = dataSource.CreateCommand(sql);
            return (long)(await command.ExecuteScalarAsync())!;
        }

        // A store that doesn't migrate refuses a database that is behind, and the host with it.
        IHost Manual()
        {
            var manual = Builder(settings: new() { ["Strata:Postgres:AutoMigrate"] = "false" });
            manual.Services.AddKeyedSingleton("strata", dataSource);
            manual.Services.AddStrata().AddPostgresCheckpoints(o => o.DataSourceKey = "strata");
            return manual.Build();
        }

        using (var host = Manual())
        {
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
            Assert.Equal("database is at checkpoints schema version 0, and this build needs 2: run its migration", refused.Message);
        }

        var builder = Builder();
        builder.Services.AddSingleton(dataSource);
        builder.Services.AddStrata()
            .AddPostgresCheckpoints(o => o.UseRegisteredDataSource = true)
            .AddPostgresHistory();
        using (var host = builder.Build())
        {
            await host.StartAsync();

            // Migrated before anything used the stores.
            Assert.Equal(
                2,
                await Scalar(
                    $"SELECT count(*) FROM strata_schema_version WHERE (component = 'checkpoints' AND version = {StoreSchemaVersions.CheckpointsVersion}) " +
                    $"OR (component = 'history' AND version = {StoreSchemaVersions.HistoryVersion})"));
            var store = host.Services.GetRequiredService<ICheckpointStore>();
            Assert.Same(host.Services.GetRequiredService<PostgresCheckpointStore>(), store);
            Assert.Same(store, host.Services.GetRequiredService<ICheckpointRetention>());
            await store.SaveAsync(Finished("t", "2026-09-16T00:00:00Z"));
            Assert.NotNull(await store.LoadAsync("t"));
            Assert.IsType<PostgresHistoryStore>(host.Services.GetRequiredService<IHistoryStore>());
            await host.StopAsync();
        }

        // The data source is the application's: the stores went with the container, and it didn't.
        Assert.Equal(1, await Scalar("SELECT count(*) FROM checkpoints"));

        // And now the store that only checks is satisfied.
        using (var host = Manual())
        {
            await host.StartAsync();
            await host.StopAsync();
        }
    }

    [RedisFact]
    public async Task The_Redis_Stores_Share_The_Connection_The_Prefix_And_The_Ttl()
    {
        using var sandbox = new RedisSandbox("hosting");
        var owned = Builder(settings: new()
        {
            ["ConnectionStrings:strata-redis"] = TestBackends.RedisUrl,
            ["Strata:Redis:Prefix"] = sandbox.Prefix,
            ["Strata:Redis:FinishedThreadTtl"] = "01:00:00",
        });
        owned.Services.AddStrata().AddRedisCheckpoints().AddRedisHistory();
        using (var host = owned.Build())
        {
            await host.StartAsync();
            var store = host.Services.GetRequiredService<ICheckpointStore>();
            Assert.IsType<RedisCheckpointStore>(store);
            await store.SaveAsync(Finished("t", "2026-09-16T00:00:00Z"));
            Assert.NotNull(await sandbox.Database.KeyTimeToLiveAsync($"{sandbox.Prefix}:checkpoints:t"));
            await host.Services.GetRequiredService<IHistoryStore>().AppendAsync(
                "s", new ChatMessage(MessageRole.User, "hello".AsMemory(), Noon));
            Assert.Equal(1, await sandbox.Database.ListLengthAsync($"{sandbox.Prefix}:history:s"));
            await host.StopAsync();
        }

        // On the application's multiplexer, which the stores don't close.
        var shared = Builder(settings: new() { ["Strata:Redis:Prefix"] = sandbox.Prefix });
        shared.Services.AddSingleton<IConnectionMultiplexer>(sandbox.Connection);
        shared.Services.AddStrata().AddRedisCheckpoints(o => o.UseRegisteredMultiplexer = true);
        using (var host = shared.Build())
        {
            await host.StartAsync();
            Assert.NotNull(await host.Services.GetRequiredService<ICheckpointStore>().LoadAsync("t"));
            await host.StopAsync();
        }

        Assert.True(sandbox.Connection.IsConnected);
    }

    // --- Run defaults ---

    [Fact]
    public async Task The_Runner_Runs_With_The_Hosts_Defaults_And_A_Callers_Options_Win()
    {
        var clock = new FakeTimeProvider(Noon);
        var collector = new Collector();
        var builder = Builder(clock, new()
        {
            ["Strata:Runs:Lease:Owner"] = "worker-7",
            ["Strata:Runs:Lease:Ttl"] = "00:00:30",
        });
        builder.Services.AddSingleton<Greeter>();
        builder.Services.AddSingleton<IRunObserver>(collector);
        builder.Services.AddStrata().AddInMemoryCheckpoints().AddInMemoryHistory().AddGraph(_ =>
            new StateGraph<Tick>()
                .AddNode("Greet", (Tick tick, NodeContext context, CancellationToken _) =>
                {
                    // The container reaches the node.
                    Interlocked.Increment(ref context.Services!.GetRequiredService<Greeter>().Greeted);
                    return ValueTask.FromResult(tick with { Done = context.History is not null });
                })
                .Compile());
        using var host = builder.Build();
        await host.StartAsync();
        var runner = host.Services.GetRequiredService<StrataRunner<Tick>>();

        var result = await runner.RunAsync("t", new Tick(0, false));

        Assert.True(Assert.IsType<GraphExecutionResult<Tick>.Completed>(result).State.Done);
        Assert.Equal(1, host.Services.GetRequiredService<Greeter>().Greeted);
        await Until(() => collector.Events.OfType<RunCompleted>().Any(), "the run's events reached the registered observer");
        Assert.Equal("worker-7", Assert.Single(collector.Events.OfType<LeaseAcquired>()).Owner);

        var options = runner.CreateOptions(new Dictionary<string, string> { ["tenant"] = "initech" }, runId: "run-1");
        Assert.Same(host.Services.GetRequiredService<Greeter>(), options.Services!.GetRequiredService<Greeter>());
        Assert.Same(clock, options.TimeProvider);
        Assert.Equal(("initech", "run-1"), (options.Metadata["tenant"], options.RunId));
        Assert.Equal(new LeaseOptions("worker-7", TimeSpan.FromSeconds(30)), options.Lease);

        // A caller's own options are used as they are: no lease, no observer, no services.
        var own = new Collector();
        var plain = await runner.RunAsync("u", new Tick(0, false), new RunOptions { Observer = own, Services = host.Services });
        Assert.False(Assert.IsType<GraphExecutionResult<Tick>.Completed>(plain).State.Done);
        await Until(() => own.Events.OfType<RunCompleted>().Any(), "the run's events reached the caller's observer");
        Assert.Empty(own.Events.OfType<LeaseAcquired>());
        await host.StopAsync();
    }

    // --- The run scheduler (design/40-Run-Scheduler.md) ---

    [Fact]
    public async Task The_Scheduler_Comes_From_Configuration_And_Every_Run_The_Host_Starts_Uses_It()
    {
        var collector = new Collector();
        var builder = Builder(settings: new()
        {
            ["Strata:Scheduler:MaxConcurrent"] = "3",
            ["Strata:Scheduler:MaxQueued"] = "7",
            ["Strata:Scheduler:MaxQueueTime"] = "00:01:00",
            ["Strata:Scheduler:Reservations:0:MinPriority"] = "3",
            ["Strata:Scheduler:Reservations:0:Slots"] = "1",
            ["Strata:Scheduler:TenantKey"] = "customer",
            ["Strata:Scheduler:TenantLimit"] = "2",
            ["Strata:Scheduler:TenantLimits:acme"] = "5",
            ["Strata:Scheduler:TenantMaxQueued"] = "4",
            ["Strata:Wake:Priority"] = "4",
            ["Strata:Recovery:Priority"] = "6",
            ["Strata:Recovery:StaleAfter"] = "00:05:00",
        });
        builder.Services.AddSingleton<IRunObserver>(collector);
        builder.Services.AddStrata()
            .AddInMemoryCheckpoints()
            .AddScheduler(o => o.Name = "investigations")
            .AddGraph(_ => DurableGraph())
            .AddWakeDriver<Tick>()
            .AddRecoveryDriver<Tick>();
        using var host = builder.Build();
        await host.StartAsync();

        var scheduler = host.Services.GetRequiredService<RunScheduler>();
        var options = scheduler.Options;
        Assert.Equal(("investigations", 3, 7, TimeSpan.FromMinutes(1)), (options.Name, options.MaxConcurrent, options.MaxQueued, options.MaxQueueTime));
        Assert.Equal([new SlotReservation(3, 1)], options.Reservations);
        Assert.Equal(("customer", 2, 5, 4), (options.TenantKey, options.TenantLimit, options.TenantLimits["acme"], options.TenantMaxQueued));
        var graphName = typeof(Tick).FullName!;
        Assert.Equal(4, host.Services.GetRequiredService<IOptionsMonitor<WakeDriverOptions>>().Get(graphName).Priority);
        Assert.Equal(6, host.Services.GetRequiredService<IOptionsMonitor<RecoveryDriverOptions>>().Get(graphName).Priority);

        // The runner's runs wait for its slots, at the priority asked for.
        var runner = host.Services.GetRequiredService<StrataRunner<Tick>>();
        Assert.Same(scheduler, runner.CreateOptions().Scheduler);
        Assert.Equal((0, 2), (runner.CreateOptions().Priority, runner.CreateOptions(priority: 2).Priority));
        Assert.IsType<GraphExecutionResult<Tick>.Completed>(await runner.RunAsync("t", new Tick(0, false)));
        await Until(() => collector.Events.OfType<RunStarted>().Any(), "the run started");
        Assert.NotNull(collector.Events.OfType<RunStarted>().Single().QueuedFor);
        await host.StopAsync();
    }

    [Fact]
    public async Task Invalid_Scheduler_Options_Stop_The_Host_From_Starting()
    {
        var builder = Builder(settings: new() { ["Strata:Scheduler:MaxConcurrent"] = "0" });
        builder.Services.AddStrata().AddInMemoryCheckpoints().AddScheduler();
        using var host = builder.Build();

        await Assert.ThrowsAnyAsync<ArgumentException>(() => host.StartAsync());
    }

    [Fact]
    public async Task The_Scheduler_Is_Closed_When_The_Host_Stops_And_Nothing_Holding_A_Slot_Is_Touched()
    {
        var builder = Builder();
        builder.Services.AddStrata().AddInMemoryCheckpoints().AddScheduler(o => o.MaxConcurrent = 1);
        using var host = builder.Build();
        await host.StartAsync();
        var scheduler = host.Services.GetRequiredService<RunScheduler>();
        using var holder = await scheduler.AdmitAsync(new AdmissionRequest());
        var waiting = scheduler.AdmitAsync(new AdmissionRequest()).AsTask();
        Assert.False(scheduler.IsClosed);

        await host.StopAsync();

        Assert.True(scheduler.IsClosed);
        Assert.Equal(NotAdmittedReason.Closed, (await waiting.WaitAsync(Patience)).Refusal);
        Assert.Equal(1, scheduler.Snapshot().Active);
    }

    [Fact]
    public async Task The_Wake_Driver_Runs_At_Its_Own_Priority()
    {
        var clock = new FakeTimeProvider(Noon);
        var builder = Builder(clock);

        // Two slots, one of them only for priority 5 and up, and nothing may wait.
        builder.Services.AddStrata()
            .AddInMemoryCheckpoints()
            .AddScheduler(o =>
            {
                o.MaxConcurrent = 2;
                o.MaxQueued = 0;
                o.Reservations.Add(new StrataSlotReservation { MinPriority = 5, Slots = 1 });
            })
            .AddGraph(_ => TimerGraph())
            .AddWakeDriver<Tick>(o => o.Priority = 5);
        using var host = builder.Build();
        var runner = host.Services.GetRequiredService<StrataRunner<Tick>>();
        Assert.IsType<GraphExecutionResult<Tick>.Interrupted>(await runner.RunAsync("t", new Tick(0, false)));
        var scheduler = host.Services.GetRequiredService<RunScheduler>();
        using var elsewhere = await scheduler.AdmitAsync(new AdmissionRequest());

        // What's left is the reserved slot: a run at the default priority is refused it...
        Assert.Equal(
            NotAdmittedReason.QueueFull,
            Assert.IsType<GraphExecutionResult<Tick>.NotAdmitted>(await runner.RunAsync("u", new Tick(0, false))).Reason);

        // ...and the wake driver, at 5, takes it.
        await host.StartAsync();
        clock.Advance(TimeSpan.FromMinutes(6));
        await Until(
            async () => (await Log(host.Services, "t")).Count == 2,
            "the due timer was woken",
            () => clock.Advance(TimeSpan.FromSeconds(5)));
        await host.StopAsync();
    }

    // --- Drivers ---

    [Fact]
    public void A_Failing_Driver_Waits_Twice_As_Long_Each_Time_Up_To_A_Cap()
    {
        var five = TimeSpan.FromSeconds(5);
        Assert.Equal(five, StrataDriver.WaitAfter(five, 0));
        Assert.Equal(TimeSpan.FromSeconds(10), StrataDriver.WaitAfter(five, 1));
        Assert.Equal(TimeSpan.FromSeconds(20), StrataDriver.WaitAfter(five, 2));
        Assert.Equal(TimeSpan.FromSeconds(160), StrataDriver.WaitAfter(five, 5));
        Assert.Equal(TimeSpan.FromMinutes(5), StrataDriver.WaitAfter(five, 6));
        Assert.Equal(TimeSpan.FromMinutes(5), StrataDriver.WaitAfter(five, 1000));

        // An interval longer than the cap is its own cap: a failing driver is never faster than a healthy one.
        Assert.Equal(TimeSpan.FromHours(1), StrataDriver.WaitAfter(TimeSpan.FromHours(1), 3));
    }

    [Fact]
    public async Task The_Wake_Driver_Wakes_A_Due_Timer()
    {
        var clock = new FakeTimeProvider(Noon);
        var builder = Builder(clock);
        builder.Services.AddStrata().AddInMemoryCheckpoints().AddGraph(_ => TimerGraph()).AddWakeDriver<Tick>();
        using var host = builder.Build();
        var runner = host.Services.GetRequiredService<StrataRunner<Tick>>();
        Assert.IsType<GraphExecutionResult<Tick>.Interrupted>(await runner.RunAsync("t", new Tick(0, false)));
        await host.StartAsync();

        // Not due for five minutes: rounds come and go and nothing is written.
        for (var i = 0; i < 6; i++)
        {
            clock.Advance(TimeSpan.FromSeconds(5));
            await Task.Delay(10);
        }

        Assert.Single(await Log(host.Services, "t"));

        // Due: within an interval the driver has resumed it.
        clock.Advance(TimeSpan.FromMinutes(5));
        await Until(
            async () => (await Log(host.Services, "t")).Count == 2,
            "the due timer was woken",
            () => clock.Advance(TimeSpan.FromSeconds(5)));
        Assert.NotNull((await Log(host.Services, "t"))[1].ResumedAt);

        var wake = Assert.Single(host.Services.GetRequiredService<IStrataDriverStatus>().Drivers);
        Assert.Equal(("wake:Tick", 0), (wake.Name, wake.FailuresInARow));
        Assert.True(wake.Rounds >= 2);
        await host.StopAsync();
    }

    [Fact]
    public async Task The_Recovery_Driver_Recovers_A_Run_That_Died()
    {
        var clock = new FakeTimeProvider(Noon);
        var builder = Builder(clock, new() { ["Strata:Runs:Lease:Ttl"] = "00:00:30" });
        builder.Services.AddStrata().AddInMemoryCheckpoints().AddGraph(_ => DurableGraph()).AddRecoveryDriver<Tick>();
        using var host = builder.Build();
        var runner = host.Services.GetRequiredService<StrataRunner<Tick>>();

        // A thread whose process died after First, as its last checkpoint left it.
        Assert.IsType<GraphExecutionResult<Tick>.Completed>(await runner.RunAsync("seed", new Tick(0, false)));
        var dead = (await Log(host.Services, "seed"))[1] with { ThreadId = "dead" };
        await runner.Store.SaveAsync(dead);

        await host.StartAsync();

        await Until(
            async () => (await Log(host.Services, "dead"))[^1].Status == CheckpointStatus.Completed,
            "the dead run was recovered",
            () => clock.Advance(TimeSpan.FromSeconds(30)));
        var log = await Log(host.Services, "dead");
        Assert.Equal([CheckpointStatus.Running, CheckpointStatus.Running, CheckpointStatus.Completed], log.Select(c => c.Status));
        Assert.Equal("1", log[1].Metadata!["strata.recoveries"]);
        await host.StopAsync();
    }

    [Fact]
    public async Task The_Retention_Driver_Purges_Old_Finished_Threads_Batch_After_Batch()
    {
        var clock = new FakeTimeProvider(Noon);
        var builder = Builder(clock, new() { ["Strata:Retention:OlderThan"] = "30.00:00:00", ["Strata:Retention:BatchSize"] = "2" });
        builder.Services.AddStrata().AddInMemoryCheckpoints().AddRetention();
        using var host = builder.Build();
        var store = host.Services.GetRequiredService<ICheckpointStore>();
        foreach (var old in (string[])["a", "b", "c", "d", "e"])
        {
            await store.SaveAsync(Finished(old, "2026-01-01T00:00:00Z"));
        }

        await store.SaveAsync(Finished("recent", "2026-10-01T00:00:00Z"));
        await store.SaveAsync(StoreContracts.Checkpoint("waiting", "2026-01-01T00:00:00Z"));

        await host.StartAsync();

        // One round, three purges of two: everything old and finished is gone before the driver waits.
        var status = host.Services.GetRequiredService<IStrataDriverStatus>();
        await Until(() => status.Drivers.Any(d => d.Name == "retention" && d.Rounds == 1), "the first round finished");
        Assert.Equal(
            ["recent", "waiting"],
            (await host.Services.GetRequiredService<ICheckpointQuery>().ListThreadsAsync(new ThreadQuery()))
                .Threads.Select(t => t.ThreadId).Order(StringComparer.Ordinal));
        await host.StopAsync();
    }

    [Fact]
    public async Task A_Round_That_Fails_Is_Counted_The_Driver_Backs_Off_And_Goes_On_When_The_Store_Is_Back()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new FlakyStore(clock) { Failing = true };
        var builder = Builder(clock);
        builder.Services.AddStrata()
            .AddCheckpointStore(_ => store)
            .AddHealthChecks()
            .AddGraph(_ => TimerGraph())
            .AddWakeDriver<Tick>();
        using var host = builder.Build();
        await host.StartAsync();
        var status = host.Services.GetRequiredService<IStrataDriverStatus>();
        var health = host.Services.GetRequiredService<HealthCheckService>();

        // The first round fails, and the host is still up.
        await Until(() => status.Drivers.Any(d => d.FailuresInARow == 1), "the first round failed");
        Assert.Equal("InvalidOperationException", Assert.Single(status.Drivers).LastFailure);
        var degraded = (await health.CheckHealthAsync()).Entries[StrataHostExtensions.DriversCheck];
        Assert.Equal(HealthStatus.Degraded, degraded.Status);
        Assert.Equal("failing: wake:Tick (InvalidOperationException, 1 in a row)", degraded.Description);

        // A minute of the clock, a second at a time. Rounds come at 0, 10 and 30 seconds, where a driver that
        // didn't back off would make thirteen.
        for (var second = 0; second < 60; second++)
        {
            clock.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(5);
        }

        Assert.InRange(store.Listings, 1, 4);

        // The store comes back: the next round works, and the driver is healthy again.
        store.Failing = false;
        await Until(
            () => status.Drivers.All(d => d.FailuresInARow == 0),
            "the driver recovered",
            () => clock.Advance(TimeSpan.FromSeconds(30)));
        Assert.Null(Assert.Single(status.Drivers).LastFailure);
        Assert.Equal(HealthStatus.Healthy, (await health.CheckHealthAsync()).Entries[StrataHostExtensions.DriversCheck].Status);
        await host.StopAsync();
    }

    [Fact]
    public async Task Stopping_Ends_A_Waiting_Driver_At_Once()
    {
        var builder = Builder(new FakeTimeProvider(Noon));
        builder.Services.AddStrata().AddInMemoryCheckpoints().AddRetention(o => o.OlderThan = TimeSpan.FromDays(30));
        using var host = builder.Build();
        await host.StartAsync();
        var status = host.Services.GetRequiredService<IStrataDriverStatus>();
        await Until(() => status.Drivers.Any(d => d.Rounds == 1), "the first round finished");

        // An hour's wait, on a clock that doesn't move.
        var stopwatch = Stopwatch.StartNew();
        await host.StopAsync();

        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5), $"stopping took {stopwatch.Elapsed}");
    }

    [Fact]
    public async Task Stopping_Lets_The_Run_In_Flight_Finish_And_Starts_No_Other()
    {
        var clock = new FakeTimeProvider(Noon);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = 0;
        var finished = 0;
        var builder = Builder(clock);
        builder.Services.AddStrata()
            .AddInMemoryCheckpoints()
            .AddGraph(_ => TimerGraph(async _ =>
            {
                Interlocked.Increment(ref started);
                entered.TrySetResult();
                await release.Task;
                Interlocked.Increment(ref finished);
            }))
            .AddWakeDriver<Tick>();
        using var host = builder.Build();
        var runner = host.Services.GetRequiredService<StrataRunner<Tick>>();
        await runner.RunAsync("a", new Tick(0, false));
        await runner.RunAsync("b", new Tick(0, false));
        await host.StartAsync();
        clock.Advance(TimeSpan.FromMinutes(6));
        await Until(() => entered.Task.IsCompleted, "a woken run is in flight", () => clock.Advance(TimeSpan.FromSeconds(5)));

        // The host is asked to stop, and waits for the run.
        var stopping = host.StopAsync();
        await Task.Delay(200);
        Assert.False(stopping.IsCompleted, "the host waits for the run in flight");

        release.SetResult();
        await stopping.WaitAsync(Patience);

        // That run finished, not cancelled, and the other due thread was left for the next host.
        Assert.Equal((1, 1), (Volatile.Read(ref started), Volatile.Read(ref finished)));
        var logs = new[] { await Log(host.Services, "a"), await Log(host.Services, "b") };
        Assert.DoesNotContain(logs.SelectMany(log => log), checkpoint => checkpoint.Status == CheckpointStatus.Cancelled);
        Assert.Equal([1, 2], logs.Select(log => log.Count).Order());
    }

    [Fact]
    public async Task Two_Hosts_On_One_Store_Wake_A_Thread_Once()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var runs = 0;
        IHost Replica()
        {
            var builder = Builder(clock);
            builder.Services.AddStrata()
                .AddCheckpointStore(_ => store)
                .AddGraph(_ => TimerGraph(_ =>
                {
                    Interlocked.Increment(ref runs);
                    return Task.CompletedTask;
                }))
                .AddWakeDriver<Tick>();
            return builder.Build();
        }

        using var first = Replica();
        using var second = Replica();
        await first.Services.GetRequiredService<StrataRunner<Tick>>().RunAsync("t", new Tick(0, false));
        await first.StartAsync();
        await second.StartAsync();

        clock.Advance(TimeSpan.FromMinutes(6));
        await Until(() => Volatile.Read(ref runs) > 0, "the timer was woken", () => clock.Advance(TimeSpan.FromSeconds(5)));
        for (var i = 0; i < 10; i++)
        {
            clock.Advance(TimeSpan.FromSeconds(5));
            await Task.Delay(10);
        }

        Assert.Equal(1, Volatile.Read(ref runs));
        Assert.Equal(2, (await store.GetLogAsync("t")).Entries.Count);
        await first.StopAsync();
        await second.StopAsync();
    }

    // --- Health ---

    [Fact]
    public async Task The_Checkpoint_Check_Says_Whether_The_Store_Answers()
    {
        var store = new FlakyStore(TimeProvider.System);
        var builder = Builder();
        builder.Services.AddStrata()
            .AddCheckpointStore(_ => store)
            .AddHealthChecks(tags: ["ready"], timeout: TimeSpan.FromMilliseconds(200));
        using var host = builder.Build();
        await host.StartAsync();
        var health = host.Services.GetRequiredService<HealthCheckService>();
        async Task<HealthReportEntry> Checkpoints() => (await health.CheckHealthAsync()).Entries[StrataHostExtensions.CheckpointsCheck];

        var healthy = await Checkpoints();
        Assert.Equal(HealthStatus.Healthy, healthy.Status);
        Assert.Equal(["ready"], healthy.Tags);

        // A failure is unhealthy. Its type is the description: the message can hold a connection string.
        store.Failing = true;
        var failing = await Checkpoints();
        Assert.Equal((HealthStatus.Unhealthy, "InvalidOperationException"), (failing.Status, failing.Description));
        Assert.IsType<InvalidOperationException>(failing.Exception);

        // So is a store that doesn't answer, even one that ignores its token.
        store.Failing = false;
        store.Hanging = true;
        var hanging = await Checkpoints();
        Assert.Equal((HealthStatus.Unhealthy, "no answer within 0.2 s"), (hanging.Status, hanging.Description));

        // With no drivers there is nothing to be degraded.
        Assert.Equal(HealthStatus.Healthy, (await health.CheckHealthAsync()).Entries[StrataHostExtensions.DriversCheck].Status);
        await host.StopAsync();
    }

    // --- Instrumentation ---

    [Fact]
    public async Task Instrumentation_Adds_Stratas_Sources_And_Meters_To_The_Applications_Providers()
    {
        async Task<(List<Activity> Activities, List<Metric> Metrics)> Exported(bool instrumented)
        {
            var activities = new List<Activity>();
            var metrics = new List<Metric>();
            var builder = Builder();
            builder.Services.AddOpenTelemetry()
                .WithTracing(tracing => tracing.AddInMemoryExporter(activities))
                .WithMetrics(metering => metering.AddInMemoryExporter(metrics));
            var strata = builder.Services.AddStrata().AddInMemoryCheckpoints().AddGraph(_ => DurableGraph());
            if (instrumented)
            {
                strata.AddInstrumentation();
            }

            using var host = builder.Build();
            await host.StartAsync();
            await host.Services.GetRequiredService<StrataRunner<Tick>>().RunAsync($"t-{Guid.NewGuid():N}", new Tick(0, false));
            host.Services.GetRequiredService<TracerProvider>().ForceFlush();
            host.Services.GetRequiredService<MeterProvider>().ForceFlush();
            await host.StopAsync();
            return (activities, metrics);
        }

        var (silentActivities, silentMetrics) = await Exported(instrumented: false);
        Assert.DoesNotContain(silentActivities, activity => activity.Source.Name == StrataTelemetry.Name);
        Assert.DoesNotContain(silentMetrics, metric => metric.MeterName == StrataTelemetry.Name);

        var (activities, metrics) = await Exported(instrumented: true);
        Assert.Contains(activities, activity => activity.Source.Name == StrataTelemetry.Name);
        Assert.Contains(metrics, metric => metric.MeterName == StrataTelemetry.Name);

        Assert.Equal([StrataTelemetry.Name, StrataCloudTelemetry.Name], StrataInstrumentation.ActivitySourceNames);
        Assert.Equal(
            [StrataTelemetry.Name, StorageTelemetry.Name, StrataCloudTelemetry.Name, StrataHostingTelemetry.Name],
            StrataInstrumentation.MeterNames);
    }
}
