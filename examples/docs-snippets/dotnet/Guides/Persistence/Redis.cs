// Samples for https://stratamesh.dev/guides/persistence/redis/
//
// Each sample has its own using lines, so each one is in a namespace block of its own. The tests need the Redis at
// STRATA_TEST_REDIS_URL, which CI sets to redis://localhost:6379, the URL the samples show. Each test writes under a
// prefix of its own, which a hidden `with` puts over the sample's "acme", and deletes its keys afterwards.

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Redis
{
    using StackExchange.Redis;

    /// <summary>
    /// A test that needs the Redis in <c>STRATA_TEST_REDIS_URL</c>: skipped when it is unset, and failed when
    /// <c>STRATA_REQUIRE_BACKENDS=1</c> (as in strata's own backend tests).
    /// </summary>
    internal sealed class RedisFactAttribute : FactAttribute
    {
        public const string Variable = "STRATA_TEST_REDIS_URL";

        public RedisFactAttribute()
        {
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(Variable))
                && Environment.GetEnvironmentVariable("STRATA_REQUIRE_BACKENDS") != "1")
            {
                Skip = $"{Variable} is not set (start `docker compose up -d --wait`)";
            }
        }

        public static string Url => Environment.GetEnvironmentVariable(Variable) is { Length: > 0 } url
            ? url
            : throw new InvalidOperationException($"{Variable} is not set, but STRATA_REQUIRE_BACKENDS=1 says these tests must run");
    }

    /// <summary>A key prefix unique to one test: every key under it is deleted on dispose.</summary>
    internal sealed class RedisSandbox : IDisposable
    {
        public RedisSandbox(string label)
        {
            Prefix = $"docs_redis_{Environment.ProcessId}_{Guid.NewGuid():N}_{label}";
            Connection = Storage.Redis.RedisConnection.Connect(RedisFactAttribute.Url);
        }

        public string Prefix { get; }

        public ConnectionMultiplexer Connection { get; }

        public IDatabase Database => Connection.GetDatabase();

        /// <summary>The keys under the prefix, with the prefix written as <paramref name="shownAs"/>.</summary>
        public string[] Keys(string shownAs) =>
        [
            .. Connection.GetServer(Connection.GetEndPoints()[0])
                .Keys(pattern: $"{Prefix}:*")
                .Select(key => shownAs + key.ToString()[Prefix.Length..])
                .Order(StringComparer.Ordinal),
        ];

        public void Dispose()
        {
            var keys = Connection.GetServer(Connection.GetEndPoints()[0]).Keys(pattern: $"{Prefix}:*").ToArray();
            if (keys.Length > 0)
            {
                Database.KeyDelete(keys);
            }

            Connection.Dispose();
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Redis
{
    // docs:begin guides/persistence/redis/create
    using Datalium.Strata.Storage.Redis;

    // docs:hide
    public partial class RedisSamples
    {
        [RedisFact]
        public async Task Create()
        {
            using var sandbox = new RedisSandbox("create");

            // docs:show
            using var store = await RedisCheckpointStore.ConnectAsync("redis://localhost:6379", new RedisStoreOptions
            {
                Prefix = "acme",
                FinishedThreadTtl = TimeSpan.FromDays(30),
                // docs:hide
            } with
            {
                Prefix = sandbox.Prefix,
                // docs:show
            });
            // docs:end guides/persistence/redis/create

            // The store connected before it returned; it sends its first command now.
            Assert.Null(await store.LoadAsync("redis-create-nothing"));
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Redis
{
    // docs:begin guides/persistence/redis/multiplexer
    using StackExchange.Redis;

    // docs:hide
    using Datalium.Strata.Storage.Redis;

    public partial class RedisSamples
    {
        [RedisFact]
        public async Task Multiplexer()
        {
            using var sandbox = new RedisSandbox("multiplexer");

            // docs:show
            var multiplexer = await ConnectionMultiplexer.ConnectAsync("localhost:6379");
            var store = new RedisCheckpointStore(multiplexer.GetDatabase(), new RedisStoreOptions { Prefix = "acme" });
            // docs:end guides/persistence/redis/multiplexer

            // That store sends nothing until its first call, so it never touched "acme". The test's own prefix:
            store.Dispose();
            store = new RedisCheckpointStore(multiplexer.GetDatabase(), new RedisStoreOptions { Prefix = sandbox.Prefix });
            Assert.Null(await store.LoadAsync("redis-multiplexer-nothing"));

            // The store doesn't own the multiplexer, so disposing the store leaves it open.
            store.Dispose();
            Assert.True(multiplexer.IsConnected);
            await multiplexer.DisposeAsync();
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Redis
{
    // docs:begin guides/persistence/redis/run-thread
    using Datalium.Strata.Graph;
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Storage.Redis;

    // docs:hide
    public partial class RedisSamples
    {
        [RedisFact]
        public async Task RunThread()
        {
            using var sandbox = new RedisSandbox("run");

            // docs:show
            var graph = new StateGraph<Refund>()
                .AddNode("Prepare", refund => refund with { Status = "awaiting approval" })
                .AddInterruptBoundary("Prepare")
                .AddNode("Pay", refund => refund with { Status = "paid" })
                .AddEdge("Prepare", "Pay")
                .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });

            using var store = await RedisCheckpointStore.ConnectAsync("redis://localhost:6379", new RedisStoreOptions
            {
                Prefix = "acme",
                FinishedThreadTtl = TimeSpan.FromDays(30),
                // docs:hide
            } with
            {
                Prefix = sandbox.Prefix,
                // docs:show
            });

            var paused = await graph.RunAsync("refund-1", new Refund("Ada", 42m, "new"), store);
            Console.WriteLine($"refund-1: {paused.GetType().Name}");

            var done = await graph.ResumeAsync("refund-1", HumanSignal.Approve("lead"), store);
            Console.WriteLine($"refund-1: {done.GetType().Name}");

            var waiting = await graph.RunAsync("refund-2", new Refund("Grace", 7m, "new"), store);
            Console.WriteLine($"refund-2: {waiting.GetType().Name}");

            // docs:hide
            // What the page says the program prints.
            Assert.Equal(
                ["Interrupted", "Completed", "Interrupted"],
                new[] { paused, done, waiting }.Select(result => result.GetType().Name));

            // The keys the page lists, under the test's prefix instead of "acme".
            Assert.Equal(
                [
                    "acme:checkpoints:refund-1",
                    "acme:checkpoints:refund-2",
                    "acme:threads",
                    "acme:threads:completed",
                    "acme:threads:indexed",
                    "acme:threads:interrupted",
                ],
                sandbox.Keys(shownAs: "acme"));

            // "Let finished threads expire": the completed thread has the 30-day ttl, the waiting one none.
            var ttl = await sandbox.Database.KeyTimeToLiveAsync($"{sandbox.Prefix}:checkpoints:refund-1");
            Assert.InRange(ttl!.Value, TimeSpan.FromDays(30) - TimeSpan.FromMinutes(1), TimeSpan.FromDays(30));
            Assert.Null(await sandbox.Database.KeyTimeToLiveAsync($"{sandbox.Prefix}:checkpoints:refund-2"));
        }
    }

    // docs:show
    public sealed record Refund(string Customer, decimal Amount, string Status);
    // docs:end guides/persistence/redis/run-thread
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Redis
{
    // docs:begin guides/persistence/redis/retry
    using Datalium.Strata.Storage;
    using Datalium.Strata.Storage.Redis;

    // docs:hide
    public partial class RedisSamples
    {
        [RedisFact]
        public async Task Retry()
        {
            using var sandbox = new RedisSandbox("retry");

            // docs:show
            using var store = await RedisCheckpointStore.ConnectAsync("redis://localhost:6379", new RedisStoreOptions
            {
                Prefix = "acme",
                Retry = new StoreRetryPolicy { MaxAttempts = 6, MaxDelay = TimeSpan.FromSeconds(5) },
                // docs:hide
            } with
            {
                Prefix = sandbox.Prefix,
                // docs:show
            });
            // docs:end guides/persistence/redis/retry

            Assert.Null(await store.LoadAsync("redis-retry-nothing"));

            // "StoreRetryPolicy.None turns retries off": one attempt.
            Assert.Equal(1, StoreRetryPolicy.None.MaxAttempts);
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Redis
{
    using Datalium.Strata.Hosting;
    using Datalium.Strata.Storage.Redis;
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Memory;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Options;

    public partial class RedisSamples
    {
        private static readonly string AppSettings =
            Path.Combine(AppContext.BaseDirectory, "files", "guides", "persistence", "redis", "appsettings.json");

        [RedisFact]
        public async Task Host()
        {
            using var sandbox = new RedisSandbox("host");

            // The file as the page shows it.
            var file = new ConfigurationBuilder().AddJsonFile(AppSettings).Build();
            Assert.Equal("localhost:6379", file.GetConnectionString("strata-redis"));
            Assert.Equal("acme", file["Strata:Redis:Prefix"]);

            var builder = Microsoft.Extensions.Hosting.Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
            builder.Configuration.AddJsonFile(AppSettings);
            builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:strata-redis"] = RedisFactAttribute.Url,
                ["Strata:Redis:Prefix"] = sandbox.Prefix,
            });

            // docs:begin guides/persistence/redis/host
            builder.Services.AddStrata()
                .AddRedisCheckpoints()   // Strata:Redis, ConnectionStrings:strata-redis
                .AddRedisHistory();
            // docs:end guides/persistence/redis/host

            using var host = builder.Build();
            Assert.Equal(TimeSpan.FromDays(30), host.Services.GetRequiredService<IOptions<StrataRedisOptions>>().Value.FinishedThreadTtl);

            await host.StartAsync();
            Assert.IsType<RedisCheckpointStore>(host.Services.GetRequiredService<ICheckpointStore>());
            Assert.IsType<RedisHistoryStore>(host.Services.GetRequiredService<IHistoryStore>());
            await host.StopAsync();
        }
    }
}
