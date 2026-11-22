// Samples for https://stratamesh.dev/guides/persistence/choose-a-store/
//
// Each sample is in a namespace block of its own, so it can show its own `using` lines. The Postgres tests need
// STRATA_TEST_POSTGRES_URL and the Redis test STRATA_TEST_REDIS_URL: they skip without them, and fail without
// them when STRATA_REQUIRE_BACKENDS=1.

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.ChooseAStore
{
    // docs:begin guides/persistence/choose-a-store/in-memory
    using Datalium.Strata.Storage;
    using Datalium.Strata.Storage.Memory;

    // docs:hide
    public partial class ChooseAStoreSamples
    {
        [Fact]
        public async Task InMemory()
        {
            // docs:show
            var checkpoints = new InMemoryCheckpointStore();
            var history = new InMemoryHistoryStore();
            // docs:end guides/persistence/choose-a-store/in-memory

            await StoreChecks.PauseAndResumeAsync(checkpoints, history, "guides-persistence-choose-a-store-in-memory");
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.ChooseAStore
{
    // docs:begin guides/persistence/choose-a-store/sqlite
    using Datalium.Strata.Storage.Memory;
    using Microsoft.EntityFrameworkCore;

    // docs:hide
    public partial class ChooseAStoreSamples
    {
        [Fact]
        public async Task OpenSqlite()
        {
            using var file = new TempSqliteFile();
            // docs:show
            var path = "strata.sqlite";
            // docs:hide
            path = file.Path;
            // docs:show
            using var checkpoints = new EfCoreCheckpointStore(
                new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>()
                    .UseSqlite($"Data Source={path}").Options));
            using var history = new EfCoreHistoryStore(
                new HistoryDbContext(new DbContextOptionsBuilder<HistoryDbContext>()
                    .UseSqlite($"Data Source={path}").Options));
            // docs:end guides/persistence/choose-a-store/sqlite

            // A missing file is created, and the two stores share it.
            Assert.True(File.Exists(path));
            await StoreChecks.PauseAndResumeAsync(checkpoints, history, "guides-persistence-choose-a-store-sqlite");

            // For tests, a SQLite database that lives in memory.
            using var inMemory = EfCoreCheckpointStore.CreateInMemory();
            Assert.Null(await inMemory.LoadAsync("guides-persistence-choose-a-store-sqlite"));
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.ChooseAStore
{
    // docs:begin guides/persistence/choose-a-store/postgres
    using Datalium.Strata.Storage.Postgres;

    // docs:hide
    using Datalium.Strata.DocsSnippets.Guides.Persistence.Postgres;

    public partial class ChooseAStoreSamples
    {
        [PostgresFact]
        public async Task OpenPostgres()
        {
            using var sandbox = new PostgresSandbox("choose_a_store_postgres");
            // docs:show
            var url = "postgres://strata:strata@localhost:5432/strata";
            // docs:hide
            url = sandbox.ConnectionString;
            // docs:show
            await using var checkpoints = await PostgresCheckpointStore.OpenAsync(url);
            await using var history = await PostgresHistoryStore.OpenAsync(url);
            // docs:end guides/persistence/choose-a-store/postgres

            await StoreChecks.PauseAndResumeAsync(checkpoints, history, "guides-persistence-choose-a-store-postgres");
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.ChooseAStore
{
    // docs:begin guides/persistence/choose-a-store/redis
    using Datalium.Strata.Storage.Redis;

    // docs:hide
    public partial class ChooseAStoreSamples
    {
        [RedisFact]
        public async Task OpenRedis()
        {
            // docs:show
            var url = "redis://localhost:6379";
            // docs:hide
            url = RedisBackend.Url;
            // docs:show
            using var checkpoints = await RedisCheckpointStore.ConnectAsync(url, new RedisStoreOptions { Prefix = "acme" });
            using var history = await RedisHistoryStore.ConnectAsync(url, new RedisStoreOptions { Prefix = "acme" });
            // docs:end guides/persistence/choose-a-store/redis

            // Other tests share the "acme" prefix, so this one only reads, from a thread and a session no one writes.
            var unique = $"guides-persistence-choose-a-store-redis-{Guid.NewGuid():N}";
            Assert.Null(await checkpoints.LoadAsync(unique));
            Assert.Empty(await history.GetHistoryAsync(unique, 10).ToListAsync());
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.ChooseAStore
{
    // docs:begin guides/persistence/choose-a-store/store-options
    using Datalium.Strata.Storage;
    using Datalium.Strata.Storage.Postgres;

    // docs:hide
    using Datalium.Strata.DocsSnippets.Guides.Persistence.Postgres;

    public partial class ChooseAStoreSamples
    {
        [PostgresFact]
        public async Task StoreOptions()
        {
            using var sandbox = new PostgresSandbox("choose_a_store_options");
            var url = sandbox.ConnectionString;
            // docs:show
            await using var checkpoints = await PostgresCheckpointStore.OpenAsync(url, new PostgresStoreOptions
            {
                GroupCommit = new GroupCommitOptions(),
                Deltas = new DeltaOptions(),
                Offload = new OffloadOptions(),
            });
            // docs:end guides/persistence/choose-a-store/store-options

            await using var history = await PostgresHistoryStore.OpenAsync(url);
            await StoreChecks.PauseAndResumeAsync(checkpoints, history, "guides-persistence-choose-a-store-options");
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.ChooseAStore
{
    using Datalium.Strata.Graph;
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Memory;
    using Microsoft.Data.Sqlite;

    internal sealed record Refund(string Customer, decimal Amount, string Status);

    internal static class StoreChecks
    {
        /// <summary>
        /// Runs a refund graph on <paramref name="checkpoints"/> until it waits, resumes it from the store, and keeps
        /// a message in <paramref name="history"/>: what the page says every store does with the same graph code.
        /// </summary>
        public static async Task PauseAndResumeAsync(ICheckpointStore checkpoints, IHistoryStore history, string threadId)
        {
            var graph = new StateGraph<Refund>()
                .AddNode("Prepare", refund => refund with { Status = "awaiting approval" })
                .AddInterruptBoundary("Prepare")
                .AddNode("Pay", refund => refund with { Status = "paid" })
                .AddEdge("Prepare", "Pay")
                .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });

            var paused = await graph.RunAsync(threadId, new Refund("acme", 120m, "new"), checkpoints);
            Assert.Equal("Prepare", Assert.IsType<GraphExecutionResult<Refund>.Interrupted>(paused).SuspendedNode);
            await history.AppendAsync(threadId,
                new ChatMessage(MessageRole.User, "Refund order 1042".AsMemory(), DateTimeOffset.UtcNow));

            var resumed = await graph.ResumeAsync(threadId, HumanSignal.Approve("jdoe"), checkpoints);
            Assert.Equal("paid", Assert.IsType<GraphExecutionResult<Refund>.Completed>(resumed).State.Status);
            var message = Assert.Single(await history.GetHistoryAsync(threadId, 10).ToListAsync());
            Assert.Equal("Refund order 1042", message.Content.ToString());
        }
    }

    /// <summary>A SQLite file in the temp directory, deleted on dispose.</summary>
    internal sealed class TempSqliteFile : IDisposable
    {
        public string Path { get; } =
            System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"docs-choose-a-store-{Guid.NewGuid():N}.sqlite");

        public void Dispose()
        {
            // Microsoft.Data.Sqlite pools connections, which keeps the file open after a store is disposed.
            SqliteConnection.ClearAllPools();
            File.Delete(Path);
        }
    }

    /// <summary>
    /// The Redis the tests use, from <c>STRATA_TEST_REDIS_URL</c>, as the strata tests' <c>TestBackends</c> reads
    /// it: a test skips when it is unset, unless <c>STRATA_REQUIRE_BACKENDS=1</c>, which fails it instead.
    /// </summary>
    internal static class RedisBackend
    {
        public const string Variable = "STRATA_TEST_REDIS_URL";

        public static string? SkipReason =>
            string.IsNullOrEmpty(Environment.GetEnvironmentVariable(Variable))
            && Environment.GetEnvironmentVariable("STRATA_REQUIRE_BACKENDS") != "1"
                ? $"{Variable} is not set (start `docker compose up -d --wait`)"
                : null;

        public static string Url
        {
            get
            {
                var url = Environment.GetEnvironmentVariable(Variable);
                return string.IsNullOrEmpty(url)
                    ? throw new InvalidOperationException(
                        $"{Variable} is not set, but STRATA_REQUIRE_BACKENDS=1 says these tests must run")
                    : url;
            }
        }
    }

    /// <summary>A test that needs the Redis from <see cref="RedisBackend"/>.</summary>
    public sealed class RedisFactAttribute : FactAttribute
    {
        public RedisFactAttribute() => Skip = RedisBackend.SkipReason;
    }
}
