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
    // docs:begin guides/persistence/choose-a-store/read-only
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Storage.Memory;

    // docs:hide
    using System.Security.Cryptography;

    public partial class ChooseAStoreSamples
    {
        [Fact]
        public async Task OpenReadOnly()
        {
            using var file = new TempSqliteFile();
            await StoreChecks.PauseAsync(file.Path, "refund-1042");
            var before = Hash(file.Path);

            // docs:show
            var path = "strata.sqlite";
            // docs:hide
            path = file.Path;
            // docs:show
            using var store = await EfCoreCheckpointStore.OpenReadOnlyAsync(path);

            var waiting = await store.ListThreadsAsync(new ThreadQuery
            {
                States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Interrupted },
            });
            foreach (var thread in waiting.Threads)
            {
                Console.WriteLine($"{thread.ThreadId} waits at {thread.SuspendedNode}");
            }

            var latest = await store.LoadAsync("refund-1042");
            try
            {
                await store.SaveAsync(latest!);
            }
            catch (InvalidOperationException refused)
            {
                Console.WriteLine(refused.Message);
            }
            // docs:end guides/persistence/choose-a-store/read-only

            // It reads what the writable store wrote, and refuses the write with the message the page shows.
            Assert.True(store.IsReadOnly);
            var thread1042 = Assert.Single(waiting.Threads);
            Assert.Equal(("refund-1042", "Prepare"), (thread1042.ThreadId, thread1042.SuspendedNode));
            var refusedSave = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.SaveAsync(latest!));
            Assert.Equal("the store was opened read-only: save is refused", refusedSave.Message);

            // Opening, listing, loading and the refused save left the file as it was.
            Assert.Equal(before, Hash(path));

            // A missing file is refused, and not created.
            var missing = path + ".missing";
            var notThere = await Assert.ThrowsAsync<InvalidOperationException>(
                () => EfCoreCheckpointStore.OpenReadOnlyAsync(missing));
            Assert.Equal($"no SQLite store at '{missing}': the file doesn't exist", notThere.Message);
            Assert.False(File.Exists(missing));
        }

        /// <summary>The file's SHA-256, read while SQLite may still hold it open.</summary>
        private static byte[] Hash(string path)
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            return SHA256.HashData(stream);
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.ChooseAStore
{
    using Datalium.Strata.Graph;
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Memory;
    using Datalium.Strata.Storage.Memory;
    using Microsoft.Data.Sqlite;
    using Microsoft.EntityFrameworkCore;

    internal sealed record Refund(string Customer, decimal Amount, string Status);

    internal static class StoreChecks
    {
        /// <summary>
        /// Runs the refund graph on a writable SQLite store at <paramref name="path"/> until it waits after
        /// <c>Prepare</c>: the thread a read-only store then reads.
        /// </summary>
        public static async Task PauseAsync(string path, string threadId)
        {
            using var writable = new EfCoreCheckpointStore(
                new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>()
                    .UseSqlite($"Data Source={path}").Options));
            var paused = await RefundGraph().RunAsync(threadId, new Refund("acme", 120m, "new"), writable);
            Assert.IsType<GraphExecutionResult<Refund>.Interrupted>(paused);
        }

        private static CompiledGraph<Refund> RefundGraph() =>
            new StateGraph<Refund>()
                .AddNode("Prepare", refund => refund with { Status = "awaiting approval" })
                .AddInterruptBoundary("Prepare")
                .AddNode("Pay", refund => refund with { Status = "paid" })
                .AddEdge("Prepare", "Pay")
                .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });

        /// <summary>
        /// Runs a refund graph on <paramref name="checkpoints"/> until it waits, resumes it from the store, and keeps
        /// a message in <paramref name="history"/>: what the page says every store does with the same graph code.
        /// </summary>
        public static async Task PauseAndResumeAsync(ICheckpointStore checkpoints, IHistoryStore history, string threadId)
        {
            var graph = RefundGraph();

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
