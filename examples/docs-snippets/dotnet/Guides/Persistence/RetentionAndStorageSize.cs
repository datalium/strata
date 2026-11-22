// Samples for https://stratamesh.dev/guides/persistence/retention-and-storage-size/
//
// Each sample has its own using lines, so each one is in a namespace block of its own. The samples name SQLite files
// and a blob folder relative to the working folder, the test's output folder: each test deletes them before and after.

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.RetentionAndStorageSize
{
    using Datalium.Strata.Graph;
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Storage;
    using Datalium.Strata.Storage.Memory;
    using Microsoft.Data.Sqlite;
    using Microsoft.EntityFrameworkCore;

    /// <summary>The order the purge sample runs: it waits at <c>Check</c> for approval.</summary>
    internal sealed record Order(string Item, bool Shipped);

    internal static class Files
    {
        /// <summary>A whole-file sample: files/guides/persistence/retention-and-storage-size/<paramref name="name"/>.</summary>
        public static string Sample(string name) =>
            Path.Combine(AppContext.BaseDirectory, "files", "guides", "persistence", "retention-and-storage-size", name);

        public static string TempSqlite(string label) =>
            Path.Combine(Path.GetTempPath(), $"docs_retention_{label}_{Guid.NewGuid():N}.sqlite");

        /// <summary>Deletes a SQLite file and its journal files, and a blob folder.</summary>
        public static void Delete(string sqlite, string? blobs = null)
        {
            SqliteConnection.ClearAllPools();
            foreach (var path in new[] { sqlite, sqlite + "-wal", sqlite + "-shm", sqlite + "-journal" })
            {
                File.Delete(path);
            }

            if (blobs is not null && Directory.Exists(blobs))
            {
                Directory.Delete(blobs, recursive: true);
            }
        }

        public static EfCoreCheckpointStore Sqlite(string path, SqliteStoreOptions? options = null) =>
            new(
                new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={path}").Options),
                options: options);
    }

    /// <summary>The report graph of "Turn on deltas and offload": 100 KB of evidence, then three counter steps.</summary>
    internal static class Reports
    {
        public static CompiledGraph<TReport> Build<TReport>(Func<TReport, TReport> gather, Func<TReport, TReport> step) =>
            new StateGraph<TReport>()
                .AddNode("Gather", gather)
                .AddNode("Analyse", step)
                .AddNode("Draft", step)
                .AddNode("Review", step)
                .AddEdge("Gather", "Analyse")
                .AddEdge("Analyse", "Draft")
                .AddEdge("Draft", "Review")
                .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.RetentionAndStorageSize
{
    // docs:begin guides/persistence/retention-and-storage-size/purge
    using Datalium.Strata.Interruption;

    // docs:hide
    using Datalium.Strata.Graph;

    public partial class RetentionAndStorageSizeSamples
    {
        [Fact]
        public async Task Purge()
        {
            var path = Files.TempSqlite("purge");
            try
            {
                using var store = Files.Sqlite(path);

                // Four orders wait at Check; three are approved and complete, and order-4 still waits.
                var graph = new StateGraph<Order>()
                    .AddNode("Check", order => order)
                    .AddInterruptBoundary("Check")
                    .AddNode("Ship", order => order with { Shipped = true })
                    .AddEdge("Check", "Ship")
                    .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });
                foreach (var id in new[] { "order-1", "order-2", "order-3", "order-4" })
                {
                    Assert.IsType<GraphExecutionResult<Order>.Interrupted>(await graph.RunAsync(id, new Order(id, false), store));
                }

                foreach (var id in new[] { "order-1", "order-2", "order-3" })
                {
                    Assert.IsType<GraphExecutionResult<Order>.Completed>(await graph.ResumeAsync(id, HumanSignal.Approve("lead"), store));
                }

                // docs:show
                var olderThan = DateTimeOffset.UtcNow.AddDays(-30);
                Console.WriteLine($"Older than 30 days: {await PurgeAllAsync(store, olderThan)} threads purged");

                // A cutoff just ahead of now, so the threads this example just finished count as old.
                Console.WriteLine($"Older than now: {await PurgeAllAsync(store, DateTimeOffset.UtcNow.AddSeconds(1))} threads purged");

                foreach (var id in new[] { "order-1", "order-4" })
                {
                    var latest = await store.LoadAsync(id);
                    Console.WriteLine($"{id}: {(latest is null ? "gone" : $"kept ({latest.Status})")}");
                }

                static async Task<int> PurgeAllAsync(ICheckpointRetention store, DateTimeOffset olderThan)
                {
                    const int batch = 100;
                    int total = 0, purged;
                    do
                    {
                        purged = await store.PurgeAsync(olderThan, limit: batch);
                        total += purged;
                    }
                    while (purged == batch);
                    return total;
                }
                // docs:end guides/persistence/retention-and-storage-size/purge

                // What the page says it prints: the three completed orders went, and the waiting one stayed.
                foreach (var id in new[] { "order-1", "order-2", "order-3" })
                {
                    Assert.Null(await store.LoadAsync(id));
                }

                Assert.Equal(CheckpointStatus.Interrupted, (await store.LoadAsync("order-4"))!.Status);
                Assert.Equal(0, await PurgeAllAsync(store, DateTimeOffset.UtcNow.AddSeconds(1)));
            }
            finally
            {
                Files.Delete(path);
            }
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.RetentionAndStorageSize
{
    using Datalium.Strata.Hosting;
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Storage.Memory;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Options;

    public partial class RetentionAndStorageSizeSamples
    {
        private static HostApplicationBuilder Builder(string settingsFile, Dictionary<string, string?> overrides)
        {
            var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
            builder.Configuration.AddJsonFile(Files.Sample(settingsFile));
            builder.Configuration.AddInMemoryCollection(overrides);
            return builder;
        }

        [Fact]
        public async Task Schedule()
        {
            // The file as the page shows it.
            var file = new ConfigurationBuilder().AddJsonFile(Files.Sample("schedule-appsettings.json")).Build();
            Assert.Equal("strata.db", file["Strata:Sqlite:Path"]);

            var path = Files.TempSqlite("schedule");
            try
            {
                var builder = Builder("schedule-appsettings.json", new() { ["Strata:Sqlite:Path"] = path });

                // docs:begin guides/persistence/retention-and-storage-size/schedule
                builder.Services.AddStrata()
                    .AddSqliteCheckpoints()
                    .AddRetention();
                // docs:end guides/persistence/retention-and-storage-size/schedule

                using (var host = builder.Build())
                {
                    var retention = host.Services.GetRequiredService<IOptions<RetentionOptions>>().Value;
                    Assert.Equal((TimeSpan.FromHours(1), TimeSpan.FromDays(30), 1000), (retention.Interval, retention.OlderThan, retention.BatchSize));
                    Assert.IsType<EfCoreCheckpointStore>(host.Services.GetRequiredService<ICheckpointStore>());
                    await host.StartAsync();
                    await host.StopAsync();
                }

                // "Without OlderThan, starting the host throws".
                var without = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
                without.Configuration.AddInMemoryCollection(new Dictionary<string, string?> { ["Strata:Sqlite:Path"] = path });
                without.Services.AddStrata().AddSqliteCheckpoints().AddRetention();
                using var refused = without.Build();
                var error = await Assert.ThrowsAsync<InvalidOperationException>(() => refused.StartAsync());
                Assert.Equal(
                    "the retention driver needs Strata:Retention:OlderThan: how long a thread is kept after its latest checkpoint",
                    error.Message);
            }
            finally
            {
                Files.Delete(path);
            }
        }

        [Fact]
        public async Task HostSettings()
        {
            var file = new ConfigurationBuilder().AddJsonFile(Files.Sample("host-appsettings.json")).Build();
            Assert.Equal(("strata.db", "blobs"), (file["Strata:Sqlite:Path"], file["Strata:Sqlite:Offload:Path"]));

            var path = Files.TempSqlite("host");
            var blobs = Path.Combine(Path.GetTempPath(), $"docs_retention_host_blobs_{Guid.NewGuid():N}");
            try
            {
                var builder = Builder("host-appsettings.json", new()
                {
                    ["Strata:Sqlite:Path"] = path,
                    ["Strata:Sqlite:Offload:Path"] = blobs,
                });
                builder.Services.AddStrata().AddSqliteCheckpoints().AddRetention();
                using var host = builder.Build();

                var sqlite = host.Services.GetRequiredService<IOptions<StrataSqliteOptions>>().Value;
                Assert.True(sqlite.Deltas.Enabled);
                Assert.True(sqlite.Offload.Enabled);
                Assert.Equal(TimeSpan.FromDays(30), host.Services.GetRequiredService<IOptions<RetentionOptions>>().Value.OlderThan);

                // The store offloads a large value into the folder.
                await host.StartAsync();
                var store = host.Services.GetRequiredService<ICheckpointStore>();
                var graph = Reports.Build<Report>(r => r with { Evidence = new string('x', 100_000) }, r => r with { Step = r.Step + 1 });
                Assert.IsType<GraphExecutionResult<Report>.Completed>(await graph.RunAsync("report-1", new Report("", 0), store));
                Assert.Single(Directory.GetFiles(blobs, "*", SearchOption.AllDirectories));
                await host.StopAsync();
            }
            finally
            {
                Files.Delete(path, blobs);
            }
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.RetentionAndStorageSize
{
    // docs:begin guides/persistence/retention-and-storage-size/deltas-and-offload
    using Datalium.Strata.Graph;
    using Datalium.Strata.Storage;
    using Datalium.Strata.Storage.Memory;
    using Microsoft.EntityFrameworkCore;

    // docs:hide
    using Datalium.Strata.Interruption;
    using Microsoft.Data.Sqlite;

    public partial class RetentionAndStorageSizeSamples
    {
        [Fact]
        public async Task DeltasAndOffload()
        {
            Files.Delete("reports.sqlite");
            try
            {
                await DeltasAndOffloadAsync();
            }
            finally
            {
                Files.Delete("reports.sqlite");
            }
        }

        private static async Task DeltasAndOffloadAsync()
        {
            // docs:show
            var options = new SqliteStoreOptions
            {
                Deltas = new DeltaOptions(),     // a whole row every 16
                Offload = new OffloadOptions(),  // values of 64 KiB or more, into this file
            };
            using var store = new EfCoreCheckpointStore(
                new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>()
                    .UseSqlite("Data Source=reports.sqlite").Options),
                options: options);

            var graph = new StateGraph<Report>()
                .AddNode("Gather", r => r with { Evidence = new string('x', 100_000) })
                .AddNode("Analyse", r => r with { Step = r.Step + 1 })
                .AddNode("Draft", r => r with { Step = r.Step + 1 })
                .AddNode("Review", r => r with { Step = r.Step + 1 })
                .AddEdge("Gather", "Analyse")
                .AddEdge("Analyse", "Draft")
                .AddEdge("Draft", "Review")
                .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });

            await graph.RunAsync("report-1", new Report("", 0), store);

            var latest = await store.LoadAsync("report-1");
            Console.WriteLine($"Loaded {latest!.Status}: {latest.FrozenState!.ToJsonString().Length} bytes of state");

            // docs:hide
            // What the page says it prints, and the rows it says the store wrote.
            Assert.Equal(CheckpointStatus.Completed, latest.Status);
            Assert.Equal(100_024, latest.FrozenState.ToJsonString().Length);

            using var file = new SqliteConnection("Data Source=reports.sqlite");
            file.Open();
            using var rows = file.CreateCommand();
            rows.CommandText = "SELECT sequence, status, schema_version, length(frozen_state) FROM checkpoints WHERE thread_id = 'report-1' ORDER BY sequence";
            var written = new List<string>();
            using (var reader = rows.ExecuteReader())
            {
                while (reader.Read())
                {
                    written.Add($"{reader.GetInt64(0)} {reader.GetString(1)} {reader.GetString(2)} {reader.GetInt64(3)}");
                }
            }

            Assert.Equal(
                [
                    "0 running 1.7.0 24",
                    "1 running 1.9.0 192",
                    "2 running 1.8.0 81",
                    "3 running 1.8.0 81",
                    "4 completed 1.9.0 194",
                ],
                written);

            using var blobs = file.CreateCommand();
            blobs.CommandText = "SELECT hash, length(content) FROM checkpoint_blobs";
            using (var reader = blobs.ExecuteReader())
            {
                Assert.True(reader.Read());
                Assert.Equal(("sha256:55bb3c98333f4f20aa23ec49458f604541b9b5413cdf3b90ad17ceae5653b036", 100_002L), (reader.GetString(0), reader.GetInt64(1)));
                Assert.False(reader.Read());
            }
        }
    }

    // docs:show
    public sealed record Report(string Evidence, int Step);
    // docs:end guides/persistence/retention-and-storage-size/deltas-and-offload
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.RetentionAndStorageSize
{
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Storage;
    using Datalium.Strata.Storage.Memory;

    public partial class RetentionAndStorageSizeSamples
    {
        [Fact]
        public async Task FilesystemBlobs()
        {
            var path = Files.TempSqlite("blobs");
            Files.Delete(path, "blobs");
            try
            {
                // docs:begin guides/persistence/retention-and-storage-size/filesystem-blobs
                var options = new SqliteStoreOptions
                {
                    Deltas = new DeltaOptions(),
                    Offload = new OffloadOptions { Blobs = new FileSystemCheckpointBlobStore("blobs") },
                };
                // docs:end guides/persistence/retention-and-storage-size/filesystem-blobs

                using var store = Files.Sqlite(path, options);
                var graph = Reports.Build<Report>(r => r with { Evidence = new string('x', 100_000) }, r => r with { Step = r.Step + 1 });
                Assert.IsType<GraphExecutionResult<Report>.Completed>(await graph.RunAsync("report-1", new Report("", 0), store));

                // The one file the page says the run writes.
                Assert.Equal(
                    [Path.Combine("blobs", "7265706f72742d31", "55bb3c98333f4f20aa23ec49458f604541b9b5413cdf3b90ad17ceae5653b036")],
                    Directory.GetFiles("blobs", "*", SearchOption.AllDirectories));
                Assert.Equal(100_024, (await store.LoadAsync("report-1"))!.FrozenState!.ToJsonString().Length);
            }
            finally
            {
                Files.Delete(path, "blobs");
            }
        }
    }
}
