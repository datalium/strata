// Samples for https://stratamesh.dev/guides/persistence/sqlite/
//
// The first sample is in a namespace block of its own, so it can show its own `using` lines. Every test uses a
// SQLite file of its own in the temp directory.

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Sqlite
{
    // docs:begin guides/persistence/sqlite/open
    using Datalium.Strata.Storage;
    using Datalium.Strata.Storage.Memory;
    using Microsoft.EntityFrameworkCore;

    // docs:hide
    using Microsoft.Data.Sqlite;

    public partial class SqliteSamples
    {
        [Fact]
        public async Task Open()
        {
            using var file = new TempSqliteFile();
            // docs:show
            var path = "strata.db";
            // docs:hide
            path = file.Path;
            // docs:show
            using var checkpoints = new EfCoreCheckpointStore(
                new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>()
                    .UseSqlite($"Data Source={path}").Options),
                options: new SqliteStoreOptions { GroupCommit = new GroupCommitOptions() });
            using var history = new EfCoreHistoryStore(
                new HistoryDbContext(new DbContextOptionsBuilder<HistoryDbContext>()
                    .UseSqlite($"Data Source={path}").Options));
            // docs:end guides/persistence/sqlite/open

            // The missing file was created, with the tables the page lists, at checkpoints schema version 2.
            await using (var connection = new SqliteConnection($"Data Source={path}"))
            {
                await connection.OpenAsync();
                var tables = new List<string>();
                await using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name";
                    await using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        tables.Add(reader.GetString(0));
                    }
                }

                foreach (var table in new[]
                {
                    "checkpoints", "checkpoint_threads", "checkpoint_leases", "checkpoint_blobs", "strata_schema_version",
                    "HistoryRecords",
                })
                {
                    Assert.Contains(table, tables);
                }

                await using var version = connection.CreateCommand();
                version.CommandText = "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'";
                Assert.Equal(2L, await version.ExecuteScalarAsync());

                // A file that a newer version of Strata wrote is refused, and left as it is.
                await using var newer = connection.CreateCommand();
                newer.CommandText = "UPDATE strata_schema_version SET version = 3 WHERE component = 'checkpoints'";
                await newer.ExecuteNonQueryAsync();
            }

            var refused = Assert.Throws<InvalidOperationException>(() => new EfCoreCheckpointStore(
                new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>()
                    .UseSqlite($"Data Source={path}").Options)));
            Assert.Equal(
                "database is at checkpoints schema version 3, and this build knows 2: upgrade the runtime", refused.Message);
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Sqlite
{
    using Datalium.Strata.Graph;
    using Datalium.Strata.Hosting;
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Memory;
    using Datalium.Strata.Storage;
    using Datalium.Strata.Storage.Memory;
    using Microsoft.Data.Sqlite;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Options;

    public partial class SqliteSamples
    {
        [Fact]
        public async Task ManyThreads()
        {
            using var file = new TempSqliteFile();
            using var checkpoints = new EfCoreCheckpointStore(
                new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>()
                    .UseSqlite($"Data Source={file.Path}").Options),
                options: new SqliteStoreOptions { GroupCommit = new GroupCommitOptions() });
            var graph = Refunds.Build();

            // docs:begin guides/persistence/sqlite/many-threads
            var runs = Enumerable.Range(1, 64).Select(i =>
                graph.RunAsync($"refund-{i}", new Refund($"customer-{i}", 10m * i, "new"), checkpoints));
            await Task.WhenAll(runs);

            var page = await checkpoints.ListThreadsAsync(new ThreadQuery
            {
                States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Interrupted },
            });
            Console.WriteLine($"{page.Threads.Count} refunds wait for approval");
            // docs:end guides/persistence/sqlite/many-threads

            Assert.Equal(64, page.Threads.Count);
            Assert.All(page.Threads, thread => Assert.Equal("Prepare", thread.SuspendedNode));
        }

        // The settings file is files/guides/persistence/sqlite/appsettings.json.
        [Fact]
        public async Task HostSettings()
        {
            using var file = new TempSqliteFile();
            var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
            builder.Configuration.AddJsonFile(
                Path.Combine(AppContext.BaseDirectory, "files", "guides", "persistence", "sqlite", "appsettings.json"));
            // The file's "strata.db" is relative to the working directory: the test uses a temp file instead.
            builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Strata:Sqlite:Path"] = file.Path,
            });

            // docs:begin guides/persistence/sqlite/host
            builder.Services.AddStrata()
                .AddSqliteCheckpoints()   // Strata:Sqlite
                .AddSqliteHistory();      // the same file
            // docs:end guides/persistence/sqlite/host

            using (var host = builder.Build())
            {
                Assert.True(host.Services.GetRequiredService<IOptions<StrataSqliteOptions>>().Value.GroupCommit.Enabled);
                Assert.False(File.Exists(file.Path));

                // The host opens the file when it starts.
                await host.StartAsync();
                Assert.True(File.Exists(file.Path));
                Assert.IsType<EfCoreCheckpointStore>(host.Services.GetRequiredService<ICheckpointStore>());
                Assert.IsType<EfCoreHistoryStore>(host.Services.GetRequiredService<IHistoryStore>());
                await host.StopAsync();
            }

            var fileless = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
            fileless.Services.AddStrata().AddSqliteCheckpoints().AddSqliteHistory();
            using var refusing = fileless.Build();
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => refusing.StartAsync());
            Assert.Equal("Strata's SQLite store has no file: set Strata:Sqlite:Path", refused.Message);
        }
    }

    internal sealed record Refund(string Customer, decimal Amount, string Status);

    /// <summary>The refund graph from /guides/production/hosting/: <c>Prepare</c>, an interrupt boundary, then <c>Pay</c>.</summary>
    internal static class Refunds
    {
        public static CompiledGraph<Refund> Build() =>
            new StateGraph<Refund>()
                .AddNode("Prepare", refund => refund with { Status = "awaiting approval" })
                .AddInterruptBoundary("Prepare")
                .AddNode("Pay", refund => refund with { Status = "paid" })
                .AddEdge("Prepare", "Pay")
                .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });
    }

    /// <summary>A SQLite file in the temp directory, deleted on dispose.</summary>
    internal sealed class TempSqliteFile : IDisposable
    {
        public string Path { get; } =
            System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"docs-sqlite-{Guid.NewGuid():N}.db");

        public void Dispose()
        {
            // Microsoft.Data.Sqlite pools connections, which keeps the file open after a store is disposed.
            SqliteConnection.ClearAllPools();
            File.Delete(Path);
        }
    }
}
