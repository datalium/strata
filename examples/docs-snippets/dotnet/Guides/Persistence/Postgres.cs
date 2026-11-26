// Samples for https://stratamesh.dev/guides/persistence/postgres/
//
// Each sample is in a namespace block of its own, so it can show its own `using` lines. Every test opens its
// stores in a Postgres schema of its own (PostgresSandbox), so the thread IDs the page shows can't collide with
// another test's. The tests need STRATA_TEST_POSTGRES_URL: they skip without it, and fail without it when
// STRATA_REQUIRE_BACKENDS=1.

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Postgres
{
    // docs:begin guides/persistence/postgres/open
    using Datalium.Strata.Graph;
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Memory;
    using Datalium.Strata.Storage.Postgres;

    // docs:hide
    public partial class PostgresSamples
    {
        [PostgresFact]
        public async Task Open()
        {
            using var sandbox = new PostgresSandbox("postgres_open");
            // docs:show
            var url = "postgres://strata:strata@localhost:5432/strata";
            // docs:hide
            url = sandbox.ConnectionString;
            // docs:show
            await using var checkpoints = await PostgresCheckpointStore.OpenAsync(url);
            await using var history = await PostgresHistoryStore.OpenAsync(url);

            var result = await Refunds.Build().RunAsync("refund-1042", new Refund("acme", 120m, "new"), checkpoints);
            await history.AppendAsync("refund-1042",
                new ChatMessage(MessageRole.User, "Refund order 1042".AsMemory(), DateTimeOffset.UtcNow));

            Console.WriteLine(result is GraphExecutionResult<Refund>.Interrupted waiting
                ? $"refund-1042 waits after {waiting.SuspendedNode}"
                : $"unexpected: {result}");
            // docs:end guides/persistence/postgres/open

            var interrupted = Assert.IsType<GraphExecutionResult<Refund>.Interrupted>(result);
            Assert.Equal("Prepare", interrupted.SuspendedNode);
            var saved = await checkpoints.LoadAsync("refund-1042");
            Assert.Equal(CheckpointStatus.Interrupted, saved?.Status);
            var messages = await history.GetHistoryAsync("refund-1042", 10).ToListAsync();
            Assert.Equal("Refund order 1042", Assert.Single(messages).Content.ToString());

            // The constructors don't touch the database: a store made with `new` prepares its tables on first use.
            using var fresh = new PostgresSandbox("postgres_open_new");
            await using var unprepared = new PostgresCheckpointStore(fresh.ConnectionString);
            Assert.Null(await unprepared.LoadAsync("refund-1042"));
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Postgres
{
    // docs:begin guides/persistence/postgres/tls
    using Npgsql;

    // docs:hide
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Storage.Postgres;

    public partial class PostgresSamples
    {
        [PostgresFact]
        public async Task OwnDataSource()
        {
            using var sandbox = new PostgresSandbox("postgres_tls");
            // docs:show
            var builder = new NpgsqlDataSourceBuilder(
                "Host=db.internal;Username=strata;Password=...;Database=strata;SSL Mode=VerifyFull;Maximum Pool Size=20");
            // docs:hide
            // The test's server has no TLS and no db.internal: the same calls, on the sandbox.
            builder = new NpgsqlDataSourceBuilder(sandbox.ConnectionString);
            // docs:show
            await using var dataSource = builder.Build();

            await using var checkpoints = new PostgresCheckpointStore(dataSource);
            await using var history = new PostgresHistoryStore(dataSource);
            await checkpoints.MigrateAsync();
            await history.MigrateAsync();
            // docs:end guides/persistence/postgres/tls

            var result = await Refunds.Build().RunAsync("refund-1042", new Refund("acme", 120m, "new"), checkpoints);
            Assert.IsType<GraphExecutionResult<Refund>.Interrupted>(result);

            // A store doesn't dispose a data source you give it.
            await checkpoints.DisposeAsync();
            await history.DisposeAsync();
            await using var connection = await dataSource.OpenConnectionAsync();
            Assert.Equal(System.Data.ConnectionState.Open, connection.State);
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Postgres
{
    using Datalium.Strata.Storage.Postgres;

    public partial class PostgresSamples
    {
        [PostgresFact]
        public async Task MigrateInADeployStep()
        {
            using var sandbox = new PostgresSandbox("postgres_migrate");
            var url = sandbox.ConnectionString;

            // Before the deploy step has run, the application's store refuses the database.
            await using (var early = new PostgresCheckpointStore(url, new PostgresStoreOptions { AutoMigrate = false }))
            {
                var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => early.LoadAsync("refund-1042").AsTask());
                Assert.Equal(
                    "database is at checkpoints schema version 0, and this build needs 2: run its migration",
                    refused.Message);
            }

            // docs:begin guides/persistence/postgres/auto-migrate
            // The deploy step:
            await using (var migrate = await PostgresCheckpointStore.OpenAsync(url)) { }
            await using (var migrate = await PostgresHistoryStore.OpenAsync(url)) { }

            // The application:
            var options = new PostgresStoreOptions { AutoMigrate = false };
            await using var checkpoints = new PostgresCheckpointStore(url, options);
            var latest = await checkpoints.LoadAsync("refund-1042"); // checks the tables first
            // docs:end guides/persistence/postgres/auto-migrate

            Assert.Null(latest);
            await using var history = new PostgresHistoryStore(url, options);
            Assert.Empty(await history.GetHistoryAsync("refund-1042", 10).ToListAsync());
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Postgres
{
    // docs:begin guides/persistence/postgres/retries
    using Datalium.Strata.Storage;
    using Datalium.Strata.Storage.Postgres;

    // docs:hide
    using Datalium.Strata.Interruption;

    public partial class PostgresSamples
    {
        [PostgresFact]
        public async Task Retries()
        {
            using var sandbox = new PostgresSandbox("postgres_retries");
            var url = sandbox.ConnectionString;
            // docs:show
            var options = new PostgresStoreOptions
            {
                Retry = new StoreRetryPolicy { MaxAttempts = 6, MaxDelay = TimeSpan.FromSeconds(5) },
                GroupCommit = new GroupCommitOptions(),
            };
            await using var checkpoints = await PostgresCheckpointStore.OpenAsync(url, options);
            // docs:end guides/persistence/postgres/retries

            Assert.Equal(TimeSpan.FromMilliseconds(50), options.Retry.InitialDelay);
            Assert.Equal(1, StoreRetryPolicy.None.MaxAttempts);
            var runs = Enumerable.Range(1, 8).Select(i =>
                Refunds.Build().RunAsync($"refund-{i}", new Refund($"customer-{i}", 10m * i, "new"), checkpoints));
            foreach (var result in await Task.WhenAll(runs))
            {
                Assert.IsType<GraphExecutionResult<Refund>.Interrupted>(result);
            }
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Postgres
{
    using Datalium.Strata.Hosting;
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Memory;
    using Datalium.Strata.Storage.Postgres;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Options;

    public partial class PostgresSamples
    {
        // The settings file is files/guides/persistence/postgres/appsettings.json.
        [PostgresFact]
        public async Task HostSettings()
        {
            using var sandbox = new PostgresSandbox("postgres_host");
            var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
            builder.Configuration.AddJsonFile(
                Path.Combine(AppContext.BaseDirectory, "files", "guides", "persistence", "postgres", "appsettings.json"));
            // The file's URL names the compose file's database: point the test at its own schema of it.
            builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:strata"] = sandbox.ConnectionString,
            });
            builder.Services.AddStrata()
                .AddPostgresCheckpoints()
                .AddPostgresHistory();
            using var host = builder.Build();

            var settings = host.Services.GetRequiredService<IOptions<StrataPostgresOptions>>().Value;
            Assert.True(settings.AutoMigrate);
            Assert.Equal(6, settings.Retry.MaxAttempts);
            Assert.True(settings.GroupCommit.Enabled);

            // Starting the host migrates the tables.
            await host.StartAsync();
            Assert.IsType<PostgresCheckpointStore>(host.Services.GetRequiredService<ICheckpointStore>());
            Assert.IsType<PostgresHistoryStore>(host.Services.GetRequiredService<IHistoryStore>());
            await using var check = new PostgresCheckpointStore(
                sandbox.ConnectionString, new PostgresStoreOptions { AutoMigrate = false });
            Assert.Null(await check.LoadAsync("refund-1042"));
            await host.StopAsync();
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Postgres
{
    // docs:begin guides/persistence/postgres/resume
    using System.Text.Json.Serialization;

    // docs:hide
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Memory;
    using Datalium.Strata.Storage.Postgres;

    public partial class PostgresSamples
    {
        [PostgresFact]
        public async Task Resume()
        {
            using var sandbox = new PostgresSandbox("postgres_resume");
            await using var checkpoints = await PostgresCheckpointStore.OpenAsync(sandbox.ConnectionString);
            await using var history = await PostgresHistoryStore.OpenAsync(sandbox.ConnectionString);

            // What the first sample on the page did, in this language or the other.
            Assert.IsType<GraphExecutionResult<Refund>.Interrupted>(
                await Refunds.Build().RunAsync("refund-1042", new Refund("acme", 120m, "new"), checkpoints));
            await history.AppendAsync("refund-1042",
                new ChatMessage(MessageRole.User, "Refund order 1042".AsMemory(), DateTimeOffset.UtcNow));

            // docs:show
            var result = await Refunds.Build().ResumeAsync("refund-1042", HumanSignal.Approve("jdoe"), checkpoints);
            if (result is GraphExecutionResult<Refund>.Completed done)
            {
                Console.WriteLine($"refund-1042 is {done.State.Status}");
            }

            await foreach (var message in history.GetHistoryAsync("refund-1042", 10))
            {
                Console.WriteLine($"{message.Role}: {message.Content}");
            }

            // docs:hide
            Assert.Equal("paid", Assert.IsType<GraphExecutionResult<Refund>.Completed>(result).State.Status);
            var stored = Assert.Single(await history.GetHistoryAsync("refund-1042", 10).ToListAsync());
            Assert.Equal("User: Refund order 1042", $"{stored.Role}: {stored.Content}");
        }
    }

    // docs:show
    public sealed record Refund(
        [property: JsonPropertyName("customer")] string Customer,
        [property: JsonPropertyName("amount")] decimal Amount,
        [property: JsonPropertyName("status")] string Status);
    // docs:end guides/persistence/postgres/resume
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Postgres
{
    using Datalium.Strata.Graph;
    using Datalium.Strata.Storage.Postgres;
    using Npgsql;

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

    /// <summary>
    /// The Postgres the tests use, from <c>STRATA_TEST_POSTGRES_URL</c>, as the strata tests' <c>TestBackends</c>
    /// reads it: a test skips when it is unset, unless <c>STRATA_REQUIRE_BACKENDS=1</c>, which fails it instead.
    /// </summary>
    internal static class PostgresBackend
    {
        public const string Variable = "STRATA_TEST_POSTGRES_URL";

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

    /// <summary>A test that needs the Postgres from <see cref="PostgresBackend"/>.</summary>
    public sealed class PostgresFactAttribute : FactAttribute
    {
        public PostgresFactAttribute() => Skip = PostgresBackend.SkipReason;
    }

    /// <summary>
    /// A throwaway Postgres schema: a store on <see cref="ConnectionString"/> puts its tables in it
    /// (<c>Search Path</c>), and it is dropped on dispose. Every test gets its own, so the thread IDs a page
    /// shows can't collide with another test's, and a test can be re-run.
    /// </summary>
    internal sealed class PostgresSandbox : IDisposable
    {
        private readonly string _admin;

        public PostgresSandbox(string label)
        {
            _admin = PostgresConnection.ToConnectionString(PostgresBackend.Url);
            Schema = $"docs_{label}_{Guid.NewGuid():N}";
            ConnectionString = new NpgsqlConnectionStringBuilder(_admin) { SearchPath = Schema }.ConnectionString;
            Execute($"CREATE SCHEMA {Schema}");
        }

        public string Schema { get; }

        /// <summary>An Npgsql connection string scoped to the schema.</summary>
        public string ConnectionString { get; }

        public void Dispose()
        {
            Execute($"DROP SCHEMA IF EXISTS {Schema} CASCADE");
        }

        private void Execute(string sql)
        {
            using var dataSource = NpgsqlDataSource.Create(_admin);
            using var command = dataSource.CreateCommand(sql);
            command.ExecuteNonQuery();
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.Postgres
{
    // docs:begin guides/persistence/postgres/read-only
    using Datalium.Strata.Storage.Postgres;

    // docs:hide
    using Datalium.Strata.Interruption;
    using Npgsql;

    public partial class PostgresSamples
    {
        [PostgresFact]
        public async Task OpenReadOnly()
        {
            using var sandbox = new PostgresSandbox("postgres_read_only");
            await using (var writable = await PostgresCheckpointStore.OpenAsync(sandbox.ConnectionString))
            {
                await Refunds.Build().RunAsync("refund-1042", new Refund("acme", 120m, "new"), writable);
            }

            // docs:show
            var url = "postgres://reader:secret@replica.internal:5432/strata";
            // docs:hide
            url = sandbox.ConnectionString;
            // docs:show
            await using var store = await PostgresCheckpointStore.OpenReadOnlyAsync(url);
            // docs:end guides/persistence/postgres/read-only

            Assert.True(store.IsReadOnly);
            var latest = await store.LoadAsync("refund-1042");
            Assert.Equal(CheckpointStatus.Interrupted, latest?.Status);
            Assert.Equal(
                "the store was opened read-only: save is refused",
                (await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.SaveAsync(latest!))).Message);
            Assert.Equal(
                "the store was opened read-only: migrate is refused",
                (await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.MigrateAsync())).Message);

            // A schema with no Strata tables is refused, by the database's name, and left empty.
            using var empty = new PostgresSandbox("postgres_read_only_empty");
            var database = new NpgsqlConnectionStringBuilder(empty.ConnectionString).Database;
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(
                () => PostgresCheckpointStore.OpenReadOnlyAsync(empty.ConnectionString));
            Assert.Equal($"'{database}' has no Strata checkpoint log", refused.Message);
        }
    }
}
