using Datalium.Strata.AI;
using Datalium.Strata.Hosting;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Storage.Redis;
using Datalium.Strata.Storage.Sqlite;
using Datalium.Strata.Tests.AI;
using Datalium.Strata.Tests.Backends;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using StackExchange.Redis;

namespace Datalium.Strata.Tests.Hosting;

/// <summary>
/// Long-term memory in the hosting package (<c>design/45-Long-Term-Memory.md</c>, "Hosting"): the memory store
/// registrations, <c>Strata:Memory</c>, the start-up step that prepares the store, <c>AddMemory</c>'s helper, and the
/// memory retention driver. Mirrors <c>strata-host</c>'s memory tests. The Postgres and Redis tests need
/// <c>STRATA_TEST_POSTGRES_URL</c> / <c>STRATA_TEST_REDIS_URL</c>; the SQLite ones the sqlite-vec package the test
/// project references.
/// </summary>
public class MemoryHostingTests
{
    private static readonly DateTimeOffset Noon = new(2026, 10, 6, 12, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(15);

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

    private static async Task<string> Refusal(Action<IStrataBuilder> register, Dictionary<string, string?>? settings = null)
    {
        var builder = Builder(settings: settings);
        register(builder.Services.AddStrata());
        using var host = builder.Build();
        return (await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync())).Message;
    }

    private static string NewPath() => Path.Combine(Path.GetTempPath(), $"strata_memory_hosting_{Guid.NewGuid():N}.sqlite");

    private static void Delete(string path)
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        File.Delete(path);
    }

    /// <summary>A store with the store port only, as an application's own might be.</summary>
    private sealed class BareStore : IMemoryStore
    {
        private readonly InMemoryMemoryStore _inner = new(3);

        public int Dimensions => _inner.Dimensions;

        public ValueTask UpsertAsync(MemoryRecord record, CancellationToken cancellationToken = default) => _inner.UpsertAsync(record, cancellationToken);

        public ValueTask UpsertManyAsync(IReadOnlyList<MemoryRecord> records, CancellationToken cancellationToken = default) =>
            _inner.UpsertManyAsync(records, cancellationToken);

        public ValueTask<MemoryRecord?> GetAsync(MemoryKey key, CancellationToken cancellationToken = default) => _inner.GetAsync(key, cancellationToken);

        public ValueTask<bool> DeleteAsync(MemoryKey key, CancellationToken cancellationToken = default) => _inner.DeleteAsync(key, cancellationToken);

        public ValueTask<IReadOnlyList<MemoryMatch>> SearchAsync(MemorySearch search, CancellationToken cancellationToken = default) =>
            _inner.SearchAsync(search, cancellationToken);

        public ValueTask<MemoryPage> ListAsync(MemoryQuery query, CancellationToken cancellationToken = default) => _inner.ListAsync(query, cancellationToken);
    }

    // --- Registration ---

    [Fact]
    public void A_Memory_Store_Is_One_Instance_Under_Every_Port_It_Has_And_A_Second_Registration_Replaces_It()
    {
        var builder = Builder();
        builder.Services.AddStrata().AddInMemoryMemory(o => o.Dimensions = 3);
        using (var host = builder.Build())
        {
            var store = host.Services.GetRequiredService<InMemoryMemoryStore>();
            Assert.Same(store, host.Services.GetRequiredService<IMemoryStore>());
            Assert.Same(store, host.Services.GetRequiredService<IMemoryRetention>());
            Assert.Equal(3, store.Dimensions);
        }

        // A store with fewer ports leaves none of the first one's behind.
        builder = Builder();
        builder.Services.AddStrata().AddInMemoryMemory(o => o.Dimensions = 3).AddMemoryStore(_ => new BareStore());
        using (var host = builder.Build())
        {
            Assert.IsType<BareStore>(host.Services.GetRequiredService<IMemoryStore>());
            Assert.Null(host.Services.GetService<IMemoryRetention>());
            Assert.Single(host.Services.GetServices<IMemoryStore>());
        }

        // The last registration is the store; resolving one touches no server.
        builder = Builder(settings: new() { ["Strata:Postgres:ConnectionString"] = "Host=db.invalid", ["Strata:Sqlite:Path"] = NewPath() });
        builder.Services.AddStrata().AddRedisMemory(o => o.Dimensions = 3).AddSqliteMemory(o => o.Dimensions = 3).AddPostgresMemory(o => o.Dimensions = 3);
        using (var host = builder.Build())
        {
            var store = host.Services.GetRequiredService<PostgresMemoryStore>();
            Assert.Same(store, host.Services.GetRequiredService<IMemoryStore>());
            Assert.Same(store, host.Services.GetRequiredService<IMemoryRetention>());
        }

        builder = Builder(settings: new() { ["Strata:Redis:ConnectionString"] = "localhost:1" });
        builder.Services.AddStrata().AddInMemoryMemory(o => o.Dimensions = 3).AddRedisMemory(o => o.Dimensions = 3);
        Assert.Contains(builder.Services, d => d.ServiceType == typeof(RedisMemoryStore));
        Assert.Single(builder.Services, d => d.ServiceType == typeof(IMemoryStore));
    }

    [Fact]
    public async Task A_Store_Registered_As_The_Port_Itself_Resolves()
    {
        var builder = Builder();
        builder.Services.AddStrata().AddMemoryStore<IMemoryStore>(_ => new InMemoryMemoryStore(3));
        using var host = builder.Build();

        // Forwarded to itself, the port would never resolve.
        var store = await Task.Run(() => host.Services.GetRequiredService<IMemoryStore>()).WaitAsync(Patience);
        Assert.IsType<InMemoryMemoryStore>(store);
        Assert.Single(host.Services.GetServices<IMemoryStore>());
        await host.StartAsync();
        await host.StopAsync();
    }

    [Fact]
    public void Settings_Come_From_Configuration_And_A_Delegate_Is_Applied_Over_Them()
    {
        var builder = Builder(settings: new()
        {
            ["Strata:Memory:Dimensions"] = "768",
            ["Strata:Memory:Index:Kind"] = "Hnsw",
            ["Strata:Memory:Index:M"] = "8",
            ["Strata:Memory:Index:EfConstruction"] = "32",
            ["Strata:Memory:Table"] = "ops.memories",
            ["Strata:Memory:Prefix"] = "ops",
            ["Strata:Memory:Extension"] = "/opt/sqlite/vec0.so",
            ["Strata:Memory:Model"] = "nomic-768",
            ["Strata:Memory:TenantKey"] = "org",
            ["Strata:Memory:Retention:Interval"] = "00:10:00",
            ["Strata:Memory:Retention:BatchSize"] = "50",
            ["Strata:Memory:Retention:OlderThan"] = "365.00:00:00",
            ["Strata:Memory:Retention:Namespace"] = "incidents",
            ["Strata:Memory:Retention:Tenant"] = "acme",
        });
        builder.Services.AddStrata()
            .AddInMemoryMemory(o => o.Index.M = 12)
            .AddMemory(o => o.TenantKey = "account")
            .AddMemoryRetention(o => o.BatchSize = 25);
        using var host = builder.Build();

        var memory = host.Services.GetRequiredService<IOptions<StrataMemoryHostOptions>>().Value;
        Assert.Equal((768, "ops.memories", "ops", "/opt/sqlite/vec0.so", "nomic-768"), (memory.Dimensions, memory.Table, memory.Prefix, memory.Extension, memory.Model));
        Assert.Equal("account", memory.TenantKey);
        Assert.Equal((MemoryIndexKind.Hnsw, 12, 32), (memory.Index.Kind, memory.Index.M, memory.Index.EfConstruction));
        Assert.Equal(MemoryIndex.Hnsw(12, 32), memory.Index.ToIndex());
        var retention = host.Services.GetRequiredService<IOptions<MemoryRetentionOptions>>().Value;
        Assert.Equal(
            (TimeSpan.FromMinutes(10), 25, TimeSpan.FromDays(365), "incidents", "acme"),
            (retention.Interval, retention.BatchSize, retention.OlderThan, retention.Namespace, retention.Tenant));
        Assert.Equal(768, host.Services.GetRequiredService<IMemoryStore>().Dimensions);
    }

    [Fact]
    public void Without_Configuration_Every_Setting_Has_Its_Default()
    {
        var services = new ServiceCollection();
        services.AddStrata().AddInMemoryMemory().AddMemory().AddMemoryRetention();
        using var provider = services.BuildServiceProvider();

        var memory = provider.GetRequiredService<IOptions<StrataMemoryHostOptions>>().Value;
        Assert.Equal((0, null, null, null, null, "tenant"), (memory.Dimensions, memory.Table, memory.Prefix, memory.Extension, memory.Model, memory.TenantKey));
        Assert.Equal(MemoryIndex.None, memory.Index.ToIndex());
        var retention = provider.GetRequiredService<IOptions<MemoryRetentionOptions>>().Value;
        Assert.Equal((TimeSpan.FromHours(1), 1000, null, null, null), (retention.Interval, retention.BatchSize, retention.OlderThan, retention.Namespace, retention.Tenant));
    }

    [Fact]
    public void The_Index_And_The_Table_Reach_The_Postgres_Store()
    {
        // pgvector's HNSW takes at most 2000 dimensions, and a table name is checked: both refusals are the store's.
        T Resolve<T>(Action<StrataMemoryHostOptions> configure)
            where T : Exception
        {
            var builder = Builder(settings: new() { ["Strata:Postgres:ConnectionString"] = "Host=db.invalid" });
            builder.Services.AddStrata().AddPostgresMemory(configure);
            using var host = builder.Build();
            return Assert.Throws<T>(() => host.Services.GetRequiredService<IMemoryStore>());
        }

        Resolve<ArgumentOutOfRangeException>(o =>
        {
            o.Dimensions = 3000;
            o.Index.Kind = MemoryIndexKind.Hnsw;
        });
        Assert.StartsWith("the memory table 'Bad-Table'", Resolve<ArgumentException>(o =>
        {
            o.Dimensions = 3;
            o.Table = "Bad-Table";
        }).Message, StringComparison.Ordinal);
    }

    // --- Start-up ---

    [Fact]
    public async Task A_Memory_Store_Without_What_It_Needs_Stops_The_Host_From_Starting()
    {
        Assert.Equal(
            "Strata's memory store has no dimensions: set Strata:Memory:Dimensions",
            await Refusal(strata => strata.AddInMemoryMemory()));
        Assert.Equal(
            "Strata's memory store has no dimensions: set Strata:Memory:Dimensions",
            await Refusal(strata => strata.AddPostgresMemory(), new() { ["Strata:Postgres:ConnectionString"] = "Host=db.invalid" }));
        Assert.Equal(
            "Strata's SQLite store has no file: set Strata:Sqlite:Path",
            await Refusal(strata => strata.AddSqliteMemory(o => o.Dimensions = 3)));
        Assert.StartsWith(
            "Strata's Postgres store has no connection",
            await Refusal(strata => strata.AddPostgresMemory(o => o.Dimensions = 3)),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_Sqlite_Memory_Store_Shares_The_Checkpoint_Stores_File_And_Refuses_Other_Dimensions_At_Start_Up()
    {
        var path = NewPath();
        try
        {
            var builder = Builder(settings: new() { ["Strata:Sqlite:Path"] = path, ["Strata:Memory:Dimensions"] = "3" });
            builder.Services.AddStrata().AddSqliteCheckpoints().AddSqliteMemory();
            using (var host = builder.Build())
            {
                await host.StartAsync();
                var store = Assert.IsType<SqliteMemoryStore>(host.Services.GetRequiredService<IMemoryStore>());
                await store.UpsertAsync(new MemoryRecord("incidents", "", "inc-1", new float[] { 1, 0, 0 }, "m"));
                Assert.NotNull(await store.GetAsync(new MemoryKey("incidents", "", "inc-1")));
                Assert.Null(await host.Services.GetRequiredService<ICheckpointStore>().LoadAsync("nobody"));
                await host.StopAsync();
            }

            var refusal = await Refusal(
                strata => strata.AddSqliteMemory(),
                new() { ["Strata:Sqlite:Path"] = path, ["Strata:Memory:Dimensions"] = "4" });
            Assert.Equal("memory was created with 3 dimensions, and this store was opened with 4", refusal);
        }
        finally
        {
            Delete(path);
        }
    }

    [Fact]
    public async Task A_Sqlite_Memory_Store_Without_Sqlite_Vec_Stops_The_Host()
    {
        var path = NewPath();
        try
        {
            var refusal = await Refusal(
                strata => strata.AddSqliteMemory(o =>
                {
                    o.Dimensions = 3;
                    o.Extension = "no_such_vec_extension";
                }),
                new() { ["Strata:Sqlite:Path"] = path });
            Assert.StartsWith("memory on SQLite needs the sqlite-vec extension", refusal, StringComparison.Ordinal);
        }
        finally
        {
            Delete(path);
        }
    }

    // --- The helper ---

    [Fact]
    public async Task AddMemory_Registers_A_Helper_Labelled_By_Configuration_Or_By_The_Generator()
    {
        var builder = Builder();
        builder.Services.AddSingleton<IEmbeddingGenerator<string, Embedding<float>>>(ScriptedEmbeddingGenerator.ByLength());
        builder.Services.AddStrata().AddInMemoryMemory(o => o.Dimensions = 3).AddMemory();
        using (var host = builder.Build())
        {
            await host.StartAsync();
            var memory = host.Services.GetRequiredService<StrataMemory>();
            Assert.Equal("scripted/embed-1", memory.Model);
            Assert.Same(host.Services.GetRequiredService<IMemoryStore>(), memory.Store);
            Assert.Same(memory, host.Services.GetRequiredService<StrataMemory>());

            await memory.RememberAsync(new MemoryEntry("incidents", "inc-1", "abc"));
            Assert.Equal("inc-1", Assert.Single(await memory.RecallAsync("incidents", "abd")).Record.Id);
            await host.StopAsync();
        }

        // A blank label is no label: the generator's names the vectors.
        builder = Builder(settings: new() { ["Strata:Memory:Model"] = " " });
        builder.Services.AddSingleton<IEmbeddingGenerator<string, Embedding<float>>>(ScriptedEmbeddingGenerator.ByLength());
        builder.Services.AddStrata().AddInMemoryMemory(o => o.Dimensions = 3).AddMemory();
        using (var host = builder.Build())
        {
            await host.StartAsync();
            Assert.Equal("scripted/embed-1", host.Services.GetRequiredService<StrataMemory>().Model);
            await host.StopAsync();
        }

        builder = Builder(settings: new() { ["Strata:Memory:Model"] = "nomic-768" });
        builder.Services.AddSingleton<IEmbeddingGenerator<string, Embedding<float>>>(ScriptedEmbeddingGenerator.ByLength(model: null));
        builder.Services.AddStrata().AddMemory().AddInMemoryMemory(o => o.Dimensions = 3);
        using (var host = builder.Build())
        {
            await host.StartAsync();
            Assert.Equal("nomic-768", host.Services.GetRequiredService<StrataMemory>().Model);
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task AddMemory_Without_A_Store_A_Generator_Or_A_Label_Stops_The_Host_From_Starting()
    {
        Assert.Equal(
            "AddMemory needs a memory store: AddInMemoryMemory, AddPostgresMemory, AddSqliteMemory, AddRedisMemory or AddMemoryStore",
            await Refusal(strata =>
            {
                strata.Services.AddSingleton<IEmbeddingGenerator<string, Embedding<float>>>(ScriptedEmbeddingGenerator.ByLength());
                strata.AddMemory();
            }));
        Assert.Equal(
            "AddMemory needs an IEmbeddingGenerator<string, Embedding<float>> in the container",
            await Refusal(strata => strata.AddInMemoryMemory(o => o.Dimensions = 3).AddMemory()));
        Assert.Equal(
            "the embedding generator names no model: label the memory's vectors with StrataMemoryOptions.Model",
            await Refusal(strata =>
            {
                strata.Services.AddSingleton<IEmbeddingGenerator<string, Embedding<float>>>(ScriptedEmbeddingGenerator.ByLength(model: null));
                strata.AddInMemoryMemory(o => o.Dimensions = 3).AddMemory();
            }));
    }

    // --- The retention driver ---

    [Fact]
    public async Task The_Memory_Retention_Driver_Without_What_It_Needs_Stops_The_Host_From_Starting()
    {
        Assert.Equal(
            "the memory retention driver needs Strata:Memory:Retention:OlderThan: how long a memory is kept after it was last written",
            await Refusal(strata => strata.AddInMemoryMemory(o => o.Dimensions = 3).AddMemoryRetention()));
        Assert.Equal(
            "the memory retention driver needs Strata:Memory:Retention:OlderThan: how long a memory is kept after it was last written",
            await Refusal(strata => strata.AddInMemoryMemory(o => o.Dimensions = 3).AddMemoryRetention(o => o.OlderThan = TimeSpan.Zero)));
        Assert.Equal(
            "the memory retention driver needs a memory store that purges (IMemoryRetention), and the registered one doesn't",
            await Refusal(strata => strata.AddMemoryStore(_ => new BareStore()).AddMemoryRetention(o => o.OlderThan = TimeSpan.FromDays(1))));
        // A namespace or tenant every purge would refuse stops the host with the store's message.
        Assert.Equal(
            "the namespace is empty",
            await Refusal(strata => strata.AddInMemoryMemory(o => o.Dimensions = 3).AddMemoryRetention(o =>
            {
                o.OlderThan = TimeSpan.FromDays(1);
                o.Namespace = "";
            })));
        Assert.Equal(
            "the tenant is longer than 256 bytes",
            await Refusal(strata => strata.AddInMemoryMemory(o => o.Dimensions = 3).AddMemoryRetention(o =>
            {
                o.OlderThan = TimeSpan.FromDays(1);
                o.Tenant = new string('t', 257);
            })));
        Assert.Equal(
            "the memory retention driver's BatchSize must be between 1 and 1000",
            await Refusal(strata => strata.AddInMemoryMemory(o => o.Dimensions = 3).AddMemoryRetention(o =>
            {
                o.OlderThan = TimeSpan.FromDays(1);
                o.BatchSize = 1001;
            })));
        Assert.Equal(
            "the memory retention driver's Interval must be more than zero",
            await Refusal(strata => strata.AddInMemoryMemory(o => o.Dimensions = 3).AddMemoryRetention(o =>
            {
                o.OlderThan = TimeSpan.FromDays(1);
                o.Interval = TimeSpan.Zero;
            })));
    }

    [Fact]
    public async Task The_Memory_Retention_Driver_Purges_Old_Memories_Batch_After_Batch()
    {
        var clock = new FakeTimeProvider(Noon - TimeSpan.FromDays(60));
        var builder = Builder(clock, new()
        {
            ["Strata:Memory:Dimensions"] = "3",
            ["Strata:Memory:Retention:OlderThan"] = "30.00:00:00",
            ["Strata:Memory:Retention:BatchSize"] = "2",
            ["Strata:Memory:Retention:Namespace"] = "incidents",
        });
        builder.Services.AddStrata().AddInMemoryMemory().AddMemoryRetention();
        using var host = builder.Build();
        var store = host.Services.GetRequiredService<IMemoryStore>();
        MemoryRecord Record(string ns, string id) => new(ns, "", id, new float[] { 1, 0, 0 }, "m");
        await store.UpsertManyAsync([.. ((string[])["a", "b", "c", "d", "e"]).Select(id => Record("incidents", id)), Record("runbooks", "kept")]);
        clock.SetUtcNow(Noon - TimeSpan.FromDays(1));
        await store.UpsertAsync(Record("incidents", "recent"));
        clock.SetUtcNow(Noon);

        await host.StartAsync();

        // One round, three purges of two (the last one short): every old incident is gone before the driver waits.
        var status = host.Services.GetRequiredService<IStrataDriverStatus>();
        var deadline = DateTime.UtcNow + Patience;
        while (!status.Drivers.Any(d => d.Name == "memory-retention" && d.Rounds == 1))
        {
            Assert.True(DateTime.UtcNow < deadline, "the first round finished");
            await Task.Delay(10);
        }

        Assert.Equal(["recent"], (await store.ListAsync(new MemoryQuery("incidents"))).Records.Select(r => r.Id));
        Assert.Equal(["kept"], (await store.ListAsync(new MemoryQuery("runbooks"))).Records.Select(r => r.Id));
        Assert.Equal(0, status.Drivers.Single(d => d.Name == "memory-retention").FailuresInARow);
        await host.StopAsync();
    }

    // --- Servers ---

    [PostgresFact]
    public async Task A_Postgres_Memory_Store_Is_Migrated_At_Start_Up_Or_Only_Checked()
    {
        using var sandbox = new PostgresSandbox("memory_hosting");
        await using var dataSource = sandbox.CreateVectorDataSource();

        // A store that doesn't migrate refuses a database without its table, and the host with it.
        IHost Manual()
        {
            var manual = Builder(settings: new() { ["Strata:Postgres:AutoMigrate"] = "false", ["Strata:Memory:Dimensions"] = "3" });
            manual.Services.AddSingleton(dataSource);
            manual.Services.AddStrata().AddPostgresMemory().AddStrataPostgresDataSource();
            return manual.Build();
        }

        using (var host = Manual())
        {
            var refused = await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync());
            Assert.Contains("run its migration", refused.Message, StringComparison.Ordinal);
        }

        var builder = Builder(settings: new() { ["Strata:Memory:Dimensions"] = "3", ["Strata:Memory:Index:Kind"] = "Hnsw" });
        builder.Services.AddSingleton(dataSource);
        builder.Services.AddStrata().AddPostgresMemory().AddStrataPostgresDataSource();
        using (var host = builder.Build())
        {
            await host.StartAsync();
            var store = host.Services.GetRequiredService<IMemoryStore>();
            await store.UpsertAsync(new MemoryRecord("incidents", "", "inc-1", new float[] { 1, 0, 0 }, "m"));
            Assert.NotNull(await store.GetAsync(new MemoryKey("incidents", "", "inc-1")));
            await host.StopAsync();
        }

        // Now the store that only checks is satisfied, and one of other dimensions is refused.
        using (var host = Manual())
        {
            await host.StartAsync();
            await host.StopAsync();
        }

        var other = Builder(settings: new() { ["Strata:Memory:Dimensions"] = "4" });
        other.Services.AddSingleton(dataSource);
        other.Services.AddStrata().AddPostgresMemory().AddStrataPostgresDataSource();
        using (var host = other.Build())
        {
            var refused = await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync());
            Assert.Contains("dimensions", refused.Message, StringComparison.Ordinal);
        }
    }

    [RedisFact]
    public async Task A_Redis_Memory_Store_Is_Prepared_At_Start_Up_Under_The_Redis_Prefix_Or_Its_Own()
    {
        using var sandbox = new RedisSandbox("memory_hosting");
        var own = $"{sandbox.Prefix}:own";
        try
        {
            // A blank memory prefix is none: the Redis one is used.
            var builder = Builder(settings: new()
            {
                ["Strata:Redis:Prefix"] = sandbox.Prefix,
                ["Strata:Memory:Prefix"] = " ",
                ["Strata:Memory:Dimensions"] = "3",
            });
            builder.Services.AddSingleton<IConnectionMultiplexer>(sandbox.Connection);
            builder.Services.AddStrata().AddRedisMemory().AddStrataRedisMultiplexer();
            using (var host = builder.Build())
            {
                await host.StartAsync();
                Assert.Equal("3", (string?)await sandbox.Database.HashGetAsync($"{sandbox.Prefix}:memory:schema", "dimensions"));
                await host.StopAsync();
            }

            var separate = Builder(settings: new() { ["Strata:Memory:Prefix"] = own, ["Strata:Memory:Dimensions"] = "3" });
            separate.Services.AddSingleton<IConnectionMultiplexer>(sandbox.Connection);
            separate.Services.AddStrata().AddRedisMemory().AddStrataRedisMultiplexer();
            using (var host = separate.Build())
            {
                await host.StartAsync();
                Assert.True(await sandbox.Database.KeyExistsAsync($"{own}:memory:schema"));
                await host.StopAsync();
            }

            // The application's multiplexer: the store didn't close it.
            Assert.True(sandbox.Connection.IsConnected);
        }
        finally
        {
            foreach (var prefix in (string[])[sandbox.Prefix, own])
            {
                try
                {
                    await sandbox.Database.ExecuteAsync("FT.DROPINDEX", $"{prefix}:memory:idx");
                }
                catch (RedisServerException)
                {
                    // No index was made.
                }
            }
        }
    }
}

/// <summary>The hosting tests' way of putting the stores on the application's own connection.</summary>
internal static class MemoryHostingTestExtensions
{
    public static IStrataBuilder AddStrataPostgresDataSource(this IStrataBuilder builder)
    {
        builder.Services.Configure<StrataPostgresOptions>(o => o.UseRegisteredDataSource = true);
        return builder;
    }

    public static IStrataBuilder AddStrataRedisMultiplexer(this IStrataBuilder builder)
    {
        builder.Services.Configure<StrataRedisOptions>(o => o.UseRegisteredMultiplexer = true);
        return builder;
    }
}
