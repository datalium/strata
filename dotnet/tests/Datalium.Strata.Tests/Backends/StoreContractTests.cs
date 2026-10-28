using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Storage.Redis;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// Runs <see cref="StoreContracts"/> against every backend: in-memory, SQLite (EF Core), and -- when
/// <c>STRATA_TEST_POSTGRES_URL</c> / <c>STRATA_TEST_REDIS_URL</c> are set; see <see cref="TestBackends"/> --
/// Postgres and Redis. The same code, four storage models.
/// </summary>
public sealed class StoreContractTests
{
    // --- Checkpoint stores ---

    [Fact]
    public void InMemoryCheckpointStoreSatisfiesTheContract() =>
        StoreContracts.CheckpointStoreContract(new InMemoryCheckpointStore());

    [Fact]
    public async Task InMemoryCheckpointStoreRunsAFreezeAndResume() =>
        await StoreContracts.FreezeResumeContractAsync(new InMemoryCheckpointStore());

    [Fact]
    public async Task InMemoryCheckpointStoreSavesConditionally() =>
        await StoreContracts.ConditionalSaveContractAsync(new InMemoryCheckpointStore());

    [Fact]
    public async Task InMemoryCheckpointStoreRunsAFreezeAndEnd() =>
        await StoreContracts.FreezeEndContractAsync(new InMemoryCheckpointStore());

    [Fact]
    public async Task InMemoryCheckpointStoreRunsAFreezeAndWake() =>
        await StoreContracts.FreezeWakeContractAsync(new InMemoryCheckpointStore());

    // Leases expire on the clock the in-memory and SQLite stores are given, and on the server's in Postgres
    // and Redis, where the contract really waits (design/32-Thread-Leases.md, "The clock"): there a brief
    // lease has to outlast the few calls made while it is held, on a slow machine too.
    private static readonly TimeSpan Brief = TimeSpan.FromMilliseconds(400);
    private static readonly TimeSpan BriefOnAServer = TimeSpan.FromMilliseconds(1500);

    private static Func<TimeSpan, Task> Advancing(FakeTimeProvider clock) =>
        ttl =>
        {
            clock.Advance(ttl + TimeSpan.FromMilliseconds(1));
            return Task.CompletedTask;
        };

    private static Task Waiting(TimeSpan ttl) => Task.Delay(ttl + TimeSpan.FromMilliseconds(300));

    [Fact]
    public async Task InMemoryCheckpointStoreKeepsLeases()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await StoreContracts.LeaseContractAsync(new InMemoryCheckpointStore(clock), Brief, Advancing(clock));
    }

    [Fact]
    public async Task InMemoryCheckpointStoreRunsUnderALease() =>
        await StoreContracts.LeasedRunContractAsync(new InMemoryCheckpointStore());

    [Fact]
    public async Task InMemoryCheckpointStoreRunsDurably() =>
        await StoreContracts.DurableRunContractAsync(new InMemoryCheckpointStore());

    [Fact]
    public async Task SqliteCheckpointStoreRunsDurably()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        await StoreContracts.DurableRunContractAsync(store);
    }

    [PostgresFact]
    public async Task PostgresCheckpointStoreRunsDurably()
    {
        using var sandbox = new PostgresSandbox("durable_run");
        using var store = new PostgresCheckpointStore(sandbox.CreateDataSource());
        await StoreContracts.DurableRunContractAsync(store);
    }

    [RedisFact]
    public async Task RedisCheckpointStoreRunsDurably()
    {
        using var sandbox = new RedisSandbox("durable_run");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await StoreContracts.DurableRunContractAsync(store);
    }

    [Fact]
    public async Task SqliteCheckpointStoreKeepsLeases()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        using var store = EfCoreCheckpointStore.CreateInMemory(clock);
        await StoreContracts.LeaseContractAsync(store, Brief, Advancing(clock));
    }

    [Fact]
    public async Task SqliteCheckpointStoreRunsUnderALease()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        await StoreContracts.LeasedRunContractAsync(store);
    }

    [PostgresFact]
    public async Task PostgresCheckpointStoreKeepsLeases()
    {
        using var sandbox = new PostgresSandbox("leases");
        using var store = new PostgresCheckpointStore(sandbox.CreateDataSource());
        await StoreContracts.LeaseContractAsync(store, BriefOnAServer, Waiting);
    }

    [PostgresFact]
    public async Task PostgresCheckpointStoreRunsUnderALease()
    {
        using var sandbox = new PostgresSandbox("leased_run");
        using var store = new PostgresCheckpointStore(sandbox.CreateDataSource());
        await StoreContracts.LeasedRunContractAsync(store);
    }

    [RedisFact]
    public async Task RedisCheckpointStoreKeepsLeases()
    {
        using var sandbox = new RedisSandbox("leases");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await StoreContracts.LeaseContractAsync(store, BriefOnAServer, Waiting);
    }

    [RedisFact]
    public async Task RedisCheckpointStoreRunsUnderALease()
    {
        using var sandbox = new RedisSandbox("leased_run");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await StoreContracts.LeasedRunContractAsync(store);
    }

    [Fact]
    public void SqliteCheckpointStoreSatisfiesTheContract()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        StoreContracts.CheckpointStoreContract(store);
    }

    [Fact]
    public async Task SqliteCheckpointStoreRunsAFreezeAndResume()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        await StoreContracts.FreezeResumeContractAsync(store);
    }

    [Fact]
    public async Task SqliteCheckpointStoreSavesConditionally()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        await StoreContracts.ConditionalSaveContractAsync(store);
    }

    [Fact]
    public async Task SqliteCheckpointStoreRunsAFreezeAndEnd()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        await StoreContracts.FreezeEndContractAsync(store);
    }

    [Fact]
    public async Task SqliteCheckpointStoreRunsAFreezeAndWake()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        await StoreContracts.FreezeWakeContractAsync(store);
    }

    [PostgresFact]
    public void PostgresCheckpointStoreSatisfiesTheContract()
    {
        using var sandbox = new PostgresSandbox("checkpoint");
        using var store = new PostgresCheckpointStore(sandbox.CreateDataSource());
        StoreContracts.CheckpointStoreContract(store);
    }

    [PostgresFact]
    public async Task PostgresCheckpointStoreRunsAFreezeAndResume()
    {
        using var sandbox = new PostgresSandbox("freeze");
        using var store = new PostgresCheckpointStore(sandbox.CreateDataSource());
        await StoreContracts.FreezeResumeContractAsync(store);
    }

    [PostgresFact]
    public async Task PostgresCheckpointStoreSavesConditionally()
    {
        using var sandbox = new PostgresSandbox("conditional");
        using var store = new PostgresCheckpointStore(sandbox.CreateDataSource());
        await StoreContracts.ConditionalSaveContractAsync(store);
    }

    [PostgresFact]
    public async Task PostgresCheckpointStoreRunsAFreezeAndEnd()
    {
        using var sandbox = new PostgresSandbox("end");
        using var store = new PostgresCheckpointStore(sandbox.CreateDataSource());
        await StoreContracts.FreezeEndContractAsync(store);
    }

    [PostgresFact]
    public async Task PostgresCheckpointStoreRunsAFreezeAndWake()
    {
        using var sandbox = new PostgresSandbox("wake");
        using var store = new PostgresCheckpointStore(sandbox.CreateDataSource());
        await StoreContracts.FreezeWakeContractAsync(store);
    }

    [RedisFact]
    public void RedisCheckpointStoreSatisfiesTheContract()
    {
        using var sandbox = new RedisSandbox("checkpoint");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        StoreContracts.CheckpointStoreContract(store);
    }

    [RedisFact]
    public async Task RedisCheckpointStoreRunsAFreezeAndResume()
    {
        using var sandbox = new RedisSandbox("freeze");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await StoreContracts.FreezeResumeContractAsync(store);
    }

    [RedisFact]
    public async Task RedisCheckpointStoreSavesConditionally()
    {
        using var sandbox = new RedisSandbox("conditional");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await StoreContracts.ConditionalSaveContractAsync(store);
    }

    [RedisFact]
    public async Task RedisCheckpointStoreRunsAFreezeAndEnd()
    {
        using var sandbox = new RedisSandbox("end");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await StoreContracts.FreezeEndContractAsync(store);
    }

    [RedisFact]
    public async Task RedisCheckpointStoreRunsAFreezeAndWake()
    {
        using var sandbox = new RedisSandbox("wake");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await StoreContracts.FreezeWakeContractAsync(store);
    }

    // --- History stores ---

    [Fact]
    public async Task InMemoryHistoryStoreSatisfiesTheContract()
    {
        await StoreContracts.HistoryStoreContractAsync(new InMemoryHistoryStore());
        await StoreContracts.HistoryRoundTripContractAsync(new InMemoryHistoryStore());
    }

    [PostgresFact]
    public async Task PostgresHistoryStoreSatisfiesTheContract()
    {
        using var sandbox = new PostgresSandbox("history");
        using var store = new PostgresHistoryStore(sandbox.CreateDataSource());
        await StoreContracts.HistoryStoreContractAsync(store);
    }

    [PostgresFact]
    public async Task PostgresHistoryStoreRoundTripsEveryRoleAndAwkwardContent()
    {
        using var sandbox = new PostgresSandbox("history_round_trip");
        using var store = new PostgresHistoryStore(sandbox.CreateDataSource());
        await StoreContracts.HistoryRoundTripContractAsync(store);
    }

    [RedisFact]
    public async Task RedisHistoryStoreSatisfiesTheContract()
    {
        using var sandbox = new RedisSandbox("history");
        var store = new RedisHistoryStore(sandbox.Database, sandbox.Prefix);
        await StoreContracts.HistoryStoreContractAsync(store);
    }

    [RedisFact]
    public async Task RedisHistoryStoreRoundTripsEveryRoleAndAwkwardContent()
    {
        using var sandbox = new RedisSandbox("history_round_trip");
        var store = new RedisHistoryStore(sandbox.Database, sandbox.Prefix);
        await StoreContracts.HistoryRoundTripContractAsync(store);
    }
}
