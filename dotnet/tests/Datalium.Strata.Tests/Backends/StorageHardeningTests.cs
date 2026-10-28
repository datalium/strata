using System.Diagnostics.Metrics;
using System.Text.Json.Nodes;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Storage.Redis;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Time.Testing;
using Npgsql;
using StackExchange.Redis;
using static Datalium.Strata.Tests.StateGraphInterruptionTests;

// The Redis exceptions the tests raise are made with StackExchange.Redis's short constructors, which it
// marks obsolete in favour of ones that take command flags the stores never look at.
#pragma warning disable CS0618

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// Storage hardening (<c>design/34-Storage-Hardening.md</c>, SG-30): retries after transient failures, a save
/// that may have happened, the schema-version table, and retention. Mirrors <c>strata-memory</c>'s
/// <c>retry.rs</c> and <c>tests/storage_hardening.rs</c>. The Postgres and Redis tests need
/// <c>STRATA_TEST_POSTGRES_URL</c> / <c>STRATA_TEST_REDIS_URL</c>.
/// </summary>
public class StorageHardeningTests
{
    private static readonly CompilationOptions Durable = new() { Checkpointing = CheckpointPolicy.EveryNode };

    private static JsonNode Fixture(string name) =>
        CheckpointQueryContractTests.Fixture(Path.Combine("checkpoint-log", name));

    private static void AssertJson(JsonNode? expected, JsonNode? actual) =>
        Assert.True(JsonNode.DeepEquals(expected, actual), $"expected {expected?.ToJsonString()}, got {actual?.ToJsonString()}");

    /// <summary>What a connection lost under a call looks like to the Postgres stores.</summary>
    private static NpgsqlException Lost() => new("the connection was lost", new IOException("reset by peer"));

    /// <summary>Throws <paramref name="failure"/>'s exception the first <paramref name="times"/> times <paramref name="point"/> is reached.</summary>
    private static Func<string, ValueTask> FaultAt(string point, Func<Exception> failure, int times = 1)
    {
        var left = times;
        return reached =>
            reached == point && Interlocked.Decrement(ref left) >= 0 ? throw failure() : ValueTask.CompletedTask;
    }

    private static CompiledGraph<WireTransferState> DraftThenSend(Func<Task> whileDrafting) =>
        new StateGraph<WireTransferState>()
            .AddNode("Draft", async (WireTransferState s, NodeContext _, CancellationToken _) =>
            {
                await whileDrafting();
                return s with { InvoiceGenerated = true };
            })
            .AddNode("Send", s => s with { IsCompleted = true })
            .AddEdge("Draft", "Send")
            .Compile(Durable);

    private static async Task<List<CheckpointStatus>> Statuses(ICheckpointQuery store, string threadId) =>
        [.. (await store.GetLogAsync(threadId)).Entries.Select(e => e.Checkpoint.Status)];

    /// <summary>
    /// Counts <c>strata.store.retries</c> while it lives, recorded from the test's own flow only: the listener is
    /// process-wide, and another test's store retrying at the same moment would otherwise be counted too.
    /// </summary>
    private sealed class RetryCounter : IDisposable
    {
        // Set by the constructor (synchronous, so it stays set in the test's flow) and seen by every store call the test
        // awaits; a measurement recorded on any other flow is someone else's.
        private static readonly AsyncLocal<RetryCounter?> Current = new();

        private readonly MeterListener _listener = new();
        private long _count;

        public RetryCounter()
        {
            Current.Value = this;
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == StorageTelemetry.Name && instrument.Name == "strata.store.retries")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                if (Current.Value != this)
                {
                    return;
                }

                Interlocked.Add(ref _count, measurement);
                var named = tags.ToArray().ToDictionary(tag => tag.Key, tag => tag.Value?.ToString());
                Last = (named.GetValueOrDefault("strata.store"), named.GetValueOrDefault("strata.store.operation"));
            });
            _listener.Start();
        }

        public long Count => Interlocked.Read(ref _count);

        /// <summary>The store and the operation of the last repeat counted.</summary>
        public (string? Store, string? Operation) Last { get; private set; }

        public void Dispose()
        {
            _listener.Dispose();
            if (Current.Value == this)
            {
                Current.Value = null;
            }
        }
    }

    // --- The policy ---

    [Fact]
    public void TheWaitDoublesIsCappedAndIsJitteredOverAllOfIt()
    {
        var policy = new StoreRetryPolicy { InitialDelay = TimeSpan.FromMilliseconds(100), MaxDelay = TimeSpan.FromMilliseconds(350) };

        Assert.Equal([100, 200, 350, 350], Enumerable.Range(1, 4).Select(repeat => policy.DelayBefore(repeat, 1.0).TotalMilliseconds));
        Assert.Equal(TimeSpan.Zero, policy.DelayBefore(3, 0.0));
        Assert.Equal(TimeSpan.FromMilliseconds(100), policy.DelayBefore(2, 0.5));
        Assert.Equal(1, StoreRetryPolicy.None.MaxAttempts);
        Assert.Equal(4, new StoreRetryPolicy().MaxAttempts);
    }

    [Fact]
    public async Task ATransientFailureIsRepeatedUntilTheAttemptsRunOutAndAnyOtherIsNot()
    {
        var clock = new FakeTimeProvider();
        var retrier = new StoreRetrier(
            new StoreRetryPolicy { MaxAttempts = 3, InitialDelay = TimeSpan.Zero }, clock, "test", ex => ex is IOException);
        using var retries = new RetryCounter();

        // It comes back on the third attempt.
        var attempts = 0;
        Assert.Equal(3, await retrier.RunAsync("load", (number, _) =>
            number < 3 ? throw new IOException("down") : new ValueTask<int>(++attempts + 2), CancellationToken.None));
        Assert.True(retries.Count >= 2);

        // It never comes back: the last failure is the call's.
        var made = 0;
        await Assert.ThrowsAsync<IOException>(async () => await retrier.RunAsync<int>("load", (_, _) =>
        {
            made++;
            throw new IOException("down");
        }, CancellationToken.None));
        Assert.Equal(3, made);

        // Not transient: made once.
        made = 0;
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await retrier.RunAsync<int>("load", (_, _) =>
        {
            made++;
            throw new InvalidOperationException("a bug");
        }, CancellationToken.None));
        Assert.Equal(1, made);

        // What an attempt can't safely do twice isn't repeated either.
        made = 0;
        await Assert.ThrowsAsync<IOException>(async () => await retrier.RunAsync<int>(
            "lease",
            (_, _) =>
            {
                made++;
                throw new IOException("down");
            },
            CancellationToken.None,
            unless: _ => true));
        Assert.Equal(1, made);
    }

    [Fact]
    public async Task ACallersTokenStopsTheWaitBetweenAttempts()
    {
        var clock = new FakeTimeProvider();
        var retrier = new StoreRetrier(
            new StoreRetryPolicy { InitialDelay = TimeSpan.FromHours(1), MaxDelay = TimeSpan.FromHours(1) }, clock, "test", _ => true);
        using var cancellation = new CancellationTokenSource();
        var made = 0;

        var call = retrier.RunAsync<int>("load", (_, _) =>
        {
            made++;
            cancellation.CancelAfter(TimeSpan.FromMilliseconds(20));
            throw new IOException("down");
        }, cancellation.Token).AsTask();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(1, made);
    }

    [Fact]
    public void EachStoreKnowsItsTransientFailures()
    {
        static PostgresException State(string sqlState) => new("failed", "FATAL", "FATAL", sqlState);

        foreach (var transient in (string[])["08006", "08001", "57P01", "57P02", "57P03", "40001", "40P01", "53300"])
        {
            Assert.True(PostgresFaults.IsTransient(State(transient)), transient);
        }

        foreach (var final in (string[])["23505", "42P01", "22001", "28P01"])
        {
            Assert.False(PostgresFaults.IsTransient(State(final)), final);
        }

        Assert.True(PostgresFaults.IsTransient(Lost()));
        Assert.True(PostgresFaults.IsTransient(new NpgsqlException("timed out", new TimeoutException())));
        Assert.False(PostgresFaults.IsTransient(new NpgsqlException("a protocol error")));
        Assert.False(PostgresFaults.IsTransient(new InvalidOperationException("a bug")));

        Assert.True(RedisFaults.IsTransient(new RedisConnectionException(ConnectionFailureType.SocketFailure, "gone")));
        Assert.True(RedisFaults.IsTransient(new RedisTimeoutException("slow", CommandStatus.Sent)));
        Assert.True(RedisFaults.IsTransient(new RedisServerException("LOADING Redis is loading the dataset in memory")));
        Assert.True(RedisFaults.IsTransient(new RedisServerException("BUSY Redis is busy running a script")));
        Assert.False(RedisFaults.IsTransient(new RedisServerException("WRONGTYPE Operation against a key holding the wrong kind of value")));
        Assert.False(RedisFaults.IsTransient(new InvalidOperationException("a bug")));
    }

    [Fact]
    public void ALookAtTheLogSaysWhatBecameOfASave()
    {
        var saved = StoreContracts.Checkpoint("t", "2026-09-16T00:00:00Z");
        var reordered = Checkpoint.FromWireJson(JsonNode.Parse(saved.ToWireJson().ToJsonString())!);
        var other = saved with { CreatedAt = "2026-09-17T00:00:00Z" };

        // The checkpoint is where the save would have put it: it happened.
        Assert.Equal(SaveFate.Saved, StoredCheckpoints.FateOf(saved, reordered, conditional: true));
        Assert.Equal(SaveFate.Saved, StoredCheckpoints.FateOf(saved, reordered, conditional: false));

        // Another checkpoint took a conditional save's place: it lost. A plain save has no place to lose.
        Assert.Equal(SaveFate.Lost, StoredCheckpoints.FateOf(saved, other, conditional: true));
        Assert.Equal(SaveFate.NotWritten, StoredCheckpoints.FateOf(saved, other, conditional: false));

        // Nothing there: nothing was written.
        Assert.Equal(SaveFate.NotWritten, StoredCheckpoints.FateOf(saved, null, conditional: true));
        Assert.Equal(SaveFate.NotWritten, StoredCheckpoints.FateOf(saved, null, conditional: false));
    }

    // --- Postgres: a dropped connection ---

    /// <summary>Terminates every backend of <paramref name="applicationName"/>'s pool; returns how many.</summary>
    private static async Task<int> Terminate(NpgsqlDataSource admin, string applicationName)
    {
        await using var command = admin.CreateCommand(
            "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1) t");
        command.Parameters.Add(new NpgsqlParameter { Value = applicationName });
        return (int)(long)(await command.ExecuteScalarAsync())!;
    }

    [PostgresFact]
    public async Task ADroppedPostgresConnectionDoesNotFailARun()
    {
        using var sandbox = new PostgresSandbox("dropped");
        var name = $"strata-drop-{Guid.NewGuid():N}";
        var admin = sandbox.CreateDataSource();
        using var store = new PostgresCheckpointStore(sandbox.CreateDataSource(name));
        using var retries = new RetryCounter();
        var terminated = 0;
        var graph = DraftThenSend(async () => terminated = await Terminate(admin, name));

        var result = await graph.RunAsync("t", InitialState(), store);

        // Every connection the store had was terminated between two saves, and the run went on.
        Assert.True(terminated > 0, "the store's connections were terminated");
        Assert.True(Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(result).State.IsCompleted);
        Assert.Equal(
            [CheckpointStatus.Running, CheckpointStatus.Running, CheckpointStatus.Completed], await Statuses(store, "t"));
        Assert.True(retries.Count >= 1, "the save after the drop was repeated");

        // A load and a listing ride through one too.
        Assert.True(await Terminate(admin, name) > 0);
        Assert.Equal(CheckpointStatus.Completed, (await store.LoadAsync("t"))?.Status);
        Assert.True(await Terminate(admin, name) > 0);
        Assert.Equal(["t"], (await store.ListThreadsAsync(new ThreadQuery())).Threads.Select(t => t.ThreadId));
    }

    [PostgresFact]
    public async Task WithoutRetriesADroppedConnectionIsTheCallsFailure()
    {
        using var sandbox = new PostgresSandbox("dropped_none");
        var name = $"strata-drop-{Guid.NewGuid():N}";
        var admin = sandbox.CreateDataSource();
        using var store = new PostgresCheckpointStore(
            sandbox.CreateDataSource(name), new PostgresStoreOptions { Retry = StoreRetryPolicy.None });
        await store.SaveAsync(StoreContracts.Checkpoint("t", "2026-09-16T00:00:00Z"));

        Assert.True(await Terminate(admin, name) > 0);

        await Assert.ThrowsAnyAsync<NpgsqlException>(async () => await store.LoadAsync("t"));
    }

    // --- Postgres: a save that may have happened ---

    [PostgresFact]
    public async Task ASaveWhoseCommitMayHaveFailedIsNotWrittenTwice()
    {
        using var sandbox = new PostgresSandbox("ambiguous");
        using var store = new PostgresCheckpointStore(
            sandbox.CreateDataSource(),
            new PostgresStoreOptions { Fault = FaultAt("save:after-commit", Lost, times: 3) });
        var first = StoreContracts.Checkpoint("t", "2026-09-16T00:00:00Z");
        var second = StoreContracts.Checkpoint("t", "2026-09-17T00:00:00Z");

        // The commit happened and the answer was lost: the store looks, finds the checkpoint, and says so.
        await store.SaveAsync(first);
        Assert.Single((await store.GetLogAsync("t")).Entries);

        // A conditional save keeps its answer.
        Assert.True(await store.SaveIfLatestAsync(second, expectedSequence: 0));
        Assert.Equal(2, (await store.GetLogAsync("t")).Entries.Count);

        // So does one under a lease, and the lease is still its owner's.
        var lease = (await store.TryAcquireLeaseAsync("t", "worker-1", TimeSpan.FromMinutes(1))).Lease!;
        var third = StoreContracts.Checkpoint("t", "2026-09-18T00:00:00Z");
        Assert.True(await store.SaveUnderLeaseAsync(third, lease, expectedSequence: 1));
        Assert.Equal(3, (await store.GetLogAsync("t")).Entries.Count);
        Assert.Equal(lease.Token, (await store.GetLeaseAsync("t"))?.Token);
        StoreContracts.AssertSame(third, await store.LoadAsync("t"));
    }

    [PostgresFact]
    public async Task APostgresSaveInDoubtIsMadeAgainIfItWasNotWrittenAndLostIfAnotherTookItsPlace()
    {
        using var sandbox = new PostgresSandbox("in_doubt");
        var dataSource = sandbox.CreateDataSource();

        // The commit was sent and never happened. The store looks, finds nothing, and saves again: once.
        using var store = new PostgresCheckpointStore(
            dataSource, new PostgresStoreOptions { Fault = FaultAt("save:commit", Lost) });
        using (var retries = new RetryCounter())
        {
            await store.SaveAsync(StoreContracts.Checkpoint("t", "2026-09-16T00:00:00Z"));
            Assert.Single((await store.GetLogAsync("t")).Entries);
            Assert.Equal(1, retries.Count);
        }

        // The same for a conditional save, except that before the store looks, another writer takes its place:
        // it lost the race, and says so.
        using var other = new PostgresCheckpointStore(dataSource);
        var commit = FaultAt("save:commit", Lost);
        using var raced = new PostgresCheckpointStore(dataSource, new PostgresStoreOptions
        {
            Fault = async point =>
            {
                if (point == "save:recheck")
                {
                    await other.SaveAsync(StoreContracts.Checkpoint("t", "2026-09-18T00:00:00Z"));
                }

                await commit(point);
            },
        });

        Assert.False(await raced.SaveIfLatestAsync(StoreContracts.Checkpoint("t", "2026-09-17T00:00:00Z"), expectedSequence: 0));

        Assert.Equal(
            ["2026-09-16T00:00:00Z", "2026-09-18T00:00:00Z"],
            (await store.GetLogAsync("t")).Entries.Select(e => e.Checkpoint.CreatedAt));
    }

    [PostgresFact]
    public async Task WithoutRetriesATransientFailureIsTheCalls()
    {
        using var sandbox = new PostgresSandbox("no_retries");
        using var store = new PostgresCheckpointStore(
            sandbox.CreateDataSource(),
            new PostgresStoreOptions { Retry = StoreRetryPolicy.None, Fault = FaultAt("save:before-commit", Lost) });
        using var retries = new RetryCounter();

        await Assert.ThrowsAsync<NpgsqlException>(async () =>
            await store.SaveAsync(StoreContracts.Checkpoint("t", "2026-09-16T00:00:00Z")));

        Assert.Empty((await store.GetLogAsync("t")).Entries);
        Assert.Equal(0, retries.Count);
    }

    [PostgresFact]
    public async Task ASaveThatFailedBeforeItsCommitIsRepeatedAndWritesOnce()
    {
        using var sandbox = new PostgresSandbox("before_commit");
        using var store = new PostgresCheckpointStore(
            sandbox.CreateDataSource(),
            new PostgresStoreOptions { Fault = FaultAt("save:before-commit", Lost, times: 2) });
        using var retries = new RetryCounter();

        await store.SaveAsync(StoreContracts.Checkpoint("t", "2026-09-16T00:00:00Z"));

        Assert.Single((await store.GetLogAsync("t")).Entries);
        Assert.Equal(2, retries.Count);
        Assert.Equal(("postgres", "save"), retries.Last);
    }

    [PostgresFact]
    public async Task AnAcquisitionOrAnAppendThatMayHaveHappenedIsNotRepeated()
    {
        using var sandbox = new PostgresSandbox("not_repeated");
        var dataSource = sandbox.CreateDataSource();
        using var store = new PostgresCheckpointStore(
            dataSource, new PostgresStoreOptions { Fault = FaultAt("lease:after-commit", Lost) });
        using var history = new PostgresHistoryStore(
            dataSource, new PostgresStoreOptions { Fault = FaultAt("append:after-commit", Lost) });

        // The store can't tell its own lease from another's with the same name: the failure is the call's, and
        // the lease that was taken runs out by itself.
        await Assert.ThrowsAsync<NpgsqlException>(async () =>
            await store.TryAcquireLeaseAsync("t", "worker-1", TimeSpan.FromMinutes(1)));
        Assert.Equal(("worker-1", 1L), ((await store.GetLeaseAsync("t"))?.Owner, (await store.GetLeaseAsync("t"))?.Token));

        // A message can't be told from another like it either.
        var message = new ChatMessage(MessageRole.User, "hello".AsMemory(), new DateTimeOffset(2026, 9, 16, 0, 0, 0, TimeSpan.Zero));
        await Assert.ThrowsAsync<NpgsqlException>(() => history.AppendAsync("s", message));
        var stored = new List<ChatMessage>();
        await foreach (var read in history.GetHistoryAsync("s"))
        {
            stored.Add(read);
        }

        Assert.Single(stored);
    }

    // --- Migrations ---

    private static async Task<JsonNode> PostgresVersions(NpgsqlDataSource dataSource)
    {
        var versions = new JsonObject();
        await using var command = dataSource.CreateCommand("SELECT component, version FROM strata_schema_version ORDER BY component");
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            versions[reader.GetString(0)] = reader.GetInt64(1);
        }

        return versions;
    }

    [PostgresFact]
    public async Task AFreshPostgresDatabaseIsAtTheSharedVersions()
    {
        using var sandbox = new PostgresSandbox("versions");
        var dataSource = sandbox.CreateDataSource();
        await using (await PostgresCheckpointStoreOver(dataSource))
        {
        }

        using (PostgresSandbox.OpenHistory(dataSource))
        {
        }

        AssertJson(Fixture("schema-versions.json")["components"], await PostgresVersions(dataSource));
        AssertJson(
            Fixture("postgres-schema-versions-columns.json"),
            PostgresStoreTests.Columns(dataSource, StoreSchemaVersions.Table));
    }

    private static async Task<PostgresCheckpointStore> PostgresCheckpointStoreOver(
        NpgsqlDataSource dataSource, PostgresStoreOptions? options = null)
    {
        var store = new PostgresCheckpointStore(dataSource, options);
        await store.MigrateAsync();
        return store;
    }

    [PostgresFact]
    public async Task APostgresDatabaseFromBeforeTheVersionTableIsBroughtToVersionOne()
    {
        using var sandbox = new PostgresSandbox("baseline");
        var dataSource = sandbox.CreateDataSource();
        await using (var old = await PostgresCheckpointStoreOver(dataSource))
        {
            await old.SaveAsync(StoreContracts.Checkpoint("t-old", "2026-09-17T00:00:00Z"));
        }

        // As a runtime from before leases and the version table left it.
        await using (var drop = dataSource.CreateCommand("DROP TABLE checkpoint_leases, strata_schema_version"))
        {
            await drop.ExecuteNonQueryAsync();
        }

        await using var store = await PostgresCheckpointStoreOver(dataSource);

        Assert.Equal(StoreSchemaVersions.CheckpointsVersion, (await PostgresVersions(dataSource))["checkpoints"]!.GetValue<long>());
        Assert.NotNull(await store.LoadAsync("t-old"));
        Assert.Equal(1, (await store.TryAcquireLeaseAsync("t-old", "worker-1", TimeSpan.FromMinutes(1))).Lease?.Token);
    }

    [PostgresFact]
    public async Task ANewerPostgresDatabaseIsRefusedAndLeftAlone()
    {
        using var sandbox = new PostgresSandbox("newer");
        var dataSource = sandbox.CreateDataSource();
        await using (await PostgresCheckpointStoreOver(dataSource))
        {
        }

        await using (var bump = dataSource.CreateCommand("UPDATE strata_schema_version SET version = 3 WHERE component = 'checkpoints'"))
        {
            await bump.ExecuteNonQueryAsync();
        }

        using var store = new PostgresCheckpointStore(dataSource);
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.LoadAsync("t"));

        Assert.Equal("database is at checkpoints schema version 3, and this build knows 2: upgrade the runtime", refused.Message);
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.MigrateAsync());
        Assert.Equal(3L, (await PostgresVersions(dataSource))["checkpoints"]!.GetValue<long>());
    }

    [PostgresFact]
    public async Task AStoreThatDoesNotMigrateRefusesADatabaseThatIsBehind()
    {
        using var sandbox = new PostgresSandbox("manual");
        var dataSource = sandbox.CreateDataSource();
        using var store = new PostgresCheckpointStore(dataSource, new PostgresStoreOptions { AutoMigrate = false });

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.LoadAsync("t"));
        Assert.Equal("database is at checkpoints schema version 0, and this build needs 2: run its migration", refused.Message);

        // It changed nothing: not even the version table is there.
        await using (var probe = dataSource.CreateCommand("SELECT to_regclass('strata_schema_version') IS NULL AND to_regclass('checkpoints') IS NULL"))
        {
            Assert.True((bool)(await probe.ExecuteScalarAsync())!);
        }

        // A deploy step migrates, and the store then works.
        await store.MigrateAsync();
        Assert.Null(await store.LoadAsync("t"));
        using var again = new PostgresCheckpointStore(dataSource, new PostgresStoreOptions { AutoMigrate = false });
        Assert.Null(await again.LoadAsync("t"));
    }

    private static readonly SchemaStep[] LaterSteps =
    [
        new(3, "probe", ["CREATE TABLE strata_probe (step BIGINT NOT NULL)", "INSERT INTO strata_probe VALUES (3)"]),
        new(4, "probe again", ["INSERT INTO strata_probe VALUES (4)"]),
    ];

    [PostgresFact]
    public async Task LaterStepsRunOnceInOrderInPostgres()
    {
        using var sandbox = new PostgresSandbox("steps");
        var dataSource = sandbox.CreateDataSource();
        await using (var old = await PostgresCheckpointStoreOver(dataSource))
        {
            await old.SaveAsync(StoreContracts.Checkpoint("t", "2026-09-17T00:00:00Z"));
        }

        // A later build knows two more steps; opening twice runs each once.
        var later = new PostgresStoreOptions { LaterSteps = LaterSteps };
        for (var open = 0; open < 2; open++)
        {
            await using var store = await PostgresCheckpointStoreOver(dataSource, later);
            Assert.NotNull(await store.LoadAsync("t"));
        }

        Assert.Equal(4L, (await PostgresVersions(dataSource))["checkpoints"]!.GetValue<long>());
        await using var probe = dataSource.CreateCommand("SELECT string_agg(step::text, ',' ORDER BY step) FROM strata_probe");
        Assert.Equal("3,4", (string)(await probe.ExecuteScalarAsync())!);

        // And this build, which knows neither, refuses what the later one left.
        using var older = new PostgresCheckpointStore(dataSource);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await older.LoadAsync("t"));
    }

    [Fact]
    public void ASqliteFileHasTheVersionTableAndRunsLaterStepsOnce()
    {
        var path = Path.Combine(Path.GetTempPath(), $"strata_versions_{Guid.NewGuid():N}.sqlite");
        try
        {
            EfCoreCheckpointStore Open(IReadOnlyList<SchemaStep>? laterSteps = null)
            {
                var options = new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={path};Pooling=False").Options;
                return laterSteps is null
                    ? new EfCoreCheckpointStore(new CheckpointDbContext(options))
                    : new EfCoreCheckpointStore(new CheckpointDbContext(options), ownedConnection: null, timeProvider: null, laterSteps);
            }

            T Scalar<T>(string sql)
            {
                using var connection = new SqliteConnection($"Data Source={path};Pooling=False");
                connection.Open();
                using var command = connection.CreateCommand();
                command.CommandText = sql;
                return (T)command.ExecuteScalar()!;
            }

            using (var store = Open())
            {
                store.Save(StoreContracts.Checkpoint("t", "2026-09-17T00:00:00Z"));
            }

            Assert.Equal(StoreSchemaVersions.CheckpointsVersion, Scalar<long>("SELECT version FROM strata_schema_version WHERE component = 'checkpoints'"));
            var columns = new JsonArray();
            using (var connection = new SqliteConnection($"Data Source={path};Pooling=False"))
            {
                connection.Open();
                using var command = connection.CreateCommand();
                command.CommandText = "PRAGMA table_info(strata_schema_version)";
                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    columns.Add(new JsonObject
                    {
                        ["name"] = reader.GetString(1),
                        ["type"] = reader.GetString(2),
                        ["notnull"] = reader.GetInt64(3) != 0,
                        ["pk"] = reader.GetInt64(5),
                    });
                }
            }

            AssertJson(Fixture("schema-versions-columns.json"), columns);

            var later = LaterSteps.Select(step => step with { Statements = [.. step.Statements.Select(s => s.Replace("BIGINT", "INTEGER"))] }).ToList();
            for (var open = 0; open < 2; open++)
            {
                using var store = Open(later);
                Assert.NotNull(store.Load("t"));
            }

            Assert.Equal(4L, Scalar<long>("SELECT version FROM strata_schema_version WHERE component = 'checkpoints'"));
            Assert.Equal("3,4", Scalar<string>("SELECT group_concat(step, ',') FROM (SELECT step FROM strata_probe ORDER BY step)"));

            // This build knows neither step, and refuses the file.
            var refused = Assert.Throws<InvalidOperationException>(() => Open());
            Assert.Equal("database is at checkpoints schema version 4, and this build knows 2: upgrade the runtime", refused.Message);
        }
        finally
        {
            File.Delete(path);
        }
    }

    // --- Retention ---

    [Fact]
    public async Task TheInMemoryStorePurgesFinishedThreads() =>
        await StoreContracts.PurgeContractAsync(new InMemoryCheckpointStore());

    [Fact]
    public async Task TheSqliteStorePurgesFinishedThreads()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        await StoreContracts.PurgeContractAsync(store);
    }

    [PostgresFact]
    public async Task ThePostgresStorePurgesFinishedThreads()
    {
        using var sandbox = new PostgresSandbox("purge");
        using var store = new PostgresCheckpointStore(sandbox.CreateDataSource());
        await StoreContracts.PurgeContractAsync(store);
    }

    [RedisFact]
    public async Task TheRedisStorePurgesFinishedThreads()
    {
        using var sandbox = new RedisSandbox("purge");
        await StoreContracts.PurgeContractAsync(new RedisCheckpointStore(sandbox.Database, sandbox.Prefix));

        // Nothing of the purged threads is left under the prefix: no list, no token counter, no index entry.
        Assert.Null(await sandbox.Database.SortedSetScoreAsync($"{sandbox.Prefix}:threads", "old-ended"));
        Assert.False(await sandbox.Database.KeyExistsAsync($"{sandbox.Prefix}:checkpoints:old-ended"));
        Assert.Null(await sandbox.Database.SortedSetScoreAsync($"{sandbox.Prefix}:threads:ended", "old-ended"));
    }

    // --- Redis ---

    private static Checkpoint Finished(string threadId, CheckpointStatus status, string createdAt) =>
        StoreContracts.Checkpoint(threadId, createdAt) with { Status = status };

    [RedisFact]
    public async Task AFinishedRedisThreadExpiresAndOneThatRunsAgainDoesNot()
    {
        using var sandbox = new RedisSandbox("ttl");
        var store = new RedisCheckpointStore(
            sandbox.Database, new RedisStoreOptions { Prefix = sandbox.Prefix, FinishedThreadTtl = TimeSpan.FromHours(1) });
        RedisKey list = $"{sandbox.Prefix}:checkpoints:t";

        await store.SaveAsync(StoreContracts.Checkpoint("t", "2026-09-16T00:00:00Z"));
        Assert.Null(await sandbox.Database.KeyTimeToLiveAsync(list));

        // Completed: it has an hour.
        Assert.True(await store.SaveIfLatestAsync(Finished("t", CheckpointStatus.Completed, "2026-09-16T00:01:00Z"), 0));
        Assert.InRange((await sandbox.Database.KeyTimeToLiveAsync(list))!.Value, TimeSpan.FromMinutes(59), TimeSpan.FromHours(1));

        // The id runs again: it doesn't expire under the run.
        await store.SaveAsync(StoreContracts.Checkpoint("t", "2026-09-16T00:02:00Z"));
        Assert.Null(await sandbox.Database.KeyTimeToLiveAsync(list));

        // Ended, under a lease: the token counter goes with the list.
        var lease = (await store.TryAcquireLeaseAsync("t", "worker-1", TimeSpan.FromMinutes(1))).Lease!;
        Assert.True(await store.SaveUnderLeaseAsync(Finished("t", CheckpointStatus.Ended, "2026-09-16T00:03:00Z"), lease));
        Assert.NotNull(await sandbox.Database.KeyTimeToLiveAsync(list));
        Assert.NotNull(await sandbox.Database.KeyTimeToLiveAsync($"{sandbox.Prefix}:lease-tokens:t"));

        // A store without the option never sets one.
        var plain = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await plain.SaveAsync(Finished("u", CheckpointStatus.Completed, "2026-09-16T00:00:00Z"));
        Assert.Null(await sandbox.Database.KeyTimeToLiveAsync($"{sandbox.Prefix}:checkpoints:u"));
    }

    [RedisFact]
    public async Task AListingOrARebuildDropsAThreadWhoseListIsGone()
    {
        using var sandbox = new RedisSandbox("expired");
        var store = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await store.SaveAsync(Finished("gone", CheckpointStatus.Completed, "2026-09-16T00:00:00Z"));
        await store.SaveAsync(Finished("gone-too", CheckpointStatus.Completed, "2026-09-16T00:00:00Z"));
        await store.SaveAsync(StoreContracts.Checkpoint("here", "2026-09-17T00:00:00Z"));

        // As an expiry leaves it: the list is gone, its index entries are not.
        await sandbox.Database.KeyDeleteAsync($"{sandbox.Prefix}:checkpoints:gone");
        Assert.NotNull(await sandbox.Database.SortedSetScoreAsync($"{sandbox.Prefix}:threads:completed", "gone"));

        var completed = new ThreadQuery { States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Completed } };
        Assert.Equal(["gone-too"], (await store.ListThreadsAsync(completed)).Threads.Select(t => t.ThreadId));
        Assert.Null(await sandbox.Database.SortedSetScoreAsync($"{sandbox.Prefix}:threads", "gone"));
        Assert.Null(await sandbox.Database.SortedSetScoreAsync($"{sandbox.Prefix}:threads:completed", "gone"));

        // A rebuild finds the ones no listing has come across.
        await sandbox.Database.KeyDeleteAsync($"{sandbox.Prefix}:checkpoints:gone-too");
        Assert.Equal(1, await store.RebuildIndexAsync());
        Assert.Null(await sandbox.Database.SortedSetScoreAsync($"{sandbox.Prefix}:threads", "gone-too"));
        Assert.Null(await sandbox.Database.SortedSetScoreAsync($"{sandbox.Prefix}:threads:completed", "gone-too"));
        Assert.Equal(["here"], (await store.ListThreadsAsync(new ThreadQuery())).Threads.Select(t => t.ThreadId));
    }

    [RedisFact]
    public async Task ARedisSaveInDoubtIsMadeAgainIfItDidNotRunAndLostIfAnotherTookItsPlace()
    {
        using var sandbox = new RedisSandbox("in_doubt");
        static RedisConnectionException Gone() => new(ConnectionFailureType.SocketFailure, "gone");

        // The script was sent and never ran. The store looks, finds nothing, and saves again: once.
        var store = new RedisCheckpointStore(
            sandbox.Database, new RedisStoreOptions { Prefix = sandbox.Prefix, Fault = FaultAt("save:before", Gone) });
        using (var retries = new RetryCounter())
        {
            await store.SaveAsync(StoreContracts.Checkpoint("t", "2026-09-16T00:00:00Z"));
            Assert.Single((await store.GetLogAsync("t")).Entries);
            Assert.Equal(1, retries.Count);
        }

        // The same for a conditional save, except that before the store looks, another writer takes its place:
        // it lost the race, and says so.
        var other = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        var before = FaultAt("save:before", Gone);
        store = new RedisCheckpointStore(sandbox.Database, new RedisStoreOptions
        {
            Prefix = sandbox.Prefix,
            Fault = async point =>
            {
                if (point == "save:recheck")
                {
                    await other.SaveAsync(StoreContracts.Checkpoint("t", "2026-09-18T00:00:00Z"));
                }

                await before(point);
            },
        });

        Assert.False(await store.SaveIfLatestAsync(StoreContracts.Checkpoint("t", "2026-09-17T00:00:00Z"), 0));

        Assert.Equal(
            ["2026-09-16T00:00:00Z", "2026-09-18T00:00:00Z"],
            (await store.GetLogAsync("t")).Entries.Select(e => e.Checkpoint.CreatedAt));
    }

    [RedisFact]
    public async Task ARedisSaveThatMayHaveRunIsNotWrittenTwice()
    {
        using var sandbox = new RedisSandbox("ambiguous");
        var store = new RedisCheckpointStore(sandbox.Database, new RedisStoreOptions
        {
            Prefix = sandbox.Prefix,
            Fault = FaultAt("save:after", () => new RedisConnectionException(ConnectionFailureType.SocketFailure, "gone"), times: 3),
        });
        using var retries = new RetryCounter();

        await store.SaveAsync(StoreContracts.Checkpoint("t", "2026-09-16T00:00:00Z"));
        Assert.True(await store.SaveIfLatestAsync(StoreContracts.Checkpoint("t", "2026-09-17T00:00:00Z"), 0));
        var lease = (await store.TryAcquireLeaseAsync("t", "worker-1", TimeSpan.FromMinutes(1))).Lease!;
        Assert.True(await store.SaveUnderLeaseAsync(StoreContracts.Checkpoint("t", "2026-09-18T00:00:00Z"), lease, expectedSequence: 1));

        Assert.Equal(3, (await store.GetLogAsync("t")).Entries.Count);
        Assert.Equal(3, retries.Count);
        Assert.Equal(("redis", "save"), retries.Last);

        // A conditional save that really lost its race still says so.
        Assert.False(await store.SaveIfLatestAsync(StoreContracts.Checkpoint("t", "2026-09-19T00:00:00Z"), 0));
    }

    [RedisFact]
    public async Task ADroppedRedisConnectionDoesNotFailARun()
    {
        using var sandbox = new RedisSandbox("dropped");

        // CLIENT is an admin command: the store's connection and the one that kills it are made for this test.
        static async Task<ConnectionMultiplexer> Admin()
        {
            var options = RedisConnection.ToConfigurationOptions(TestBackends.RedisUrl);
            options.AllowAdmin = true;
            return await ConnectionMultiplexer.ConnectAsync(options);
        }

        await using var connection = await Admin();
        await using var killer = await Admin();
        var store = new RedisCheckpointStore(connection.GetDatabase(), new RedisStoreOptions
        {
            Prefix = sandbox.Prefix,

            // Long enough for the client to reconnect on a slow machine.
            Retry = new StoreRetryPolicy { MaxAttempts = 10, InitialDelay = TimeSpan.FromMilliseconds(100), MaxDelay = TimeSpan.FromSeconds(2) },
        });
        var killed = false;
        var graph = DraftThenSend(async () =>
        {
            var id = (long)await connection.GetDatabase().ExecuteAsync("CLIENT", "ID");
            killed = (long)await killer.GetDatabase().ExecuteAsync("CLIENT", "KILL", "ID", id) == 1;
        });

        var result = await graph.RunAsync("t", InitialState(), store);

        Assert.True(killed, "the store's connection was killed");
        Assert.True(Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(result).State.IsCompleted);
        Assert.Equal(
            [CheckpointStatus.Running, CheckpointStatus.Running, CheckpointStatus.Completed], await Statuses(store, "t"));
    }
}
