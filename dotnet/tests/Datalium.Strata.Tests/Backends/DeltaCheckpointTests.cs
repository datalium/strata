using System.Diagnostics.Metrics;
using System.Text.Json.Nodes;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Storage.Redis;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using StackExchange.Redis;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// Delta checkpoints (<c>design/41-Checkpoint-Efficiency.md</c>, SG-15 part 2): the shared rows fixture, chains
/// with a whole row every <see cref="DeltaOptions.FullEvery"/>, a base that moved, a cold cache, nested levels,
/// a save in doubt, group commit, and the store contracts, against SQLite, and Postgres and Redis when
/// <c>STRATA_TEST_POSTGRES_URL</c> / <c>STRATA_TEST_REDIS_URL</c> are set. Mirrors <c>strata-memory</c>'s
/// <c>tests/deltas.rs</c>.
/// </summary>
public sealed class DeltaCheckpointTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(30);

    private static readonly DeltaOptions On = new();

    private const string Whole = "1.7.0";
    private const string Delta = "1.8.0";

    /// <summary>A running checkpoint at <paramref name="step"/>, with a payload that never changes.</summary>
    private static Checkpoint Running(string threadId, int step, Checkpoint? nested = null) => new()
    {
        SchemaVersion = Whole,
        ThreadId = threadId,
        Status = CheckpointStatus.Running,
        SuspendedNode = "A",
        NextNode = nested is null ? "B" : "Work",
        FrozenState = new JsonObject { ["step"] = step, ["payload"] = new string('e', 2048), ["flags"] = new JsonArray(true, null) },
        Metadata = new Dictionary<string, string> { ["strata.hops"] = $"{step}" },
        CreatedAt = $"2026-10-15T00:{step / 60:00}:{step % 60:00}Z",
        Nested = nested,
    };

    private static Checkpoint Child(string name, JsonObject state) => new()
    {
        SchemaVersion = Whole,
        ThreadId = name,
        Status = CheckpointStatus.Running,
        SuspendedNode = null,
        NextNode = "Scan",
        FrozenState = state,
        CreatedAt = "2026-10-15T00:00:00Z",
    };

    // --- The backends ---

    /// <summary>
    /// One database (or key prefix) that a test opens stores on, as many as it needs, and reads as stored: each
    /// row of a thread as <c>{schema_version, frozen_state, nested}</c>.
    /// </summary>
    private sealed class Backend : IDisposable
    {
        private readonly List<IDisposable> _opened = [];

        public required string Name { get; init; }

        public required Func<DeltaOptions?, GroupCommitOptions?, Func<string, ValueTask>?, ICheckpointStore> OpenStore { get; init; }

        public required Func<string, Task<List<JsonNode>>> Raw { get; init; }

        /// <summary>Stores a row as it is, as any other writer would: a stored checkpoint's JSON at a sequence.</summary>
        public required Func<string, long, JsonNode, Task> InsertRaw { get; init; }

        public required Action Cleanup { get; init; }

        public ICheckpointStore Open(DeltaOptions? deltas = null, GroupCommitOptions? groupCommit = null, Func<string, ValueTask>? fault = null)
        {
            var store = OpenStore(deltas, groupCommit, fault);
            _opened.Add((IDisposable)store);
            return store;
        }

        public async Task<List<string>> Versions(string threadId) =>
            [.. (await Raw(threadId)).Select(row => row["schema_version"]!.GetValue<string>())];

        public void Dispose()
        {
            foreach (var store in _opened)
            {
                store.Dispose();
            }

            Cleanup();
        }
    }

    private static JsonObject RowJson(string version, string frozenState, string? nested) => new()
    {
        ["schema_version"] = version,
        ["frozen_state"] = JsonNode.Parse(frozenState),
        ["nested"] = nested is null ? null : JsonNode.Parse(nested),
    };

    /// <summary>Inserts a stored checkpoint's columns, as the checkpoint log holds them, with raw SQL.</summary>
    private static async Task InsertRowAsync(System.Data.Common.DbConnection connection, long sequence, JsonNode stored, bool jsonb)
    {
        await using var command = connection.CreateCommand();
        var cast = jsonb ? "::jsonb" : string.Empty;
        command.CommandText = $"""
            INSERT INTO checkpoints
                (thread_id, sequence, schema_version, status, suspended_node, next_node, frozen_state, metadata,
                 created_at, resumed_at, nested)
            VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7{cast}, @p8{cast}, @p9, @p10, @p11{cast})
            """;
        object Text(string key, bool json = false) => stored[key] switch
        {
            null => DBNull.Value,
            var node when json => node.ToJsonString(),
            var node => node.GetValue<string>(),
        };
        object[] values =
        [
            Text("thread_id"), sequence, Text("schema_version"), Text("status"), Text("suspended_node"), Text("next_node"),
            Text("frozen_state", json: true), Text("metadata", json: true), Text("created_at"), Text("resumed_at"),
            Text("nested", json: true),
        ];
        for (var i = 0; i < values.Length; i++)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = $"@p{i + 1}";
            parameter.Value = values[i];
            command.Parameters.Add(parameter);
        }

        await command.ExecuteNonQueryAsync();
    }

    private static Backend Sqlite()
    {
        var path = Path.Combine(Path.GetTempPath(), $"strata-deltas-{Guid.NewGuid():N}.db");
        SqliteConnection Raw()
        {
            var connection = new SqliteConnection($"Data Source={path}");
            connection.Open();
            return connection;
        }

        return new Backend
        {
            Name = "sqlite",
            OpenStore = (deltas, groupCommit, fault) => new EfCoreCheckpointStore(
                new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={path}").Options),
                ownedConnection: null,
                timeProvider: null,
                new SqliteStoreOptions { Deltas = deltas, GroupCommit = groupCommit, Fault = fault }),
            Raw = async threadId =>
            {
                await using var connection = Raw();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    "SELECT schema_version, frozen_state, nested FROM checkpoints WHERE thread_id = $t ORDER BY sequence";
                command.Parameters.AddWithValue("$t", threadId);
                var rows = new List<JsonNode>();
                await using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    rows.Add(RowJson(reader.GetString(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
                }

                return rows;
            },
            InsertRaw = async (_, sequence, stored) =>
            {
                await using var connection = Raw();
                await InsertRowAsync(connection, sequence, stored, jsonb: false);
            },
            Cleanup = () =>
            {
                SqliteConnection.ClearAllPools();
                try
                {
                    File.Delete(path);
                }
                catch (IOException)
                {
                    // Best-effort cleanup of a temp file.
                }
            },
        };
    }

    private static Backend Postgres(string label)
    {
        var sandbox = new PostgresSandbox(label);
        var dataSource = sandbox.CreateDataSource();
        return new Backend
        {
            Name = "postgres",
            OpenStore = (deltas, groupCommit, fault) => PostgresSandbox.Open(
                dataSource, new PostgresStoreOptions { Deltas = deltas, GroupCommit = groupCommit, Fault = fault }),
            Raw = async threadId =>
            {
                await using var command = dataSource.CreateCommand(
                    "SELECT schema_version, frozen_state::text, nested::text FROM checkpoints WHERE thread_id = $1 ORDER BY sequence");
                command.Parameters.Add(new NpgsqlParameter { Value = threadId });
                var rows = new List<JsonNode>();
                await using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    rows.Add(RowJson(reader.GetString(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
                }

                return rows;
            },
            InsertRaw = async (_, sequence, stored) =>
            {
                await using var connection = await dataSource.OpenConnectionAsync();
                await InsertRowAsync(connection, sequence, stored, jsonb: true);
            },
            Cleanup = sandbox.Dispose,
        };
    }

    private static Backend Redis(string label)
    {
        var sandbox = new RedisSandbox(label);
        RedisKey Key(string threadId) => $"{sandbox.Prefix}:checkpoints:{threadId}";
        return new Backend
        {
            Name = "redis",
            OpenStore = (deltas, _, fault) => new RedisCheckpointStore(
                sandbox.Database, new RedisStoreOptions { Prefix = sandbox.Prefix, Deltas = deltas, Fault = fault }),
            Raw = async threadId =>
                [.. (await sandbox.Database.ListRangeAsync(Key(threadId))).Select(element => JsonNode.Parse((string)element!)!)],
            InsertRaw = async (threadId, _, stored) =>
                await sandbox.Database.ListRightPushAsync(Key(threadId), stored.ToJsonString()),
            Cleanup = sandbox.Dispose,
        };
    }

    private static async Task<List<Checkpoint>> Log(ICheckpointStore store, string threadId) =>
        [.. (await ((ICheckpointQuery)store).GetLogAsync(threadId, limit: ThreadQuery.MaxLimit)).Entries.Select(e => e.Checkpoint)];

    private static void AssertLog(IReadOnlyList<Checkpoint> expected, IReadOnlyList<Checkpoint> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            StoreContracts.AssertSame(expected[i], actual[i]);
        }
    }

    // --- The shared rows fixture ---

    [Fact]
    public async Task Sqlite_Reads_The_Shared_Delta_Rows() => await SharedRowsAsync(Sqlite());

    [PostgresFact]
    public async Task Postgres_Reads_The_Shared_Delta_Rows() => await SharedRowsAsync(Postgres("delta_rows"));

    [RedisFact]
    public async Task Redis_Reads_The_Shared_Delta_Rows() => await SharedRowsAsync(Redis("delta_rows"));

    /// <summary>
    /// <c>checkpoint-log/delta.rows.json</c>, stored as it is a row at a time: a load after each row gives its
    /// checkpoint, the log gives every row's, a page that starts at a delta reads the rows it needs, and a listing
    /// gives the version the latest was saved with. The store reading them has deltas off.
    /// </summary>
    private static async Task SharedRowsAsync(Backend backend)
    {
        using var _ = backend;
        var fixture = CheckpointQueryContractTests.Fixture(Path.Combine("checkpoint-log", "delta.rows.json"));
        var threadId = fixture["thread_id"]!.GetValue<string>();
        var rows = fixture["rows"]!.AsArray();
        var store = backend.Open();
        var decoded = rows.Select(row => Checkpoint.FromWireJson(row!["decoded"]!)).ToList();

        foreach (var row in rows)
        {
            var sequence = row!["sequence"]!.GetValue<long>();
            await backend.InsertRaw(threadId, sequence, row["stored"]!);
            StoreContracts.AssertSame(decoded[(int)sequence], await store.LoadAsync(threadId));
            var latest = await store.LoadLatestAsync(threadId);
            Assert.Equal(sequence, latest!.Sequence);
            StoreContracts.AssertSame(decoded[(int)sequence], latest.Checkpoint);
        }

        Assert.Equal([Whole, Delta, Delta], await backend.Versions(threadId));
        AssertLog(decoded, await Log(store, threadId));
        var page = (await ((ICheckpointQuery)store).GetLogAsync(threadId, afterSequence: 1, limit: 1)).Entries;
        StoreContracts.AssertSame(decoded[2], Assert.Single(page).Checkpoint);

        if (store is RedisCheckpointStore redis)
        {
            await redis.RebuildIndexAsync();
        }

        var listed = Assert.Single((await ((ICheckpointQuery)store).ListThreadsAsync(new ThreadQuery())).Threads);
        Assert.Equal((threadId, 2L, Whole), (listed.ThreadId, listed.Sequence, listed.SchemaVersion));
    }

    // --- Chains ---

    [Fact]
    public async Task Sqlite_Stores_Running_Checkpoints_As_Deltas_With_Every_FullEvery_Th_Row_Whole() => await ChainAsync(Sqlite());

    [PostgresFact]
    public async Task Postgres_Stores_Running_Checkpoints_As_Deltas_With_Every_FullEvery_Th_Row_Whole() =>
        await ChainAsync(Postgres("delta_chain"));

    [RedisFact]
    public async Task Redis_Stores_Running_Checkpoints_As_Deltas_With_Every_FullEvery_Th_Row_Whole() =>
        await ChainAsync(Redis("delta_chain"));

    /// <summary>
    /// Ten running saves, an interrupted one, two more running: a delta carries only what changed, every fourth
    /// row and every non-running one is whole, the log decodes to what was saved, and a store with deltas off
    /// reads it the same. The checkpoint-size metric records the deltas.
    /// </summary>
    private static async Task ChainAsync(Backend backend)
    {
        using var _ = backend;
        using var sizes = new StoredSizes(backend.Name);
        var store = backend.Open(new DeltaOptions { FullEvery = 4 });
        var saved = new List<Checkpoint>();
        for (var step = 0; step < 13; step++)
        {
            var checkpoint = step == 10
                ? Running("t", step) with { Status = CheckpointStatus.Interrupted, SchemaVersion = "1.0.0" }
                : Running("t", step);
            await store.SaveAsync(checkpoint);
            saved.Add(checkpoint);
            StoreContracts.AssertSame(checkpoint, await store.LoadAsync("t"));
        }

        Assert.Equal(
            [Whole, Delta, Delta, Delta, Whole, Delta, Delta, Delta, Whole, Delta, "1.0.0", Delta, Delta],
            await backend.Versions("t"));
        var raw = await backend.Raw("t");
        Assert.True(
            JsonNode.DeepEquals(
                JsonNode.Parse("""{"checkpoint_version":"1.7.0","delta_of":0,"depth":1,"set":{"step":1},"unset":[]}"""),
                raw[1]["frozen_state"]),
            raw[1]["frozen_state"]!.ToJsonString());
        AssertLog(saved, await Log(store, "t"));
        AssertLog(saved, await Log(backend.Open(), "t"));
        Assert.True(sizes.Deltas > 0, $"no {backend.Name} delta was recorded");
    }

    /// <summary>Counts <c>strata.store.checkpoint.size</c> records with encoding <c>delta</c> for one store while it lives.</summary>
    private sealed class StoredSizes : IDisposable
    {
        private readonly MeterListener _listener = new();
        private int _deltas;

        public StoredSizes(string store)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == StorageTelemetry.Name && instrument.Name == "strata.store.checkpoint.size")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                var named = tags.ToArray().ToDictionary(tag => tag.Key, tag => tag.Value as string);
                if (named.GetValueOrDefault("strata.store") == store && named.GetValueOrDefault("strata.store.encoding") == "delta")
                {
                    Interlocked.Increment(ref _deltas);
                }
            });
            _listener.Start();
        }

        public int Deltas => Volatile.Read(ref _deltas);

        public void Dispose() => _listener.Dispose();
    }

    // --- A base that moved, a cold cache ---

    [Fact]
    public async Task Sqlite_Writes_Whole_When_Another_Writer_Moved_The_Base() => await BaseMovedAsync(Sqlite());

    [PostgresFact]
    public async Task Postgres_Writes_Whole_When_Another_Writer_Moved_The_Base() => await BaseMovedAsync(Postgres("delta_moved"));

    [RedisFact]
    public async Task Redis_Writes_Whole_When_Another_Writer_Moved_The_Base() => await BaseMovedAsync(Redis("delta_moved"));

    /// <summary>
    /// A store's cache says row 1 is the thread's latest; another writer appends row 2. The next save, plain,
    /// conditional or fenced, is written whole, and the one after is a delta of it.
    /// </summary>
    private static async Task BaseMovedAsync(Backend backend)
    {
        using var _ = backend;
        var store = backend.Open(On);
        var other = backend.Open();
        var saved = new List<Checkpoint>();
        async Task Save(ICheckpointStore by, int step)
        {
            await by.SaveAsync(Running("t", step));
            saved.Add(Running("t", step));
        }

        await Save(store, 0);
        await Save(store, 1);
        await Save(other, 2);
        await Save(store, 3);
        await Save(store, 4);
        Assert.True(await store.SaveIfLatestAsync(Running("t", 5), 4));
        saved.Add(Running("t", 5));

        // The conditional and the fenced forms.
        await Save(other, 6);
        Assert.True(await store.SaveIfLatestAsync(Running("t", 7), 6));
        saved.Add(Running("t", 7));
        var lease = (await store.TryAcquireLeaseAsync("t", "w", TimeSpan.FromMinutes(5))).Lease!;
        Assert.True(await store.SaveUnderLeaseAsync(Running("t", 8), lease));
        saved.Add(Running("t", 8));
        await Save(other, 9);
        Assert.True(await store.SaveUnderLeaseAsync(Running("t", 10), lease));
        saved.Add(Running("t", 10));

        Assert.Equal(
            [Whole, Delta, Whole, Whole, Delta, Delta, Whole, Whole, Delta, Whole, Whole],
            await backend.Versions("t"));
        AssertLog(saved, await Log(store, "t"));
    }

    [Fact]
    public async Task Sqlite_Writes_Whole_From_A_Cold_Cache_And_Deltas_After_A_Load() => await ColdCacheAsync(Sqlite());

    [PostgresFact]
    public async Task Postgres_Writes_Whole_From_A_Cold_Cache_And_Deltas_After_A_Load() => await ColdCacheAsync(Postgres("delta_cold"));

    [RedisFact]
    public async Task Redis_Writes_Whole_From_A_Cold_Cache_And_Deltas_After_A_Load() => await ColdCacheAsync(Redis("delta_cold"));

    /// <summary>
    /// A new store doesn't know the thread and saves it whole; one that has loaded the thread's latest, a delta;
    /// one that remembers a single thread forgets the first when it saves a second.
    /// </summary>
    private static async Task ColdCacheAsync(Backend backend)
    {
        using var _ = backend;
        await backend.Open(On).SaveAsync(Running("t", 0));
        var first = backend.Open(On);
        await first.SaveAsync(Running("t", 1));
        var loaded = backend.Open(On);
        Assert.NotNull(await loaded.LoadLatestAsync("t"));
        await loaded.SaveAsync(Running("t", 2));

        var small = backend.Open(new DeltaOptions { CachedThreads = 1 });
        await small.SaveAsync(Running("t", 3));
        await small.SaveAsync(Running("u", 0));
        await small.SaveAsync(Running("t", 4));
        await small.SaveAsync(Running("t", 5));

        Assert.Equal([Whole, Whole, Delta, Whole, Whole, Delta], await backend.Versions("t"));
        AssertLog([.. Enumerable.Range(0, 6).Select(step => Running("t", step))], await Log(first, "t"));
    }

    // --- Nested levels ---

    [Fact]
    public async Task Sqlite_Encodes_Each_Level_On_Its_Own() => await NestedAsync(Sqlite());

    [PostgresFact]
    public async Task Postgres_Encodes_Each_Level_On_Its_Own() => await NestedAsync(Postgres("delta_nested"));

    [RedisFact]
    public async Task Redis_Encodes_Each_Level_On_Its_Own() => await NestedAsync(Redis("delta_nested"));

    /// <summary>A level that appears is whole beside a delta, then a delta itself; a level that goes leaves no trace.</summary>
    private static async Task NestedAsync(Backend backend)
    {
        using var _ = backend;
        var store = backend.Open(On);
        List<Checkpoint> saved =
        [
            Running("t", 0),
            Running("t", 1, Child("Work", new JsonObject { ["n"] = 0, ["draft"] = "x" })),
            Running("t", 2, Child("Work", new JsonObject { ["n"] = 1 })),
            Running("t", 3),
        ];
        foreach (var checkpoint in saved)
        {
            await store.SaveAsync(checkpoint);
        }

        var raw = await backend.Raw("t");
        Assert.Equal([Whole, Delta, Delta, Delta], raw.Select(row => row["schema_version"]!.GetValue<string>()));
        Assert.True(raw[1]["frozen_state"]!.AsObject().ContainsKey("set"));
        Assert.True(
            JsonNode.DeepEquals(JsonNode.Parse("""{"state":{"n":0,"draft":"x"}}"""), raw[1]["nested"]!["frozen_state"]),
            raw[1]["nested"]!.ToJsonString());
        Assert.True(
            JsonNode.DeepEquals(JsonNode.Parse("""{"set":{"n":1},"unset":["draft"]}"""), raw[2]["nested"]!["frozen_state"]),
            raw[2]["nested"]!.ToJsonString());
        Assert.Null(raw[3]["nested"]);
        AssertLog(saved, await Log(store, "t"));
    }

    // --- A save in doubt ---

    /// <summary>A fault hook that throws <paramref name="failure"/> once at <paramref name="point"/>, after <c>Arm</c>.</summary>
    private sealed class Armed(string point, Func<Exception> failure)
    {
        private int _armed;

        public void Arm() => Volatile.Write(ref _armed, 1);

        public ValueTask Fault(string reached) =>
            reached == point && Interlocked.Exchange(ref _armed, 0) == 1 ? throw failure() : ValueTask.CompletedTask;
    }

    [PostgresFact]
    public async Task Postgres_Looks_Up_A_Delta_Save_Whose_Commit_May_Have_Happened() =>
        await InDoubtAsync(Postgres("delta_doubt"), new Armed("save:after-commit", () => new NpgsqlException("lost", new IOException("reset"))));

    [RedisFact]
    public async Task Redis_Looks_Up_A_Delta_Save_That_May_Have_Run() =>
        await InDoubtAsync(Redis("delta_doubt"), new Armed("save:after", () => new RedisConnectionException(ConnectionFailureType.SocketFailure, "gone")));

    /// <summary>
    /// A delta save whose answer is lost after it was written is looked up, as what it decodes to, and found:
    /// written once. The store then no longer knows the thread's latest, so the next save is whole.
    /// </summary>
    private static async Task InDoubtAsync(Backend backend, Armed fault)
    {
        using var _ = backend;
        var store = backend.Open(On, fault: fault.Fault);
        await store.SaveAsync(Running("t", 0));
        fault.Arm();
        await store.SaveAsync(Running("t", 1));
        Assert.True(await store.SaveIfLatestAsync(Running("t", 2), 1));
        await store.SaveAsync(Running("t", 3));

        Assert.Equal([Whole, Delta, Whole, Delta], await backend.Versions("t"));
        AssertLog([.. Enumerable.Range(0, 4).Select(step => Running("t", step))], await Log(store, "t"));
    }

    // --- Group commit ---

    private static readonly GroupCommitOptions OneAtATime = new() { MaxConcurrentFlushes = 1 };

    /// <summary>Holds the first flush that reaches <paramref name="point"/> after <see cref="Arm"/> until <see cref="Release"/>.</summary>
    private sealed class FlushHold(string point)
    {
        private readonly TaskCompletionSource _held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _armed;

        public void Arm() => Volatile.Write(ref _armed, 1);

        public Task Held => _held.Task.WaitAsync(Patience);

        public void Release() => _released.TrySetResult();

        public async ValueTask Fault(string reached)
        {
            if (reached == point && Interlocked.Exchange(ref _armed, 0) == 1)
            {
                _held.TrySetResult();
                await _released.Task.WaitAsync(Patience).ConfigureAwait(false);
            }
        }
    }

    [Fact]
    public async Task Sqlite_Writes_Whole_In_A_Flush_What_Can_Not_Be_A_Delta() => await GroupCommitAsync(Sqlite(), "flush:start");

    [PostgresFact]
    public async Task Postgres_Writes_Whole_In_A_Flush_What_Can_Not_Be_A_Delta() =>
        await GroupCommitAsync(Postgres("delta_flush"), "save:before-commit");

    /// <summary>
    /// With group commit: a delta whose base another writer moved is written whole in its flush, two saves of one
    /// thread in one flush are both whole, and the save after them is a delta of the second.
    /// </summary>
    private static async Task GroupCommitAsync(Backend backend, string holdAt)
    {
        using var _ = backend;
        var hold = new FlushHold(holdAt);
        var store = backend.Open(On, OneAtATime, hold.Fault);
        var other = backend.Open();
        await store.SaveAsync(Running("t", 0));
        await store.SaveAsync(Running("t", 1));
        await other.SaveAsync(Running("t", 2));
        await store.SaveAsync(Running("t", 3));
        await store.SaveAsync(Running("t", 4));

        hold.Arm();
        var blocker = store.SaveAsync(Running("blocker", 0)).AsTask();
        await hold.Held;
        var fifth = store.SaveAsync(Running("t", 5)).AsTask();
        var sixth = store.SaveAsync(Running("t", 6)).AsTask();
        hold.Release();
        await Task.WhenAll(blocker, fifth, sixth).WaitAsync(Patience);
        await store.SaveAsync(Running("t", 7));

        Assert.Equal([Whole, Delta, Whole, Whole, Delta, Whole, Whole, Delta], await backend.Versions("t"));
        AssertLog([.. Enumerable.Range(0, 8).Select(step => Running("t", step))], await Log(store, "t"));
    }

    // --- The engine and the store contracts ---

    private sealed record Counter(int Step, string Payload);

    [Fact]
    public async Task Sqlite_Keeps_A_Durable_Run_As_Deltas() => await DurableRunAsync(Sqlite());

    [PostgresFact]
    public async Task Postgres_Keeps_A_Durable_Run_As_Deltas() => await DurableRunAsync(Postgres("delta_run"));

    [RedisFact]
    public async Task Redis_Keeps_A_Durable_Run_As_Deltas() => await DurableRunAsync(Redis("delta_run"));

    /// <summary>
    /// A graph that checkpoints every node, run against a store with deltas: every running row after the first is
    /// a delta but every fourth, the completed row is whole, and the log decodes to what the same run leaves in
    /// the in-memory store.
    /// </summary>
    private static async Task DurableRunAsync(Backend backend)
    {
        using var _ = backend;
        var builder = new StateGraph<Counter>();
        for (var node = 1; node <= 8; node++)
        {
            builder.AddNode($"N{node}", s => s with { Step = s.Step + 1 });
            if (node > 1)
            {
                builder.AddEdge($"N{node - 1}", $"N{node}");
            }
        }

        var graph = builder.Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });
        var initial = new Counter(0, new string('p', 4096));
        var store = backend.Open(new DeltaOptions { FullEvery = 4 });
        Assert.IsType<GraphExecutionResult<Counter>.Completed>(await graph.RunAsync("run", initial, store));
        var memory = new InMemoryCheckpointStore();
        Assert.IsType<GraphExecutionResult<Counter>.Completed>(await graph.RunAsync("run", initial, memory));

        var versions = await backend.Versions("run");
        Assert.Equal(9, versions.Count);
        Assert.Equal([Whole, Delta, Delta, Delta, Whole, Delta, Delta, Delta], versions[..8]);
        Assert.NotEqual(Delta, versions[8]);
        var expected = await Log(memory, "run");
        var actual = await Log(store, "run");
        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Status, actual[i].Status);
            Assert.Equal(expected[i].NextNode, actual[i].NextNode);
            Assert.True(JsonNode.DeepEquals(expected[i].FrozenState, actual[i].FrozenState), $"row {i} differs");
        }
    }

    [Fact]
    public async Task Sqlite_Satisfies_The_Store_Contracts_With_Deltas()
    {
        using var backend = Sqlite();
        await ContractsAsync((EfCoreCheckpointStore)backend.Open(On));
        using var grouped = Sqlite();
        await ContractsAsync((EfCoreCheckpointStore)grouped.Open(On, OneAtATime));
    }

    [PostgresFact]
    public async Task Postgres_Satisfies_The_Store_Contracts_With_Deltas()
    {
        using var backend = Postgres("delta_contracts");
        await ContractsAsync((PostgresCheckpointStore)backend.Open(On));
        using var grouped = Postgres("delta_contracts_grouped");
        await ContractsAsync((PostgresCheckpointStore)grouped.Open(On, new GroupCommitOptions()));
    }

    [RedisFact]
    public async Task Redis_Satisfies_The_Store_Contracts_With_Deltas()
    {
        using var backend = Redis("delta_contracts");
        await ContractsAsync((RedisCheckpointStore)backend.Open(On));
    }

    private static async Task ContractsAsync<TStore>(TStore store)
        where TStore : ICheckpointStore, ICheckpointQuery
    {
        StoreContracts.CheckpointStoreContract(store);
        await StoreContracts.FreezeResumeContractAsync(store);
        await StoreContracts.ConditionalSaveContractAsync(store);
        await StoreContracts.FreezeEndContractAsync(store);
        await StoreContracts.LeasedRunContractAsync(store);
        await StoreContracts.DurableRunContractAsync(store);
    }

    [Fact]
    public void The_Options_Are_Checked()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new DeltaOptions { FullEvery = 1 }.Checked());
        Assert.Throws<ArgumentOutOfRangeException>(() => new DeltaOptions { CachedThreads = 0 }.Checked());
        Assert.Equal((16, 10_000), (On.FullEvery, On.CachedThreads));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            EfCoreCheckpointStore.CreateInMemory(options: new SqliteStoreOptions { Deltas = new DeltaOptions { FullEvery = 0 } }));
    }
}
