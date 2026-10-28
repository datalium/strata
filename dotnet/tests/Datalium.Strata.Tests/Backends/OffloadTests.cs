using System.Diagnostics.Metrics;
using System.Security.Cryptography;
using System.Text;
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
/// Value offload (<c>design/41-Checkpoint-Efficiency.md</c>, SG-15 part 3): the shared rows fixture, values offloaded
/// once and inherited, each blob store (the store's own database, in memory, the filesystem), a missing and a
/// tampered blob, purge and the Redis ttl, schema version 2, a base row another process replaced, group commit, a
/// durable run and the store contracts, against SQLite, and Postgres and Redis when <c>STRATA_TEST_POSTGRES_URL</c> /
/// <c>STRATA_TEST_REDIS_URL</c> are set. Mirrors <c>strata-memory</c>'s <c>tests/offload.rs</c>.
/// </summary>
public sealed class OffloadTests
{
    private const string Whole = "1.7.0";
    private const string Delta = "1.8.0";
    private const string Offloaded = "1.9.0";

    /// <summary>The threshold the tests offload from: the evidence is over it, the rest under.</summary>
    private const int Threshold = 1024;

    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(30);

    /// <summary>A running checkpoint at <paramref name="step"/>, whose 2 KiB <c>evidence</c> is offloaded.</summary>
    private static Checkpoint Running(string threadId, int step, string evidence = "e") => new()
    {
        SchemaVersion = Whole,
        ThreadId = threadId,
        Status = CheckpointStatus.Running,
        SuspendedNode = "A",
        NextNode = "B",
        FrozenState = new JsonObject
        {
            ["step"] = step,
            ["evidence"] = new JsonObject { ["log"] = string.Concat(Enumerable.Repeat(evidence, 2048 / evidence.Length)) },
        },
        Metadata = new Dictionary<string, string> { ["strata.hops"] = $"{step}" },
        CreatedAt = $"2026-10-15T00:{step / 60:00}:{step % 60:00}Z",
    };

    private static Checkpoint Finished(string threadId, CheckpointStatus status, string createdAt) =>
        Running(threadId, 99) with { Status = status, SchemaVersion = "1.4.0", CreatedAt = createdAt, NextNode = null };

    private static OffloadOptions Offload(ICheckpointBlobStore? blobs = null) => new() { Threshold = Threshold, Blobs = blobs };

    /// <summary>Offload options that read <paramref name="blobs"/> but offload nothing new.</summary>
    private static OffloadOptions? ReadOnly(ICheckpointBlobStore? blobs) =>
        blobs is null ? null : new OffloadOptions { Threshold = int.MaxValue, Blobs = blobs };

    // --- The blob stores and the backends ---

    /// <summary>Where a test's blobs go: the store's own database, or a blob store of its own.</summary>
    private sealed class Blobs : IDisposable
    {
        private Blobs(ICheckpointBlobStore? store) => Store = store;

        public ICheckpointBlobStore? Store { get; }

        public static Blobs Own() => new(null);

        public static Blobs Memory() => new(new InMemoryCheckpointBlobStore());

        public static Blobs Files() =>
            new(new FileSystemCheckpointBlobStore(Path.Combine(Path.GetTempPath(), $"strata-blobs-{Guid.NewGuid():N}")));

        public OffloadOptions Offload() => OffloadTests.Offload(Store);

        public void Dispose()
        {
            if (Store is FileSystemCheckpointBlobStore files && Directory.Exists(files.Root))
            {
                Directory.Delete(files.Root, recursive: true);
            }
        }
    }

    private sealed record Options(
        DeltaOptions? Deltas = null, OffloadOptions? Offload = null, GroupCommitOptions? GroupCommit = null, TimeSpan? Ttl = null);

    /// <summary>
    /// One database (or key prefix) a test opens stores on, as many as it needs, and reads and writes as stored: each
    /// row of a thread as <c>{schema_version, frozen_state, nested}</c>, and the blobs in the store's own database.
    /// </summary>
    private sealed class Backend : IDisposable
    {
        private readonly List<IDisposable> _opened = [];

        public required string Name { get; init; }

        public required Func<Options, ICheckpointStore> OpenStore { get; init; }

        public required Func<string, Task<List<JsonNode>>> Raw { get; init; }

        public required Func<string, long, JsonNode, Task> InsertRaw { get; init; }

        public required Func<string, Task<Dictionary<string, byte[]>>> OwnBlobs { get; init; }

        /// <summary>Stores or overwrites a blob in the store's own database, as any other writer would.</summary>
        public required Func<string, string, byte[], Task> PutOwnBlob { get; init; }

        /// <summary>Deletes a thread's rows and blobs, as another process's purge would, behind every store's back.</summary>
        public required Func<string, Task> DeleteThread { get; init; }

        /// <summary>Deletes every blob in the store's own database, and nothing else.</summary>
        public required Func<Task> DeleteOwnBlobs { get; init; }

        public required Action Cleanup { get; init; }

        public ICheckpointStore Open(Options? options = null)
        {
            var store = OpenStore(options ?? new Options());
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
        var path = Path.Combine(Path.GetTempPath(), $"strata-offload-{Guid.NewGuid():N}.db");
        SqliteConnection Raw()
        {
            var connection = new SqliteConnection($"Data Source={path}");
            connection.Open();
            return connection;
        }

        async Task Execute(string sql, params (string Name, object Value)[] parameters)
        {
            await using var connection = Raw();
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            foreach (var (name, value) in parameters)
            {
                command.Parameters.AddWithValue(name, value);
            }

            await command.ExecuteNonQueryAsync();
        }

        return new Backend
        {
            Name = "sqlite",
            OpenStore = options => new EfCoreCheckpointStore(
                new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={path}").Options),
                ownedConnection: null,
                timeProvider: null,
                new SqliteStoreOptions { Deltas = options.Deltas, Offload = options.Offload, GroupCommit = options.GroupCommit }),
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
            OwnBlobs = async threadId =>
            {
                await using var connection = Raw();
                await using var command = connection.CreateCommand();
                command.CommandText = "SELECT hash, content FROM checkpoint_blobs WHERE thread_id = $t";
                command.Parameters.AddWithValue("$t", threadId);
                var blobs = new Dictionary<string, byte[]>();
                await using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    blobs[reader.GetString(0)] = (byte[])reader.GetValue(1);
                }

                return blobs;
            },
            PutOwnBlob = (threadId, hash, content) => Execute(
                "INSERT OR REPLACE INTO checkpoint_blobs (thread_id, hash, content) VALUES ($t, $h, $c)",
                ("$t", threadId), ("$h", hash), ("$c", content)),
            DeleteThread = async threadId =>
            {
                foreach (var table in (string[])["checkpoints", "checkpoint_threads", "checkpoint_blobs"])
                {
                    await Execute($"DELETE FROM {table} WHERE thread_id = $t", ("$t", threadId));
                }
            },
            DeleteOwnBlobs = () => Execute("DELETE FROM checkpoint_blobs"),
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
        async Task Execute(string sql, params object[] values)
        {
            await using var command = dataSource.CreateCommand(sql);
            foreach (var value in values)
            {
                command.Parameters.Add(new NpgsqlParameter { Value = value });
            }

            await command.ExecuteNonQueryAsync();
        }

        return new Backend
        {
            Name = "postgres",
            OpenStore = options => PostgresSandbox.Open(
                dataSource,
                new PostgresStoreOptions { Deltas = options.Deltas, Offload = options.Offload, GroupCommit = options.GroupCommit }),
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
            OwnBlobs = async threadId =>
            {
                await using var command = dataSource.CreateCommand("SELECT hash, content FROM checkpoint_blobs WHERE thread_id = $1");
                command.Parameters.Add(new NpgsqlParameter { Value = threadId });
                var blobs = new Dictionary<string, byte[]>();
                await using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    blobs[reader.GetString(0)] = reader.GetFieldValue<byte[]>(1);
                }

                return blobs;
            },
            PutOwnBlob = (threadId, hash, content) => Execute(
                """
                INSERT INTO checkpoint_blobs (thread_id, hash, content) VALUES ($1, $2, $3)
                ON CONFLICT (thread_id, hash) DO UPDATE SET content = EXCLUDED.content
                """,
                threadId,
                hash,
                content),
            DeleteThread = async threadId =>
            {
                foreach (var table in (string[])["checkpoints", "checkpoint_threads", "checkpoint_blobs"])
                {
                    await Execute($"DELETE FROM {table} WHERE thread_id = $1", threadId);
                }
            },
            DeleteOwnBlobs = () => Execute("DELETE FROM checkpoint_blobs"),
            Cleanup = sandbox.Dispose,
        };
    }

    private static Backend Redis(string label)
    {
        var sandbox = new RedisSandbox(label);
        RedisKey Key(string threadId) => $"{sandbox.Prefix}:checkpoints:{threadId}";
        RedisKey BlobKey(string threadId) => $"{sandbox.Prefix}:blobs:{threadId}";
        return new Backend
        {
            Name = "redis",
            OpenStore = options => new RedisCheckpointStore(
                sandbox.Database,
                new RedisStoreOptions
                {
                    Prefix = sandbox.Prefix,
                    Deltas = options.Deltas,
                    Offload = options.Offload,
                    FinishedThreadTtl = options.Ttl,
                }),
            Raw = async threadId =>
                [.. (await sandbox.Database.ListRangeAsync(Key(threadId))).Select(element => JsonNode.Parse((string)element!)!)],
            InsertRaw = async (threadId, _, stored) =>
                await sandbox.Database.ListRightPushAsync(Key(threadId), stored.ToJsonString()),
            OwnBlobs = async threadId =>
                (await sandbox.Database.HashGetAllAsync(BlobKey(threadId))).ToDictionary(
                    entry => (string)entry.Name!, entry => (byte[])entry.Value!),
            PutOwnBlob = async (threadId, hash, content) => await sandbox.Database.HashSetAsync(BlobKey(threadId), hash, content),
            DeleteThread = async threadId => await sandbox.Database.KeyDeleteAsync([Key(threadId), BlobKey(threadId)]),
            DeleteOwnBlobs = async () => await sandbox.Database.KeyDeleteAsync(BlobKey("t")),
            Cleanup = sandbox.Dispose,
        };
    }

    /// <summary>The thread's blobs wherever <paramref name="blobs"/> keeps them, hash to content.</summary>
    private static async Task<Dictionary<string, byte[]>> BlobsOf(Backend backend, Blobs blobs, string threadId)
    {
        switch (blobs.Store)
        {
            case null:
                return await backend.OwnBlobs(threadId);
            case FileSystemCheckpointBlobStore files:
                var directory = files.ThreadDirectory(threadId);
                return Directory.Exists(directory)
                    ? Directory.GetFiles(directory).ToDictionary(file => $"sha256:{Path.GetFileName(file)}", File.ReadAllBytes)
                    : [];
            case InMemoryCheckpointBlobStore memory:
                var found = new Dictionary<string, byte[]>();
                foreach (var hash in await Hashes(backend, threadId))
                {
                    if (await memory.GetAsync(threadId, hash) is { } content)
                    {
                        found[hash] = content.ToArray();
                    }
                }

                Assert.Equal(memory.Count(threadId), found.Count);
                return found;
            default:
                throw new InvalidOperationException("an unknown blob store");
        }
    }

    /// <summary>Every hash the thread's rows refer to.</summary>
    private static async Task<List<string>> Hashes(Backend backend, string threadId)
    {
        var found = new SortedSet<string>(StringComparer.Ordinal);
        void Collect(JsonNode? node)
        {
            switch (node)
            {
                case JsonObject json:
                    foreach (var (key, child) in json)
                    {
                        if (key == "blobs" && child is JsonObject blobs)
                        {
                            found.UnionWith(blobs.Select(reference => reference.Value!["hash"]!.GetValue<string>()));
                        }
                        else
                        {
                            Collect(child);
                        }
                    }

                    break;
                case JsonArray array:
                    foreach (var item in array)
                    {
                        Collect(item);
                    }

                    break;
            }
        }

        foreach (var row in await backend.Raw(threadId))
        {
            Collect(row);
        }

        return [.. found];
    }

    private static string Sha256Hash(byte[] content) => "sha256:" + Convert.ToHexStringLower(SHA256.HashData(content));

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

    // --- The blob stores on their own ---

    [Fact]
    public async Task The_Filesystem_Blob_Store_Keeps_A_Blob_Per_Thread_And_Hash_And_Deletes_A_Thread()
    {
        using var blobs = Blobs.Files();
        var store = (FileSystemCheckpointBlobStore)blobs.Store!;
        var content = Encoding.UTF8.GetBytes("""{"a":1}""");
        var hash = Sha256Hash(content);
        await store.PutAsync("t/1", hash, content);
        await store.PutAsync("t/1", hash, content);
        Assert.Equal(content, (await store.GetAsync("t/1", hash))!.Value.ToArray());
        Assert.Null(await store.GetAsync("t/2", hash));
        Assert.EndsWith("742f31", store.ThreadDirectory("t/1"), StringComparison.Ordinal);
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.GetAsync("t/1", "md5:00"));
        await store.DeleteThreadAsync("t/1");
        await store.DeleteThreadAsync("t/1");
        Assert.Null(await store.GetAsync("t/1", hash));
    }

    [Fact]
    public async Task The_In_Memory_Blob_Store_Keeps_What_It_Was_Given_First()
    {
        var store = new InMemoryCheckpointBlobStore();
        await store.PutAsync("t", "sha256:00", new byte[] { 1 });
        await store.PutAsync("t", "sha256:00", new byte[] { 2 });
        Assert.Equal([1], (await store.GetAsync("t", "sha256:00"))!.Value.ToArray());
        Assert.Equal(1, store.Count("t"));
        await store.DeleteThreadAsync("t");
        Assert.Equal(0, store.Count("t"));
    }

    // --- The shared rows fixture ---

    [Fact]
    public async Task Sqlite_Reads_The_Shared_Offloaded_Rows() => await SharedRowsAsync(Sqlite(), Blobs.Own());

    [Fact]
    public async Task Sqlite_Reads_The_Shared_Offloaded_Rows_From_A_Filesystem_Blob_Store() =>
        await SharedRowsAsync(Sqlite(), Blobs.Files());

    [PostgresFact]
    public async Task Postgres_Reads_The_Shared_Offloaded_Rows() => await SharedRowsAsync(Postgres("offload_rows"), Blobs.Own());

    [RedisFact]
    public async Task Redis_Reads_The_Shared_Offloaded_Rows() => await SharedRowsAsync(Redis("offload_rows"), Blobs.Own());

    /// <summary>
    /// <c>checkpoint-log/offloaded.rows.json</c>, stored as it is a row at a time with its blobs where
    /// <paramref name="blobs"/> keeps them: a load after each row gives its checkpoint, the log gives every row's, a
    /// page that starts at a delta reads the rows it needs, and a listing gives the version the latest was saved
    /// with. The store reading them has offload off (but knows where its blobs are).
    /// </summary>
    private static async Task SharedRowsAsync(Backend backend, Blobs blobs)
    {
        using var _ = backend;
        using var __ = blobs;
        var fixture = CheckpointQueryContractTests.Fixture(Path.Combine("checkpoint-log", "offloaded.rows.json"));
        var threadId = fixture["thread_id"]!.GetValue<string>();
        var rows = fixture["rows"]!.AsArray();
        var store = backend.Open(new Options(Offload: ReadOnly(blobs.Store)));
        foreach (var (hash, text) in fixture["blobs"]!.AsObject())
        {
            var content = Encoding.UTF8.GetBytes(text!.GetValue<string>());
            Assert.Equal(hash, Sha256Hash(content));
            if (blobs.Store is { } elsewhere)
            {
                await elsewhere.PutAsync(threadId, hash, content);
            }
            else
            {
                await backend.PutOwnBlob(threadId, hash, content);
            }
        }

        var decoded = rows.Select(row => Checkpoint.FromWireJson(row!["decoded"]!)).ToList();
        foreach (var row in rows)
        {
            var sequence = row!["sequence"]!.GetValue<long>();
            await backend.InsertRaw(threadId, sequence, row["stored"]!);
            var latest = await store.LoadLatestAsync(threadId);
            Assert.Equal(sequence, latest!.Sequence);
            StoreContracts.AssertSame(decoded[(int)sequence], latest.Checkpoint);
        }

        Assert.Equal([Offloaded, Offloaded, Offloaded, Offloaded], await backend.Versions(threadId));
        AssertLog(decoded, await Log(store, threadId));
        var page = (await ((ICheckpointQuery)store).GetLogAsync(threadId, afterSequence: 1, limit: 1)).Entries;
        StoreContracts.AssertSame(decoded[2], Assert.Single(page).Checkpoint);

        if (store is RedisCheckpointStore redis)
        {
            await redis.RebuildIndexAsync();
        }

        var listed = Assert.Single((await ((ICheckpointQuery)store).ListThreadsAsync(new ThreadQuery())).Threads);
        Assert.Equal((threadId, 3L, "1.0.0"), (listed.ThreadId, listed.Sequence, listed.SchemaVersion));
    }

    // --- Offloaded once, inherited ---

    [Fact]
    public async Task Sqlite_Offloads_A_Value_Once_Into_Its_Own_File() => await OffloadedOnceAsync(Sqlite(), Blobs.Own());

    [Fact]
    public async Task Sqlite_Offloads_A_Value_Once_Into_A_Blob_Store_In_Memory() => await OffloadedOnceAsync(Sqlite(), Blobs.Memory());

    [Fact]
    public async Task Sqlite_Offloads_A_Value_Once_Into_A_Filesystem_Blob_Store() => await OffloadedOnceAsync(Sqlite(), Blobs.Files());

    [PostgresFact]
    public async Task Postgres_Offloads_A_Value_Once_Into_Its_Own_Database() =>
        await OffloadedOnceAsync(Postgres("offload_once"), Blobs.Own());

    [PostgresFact]
    public async Task Postgres_Offloads_A_Value_Once_Into_A_Filesystem_Blob_Store() =>
        await OffloadedOnceAsync(Postgres("offload_files"), Blobs.Files());

    [RedisFact]
    public async Task Redis_Offloads_A_Value_Once_Into_Its_Own_Hash() => await OffloadedOnceAsync(Redis("offload_once"), Blobs.Own());

    [RedisFact]
    public async Task Redis_Offloads_A_Value_Once_Into_A_Blob_Store_In_Memory() =>
        await OffloadedOnceAsync(Redis("offload_memory"), Blobs.Memory());

    /// <summary>
    /// A thread whose 2 KiB evidence never changes, saved six times with deltas and offload on: the evidence is
    /// written once, the deltas inherit it, the interrupted row refers to it without writing it again, a changed
    /// value is a second blob, and both this store and one with offload off read what was saved. The store records
    /// the offloaded rows and the blobs.
    /// </summary>
    private static async Task OffloadedOnceAsync(Backend backend, Blobs blobs)
    {
        using var _ = backend;
        using var __ = blobs;
        using var recorded = new Recorded(backend.Name);
        var store = backend.Open(new Options(new DeltaOptions(), blobs.Offload()));
        var saved = new List<Checkpoint>();
        for (var step = 0; step < 6; step++)
        {
            var checkpoint = Running("t", step, step < 4 ? "e" : "f");
            if (step == 3)
            {
                checkpoint = checkpoint with { Status = CheckpointStatus.Interrupted, SchemaVersion = "1.0.0" };
            }

            await store.SaveAsync(checkpoint);
            StoreContracts.AssertSame(checkpoint, await store.LoadAsync("t"));
            saved.Add(checkpoint);
        }

        Assert.Equal([Offloaded, Delta, Delta, Offloaded, Offloaded, Delta], await backend.Versions("t"));
        var raw = await backend.Raw("t");
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse("""{"step":0}"""), raw[0]["frozen_state"]!["state"]));
        Assert.NotNull(raw[0]["frozen_state"]!["blobs"]!["evidence"]!["hash"]);
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse("""{"step":1}"""), raw[1]["frozen_state"]!["set"]));
        Assert.True(JsonNode.DeepEquals(raw[0]["frozen_state"]!["blobs"], raw[3]["frozen_state"]!["blobs"]));
        var stored = await BlobsOf(backend, blobs, "t");
        Assert.Equal(2, stored.Count);
        foreach (var (hash, content) in stored)
        {
            Assert.Equal(hash, Sha256Hash(content));
        }

        AssertLog(saved, await Log(store, "t"));
        AssertLog(saved, await Log(backend.Open(new Options(Offload: ReadOnly(blobs.Store))), "t"));
        Assert.True(recorded.Blobs >= 2, $"{recorded.Blobs} blobs were counted");
        Assert.True(recorded.BlobBytes >= 4096, $"{recorded.BlobBytes} blob bytes were recorded");
        Assert.True(recorded.Offloaded > 0, "no offloaded row was recorded");
    }

    /// <summary>
    /// Counts one store's <c>strata.store.blobs</c>, <c>strata.store.blob.size</c> and offloaded
    /// <c>strata.store.checkpoint.size</c> while it lives. The listener is the meter's, so another test's store of the
    /// same kind may add to it: tests assert at least.
    /// </summary>
    private sealed class Recorded : IDisposable
    {
        private readonly MeterListener _listener = new();
        private long _blobs;
        private long _blobBytes;
        private long _offloaded;

        public Recorded(string store)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == StorageTelemetry.Name
                    && instrument.Name is "strata.store.blobs" or "strata.store.blob.size" or "strata.store.checkpoint.size")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                var named = tags.ToArray().ToDictionary(tag => tag.Key, tag => tag.Value as string);
                if (named.GetValueOrDefault("strata.store") != store)
                {
                    return;
                }

                switch (instrument.Name)
                {
                    case "strata.store.blobs":
                        Interlocked.Add(ref _blobs, value);
                        break;
                    case "strata.store.blob.size":
                        Interlocked.Add(ref _blobBytes, value);
                        break;
                    case "strata.store.checkpoint.size" when named.GetValueOrDefault("strata.store.encoding") is "offloaded":
                        Interlocked.Increment(ref _offloaded);
                        break;
                }
            });
            _listener.Start();
        }

        public long Blobs => Interlocked.Read(ref _blobs);

        public long BlobBytes => Interlocked.Read(ref _blobBytes);

        public long Offloaded => Interlocked.Read(ref _offloaded);

        public void Dispose() => _listener.Dispose();
    }

    // --- A missing and a tampered blob ---

    [Fact]
    public async Task Sqlite_Refuses_A_Missing_Or_Tampered_Blob() => await MissingOrTamperedAsync(Sqlite());

    [PostgresFact]
    public async Task Postgres_Refuses_A_Missing_Or_Tampered_Blob() => await MissingOrTamperedAsync(Postgres("offload_tampered"));

    [RedisFact]
    public async Task Redis_Refuses_A_Missing_Or_Tampered_Blob() => await MissingOrTamperedAsync(Redis("offload_tampered"));

    /// <summary>
    /// A row whose blob no longer matches its hash, or is gone, fails the load with a message that names the row,
    /// the blob and the key.
    /// </summary>
    private static async Task MissingOrTamperedAsync(Backend backend)
    {
        using var _ = backend;
        var store = backend.Open(new Options(Offload: Offload()));
        await store.SaveAsync(Running("t", 0));
        var hash = Assert.Single(await Hashes(backend, "t"));

        await backend.PutOwnBlob("t", hash, Encoding.UTF8.GetBytes("""{"log":"tampered"}"""));
        var mismatch = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.LoadAsync("t"));
        Assert.Equal($"checkpoint 't' #0 refers to blob {hash} at 'evidence', whose content doesn't match", mismatch.Message);

        await backend.DeleteOwnBlobs();
        var missing = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.LoadAsync("t"));
        Assert.Equal($"checkpoint 't' #0 refers to blob {hash} at 'evidence', which the blob store doesn't have", missing.Message);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await ((ICheckpointQuery)store).GetLogAsync("t"));
    }

    [Fact]
    public async Task A_Blob_Missing_From_A_Filesystem_Blob_Store_Fails_The_Load()
    {
        using var backend = Sqlite();
        using var blobs = Blobs.Files();
        var store = backend.Open(new Options(Offload: blobs.Offload()));
        await store.SaveAsync(Running("t", 0));
        Directory.Delete(((FileSystemCheckpointBlobStore)blobs.Store!).ThreadDirectory("t"), recursive: true);
        var missing = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.LoadAsync("t"));
        Assert.EndsWith("which the blob store doesn't have", missing.Message, StringComparison.Ordinal);
    }

    // --- Retention ---

    [Fact]
    public async Task Sqlite_Purges_A_Threads_Blobs() => await PurgedAsync(Sqlite(), Blobs.Own());

    [Fact]
    public async Task Sqlite_Purges_A_Threads_Blobs_From_A_Filesystem_Blob_Store() => await PurgedAsync(Sqlite(), Blobs.Files());

    [PostgresFact]
    public async Task Postgres_Purges_A_Threads_Blobs() => await PurgedAsync(Postgres("offload_purge"), Blobs.Own());

    [PostgresFact]
    public async Task Postgres_Purges_A_Threads_Blobs_From_A_Blob_Store_In_Memory() =>
        await PurgedAsync(Postgres("offload_purge_memory"), Blobs.Memory());

    [RedisFact]
    public async Task Redis_Purges_A_Threads_Blobs() => await PurgedAsync(Redis("offload_purge"), Blobs.Own());

    /// <summary>A purge deletes a finished thread's blobs with its rows, wherever they are, and leaves another thread's.</summary>
    private static async Task PurgedAsync(Backend backend, Blobs blobs)
    {
        using var _ = backend;
        using var __ = blobs;
        var store = backend.Open(new Options(Offload: blobs.Offload()));
        await store.SaveAsync(Running("done", 0));
        await store.SaveAsync(Finished("done", CheckpointStatus.Completed, "2026-09-01T00:00:00Z"));
        await store.SaveAsync(Running("kept", 0, "k"));
        Assert.Single(await BlobsOf(backend, blobs, "done"));

        var purged = await ((ICheckpointRetention)store).PurgeAsync(DateTimeOffset.UtcNow);

        Assert.Equal(1, purged);
        Assert.Empty(await BlobsOf(backend, blobs, "done"));
        Assert.Single(await BlobsOf(backend, blobs, "kept"));
        Assert.NotNull(await store.LoadAsync("kept"));
    }

    [RedisFact]
    public async Task Redis_Expires_A_Finished_Threads_Blob_Hash_With_Its_List()
    {
        using var sandbox = new RedisSandbox("offload_ttl");
        using var store = new RedisCheckpointStore(sandbox.Database, new RedisStoreOptions
        {
            Prefix = sandbox.Prefix,
            Offload = Offload(),
            FinishedThreadTtl = TimeSpan.FromMinutes(10),
        });
        async Task<TimeSpan?> Ttl(string key) => await sandbox.Database.KeyTimeToLiveAsync($"{sandbox.Prefix}:{key}");

        await store.SaveAsync(Running("t", 0));
        Assert.True(await sandbox.Database.KeyExistsAsync($"{sandbox.Prefix}:blobs:t"));
        Assert.Null(await Ttl("blobs:t"));
        await store.SaveAsync(Finished("t", CheckpointStatus.Completed, "2026-10-15T01:00:00Z"));
        Assert.NotNull(await Ttl("blobs:t"));
        Assert.NotNull(await Ttl("checkpoints:t"));
        await store.SaveAsync(Running("t", 2));
        Assert.Null(await Ttl("blobs:t"));
    }

    // --- Schema version 2 ---

    [Fact]
    public async Task A_Version_One_Sqlite_File_Gains_The_Blob_Table()
    {
        var path = Path.Combine(Path.GetTempPath(), $"strata-offload-v2-{Guid.NewGuid():N}.db");
        EfCoreCheckpointStore Open() =>
            new(new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={path}").Options));
        try
        {
            Open().Dispose();
            await using (var connection = new SqliteConnection($"Data Source={path};Pooling=False"))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    "DROP TABLE checkpoint_blobs; UPDATE strata_schema_version SET version = 1 WHERE component = 'checkpoints'";
                await command.ExecuteNonQueryAsync();
            }

            Open().Dispose();

            await using (var connection = new SqliteConnection($"Data Source={path};Pooling=False"))
            {
                await connection.OpenAsync();
                await using var version = connection.CreateCommand();
                version.CommandText = "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'";
                Assert.Equal(StoreSchemaVersions.CheckpointsVersion, (long)(await version.ExecuteScalarAsync())!);
                await using var info = connection.CreateCommand();
                info.CommandText = "PRAGMA table_info(checkpoint_blobs)";
                var columns = new JsonArray();
                await using var reader = await info.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    columns.Add(new JsonObject
                    {
                        ["name"] = reader.GetString(1),
                        ["type"] = reader.GetString(2),
                        ["notnull"] = reader.GetInt64(3) != 0,
                        ["pk"] = reader.GetInt64(5),
                    });
                }

                var expected = CheckpointQueryContractTests.Fixture(Path.Combine("checkpoint-log", "blobs-columns.json"));
                Assert.True(JsonNode.DeepEquals(expected, columns), columns.ToJsonString());
            }
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            File.Delete(path);
        }
    }

    [PostgresFact]
    public async Task A_Version_One_Postgres_Database_Gains_The_Blob_Table()
    {
        using var sandbox = new PostgresSandbox("offload_v2");
        var dataSource = sandbox.CreateDataSource();
        PostgresSandbox.Open(dataSource).Dispose();
        await using (var downgrade = dataSource.CreateCommand(
            "DROP TABLE checkpoint_blobs; UPDATE strata_schema_version SET version = 1 WHERE component = 'checkpoints'"))
        {
            await downgrade.ExecuteNonQueryAsync();
        }

        PostgresSandbox.Open(dataSource).Dispose();

        await using (var version = dataSource.CreateCommand(
            "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'"))
        {
            Assert.Equal(StoreSchemaVersions.CheckpointsVersion, (long)(await version.ExecuteScalarAsync())!);
        }

        await using var info = dataSource.CreateCommand(
            """
            SELECT c.column_name::text, c.data_type::text, c.is_nullable::text = 'NO', COALESCE(k.ordinal_position, 0)::int
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name, kcu.ordinal_position
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = current_schema() AND tc.table_name = 'checkpoint_blobs'
            ) k ON k.column_name = c.column_name
            WHERE c.table_schema = current_schema() AND c.table_name = 'checkpoint_blobs'
            ORDER BY c.ordinal_position
            """);
        var columns = new JsonArray();
        await using var reader = await info.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columns.Add(new JsonObject
            {
                ["name"] = reader.GetString(0),
                ["type"] = reader.GetString(1),
                ["notnull"] = reader.GetBoolean(2),
                ["pk"] = reader.GetInt32(3),
            });
        }

        var expected = CheckpointQueryContractTests.Fixture(Path.Combine("checkpoint-log", "postgres-blobs-columns.json"));
        Assert.True(JsonNode.DeepEquals(expected, columns), columns.ToJsonString());
    }

    // --- A base row another process replaced ---

    [Fact]
    public async Task Sqlite_Does_Not_Rely_On_A_Row_Another_Process_Replaced() => await BaseReplacedAsync(Sqlite());

    [PostgresFact]
    public async Task Postgres_Does_Not_Rely_On_A_Row_Another_Process_Replaced() =>
        await BaseReplacedAsync(Postgres("offload_replaced"));

    [RedisFact]
    public async Task Redis_Does_Not_Rely_On_A_Row_Another_Process_Replaced() => await BaseReplacedAsync(Redis("offload_replaced"));

    /// <summary>
    /// A store remembers the thread's latest row (a delta's base, and the blobs it needn't write again). Behind its
    /// back the thread is purged and run again by another process up to the same sequence. Its next save must not
    /// take that row for the one it remembers: it is written whole, with its blob written again, and the thread reads
    /// back as saved (<c>design/41-Checkpoint-Efficiency.md</c>, PR 3: the base row's token).
    /// </summary>
    private static async Task BaseReplacedAsync(Backend backend)
    {
        using var _ = backend;
        var store = backend.Open(new Options(new DeltaOptions(), Offload()));
        await store.SaveAsync(Running("t", 0));
        await store.SaveAsync(Running("t", 1));

        await backend.DeleteThread("t");
        var other = backend.Open();
        await other.SaveAsync(Running("t", 40, "x"));
        await other.SaveAsync(Running("t", 41, "x"));

        await store.SaveAsync(Running("t", 2));

        Assert.Equal([Whole, Whole, Offloaded], await backend.Versions("t"));
        Assert.Null((await backend.Raw("t"))[2]["frozen_state"]!["delta_of"]);
        AssertLog([Running("t", 40, "x"), Running("t", 41, "x"), Running("t", 2)], await Log(store, "t"));

        // Offload alone: a save that isn't a delta but leaves out a blob it remembers relies on the row before it just
        // the same.
        var offloading = backend.Open(new Options(Offload: Offload()));
        await offloading.SaveAsync(Running("u", 0));
        await backend.DeleteThread("u");
        await other.SaveAsync(Running("u", 40, "x"));
        await offloading.SaveAsync(Running("u", 1));
        Assert.Equal([Whole, Offloaded], await backend.Versions("u"));
        AssertLog([Running("u", 40, "x"), Running("u", 1)], await Log(offloading, "u"));
    }

    // --- Group commit ---

    [Fact]
    public async Task Sqlite_Offloads_Through_Group_Commit() => await GroupCommitAsync(Sqlite(), Blobs.Own());

    [Fact]
    public async Task Sqlite_Offloads_Through_Group_Commit_Into_A_Filesystem_Blob_Store() =>
        await GroupCommitAsync(Sqlite(), Blobs.Files());

    [PostgresFact]
    public async Task Postgres_Offloads_Through_Group_Commit() => await GroupCommitAsync(Postgres("offload_flush"), Blobs.Own());

    /// <summary>Concurrent saves of several threads through group commit, with offload: each thread's evidence is written once, and every log reads back.</summary>
    private static async Task GroupCommitAsync(Backend backend, Blobs blobs)
    {
        using var _ = backend;
        using var __ = blobs;
        var store = backend.Open(new Options(new DeltaOptions(), blobs.Offload(), new GroupCommitOptions()));
        var threads = Enumerable.Range(0, 8).Select(n => $"g{n}").ToList();
        for (var step = 0; step < 4; step++)
        {
            await Task.WhenAll(threads.Select(thread => store.SaveAsync(Running(thread, step)).AsTask())).WaitAsync(Patience);
        }

        foreach (var thread in threads)
        {
            Assert.Single(await BlobsOf(backend, blobs, thread));
            AssertLog([.. Enumerable.Range(0, 4).Select(step => Running(thread, step))], await Log(store, thread));
        }
    }

    // --- The engine and the store contracts ---

    private sealed record Counter(int Step, string Payload);

    [Fact]
    public async Task Sqlite_Keeps_A_Durable_Run_Offloaded() => await DurableRunAsync(Sqlite());

    [PostgresFact]
    public async Task Postgres_Keeps_A_Durable_Run_Offloaded() => await DurableRunAsync(Postgres("offload_run"));

    [RedisFact]
    public async Task Redis_Keeps_A_Durable_Run_Offloaded() => await DurableRunAsync(Redis("offload_run"));

    /// <summary>
    /// A graph that checkpoints every node, run against a store with deltas and offload: the payload is written once,
    /// and the log decodes to what the same run leaves in the in-memory store.
    /// </summary>
    private static async Task DurableRunAsync(Backend backend)
    {
        using var _ = backend;
        var builder = new StateGraph<Counter>();
        for (var node = 1; node <= 6; node++)
        {
            builder.AddNode($"N{node}", s => s with { Step = s.Step + 1 });
            if (node > 1)
            {
                builder.AddEdge($"N{node - 1}", $"N{node}");
            }
        }

        var graph = builder.Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });
        var initial = new Counter(0, new string('p', 4096));
        var store = backend.Open(new Options(new DeltaOptions(), Offload()));
        Assert.IsType<GraphExecutionResult<Counter>.Completed>(await graph.RunAsync("run", initial, store));
        var memory = new InMemoryCheckpointStore();
        Assert.IsType<GraphExecutionResult<Counter>.Completed>(await graph.RunAsync("run", initial, memory));

        Assert.Single(await backend.OwnBlobs("run"));
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

    /// <summary>Every value offloaded: the contracts' small states put every top-level value in the blob store.</summary>
    private static readonly OffloadOptions Everything = new() { Threshold = 1 };

    [Fact]
    public async Task Sqlite_Satisfies_The_Store_Contracts_With_Offload()
    {
        using var backend = Sqlite();
        await ContractsAsync((EfCoreCheckpointStore)backend.Open(new Options(new DeltaOptions(), Everything)));
        using var grouped = Sqlite();
        await ContractsAsync((EfCoreCheckpointStore)grouped.Open(new Options(new DeltaOptions(), Everything, new GroupCommitOptions())));
        using var files = Blobs.Files();
        using var elsewhere = Sqlite();
        await ContractsAsync((EfCoreCheckpointStore)elsewhere.Open(new Options(Offload: Everything with { Blobs = files.Store })));
    }

    [PostgresFact]
    public async Task Postgres_Satisfies_The_Store_Contracts_With_Offload()
    {
        using var backend = Postgres("offload_contracts");
        await ContractsAsync((PostgresCheckpointStore)backend.Open(new Options(new DeltaOptions(), Everything)));
        using var grouped = Postgres("offload_contracts_grouped");
        await ContractsAsync(
            (PostgresCheckpointStore)grouped.Open(new Options(new DeltaOptions(), Everything, new GroupCommitOptions())));
    }

    [RedisFact]
    public async Task Redis_Satisfies_The_Store_Contracts_With_Offload()
    {
        using var backend = Redis("offload_contracts");
        await ContractsAsync((RedisCheckpointStore)backend.Open(new Options(new DeltaOptions(), Everything)));
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
        Assert.Throws<ArgumentOutOfRangeException>(() => new OffloadOptions { Threshold = 0 }.Checked());
        Assert.Equal(64 * 1024, new OffloadOptions().Threshold);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            EfCoreCheckpointStore.CreateInMemory(options: new SqliteStoreOptions { Offload = new OffloadOptions { Threshold = 0 } }));
        // No constructor touches the database, so the refusal needs none.
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new PostgresCheckpointStore("Host=localhost", new PostgresStoreOptions { Offload = new OffloadOptions { Threshold = 0 } }));
    }
}
