using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Storage.Postgres;
using Datalium.Strata.Storage.Redis;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;
using StackExchange.Redis;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// Read-only checkpoint stores (<c>design/51-Read-Only-Store-Open.md</c>, FR-MP-12): opening a store read-only creates
/// nothing, never migrates, its reads never write and every write is refused. Per store: a seeded store is snapshotted,
/// opened read-only and read every way, and found unchanged, and the reads match a writable store's; each refusal at open
/// changes nothing; the shared query cases, a version-1 database and offloaded logs are read; every write is refused with
/// the shared message; and the database itself refuses a write on the store's connection. The messages come from
/// <c>schema/fixtures/checkpoint-log/read-only-errors.json</c>. SQLite always runs; Postgres and Redis when
/// <c>STRATA_TEST_POSTGRES_URL</c> / <c>STRATA_TEST_REDIS_URL</c> are set. Mirrors <c>strata-memory</c>'s read-only store
/// tests.
/// </summary>
public sealed class ReadOnlyStoreTests
{
    // --- The shared messages ---

    private static JsonNode Errors => CheckpointQueryContractTests.Fixture(Path.Combine("checkpoint-log", "read-only-errors.json"));

    /// <summary><paramref name="template"/> with each <c>{name}</c> replaced by its value.</summary>
    private static string Fill(string template, params (string Name, object Value)[] values)
    {
        foreach (var (name, value) in values)
        {
            Assert.Contains($"{{{name}}}", template);
            template = template.Replace($"{{{name}}}", Convert.ToString(value, CultureInfo.InvariantCulture), StringComparison.Ordinal);
        }

        return template;
    }

    /// <summary>The fixture's message for refusing to open, as <paramref name="name"/>.</summary>
    private static string OpenError(string name, params (string Name, object Value)[] values) =>
        Fill(Errors["open"]![name]!.GetValue<string>(), values);

    /// <summary>The fixture's message for refusing <paramref name="operation"/>.</summary>
    private static string WriteError(string operation) =>
        Fill(Errors["write"]!["template"]!.GetValue<string>(), ("operation", operation));

    private static List<string> WriteOperations => [.. Errors["write"]!["operations"]!.AsArray().Select(o => o!.GetValue<string>())];

    private static long Known => StoreSchemaVersions.CheckpointsVersion;

    // --- What a store is seeded with ---

    /// <summary>A thread whose evidence is offloaded.</summary>
    private const string OffloadedThread = "read-only-offloaded";

    /// <summary>A thread of the seed, which a lease holds.</summary>
    private const string LeasedThread = "failed-acme";

    /// <summary>A thread in a Redis index whose list was deleted behind every store's back.</summary>
    private const string StaleThread = "read-only-stale";

    private const int Threshold = 1024;

    private static readonly TimeSpan LeaseTtl = TimeSpan.FromHours(1);

    /// <summary>A running checkpoint of <see cref="OffloadedThread"/> whose 2 KiB evidence is offloaded.</summary>
    private static Checkpoint Offloaded(int step) => new()
    {
        SchemaVersion = "1.7.0",
        ThreadId = OffloadedThread,
        Status = CheckpointStatus.Running,
        SuspendedNode = "A",
        NextNode = "B",
        FrozenState = new JsonObject
        {
            ["step"] = step,
            ["evidence"] = new JsonObject { ["log"] = new string('e', 2048) + step.ToString(CultureInfo.InvariantCulture) },
        },
        Metadata = new Dictionary<string, string> { ["strata.hops"] = $"{step}" },
        CreatedAt = $"2026-08-01T00:00:{step:00}Z",
    };

    private static List<Checkpoint> OffloadedLog => [.. Enumerable.Range(0, 3).Select(Offloaded)];

    /// <summary>Deltas and offload into the store's own database (or <paramref name="blobs"/>): what the seed is written with.</summary>
    private static OffloadOptions Offload(ICheckpointBlobStore? blobs = null) => new() { Threshold = Threshold, Blobs = blobs };

    /// <summary>The query cases' seed, an offloaded log and a held lease.</summary>
    private static async Task SeedAsync(ICheckpointStore store)
    {
        await CheckpointQueryContractTests.SaveSeedAsync(store);
        foreach (var checkpoint in OffloadedLog)
        {
            await store.SaveAsync(checkpoint);
        }

        Assert.NotNull((await store.TryAcquireLeaseAsync(LeasedThread, "worker-1", LeaseTtl)).Lease);
    }

    private static List<string> SeededThreads =>
        [.. CheckpointQueryContractTests.Seed().Select(c => c.ThreadId).Append(OffloadedThread).Distinct().Order(StringComparer.Ordinal)];

    // --- Reading everything ---

    /// <summary>
    /// Every read of <paramref name="store"/>, as text: every page of the default listing and of each shared query case,
    /// then for each of <paramref name="threads"/> its whole log (two entries a page), its latest checkpoint loaded both
    /// ways and its lease. <paramref name="exactPages"/> keeps the page boundaries; Redis may page short, so it is held to
    /// the threads and their order.
    /// </summary>
    private static async Task<string> ReadEverythingAsync(ICheckpointStore store, IEnumerable<string> threads, bool exactPages = true)
    {
        var query = (ICheckpointQuery)store;
        var transcript = new JsonObject();
        var cases = CheckpointQueryContractTests.Fixture(Path.Combine("checkpoint-query", "cases.json"));
        List<ThreadQuery> queries = [new ThreadQuery(), .. cases["list"]!.AsArray().Select(c => CheckpointQueryContractTests.Query(c!["query"]!))];
        var listings = new JsonArray();
        foreach (var start in queries)
        {
            var pages = new JsonArray();
            var next = start;
            while (true)
            {
                var page = await query.ListThreadsAsync(next);
                pages.Add(new JsonArray([.. page.Threads.Select(CheckpointQueryContractTests.SummaryJson)]));
                if (page.NextCursor is null)
                {
                    break;
                }

                next = next with { Cursor = page.NextCursor };
                Assert.True(pages.Count < 50, "the cursor never ended");
            }

            listings.Add(exactPages ? pages : new JsonArray([.. pages.SelectMany(p => p!.AsArray()).Select(s => s!.DeepClone())]));
        }

        transcript["listings"] = listings;
        var now = DateTimeOffset.UtcNow;
        foreach (var threadId in threads)
        {
            var log = new JsonArray();
            long? after = null;
            do
            {
                var page = await query.GetLogAsync(threadId, after, 2);
                foreach (var entry in page.Entries)
                {
                    log.Add(new JsonObject { ["sequence"] = entry.Sequence, ["checkpoint"] = entry.Checkpoint.ToWireJson() });
                }

                after = page.NextAfterSequence;
            }
            while (after is not null);

            var latest = await store.LoadLatestAsync(threadId);
            var lease = await store.GetLeaseAsync(threadId);
            transcript[threadId] = new JsonObject
            {
                ["log"] = log,
                ["load"] = (await store.LoadAsync(threadId))?.ToWireJson(),
                ["latest"] = latest is null
                    ? null
                    : new JsonObject { ["sequence"] = latest.Sequence, ["checkpoint"] = latest.Checkpoint.ToWireJson() },

                // Redis computes a lease's expiry from its clock at each read: the owner, the token and that it is live.
                ["lease"] = lease is null
                    ? null
                    : new JsonObject { ["owner"] = lease.Owner, ["token"] = lease.Token, ["live"] = lease.ExpiresAt > now },
            };
        }

        return transcript.ToJsonString();
    }

    /// <summary>
    /// Calls every write of <paramref name="store"/>, and each of <paramref name="more"/>, and finds each refused with the
    /// fixture's message, the fixture's every operation among them.
    /// </summary>
    private static async Task AssertEveryWriteRefusedAsync(ICheckpointStore store, params (string Operation, Func<Task> Write)[] more)
    {
        var checkpoint = CheckpointQueryContractTests.Seed()[0];
        var lease = new ThreadLease(checkpoint.ThreadId, "worker-1", 1, DateTimeOffset.UtcNow.AddHours(1));
        List<(string Operation, Func<Task> Write)> writes =
        [
            ("save", async () => await store.SaveAsync(checkpoint)),
            ("save", () => Task.Run(() => store.Save(checkpoint))),
            ("save", async () => await store.SaveIfLatestAsync(checkpoint, 0)),
            ("save", async () => await store.SaveIfLatestAsync(checkpoint, null)),
            ("save", async () => await store.SaveUnderLeaseAsync(checkpoint, lease)),
            ("save", async () => await store.SaveUnderLeaseAsync(checkpoint, lease, 0)),
            ("acquire lease", async () => await store.TryAcquireLeaseAsync("read-only-lease", "worker-2", TimeSpan.FromMinutes(1))),
            ("renew lease", async () => await store.RenewLeaseAsync(lease, TimeSpan.FromMinutes(1))),
            ("release lease", async () => await store.ReleaseLeaseAsync(lease)),
            ("purge", async () => await ((ICheckpointRetention)store).PurgeAsync(DateTimeOffset.UtcNow)),
            .. more,
        ];
        // Every operation named is one the fixture lists, and every operation all stores share is called. Postgres's
        // "migrate" and Redis's "rebuild index" come in through more.
        string[] storeSpecific = ["migrate", "rebuild index"];
        var called = writes.Select(w => w.Operation).ToHashSet(StringComparer.Ordinal);
        Assert.Subset(WriteOperations.ToHashSet(StringComparer.Ordinal), called);
        Assert.Superset(WriteOperations.Except(storeSpecific).ToHashSet(StringComparer.Ordinal), called);
        foreach (var (operation, write) in writes)
        {
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(write);
            Assert.Equal(WriteError(operation), refused.Message);
        }
    }

    // --- SQLite ---

    private sealed class TempDirectory : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"strata-read-only-{Guid.NewGuid():N}");

        public TempDirectory() => Directory.CreateDirectory(Path);

        public string File(string name) => System.IO.Path.Combine(Path, name);

        public void Dispose()
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(Path))
            {
                Directory.Delete(Path, recursive: true);
            }
        }
    }

    /// <summary>A writable store on the file at <paramref name="path"/>, not pooled, so nothing keeps the file open after it.</summary>
    private static EfCoreCheckpointStore WritableSqlite(string path, SqliteStoreOptions? options = null) => new(
        new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={path};Pooling=False").Options),
        ownedConnection: null,
        timeProvider: null,
        options);

    /// <summary>A SQLite file seeded by <see cref="SeedAsync"/>, written with deltas and offload.</summary>
    private static async Task<string> SeededSqliteAsync(TempDirectory dir, string name = "store.db")
    {
        var path = dir.File(name);
        using (var store = WritableSqlite(path, new SqliteStoreOptions { Deltas = new DeltaOptions(), Offload = Offload() }))
        {
            await SeedAsync(store);
        }

        return path;
    }

    private static async Task ExecuteSqliteAsync(string path, string sql)
    {
        await using var connection = new SqliteConnection($"Data Source={path};Pooling=False");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<object?> QuerySqliteAsync(string path, string sql)
    {
        await using var connection = new SqliteConnection($"Data Source={path};Pooling=False;Mode=ReadOnly");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }

    /// <summary>Every file under <paramref name="dir"/> with the SHA-256 of its content: a journal, WAL or shm file appearing shows.</summary>
    private static string FilesOf(string dir) => string.Join(
        "\n",
        Directory.GetFiles(dir, "*", SearchOption.AllDirectories)
            .Order(StringComparer.Ordinal)
            .Select(file => $"{Path.GetRelativePath(dir, file)} {Convert.ToHexStringLower(SHA256.HashData(File.ReadAllBytes(file)))}"));

    [Fact]
    public async Task Opening_a_SQLite_file_read_only_changes_nothing_and_reads_as_a_writable_store()
    {
        using var dir = new TempDirectory();
        var path = await SeededSqliteAsync(dir);
        var before = FilesOf(dir.Path);
        Assert.Single(Directory.GetFiles(dir.Path));

        string readOnly;
        using (var store = await EfCoreCheckpointStore.OpenReadOnlyAsync(path))
        {
            Assert.True(store.IsReadOnly);
            readOnly = await ReadEverythingAsync(store, SeededThreads);
            await AssertEveryWriteRefusedAsync(store);
        }

        Assert.Equal(before, FilesOf(dir.Path));

        using (var writable = WritableSqlite(path))
        {
            Assert.False(writable.IsReadOnly);
            Assert.Equal(await ReadEverythingAsync(writable, SeededThreads), readOnly);
        }
    }

    [UnixFact]
    public async Task A_SQLite_file_and_directory_made_read_only_are_read_the_same()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        using var dir = new TempDirectory();
        var path = await SeededSqliteAsync(dir);
        string expected;
        using (var store = await EfCoreCheckpointStore.OpenReadOnlyAsync(path))
        {
            expected = await ReadEverythingAsync(store, SeededThreads);
        }

        var before = FilesOf(dir.Path);
        File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.GroupRead | UnixFileMode.OtherRead);
        File.SetUnixFileMode(dir.Path, UnixFileMode.UserRead | UnixFileMode.UserExecute | UnixFileMode.GroupRead | UnixFileMode.GroupExecute);
        try
        {
            using var store = await EfCoreCheckpointStore.OpenReadOnlyAsync(path);
            Assert.Equal(expected, await ReadEverythingAsync(store, SeededThreads));
        }
        finally
        {
            File.SetUnixFileMode(dir.Path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }

        Assert.Equal(before, FilesOf(dir.Path));
    }

    [Fact]
    public async Task A_read_only_SQLite_store_answers_the_shared_query_cases()
    {
        using var dir = new TempDirectory();
        var path = dir.File("cases.db");
        using (var writable = WritableSqlite(path))
        {
            await CheckpointQueryContractTests.SaveSeedAsync(writable);
        }

        using var store = await EfCoreCheckpointStore.OpenReadOnlyAsync(path);
        await CheckpointQueryContractTests.RunCasesAsync(store, exact: true);
    }

    [Fact]
    public async Task A_missing_SQLite_file_is_refused_and_not_created()
    {
        using var dir = new TempDirectory();
        var path = dir.File("missing.db");
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => EfCoreCheckpointStore.OpenReadOnlyAsync(path));
        Assert.Equal(OpenError("missing_sqlite_file", ("path", path)), refused.Message);
        Assert.Empty(Directory.GetFileSystemEntries(dir.Path));
    }

    [Fact]
    public async Task A_SQLite_file_with_no_Strata_log_is_refused_and_left_as_it_was()
    {
        using var dir = new TempDirectory();
        var empty = dir.File("empty.db");
        await File.WriteAllBytesAsync(empty, []);
        var other = dir.File("other.db");
        await ExecuteSqliteAsync(other, "CREATE TABLE other (x TEXT)");
        var before = FilesOf(dir.Path);
        foreach (var path in (string[])[empty, other])
        {
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => EfCoreCheckpointStore.OpenReadOnlyAsync(path));
            Assert.Equal(OpenError("no_strata_log", ("store", path)), refused.Message);
        }

        Assert.Equal(before, FilesOf(dir.Path));
    }

    [Fact]
    public async Task A_SQLite_file_at_version_0_or_newer_than_the_build_is_refused_and_left_as_it_was()
    {
        using var dir = new TempDirectory();

        // A log from before the version table: the checkpoints table only.
        var zero = dir.File("zero.db");
        await ExecuteSqliteAsync(zero, EfCoreCheckpointStore.CheckpointLogDdl);
        var newer = await SeededSqliteAsync(dir, "newer.db");
        await ExecuteSqliteAsync(newer, $"UPDATE strata_schema_version SET version = {Known + 1} WHERE component = 'checkpoints'");
        var before = FilesOf(dir.Path);

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => EfCoreCheckpointStore.OpenReadOnlyAsync(zero));
        Assert.Equal(OpenError("version_zero", ("known", Known)), refused.Message);
        refused = await Assert.ThrowsAsync<InvalidOperationException>(() => EfCoreCheckpointStore.OpenReadOnlyAsync(newer));
        Assert.Equal(OpenError("newer_version", ("found", Known + 1), ("known", Known)), refused.Message);

        Assert.Equal(before, FilesOf(dir.Path));
        Assert.Equal(0L, await QuerySqliteAsync(zero, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'strata_schema_version'"));
        Assert.Equal(Known + 1, await QuerySqliteAsync(newer, "SELECT version FROM strata_schema_version"));
    }

    [Fact]
    public async Task A_SQLite_file_with_a_hot_journal_is_refused_and_left_as_it_was()
    {
        using var dir = new TempDirectory();
        var path = await SeededSqliteAsync(dir);
        var crashed = dir.File("crashed.db");

        // A writer in the middle of a transaction: the journal holds the pages it changed. A copy of the file and its
        // journal taken now is what a crash leaves.
        await using (var writer = new SqliteConnection($"Data Source={path};Pooling=False"))
        {
            await writer.OpenAsync();

            // A cache of one page spills the transaction's changes to the file, after their journal.
            await using (var cache = writer.CreateCommand())
            {
                cache.CommandText = "PRAGMA cache_size = 1";
                await cache.ExecuteNonQueryAsync();
            }

            await using var transaction = (SqliteTransaction)await writer.BeginTransactionAsync();
            await using (var change = writer.CreateCommand())
            {
                change.Transaction = transaction;
                change.CommandText = "UPDATE checkpoints SET frozen_state = frozen_state || ' '";
                await change.ExecuteNonQueryAsync();
            }

            Assert.True(File.Exists(path + "-journal"));
            File.Copy(path, crashed);
            File.Copy(path + "-journal", crashed + "-journal");
            await transaction.RollbackAsync();
        }

        var before = FilesOf(dir.Path);
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => EfCoreCheckpointStore.OpenReadOnlyAsync(crashed));
        Assert.Equal(OpenError("hot_journal", ("path", crashed)), refused.Message);
        Assert.Equal(before, FilesOf(dir.Path));
    }

    /// <summary>
    /// Review decision 8: a file an application switched to WAL. SQLite reads it, the database file unchanged, and what a
    /// writer still has only in its WAL is read too. But a read-only connection to a WAL file creates its <c>-wal</c> and
    /// <c>-shm</c> files, and can't remove them when it closes (that takes a checkpoint, which is a write): reading a WAL
    /// file needs a directory it can create them in, or a writer that already has. Pinned here so a change in SQLite shows.
    /// </summary>
    [Fact]
    public async Task A_WAL_SQLite_file_is_read_but_its_wal_and_shm_files_are_created()
    {
        using var dir = new TempDirectory();
        var path = await SeededSqliteAsync(dir);
        Assert.Equal("wal", await QueryWritableSqliteAsync(path, "PRAGMA journal_mode = WAL"));
        Assert.Single(Directory.GetFiles(dir.Path));
        var file = FilesOf(dir.Path);

        string expected;
        using (var writable = WritableSqlite(path))
        {
            expected = await ReadEverythingAsync(writable, SeededThreads);
        }

        Assert.Equal(file, FilesOf(dir.Path));
        using (var store = await EfCoreCheckpointStore.OpenReadOnlyAsync(path))
        {
            Assert.Equal(expected, await ReadEverythingAsync(store, SeededThreads));
        }

        Assert.Equal(
            [Path.GetFileName(path), $"{Path.GetFileName(path)}-shm", $"{Path.GetFileName(path)}-wal"],
            Directory.GetFiles(dir.Path).Select(Path.GetFileName).Order(StringComparer.Ordinal));
        Assert.Equal(file, FilesOf(dir.Path).Split('\n')[0]);

        // A writer's change still only in its WAL is read.
        await using (var writer = new SqliteConnection($"Data Source={path};Pooling=False"))
        {
            await writer.OpenAsync();
            await using (var change = writer.CreateCommand())
            {
                change.CommandText = $"PRAGMA wal_autocheckpoint = 0; DELETE FROM checkpoints WHERE thread_id = '{LeasedThread}'";
                await change.ExecuteNonQueryAsync();
            }

            using var store = await EfCoreCheckpointStore.OpenReadOnlyAsync(path);
            Assert.Empty((await store.GetLogAsync(LeasedThread)).Entries);
        }
    }

    private static async Task<object?> QueryWritableSqliteAsync(string path, string sql)
    {
        await using var connection = new SqliteConnection($"Data Source={path};Pooling=False");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }

    [Fact]
    public async Task A_version_1_SQLite_file_is_read_and_left_at_version_1()
    {
        using var dir = new TempDirectory();
        var path = dir.File("older.sqlite");
        await CrossEngineTests.WriteOlderSqliteAsync(path);
        SqliteConnection.ClearAllPools();
        var before = FilesOf(dir.Path);

        using (var store = await EfCoreCheckpointStore.OpenReadOnlyAsync(path))
        {
            await AssertOlderCheckpointsAsync(store);
        }

        Assert.Equal(before, FilesOf(dir.Path));
        Assert.Equal(1L, await QuerySqliteAsync(path, "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'"));
        Assert.Equal(0L, await QuerySqliteAsync(path, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'checkpoint_blobs'"));
    }

    /// <summary>What the cross-engine test's version-1 writer wrote, read back: each thread's latest, its log and the listing.</summary>
    private static async Task AssertOlderCheckpointsAsync<TStore>(TStore store)
        where TStore : ICheckpointStore, ICheckpointQuery
    {
        var older = CrossEngineTests.OlderCheckpoints();
        foreach (var (threadId, expected) in older)
        {
            var loaded = await store.LoadAsync(threadId);
            Assert.True(JsonNode.DeepEquals(expected, loaded?.ToWireJson()), $"{threadId}: {loaded?.ToWireJson().ToJsonString()}");
            var log = (await store.GetLogAsync(threadId)).Entries;
            Assert.Equal(0L, Assert.Single(log).Sequence);
            Assert.True(JsonNode.DeepEquals(expected, log[0].Checkpoint.ToWireJson()), threadId);
        }

        var page = await store.ListThreadsAsync(new ThreadQuery { Limit = ThreadQuery.MaxLimit });
        Assert.Equal(older.Select(o => o.ThreadId).Order(StringComparer.Ordinal), page.Threads.Select(t => t.ThreadId).Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task A_read_only_SQLite_store_reads_offloaded_logs_from_its_own_blobs_and_a_blob_directory()
    {
        using var dir = new TempDirectory();
        var own = dir.File("own.db");
        using (var writable = WritableSqlite(own, new SqliteStoreOptions { Offload = Offload() }))
        {
            await SaveOffloadedLogAsync(writable);
        }

        Assert.True(await QuerySqliteAsync(own, $"SELECT COUNT(*) FROM checkpoint_blobs WHERE thread_id = '{OffloadedThread}'") is > 0L);
        var blobDir = Path.Combine(dir.Path, "blobs");
        var elsewhere = dir.File("elsewhere.db");
        using (var writable = WritableSqlite(elsewhere, new SqliteStoreOptions { Offload = Offload(new FileSystemCheckpointBlobStore(blobDir)) }))
        {
            await SaveOffloadedLogAsync(writable);
        }

        var before = FilesOf(dir.Path);
        using (var store = await EfCoreCheckpointStore.OpenReadOnlyAsync(own))
        {
            await AssertOffloadedLogAsync(store);
        }

        var options = new SqliteStoreOptions { Offload = new OffloadOptions { Blobs = new FileSystemCheckpointBlobStore(blobDir) } };
        using (var store = await EfCoreCheckpointStore.OpenReadOnlyAsync(elsewhere, options))
        {
            await AssertOffloadedLogAsync(store);
        }

        Assert.Equal(before, FilesOf(dir.Path));
    }

    private static async Task SaveOffloadedLogAsync(ICheckpointStore store)
    {
        foreach (var checkpoint in OffloadedLog)
        {
            await store.SaveAsync(checkpoint);
        }
    }

    /// <summary>Every row of the offloaded log, and its latest, read whole.</summary>
    private static async Task AssertOffloadedLogAsync<TStore>(TStore store)
        where TStore : ICheckpointStore, ICheckpointQuery
    {
        var log = (await store.GetLogAsync(OffloadedThread)).Entries;
        Assert.Equal([0L, 1L, 2L], log.Select(e => e.Sequence));
        for (var i = 0; i < log.Count; i++)
        {
            StoreContracts.AssertSame(OffloadedLog[i], log[i].Checkpoint);
        }

        StoreContracts.AssertSame(OffloadedLog[^1], await store.LoadAsync(OffloadedThread));
    }

    [Fact]
    public async Task A_missing_blob_directory_is_refused_and_not_created()
    {
        using var dir = new TempDirectory();
        var path = await SeededSqliteAsync(dir);
        var blobs = Path.Combine(dir.Path, "no-blobs");
        var options = new SqliteStoreOptions { Offload = new OffloadOptions { Blobs = new FileSystemCheckpointBlobStore(blobs) } };
        var refused = await Assert.ThrowsAsync<DirectoryNotFoundException>(() => EfCoreCheckpointStore.OpenReadOnlyAsync(path, options));
        Assert.Equal(OpenError("missing_blob_directory", ("dir", blobs)), refused.Message);
        Assert.False(Directory.Exists(blobs));
    }

    [Fact]
    public async Task SQLite_itself_refuses_a_write_on_a_read_only_stores_connection()
    {
        using var dir = new TempDirectory();
        var path = await SeededSqliteAsync(dir);
        var before = FilesOf(dir.Path);
        using (var store = await EfCoreCheckpointStore.OpenReadOnlyAsync(path))
        {
            var connection = store.ReadOnlyConnection!;
            await using (var queryOnly = connection.CreateCommand())
            {
                queryOnly.CommandText = "PRAGMA query_only";
                Assert.Equal(1L, await queryOnly.ExecuteScalarAsync());
            }

            await using var write = connection.CreateCommand();
            write.CommandText = "INSERT INTO strata_schema_version (component, version) VALUES ('read-only', 1)";
            var refused = await Assert.ThrowsAsync<SqliteException>(() => write.ExecuteNonQueryAsync());
            Assert.Equal(8, refused.SqliteErrorCode); // SQLITE_READONLY
        }

        Assert.Equal(before, FilesOf(dir.Path));
    }

    // --- PostgreSQL ---

    /// <summary>The role the Postgres tests read as: granted only <c>CONNECT</c> (to everyone) and <c>SELECT</c>.</summary>
    private const string ReaderRole = "strata_reader_dotnet";

    /// <summary>Must equal the Postgres stores' DDL lock key.</summary>
    private const long DdlLockKey = 0x5354_5241_5441;

    private static readonly SemaphoreSlim RoleGate = new(1, 1);

    private static string AdminConnectionString => PostgresConnection.ToConnectionString(TestBackends.PostgresUrl);

    private static string InSchema(string schema) =>
        new NpgsqlConnectionStringBuilder(AdminConnectionString) { SearchPath = schema }.ConnectionString;

    private static async Task<object?> PostgresScalarAsync(string connectionString, string sql)
    {
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        await using var command = dataSource.CreateCommand(sql);
        return await command.ExecuteScalarAsync();
    }

    private static async Task<string> DatabaseNameAsync() =>
        (string)(await PostgresScalarAsync(AdminConnectionString, "SELECT current_database()"))!;

    /// <summary>
    /// The connection string of <see cref="ReaderRole"/>, created once if it isn't there (the Rust tests use their own
    /// role on the same server), allowed to read <paramref name="schema"/>.
    /// </summary>
    private static async Task<string> ReaderConnectionStringAsync(string schema)
    {
        await using var admin = NpgsqlDataSource.Create(AdminConnectionString);
        await RoleGate.WaitAsync();
        try
        {
            await using var create = admin.CreateCommand(
                $"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{ReaderRole}') THEN " +
                $"CREATE ROLE {ReaderRole} LOGIN PASSWORD '{ReaderRole}'; END IF; END $$");
            await create.ExecuteNonQueryAsync();
        }
        catch (PostgresException e) when (e.SqlState is PostgresErrorCodes.DuplicateObject or PostgresErrorCodes.UniqueViolation)
        {
            // Another process created it at the same moment.
        }
        finally
        {
            RoleGate.Release();
        }

        await using var grant = admin.CreateCommand(
            $"GRANT USAGE ON SCHEMA {schema} TO {ReaderRole}; GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {ReaderRole}");
        await grant.ExecuteNonQueryAsync();
        return new NpgsqlConnectionStringBuilder(AdminConnectionString)
        {
            SearchPath = schema,
            Username = ReaderRole,
            Password = ReaderRole,
        }.ConnectionString;
    }

    /// <summary>
    /// Everything in <paramref name="schema"/>, as text: its tables, indexes, sequences, triggers and functions, and every
    /// row of every table, the version row included.
    /// </summary>
    private static async Task<string> PostgresSnapshotAsync(string schema)
    {
        await using var dataSource = NpgsqlDataSource.Create(AdminConnectionString);
        var lines = new List<string>();
        async Task<List<string>> Read(string sql)
        {
            await using var command = dataSource.CreateCommand(sql);
            command.Parameters.Add(new NpgsqlParameter { Value = schema, NpgsqlDbType = NpgsqlDbType.Text });
            var rows = new List<string>();
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                rows.Add(reader.GetString(0));
            }

            return rows;
        }

        lines.AddRange(await Read(
            "SELECT c.relkind::text || ' ' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 ORDER BY 1"));
        lines.AddRange(await Read(
            "SELECT 'trigger ' || t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace " +
            "WHERE n.nspname = $1 AND NOT t.tgisinternal ORDER BY 1"));
        lines.AddRange(await Read(
            "SELECT 'function ' || p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1 ORDER BY 1"));
        foreach (var table in await Read(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relkind = 'r' ORDER BY 1"))
        {
            await using var command = dataSource.CreateCommand(
                $"SELECT COALESCE(string_agg(row_to_json(t)::text, E'\\n' ORDER BY row_to_json(t)::text), '') FROM \"{schema}\".\"{table}\" t");
            lines.Add($"{table}:\n{await command.ExecuteScalarAsync()}");
        }

        return string.Join("\n", lines);
    }

    /// <summary>
    /// Holds the stores' DDL advisory lock in a session of its own until disposed: a read-only store must never wait for
    /// it. Held for as long as a test reads, it stalls any migration on the server meanwhile, so it is held briefly.
    /// </summary>
    private static async Task<IAsyncDisposable> HoldDdlLockAsync()
    {
        var connection = new NpgsqlConnection(AdminConnectionString);
        await connection.OpenAsync();
        await using (var take = new NpgsqlCommand("SELECT pg_advisory_lock(@key)", connection))
        {
            take.Parameters.Add(new NpgsqlParameter("key", NpgsqlDbType.Bigint) { Value = DdlLockKey });
            await take.ExecuteNonQueryAsync();
        }

        return new Held(connection);
    }

    private sealed class Held(NpgsqlConnection connection) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await using (var release = new NpgsqlCommand("SELECT pg_advisory_unlock(@key)", connection))
            {
                release.Parameters.Add(new NpgsqlParameter("key", NpgsqlDbType.Bigint) { Value = DdlLockKey });
                await release.ExecuteNonQueryAsync();
            }

            await connection.DisposeAsync();
        }
    }

    /// <summary>A bound on a read-only open and its reads: one that waited for the DDL lock would run into it.</summary>
    private static CancellationToken Patience => new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token;

    [PostgresFact]
    public async Task Opening_a_Postgres_database_read_only_as_a_SELECT_only_role_changes_nothing_and_reads_as_a_writable_store()
    {
        using var sandbox = new PostgresSandbox("ro");
        using (var writable = new PostgresCheckpointStore(
            sandbox.CreateDataSource(), new PostgresStoreOptions { Deltas = new DeltaOptions(), Offload = Offload() }))
        {
            await SeedAsync(writable);
        }

        var reader = await ReaderConnectionStringAsync(sandbox.Schema);
        var before = await PostgresSnapshotAsync(sandbox.Schema);

        // The role can't write, whatever the session says.
        await using (var direct = NpgsqlDataSource.Create(reader))
        {
            await using var write = direct.CreateCommand("INSERT INTO strata_schema_version (component, version) VALUES ('read-only', 1)");
            Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, (await Assert.ThrowsAsync<PostgresException>(() => write.ExecuteNonQueryAsync())).SqlState);
        }

        string readOnly;
        await using (await HoldDdlLockAsync())
        {
            await using var store = await PostgresCheckpointStore.OpenReadOnlyAsync(reader, cancellationToken: Patience);
            Assert.True(store.IsReadOnly);
            readOnly = await ReadEverythingAsync(store, SeededThreads);
            await AssertEveryWriteRefusedAsync(store, ("migrate", () => store.MigrateAsync()));
        }

        Assert.Equal(before, await PostgresSnapshotAsync(sandbox.Schema));

        using var again = new PostgresCheckpointStore(sandbox.CreateDataSource());
        Assert.False(again.IsReadOnly);
        Assert.Equal(await ReadEverythingAsync(again, SeededThreads), readOnly);
    }

    [PostgresFact]
    public async Task A_read_only_Postgres_store_answers_the_shared_query_cases()
    {
        using var sandbox = new PostgresSandbox("ro_cases");
        using (var writable = new PostgresCheckpointStore(sandbox.CreateDataSource()))
        {
            await CheckpointQueryContractTests.SaveSeedAsync(writable);
        }

        await using var store = await PostgresCheckpointStore.OpenReadOnlyAsync(InSchema(sandbox.Schema));
        await CheckpointQueryContractTests.RunCasesAsync(store, exact: true);
    }

    [PostgresFact]
    public async Task A_Postgres_database_with_no_Strata_log_at_version_0_or_newer_is_refused_and_left_as_it_was()
    {
        var database = await DatabaseNameAsync();
        using var empty = new PostgresSandbox("ro_empty");
        using var zero = new PostgresSandbox("ro_zero");
        await PostgresScalarAsync(InSchema(zero.Schema), PostgresCheckpointStore.CheckpointLogDdl);
        using var newer = new PostgresSandbox("ro_newer");
        using (var writable = new PostgresCheckpointStore(newer.CreateDataSource()))
        {
            await SeedAsync(writable);
        }

        await PostgresScalarAsync(InSchema(newer.Schema), $"UPDATE strata_schema_version SET version = {Known + 1} WHERE component = 'checkpoints'");
        var before = (await PostgresSnapshotAsync(empty.Schema), await PostgresSnapshotAsync(zero.Schema), await PostgresSnapshotAsync(newer.Schema));

        await using (await HoldDdlLockAsync())
        {
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(
                () => PostgresCheckpointStore.OpenReadOnlyAsync(InSchema(empty.Schema), cancellationToken: Patience));
            Assert.Equal(OpenError("no_strata_log", ("store", database)), refused.Message);
            refused = await Assert.ThrowsAsync<InvalidOperationException>(
                () => PostgresCheckpointStore.OpenReadOnlyAsync(InSchema(zero.Schema), cancellationToken: Patience));
            Assert.Equal(OpenError("version_zero", ("known", Known)), refused.Message);
            refused = await Assert.ThrowsAsync<InvalidOperationException>(
                () => PostgresCheckpointStore.OpenReadOnlyAsync(InSchema(newer.Schema), cancellationToken: Patience));
            Assert.Equal(OpenError("newer_version", ("found", Known + 1), ("known", Known)), refused.Message);
        }

        Assert.Equal(before, (await PostgresSnapshotAsync(empty.Schema), await PostgresSnapshotAsync(zero.Schema), await PostgresSnapshotAsync(newer.Schema)));
        Assert.Equal(false, await PostgresScalarAsync(InSchema(zero.Schema), "SELECT to_regclass('strata_schema_version') IS NOT NULL"));
    }

    [PostgresFact]
    public async Task A_version_1_Postgres_database_is_read_and_left_at_version_1()
    {
        var schema = $"strata_test_{Guid.NewGuid().ToString("N")[..12]}_ro_older";
        try
        {
            await CrossEngineTests.WriteOlderPostgresAsync(TestBackends.PostgresUrl, schema);
            var before = await PostgresSnapshotAsync(schema);
            await using (var store = await PostgresCheckpointStore.OpenReadOnlyAsync(InSchema(schema)))
            {
                await AssertOlderCheckpointsAsync(store);
            }

            Assert.Equal(before, await PostgresSnapshotAsync(schema));
            Assert.Equal(1L, await PostgresScalarAsync(InSchema(schema), "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'"));
            Assert.Equal(false, await PostgresScalarAsync(InSchema(schema), "SELECT to_regclass('checkpoint_blobs') IS NOT NULL"));
        }
        finally
        {
            await PostgresScalarAsync(AdminConnectionString, $"DROP SCHEMA IF EXISTS {schema} CASCADE");
        }
    }

    [PostgresFact]
    public async Task A_read_only_Postgres_store_reads_offloaded_logs_from_its_own_blobs_and_a_blob_directory()
    {
        using var own = new PostgresSandbox("ro_own");
        using (var writable = new PostgresCheckpointStore(own.CreateDataSource(), new PostgresStoreOptions { Offload = Offload() }))
        {
            await SaveOffloadedLogAsync(writable);
        }

        using var dir = new TempDirectory();
        using var elsewhere = new PostgresSandbox("ro_elsewhere");
        var blobs = new FileSystemCheckpointBlobStore(dir.Path);
        using (var writable = new PostgresCheckpointStore(elsewhere.CreateDataSource(), new PostgresStoreOptions { Offload = Offload(blobs) }))
        {
            await SaveOffloadedLogAsync(writable);
        }

        Assert.True(await PostgresScalarAsync(InSchema(own.Schema), "SELECT COUNT(*) FROM checkpoint_blobs") is > 0L);
        var before = (await PostgresSnapshotAsync(own.Schema), await PostgresSnapshotAsync(elsewhere.Schema), FilesOf(dir.Path));
        await using (var store = await PostgresCheckpointStore.OpenReadOnlyAsync(InSchema(own.Schema)))
        {
            await AssertOffloadedLogAsync(store);
        }

        var options = new PostgresStoreOptions { Offload = new OffloadOptions { Blobs = blobs } };
        await using (var store = await PostgresCheckpointStore.OpenReadOnlyAsync(InSchema(elsewhere.Schema), options))
        {
            await AssertOffloadedLogAsync(store);
        }

        Assert.Equal(before, (await PostgresSnapshotAsync(own.Schema), await PostgresSnapshotAsync(elsewhere.Schema), FilesOf(dir.Path)));

        var missing = Path.Combine(dir.Path, "no-blobs");
        var refused = await Assert.ThrowsAsync<DirectoryNotFoundException>(() => PostgresCheckpointStore.OpenReadOnlyAsync(
            InSchema(own.Schema), new PostgresStoreOptions { Offload = new OffloadOptions { Blobs = new FileSystemCheckpointBlobStore(missing) } }));
        Assert.Equal(OpenError("missing_blob_directory", ("dir", missing)), refused.Message);
        Assert.False(Directory.Exists(missing));
    }

    [PostgresFact]
    public async Task Postgres_itself_refuses_a_write_in_a_read_only_stores_session()
    {
        using var sandbox = new PostgresSandbox("ro_enforced");
        using (var writable = new PostgresCheckpointStore(sandbox.CreateDataSource()))
        {
            await CheckpointQueryContractTests.SaveSeedAsync(writable);
        }

        var before = await PostgresSnapshotAsync(sandbox.Schema);

        // As the role that owns the tables: only the session setting stands in the way.
        await using (var store = await PostgresCheckpointStore.OpenReadOnlyAsync(InSchema(sandbox.Schema)))
        {
            await using (var show = store.DataSource.CreateCommand("SHOW default_transaction_read_only"))
            {
                Assert.Equal("on", await show.ExecuteScalarAsync());
            }

            await using var write = store.DataSource.CreateCommand(
                "INSERT INTO strata_schema_version (component, version) VALUES ('read-only', 1)");
            Assert.Equal(PostgresErrorCodes.ReadOnlySqlTransaction, (await Assert.ThrowsAsync<PostgresException>(() => write.ExecuteNonQueryAsync())).SqlState);
        }

        Assert.Equal(before, await PostgresSnapshotAsync(sandbox.Schema));
    }

    // --- Redis ---

    /// <summary>
    /// Every key under <paramref name="prefix"/>, as text: its name, type, value (<c>DUMP</c>) and when it expires
    /// (<c>PEXPIRETIME</c>, which doesn't move as time passes).
    /// </summary>
    private static async Task<string> RedisSnapshotAsync(RedisSandbox sandbox)
    {
        var server = sandbox.Connection.GetServer(sandbox.Connection.GetEndPoints()[0]);
        var lines = new List<string>();
        foreach (var key in server.Keys(pattern: $"{sandbox.Prefix}:*").Select(k => (string)k!).Order(StringComparer.Ordinal))
        {
            var type = await sandbox.Database.KeyTypeAsync(key);
            var dump = await sandbox.Database.KeyDumpAsync(key);
            var expires = await sandbox.Database.ExecuteAsync("PEXPIRETIME", key);
            lines.Add($"{key} {type} {Convert.ToBase64String(dump!)} {expires}");
        }

        return string.Join("\n", lines);
    }

    private static RedisStoreOptions Prefixed(RedisSandbox sandbox, OffloadOptions? offload = null, DeltaOptions? deltas = null) =>
        new() { Prefix = sandbox.Prefix, Offload = offload, Deltas = deltas };

    [RedisFact]
    public async Task Opening_a_Redis_prefix_read_only_changes_nothing_and_reads_as_a_writable_store()
    {
        using var sandbox = new RedisSandbox("ro");
        var writable = new RedisCheckpointStore(sandbox.Database, Prefixed(sandbox, Offload(), new DeltaOptions()));
        await SeedAsync(writable);

        // A thread in the index whose list is gone: listed, a read-only store skips it and leaves it there.
        await writable.SaveAsync(StoreContracts.Checkpoint(StaleThread, "2026-09-20T00:00:00Z"));
        await sandbox.Database.KeyDeleteAsync($"{sandbox.Prefix}:checkpoints:{StaleThread}");
        List<string> threads = [.. SeededThreads, StaleThread];
        var before = await RedisSnapshotAsync(sandbox);

        string readOnly;
        using (var store = await RedisCheckpointStore.ConnectReadOnlyAsync(TestBackends.RedisUrl, Prefixed(sandbox)))
        {
            Assert.True(store.IsReadOnly);
            readOnly = await ReadEverythingAsync(store, threads, exactPages: false);
            await AssertEveryWriteRefusedAsync(store, ("rebuild index", async () => await store.RebuildIndexAsync()));
        }

        Assert.Equal(before, await RedisSnapshotAsync(sandbox));
        Assert.NotNull(await sandbox.Database.SortedSetScoreAsync($"{sandbox.Prefix}:threads", StaleThread));

        var again = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        Assert.False(again.IsReadOnly);
        Assert.Equal(await ReadEverythingAsync(again, threads, exactPages: false), readOnly);

        // A writable store's listing takes the stale thread out of the index.
        Assert.Null(await sandbox.Database.SortedSetScoreAsync($"{sandbox.Prefix}:threads", StaleThread));
    }

    [RedisFact]
    public async Task A_read_only_Redis_store_answers_the_shared_query_cases()
    {
        using var sandbox = new RedisSandbox("ro_cases");
        await CheckpointQueryContractTests.SaveSeedAsync(new RedisCheckpointStore(sandbox.Database, sandbox.Prefix));
        using var store = await RedisCheckpointStore.ConnectReadOnlyAsync(TestBackends.RedisUrl, Prefixed(sandbox));
        await CheckpointQueryContractTests.RunCasesAsync(store, exact: false);
    }

    [RedisFact]
    public async Task A_read_only_Redis_store_ignores_write_options_a_writable_store_would_refuse()
    {
        using var sandbox = new RedisSandbox("ro_write_options");
        await CheckpointQueryContractTests.SaveSeedAsync(new RedisCheckpointStore(sandbox.Database, sandbox.Prefix));

        // Each of these makes the writable constructor throw; the read-only store doesn't write, so it opens.
        using var store = await RedisCheckpointStore.ConnectReadOnlyAsync(
            TestBackends.RedisUrl,
            new RedisStoreOptions
            {
                Prefix = sandbox.Prefix,
                FinishedThreadTtl = TimeSpan.Zero,
                Deltas = new DeltaOptions { FullEvery = 1 },
                Offload = new OffloadOptions { Threshold = 0 },
            });
        Assert.True(store.IsReadOnly);
        await CheckpointQueryContractTests.RunCasesAsync(store, exact: false);
    }

    [RedisFact]
    public async Task A_Redis_prefix_with_no_thread_index_is_refused_and_the_marker_not_created()
    {
        using var empty = new RedisSandbox("ro_empty");

        // A prefix only a runtime from before the index wrote to: a list, and no marker.
        using var unindexed = new RedisSandbox("ro_unindexed");
        await unindexed.Database.ListRightPushAsync(
            $"{unindexed.Prefix}:checkpoints:old", StoreContracts.Checkpoint("old", "2026-09-20T00:00:00Z").ToWireJson().ToJsonString());
        var before = await RedisSnapshotAsync(unindexed);

        foreach (var sandbox in (RedisSandbox[])[empty, unindexed])
        {
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(
                () => RedisCheckpointStore.ConnectReadOnlyAsync(TestBackends.RedisUrl, Prefixed(sandbox)));
            Assert.Equal(OpenError("redis_prefix_not_indexed", ("prefix", sandbox.Prefix)), refused.Message);
        }

        Assert.Equal(string.Empty, await RedisSnapshotAsync(empty));
        Assert.Equal(before, await RedisSnapshotAsync(unindexed));
    }

    [RedisFact]
    public async Task A_read_only_Redis_store_reads_offloaded_logs_from_its_own_blobs_and_a_blob_directory()
    {
        using var own = new RedisSandbox("ro_own");
        await SaveOffloadedLogAsync(new RedisCheckpointStore(own.Database, Prefixed(own, Offload())));
        Assert.True(await own.Database.HashLengthAsync($"{own.Prefix}:blobs:{OffloadedThread}") > 0);
        using var dir = new TempDirectory();
        using var elsewhere = new RedisSandbox("ro_elsewhere");
        var blobs = new FileSystemCheckpointBlobStore(dir.Path);
        await SaveOffloadedLogAsync(new RedisCheckpointStore(elsewhere.Database, Prefixed(elsewhere, Offload(blobs))));

        var before = (await RedisSnapshotAsync(own), await RedisSnapshotAsync(elsewhere), FilesOf(dir.Path));
        using (var store = await RedisCheckpointStore.ConnectReadOnlyAsync(TestBackends.RedisUrl, Prefixed(own)))
        {
            await AssertOffloadedLogAsync(store);
        }

        using (var store = await RedisCheckpointStore.ConnectReadOnlyAsync(
            TestBackends.RedisUrl, Prefixed(elsewhere, new OffloadOptions { Blobs = blobs })))
        {
            await AssertOffloadedLogAsync(store);
        }

        Assert.Equal(before, (await RedisSnapshotAsync(own), await RedisSnapshotAsync(elsewhere), FilesOf(dir.Path)));

        var missing = Path.Combine(dir.Path, "no-blobs");
        var refused = await Assert.ThrowsAsync<DirectoryNotFoundException>(() => RedisCheckpointStore.ConnectReadOnlyAsync(
            TestBackends.RedisUrl, Prefixed(own, new OffloadOptions { Blobs = new FileSystemCheckpointBlobStore(missing) })));
        Assert.Equal(OpenError("missing_blob_directory", ("dir", missing)), refused.Message);
        Assert.False(Directory.Exists(missing));
    }

    /// <summary>
    /// The ACL user the Redis test reads as: every command but writes, the dangerous ones (<c>INFO</c> among them, so the
    /// store can't ask the version), <c>EVAL</c>, <c>EVALSHA</c> and <c>FCALL</c>.
    /// </summary>
    private const string RedisReader = "strata_reader_dotnet";

    [RedisFact]
    public async Task A_read_only_Redis_store_works_for_a_user_allowed_only_reads_and_EVAL_RO()
    {
        using var sandbox = new RedisSandbox("ro_acl");
        var server = sandbox.Connection.GetServer(sandbox.Connection.GetEndPoints()[0]);
        if (server.Version < new Version(7, 0))
        {
            // Before Redis 7 there is no EVAL_RO: read-only is the store's promise, not the server's.
            return;
        }

        var writable = new RedisCheckpointStore(sandbox.Database, sandbox.Prefix);
        await CheckpointQueryContractTests.SaveSeedAsync(writable);
        Assert.NotNull((await writable.TryAcquireLeaseAsync(LeasedThread, "worker-1", LeaseTtl)).Lease);
        await server.ExecuteAsync(
            "ACL", "SETUSER", RedisReader, "reset", "on", $">{RedisReader}", "~*", "&*", "+@all", "-@write", "-@dangerous", "-eval", "-evalsha", "-fcall");
        var before = await RedisSnapshotAsync(sandbox);

        var options = RedisConnection.ToConfigurationOptions(TestBackends.RedisUrl);
        options.User = RedisReader;
        options.Password = RedisReader;
        var url = options.ToString(includePassword: true);
        using (var store = await RedisCheckpointStore.ConnectReadOnlyAsync(url, Prefixed(sandbox)))
        {
            await CheckpointQueryContractTests.RunCasesAsync(store, exact: false);
            Assert.Equal(("worker-1", 1L), await store.GetLeaseAsync(LeasedThread) is { } lease ? (lease.Owner, lease.Token) : default);
        }

        // The user really can't run a script that may write, nor write.
        using (var connection = await ConnectionMultiplexer.ConnectAsync(url))
        {
            var database = connection.GetDatabase();
            await Assert.ThrowsAsync<RedisServerException>(() => database.ScriptEvaluateAsync("return 1"));
            await Assert.ThrowsAsync<RedisServerException>(() => database.StringSetAsync($"{sandbox.Prefix}:x", "1"));
        }

        Assert.Equal(before, await RedisSnapshotAsync(sandbox));
    }
}

/// <summary>A test that needs Unix file modes: skipped on Windows.</summary>
public sealed class UnixFactAttribute : FactAttribute
{
    public UnixFactAttribute()
    {
        if (OperatingSystem.IsWindows())
        {
            Skip = "Unix file modes only";
        }
    }
}
