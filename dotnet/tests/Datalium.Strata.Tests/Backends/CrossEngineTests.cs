using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Storage.Postgres;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// The cross-engine migration test (<c>design/Arch-Specs/STRATA-SPEC-005.md</c>, "Conformance";
/// <c>schema/fixtures/cross-engine/scenario.json</c>): one engine writes a SQLite file and a Postgres schema at the
/// checkpoints component's schema version 1, holding a checkpoint at every engine-visible version, and another at
/// the current version written with deltas and offload; the other engine opens them with its current stores,
/// migrates the older one, reads every checkpoint and resumes threads to the end. Mirrors <c>strata-memory</c>'s
/// <c>tests/cross_engine.rs</c>; keep the two in step.
/// <para>
/// <c>STRATA_INTEROP_DIR</c> names the directory the engines share and <c>STRATA_INTEROP_STEP</c> what this run
/// does there: <c>write</c> this engine's databases (<c>dotnet-*</c>), or <c>read</c> the Rust engine's
/// (<c>rust-*</c>). <c>.github/workflows/cross-engine-ci.yml</c> runs the four steps. Without them, this engine
/// writes into a directory of its own and reads what it wrote, so the test runs in every build. Postgres needs
/// <c>STRATA_TEST_POSTGRES_URL</c> (see <see cref="TestBackends"/>); a cross-engine step requires it.
/// </para>
/// </summary>
public sealed class CrossEngineTests
{
    private const string Engine = "dotnet";
    private const string Other = "rust";
    private const string Older = "older";
    private const string Current = "current";

    private static readonly CompilationOptions Durable = new() { Checkpointing = CheckpointPolicy.EveryNode };

    private static JsonNode Scenario => CheckpointQueryContractTests.Fixture(Path.Combine("cross-engine", "scenario.json"));

    /// <summary>What a store call does once the writer's process has died.</summary>
    private sealed class ProcessDied() : Exception("the process died");

    // --- Where the databases are ---

    /// <summary>One of the two databases an engine writes, in one backend.</summary>
    private abstract class Database
    {
        public abstract string Label { get; }

        /// <summary>Removes whatever an earlier run left, and makes the Postgres schema.</summary>
        public abstract Task CreateAsync();

        public abstract Task RemoveAsync();

        /// <summary>A current store on this database, with deltas and offload on.</summary>
        public abstract Task<IStore> OpenAsync();

        /// <summary>Runs <paramref name="sql"/> as any other writer would, behind every store's back.</summary>
        public abstract Task ExecuteAsync(string sql);

        /// <summary>The <c>checkpoints</c> component's recorded version, and whether the blob table is there.</summary>
        public abstract Task<(long Version, bool Blobs)> LayoutAsync();

        /// <summary>The <c>schema_version</c> column of each of <paramref name="threadId"/>'s rows, oldest first.</summary>
        public abstract Task<List<string>> VersionsAsync(string threadId);

        protected const string VersionSql = "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'";

        protected static int Threshold => Scenario["current"]!["offload_threshold"]!.GetValue<int>();
    }

    private sealed class SqliteDatabase(string path) : Database
    {
        public override string Label => path;

        private SqliteConnection Raw()
        {
            var connection = new SqliteConnection($"Data Source={path};Pooling=False");
            connection.Open();
            return connection;
        }

        public override Task CreateAsync() => RemoveAsync();

        public override Task RemoveAsync()
        {
            SqliteConnection.ClearAllPools();
            File.Delete(path);
            return Task.CompletedTask;
        }

        public override Task<IStore> OpenAsync() => Task.FromResult<IStore>(new Store(new EfCoreCheckpointStore(
            new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={path}").Options),
            ownedConnection: null,
            timeProvider: null,
            new SqliteStoreOptions { Deltas = new DeltaOptions(), Offload = new OffloadOptions { Threshold = Threshold } })));

        public override async Task ExecuteAsync(string sql)
        {
            await using var connection = Raw();
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }

        public override async Task<(long Version, bool Blobs)> LayoutAsync()
        {
            await using var connection = Raw();
            await using var version = connection.CreateCommand();
            version.CommandText = VersionSql;
            await using var blobs = connection.CreateCommand();
            blobs.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'checkpoint_blobs'";
            return ((long)(await version.ExecuteScalarAsync())!, (long)(await blobs.ExecuteScalarAsync())! == 1);
        }

        public override async Task<List<string>> VersionsAsync(string threadId)
        {
            await using var connection = Raw();
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT schema_version FROM checkpoints WHERE thread_id = $t ORDER BY sequence";
            command.Parameters.AddWithValue("$t", threadId);
            var versions = new List<string>();
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                versions.Add(reader.GetString(0));
            }

            return versions;
        }
    }

    private sealed class PostgresDatabase(string url, string schema) : Database
    {
        private readonly string _connectionString = PostgresConnection.ToConnectionString(url);

        /// <summary>Postgres truncates a longer identifier, so two databases' schemas could become one.</summary>
        private readonly string _schema = schema.Length <= 63
            ? schema
            : throw new ArgumentException($"schema name '{schema}' is longer than Postgres's 63 characters", nameof(schema));

        public override string Label => $"postgres schema {_schema}";

        private NpgsqlDataSource InSchema() =>
            NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(_connectionString) { SearchPath = _schema }.ConnectionString);

        private async Task AdminAsync(string sql)
        {
            await using var admin = NpgsqlDataSource.Create(_connectionString);
            await using var command = admin.CreateCommand(sql);
            await command.ExecuteNonQueryAsync();
        }

        public override Task CreateAsync() => AdminAsync($"DROP SCHEMA IF EXISTS {_schema} CASCADE; CREATE SCHEMA {_schema}");

        public override Task RemoveAsync() => AdminAsync($"DROP SCHEMA IF EXISTS {_schema} CASCADE");

        public override async Task<IStore> OpenAsync()
        {
            var dataSource = InSchema();
            var store = new PostgresCheckpointStore(
                dataSource,
                new PostgresStoreOptions { Deltas = new DeltaOptions(), Offload = new OffloadOptions { Threshold = Threshold } });
            // Rust's store migrates when it connects; .NET's on first use, or here.
            await store.MigrateAsync();
            return new Store(store, dataSource);
        }

        public override async Task ExecuteAsync(string sql)
        {
            await using var dataSource = InSchema();
            await using var command = dataSource.CreateCommand(sql);
            await command.ExecuteNonQueryAsync();
        }

        public override async Task<(long Version, bool Blobs)> LayoutAsync()
        {
            await using var dataSource = InSchema();
            await using var version = dataSource.CreateCommand(VersionSql);
            await using var blobs = dataSource.CreateCommand("SELECT to_regclass('checkpoint_blobs') IS NOT NULL");
            return ((long)(await version.ExecuteScalarAsync())!, (bool)(await blobs.ExecuteScalarAsync())!);
        }

        public override async Task<List<string>> VersionsAsync(string threadId)
        {
            await using var dataSource = InSchema();
            await using var command = dataSource.CreateCommand(
                "SELECT schema_version FROM checkpoints WHERE thread_id = $1 ORDER BY sequence");
            command.Parameters.AddWithValue(threadId);
            var versions = new List<string>();
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                versions.Add(reader.GetString(0));
            }

            return versions;
        }
    }

    /// <summary>A store and its query port, disposed together with what it was opened on.</summary>
    private interface IStore : ICheckpointStore, ICheckpointQuery, IAsyncDisposable;

    private sealed class Store(object inner, IAsyncDisposable? dataSource = null) : IStore
    {
        private ICheckpointStore Checkpoints => (ICheckpointStore)inner;

        private ICheckpointQuery Query => (ICheckpointQuery)inner;

        public void Save(Checkpoint checkpoint) => Checkpoints.Save(checkpoint);

        public Checkpoint? Load(string threadId) => Checkpoints.Load(threadId);

        public ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
            Checkpoints.SaveAsync(checkpoint, cancellationToken);

        public ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
            Checkpoints.LoadAsync(threadId, cancellationToken);

        public ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
            Checkpoints.LoadLatestAsync(threadId, cancellationToken);

        public ValueTask<bool> SaveIfLatestAsync(
            Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
            Checkpoints.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken);

        public ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default) =>
            Query.ListThreadsAsync(query, cancellationToken);

        public ValueTask<CheckpointLogPage> GetLogAsync(
            string threadId, long? afterSequence = null, int limit = ThreadQuery.DefaultLimit, CancellationToken cancellationToken = default) =>
            Query.GetLogAsync(threadId, afterSequence, limit, cancellationToken);

        public async ValueTask DisposeAsync()
        {
            ((IDisposable)inner).Dispose();
            if (dataSource is not null)
            {
                await dataSource.DisposeAsync();
            }

            SqliteConnection.ClearAllPools();
        }
    }

    /// <summary>A store as the writer's process sees it: once it has died, nothing it does reaches the database.</summary>
    private sealed class Process(ICheckpointStore inner) : ICheckpointStore
    {
        private volatile bool _dead;

        public void Die() => _dead = true;

        private T Alive<T>(Func<T> call) => _dead ? throw new ProcessDied() : call();

        public void Save(Checkpoint checkpoint) => Alive(() => { inner.Save(checkpoint); return 0; });

        public Checkpoint? Load(string threadId) => Alive(() => inner.Load(threadId));

        public ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
            Alive(() => inner.SaveAsync(checkpoint, cancellationToken));

        public ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
            Alive(() => inner.LoadAsync(threadId, cancellationToken));

        public ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
            Alive(() => inner.LoadLatestAsync(threadId, cancellationToken));

        public ValueTask<bool> SaveIfLatestAsync(
            Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
            Alive(() => inner.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken));
    }

    // --- The graphs ---

    /// <summary><c>frozen.json</c>'s and <c>identified.json</c>'s state.</summary>
    public sealed record WireTransfer(
        [property: JsonPropertyName("invoice_generated")] bool InvoiceGenerated,
        [property: JsonPropertyName("amount")] double Amount,
        [property: JsonPropertyName("currency")] string Currency,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy,
        [property: JsonPropertyName("is_completed")] bool IsCompleted);

    /// <summary><c>PrepareWireTransfer</c> (a boundary) -> <c>ExecuteWireTransfer</c>.</summary>
    private static CompiledGraph<WireTransfer> WireTransferGraph(CompilationOptions options) =>
        new StateGraph<WireTransfer>()
            .AddNode("PrepareWireTransfer", s => s with { InvoiceGenerated = true })
            .AddInterruptBoundary("PrepareWireTransfer")
            .AddNode("ExecuteWireTransfer", s => s with { IsCompleted = true })
            .AddEdge("PrepareWireTransfer", "ExecuteWireTransfer")
            .Compile(options);

    /// <summary>
    /// <c>running.json</c>'s and <c>claimed.json</c>'s state, and the current database's: every node adds its name to
    /// <c>trail</c>.
    /// </summary>
    public sealed record Job(
        [property: JsonPropertyName("trail")] string Trail,
        [property: JsonPropertyName("approved")] bool Approved = false,
        [property: JsonPropertyName("limit")] int Limit = 0,
        [property: JsonPropertyName("evidence"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? Evidence = null);

    /// <summary>One run of a node: which, under which key, whether it may have run before, and the decision it saw.</summary>
    private sealed record Visit(string Node, string Key, bool Recovery, string? Reviewer);

    /// <summary>Every node a graph ran, and what to do when one is reached.</summary>
    private sealed class Journal
    {
        private readonly List<Visit> _visits = [];

        public (string Node, Process Process)? DiesAt { get; set; }

        public List<Visit> Visits
        {
            get
            {
                lock (_visits)
                {
                    return [.. _visits];
                }
            }
        }

        public Job Visit(Job job, NodeContext context)
        {
            lock (_visits)
            {
                _visits.Add(new Visit(context.Node, context.IdempotencyKey, context.IsRecovery, context.Decision?.Reviewer));
            }

            if (DiesAt is { } dies && dies.Node == context.Node)
            {
                dies.Process.Die();
            }

            return job with { Trail = job.Trail + context.Node };
        }
    }

    private static Func<Job, NodeContext, CancellationToken, ValueTask<Job>> Node(Journal journal) =>
        (job, context, _) => ValueTask.FromResult(journal.Visit(job, context));

    /// <summary>The nodes in a line, each adding its name to the trail.</summary>
    private static StateGraph<Job> Chain(Journal journal, IReadOnlyList<string> nodes)
    {
        var graph = new StateGraph<Job>();
        for (var i = 0; i < nodes.Count; i++)
        {
            graph.AddNode(nodes[i], Node(journal));
            if (i > 0)
            {
                graph.AddEdge(nodes[i - 1], nodes[i]);
            }
        }

        return graph;
    }

    private static CompiledGraph<Job> Line(Journal journal, IReadOnlyList<string> nodes) => Chain(journal, nodes).Compile(Durable);

    /// <summary><c>A</c> (a boundary, rejected to <c>Rejected</c>) -> <c>B</c> -> <c>C</c>.</summary>
    private static CompiledGraph<Job> Approval(Journal journal) =>
        Chain(journal, ["A", "B", "C"])
            .AddInterruptBoundary("A", onReject: "Rejected")
            .AddNode("Rejected", Node(journal))
            .Compile(Durable);

    private static List<string> CurrentNodes => [.. Scenario["current"]!["nodes"]!.AsArray().Select(n => n!.GetValue<string>())];

    private static TState Completed<TState>(string label, GraphExecutionResult<TState> result)
    {
        if (result is GraphExecutionResult<TState>.Failed failed)
        {
            Assert.Fail($"{label}: expected a completed run, got: {failed.Message}");
        }

        return Assert.IsType<GraphExecutionResult<TState>.Completed>(result).State;
    }

    private static void AssertJson(JsonNode? expected, JsonNode? actual, string label) =>
        Assert.True(JsonNode.DeepEquals(expected, actual), $"{label}: expected {expected?.ToJsonString()}, got {actual?.ToJsonString()}");

    // --- The lifecycle fixtures ---

    /// <summary>
    /// Every <c>checkpoint-lifecycle</c> fixture as (thread id, checkpoint), the thread id the writer saves it under,
    /// sorted by file name.
    /// </summary>
    private static List<(string ThreadId, JsonNode Checkpoint)> Lifecycle([CallerFilePath] string sourceFilePath = "")
    {
        var older = Scenario["older"]!;
        var fixtures = older["fixtures"]!.GetValue<string>();
        var dir = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(sourceFilePath)!, "..", "..", "..", "..", "schema", "fixtures", fixtures));
        var prefix = older["thread_prefix"]!.GetValue<string>();
        return
        [
            .. Directory.GetFiles(dir, "*.json")
                .Select(Path.GetFileName)
                .Order(StringComparer.Ordinal)
                .Select(name =>
                {
                    var threadId = prefix + Path.GetFileNameWithoutExtension(name);
                    var checkpoint = CheckpointQueryContractTests.Fixture(Path.Combine(fixtures, name!));
                    checkpoint["thread_id"] = threadId;
                    return (threadId, checkpoint);
                }),
        ];
    }

    private static async Task<SortedSet<string>> ThreadIdsAsync(ICheckpointQuery store)
    {
        var ids = new SortedSet<string>(StringComparer.Ordinal);
        var query = new ThreadQuery();
        while (true)
        {
            var page = await store.ListThreadsAsync(query);
            ids.UnionWith(page.Threads.Select(t => t.ThreadId));
            if (page.NextCursor is null)
            {
                return ids;
            }

            query = new ThreadQuery { Cursor = page.NextCursor };
        }
    }

    // --- Writing ---

    /// <summary>The older database: every lifecycle fixture, then the layout of the checkpoints component's version 1.</summary>
    private static async Task WriteOlderAsync(Database database)
    {
        await database.CreateAsync();
        await using (var store = await database.OpenAsync())
        {
            foreach (var (threadId, checkpoint) in Lifecycle())
            {
                await store.SaveAsync(Checkpoint.FromWireJson(checkpoint));
                Assert.NotNull(await store.LoadAsync(threadId));
            }
        }

        var version = Scenario["older"]!["written_checkpoints_version"]!.GetValue<long>();
        await database.ExecuteAsync(
            $"DROP TABLE checkpoint_blobs; UPDATE strata_schema_version SET version = {version} WHERE component = 'checkpoints'");
        Assert.Equal((version, false), await database.LayoutAsync());
    }

    /// <summary>For the read-only store tests (<c>design/51-Read-Only-Store-Open.md</c>): <see cref="WriteOlderAsync"/> into a SQLite file.</summary>
    internal static Task WriteOlderSqliteAsync(string path) => WriteOlderAsync(new SqliteDatabase(path));

    /// <summary>For the read-only store tests: <see cref="WriteOlderAsync"/> into a Postgres schema, made anew.</summary>
    internal static Task WriteOlderPostgresAsync(string url, string schema) => WriteOlderAsync(new PostgresDatabase(url, schema));

    /// <summary>For the read-only store tests: what <see cref="WriteOlderAsync"/> writes, as (thread id, checkpoint).</summary>
    internal static List<(string ThreadId, JsonNode Checkpoint)> OlderCheckpoints() => Lifecycle();

    private static List<string> WrittenVersions =>
        [.. Scenario["current"]!["written"]!.AsArray().Select(row => row!["schema_version"]!.GetValue<string>())];

    /// <summary>The current database: a run with deltas and offload whose process dies.</summary>
    private static async Task WriteCurrentAsync(Database database)
    {
        var current = Scenario["current"]!;
        var threadId = current["thread_id"]!.GetValue<string>();
        await database.CreateAsync();
        await using (var store = await database.OpenAsync())
        {
            var process = new Process(store);
            var journal = new Journal { DiesAt = (current["dies_at"]!.GetValue<string>(), process) };
            var evidence = new string('e', current["evidence_bytes"]!.GetValue<int>());
            await Assert.ThrowsAsync<ProcessDied>(() =>
                Line(journal, CurrentNodes).RunAsync(threadId, new Job(string.Empty, Evidence: evidence), process));
        }

        Assert.Equal(WrittenVersions, await database.VersionsAsync(threadId));
    }

    // --- Reading ---

    /// <summary>The other engine's older database: migrated on open, every checkpoint as its fixture, and the threads <c>resumes</c> names finished.</summary>
    private static async Task ReadOlderAsync(Database database)
    {
        var older = Scenario["older"]!;
        var label = database.Label;
        var written = older["written_checkpoints_version"]!.GetValue<long>();
        Assert.Equal((written, false), await database.LayoutAsync());

        await using var store = await database.OpenAsync();
        Assert.Equal((StoreSchemaVersions.CheckpointsVersion, true), await database.LayoutAsync());

        var fixtures = Lifecycle();
        foreach (var (threadId, expected) in fixtures)
        {
            var loaded = await store.LoadAsync(threadId);
            Assert.True(loaded is not null, $"{label}: {threadId} is missing");
            AssertJson(expected, loaded.ToWireJson(), $"{label}: {threadId}");
        }

        Assert.Equal(new SortedSet<string>(fixtures.Select(f => f.ThreadId), StringComparer.Ordinal), await ThreadIdsAsync(store));

        foreach (var resume in older["resumes"]!.AsArray())
        {
            var threadId = resume!["thread_id"]!.GetValue<string>();
            var what = $"{label}: {threadId}";
            switch (threadId)
            {
                case "lifecycle-frozen" or "lifecycle-identified":
                    {
                        var options = threadId == "lifecycle-identified"
                            ? new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode, GraphId = "wire-transfer", GraphVersion = "2" }
                            : Durable;
                        var signal = resume["signal"]!;
                        var human = HumanSignal.Approve(signal["approve"]!.GetValue<string>());
                        foreach (var (key, value) in signal["overrides"]!.AsObject())
                        {
                            human = human.WithOverride(key, value?.DeepClone());
                        }

                        var state = Completed(what, await WireTransferGraph(options).ResumeAsync(threadId, human, store));
                        AssertJson(resume["state"], JsonSerializer.SerializeToNode(state), what);
                        break;
                    }

                case "lifecycle-running" or "lifecycle-claimed":
                    {
                        var journal = new Journal();
                        var graph = threadId == "lifecycle-running" ? Line(journal, ["A", "B", "C"]) : Approval(journal);
                        var state = Completed(what, await graph.RecoverAsync(threadId, store));
                        AssertJson(resume["state"], JsonSerializer.SerializeToNode(state), what);
                        var recovered = resume["recovered"]!;
                        Assert.Equal(
                            new Visit(
                                recovered["node"]!.GetValue<string>(),
                                recovered["key"]!.GetValue<string>(),
                                Recovery: true,
                                recovered["reviewer"]?.GetValue<string>()),
                            journal.Visits[0]);
                        break;
                    }

                default:
                    Assert.Fail($"the scenario resumes {threadId}, which this test doesn't know");
                    break;
            }

            Assert.Equal(CheckpointStatus.Completed, (await store.LoadAsync(threadId))!.Status);
        }
    }

    /// <summary>The other engine's current database: its rows decoded and its run recovered to the end.</summary>
    private static async Task ReadCurrentAsync(Database database)
    {
        var current = Scenario["current"]!;
        var threadId = current["thread_id"]!.GetValue<string>();
        var label = database.Label;
        var written = current["written"]!.AsArray();
        Assert.Equal(WrittenVersions, await database.VersionsAsync(threadId));

        await using var store = await database.OpenAsync();
        var evidence = new string('e', current["evidence_bytes"]!.GetValue<int>());
        var log = (await store.GetLogAsync(threadId)).Entries;
        Assert.Equal(written.Count, log.Count);
        for (var i = 0; i < log.Count; i++)
        {
            var checkpoint = log[i].Checkpoint;
            var row = written[i]!;
            var state = checkpoint.FrozenState.Deserialize<Job>()!;
            Assert.Equal(
                (row["status"]!.GetValue<string>(), row["next_node"]?.GetValue<string>(), row["trail"]!.GetValue<string>(), true),
                (checkpoint.ToWireJson()["status"]!.GetValue<string>(), checkpoint.NextNode, state.Trail, state.Evidence == evidence));
            // The engine never sees a store's encodings.
            Assert.DoesNotContain(checkpoint.SchemaVersion, (string[])["1.8.0", "1.9.0"]);
        }

        var journal = new Journal();
        var finished = Completed(label, await Line(journal, CurrentNodes).RecoverAsync(threadId, store));
        Assert.Equal(current["state"]!["trail"]!.GetValue<string>(), finished.Trail);
        Assert.Equal(evidence, finished.Evidence);
        var visits = journal.Visits;
        Assert.Equal(current["visits"]!.AsArray().Select(v => v!.GetValue<string>()), visits.Select(v => v.Node));
        Assert.Equal((current["recovered"]!["key"]!.GetValue<string>(), true), (visits[0].Key, visits[0].Recovery));
        Assert.Equal(CheckpointStatus.Completed, (await store.LoadAsync(threadId))!.Status);
    }

    // --- The steps ---

    /// <summary>The databases <paramref name="engine"/> writes: SQLite in <paramref name="dir"/>, and Postgres when there is one.</summary>
    private static List<(string Kind, Database Database)> Databases(string dir, (string Url, string Prefix)? postgres, string engine)
    {
        List<(string, Database)> all =
        [
            (Older, new SqliteDatabase(Path.Combine(dir, $"{engine}-{Older}.sqlite"))),
            (Current, new SqliteDatabase(Path.Combine(dir, $"{engine}-{Current}.sqlite"))),
        ];
        if (postgres is var (url, prefix))
        {
            all.Add((Older, new PostgresDatabase(url, $"{prefix}_{engine}_{Older}")));
            all.Add((Current, new PostgresDatabase(url, $"{prefix}_{engine}_{Current}")));
        }

        return all;
    }

    private static async Task WriteAsync(string dir, (string, string)? postgres)
    {
        foreach (var (kind, database) in Databases(dir, postgres, Engine))
        {
            await (kind == Older ? WriteOlderAsync(database) : WriteCurrentAsync(database));
        }
    }

    private static async Task ReadAsync(string dir, (string, string)? postgres, string engine)
    {
        foreach (var (kind, database) in Databases(dir, postgres, engine))
        {
            await (kind == Older ? ReadOlderAsync(database) : ReadCurrentAsync(database));
        }
    }

    [Fact]
    public async Task An_Engine_Reads_Migrates_And_Resumes_What_The_Other_Wrote()
    {
        var step = Environment.GetEnvironmentVariable("STRATA_INTEROP_STEP") ?? string.Empty;
        var shared = Environment.GetEnvironmentVariable("STRATA_INTEROP_DIR");
        if (!string.IsNullOrEmpty(shared))
        {
            // A step of the cross-engine job: Postgres is part of it.
            var url = Environment.GetEnvironmentVariable(TestBackends.PostgresVar);
            Assert.False(string.IsNullOrEmpty(url), $"a cross-engine step needs {TestBackends.PostgresVar}");
            var postgres = (url!, "strata_interop");
            Directory.CreateDirectory(shared);
            switch (step)
            {
                case "write":
                    await WriteAsync(shared, postgres);
                    break;
                case "read":
                    await ReadAsync(shared, postgres, Other);
                    break;
                default:
                    Assert.Fail($"STRATA_INTEROP_STEP is \"{step}\": expected write or read");
                    break;
            }

            return;
        }

        // On its own: this engine reads what it wrote.
        var dir = Path.Combine(Path.GetTempPath(), $"strata-cross-engine-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        (string, string)? own = TestBackends.SkipReason(TestBackends.PostgresVar) is null
            ? (TestBackends.PostgresUrl, $"strata_test_{Guid.NewGuid().ToString("N")[..12]}_interop")
            : null;
        try
        {
            await WriteAsync(dir, own);
            await ReadAsync(dir, own, Engine);
        }
        finally
        {
            foreach (var (_, database) in Databases(dir, own, Engine))
            {
                await database.RemoveAsync();
            }

            Directory.Delete(dir, recursive: true);
        }
    }
}
