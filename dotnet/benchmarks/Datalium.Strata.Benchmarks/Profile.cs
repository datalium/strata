using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Reflection;
using System.Runtime;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
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

namespace Datalium.Strata.Benchmarks;

/// <summary>
/// The profile (<c>design/37-Benchmarks.md</c>, SG-27): what the microbenchmarks' means don't say. How the
/// engine's latency is distributed (P50 to P99.9), how many transitions a second it makes under
/// concurrency, what a suspended thread holds in memory, and what each store does: saves and loads a
/// second, and a run that checkpoints every node. Mirrors <c>rust/strata-bench/src/main.rs</c>: same
/// scenarios, same variants, same metric names. Keep the two in step.
/// <code>
/// dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- profile
/// dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- profile --only throughput
/// dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- profile --out dotnet.json
/// dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- profile --quick
/// </code>
/// The Postgres and Redis rows need <c>STRATA_TEST_POSTGRES_URL</c> / <c>STRATA_TEST_REDIS_URL</c> (the
/// repo's <c>docker-compose.yml</c>), the variables the tests read; without one, that store is left out
/// and the profile says so.
/// </summary>
public static class Profile
{
    private const string PostgresVar = "STRATA_TEST_POSTGRES_URL";
    private const string RedisVar = "STRATA_TEST_REDIS_URL";

    private static readonly string[] ScenarioNames =
        ["transition_latency", "run_latency", "throughput", "suspended_memory", "stores", "large_state"];

    private sealed record Options(bool Quick, string? Out, IReadOnlyList<string> Only)
    {
        public bool Runs(string scenario) => Only.Count == 0 || Only.Contains(scenario);

        /// <summary><paramref name="full"/>, or <paramref name="quick"/> under <c>--quick</c>.</summary>
        public int Count(int full, int quick) => Quick ? quick : full;
    }

    /// <summary>
    /// One line of the report: a scenario, the variant of it, and what was measured. A metric's name ends
    /// with its unit (<c>_ns</c>, <c>_per_sec</c>, <c>_bytes</c>).
    /// </summary>
    private sealed record Row(string Scenario, string Variant, IReadOnlyList<(string Name, double Value)> Metrics);

    private sealed class Report
    {
        public List<Row> Rows { get; } = [];

        public List<string> Notes { get; } = [];

        public void Add(string scenario, string variant, params (string Name, double Value)[] metrics)
        {
            var row = new Row(scenario, variant, metrics);
            Console.Error.WriteLine($"  {row.Scenario} {row.Variant}: {Shown(row.Metrics)}");
            Rows.Add(row);
        }

        public void Note(string note)
        {
            Console.Error.WriteLine($"  note: {note}");
            Notes.Add(note);
        }
    }

    public static async Task<int> RunAsync(string[] args)
    {
        Options options;
        try
        {
            options = Parse(args);
        }
        catch (ArgumentException e)
        {
            Console.Error.WriteLine($"profile: {e.Message}");
            return 2;
        }

#if DEBUG
        Console.Error.WriteLine("profile: this is a debug build; measure with -c Release");
#endif

        var startedAt = DateTimeOffset.UtcNow;
        using var scratch = new Scratch();
        var report = new Report();
        if (options.Runs("transition_latency"))
        {
            Console.Error.WriteLine("transition_latency");
            await TransitionLatencyAsync(options, report);
        }

        if (options.Runs("run_latency"))
        {
            Console.Error.WriteLine("run_latency");
            await RunLatencyAsync(options, report);
        }

        if (options.Runs("throughput"))
        {
            Console.Error.WriteLine("throughput");
            await ThroughputAsync(options, report);
        }

        if (options.Runs("suspended_memory"))
        {
            Console.Error.WriteLine("suspended_memory");
            await SuspendedMemoryAsync(options, scratch, report);
        }

        if (options.Runs("stores"))
        {
            Console.Error.WriteLine("stores");
            await StoresAsync(options, scratch, report);
        }

        if (options.Runs("large_state"))
        {
            Console.Error.WriteLine("large_state");
            await LargeStateAsync(options, scratch, report);
        }

        Console.OutputEncoding = Encoding.UTF8;
        Console.WriteLine(Markdown(report, options));
        if (options.Out is { } path)
        {
            var json = Document(report, options, startedAt).ToJsonString(new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(path, json + "\n");
        }

        return 0;
    }

    private static Options Parse(string[] args)
    {
        var (quick, output, only) = (false, (string?)null, new List<string>());
        for (var index = 0; index < args.Length; index++)
        {
            switch (args[index])
            {
                case "--quick":
                    quick = true;
                    break;
                case "--out":
                    output = ++index < args.Length ? args[index] : throw new ArgumentException("--out needs a path");
                    break;
                case "--only":
                    var names = ++index < args.Length ? args[index] : throw new ArgumentException("--only needs a scenario");
                    foreach (var name in names.Split(','))
                    {
                        if (!ScenarioNames.Contains(name))
                        {
                            throw new ArgumentException($"unknown scenario '{name}': one of {string.Join(", ", ScenarioNames)}");
                        }

                        only.Add(name);
                    }

                    break;
                default:
                    throw new ArgumentException($"unknown argument '{args[index]}'");
            }
        }

        return new Options(quick, output, only);
    }

    /// <summary>A latency distribution as metrics in nanoseconds.</summary>
    private static (string, double)[] Latency(Distribution distribution) =>
    [
        ("samples", distribution.Samples),
        ("mean_ns", distribution.Mean),
        ("p50_ns", distribution.P50),
        ("p90_ns", distribution.P90),
        ("p99_ns", distribution.P99),
        ("p999_ns", distribution.P999),
        ("max_ns", distribution.Max),
    ];

    /// <summary>A duration for a reader: <c>812 ns</c>, <c>4.31 µs</c>, <c>1.20 ms</c>.</summary>
    private static string Duration(double nanos) => nanos switch
    {
        < 1_000 => string.Create(CultureInfo.InvariantCulture, $"{nanos:F0} ns"),
        < 1_000_000 => string.Create(CultureInfo.InvariantCulture, $"{nanos / 1_000:F2} µs"),
        _ => string.Create(CultureInfo.InvariantCulture, $"{nanos / 1_000_000:F2} ms"),
    };

    private static string Shown(IEnumerable<(string Name, double Value)> metrics) =>
        string.Join(", ", metrics.Select(metric => metric.Name.EndsWith("_ns", StringComparison.Ordinal)
            ? $"{metric.Name[..^3]} {Duration(metric.Value)}"
            : $"{metric.Name} {metric.Value.ToString("N0", CultureInfo.InvariantCulture)}"));

    /// <summary>
    /// Runs <paramref name="graph"/> for a second before anything is measured (a tenth of one under
    /// <c>--quick</c>), so that no sample is among the code's first executions: the JIT compiles a method
    /// again once it has run for a while, and until then the code is several times slower. The Rust profile
    /// does the same so the two stay alike.
    /// </summary>
    private static async Task WarmAsync(CompiledGraph<Work> graph, ICheckpointStore store, Options options)
    {
        var start = Scenarios.NewWork(0);
        var until = Stopwatch.GetTimestamp() + (options.Count(1_000, 100) * Stopwatch.Frequency / 1_000);
        while (Stopwatch.GetTimestamp() < until)
        {
            Scenarios.Completed(await graph.RunAsync("warm", start, store));
        }
    }

    /// <summary>
    /// The gap between one handler returning and the next starting, over runs of a hundred-node line: the
    /// engine's scheduling overhead per transition.
    /// </summary>
    private static async Task TransitionLatencyAsync(Options options, Report report)
    {
        const int Nodes = 100;
        var runs = options.Count(2_000, 50);
        var (store, start) = (new LatestOnlyStore(), Scenarios.NewWork(0));
        foreach (var handler in Scenarios.Handlers)
        {
            var gaps = new Gaps();
            var graph = Scenarios.Timed(Nodes, handler, gaps);
            await WarmAsync(graph, store, options);

            gaps.Take();
            for (var run = 0; run < runs; run++)
            {
                Scenarios.Completed(await graph.RunAsync("bench", start, store));
            }

            report.Add("transition_latency", handler.Name(), Latency(Distribution.Of(gaps.Take())));
        }
    }

    /// <summary>A whole run of the ten-node line, one after another.</summary>
    private static async Task RunLatencyAsync(Options options, Report report)
    {
        var (store, start) = (new LatestOnlyStore(), Scenarios.NewWork(0));
        foreach (var handler in Scenarios.Handlers)
        {
            var runs = handler == HandlerKind.Async ? options.Count(100_000, 2_000) : options.Count(20_000, 500);
            var graph = Scenarios.Linear(Scenarios.RunNodes, handler);
            await WarmAsync(graph, store, options);

            var samples = new long[runs];
            for (var run = 0; run < runs; run++)
            {
                var started = Stopwatch.GetTimestamp();
                Scenarios.Completed(await graph.RunAsync("bench", start, store));
                samples[run] = Stats.NanosecondsSince(started);
            }

            report.Add("run_latency", $"{handler.Name()}/{Scenarios.RunNodes}_nodes", Latency(Distribution.Of(samples)));
        }
    }

    /// <summary>Transitions a second with 1, 4, 16 and 64 ten-node runs going at once.</summary>
    private static async Task ThroughputAsync(Options options, Report report)
    {
        var window = TimeSpan.FromMilliseconds(options.Count(2_000, 200));
        var (store, start) = (new LatestOnlyStore(), Scenarios.NewWork(0));
        foreach (var handler in Scenarios.Handlers)
        {
            var graph = Scenarios.Linear(Scenarios.RunNodes, handler);
            foreach (var concurrency in new[] { 1, 4, 16, 64 })
            {
                long runs = 0;
                var started = Stopwatch.GetTimestamp();
                var deadline = started + (long)(window.TotalSeconds * Stopwatch.Frequency);
                var tasks = Enumerable.Range(0, concurrency).Select(_ => Task.Run(async () =>
                {
                    while (Stopwatch.GetTimestamp() < deadline)
                    {
                        Scenarios.Completed(await graph.RunAsync("bench", start, store));
                        Interlocked.Increment(ref runs);
                    }
                })).ToArray();
                await Task.WhenAll(tasks);

                var seconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
                var total = (double)Interlocked.Read(ref runs);
                report.Add(
                    "throughput",
                    $"{handler.Name()}/{concurrency}_concurrent",
                    ("transitions_per_sec", total * Scenarios.RunNodes / seconds),
                    ("runs_per_sec", total / seconds));
            }
        }
    }

    /// <summary>A folder of the profile's own under the system's temporary one.</summary>
    private sealed class Scratch : IDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"strata-bench-{Environment.ProcessId}");

        public Scratch() => Directory.CreateDirectory(_path);

        public string File(string name) => Path.Combine(_path, name);

        public void Dispose()
        {
            // The stores' connections are pooled, as an application's are; the pool holds the files open.
            SqliteConnection.ClearAllPools();
            try
            {
                Directory.Delete(_path, recursive: true);
            }
            catch (IOException)
            {
                // A file still open is the operating system's to clean up.
            }
        }
    }

    private static EfCoreCheckpointStore OpenSqlite(
        string path, GroupCommitOptions? groupCommit = null, DeltaOptions? deltas = null, OffloadOptions? offload = null)
    {
        var options = new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={path}").Options;
        return new EfCoreCheckpointStore(
            new CheckpointDbContext(options),
            options: new SqliteStoreOptions { GroupCommit = groupCommit, Deltas = deltas, Offload = offload });
    }

    /// <summary>The size of a SQLite database: its file and its write-ahead log, if any.</summary>
    private static long DatabaseBytes(string path)
    {
        static long Size(string file) => System.IO.File.Exists(file) ? new FileInfo(file).Length : 0;
        return Size(path) + Size(path + "-wal");
    }

    /// <summary>
    /// Threads frozen at an interrupt boundary with a 1 KiB state: the bytes each keeps on the managed
    /// heap, and on disk for SQLite. The in-memory store's bytes are the checkpoint itself; with a real
    /// store the process keeps nothing.
    /// </summary>
    private static async Task SuspendedMemoryAsync(Options options, Scratch scratch, Report report)
    {
        var threads = options.Count(10_000, 200);
        var (graph, start) = (Scenarios.Approval(), Scenarios.NewWork(Scenarios.PayloadBytes));
        var checkpointBytes = Encoding.UTF8.GetByteCount((await Scenarios.SampleCheckpointAsync()).ToWireJson().ToJsonString());

        var path = scratch.File("suspended.db");
        using var sqlite = OpenSqlite(path);
        (string Name, ICheckpointStore Store)[] stores = [("memory", new InMemoryCheckpointStore()), ("sqlite", sqlite)];
        foreach (var (name, store) in stores)
        {
            // Whatever the first runs set up once is not a thread's.
            for (var index = 0; index < 8; index++)
            {
                Scenarios.Interrupted(await graph.RunAsync($"warm-{index}", start, store));
            }

            var diskBefore = DatabaseBytes(path);
            var before = GC.GetTotalMemory(forceFullCollection: true);
            for (var index = 0; index < threads; index++)
            {
                Scenarios.Interrupted(await graph.RunAsync($"t-{index}", start, store));
            }

            var held = GC.GetTotalMemory(forceFullCollection: true) - before;
            GC.KeepAlive(store);

            var metrics = new List<(string, double)>
            {
                ("threads", threads),
                ("heap_bytes_per_thread", (double)held / threads),
                ("checkpoint_json_bytes", checkpointBytes),
            };
            if (name == "sqlite")
            {
                metrics.Add(("disk_bytes_per_thread", (double)(DatabaseBytes(path) - diskBefore) / threads));
            }

            report.Add("suspended_memory", name, [.. metrics]);
        }

        report.Note(
            "suspended_memory: heap bytes are the managed heap's growth after a full collection; SQLite's own page cache is not counted");
    }

    /// <summary>
    /// A store the profile measures, how much to ask of it, and the same store with group commit on
    /// (<c>design/41-Checkpoint-Efficiency.md</c>), where it has it.
    /// </summary>
    private sealed record Target(string Name, ICheckpointStore Store, int Operations, int Runs, ICheckpointStore? Batched = null);

    /// <summary>The writer counts <c>store_save</c> measures at once.</summary>
    private static readonly int[] WriterCounts = [16, 64, 256];

    /// <summary>The concurrent runs <c>durable_throughput</c> measures.</summary>
    private const int DurableWorkers = 64;

    private static string? BackendUrl(string variable, Report report)
    {
        var url = Environment.GetEnvironmentVariable(variable);
        if (!string.IsNullOrEmpty(url))
        {
            return url;
        }

        report.Note($"stores: {variable} is not set, so that store was not measured");
        return null;
    }

    /// <summary><paramref name="checkpoint"/> under another thread id, with a state of its own.</summary>
    private static Checkpoint Renamed(Checkpoint checkpoint, string threadId) =>
        checkpoint with { ThreadId = threadId, FrozenState = checkpoint.FrozenState?.DeepClone() };

    /// <summary>
    /// Each store's saves and loads of a 1 KiB checkpoint, and a ten-node run that checkpoints after every
    /// node (eleven saves).
    /// </summary>
    private static async Task StoresAsync(Options options, Scratch scratch, Report report)
    {
        var (local, remote) = (options.Count(20_000, 200), options.Count(2_000, 50));
        var (localRuns, remoteRuns) = (options.Count(2_000, 20), options.Count(200, 5));
        var batched = new GroupCommitOptions();

        using var sqlite = OpenSqlite(scratch.File("stores.db"));
        using var sqliteBatched = OpenSqlite(scratch.File("stores-batched.db"), batched);
        var targets = new List<Target>
        {
            new("memory", new InMemoryCheckpointStore(), local, localRuns),
            new("sqlite", sqlite, remote, remoteRuns, sqliteBatched),
        };

        // A Postgres schema and a Redis key prefix of the profile's own, removed when it is done.
        var sandbox = $"strata_bench_{Environment.ProcessId}";
        NpgsqlDataSource? admin = null, scoped = null;
        ConnectionMultiplexer? redis = null;
        try
        {
            if (BackendUrl(PostgresVar, report) is { } postgresUrl)
            {
                var connectionString = PostgresConnection.ToConnectionString(postgresUrl);
                admin = NpgsqlDataSource.Create(connectionString);
                await using (var create = admin.CreateCommand($"CREATE SCHEMA {sandbox}"))
                {
                    await create.ExecuteNonQueryAsync();
                }

                scoped = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString) { SearchPath = sandbox }.ConnectionString);
                var store = new PostgresCheckpointStore(scoped);
                await store.MigrateAsync();
                targets.Add(new Target(
                    "postgres", store, remote, remoteRuns, new PostgresCheckpointStore(scoped, new PostgresStoreOptions { GroupCommit = batched })));
            }

            if (BackendUrl(RedisVar, report) is { } redisUrl)
            {
                redis = await RedisConnection.ConnectAsync(redisUrl);
                targets.Add(new Target("redis", new RedisCheckpointStore(redis.GetDatabase(), sandbox), remote, remoteRuns));
            }

            var template = await Scenarios.SampleCheckpointAsync();
            var (graph, start) = (Scenarios.Durable(), Scenarios.NewWork(Scenarios.PayloadBytes));
            foreach (var target in targets)
            {
                await MeasureStoreAsync(target, template, graph, start, options, report);
            }
        }
        finally
        {
            if (admin is not null)
            {
                await using (var drop = admin.CreateCommand($"DROP SCHEMA IF EXISTS {sandbox} CASCADE"))
                {
                    await drop.ExecuteNonQueryAsync();
                }

                if (scoped is not null)
                {
                    await scoped.DisposeAsync();
                }

                await admin.DisposeAsync();
            }

            if (redis is not null)
            {
                var server = redis.GetServer(redis.GetEndPoints()[0]);
                var keys = server.Keys(pattern: $"{sandbox}:*").ToArray();
                foreach (var chunk in keys.Chunk(500))
                {
                    await redis.GetDatabase().KeyDeleteAsync(chunk);
                }

                await redis.DisposeAsync();
            }
        }
    }

    private static async Task MeasureStoreAsync(
        Target target, Checkpoint template, CompiledGraph<Work> graph, Work start, Options options, Report report)
    {
        const int WarmUp = 50;
        var store = target.Store;

        // The store's first calls are not measured: they open, prepare, cache and compile.
        for (var index = 0; index < WarmUp; index++)
        {
            await store.SaveAsync(Renamed(template, $"warm-{index}"));
            await store.LoadLatestAsync($"warm-{index}");
        }

        for (var index = 0; index < 3; index++)
        {
            Scenarios.Completed(await graph.RunAsync($"warm-durable-{index}", start, store));
        }

        // One writer, a thread per save.
        var checkpoints = Enumerable.Range(0, target.Operations).Select(index => Renamed(template, $"one-{index}")).ToArray();
        var samples = new long[target.Operations];
        for (var index = 0; index < checkpoints.Length; index++)
        {
            var started = Stopwatch.GetTimestamp();
            await store.SaveAsync(checkpoints[index]);
            samples[index] = Stats.NanosecondsSince(started);
        }

        var distribution = Distribution.Of(samples);
        report.Add(
            "store_save",
            $"{target.Name}/1_writer",
            ("ops_per_sec", target.Operations * 1e9 / samples.Sum()),
            ("p50_ns", distribution.P50),
            ("p99_ns", distribution.P99));

        // Sixteen writers at once, each with threads of its own; more come after the durable run, so what they leave
        // in the store doesn't weigh on it.
        await SaveConcurrentlyAsync(store, template, target, WriterCounts[0], "many", string.Empty, options, report);

        // One reader, the latest checkpoint of each thread saved above.
        for (var index = 0; index < target.Operations; index++)
        {
            var started = Stopwatch.GetTimestamp();
            var latest = await store.LoadLatestAsync($"one-{index}");
            samples[index] = Stats.NanosecondsSince(started);
            if (latest is null)
            {
                throw new InvalidOperationException($"one-{index} was saved");
            }
        }

        distribution = Distribution.Of(samples);
        report.Add(
            "store_load",
            $"{target.Name}/1_reader",
            ("ops_per_sec", target.Operations * 1e9 / samples.Sum()),
            ("p50_ns", distribution.P50),
            ("p99_ns", distribution.P99));

        // A run that checkpoints after every node.
        var runs = new long[target.Runs];
        for (var index = 0; index < runs.Length; index++)
        {
            var started = Stopwatch.GetTimestamp();
            Scenarios.Completed(await graph.RunAsync($"durable-{index}", start, store));
            runs[index] = Stats.NanosecondsSince(started);
        }

        distribution = Distribution.Of(runs);
        report.Add(
            "durable_run",
            $"{target.Name}/{Scenarios.RunNodes}_nodes",
            ("mean_ns", distribution.Mean),
            ("p50_ns", distribution.P50),
            ("p99_ns", distribution.P99),
            ("checkpoints_per_run", Scenarios.RunNodes + 1));
        foreach (var writers in WriterCounts.Skip(1))
        {
            await SaveConcurrentlyAsync(store, template, target, writers, "many", string.Empty, options, report);
        }

        await DurableThroughputAsync(store, graph, start, target, "durable", string.Empty, report);

        // The same store with group commit on: its saves and runs, measured the same way.
        if (target.Batched is { } batched)
        {
            for (var index = 0; index < WarmUp; index++)
            {
                await batched.SaveAsync(Renamed(template, $"batched-warm-{index}"));
            }

            await SaveConcurrentlyAsync(batched, template, target, 1, "batched-one", "_batched", options, report);
            foreach (var writers in WriterCounts)
            {
                await SaveConcurrentlyAsync(batched, template, target, writers, "batched-many", "_batched", options, report);
            }

            await DurableThroughputAsync(batched, graph, start, target, "batched-durable", "_batched", report);
        }
    }

    /// <summary>
    /// <paramref name="writers"/> writers saving at once, each a thread per save: <c>store_save</c>'s
    /// <c>{store}/{writers}_writers{suffix}</c>. Each writer saves at least 32 checkpoints (one in a quick run), so the
    /// many-writer cases last long enough to measure.
    /// </summary>
    private static async Task SaveConcurrentlyAsync(
        ICheckpointStore store, Checkpoint template, Target target, int writers, string prefix, string suffix, Options options, Report report)
    {
        var perWriter = Math.Max(target.Operations / writers, options.Count(32, 1));
        var batches = Enumerable.Range(0, writers)
            .Select(writer => Enumerable.Range(0, perWriter).Select(index => Renamed(template, $"{prefix}-{writers}-{writer}-{index}")).ToArray())
            .ToArray();
        var wall = Stopwatch.GetTimestamp();
        await Task.WhenAll(batches.Select(batch => Task.Run(async () =>
        {
            foreach (var checkpoint in batch)
            {
                await store.SaveAsync(checkpoint);
            }
        })));
        report.Add(
            "store_save",
            writers == 1 ? $"{target.Name}/1_writer{suffix}" : $"{target.Name}/{writers}_writers{suffix}",
            ("ops_per_sec", perWriter * writers / Stopwatch.GetElapsedTime(wall).TotalSeconds));
    }

    /// <summary>
    /// <see cref="DurableWorkers"/> workers each running durable ten-node runs one after another:
    /// <c>durable_throughput</c>'s <c>{store}/64_concurrent{suffix}</c>, in runs and saves a second.
    /// </summary>
    private static async Task DurableThroughputAsync(
        ICheckpointStore store, CompiledGraph<Work> graph, Work start, Target target, string prefix, string suffix, Report report)
    {
        var perWorker = Math.Max(target.Runs / 16, 1);
        var wall = Stopwatch.GetTimestamp();
        await Task.WhenAll(Enumerable.Range(0, DurableWorkers).Select(worker => Task.Run(async () =>
        {
            for (var index = 0; index < perWorker; index++)
            {
                Scenarios.Completed(await graph.RunAsync($"{prefix}-{worker}-{index}", start, store));
            }
        })));
        var seconds = Stopwatch.GetElapsedTime(wall).TotalSeconds;
        var runs = (double)perWorker * DurableWorkers;
        report.Add(
            "durable_throughput",
            $"{target.Name}/{DurableWorkers}_concurrent{suffix}",
            ("runs_per_sec", runs / seconds),
            ("saves_per_sec", runs * (Scenarios.RunNodes + 1) / seconds));
    }

    /// <summary>The payload <c>store_save_large</c> and <c>store_load_chain</c> carry: 64 KiB that never changes.</summary>
    private const int LargePayloadBytes = 64 * 1024;

    /// <summary>The rows of a thread <c>store_save_large</c> writes, as a run of that many nodes would.</summary>
    private const int LargeChain = 20;

    /// <summary>The rows <c>store_load_chain</c>'s thread has: with deltas, its latest is 15 deltas from a whole row.</summary>
    private const int ChainRows = 16;

    /// <summary>What a durable run of the <c>Work</c> state with <paramref name="payload"/> writes after node <paramref name="step"/>.</summary>
    private static Checkpoint LargeRunning(string threadId, int step, string payload) => new()
    {
        SchemaVersion = CheckpointSchema.RunningVersion,
        ThreadId = threadId,
        Status = CheckpointStatus.Running,
        SuspendedNode = $"N{step}",
        NextNode = $"N{step + 1}",
        FrozenState = new JsonObject { ["step"] = step, ["payload"] = payload },
        Metadata = new Dictionary<string, string> { ["strata.hops"] = $"{step}" },
        CreatedAt = "2026-10-15T00:00:00Z",
    };

    /// <summary>
    /// Sums <c>strata.store.checkpoint.size</c>, the JSON each save stored, and <c>strata.store.blob.size</c>, the values
    /// it offloaded, while it lives.
    /// </summary>
    private sealed class StoredBytes : IDisposable
    {
        private readonly MeterListener _listener = new();
        private long _bytes;
        private long _saves;

        public StoredBytes()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == StorageTelemetry.Name
                    && instrument.Name is "strata.store.checkpoint.size" or "strata.store.blob.size")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((instrument, bytes, _, _) =>
            {
                Interlocked.Add(ref _bytes, bytes);
                if (instrument.Name == "strata.store.checkpoint.size")
                {
                    Interlocked.Increment(ref _saves);
                }
            });
            _listener.Start();
        }

        /// <summary>The mean bytes a save stored since the last call.</summary>
        public double TakeMean()
        {
            var (bytes, saves) = (Interlocked.Exchange(ref _bytes, 0), Interlocked.Exchange(ref _saves, 0));
            return saves == 0 ? 0 : (double)bytes / saves;
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// How <c>large_state</c> stores the state, and what <c>store_load_chain</c> calls the thread: whole, as deltas,
    /// with the payload offloaded (to the store's own database), and both.
    /// </summary>
    private static readonly (string Variant, string Shape, DeltaOptions? Deltas, OffloadOptions? Offload)[] LargeVariants =
    [
        ("whole", "whole", null, null),
        ("delta", $"{ChainRows - 1}_deltas", new DeltaOptions(), null),
        ("offload", "offloaded", null, new OffloadOptions()),
        ("delta_offload", $"{ChainRows - 1}_deltas_offloaded", new DeltaOptions(), new OffloadOptions()),
    ];

    /// <summary>A store <c>large_state</c> measures, once per <see cref="LargeVariants"/>.</summary>
    private sealed record LargeTarget(string Name, IReadOnlyList<ICheckpointStore> Stores, int Saves, int Loads);

    /// <summary>
    /// A state whose 64 KiB payload never changes and whose step does (<c>design/41-Checkpoint-Efficiency.md</c>,
    /// "Measuring"): <c>store_save_large</c> saves it as a run would, a running checkpoint per node, stored whole, as
    /// deltas, with the payload offloaded, and as deltas with it offloaded; <c>store_load_chain</c> loads a thread whose
    /// latest row is whole, one whose latest is 15 deltas from a whole one, and each with the payload offloaded.
    /// </summary>
    private static async Task LargeStateAsync(Options options, Scratch scratch, Report report)
    {
        var (saves, loads) = (options.Count(400, LargeChain), options.Count(500, 10));
        var (remoteSaves, remoteLoads) = (options.Count(200, LargeChain), options.Count(200, 10));
        var sqlite = LargeVariants
            .Select(v => OpenSqlite(scratch.File($"large-{v.Variant}.db"), deltas: v.Deltas, offload: v.Offload))
            .ToList();
        var targets = new List<LargeTarget> { new("sqlite", sqlite, saves, loads) };

        var sandbox = $"strata_bench_{Environment.ProcessId}_large";
        NpgsqlDataSource? admin = null, scoped = null;
        ConnectionMultiplexer? redis = null;
        try
        {
            if (BackendUrl(PostgresVar, report) is { } postgresUrl)
            {
                var connectionString = PostgresConnection.ToConnectionString(postgresUrl);
                admin = NpgsqlDataSource.Create(connectionString);
                await using (var create = admin.CreateCommand($"CREATE SCHEMA {sandbox}"))
                {
                    await create.ExecuteNonQueryAsync();
                }

                scoped = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString) { SearchPath = sandbox }.ConnectionString);
                var stores = LargeVariants
                    .Select(v => new PostgresCheckpointStore(scoped, new PostgresStoreOptions { Deltas = v.Deltas, Offload = v.Offload }))
                    .ToList();
                await stores[0].MigrateAsync();
                targets.Add(new LargeTarget("postgres", stores, remoteSaves, remoteLoads));
            }

            if (BackendUrl(RedisVar, report) is { } redisUrl)
            {
                redis = await RedisConnection.ConnectAsync(redisUrl);
                targets.Add(new LargeTarget(
                    "redis",
                    [
                        .. LargeVariants.Select(v => new RedisCheckpointStore(
                            redis.GetDatabase(), new RedisStoreOptions { Prefix = sandbox, Deltas = v.Deltas, Offload = v.Offload })),
                    ],
                    remoteSaves,
                    remoteLoads));
            }

            var payload = new string('x', LargePayloadBytes);
            using var stored = new StoredBytes();
            foreach (var target in targets)
            {
                for (var v = 0; v < LargeVariants.Length; v++)
                {
                    var (variant, shape, store) = (LargeVariants[v].Variant, LargeVariants[v].Shape, target.Stores[v]);
                    for (var step = 0; step < LargeChain; step++)
                    {
                        await store.SaveAsync(LargeRunning($"large-warm-{variant}", step, payload));
                    }

                    stored.TakeMean();
                    var wall = Stopwatch.GetTimestamp();
                    for (var index = 0; index < target.Saves; index++)
                    {
                        await store.SaveAsync(LargeRunning($"large-{variant}-{index / LargeChain}", index % LargeChain, payload));
                    }

                    var seconds = Stopwatch.GetElapsedTime(wall).TotalSeconds;
                    report.Add(
                        "store_save_large",
                        $"{target.Name}/{variant}",
                        ("ops_per_sec", target.Saves / seconds),
                        ("stored_bytes", stored.TakeMean()));

                    for (var step = 0; step < ChainRows; step++)
                    {
                        await store.SaveAsync(LargeRunning($"chain-{variant}", step, payload));
                    }

                    var samples = new long[target.Loads];
                    for (var index = 0; index < samples.Length; index++)
                    {
                        var started = Stopwatch.GetTimestamp();
                        var latest = await store.LoadLatestAsync($"chain-{variant}");
                        samples[index] = Stats.NanosecondsSince(started);
                        if (latest?.Sequence != ChainRows - 1)
                        {
                            throw new InvalidOperationException($"chain-{variant} has {ChainRows} rows");
                        }
                    }

                    var distribution = Distribution.Of(samples);
                    report.Add(
                        "store_load_chain",
                        $"{target.Name}/{shape}",
                        ("ops_per_sec", target.Loads * 1e9 / samples.Sum()),
                        ("p50_ns", distribution.P50),
                        ("p99_ns", distribution.P99));
                }
            }
        }
        finally
        {
            foreach (var store in sqlite)
            {
                store.Dispose();
            }

            if (admin is not null)
            {
                await using (var drop = admin.CreateCommand($"DROP SCHEMA IF EXISTS {sandbox} CASCADE"))
                {
                    await drop.ExecuteNonQueryAsync();
                }

                if (scoped is not null)
                {
                    await scoped.DisposeAsync();
                }

                await admin.DisposeAsync();
            }

            if (redis is not null)
            {
                var server = redis.GetServer(redis.GetEndPoints()[0]);
                var keys = server.Keys(pattern: $"{sandbox}:*").ToArray();
                foreach (var chunk in keys.Chunk(500))
                {
                    await redis.GetDatabase().KeyDeleteAsync(chunk);
                }

                await redis.DisposeAsync();
            }
        }
    }

    private static string Version =>
        typeof(Profile).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0]
        ?? "unknown";

    private static string Os =>
        OperatingSystem.IsWindows() ? "windows" : OperatingSystem.IsMacOS() ? "macos" : OperatingSystem.IsLinux() ? "linux" : "other";

    private static string Arch => RuntimeInformation.ProcessArchitecture switch
    {
        Architecture.X64 => "x86_64",
        Architecture.Arm64 => "aarch64",
        var other => other.ToString().ToLowerInvariant(),
    };

    private static string Markdown(Report report, Options options)
    {
        var text = new StringBuilder();
        text.Append(CultureInfo.InvariantCulture, $"### .NET profile, {Version}{(options.Quick ? " (quick: not a measurement)" : string.Empty)}\n\n");
        text.Append(CultureInfo.InvariantCulture, $"{Os} {Arch}, {Environment.ProcessorCount} logical cores, {RuntimeInformation.FrameworkDescription}, ");
        text.Append(GCSettings.IsServerGC ? "server GC" : "workstation GC");
        text.Append("\n\n| Scenario | Variant | Measured |\n|---|---|---|\n");
        foreach (var row in report.Rows)
        {
            text.Append(CultureInfo.InvariantCulture, $"| `{row.Scenario}` | `{row.Variant}` | {Shown(row.Metrics)} |\n");
        }

        foreach (var note in report.Notes)
        {
            text.Append(CultureInfo.InvariantCulture, $"\n- {note}");
        }

        return text.ToString();
    }

    private static JsonObject Document(Report report, Options options, DateTimeOffset startedAt)
    {
        var results = new JsonArray();
        foreach (var row in report.Rows)
        {
            var metrics = new JsonObject();
            foreach (var (name, value) in row.Metrics)
            {
                metrics[name] = value;
            }

            results.Add(new JsonObject { ["scenario"] = row.Scenario, ["variant"] = row.Variant, ["metrics"] = metrics });
        }

        return new JsonObject
        {
            ["engine"] = "dotnet",
            ["version"] = Version,
            ["quick"] = options.Quick,
            ["started_at"] = startedAt.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture),
            ["machine"] = new JsonObject
            {
                ["os"] = Os,
                ["arch"] = Arch,
                ["logical_cores"] = Environment.ProcessorCount,
                ["runtime"] = RuntimeInformation.FrameworkDescription,
                ["server_gc"] = GCSettings.IsServerGC,
            },
            ["results"] = results,
            ["notes"] = new JsonArray([.. report.Notes.Select(note => (JsonNode)note)]),
        };
    }
}
