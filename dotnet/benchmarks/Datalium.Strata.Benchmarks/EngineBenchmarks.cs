using BenchmarkDotNet.Attributes;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Benchmarks;

// The engine's microbenchmarks (design/37-Benchmarks.md, SG-27): what a run costs when the handlers do
// nothing and the store costs nothing, so the time is the engine's. Mirrors
// rust/strata-bench/benches/engine.rs, one class per criterion group. Keep the two in step.
//
//   dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- --filter '*'
//   dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- --filter '*Traversal*'
//   dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- --filter '*' --job Dry   (CI)

/// <summary>
/// A run of a line of no-op nodes: the per-node cost is the time over the node count.
/// </summary>
[MemoryDiagnoser]
public class Traversal
{
    private readonly LatestOnlyStore _store = new();
    private readonly Work _start = Scenarios.NewWork(0);
    private CompiledGraph<Work> _graph = null!;

    [ParamsAllValues]
    public HandlerKind Handler { get; set; }

    [Params(1, 10, 100)]
    public int Nodes { get; set; }

    [GlobalSetup]
    public void Setup() => _graph = Scenarios.Linear(Nodes, Handler);

    [Benchmark]
    public async Task<Work> Run() => Scenarios.Completed(await _graph.RunAsync("bench", _start, _store));
}

/// <summary>
/// The same ten-node run with an empty state and a 16 KiB one: what a state's size costs a run that
/// doesn't checkpoint.
/// </summary>
[MemoryDiagnoser]
public class StateSize
{
    private readonly LatestOnlyStore _store = new();
    private readonly CompiledGraph<Work> _graph = Scenarios.Linear(Scenarios.RunNodes, HandlerKind.Async);
    private Work _start = null!;

    [Params(0, 16 * 1024)]
    public int Bytes { get; set; }

    [GlobalSetup]
    public void Setup() => _start = Scenarios.NewWork(Bytes);

    [Benchmark]
    public async Task<Work> Run() => Scenarios.Completed(await _graph.RunAsync("bench", _start, _store));
}

/// <summary>A parallel group of no-op branches.</summary>
[MemoryDiagnoser]
public class FanOutGroup
{
    private readonly LatestOnlyStore _store = new();
    private readonly Work _start = Scenarios.NewWork(0);
    private CompiledGraph<Work> _graph = null!;

    [Params(2, 8, 32)]
    public int Branches { get; set; }

    [GlobalSetup]
    public void Setup() => _graph = Scenarios.Group(Branches);

    [Benchmark]
    public async Task<Work> Run() => Scenarios.Completed(await _graph.RunAsync("bench", _start, _store));
}

/// <summary>A map over no-op children.</summary>
[MemoryDiagnoser]
public class FanOutMap
{
    private readonly LatestOnlyStore _store = new();
    private readonly Work _start = Scenarios.NewWork(0);
    private CompiledGraph<Work> _graph = null!;

    [Params(8, 64)]
    public int Items { get; set; }

    [GlobalSetup]
    public void Setup() => _graph = Scenarios.Map(Items);

    [Benchmark]
    public async Task<Work> Run() => Scenarios.Completed(await _graph.RunAsync("bench", _start, _store));
}

/// <summary>
/// A run to an interrupt boundary and the resume that finishes it: freeze, flush, evict, rehydrate, with
/// a 1 KiB state.
/// </summary>
[MemoryDiagnoser]
public class Interrupt
{
    private readonly LatestOnlyStore _store = new();
    private readonly Work _start = Scenarios.NewWork(Scenarios.PayloadBytes);
    private readonly HumanSignal _approval = HumanSignal.Approve("bench");
    private readonly CompiledGraph<Work> _graph = Scenarios.Approval();

    [Benchmark]
    public async Task<Work> RoundTrip()
    {
        Scenarios.Interrupted(await _graph.RunAsync("bench", _start, _store));
        return Scenarios.Completed(await _graph.ResumeAsync("bench", _approval, _store));
    }
}

/// <summary>
/// The ten-node run with a checkpoint at boundaries only (none here) and after every node: what
/// <c>EveryNode</c> costs the engine before any store's I/O.
/// </summary>
[MemoryDiagnoser]
public class Checkpointing
{
    private readonly LatestOnlyStore _store = new();
    private readonly Work _start = Scenarios.NewWork(Scenarios.PayloadBytes);
    private readonly CompiledGraph<Work> _boundaries = Scenarios.Linear(Scenarios.RunNodes, HandlerKind.Async);
    private readonly CompiledGraph<Work> _everyNode = Scenarios.Durable();

    [Benchmark(Baseline = true)]
    public async Task<Work> Boundaries() => Scenarios.Completed(await _boundaries.RunAsync("bench", _start, _store));

    [Benchmark]
    public async Task<Work> EveryNode() => Scenarios.Completed(await _everyNode.RunAsync("bench", _start, _store));
}

/// <summary>One node emitting a thousand tokens to a reader that drains them.</summary>
[MemoryDiagnoser]
public class Streaming
{
    private const int TokenCount = 1000;
    private readonly LatestOnlyStore _store = new();
    private readonly Work _start = Scenarios.NewWork(0);
    private readonly CompiledGraph<Work> _graph = Scenarios.Streamer(TokenCount);

    [Benchmark(OperationsPerInvoke = TokenCount)]
    public async Task<Work> Tokens()
    {
        var (reader, completion) = _graph.RunStreaming("bench", _start, _store);
        var seen = 0;
        await foreach (var _ in reader.ReadAllAsync())
        {
            seen++;
        }

        if (seen < TokenCount)
        {
            throw new InvalidOperationException($"{seen} chunks for {TokenCount} tokens");
        }

        return Scenarios.Completed(await completion);
    }
}
