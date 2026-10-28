using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Benchmarks;

/// <summary>
/// The state every scenario runs on: a counter each node adds one to, and a payload that only gives the
/// state a size (a checkpoint serializes it).
/// </summary>
public sealed record Work(long Step, string Payload);

/// <summary>
/// Which kind of handler a scenario's nodes have. A sync handler runs on the thread pool, an async one
/// on the executor.
/// </summary>
public enum HandlerKind
{
    Async,
    Sync,
}

/// <summary>
/// The scenarios Strata's benchmarks run (<c>design/37-Benchmarks.md</c>, SG-27): the graphs, the state and
/// a store that costs nothing. The BenchmarkDotNet microbenchmarks (<c>EngineBenchmarks.cs</c>) and the
/// profile (<c>Profile.cs</c>) both build on them, so a number in one is about the same graph as a number
/// in the other. Mirrors <c>rust/strata-bench/src/lib.rs</c>: same scenario names, same graphs, same
/// parameters. Keep the two in step.
/// </summary>
public static class Scenarios
{
    /// <summary>The nodes of the graph the run-level scenarios use.</summary>
    public const int RunNodes = 10;

    /// <summary>The payload of a state that stands for a real one: 1 KiB of text.</summary>
    public const int PayloadBytes = 1024;

    public static readonly HandlerKind[] Handlers = [HandlerKind.Async, HandlerKind.Sync];

    /// <summary>A state at step 0 with <paramref name="payloadBytes"/> of payload.</summary>
    public static Work NewWork(int payloadBytes) => new(0, new string('x', payloadBytes));

    public static string Name(this HandlerKind handler) => handler == HandlerKind.Sync ? "sync" : "async";

    private static Work Stepped(Work work) => work with { Step = work.Step + 1 };

    private static string NodeName(int index) => "n" + index.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// <paramref name="nodes"/> nodes in a line, each adding one to the step: the cost of a run is the
    /// engine's, since the handlers do nothing.
    /// </summary>
    public static CompiledGraph<Work> Linear(int nodes, HandlerKind handler, CompilationOptions? options = null)
    {
        var graph = new StateGraph<Work>();
        for (var index = 0; index < nodes; index++)
        {
            graph = handler == HandlerKind.Sync
                ? graph.AddNode(NodeName(index), Stepped)
                : graph.AddNode(NodeName(index), (work, _, _) => ValueTask.FromResult(Stepped(work)));
        }

        for (var index = 1; index < nodes; index++)
        {
            graph = graph.AddEdge(NodeName(index - 1), NodeName(index));
        }

        return graph.Compile(options ?? new CompilationOptions());
    }

    /// <summary>
    /// <see cref="Linear"/> with <see cref="RunNodes"/> async nodes that checkpoints after every node
    /// (<c>design/33-Durable-Execution.md</c>).
    /// </summary>
    public static CompiledGraph<Work> Durable() =>
        Linear(RunNodes, HandlerKind.Async, new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });

    /// <summary>
    /// One parallel group of <paramref name="branches"/> async branches, each adding one to the step; the
    /// reducer adds up what the branches added.
    /// </summary>
    public static CompiledGraph<Work> Group(int branches)
    {
        var fan = new ParallelBranches<Work>();
        for (var index = 0; index < branches; index++)
        {
            fan = fan.Branch(
                "b" + index.ToString(CultureInfo.InvariantCulture),
                (work, _, _) => ValueTask.FromResult(Stepped(work)));
        }

        return new StateGraph<Work>()
            .AddParallelGroup(
                "fan",
                fan,
                (entry, results) => entry with { Step = entry.Step + results.Sum(result => result.Step - entry.Step) })
            .Compile();
    }

    /// <summary>
    /// One map over <paramref name="items"/> items, each run through a one-node child graph, at the map's
    /// default concurrency (8); the reducer counts the children that finished.
    /// </summary>
    public static CompiledGraph<Work> Map(int items)
    {
        var child = Linear(1, HandlerKind.Async);
        return new StateGraph<Work>()
            .AddMap(
                "each",
                _ => Enumerable.Range(0, items),
                item => item.ToString(CultureInfo.InvariantCulture),
                child,
                (_, _) => NewWork(0),
                (entry, results) => entry with { Step = entry.Step + results.Sum(result => result.Step) })
            .Compile();
    }

    /// <summary>
    /// <c>prepare</c>, then <c>approve</c> (an interrupt boundary), then <c>finish</c>: a run freezes after
    /// <c>approve</c>, and a resume runs <c>finish</c>.
    /// </summary>
    public static CompiledGraph<Work> Approval() =>
        new StateGraph<Work>()
            .AddNode("prepare", (work, _, _) => ValueTask.FromResult(Stepped(work)))
            .AddNode("approve", (work, _, _) => ValueTask.FromResult(Stepped(work)))
            .AddNode("finish", (work, _, _) => ValueTask.FromResult(Stepped(work)))
            .AddEdge("prepare", "approve")
            .AddEdge("approve", "finish")
            .AddInterruptBoundary("approve")
            .Compile();

    /// <summary>One streaming node that emits <paramref name="tokens"/> tokens.</summary>
    public static CompiledGraph<Work> Streamer(int tokens) =>
        new StateGraph<Work>()
            .AddStreamingNode("speak", async (work, context, cancellationToken) =>
            {
                var stream = context.Stream ?? throw new InvalidOperationException("not a streaming run");
                for (var index = 0; index < tokens; index++)
                {
                    await stream.EmitTokenAsync("token", cancellationToken).ConfigureAwait(false);
                }

                return Stepped(work);
            })
            .Compile();

    /// <summary><see cref="Linear"/> whose handlers record the gap between them in <paramref name="gaps"/>.</summary>
    public static CompiledGraph<Work> Timed(int nodes, HandlerKind handler, Gaps gaps)
    {
        Work Step(Work work)
        {
            gaps.Enter(first: work.Step == 0);
            var next = Stepped(work);
            gaps.Exit();
            return next;
        }

        var graph = new StateGraph<Work>();
        for (var index = 0; index < nodes; index++)
        {
            graph = handler == HandlerKind.Sync
                ? graph.AddNode(NodeName(index), Step)
                : graph.AddNode(NodeName(index), (work, _, _) => ValueTask.FromResult(Step(work)));
        }

        for (var index = 1; index < nodes; index++)
        {
            graph = graph.AddEdge(NodeName(index - 1), NodeName(index));
        }

        return graph.Compile();
    }

    /// <summary>
    /// The final state of a run that completed; throws on any other outcome, so a benchmark can't time a
    /// run that failed.
    /// </summary>
    public static Work Completed(GraphExecutionResult<Work> result) => result switch
    {
        GraphExecutionResult<Work>.Completed completed => completed.State,
        GraphExecutionResult<Work>.Interrupted interrupted =>
            throw new InvalidOperationException($"the run froze at '{interrupted.SuspendedNode}'"),
        GraphExecutionResult<Work>.Cancelled cancelled =>
            throw new InvalidOperationException($"the run was cancelled at '{cancelled.Node}': {cancelled.Reason}"),
        GraphExecutionResult<Work>.Failed failed => throw new InvalidOperationException($"the run failed: {failed.Message}"),
        _ => throw new InvalidOperationException($"unexpected result {result}"),
    };

    /// <summary>Throws unless a run froze at an interrupt boundary.</summary>
    public static void Interrupted(GraphExecutionResult<Work> result)
    {
        switch (result)
        {
            case GraphExecutionResult<Work>.Interrupted:
                return;
            case GraphExecutionResult<Work>.Completed:
                throw new InvalidOperationException("the run completed");
            case GraphExecutionResult<Work>.Cancelled cancelled:
                throw new InvalidOperationException($"the run was cancelled at '{cancelled.Node}': {cancelled.Reason}");
            case GraphExecutionResult<Work>.Failed failed:
                throw new InvalidOperationException($"the run failed: {failed.Message}");
            default:
                throw new InvalidOperationException($"unexpected result {result}");
        }
    }

    /// <summary>
    /// An <c>interrupted</c> checkpoint of <see cref="Approval"/> with a <see cref="PayloadBytes"/> state:
    /// what the store scenarios save, under one thread id after another.
    /// </summary>
    public static async Task<Checkpoint> SampleCheckpointAsync()
    {
        var store = new LatestOnlyStore();
        Interrupted(await Approval().RunAsync("sample", NewWork(PayloadBytes), store).ConfigureAwait(false));
        return store.Load("sample") ?? throw new InvalidOperationException("no checkpoint");
    }
}

/// <summary>
/// What the engine does between one handler returning and the next one starting, measured from inside the
/// handlers: each notes the time it starts and the time it returns, and the gap from one node's return to
/// the next node's start is a sample. The first node of a run has no gap.
/// </summary>
public sealed class Gaps
{
    private readonly List<long> _samples = [];
    private long _lastExit;

    internal void Enter(bool first)
    {
        var now = Stopwatch.GetTimestamp();
        if (!first)
        {
            var gap = Math.Max(0, now - Volatile.Read(ref _lastExit));
            lock (_samples)
            {
                _samples.Add(gap);
            }
        }
    }

    internal void Exit() => Volatile.Write(ref _lastExit, Stopwatch.GetTimestamp());

    /// <summary>The gaps recorded since the last call, in nanoseconds.</summary>
    public List<long> Take()
    {
        lock (_samples)
        {
            var taken = _samples.ConvertAll(Stats.Nanoseconds);
            _samples.Clear();
            return taken;
        }
    }
}

/// <summary>
/// A checkpoint store that keeps a thread's latest checkpoint and nothing else, in memory, and counts the
/// saves. The engine's own cost is what is left when the store costs this little, and a thread run a
/// million times doesn't grow a log.
/// </summary>
public sealed class LatestOnlyStore : ICheckpointStore
{
    private readonly ConcurrentDictionary<string, Checkpoint> _checkpoints = new(StringComparer.Ordinal);
    private long _saves;

    /// <summary>The checkpoints saved so far.</summary>
    public long Saves => Interlocked.Read(ref _saves);

    public void Save(Checkpoint checkpoint)
    {
        Interlocked.Increment(ref _saves);
        _checkpoints[checkpoint.ThreadId] = checkpoint;
    }

    public Checkpoint? Load(string threadId) => _checkpoints.GetValueOrDefault(threadId);
}
