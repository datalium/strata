using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage.Memory;

namespace Datalium.Strata.Tests;

/// <summary>
/// The engine against a genuinely asynchronous checkpoint store (<c>design/19-Async-Queryable-Store.md</c>):
/// every save point awaits a save that completes later, a cancellation never cuts a save short, a store
/// that throws still ends the run's telemetry, and a store that only implements the synchronous members
/// keeps working through the interface's default async ones. Mirrors
/// <c>rust/strata-core/tests/async_store.rs</c>; keep the two in step.
/// </summary>
public class AsyncStoreTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Doc([property: JsonPropertyName("steps")] IReadOnlyList<string> Steps);

    private static Doc NewDoc() => new([]);

    private static Func<Doc, Doc> Did(string step) => doc => doc with { Steps = [.. doc.Steps, step] };

    private static HumanSignal Approve() => new(IsApproved: true, ReviewerToken: "x", FieldOverrides: null);

    /// <summary>
    /// An append-only store whose async calls really suspend: each waits <see cref="Delay"/>, and, when
    /// gated, says it has started and waits to be released before it saves. Its synchronous members are
    /// never called by the engine, so they throw.
    /// </summary>
    private sealed class SlowStore : ICheckpointStore
    {
        private readonly ConcurrentDictionary<string, List<Checkpoint>> _log = new();

        public TimeSpan Delay { get; init; } = TimeSpan.FromMilliseconds(10);

        public TaskCompletionSource? Started { get; init; }

        public TaskCompletionSource? Release { get; init; }

        public bool Throw { get; init; }

        public IReadOnlyList<Checkpoint> Saved(string threadId) =>
            _log.TryGetValue(threadId, out var log) ? [.. log] : [];

        public void Save(Checkpoint checkpoint) => throw new NotSupportedException("the engine saves through SaveAsync");

        public Checkpoint? Load(string threadId) => throw new NotSupportedException("the engine loads through LoadAsync");

        public async ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default)
        {
            if (Throw)
            {
                throw new InvalidOperationException("store bug");
            }

            await Task.Delay(Delay, cancellationToken);
            if (Started is not null && Release is not null)
            {
                Started.TrySetResult();
                await Release.Task.WaitAsync(cancellationToken);
            }

            _log.GetOrAdd(checkpoint.ThreadId, _ => []).Add(checkpoint);
        }

        public async ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default)
        {
            await Task.Delay(Delay, cancellationToken);
            return Saved(threadId) is { Count: > 0 } log ? log[^1] : null;
        }
    }

    /// <summary>A store written before the async members existed: synchronous only.</summary>
    private sealed class LegacyStore : ICheckpointStore
    {
        private readonly ConcurrentDictionary<string, Checkpoint> _latest = new();

        public void Save(Checkpoint checkpoint) => _latest[checkpoint.ThreadId] = checkpoint;

        public Checkpoint? Load(string threadId) => _latest.GetValueOrDefault(threadId);
    }

    /// <summary><c>Prepare</c> -> (boundary) -> <c>Execute</c>.</summary>
    private static CompiledGraph<Doc> FreezingGraph() =>
        new StateGraph<Doc>()
            .AddNode("Prepare", Did("prepare"))
            .AddInterruptBoundary("Prepare")
            .AddNode("Execute", Did("execute"))
            .AddEdge("Prepare", "Execute")
            .Compile();

    [Fact]
    public async Task Every_Save_Point_Waits_For_A_Store_That_Really_Suspends()
    {
        var store = new SlowStore();

        // Freeze, then resume: the frozen checkpoint, then the resume stamp.
        var graph = FreezingGraph();
        Assert.IsType<GraphExecutionResult<Doc>.Interrupted>(await graph.RunAsync("t", NewDoc(), store));
        Assert.Single(store.Saved("t"));
        var completed = Assert.IsType<GraphExecutionResult<Doc>.Completed>(await graph.ResumeAsync("t", Approve(), store));
        Assert.Equal(["prepare", "execute"], completed.State.Steps);
        Assert.Equal(2, store.Saved("t").Count);
        Assert.NotNull(store.Saved("t")[1].ResumedAt);

        // A failure.
        var failing = new StateGraph<Doc>()
            .AddFallibleNode("Charge", Doc (Doc _) => throw NodeException.Fatal("declined"))
            .Compile();
        Assert.IsType<GraphExecutionResult<Doc>.NodeFailed>(await failing.RunAsync("f", NewDoc(), store));
        Assert.Equal(CheckpointStatus.Failed, store.Saved("f")[0].Status);

        // A freeze inside a subgraph: the parent's checkpoint nests the child's.
        var parent = new StateGraph<Doc>()
            .AddSubgraph("Work", FreezingGraph(), doc => doc, (_, child) => child)
            .Compile();
        var interrupted = Assert.IsType<GraphExecutionResult<Doc>.Interrupted>(await parent.RunAsync("n", NewDoc(), store));
        Assert.Equal(["Work", "Prepare"], interrupted.WaitingAt);
        Assert.Equal(["Work", "Prepare"], store.Saved("n")[0].WaitingAt());
        Assert.IsType<GraphExecutionResult<Doc>.Completed>(await parent.ResumeAsync("n", Approve(), store));
    }

    [Fact]
    public async Task A_Cancellation_During_A_Boundary_Save_Lets_The_Save_Finish()
    {
        // design/17: a cancellation that arrives during an interrupt boundary's save lets the save
        // finish, and the run returns Interrupted. The engine never passes the run's token to a save.
        var store = new SlowStore
        {
            Delay = TimeSpan.Zero,
            Started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
            Release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        using var cts = new CancellationTokenSource();
        var run = FreezingGraph().RunAsync("t", NewDoc(), store, cts.Token);

        await store.Started!.Task.WaitAsync(Patience);
        await cts.CancelAsync();
        // Room for an engine that cancelled the save to give up on it.
        await Task.Delay(20);
        store.Release!.SetResult();

        Assert.IsType<GraphExecutionResult<Doc>.Interrupted>(await run.WaitAsync(Patience));
        var saved = Assert.Single(store.Saved("t"));
        Assert.Equal(CheckpointStatus.Interrupted, saved.Status);
    }

    [Fact]
    public async Task A_Run_Started_Cancelled_Still_Persists_Its_Cancelled_Checkpoint()
    {
        // The token has fired before the save even starts: an engine that passed it to the store would
        // never write the record cancellation exists to leave.
        var store = new SlowStore();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var result = await FreezingGraph().RunAsync("t", NewDoc(), store, cts.Token);

        Assert.IsType<GraphExecutionResult<Doc>.Cancelled>(result);
        var saved = Assert.Single(store.Saved("t"));
        Assert.Equal(CheckpointStatus.Cancelled, saved.Status);
    }

    [Fact]
    public async Task A_Store_That_Throws_Still_Ends_The_Run_Before_The_Exception_Propagates()
    {
        var channel = new RunEventChannel();
        var run = FreezingGraph().RunAsync("t", NewDoc(), new SlowStore { Throw = true }, new RunOptions { Observer = channel });

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => run);
        Assert.Equal("store bug", thrown.Message);
        var events = new List<RunEvent>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            events.Add(e);
        }

        Assert.IsType<RunCompleted>(events[^1]);
    }

    [Fact]
    public async Task A_Store_With_Only_The_Synchronous_Members_Still_Drives_The_Engine()
    {
        // Source compatibility: the default SaveAsync / LoadAsync call Save / Load.
        var store = new LegacyStore();
        var graph = FreezingGraph();

        Assert.IsType<GraphExecutionResult<Doc>.Interrupted>(await graph.RunAsync("t", NewDoc(), store));
        Assert.IsType<GraphExecutionResult<Doc>.Completed>(await graph.ResumeAsync("t", Approve(), store));
        Assert.NotNull(store.Load("t")!.ResumedAt);
    }

    [Fact]
    public async Task One_EfCore_Store_Serves_Concurrent_Runs()
    {
        // A DbContext isn't thread-safe; the store serializes its calls, so runs sharing it can't race.
        using var store = EfCoreCheckpointStore.CreateInMemory();
        var graph = FreezingGraph();

        await Task.WhenAll(Enumerable.Range(0, 16).Select(i => Task.Run(async () =>
        {
            Assert.IsType<GraphExecutionResult<Doc>.Interrupted>(await graph.RunAsync($"t{i}", NewDoc(), store));
            Assert.IsType<GraphExecutionResult<Doc>.Completed>(await graph.ResumeAsync($"t{i}", Approve(), store));
        })));

        var page = await store.ListThreadsAsync(new ThreadQuery { States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Resumed } });
        Assert.Equal(16, page.Threads.Count);
    }
}
