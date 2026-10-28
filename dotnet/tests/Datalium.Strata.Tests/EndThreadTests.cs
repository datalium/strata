using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using static Datalium.Strata.Tests.StateGraphInterruptionTests;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/end_thread.rs</c> (<c>design/28-Ending-A-Thread.md</c>, SG-34): ending a
/// thread where it stands, through the public API only. Keep the two in step.
/// </summary>
public class EndThreadTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Order(
        [property: JsonPropertyName("amount")] int Amount,
        [property: JsonPropertyName("log")] IReadOnlyList<string> Log,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy);

    public sealed record Job(
        [property: JsonPropertyName("n")] int N,
        [property: JsonPropertyName("trail")] IReadOnlyList<string> Trail,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy);

    /// <summary>An <see cref="InMemoryCheckpointStore"/> that lets another writer in just before the first conditional save.</summary>
    private sealed class Interloper(InMemoryCheckpointStore inner, Checkpoint sneakIn) : ICheckpointStore
    {
        private Checkpoint? _sneakIn = sneakIn;

        public void Save(Checkpoint checkpoint) => inner.Save(checkpoint);

        public Checkpoint? Load(string threadId) => inner.Load(threadId);

        public ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
            inner.LoadLatestAsync(threadId, cancellationToken);

        public ValueTask<bool> SaveIfLatestAsync(
            Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Exchange(ref _sneakIn, null) is { } other)
            {
                inner.Save(other);
            }

            return inner.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken);
        }
    }

    /// <summary>Only <c>Save</c> and <c>Load</c>: the interface's defaults supply the rest.</summary>
    private sealed class PlainStore : ICheckpointStore
    {
        private readonly Dictionary<string, Checkpoint> _latest = [];

        public void Save(Checkpoint checkpoint) => _latest[checkpoint.ThreadId] = checkpoint;

        public Checkpoint? Load(string threadId) => _latest.GetValueOrDefault(threadId);
    }

    // --- Graphs ---

    private static (CompiledGraph<WireTransferState> Graph, Func<int> Executed) CountingWireTransferGraph()
    {
        var executed = 0;
        var graph = new StateGraph<WireTransferState>()
            .AddNode("PrepareWireTransfer", s => s with { InvoiceGenerated = true })
            .AddInterruptBoundary("PrepareWireTransfer")
            .AddNode("ExecuteWireTransfer", s =>
            {
                Interlocked.Increment(ref executed);
                return s with { IsCompleted = true };
            })
            .AddEdge("PrepareWireTransfer", "ExecuteWireTransfer")
            .Compile();
        return (graph, () => Volatile.Read(ref executed));
    }

    /// <summary><c>nested.json</c>'s graph: <c>Prepare</c> -> <c>Work</c> (<c>Draft</c> -> <c>Approve</c>, a boundary -> <c>Send</c>) -> <c>Finish</c>.</summary>
    private static CompiledGraph<Order> OrderGraph()
    {
        var child = new StateGraph<Job>()
            .AddNode("Draft", j => j with { N = j.N * 2, Trail = [.. j.Trail, "draft"] })
            .AddNode("Approve", j => j with { Trail = [.. j.Trail, "approve"] })
            .AddInterruptBoundary("Approve")
            .AddNode("Send", j => j with { N = j.N + 1, Trail = [.. j.Trail, "send"] })
            .AddEdge("Draft", "Approve")
            .AddEdge("Approve", "Send")
            .Compile();
        return new StateGraph<Order>()
            .AddNode("Prepare", o => o with { Log = [.. o.Log, "prepared"] })
            .AddSubgraph(
                "Work",
                child,
                o => new Job(o.Amount, [], null),
                (entry, job) => entry with { Amount = job.N, Log = [.. entry.Log, .. job.Trail], ApprovedBy = job.ApprovedBy })
            .AddNode("Finish", o => o with { Log = [.. o.Log, "finished"] })
            .AddEdge("Prepare", "Work")
            .AddEdge("Work", "Finish")
            .Compile();
    }

    private static Order NewOrder() => new(5, [], null);

    /// <summary><c>Approve</c> (a boundary) -> <c>Apply</c>, which fails fatally.</summary>
    private static CompiledGraph<WireTransferState> FailingApplyGraph() =>
        new StateGraph<WireTransferState>()
            .AddNode("Approve", s => s)
            .AddInterruptBoundary("Approve")
            .AddFallibleNode("Apply", (WireTransferState _) => throw NodeException.Fatal("gateway refused"))
            .AddEdge("Approve", "Apply")
            .Compile();

    /// <summary><c>Charge</c>, an async node that never finishes on its own.</summary>
    private static CompiledGraph<WireTransferState> HangingGraph() =>
        new StateGraph<WireTransferState>()
            .AddNode("Charge", async (WireTransferState s, NodeContext _, CancellationToken ct) =>
            {
                await Task.Delay(Timeout.Infinite, ct);
                return s;
            })
            .Compile();

    // --- Helpers ---

    private static Checkpoint LifecycleFixture(string name, [CallerFilePath] string sourceFilePath = "")
    {
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(sourceFilePath)!, "..", "..", "..", "schema", "fixtures", "checkpoint-lifecycle", name));
        return Checkpoint.FromWireJson(JsonNode.Parse(File.ReadAllText(path))!);
    }

    private static Checkpoint WithoutTimestamps(Checkpoint checkpoint) =>
        checkpoint with
        {
            CreatedAt = string.Empty,
            Nested = checkpoint.Nested is null ? null : WithoutTimestamps(checkpoint.Nested),
        };

    private static void AssertMatchesFixture(string name, Checkpoint actual)
    {
        var expected = WithoutTimestamps(LifecycleFixture(name)).ToWireJson();
        var got = WithoutTimestamps(actual).ToWireJson();
        Assert.True(JsonNode.DeepEquals(expected, got), got.ToJsonString());
    }

    private static GraphExecutionResult<T>.Cancelled Ended<T>(GraphExecutionResult<T> result)
    {
        if (result is GraphExecutionResult<T>.Failed failed)
        {
            Assert.Fail($"expected an ended thread, got: {failed.Message}");
        }

        var cancelled = Assert.IsType<GraphExecutionResult<T>.Cancelled>(result);
        Assert.Equal(CancelReason.Ended, cancelled.Reason);
        return cancelled;
    }

    private static string Refused<T>(GraphExecutionResult<T> result) =>
        Assert.IsAssignableFrom<GraphExecutionResult<T>.Failed>(result).Message;

    private static async Task<List<Checkpoint>> LogOf(InMemoryCheckpointStore store, string threadId) =>
        [.. (await store.GetLogAsync(threadId)).Entries.Select(e => e.Checkpoint)];

    private static async Task<(CompiledGraph<WireTransferState> Graph, Func<int> Executed)> FrozenWireTransfer(
        string threadId, ICheckpointStore store)
    {
        var (graph, executed) = CountingWireTransferGraph();
        Assert.IsType<GraphExecutionResult<WireTransferState>.Interrupted>(
            await graph.RunAsync(threadId, InitialState(), store));
        return (graph, executed);
    }

    // --- Ending ---

    [Fact]
    public async Task Ending_An_Interrupted_Thread_Writes_An_Ended_Checkpoint_Where_It_Stood()
    {
        var store = new InMemoryCheckpointStore();
        var (graph, executed) = CountingWireTransferGraph();
        await graph.RunAsync("t", InitialState(), store, new RunOptions { Metadata = { ["tenant"] = "acme" } });

        var ended = Ended(await graph.EndAsync(
            "t", ThreadEnd.By("oncall"), store, new RunOptions { Metadata = { ["incident"] = "INC-7" } }));

        Assert.Equal("PrepareWireTransfer", ended.Node);
        Assert.True(ended.State.InvoiceGenerated);
        Assert.Equal(["PrepareWireTransfer"], ended.StoppedAt);
        var checkpoint = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Ended, checkpoint.Status);
        Assert.Equal(CheckpointSchema.EndedVersion, checkpoint.SchemaVersion);
        Assert.Equal("PrepareWireTransfer", checkpoint.SuspendedNode);
        Assert.Null(checkpoint.NextNode);
        Assert.Null(checkpoint.ResumedAt);

        // The stored caller keys, the end's laid over them, and the end's own.
        Assert.Equal(
            new Dictionary<string, string>
            {
                ["tenant"] = "acme",
                ["incident"] = "INC-7",
                [ThreadEnd.ReasonKey] = "requested",
                [ThreadEnd.EndedByKey] = "oncall",
            },
            checkpoint.Metadata);
        Assert.Equal(2, (await LogOf(store, "t")).Count);
        Assert.Equal(0, executed());
    }

    [Fact]
    public async Task Ending_Matches_The_Shared_Ended_Fixture()
    {
        var store = new InMemoryCheckpointStore();
        var (graph, _) = await FrozenWireTransfer("thread_ended_demo", store);

        Ended(await graph.EndAsync("thread_ended_demo", ThreadEnd.By("oncall"), store));

        AssertMatchesFixture("ended.json", store.Load("thread_ended_demo")!);
    }

    [Fact]
    public async Task Ending_A_Nested_Wait_Ends_Every_Level_And_Matches_The_Shared_Fixture()
    {
        var store = new InMemoryCheckpointStore();
        var graph = OrderGraph();
        await graph.RunAsync(
            "thread_nested_ended_demo", NewOrder(), store, new RunOptions { Metadata = { ["tenant"] = "acme" } });

        var ended = Ended(await graph.EndAsync("thread_nested_ended_demo", store));

        Assert.Equal("Work", ended.Node);
        Assert.Equal(["prepared"], ended.State.Log);
        Assert.Equal(["Work", "Approve"], ended.StoppedAt);
        var checkpoint = store.Load("thread_nested_ended_demo")!;
        Assert.Equal(CheckpointStatus.Ended, checkpoint.Nested!.Status);
        Assert.Null(checkpoint.Nested.NextNode);
        AssertMatchesFixture("nested-ended.json", checkpoint);
    }

    [Fact]
    public async Task Ending_A_Failed_Thread_Keeps_The_Decision_And_Drops_The_Failure_Keys()
    {
        var store = new InMemoryCheckpointStore();
        var graph = FailingApplyGraph();
        await graph.RunAsync("t", InitialState(), store);
        Assert.IsAssignableFrom<GraphExecutionResult<WireTransferState>.Failed>(
            await graph.ResumeAsync("t", HumanSignal.Approve("alice"), store));
        var failed = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Failed, failed.Status);

        var ended = Ended(await graph.EndAsync("t", store));

        Assert.Equal("Apply", ended.Node);
        var checkpoint = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Ended, checkpoint.Status);
        Assert.Equal(failed.Decision!.ToWireJson().ToJsonString(), checkpoint.Decision!.ToWireJson().ToJsonString());
        Assert.Equal("alice", checkpoint.Decision.Reviewer);
        Assert.Equal(new Dictionary<string, string> { [ThreadEnd.ReasonKey] = "requested" }, checkpoint.Metadata);
    }

    [Fact]
    public async Task Ending_A_Cancelled_Thread_Drops_The_Cancel_Reason()
    {
        var store = new InMemoryCheckpointStore();
        var graph = HangingGraph();
        var cancelled = Assert.IsType<GraphExecutionResult<WireTransferState>.Cancelled>(
            await graph.RunAsync("t", InitialState(), store, new CancellationToken(canceled: true)));
        Assert.Equal(CancelReason.Requested, cancelled.Reason);

        var ended = Ended(await graph.EndAsync("t", store));

        Assert.Equal("Charge", ended.Node);
        var checkpoint = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Ended, checkpoint.Status);
        Assert.False(checkpoint.Metadata!.ContainsKey("cancel_reason"));
    }

    [Fact]
    public async Task Ending_An_Ended_Thread_Returns_The_Same_Result_And_Writes_Nothing()
    {
        var store = new InMemoryCheckpointStore();
        var (graph, _) = await FrozenWireTransfer("t", store);
        var first = Ended(await graph.EndAsync("t", ThreadEnd.By("oncall"), store));
        var log = await LogOf(store, "t");

        var second = Ended(await graph.EndAsync("t", ThreadEnd.By("someone-else"), store));

        Assert.Equal(first.Node, second.Node);
        Assert.Equal(first.State, second.State);
        Assert.Equal(first.StoppedAt, second.StoppedAt);
        Assert.Equal(log, await LogOf(store, "t"));
    }

    // --- Resuming an ended thread ---

    [Fact]
    public async Task A_Resume_Of_An_Ended_Thread_Is_Refused_And_Writes_Nothing()
    {
        var store = new InMemoryCheckpointStore();
        var (graph, executed) = await FrozenWireTransfer("t", store);
        Ended(await graph.EndAsync("t", store));
        var log = await LogOf(store, "t");

        var message = Refused(await graph.ResumeAsync("t", HumanSignal.Approve("alice"), store));
        Assert.Contains("was ended", message);

        var (reader, completion) = graph.ResumeStreaming("t", HumanSignal.Approve("alice"), store);
        Assert.IsAssignableFrom<GraphExecutionResult<WireTransferState>.Failed>(await completion.WaitAsync(Patience));
        Assert.Empty(await reader.ReadAllAsync().ToListAsync());
        Assert.Equal(log, await LogOf(store, "t"));
        Assert.Equal(0, executed());
    }

    [Fact]
    public async Task A_Resume_Of_An_Ended_Nested_Thread_Is_Refused()
    {
        var store = new InMemoryCheckpointStore();
        var graph = OrderGraph();
        await graph.RunAsync("t", NewOrder(), store);
        Ended(await graph.EndAsync("t", store));

        Assert.Contains("was ended", Refused(await graph.ResumeAsync("t", HumanSignal.Approve("alice"), store)));
    }

    // --- Refusals ---

    [Fact]
    public async Task An_Unknown_Thread_Is_Refused() =>
        Assert.Contains("no checkpoint found", Refused(await OrderGraph().EndAsync("nobody", new InMemoryCheckpointStore())));

    [Fact]
    public async Task A_Resumed_Thread_Is_Refused_And_Nothing_Is_Written()
    {
        var store = new InMemoryCheckpointStore();
        var (graph, _) = await FrozenWireTransfer("t", store);
        store.Save(store.Load("t")! with { ResumedAt = "2026-10-06T01:00:00Z" });
        var log = await LogOf(store, "t");

        var message = Refused(await graph.EndAsync("t", store));

        Assert.Contains("may still be running", message);
        Assert.Equal(log, await LogOf(store, "t"));
    }

    [Fact]
    public async Task A_Completed_Thread_Is_Refused_And_Nothing_Is_Written()
    {
        var store = new InMemoryCheckpointStore();
        store.Save(new Checkpoint
        {
            SchemaVersion = CheckpointSchema.Version,
            ThreadId = "t",
            Status = CheckpointStatus.Completed,
            FrozenState = System.Text.Json.JsonSerializer.SerializeToNode(InitialState()),
            CreatedAt = "2026-10-06T00:00:00Z",
        });

        var message = Refused(await WireTransferGraph().EndAsync("t", store));

        Assert.Contains("has completed", message);
        Assert.Single(await LogOf(store, "t"));
    }

    // --- Races ---

    [Fact]
    public async Task An_End_That_Loses_To_A_Resume_Is_Refused()
    {
        var log = new InMemoryCheckpointStore();
        var (graph, _) = await FrozenWireTransfer("t", log);
        var stamp = log.Load("t")! with { ResumedAt = "2026-10-06T01:00:00Z" };

        var message = Refused(await graph.EndAsync("t", new Interloper(log, stamp)));

        Assert.Contains("may still be running", message);
        Assert.Same(stamp, log.Load("t"));
    }

    [Fact]
    public async Task An_End_That_Loses_To_Another_End_Returns_Its_Result()
    {
        var log = new InMemoryCheckpointStore();
        var (graph, _) = await FrozenWireTransfer("t", log);

        // What the other end wrote.
        var other = new InMemoryCheckpointStore();
        other.Save(log.Load("t")!);
        Ended(await graph.EndAsync("t", ThreadEnd.By("first"), other));
        var theirs = other.Load("t")!;

        Ended(await graph.EndAsync("t", ThreadEnd.By("second"), new Interloper(log, theirs)));

        Assert.Same(theirs, log.Load("t"));
        Assert.Equal(2, (await LogOf(log, "t")).Count);
    }

    [Fact]
    public async Task A_Resume_That_Loses_To_An_End_Runs_Nothing()
    {
        var log = new InMemoryCheckpointStore();
        var (graph, executed) = await FrozenWireTransfer("t", log);
        var other = new InMemoryCheckpointStore();
        other.Save(log.Load("t")!);
        Ended(await graph.EndAsync("t", other));

        var message = Refused(await graph.ResumeAsync("t", HumanSignal.Approve("alice"), new Interloper(log, other.Load("t")!)));

        Assert.Contains("changed before it could be resumed", message);
        Assert.Equal(0, executed());
        Assert.Equal(CheckpointStatus.Ended, log.Load("t")!.Status);
    }

    [Fact]
    public async Task A_Store_Without_The_Conditional_Save_Still_Ends_A_Thread()
    {
        var store = new PlainStore();
        var graph = WireTransferGraph();
        await graph.RunAsync("t", InitialState(), store);

        Ended(await graph.EndAsync("t", store));

        Assert.Equal(CheckpointStatus.Ended, store.Load("t")!.Status);
    }

    // --- Observers ---

    private static string Describe(RunEvent e) => e switch
    {
        RunStarted s => $"RunStarted {s.Mode} {s.Node}",
        CheckpointSaved c => $"CheckpointSaved {c.Status} {string.Join('/', c.WaitingAt)}",
        RunCompleted c => $"RunCompleted {c.Outcome} {c.CancelReason}",
        _ => e.GetType().Name,
    };

    private static async Task<List<string>> ObserveEnd(CompiledGraph<Order> graph, ICheckpointStore store)
    {
        var channel = new RunEventChannel();
        Ended(await graph.EndAsync("t", new ThreadEnd(), store, new RunOptions { Observer = channel }));
        var events = new List<string>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            events.Add(Describe(e));
        }

        return events;
    }

    [Fact]
    public async Task An_End_Is_Observed_As_A_Run_That_Was_Cancelled_Because_It_Ended()
    {
        var store = new InMemoryCheckpointStore();
        var graph = OrderGraph();
        await graph.RunAsync("t", NewOrder(), store);

        Assert.Equal(
            ["RunStarted End Work", "CheckpointSaved Ended Work/Approve", "RunCompleted Cancelled Ended"],
            await ObserveEnd(graph, store));

        // The idempotent end writes nothing, so it saves nothing.
        Assert.Equal(["RunStarted End Work", "RunCompleted Cancelled Ended"], await ObserveEnd(graph, store));
    }
}
