using System.Text.Json;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/subgraphs.rs</c> (<c>design/15-Subgraph-Composition.md</c>): a
/// compiled child graph with its own state type runs as one node of a parent, a child failure
/// becomes a failure of that node (keeping its transient/fatal kind, so the parent's recovery
/// policy applies), and a subgraph is one ordinary node to the rest of the graph. Keep the two in
/// step.
/// </summary>
public class SubgraphTests
{
    /// <summary>The parent's state.</summary>
    public sealed record Order(
        [property: JsonPropertyName("amount")] int Amount,
        [property: JsonPropertyName("log")] IReadOnlyList<string> Log);

    /// <summary>A child's state — a different type from the parent's on purpose.</summary>
    public sealed record Job(
        [property: JsonPropertyName("n")] int N,
        [property: JsonPropertyName("trail")] IReadOnlyList<string> Trail);

    private sealed class TestStore : ICheckpointStore
    {
        private readonly Dictionary<string, Checkpoint> _checkpoints = [];

        public IReadOnlyList<string> ThreadIds => [.. _checkpoints.Keys.Order(StringComparer.Ordinal)];

        public void Save(Checkpoint checkpoint) => _checkpoints[checkpoint.ThreadId] = checkpoint;

        public Checkpoint? Load(string threadId) => _checkpoints.GetValueOrDefault(threadId);
    }

    private static Order NewOrder(int amount) => new(amount, []);

    private static Func<Job, Job> Step(string label, int add) =>
        job => job with { N = job.N + add, Trail = [.. job.Trail, label] };

    private static Func<Order, Order> Note(string text) =>
        order => order with { Log = [.. order.Log, text] };

    /// <summary><c>Double</c> -> <c>AddOne</c>: <c>n -> 2n + 1</c>.</summary>
    private static CompiledGraph<Job> ArithmeticChild() =>
        new StateGraph<Job>()
            .AddNode("Double", job => job with { N = job.N * 2, Trail = [.. job.Trail, "double"] })
            .AddNode("AddOne", Step("add_one", 1))
            .AddEdge("Double", "AddOne")
            .Compile();

    private static Job ToJob(Order order) => new(order.Amount, []);

    private static Order FromJob(Order entry, Job job) =>
        entry with { Amount = job.N, Log = [.. entry.Log, .. job.Trail] };

    /// <summary><c>Prepare</c> -> <c>Work</c> (the subgraph) -> <c>Finish</c>.</summary>
    private static CompiledGraph<Order> GraphAround(CompiledGraph<Job> child, RecoveryPolicy? policy = null)
    {
        var graph = new StateGraph<Order>()
            .AddNode("Prepare", Note("prepared"))
            .AddSubgraph("Work", child, ToJob, FromJob)
            .AddNode("Finish", Note("finished"))
            .AddEdge("Prepare", "Work")
            .AddEdge("Work", "Finish");
        if (policy is not null)
        {
            // A fallback node has no ordinary edge into it, so it's only added when a policy
            // actually routes there (otherwise it'd be an orphan).
            if (policy.Fallback is not null)
            {
                graph.AddNode("Fallback", Note("fell back"));
            }

            graph.SetRecoveryPolicy("Work", policy);
        }

        return graph.Compile();
    }

    private static TState Completed<TState>(GraphExecutionResult<TState> result) =>
        Assert.IsType<GraphExecutionResult<TState>.Completed>(result).State;

    /// <summary>
    /// A child whose only node fails <paramref name="failures"/> times (with <paramref name="error"/>),
    /// then succeeds. <paramref name="calls"/> counts every attempt.
    /// </summary>
    private static CompiledGraph<Job> FlakyChild(
        Counter calls, int failures, Func<string, NodeException> error, int ownRetries) =>
        new StateGraph<Job>()
            .AddFallibleNode("Flaky", job =>
            {
                var call = calls.Increment();
                if (call <= failures)
                {
                    throw error($"call {call} failed");
                }

                return job with { N = job.N + 100, Trail = [.. job.Trail, "flaky"] };
            })
            .SetRecoveryPolicy("Flaky", RecoveryPolicy.Retry(ownRetries))
            .Compile();

    private sealed class Counter
    {
        private int _value;

        public int Value => Volatile.Read(ref _value);

        public int Increment() => Interlocked.Increment(ref _value);
    }

    private static NodeException Transient(string message) => NodeException.Transient(message);

    private static NodeException Fatal(string message) => NodeException.Fatal(message);

    [Fact]
    public async Task A_Child_Graph_With_Its_Own_State_Type_Runs_As_One_Node()
    {
        var graph = GraphAround(ArithmeticChild());

        var result = Completed(await graph.RunAsync("t", NewOrder(5), new TestStore()));

        Assert.Equal(11, result.Amount);
        Assert.Equal(["prepared", "double", "add_one", "finished"], result.Log);
    }

    [Fact]
    public async Task The_Output_Mapper_Receives_The_State_The_Node_Started_With()
    {
        var child = new StateGraph<Job>().AddNode("Only", Step("only", 1)).Compile();
        // The output mapper keeps the parent's own log rather than the child's.
        var graph = new StateGraph<Order>()
            .AddNode("Prepare", Note("prepared"))
            .AddSubgraph("Work", child, ToJob, (entry, job) => entry with { Amount = job.N })
            .AddEdge("Prepare", "Work")
            .Compile();

        var result = Completed(await graph.RunAsync("t", NewOrder(1), new TestStore()));

        Assert.Equal(2, result.Amount);
        Assert.Equal(["prepared"], result.Log);
    }

    [Fact]
    public async Task The_Childs_Own_Conditional_Edges_Run_Inside_The_Node()
    {
        // A child that loops (guarded) until n >= 10.
        var child = new StateGraph<Job>()
            .AddNode("Grow", Step("grow", 4))
            .AddNode("Done", Step("done", 0))
            .AddConditionalEdge("Grow", "Grow", job => job.N < 10)
            .AddEdge("Grow", "Done")
            .Compile();
        var graph = GraphAround(child);

        var result = Completed(await graph.RunAsync("t", NewOrder(1), new TestStore()));

        Assert.Equal(13, result.Amount);
        Assert.Equal(["prepared", "grow", "grow", "grow", "done", "finished"], result.Log);
    }

    [Fact]
    public async Task A_Child_Recovers_Itself_And_The_Parent_Never_Sees_The_Failure()
    {
        var calls = new Counter();
        // Fails twice, succeeds on the third attempt; the child's own policy allows two retries.
        var child = FlakyChild(calls, failures: 2, Transient, ownRetries: 2);
        var graph = GraphAround(child, RecoveryPolicy.Default);
        var store = new TestStore();

        var result = Completed(await graph.RunAsync("t", NewOrder(1), store));

        Assert.Equal(3, calls.Value);
        Assert.Equal(101, result.Amount);
        Assert.Equal(["prepared", "flaky", "finished"], result.Log);
        Assert.Empty(store.ThreadIds);
    }

    [Fact]
    public async Task A_Fatal_Child_Failure_Fails_The_Parent_Node_And_Persists_The_Parents_State()
    {
        var calls = new Counter();
        var child = FlakyChild(calls, failures: int.MaxValue, Fatal, ownRetries: 5);
        var graph = GraphAround(child);
        var store = new TestStore();

        var failed = Assert.IsType<GraphExecutionResult<Order>.NodeFailed>(
            await graph.RunAsync("order-1", NewOrder(1), store));

        Assert.Equal("Work", failed.Node);
        Assert.Equal(1, failed.Attempts);
        Assert.Equal("subgraph 'Work' failed at 'Flaky' after 1 attempt(s): call 1 failed", failed.Message);

        // The child's own policy would allow retries, but a fatal error skips them.
        Assert.Equal(1, calls.Value);

        // The one checkpoint is the *parent's*, holding the parent's state from before the
        // subgraph node ran.
        Assert.Equal(["order-1"], store.ThreadIds);
        var checkpoint = store.Load("order-1")!;
        Assert.Equal(CheckpointStatus.Failed, checkpoint.Status);
        Assert.Equal("Work", checkpoint.SuspendedNode);
        var frozen = checkpoint.FrozenState.Deserialize<Order>()!;
        Assert.Equal(1, frozen.Amount);
        Assert.Equal(["prepared"], frozen.Log);
        Assert.Equal("fatal", checkpoint.Metadata!["error_kind"]);
    }

    [Fact]
    public async Task A_Transient_Child_Failure_Is_Transient_To_The_Parent_And_A_Retry_Reruns_The_Whole_Child()
    {
        var calls = new Counter();
        // Every attempt fails. The child retries once (2 calls per child run); the parent retries
        // twice (3 child runs) => 6 calls.
        var child = FlakyChild(calls, failures: int.MaxValue, Transient, ownRetries: 1);
        var graph = GraphAround(child, RecoveryPolicy.Retry(2));
        var store = new TestStore();

        var failed = Assert.IsType<GraphExecutionResult<Order>.NodeFailed>(
            await graph.RunAsync("t", NewOrder(1), store));

        Assert.Equal(3, failed.Attempts);
        Assert.Equal(6, calls.Value);
        Assert.Equal("transient", store.Load("t")!.Metadata!["error_kind"]);
    }

    [Fact]
    public async Task A_Fatal_Child_Failure_Is_Not_Retried_By_The_Parents_Policy()
    {
        var calls = new Counter();
        var child = FlakyChild(calls, failures: int.MaxValue, Fatal, ownRetries: 0);
        var graph = GraphAround(child, RecoveryPolicy.Retry(5));

        var failed = Assert.IsType<GraphExecutionResult<Order>.NodeFailed>(
            await graph.RunAsync("t", NewOrder(1), new TestStore()));

        Assert.Equal(1, failed.Attempts);
        Assert.Equal(1, calls.Value);
    }

    [Fact]
    public async Task A_Transient_Child_Failure_Recovers_When_The_Parent_Retries()
    {
        var calls = new Counter();
        // The child has no policy of its own; the first run fails, the second succeeds.
        var child = FlakyChild(calls, failures: 1, Transient, ownRetries: 0);
        var graph = GraphAround(child, RecoveryPolicy.Retry(1));

        var result = Completed(await graph.RunAsync("t", NewOrder(1), new TestStore()));

        Assert.Equal(2, calls.Value);
        Assert.Equal(["prepared", "flaky", "finished"], result.Log);
    }

    [Fact]
    public async Task A_Fallback_Runs_On_The_State_From_Before_The_Subgraph()
    {
        // The child does real work (Double), then fails: none of it may leak.
        var child = new StateGraph<Job>()
            .AddNode("Double", Step("double", 50))
            .AddFallibleNode("Boom", _ => throw NodeException.Fatal("boom"))
            .AddEdge("Double", "Boom")
            .Compile();
        var graph = GraphAround(child, RecoveryPolicy.Default.WithFallback("Fallback"));
        var store = new TestStore();

        var result = Completed(await graph.RunAsync("t", NewOrder(5), store));

        Assert.Equal(5, result.Amount); // the child's partial work is discarded
        Assert.Equal(["prepared", "fell back"], result.Log);
        Assert.Empty(store.ThreadIds);
    }

    [Fact]
    public async Task A_Child_Routing_Gap_Is_A_Fatal_Failure()
    {
        var child = new StateGraph<Job>()
            .AddNode("Start", Step("start", 0))
            .AddNode("Never", Step("never", 0))
            .AddConditionalEdge("Start", "Never", _ => false)
            .Compile();
        var graph = GraphAround(child, RecoveryPolicy.Retry(3));

        var failed = Assert.IsType<GraphExecutionResult<Order>.NodeFailed>(
            await graph.RunAsync("t", NewOrder(1), new TestStore()));

        Assert.Equal(1, failed.Attempts); // retrying can't fix a routing gap
        Assert.Equal(
            "subgraph 'Work': no outgoing edge condition matched for node 'Start'",
            failed.Message);
    }

    [Fact]
    public async Task Subgraphs_Nest()
    {
        // The middle graph runs the arithmetic child and then adds 1000.
        var middle = new StateGraph<Job>()
            .AddSubgraph("Inner", ArithmeticChild(), job => job, (_, job) => job)
            .AddNode("Bonus", Step("bonus", 1000))
            .AddEdge("Inner", "Bonus")
            .Compile();
        var graph = GraphAround(middle);

        var result = Completed(await graph.RunAsync("t", NewOrder(5), new TestStore()));

        Assert.Equal(1011, result.Amount);
        Assert.Equal(["prepared", "double", "add_one", "bonus", "finished"], result.Log);
    }

    [Fact]
    public async Task A_Failure_Deep_In_A_Nested_Subgraph_Reaches_The_Top_With_Its_Kind()
    {
        var calls = new Counter();
        var inner = FlakyChild(calls, failures: int.MaxValue, Fatal, ownRetries: 0);
        var middle = new StateGraph<Job>()
            .AddSubgraph("Inner", inner, job => job, (_, job) => job)
            .Compile();
        var graph = GraphAround(middle, RecoveryPolicy.Retry(4));

        var failed = Assert.IsType<GraphExecutionResult<Order>.NodeFailed>(
            await graph.RunAsync("t", NewOrder(1), new TestStore()));

        Assert.Equal(1, failed.Attempts); // still fatal two levels up
        Assert.Equal(
            "subgraph 'Work' failed at 'Inner' after 1 attempt(s): " +
            "subgraph 'Inner' failed at 'Flaky' after 1 attempt(s): call 1 failed",
            failed.Message);
        Assert.Equal(1, calls.Value);
    }

    [Fact]
    public async Task One_Child_Graph_Can_Back_Several_Nodes()
    {
        var child = ArithmeticChild();
        var graph = new StateGraph<Order>()
            .AddSubgraph("First", child, ToJob, FromJob)
            .AddSubgraph("Second", child, ToJob, FromJob)
            .AddEdge("First", "Second")
            .Compile();

        var result = Completed(await graph.RunAsync("t", NewOrder(1), new TestStore()));

        Assert.Equal(7, result.Amount); // 1 -> 3 -> 7
    }

    [Fact]
    public async Task A_Subgraph_Can_Be_The_Parents_Interrupt_Boundary()
    {
        var graph = new StateGraph<Order>()
            .AddNode("Prepare", Note("prepared"))
            .AddSubgraph("Work", ArithmeticChild(), ToJob, FromJob)
            .AddNode("Finish", Note("finished"))
            .AddEdge("Prepare", "Work")
            .AddEdge("Work", "Finish")
            .AddInterruptBoundary("Work")
            .Compile();
        var store = new TestStore();

        var interrupted = Assert.IsType<GraphExecutionResult<Order>.Interrupted>(
            await graph.RunAsync("t", NewOrder(5), store));
        Assert.Equal("Work", interrupted.SuspendedNode);
        Assert.Equal("Finish", interrupted.NextNode);
        Assert.Equal(11, interrupted.FrozenState.Amount);

        var result = Completed(await graph.ResumeAsync("t", new HumanSignal(IsApproved: true, ReviewerToken: "x", FieldOverrides: null), store));
        Assert.Equal(11, result.Amount);
        Assert.Equal(["prepared", "double", "add_one", "finished"], result.Log);
    }

    [Fact]
    public async Task A_Supervisor_Routes_To_One_Of_Several_Subgraphs()
    {
        var small = new StateGraph<Job>().AddNode("Small", Step("small", 1)).Compile();
        var large = new StateGraph<Job>().AddNode("Large", Step("large", 1000)).Compile();
        var graph = new StateGraph<Order>()
            .AddNode("Supervisor", Note("routed"))
            .AddSubgraph("SmallAgent", small, ToJob, FromJob)
            .AddSubgraph("LargeAgent", large, ToJob, FromJob)
            .AddConditionalEdge("Supervisor", "LargeAgent", o => o.Amount >= 100)
            .AddEdge("Supervisor", "SmallAgent")
            .Compile();

        var smallResult = Completed(await graph.RunAsync("a", NewOrder(5), new TestStore()));
        var largeResult = Completed(await graph.RunAsync("b", NewOrder(500), new TestStore()));

        Assert.Equal(["routed", "small"], smallResult.Log);
        Assert.Equal(["routed", "large"], largeResult.Log);
        Assert.Equal(1500, largeResult.Amount);
    }

    [Fact]
    public async Task A_Boundary_Naming_A_Node_The_Child_Does_Not_Have_Never_Triggers_So_It_Is_Accepted()
    {
        var child = new StateGraph<Job>()
            .AddNode("Only", Step("only", 1))
            .AddInterruptBoundary("Ghost")
            .Compile();

        var graph = GraphAround(child);

        Assert.Equal(2, Completed(await graph.RunAsync("t", NewOrder(1), new TestStore())).Amount);
    }

    [Fact]
    public void A_Recovery_Policy_Can_Be_Set_On_A_Subgraph_Node()
    {
        // A subgraph counts as fallible, so Compile accepts a policy on it.
        Assert.NotNull(GraphAround(ArithmeticChild(), RecoveryPolicy.Retry(1)));
    }

    [Fact]
    public async Task RunStreaming_Reports_A_Plain_Childs_Nodes_Named_Under_The_Subgraph()
    {
        var graph = GraphAround(ArithmeticChild());

        var (reader, completion) = graph.RunStreaming("t", NewOrder(5), new TestStore());
        var chunks = new List<(StreamChunkType Type, string Node)>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            chunks.Add((chunk.Type, chunk.NodeName));
        }

        Assert.Equal(11, Completed(await completion).Amount);
        // The child's nodes are visible, qualified by the subgraph node, and sit between its own
        // NodeStart and NodeEnd. design/15 has the rules.
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Work"),
                (StreamChunkType.NodeStart, "Work/Double"),
                (StreamChunkType.NodeEnd, "Work/Double"),
                (StreamChunkType.NodeStart, "Work/AddOne"),
                (StreamChunkType.NodeEnd, "Work/AddOne"),
                (StreamChunkType.NodeEnd, "Work"),
            ],
            chunks.Where(c => c.Node == "Work" || c.Node.StartsWith("Work/", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task A_Bug_In_A_Child_Propagates_Like_A_Plain_Node_And_Is_Not_Retried()
    {
        var calls = new Counter();
        var child = new StateGraph<Job>()
            .AddNode("Bug", _ =>
            {
                calls.Increment();
                throw new InvalidOperationException("child bug");
            })
            .Compile();
        var graph = GraphAround(child, RecoveryPolicy.Retry(3));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => graph.RunAsync("t", NewOrder(1), new TestStore()));

        Assert.Equal("child bug", ex.Message);
        Assert.Equal(1, calls.Value);
    }
}
