using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/parallel_groups.rs</c> (<c>design/14-Parallel-Execution.md</c>):
/// branches run concurrently, results reach the reducer in declaration order, a failing branch fails
/// the group as a whole (retry / fallback / <c>Failed</c> checkpoint), and a group is one ordinary
/// node to the rest of the graph. Keep the two in step.
/// </summary>
public class ParallelGroupTests
{
    public sealed record Report(
        [property: JsonPropertyName("seed")] int Seed,
        [property: JsonPropertyName("results")] IReadOnlyList<string> Results,
        [property: JsonPropertyName("notes")] IReadOnlyList<string> Notes);

    private static Report NewReport() => new(Seed: 7, Results: [], Notes: []);

    /// <summary>A branch that records <paramref name="label"/> into <c>Results</c> after <paramref name="delayMs"/>.</summary>
    private static Func<Report, Report> Labelled(string label, int delayMs) =>
        state =>
        {
            Thread.Sleep(delayMs);
            return state with { Results = [.. state.Results, label] };
        };

    /// <summary>Concatenates every branch's <c>Results</c>, in the order the reducer is given them, onto the entry state.</summary>
    private static Report Concat(Report entry, IReadOnlyList<Report> outputs) =>
        entry with { Results = [.. entry.Results, .. outputs.SelectMany(o => o.Results)] };

    private static Func<Report, Report> AddNote(string note) =>
        state => state with { Notes = [.. state.Notes, note] };

    /// <summary><c>Prepare</c> -> <c>Gather</c> (the parallel group) -> <c>Finish</c>.</summary>
    private static CompiledGraph<Report> GraphAround(ParallelBranches<Report> branches, RecoveryPolicy? policy = null)
    {
        var graph = new StateGraph<Report>()
            .AddNode("Prepare", AddNote("prepared"))
            .AddParallelGroup("Gather", branches, Concat)
            .AddNode("Finish", AddNote("finished"))
            .AddEdge("Prepare", "Gather")
            .AddEdge("Gather", "Finish");
        if (policy is not null)
        {
            // A fallback node has no ordinary edge into it, so it's only added when a policy
            // actually routes there (otherwise it'd be an orphan).
            if (policy.Fallback is not null)
            {
                graph.AddNode("Fallback", AddNote("fell back"));
            }

            graph.SetRecoveryPolicy("Gather", policy);
        }

        return graph.Compile();
    }

    private static Report Completed(GraphExecutionResult<Report> result) =>
        Assert.IsType<GraphExecutionResult<Report>.Completed>(result).State;

    [Fact]
    public async Task Branches_Run_Concurrently_Not_One_After_Another()
    {
        var inFlight = 0;
        var peak = 0;
        var branches = new ParallelBranches<Report>();
        foreach (var label in new[] { "a", "b", "c" })
        {
            branches.Branch(label, state =>
            {
                var now = Interlocked.Increment(ref inFlight);
                InterlockedMax(ref peak, now);
                Thread.Sleep(200);
                Interlocked.Decrement(ref inFlight);
                return state;
            });
        }

        Completed(await GraphAround(branches).RunAsync("t", NewReport(), new InMemoryCheckpointStore()));

        // Run one after another the peak would be 1.
        Assert.Equal(3, peak);
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int seen;
        do
        {
            seen = Volatile.Read(ref target);
        }
        while (value > seen && Interlocked.CompareExchange(ref target, value, seen) != seen);
    }

    [Fact]
    public async Task Results_Reach_The_Reducer_In_Declaration_Order_Not_Completion_Order()
    {
        var finished = new List<string>();
        var branches = new ParallelBranches<Report>();
        // Declared slowest-first, so they complete in the reverse order.
        foreach (var (label, delayMs) in new[] { ("slow", 240), ("medium", 120), ("fast", 10) })
        {
            var inner = Labelled(label, delayMs);
            branches.Branch(label, state =>
            {
                var output = inner(state);
                lock (finished)
                {
                    finished.Add(label);
                }

                return output;
            });
        }

        var state = Completed(await GraphAround(branches).RunAsync("t", NewReport(), new InMemoryCheckpointStore()));

        Assert.Equal(["fast", "medium", "slow"], finished);
        Assert.Equal(["slow", "medium", "fast"], state.Results);
    }

    [Fact]
    public async Task Every_Branch_Starts_From_The_Entry_State_And_The_Reducer_Sees_It_As_The_Base()
    {
        Report AssertPreparedThenAdd(Report state, string label)
        {
            // Branches can't see each other's work: both start from `Prepare`'s output.
            Assert.Equal(["prepared"], state.Notes);
            return state with { Results = [.. state.Results, label] };
        }

        var branches = new ParallelBranches<Report>()
            .Branch("a", s => AssertPreparedThenAdd(s, "a"))
            .Branch("b", s => AssertPreparedThenAdd(s, "b"));
        var graph = new StateGraph<Report>()
            .AddNode("Prepare", AddNote("prepared"))
            .AddParallelGroup("Gather", branches, (entry, outputs) => entry with
            {
                Results = [.. outputs.SelectMany(o => o.Results)],
                Notes = [.. entry.Notes, $"base seed {entry.Seed}"],
            })
            .AddEdge("Prepare", "Gather")
            .Compile();

        var state = Completed(await graph.RunAsync("t", NewReport(), new InMemoryCheckpointStore()));

        Assert.Equal(["a", "b"], state.Results);
        Assert.Equal(["prepared", "base seed 7"], state.Notes);
    }

    [Fact]
    public async Task The_Merged_State_Flows_On_Through_Ordinary_Edges()
    {
        var branches = new ParallelBranches<Report>().Branch("a", Labelled("a", 0)).Branch("b", Labelled("b", 0));

        var state = Completed(await GraphAround(branches).RunAsync("t", NewReport(), new InMemoryCheckpointStore()));

        Assert.Equal(["a", "b"], state.Results);
        Assert.Equal(["prepared", "finished"], state.Notes);
    }

    [Fact]
    public async Task A_Failing_Branch_Cancels_Its_Siblings_And_Fails_The_Group_At_Once()
    {
        var asyncSiblingCancelled = new TaskCompletionSource();
        using var syncSiblingGate = new ManualResetEventSlim();
        var syncSiblingDone = 0;
        var branches = new ParallelBranches<Report>()
            .Branch("broken", _ => throw NodeException.Fatal("card declined"))
            .Branch("waiting", async (state, _, ct) =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                    asyncSiblingCancelled.SetResult();
                    throw;
                }

                return state;
            })
            .Branch("blocking", state =>
            {
                syncSiblingGate.Wait(TimeSpan.FromSeconds(10));
                Interlocked.Increment(ref syncSiblingDone);
                return state;
            });
        var store = new InMemoryCheckpointStore();

        var result = await GraphAround(branches).RunAsync("thread_failed", NewReport(), store);

        var failed = Assert.IsType<GraphExecutionResult<Report>.NodeFailed>(result);
        Assert.Equal("Gather", failed.Node);
        Assert.Equal(1, failed.Attempts);
        // The cancelled siblings are not failures.
        Assert.Equal("branch 'broken': card declined", failed.Message);

        // The async sibling was told; the sync one can't be, and the group didn't wait for it.
        await asyncSiblingCancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, Volatile.Read(ref syncSiblingDone));
        syncSiblingGate.Set();

        var persisted = store.Load("thread_failed");
        Assert.NotNull(persisted);
        Assert.Equal(CheckpointStatus.Failed, persisted.Status);
        Assert.Equal("Gather", persisted.SuspendedNode);
        // The last committed state — after `Prepare`, before any branch's work.
        Assert.True(JsonNode.DeepEquals(
            JsonNode.Parse("""{ "seed": 7, "results": [], "notes": ["prepared"] }"""), persisted.FrozenState));
        Assert.Equal("fatal", persisted.Metadata!["error_kind"]);
    }

    [Fact]
    public async Task A_Fatal_Branch_Makes_The_Group_Fatal_And_A_Sibling_It_Cancelled_Is_Not_Named()
    {
        var branches = new ParallelBranches<Report>()
            .Branch("flaky", async (_, _, ct) =>
            {
                // Cancelled by `broken`'s failure long before it gets here.
                await Task.Delay(TimeSpan.FromSeconds(10), ct);
                throw NodeException.Transient("timeout");
            })
            .Branch("broken", _ => throw NodeException.Fatal("bad input"))
            .Branch("fine", Labelled("fine", 0));
        // Would retry five times if the group were treated as transient.
        var graph = GraphAround(branches, RecoveryPolicy.Retry(5));

        var result = await graph.RunAsync("t", NewReport(), new InMemoryCheckpointStore());

        var failed = Assert.IsType<GraphExecutionResult<Report>.NodeFailed>(result);
        Assert.Equal(1, failed.Attempts);
        Assert.Equal("branch 'broken': bad input", failed.Message);
    }

    [Fact]
    public async Task A_Transient_Failure_Retries_The_Whole_Group()
    {
        var flakyCalls = 0;
        var steadyCalls = 0;
        var branches = new ParallelBranches<Report>()
            .Branch("flaky", state => Interlocked.Increment(ref flakyCalls) <= 2
                ? throw NodeException.Transient("timeout")
                : state with { Results = [.. state.Results, "flaky"] })
            .Branch("steady", state =>
            {
                Interlocked.Increment(ref steadyCalls);
                return state with { Results = [.. state.Results, "steady"] };
            });
        var graph = GraphAround(branches, RecoveryPolicy.Retry(3));

        var state = Completed(await graph.RunAsync("t", NewReport(), new InMemoryCheckpointStore()));

        Assert.Equal(["flaky", "steady"], state.Results);
        Assert.Equal(3, flakyCalls);
        // The healthy branch is re-run on every group attempt too.
        Assert.Equal(3, steadyCalls);
    }

    [Fact]
    public async Task An_Unrecoverable_Group_Routes_To_The_Fallback_On_The_Pre_Group_State()
    {
        var branches = new ParallelBranches<Report>()
            .Branch("ok", Labelled("ok", 0))
            .Branch("down", _ => throw NodeException.Transient("outage"));
        var graph = GraphAround(branches, RecoveryPolicy.Retry(1).WithFallback("Fallback"));

        var state = Completed(await graph.RunAsync("t", NewReport(), new InMemoryCheckpointStore()));

        // `ok`'s result was discarded with the failed group; `Fallback` saw `Prepare`'s state.
        Assert.Empty(state.Results);
        Assert.Equal(["prepared", "fell back"], state.Notes);
    }

    [Fact]
    public async Task A_Group_Can_Be_An_Interrupt_Boundary_And_Freezes_The_Merged_State()
    {
        var branches = new ParallelBranches<Report>().Branch("a", Labelled("a", 0)).Branch("b", Labelled("b", 0));
        var graph = new StateGraph<Report>()
            .AddParallelGroup("Gather", branches, Concat)
            .AddInterruptBoundary("Gather")
            .AddNode("Finish", AddNote("finished"))
            .AddEdge("Gather", "Finish")
            .Compile();
        var store = new InMemoryCheckpointStore();

        var result = await graph.RunAsync("t", NewReport(), store);

        var interrupted = Assert.IsType<GraphExecutionResult<Report>.Interrupted>(result);
        Assert.Equal("Gather", interrupted.SuspendedNode);
        Assert.Equal("Finish", interrupted.NextNode);
        Assert.Equal(["a", "b"], interrupted.FrozenState.Results);

        var resumed = Completed(await graph.ResumeAsync(
            "t", new HumanSignal(IsApproved: true, ReviewerToken: "x", FieldOverrides: null), store));
        Assert.Equal(["a", "b"], resumed.Results);
        Assert.Equal(["finished"], resumed.Notes);
    }

    [Fact]
    public async Task RunStreaming_Reports_The_Group_As_One_Node()
    {
        var branches = new ParallelBranches<Report>().Branch("a", Labelled("a", 0)).Branch("b", Labelled("b", 0));
        var graph = GraphAround(branches);

        var (reader, completion) = graph.RunStreaming("t", NewReport(), new InMemoryCheckpointStore());
        var chunks = new List<(StreamChunkType Type, string Node)>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            chunks.Add((chunk.Type, chunk.NodeName));
        }

        Assert.Equal(["a", "b"], Completed(await completion).Results);
        Assert.Equal(
            [(StreamChunkType.NodeStart, "Gather"), (StreamChunkType.NodeEnd, "Gather")],
            chunks.Where(c => c.Node == "Gather"));
    }

    [Fact]
    public async Task A_Branch_Bug_Propagates_Like_A_Plain_Node_And_Is_Not_Retried()
    {
        var calls = 0;
        var branches = new ParallelBranches<Report>()
            .Branch("ok", Labelled("ok", 0))
            .Branch("bug", _ =>
            {
                Interlocked.Increment(ref calls);
                throw new InvalidOperationException("branch bug");
            });
        var graph = GraphAround(branches, RecoveryPolicy.Retry(3));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => graph.RunAsync("t", NewReport(), new InMemoryCheckpointStore()));

        Assert.Equal("branch bug", ex.Message);
        Assert.Equal(1, calls);
    }

    [Fact]
    public void Compile_Rejects_A_Group_With_No_Branches()
    {
        var graph = new StateGraph<Report>().AddParallelGroup("Gather", new ParallelBranches<Report>(), Concat);

        var ex = Assert.Throws<InvalidOperationException>(graph.Compile);
        Assert.Contains("'Gather'", ex.Message);
        Assert.Contains("no branches", ex.Message);
    }

    [Fact]
    public void Compile_Rejects_Duplicate_Branch_Names()
    {
        var branches = new ParallelBranches<Report>().Branch("a", Labelled("a", 0)).Branch("a", Labelled("a", 0));
        var graph = new StateGraph<Report>().AddParallelGroup("Gather", branches, Concat);

        var ex = Assert.Throws<InvalidOperationException>(graph.Compile);
        Assert.Contains("'a'", ex.Message);
    }

    [Fact]
    public void Re_Adding_A_Name_Replaces_A_Group_And_Its_Compile_Checks()
    {
        // An empty group would be rejected — but it was replaced by a plain node.
        var graph = new StateGraph<Report>()
            .AddParallelGroup("Gather", new ParallelBranches<Report>(), Concat)
            .AddNode("Gather", s => s);

        graph.Compile();
    }

    [Fact]
    public async Task Adding_A_Branch_After_The_Group_Is_Declared_Does_Not_Change_The_Graph()
    {
        var branches = new ParallelBranches<Report>().Branch("a", Labelled("a", 0));
        var graph = GraphAround(branches);
        branches.Branch("late", Labelled("late", 0));

        var state = Completed(await graph.RunAsync("t", NewReport(), new InMemoryCheckpointStore()));

        Assert.Equal(["a"], state.Results);
    }
}
