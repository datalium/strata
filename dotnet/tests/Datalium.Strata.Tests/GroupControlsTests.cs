using System.Collections.Concurrent;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/group_controls.rs</c> (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>,
/// SG-07): a parallel group's concurrency limit, per-branch timeouts, partial results and sibling
/// cancellation. Keep the two in step.
/// </summary>
public class GroupControlsTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Job(int N, IReadOnlyList<string> Trail);

    private static Job NewJob() => new(0, []);

    private static Job Done(Job job, string branch) => job with { Trail = [.. job.Trail, branch] };

    private static Func<Job, NodeContext, CancellationToken, ValueTask<Job>> Succeeds(string name) =>
        (job, _, _) => ValueTask.FromResult(Done(job, name));

    private static Func<Job, NodeContext, CancellationToken, ValueTask<Job>> Fails(NodeException error) =>
        (_, _, _) => ValueTask.FromException<Job>(error);

    /// <summary>Waits <paramref name="delay"/> on <paramref name="clock"/>, then succeeds.</summary>
    private static Func<Job, NodeContext, CancellationToken, ValueTask<Job>> Slow(
        string name, TimeSpan delay, TimeProvider clock) =>
        async (job, _, ct) =>
        {
            await Task.Delay(delay, clock, ct);
            return Done(job, name);
        };

    private static async Task<List<RunEvent>> Drain(RunEventChannel channel)
    {
        var events = new List<RunEvent>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            events.Add(e);
        }

        return events;
    }

    /// <summary>Runs <paramref name="graph"/>, advancing <paramref name="clock"/> a second at a time until it ends.</summary>
    private static async Task<(GraphExecutionResult<Job> Result, List<RunEvent> Events)> Run(
        CompiledGraph<Job> graph, FakeTimeProvider? clock = null, ICheckpointStore? store = null)
    {
        var channel = new RunEventChannel();
        var task = graph.RunAsync("t", NewJob(), store ?? new InMemoryCheckpointStore(),
            new RunOptions { Observer = channel, TimeProvider = clock });
        for (var i = 0; clock is not null && i < 100_000 && !task.IsCompleted; i++)
        {
            clock.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(1);
        }

        var result = await task.WaitAsync(Patience);
        return (result, await Drain(channel));
    }

    private static List<(string Path, BranchResult Result, BranchEndReason? Reason)> Branches(IEnumerable<RunEvent> events) =>
        [.. events.OfType<BranchCompleted>().Select(e => (e.NodePath!, e.Result, e.Reason)).OrderBy(e => e.Item1, StringComparer.Ordinal)];

    /// <summary>A group of the named branches whose reducer keeps the outcomes it was handed.</summary>
    private static CompiledGraph<Job> Group(
        ParallelBranches<Job> branches, ParallelGroupOptions<Job> options, ConcurrentQueue<BranchOutcomes<Job>> seen) =>
        new StateGraph<Job>()
            .AddParallelGroupWithOutcomes(
                "Investigate",
                branches,
                (entry, results) =>
                {
                    seen.Enqueue(results);
                    return entry with { Trail = [.. results.Succeeded.SelectMany(s => s.Trail)] };
                },
                options)
            .Compile();

    [Fact]
    public async Task The_Concurrency_Limit_Holds_And_Branches_Start_In_Declaration_Order()
    {
        var inFlight = 0;
        var peak = 0;
        var started = new ConcurrentQueue<string>();
        Func<Job, NodeContext, CancellationToken, ValueTask<Job>> Tracked(string name) => async (job, _, ct) =>
        {
            started.Enqueue(name);
            var now = Interlocked.Increment(ref inFlight);
            InterlockedMax(ref peak, now);
            await Task.Delay(20, ct);
            Interlocked.Decrement(ref inFlight);
            return Done(job, name);
        };
        var branches = new ParallelBranches<Job>();
        foreach (var name in new[] { "a", "b", "c", "d", "e" })
        {
            branches.Branch(name, Tracked(name));
        }

        var graph = new StateGraph<Job>()
            .AddParallelGroup("Investigate", branches, (entry, results) => entry with { Trail = [.. results.SelectMany(r => r.Trail)] },
                new ParallelGroupOptions<Job> { MaxConcurrency = 2 })
            .Compile();

        var (result, _) = await Run(graph);

        var state = Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State;
        Assert.Equal(["a", "b", "c", "d", "e"], state.Trail);
        Assert.Equal(2, peak);
        Assert.Equal(["a", "b", "c", "d", "e"], started);

        static void InterlockedMax(ref int target, int value)
        {
            int current;
            while ((current = Volatile.Read(ref target)) < value
                   && Interlocked.CompareExchange(ref target, value, current) != current)
            {
            }
        }
    }

    [Fact]
    public async Task A_Branch_Timeout_Times_Out_Only_That_Branch()
    {
        var clock = new FakeTimeProvider();
        var seen = new ConcurrentQueue<BranchOutcomes<Job>>();
        var graph = Group(
            new ParallelBranches<Job>()
                .Branch("telemetry", Succeeds("telemetry"))
                .Branch("kubernetes", Slow("kubernetes", TimeSpan.FromHours(1), clock))
                .Branch("database", Slow("database", TimeSpan.FromSeconds(10), clock)),
            new ParallelGroupOptions<Job> { BranchTimeout = TimeSpan.FromSeconds(45), Partial = PartialResults.KeepSucceeded },
            seen);

        var (result, events) = await Run(graph, clock);

        Assert.Equal(["telemetry", "database"], Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State.Trail);
        var kubernetes = Assert.Single(seen)["kubernetes"]!;
        Assert.Equal(BranchStatus.TimedOut, kubernetes.Status);
        Assert.Equal(NodeFailureKind.Transient, kubernetes.Error!.Kind);
        Assert.Equal("branch 'kubernetes' timed out after 45000 ms", kubernetes.Error.Message);
        Assert.Equal(
            [
                ("Investigate/database", BranchResult.Succeeded, null),
                ("Investigate/kubernetes", BranchResult.Failed, BranchEndReason.TimedOut),
                ("Investigate/telemetry", BranchResult.Succeeded, null),
            ],
            Branches(events));
    }

    [Fact]
    public async Task A_Queued_Branch_Is_Not_Charged_For_Its_Wait()
    {
        var clock = new FakeTimeProvider();
        var seen = new ConcurrentQueue<BranchOutcomes<Job>>();
        // One at a time, 30 s each, 45 s each: the second would time out if its clock started with the first's.
        var graph = Group(
            new ParallelBranches<Job>()
                .Branch("first", Slow("first", TimeSpan.FromSeconds(30), clock))
                .Branch("second", Slow("second", TimeSpan.FromSeconds(30), clock)),
            new ParallelGroupOptions<Job> { BranchTimeout = TimeSpan.FromSeconds(45), MaxConcurrency = 1 },
            seen);

        var (result, _) = await Run(graph, clock);

        Assert.Equal(["first", "second"], Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State.Trail);
    }

    [Fact]
    public async Task Keep_Succeeded_Hands_Every_Outcome_To_The_Reducer_In_Declaration_Order()
    {
        var seen = new ConcurrentQueue<BranchOutcomes<Job>>();
        var graph = Group(
            new ParallelBranches<Job>()
                .Branch("a", Succeeds("a"))
                .Branch("b", Fails(NodeException.Fatal("bad query")))
                .Branch("c", Fails(NodeException.Transient("api down")))
                .Branch("d", Succeeds("d")),
            new ParallelGroupOptions<Job> { Partial = PartialResults.KeepSucceeded },
            seen);

        var (result, events) = await Run(graph);

        Assert.Equal(["a", "d"], Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State.Trail);
        var outcomes = Assert.Single(seen);
        Assert.False(outcomes.JoinMet);
        Assert.Empty(outcomes.Deciding);
        Assert.Equal(["a", "b", "c", "d"], outcomes.Select(o => o.Name));
        Assert.Equal(
            [BranchStatus.Succeeded, BranchStatus.Failed, BranchStatus.Failed, BranchStatus.Succeeded],
            outcomes.Select(o => o.Status));
        Assert.Equal(["a"], outcomes["a"]!.State!.Trail);
        Assert.Null(outcomes["b"]!.State);
        Assert.Equal("bad query", outcomes["b"]!.Error!.Message);
        Assert.Equal(NodeFailureKind.Transient, outcomes["c"]!.Error!.Kind);
        // No branch was cancelled for its siblings' failures.
        Assert.DoesNotContain(events.OfType<BranchCompleted>(), e => e.Result == BranchResult.Cancelled);
    }

    [Fact]
    public async Task The_Plain_Reducer_Gets_The_Succeeded_States_Under_Keep_Succeeded()
    {
        IReadOnlyList<Job>? handed = null;
        var graph = new StateGraph<Job>()
            .AddParallelGroup(
                "Investigate",
                new ParallelBranches<Job>().Branch("a", Succeeds("a")).Branch("b", Fails(NodeException.Fatal("x"))).Branch("c", Succeeds("c")),
                (entry, results) =>
                {
                    handed = results;
                    return entry;
                },
                new ParallelGroupOptions<Job> { Partial = PartialResults.KeepSucceeded })
            .Compile();

        await Run(graph);

        Assert.Equal([["a"], ["c"]], handed!.Select(j => j.Trail));
    }

    [Fact]
    public async Task When_Every_Branch_Fails_The_Group_Fails_With_The_Longest_Hint()
    {
        var store = new InMemoryCheckpointStore();
        var graph = Group(
            new ParallelBranches<Job>()
                .Branch("a", Fails(NodeException.Transient("429", TimeSpan.FromSeconds(2))))
                .Branch("b", Fails(NodeException.Transient("429", TimeSpan.FromSeconds(5)))),
            new ParallelGroupOptions<Job> { Partial = PartialResults.KeepSucceeded },
            new ConcurrentQueue<BranchOutcomes<Job>>());

        var (result, _) = await Run(graph, store: store);

        var failed = Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result);
        Assert.Equal("branch 'a': 429; branch 'b': 429", failed.Message);
        var metadata = store.Load("t")!.Metadata!;
        Assert.Equal("transient", metadata["error_kind"]);
        Assert.Equal("5000", metadata["strata.retry_after_ms"]);
    }

    [Fact]
    public async Task Under_All_A_Fatal_Failure_Makes_The_Group_Fatal()
    {
        var store = new InMemoryCheckpointStore();
        var graph = Group(
            new ParallelBranches<Job>()
                .Branch("a", Fails(NodeException.Transient("api down")))
                .Branch("b", Fails(NodeException.Fatal("bad query"))),
            new ParallelGroupOptions<Job> { Partial = PartialResults.KeepSucceeded },
            new ConcurrentQueue<BranchOutcomes<Job>>());

        var (result, _) = await Run(graph, store: store);

        Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result);
        Assert.Equal("fatal", store.Load("t")!.Metadata!["error_kind"]);
    }

    /// <summary><c>a</c> fails at once; <c>b</c> takes 50 ms and records whether it saw its token fire.</summary>
    private static (CompiledGraph<Job> Graph, Func<bool> BSawCancellation) FailFast(bool cancelSiblings)
    {
        var sawCancellation = false;
        var graph = new StateGraph<Job>()
            .AddParallelGroup(
                "Investigate",
                new ParallelBranches<Job>()
                    .Branch("a", Fails(NodeException.Fatal("bad query")))
                    .Branch("b", async (job, _, ct) =>
                    {
                        try
                        {
                            await Task.Delay(50, ct);
                        }
                        catch (OperationCanceledException)
                        {
                            sawCancellation = true;
                            throw;
                        }

                        return Done(job, "b");
                    }),
                (entry, _) => entry,
                new ParallelGroupOptions<Job> { CancelSiblingsOnFailure = cancelSiblings })
            .Compile();
        return (graph, () => sawCancellation);
    }

    [Fact]
    public async Task By_Default_A_Failure_Cancels_The_Siblings()
    {
        var (graph, sawCancellation) = FailFast(cancelSiblings: true);

        var (result, events) = await Run(graph);

        Assert.Equal("branch 'a': bad query", Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result).Message);
        Assert.Equal(
            [
                ("Investigate/a", BranchResult.Failed, null),
                ("Investigate/b", BranchResult.Cancelled, BranchEndReason.SiblingFailed),
            ],
            Branches(events));
        await Task.Delay(100);
        Assert.True(sawCancellation());
    }

    [Fact]
    public async Task Without_Sibling_Cancellation_Every_Branch_Finishes_Before_The_Group_Fails()
    {
        var (graph, sawCancellation) = FailFast(cancelSiblings: false);

        var (result, events) = await Run(graph);

        Assert.Equal("branch 'a': bad query", Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result).Message);
        Assert.Equal(
            [
                ("Investigate/a", BranchResult.Failed, null),
                ("Investigate/b", BranchResult.Succeeded, null),
            ],
            Branches(events));
        Assert.False(sawCancellation());
    }

    [Fact]
    public void Bad_Options_Are_Refused()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ParallelGroupOptions<Job> { MaxConcurrency = 0 });
        Assert.Throws<ArgumentOutOfRangeException>(() => new ParallelGroupOptions<Job> { BranchTimeout = TimeSpan.Zero });
    }
}
