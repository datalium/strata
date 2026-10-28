using System.Collections.Concurrent;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/retry_backoff.rs</c> (<c>design/25-Retry-Backoff.md</c>, SG-04):
/// backoff between retries (growth, cap, jitter, the retry hint), the wait under cancellation and a
/// deadline, <see cref="NodeContext.LastFailure"/> in a fallback, and groups and subgraphs backing
/// off. Every wait runs on a <see cref="FakeTimeProvider"/>, which the tests advance; Rust uses
/// tokio's paused clock.
/// </summary>
public class RetryBackoffTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Job(int N, IReadOnlyList<string> Trail);

    private static Job NewJob() => new(0, []);

    private static TimeSpan Seconds(double s) => TimeSpan.FromSeconds(s);

    /// <summary>A fallible node that throws the next of <paramref name="failures"/> per call, then succeeds.</summary>
    private sealed class Flaky(params NodeException[] failures)
    {
        private int _calls;

        public int Calls => _calls;

        /// <summary>When each attempt started, on the run's clock.</summary>
        public ConcurrentQueue<DateTimeOffset> Started { get; } = new();

        public Func<Job, NodeContext, CancellationToken, ValueTask<Job>> Handler(TimeProvider clock) =>
            (job, _, _) =>
            {
                Started.Enqueue(clock.GetUtcNow());
                var call = Interlocked.Increment(ref _calls);
                return call <= failures.Length
                    ? ValueTask.FromException<Job>(failures[call - 1])
                    : ValueTask.FromResult(job with { Trail = [.. job.Trail, "charged"] });
            };
    }

    private static NodeException Transient(TimeSpan? hint = null) => NodeException.Transient("rate limited", hint);

    private static NodeException[] Times(int n, Func<NodeException> failure) => [.. Enumerable.Range(0, n).Select(_ => failure())];

    private static CompiledGraph<Job> OneNode(Flaky flaky, TimeProvider clock, RecoveryPolicy policy) =>
        new StateGraph<Job>()
            .AddFallibleNode("Charge", flaky.Handler(clock))
            .SetRecoveryPolicy("Charge", policy)
            .Compile();

    /// <summary>
    /// Runs <paramref name="run"/>, advancing <paramref name="clock"/> by <paramref name="step"/> until it
    /// ends, and returns its result with the delays of its <see cref="RetryScheduled"/> events.
    /// </summary>
    private static async Task<(GraphExecutionResult<Job> Result, List<TimeSpan> Delays)> Drive(
        FakeTimeProvider clock,
        Func<RunOptions, Task<GraphExecutionResult<Job>>> run,
        TimeSpan? step = null,
        RunOptions? options = null)
    {
        var channel = new RunEventChannel();
        var task = run(new RunOptions { Observer = channel, TimeProvider = clock, Deadline = options?.Deadline });
        for (var i = 0; i < 100_000 && !task.IsCompleted; i++)
        {
            clock.Advance(step ?? TimeSpan.FromSeconds(1));
            await Task.Delay(1);
        }

        var result = await task.WaitAsync(Patience);
        var delays = new List<TimeSpan>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            if (e is RetryScheduled retry)
            {
                delays.Add(retry.Delay);
            }
        }

        return (result, delays);
    }

    private static Job Completed(GraphExecutionResult<Job> result) =>
        Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State;

    [Fact]
    public async Task Without_Backoff_Retries_Start_At_Once_Hint_Or_Not()
    {
        var clock = new FakeTimeProvider();
        var flaky = new Flaky(Transient(Seconds(5)), Transient());
        var graph = OneNode(flaky, clock, RecoveryPolicy.Retry(2));

        // The clock never moves, so any wait would hang the run.
        var channel = new RunEventChannel();
        var result = await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(),
            new RunOptions { TimeProvider = clock, Observer = channel }).WaitAsync(Patience);

        Completed(result);
        Assert.Equal(3, flaky.Calls);
        var delays = new List<TimeSpan>();
        await foreach (var e in channel.Reader.ReadAllAsync())
        {
            if (e is RetryScheduled retry)
            {
                delays.Add(retry.Delay);
            }
        }

        Assert.Equal([TimeSpan.Zero, TimeSpan.Zero], delays);
    }

    [Fact]
    public async Task Delays_Double_From_Initial_And_Stop_At_Max()
    {
        var clock = new FakeTimeProvider();
        var flaky = new Flaky(Times(5, () => Transient()));
        var graph = OneNode(flaky, clock, RecoveryPolicy.Retry(5).WithBackoff(Seconds(1), Seconds(5), BackoffJitter.None));

        var (result, delays) = await Drive(clock, o => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));

        Completed(result);
        Assert.Equal([Seconds(1), Seconds(2), Seconds(4), Seconds(5), Seconds(5)], delays);

        // Each attempt started no sooner than the delay before it.
        var started = flaky.Started.ToArray();
        for (var i = 1; i < started.Length; i++)
        {
            Assert.True(started[i] - started[i - 1] >= delays[i - 1], $"attempt {i + 1} started early");
        }
    }

    [Theory]
    [InlineData(BackoffJitter.Full, 0.0)]
    [InlineData(BackoffJitter.Equal, 0.5)]
    public async Task Jitter_Stays_Within_Its_Bounds(BackoffJitter jitter, double floor)
    {
        var ceilings = new[] { Seconds(1), Seconds(2), Seconds(4), Seconds(8), Seconds(8) };
        var all = new List<double>();
        for (var run = 0; run < 5; run++)
        {
            var clock = new FakeTimeProvider();
            var flaky = new Flaky(Times(5, () => Transient()));
            var graph = OneNode(flaky, clock, RecoveryPolicy.Retry(5).WithBackoff(Seconds(1), Seconds(8), jitter));

            var (result, delays) = await Drive(clock, o => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));

            Completed(result);
            Assert.Equal(5, delays.Count);
            for (var i = 0; i < delays.Count; i++)
            {
                Assert.InRange(delays[i], ceilings[i] * floor, ceilings[i]);
                all.Add(delays[i] / ceilings[i]);
            }
        }

        // Jittered: not every delay sits at its ceiling.
        Assert.Contains(all, ratio => ratio < 0.99);
    }

    [Fact]
    public async Task The_Hint_Replaces_The_Delay_Uncapped_Up_To_Max_And_Unjittered()
    {
        var clock = new FakeTimeProvider();
        // A 3 s hint, a 60 s hint, then none: the third retry's computed ceiling is 4 s.
        var flaky = new Flaky(Transient(Seconds(3)), Transient(Seconds(60)), Transient());
        var graph = OneNode(flaky, clock, RecoveryPolicy.Retry(3).WithBackoff(Seconds(1), Seconds(10)));

        var (result, delays) = await Drive(clock, o => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));

        Completed(result);
        Assert.Equal(3, delays.Count);
        Assert.Equal(Seconds(3), delays[0]);
        Assert.Equal(Seconds(10), delays[1]);
        // The earlier hints don't carry over: back to full jitter under the 4 s ceiling.
        Assert.InRange(delays[2], TimeSpan.Zero, Seconds(4));
    }

    /// <summary>Cancels a run's token once its first retry has been scheduled.</summary>
    private sealed class CancelOnRetry(CancellationTokenSource source) : IRunObserver
    {
        public void OnEvent(RunEvent runEvent)
        {
            if (runEvent is RetryScheduled)
            {
                source.Cancel();
            }
        }
    }

    [Fact]
    public async Task Cancelling_The_Run_Stops_The_Wait()
    {
        var clock = new FakeTimeProvider();
        var flaky = new Flaky(Transient());
        var graph = OneNode(flaky, clock, RecoveryPolicy.Retry(1).WithBackoff(TimeSpan.FromHours(1), TimeSpan.FromHours(1), BackoffJitter.None));
        var store = new InMemoryCheckpointStore();
        using var source = new CancellationTokenSource();

        // The clock never moves: only the cancellation can end the wait.
        var result = await graph.RunAsync("t", NewJob(), store,
            new RunOptions { TimeProvider = clock, Observer = new CancelOnRetry(source) }, source.Token).WaitAsync(Patience);

        var cancelled = Assert.IsType<GraphExecutionResult<Job>.Cancelled>(result);
        Assert.Equal("Charge", cancelled.Node);
        Assert.Equal(CancelReason.Requested, cancelled.Reason);
        Assert.Equal(1, flaky.Calls);
        var checkpoint = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Cancelled, checkpoint.Status);
        Assert.Equal("Charge", checkpoint.NextNode);
    }

    [Fact]
    public async Task A_Deadline_Inside_The_Wait_Cancels_When_It_Passes_And_Not_Before()
    {
        var clock = new FakeTimeProvider();
        var flaky = new Flaky(Transient());
        var graph = OneNode(flaky, clock, RecoveryPolicy.Retry(1).WithBackoff(Seconds(60), Seconds(60), BackoffJitter.None));
        var deadline = clock.GetUtcNow() + Seconds(5);

        var task = graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(),
            new RunOptions { TimeProvider = clock, Deadline = deadline });
        while (flaky.Calls == 0)
        {
            await Task.Delay(1).WaitAsync(Patience);
        }

        clock.Advance(Seconds(4));
        await Task.Delay(50);
        Assert.False(task.IsCompleted, "the run ended before its deadline");

        clock.Advance(Seconds(1));
        var result = await task.WaitAsync(Patience);

        var cancelled = Assert.IsType<GraphExecutionResult<Job>.Cancelled>(result);
        Assert.Equal(CancelReason.Deadline, cancelled.Reason);
        Assert.Equal(1, flaky.Calls);
    }

    /// <summary><c>Charge</c> (falls back to <c>Fallback</c>) -> <c>After</c>, recording what each sees.</summary>
    private sealed class Seen
    {
        public ConcurrentQueue<(string Node, NodeFailure? Failure)> Contexts { get; } = new();

        public Func<Job, NodeContext, CancellationToken, ValueTask<Job>> Record(string node, Func<int, NodeException?>? fail = null)
        {
            var calls = 0;
            return (job, ctx, _) =>
            {
                Contexts.Enqueue((node, ctx.LastFailure));
                return fail?.Invoke(Interlocked.Increment(ref calls)) is { } failure
                    ? ValueTask.FromException<Job>(failure)
                    : ValueTask.FromResult(job with { Trail = [.. job.Trail, node] });
            };
        }

        public NodeFailure?[] Of(string node) => [.. Contexts.Where(c => c.Node == node).Select(c => c.Failure)];
    }

    private static StateGraph<Job> FallbackGraph(
        Func<Job, NodeContext, CancellationToken, ValueTask<Job>> charge,
        Seen seen,
        RecoveryPolicy policy,
        Func<int, NodeException?>? fallbackFails = null) =>
        new StateGraph<Job>()
            .AddNode("Start", job => job)
            .AddFallibleNode("Charge", charge)
            .AddFallibleNode("Fallback", seen.Record("Fallback", fallbackFails))
            .AddFallibleNode("After", seen.Record("After"))
            .AddEdge("Start", "Charge")
            .AddEdge("Charge", "After")
            .AddEdge("Fallback", "After")
            .SetRecoveryPolicy("Charge", policy);

    [Fact]
    public async Task The_Fallback_Sees_Why_A_Transient_Failure_Ran_Out_Of_Retries()
    {
        var clock = new FakeTimeProvider();
        var seen = new Seen();
        var graph = FallbackGraph(
                seen.Record("Charge", _ => Transient(Seconds(2))), seen,
                RecoveryPolicy.Retry(1).WithBackoff(Seconds(1), Seconds(5)).WithFallback("Fallback"))
            .Compile();

        var (result, _) = await Drive(clock, o => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));

        Assert.Equal(["Fallback", "After"], Completed(result).Trail);
        var failure = Assert.Single(seen.Of("Fallback"));
        Assert.NotNull(failure);
        Assert.Equal("Charge", failure.Node);
        Assert.Equal("Charge", failure.NodePath);
        Assert.Equal(NodeFailureKind.Transient, failure.Kind);
        Assert.Equal("rate limited", failure.Message);
        Assert.Equal(Seconds(2), failure.RetryAfter);
        Assert.Equal(2, failure.Attempts);
        Assert.False(failure.TimedOut);

        // Nobody else sees it: not the failing node, nor the node after the fallback.
        Assert.All(seen.Of("Charge"), Assert.Null);
        Assert.Null(Assert.Single(seen.Of("After")));
    }

    [Fact]
    public async Task The_Fallback_Sees_A_Fatal_Failure_After_One_Attempt()
    {
        var seen = new Seen();
        var graph = FallbackGraph(
                seen.Record("Charge", _ => NodeException.Fatal("card declined")), seen,
                RecoveryPolicy.Retry(3).WithFallback("Fallback"))
            .Compile();

        Completed(await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore()));

        var failure = Assert.Single(seen.Of("Fallback"))!;
        Assert.Equal(NodeFailureKind.Fatal, failure.Kind);
        Assert.Equal("card declined", failure.Message);
        Assert.Null(failure.RetryAfter);
        Assert.Equal(1, failure.Attempts);
    }

    [Fact]
    public async Task The_Fallback_Sees_That_The_Node_Timed_Out()
    {
        var clock = new FakeTimeProvider();
        var seen = new Seen();
        var graph = FallbackGraph(
                async (job, _, ct) =>
                {
                    await Task.Delay(TimeSpan.FromHours(1), clock, ct);
                    return job;
                },
                seen,
                RecoveryPolicy.Retry(0).WithFallback("Fallback"))
            .SetTimeout("Charge", Seconds(1))
            .Compile();

        var (result, _) = await Drive(clock, o => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));

        Completed(result);
        var failure = Assert.Single(seen.Of("Fallback"))!;
        Assert.True(failure.TimedOut);
        Assert.Equal(NodeFailureKind.Transient, failure.Kind);
        Assert.Equal("node 'Charge' timed out after 1000 ms", failure.Message);
    }

    [Fact]
    public async Task Every_Attempt_Of_A_Retried_Fallback_Sees_It()
    {
        var seen = new Seen();
        var graph = FallbackGraph(
                seen.Record("Charge", _ => NodeException.Fatal("card declined")), seen,
                RecoveryPolicy.Retry(0).WithFallback("Fallback"),
                fallbackFails: call => call == 1 ? NodeException.Transient("busy") : null)
            .SetRecoveryPolicy("Fallback", RecoveryPolicy.Retry(1))
            .Compile();

        Completed(await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore()));

        var failures = seen.Of("Fallback");
        Assert.Equal(2, failures.Length);
        Assert.All(failures, f => Assert.Equal("card declined", f?.Message));
    }

    [Fact]
    public async Task Inside_A_Subgraph_The_Path_Is_Qualified_And_The_Parent_Sees_Nothing()
    {
        var seen = new Seen();
        var child = FallbackGraph(
                seen.Record("Charge", _ => NodeException.Fatal("card declined")), seen,
                RecoveryPolicy.Retry(0).WithFallback("Fallback"))
            .Compile();
        var parent = new StateGraph<Job>()
            .AddSubgraph("Work", child, job => job, (_, done) => done)
            .AddFallibleNode("Next", seen.Record("Next"))
            .AddEdge("Work", "Next")
            .Compile();

        Completed(await parent.RunAsync("t", NewJob(), new InMemoryCheckpointStore()));

        var failure = Assert.Single(seen.Of("Fallback"))!;
        Assert.Equal("Charge", failure.Node);
        Assert.Equal("Work/Charge", failure.NodePath);
        Assert.Null(Assert.Single(seen.Of("Next")));
    }

    [Fact]
    public async Task A_Group_Used_As_A_Fallback_Hands_It_To_Its_Branches()
    {
        var seen = new Seen();
        var graph = new StateGraph<Job>()
            .AddFallibleNode("Charge", seen.Record("Charge", _ => NodeException.Fatal("card declined")))
            .AddParallelGroup(
                "Fallback",
                new ParallelBranches<Job>()
                    .Branch("a", seen.Record("a"))
                    .Branch("b", seen.Record("b")),
                (job, _) => job)
            .SetRecoveryPolicy("Charge", RecoveryPolicy.Retry(0).WithFallback("Fallback"))
            .Compile();

        Completed(await graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore()));

        Assert.Equal("card declined", Assert.Single(seen.Of("a"))?.Message);
        Assert.Equal("card declined", Assert.Single(seen.Of("b"))?.Message);
    }

    [Fact]
    public async Task A_Group_Backs_Off_For_The_Longest_Hint_Among_Its_Failed_Branches()
    {
        var clock = new FakeTimeProvider();
        var calls = 0;
        Func<Job, NodeContext, CancellationToken, ValueTask<Job>> Branch(TimeSpan hint) => (job, _, _) =>
            Interlocked.Increment(ref calls) <= 2
                ? ValueTask.FromException<Job>(Transient(hint))
                : ValueTask.FromResult(job);
        var graph = new StateGraph<Job>()
            .AddParallelGroup(
                "Ask",
                new ParallelBranches<Job>().Branch("a", Branch(Seconds(2))).Branch("b", Branch(Seconds(5))),
                (job, _) => job)
            .SetRecoveryPolicy("Ask", RecoveryPolicy.Retry(1).WithBackoff(Seconds(1), Seconds(10), BackoffJitter.None))
            .Compile();

        var (result, delays) = await Drive(clock, o => graph.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));

        Completed(result);
        Assert.Equal([Seconds(5)], delays);
    }

    /// <summary>A child whose only node fails transiently with a 3 s hint, with no retries of its own.</summary>
    private static CompiledGraph<Job> RateLimitedChild(Func<int, bool> fails)
    {
        var calls = 0;
        return new StateGraph<Job>()
            .AddFallibleNode("Ask", (job, _, _) => fails(Interlocked.Increment(ref calls))
                ? ValueTask.FromException<Job>(Transient(Seconds(3)))
                : ValueTask.FromResult(job))
            .Compile();
    }

    [Fact]
    public async Task A_Subgraph_Backs_Off_For_Its_Childs_Hint()
    {
        var clock = new FakeTimeProvider();
        var parent = new StateGraph<Job>()
            .AddSubgraph("Work", RateLimitedChild(call => call == 1), job => job, (_, done) => done)
            .SetRecoveryPolicy("Work", RecoveryPolicy.Retry(1).WithBackoff(Seconds(1), Seconds(10), BackoffJitter.None))
            .Compile();

        var (result, delays) = await Drive(clock, o => parent.RunAsync("t", NewJob(), new InMemoryCheckpointStore(), o));

        Completed(result);
        Assert.Equal([Seconds(3)], delays);
    }

    [Fact]
    public async Task A_Failed_Checkpoint_Records_The_Hint_At_Every_Level()
    {
        var parent = new StateGraph<Job>()
            .AddSubgraph("Work", RateLimitedChild(_ => true), job => job, (_, done) => done)
            .Compile();
        var store = new InMemoryCheckpointStore();

        var result = await parent.RunAsync("t", NewJob(), store);

        Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result);
        var checkpoint = store.Load("t")!;
        Assert.Equal("3000", checkpoint.Metadata!["strata.retry_after_ms"]);
        Assert.Equal("3000", checkpoint.Nested!.Metadata!["strata.retry_after_ms"]);
    }

    [Fact]
    public async Task A_Failed_Checkpoint_Without_A_Hint_Has_No_Key()
    {
        var store = new InMemoryCheckpointStore();
        var graph = OneNode(new Flaky(Transient()), TimeProvider.System, RecoveryPolicy.Retry(0));

        Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(await graph.RunAsync("t", NewJob(), store));

        Assert.False(store.Load("t")!.Metadata!.ContainsKey("strata.retry_after_ms"));
    }

    [Fact]
    public void Bad_Backoffs_And_Hints_Are_Refused()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => RecoveryPolicy.Retry(1).WithBackoff(Seconds(-1), Seconds(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => RecoveryPolicy.Retry(1).WithBackoff(Seconds(2), Seconds(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => NodeException.Transient("x", Seconds(-1)));

        // A null hint is a transient failure with none.
        Assert.Null(NodeException.Transient("x", (TimeSpan?)null).RetryAfter);
    }
}
