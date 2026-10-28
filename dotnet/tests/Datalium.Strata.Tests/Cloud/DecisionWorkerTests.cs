using System.Text.Json.Nodes;
using Datalium.Strata.Cloud;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Scheduling;
using static Datalium.Strata.Tests.Cloud.CloudStoreTests;

namespace Datalium.Strata.Tests.Cloud;

/// <summary>
/// Mirrors <c>rust/strata-cloud/tests/worker.rs</c> (<c>design/30-Strata-Cloud-Store.md</c>, "The worker"):
/// the decision worker against the emulated Cloud. Keep the two in step.
/// </summary>
[Collection("Cloud")]
public class DecisionWorkerTests
{
    private sealed class Setup : IDisposable
    {
        public Setup(bool conditional = true)
        {
            Cloud = new EmulatedCloud { Conditional = conditional };
            Store = new StrataCloudCheckpointStore(CloudTest.Options(Time), new HttpClient(Cloud));
            (Graph, Executed) = RolloutGraph();
        }

        public RecordingTimeProvider Time { get; } = new();

        public EmulatedCloud Cloud { get; }

        public StrataCloudCheckpointStore Store { get; }

        public CompiledGraph<Rollout> Graph { get; }

        public Func<int> Executed { get; }

        public DecisionWorker<Rollout> Worker(DecisionWorkerOptions<Rollout>? options = null, ICheckpointStore? store = null) =>
            new(Graph, store ?? Store, new StrataCloudDecisions(CloudTest.Options(Time), new HttpClient(Cloud)), options);

        public async Task Interrupt(string threadId) =>
            Assert.IsType<GraphExecutionResult<Rollout>.Interrupted>(await Graph.RunAsync(threadId, Initial(), Store));

        public void Dispose() => Store.Dispose();
    }

    [Fact]
    public async Task Resumes_a_decided_thread_and_reports_it()
    {
        using var setup = new Setup();
        await setup.Interrupt("inc-1");
        var approval = setup.Cloud.Decide("inc-1", true, "alice", overrides: new JsonObject { ["replicas"] = 3 });
        var reported = new List<HandledDecision<Rollout>>();
        var optionsFor = new List<string>();
        var worker = setup.Worker(new DecisionWorkerOptions<Rollout>
        {
            RunOptions = decision =>
            {
                optionsFor.Add(decision.ApprovalId);
                return new RunOptions { Metadata = { ["approval"] = decision.ApprovalId } };
            },
            OnHandled = (handled, _) =>
            {
                reported.Add(handled);
                return ValueTask.CompletedTask;
            },
        });

        var handled = Assert.Single(await worker.PollAsync(TimeSpan.Zero));

        Assert.Equal(DecisionOutcome.Resumed, handled.Outcome);
        Assert.Equal(approval, handled.Decision.ApprovalId);
        Assert.Equal("user-alice", handled.Decision.DecidedBy);
        Assert.Equal("alice@example.com", handled.Decision.DecidedByEmail);
        Assert.Null(handled.Decision.Notes);
        var state = Assert.IsType<GraphExecutionResult<Rollout>.Completed>(handled.Result).State;
        Assert.Equal(["prepared", "scaled to 3"], state.Log);
        Assert.Equal(1, setup.Executed());
        Assert.Equal([handled], reported);
        Assert.Equal([approval], optionsFor);

        // The resume's stamp acknowledged it: the feed no longer offers it.
        Assert.Empty(await worker.PollAsync(TimeSpan.Zero));
    }

    [Fact]
    public async Task A_thread_resumed_by_someone_else_first_is_already_handled()
    {
        using var setup = new Setup();
        await setup.Interrupt("inc-2");
        setup.Cloud.Decide("inc-2", true, "alice");
        var store = new FirstLoad(setup.Store, async inner =>
        {
            var latest = await inner.LoadLatestAsync("inc-2");
            await inner.SaveAsync(latest!.Checkpoint with { ResumedAt = "2026-10-09T12:00:00Z" });
        });

        var handled = Assert.Single(await setup.Worker(store: store).PollAsync(TimeSpan.Zero));

        Assert.Equal(DecisionOutcome.AlreadyHandled, handled.Outcome);
        Assert.Equal(0, setup.Executed());
    }

    [Fact]
    public async Task A_decision_for_a_thread_that_is_running_is_already_handled()
    {
        // design/33-Durable-Execution.md: a durable graph's resume makes its thread running, so a worker that
        // was handed the decision just before finds a thread somebody is already running.
        using var setup = new Setup();
        await setup.Interrupt("inc-running");
        setup.Cloud.Decide("inc-running", true, "alice");
        var store = new FirstLoad(setup.Store, async inner =>
        {
            var latest = await inner.LoadLatestAsync("inc-running");
            await inner.SaveAsync(
                latest!.Checkpoint with { SchemaVersion = CheckpointSchema.RunningVersion, Status = CheckpointStatus.Running });
        });

        var handled = Assert.Single(await setup.Worker(store: store).PollAsync(TimeSpan.Zero));

        Assert.Equal(DecisionOutcome.AlreadyHandled, handled.Outcome);
        Assert.Equal(0, setup.Executed());
    }

    [Fact]
    public async Task A_stale_decision_runs_nothing_once_the_thread_has_moved_on()
    {
        using var setup = new Setup();
        await setup.Interrupt("inc-3");
        setup.Cloud.Decide("inc-3", false, "alice");

        // Before the worker's resume, the thread moves on to a later checkpoint of the same boundary kind:
        // the reviewer's decision answered the earlier one.
        var store = new FirstLoad(setup.Store, async inner =>
        {
            var latest = await inner.LoadLatestAsync("inc-3");
            await inner.SaveAsync(latest!.Checkpoint with { ResumedAt = "2026-10-09T12:00:00Z" });
            await inner.SaveAsync(latest.Checkpoint with { CreatedAt = "2026-10-09T12:00:01Z" });
        });

        var handled = Assert.Single(await setup.Worker(store: store).PollAsync(TimeSpan.Zero));

        Assert.Equal(DecisionOutcome.AlreadyHandled, handled.Outcome);
        Assert.Equal(0, setup.Executed());
        Assert.Null(setup.Cloud.Latest("inc-3")["resumed_at"]);
    }

    [Fact]
    public async Task Pages_past_a_foreign_thread_and_a_refused_decision_handling_each_once()
    {
        using var setup = new Setup();
        await setup.Interrupt("other-1");
        await setup.Interrupt("inc-4");
        setup.Cloud.Decide("other-1", true, "alice");
        setup.Cloud.Decide("inc-4", true, "alice", overrides: new JsonObject { ["replicas"] = "lots" });
        var worker = setup.Worker(new DecisionWorkerOptions<Rollout> { ThreadIdPrefix = "inc-", Limit = 1 });

        var first = await worker.PollAsync(TimeSpan.Zero);

        Assert.Equal([DecisionOutcome.NotMine, DecisionOutcome.Failed], first.Select(h => h.Outcome));
        Assert.Null(first[0].Result);
        Assert.IsType<GraphExecutionResult<Rollout>.Failed>(first[1].Result);
        Assert.Equal(0, setup.Executed());

        // Cloud still offers both: neither is handled again, and a new decision behind them is.
        await setup.Interrupt("inc-5");
        setup.Cloud.Decide("inc-5", true, "bob");
        var second = await worker.PollAsync(TimeSpan.Zero);
        var handled = Assert.Single(second);
        Assert.Equal("inc-5", handled.Decision.ThreadId);
        Assert.Equal(DecisionOutcome.Resumed, handled.Outcome);
        Assert.Empty(await worker.PollAsync(TimeSpan.Zero));
    }

    [Fact]
    public async Task Owns_decides_which_threads_are_resumed()
    {
        using var setup = new Setup();
        await setup.Interrupt("inc-6");
        setup.Cloud.Decide("inc-6", true, "alice");
        var worker = setup.Worker(new DecisionWorkerOptions<Rollout> { Owns = (_, _) => ValueTask.FromResult(false) });

        Assert.Equal(DecisionOutcome.NotMine, Assert.Single(await worker.PollAsync(TimeSpan.Zero)).Outcome);
        Assert.Equal(0, setup.Executed());
    }

    [Fact]
    public async Task A_thread_another_graph_wrote_is_not_mine_and_runs_nothing()
    {
        using var setup = new Setup();
        var (v1, executedByV1) = RolloutGraph(new CompilationOptions { GraphId = "rollout", GraphVersion = "1" });
        var (v2, executedByV2) = RolloutGraph(new CompilationOptions { GraphId = "rollout", GraphVersion = "2" });
        Assert.IsType<GraphExecutionResult<Rollout>.Interrupted>(await v1.RunAsync("inc-8", Initial(), setup.Store));
        setup.Cloud.Decide("inc-8", true, "alice");
        DecisionWorker<Rollout> WorkerFor(CompiledGraph<Rollout> graph) =>
            new(graph, setup.Store, new StrataCloudDecisions(CloudTest.Options(setup.Time), new HttpClient(setup.Cloud)));

        // design/31-Graph-Identity.md: v2's worker leaves v1's thread alone, with no Owns lookup.
        var notMine = Assert.Single(await WorkerFor(v2).PollAsync(TimeSpan.Zero));
        Assert.Equal(DecisionOutcome.NotMine, notMine.Outcome);
        Assert.Null(notMine.Result);
        Assert.Equal(0, executedByV2());
        Assert.Null(setup.Cloud.Latest("inc-8")["resumed_at"]);

        // v1's worker resumes it.
        var mine = Assert.Single(await WorkerFor(v1).PollAsync(TimeSpan.Zero));
        Assert.Equal(DecisionOutcome.Resumed, mine.Outcome);
        Assert.Equal(1, executedByV1());
    }

    [Theory]
    [InlineData(true, 1)]
    [InlineData(false, 2)]
    public async Task Two_workers_one_decision_run_once_with_the_conditional_save_and_twice_without(bool conditional, int runs)
    {
        using var setup = new Setup(conditional);
        await setup.Interrupt("inc-7");
        setup.Cloud.Decide("inc-7", true, "alice");

        // Both workers' resumes load the decided checkpoint before either stamps it.
        var loads = 0;
        var bothLoaded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        setup.Cloud.BeforeLoad = async () =>
        {
            if (Interlocked.Increment(ref loads) == 2)
            {
                bothLoaded.SetResult();
            }

            await bothLoaded.Task.WaitAsync(TimeSpan.FromSeconds(10));
        };

        var results = await Task.WhenAll(setup.Worker().PollAsync(TimeSpan.Zero), setup.Worker().PollAsync(TimeSpan.Zero));
        setup.Cloud.BeforeLoad = null;

        var outcomes = results.Select(r => Assert.Single(r).Outcome).Order().ToList();
        Assert.Equal(runs, setup.Executed());
        Assert.Equal(
            conditional ? [DecisionOutcome.Resumed, DecisionOutcome.AlreadyHandled] : [DecisionOutcome.Resumed, DecisionOutcome.Resumed],
            outcomes);
    }

    [Fact]
    public async Task Run_waits_out_an_unavailable_feed_honours_retry_after_and_stops_on_401()
    {
        var time = new RecordingTimeProvider();
        var feed = new ScriptedCloud()
            .Reply(503, CloudTest.Envelope(503, "STRATA_CLOUD_API_DATABASE_ERROR"))
            .Reply(429, CloudTest.Envelope(429, "STRATA_CLOUD_API_RATE_LIMITED"), new Dictionary<string, string> { ["Retry-After"] = "5" })
            .Reply(401, CloudTest.Envelope(401, "STRATA_CLOUD_API_TOKEN_REVOKED"));
        var (graph, _) = RolloutGraph();
        var worker = new DecisionWorker<Rollout>(
            graph,
            new Storage.InMemoryCheckpointStore(),
            new StrataCloudDecisions(CloudTest.Options(time), new HttpClient(feed)),
            new DecisionWorkerOptions<Rollout>
            {
                ErrorBackoff = new RetryBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), BackoffJitter.None),
            });

        var stopped = await Assert.ThrowsAsync<StrataCloudException>(() => worker.RunAsync(CancellationToken.None));

        Assert.Equal(StrataCloudErrorKind.Unauthorized, stopped.Kind);
        Assert.Equal([TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5)], time.Waits);
        Assert.All(feed.Requests, r => Assert.Equal("25", r.Query["wait"]));
    }

    [Fact]
    public async Task Run_waits_the_idle_delay_after_a_round_with_nothing_new_and_stops_when_cancelled()
    {
        using var setup = new Setup();
        await setup.Interrupt("other-2");
        setup.Cloud.Decide("other-2", true, "alice");
        var worker = setup.Worker(new DecisionWorkerOptions<Rollout> { ThreadIdPrefix = "inc-" });
        using var stop = new CancellationTokenSource();

        var run = worker.RunAsync(stop.Token);
        while (setup.Time.Waits.Count(w => w == TimeSpan.FromSeconds(2)) < 3)
        {
            await Task.Delay(5);
        }

        await stop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        // Every round after the first found only the foreign decision, and waited 2 s before the next.
        Assert.All(setup.Time.Waits, w => Assert.Equal(TimeSpan.FromSeconds(2), w));
        Assert.Equal(0, setup.Executed());
    }

    [Fact]
    public async Task A_decision_whose_thread_is_held_is_left_for_the_next_round()
    {
        using var setup = new Setup();
        await setup.Interrupt("inc-held");
        setup.Cloud.Decide("inc-held", true, "alice");
        var store = setup.Store;
        var reported = 0;
        var worker = setup.Worker(
            new DecisionWorkerOptions<Rollout>
            {
                RunOptions = _ => new RunOptions { Lease = new LeaseOptions("worker-1", TimeSpan.FromMinutes(1)) },
                OnHandled = (_, _) =>
                {
                    reported++;
                    return ValueTask.CompletedTask;
                },
            });

        // Another owner holds the thread on Cloud (design/32-Thread-Leases.md, design/43-Cloud-Leases-And-Listing.md):
        // the decision is theirs to act on for now.
        var holder = (await store.TryAcquireLeaseAsync("inc-held", "worker-2", TimeSpan.FromMinutes(1))).Lease!;
        Assert.Empty(await worker.PollAsync(TimeSpan.Zero));
        Assert.Equal((0, 0), (reported, setup.Executed()));
        Assert.Null(setup.Cloud.Latest("inc-held")["resumed_at"]);

        // It wasn't remembered as handled, so the next round resumes it, under the worker's own lease.
        await store.ReleaseLeaseAsync(holder);
        var handled = Assert.Single(await worker.PollAsync(TimeSpan.Zero));
        Assert.Equal(DecisionOutcome.Resumed, handled.Outcome);
        Assert.Equal((1, 1), (reported, setup.Executed()));
        Assert.Null(await store.GetLeaseAsync("inc-held"));
    }

    [Fact]
    public async Task A_decision_whose_resume_is_not_admitted_is_left_for_the_next_round()
    {
        using var setup = new Setup();
        await setup.Interrupt("inc-busy");
        setup.Cloud.Decide("inc-busy", true, "alice");
        var scheduler = new RunScheduler(new RunSchedulerOptions { MaxConcurrent = 1, MaxQueued = 0 });
        var reported = 0;
        var worker = setup.Worker(new DecisionWorkerOptions<Rollout>
        {
            RunOptions = _ => new RunOptions { Scheduler = scheduler },
            OnHandled = (_, _) =>
            {
                reported++;
                return ValueTask.CompletedTask;
            },
        });

        // The process is busy (design/40-Run-Scheduler.md): nothing was tried, so nothing is remembered.
        using (var busy = await scheduler.AdmitAsync(new AdmissionRequest()))
        {
            Assert.Empty(await worker.PollAsync(TimeSpan.Zero));
            Assert.Equal((0, 0), (reported, setup.Executed()));
            Assert.Null(setup.Cloud.Latest("inc-busy")["resumed_at"]);
        }

        var handled = Assert.Single(await worker.PollAsync(TimeSpan.Zero));
        Assert.Equal(DecisionOutcome.Resumed, handled.Outcome);
        Assert.Equal((1, 1), (reported, setup.Executed()));
    }

    /// <summary>A store that does something else to the thread just before its first load.</summary>
    private sealed class FirstLoad(ICheckpointStore inner, Func<ICheckpointStore, Task> before) : ICheckpointStore
    {
        private Func<ICheckpointStore, Task>? _before = before;

        public void Save(Checkpoint checkpoint) => inner.Save(checkpoint);

        public Checkpoint? Load(string threadId) => inner.Load(threadId);

        public ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
            inner.SaveAsync(checkpoint, cancellationToken);

        public async ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Exchange(ref _before, null) is { } first)
            {
                await first(inner);
            }

            return await inner.LoadLatestAsync(threadId, cancellationToken);
        }

        public ValueTask<bool> SaveIfLatestAsync(Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
            inner.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken);
    }
}
