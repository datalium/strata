using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Scheduling;
using Datalium.Strata.Storage;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.Tests.Scheduling;

/// <summary>
/// The engine under a run scheduler (<c>design/40-Run-Scheduler.md</c>, "What a run does with its slot"): every run
/// mode and both drivers wait for a slot before they touch the store, a run that isn't admitted leaves nothing behind,
/// and every way a run ends gives its slot back. Mirrors <c>rust/strata-core/tests/scheduler_engine.rs</c>.
/// </summary>
public sealed class SchedulerEngineTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Work(IReadOnlyList<string> Trail, IReadOnlyList<string> Items);

    private static Work NewWork(params string[] items) => new([], items);

    private static Func<Work, Work> Step(string label) => work => work with { Trail = [.. work.Trail, label] };

    /// <summary><c>Prepare</c> → <c>Approve</c> (a boundary) → <c>Finish</c>.</summary>
    private static CompiledGraph<Work> Approval(CompilationOptions? options = null) =>
        new StateGraph<Work>()
            .AddNode("Prepare", Step("prepare"))
            .AddNode("Approve", Step("approve"))
            .AddNode("Finish", Step("finish"))
            .AddEdge("Prepare", "Approve")
            .AddEdge("Approve", "Finish")
            .AddInterruptBoundary("Approve")
            .Compile(options ?? new CompilationOptions());

    private static RunScheduler Scheduler(int maxConcurrent = 1, int maxQueued = 1024, int? tenantLimit = null) =>
        new(new RunSchedulerOptions
        {
            Name = Guid.NewGuid().ToString(),
            MaxConcurrent = maxConcurrent,
            MaxQueued = maxQueued,
            TenantLimit = tenantLimit,
        });

    /// <summary>Waits, a while at most, until <paramref name="waiting"/> runs wait for a slot.</summary>
    private static async Task Queued(RunScheduler scheduler, int waiting)
    {
        using var patience = new CancellationTokenSource(Patience);
        while (scheduler.Snapshot().Queued < waiting)
        {
            await Task.Delay(5, patience.Token);
        }
    }

    /// <summary>Takes a slot the way a run elsewhere in the process would.</summary>
    private static async Task<Admission> Hold(RunScheduler scheduler, string? tenant = null)
    {
        var admission = await scheduler.AdmitAsync(new AdmissionRequest { Tenant = tenant }).AsTask().WaitAsync(Patience);
        Assert.True(admission.IsAdmitted);
        return admission;
    }

    private static GraphExecutionResult<Work>.NotAdmitted Refused(GraphExecutionResult<Work> result) =>
        Assert.IsType<GraphExecutionResult<Work>.NotAdmitted>(result);

    /// <summary>A store that counts every call made to it.</summary>
    private sealed class Watched(InMemoryCheckpointStore inner) : ICheckpointStore, ICheckpointQuery
    {
        private int _calls;

        public Watched()
            : this(new InMemoryCheckpointStore())
        {
        }

        public int Calls => Volatile.Read(ref _calls);

        public InMemoryCheckpointStore Inner => inner;

        private T Count<T>(Func<T> call)
        {
            Interlocked.Increment(ref _calls);
            return call();
        }

        public void Save(Checkpoint checkpoint) => Count(() => { inner.Save(checkpoint); return 0; });

        public Checkpoint? Load(string threadId) => Count(() => inner.Load(threadId));

        public ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
            Count(() => ((ICheckpointStore)inner).SaveAsync(checkpoint, cancellationToken));

        public ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
            Count(() => ((ICheckpointStore)inner).LoadAsync(threadId, cancellationToken));

        public ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
            Count(() => inner.LoadLatestAsync(threadId, cancellationToken));

        public ValueTask<bool> SaveIfLatestAsync(
            Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
            Count(() => inner.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken));

        public ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
            string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default) =>
            Count(() => inner.TryAcquireLeaseAsync(threadId, owner, ttl, cancellationToken));

        public ValueTask<ThreadLease?> RenewLeaseAsync(ThreadLease lease, TimeSpan ttl, CancellationToken cancellationToken = default) =>
            Count(() => inner.RenewLeaseAsync(lease, ttl, cancellationToken));

        public ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken cancellationToken = default) =>
            Count(() => inner.ReleaseLeaseAsync(lease, cancellationToken));

        public ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken cancellationToken = default) =>
            Count(() => inner.GetLeaseAsync(threadId, cancellationToken));

        public ValueTask<bool> SaveUnderLeaseAsync(
            Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default) =>
            Count(() => inner.SaveUnderLeaseAsync(checkpoint, lease, expectedSequence, cancellationToken));

        public ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default) =>
            Count(() => inner.ListThreadsAsync(query, cancellationToken));

        public ValueTask<CheckpointLogPage> GetLogAsync(
            string threadId, long? afterSequence = null, int limit = ThreadQuery.DefaultLimit, CancellationToken cancellationToken = default) =>
            Count(() => inner.GetLogAsync(threadId, afterSequence, limit, cancellationToken));
    }

    /// <summary>Every event of every run, however many.</summary>
    private sealed class Events : IRunObserver
    {
        public ConcurrentQueue<RunEvent> Seen { get; } = new();

        public void OnEvent(RunEvent runEvent) => Seen.Enqueue(runEvent);
    }

    [Fact]
    public async Task A_run_and_a_resume_wait_for_a_slot_before_they_touch_the_store()
    {
        var scheduler = Scheduler();
        var store = new Watched();
        var graph = Approval();
        var options = new RunOptions { Scheduler = scheduler };

        var holder = await Hold(scheduler);
        var run = graph.RunAsync("t", NewWork(), store, options);
        await Task.Delay(100);
        Assert.False(run.IsCompleted);
        Assert.Equal(0, store.Calls);
        Assert.Equal(1, scheduler.Snapshot().Queued);

        holder.Dispose();
        Assert.IsType<GraphExecutionResult<Work>.Interrupted>(await run.WaitAsync(Patience));
        var afterRun = store.Calls;
        Assert.True(afterRun > 0);

        holder = await Hold(scheduler);
        var resume = graph.ResumeAsync("t", HumanSignal.Approve("alice"), store, options);
        await Task.Delay(100);
        Assert.False(resume.IsCompleted);
        Assert.Equal(afterRun, store.Calls);

        holder.Dispose();
        var done = Assert.IsType<GraphExecutionResult<Work>.Completed>(await resume.WaitAsync(Patience));
        Assert.Equal(["prepare", "approve", "finish"], done.State.Trail);
        Assert.Equal(0, scheduler.Snapshot().Active);
    }

    [Fact]
    public async Task Every_run_mode_not_admitted_returns_not_admitted_and_leaves_nothing_behind()
    {
        var scheduler = Scheduler(maxQueued: 0);
        var store = new Watched();
        var graph = Approval(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });
        await graph.RunAsync("waiting", NewWork(), store.Inner);
        var events = new Events();
        var options = new RunOptions { Scheduler = scheduler, Observer = events };
        using var holder = await Hold(scheduler);

        var results = new List<GraphExecutionResult<Work>>
        {
            await graph.RunAsync("fresh", NewWork(), store, options),
            await graph.ResumeAsync("waiting", HumanSignal.Approve("alice"), store, options),
            await graph.SignalAsync("waiting", new EventSignal("payment"), store, options),
            await graph.RecoverAsync("waiting", store, options),
        };
        foreach (var (reader, completion) in new[]
        {
            graph.RunStreaming("fresh", NewWork(), store, options),
            graph.ResumeStreaming("waiting", HumanSignal.Approve("alice"), store, options),
            graph.RecoverStreaming("waiting", store, options),
        })
        {
            results.Add(await completion.WaitAsync(Patience));
            Assert.Empty(reader.ReadAllAsync().ToBlockingEnumerable());
        }

        Assert.All(results, result =>
        {
            var refused = Refused(result);
            Assert.Equal(NotAdmittedReason.QueueFull, refused.Reason);
            Assert.Equal("run not admitted: the scheduler's queue is full", refused.Message);
        });
        Assert.Equal(0, store.Calls);
        await Task.Delay(50);
        Assert.Empty(events.Seen);
        Assert.Null(store.Inner.Load("fresh"));
        Assert.Equal(CheckpointStatus.Interrupted, store.Inner.Load("waiting")!.Status);
    }

    [Fact]
    public async Task Every_way_a_run_ends_gives_its_slot_back()
    {
        var scheduler = Scheduler(maxQueued: 0);
        var options = new RunOptions { Scheduler = scheduler };
        var store = new InMemoryCheckpointStore();
        var graph = new StateGraph<Work>()
            .AddFallibleNode("Work", work => work.Items.FirstOrDefault() switch
            {
                "fail" => throw NodeException.Fatal("no"),
                "throw" => throw new InvalidOperationException("bug"),
                "wait" => work,
                _ => Step("work")(work),
            })
            .AddNode("Wait", Step("wait"))
            .AddNode("Done", Step("done"))
            .AddConditionalEdge("Work", "Wait", work => work.Items.FirstOrDefault() == "wait")
            .AddEdge("Work", "Done")
            .AddEdge("Wait", "Done")
            .AddInterruptBoundary("Wait")
            .Compile();

        Assert.IsType<GraphExecutionResult<Work>.Completed>(await graph.RunAsync("a", NewWork(), store, options));
        Assert.IsType<GraphExecutionResult<Work>.Interrupted>(await graph.RunAsync("b", NewWork("wait"), store, options));
        Assert.IsType<GraphExecutionResult<Work>.NodeFailed>(await graph.RunAsync("c", NewWork("fail"), store, options));
        await Assert.ThrowsAnyAsync<Exception>(() => graph.RunAsync("d", NewWork("throw"), store, options));
        using (var cancelled = new CancellationTokenSource())
        {
            await cancelled.CancelAsync();

            // Admitted on arrival with a free slot, so it starts, and stops at its first check.
            Assert.IsType<GraphExecutionResult<Work>.Cancelled>(
                await graph.RunAsync("e", NewWork(), store, options, cancelled.Token));
        }

        Assert.Equal(0, scheduler.Snapshot().Active);
        Assert.IsType<GraphExecutionResult<Work>.Completed>(await graph.RunAsync("f", NewWork(), store, options));
    }

    [Fact]
    public async Task A_subgraph_a_group_and_a_map_run_in_their_runs_one_slot()
    {
        var scheduler = Scheduler(maxQueued: 0);
        var child = new StateGraph<Work>().AddNode("Inner", Step("inner")).Compile();
        var graph = new StateGraph<Work>()
            .AddParallelGroup(
                "Group",
                new ParallelBranches<Work>().Branch("b1", Step("b1")).Branch("b2", Step("b2")),
                (work, results) => work with { Trail = [.. work.Trail, .. results.SelectMany(r => r.Trail)] })
            .AddSubgraph("Sub", child, work => work, (_, done) => done)
            .AddMap(
                "Map",
                work => work.Items,
                item => item,
                child,
                (_, item) => NewWork(item),
                (work, done) => work with { Trail = [.. work.Trail, $"mapped {done.Count}"] })
            .AddEdge("Group", "Sub")
            .AddEdge("Sub", "Map")
            .Compile();

        var result = await graph.RunAsync(
            "t", NewWork("x", "y", "z"), new InMemoryCheckpointStore(), new RunOptions { Scheduler = scheduler });

        var done = Assert.IsType<GraphExecutionResult<Work>.Completed>(result);
        Assert.Equal(["b1", "b2", "inner", "mapped 3"], done.State.Trail);
    }

    [Fact]
    public async Task Ending_a_thread_takes_no_slot()
    {
        var scheduler = Scheduler(maxQueued: 0);
        var store = new InMemoryCheckpointStore();
        var graph = Approval();
        await graph.RunAsync("t", NewWork(), store);
        using var holder = await Hold(scheduler);

        var ended = await graph.EndAsync("t", ThreadEnd.By("alice"), store, new RunOptions { Scheduler = scheduler });

        Assert.Equal(CancelReason.Ended, Assert.IsType<GraphExecutionResult<Work>.Cancelled>(ended).Reason);
    }

    [Fact]
    public async Task A_token_or_a_deadline_ends_a_wait_with_nothing_written()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var scheduler = Scheduler();
        var store = new Watched();
        var graph = Approval();
        using var holder = await Hold(scheduler);

        using var token = new CancellationTokenSource();
        var cancelled = graph.RunAsync("a", NewWork(), store, new RunOptions { Scheduler = scheduler }, token.Token);
        await token.CancelAsync();
        Assert.Equal(NotAdmittedReason.Cancelled, Refused(await cancelled.WaitAsync(Patience)).Reason);

        var late = graph.RunAsync(
            "b",
            NewWork(),
            store,
            new RunOptions { Scheduler = scheduler, TimeProvider = clock, Deadline = clock.GetUtcNow() + TimeSpan.FromSeconds(5) });
        clock.Advance(TimeSpan.FromSeconds(5));
        var refused = Refused(await late.WaitAsync(Patience));
        Assert.Equal(NotAdmittedReason.Deadline, refused.Reason);
        Assert.Equal(TimeSpan.FromSeconds(5), refused.Waited);

        Assert.Equal(0, store.Calls);
    }

    [Fact]
    public async Task The_tenant_is_the_runs_metadata_and_a_full_tenant_holds_back_only_its_own()
    {
        var scheduler = Scheduler(maxConcurrent: 4, tenantLimit: 1);
        var store = new InMemoryCheckpointStore();
        var graph = Approval();
        var holder = await Hold(scheduler, "acme");

        var acme = graph.RunAsync("acme-1", NewWork(), store, new RunOptions { Scheduler = scheduler, Metadata = { ["tenant"] = "acme" } });
        var globex = graph.RunAsync("globex-1", NewWork(), store, new RunOptions { Scheduler = scheduler, Metadata = { ["tenant"] = "globex" } });

        Assert.IsType<GraphExecutionResult<Work>.Interrupted>(await globex.WaitAsync(Patience));
        Assert.False(acme.IsCompleted);

        holder.Dispose();
        Assert.IsType<GraphExecutionResult<Work>.Interrupted>(await acme.WaitAsync(Patience));
    }

    [Fact]
    public async Task A_higher_priority_run_starts_first()
    {
        var scheduler = Scheduler();
        var order = new ConcurrentQueue<string>();
        var graph = new StateGraph<Work>()
            .AddNode("Work", work =>
            {
                order.Enqueue(work.Items[0]);
                return work;
            })
            .Compile();
        var store = new InMemoryCheckpointStore();
        var holder = await Hold(scheduler);

        var low = graph.RunAsync("low", NewWork("low"), store, new RunOptions { Scheduler = scheduler, Priority = 0 });
        var high = graph.RunAsync("high", NewWork("high"), store, new RunOptions { Scheduler = scheduler, Priority = 5 });
        await Queued(scheduler, 2);

        holder.Dispose();
        await Task.WhenAll(low, high).WaitAsync(Patience);

        Assert.Equal(["high", "low"], order);
    }

    [Fact]
    public async Task The_run_says_how_long_it_queued_on_its_first_event_and_its_span()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var scheduler = Scheduler();
        var graph = Approval();
        var store = new InMemoryCheckpointStore();
        var spans = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == StrataTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        var holder = await Hold(scheduler);
        var events = new RunEventChannel();

        var run = graph.RunAsync(
            "queued", NewWork(), store, new RunOptions { Scheduler = scheduler, Priority = 2, Observer = events, TimeProvider = clock });
        await Queued(scheduler, 1);

        clock.Advance(TimeSpan.FromSeconds(3));
        holder.Dispose();
        await run.WaitAsync(Patience);

        var started = Assert.IsType<RunStarted>(await events.Reader.ReadAsync());
        Assert.Equal(TimeSpan.FromSeconds(3), started.QueuedFor);
        var span = spans.Single(a => a.OperationName == "strata.run" && (string?)a.GetTagItem("strata.thread_id") == "queued");
        Assert.Equal(2, span.GetTagItem("strata.run.priority"));
        Assert.Equal(3000.0, span.GetTagItem("strata.scheduler.wait_ms"));

        // Without a scheduler there is no queue to have waited in.
        var plain = new RunEventChannel();
        await graph.RunAsync("plain", NewWork(), store, new RunOptions { Observer = plain });
        Assert.Null(Assert.IsType<RunStarted>(await plain.Reader.ReadAsync()).QueuedFor);
        var plainSpan = spans.Single(a => a.OperationName == "strata.run" && (string?)a.GetTagItem("strata.thread_id") == "plain");
        Assert.Null(plainSpan.GetTagItem("strata.scheduler.wait_ms"));
    }

    [Fact]
    public async Task Admissions_and_waits_are_counted_by_outcome()
    {
        var scheduler = Scheduler(maxQueued: 0);
        var name = scheduler.Options.Name;
        var outcomes = new ConcurrentQueue<(string Instrument, string? Outcome, long Value)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == StrataTelemetry.Name && instrument.Name.StartsWith("strata.scheduler.", StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, 1, tags));
        listener.Start();

        var graph = Approval();
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync("a", NewWork(), store, new RunOptions { Scheduler = scheduler });
        using (await Hold(scheduler))
        {
            await graph.RunAsync("b", NewWork(), store, new RunOptions { Scheduler = scheduler });
        }

        Assert.Equal(2, outcomes.Count(o => o is { Instrument: "strata.scheduler.admissions", Outcome: "admitted" }));
        Assert.Equal(1, outcomes.Count(o => o is { Instrument: "strata.scheduler.admissions", Outcome: "queue_full" }));
        Assert.Equal(3, outcomes.Count(o => o.Instrument == "strata.scheduler.wait"));
        Assert.Equal(0, outcomes.Where(o => o.Instrument == "strata.scheduler.active").Sum(o => o.Value));
        Assert.Equal(0, outcomes.Where(o => o.Instrument == "strata.scheduler.queued").Sum(o => o.Value));

        void Record(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            string? scheduler = null;
            string? outcome = null;
            foreach (var tag in tags)
            {
                if (tag.Key == "strata.scheduler.name")
                {
                    scheduler = (string?)tag.Value;
                }
                else if (tag.Key == "strata.scheduler.outcome")
                {
                    outcome = (string?)tag.Value;
                }
            }

            if (scheduler == name)
            {
                outcomes.Enqueue((instrument.Name, outcome, value));
            }
        }
    }

    [Fact]
    public async Task The_wake_driver_skips_a_thread_not_admitted_and_wakes_it_next_round()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var scheduler = Scheduler(maxQueued: 0);
        var store = new InMemoryCheckpointStore();
        var graph = new StateGraph<Work>()
            .AddNode("Start", Step("start"))
            .AddTimer("Later", TimeSpan.FromMinutes(1))
            .AddNode("Finish", Step("finish"))
            .AddEdge("Start", "Later")
            .AddEdge("Later", "Finish")
            .Compile();
        await graph.RunAsync("t", NewWork(), store, new RunOptions { TimeProvider = clock, Metadata = { ["tenant"] = "acme" } });
        clock.Advance(TimeSpan.FromMinutes(2));
        var wake = new WakeOptions { Run = new RunOptions { Scheduler = scheduler, TimeProvider = clock } };

        WakeReport<Work> report;
        using (await Hold(scheduler))
        {
            report = await graph.WakeDueAsync(store, wake);
        }

        Assert.Empty(report.Woken);
        var skipped = Assert.Single(report.Skipped);
        Assert.Equal("it was not admitted: run not admitted: the scheduler's queue is full", skipped.Reason);
        Assert.Equal(CheckpointStatus.Interrupted, store.Load("t")!.Status);

        report = await graph.WakeDueAsync(store, wake);
        Assert.IsType<GraphExecutionResult<Work>.Completed>(Assert.Single(report.Woken).Result);
    }

    [Fact]
    public async Task A_drivers_stop_token_ends_its_wait_for_a_slot()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var scheduler = Scheduler();
        var store = new InMemoryCheckpointStore();
        var graph = new StateGraph<Work>()
            .AddTimer("Later", TimeSpan.FromMinutes(1))
            .AddNode("Finish", Step("finish"))
            .AddEdge("Later", "Finish")
            .Compile();
        await graph.RunAsync("t", NewWork(), store, new RunOptions { TimeProvider = clock });
        clock.Advance(TimeSpan.FromMinutes(2));
        using var stop = new CancellationTokenSource();
        using var holder = await Hold(scheduler);

        var round = graph.WakeDueAsync(
            store, new WakeOptions { Run = new RunOptions { Scheduler = scheduler, TimeProvider = clock }, Stop = stop.Token });
        await Queued(scheduler, 1);

        await stop.CancelAsync();
        var report = await round.WaitAsync(Patience);

        Assert.Empty(report.Woken);
        Assert.Contains("cancelled while waiting for a slot", Assert.Single(report.Skipped).Reason);
        Assert.Equal(CheckpointStatus.Interrupted, store.Load("t")!.Status);
    }

    [Fact]
    public async Task The_recovery_driver_skips_a_thread_not_admitted()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow.AddYears(10));
        var scheduler = Scheduler(maxQueued: 0);
        var store = new Watched();
        var running = Checkpoint.FromWireJson(
            RunSchedulerTests.Fixture(Path.Combine("checkpoint-lifecycle", "running.json")));
        store.Inner.Save(running);
        var graph = Approval(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });
        using var holder = await Hold(scheduler);

        var report = await graph.RecoverStaleAsync(
            store,
            new RecoverOptions
            {
                StaleAfter = TimeSpan.FromSeconds(1),
                MatchGraph = false,
                Run = new RunOptions { Scheduler = scheduler, TimeProvider = clock },
            });

        Assert.Empty(report.Recovered);
        Assert.Equal(
            $"it was not admitted: run not admitted: the scheduler's queue is full",
            Assert.Single(report.Skipped).Reason);

        // Listed, and nothing more: the thread was never loaded.
        Assert.Equal(1, store.Calls);
    }
}
