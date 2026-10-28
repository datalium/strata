using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Microsoft.Extensions.Time.Testing;
using static Datalium.Strata.Tests.StateGraphInterruptionTests;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/thread_leases.rs</c> (<c>design/32-Thread-Leases.md</c>, SG-12): running a
/// thread under a lease, through the public API only. Keep the two in step.
/// </summary>
public class ThreadLeaseTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);
    private static readonly DateTimeOffset Noon = new(2026, 10, 9, 12, 0, 0, TimeSpan.Zero);

    /// <summary>Every event of every run, in arrival order.</summary>
    private sealed class Collector : IRunObserver
    {
        private readonly ConcurrentQueue<RunEvent> _events = new();

        public void OnEvent(RunEvent runEvent) => _events.Enqueue(runEvent);

        public async Task<List<RunEvent>> After(int runs)
        {
            var deadline = DateTime.UtcNow + Patience;
            while (_events.Count(e => e is RunCompleted) < runs)
            {
                Assert.True(DateTime.UtcNow < deadline, "the runs' events");
                await Task.Delay(5);
            }

            return [.. _events];
        }
    }

    /// <summary>Forwards everything to <paramref name="inner"/>: what a store that wraps another must do to keep its leases.</summary>
    private class Forwarding(ICheckpointStore inner) : ICheckpointStore
    {
        public void Save(Checkpoint checkpoint) => inner.Save(checkpoint);

        public Checkpoint? Load(string threadId) => inner.Load(threadId);

        public ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
            inner.SaveAsync(checkpoint, cancellationToken);

        public ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
            inner.LoadAsync(threadId, cancellationToken);

        public ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
            inner.LoadLatestAsync(threadId, cancellationToken);

        public ValueTask<bool> SaveIfLatestAsync(
            Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
            inner.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken);

        public virtual ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
            string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default) =>
            inner.TryAcquireLeaseAsync(threadId, owner, ttl, cancellationToken);

        public virtual ValueTask<ThreadLease?> RenewLeaseAsync(
            ThreadLease lease, TimeSpan ttl, CancellationToken cancellationToken = default) =>
            inner.RenewLeaseAsync(lease, ttl, cancellationToken);

        public ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken cancellationToken = default) =>
            inner.ReleaseLeaseAsync(lease, cancellationToken);

        public ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken cancellationToken = default) =>
            inner.GetLeaseAsync(threadId, cancellationToken);

        public ValueTask<bool> SaveUnderLeaseAsync(
            Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default) =>
            inner.SaveUnderLeaseAsync(checkpoint, lease, expectedSequence, cancellationToken);
    }

    /// <summary>While armed, lets <c>worker-2</c> take the thread the moment a run has acquired it.</summary>
    private sealed class Usurper(InMemoryCheckpointStore inner, FakeTimeProvider clock) : Forwarding(inner)
    {
        public bool Armed { get; set; }

        public override async ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
            string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default)
        {
            var attempt = await inner.TryAcquireLeaseAsync(threadId, owner, ttl, cancellationToken);
            if (Armed && attempt.Acquired)
            {
                await TakeOver(inner, clock, threadId);
            }

            return attempt;
        }
    }

    /// <summary>Renewals throw while <see cref="Failing"/>, as against a store that can't be reached.</summary>
    private sealed class FlakyRenewals(InMemoryCheckpointStore inner) : Forwarding(inner)
    {
        private int _attempts;

        public bool Failing { get; set; }

        public int Attempts => Volatile.Read(ref _attempts);

        public override ValueTask<ThreadLease?> RenewLeaseAsync(
            ThreadLease lease, TimeSpan ttl, CancellationToken cancellationToken = default)
        {
            var failing = Failing;
            Interlocked.Increment(ref _attempts);
            return failing ? throw new InvalidOperationException("the store is unreachable") : inner.RenewLeaseAsync(lease, ttl, cancellationToken);
        }
    }

    /// <summary>Only <c>Save</c> and <c>Load</c>: the interface's defaults supply the rest, leases not among them.</summary>
    private sealed class PlainStore : ICheckpointStore
    {
        private readonly Dictionary<string, Checkpoint> _latest = [];

        public void Save(Checkpoint checkpoint) => _latest[checkpoint.ThreadId] = checkpoint;

        public Checkpoint? Load(string threadId) => _latest.GetValueOrDefault(threadId);
    }

    // --- Graphs ---

    /// <summary>
    /// <c>Prepare</c> (a boundary, which runs <paramref name="during"/> first) -> <c>Execute</c>, and how many
    /// times each ran.
    /// </summary>
    private static (CompiledGraph<WireTransferState> Graph, Func<(int Prepared, int Executed)> Ran) WireTransfer(
        Func<NodeContext, CancellationToken, Task>? during = null)
    {
        var prepared = 0;
        var executed = 0;
        var graph = new StateGraph<WireTransferState>()
            .AddNode("Prepare", async (WireTransferState s, NodeContext context, CancellationToken ct) =>
            {
                Interlocked.Increment(ref prepared);
                if (during is not null)
                {
                    await during(context, ct);
                }

                return s with { InvoiceGenerated = true };
            })
            .AddInterruptBoundary("Prepare")
            .AddNode("Execute", s =>
            {
                Interlocked.Increment(ref executed);
                return s with { IsCompleted = true };
            })
            .AddEdge("Prepare", "Execute")
            .Compile();
        return (graph, () => (Volatile.Read(ref prepared), Volatile.Read(ref executed)));
    }

    // --- Helpers ---

    private static RunOptions Leased(
        FakeTimeProvider clock, string owner = "worker-1", IRunObserver? observer = null) =>
        new() { Lease = new LeaseOptions(owner, Ttl), TimeProvider = clock, Observer = observer };

    /// <summary>The thread's lease runs out on the store's clock and <c>worker-2</c> takes it.</summary>
    private static async Task<ThreadLease> TakeOver(InMemoryCheckpointStore store, FakeTimeProvider storeClock, string threadId)
    {
        storeClock.Advance(Ttl + TimeSpan.FromMilliseconds(1));
        var attempt = await store.TryAcquireLeaseAsync(threadId, "worker-2", Ttl);
        Assert.True(attempt.Acquired, "the expired lease is free to take");
        return attempt.Lease!;
    }

    private static string Refused<T>(GraphExecutionResult<T> result) =>
        Assert.IsAssignableFrom<GraphExecutionResult<T>.Failed>(result).Message;

    private static GraphExecutionResult<T>.Cancelled LeaseLost<T>(GraphExecutionResult<T> result)
    {
        if (result is GraphExecutionResult<T>.Failed failed)
        {
            Assert.Fail($"expected a lost lease, got: {failed.Message}");
        }

        var cancelled = Assert.IsType<GraphExecutionResult<T>.Cancelled>(result);
        Assert.Equal(CancelReason.LeaseLost, cancelled.Reason);
        return cancelled;
    }

    private static string Text(DateTimeOffset time) =>
        time.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);

    private static async Task<int> LogLength(InMemoryCheckpointStore store, string threadId) =>
        (await store.GetLogAsync(threadId)).Entries.Count;

    private static async Task Until(Func<Task<bool>> condition, string what)
    {
        var deadline = DateTime.UtcNow + Patience;
        while (!await condition())
        {
            Assert.True(DateTime.UtcNow < deadline, what);
            await Task.Delay(5);
        }
    }

    // --- Holding ---

    [Fact]
    public async Task A_Run_Holds_Its_Lease_While_It_Runs_And_Releases_It_When_It_Freezes()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        ThreadLease? during = null;
        var (graph, _) = WireTransfer(async (context, _) => during = await store.GetLeaseAsync(context.ThreadId));
        var collector = new Collector();

        var result = await graph.RunAsync("t", InitialState(), store, Leased(clock, observer: collector));

        Assert.IsType<GraphExecutionResult<WireTransferState>.Interrupted>(result);
        Assert.Equal(new ThreadLease("t", "worker-1", 1, Noon + Ttl), during);
        Assert.Null(await store.GetLeaseAsync("t"));
        Assert.Equal(CheckpointStatus.Interrupted, store.Load("t")?.Status);

        // The lease is the first thing the run takes, before any node starts.
        var events = await collector.After(1);
        Assert.Equal(
            ["RunStarted", "LeaseAcquired", "NodeStarted", "NodeCompleted", "CheckpointSaved", "Interrupted", "RunCompleted"],
            events.Select(e => e.GetType().Name));
        var acquired = events.OfType<LeaseAcquired>().Single();
        Assert.Equal(("worker-1", 1L, Noon + Ttl), (acquired.Owner, acquired.Token, acquired.ExpiresAt));
    }

    [Fact]
    public async Task A_Held_Thread_Refuses_A_Run_And_Runs_Nothing()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var (graph, ran) = WireTransfer();
        var holder = (await store.TryAcquireLeaseAsync("t", "worker-2", Ttl)).Lease!;

        var result = await graph.RunAsync("t", InitialState(), store, Leased(clock));

        Assert.Equal($"thread 't' is held by 'worker-2' until {Text(Noon + Ttl)}", Refused(result));
        Assert.Equal((0, 0), ran());
        Assert.Equal(0, await LogLength(store, "t"));
        Assert.Equal(holder, await store.GetLeaseAsync("t"));

        // The same owner name is refused too: an owner names a process.
        Assert.StartsWith(
            "thread 't' is held by 'worker-2' until ",
            Refused(await graph.RunAsync("t", InitialState(), store, Leased(clock, "worker-2"))),
            StringComparison.Ordinal);

        // A streaming run is refused the same way, with no chunks.
        var (reader, completion) = graph.RunStreaming("t", InitialState(), store, Leased(clock));
        Assert.StartsWith("thread 't' is held by 'worker-2' until ", Refused(await completion), StringComparison.Ordinal);
        Assert.False(await reader.WaitToReadAsync());
        Assert.Equal((0, 0), ran());
    }

    [Fact]
    public async Task A_Held_Thread_Refuses_A_Resume_A_Signal_And_An_End_And_Stays_Resumable()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var (graph, ran) = WireTransfer();
        var waiting = new StateGraph<int>()
            .AddWaitForEvent("Deploying", "deployment.finished")
            .AddNode("After", n => n + 1)
            .AddEdge("Deploying", "After")
            .Compile();
        await graph.RunAsync("t", InitialState(), store, Leased(clock));
        await waiting.RunAsync("w", 1, store, Leased(clock));
        var onT = (await store.TryAcquireLeaseAsync("t", "worker-2", Ttl)).Lease!;
        var onW = (await store.TryAcquireLeaseAsync("w", "worker-2", Ttl)).Lease!;
        const string Held = "is held by 'worker-2' until ";

        Assert.Contains(Held, Refused(await graph.ResumeAsync("t", HumanSignal.Approve("alice"), store, Leased(clock))), StringComparison.Ordinal);
        Assert.Contains(Held, Refused(await graph.EndAsync("t", ThreadEnd.By("oncall"), store, Leased(clock))), StringComparison.Ordinal);
        Assert.Contains(
            Held,
            Refused(await waiting.SignalAsync("w", new EventSignal("deployment.finished"), store, Leased(clock))),
            StringComparison.Ordinal);

        // Nothing was written, and nothing ran.
        Assert.Equal((1, 1), (await LogLength(store, "t"), await LogLength(store, "w")));
        Assert.Null(store.Load("t")?.ResumedAt);
        Assert.Null(store.Load("w")?.ResumedAt);
        Assert.Equal((1, 0), ran());

        // Once the holder lets go, both go on.
        await store.ReleaseLeaseAsync(onT);
        await store.ReleaseLeaseAsync(onW);
        Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(
            await graph.ResumeAsync("t", HumanSignal.Approve("alice"), store, Leased(clock)));
        Assert.Equal(
            2,
            Assert.IsType<GraphExecutionResult<int>.Completed>(
                await waiting.SignalAsync("w", new EventSignal("deployment.finished"), store, Leased(clock))).State);
    }

    [Fact]
    public async Task A_Refused_Resume_Never_Takes_The_Lease()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var (graph, _) = WireTransfer();
        await graph.RunAsync("t", InitialState(), store, Leased(clock));
        var collector = new Collector();

        // An override that doesn't fit the state, a thread that doesn't exist, and an end of one.
        var misfit = HumanSignal.Approve("alice").WithOverride("amount", "not a number");
        Refused(await graph.ResumeAsync("t", misfit, store, Leased(clock, observer: collector)));
        Refused(await graph.ResumeAsync("missing", HumanSignal.Approve("alice"), store, Leased(clock, observer: collector)));
        Refused(await graph.EndAsync("missing", ThreadEnd.By("oncall"), store, Leased(clock, observer: collector)));

        Assert.DoesNotContain(await collector.After(3), e => e is LeaseAcquired);
        // Only the first run ever acquired: the thread's next token is 2, and "missing" never had one.
        Assert.Equal(2, (await store.TryAcquireLeaseAsync("t", "probe", Ttl)).Lease?.Token);
        Assert.Equal(1, (await store.TryAcquireLeaseAsync("missing", "probe", Ttl)).Lease?.Token);
    }

    [Fact]
    public async Task A_Resume_And_An_End_Run_Under_The_Lease_And_Release_It()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var (graph, ran) = WireTransfer();
        await graph.RunAsync("resumed", InitialState(), store, Leased(clock));
        await graph.RunAsync("ended", InitialState(), store, Leased(clock));
        var collector = new Collector();

        Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(
            await graph.ResumeAsync("resumed", HumanSignal.Approve("alice"), store, Leased(clock, observer: collector)));
        var ended = Assert.IsType<GraphExecutionResult<WireTransferState>.Cancelled>(
            await graph.EndAsync("ended", ThreadEnd.By("oncall"), store, Leased(clock, observer: collector)));

        Assert.Equal(CancelReason.Ended, ended.Reason);
        Assert.NotNull(store.Load("resumed")?.ResumedAt);
        Assert.Equal(CheckpointStatus.Ended, store.Load("ended")?.Status);
        Assert.Equal((2, 1), ran());
        Assert.Null(await store.GetLeaseAsync("resumed"));
        Assert.Null(await store.GetLeaseAsync("ended"));

        // Each took the thread's second lease, before its first save.
        var events = await collector.After(2);
        Assert.Equal([2L, 2L], events.OfType<LeaseAcquired>().Select(e => e.Token));
        foreach (var run in events.GroupBy(e => e.RunId))
        {
            var names = run.Select(e => e.GetType().Name).ToList();
            Assert.True(names.IndexOf("LeaseAcquired") < names.IndexOf("CheckpointSaved"), string.Join(", ", names));
        }

        // An ended thread, ended again, writes nothing, so it takes no lease.
        await graph.EndAsync("ended", store);
        Assert.Equal(3, (await store.TryAcquireLeaseAsync("ended", "probe", Ttl)).Lease?.Token);
    }

    // --- Renewing ---

    [Fact]
    public async Task A_Run_Longer_Than_Its_Ttl_Keeps_Its_Lease_By_Renewing()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (graph, _) = WireTransfer(async (_, _) =>
        {
            started.SetResult();
            await release.Task;
        });

        var run = graph.RunAsync("t", InitialState(), store, Leased(clock));
        await started.Task.WaitAsync(Patience);

        // More than two ttls go by, a third of one at a time: each third is a renewal.
        for (var third = 1; third <= 7; third++)
        {
            var before = (await store.GetLeaseAsync("t"))!.ExpiresAt;
            clock.Advance(Ttl / 3);
            await Until(async () => (await store.GetLeaseAsync("t"))?.ExpiresAt > before, $"renewal {third}");
        }

        var held = await store.GetLeaseAsync("t");
        Assert.Equal(("worker-1", 1L), (held?.Owner, held?.Token));
        Assert.False((await store.TryAcquireLeaseAsync("t", "worker-2", Ttl)).Acquired);

        release.SetResult();
        Assert.IsType<GraphExecutionResult<WireTransferState>.Interrupted>(await run.WaitAsync(Patience));
        Assert.Null(await store.GetLeaseAsync("t"));

        // Nothing renews a lease the run has given up.
        clock.Advance(Ttl);
        await Task.Delay(50);
        Assert.Null(await store.GetLeaseAsync("t"));
    }

    [Fact]
    public async Task Renewals_Failing_For_A_Whole_Ttl_Lose_The_Lease()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new FlakyRenewals(new InMemoryCheckpointStore(clock));
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (graph, _) = WireTransfer(async (_, ct) =>
        {
            started.SetResult();
            await Task.Delay(Timeout.Infinite, ct);
        });

        var run = graph.RunAsync("t", InitialState(), store, Leased(clock));
        await started.Task.WaitAsync(Patience);
        store.Failing = true;

        // Two failed renewals aren't a loss: the run goes on.
        for (var third = 1; third <= 2; third++)
        {
            clock.Advance(Ttl / 3);
            await Until(() => Task.FromResult(store.Attempts == third), $"renewal {third}");
        }

        await Task.Delay(50);
        Assert.False(run.IsCompleted);

        // The third makes a whole ttl without one: the run can't show it holds the thread any more.
        clock.Advance(Ttl / 3);
        var lost = LeaseLost(await run.WaitAsync(Patience));
        Assert.Equal("Prepare", lost.Node);
        Assert.Null(store.Load("t"));
    }

    [Fact]
    public async Task Renewals_Failing_For_Less_Than_A_Ttl_Lose_Nothing()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new FlakyRenewals(new InMemoryCheckpointStore(clock));
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (graph, _) = WireTransfer(async (_, _) =>
        {
            started.SetResult();
            await release.Task;
        });

        var run = graph.RunAsync("t", InitialState(), store, Leased(clock));
        await started.Task.WaitAsync(Patience);
        store.Failing = true;
        for (var third = 1; third <= 2; third++)
        {
            clock.Advance(Ttl / 3);
            await Until(() => Task.FromResult(store.Attempts == third), $"renewal {third}");
        }

        // The store is back: the next renewal succeeds, and the count of time without one starts over.
        store.Failing = false;
        for (var third = 3; third <= 6; third++)
        {
            clock.Advance(Ttl / 3);
            await Until(() => Task.FromResult(store.Attempts == third), $"renewal {third}");
        }

        release.SetResult();
        Assert.IsType<GraphExecutionResult<WireTransferState>.Interrupted>(await run.WaitAsync(Patience));
        Assert.Equal(CheckpointStatus.Interrupted, store.Load("t")?.Status);
    }

    // --- Losing ---

    [Fact]
    public async Task A_Lease_Taken_Over_Mid_Run_Cancels_The_Run_And_Writes_Nothing()
    {
        var storeClock = new FakeTimeProvider(Noon);
        var runClock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(storeClock);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tokenFired = false;
        var (graph, ran) = WireTransfer(async (_, ct) =>
        {
            started.SetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                tokenFired = true;
                throw;
            }
        });
        var collector = new Collector();

        var run = graph.RunAsync("t", InitialState(), store, Leased(runClock, observer: collector));
        await started.Task.WaitAsync(Patience);

        // The worker stalls past its ttl, another takes the thread, and the worker's next renewal finds out.
        var usurper = await TakeOver(store, storeClock, "t");
        runClock.Advance(Ttl / 3);
        var lost = LeaseLost(await run.WaitAsync(Patience));

        Assert.Equal(("t", "Prepare"), (lost.ThreadId, lost.Node));
        Assert.Equal(["Prepare"], lost.StoppedAt);
        Assert.Equal(InitialState(), lost.State);

        // The engine doesn't wait for a handler it has given up on, so the handler sees its token a moment
        // after the run has returned.
        await Until(() => Task.FromResult(Volatile.Read(ref tokenFired)), "the handler's token fired");
        Assert.Equal((1, 0), ran());

        // Not even a cancelled checkpoint: the thread's history is the new owner's, who still holds it.
        Assert.Equal(0, await LogLength(store, "t"));
        Assert.Equal(usurper, await store.GetLeaseAsync("t"));

        var events = await collector.After(1);
        var lease = events.OfType<LeaseLost>().Single();
        Assert.Equal(("worker-1", 1L), (lease.Owner, lease.Token));
        Assert.DoesNotContain(events, e => e is CheckpointSaved);
        var completed = Assert.IsType<RunCompleted>(events[^1]);
        Assert.Equal((RunOutcome.Cancelled, CancelReason.LeaseLost), (completed.Outcome, completed.CancelReason));
        Assert.True(events.IndexOf(lease) < events.IndexOf(completed));
    }

    [Fact]
    public async Task A_Fenced_Interrupted_Checkpoint_Becomes_A_Lost_Lease()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var (graph, _) = WireTransfer((context, _) => TakeOver(store, clock, context.ThreadId));

        // The run's own clock never moves, so no renewal tells it: its save is what is refused.
        var lost = LeaseLost(await graph.RunAsync("t", InitialState(), store, Leased(new FakeTimeProvider(Noon))));

        Assert.Equal("Prepare", lost.Node);
        Assert.Equal(["Prepare"], lost.StoppedAt);
        Assert.True(lost.State.InvoiceGenerated);
        Assert.Equal(0, await LogLength(store, "t"));
        Assert.Equal("worker-2", (await store.GetLeaseAsync("t"))?.Owner);
    }

    [Fact]
    public async Task A_Fenced_Failed_Checkpoint_Becomes_A_Lost_Lease()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var graph = new StateGraph<WireTransferState>()
            .AddFallibleNode("Charge", async (WireTransferState _, NodeContext context, CancellationToken _) =>
            {
                await TakeOver(store, clock, context.ThreadId);
                throw NodeException.Fatal("gateway refused");
            })
            .Compile();

        // Not NodeFailed: the failure isn't recorded, so it isn't the thread's outcome.
        var lost = LeaseLost(await graph.RunAsync("t", InitialState(), store, Leased(new FakeTimeProvider(Noon))));

        Assert.Equal("Charge", lost.Node);
        Assert.Equal(InitialState(), lost.State);
        Assert.Equal(0, await LogLength(store, "t"));
    }

    [Fact]
    public async Task A_Fenced_Cancelled_Checkpoint_Becomes_A_Lost_Lease()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        using var cancel = new CancellationTokenSource();
        var (graph, _) = WireTransfer(async (context, ct) =>
        {
            await TakeOver(store, clock, context.ThreadId);
            await cancel.CancelAsync();
            await Task.Delay(Timeout.Infinite, ct);
        });

        // The caller cancelled, but the run finds its lease gone when it goes to say so.
        var lost = LeaseLost(
            await graph.RunAsync("t", InitialState(), store, Leased(new FakeTimeProvider(Noon)), cancel.Token));

        Assert.Equal("Prepare", lost.Node);
        Assert.Equal(0, await LogLength(store, "t"));
    }

    [Fact]
    public async Task A_Fenced_Resume_Stamp_Runs_Nothing_And_A_Fenced_End_Ends_Nothing()
    {
        var clock = new FakeTimeProvider(Noon);
        var inner = new InMemoryCheckpointStore(clock);
        var store = new Usurper(inner, clock);
        var (graph, ran) = WireTransfer();
        await graph.RunAsync("t", InitialState(), store, Leased(clock));
        store.Armed = true;

        var resumed = LeaseLost(await graph.ResumeAsync("t", HumanSignal.Approve("alice"), store, Leased(clock)));

        Assert.Equal("Execute", resumed.Node);
        Assert.Equal((1, 0), ran());
        Assert.Null(inner.Load("t")?.ResumedAt);
        Assert.Equal(1, await LogLength(inner, "t"));

        clock.Advance(Ttl + TimeSpan.FromMilliseconds(1));
        var ended = LeaseLost(await graph.EndAsync("t", ThreadEnd.By("oncall"), store, Leased(clock)));

        Assert.Equal("Prepare", ended.Node);
        Assert.Equal(CheckpointStatus.Interrupted, inner.Load("t")?.Status);
        Assert.Equal(1, await LogLength(inner, "t"));

        // The thread is still resumable by whoever holds it next.
        store.Armed = false;
        clock.Advance(Ttl + TimeSpan.FromMilliseconds(1));
        Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(
            await graph.ResumeAsync("t", HumanSignal.Approve("alice"), store, Leased(clock)));
    }

    [Fact]
    public async Task A_Lease_Lost_Inside_A_Subgraph_Stops_The_Child()
    {
        var storeClock = new FakeTimeProvider(Noon);
        var runClock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(storeClock);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var after = 0;
        var child = new StateGraph<int>()
            .AddNode("Charge", async (int n, NodeContext _, CancellationToken ct) =>
            {
                started.SetResult();
                await Task.Delay(Timeout.Infinite, ct);
                return n;
            })
            .AddNode("Settle", n =>
            {
                Interlocked.Increment(ref after);
                return n;
            })
            .AddEdge("Charge", "Settle")
            .Compile();
        var graph = new StateGraph<int>()
            .AddSubgraph("Work", child, n => n, (_, n) => n)
            .AddNode("Finish", n =>
            {
                Interlocked.Increment(ref after);
                return n;
            })
            .AddEdge("Work", "Finish")
            .Compile();

        var run = graph.RunAsync("t", 7, store, Leased(runClock));
        await started.Task.WaitAsync(Patience);
        await TakeOver(store, storeClock, "t");
        runClock.Advance(Ttl / 3);
        var lost = LeaseLost(await run.WaitAsync(Patience));

        // One lease covers every level of the run.
        Assert.Equal("Work", lost.Node);
        Assert.Equal(["Work", "Charge"], lost.StoppedAt);
        Assert.Equal(7, lost.State);
        Assert.Equal(0, Volatile.Read(ref after));
        Assert.Equal(0, await LogLength(store, "t"));
    }

    // --- Options and stores ---

    [Fact]
    public async Task A_Store_Without_Leases_Refuses_A_Run_With_One()
    {
        var (graph, ran) = WireTransfer();
        var store = new PlainStore();

        await Assert.ThrowsAsync<NotSupportedException>(() =>
            graph.RunAsync("t", InitialState(), store, Leased(new FakeTimeProvider(Noon))));

        Assert.Equal((0, 0), ran());
        Assert.Null(store.Load("t"));

        // Without a lease it runs as it always did.
        Assert.IsType<GraphExecutionResult<WireTransferState>.Interrupted>(await graph.RunAsync("t", InitialState(), store));
    }

    [Fact]
    public async Task Bad_Lease_Options_Are_Refused_Before_The_Run_Starts()
    {
        var (graph, ran) = WireTransfer();
        var store = new InMemoryCheckpointStore();

        foreach (var lease in new[] { new LeaseOptions(" ", Ttl), new LeaseOptions("worker-1", TimeSpan.Zero) })
        {
            var options = new RunOptions { Lease = lease };
            await Assert.ThrowsAnyAsync<ArgumentException>(() => graph.RunAsync("t", InitialState(), store, options));
            await Assert.ThrowsAnyAsync<ArgumentException>(() => graph.ResumeAsync("t", HumanSignal.Approve("a"), store, options));
            await Assert.ThrowsAnyAsync<ArgumentException>(() => graph.EndAsync("t", ThreadEnd.By("a"), store, options));
        }

        Assert.Equal((0, 0), ran());
    }

    // --- The wake driver ---

    [Fact]
    public async Task The_Wake_Driver_Skips_A_Held_Thread_And_Wakes_It_Once_Released()
    {
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var graph = new StateGraph<int>()
            .AddTimer("Window", TimeSpan.FromMinutes(1))
            .AddNode("After", n => n + 1)
            .AddEdge("Window", "After")
            .Compile();
        await graph.RunAsync("t", 1, store, Leased(clock));
        clock.Advance(TimeSpan.FromMinutes(2));
        var holder = (await store.TryAcquireLeaseAsync("t", "worker-2", Ttl)).Lease!;
        var options = new WakeOptions { Run = Leased(clock) };

        var skipped = await graph.WakeDueAsync(store, options);

        Assert.Empty(skipped.Woken);
        Assert.Equal(
            [new SkippedThread("t", $"it is held by 'worker-2' until {Text(holder.ExpiresAt)}")], skipped.Skipped);
        Assert.Null(store.Load("t")?.ResumedAt);

        await store.ReleaseLeaseAsync(holder);
        var woken = await graph.WakeDueAsync(store, options);

        Assert.Empty(woken.Skipped);
        Assert.Equal(2, Assert.IsType<GraphExecutionResult<int>.Completed>(Assert.Single(woken.Woken).Result).State);
        Assert.Null(await store.GetLeaseAsync("t"));
    }

    // --- Telemetry ---

    [Fact]
    public async Task The_Run_Span_Carries_The_Lease_And_The_Counters_Count_It()
    {
        var spans = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == StrataTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        var counted = new ConcurrentBag<string>();
        using var meter = new MeterListener();
        meter.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == StrataTelemetry.Name && instrument.Name.StartsWith("strata.lease.", StringComparison.Ordinal))
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        meter.SetMeasurementEventCallback<long>((instrument, _, tags, _) =>
        {
            var outcome = string.Empty;
            foreach (var tag in tags)
            {
                if (tag.Key == "strata.lease.outcome")
                {
                    outcome = $" {tag.Value}";
                }
            }

            counted.Add(instrument.Name + outcome);
        });
        meter.Start();
        var storeClock = new FakeTimeProvider(Noon);
        var runClock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(storeClock);
        var kept = $"lease-span-kept-{Guid.NewGuid():N}";
        var lostThread = $"lease-span-lost-{Guid.NewGuid():N}";
        var (graph, _) = WireTransfer();

        // One run keeps its lease, one is refused it, and one loses it.
        await graph.RunAsync(kept, InitialState(), store, Leased(runClock));
        await store.TryAcquireLeaseAsync(kept, "worker-2", Ttl);
        Refused(await graph.ResumeAsync(kept, HumanSignal.Approve("alice"), store, Leased(runClock)));
        var (fenced, _) = WireTransfer((context, _) => TakeOver(store, storeClock, context.ThreadId));
        LeaseLost(await fenced.RunAsync(lostThread, InitialState(), store, Leased(runClock)));

        Activity Run(string threadId, string name) =>
            spans.Single(s => s.DisplayName == name && (string?)s.GetTagItem("strata.thread_id") == threadId);
        var keptRun = Run(kept, "strata.run");
        Assert.Equal("worker-1", keptRun.GetTagItem("strata.lease.owner"));
        Assert.Equal(1L, keptRun.GetTagItem("strata.lease.token"));
        Assert.Null(Run(kept, "strata.resume").GetTagItem("strata.lease.owner"));
        var lostRun = Run(lostThread, "strata.run");
        Assert.Equal("lease_lost", lostRun.GetTagItem("strata.cancel_reason"));
        Assert.Contains(lostRun.Events, e => e.Name == "lease_lost");
        Assert.NotEqual(ActivityStatusCode.Error, lostRun.Status);

        Assert.Contains("strata.lease.acquisitions acquired", counted);
        Assert.Contains("strata.lease.acquisitions held", counted);
        Assert.Contains("strata.lease.losses", counted);
    }
}
