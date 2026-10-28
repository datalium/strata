using System.Text.Json.Nodes;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// The assertions every <see cref="ICheckpointStore"/> and <see cref="IHistoryStore"/> must satisfy,
/// written once as plain methods so the exact same code runs against every backend (see
/// <see cref="StoreContractTests"/>) -- the xUnit twin of <c>rust/strata-memory/tests/checkpoint_store_contract.rs</c>
/// and <c>history_store_contract.rs</c>. Nothing here knows which backend it is running against, which
/// is what "the store abstraction is genuinely pluggable" means in a test.
/// </summary>
internal static class StoreContracts
{
    private static Checkpoint Checkpoint(string threadId, CheckpointStatus status, string marker) => new()
    {
        SchemaVersion = "1.0.0",
        ThreadId = threadId,
        Status = status,
        SuspendedNode = "PrepareWireTransfer",
        NextNode = "ExecuteWireTransfer",
        FrozenState = JsonNode.Parse(
            $$"""
            {
                "marker": "{{marker}}",
                "amount": 4200.5,
                "count": 9007199254740993,
                "flags": [true, false, null],
                "nested": { "text": "say \"hi\" \\ é 日本語 🚀\nnewline", "empty": "" },
                "approved_by": null
            }
            """),
        Metadata = null,
        CreatedAt = "2026-09-16T00:00:00Z",
        ResumedAt = null,
    };

    /// <summary>An interrupted checkpoint for <paramref name="threadId"/> written at <paramref name="createdAt"/>.</summary>
    internal static Checkpoint Checkpoint(string threadId, string createdAt) =>
        Checkpoint(threadId, CheckpointStatus.Interrupted, "head") with { CreatedAt = createdAt };

    internal static void AssertSame(Checkpoint expected, Checkpoint? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.SchemaVersion, actual.SchemaVersion);
        Assert.Equal(expected.ThreadId, actual.ThreadId);
        Assert.Equal(expected.Status, actual.Status);
        Assert.Equal(expected.SuspendedNode, actual.SuspendedNode);
        Assert.Equal(expected.NextNode, actual.NextNode);
        Assert.True(
            JsonNode.DeepEquals(expected.FrozenState, actual.FrozenState),
            $"frozen_state differs:\n expected {expected.FrozenState?.ToJsonString()}\n actual   {actual.FrozenState?.ToJsonString()}");
        Assert.Equal(expected.Metadata, actual.Metadata);
        Assert.Equal(expected.CreatedAt, actual.CreatedAt);
        Assert.Equal(expected.ResumedAt, actual.ResumedAt);

        // A decision comes back whole, typed overrides included (design/21-Typed-Human-Signals.md).
        Assert.Equal(expected.Decision is null, actual.Decision is null);
        if (expected.Decision is not null)
        {
            Assert.Equal(expected.Decision.ToWireJson().ToJsonString(), actual.Decision!.ToWireJson().ToJsonString());
        }

        // A wait comes back whole (design/29-Event-Waits-And-Timers.md).
        Assert.Equal(expected.Wait, actual.Wait);

        // A child nests to any depth, and each level must come back whole.
        Assert.Equal(expected.Nested is null, actual.Nested is null);
        if (expected.Nested is not null)
        {
            AssertSame(expected.Nested, actual.Nested);
        }
    }

    public static void CheckpointStoreContract(ICheckpointStore store)
    {
        // Unknown thread -> null, not an error.
        Assert.Null(store.Load("missing"));

        // Save then load returns exactly what was saved.
        var first = Checkpoint("t1", CheckpointStatus.Interrupted, "first");
        store.Save(first);
        AssertSame(first, store.Load("t1"));

        // A second save for the same thread becomes the one Load returns -- including the fields a
        // resume sets (resumed_at, metadata).
        var second = first with
        {
            ResumedAt = "2026-09-16T01:00:00Z",
            Metadata = new Dictionary<string, string> { ["approved_by"] = "admin_01", ["reviewer_token"] = "token-abc" },
        };
        store.Save(second);
        AssertSame(second, store.Load("t1"));

        // Saving again never resurrects an older checkpoint.
        var third = Checkpoint("t1", CheckpointStatus.Completed, "third");
        store.Save(third);
        AssertSame(third, store.Load("t1"));

        // Threads are isolated from each other, and every status round-trips.
        var failed = Checkpoint("t2", CheckpointStatus.Failed, "other thread");
        store.Save(failed);
        AssertSame(failed, store.Load("t2"));
        AssertSame(third, store.Load("t1"));

        // A checkpoint with no nodes at all (a completed thread) round-trips too.
        var bare = Checkpoint("t3", CheckpointStatus.Completed, "bare") with { SuspendedNode = null, NextNode = null };
        store.Save(bare);
        AssertSame(bare, store.Load("t3"));

        // A checkpoint nesting a child's -- which nests a grandchild's -- round-trips whole, with each
        // level's own fields intact (design/16-Nested-Interrupts.md).
        var grandchild = Checkpoint("Inner", CheckpointStatus.Interrupted, "grandchild") with
        {
            Metadata = new Dictionary<string, string> { ["note"] = "deep" },
        };
        var child = Checkpoint("Work", CheckpointStatus.Interrupted, "child") with
        {
            SchemaVersion = CheckpointSchema.NestedVersion,
            Nested = grandchild,
        };
        var parent = Checkpoint("t4", CheckpointStatus.Interrupted, "parent") with
        {
            SchemaVersion = CheckpointSchema.NestedVersion,
            Nested = child,
        };
        store.Save(parent);
        AssertSame(parent, store.Load("t4"));

        // A later save without one doesn't inherit the earlier row's child.
        var resumed = Checkpoint("t4", CheckpointStatus.Completed, "resumed") with { ResumedAt = "2026-09-16T01:00:00Z" };
        store.Save(resumed);
        AssertSame(resumed, store.Load("t4"));

        // A failed parent keeps the child's failed checkpoint the same way
        // (design/16-Nested-Interrupts.md, "Resuming a failed thread").
        var failedChild = Checkpoint("Work", CheckpointStatus.Failed, "failed child") with
        {
            Metadata = new Dictionary<string, string> { ["error"] = "mailbox closed", ["error_kind"] = "fatal", ["attempts"] = "1" },
        };
        var failedParent = Checkpoint("t5", CheckpointStatus.Failed, "failed parent") with
        {
            SchemaVersion = CheckpointSchema.NestedVersion,
            Nested = failedChild,
        };
        store.Save(failedParent);
        AssertSame(failedParent, store.Load("t5"));

        // A cancelled checkpoint round-trips, flat and nesting a cancelled child; so does a failed
        // parent nesting a cancelled child, stopped by the parent node's timeout
        // (design/17-Async-Execution.md).
        var cancelled = Checkpoint("t6", CheckpointStatus.Cancelled, "cancelled") with
        {
            SchemaVersion = CheckpointSchema.CancelledVersion,
            Metadata = new Dictionary<string, string> { ["cancel_reason"] = "requested" },
        };
        store.Save(cancelled);
        AssertSame(cancelled, store.Load("t6"));

        var cancelledParent = cancelled with
        {
            ThreadId = "t7",
            Nested = cancelled with { ThreadId = "Work" },
        };
        store.Save(cancelledParent);
        AssertSame(cancelledParent, store.Load("t7"));

        var timedOutParent = failedParent with
        {
            ThreadId = "t8",
            SchemaVersion = CheckpointSchema.CancelledVersion,
            Nested = cancelled with
            {
                ThreadId = "Work",
                Metadata = new Dictionary<string, string> { ["cancel_reason"] = "timeout" },
            },
        };
        store.Save(timedOutParent);
        AssertSame(timedOutParent, store.Load("t8"));

        // A decision round-trips, with typed overrides, at the top level and nested inside a failed
        // parent; a later save without one doesn't inherit it (design/21-Typed-Human-Signals.md).
        var decision = HumanDecision.FromWireJson(JsonNode.Parse("""
            {"boundary":"Approve","is_approved":false,"reviewer":"bob@acme.io",
             "overrides":{"plan":{"steps":["drain"]},"replicas":3,"dry_run":true,"note":null,"reason":"no"},
             "decided_at":"2026-10-04T00:00:00Z"}
            """)!);
        var decided = Checkpoint("t9", CheckpointStatus.Failed, "decided") with
        {
            SchemaVersion = CheckpointSchema.DecisionVersion,
            Decision = decision,
        };
        store.Save(decided);
        AssertSame(decided, store.Load("t9"));

        var decidedParent = failedParent with
        {
            ThreadId = "t10",
            SchemaVersion = CheckpointSchema.DecisionVersion,
            Nested = failedChild with { SchemaVersion = CheckpointSchema.DecisionVersion, Decision = decision },
        };
        store.Save(decidedParent);
        AssertSame(decidedParent, store.Load("t10"));

        var undecided = Checkpoint("t9", CheckpointStatus.Interrupted, "undecided");
        store.Save(undecided);
        AssertSame(undecided, store.Load("t9"));
    }

    /// <summary>
    /// The human-in-the-loop lifecycle end to end against <paramref name="store"/>: the graph freezes, a
    /// <i>different</i> graph instance (standing in for another process) resumes it from the store, and a
    /// second resume is refused.
    /// </summary>
    public static async Task FreezeResumeContractAsync(ICheckpointStore store)
    {
        var frozen = await StateGraphInterruptionTests.WireTransferGraph()
            .RunAsync("wire-1", StateGraphInterruptionTests.InitialState(), store);
        Assert.IsType<GraphExecutionResult<StateGraphInterruptionTests.WireTransferState>.Interrupted>(frozen);

        var persisted = await store.LoadAsync("wire-1");
        Assert.Equal(CheckpointStatus.Interrupted, persisted?.Status);
        Assert.Equal("ExecuteWireTransfer", persisted?.NextNode);

        var signal = new HumanSignal(
            IsApproved: true,
            ReviewerToken: "token-abc",
            FieldOverrides: new Dictionary<string, string> { ["approved_by"] = "admin_01" });
        var resumed = await StateGraphInterruptionTests.WireTransferGraph().ResumeAsync("wire-1", signal, store);

        var completed = Assert.IsType<GraphExecutionResult<StateGraphInterruptionTests.WireTransferState>.Completed>(resumed);
        Assert.Equal(
            new StateGraphInterruptionTests.WireTransferState(
                InvoiceGenerated: true, Amount: 4200.5, Currency: "USD", ApprovedBy: "admin_01", IsCompleted: true),
            completed.State);

        // The store now holds the resumed checkpoint, and resuming it again is refused rather than replayed.
        Assert.NotNull((await store.LoadAsync("wire-1"))?.ResumedAt);
        var again = await StateGraphInterruptionTests.WireTransferGraph().ResumeAsync("wire-1", signal, store);
        Assert.IsNotType<GraphExecutionResult<StateGraphInterruptionTests.WireTransferState>.Completed>(again);
    }

    /// <summary>
    /// The conditional save (<c>design/28-Ending-A-Thread.md</c>, "Races"): <c>LoadLatestAsync</c> reports the
    /// sequence the log has, <c>SaveIfLatestAsync</c> appends only on that sequence and writes nothing otherwise,
    /// and a null sequence saves anyway.
    /// </summary>
    public static async Task ConditionalSaveContractAsync(ICheckpointStore store)
    {
        Assert.Null(await store.LoadLatestAsync("cas"));

        var first = Checkpoint("cas", CheckpointStatus.Interrupted, "first");
        await store.SaveAsync(first);
        var latest = await store.LoadLatestAsync("cas");
        AssertSame(first, latest?.Checkpoint);
        Assert.Equal(0, latest?.Sequence);

        var stamp = first with { ResumedAt = "2026-09-16T01:00:00Z" };
        Assert.True(await store.SaveIfLatestAsync(stamp, 0));
        latest = await store.LoadLatestAsync("cas");
        AssertSame(stamp, latest?.Checkpoint);
        Assert.Equal(1, latest?.Sequence);

        // A stale sequence loses, and leaves the log as it was.
        var late = Checkpoint("cas", CheckpointStatus.Failed, "late");
        Assert.False(await store.SaveIfLatestAsync(late, 0));
        AssertSame(stamp, await store.LoadAsync("cas"));

        // No sequence: an unconditional save.
        Assert.True(await store.SaveIfLatestAsync(late, null));
        latest = await store.LoadLatestAsync("cas");
        AssertSame(late, latest?.Checkpoint);
        Assert.Equal(2, latest?.Sequence);
    }

    /// <summary>
    /// Ending a frozen thread against <paramref name="store"/> (<c>design/28-Ending-A-Thread.md</c>): the end
    /// writes one <c>ended</c> checkpoint, a resume is refused, and a second end returns the same answer
    /// without writing.
    /// </summary>
    public static async Task FreezeEndContractAsync(ICheckpointStore store)
    {
        var graph = StateGraphInterruptionTests.WireTransferGraph();
        Assert.IsType<GraphExecutionResult<StateGraphInterruptionTests.WireTransferState>.Interrupted>(
            await graph.RunAsync("wire-end", StateGraphInterruptionTests.InitialState(), store));

        for (var i = 0; i < 2; i++)
        {
            var ended = Assert.IsType<GraphExecutionResult<StateGraphInterruptionTests.WireTransferState>.Cancelled>(
                await graph.EndAsync("wire-end", ThreadEnd.By("oncall"), store));
            Assert.Equal(CancelReason.Ended, ended.Reason);
            Assert.Equal("PrepareWireTransfer", ended.Node);
            Assert.True(ended.State.InvoiceGenerated);
        }

        var checkpoint = await store.LoadAsync("wire-end");
        Assert.Equal(CheckpointStatus.Ended, checkpoint?.Status);
        Assert.Equal(CheckpointSchema.EndedVersion, checkpoint?.SchemaVersion);
        Assert.Null(checkpoint?.NextNode);
        Assert.Equal("oncall", checkpoint?.Metadata?[ThreadEnd.EndedByKey]);

        Assert.IsAssignableFrom<GraphExecutionResult<StateGraphInterruptionTests.WireTransferState>.Failed>(
            await graph.ResumeAsync("wire-end", HumanSignal.Approve("alice"), store));
        AssertSame(checkpoint!, await store.LoadAsync("wire-end"));
    }

    /// <summary>
    /// Waits against <paramref name="store"/> (<c>design/29-Event-Waits-And-Timers.md</c>): the waiting
    /// lifecycle fixtures round-trip, a timer is listed as due by the store itself and woken once, and an
    /// event wait is signalled.
    /// </summary>
    public static async Task FreezeWakeContractAsync<TStore>(TStore store)
        where TStore : ICheckpointStore, ICheckpointQuery
    {
        foreach (var name in new[] { "waiting-event.json", "waiting-approval.json", "nested-timer.json" })
        {
            var waiting = Interruption.Checkpoint.FromWireJson(
                CheckpointQueryContractTests.Fixture(Path.Combine("checkpoint-lifecycle", name)));
            await store.SaveAsync(waiting);
            AssertSame(waiting, await store.LoadAsync(waiting.ThreadId));
        }

        var graph = new StateGraph<int>()
            .AddTimer("Window", TimeSpan.FromMinutes(1))
            .AddWaitForEvent("Deploying", "deployment.finished", TimeSpan.FromHours(1))
            .AddNode("After", s => s + 1)
            .AddEdge("Window", "Deploying")
            .AddEdge("Deploying", "After")
            .Compile();
        var clock = new Microsoft.Extensions.Time.Testing.FakeTimeProvider(DateTimeOffset.UtcNow);
        var graphKey = new Dictionary<string, string> { ["graph"] = "contract" };
        Assert.IsType<GraphExecutionResult<int>.Interrupted>(
            await graph.RunAsync("wait-contract", 1, store, new RunOptions { TimeProvider = clock, Metadata = graphKey }));

        // Only this graph's threads: the fixtures' are left alone.
        clock.Advance(TimeSpan.FromMinutes(2));
        var options = new WakeOptions { Metadata = graphKey, Run = new RunOptions { TimeProvider = clock } };
        var report = await graph.WakeDueAsync(store, options);
        Assert.Equal([("wait-contract", WakeReason.Timer)], report.Woken.Select(w => (w.ThreadId, w.Reason)));
        // It froze again at the event wait, which isn't due for an hour.
        Assert.IsType<GraphExecutionResult<int>.Interrupted>(report.Woken[0].Result);
        Assert.Empty((await graph.WakeDueAsync(store, options)).Woken);

        var signalled = await graph.SignalAsync("wait-contract", new EventSignal("deployment.finished"), store);
        Assert.Equal(2, Assert.IsType<GraphExecutionResult<int>.Completed>(signalled).State);
    }

    /// <summary>
    /// Thread leases (<c>design/32-Thread-Leases.md</c>, "The store's rules"): acquiring, renewing and
    /// releasing, a held thread refused, an expired lease taken over with a higher token, and the stale owner's
    /// save fenced. <paramref name="pass"/> lets that much time go by on the store's clock, and
    /// <paramref name="brief"/> is a ttl short enough to wait out.
    /// </summary>
    public static async Task LeaseContractAsync(ICheckpointStore store, TimeSpan brief, Func<TimeSpan, Task> pass)
    {
        var lasting = TimeSpan.FromMinutes(5);
        const string Thread = "leased";
        async Task<long?> Sequence() => (await store.LoadLatestAsync(Thread))?.Sequence;

        // Nobody holds a thread that never had a lease, and its first lease has token 1.
        Assert.Null(await store.GetLeaseAsync(Thread));
        var first = await store.TryAcquireLeaseAsync(Thread, "alice", lasting);
        Assert.True(first.Acquired);
        Assert.Null(first.HeldBy);
        var alice = first.Lease!;
        Assert.Equal((Thread, "alice", 1L), (alice.ThreadId, alice.Owner, alice.Token));
        var held = await store.GetLeaseAsync(Thread);
        Assert.Equal(("alice", 1L), (held?.Owner, held?.Token));

        // A held thread is refused to anyone, the same owner name included, and nothing changes.
        foreach (var owner in new[] { "bob", "alice" })
        {
            var refused = await store.TryAcquireLeaseAsync(Thread, owner, lasting);
            Assert.False(refused.Acquired);
            Assert.Null(refused.Lease);
            Assert.Equal(("alice", 1L), (refused.HeldBy?.Owner, refused.HeldBy?.Token));
        }

        // Saves under the lease: unconditional, and conditional on the sequence like SaveIfLatestAsync.
        var one = Checkpoint(Thread, CheckpointStatus.Interrupted, "one");
        Assert.True(await store.SaveUnderLeaseAsync(one, alice));
        var two = Checkpoint(Thread, CheckpointStatus.Interrupted, "two");
        Assert.False(await store.SaveUnderLeaseAsync(two, alice, expectedSequence: 5));
        Assert.Equal(0, await Sequence());
        Assert.True(await store.SaveUnderLeaseAsync(two, alice, expectedSequence: 0));
        Assert.Equal(1, await Sequence());

        // A renewal extends it.
        var renewed = await store.RenewLeaseAsync(alice, lasting + lasting);
        Assert.Equal(("alice", 1L), (renewed?.Owner, renewed?.Token));
        Assert.True(renewed!.ExpiresAt > alice.ExpiresAt);

        // A release frees the thread at once, and the next lease gets a higher token.
        Assert.True(await store.ReleaseLeaseAsync(alice));
        Assert.Null(await store.GetLeaseAsync(Thread));
        var second = await store.TryAcquireLeaseAsync(Thread, "bob", brief);
        var bob = second.Lease!;
        Assert.Equal(("bob", 2L), (bob.Owner, bob.Token));

        // Alice is fenced: her save writes nothing, her renewal is refused, her release changes nothing.
        var stale = Checkpoint(Thread, CheckpointStatus.Failed, "stale");
        var lost = await Assert.ThrowsAsync<LeaseLostException>(async () => await store.SaveUnderLeaseAsync(stale, alice));
        Assert.Equal(alice, lost.Lease);
        await Assert.ThrowsAsync<LeaseLostException>(async () => await store.SaveUnderLeaseAsync(stale, alice, expectedSequence: 1));
        Assert.Equal(1, await Sequence());
        Assert.Null(await store.RenewLeaseAsync(alice, lasting));
        Assert.False(await store.ReleaseLeaseAsync(alice));
        Assert.Equal("bob", (await store.GetLeaseAsync(Thread))?.Owner);

        // An expired lease nobody took: the thread isn't held, but its owner's token is still the latest, so
        // it can still save and renew.
        await pass(brief);
        Assert.Null(await store.GetLeaseAsync(Thread));
        var three = Checkpoint(Thread, CheckpointStatus.Interrupted, "three");
        Assert.True(await store.SaveUnderLeaseAsync(three, bob));
        Assert.Equal(2, await Sequence());
        bob = (await store.RenewLeaseAsync(bob, brief))!;
        Assert.Equal(("bob", 2L), ((await store.GetLeaseAsync(Thread))?.Owner, bob.Token));

        // Expired again, and this time taken over: the token goes up, and bob is fenced.
        await pass(brief);
        var third = await store.TryAcquireLeaseAsync(Thread, "carol", lasting);
        var carol = third.Lease!;
        Assert.Equal(("carol", 3L), (carol.Owner, carol.Token));
        await Assert.ThrowsAsync<LeaseLostException>(async () => await store.SaveUnderLeaseAsync(stale, bob));
        Assert.Null(await store.RenewLeaseAsync(bob, lasting));
        Assert.Equal(2, await Sequence());

        // Saves with no lease are as they were, held thread or not.
        await store.SaveAsync(Checkpoint(Thread, CheckpointStatus.Interrupted, "unleased"));
        Assert.True(await store.SaveIfLatestAsync(Checkpoint(Thread, CheckpointStatus.Interrupted, "unleased too"), 3));
        var last = Checkpoint(Thread, CheckpointStatus.Completed, "last");
        Assert.True(await store.SaveUnderLeaseAsync(last, carol, expectedSequence: 4));
        AssertSame(last, await store.LoadAsync(Thread));

        // Leases are per thread.
        var other = await store.TryAcquireLeaseAsync("leased-other", "carol", lasting);
        Assert.Equal(1, other.Lease?.Token);
        Assert.Equal("carol", (await store.GetLeaseAsync(Thread))?.Owner);

        // A lease for another thread, a blank owner and a ttl under a millisecond are the caller's bugs.
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.SaveUnderLeaseAsync(last, other.Lease!));
        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await store.TryAcquireLeaseAsync(Thread, " ", lasting));
        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await store.TryAcquireLeaseAsync(Thread, "dave", TimeSpan.Zero));
        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await store.RenewLeaseAsync(carol, TimeSpan.Zero));
    }

    /// <summary>
    /// A leased run, end to end against <paramref name="store"/> (<c>design/32-Thread-Leases.md</c>, "What a
    /// run does"): the run freezes and lets go, a held thread refuses a resume and stays resumable, and the
    /// resume then takes the next token and lets go too.
    /// </summary>
    public static async Task LeasedRunContractAsync(ICheckpointStore store)
    {
        var graph = StateGraphInterruptionTests.WireTransferGraph();
        var options = new RunOptions { Lease = new LeaseOptions("worker-1", TimeSpan.FromMinutes(1)) };
        Assert.IsType<GraphExecutionResult<StateGraphInterruptionTests.WireTransferState>.Interrupted>(
            await graph.RunAsync("wire-leased", StateGraphInterruptionTests.InitialState(), store, options));
        Assert.Null(await store.GetLeaseAsync("wire-leased"));

        var other = (await store.TryAcquireLeaseAsync("wire-leased", "worker-2", TimeSpan.FromMinutes(1))).Lease!;
        Assert.Equal(2, other.Token);
        var refused = Assert.IsAssignableFrom<GraphExecutionResult<StateGraphInterruptionTests.WireTransferState>.Failed>(
            await graph.ResumeAsync("wire-leased", HumanSignal.Approve("alice"), store, options));
        Assert.StartsWith("thread 'wire-leased' is held by 'worker-2' until ", refused.Message, StringComparison.Ordinal);
        Assert.Null((await store.LoadAsync("wire-leased"))?.ResumedAt);

        Assert.True(await store.ReleaseLeaseAsync(other));
        Assert.IsType<GraphExecutionResult<StateGraphInterruptionTests.WireTransferState>.Completed>(
            await graph.ResumeAsync("wire-leased", HumanSignal.Approve("alice"), store, options));
        Assert.NotNull((await store.LoadAsync("wire-leased"))?.ResumedAt);
        Assert.Null(await store.GetLeaseAsync("wire-leased"));
        Assert.Equal(4, (await store.TryAcquireLeaseAsync("wire-leased", "worker-3", TimeSpan.FromMinutes(1))).Lease?.Token);
    }

    /// <summary>
    /// A durable run and a recovery, end to end against <paramref name="store"/>
    /// (<c>design/33-Durable-Execution.md</c>): the run writes a row per node and says it finished; a thread
    /// whose process died is listed as running, left alone while a lease holds it, and recovered by the
    /// driver once the lease is gone. Mirrors <c>strata-memory</c>'s <c>run_durable_run</c>.
    /// </summary>
    public static async Task DurableRunContractAsync<TStore>(TStore store)
        where TStore : ICheckpointStore, ICheckpointQuery
    {
        var graph = new StateGraph<StateGraphInterruptionTests.WireTransferState>()
            .AddNode("Draft", s => s with { InvoiceGenerated = true })
            .AddNode("Send", s => s with { IsCompleted = true })
            .AddEdge("Draft", "Send")
            .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });
        var options = new RunOptions { Lease = new LeaseOptions("worker-1", TimeSpan.FromMinutes(1)) };
        var runningOnly = new ThreadQuery { States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Running } };
        async Task<List<Checkpoint>> Log(string threadId) =>
            [.. (await store.GetLogAsync(threadId)).Entries.Select(e => e.Checkpoint)];
        CheckpointStatus[] ranThrough = [CheckpointStatus.Running, CheckpointStatus.Running, CheckpointStatus.Completed];

        Assert.IsType<GraphExecutionResult<StateGraphInterruptionTests.WireTransferState>.Completed>(
            await graph.RunAsync("wire-durable", StateGraphInterruptionTests.InitialState(), store, options));
        Assert.Equal(ranThrough, (await Log("wire-durable")).Select(c => c.Status));
        Assert.Empty((await store.ListThreadsAsync(runningOnly)).Threads);

        // A thread whose process died after Draft, as its last checkpoint left it, still held by the dead
        // worker's lease.
        var dead = (await Log("wire-durable"))[1] with { ThreadId = "wire-dead" };
        var lease = (await store.TryAcquireLeaseAsync("wire-dead", "worker-9", TimeSpan.FromMinutes(1))).Lease!;
        Assert.True(await store.SaveUnderLeaseAsync(dead, lease));
        Assert.Equal(["wire-dead"], (await store.ListThreadsAsync(runningOnly)).Threads.Select(t => t.ThreadId));

        var held = await graph.RecoverStaleAsync(store, new RecoverOptions { Run = options });
        Assert.Empty(held.Recovered);
        Assert.StartsWith("it is held by 'worker-9' until ", Assert.Single(held.Skipped).Reason, StringComparison.Ordinal);

        // The lease is let go, as if it had run out, and the thread is nobody's.
        Assert.True(await store.ReleaseLeaseAsync(lease));
        var report = await graph.RecoverStaleAsync(store, new RecoverOptions { Run = options });

        var recovered = Assert.Single(report.Recovered);
        Assert.Equal(("wire-dead", 1), (recovered.ThreadId, recovered.Recoveries));
        Assert.Equal(["Send"], recovered.At);
        Assert.True(
            Assert.IsType<GraphExecutionResult<StateGraphInterruptionTests.WireTransferState>.Completed>(recovered.Result)
                .State.IsCompleted);
        var log = await Log("wire-dead");
        Assert.Equal(ranThrough, log.Select(c => c.Status));
        Assert.Equal("1", log[1].Metadata!["strata.recoveries"]);
        Assert.Empty((await store.ListThreadsAsync(runningOnly)).Threads);
        Assert.Null(await store.GetLeaseAsync("wire-dead"));
    }

    /// <summary>
    /// Retention against <paramref name="store"/> (<c>design/34-Storage-Hardening.md</c>, "Retention"): a purge
    /// takes whole threads by their latest checkpoint's state and age, oldest first, up to its limit, and
    /// leaves newer ones, other states, and a thread a live lease holds. Mirrors <c>strata-memory</c>'s
    /// <c>run_purge_contract</c>.
    /// </summary>
    public static async Task PurgeContractAsync<TStore>(TStore store)
        where TStore : ICheckpointStore, ICheckpointQuery, ICheckpointRetention
    {
        Checkpoint At(string threadId, CheckpointStatus status, string createdAt) =>
            Checkpoint(threadId, status, threadId) with { CreatedAt = createdAt };
        async Task<List<string>> Threads() =>
            [.. (await store.ListThreadsAsync(new ThreadQuery())).Threads.Select(t => t.ThreadId).Order(StringComparer.Ordinal)];
        var cutoff = new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero);

        await store.SaveAsync(At("old-completed", CheckpointStatus.Completed, "2026-01-01T00:00:00Z"));
        await store.SaveAsync(At("old-ended", CheckpointStatus.Interrupted, "2026-01-01T00:00:00Z"));
        await store.SaveAsync(At("old-ended", CheckpointStatus.Ended, "2026-01-02T00:00:00Z"));
        await store.SaveAsync(At("new-completed", CheckpointStatus.Completed, "2026-06-01T00:00:00Z"));
        await store.SaveAsync(At("at-the-cutoff", CheckpointStatus.Completed, "2026-03-01T00:00:00Z"));
        await store.SaveAsync(At("old-failed", CheckpointStatus.Failed, "2026-01-01T00:00:00Z"));
        await store.SaveAsync(At("old-waiting", CheckpointStatus.Interrupted, "2026-01-01T00:00:00Z"));
        await store.SaveAsync(At("held-completed", CheckpointStatus.Completed, "2026-01-01T00:00:00Z"));
        var held = (await store.TryAcquireLeaseAsync("held-completed", "worker-1", TimeSpan.FromHours(1))).Lease!;

        // Finished threads older than the cutoff go, whole. A newer one, one written at the cutoff, a failed
        // one, a waiting one and one a live lease holds stay.
        Assert.Equal(2, await store.PurgeAsync(cutoff));
        Assert.Null(await store.LoadAsync("old-completed"));
        Assert.Null(await store.LoadAsync("old-ended"));
        Assert.Empty((await store.GetLogAsync("old-ended")).Entries);
        Assert.Equal(
            ["at-the-cutoff", "held-completed", "new-completed", "old-failed", "old-waiting"], await Threads());
        Assert.Equal(0, await store.PurgeAsync(cutoff));

        // Named states are honoured, and only they.
        Assert.Equal(
            1, await store.PurgeAsync(cutoff, new HashSet<CheckpointThreadState> { CheckpointThreadState.Failed }));
        Assert.Equal(["at-the-cutoff", "held-completed", "new-completed", "old-waiting"], await Threads());
        Assert.Equal(0, await store.PurgeAsync(cutoff, new HashSet<CheckpointThreadState>()));

        // The limit takes the oldest first.
        await store.SaveAsync(At("c1", CheckpointStatus.Completed, "2026-01-03T00:00:00Z"));
        await store.SaveAsync(At("c2", CheckpointStatus.Completed, "2026-01-04T00:00:00Z"));
        await store.SaveAsync(At("c3", CheckpointStatus.Completed, "2026-01-05T00:00:00Z"));
        Assert.Equal(2, await store.PurgeAsync(cutoff, limit: 2));
        Assert.Equal(["at-the-cutoff", "c3", "held-completed", "new-completed", "old-waiting"], await Threads());
        Assert.Equal(1, await store.PurgeAsync(cutoff, limit: 2));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.PurgeAsync(cutoff, limit: 0));

        // Once its lease is let go, the held thread goes too, with its lease record: the id starts over.
        Assert.True(await store.ReleaseLeaseAsync(held));
        Assert.Equal(1, await store.PurgeAsync(cutoff));
        Assert.Equal(["at-the-cutoff", "new-completed", "old-waiting"], await Threads());
        Assert.Equal(1, (await store.TryAcquireLeaseAsync("held-completed", "worker-2", TimeSpan.FromMinutes(1))).Lease?.Token);

        // A purged id can be used again, from sequence 0.
        await store.SaveAsync(At("old-completed", CheckpointStatus.Interrupted, "2026-07-01T00:00:00Z"));
        Assert.Equal(0, (await store.LoadLatestAsync("old-completed"))?.Sequence);
    }

    private static ChatMessage Message(MessageRole role, string content, int secondsOffset) =>
        new(role, content.AsMemory(), new DateTimeOffset(2026, 9, 16, 0, 0, 0, TimeSpan.Zero).AddSeconds(secondsOffset));

    private static async Task<List<string>> ContentsAsync(IAsyncEnumerable<ChatMessage> source)
    {
        var results = new List<string>();
        await foreach (var message in source)
        {
            results.Add(message.Content.ToString());
        }

        return results;
    }

    public static async Task HistoryStoreContractAsync(IHistoryStore store)
    {
        // Unknown session -> empty history, not an error.
        Assert.Empty(await ContentsAsync(store.GetHistoryAsync("missing")));

        // Append then retrieve, in chronological (insertion) order.
        await store.AppendAsync("s1", Message(MessageRole.User, "hi", 0));
        await store.AppendAsync("s1", Message(MessageRole.Assistant, "hello!", 1));
        Assert.Equal(["hi", "hello!"], await ContentsAsync(store.GetHistoryAsync("s1", 10)));

        // limit returns only the most recent N, still chronological.
        await store.AppendAsync("s1", Message(MessageRole.User, "third", 2));
        Assert.Equal(["hello!", "third"], await ContentsAsync(store.GetHistoryAsync("s1", 2)));

        // A limit larger than the history is not an error; a limit of zero is an empty history (in
        // Redis, `LRANGE key -0 -1` would be *everything*).
        Assert.Equal(3, (await ContentsAsync(store.GetHistoryAsync("s1", 1_000))).Count);
        Assert.Empty(await ContentsAsync(store.GetHistoryAsync("s1", 0)));

        // Sessions are isolated from each other.
        await store.AppendAsync("s2", Message(MessageRole.System, "other session", 0));
        Assert.Equal(3, (await ContentsAsync(store.GetHistoryAsync("s1", 10))).Count);
        Assert.Equal(["other session"], await ContentsAsync(store.GetHistoryAsync("s2", 10)));
    }

    /// <summary>Every role, a sub-second timestamp, and content that would break a naive encoding all come back intact.</summary>
    public static async Task HistoryRoundTripContractAsync(IHistoryStore store)
    {
        string[] awkward =
        [
            string.Empty,
            "say \"hi\" \\ then\nnewline\ttab",
            "naïve — 日本語 🚀",
            "{\"looks\":\"like json\"}",
        ];
        MessageRole[] roles = [MessageRole.System, MessageRole.User, MessageRole.Assistant, MessageRole.Tool];
        var timestamp = DateTimeOffset.FromUnixTimeSeconds(1_700_000_000).AddTicks(1_234_560);

        for (var i = 0; i < awkward.Length; i++)
        {
            await store.AppendAsync("round-trip", new ChatMessage(roles[i], awkward[i].AsMemory(), timestamp));
        }

        var history = new List<ChatMessage>();
        await foreach (var message in store.GetHistoryAsync("round-trip", 10))
        {
            history.Add(message);
        }

        Assert.Equal(awkward.Length, history.Count);
        for (var i = 0; i < awkward.Length; i++)
        {
            Assert.Equal(roles[i], history[i].Role);
            Assert.Equal(awkward[i], history[i].Content.ToString());
            Assert.Equal(timestamp, history[i].Timestamp);
        }
    }
}
