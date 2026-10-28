using Datalium.Strata.Cloud;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Tests.Backends;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.Tests.Cloud;

/// <summary>
/// Leases and the query port on the Cloud store (<c>design/43-Cloud-Leases-And-Listing.md</c>, SG-36), beyond the
/// shared fixtures: the engine's leased runs, its wake and recovery drivers and a wake through a resume, against
/// the emulated Cloud; the store contracts and <c>cases.json</c> through it; and what the store refuses before it
/// sends anything. Mirrors Rust's <c>strata-cloud/tests/leases_and_listing.rs</c>. Keep the two in step.
/// </summary>
[Collection("Cloud")]
public class CloudLeaseAndListingTests
{
    public sealed record Transfer(bool Drafted, bool Sent);

    private static readonly Transfer Initial = new(false, false);

    private static RunOptions Leased(string owner) => new() { Lease = new LeaseOptions(owner, TimeSpan.FromMinutes(1)) };

    /// <summary><c>Draft</c> → <c>Send</c>, with a checkpoint per node; <paramref name="onDraft"/> runs inside <c>Draft</c>.</summary>
    private static CompiledGraph<Transfer> DurableGraph(Action? onDraft = null) =>
        new StateGraph<Transfer>()
            .AddNode("Draft", s =>
            {
                onDraft?.Invoke();
                return s with { Drafted = true };
            })
            .AddNode("Send", s => s with { Sent = true })
            .AddEdge("Draft", "Send")
            .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });

    /// <summary><c>Draft</c> → a one-hour timer <c>Window</c> → <c>Send</c>.</summary>
    private static CompiledGraph<Transfer> TimerGraph() =>
        new StateGraph<Transfer>()
            .AddNode("Draft", s => s with { Drafted = true })
            .AddTimer("Window", TimeSpan.FromHours(1))
            .AddNode("Send", s => s with { Sent = true })
            .AddEdge("Draft", "Window")
            .AddEdge("Window", "Send")
            .Compile();

    private static StrataCloudCheckpointStore StoreOf(EmulatedCloud cloud) => new(CloudTest.Options(), new HttpClient(cloud));

    private static async Task<List<CheckpointStatus>> Statuses(StrataCloudCheckpointStore store, string threadId) =>
        [.. (await store.GetLogAsync(threadId)).Entries.Select(entry => entry.Checkpoint.Status)];

    private static readonly CheckpointStatus[] RanThrough =
        [CheckpointStatus.Running, CheckpointStatus.Running, CheckpointStatus.Completed];

    [Fact]
    public async Task A_leased_run_saves_every_checkpoint_under_its_lease_and_releases_it()
    {
        var cloud = new EmulatedCloud();
        using var store = StoreOf(cloud);

        var result = await DurableGraph().RunAsync("wire-1", Initial, store, Leased("worker-1"));

        Assert.IsType<GraphExecutionResult<Transfer>.Completed>(result);
        Assert.Equal(RanThrough, await Statuses(store, "wire-1"));
        var saves = cloud.Requests.Where(r => r.Method == "POST" && r.Path == "/v1/sdk/checkpoints").ToList();
        Assert.Equal(3, saves.Count);
        Assert.All(saves, save => Assert.Equal("1", save.Header("strata-lease-token")));
        var acquisition = cloud.Requests.First(r => r.Method == "POST" && r.Path == "/v1/sdk/leases");
        Assert.NotNull(acquisition.Header("idempotency-key"));
        Assert.Equal((1L, "worker-1", true), cloud.Lease("wire-1"));
        Assert.Null(await store.GetLeaseAsync("wire-1"));
    }

    [Fact]
    public async Task A_thread_another_owner_holds_is_refused_with_nothing_written()
    {
        var cloud = new EmulatedCloud();
        using var store = StoreOf(cloud);
        cloud.TakeOver("wire-2", "worker-2");

        var result = await DurableGraph().RunAsync("wire-2", Initial, store, Leased("worker-1"));

        var failed = Assert.IsAssignableFrom<GraphExecutionResult<Transfer>.Failed>(result);
        Assert.Contains("is held by 'worker-2' until ", failed.Message, StringComparison.Ordinal);
        Assert.Equal(0, cloud.Saves("wire-2"));
    }

    [Fact]
    public async Task A_save_after_a_takeover_is_cancelled_as_lease_lost_and_writes_nothing()
    {
        var cloud = new EmulatedCloud();
        using var store = StoreOf(cloud);
        // While Draft runs, the thread's lease runs out and another owner takes it: the run's next save is fenced off.
        var graph = DurableGraph(() => cloud.TakeOver("wire-3", "worker-2"));

        var result = await graph.RunAsync("wire-3", Initial, store, Leased("worker-1"));

        var cancelled = Assert.IsType<GraphExecutionResult<Transfer>.Cancelled>(result);
        Assert.Equal(CancelReason.LeaseLost, cancelled.Reason);
        // Only the claim before Draft: nothing after the takeover.
        Assert.Equal(1, cloud.Saves("wire-3"));
        Assert.Equal((2L, "worker-2", false), cloud.Lease("wire-3"));
    }

    [Fact]
    public async Task WakeDue_follows_short_pages_and_wakes_every_due_timer()
    {
        var cloud = new EmulatedCloud();
        using var store = StoreOf(cloud);
        var graph = TimerGraph();
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        foreach (var threadId in new[] { "timer-1", "timer-2", "timer-3" })
        {
            Assert.IsType<GraphExecutionResult<Transfer>.Interrupted>(
                await graph.RunAsync(threadId, Initial, store, new RunOptions { TimeProvider = clock }));
        }

        // One thread a page, the first page empty: only a null cursor ends it.
        cloud.ListCap = 1;
        clock.Advance(TimeSpan.FromHours(2));

        var report = await graph.WakeDueAsync(store, new WakeOptions { Run = new RunOptions { TimeProvider = clock } });

        Assert.Equal(3, report.Woken.Count);
        Assert.All(report.Woken, woken => Assert.IsType<GraphExecutionResult<Transfer>.Completed>(woken.Result));
        var listings = cloud.Requests.Where(r => r.Path == "/v1/sdk/threads").ToList();
        Assert.Equal(4, listings.Count);
        Assert.False(listings[0].Query.ContainsKey("cursor"));
        Assert.All(listings.Skip(1), listing =>
        {
            Assert.StartsWith("emulated:", listing.Query["cursor"], StringComparison.Ordinal);
            Assert.True(listing.Query.ContainsKey("due_by"));
        });
    }

    [Fact]
    public async Task A_due_timer_wakes_through_a_resume_on_cloud()
    {
        var cloud = new EmulatedCloud();
        using var store = StoreOf(cloud);
        var graph = TimerGraph();
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await graph.RunAsync("timer-4", Initial, store, new RunOptions { TimeProvider = clock });

        var early = await graph.ResumeAsync("timer-4", LevelSignals.Due, store, new RunOptions { TimeProvider = clock });
        Assert.IsAssignableFrom<GraphExecutionResult<Transfer>.Failed>(early);

        clock.Advance(TimeSpan.FromHours(2));
        var woken = await graph.ResumeAsync(
            "timer-4", LevelSignals.Due, store, new RunOptions { Lease = Leased("worker-1").Lease, TimeProvider = clock });

        Assert.True(Assert.IsType<GraphExecutionResult<Transfer>.Completed>(woken).State.Sent);
    }

    [Fact]
    public async Task RecoverStale_leaves_a_held_running_thread_and_recovers_it_once_let_go()
    {
        var cloud = new EmulatedCloud();
        using var store = StoreOf(cloud);
        var graph = DurableGraph();
        await graph.RunAsync("wire-5", Initial, store, Leased("worker-1"));

        // A thread whose process died after Draft, still held by its lease.
        var dead = (await store.GetLogAsync("wire-5")).Entries[1].Checkpoint with { ThreadId = "wire-dead" };
        var lease = (await store.TryAcquireLeaseAsync("wire-dead", "worker-9", TimeSpan.FromMinutes(1))).Lease!;
        Assert.True(await store.SaveUnderLeaseAsync(dead, lease));

        var options = new RecoverOptions { Run = Leased("worker-1") };
        var held = await graph.RecoverStaleAsync(store, options);
        Assert.Empty(held.Recovered);
        Assert.StartsWith("it is held by 'worker-9' until ", Assert.Single(held.Skipped).Reason, StringComparison.Ordinal);

        Assert.True(await store.ReleaseLeaseAsync(lease));
        var report = await graph.RecoverStaleAsync(store, options);

        var recovered = Assert.Single(report.Recovered);
        Assert.Equal("wire-dead", recovered.ThreadId);
        Assert.IsType<GraphExecutionResult<Transfer>.Completed>(recovered.Result);
        Assert.Equal(RanThrough, await Statuses(store, "wire-dead"));
    }

    [Fact]
    public async Task The_listing_maps_each_row_to_the_summary_the_engine_makes()
    {
        var cloud = new EmulatedCloud();
        using var store = StoreOf(cloud);
        var seeded = new List<ThreadSummary>();
        foreach (var name in new[] { "nested-timer", "nested-identified", "running", "ended", "waiting-event" })
        {
            var checkpoint = Checkpoint.FromWireJson(
                CheckpointQueryContractTests.Fixture(Path.Combine("checkpoint-lifecycle", name + ".json")));
            cloud.Seed(checkpoint);
            seeded.Add(ThreadSummary.Of(0, checkpoint));
        }

        var page = await store.ListThreadsAsync(new ThreadQuery());

        Assert.Null(page.NextCursor);
        Assert.Equal(seeded.Count, page.Threads.Count);
        foreach (var summary in seeded)
        {
            var listed = page.Threads.Single(t => t.ThreadId == summary.ThreadId);
            Assert.Equal(EmulatedCloud.SummaryJson(summary).ToJsonString(), EmulatedCloud.SummaryJson(listed).ToJsonString());
            Assert.Equal(summary.Wait, listed.Wait);
            Assert.Equal(summary.Graph, listed.Graph);
            Assert.Equal(summary.UpdatedAt, listed.UpdatedAt);
        }

        var running = await store.ListThreadsAsync(
            new ThreadQuery { States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Running } });
        Assert.Equal(CheckpointThreadState.Running, Assert.Single(running.Threads).State);
    }

    [Fact]
    public async Task The_log_follows_early_stops_until_it_has_the_limit()
    {
        var cloud = new EmulatedCloud();
        using var store = StoreOf(cloud);
        await DurableGraph().RunAsync("wire-6", Initial, store);
        Assert.Equal(3, cloud.Saves("wire-6"));
        cloud.LogCap = 1;

        var full = await store.GetLogAsync("wire-6", null, 3);
        var all = await store.GetLogAsync("wire-6", null, 10);
        var rest = await store.GetLogAsync("wire-6", 0, 1);
        var unknown = await store.GetLogAsync("nobody", null, 10);

        Assert.Equal([0L, 1L, 2L], full.Entries.Select(e => e.Sequence));
        Assert.Equal(2, full.NextAfterSequence);
        Assert.Equal([0L, 1L, 2L], all.Entries.Select(e => e.Sequence));
        Assert.Null(all.NextAfterSequence);
        Assert.Equal([1L], rest.Entries.Select(e => e.Sequence));
        Assert.Equal(1, rest.NextAfterSequence);
        Assert.Empty(unknown.Entries);
        Assert.Null(unknown.NextAfterSequence);
    }

    [Fact]
    public async Task Each_acquisition_sends_an_idempotency_key_of_its_own()
    {
        var cloud = new EmulatedCloud();
        using var store = StoreOf(cloud);

        var first = await store.TryAcquireLeaseAsync("t", "worker-1", TimeSpan.FromSeconds(30));
        var second = await store.TryAcquireLeaseAsync("t", "worker-1", TimeSpan.FromSeconds(30));

        Assert.True(first.Acquired);
        // The same owner name is held off too (contract §11).
        Assert.Equal(first.Lease!.Token, second.HeldBy!.Token);
        var keys = cloud.Requests.Select(r => r.Header("idempotency-key")).ToList();
        Assert.Equal(2, keys.Count);
        Assert.NotNull(keys[0]);
        Assert.NotEqual(keys[0], keys[1]);
    }

    [Fact]
    public async Task A_423_on_a_save_without_a_lease_is_an_error_not_a_lost_lease()
    {
        var cloud = new ScriptedCloud().Reply(423, CloudTest.Envelope(423, StrataCloudWire.LeaseLostCode));
        using var store = new StrataCloudCheckpointStore(CloudTest.Options(), new HttpClient(cloud));
        var checkpoint = Checkpoint.FromWireJson(
            CheckpointQueryContractTests.Fixture(Path.Combine("checkpoint-lifecycle", "running.json")));

        var error = await Assert.ThrowsAsync<StrataCloudException>(async () => await store.SaveAsync(checkpoint));

        Assert.Equal((StrataCloudErrorKind.Rejected, 423, StrataCloudWire.LeaseLostCode), (error.Kind, error.StatusCode, error.Code));
        Assert.Single(cloud.Requests);
    }

    [Fact]
    public async Task What_a_store_would_refuse_is_refused_before_anything_is_sent()
    {
        var cloud = new ScriptedCloud();
        using var store = new StrataCloudCheckpointStore(CloudTest.Options(), new HttpClient(cloud));
        var checkpoint = Checkpoint.FromWireJson(
            CheckpointQueryContractTests.Fixture(Path.Combine("checkpoint-lifecycle", "running.json")));
        var other = new ThreadLease("another-thread", "worker-1", 1, DateTimeOffset.UtcNow);

        await Assert.ThrowsAsync<ArgumentException>(async () => await store.ListThreadsAsync(new ThreadQuery { GraphVersion = "2" }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.ListThreadsAsync(new ThreadQuery { Limit = 0 }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.ListThreadsAsync(new ThreadQuery { Limit = 1001 }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.GetLogAsync("t", null, 0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.GetLogAsync("t", null, 1001));
        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await store.TryAcquireLeaseAsync("t", " ", TimeSpan.FromSeconds(30)));
        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await store.TryAcquireLeaseAsync("t", "worker-1", TimeSpan.Zero));
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.SaveUnderLeaseAsync(checkpoint, other));
        Assert.Empty(cloud.Requests);
    }

    [Fact]
    public async Task The_shared_query_cases_are_answered_through_the_cloud_store()
    {
        var cloud = new EmulatedCloud();
        using var store = StoreOf(cloud);
        await CheckpointQueryContractTests.SaveSeedAsync(store);

        // Every filter goes over the wire and back; the emulator answers by the engine's own rules.
        await CheckpointQueryContractTests.RunCasesAsync(store, exact: true, opaqueCursor: true);
        Assert.All(
            cloud.Requests.Where(r => r.Path == "/v1/sdk/threads" && r.Query.ContainsKey("cursor")),
            listing => Assert.StartsWith("emulated:", listing.Query["cursor"], StringComparison.Ordinal));
        await Assert.ThrowsAsync<StrataCloudException>(async () => await store.ListThreadsAsync(new ThreadQuery { Cursor = "not a cursor" }));
    }

    [Fact]
    public async Task The_store_contracts_hold_through_the_cloud_store()
    {
        foreach (var contract in new Func<StrataCloudCheckpointStore, Task>[]
        {
            store => StoreContracts.FreezeResumeContractAsync(store),
            store => StoreContracts.ConditionalSaveContractAsync(store),
            store => StoreContracts.FreezeEndContractAsync(store),
            store => StoreContracts.FreezeWakeContractAsync(store),
            store => StoreContracts.LeasedRunContractAsync(store),
            store => StoreContracts.DurableRunContractAsync(store),
        })
        {
            using var store = StoreOf(new EmulatedCloud());
            await contract(store);
        }
    }
}
