using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;
using Datalium.Strata.Cloud;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Microsoft.Extensions.Time.Testing;
using static Datalium.Strata.Tests.Cloud.CloudStoreTests;

namespace Datalium.Strata.Tests.Cloud;

/// <summary>
/// A test against a live Strata Cloud (<c>design/30-Strata-Cloud-Store.md</c>, "Tests"): a local Strata Cloud
/// stack at <c>STRATA_CLOUD_URL</c>, with an SDK key minted from
/// the dev token in <c>STRATA_CLOUD_KEY</c>. Skipped without them, and not part of CI. They need Cloud's
/// catch-up to checkpoint schema 1.3.0 and later: before it, the resume's stamp is refused with a 422.
/// </summary>
public sealed class CloudLiveFactAttribute : FactAttribute
{
    public CloudLiveFactAttribute(bool console = false)
    {
        var missing = new[] { CloudLive.UrlVar, CloudLive.KeyVar }
            .Concat(console ? [CloudLive.ConsoleTokenVar] : [])
            .Where(name => string.IsNullOrEmpty(Environment.GetEnvironmentVariable(name)))
            .ToList();
        if (missing.Count > 0)
        {
            Skip = $"{string.Join(", ", missing)} not set (a live Strata Cloud; see design/30)";
        }
    }
}

internal static class CloudLive
{
    public const string UrlVar = "STRATA_CLOUD_URL";
    public const string KeyVar = "STRATA_CLOUD_KEY";
    public const string ConsoleTokenVar = "STRATA_CLOUD_CONSOLE_TOKEN";

    public static Uri Url => new(Environment.GetEnvironmentVariable(UrlVar)!);

    public static StrataCloudOptions Options => new() { BaseUrl = Url, ApiKey = Environment.GetEnvironmentVariable(KeyVar)! };

    public static string ThreadId(string name) => $"sg18-live-{name}-{Guid.NewGuid():N}";

    /// <summary>A thread id, or a graph id, unique to this run, for SG-36's tests.</summary>
    public static string Unique(string name) => $"sg36-live-{name}-{Guid.NewGuid():N}";

    /// <summary>A reviewer approves the thread's pending approval through the console API, with the dev token.</summary>
    public static async Task ApproveAsync(string threadId, string reviewer, string? notes = null)
    {
        using var console = new HttpClient { BaseAddress = Url };
        console.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
            "Bearer", Environment.GetEnvironmentVariable(ConsoleTokenVar));
        string? approvalId = null;
        for (var attempt = 0; attempt < 20 && approvalId is null; attempt++)
        {
            var queue = JsonNode.Parse(await console.GetStringAsync("v1/approvals?status=pending&order=desc&limit=100"))!;
            approvalId = queue["data"]!["items"]!.AsArray()
                .FirstOrDefault(item => item!["thread"]!["threadId"]!.GetValue<string>() == threadId)?["approvalId"]!.GetValue<string>();
            if (approvalId is null)
            {
                await Task.Delay(250);
            }
        }

        Assert.NotNull(approvalId);
        var body = new JsonObject { ["isApproved"] = true, ["reviewerToken"] = reviewer };
        if (notes is not null)
        {
            body["notes"] = notes;
        }

        using var decided = await console.PostAsync(
            $"v1/approvals/{approvalId}/decision", new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json"));
        Assert.True(decided.IsSuccessStatusCode, $"decision: HTTP {(int)decided.StatusCode}");
    }
}

[Collection("Cloud")]
public class CloudLiveTests
{
    [CloudLiveFact]
    public async Task Saves_loads_and_interrupts_on_a_live_cloud()
    {
        using var store = new StrataCloudCheckpointStore(CloudLive.Options);
        var (graph, _) = RolloutGraph();
        var threadId = CloudLive.ThreadId("interrupt");

        Assert.Null(await store.LoadAsync(threadId));
        Assert.IsType<GraphExecutionResult<Rollout>.Interrupted>(await graph.RunAsync(threadId, Initial(), store));

        var latest = await store.LoadLatestAsync(threadId);
        Assert.NotNull(latest);
        Assert.Equal(CheckpointStatus.Interrupted, latest.Checkpoint.Status);
        Assert.Equal("Prepare", latest.Checkpoint.SuspendedNode);

        // The idempotent retry: the same checkpoint again appends nothing.
        await store.SaveAsync(latest.Checkpoint);
        var again = await store.LoadLatestAsync(threadId);
        Assert.True(StrataCloudCheckpoints.AreEquivalent(latest.Checkpoint, again!.Checkpoint));
        Assert.Equal(latest.Sequence, again.Sequence);
    }

    [CloudLiveFact(console: true)]
    public async Task A_decision_in_the_console_is_resumed_by_the_worker()
    {
        using var store = new StrataCloudCheckpointStore(CloudLive.Options);
        using var decisions = new StrataCloudDecisions(CloudLive.Options);
        var (graph, executed) = RolloutGraph();
        var threadId = CloudLive.ThreadId("decision");
        Assert.IsType<GraphExecutionResult<Rollout>.Interrupted>(await graph.RunAsync(threadId, Initial(), store));

        await CloudLive.ApproveAsync(threadId, "sg18-live-reviewer", "sg38 live notes");

        var worker = new DecisionWorker<Rollout>(graph, store, decisions, new DecisionWorkerOptions<Rollout> { ThreadId = threadId });
        HandledDecision<Rollout>? handled = null;
        for (var round = 0; round < 5 && handled is null; round++)
        {
            handled = (await worker.PollAsync(TimeSpan.FromSeconds(5))).SingleOrDefault();
        }

        Assert.NotNull(handled);
        Assert.Equal(DecisionOutcome.Resumed, handled.Outcome);
        Assert.Equal("sg18-live-reviewer", handled.Decision.Signal.ReviewerToken);

        // Who decided is the console's verified identity, not the reviewer token (contract §5, SG-38).
        Assert.Equal("dev-console", handled.Decision.DecidedBy);
        Assert.Null(handled.Decision.DecidedByEmail);
        Assert.Equal("sg38 live notes", handled.Decision.Notes);
        Assert.IsType<GraphExecutionResult<Rollout>.Completed>(handled.Result);
        Assert.Equal(1, executed());
        Assert.NotNull((await store.LoadAsync(threadId))!.ResumedAt);
        Assert.Empty(await worker.PollAsync(TimeSpan.Zero));
    }

}

/// <summary>
/// Leases and the query port on a live Strata Cloud (<c>design/43-Cloud-Leases-And-Listing.md</c>): a leased run, a
/// held thread, a lost lease, the wake driver, a wake through a resume and the recovery driver, each on graphs with
/// an identity of their own per run, so the drivers list only this run's threads in a dev account that keeps
/// everything earlier runs left. Mirrors Rust's <c>strata-cloud/tests/live.rs</c>.
/// </summary>
[Collection("Cloud")]
public class CloudLiveLeaseAndListingTests
{
    public sealed record Transfer(bool Drafted, bool Sent);

    private static readonly Transfer Initial = new(false, false);

    private static readonly CheckpointStatus[] RanThrough =
        [CheckpointStatus.Running, CheckpointStatus.Running, CheckpointStatus.Completed];

    private static RunOptions Leased(string owner, TimeSpan ttl) => new() { Lease = new LeaseOptions(owner, ttl) };

    private static CompiledGraph<Transfer> DurableGraph(string graphId) =>
        new StateGraph<Transfer>()
            .AddNode("Draft", s => s with { Drafted = true })
            .AddNode("Send", s => s with { Sent = true })
            .AddEdge("Draft", "Send")
            .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode, GraphId = graphId });

    private static CompiledGraph<Transfer> TimerGraph(string graphId) =>
        new StateGraph<Transfer>()
            .AddNode("Draft", s => s with { Drafted = true })
            .AddTimer("Window", TimeSpan.FromHours(1))
            .AddNode("Send", s => s with { Sent = true })
            .AddEdge("Draft", "Window")
            .AddEdge("Window", "Send")
            .Compile(new CompilationOptions { GraphId = graphId });

    private static async Task<List<CheckpointStatus>> Statuses(StrataCloudCheckpointStore store, string threadId) =>
        [.. (await store.GetLogAsync(threadId)).Entries.Select(entry => entry.Checkpoint.Status)];

    [CloudLiveFact]
    public async Task A_leased_run_on_a_live_cloud_completes_and_releases_its_lease()
    {
        using var store = new StrataCloudCheckpointStore(CloudLive.Options);
        var threadId = CloudLive.Unique("leased");

        var result = await DurableGraph(CloudLive.Unique("graph"))
            .RunAsync(threadId, Initial, store, Leased("sg36-worker-1", TimeSpan.FromSeconds(30)));

        Assert.IsType<GraphExecutionResult<Transfer>.Completed>(result);
        Assert.Equal(RanThrough, await Statuses(store, threadId));
        Assert.Null(await store.GetLeaseAsync(threadId));
    }

    [CloudLiveFact]
    public async Task A_thread_held_on_a_live_cloud_is_refused_with_nothing_written()
    {
        using var store = new StrataCloudCheckpointStore(CloudLive.Options);
        var threadId = CloudLive.Unique("held");
        var holder = (await store.TryAcquireLeaseAsync(threadId, "sg36-worker-2", TimeSpan.FromMinutes(1))).Lease!;

        var result = await DurableGraph(CloudLive.Unique("graph"))
            .RunAsync(threadId, Initial, store, Leased("sg36-worker-1", TimeSpan.FromSeconds(30)));

        var failed = Assert.IsAssignableFrom<GraphExecutionResult<Transfer>.Failed>(result);
        Assert.Contains("is held by 'sg36-worker-2' until ", failed.Message, StringComparison.Ordinal);
        Assert.Null(await store.LoadAsync(threadId));
        Assert.True(await store.ReleaseLeaseAsync(holder));
    }

    [CloudLiveFact]
    public async Task A_save_after_a_takeover_on_a_live_cloud_is_cancelled_as_lease_lost()
    {
        using var cloud = new StrataCloudCheckpointStore(CloudLive.Options);
        var threadId = CloudLive.Unique("takeover");
        // Draft outlives the run's one-second lease, which isn't renewed, and another owner takes the thread: the
        // run's next save is fenced off.
        var graph = new StateGraph<Transfer>()
            .AddNode("Draft", async (Transfer s, NodeContext _, CancellationToken cancellationToken) =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(1500), cancellationToken);
                Assert.True((await cloud.TryAcquireLeaseAsync(threadId, "sg36-worker-2", TimeSpan.FromMinutes(1))).Acquired);
                return s with { Drafted = true };
            })
            .AddNode("Send", s => s with { Sent = true })
            .AddEdge("Draft", "Send")
            .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });

        var result = await graph.RunAsync(
            threadId, Initial, new NoRenewals(cloud), Leased("sg36-worker-1", TimeSpan.FromSeconds(1)));

        Assert.Equal(CancelReason.LeaseLost, Assert.IsType<GraphExecutionResult<Transfer>.Cancelled>(result).Reason);
        // Only the claim before Draft.
        Assert.Equal([CheckpointStatus.Running], await Statuses(cloud, threadId));
        var held = await cloud.GetLeaseAsync(threadId);
        Assert.Equal("sg36-worker-2", held!.Owner);
        Assert.True(await cloud.ReleaseLeaseAsync(held));
    }

    [CloudLiveFact]
    public async Task Due_timers_wake_on_a_live_cloud_by_the_driver_and_through_a_resume()
    {
        using var store = new StrataCloudCheckpointStore(CloudLive.Options);
        var graph = TimerGraph(CloudLive.Unique("timers"));
        var (driven, resumed) = (CloudLive.Unique("timer-driven"), CloudLive.Unique("timer-resumed"));
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        foreach (var threadId in new[] { driven, resumed })
        {
            Assert.IsType<GraphExecutionResult<Transfer>.Interrupted>(
                await graph.RunAsync(threadId, Initial, store, new RunOptions { TimeProvider = clock }));
        }

        clock.Advance(TimeSpan.FromHours(2));
        var later = new RunOptions { TimeProvider = clock };

        Assert.IsType<GraphExecutionResult<Transfer>.Completed>(await graph.ResumeAsync(resumed, LevelSignals.Due, store, later));

        // The driver lists this graph's due threads: only the other one is left.
        var report = await graph.WakeDueAsync(store, new WakeOptions { Run = later });
        var woken = Assert.Single(report.Woken);
        Assert.Equal(driven, woken.ThreadId);
        Assert.IsType<GraphExecutionResult<Transfer>.Completed>(woken.Result);
    }

    [CloudLiveFact]
    public async Task RecoverStale_lists_and_recovers_a_running_thread_on_a_live_cloud()
    {
        using var store = new StrataCloudCheckpointStore(CloudLive.Options);
        var graph = DurableGraph(CloudLive.Unique("durable"));
        var (finished, dead) = (CloudLive.Unique("finished"), CloudLive.Unique("dead"));
        await graph.RunAsync(finished, Initial, store);

        // A thread whose process died after Draft.
        await store.SaveAsync((await store.GetLogAsync(finished)).Entries[1].Checkpoint with { ThreadId = dead });
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow.AddHours(1));

        var report = await graph.RecoverStaleAsync(
            store, new RecoverOptions { StaleAfter = TimeSpan.FromMinutes(1), Run = new RunOptions { TimeProvider = clock } });

        var recovered = Assert.Single(report.Recovered);
        Assert.Equal(dead, recovered.ThreadId);
        Assert.IsType<GraphExecutionResult<Transfer>.Completed>(recovered.Result);
        Assert.Equal(RanThrough, await Statuses(store, dead));
    }

    /// <summary>The Cloud store with renewals answered here and never sent: the run believes its lease is renewed while it runs out on Cloud.</summary>
    private sealed class NoRenewals(StrataCloudCheckpointStore inner) : ICheckpointStore
    {
        public void Save(Checkpoint checkpoint) => inner.Save(checkpoint);

        public Checkpoint? Load(string threadId) => inner.Load(threadId);

        public ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
            inner.SaveAsync(checkpoint, cancellationToken);

        public ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
            inner.LoadAsync(threadId, cancellationToken);

        public ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
            inner.LoadLatestAsync(threadId, cancellationToken);

        public ValueTask<bool> SaveIfLatestAsync(Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
            inner.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken);

        public ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
            string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default) =>
            inner.TryAcquireLeaseAsync(threadId, owner, ttl, cancellationToken);

        public ValueTask<ThreadLease?> RenewLeaseAsync(ThreadLease lease, TimeSpan ttl, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult<ThreadLease?>(lease);

        public ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken cancellationToken = default) =>
            inner.ReleaseLeaseAsync(lease, cancellationToken);

        public ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken cancellationToken = default) =>
            inner.GetLeaseAsync(threadId, cancellationToken);

        public ValueTask<bool> SaveUnderLeaseAsync(
            Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default) =>
            inner.SaveUnderLeaseAsync(checkpoint, lease, expectedSequence, cancellationToken);
    }
}
