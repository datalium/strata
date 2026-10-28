using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Cloud;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.Tests.Cloud;

/// <summary>
/// Mirrors <c>rust/strata-cloud/tests/store.rs</c> (<c>design/30-Strata-Cloud-Store.md</c>, SG-18): the Cloud
/// store's behaviour beyond the shared fixtures. Keep the two in step.
/// </summary>
[Collection("Cloud")]
public class CloudStoreTests
{
    /// <summary>
    /// A state Cloud normalises: <c>amount</c> is written <c>1.0</c> (a decimal keeps its scale) and comes back
    /// <c>1</c>, <c>big</c> is above 2^53, <c>labels</c> has keys Cloud reorders.
    /// </summary>
    public sealed record Rollout(
        [property: JsonPropertyName("service")] string Service,
        [property: JsonPropertyName("amount")] decimal Amount,
        [property: JsonPropertyName("big")] long Big,
        [property: JsonPropertyName("replicas")] int Replicas,
        [property: JsonPropertyName("labels")] Dictionary<string, string> Labels,
        [property: JsonPropertyName("log")] IReadOnlyList<string> Log);

    internal static Rollout Initial() => new(
        "api", 1.0m, 9_007_199_254_740_993, 1, new() { ["zone"] = "eu", ["app"] = "api" }, []);

    /// <summary><c>Prepare</c> → boundary → <c>Execute</c>, counting executions.</summary>
    internal static (CompiledGraph<Rollout> Graph, Func<int> Executed) RolloutGraph(CompilationOptions? options = null)
    {
        var executed = 0;
        var graph = new StateGraph<Rollout>()
            .AddNode("Prepare", s => s with { Log = [.. s.Log, "prepared"] })
            .AddInterruptBoundary("Prepare")
            .AddNode("Execute", s =>
            {
                Interlocked.Increment(ref executed);
                return s with { Log = [.. s.Log, $"scaled to {s.Replicas}"] };
            })
            .AddEdge("Prepare", "Execute")
            .Compile(options ?? new CompilationOptions());
        return (graph, () => Volatile.Read(ref executed));
    }

    [Fact]
    public void Sync_members_are_not_supported()
    {
        using var store = new StrataCloudCheckpointStore(CloudTest.Options(), new HttpClient(new ScriptedCloud()));
        Assert.Throws<NotSupportedException>(() => store.Load("t"));
        Assert.Throws<NotSupportedException>(() => store.Save(Checkpoint.FromWireJson(JsonNode.Parse(
            """{"schema_version":"1.0.0","thread_id":"t","status":"interrupted","frozen_state":{},"created_at":"2026-10-09T12:00:00Z"}""")!)));
    }

    [Fact]
    public async Task A_run_saved_to_cloud_and_resumed_from_it_ends_as_on_the_in_memory_store()
    {
        var (graph, executed) = RolloutGraph();
        var cloud = new EmulatedCloud();
        using var store = new StrataCloudCheckpointStore(CloudTest.Options(), new HttpClient(cloud));

        var first = await graph.RunAsync("roll-1", Initial(), store);
        Assert.IsType<GraphExecutionResult<Rollout>.Interrupted>(first);

        // Cloud's copy really is normalised: 1.0 came back as 1, keys reordered.
        var stored = cloud.Latest("roll-1");
        Assert.Equal("1", stored["frozen_state"]!["amount"]!.ToJsonString());
        Assert.Equal(["app", "zone"], stored["frozen_state"]!["labels"]!.AsObject().Select(p => p.Key));

        var signal = HumanSignal.Approve("alice").WithOverride("replicas", 3);
        var resumed = await graph.ResumeAsync("roll-1", signal, store);

        var local = new InMemoryCheckpointStore();
        await graph.RunAsync("roll-1", Initial(), local);
        var expected = await graph.ResumeAsync("roll-1", signal, local);

        var cloudState = Assert.IsType<GraphExecutionResult<Rollout>.Completed>(resumed).State;
        var localState = Assert.IsType<GraphExecutionResult<Rollout>.Completed>(expected).State;
        Assert.Equal(localState.Log, cloudState.Log);
        Assert.Equal(localState.Amount, cloudState.Amount);
        Assert.Equal(9_007_199_254_740_993, cloudState.Big);
        Assert.Equal(localState.Labels.OrderBy(p => p.Key), cloudState.Labels.OrderBy(p => p.Key));
        Assert.Equal(["prepared", "scaled to 3"], cloudState.Log);
        Assert.Equal(2, executed());

        // The resume's stamp is the thread's latest on Cloud: the decision is acknowledged.
        Assert.NotNull(cloud.Latest("roll-1")["resumed_at"]);
    }

    [Fact]
    public async Task The_conditional_save_refuses_a_stale_sequence_and_falls_back_without_the_headers()
    {
        var cloud = new EmulatedCloud();
        using var store = new StrataCloudCheckpointStore(CloudTest.Options(), new HttpClient(cloud));
        var (graph, _) = RolloutGraph();
        await graph.RunAsync("roll-2", Initial(), store);

        var latest = await store.LoadLatestAsync("roll-2");
        Assert.Equal(cloud.Saves("roll-2") - 1, latest!.Sequence);
        var stamp = latest.Checkpoint with { ResumedAt = "2026-10-09T12:00:00Z" };

        Assert.False(await store.SaveIfLatestAsync(stamp, latest.Sequence - 1));
        Assert.Equal(latest.Sequence + 1, cloud.Saves("roll-2"));
        Assert.True(await store.SaveIfLatestAsync(stamp, latest.Sequence));

        // A Cloud without the headers: no sequence on the load, and the expected sequence is ignored.
        cloud.Conditional = false;
        var unsequenced = await store.LoadLatestAsync("roll-2");
        Assert.Null(unsequenced!.Sequence);
        Assert.True(await store.SaveIfLatestAsync(stamp with { ResumedAt = "2026-10-09T12:00:01Z" }, 0));
        Assert.Null(cloud.Requests[^1].Header("strata-sequence"));
    }

    [Fact]
    public async Task A_new_execution_past_the_plan_limit_reaches_the_host_as_an_entitlement_exception()
    {
        var cloud = new ScriptedCloud().Reply(403, CloudTest.Envelope(403, StrataCloudWire.EntitlementExceededCode));
        using var store = new StrataCloudCheckpointStore(CloudTest.Options(), new HttpClient(cloud));
        var (graph, _) = RolloutGraph();

        var refused = await Assert.ThrowsAsync<StrataCloudEntitlementException>(() => graph.RunAsync("roll-3", Initial(), store));

        Assert.Equal(StrataCloudErrorKind.EntitlementExceeded, refused.Kind);
        Assert.Single(cloud.Requests);
    }

    [Fact]
    public async Task An_attempt_that_hangs_times_out_and_is_retried()
    {
        var time = new FakeTimeProvider(DateTimeOffset.Parse("2026-10-09T12:00:00Z", System.Globalization.CultureInfo.InvariantCulture));
        var cloud = new ScriptedCloud()
            .Hang()
            .Reply(201, new JsonObject { ["thread_id"] = "t", ["sequence"] = 0, ["received_at"] = "2026-10-09T12:00:00Z" });
        var options = new StrataCloudOptions
        {
            BaseUrl = CloudTest.BaseUrl,
            ApiKey = CloudTest.ApiKey,
            TimeProvider = time,
        };
        using var store = new StrataCloudCheckpointStore(options, new HttpClient(cloud));
        using var spans = new SpanRecorder();

        var save = store.SaveAsync(Frozen()).AsTask();
        for (var i = 0; i < 200 && !save.IsCompleted; i++)
        {
            await Task.Delay(5);
            time.Advance(TimeSpan.FromSeconds(1));
        }

        await save.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(2, cloud.Requests.Count);
        Assert.Equal("timeout", spans.Completed[0].GetTagItem("error.type"));
        Assert.Equal(201, spans.Completed[1].GetTagItem("http.response.status_code"));
        Assert.Equal(2, spans.Completed[1].GetTagItem("strata.cloud.attempt"));
    }

    [Fact]
    public async Task Computed_waits_are_jittered_under_the_doubling_cap()
    {
        var time = new RecordingTimeProvider();
        var conflict = CloudTest.Envelope(409, "STRATA_CLOUD_API_SEQUENCE_CONFLICT");
        var cloud = new ScriptedCloud().Reply(409, conflict).Reply(409, conflict).Reply(409, conflict).Reply(409, conflict);
        using var store = new StrataCloudCheckpointStore(CloudTest.Options(time), new HttpClient(cloud));

        var failure = await Assert.ThrowsAsync<StrataCloudException>(() => store.SaveAsync(Frozen()).AsTask());

        Assert.Equal(StrataCloudErrorKind.Unavailable, failure.Kind);
        Assert.Equal(4, failure.Attempts);

        // Full jitter can come out as no wait at all, which the clock never sees.
        Assert.InRange(time.Waits.Count, 0, 3);
        Assert.All(time.Waits, wait => Assert.InRange(wait.TotalMilliseconds, 0, 800));
        if (time.Waits.Count == 3)
        {
            Assert.All(time.Waits.Zip([200, 400, 800]), pair => Assert.InRange(pair.First.TotalMilliseconds, 0, pair.Second));
        }
    }

    [Fact]
    public async Task A_retry_after_beyond_the_longest_wait_fails_at_once()
    {
        var time = new RecordingTimeProvider();
        var cloud = new ScriptedCloud().Reply(
            429, CloudTest.Envelope(429, "STRATA_CLOUD_API_RATE_LIMITED"), new Dictionary<string, string> { ["Retry-After"] = "120" });
        using var store = new StrataCloudCheckpointStore(CloudTest.Options(time), new HttpClient(cloud));

        var failure = await Assert.ThrowsAsync<StrataCloudException>(() => store.LoadAsync("t").AsTask());

        Assert.True(failure.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(120), failure.RetryAfter);
        Assert.Empty(time.Waits);
        Assert.Single(cloud.Requests);
    }

    [Fact]
    public async Task A_404_that_is_not_thread_not_found_is_an_error_not_an_empty_thread()
    {
        var cloud = new ScriptedCloud().Reply(404, CloudTest.Envelope(404, "STRATA_CLOUD_API_NOT_FOUND_ERROR"));
        using var store = new StrataCloudCheckpointStore(CloudTest.Options(), new HttpClient(cloud));

        var failure = await Assert.ThrowsAsync<StrataCloudException>(() => store.LoadAsync("t").AsTask());

        Assert.Equal(StrataCloudErrorKind.Rejected, failure.Kind);
        Assert.Equal(404, failure.StatusCode);
    }

    [Fact]
    public async Task The_key_is_on_every_request_and_in_no_span_message_or_string()
    {
        var cloud = new EmulatedCloud();
        var options = CloudTest.Options();
        using var store = new StrataCloudCheckpointStore(options, new HttpClient(cloud));
        using var decisions = new StrataCloudDecisions(options, new HttpClient(cloud));
        using var spans = new SpanRecorder();
        var (graph, _) = RolloutGraph();

        await graph.RunAsync("roll-4", Initial(), store);
        cloud.Decide("roll-4", true, "alice");
        await decisions.GetAsync();
        await graph.ResumeAsync("roll-4", HumanSignal.Approve("alice"), store);
        using var wrong = new StrataCloudCheckpointStore(
            new StrataCloudOptions { BaseUrl = CloudTest.BaseUrl, ApiKey = "strata_live_WRONG" }, new HttpClient(cloud));
        var refused = await Assert.ThrowsAsync<StrataCloudException>(() => wrong.LoadAsync("roll-4").AsTask());

        Assert.All(cloud.Requests.Take(cloud.Requests.Count - 1), r => Assert.Equal($"Bearer {CloudTest.ApiKey}", r.Header("authorization")));
        Assert.NotEmpty(spans.Completed);
        foreach (var span in spans.Completed)
        {
            Assert.All(span.TagObjects, tag => Assert.DoesNotContain(CloudTest.ApiKey, tag.Value?.ToString() ?? string.Empty, StringComparison.Ordinal));
            Assert.DoesNotContain(CloudTest.ApiKey, span.DisplayName, StringComparison.Ordinal);
        }

        Assert.Equal(StrataCloudErrorKind.Unauthorized, refused.Kind);
        Assert.DoesNotContain("strata_live_WRONG", refused.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(CloudTest.ApiKey, options.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Each_attempt_is_a_span_and_each_retry_is_counted()
    {
        var cloud = new ScriptedCloud()
            .Reply(503, CloudTest.Envelope(503, "STRATA_CLOUD_API_DATABASE_ERROR"), new Dictionary<string, string> { ["Retry-After"] = "2" })
            .Reply(201, new JsonObject { ["thread_id"] = "thread_hitl_demo", ["sequence"] = 0, ["received_at"] = "2026-10-09T12:00:00Z" });
        using var store = new StrataCloudCheckpointStore(CloudTest.Options(), new HttpClient(cloud));
        using var spans = new SpanRecorder();
        using var retries = new CounterRecorder("strata.cloud.retries");

        await store.SaveAsync(Frozen());

        Assert.Equal(2, spans.Completed.Count);
        Assert.All(spans.Completed, s => Assert.Equal(StrataCloudTelemetry.RequestSpan, s.OperationName));
        Assert.Equal("checkpoints.save", spans.Completed[0].GetTagItem("strata.cloud.route"));
        Assert.Equal("POST", spans.Completed[0].GetTagItem("http.request.method"));
        Assert.Equal(503, spans.Completed[0].GetTagItem("http.response.status_code"));
        Assert.Equal("503", spans.Completed[0].GetTagItem("error.type"));
        Assert.Equal("thread_hitl_demo", spans.Completed[0].GetTagItem("strata.thread.id"));
        Assert.Equal(1, spans.Completed[0].GetTagItem("strata.cloud.attempt"));
        Assert.Equal(2, spans.Completed[1].GetTagItem("strata.cloud.attempt"));
        var retry = Assert.Single(retries.Measurements);
        Assert.Equal("checkpoints.save", retry.Tags["route"]);
        Assert.Equal("status_503", retry.Tags["reason"]);
    }

    [Fact]
    public async Task A_decisions_poll_gets_a_span_only_when_it_brought_work()
    {
        var empty = new JsonObject { ["items"] = new JsonArray(), ["next_cursor"] = null };
        var work = JsonNode.Parse("""
            {"items":[{"approval_id":"A1","thread_id":"t","sequence":0,"decided_at":"2026-10-09T12:00:00Z",
            "signal":{"is_approved":true,"reviewer_token":"alice","field_overrides":{}}}],"next_cursor":null}
            """)!;
        var cloud = new ScriptedCloud().Reply(200, empty).Reply(200, work);
        using var decisions = new StrataCloudDecisions(CloudTest.Options(), new HttpClient(cloud));
        using var spans = new SpanRecorder();

        await decisions.GetAsync(wait: TimeSpan.FromSeconds(25));
        Assert.Empty(spans.Completed);

        await decisions.GetAsync(wait: TimeSpan.FromSeconds(25));
        var span = Assert.Single(spans.Completed);
        Assert.Equal("decisions", span.GetTagItem("strata.cloud.route"));
        Assert.Equal(1, span.GetTagItem("strata.cloud.items"));
    }

    [Fact]
    public async Task Leases_and_the_listing_get_a_span_per_attempt_under_routes_of_their_own()
    {
        var cloud = new ScriptedCloud()
            .Reply(503, CloudTest.Envelope(503, "STRATA_CLOUD_API_DATABASE_ERROR"))
            .Reply(200, new JsonObject { ["lease"] = null })
            .Reply(200, new JsonObject { ["threads"] = new JsonArray(), ["next_cursor"] = null });
        using var store = new StrataCloudCheckpointStore(CloudTest.Options(), new HttpClient(cloud));
        using var spans = new SpanRecorder();
        using var retries = new CounterRecorder("strata.cloud.retries");

        Assert.Null(await store.GetLeaseAsync("leased-thread"));
        await store.ListThreadsAsync(new ThreadQuery());

        var gets = spans.Completed.Where(s => (string?)s.GetTagItem("strata.cloud.route") == "leases.get").ToList();
        Assert.Equal(2, gets.Count);
        Assert.Equal("GET", gets[1].GetTagItem("http.request.method"));
        Assert.Equal("leased-thread", gets[1].GetTagItem("strata.thread.id"));
        Assert.Equal(2, gets[1].GetTagItem("strata.cloud.attempt"));
        var listing = Assert.Single(spans.Completed, s => (string?)s.GetTagItem("strata.cloud.route") == "threads.list");
        Assert.Null(listing.GetTagItem("strata.thread.id"));
        Assert.Equal("leases.get", Assert.Single(retries.Measurements).Tags["route"]);
    }

    internal static Checkpoint Frozen() => Checkpoint.FromWireJson(JsonNode.Parse(File.ReadAllText(
        Path.Combine(CloudTest.FixturesDir(), "..", "checkpoint-lifecycle", "frozen.json")))!);

    /// <summary>Every <c>Datalium.Strata.Cloud</c> span completed while it is alive, in order.</summary>
    internal sealed class SpanRecorder : IDisposable
    {
        private readonly ActivityListener _listener;
        private readonly List<Activity> _completed = [];

        public SpanRecorder()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == StrataCloudTelemetry.Name,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    lock (_completed)
                    {
                        _completed.Add(activity);
                    }
                },
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public IReadOnlyList<Activity> Completed
        {
            get
            {
                lock (_completed)
                {
                    return [.. _completed];
                }
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>The measurements of one <c>Datalium.Strata.Cloud</c> counter.</summary>
    internal sealed class CounterRecorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly List<(long Value, Dictionary<string, object?> Tags)> _measurements = [];

        public CounterRecorder(string instrument)
        {
            _listener.InstrumentPublished = (published, listener) =>
            {
                if (published.Meter.Name == StrataCloudTelemetry.Name && published.Name == instrument)
                {
                    listener.EnableMeasurementEvents(published);
                }
            };
            _listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                lock (_measurements)
                {
                    _measurements.Add((value, tags.ToArray().ToDictionary(t => t.Key, t => t.Value)));
                }
            });
            _listener.Start();
        }

        public IReadOnlyList<(long Value, Dictionary<string, object?> Tags)> Measurements
        {
            get
            {
                lock (_measurements)
                {
                    return [.. _measurements];
                }
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
