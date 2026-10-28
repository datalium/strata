using System.Globalization;
using System.Text.Json.Nodes;
using Datalium.Strata.Cloud;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Tests.Cloud;

/// <summary>
/// The shared wire fixtures (<c>schema/fixtures/cloud/</c>, <c>design/30-Strata-Cloud-Store.md</c>, "Tests"):
/// each is a recorded exchange the adapter must send and the answer it must make of it. Rust's
/// <c>strata-cloud/tests/fixtures.rs</c> reads the same files: that's the parity check.
/// </summary>
[Collection("Cloud")]
public class CloudFixtureTests
{
    public static TheoryData<string> Fixtures()
    {
        var data = new TheoryData<string>();
        foreach (var file in Directory.GetFiles(CloudTest.FixturesDir(), "*.json").Order(StringComparer.Ordinal))
        {
            data.Add(Path.GetFileNameWithoutExtension(file));
        }

        return data;
    }

    [Fact]
    public void Every_fixture_is_read()
    {
        Assert.Equal(41, Fixtures().Count);
    }

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task Fixture(string name)
    {
        var dir = CloudTest.FixturesDir();
        var fixture = JsonNode.Parse(await File.ReadAllTextAsync(Path.Combine(dir, name + ".json")))!.AsObject();
        var time = new RecordingTimeProvider();
        // No jitter, so every wait is exactly the larger of the backoff and Retry-After.
        var options = CloudTest.Options(
            time,
            StrataCloudRetryPolicy.Default with
            {
                Backoff = new RetryBackoff(TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5), BackoffJitter.None),
            },
            fixture["options"]?["gzip_threshold_bytes"]?.GetValue<int>());
        var cloud = new ScriptedCloud();
        foreach (var exchange in fixture["exchanges"]!.AsArray())
        {
            var response = exchange!["response"]!.AsObject();
            if (response["lost"]?.GetValue<bool>() == true)
            {
                cloud.Lose();
                continue;
            }

            var headers = response["headers"]!.AsObject()
                .ToDictionary(pair => pair.Key, pair => pair.Value!.GetValue<string>());
            var body = response["body_file"] is { } file ? ReadJson(dir, file.GetValue<string>()) : response["body"];
            cloud.Reply(response["status"]!.GetValue<int>(), body, headers);
        }

        var http = new HttpClient(cloud);
        var expect = fixture["expect"]!.AsObject();
        Checkpoint? sent = null;
        try
        {
            switch (fixture["operation"]!.GetValue<string>())
            {
                case "save":
                    sent = Checkpoint.FromWireJson(ReadJson(dir, fixture["checkpoint"]!.GetValue<string>()));
                    using (var store = new StrataCloudCheckpointStore(options, http))
                    {
                        bool saved;
                        if (fixture["expected_sequence"] is { } expected)
                        {
                            saved = await store.SaveIfLatestAsync(sent, expected.GetValue<long>());
                        }
                        else
                        {
                            await store.SaveAsync(sent);
                            saved = true;
                        }

                        Assert.Null(expect["error"]);
                        Assert.Equal(expect["saved"]!.GetValue<bool>(), saved);
                    }

                    break;
                case "load":
                    using (var store = new StrataCloudCheckpointStore(options, http))
                    {
                        var latest = await store.LoadLatestAsync(fixture["thread_id"]!.GetValue<string>());
                        Assert.Null(expect["error"]);
                        if (expect["checkpoint"] is null)
                        {
                            Assert.Null(latest);
                        }
                        else
                        {
                            var expected = Checkpoint.FromWireJson(expect["checkpoint"] is JsonValue path
                                ? ReadJson(dir, path.GetValue<string>())
                                : expect["checkpoint"]!.DeepClone());
                            Assert.NotNull(latest);
                            Assert.True(
                                StrataCloudCheckpoints.AreEquivalent(expected, latest.Checkpoint),
                                $"loaded {latest.Checkpoint.ToWireJson().ToJsonString()}");
                            Assert.Equal(expect["sequence"]?.GetValue<long>(), latest.Sequence);
                        }
                    }

                    break;
                case "decisions":
                    using (var decisions = new StrataCloudDecisions(options, http))
                    {
                        var query = fixture["query"]!.AsObject();
                        var page = await decisions.GetAsync(
                            query["thread_id"]?.GetValue<string>(),
                            query["limit"]?.GetValue<int>(),
                            query["cursor"]?.GetValue<string>(),
                            TimeSpan.FromSeconds(query["wait_seconds"]!.GetValue<int>()));
                        Assert.Null(expect["error"]);
                        AssertPage(expect["page"]!.AsObject(), page);
                    }

                    break;
                case "acquire":
                    using (var store = new StrataCloudCheckpointStore(options, http))
                    {
                        var attempt = await store.TryAcquireLeaseAsync(
                            fixture["thread_id"]!.GetValue<string>(), fixture["owner"]!.GetValue<string>(), Ttl(fixture));
                        Assert.Null(expect["error"]);
                        var lease = Lease(expect["lease"]!);
                        Assert.Equal(expect["acquired"]!.GetValue<bool>(), attempt.Acquired);
                        Assert.Equal(lease, attempt.Acquired ? attempt.Lease : attempt.HeldBy);
                    }

                    break;
                case "renew":
                    using (var store = new StrataCloudCheckpointStore(options, http))
                    {
                        var renewed = await store.RenewLeaseAsync(Lease(fixture["lease"]!), Ttl(fixture));
                        Assert.Null(expect["error"]);
                        Assert.Equal(expect["lease"] is { } lease ? Lease(lease) : null, renewed);
                    }

                    break;
                case "release":
                    using (var store = new StrataCloudCheckpointStore(options, http))
                    {
                        var released = await store.ReleaseLeaseAsync(Lease(fixture["lease"]!));
                        Assert.Null(expect["error"]);
                        Assert.Equal(expect["released"]!.GetValue<bool>(), released);
                    }

                    break;
                case "get_lease":
                    using (var store = new StrataCloudCheckpointStore(options, http))
                    {
                        var held = await store.GetLeaseAsync(fixture["thread_id"]!.GetValue<string>());
                        Assert.Null(expect["error"]);
                        Assert.Equal(expect["lease"] is { } lease ? Lease(lease) : null, held);
                    }

                    break;
                case "save_under_lease":
                    sent = Checkpoint.FromWireJson(ReadJson(dir, fixture["checkpoint"]!.GetValue<string>()));
                    using (var store = new StrataCloudCheckpointStore(options, http))
                    {
                        string outcome;
                        try
                        {
                            outcome = await store.SaveUnderLeaseAsync(
                                sent, Lease(fixture["lease"]!), fixture["expected_sequence"]?.GetValue<long>())
                                ? "saved"
                                : "not_latest";
                        }
                        catch (LeaseLostException)
                        {
                            outcome = "lease_lost";
                        }

                        Assert.Null(expect["error"]);
                        Assert.Equal(expect["leased_save"]!.GetValue<string>(), outcome);
                    }

                    break;
                case "list":
                    using (var store = new StrataCloudCheckpointStore(options, http))
                    {
                        var page = await store.ListThreadsAsync(Query(fixture["query"]!.AsObject()));
                        Assert.Null(expect["error"]);
                        var expected = expect["page"]!.AsObject();
                        Assert.Equal(expected["next_cursor"]?.GetValue<string>(), page.NextCursor);
                        var rows = expected["threads"]!.AsArray();
                        Assert.Equal(rows.Count, page.Threads.Count);
                        for (var i = 0; i < rows.Count; i++)
                        {
                            var actual = EmulatedCloud.SummaryJson(page.Threads[i]);
                            Assert.True(JsonNode.DeepEquals(rows[i], actual), $"row {i}: {actual.ToJsonString()}");
                        }
                    }

                    break;
                case "log":
                    using (var store = new StrataCloudCheckpointStore(options, http))
                    {
                        var page = await store.GetLogAsync(
                            fixture["thread_id"]!.GetValue<string>(),
                            fixture["after_sequence"]?.GetValue<long>(),
                            fixture["limit"]!.GetValue<int>());
                        Assert.Null(expect["error"]);
                        Assert.Equal(
                            expect["sequences"]!.AsArray().Select(n => n!.GetValue<long>()),
                            page.Entries.Select(entry => entry.Sequence));
                        var checkpoints = expect["checkpoints"]!.AsArray();
                        for (var i = 0; i < page.Entries.Count; i++)
                        {
                            Assert.True(StrataCloudCheckpoints.AreEquivalent(
                                Checkpoint.FromWireJson(checkpoints[i]!.DeepClone()), page.Entries[i].Checkpoint));
                        }

                        Assert.Equal(expect["next_after_sequence"]?.GetValue<long>(), page.NextAfterSequence);
                    }

                    break;
                default:
                    throw new InvalidOperationException("unknown operation");
            }
        }
        catch (StrataCloudException ex)
        {
            var error = expect["error"]?.AsObject();
            Assert.True(error is not null, $"unexpected {ex.Kind}: {ex.Message}");
            Assert.Equal(error["kind"]!.GetValue<string>(), Snake(ex.Kind));
            Assert.Equal(error["status"]!.GetValue<int>(), ex.StatusCode);
            Assert.Equal(error["code"]!.GetValue<string>(), ex.Code);
            Assert.Equal(error["attempts"]!.GetValue<int>(), ex.Attempts);
            Assert.Equal(error["retry_after_ms"]?.GetValue<long>(), (long?)ex.RetryAfter?.TotalMilliseconds);
            Assert.Equal(ex.Kind == StrataCloudErrorKind.EntitlementExceeded, ex is StrataCloudEntitlementException);
            Assert.DoesNotContain(CloudTest.ApiKey, ex.ToString(), StringComparison.Ordinal);
        }

        AssertRequests(dir, fixture["exchanges"]!.AsArray(), cloud.Requests, sent);

        if (expect["same_header"]?.GetValue<string>() is { } same)
        {
            var first = cloud.Requests[0].Header(same);
            Assert.NotNull(first);
            Assert.All(cloud.Requests, request => Assert.Equal(first, request.Header(same)));
        }

        var minWaits = expect["min_waits_ms"]?.AsArray().Select(w => w!.GetValue<long>()).ToList() ?? [];
        Assert.Equal(minWaits.Count, time.Waits.Count);
        for (var i = 0; i < minWaits.Count; i++)
        {
            Assert.True(time.Waits[i] >= TimeSpan.FromMilliseconds(minWaits[i]), $"wait {i} was {time.Waits[i]}");
            Assert.True(time.Waits[i] <= TimeSpan.FromMilliseconds(Math.Max(minWaits[i], 5000)), $"wait {i} was {time.Waits[i]}");
        }
    }

    private static void AssertRequests(string dir, JsonArray exchanges, List<RecordedRequest> requests, Checkpoint? sent)
    {
        Assert.Equal(exchanges.Count, requests.Count);
        for (var i = 0; i < exchanges.Count; i++)
        {
            var expected = exchanges[i]!["request"]!.AsObject();
            var actual = requests[i];
            Assert.Equal(expected["method"]!.GetValue<string>(), actual.Method);
            Assert.Equal(expected["path"]!.GetValue<string>(), actual.Path);
            // A key whose value is a list is repeated, once per value.
            var query = expected["query"]!.AsObject()
                .SelectMany(pair => pair.Value is JsonArray values
                    ? values.Select(value => KeyValuePair.Create(pair.Key, value!.GetValue<string>()))
                    : [KeyValuePair.Create(pair.Key, pair.Value!.GetValue<string>())])
                .Order(PairOrder.Instance)
                .ToList();
            Assert.Equal(query, actual.QueryPairs.Order(PairOrder.Instance).ToList());
            foreach (var (header, value) in expected["headers"]!.AsObject())
            {
                Assert.Equal(value!.GetValue<string>().Replace("{api_key}", CloudTest.ApiKey, StringComparison.Ordinal), actual.Header(header));
            }

            foreach (var absent in expected["absent_headers"]!.AsArray())
            {
                Assert.Null(actual.Header(absent!.GetValue<string>()));
            }

            foreach (var present in expected["present_headers"]?.AsArray() ?? [])
            {
                Assert.NotNull(actual.Header(present!.GetValue<string>()));
            }

            if (expected["body"] is null)
            {
                Assert.Empty(actual.Body);
            }
            else if (expected["body"] is JsonObject body)
            {
                Assert.True(JsonNode.DeepEquals(body, actual.Json), $"body {actual.Json?.ToJsonString()}");
            }
            else
            {
                Assert.Equal("checkpoint", expected["body"]!.GetValue<string>());
                Assert.True(StrataCloudCheckpoints.AreEquivalent(sent!, Checkpoint.FromWireJson(actual.Json!)));
            }
        }
    }

    private static void AssertPage(JsonObject expected, CloudDecisionPage page)
    {
        Assert.Equal(expected["next_cursor"]?.GetValue<string>(), page.NextCursor);
        var items = expected["items"]!.AsArray();
        Assert.Equal(items.Count, page.Items.Count);
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i]!.AsObject();
            var decision = page.Items[i];
            Assert.Equal(item["approval_id"]!.GetValue<string>(), decision.ApprovalId);
            Assert.Equal(item["thread_id"]!.GetValue<string>(), decision.ThreadId);
            Assert.Equal(item["sequence"]!.GetValue<long>(), decision.Sequence);
            Assert.Equal(
                DateTimeOffset.Parse(item["decided_at"]!.GetValue<string>(), CultureInfo.InvariantCulture),
                decision.DecidedAt);
            Assert.Equal(TimeSpan.Zero, decision.DecidedAt.Offset);
            Assert.Equal(item["decided_by"]?.GetValue<string>(), decision.DecidedBy);
            Assert.Equal(item["decided_by_email"]?.GetValue<string>(), decision.DecidedByEmail);
            Assert.Equal(item["notes"]?.GetValue<string>(), decision.Notes);
            var signal = item["signal"]!.AsObject();
            Assert.Equal(signal["is_approved"]!.GetValue<bool>(), decision.Signal.IsApproved);
            Assert.Equal(signal["reviewer_token"]!.GetValue<string>(), decision.Signal.ReviewerToken);
            Assert.Equal(
                signal["field_overrides"]!.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<string>()).OrderBy(p => p.Key),
                (decision.Signal.FieldOverrides ?? []).OrderBy(p => p.Key));
            var overrides = signal["overrides"]!.AsObject();
            var actual = decision.Signal.Overrides ?? new Dictionary<string, JsonNode?>();
            Assert.Equal(overrides.Count, actual.Count);
            foreach (var (key, value) in overrides)
            {
                Assert.True(JsonNode.DeepEquals(value, actual[key]), $"override '{key}'");
            }
        }
    }

    private static TimeSpan Ttl(JsonObject fixture) => TimeSpan.FromMilliseconds(fixture["ttl_ms"]!.GetValue<long>());

    private static DateTimeOffset Time(JsonNode node) =>
        DateTimeOffset.Parse(node.GetValue<string>(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

    private static ThreadLease Lease(JsonNode node) => new(
        node["thread_id"]!.GetValue<string>(),
        node["owner"]!.GetValue<string>(),
        node["token"]!.GetValue<long>(),
        Time(node["expires_at"]!));

    private static ThreadQuery Query(JsonObject json) => new()
    {
        States = json["states"]?.AsArray().Select(state => CheckpointThreadStates.Parse(state!.GetValue<string>())).ToHashSet(),
        Metadata = json["metadata"]?.AsObject().ToDictionary(pair => pair.Key, pair => pair.Value!.GetValue<string>())
            ?? new Dictionary<string, string>(),
        UpdatedAfter = json["updated_after"] is { } after ? Time(after) : null,
        UpdatedBefore = json["updated_before"] is { } before ? Time(before) : null,
        DueBy = json["due_by"] is { } due ? Time(due) : null,
        GraphId = json["graph_id"]?.GetValue<string>(),
        GraphVersion = json["graph_version"]?.GetValue<string>(),
        Limit = json["limit"]?.GetValue<int>() ?? ThreadQuery.DefaultLimit,
        Cursor = json["cursor"]?.GetValue<string>(),
    };

    private sealed class PairOrder : IComparer<KeyValuePair<string, string>>
    {
        public static readonly PairOrder Instance = new();

        public int Compare(KeyValuePair<string, string> x, KeyValuePair<string, string> y)
        {
            var byKey = string.CompareOrdinal(x.Key, y.Key);
            return byKey != 0 ? byKey : string.CompareOrdinal(x.Value, y.Value);
        }
    }

    private static JsonNode ReadJson(string dir, string relative) =>
        JsonNode.Parse(File.ReadAllText(Path.Combine(dir, relative)))!;

    private static string Snake(StrataCloudErrorKind kind) => kind switch
    {
        StrataCloudErrorKind.Unauthorized => "unauthorized",
        StrataCloudErrorKind.EntitlementExceeded => "entitlement_exceeded",
        StrataCloudErrorKind.Rejected => "rejected",
        StrataCloudErrorKind.Unavailable => "unavailable",
        StrataCloudErrorKind.InvalidResponse => "invalid_response",
        _ => kind.ToString(),
    };
}
