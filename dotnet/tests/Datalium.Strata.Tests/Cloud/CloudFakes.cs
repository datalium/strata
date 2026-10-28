using System.Globalization;
using System.IO.Compression;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Cloud;

namespace Datalium.Strata.Tests.Cloud;

/// <summary>A request as the fake saw it: headers lower-cased, the body decompressed.</summary>
internal sealed record RecordedRequest(
    string Method,
    string Path,
    Dictionary<string, string> Query,
    Dictionary<string, string> Headers,
    byte[] Body,
    List<KeyValuePair<string, string>> QueryPairs)
{
    public JsonNode? Json => Body.Length == 0 ? null : JsonNode.Parse(Body);

    public string? Header(string name) => Headers.TryGetValue(name.ToLowerInvariant(), out var value) ? value : null;
}

/// <summary>Helpers shared by the Cloud tests.</summary>
internal static class CloudTest
{
    public const string ApiKey = "strata_live_TESTKEY0123456789abcdefghijklmnopqrstuvwxyzABCDEF";

    public static readonly Uri BaseUrl = new("https://cloud.test/");

    public static StrataCloudOptions Options(TimeProvider? time = null, StrataCloudRetryPolicy? retry = null, int? gzipThreshold = null) => new()
    {
        BaseUrl = BaseUrl,
        ApiKey = ApiKey,
        RequestTimeout = TimeSpan.FromHours(1),
        TimeProvider = time ?? new RecordingTimeProvider(),
        Retry = retry ?? StrataCloudRetryPolicy.Default,
        GzipThresholdBytes = gzipThreshold ?? 64 * 1024,
    };

    public static string FixturesDir([CallerFilePath] string sourceFilePath = "") =>
        Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(sourceFilePath)!, "..", "..", "..", "..", "schema", "fixtures", "cloud"));

    public static async Task<RecordedRequest> Record(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, values) in request.Headers)
        {
            headers[name.ToLowerInvariant()] = string.Join(",", values);
        }

        var body = Array.Empty<byte>();
        if (request.Content is not null)
        {
            foreach (var (name, values) in request.Content.Headers)
            {
                headers[name.ToLowerInvariant()] = string.Join(",", values);
            }

            body = await request.Content.ReadAsByteArrayAsync(cancellationToken);
            if (headers.TryGetValue("content-encoding", out var encoding) && encoding == "gzip")
            {
                using var gzip = new GZipStream(new MemoryStream(body), CompressionMode.Decompress);
                using var plain = new MemoryStream();
                await gzip.CopyToAsync(plain, cancellationToken);
                body = plain.ToArray();
            }
        }

        var uri = request.RequestUri!;
        var query = new Dictionary<string, string>(StringComparer.Ordinal);
        var pairs = new List<KeyValuePair<string, string>>();
        foreach (var part in uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var pair = part.Split('=', 2);
            var key = Uri.UnescapeDataString(pair[0]);
            var value = Uri.UnescapeDataString(pair.Length > 1 ? pair[1] : string.Empty);
            query[key] = value;
            pairs.Add(new(key, value));
        }

        return new RecordedRequest(request.Method.Method, uri.AbsolutePath, query, headers, body, pairs);
    }

    public static HttpResponseMessage Response(int status, JsonNode? body, IReadOnlyDictionary<string, string>? headers = null)
    {
        var response = new HttpResponseMessage((HttpStatusCode)status);
        if (body is not null)
        {
            response.Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json");
        }

        foreach (var (name, value) in headers ?? new Dictionary<string, string>())
        {
            if (!response.Headers.TryAddWithoutValidation(name, value))
            {
                response.Content ??= new ByteArrayContent([]);
                response.Content.Headers.TryAddWithoutValidation(name, value);
            }
        }

        return response;
    }

    public static JsonNode Envelope(int status, string code) => new JsonObject
    {
        ["status"] = "FAILED",
        ["code"] = code,
        ["error"] = "test",
        ["timestamp"] = "2026-10-09T12:00:00.000Z",
        ["statusCode"] = status,
    };
}

/// <summary>An <see cref="HttpMessageHandler"/> answering from a script, one entry per request.</summary>
internal sealed class ScriptedCloud : HttpMessageHandler
{
    private readonly Queue<Func<RecordedRequest, CancellationToken, Task<HttpResponseMessage>>> _script = new();

    public List<RecordedRequest> Requests { get; } = [];

    public ScriptedCloud Reply(int status, JsonNode? body = null, IReadOnlyDictionary<string, string>? headers = null)
    {
        _script.Enqueue((_, _) => Task.FromResult(CloudTest.Response(status, body?.DeepClone(), headers)));
        return this;
    }

    /// <summary>The request reaches the fake, and its response is lost.</summary>
    public ScriptedCloud Lose()
    {
        _script.Enqueue((_, _) => throw new HttpRequestException("the connection was reset"));
        return this;
    }

    /// <summary>The request never gets an answer: only its timeout ends it.</summary>
    public ScriptedCloud Hang()
    {
        _script.Enqueue(async (_, cancellationToken) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            throw new InvalidOperationException("unreachable");
        });
        return this;
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var recorded = await CloudTest.Record(request, cancellationToken);
        Requests.Add(recorded);
        if (!_script.TryDequeue(out var next))
        {
            throw new InvalidOperationException($"unexpected request {recorded.Method} {recorded.Path}");
        }

        return await next(recorded, cancellationToken);
    }
}

/// <summary>
/// A <see cref="TimeProvider"/> that records every wait shorter than <see cref="Horizon"/> and fires it at
/// once; longer ones (the attempt timeouts, an hour in these tests, and a long-poll's 35 s read timeout)
/// run on the real clock. Retry waits are at most 30 s, the longest Retry-After a call waits for. So a test sees
/// exactly the waits the client asked for, without waiting them.
/// </summary>
internal sealed class RecordingTimeProvider(TimeSpan? horizon = null) : TimeProvider
{
    private readonly List<TimeSpan> _waits = [];

    public TimeSpan Horizon { get; } = horizon ?? TimeSpan.FromSeconds(31);

    public IReadOnlyList<TimeSpan> Waits
    {
        get
        {
            lock (_waits)
            {
                return [.. _waits];
            }
        }
    }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        if (dueTime == Timeout.InfiniteTimeSpan || dueTime >= Horizon)
        {
            return System.CreateTimer(callback, state, dueTime, period);
        }

        lock (_waits)
        {
            _waits.Add(dueTime);
        }

        return System.CreateTimer(callback, state, TimeSpan.Zero, period);
    }
}

/// <summary>
/// A small Strata Cloud: threads of checkpoints stored the way Cloud stores them (keys reordered, numbers
/// printed as JavaScript prints them, timestamps at whole seconds), the idempotent <c>200</c>, approvals
/// decided by the test, the decision feed, the conditional save when <see cref="Conditional"/> is on, leases and
/// the fenced save (contract §11), and the listing and the log (§12), whose cursor is its own.
/// </summary>
internal sealed class EmulatedCloud : HttpMessageHandler
{
    private const string CursorPrefix = "emulated:";

    private readonly object _lock = new();
    private readonly Dictionary<string, List<JsonObject>> _threads = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<long?>> _tokens = new(StringComparer.Ordinal);
    private readonly Dictionary<string, LeaseRow> _leases = new(StringComparer.Ordinal);
    private readonly List<(string ApprovalId, string ThreadId, long Sequence, JsonObject Signal)> _decisions = [];
    private int _approvals;

    /// <summary>Whether loads report <c>Strata-Sequence</c> and saves honour <c>Strata-If-Sequence</c>.</summary>
    public bool Conditional { get; set; } = true;

    /// <summary>
    /// At most this many threads a listing page, and a listing's first page none: Cloud's short pages
    /// (contract §12).
    /// </summary>
    public int? ListCap { get; set; }

    /// <summary>At most this many entries a log page: Cloud's early stop.</summary>
    public int? LogCap { get; set; }

    public List<RecordedRequest> Requests { get; } = [];

    /// <summary>Called on every load after it has read the head and before it answers: tests interleave two workers here.</summary>
    public Func<Task>? BeforeLoad { get; set; }

    public int Saves(string threadId)
    {
        lock (_lock)
        {
            return _threads.TryGetValue(threadId, out var log) ? log.Count : 0;
        }
    }

    public JsonObject Latest(string threadId)
    {
        lock (_lock)
        {
            return _threads[threadId][^1];
        }
    }

    /// <summary>
    /// Another owner takes the thread over, as if its lease had run out and been acquired: the next token, held
    /// for a minute.
    /// </summary>
    public long TakeOver(string threadId, string owner)
    {
        lock (_lock)
        {
            var token = (_leases.TryGetValue(threadId, out var row) ? row.Token : 0) + 1;
            _leases[threadId] = new LeaseRow(token, owner, DateTimeOffset.UtcNow.AddMinutes(1), Released: false, Key: null);
            return token;
        }
    }

    /// <summary>The thread's lease as Cloud keeps it (released or not), or <see langword="null"/>.</summary>
    public (long Token, string Owner, bool Released)? Lease(string threadId)
    {
        lock (_lock)
        {
            return _leases.TryGetValue(threadId, out var row) ? (row.Token, row.Owner, row.Released) : null;
        }
    }

    /// <summary>Stores <paramref name="checkpoint"/> as the thread's latest, as a save would.</summary>
    public void Seed(Datalium.Strata.Interruption.Checkpoint checkpoint)
    {
        lock (_lock)
        {
            Log(checkpoint.ThreadId).Add(Normalize(checkpoint.ToWireJson())!.AsObject());
            Tokens(checkpoint.ThreadId).Add(null);
        }
    }

    /// <summary>A reviewer decides the thread's current approval (its latest, interrupted checkpoint).</summary>
    public string Decide(string threadId, bool approved, string reviewer, JsonObject? fieldOverrides = null, JsonObject? overrides = null)
    {
        lock (_lock)
        {
            var sequence = _threads[threadId].Count - 1;
            var approvalId = $"A{++_approvals:D4}";
            var signal = new JsonObject
            {
                ["is_approved"] = approved,
                ["reviewer_token"] = reviewer,
                ["field_overrides"] = fieldOverrides ?? new JsonObject(),
            };
            if (overrides is not null)
            {
                signal["overrides"] = overrides;
            }

            _decisions.Add((approvalId, threadId, sequence, signal));
            return approvalId;
        }
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var recorded = await CloudTest.Record(request, cancellationToken);
        lock (_lock)
        {
            Requests.Add(recorded);
        }

        if (recorded.Header("authorization") != $"Bearer {CloudTest.ApiKey}")
        {
            return CloudTest.Response(401, CloudTest.Envelope(401, "STRATA_CLOUD_API_UNAUTHORIZED_ERROR"));
        }

        switch (recorded.Path)
        {
            case "/v1/sdk/checkpoints" when recorded.Method == "POST":
                return Save(recorded);
            case "/v1/sdk/checkpoints" when recorded.Method == "GET":
                return ReadLog(recorded.Query);
            case "/v1/sdk/checkpoints/latest":
                // The head is read first and the answer held: two loads released together saw the same head.
                var loaded = Load(recorded.Query["thread_id"]);
                if (BeforeLoad is { } beforeLoad)
                {
                    await beforeLoad();
                }

                return loaded;
            case "/v1/sdk/decisions":
                return Feed(recorded.Query);
            case "/v1/sdk/leases":
                return Leases(recorded);
            case "/v1/sdk/threads":
                return List(recorded);
            default:
                return CloudTest.Response(404, CloudTest.Envelope(404, "STRATA_CLOUD_API_NOT_FOUND_ERROR"));
        }
    }

    private List<JsonObject> Log(string threadId)
    {
        if (!_threads.TryGetValue(threadId, out var log))
        {
            log = [];
            _threads[threadId] = log;
        }

        return log;
    }

    private List<long?> Tokens(string threadId)
    {
        if (!_tokens.TryGetValue(threadId, out var tokens))
        {
            tokens = [];
            _tokens[threadId] = tokens;
        }

        return tokens;
    }

    private HttpResponseMessage Save(RecordedRequest request)
    {
        var stored = Normalize(request.Json!)!.AsObject();
        var threadId = stored["thread_id"]!.GetValue<string>();
        long? token = request.Header("strata-lease-token") is { } header ? long.Parse(header, CultureInfo.InvariantCulture) : null;
        lock (_lock)
        {
            // The lease first: a token that isn't the thread's latest stores nothing.
            if (token is { } leased && (!_leases.TryGetValue(threadId, out var row) || row.Token != leased))
            {
                return CloudTest.Response(423, CloudTest.Envelope(423, "STRATA_CLOUD_API_LEASE_LOST"));
            }

            var log = Log(threadId);
            var tokens = Tokens(threadId);
            // The precondition next: two resume stamps made in the same second are identical, and the second must
            // be refused, not taken for the first one's retry (design/30, "The conditional save"). Under a lease,
            // our own checkpoint at the head, saved under our token, is the retry's 200.
            if (Conditional && request.Header("strata-if-sequence") is { } expected
                && long.Parse(expected, CultureInfo.InvariantCulture) != log.Count - 1)
            {
                if (token is not null && log.Count > 0 && JsonNode.DeepEquals(log[^1], stored) && tokens[^1] == token)
                {
                    return CloudTest.Response(200, Receipt(threadId, log.Count - 1));
                }

                return CloudTest.Response(412, CloudTest.Envelope(412, "STRATA_CLOUD_API_PRECONDITION_FAILED"));
            }

            if (log.Count > 0 && JsonNode.DeepEquals(log[^1], stored))
            {
                return CloudTest.Response(200, Receipt(threadId, log.Count - 1));
            }

            log.Add(stored);
            tokens.Add(token);
            return CloudTest.Response(201, Receipt(threadId, log.Count - 1));
        }
    }

    private HttpResponseMessage Load(string threadId)
    {
        lock (_lock)
        {
            if (!_threads.TryGetValue(threadId, out var log) || log.Count == 0)
            {
                return CloudTest.Response(404, CloudTest.Envelope(404, "STRATA_CLOUD_API_THREAD_NOT_FOUND"));
            }

            var headers = Conditional
                ? new Dictionary<string, string> { ["Strata-Sequence"] = (log.Count - 1).ToString(CultureInfo.InvariantCulture) }
                : null;
            return CloudTest.Response(200, log[^1].DeepClone(), headers);
        }
    }

    private HttpResponseMessage Leases(RecordedRequest request)
    {
        var now = DateTimeOffset.UtcNow;
        lock (_lock)
        {
            switch (request.Method)
            {
                case "POST":
                    {
                        var body = request.Json!.AsObject();
                        var threadId = body["thread_id"]!.GetValue<string>();
                        var key = request.Header("idempotency-key");
                        if (_leases.TryGetValue(threadId, out var row))
                        {
                            var mine = key is not null && row.Key == key && !row.Released;
                            if (mine || row.Held(now))
                            {
                                return CloudTest.Response(200, new JsonObject { ["acquired"] = mine, ["lease"] = row.Json(threadId) });
                            }
                        }

                        var acquired = new LeaseRow(
                            (row?.Token ?? 0) + 1,
                            body["owner"]!.GetValue<string>(),
                            now.AddMilliseconds(body["ttl_ms"]!.GetValue<long>()),
                            Released: false,
                            key);
                        _leases[threadId] = acquired;
                        return CloudTest.Response(200, new JsonObject { ["acquired"] = true, ["lease"] = acquired.Json(threadId) });
                    }

                case "PUT":
                    {
                        var body = request.Json!.AsObject();
                        var threadId = body["thread_id"]!.GetValue<string>();
                        if (_leases.TryGetValue(threadId, out var row) && row.Token == body["token"]!.GetValue<long>())
                        {
                            var renewed = row with
                            {
                                Owner = body["owner"]!.GetValue<string>(),
                                ExpiresAt = now.AddMilliseconds(body["ttl_ms"]!.GetValue<long>()),
                                Released = false,
                            };
                            _leases[threadId] = renewed;
                            return CloudTest.Response(200, new JsonObject { ["lease"] = renewed.Json(threadId) });
                        }

                        return CloudTest.Response(200, new JsonObject { ["lease"] = null });
                    }

                case "DELETE":
                    {
                        var threadId = request.Query["thread_id"];
                        var token = long.Parse(request.Query["token"], CultureInfo.InvariantCulture);
                        if (_leases.TryGetValue(threadId, out var row) && row.Token == token)
                        {
                            _leases[threadId] = row with { Released = true };
                            return CloudTest.Response(200, new JsonObject { ["released"] = true });
                        }

                        return CloudTest.Response(200, new JsonObject { ["released"] = false });
                    }

                default:
                    {
                        var threadId = request.Query["thread_id"];
                        var held = _leases.TryGetValue(threadId, out var row) && row.Held(now) ? row.Json(threadId) : null;
                        return CloudTest.Response(200, new JsonObject { ["lease"] = held });
                    }
            }
        }
    }

    /// <summary>
    /// <c>GET /v1/sdk/threads</c>, filtered and ordered by the engine's own rules, with a cursor of the emulator's
    /// own, which the adapter must pass back as it came.
    /// </summary>
    private HttpResponseMessage List(RecordedRequest request)
    {
        static DateTimeOffset Time(string text) =>
            DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

        var states = new HashSet<Datalium.Strata.Interruption.CheckpointThreadState>();
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal);
        var query = new Datalium.Strata.Interruption.ThreadQuery();
        Datalium.Strata.Interruption.ThreadCursor? cursor = null;
        foreach (var (key, value) in request.QueryPairs)
        {
            switch (key)
            {
                case "state":
                    states.Add(Datalium.Strata.Interruption.CheckpointThreadStates.Parse(value));
                    break;
                case "due_by":
                    query = query with { DueBy = Time(value) };
                    break;
                case "updated_after":
                    query = query with { UpdatedAfter = Time(value) };
                    break;
                case "updated_before":
                    query = query with { UpdatedBefore = Time(value) };
                    break;
                case "graph_id":
                    query = query with { GraphId = value };
                    break;
                case "graph_version":
                    query = query with { GraphVersion = value };
                    break;
                case "limit":
                    query = query with { Limit = int.Parse(value, CultureInfo.InvariantCulture) };
                    break;
                case "cursor" when value.StartsWith(CursorPrefix, StringComparison.Ordinal):
                    cursor = Datalium.Strata.Interruption.ThreadCursor.Decode(value[CursorPrefix.Length..]);
                    break;
                case "cursor":
                    return CloudTest.Response(400, CloudTest.Envelope(400, "STRATA_CLOUD_API_VALIDATION_ERROR"));
                case var meta when meta.StartsWith("meta.", StringComparison.Ordinal):
                    metadata[meta["meta.".Length..]] = value;
                    break;
                default:
                    return CloudTest.Response(422, CloudTest.Envelope(422, "STRATA_CLOUD_API_VALIDATION_ERROR"));
            }
        }

        query = query with { States = states, Metadata = metadata };
        lock (_lock)
        {
            var heads = _threads.Values
                .Where(log => log.Count > 0)
                .Select(log => (Sequence: (long)log.Count - 1, Checkpoint: Datalium.Strata.Interruption.Checkpoint.FromWireJson(log[^1])))
                .Where(head => query.Matches(head.Checkpoint))
                .Select(head => (head.Sequence, head.Checkpoint, UpdatedAt: Datalium.Strata.Interruption.CheckpointTimestamps.UpdatedAt(head.Checkpoint)))
                .OrderByDescending(head => head.UpdatedAt, StringComparer.Ordinal)
                .ThenByDescending(head => head.Checkpoint.ThreadId, StringComparer.Ordinal)
                .Where(head => cursor is null || cursor.Admits(head.UpdatedAt, head.Checkpoint.ThreadId))
                .ToList();
            // A listing's first page is short and empty.
            var take = ListCap is { } cap ? (cursor is null ? 0 : Math.Min(cap, query.Limit)) : query.Limit;
            var page = heads.Take(take).ToList();
            string? next = null;
            if (page.Count < heads.Count)
            {
                // An empty page continues from where it started: before the first thread.
                var last = page.Count > 0
                    ? new Datalium.Strata.Interruption.ThreadCursor(page[^1].UpdatedAt, page[^1].Checkpoint.ThreadId)
                    : new Datalium.Strata.Interruption.ThreadCursor("9999", string.Empty);
                next = CursorPrefix + last.Encode();
            }

            return CloudTest.Response(200, new JsonObject
            {
                ["threads"] = new JsonArray(page
                    .Select(head => (JsonNode)SummaryJson(Datalium.Strata.Interruption.ThreadSummary.Of(head.Sequence, head.Checkpoint)))
                    .ToArray()),
                ["next_cursor"] = next,
            });
        }
    }

    private HttpResponseMessage ReadLog(Dictionary<string, string> query)
    {
        lock (_lock)
        {
            var after = query.TryGetValue("after_sequence", out var text) ? long.Parse(text, CultureInfo.InvariantCulture) : -1;
            var limit = query.TryGetValue("limit", out var size) ? int.Parse(size, CultureInfo.InvariantCulture) : 100;
            var take = LogCap is { } cap ? Math.Min(cap, limit) : limit;
            var rest = (_threads.TryGetValue(query["thread_id"], out var log) ? log : [])
                .Select((checkpoint, sequence) => (Sequence: (long)sequence, Checkpoint: checkpoint))
                .Where(entry => entry.Sequence > after)
                .ToList();
            var entries = rest.Take(take).ToList();
            return CloudTest.Response(200, new JsonObject
            {
                ["entries"] = new JsonArray(entries
                    .Select(entry => (JsonNode)new JsonObject
                    {
                        ["sequence"] = entry.Sequence,
                        ["checkpoint"] = entry.Checkpoint.DeepClone(),
                    })
                    .ToArray()),
                ["next_after_sequence"] = rest.Count > entries.Count ? entries[^1].Sequence : null,
            });
        }
    }

    /// <summary>A summary in the wire's form (contract §12, as <c>strata thread list --json</c> prints it).</summary>
    public static JsonObject SummaryJson(Datalium.Strata.Interruption.ThreadSummary summary) => new()
    {
        ["thread_id"] = summary.ThreadId,
        ["sequence"] = summary.Sequence,
        ["state"] = Datalium.Strata.Interruption.CheckpointThreadStates.Name(summary.State),
        ["status"] = summary.Status.ToString().ToLowerInvariant(),
        ["suspended_node"] = summary.SuspendedNode,
        ["next_node"] = summary.NextNode,
        ["waiting_at"] = new JsonArray(summary.WaitingAt.Select(node => (JsonNode)node).ToArray()),
        ["metadata"] = new JsonObject(summary.Metadata.Select(pair => KeyValuePair.Create(pair.Key, (JsonNode?)pair.Value))),
        ["updated_at"] = Datalium.Strata.Interruption.CheckpointTimestamps.Text(summary.UpdatedAt),
        ["schema_version"] = summary.SchemaVersion,
        ["wait"] = summary.Wait?.ToWireJson(),
        ["graph"] = summary.Graph?.ToWireJson(),
    };

    private HttpResponseMessage Feed(Dictionary<string, string> query)
    {
        lock (_lock)
        {
            var waiting = _decisions
                .Where(d => !query.TryGetValue("thread_id", out var only) || only == d.ThreadId)
                .Where(d =>
                {
                    var log = _threads[d.ThreadId];
                    var latest = log[^1];
                    return log.Count - 1 == d.Sequence
                        && latest["status"]!.GetValue<string>() == "interrupted"
                        && latest["resumed_at"] is null;
                })
                .ToList();
            var start = query.TryGetValue("cursor", out var cursor)
                ? waiting.FindIndex(d => d.ApprovalId == cursor) + 1
                : 0;
            var limit = query.TryGetValue("limit", out var size) ? int.Parse(size, CultureInfo.InvariantCulture) : 25;
            var page = waiting.Skip(start).Take(limit).ToList();
            var more = start + page.Count < waiting.Count;
            var items = new JsonArray(page.Select(d => (JsonNode)new JsonObject
            {
                ["approval_id"] = d.ApprovalId,
                ["thread_id"] = d.ThreadId,
                ["sequence"] = d.Sequence,
                ["decided_at"] = "2026-10-09T12:00:00Z",
                ["decided_by"] = $"user-{d.Signal["reviewer_token"]}",
                ["decided_by_email"] = $"{d.Signal["reviewer_token"]}@example.com",
                ["notes"] = null,
                ["signal"] = d.Signal.DeepClone(),
            }).ToArray());
            return CloudTest.Response(200, new JsonObject
            {
                ["items"] = items,
                ["next_cursor"] = more ? page[^1].ApprovalId : null,
            });
        }
    }

    /// <summary>A thread's lease on the emulated Cloud (contract §11).</summary>
    private sealed record LeaseRow(long Token, string Owner, DateTimeOffset ExpiresAt, bool Released, string? Key)
    {
        public bool Held(DateTimeOffset now) => !Released && ExpiresAt > now;

        public JsonObject Json(string threadId) => new()
        {
            ["thread_id"] = threadId,
            ["owner"] = Owner,
            ["token"] = Token,
            ["expires_at"] = ExpiresAt.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture),
        };
    }

    private static JsonObject Receipt(string threadId, int sequence) => new()
    {
        ["thread_id"] = threadId,
        ["sequence"] = sequence,
        ["received_at"] = "2026-10-09T12:00:00Z",
    };

    /// <summary>What Cloud's JSONB and timestamp normalisation make of a checkpoint (contract §3).</summary>
    public static JsonNode? Normalize(JsonNode? node, string? key = null)
    {
        switch (node)
        {
            case JsonObject json:
                var sorted = new JsonObject();
                foreach (var (name, value) in json.OrderBy(pair => pair.Key, StringComparer.Ordinal))
                {
                    sorted[name] = Normalize(value, name);
                }

                return sorted;
            case JsonArray array:
                return new JsonArray(array.Select(item => Normalize(item)).ToArray());
            case JsonValue value when value.GetValueKind() == JsonValueKind.Number:
                var text = value.ToJsonString();
                if (decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var number))
                {
                    var plain = (number / 1.0000000000000000000000000000m).ToString(CultureInfo.InvariantCulture);
                    return JsonNode.Parse(plain);
                }

                return JsonNode.Parse(text);
            case JsonValue value when key is "created_at" or "resumed_at" && value.GetValueKind() == JsonValueKind.String:
                var time = DateTimeOffset.Parse(value.GetValue<string>(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
                return JsonValue.Create(time.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture));
            default:
                return node?.DeepClone();
        }
    }
}
