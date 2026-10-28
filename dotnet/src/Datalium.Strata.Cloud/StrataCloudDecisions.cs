using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Cloud;

/// <summary>
/// A decision a reviewer made in the Strata Cloud console, waiting for the runtime: the
/// <see cref="Signal"/> to resume <see cref="ThreadId"/> with. <see cref="Sequence"/> is the interrupted
/// checkpoint it answers, the thread's latest when Cloud offered it.
/// </summary>
/// <remarks>
/// <see cref="HumanSignal.ReviewerToken"/> is the reviewer's own choice, not who decided: an application that
/// lets only some people approve checks <see cref="DecidedBy"/> or <see cref="DecidedByEmail"/>, which the
/// console verified (contract §5, <c>design/30-Strata-Cloud-Store.md</c>).
/// </remarks>
public sealed record CloudDecision(
    string ApprovalId,
    string ThreadId,
    long Sequence,
    DateTimeOffset DecidedAt,
    HumanSignal Signal)
{
    /// <summary>
    /// Who decided, as the console verified it: the IdP user id of a person, <c>idp-apikey:&lt;key id&gt;</c>,
    /// <c>idp-system:&lt;account id&gt;</c>, or <c>dev-console</c> for the development token.
    /// <see langword="null"/> from a Cloud that doesn't send it.
    /// </summary>
    public string? DecidedBy { get; init; }

    /// <summary>
    /// The decider's email as the IdP vouched for it when they decided, exactly as sent (compare it
    /// case-insensitively). <see langword="null"/> for an API key, a system account, the development token, a
    /// person the IdP sent none for, and from a Cloud that doesn't send it.
    /// </summary>
    public string? DecidedByEmail { get; init; }

    /// <summary>The reviewer's notes, or <see langword="null"/> when they wrote none.</summary>
    public string? Notes { get; init; }
}

/// <summary>A page of the decision feed. <see cref="NextCursor"/> is <see langword="null"/> on the last page.</summary>
public sealed record CloudDecisionPage(IReadOnlyList<CloudDecision> Items, string? NextCursor);

/// <summary>
/// The client for Strata Cloud's decision feed, <c>GET /v1/sdk/decisions</c>
/// (<c>design/30-Strata-Cloud-Store.md</c>, "Decisions"). A decision is offered, oldest first, until
/// something is saved to its thread: the resume's own save is the acknowledgement. Most hosts run a
/// <see cref="DecisionWorker{TState}"/> over it rather than calling it themselves.
/// </summary>
public sealed class StrataCloudDecisions : IDisposable
{
    private static readonly TimeSpan LongPollMargin = TimeSpan.FromSeconds(10);

    private readonly CloudTransport _transport;

    /// <inheritdoc cref="StrataCloudCheckpointStore(StrataCloudOptions, HttpClient?)"/>
    public StrataCloudDecisions(StrataCloudOptions options, HttpClient? httpClient = null)
    {
        _transport = new CloudTransport(options, httpClient);
    }

    internal StrataCloudOptions Options => _transport.Options;

    /// <summary>
    /// One request for the decisions waiting now: for one thread when <paramref name="threadId"/> is set
    /// (prefer it when you know the thread), at most <paramref name="limit"/> (Cloud's default 25), after
    /// <paramref name="cursor"/>. With a <paramref name="wait"/> (whole seconds; Cloud holds at most 25) the
    /// request is held until a decision appears or the wait is up, and an empty page is the normal answer:
    /// ask again. The read timeout is the wait plus 10 s.
    ///
    /// Not retried: a <c>429</c>, a <c>5xx</c>, a network error or a timeout is
    /// <see cref="StrataCloudErrorKind.Unavailable"/>, with <see cref="StrataCloudException.RetryAfter"/>
    /// when Cloud said how long to wait.
    /// </summary>
    public async Task<CloudDecisionPage> GetAsync(
        string? threadId = null,
        int? limit = null,
        string? cursor = null,
        TimeSpan? wait = null,
        CancellationToken cancellationToken = default)
    {
        var waitSeconds = wait is { } w && w > TimeSpan.Zero ? (int)Math.Min(999, Math.Ceiling(w.TotalSeconds)) : 0;
        var query = new List<KeyValuePair<string, string>>();
        if (threadId is not null)
        {
            query.Add(new("thread_id", threadId));
        }

        if (limit is { } pageSize)
        {
            query.Add(new("limit", pageSize.ToString(CultureInfo.InvariantCulture)));
        }

        if (cursor is not null)
        {
            query.Add(new("cursor", cursor));
        }

        if (waitSeconds > 0)
        {
            query.Add(new("wait", waitSeconds.ToString(CultureInfo.InvariantCulture)));
        }

        var request = new CloudRequest(
            StrataCloudWire.DecisionsRoute,
            "decisions",
            HttpMethod.Get,
            StrataCloudWire.DecisionsPath,
            query,
            ThreadId: threadId,
            DeferSpan: true);
        var timeout = waitSeconds > 0 ? TimeSpan.FromSeconds(waitSeconds) + LongPollMargin : Options.RequestTimeout;
        var startedUtc = DateTime.UtcNow;
        CloudResponse response;
        try
        {
            response = await _transport
                .SendAsync(request, static _ => false, maxAttempts: 1, timeout, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (StrataCloudException ex)
        {
            CloudTransport.RecordDeferred(request, startedUtc, ex.StatusCode, ex.StatusCode is null ? "network" : null, items: null);
            throw;
        }

        if (response.Status != 200)
        {
            CloudTransport.RecordDeferred(request, response.StartedUtc, response.Status, errorType: null, items: null);
            throw CloudTransport.Failure(request, response);
        }

        CloudDecisionPage page;
        try
        {
            page = Parse(response.Body);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or NullReferenceException or FormatException)
        {
            CloudTransport.RecordDeferred(request, response.StartedUtc, response.Status, "invalid_response", items: null);
            throw CloudTransport.Invalid(request, response, ex);
        }

        // Idle long-polls are the normal case: only a poll that brought work gets a span.
        if (page.Items.Count > 0)
        {
            CloudTransport.RecordDeferred(request, response.StartedUtc, response.Status, errorType: null, page.Items.Count);
        }

        return page;
    }

    public void Dispose() => _transport.Dispose();

    private static CloudDecisionPage Parse(byte[] body)
    {
        var json = JsonNode.Parse(body)!.AsObject();
        var items = json["items"]!.AsArray().Select(item => Decision(item!.AsObject())).ToList();
        return new CloudDecisionPage(items, json["next_cursor"]?.GetValue<string>());
    }

    private static CloudDecision Decision(JsonObject item)
    {
        var signal = item["signal"]!.AsObject();
        var fieldOverrides = signal["field_overrides"] is JsonObject strings
            ? strings.ToDictionary(pair => pair.Key, pair => pair.Value!.GetValue<string>(), StringComparer.Ordinal)
            : [];
        var human = new HumanSignal(
            signal["is_approved"]!.GetValue<bool>(),
            signal["reviewer_token"]?.GetValue<string>() ?? string.Empty,
            fieldOverrides);
        if (signal["overrides"] is JsonObject typed && typed.Count > 0)
        {
            human = human with
            {
                Overrides = typed.ToDictionary(pair => pair.Key, pair => pair.Value?.DeepClone(), StringComparer.Ordinal),
            };
        }

        return new CloudDecision(
            item["approval_id"]!.GetValue<string>(),
            item["thread_id"]!.GetValue<string>(),
            item["sequence"]!.GetValue<long>(),
            DateTimeOffset.Parse(
                item["decided_at"]!.GetValue<string>(),
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal),
            human)
        {
            DecidedBy = item["decided_by"]?.GetValue<string>(),
            DecidedByEmail = item["decided_by_email"]?.GetValue<string>(),
            Notes = item["notes"]?.GetValue<string>(),
        };
    }
}
