using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Cloud;

/// <summary>
/// The checkpoint store backed by Strata Cloud (<c>design/30-Strata-Cloud-Store.md</c>, leases and the query port
/// in <c>design/43-Cloud-Leases-And-Listing.md</c>): a drop-in <see cref="ICheckpointStore"/> and
/// <see cref="ICheckpointQuery"/> over the SDK wire contract v1. A save is <c>POST /v1/sdk/checkpoints</c>,
/// a load <c>GET /v1/sdk/checkpoints/latest</c>; a thread Cloud doesn't know loads as <see langword="null"/>.
///
/// Async only: the engine calls <see cref="SaveAsync"/> and the load members, and <see cref="Save"/> /
/// <see cref="Load"/> throw <see cref="NotSupportedException"/>, as the port allows, rather than block on
/// HTTP. Failures are <see cref="StrataCloudException"/>s, retried as contract §8 says first; a save that
/// would start a thread past the plan's limit is a <see cref="StrataCloudEntitlementException"/>. The
/// engine never passes a run's token to a store call: each attempt is bounded by
/// <see cref="StrataCloudOptions.RequestTimeout"/>.
///
/// <para>
/// <b>Racing resumes.</b> <see cref="LoadLatestAsync"/> reports a sequence, and <see cref="SaveIfLatestAsync"/>
/// is a real compare-and-set, only when Cloud answers with the <see cref="StrataCloudWire.SequenceHeader"/>
/// header. A Cloud without it gives no sequence, so the engine's resume stamp is saved unconditionally:
/// two workers that receive the same decision can both resume the thread (<c>design/28-Ending-A-Thread.md</c>,
/// "Races"). Until Cloud has it, give workers disjoint threads.
/// </para>
///
/// <para>
/// <b>Leases.</b> The five lease members are Cloud's <c>/v1/sdk/leases</c> routes, on Cloud's clock, and
/// <see cref="SaveUnderLeaseAsync"/> sends the lease's token as <see cref="StrataCloudWire.LeaseTokenHeader"/>:
/// Cloud's <c>423</c> is a <see cref="LeaseLostException"/>, never retried. An acquisition sends an
/// <see cref="StrataCloudWire.IdempotencyKeyHeader"/>, new for each call, so a retry after a lost answer gets the
/// lease back.
/// </para>
///
/// <para>
/// <b>The query port.</b> <see cref="ListThreadsAsync"/> is <c>GET /v1/sdk/threads</c>, one request per call,
/// Cloud's page and cursor as they come (a page with a metadata or graph filter may be short while more follow);
/// <see cref="GetLogAsync"/> is <c>GET /v1/sdk/checkpoints</c>, following Cloud's pages until it has the limit or
/// the log ends. There is no <see cref="ICheckpointRetention"/>: purging is Cloud's.
/// </para>
/// </summary>
public sealed class StrataCloudCheckpointStore : ICheckpointStore, ICheckpointQuery, IDisposable
{
    private readonly CloudTransport _transport;

    /// <param name="options">Where Cloud is, the key, timeouts and retries.</param>
    /// <param name="httpClient">
    /// The client to send with, which the caller keeps and disposes; when <see langword="null"/>, the store
    /// creates one with <see cref="StrataCloudOptions.ConnectTimeout"/> and gzip decompression.
    /// </param>
    public StrataCloudCheckpointStore(StrataCloudOptions options, HttpClient? httpClient = null)
    {
        _transport = new CloudTransport(options, httpClient);
    }

    /// <summary>Not supported: the store is async only. The engine calls <see cref="SaveAsync"/>.</summary>
    public void Save(Checkpoint checkpoint) =>
        throw new NotSupportedException("StrataCloudCheckpointStore is async only: call SaveAsync");

    /// <summary>Not supported: the store is async only. The engine calls <see cref="LoadAsync"/>.</summary>
    public Checkpoint? Load(string threadId) =>
        throw new NotSupportedException("StrataCloudCheckpointStore is async only: call LoadAsync");

    /// <summary>Appends <paramref name="checkpoint"/> to its thread; Cloud's idempotent <c>200</c> counts as saved.</summary>
    public async ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
        await SaveCoreAsync(checkpoint, expectedSequence: null, lease: null, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Appends <paramref name="checkpoint"/> only if the thread's latest checkpoint is still at
    /// <paramref name="expectedSequence"/>, sending it as <see cref="StrataCloudWire.IfSequenceHeader"/>:
    /// <c>412</c> is <see langword="false"/>. A Cloud that predates the header ignores it and appends, which
    /// is the port's unconditional default. A <see langword="null"/> sequence saves unconditionally.
    /// </summary>
    public ValueTask<bool> SaveIfLatestAsync(
        Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
        SaveCoreAsync(checkpoint, expectedSequence, lease: null, cancellationToken);

    /// <summary>The thread's latest checkpoint, or <see langword="null"/> when Cloud has none for it.</summary>
    public async ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
        (await LoadLatestAsync(threadId, cancellationToken).ConfigureAwait(false))?.Checkpoint;

    /// <summary>
    /// The thread's latest checkpoint with the sequence Cloud reports in <see cref="StrataCloudWire.SequenceHeader"/>,
    /// or no sequence without it; <see langword="null"/> when Cloud has no checkpoint for the thread.
    /// </summary>
    public async ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(threadId);
        var request = new CloudRequest(
            StrataCloudWire.LoadRoute,
            "load",
            HttpMethod.Get,
            StrataCloudWire.LoadPath,
            [new("thread_id", threadId)],
            ThreadId: threadId);
        var response = await SendAsync(request, ReadRetryable, cancellationToken).ConfigureAwait(false);
        if (response.Status == 404 && ReadCode(response.Body) == StrataCloudWire.ThreadNotFoundCode)
        {
            return null;
        }

        if (response.Status != 200)
        {
            throw CloudTransport.Failure(request, response);
        }

        try
        {
            var checkpoint = Checkpoint.FromWireJson(JsonNode.Parse(response.Body)!);
            return new LatestCheckpoint(checkpoint, response.Sequence);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or NullReferenceException or FormatException)
        {
            throw CloudTransport.Invalid(request, response, ex);
        }
    }

    /// <summary>
    /// Takes <paramref name="threadId"/>'s lease for <paramref name="owner"/> on Cloud, <c>POST /v1/sdk/leases</c>,
    /// with an <see cref="StrataCloudWire.IdempotencyKeyHeader"/> of its own, the same on every attempt: a retry
    /// after a lost answer gets back the lease the first attempt took. A thread Cloud says is held, by anyone (this
    /// owner's name too), is an attempt with <see cref="LeaseAttempt.HeldBy"/> set.
    /// </summary>
    public async ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
        string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(threadId);
        ThreadLeases.Check(owner, ttl);
        var request = new CloudRequest(
            StrataCloudWire.AcquireRoute,
            "acquire a lease",
            HttpMethod.Post,
            StrataCloudWire.LeasesPath,
            [],
            Body: Json(new JsonObject
            {
                ["thread_id"] = threadId,
                ["owner"] = owner,
                ["ttl_ms"] = ThreadLeases.Milliseconds(ttl),
            }),
            ThreadId: threadId,
            IdempotencyKey: Guid.CreateVersion7().ToString());
        var response = await SendAsync(request, ReadRetryable, cancellationToken).ConfigureAwait(false);
        return Read(request, response, json =>
        {
            var lease = Lease(json["lease"]!.AsObject());
            return json["acquired"]!.GetValue<bool>() ? new LeaseAttempt(lease, null) : new LeaseAttempt(null, lease);
        });
    }

    /// <summary>
    /// Extends <paramref name="lease"/> on Cloud, <c>PUT /v1/sdk/leases</c>: the lease as it now is, or
    /// <see langword="null"/> when another owner acquired the thread since.
    /// </summary>
    public async ValueTask<ThreadLease?> RenewLeaseAsync(
        ThreadLease lease, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ThreadLeases.CheckTtl(ttl);
        var request = new CloudRequest(
            StrataCloudWire.RenewRoute,
            "renew a lease",
            HttpMethod.Put,
            StrataCloudWire.LeasesPath,
            [],
            Body: Json(new JsonObject
            {
                ["thread_id"] = lease.ThreadId,
                ["owner"] = lease.Owner,
                ["token"] = lease.Token,
                ["ttl_ms"] = ThreadLeases.Milliseconds(ttl),
            }),
            ThreadId: lease.ThreadId);
        var response = await SendAsync(request, ReadRetryable, cancellationToken).ConfigureAwait(false);
        return Read(request, response, json => json["lease"] is JsonObject renewed ? Lease(renewed) : null);
    }

    /// <summary>Gives <paramref name="lease"/> up on Cloud, <c>DELETE /v1/sdk/leases</c>; says whether it did.</summary>
    public async ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        var request = new CloudRequest(
            StrataCloudWire.ReleaseRoute,
            "release a lease",
            HttpMethod.Delete,
            StrataCloudWire.LeasesPath,
            [new("thread_id", lease.ThreadId), new("token", lease.Token.ToString(CultureInfo.InvariantCulture))],
            ThreadId: lease.ThreadId);
        var response = await SendAsync(request, ReadRetryable, cancellationToken).ConfigureAwait(false);
        return Read(request, response, json => json["released"]!.GetValue<bool>());
    }

    /// <summary>The lease Cloud says <paramref name="threadId"/> is held under now, or <see langword="null"/>.</summary>
    public async ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(threadId);
        var request = new CloudRequest(
            StrataCloudWire.GetLeaseRoute,
            "get a lease",
            HttpMethod.Get,
            StrataCloudWire.LeasesPath,
            [new("thread_id", threadId)],
            ThreadId: threadId);
        var response = await SendAsync(request, ReadRetryable, cancellationToken).ConfigureAwait(false);
        return Read(request, response, json => json["lease"] is JsonObject held ? Lease(held) : null);
    }

    /// <summary>
    /// Appends <paramref name="checkpoint"/> only if <paramref name="lease"/>'s token is still its thread's latest,
    /// sent as <see cref="StrataCloudWire.LeaseTokenHeader"/>: Cloud's <c>423</c> throws
    /// <see cref="LeaseLostException"/>. With <paramref name="expectedSequence"/> it is also conditional, and a
    /// <c>412</c> is <see langword="false"/>: under a lease Cloud answers a retry of our own committed save
    /// <c>200</c>, so a <c>412</c> is always another writer, and nothing is looked up.
    /// </summary>
    public ValueTask<bool> SaveUnderLeaseAsync(
        Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default)
    {
        ThreadLeases.CheckThread(checkpoint, lease);
        return SaveCoreAsync(checkpoint, expectedSequence, lease, cancellationToken);
    }

    /// <summary>
    /// One page of Cloud's thread listing, <c>GET /v1/sdk/threads</c>, with every filter of
    /// <paramref name="query"/> and its cursor passed back as Cloud gave it. The page may be short while more
    /// follow: only a <see langword="null"/> <see cref="ThreadPage.NextCursor"/> ends a listing.
    /// </summary>
    public async ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        // What ThreadQuery.Validate checks, but the cursor: Cloud's is its own, opaque.
        ThreadQuery.CheckLimit(query.Limit);
        if (query.GraphVersion is not null && query.GraphId is null)
        {
            throw new ArgumentException("a thread query's GraphVersion needs a GraphId", nameof(query));
        }

        var parameters = new List<KeyValuePair<string, string>>();
        foreach (var state in query.States ?? new HashSet<CheckpointThreadState>())
        {
            parameters.Add(new("state", state.Name()));
        }

        if (query.DueByText is { } dueBy)
        {
            parameters.Add(new("due_by", dueBy));
        }

        if (query.UpdatedAfterText is { } after)
        {
            parameters.Add(new("updated_after", after));
        }

        if (query.UpdatedBeforeText is { } before)
        {
            parameters.Add(new("updated_before", before));
        }

        foreach (var (key, value) in query.Metadata)
        {
            parameters.Add(new("meta." + key, value));
        }

        if (query.GraphId is { } graphId)
        {
            parameters.Add(new("graph_id", graphId));
        }

        if (query.GraphVersion is { } graphVersion)
        {
            parameters.Add(new("graph_version", graphVersion));
        }

        parameters.Add(new("limit", query.Limit.ToString(CultureInfo.InvariantCulture)));
        if (query.Cursor is { } cursor)
        {
            parameters.Add(new("cursor", cursor));
        }

        var request = new CloudRequest(
            StrataCloudWire.ThreadsRoute, "list threads", HttpMethod.Get, StrataCloudWire.ThreadsPath, parameters);
        var response = await SendAsync(request, ReadRetryable, cancellationToken).ConfigureAwait(false);
        return Read(request, response, json => new ThreadPage(
            json["threads"]!.AsArray().Select(row => Summary(row!.AsObject())).ToList(),
            json["next_cursor"]?.GetValue<string>()));
    }

    /// <summary>
    /// Up to <paramref name="limit"/> of the thread's checkpoints after <paramref name="afterSequence"/>, oldest
    /// first, from <c>GET /v1/sdk/checkpoints</c>. Cloud may stop a page early (at about 4 MB of stored state) with
    /// more to come: this follows it until the page is full or the log ends.
    /// </summary>
    public async ValueTask<CheckpointLogPage> GetLogAsync(
        string threadId,
        long? afterSequence = null,
        int limit = ThreadQuery.DefaultLimit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(threadId);
        ThreadQuery.CheckLimit(limit);
        var entries = new List<CheckpointLogEntry>();
        var after = afterSequence;
        while (entries.Count < limit)
        {
            var parameters = new List<KeyValuePair<string, string>> { new("thread_id", threadId) };
            if (after is { } sequence)
            {
                parameters.Add(new("after_sequence", sequence.ToString(CultureInfo.InvariantCulture)));
            }

            parameters.Add(new("limit", (limit - entries.Count).ToString(CultureInfo.InvariantCulture)));
            var request = new CloudRequest(
                StrataCloudWire.LogRoute, "read a log", HttpMethod.Get, StrataCloudWire.LogPath, parameters, ThreadId: threadId);
            var response = await SendAsync(request, ReadRetryable, cancellationToken).ConfigureAwait(false);
            var (page, next) = Read(request, response, json => (
                json["entries"]!.AsArray()
                    .Select(entry => new CheckpointLogEntry(
                        entry!["sequence"]!.GetValue<long>(), Checkpoint.FromWireJson(entry["checkpoint"]!)))
                    .ToList(),
                json["next_after_sequence"]?.GetValue<long>()));
            entries.AddRange(page);
            if (next is null || page.Count == 0)
            {
                break;
            }

            after = next;
        }

        if (entries.Count > limit)
        {
            entries.RemoveRange(limit, entries.Count - limit);
        }

        return CheckpointLogPage.For(entries, limit);
    }

    public void Dispose() => _transport.Dispose();

    private Task<CloudResponse> SendAsync(CloudRequest request, Func<int, bool> retryable, CancellationToken cancellationToken) =>
        _transport.SendAsync(request, retryable, _transport.Options.Retry.MaxAttempts, _transport.Options.RequestTimeout, cancellationToken);

    /// <summary>The save behind every save member: conditional with an expected sequence, fenced with a lease.</summary>
    private async ValueTask<bool> SaveCoreAsync(
        Checkpoint checkpoint, long? expectedSequence, ThreadLease? lease, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        var request = new CloudRequest(
            StrataCloudWire.SaveRoute,
            "save",
            HttpMethod.Post,
            StrataCloudWire.SavePath,
            [],
            Body: Encoding.UTF8.GetBytes(checkpoint.ToWireJson().ToJsonString()),
            IfSequence: expectedSequence,
            ThreadId: checkpoint.ThreadId,
            LeaseToken: lease?.Token);
        var response = await SendAsync(request, SaveRetryable, cancellationToken).ConfigureAwait(false);
        switch (response.Status)
        {
            case 200 or 201:
                return true;
            case 412 when expectedSequence is { } expected:
                // Unleased, an earlier attempt that may have committed makes this 412 possibly our own write. Under
                // a lease Cloud answers our own committed retry 200, so a 412 is always another writer.
                return lease is null
                    && response.Ambiguous
                    && await IsLatestAsync(checkpoint, expected + 1, cancellationToken).ConfigureAwait(false);
            case 423 when lease is not null && ReadCode(response.Body) == StrataCloudWire.LeaseLostCode:
                throw new LeaseLostException(lease);
            default:
                throw CloudTransport.Failure(request, response);
        }
    }

    /// <summary>Whether the thread's latest checkpoint is <paramref name="checkpoint"/>, at <paramref name="sequence"/>.</summary>
    private async ValueTask<bool> IsLatestAsync(Checkpoint checkpoint, long sequence, CancellationToken cancellationToken)
    {
        var latest = await LoadLatestAsync(checkpoint.ThreadId, cancellationToken).ConfigureAwait(false);
        return latest is not null
            && (latest.Sequence is null || latest.Sequence == sequence)
            && StrataCloudCheckpoints.AreEquivalent(latest.Checkpoint, checkpoint);
    }

    /// <summary>Saves are retried on a <c>409</c> too: Cloud lost a race for a sequence. Never on <c>412</c> or <c>423</c>.</summary>
    private static bool SaveRetryable(int status) => status is 408 or 409 or 429 or 500 or 502 or 503 or 504;

    /// <summary>Every other call: loads, the lease routes and the listing.</summary>
    private static bool ReadRetryable(int status) => status is 408 or 429 or 500 or 502 or 503 or 504;

    /// <summary>A <c>200</c>'s body read by <paramref name="read"/>; any other status is the call's failure.</summary>
    private static T Read<T>(CloudRequest request, CloudResponse response, Func<JsonObject, T> read)
    {
        if (response.Status != 200)
        {
            throw CloudTransport.Failure(request, response);
        }

        try
        {
            return read(JsonNode.Parse(response.Body)!.AsObject());
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or NullReferenceException
            or FormatException or ArgumentException)
        {
            throw CloudTransport.Invalid(request, response, ex);
        }
    }

    private static byte[] Json(JsonObject body) => Encoding.UTF8.GetBytes(body.ToJsonString());

    private static ThreadLease Lease(JsonObject json) => new(
        json["thread_id"]!.GetValue<string>(),
        json["owner"]!.GetValue<string>(),
        json["token"]!.GetValue<long>(),
        Time(json["expires_at"]!.GetValue<string>()));

    /// <summary>A row of <c>GET /v1/sdk/threads</c>: the query port's summary, as <c>strata thread list --json</c> prints it.</summary>
    private static ThreadSummary Summary(JsonObject row) => new(
        row["thread_id"]!.GetValue<string>(),
        row["sequence"]!.GetValue<long>(),
        CheckpointThreadStates.Parse(row["state"]!.GetValue<string>()),
        Status(row["status"]!.GetValue<string>()),
        row["suspended_node"]?.GetValue<string>(),
        row["next_node"]?.GetValue<string>(),
        row["waiting_at"] is JsonArray waitingAt ? waitingAt.Select(node => node!.GetValue<string>()).ToList() : [],
        row["metadata"] is JsonObject metadata
            ? metadata.ToDictionary(pair => pair.Key, pair => pair.Value!.GetValue<string>(), StringComparer.Ordinal)
            : new Dictionary<string, string>(),
        Time(row["updated_at"]!.GetValue<string>()),
        row["schema_version"]!.GetValue<string>())
    {
        Wait = row["wait"] is JsonObject wait ? CheckpointWait.FromWireJson(wait) : null,
        Graph = row["graph"] is JsonObject graph ? GraphIdentity.FromWireJson(graph) : null,
    };

    private static CheckpointStatus Status(string status) => status switch
    {
        "interrupted" => CheckpointStatus.Interrupted,
        "completed" => CheckpointStatus.Completed,
        "failed" => CheckpointStatus.Failed,
        "cancelled" => CheckpointStatus.Cancelled,
        "ended" => CheckpointStatus.Ended,
        "running" => CheckpointStatus.Running,
        _ => throw new FormatException($"unknown checkpoint status '{status}'"),
    };

    private static DateTimeOffset Time(string text) =>
        DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

    /// <summary>The error envelope's <c>code</c>, if the body is one.</summary>
    private static string? ReadCode(byte[] body)
    {
        try
        {
            using var document = JsonDocument.Parse(body);
            return document.RootElement.ValueKind == JsonValueKind.Object
                && document.RootElement.TryGetProperty("code", out var code)
                && code.ValueKind == JsonValueKind.String
                    ? code.GetString()
                    : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
