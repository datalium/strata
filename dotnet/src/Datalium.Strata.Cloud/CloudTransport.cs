using System.Diagnostics;
using System.IO.Compression;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

namespace Datalium.Strata.Cloud;

/// <summary>One request to Strata Cloud, as the transport sends it on every attempt.</summary>
internal sealed record CloudRequest(
    string Route,
    string Operation,
    HttpMethod Method,
    string Path,
    IReadOnlyList<KeyValuePair<string, string>> Query,
    byte[]? Body = null,
    long? IfSequence = null,
    string? ThreadId = null,
    bool DeferSpan = false,
    long? LeaseToken = null,
    string? IdempotencyKey = null);

/// <summary>
/// The final answer to a <see cref="CloudRequest"/>: its status, the headers the adapter reads, the body,
/// whether an earlier attempt failed in a way that may have committed it, and when the last attempt began.
/// </summary>
internal sealed record CloudResponse(
    int Status,
    TimeSpan? RetryAfter,
    long? Sequence,
    byte[] Body,
    bool Ambiguous,
    int Attempts,
    DateTime StartedUtc);

/// <summary>
/// The HTTP plumbing <see cref="StrataCloudCheckpointStore"/> and <see cref="StrataCloudDecisions"/> share
/// (<c>design/30-Strata-Cloud-Store.md</c>, "The wire"): authentication, gzip, attempt timeouts on the
/// options' clock, retries with backoff and <c>Retry-After</c>, the error mapping, and a span per attempt.
/// </summary>
internal sealed class CloudTransport : IDisposable
{
    private const string UserAgent = "Datalium.Strata.Cloud/1.0";

    private readonly HttpClient _http;
    private readonly bool _ownsHttp;
    private readonly Uri _base;

    public CloudTransport(StrataCloudOptions options, HttpClient? httpClient)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        Options = options;
        var root = options.BaseUrl.AbsoluteUri;
        _base = new Uri(root.EndsWith('/') ? root : root + "/");
        if (httpClient is null)
        {
            _http = new HttpClient(new SocketsHttpHandler
            {
                ConnectTimeout = options.ConnectTimeout,
                AutomaticDecompression = DecompressionMethods.GZip,
                PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            })
            {
                // Each attempt has its own timeout, on the options' clock.
                Timeout = Timeout.InfiniteTimeSpan,
            };
            _ownsHttp = true;
        }
        else
        {
            _http = httpClient;
        }
    }

    public StrataCloudOptions Options { get; }

    /// <summary>
    /// Sends <paramref name="request"/> up to <paramref name="maxAttempts"/> times while the answer is a
    /// network error, a timeout or a status <paramref name="retryable"/> accepts, waiting between attempts
    /// as <see cref="StrataCloudRetryPolicy"/> says. Returns the first answer that isn't retried (success or
    /// not); throws <see cref="StrataCloudErrorKind.Unavailable"/> when the attempts run out.
    /// </summary>
    public async Task<CloudResponse> SendAsync(
        CloudRequest request, Func<int, bool> retryable, int maxAttempts, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var policy = Options.Retry;
        var ambiguous = false;
        for (var attempt = 1; ; attempt++)
        {
            var startedUtc = DateTime.UtcNow;
            var started = Stopwatch.GetTimestamp();
            using var activity = request.DeferSpan ? null : StartSpan(request, attempt, startedUtc);
            AttemptResult result;
            try
            {
                result = await SendOnceAsync(request, timeout, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                activity?.SetTag("error.type", "cancelled");
                throw;
            }

            var status = result.Status;
            Record(request, activity, status, result.ErrorType, Stopwatch.GetElapsedTime(started));
            if (status is { } final && !retryable(final))
            {
                return new CloudResponse(
                    final, result.RetryAfter, result.Sequence, result.Body, ambiguous, attempt, startedUtc);
            }

            // A network error, a timeout or a gateway's 5xx may have reached Cloud and committed; a 429 or a
            // 503 never did (contract §6, §7).
            ambiguous |= status is null or 408 or 500 or 502 or 504;
            var reason = status is { } code ? $"status_{code}" : result.ErrorType!;
            if (attempt >= maxAttempts || result.RetryAfter > policy.MaxRetryAfter)
            {
                throw Unavailable(request, status, ReadCode(result.Body), result.RetryAfter, attempt, result.Failure, result.Error);
            }

            StrataCloudTelemetry.Retries.Add(
                1, new KeyValuePair<string, object?>("route", request.Route), new KeyValuePair<string, object?>("reason", reason));
            var delay = policy.DelayBefore(attempt, result.RetryAfter, Random.Shared);
            await Task.Delay(delay, Options.TimeProvider, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>The exception for a final answer that isn't a success the caller handles.</summary>
    public static StrataCloudException Failure(CloudRequest request, CloudResponse response)
    {
        var code = ReadCode(response.Body);
        var status = response.Status;
        var detail = Detail(status, code);
        return status switch
        {
            401 => new StrataCloudException(
                StrataCloudErrorKind.Unauthorized,
                $"strata cloud: {request.Operation} unauthorized {detail}: check the API key",
                status, code, response.RetryAfter, response.Attempts),
            403 when code == StrataCloudWire.EntitlementExceededCode => new StrataCloudEntitlementException(
                $"strata cloud: {request.Operation} refused {detail}: the plan's limit of new threads this month is reached",
                code, response.Attempts),
            408 or 429 or >= 500 => Unavailable(request, status, code, response.RetryAfter, response.Attempts, failure: null, error: null),
            _ => new StrataCloudException(
                StrataCloudErrorKind.Rejected,
                $"strata cloud: {request.Operation} refused {detail}",
                status, code, response.RetryAfter, response.Attempts),
        };
    }

    /// <summary>A success whose body the adapter can't read.</summary>
    public static StrataCloudException Invalid(CloudRequest request, CloudResponse response, Exception? error) =>
        new(StrataCloudErrorKind.InvalidResponse,
            $"strata cloud: {request.Operation} gave a response the wire contract doesn't allow (HTTP {response.Status})",
            response.Status, code: null, retryAfter: null, response.Attempts, error);

    /// <summary>Records a span for a deferred request (a decisions poll), started when the request was.</summary>
    public static void RecordDeferred(CloudRequest request, DateTime startedUtc, int? status, string? errorType, int? items)
    {
        using var activity = StartSpan(request, attempt: 1, startedUtc);
        if (activity is null)
        {
            return;
        }

        if (items is { } count)
        {
            activity.SetTag("strata.cloud.items", count);
        }

        Tag(activity, status, errorType);
    }

    public void Dispose()
    {
        if (_ownsHttp)
        {
            _http.Dispose();
        }
    }

    private static StrataCloudException Unavailable(
        CloudRequest request, int? status, string? code, TimeSpan? retryAfter, int attempts, string? failure, Exception? error)
    {
        var detail = status is { } s ? " " + Detail(s, code) : string.Empty;
        var tail = failure is null ? string.Empty : $": {failure}";
        var times = attempts == 1 ? "1 attempt" : $"{attempts} attempts";
        return new StrataCloudException(
            StrataCloudErrorKind.Unavailable,
            $"strata cloud: {request.Operation} unavailable after {times}{detail}{tail}",
            status, code, retryAfter, attempts, error);
    }

    private static string Detail(int status, string? code) => code is null ? $"(HTTP {status})" : $"(HTTP {status} {code})";

    private async Task<AttemptResult> SendOnceAsync(CloudRequest request, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var message = Build(request);
        using var timeoutSource = new CancellationTokenSource(timeout, Options.TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token);
        try
        {
            using var response = await _http
                .SendAsync(message, HttpCompletionOption.ResponseContentRead, linked.Token)
                .ConfigureAwait(false);
            var body = await response.Content.ReadAsByteArrayAsync(linked.Token).ConfigureAwait(false);
            return new AttemptResult(
                (int)response.StatusCode, RetryAfter(response.Headers), Sequence(response.Headers), body, null, null, null);
        }
        catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            // Our attempt timeout, or the caller's HttpClient.Timeout.
            return new AttemptResult(null, null, null, [], "timeout", $"timed out after {timeout.TotalSeconds:0.###} s", ex);
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException)
        {
            return new AttemptResult(null, null, null, [], "network", $"network error: {ex.Message}", ex);
        }
    }

    private HttpRequestMessage Build(CloudRequest request)
    {
        var query = string.Join(
            '&', request.Query.Select(pair => $"{Uri.EscapeDataString(pair.Key)}={Uri.EscapeDataString(pair.Value)}"));
        var message = new HttpRequestMessage(request.Method, new Uri(_base, query.Length == 0 ? request.Path : $"{request.Path}?{query}"));
        message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", Options.ApiKey);
        message.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        message.Headers.UserAgent.ParseAdd(UserAgent);
        if (request.IfSequence is { } expected)
        {
            message.Headers.Add(StrataCloudWire.IfSequenceHeader, expected.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        if (request.LeaseToken is { } token)
        {
            message.Headers.Add(StrataCloudWire.LeaseTokenHeader, token.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        if (request.IdempotencyKey is { } key)
        {
            message.Headers.Add(StrataCloudWire.IdempotencyKeyHeader, key);
        }

        if (request.Body is { } body)
        {
            var gzip = body.Length > Options.GzipThresholdBytes;
            var content = new ByteArrayContent(gzip ? Gzip(body) : body);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            if (gzip)
            {
                content.Headers.ContentEncoding.Add("gzip");
            }

            message.Content = content;
        }

        return message;
    }

    private static byte[] Gzip(byte[] body)
    {
        using var buffer = new MemoryStream();
        using (var gzip = new GZipStream(buffer, CompressionLevel.Fastest, leaveOpen: true))
        {
            gzip.Write(body);
        }

        return buffer.ToArray();
    }

    private TimeSpan? RetryAfter(HttpResponseHeaders headers)
    {
        if (headers.RetryAfter is not { } retryAfter)
        {
            return null;
        }

        if (retryAfter.Delta is { } delta)
        {
            return delta < TimeSpan.Zero ? TimeSpan.Zero : delta;
        }

        if (retryAfter.Date is { } date)
        {
            var wait = date - Options.TimeProvider.GetUtcNow();
            return wait < TimeSpan.Zero ? TimeSpan.Zero : wait;
        }

        return null;
    }

    private static long? Sequence(HttpResponseHeaders headers) =>
        headers.TryGetValues(StrataCloudWire.SequenceHeader, out var values)
        && long.TryParse(values.FirstOrDefault(), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var sequence)
        && sequence >= 0
            ? sequence
            : null;

    /// <summary>The error envelope's <c>code</c>, if the body is one. Nothing else is read from it.</summary>
    private static string? ReadCode(byte[] body)
    {
        if (body.Length == 0)
        {
            return null;
        }

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

    private static Activity? StartSpan(CloudRequest request, int attempt, DateTime startedUtc)
    {
        var activity = StrataCloudTelemetry.Source.StartActivity(
            StrataCloudTelemetry.RequestSpan, ActivityKind.Client, parentContext: default, startTime: startedUtc);
        if (activity is null)
        {
            return null;
        }

        activity.SetTag("strata.cloud.route", request.Route);
        activity.SetTag("http.request.method", request.Method.Method);
        activity.SetTag("strata.cloud.attempt", attempt);
        if (request.ThreadId is { } threadId)
        {
            activity.SetTag("strata.thread.id", threadId);
        }

        return activity;
    }

    private static void Record(CloudRequest request, Activity? activity, int? status, string? errorType, TimeSpan elapsed)
    {
        Tag(activity, status, errorType);
        StrataCloudTelemetry.RequestDuration.Record(
            elapsed.TotalSeconds,
            new KeyValuePair<string, object?>("route", request.Route),
            new KeyValuePair<string, object?>("status", status?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? errorType));
    }

    private static void Tag(Activity? activity, int? status, string? errorType)
    {
        if (activity is null)
        {
            return;
        }

        if (status is { } code)
        {
            activity.SetTag("http.response.status_code", code);
            if (code >= 400)
            {
                activity.SetTag("error.type", code.ToString(System.Globalization.CultureInfo.InvariantCulture));
                activity.SetStatus(ActivityStatusCode.Error);
            }
        }
        else if (errorType is not null)
        {
            activity.SetTag("error.type", errorType);
            activity.SetStatus(ActivityStatusCode.Error);
        }
    }

    private sealed record AttemptResult(
        int? Status, TimeSpan? RetryAfter, long? Sequence, byte[] Body, string? ErrorType, string? Failure, Exception? Error);
}
