using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Datalium.Strata.Mcp;

/// <summary>
/// Streamable HTTP, both eras (<c>design/50-MCP-Client.md</c>, "Streamable HTTP"): a POST per message, a JSON or SSE
/// answer, the protocol-version header always, <c>Mcp-Method</c> on modern requests, the legacy session id, and the
/// token hook before each request.
/// </summary>
internal sealed class HttpChannel : McpChannel
{
    private readonly HttpMcpServer _server;
    private readonly HttpClient _http;
    private readonly bool _ownsHttp;
    private volatile bool _closed;

    public HttpChannel(HttpMcpServer server, McpClientOptions options)
        : base(options)
    {
        _server = server;
        if (server.HttpClient is { } given)
        {
            _http = given;
        }
        else
        {
            // No redirects: a token must not follow one to another origin.
            _http = new HttpClient(new SocketsHttpHandler { AllowAutoRedirect = false }) { Timeout = Timeout.InfiniteTimeSpan };
            _ownsHttp = true;
        }
    }

    /// <summary>The version requests are made under; <see langword="null"/> until the era is known.</summary>
    public string? Version { get; set; }

    public bool Modern { get; set; }

    /// <summary>The legacy session, from the <c>initialize</c> response.</summary>
    public string? SessionId { get; private set; }

    /// <summary>Starts a new legacy session, when the server says the old one ended (a 404).</summary>
    public Func<CancellationToken, Task>? Reinitialize { get; set; }

    public override McpEndpointInfo Endpoint =>
        new("tcp", _server.Endpoint.Host, _server.Endpoint.Port, Modern ? null : SessionId);

    public override bool IsOpen => !_closed;

    public override async Task<ChannelAnswer> SendAsync(OutgoingRequest request, bool probe, CancellationToken cancellationToken)
    {
        var (status, response) = await ExchangeAsync(request, cancellationToken).ConfigureAwait(false);
        if (probe)
        {
            return new ChannelAnswer(response, status);
        }

        if (status == 404 && !Modern && SessionId is not null && request.Method != "initialize" && Reinitialize is { } reinitialize)
        {
            SessionId = null;
            await reinitialize(cancellationToken).ConfigureAwait(false);
            (status, response) = await ExchangeAsync(request, cancellationToken).ConfigureAwait(false);
        }

        if (status is >= 200 and <= 299)
        {
            return response is not null
                ? new ChannelAnswer(response, status)
                : throw new McpException($"the MCP server answered {request.Method} with HTTP {status} and no JSON-RPC response");
        }

        if (status is 401 or 403)
        {
            throw new HttpRequestException($"unauthorized ({status})", null, (HttpStatusCode)status);
        }

        if (status is >= 400 and <= 499 && response?.Error is not null)
        {
            return new ChannelAnswer(response, status);
        }

        throw new HttpRequestException($"the MCP server answered {request.Method} with HTTP {status}", null, (HttpStatusCode)status);
    }

    public override async Task AbandonAsync(OutgoingRequest request)
    {
        // Modern: aborting the request was the cancellation. Legacy: closing doesn't cancel, so say it.
        if (Modern || _closed)
        {
            return;
        }

        try
        {
            using var bounded = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await PostNotificationAsync(Cancellation(request.Id), bounded.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is HttpRequestException or OperationCanceledException or IOException)
        {
            // Fire and forget.
        }
    }

    public override Task NotifyAsync(string method, JsonObject? parameters, CancellationToken cancellationToken) =>
        PostNotificationAsync(Notification(method, parameters), cancellationToken);

    public override async ValueTask CloseAsync(Exception reason)
    {
        if (_closed)
        {
            return;
        }

        _closed = true;
        if (!Modern && SessionId is not null)
        {
            try
            {
                using var bounded = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                using var delete = new HttpRequestMessage(HttpMethod.Delete, _server.Endpoint);
                await AddHeadersAsync(delete, null, [], bounded.Token).ConfigureAwait(false);
                // 405 means the server doesn't let clients end sessions: fine.
                using var _ = await _http.SendAsync(delete, bounded.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is HttpRequestException or OperationCanceledException or IOException)
            {
                // The session expires on the server's side.
            }
        }

        if (_ownsHttp)
        {
            _http.Dispose();
        }
    }

    /// <summary>Posts <paramref name="request"/> and reads its answer: the status, and the response if the body held
    /// one.</summary>
    private async Task<(int Status, RpcResponse? Response)> ExchangeAsync(OutgoingRequest request, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_closed, this);
        using var message = new HttpRequestMessage(HttpMethod.Post, _server.Endpoint)
        {
            Content = Json(request.Body),
        };
        await AddHeadersAsync(message, request.Method, request.Headers, cancellationToken).ConfigureAwait(false);
        message.Headers.Accept.ParseAdd("application/json");
        message.Headers.Accept.ParseAdd("text/event-stream");

        using var response = await _http.SendAsync(message, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        var status = (int)response.StatusCode;
        if (request.Method == "initialize" && response.IsSuccessStatusCode
            && response.Headers.TryGetValues("Mcp-Session-Id", out var sessions) && sessions.FirstOrDefault() is { Length: > 0 } session)
        {
            SessionId = session;
        }

        var mediaType = response.Content.Headers.ContentType?.MediaType;
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        if (response.IsSuccessStatusCode && string.Equals(mediaType, "text/event-stream", StringComparison.OrdinalIgnoreCase))
        {
            return (status, await ReadEventsAsync(stream, request.Id, cancellationToken).ConfigureAwait(false));
        }

        var text = await ReadBoundedAsync(stream, cancellationToken).ConfigureAwait(false);
        return (status, text.Length == 0 ? null : RpcResponse.Parse(text));
    }

    /// <summary>
    /// The response to request <paramref name="id"/> from an SSE stream. Notifications before it are ignored; a legacy
    /// server's requests on it are answered with a POST.
    /// </summary>
    private async Task<RpcResponse> ReadEventsAsync(Stream stream, long id, CancellationToken cancellationToken)
    {
        var limited = new LimitedStream(stream, Options.MaxMessageBytes);
        await foreach (var item in McpSse.ReadAsync(limited, cancellationToken).ConfigureAwait(false))
        {
            limited.Reset();
            if (item.EventType != "message" || string.IsNullOrWhiteSpace(item.Data))
            {
                continue;
            }

            using var document = ParseEvent(item.Data);
            if (document is null)
            {
                Report(McpLogLevel.Debug, "ignored an event that isn't JSON");
                continue;
            }

            var element = document.RootElement;
            if (element.ValueKind == JsonValueKind.Object && element.TryGetProperty("method", out var method))
            {
                if (element.TryGetProperty("id", out _))
                {
                    var answer = AnswerServerRequest(element);
                    try
                    {
                        await PostNotificationAsync(answer, cancellationToken).ConfigureAwait(false);
                    }
                    catch (HttpRequestException)
                    {
                        Report(McpLogLevel.Debug, "the MCP server refused Strata's answer to its request");
                    }
                }
                else
                {
                    Report(McpLogLevel.Debug, $"ignored the notification {method.GetRawText()}");
                }

                continue;
            }

            if (RpcResponse.From(element) is { } response && (response.Id == id || response.Id is null))
            {
                return response;
            }
        }

        throw new IOException("the MCP server ended its event stream without a response");
    }

    private static JsonDocument? ParseEvent(string data)
    {
        try
        {
            return JsonDocument.Parse(data);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private async Task PostNotificationAsync(JsonObject body, CancellationToken cancellationToken)
    {
        using var message = new HttpRequestMessage(HttpMethod.Post, _server.Endpoint) { Content = Json(body) };
        var method = body.TryGetPropertyValue("method", out var m) ? m?.GetValue<string>() : null;
        await AddHeadersAsync(message, method, [], cancellationToken).ConfigureAwait(false);
        message.Headers.Accept.ParseAdd("application/json");
        message.Headers.Accept.ParseAdd("text/event-stream");
        using var response = await _http.SendAsync(message, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException($"the MCP server answered a notification with HTTP {(int)response.StatusCode}", null, response.StatusCode);
        }
    }

    private async Task AddHeadersAsync(
        HttpRequestMessage message, string? method, IReadOnlyList<KeyValuePair<string, string>> extra, CancellationToken cancellationToken)
    {
        if (Version is { } version)
        {
            message.Headers.TryAddWithoutValidation("MCP-Protocol-Version", version);
        }

        if (Modern && method is not null)
        {
            message.Headers.TryAddWithoutValidation("Mcp-Method", McpHeaders.Encode(method));
        }

        if (!Modern && SessionId is { } session)
        {
            message.Headers.TryAddWithoutValidation("Mcp-Session-Id", session);
        }

        foreach (var (name, value) in extra)
        {
            message.Headers.TryAddWithoutValidation(name, value);
        }

        if (_server.AccessToken is { } hook && await hook(cancellationToken).ConfigureAwait(false) is { Length: > 0 } token)
        {
            message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        }

        foreach (var (name, value) in _server.Headers)
        {
            message.Headers.Remove(name);
            message.Headers.TryAddWithoutValidation(name, value);
        }
    }

    private static ByteArrayContent Json(JsonObject body)
    {
        var content = new ByteArrayContent(Encoding.UTF8.GetBytes(body.ToJsonString()));
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        return content;
    }

    private async Task<string> ReadBoundedAsync(Stream stream, CancellationToken cancellationToken)
    {
        var limited = new LimitedStream(stream, Options.MaxMessageBytes);
        using var reader = new StreamReader(limited, Encoding.UTF8);
        return await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>A read-only stream that fails once more than a bound has been read since the last <see cref="Reset"/>.</summary>
internal sealed class LimitedStream(Stream inner, long limit) : Stream
{
    private long _read;

    public void Reset() => _read = 0;

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count) => Count(inner.Read(buffer, offset, count));

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        Count(await inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false));

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    private int Count(int read)
    {
        _read += read;
        return _read > limit ? throw new McpException($"the MCP server sent a message bigger than {limit} bytes") : read;
    }
}
