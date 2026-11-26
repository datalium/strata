using System.Text.Json;
using System.Text.Json.Nodes;

namespace Datalium.Strata.Mcp;

/// <summary>One request on its way to a server: its id and method, its whole JSON-RPC body, and (HTTP) extra headers.</summary>
internal sealed record OutgoingRequest(long Id, string Method, JsonObject Body, IReadOnlyList<KeyValuePair<string, string>> Headers);

/// <summary>A server's answer to a request: the response, and on HTTP the status it came with.</summary>
internal sealed record ChannelAnswer(RpcResponse? Response, int? HttpStatus = null);

/// <summary>
/// One connection to a server, over which requests and notifications go (<c>design/50-MCP-Client.md</c>, "The
/// protocol"). It reports transport failures as exceptions (<see cref="IOException"/>,
/// <see cref="HttpRequestException"/>) and a server's JSON-RPC errors as responses; the client decides what they mean.
/// </summary>
internal abstract class McpChannel : IAsyncDisposable
{
    protected McpChannel(McpClientOptions options)
    {
        Options = options;
    }

    protected McpClientOptions Options { get; }

    public abstract McpEndpointInfo Endpoint { get; }

    /// <summary>Whether requests can still go out; a stdio server that exited can't take any.</summary>
    public abstract bool IsOpen { get; }

    /// <summary>
    /// Sends <paramref name="request"/> and waits for its response, until <paramref name="cancellationToken"/> fires.
    /// With <paramref name="probe"/>, an HTTP channel returns any status with whatever response its body held, for
    /// the era rule, instead of failing on it.
    /// </summary>
    public abstract Task<ChannelAnswer> SendAsync(OutgoingRequest request, bool probe, CancellationToken cancellationToken);

    /// <summary>Tells the server a request was abandoned (cancelled, or timed out), as the transport does it.</summary>
    public abstract Task AbandonAsync(OutgoingRequest request);

    /// <summary>Sends a notification.</summary>
    public abstract Task NotifyAsync(string method, JsonObject? parameters, CancellationToken cancellationToken);

    /// <summary>Closes the connection; requests still waiting fail with <paramref name="reason"/>.</summary>
    public abstract ValueTask CloseAsync(Exception reason);

    public ValueTask DisposeAsync() => CloseAsync(new ObjectDisposedException(nameof(McpClient), "the MCP connection is closed"));

    protected void Report(McpLogLevel level, string message)
    {
        try
        {
            Options.Log?.Invoke(new McpLogEntry(level, message));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // A logger that throws is the application's problem, not the connection's.
        }
    }

    /// <summary>The answer Strata gives a request from a server: <c>ping</c> gets an empty result, the rest
    /// method-not-found, since Strata declares no client capabilities.</summary>
    protected static JsonObject AnswerServerRequest(JsonElement message)
    {
        var id = JsonNode.Parse(message.GetProperty("id").GetRawText());
        var method = message.TryGetProperty("method", out var m) && m.ValueKind == JsonValueKind.String ? m.GetString() : null;
        return method == "ping"
            ? new JsonObject { ["jsonrpc"] = "2.0", ["id"] = id, ["result"] = new JsonObject() }
            : new JsonObject
            {
                ["jsonrpc"] = "2.0",
                ["id"] = id,
                ["error"] = new JsonObject
                {
                    ["code"] = McpProtocol.MethodNotFound,
                    ["message"] = $"Method not found: {method}: Strata's MCP client declares no client capabilities",
                },
            };
    }

    protected static JsonObject Notification(string method, JsonObject? parameters)
    {
        var body = new JsonObject { ["jsonrpc"] = "2.0", ["method"] = method };
        if (parameters is not null)
        {
            body["params"] = parameters;
        }

        return body;
    }

    protected static JsonObject Cancellation(long id) => Notification(
        "notifications/cancelled", new JsonObject { ["requestId"] = id, ["reason"] = "cancelled by the caller" });
}
