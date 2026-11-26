using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Net.Http;

namespace Datalium.Strata.Mcp;

/// <summary>
/// The MCP client's instrumentation (<c>design/50-MCP-Client.md</c>, "Telemetry"), following the OpenTelemetry MCP
/// conventions: a client span per MCP request, and the <c>mcp.client.operation.duration</c> histogram, on an
/// <see cref="ActivitySource"/> and a <see cref="Meter"/> named <see cref="Name"/>. Listen with
/// <c>AddSource(McpTelemetry.Name)</c> / <c>AddMeter(McpTelemetry.Name)</c>; <c>AddStrata()</c>'s instrumentation
/// adds both. No arguments, results, tokens or stderr are recorded.
/// </summary>
public static class McpTelemetry
{
    /// <summary>The name of the client's <see cref="ActivitySource"/> and <see cref="Meter"/>.</summary>
    public const string Name = "Datalium.Strata.Mcp";

    private static readonly string? Version = typeof(McpTelemetry).Assembly.GetName().Version?.ToString();

    internal static readonly ActivitySource Source = new(Name, Version);

    internal static readonly Meter Meter = new(Name, Version);

    internal static readonly Histogram<double> OperationDuration = Meter.CreateHistogram<double>(
        "mcp.client.operation.duration", "s", "How long an MCP request took, from the client's side.");
}

/// <summary>One MCP request's span and duration.</summary>
internal sealed class McpRequestScope : IDisposable
{
    private readonly long _started = Stopwatch.GetTimestamp();
    private readonly TagList _tags;
    private string? _errorType;
    private bool _done;

    public McpRequestScope(string method, string? tool, long id, string? version, McpEndpointInfo endpoint)
    {
        Activity = McpTelemetry.Source.StartActivity(tool is null ? method : $"{method} {tool}", ActivityKind.Client);
        if (Activity is not null)
        {
            Activity.SetTag("mcp.method.name", method);
            Activity.SetTag("mcp.protocol.version", version);
            Activity.SetTag("jsonrpc.request.id", id.ToString(CultureInfo.InvariantCulture));
            Activity.SetTag("network.transport", endpoint.Transport);
            if (endpoint.Address is { } address)
            {
                Activity.SetTag("server.address", address);
                Activity.SetTag("server.port", endpoint.Port);
            }

            if (endpoint.SessionId is { } session)
            {
                Activity.SetTag("mcp.session.id", session);
            }

            if (tool is not null)
            {
                Activity.SetTag("gen_ai.tool.name", tool);
                Activity.SetTag("gen_ai.operation.name", "execute_tool");
            }
        }

        _tags = new TagList
        {
            { "mcp.method.name", method },
            { "mcp.protocol.version", version },
            { "network.transport", endpoint.Transport },
        };
        if (tool is not null)
        {
            _tags.Add("gen_ai.tool.name", tool);
        }
    }

    public Activity? Activity { get; }

    /// <summary>The server answered, and the tool reports an error: not a failure of the request.</summary>
    public void ToolError()
    {
        _errorType = "tool_error";
        Activity?.SetTag("error.type", "tool_error");
    }

    public void Failed(Exception error, bool cancelled)
    {
        if (cancelled)
        {
            return;
        }

        string type;
        switch (error)
        {
            case McpException { Code: { } code }:
                type = code.ToString(CultureInfo.InvariantCulture);
                Activity?.SetTag("rpc.status_code", type);
                break;
            case HttpRequestException { StatusCode: { } status }:
                type = ((int)status).ToString(CultureInfo.InvariantCulture);
                break;
            case TimeoutException:
                type = "timeout";
                break;
            case McpException:
                type = "protocol";
                break;
            default:
                type = "transport";
                break;
        }

        _errorType = type;
        Activity?.SetTag("error.type", type);
        Activity?.SetStatus(ActivityStatusCode.Error, error.Message);
    }

    public void Dispose()
    {
        if (_done)
        {
            return;
        }

        _done = true;
        var tags = _tags;
        if (_errorType is not null)
        {
            tags.Add("error.type", _errorType);
        }

        McpTelemetry.OperationDuration.Record(Stopwatch.GetElapsedTime(_started).TotalSeconds, tags);
        Activity?.Dispose();
    }
}

/// <summary>What a span says about where a request went.</summary>
internal sealed record McpEndpointInfo(string Transport, string? Address = null, int? Port = null, string? SessionId = null);
