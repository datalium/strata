using System.Text.Json;

namespace Datalium.Strata.Mcp;

/// <summary>How an <see cref="McpClient"/> behaves (<c>design/50-MCP-Client.md</c>). The defaults suit most servers.</summary>
public sealed record McpClientOptions
{
    /// <summary>The name Strata gives the server as its <c>clientInfo</c>.</summary>
    public string ClientName { get; init; } = "datalium-strata";

    /// <summary>The version Strata gives the server as its <c>clientInfo</c>: Strata's own by default.</summary>
    public string ClientVersion { get; init; } = McpProtocol.StrataVersion;

    /// <summary>The bound on the requests Strata makes on its own: <c>server/discover</c>, <c>initialize</c>,
    /// <c>tools/list</c>.</summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The bound on a <c>tools/call</c>; none by default, since a call is already bounded by its node's timeout and its
    /// run's deadline, through its token. When it passes, the call fails with a <see cref="TimeoutException"/>.
    /// </summary>
    public TimeSpan? CallTimeout { get; init; }

    /// <summary>How long a stdio server has to answer the <c>server/discover</c> probe before it is taken for a legacy
    /// one.</summary>
    public TimeSpan ProbeTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Whether a stdio server that exited is started again on the next call, as the spec says clients
    /// should.</summary>
    public bool RestartOnExit { get; init; } = true;

    /// <summary>The largest message Strata reads from a server. A bigger one fails the request and, on stdio or
    /// streams, the connection.</summary>
    public int MaxMessageBytes { get; init; } = 16 * 1024 * 1024;

    /// <summary>
    /// Told what Strata otherwise drops: the server's stderr, tools left out of the list, messages ignored. Never told
    /// a token. An exception it throws is ignored.
    /// </summary>
    public Action<McpLogEntry>? Log { get; init; }
}

/// <summary>How much an <see cref="McpLogEntry"/> matters.</summary>
public enum McpLogLevel
{
    /// <summary>What a server said that Strata ignores: stderr, notifications.</summary>
    Debug,

    /// <summary>The connection's progress: the era chosen, a restart.</summary>
    Information,

    /// <summary>Something left out: a tool listed twice, a tool with an invalid <c>x-mcp-header</c>.</summary>
    Warning,
}

/// <summary>One thing an <see cref="McpClient"/> reports to <see cref="McpClientOptions.Log"/>.</summary>
public sealed record McpLogEntry(McpLogLevel Level, string Message);

/// <summary>Which tools of a server a <see cref="McpClient.ToolProvider"/> offers, and under which names.</summary>
public sealed record McpToolOptions
{
    /// <summary>Put before each tool's name (<c>"github."</c>), since a registry refuses duplicate names. The server
    /// is still called with its own name.</summary>
    public string? Prefix { get; init; }

    /// <summary>Only these tools, by the server's names (before the prefix); all of them when <see langword="null"/>.
    /// A name the server didn't list throws <see cref="Tools.ToolNotFoundException"/>.</summary>
    public IReadOnlyList<string>? Include { get; init; }
}

/// <summary>A server's name and version, as it reports them: for display only, the spec says.</summary>
public sealed record McpImplementation(string Name, string? Version, string? Title = null);

/// <summary>
/// One tool, as the server listed it. Everything here comes from the server and is untrusted: the description can try
/// to steer a model, and the annotations are hints, not guarantees.
/// </summary>
/// <param name="Name">The server's name for it.</param>
/// <param name="InputSchema">The JSON Schema of its arguments, the Strata tool's <c>parameters</c> unchanged.</param>
public sealed record McpTool(
    string Name, string? Title, string? Description, JsonElement InputSchema, JsonElement? OutputSchema, JsonElement? Annotations);

/// <summary>A <c>tools/call</c> result, whole: the content blocks, the structured content, and whether the tool
/// reports an error.</summary>
public sealed record McpToolResult(IReadOnlyList<JsonElement> Content, JsonElement? StructuredContent, bool IsError)
{
    /// <summary>
    /// The text a Strata tool returns for this result (<c>design/50-MCP-Client.md</c>, "Results"): the structured
    /// content as JSON, else the content blocks joined by blank lines, with placeholders for binary content; an error
    /// as <c>{"error": "…"}</c>, the shape the agent gives the model for every error.
    /// </summary>
    public string ToText() => McpResults.ToText(this);
}

/// <summary>
/// An MCP failure that isn't the transport's: an error the server answered (<see cref="Code"/> is its JSON-RPC code),
/// a version Strata can't speak, an answer that breaks the protocol, a request for input Strata doesn't provide.
/// </summary>
public class McpException : Exception
{
    public McpException(string message, int? code = null, Exception? innerException = null)
        : base(message, innerException)
    {
        Code = code;
    }

    /// <summary>The JSON-RPC error code, when the server answered with one.</summary>
    public int? Code { get; }
}
