namespace Datalium.Strata.Mcp;

/// <summary>
/// How to reach one MCP server (<c>design/50-MCP-Client.md</c>): a child process over stdio, a Streamable HTTP
/// endpoint, or any pair of streams with stdio's framing. Records, so <c>with</c> adjusts one. Describing a server
/// connects to nothing; <see cref="McpClient.ConnectAsync"/> does.
/// </summary>
public abstract record McpServer
{
    private protected McpServer()
    {
    }

    /// <summary>A server Strata starts as a child process and talks to over its stdin and stdout.</summary>
    /// <param name="command">The program: a path, or a name found through <c>PATH</c> (and on Windows <c>PATHEXT</c>,
    /// so <c>npx</c> finds <c>npx.cmd</c>).</param>
    /// <param name="arguments">Its arguments, passed as they are.</param>
    public static StdioMcpServer Stdio(string command, IEnumerable<string>? arguments = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(command);
        return new StdioMcpServer { Command = command, Arguments = arguments?.ToArray() ?? [] };
    }

    /// <summary>A server at a Streamable HTTP endpoint.</summary>
    public static HttpMcpServer Http(Uri endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        if (!endpoint.IsAbsoluteUri || (endpoint.Scheme != Uri.UriSchemeHttp && endpoint.Scheme != Uri.UriSchemeHttps))
        {
            throw new ArgumentException("An MCP endpoint is an absolute http or https URI.", nameof(endpoint));
        }

        return new HttpMcpServer { Endpoint = endpoint };
    }

    /// <summary>
    /// A server already connected through two streams, with stdio's framing (one JSON-RPC message per line): a
    /// socket, or an in-memory pipe in tests. Strata reads <paramref name="fromServer"/>, writes
    /// <paramref name="toServer"/>, and disposes both with the client. It can't start such a server again.
    /// </summary>
    public static StreamMcpServer Streams(Stream fromServer, Stream toServer)
    {
        ArgumentNullException.ThrowIfNull(fromServer);
        ArgumentNullException.ThrowIfNull(toServer);
        return new StreamMcpServer(fromServer, toServer);
    }
}

/// <summary>A server Strata starts as a child process (<see cref="McpServer.Stdio"/>).</summary>
public sealed record StdioMcpServer : McpServer
{
    /// <summary>The program to run.</summary>
    public required string Command { get; init; }

    /// <summary>Its arguments.</summary>
    public IReadOnlyList<string> Arguments { get; init; } = [];

    /// <summary>The directory it starts in; this process's when <see langword="null"/>.</summary>
    public string? WorkingDirectory { get; init; }

    /// <summary>
    /// Variables added to its environment, or removed from it with a <see langword="null"/> value. Added to this
    /// process's environment when <see cref="InheritEnvironment"/> is on (the default).
    /// </summary>
    public IReadOnlyDictionary<string, string?> Environment { get; init; } = new Dictionary<string, string?>();

    /// <summary>
    /// Whether the server starts with this process's environment, secrets included. Off, it gets only
    /// <see cref="Environment"/>, plus what the OS needs to start a process (<c>PATH</c>, and on Windows
    /// <c>SystemRoot</c>).
    /// </summary>
    public bool InheritEnvironment { get; init; } = true;

    /// <summary>How long <see cref="McpClient.DisposeAsync"/> waits for the server to exit once its stdin is
    /// closed, before stopping it and the processes it started.</summary>
    public TimeSpan ShutdownTimeout { get; init; } = TimeSpan.FromSeconds(5);
}

/// <summary>A server at a Streamable HTTP endpoint (<see cref="McpServer.Http"/>).</summary>
public sealed record HttpMcpServer : McpServer
{
    /// <summary>The MCP endpoint every request is POSTed to.</summary>
    public required Uri Endpoint { get; init; }

    /// <summary>Headers sent on every request, after <c>Authorization</c>. A header named <c>Authorization</c>,
    /// or ending in <c>-Key</c> or <c>-Token</c>, is never written in an error.</summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// Called before each request, so the code that owns a token can refresh it; a token it returns is sent as
    /// <c>Authorization: Bearer &lt;token&gt;</c>, and <see langword="null"/> sends none. A hook that throws fails the
    /// request, and nothing is retried.
    /// </summary>
    public Func<CancellationToken, ValueTask<string?>>? AccessToken { get; init; }

    /// <summary>
    /// The client to send with (one from <c>IHttpClientFactory</c>, say), left open when the MCP client is disposed.
    /// When <see langword="null"/>, the MCP client makes one of its own, which follows no redirects, and disposes it.
    /// </summary>
    public HttpClient? HttpClient { get; init; }
}

/// <summary>A server reached through two streams (<see cref="McpServer.Streams"/>).</summary>
public sealed record StreamMcpServer : McpServer
{
    internal StreamMcpServer(Stream fromServer, Stream toServer)
    {
        FromServer = fromServer;
        ToServer = toServer;
    }

    /// <summary>What the server writes.</summary>
    public Stream FromServer { get; init; }

    /// <summary>What Strata writes to the server.</summary>
    public Stream ToServer { get; init; }
}
