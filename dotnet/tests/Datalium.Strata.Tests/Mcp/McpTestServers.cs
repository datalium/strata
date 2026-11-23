using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json.Nodes;
using Datalium.Strata.Mcp;
using Datalium.Strata.McpFixture;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Datalium.Strata.Tests.Mcp;

/// <summary>
/// The fixture servers of the MCP tests (<c>design/50-MCP-Client.md</c>, "Tests"): the scripted server in memory behind
/// the streams transport, as a stdio child process, and behind Kestrel on loopback. No network, no MCP SDK.
/// </summary>
internal static class McpTestServers
{
    /// <summary>A scripted server in memory: its streams, for <see cref="McpServer.Streams"/>, and the server, to
    /// inspect.</summary>
    public static (StreamMcpServer Server, FixtureServer Fixture) InMemory(FixtureScript? script = null)
    {
        var toServer = new Pipe();
        var fromServer = new Pipe();
        var fixture = new FixtureServer(script ?? new FixtureScript());
        var output = fromServer.Writer.AsStream();
        _ = Task.Run(() => fixture.RunLinesAsync(
            toServer.Reader.AsStream(),
            output,
            () =>
            {
                // "Exiting": the client sees its input end.
                fromServer.Writer.Complete();
                toServer.Reader.Complete();
            }));
        return (McpServer.Streams(fromServer.Reader.AsStream(), toServer.Writer.AsStream()), fixture);
    }

    /// <summary>Two raw pipes, for a test that plays the server itself, line by line.</summary>
    public static (StreamMcpServer Server, StreamReader FromClient, StreamWriter ToClient) Manual()
    {
        var toServer = new Pipe();
        var fromServer = new Pipe();
        var reader = new StreamReader(toServer.Reader.AsStream(), Encoding.UTF8);
        var writer = new StreamWriter(fromServer.Writer.AsStream(), new UTF8Encoding(false)) { AutoFlush = true, NewLine = "\n" };
        return (McpServer.Streams(fromServer.Reader.AsStream(), toServer.Writer.AsStream()), reader, writer);
    }

    /// <summary>The stdio fixture server, run by <c>dotnet</c> with these arguments.</summary>
    public static StdioMcpServer Stdio(params string[] arguments) => McpServer.Stdio("dotnet", [FixtureDll(), .. arguments]);

    /// <summary>The fixture server's assembly, built beside the tests in the same configuration.</summary>
    public static string FixtureDll([CallerFilePath] string here = "")
    {
        var configuration = typeof(McpTestServers).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration ?? "Debug";
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(here)!, "..", "..", "Datalium.Strata.McpFixtureServer", "bin", configuration, "net10.0",
            "Datalium.Strata.McpFixtureServer.dll"));
        return File.Exists(path) ? path : throw new FileNotFoundException("build the MCP fixture server first", path);
    }

    /// <summary>Options that send the log to <paramref name="log"/>.</summary>
    public static McpClientOptions Logging(ConcurrentQueue<McpLogEntry> log, McpClientOptions? options = null) =>
        (options ?? new McpClientOptions()) with { Log = log.Enqueue };
}

/// <summary>What the Kestrel fixture does: its era, how it answers, and what it checks.</summary>
internal sealed record HttpFixtureOptions
{
    /// <summary><c>modern</c>, <c>legacy</c> or <c>dual</c>.</summary>
    public string Era { get; init; } = "modern";

    /// <summary>The legacy version <c>initialize</c> answers with.</summary>
    public string LegacyVersion { get; init; } = "2025-11-25";

    /// <summary>Answer every request as an SSE stream.</summary>
    public bool Sse { get; init; }

    /// <summary>A token every request must carry (<c>Bearer …</c>); otherwise 401.</summary>
    public Func<string?, bool>? Authorized { get; init; }

    /// <summary>Forget the legacy session after this many requests, once.</summary>
    public int? ExpireSessionAfter { get; init; }

    /// <summary>The answer to <c>server/discover</c>, when set: a status and a JSON body.</summary>
    public (int Status, string Body)? Discover { get; init; }

    public bool MismatchOnce { get; init; }

    public bool BadHeaderTool { get; init; }

    public int DeleteStatus { get; init; } = 200;
}

/// <summary>
/// The scripted server behind Kestrel on <c>127.0.0.1</c>: Streamable HTTP in either era, with the modern header checks
/// a real server makes (version, method, name) and the legacy session. Records every request it gets.
/// </summary>
internal sealed class HttpFixture : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly HttpFixtureOptions _options;
    private readonly ConcurrentDictionary<string, bool> _sessions = new();
    private int _requests;
    private int _expired;

    private HttpFixture(WebApplication app, HttpFixtureOptions options, FixtureServer fixture)
    {
        _app = app;
        _options = options;
        Fixture = fixture;
    }

    public FixtureServer Fixture { get; }

    public Uri Endpoint { get; private set; } = null!;

    /// <summary>Each request: its method, headers and body.</summary>
    public ConcurrentQueue<(string Method, Dictionary<string, string> Headers, JsonObject? Body)> Requests { get; } = new();

    /// <summary>Requests whose client went away before the answer.</summary>
    public ConcurrentQueue<string> Aborted { get; } = new();

    public static async Task<HttpFixture> StartAsync(HttpFixtureOptions? options = null)
    {
        options ??= new HttpFixtureOptions();
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseKestrel(k => k.Listen(IPAddress.Loopback, 0));
        var app = builder.Build();
        var fixture = new FixtureServer(new FixtureScript
        {
            Initialize = options.Era == "modern" ? null : options.LegacyVersion,
            MismatchOnce = options.MismatchOnce,
            BadHeaderTool = options.BadHeaderTool,
        });
        var server = new HttpFixture(app, options, fixture);
        app.Map("/mcp", server.HandleAsync);
        await app.StartAsync();
        var address = app.Urls.First();
        server.Endpoint = new Uri(address.TrimEnd('/') + "/mcp");
        return server;
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }

    private async Task HandleAsync(HttpContext http)
    {
        var request = http.Request;
        var headers = request.Headers.ToDictionary(h => h.Key, h => h.Value.ToString(), StringComparer.OrdinalIgnoreCase);
        JsonObject? body = null;
        if (request.Method == "POST")
        {
            body = JsonNode.Parse(await new StreamReader(request.Body).ReadToEndAsync()) as JsonObject;
        }

        Requests.Enqueue((request.Method, headers, body));
        var count = Interlocked.Increment(ref _requests);

        if (_options.Authorized is { } authorized && !authorized(headers.GetValueOrDefault("Authorization")))
        {
            http.Response.StatusCode = 401;
            return;
        }

        if (request.Method == "DELETE")
        {
            if (headers.GetValueOrDefault("Mcp-Session-Id") is { } ended)
            {
                _sessions.TryRemove(ended, out _);
            }

            http.Response.StatusCode = _options.DeleteStatus;
            return;
        }

        if (request.Method != "POST" || body is null)
        {
            http.Response.StatusCode = 405;
            return;
        }

        var method = body["method"]?.GetValue<string>();
        var meta = body["params"]?["_meta"];
        var modernRequest = meta?["io.modelcontextprotocol/protocolVersion"] is not null;

        if (method == "server/discover" && _options.Discover is { } scripted)
        {
            await Json(http, scripted.Status, scripted.Body);
            return;
        }

        if (modernRequest)
        {
            if (_options.Era == "legacy")
            {
                // What a legacy server says to a request before initialize.
                await Json(http, 400, """{"jsonrpc":"2.0","id":null,"error":{"code":-32000,"message":"Bad Request: Server not initialized"}}""");
                return;
            }

            var version = meta!["io.modelcontextprotocol/protocolVersion"]!.GetValue<string>();
            string? mismatch = null;
            if (headers.GetValueOrDefault("MCP-Protocol-Version") != version)
            {
                mismatch = "MCP-Protocol-Version";
            }
            else if (headers.GetValueOrDefault("Mcp-Method") != method)
            {
                mismatch = "Mcp-Method";
            }
            else if (method == "tools/call" && Decode(headers.GetValueOrDefault("Mcp-Name")) != body["params"]?["name"]?.GetValue<string>())
            {
                mismatch = "Mcp-Name";
            }

            if (mismatch is not null)
            {
                await Json(http, 400, $$$"""{"jsonrpc":"2.0","id":{{{body["id"]!.ToJsonString()}}},"error":{"code":-32020,"message":"Header mismatch: {{{mismatch}}}"}}""");
                return;
            }
        }
        else if (body["id"] is not null && method is not null)
        {
            // Legacy: a session from initialize on, which may expire.
            if (method == "initialize")
            {
                if (_options.Era == "modern")
                {
                    await Json(http, 400, $$$"""{"jsonrpc":"2.0","id":{{{body["id"]!.ToJsonString()}}},"error":{"code":-32601,"message":"Method not found: supported versions are 2026-07-28"}}""");
                    return;
                }

                var session = Guid.NewGuid().ToString("N");
                _sessions[session] = true;
                http.Response.Headers["Mcp-Session-Id"] = session;
            }
            else
            {
                var session = headers.GetValueOrDefault("Mcp-Session-Id");
                if (_options.ExpireSessionAfter is { } after && count > after && Interlocked.Exchange(ref _expired, 1) == 0 && session is not null)
                {
                    _sessions.TryRemove(session, out _);
                }

                if (session is null || !_sessions.ContainsKey(session))
                {
                    http.Response.StatusCode = 404;
                    return;
                }
            }
        }

        var channel = new HttpFixtureChannel(http, headers, _options.Sse);
        JsonObject? response;
        try
        {
            response = await Fixture.HandleAsync(body, channel).WaitAsync(http.RequestAborted);
        }
        catch (OperationCanceledException)
        {
            Aborted.Enqueue(body["id"]?.ToJsonString() ?? "?");
            return;
        }

        if (response is null)
        {
            if (!channel.Started)
            {
                http.Response.StatusCode = 202;
            }

            return;
        }

        var status = response["error"]?["code"]?.GetValue<int>() == -32020 ? 400 : 200;
        if (channel.Started || (_options.Sse && status == 200))
        {
            await channel.SendAsync(response);
            return;
        }

        await Json(http, status, response.ToJsonString());
    }

    private static string? Decode(string? header) =>
        header is not null && header.StartsWith("=?base64?", StringComparison.Ordinal) && header.EndsWith("?=", StringComparison.Ordinal)
            ? Encoding.UTF8.GetString(Convert.FromBase64String(header[9..^2]))
            : header;

    private static async Task Json(HttpContext http, int status, string body)
    {
        http.Response.StatusCode = status;
        http.Response.ContentType = "application/json";
        await http.Response.WriteAsync(body);
    }

    private sealed class HttpFixtureChannel(HttpContext http, IReadOnlyDictionary<string, string> headers, bool sse) : IFixtureChannel
    {
        private readonly SemaphoreSlim _writing = new(1, 1);

        public bool Started { get; private set; }

        public IReadOnlyDictionary<string, string> Headers { get; } = headers;

        public async Task SendAsync(JsonObject message)
        {
            if (!sse)
            {
                // A JSON answer has no room for messages ahead of the response.
                return;
            }

            await _writing.WaitAsync();
            try
            {
                if (!Started)
                {
                    Started = true;
                    http.Response.StatusCode = 200;
                    http.Response.ContentType = "text/event-stream";
                    await http.Response.WriteAsync(": stream\n\n");
                }

                await http.Response.WriteAsync("data: " + message.ToJsonString() + "\n\n");
                await http.Response.Body.FlushAsync();
            }
            finally
            {
                _writing.Release();
            }
        }

        public Task WriteRawAsync(string line) => Task.CompletedTask;

        public void Exit() => http.Abort();
    }
}
