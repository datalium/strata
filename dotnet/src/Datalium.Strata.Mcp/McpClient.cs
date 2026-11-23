using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Tools;

namespace Datalium.Strata.Mcp;

/// <summary>
/// A connection to one MCP server (<c>design/50-MCP-Client.md</c>): Strata's own implementation of the MCP spec, for
/// tools. Connecting finds which era the server speaks (the stateless <c>2026-07-28</c>, or a legacy version through
/// the <c>initialize</c> handshake) and lists its tools; <see cref="ToolProvider"/> offers them to a
/// <see cref="ToolRegistry"/>, where they behave like <c>[StrataTool]</c> tools.
/// </summary>
/// <remarks>
/// Safe for concurrent calls: requests share the connection, matched by id. A stdio server that exits is started again
/// on the next call (<see cref="McpClientOptions.RestartOnExit"/>). Dispose it with <c>await using</c>: a stdio server
/// gets its stdin closed, a while to exit, and is then stopped with the processes it started.
/// </remarks>
public sealed class McpClient : IAsyncDisposable
{
    private const int MaxPages = 1000;

    private readonly McpServer _server;
    private readonly McpClientOptions _options;
    private readonly SemaphoreSlim _connecting = new(1, 1);
    private readonly ConcurrentDictionary<string, IReadOnlyList<HeaderParameter>> _headerParameters = new(StringComparer.Ordinal);
    private McpChannel? _channel;
    private long _nextId;
    private volatile bool _disposed;
    private bool _modern;

    private McpClient(McpServer server, McpClientOptions options)
    {
        _server = server;
        _options = options;
    }

    /// <summary>The protocol version in use: <c>"2026-07-28"</c>, <c>"2025-11-25"</c>, ….</summary>
    public string ProtocolVersion { get; private set; } = McpProtocol.Modern;

    /// <summary>The server's name and version, as it reports them: for display only.</summary>
    public McpImplementation? ServerInfo { get; private set; }

    /// <summary>What the server says about using it, for a model's instructions if the application wants.</summary>
    public string? Instructions { get; private set; }

    /// <summary>The tools listed at connect, in the server's order.</summary>
    public IReadOnlyList<McpTool> Tools { get; private set; } = [];

    /// <summary>The stdio server's process id, for the tests that check it stops.</summary>
    internal int? ProcessId => (_channel as StdioChannel)?.ProcessId;

    /// <summary>
    /// Connects to <paramref name="server"/>: starts it (stdio), finds the era it speaks, and lists its tools. A server
    /// that is down, or speaks no version Strata knows, fails here rather than on the first call.
    /// </summary>
    /// <exception cref="McpException">The server speaks no version Strata knows, or answered with an error.</exception>
    /// <exception cref="IOException">The server didn't start, or closed the connection.</exception>
    /// <exception cref="HttpRequestException">The endpoint can't be reached, or refused the request.</exception>
    public static async Task<McpClient> ConnectAsync(McpServer server, McpClientOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(server);
        var client = new McpClient(server, options ?? new McpClientOptions());
        try
        {
            await client.OpenAsync(cancellationToken).ConfigureAwait(false);
            client.Tools = await client.ListToolsAsync(cancellationToken).ConfigureAwait(false);
            return client;
        }
        catch
        {
            await client.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// The server's tools as a Strata tool provider, for <see cref="ToolRegistry.Register"/>: each under
    /// <see cref="McpToolOptions.Prefix"/> + its name, described by its description (else its title), with its input
    /// schema as its parameters. A call returns the result as text (<see cref="McpToolResult.ToText"/>).
    /// </summary>
    /// <exception cref="ToolNotFoundException">An <see cref="McpToolOptions.Include"/> name the server didn't list.</exception>
    public IStrataToolProvider ToolProvider(McpToolOptions? options = null)
    {
        options ??= new McpToolOptions();
        IEnumerable<McpTool> chosen = Tools;
        if (options.Include is { } include)
        {
            var byName = Tools.ToDictionary(t => t.Name, StringComparer.Ordinal);
            var wanted = new HashSet<string>(StringComparer.Ordinal);
            foreach (var name in include)
            {
                if (!byName.ContainsKey(name))
                {
                    throw new ToolNotFoundException(name);
                }

                wanted.Add(name);
            }

            chosen = Tools.Where(t => wanted.Contains(t.Name));
        }

        return new McpToolProvider(this, options.Prefix ?? string.Empty, chosen.ToArray());
    }

    /// <summary>
    /// Calls the tool <paramref name="name"/> (the server's name) with <paramref name="arguments"/>, outside any
    /// registry, and returns the whole result, binary content included.
    /// </summary>
    /// <exception cref="McpException">The server answered with an error (<see cref="McpException.Code"/>).</exception>
    /// <exception cref="TimeoutException"><see cref="McpClientOptions.CallTimeout"/> passed.</exception>
    /// <exception cref="ObjectDisposedException">The client was disposed.</exception>
    public async Task<McpToolResult> CallToolAsync(string name, JsonElement arguments, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(name);
        ObjectDisposedException.ThrowIf(_disposed, this);
        for (var attempt = 0; ; attempt++)
        {
            var parameters = new JsonObject
            {
                ["name"] = name,
                ["arguments"] = arguments.ValueKind == JsonValueKind.Undefined ? new JsonObject() : JsonNode.Parse(arguments.GetRawText()),
            };
            var headers = new List<KeyValuePair<string, string>>();
            if (_modern && _server is HttpMcpServer)
            {
                headers.Add(new("Mcp-Name", McpHeaders.Encode(name)));
                if (_headerParameters.TryGetValue(name, out var mirrored))
                {
                    headers.AddRange(McpHeaders.For(mirrored, arguments));
                }
            }

            var response = await ExchangeAsync("tools/call", parameters, name, headers, _options.CallTimeout, probe: false, cancellationToken)
                .ConfigureAwait(false);
            if (response!.Error is { Code: McpProtocol.HeaderMismatch } && _modern && _server is HttpMcpServer && attempt == 0)
            {
                // The spec's advice: the tool's schema may have changed; list again, then retry once.
                await ListToolsAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            return McpResults.Read(Complete(response, "tools/call"));
        }
    }

    /// <summary>Stops the connection: stdio closes the server's stdin, waits, then stops its process tree; legacy HTTP
    /// ends its session. Calls after it fail with <see cref="ObjectDisposedException"/>.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await _connecting.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_channel is { } channel)
            {
                _channel = null;
                await channel.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _connecting.Release();
        }
    }

    private async Task OpenAsync(CancellationToken cancellationToken)
    {
        McpChannel channel = _server switch
        {
            StdioMcpServer stdio => StdioChannel.Start(stdio, _options),
            StreamMcpServer streams => new LineChannel(streams.FromServer, streams.ToServer, _options),
            HttpMcpServer http => new HttpChannel(http, _options),
            _ => throw new ArgumentException($"an MCP server of type {_server.GetType().Name} isn't one Strata can reach"),
        };
        _channel = channel;
        await NegotiateAsync(channel, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The era probe (<c>design/50-MCP-Client.md</c>, "Opening a connection"), then the legacy handshake if
    /// the server needs it.</summary>
    private async Task NegotiateAsync(McpChannel channel, CancellationToken cancellationToken)
    {
        var http = channel as HttpChannel;
        _modern = true;
        ProtocolVersion = McpProtocol.Modern;
        if (http is not null)
        {
            http.Modern = true;
            http.Version = McpProtocol.Modern;
        }

        EraOutcome outcome;
        RpcResponse? answer;
        var lastStatus = 0;
        if (http is not null)
        {
            var reply = await SendOnAsync(channel, "server/discover", new JsonObject(), null, [], _options.RequestTimeout, probe: true, cancellationToken)
                .ConfigureAwait(false);
            answer = reply.Response;
            lastStatus = reply.HttpStatus ?? 0;
            outcome = McpProtocol.ClassifyHttp(lastStatus, answer);
        }
        else
        {
            try
            {
                answer = (await SendOnAsync(channel, "server/discover", new JsonObject(), null, [], _options.ProbeTimeout, probe: true, cancellationToken)
                    .ConfigureAwait(false)).Response;
            }
            catch (TimeoutException)
            {
                answer = null;
                Report(McpLogLevel.Information, "the MCP server didn't answer server/discover: taking it for a legacy server");
            }

            outcome = McpProtocol.Classify(answer, http: false);
        }

        switch (outcome.Era)
        {
            case "fail" when http is not null && lastStatus is 401 or 403 or >= 500:
                // Not an era: the endpoint refused, or failed.
                throw new HttpRequestException(outcome.Error, null, (System.Net.HttpStatusCode)lastStatus);
            case "fail":
                var code = answer?.Error?.Code;
                throw new McpException(outcome.Error!, code is { } c && McpProtocol.IsModernError(c) ? c : null);
            case "modern":
                ReadIdentity(answer!.Result, answer.Result.TryGetProperty("_meta", out var meta) && meta.ValueKind == JsonValueKind.Object
                    && meta.TryGetProperty(McpProtocol.MetaServerInfo, out var info) ? info : default);
                Report(McpLogLevel.Information, $"the MCP server speaks {McpProtocol.Modern}");
                return;
            default:
                await InitializeAsync(channel, outcome.Version!, cancellationToken).ConfigureAwait(false);
                if (http is not null)
                {
                    http.Reinitialize = token => InitializeAsync(http, ProtocolVersion, token);
                }

                return;
        }
    }

    /// <summary>The legacy handshake: <c>initialize</c>, then <c>notifications/initialized</c>.</summary>
    private async Task InitializeAsync(McpChannel channel, string version, CancellationToken cancellationToken)
    {
        _modern = false;
        var http = channel as HttpChannel;
        if (http is not null)
        {
            http.Modern = false;
            http.Version = null;
        }

        var parameters = new JsonObject
        {
            ["protocolVersion"] = version,
            ["capabilities"] = new JsonObject(),
            ["clientInfo"] = new JsonObject { ["name"] = _options.ClientName, ["version"] = _options.ClientVersion },
        };
        var reply = await SendOnAsync(channel, "initialize", parameters, null, [], _options.RequestTimeout, probe: false, cancellationToken)
            .ConfigureAwait(false);
        var result = Complete(reply.Response!, "initialize");
        var chosen = result.TryGetProperty("protocolVersion", out var v) && v.ValueKind == JsonValueKind.String ? v.GetString()! : string.Empty;
        if (!McpProtocol.KnowsLegacy(chosen, http is not null))
        {
            throw new McpException(
                $"the MCP server chose protocol version '{chosen}', which Strata doesn't speak "
                + $"(it speaks {string.Join(", ", [McpProtocol.Modern, .. McpProtocol.Legacy(http is not null)])})");
        }

        ProtocolVersion = chosen;
        if (http is not null)
        {
            http.Version = chosen;
        }

        ReadIdentity(result, result.TryGetProperty("serverInfo", out var info) ? info : default);
        await channel.NotifyAsync("notifications/initialized", null, cancellationToken).ConfigureAwait(false);
        Report(McpLogLevel.Information, $"the MCP server speaks {chosen}");
    }

    private void ReadIdentity(JsonElement result, JsonElement info)
    {
        Instructions = result.TryGetProperty("instructions", out var i) && i.ValueKind == JsonValueKind.String ? i.GetString() : null;
        ServerInfo = info.ValueKind == JsonValueKind.Object && info.TryGetProperty("name", out var n) && n.ValueKind == JsonValueKind.String
            ? new McpImplementation(n.GetString()!, Text(info, "version"), Text(info, "title"))
            : null;
    }

    /// <summary><c>tools/list</c>, every page, with the guards against a server that loops.</summary>
    private async Task<IReadOnlyList<McpTool>> ListToolsAsync(CancellationToken cancellationToken)
    {
        var tools = new List<McpTool>();
        var names = new HashSet<string>(StringComparer.Ordinal);
        var cursors = new HashSet<string>(StringComparer.Ordinal);
        var checkHeaders = _modern && _server is HttpMcpServer;
        string? cursor = null;
        for (var page = 0; ; page++)
        {
            if (page == MaxPages)
            {
                throw new McpException($"the MCP server's tool list has more than {MaxPages} pages");
            }

            var parameters = new JsonObject();
            if (cursor is not null)
            {
                parameters["cursor"] = cursor;
            }

            var result = Complete(
                (await ExchangeAsync("tools/list", parameters, null, [], _options.RequestTimeout, probe: false, cancellationToken).ConfigureAwait(false))!,
                "tools/list");
            if (result.TryGetProperty("tools", out var listed) && listed.ValueKind == JsonValueKind.Array)
            {
                foreach (var entry in listed.EnumerateArray())
                {
                    if (Read(entry) is not { } tool)
                    {
                        Report(McpLogLevel.Warning, "left out a tool with no name");
                        continue;
                    }

                    if (!names.Add(tool.Name))
                    {
                        Report(McpLogLevel.Warning, $"left out the second tool named '{tool.Name}'");
                        continue;
                    }

                    if (checkHeaders)
                    {
                        var (mirrored, rejected) = McpHeaders.Read(tool.InputSchema);
                        if (rejected is not null)
                        {
                            Report(McpLogLevel.Warning, $"left out the tool '{tool.Name}': {rejected}");
                            continue;
                        }

                        _headerParameters[tool.Name] = mirrored;
                    }

                    tools.Add(tool);
                }
            }

            cursor = result.TryGetProperty("nextCursor", out var next) && next.ValueKind == JsonValueKind.String ? next.GetString() : null;
            if (cursor is null)
            {
                return tools;
            }

            if (!cursors.Add(cursor))
            {
                throw new McpException($"the MCP server repeated the tools/list cursor '{cursor}'");
            }
        }
    }

    private static McpTool? Read(JsonElement tool)
    {
        if (tool.ValueKind != JsonValueKind.Object || Text(tool, "name") is not { Length: > 0 } name)
        {
            return null;
        }

        JsonElement? annotations = tool.TryGetProperty("annotations", out var a) && a.ValueKind == JsonValueKind.Object ? a.Clone() : null;
        var title = Text(tool, "title") ?? (annotations is { } an ? Text(an, "title") : null);
        var input = tool.TryGetProperty("inputSchema", out var i) && i.ValueKind == JsonValueKind.Object ? i.Clone() : EmptySchema;
        JsonElement? output = tool.TryGetProperty("outputSchema", out var o) && o.ValueKind == JsonValueKind.Object ? o.Clone() : null;
        return new McpTool(name, title, Text(tool, "description"), input, output, annotations);
    }

    private static readonly JsonElement EmptySchema = JsonDocument.Parse("""{"type":"object"}""").RootElement.Clone();

    /// <summary>A request on the current connection, which a stdio server that exited is started again for.</summary>
    private async Task<RpcResponse?> ExchangeAsync(
        string method, JsonObject parameters, string? tool, IReadOnlyList<KeyValuePair<string, string>> headers, TimeSpan? timeout, bool probe,
        CancellationToken cancellationToken)
    {
        var channel = await ChannelAsync(cancellationToken).ConfigureAwait(false);
        return (await SendOnAsync(channel, method, parameters, tool, headers, timeout, probe, cancellationToken).ConfigureAwait(false)).Response;
    }

    /// <summary>The open connection: a stdio server that exited is started and probed again, when
    /// <see cref="McpClientOptions.RestartOnExit"/> allows.</summary>
    private async Task<McpChannel> ChannelAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var channel = _channel;
        if (channel is { IsOpen: true })
        {
            return channel;
        }

        await _connecting.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            channel = _channel;
            if (channel is { IsOpen: true })
            {
                return channel;
            }

            if (channel is not null && (_server is not StdioMcpServer || !_options.RestartOnExit))
            {
                return channel;
            }

            if (channel is not null)
            {
                _channel = null;
                await channel.CloseAsync(new IOException("the MCP server exited")).ConfigureAwait(false);
            }

            Report(McpLogLevel.Information, "starting the MCP server again");
            await OpenAsync(cancellationToken).ConfigureAwait(false);
            return _channel!;
        }
        finally
        {
            _connecting.Release();
        }
    }

    /// <summary>
    /// One request on <paramref name="channel"/>, in its client span: the body with <c>_meta</c>, the timeout, the
    /// caller's token, and what an abandoned request tells the server.
    /// </summary>
    private async Task<ChannelAnswer> SendOnAsync(
        McpChannel channel, string method, JsonObject parameters, string? tool, IReadOnlyList<KeyValuePair<string, string>> headers,
        TimeSpan? timeout, bool probe, CancellationToken cancellationToken)
    {
        var id = Interlocked.Increment(ref _nextId);
        using var scope = new McpRequestScope(method, tool, id, ProtocolVersion, channel.Endpoint);
        if (Meta(scope.Activity ?? Activity.Current) is { Count: > 0 } meta)
        {
            parameters["_meta"] = meta;
        }
        var request = new OutgoingRequest(
            id, method, new JsonObject { ["jsonrpc"] = "2.0", ["id"] = id, ["method"] = method, ["params"] = parameters }, headers);

        using var timer = timeout is { } t ? new CancellationTokenSource(t) : null;
        using var linked = timer is null
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token);
        try
        {
            var answer = await channel.SendAsync(request, probe, linked.Token).ConfigureAwait(false);
            if (answer.Response is { Error: { } error } && !probe)
            {
                scope.Failed(new McpException(error.Message, error.Code), cancelled: false);
            }
            else if (method == "tools/call" && answer.Response is { Result: { ValueKind: JsonValueKind.Object } result }
                && result.TryGetProperty("isError", out var isError) && isError.ValueKind == JsonValueKind.True)
            {
                scope.ToolError();
            }

            return answer;
        }
        catch (OperationCanceledException) when (linked.IsCancellationRequested)
        {
            await channel.AbandonAsync(request).ConfigureAwait(false);
            if (cancellationToken.IsCancellationRequested)
            {
                scope.Failed(new OperationCanceledException(cancellationToken), cancelled: true);
                throw new OperationCanceledException($"the MCP request {method} was cancelled", null, cancellationToken);
            }

            var timedOut = new TimeoutException(
                $"the MCP request {method} timed out after {timeout!.Value.TotalSeconds.ToString(CultureInfo.InvariantCulture)} s");
            scope.Failed(timedOut, cancelled: false);
            throw timedOut;
        }
        catch (Exception ex)
        {
            scope.Failed(ex, cancelled: false);
            throw;
        }
    }

    /// <summary>The request's <c>_meta</c>: the modern keys, and the trace context when there is one. A legacy request
    /// without a trace context gets none.</summary>
    private JsonObject Meta(Activity? activity)
    {
        var meta = new JsonObject();
        if (_modern)
        {
            meta[McpProtocol.MetaProtocolVersion] = McpProtocol.Modern;
            meta[McpProtocol.MetaClientCapabilities] = new JsonObject();
            meta[McpProtocol.MetaClientInfo] = new JsonObject { ["name"] = _options.ClientName, ["version"] = _options.ClientVersion };
        }

        if (activity is not null)
        {
            DistributedContextPropagator.Current.Inject(activity, meta, static (carrier, name, value) =>
            {
                if (name is "traceparent" or "tracestate" && carrier is JsonObject target)
                {
                    target[name] = value;
                }
            });
        }

        return meta;
    }

    /// <summary>A response's result, or its error as an <see cref="McpException"/>; a result Strata can't use fails
    /// too.</summary>
    private static JsonElement Complete(RpcResponse response, string method)
    {
        if (response.Error is { } error)
        {
            throw new McpException($"mcp error {error.Code.ToString(CultureInfo.InvariantCulture)}: {error.Message}", error.Code);
        }

        var result = response.Result;
        if (result.ValueKind != JsonValueKind.Object)
        {
            throw new McpException($"the MCP server's result for {method} isn't an object");
        }

        var type = result.TryGetProperty("resultType", out var t) ? t : default;
        if (type.ValueKind == JsonValueKind.Undefined || (type.ValueKind == JsonValueKind.String && type.GetString() == "complete"))
        {
            return result;
        }

        if (type.ValueKind == JsonValueKind.String && type.GetString() == "input_required")
        {
            var asked = result.TryGetProperty("inputRequests", out var requests) && requests.ValueKind == JsonValueKind.Object
                ? string.Join(", ", requests.EnumerateObject().Select(r => Text(r.Value, "method") ?? r.Name).Distinct())
                : "nothing named";
            throw new McpException($"the MCP server asked for input Strata doesn't provide ({asked}) to {method}");
        }

        throw new McpException($"the MCP server answered {method} with the resultType {type.GetRawText()}, which isn't valid");
    }

    private void Report(McpLogLevel level, string message)
    {
        try
        {
            _options.Log?.Invoke(new McpLogEntry(level, message));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // The application's logger is its own business.
        }
    }

    private static string? Text(JsonElement element, string property) =>
        element.ValueKind == JsonValueKind.Object && element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    /// <summary>The server's tools as a Strata provider: names with the prefix, calls with the server's names.</summary>
    private sealed class McpToolProvider : IStrataToolProvider
    {
        private readonly McpClient _client;
        private readonly Dictionary<string, string> _serverNames = new(StringComparer.Ordinal);
        private readonly string _manifest;

        public McpToolProvider(McpClient client, string prefix, IReadOnlyList<McpTool> tools)
        {
            _client = client;
            var manifest = new JsonArray();
            foreach (var tool in tools)
            {
                var name = prefix + tool.Name;
                _serverNames[name] = tool.Name;
                manifest.Add(new JsonObject
                {
                    ["name"] = name,
                    ["description"] = tool.Description ?? tool.Title ?? string.Empty,
                    ["parameters"] = JsonNode.Parse(tool.InputSchema.GetRawText()),
                });
            }

            _manifest = manifest.ToJsonString();
        }

        public string GetToolsManifest() => _manifest;

        public Task<string> DispatchToolAsync(string toolName, string jsonArguments) =>
            DispatchToolAsync(toolName, jsonArguments, ToolCallContext.None);

        public async Task<string> DispatchToolAsync(
            string toolName, string jsonArguments, ToolCallContext context, CancellationToken cancellationToken = default)
        {
            if (!_serverNames.TryGetValue(toolName, out var serverName))
            {
                throw new ToolNotFoundException(toolName);
            }

            JsonElement arguments;
            if (string.IsNullOrWhiteSpace(jsonArguments))
            {
                arguments = JsonDocument.Parse("{}").RootElement.Clone();
            }
            else
            {
                try
                {
                    using var parsed = JsonDocument.Parse(jsonArguments);
                    arguments = parsed.RootElement.Clone();
                }
                catch (JsonException ex)
                {
                    throw new ToolArgumentsException(toolName, "the arguments aren't valid JSON", ex);
                }

                if (arguments.ValueKind != JsonValueKind.Object)
                {
                    throw new ToolArgumentsException(toolName, "the arguments aren't a JSON object");
                }
            }

            var result = await _client.CallToolAsync(serverName, arguments, cancellationToken).ConfigureAwait(false);
            return result.ToText();
        }
    }
}
