using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Datalium.Strata.McpFixture;

/// <summary>
/// What a fixture server does (<c>design/50-MCP-Client.md</c>, "Tests"): how it answers the era probe and
/// <c>initialize</c>, and how it lists its tools. Parsed from the stdio server's arguments, or built by a test.
/// </summary>
public sealed record FixtureScript
{
    /// <summary>
    /// The answer to <c>server/discover</c>: <c>result</c> (a DiscoverResult listing <c>2026-07-28</c>, and the
    /// <see cref="Initialize"/> version for a dual-era server), <c>versions:a,b</c> (a DiscoverResult listing those),
    /// <c>unsupported:a,b</c> (-32022 with those supported), <c>error:code</c>, or <c>silence</c>.
    /// </summary>
    public string Discover { get; init; } = "result";

    /// <summary>The version <c>initialize</c> answers with; <see langword="null"/> answers it with -32601.</summary>
    public string? Initialize { get; init; }

    public int PageSize { get; init; } = 100;

    /// <summary>Every page names the same next cursor.</summary>
    public bool RepeatCursor { get; init; }

    /// <summary>Lists <c>echo</c> twice.</summary>
    public bool DuplicateTool { get; init; }

    /// <summary>Lists a tool with an invalid <c>x-mcp-header</c>.</summary>
    public bool BadHeaderTool { get; init; }

    /// <summary>The first <c>tools/call</c> of <c>region</c> fails with HeaderMismatch.</summary>
    public bool MismatchOnce { get; init; }

    public string ServerName { get; init; } = "fixture";

    public static FixtureScript Parse(IReadOnlyList<string> args)
    {
        var script = new FixtureScript();
        for (var i = 0; i < args.Count; i++)
        {
            script = args[i] switch
            {
                "--discover" => script with { Discover = args[++i] },
                "--initialize" => script with { Initialize = args[++i] },
                "--page-size" => script with { PageSize = int.Parse(args[++i], System.Globalization.CultureInfo.InvariantCulture) },
                "--repeat-cursor" => script with { RepeatCursor = true },
                "--duplicate-tool" => script with { DuplicateTool = true },
                "--bad-header-tool" => script with { BadHeaderTool = true },
                "--name" => script with { ServerName = args[++i] },
                _ => script,
            };
        }

        return script;
    }
}

/// <summary>How the server reaches its client, which differs between a pipe and an HTTP response.</summary>
public interface IFixtureChannel
{
    /// <summary>Sends a message ahead of the response: a notification, or a request to the client.</summary>
    Task SendAsync(JsonObject message);

    /// <summary>Writes a line that isn't JSON (stdio framing only).</summary>
    Task WriteRawAsync(string line);

    /// <summary>The server goes away in the middle of the call.</summary>
    void Exit();

    /// <summary>The HTTP headers of the request, when there are any.</summary>
    IReadOnlyDictionary<string, string> Headers { get; }
}

/// <summary>
/// A scripted MCP server for Strata's tests: modern, legacy (any version), dual-era or misbehaving, with tools that
/// echo, add, fail, sleep, exit, send too much, send garbage, ask the client, and report what they were sent. It
/// records every message it receives.
/// </summary>
public sealed class FixtureServer(FixtureScript script)
{
    public const string Modern = "2026-07-28";

    private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonObject>> _answers = new();
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _running = new();
    private int _mismatches;
    private int _asked;

    public FixtureScript Script { get; } = script;

    /// <summary>Every message received, in order.</summary>
    public ConcurrentQueue<JsonObject> Received { get; } = new();

    /// <summary>The ids of the requests the client cancelled.</summary>
    public ConcurrentQueue<string> Cancelled { get; } = new();

    /// <summary>Set once a slow call has started, so a test can cancel it.</summary>
    public TaskCompletionSource SlowStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>A child process the server started, for the process-tree test.</summary>
    public int? ChildProcessId { get; set; }

    /// <summary>
    /// Handles one message: returns the response for a request (<see langword="null"/> for a notification, a
    /// response from the client, or a request it doesn't answer).
    /// </summary>
    public async Task<JsonObject?> HandleAsync(JsonObject message, IFixtureChannel channel)
    {
        Received.Enqueue((JsonObject)message.DeepClone());
        var method = message["method"]?.GetValue<string>();
        var id = message["id"];
        if (method is null)
        {
            if (id is not null && _answers.TryRemove(id.ToJsonString(), out var waiting))
            {
                waiting.TrySetResult(message);
            }

            return null;
        }

        if (id is null)
        {
            if (method == "notifications/cancelled" && message["params"]?["requestId"] is { } cancelled)
            {
                Cancelled.Enqueue(cancelled.ToJsonString());
                if (_running.TryRemove(cancelled.ToJsonString(), out var cts))
                {
                    await cts.CancelAsync();
                }
            }

            return null;
        }

        var parameters = message["params"] as JsonObject ?? [];
        try
        {
            var result = method switch
            {
                "server/discover" => Discover(),
                "initialize" => Script.Initialize is { } version ? Initialize(version) : throw new RpcFault(-32601, "Method not found: initialize"),
                "tools/list" => ListTools(parameters),
                "tools/call" => await CallAsync(id, parameters, channel),
                "ping" => [],
                _ => throw new RpcFault(-32601, $"Method not found: {method}"),
            };
            return result is null ? null : new JsonObject { ["jsonrpc"] = "2.0", ["id"] = id.DeepClone(), ["result"] = result };
        }
        catch (RpcFault fault)
        {
            var error = new JsonObject { ["code"] = fault.Code, ["message"] = fault.Message };
            if (fault.Payload is not null)
            {
                error["data"] = fault.Payload;
            }

            return new JsonObject { ["jsonrpc"] = "2.0", ["id"] = id.DeepClone(), ["error"] = error };
        }
    }

    private JsonObject? Discover()
    {
        var discover = Script.Discover;
        if (discover == "silence")
        {
            return null;
        }

        if (discover.StartsWith("error:", StringComparison.Ordinal))
        {
            throw new RpcFault(int.Parse(discover[6..], System.Globalization.CultureInfo.InvariantCulture), "not a method I know");
        }

        if (discover.StartsWith("unsupported:", StringComparison.Ordinal))
        {
            throw new RpcFault(-32022, "Unsupported protocol version", new JsonObject { ["supported"] = Versions(discover[12..]) });
        }

        var versions = discover.StartsWith("versions:", StringComparison.Ordinal)
            ? Versions(discover[9..])
            : Script.Initialize is { } legacy ? new JsonArray(Modern, legacy) : new JsonArray(Modern);
        return new JsonObject
        {
            ["resultType"] = "complete",
            ["supportedVersions"] = versions,
            ["capabilities"] = new JsonObject { ["tools"] = new JsonObject() },
            ["instructions"] = "modern instructions",
            ["_meta"] = new JsonObject
            {
                ["io.modelcontextprotocol/serverInfo"] = new JsonObject { ["name"] = Script.ServerName, ["version"] = "2.0.0" },
            },
        };
    }

    private static JsonArray Versions(string list) => new([.. list.Split(',').Select(v => (JsonNode)JsonValue.Create(v))]);

    private JsonObject Initialize(string version) => new()
    {
        ["protocolVersion"] = version,
        ["capabilities"] = new JsonObject { ["tools"] = new JsonObject() },
        ["serverInfo"] = new JsonObject { ["name"] = Script.ServerName + "-legacy", ["version"] = "1.0.0" },
        ["instructions"] = "legacy instructions",
    };

    private JsonObject ListTools(JsonObject parameters)
    {
        var all = Tools();
        var start = parameters["cursor"]?.GetValue<string>() is { } cursor && int.TryParse(cursor, out var at) ? at : 0;
        var page = all.Skip(start).Take(Script.PageSize).Select(t => t.DeepClone()).ToArray();
        var result = new JsonObject { ["tools"] = new JsonArray(page) };
        if (Script.RepeatCursor)
        {
            result["nextCursor"] = "1";
        }
        else if (start + Script.PageSize < all.Count)
        {
            result["nextCursor"] = (start + Script.PageSize).ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        return result;
    }

    private List<JsonObject> Tools()
    {
        static JsonObject Tool(string name, string? description, JsonObject? properties = null, JsonObject? extra = null)
        {
            var tool = new JsonObject
            {
                ["name"] = name,
                ["inputSchema"] = new JsonObject { ["type"] = "object", ["properties"] = properties ?? [] },
            };
            if (description is not null)
            {
                tool["description"] = description;
            }

            foreach (var (key, value) in extra ?? [])
            {
                tool[key] = value?.DeepClone();
            }

            return tool;
        }

        static JsonObject Property(string type, string? header = null)
        {
            var property = new JsonObject { ["type"] = type };
            if (header is not null)
            {
                property["x-mcp-header"] = header;
            }

            return property;
        }

        var tools = new List<JsonObject>
        {
            Tool("echo", "Echoes its text.", new JsonObject { ["text"] = Property("string") }),
            Tool("add", "Adds two integers.", new JsonObject { ["a"] = Property("integer"), ["b"] = Property("integer") }),
            Tool("fail", "Always reports an error."),
            Tool("image", "Returns a picture."),
            Tool("slow", "Waits.", new JsonObject { ["ms"] = Property("integer") }),
            Tool("exit", "Exits in the middle of the call."),
            Tool("big", "Returns this many bytes.", new JsonObject { ["bytes"] = Property("integer") }),
            Tool("garbage", "Writes a line that isn't JSON."),
            Tool("ask", "Asks the client for things before answering."),
            Tool("meta", "Returns the _meta it was sent."),
            Tool("headers", "Returns the HTTP headers it was sent."),
            Tool("args", "Returns its command line."),
            Tool("env", "Returns an environment variable.", new JsonObject { ["name"] = Property("string") }),
            Tool("child", "Returns the id of the process it started."),
            Tool("progress", "Sends notifications before answering."),
            Tool("input", "Asks for input."),
            Tool("region", "Mirrors its region into a header.", new JsonObject
            {
                ["region"] = Property("string", "Region"),
                ["limit"] = Property("integer", "Limit"),
            }),
            Tool("titled", null, extra: new JsonObject
            {
                ["title"] = "A titled tool",
                ["annotations"] = new JsonObject { ["readOnlyHint"] = true },
                ["outputSchema"] = new JsonObject { ["type"] = "object" },
            }),
        };
        if (Script.DuplicateTool)
        {
            tools.Add(Tool("echo", "The second echo."));
        }

        if (Script.BadHeaderTool)
        {
            tools.Add(Tool("bad_header", "Its header is on a number.", new JsonObject { ["price"] = Property("number", "Price") }));
        }

        return tools;
    }

    private async Task<JsonObject?> CallAsync(JsonNode id, JsonObject parameters, IFixtureChannel channel)
    {
        var name = parameters["name"]?.GetValue<string>();
        var arguments = parameters["arguments"] as JsonObject ?? [];
        switch (name)
        {
            case "echo":
                return Text(arguments["text"]?.GetValue<string>() ?? string.Empty);
            case "add":
                var sum = arguments["a"]!.GetValue<long>() + arguments["b"]!.GetValue<long>();
                return new JsonObject
                {
                    ["content"] = new JsonArray(new JsonObject { ["type"] = "text", ["text"] = $"{{\"sum\": {sum}}}" }),
                    ["structuredContent"] = new JsonObject { ["sum"] = sum },
                };
            case "fail":
                var failed = Text("it broke");
                failed["isError"] = true;
                return failed;
            case "image":
                return new JsonObject
                {
                    ["content"] = new JsonArray(
                        new JsonObject { ["type"] = "text", ["text"] = "a picture:" },
                        new JsonObject { ["type"] = "image", ["data"] = "iVBORw0KGgo=", ["mimeType"] = "image/png" }),
                };
            case "slow":
                using (var cts = new CancellationTokenSource())
                {
                    _running[id.ToJsonString()] = cts;
                    SlowStarted.TrySetResult();
                    try
                    {
                        await Task.Delay(arguments["ms"]?.GetValue<int>() ?? 60_000, cts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        // The spec: no response for a cancelled request.
                        return null;
                    }
                    finally
                    {
                        _running.TryRemove(id.ToJsonString(), out _);
                    }
                }

                return Text("done");
            case "exit":
                channel.Exit();
                return null;
            case "big":
                return Text(new string('x', arguments["bytes"]?.GetValue<int>() ?? 1024));
            case "garbage":
                await channel.WriteRawAsync("this is not JSON");
                return null;
            case "ask":
                var n = Interlocked.Increment(ref _asked);
                var ping = Ask($"ping-{n}", "ping", channel);
                var sampling = Ask($"sample-{n}", "sampling/createMessage", channel);
                var answers = await Task.WhenAll(ping, sampling);
                return Text(new JsonArray([.. answers.Select(a => a.DeepClone())]).ToJsonString());
            case "meta":
                return Text(parameters["_meta"]?.ToJsonString() ?? "null");
            case "headers":
                return Text(JsonSerializer.Serialize(channel.Headers));
            case "args":
                return Text(JsonSerializer.Serialize(Environment.GetCommandLineArgs().Skip(1)));
            case "env":
                return Text(Environment.GetEnvironmentVariable(arguments["name"]?.GetValue<string>() ?? string.Empty) ?? "(unset)");
            case "child":
                return Text(ChildProcessId?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "none");
            case "progress":
                await channel.SendAsync(new JsonObject
                {
                    ["jsonrpc"] = "2.0",
                    ["method"] = "notifications/progress",
                    ["params"] = new JsonObject { ["progressToken"] = "p", ["progress"] = 1 },
                });
                await channel.SendAsync(new JsonObject
                {
                    ["jsonrpc"] = "2.0",
                    ["method"] = "notifications/message",
                    ["params"] = new JsonObject { ["level"] = "info", ["data"] = "working" },
                });
                return Text("progressed");
            case "input":
                return new JsonObject
                {
                    ["resultType"] = "input_required",
                    ["inputRequests"] = new JsonObject
                    {
                        ["confirm"] = new JsonObject { ["method"] = "elicitation/create", ["params"] = new JsonObject() },
                    },
                };
            case "region":
                if (Script.MismatchOnce && Interlocked.Increment(ref _mismatches) == 1)
                {
                    throw new RpcFault(-32020, "Header mismatch: Mcp-Param-Region");
                }

                return Text(JsonSerializer.Serialize(channel.Headers.Where(h => h.Key.StartsWith("Mcp-", StringComparison.OrdinalIgnoreCase))
                    .ToDictionary(h => h.Key, h => h.Value)));
            case "titled":
                return Text("titled");
            default:
                throw new RpcFault(-32602, $"Unknown tool: {name}");
        }
    }

    private async Task<JsonObject> Ask(string id, string method, IFixtureChannel channel)
    {
        var answer = new TaskCompletionSource<JsonObject>(TaskCreationOptions.RunContinuationsAsynchronously);
        _answers[JsonValue.Create(id).ToJsonString()] = answer;
        await channel.SendAsync(new JsonObject { ["jsonrpc"] = "2.0", ["id"] = id, ["method"] = method, ["params"] = new JsonObject() });
        return await answer.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    private static JsonObject Text(string text) => new()
    {
        ["content"] = new JsonArray(new JsonObject { ["type"] = "text", ["text"] = text }),
    };

    /// <summary>
    /// Serves stdio's framing on two streams until the input ends: each request handled on its own task, so a slow
    /// one doesn't hold up the rest. <paramref name="exit"/> is what the <c>exit</c> tool does.
    /// </summary>
    public async Task RunLinesAsync(Stream input, Stream output, Action exit, CancellationToken cancellationToken = default)
    {
        var writing = new SemaphoreSlim(1, 1);
        var channel = new LineFixtureChannel(output, writing, exit);
        using var reader = new StreamReader(input, new UTF8Encoding(false));
        var running = new List<Task>();
        while (!cancellationToken.IsCancellationRequested && await reader.ReadLineAsync(cancellationToken) is { } line)
        {
            if (line.Length == 0)
            {
                continue;
            }

            if (JsonNode.Parse(line) is not JsonObject message)
            {
                continue;
            }

            // Called here, not on another task: it records the message before its first await, so Received keeps
            // the order messages arrived in, and a slow tool still doesn't hold up the rest.
            running.Add(RespondAsync(message, channel));
        }

        await Task.WhenAll(running.Where(t => t.IsCompleted));
    }

    private async Task RespondAsync(JsonObject message, IFixtureChannel channel)
    {
        var response = await HandleAsync(message, channel);
        if (response is not null)
        {
            await channel.SendAsync(response);
        }
    }

    private sealed class LineFixtureChannel(Stream output, SemaphoreSlim writing, Action exit) : IFixtureChannel
    {
        public IReadOnlyDictionary<string, string> Headers { get; } = new Dictionary<string, string>();

        public Task SendAsync(JsonObject message) => WriteRawAsync(message.ToJsonString());

        public async Task WriteRawAsync(string line)
        {
            var bytes = Encoding.UTF8.GetBytes(line + "\n");
            await writing.WaitAsync();
            try
            {
                await output.WriteAsync(bytes);
                await output.FlushAsync();
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException)
            {
                // The client went away.
            }
            finally
            {
                writing.Release();
            }
        }

        public void Exit() => exit();
    }
}

/// <summary>A JSON-RPC error the fixture answers with.</summary>
public sealed class RpcFault(int code, string message, JsonNode? data = null) : Exception(message)
{
    public int Code { get; } = code;

    public JsonNode? Payload { get; } = data;
}
