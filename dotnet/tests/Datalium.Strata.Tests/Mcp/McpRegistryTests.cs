using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.AI;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Mcp;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Tests.AI;
using Datalium.Strata.Tests.Tools;
using Datalium.Strata.Tools;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.Tests.Mcp;

/// <summary>
/// An MCP server's tools as Strata tools (<c>design/50-MCP-Client.md</c>, "Tools", "Telemetry"): through a registry
/// beside a <c>[StrataTool]</c> provider, with subsets and middleware, in an agent, and in the client spans and the
/// duration metric.
/// </summary>
[Collection(McpTelemetryCollection.Name)]
public sealed class McpRegistryTests
{
    public sealed record Job(string Answer = "");

    private static async Task<(McpClient Client, McpFixture.FixtureServer Fixture)> ConnectAsync(McpClientOptions? options = null)
    {
        var (server, fixture) = McpTestServers.InMemory();
        return (await McpClient.ConnectAsync(server, options), fixture);
    }

    [Fact]
    public async Task An_Mcp_Tool_Sits_Beside_A_Generated_One_Under_Its_Prefix()
    {
        var (client, fixture) = await ConnectAsync();
        await using var _ = client;
        var tools = new ToolRegistry()
            .Register(new SampleTools())
            .Register(client.ToolProvider(new McpToolOptions { Prefix = "fx.", Include = ["echo", "add", "fail", "titled"] }));

        Assert.Equal(["check_inventory", "read_user_balance", "fx.echo", "fx.add", "fx.fail", "fx.titled"], tools.Tools.Select(t => t.Name).Take(6));
        var echo = tools.Tools.Single(t => t.Name == "fx.echo");
        Assert.Equal("Echoes its text.", echo.Description);
        Assert.Equal("string", echo.Parameters.GetProperty("properties").GetProperty("text").GetProperty("type").GetString());
        Assert.Equal("A titled tool", tools.Tools.Single(t => t.Name == "fx.titled").Description);

        Assert.Equal("hello", await tools.DispatchToolAsync("fx.echo", """{"text":"hello"}"""));
        Assert.Equal("""{"sum":7}""", await tools.DispatchToolAsync("fx.add", """{"a":3,"b":4}"""));
        Assert.Equal("""{"error":"it broke"}""", await tools.DispatchToolAsync("fx.fail", "{}"));
        Assert.Equal("SKU a count is 42 in Zone 1", JsonNode.Parse(await tools.DispatchToolAsync("check_inventory", """{"sku":"a","warehouseId":1}"""))!["result"]!.GetValue<string>());

        // The server is called by its own name.
        Assert.Contains(fixture.Received, m => m["params"]?["name"]?.GetValue<string>() == "echo");
        await Assert.ThrowsAsync<ToolNotFoundException>(() => tools.DispatchToolAsync("fx.slow", "{}"));
    }

    [Fact]
    public async Task An_Include_The_Server_Did_Not_List_Is_Refused()
    {
        var (client, _) = await ConnectAsync();
        await using var __ = client;
        var error = Assert.Throws<ToolNotFoundException>(() => client.ToolProvider(new McpToolOptions { Include = ["echo", "missing"] }));
        Assert.Equal("missing", error.ToolName);
    }

    [Fact]
    public async Task Two_Servers_With_The_Same_Tool_Collide_Unless_Prefixed()
    {
        var (one, _) = await ConnectAsync();
        var (two, _) = await ConnectAsync();
        await using var _1 = one;
        await using var _2 = two;

        var tools = new ToolRegistry().Register(one.ToolProvider());
        Assert.Throws<DuplicateToolException>(() => tools.Register(two.ToolProvider()));
        tools.Register(two.ToolProvider(new McpToolOptions { Prefix = "two." }));
        Assert.Equal("x", await tools.DispatchToolAsync("two.echo", """{"text":"x"}"""));
    }

    [Fact]
    public async Task Arguments_That_Are_Not_A_Json_Object_Are_Refused_Before_The_Server_Sees_Them()
    {
        var (client, fixture) = await ConnectAsync();
        await using var _ = client;
        var tools = new ToolRegistry().Register(client.ToolProvider());

        var notObject = await Assert.ThrowsAsync<ToolArgumentsException>(() => tools.DispatchToolAsync("echo", "[1]"));
        Assert.Equal("the arguments aren't a JSON object", notObject.Problems);
        var notJson = await Assert.ThrowsAsync<ToolArgumentsException>(() => tools.DispatchToolAsync("echo", "{nope"));
        Assert.Equal("the arguments aren't valid JSON", notJson.Problems);
        Assert.DoesNotContain(fixture.Received, m => m["params"]?["name"]?.GetValue<string>() == "echo");

        // No arguments at all are an empty object, as a model sends for a tool that takes none.
        await tools.DispatchToolAsync("meta", "");
        Assert.Equal("{}", fixture.Received.Last()["params"]!["arguments"]!.ToJsonString());
    }

    [Fact]
    public async Task Subsets_And_Middleware_Apply_To_Mcp_Tools()
    {
        var (client, fixture) = await ConnectAsync();
        await using var _ = client;
        var seen = new ConcurrentQueue<string>();
        var tools = new ToolRegistry()
            .Register(client.ToolProvider())
            .Use(new Recording(seen))
            .Use(new Policy());

        var subset = tools.Subset("echo", "fail", "add");
        await Assert.ThrowsAsync<ToolNotPermittedException>(() => subset.DispatchToolAsync("slow", "{}"));
        Assert.Equal("hi", await subset.DispatchToolAsync("echo", """{"text":"hi"}"""));
        await Assert.ThrowsAsync<ToolCallDeniedException>(() => subset.DispatchToolAsync("fail", "{}"));
        Assert.Equal("cached", await subset.DispatchToolAsync("add", """{"a":1,"b":1}"""));

        Assert.Equal(["echo", "fail", "add"], seen);
        Assert.DoesNotContain(fixture.Received, m => m["params"]?["name"]?.GetValue<string>() is "fail" or "add");
    }

    [Fact]
    public async Task A_Tools_Own_Error_Reaches_The_Model_And_The_Agent_Goes_On()
    {
        var (client, _) = await ConnectAsync();
        await using var __ = client;
        var chat = new ScriptedChatClient(Scripted.Tools(("1", "fail", "{}")), Scripted.Text("recovered"));
        var agent = new StrataAgent<string>(chat, new ToolRegistry().Register(client.ToolProvider()), new StrataAgentOptions<string> { Id = "mcp" });
        var graph = new StateGraph<Job>()
            .AddAgentNode("Ask", agent, (_, _) => [new(ChatRole.User, "go")], (job, result) => job with { Answer = result.Output })
            .Compile();

        var result = await graph.RunAsync("mcp-agent", new Job(), new InMemoryCheckpointStore());

        Assert.Equal("recovered", Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State.Answer);
        var second = chat.Requests.ElementAt(1).Messages;
        var told = second.SelectMany(m => m.Contents).OfType<FunctionResultContent>().Single();
        Assert.Equal("""{"error":"it broke"}""", told.Result?.ToString());
    }

    [Fact]
    public async Task A_Call_Through_The_Registry_Gets_An_Mcp_Client_Span_Under_Its_Tool_Span()
    {
        var spans = new ConcurrentBag<Activity>();
        using var listener = Listen(spans);
        McpClient client;
        McpFixture.FixtureServer fixture;
        ActivityTraceId trace;
        // Everything under one root, so spans of tests running meanwhile are left out.
        using (var root = new ActivitySource("mcp-test").StartActivity("root")!)
        {
            trace = root.TraceId;
            (client, fixture) = await ConnectAsync();
            var tools = new ToolRegistry().Register(client.ToolProvider(new McpToolOptions { Prefix = "fx." }));
            await tools.DispatchToolAsync("fx.echo", """{"text":"x"}""");
            await tools.DispatchToolAsync("fx.fail", "{}");
            await Assert.ThrowsAsync<McpException>(() => client.CallToolAsync("nope", JsonDocument.Parse("{}").RootElement));
            await tools.DispatchToolAsync("fx.meta", "{}");
        }

        await using var _ = client;
        var ours = spans.Where(s => s.TraceId == trace).ToList();
        var mine = ours.Where(s => s.Source.Name == McpTelemetry.Name).ToList();
        var tool = ours.Single(s => s.OperationName == "strata.tool" && (string?)s.GetTagItem("gen_ai.tool.name") == "fx.echo");
        var call = mine.Single(s => s.DisplayName == "tools/call echo");
        Assert.Equal(ActivityKind.Client, call.Kind);
        Assert.Equal(tool.SpanId, call.ParentSpanId);
        Assert.Equal("tools/call", call.GetTagItem("mcp.method.name"));
        Assert.Equal("2026-07-28", call.GetTagItem("mcp.protocol.version"));
        Assert.Equal("pipe", call.GetTagItem("network.transport"));
        Assert.Equal("echo", call.GetTagItem("gen_ai.tool.name"));
        Assert.Equal("execute_tool", call.GetTagItem("gen_ai.operation.name"));
        Assert.NotNull(call.GetTagItem("jsonrpc.request.id"));
        Assert.Equal(ActivityStatusCode.Unset, call.Status);

        var failed = mine.Single(s => s.DisplayName == "tools/call fail");
        Assert.Equal("tool_error", failed.GetTagItem("error.type"));
        Assert.Equal(ActivityStatusCode.Unset, failed.Status);

        var unknown = mine.Single(s => s.DisplayName == "tools/call nope");
        Assert.Equal("-32602", unknown.GetTagItem("error.type"));
        Assert.Equal("-32602", unknown.GetTagItem("rpc.status_code"));
        Assert.Equal(ActivityStatusCode.Error, unknown.Status);

        Assert.Contains(mine, s => s.DisplayName == "server/discover");
        Assert.Contains(mine, s => s.DisplayName == "tools/list");

        // The server's spans can join the caller's trace: traceparent in _meta, the MCP span's.
        var meta = mine.Single(s => s.DisplayName == "tools/call meta");
        var sent = fixture.Received.Single(m => m["params"]?["name"]?.GetValue<string>() == "meta")["params"]!["_meta"]!;
        Assert.Equal(meta.Id, sent["traceparent"]!.GetValue<string>());
    }

    [Fact]
    public async Task Each_Request_Is_Measured_By_Method_Tool_Version_Transport_And_Error()
    {
        var measured = new ConcurrentQueue<(double Seconds, Dictionary<string, object?> Tags)>();
        using var meters = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == McpTelemetry.Name && instrument.Name == "mcp.client.operation.duration")
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        meters.SetMeasurementEventCallback<double>((_, value, tags, _) =>
            measured.Enqueue((value, tags.ToArray().ToDictionary(t => t.Key, t => t.Value))));
        meters.Start();
        var (client, _) = await ConnectAsync(new McpClientOptions { ClientName = "metered" });
        await using var __ = client;

        await client.CallToolAsync("fail", JsonDocument.Parse("{}").RootElement);

        var call = measured.Last(m => (string?)m.Tags.GetValueOrDefault("gen_ai.tool.name") == "fail");
        Assert.Equal("tools/call", call.Tags["mcp.method.name"]);
        Assert.Equal("2026-07-28", call.Tags["mcp.protocol.version"]);
        Assert.Equal("pipe", call.Tags["network.transport"]);
        Assert.Equal("tool_error", call.Tags["error.type"]);
        Assert.True(call.Seconds >= 0);
        Assert.Contains(measured, m => (string?)m.Tags["mcp.method.name"] == "server/discover" && !m.Tags.ContainsKey("gen_ai.tool.name"));
        Assert.DoesNotContain(measured, m => m.Tags.ContainsKey("jsonrpc.request.id") || m.Tags.ContainsKey("mcp.session.id"));
    }

    [Fact]
    public async Task Without_A_Trace_There_Is_No_Traceparent()
    {
        var (client, fixture) = await ConnectAsync();
        await using var _ = client;
        Assert.Null(Activity.Current);
        await client.CallToolAsync("meta", JsonDocument.Parse("{}").RootElement);
        Assert.Null(fixture.Received.Last(m => m["method"]?.GetValue<string>() == "tools/call")["params"]!["_meta"]!["traceparent"]);
    }

    internal static ActivityListener Listen(ConcurrentBag<Activity> spans)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name is StrataTelemetry.Name or McpTelemetry.Name or "mcp-test",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private sealed class Recording(ConcurrentQueue<string> seen) : IToolMiddleware
    {
        public Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken)
        {
            seen.Enqueue(call.Name);
            return next(call, cancellationToken);
        }
    }

    /// <summary>Denies <c>fail</c>, and answers <c>add</c> without calling the server.</summary>
    private sealed class Policy : IToolMiddleware
    {
        public Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken) => call.Name switch
        {
            "fail" => throw new ToolCallDeniedException(call.Name, "not today"),
            "add" => Task.FromResult("cached"),
            _ => next(call, cancellationToken),
        };
    }
}

/// <summary>The MCP tests that listen to telemetry, run one at a time so they see only their own spans.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class McpTelemetryCollection
{
    public const string Name = "MCP telemetry";
}
