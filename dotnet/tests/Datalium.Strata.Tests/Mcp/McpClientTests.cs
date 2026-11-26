using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Mcp;
using Datalium.Strata.McpFixture;

namespace Datalium.Strata.Tests.Mcp;

/// <summary>
/// The MCP client against the scripted server in memory (<c>design/50-MCP-Client.md</c>, "Tests"): era detection,
/// both eras' exchanges, the tool list, results, errors, cancellation and timeouts.
/// </summary>
public sealed class McpClientTests
{
    private static JsonElement Args(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static async Task<(McpClient Client, FixtureServer Fixture)> ConnectAsync(FixtureScript? script = null, McpClientOptions? options = null)
    {
        var (server, fixture) = McpTestServers.InMemory(script);
        return (await McpClient.ConnectAsync(server, options), fixture);
    }

    private static string Text(McpToolResult result) => result.ToText();

    private static List<JsonObject> Requests(FixtureServer fixture) =>
        fixture.Received.Where(m => m["id"] is not null && m["method"] is not null).ToList();

    [Fact]
    public async Task A_Modern_Server_Is_Discovered_And_Its_Tools_Listed_In_Its_Order()
    {
        var (client, fixture) = await ConnectAsync();
        await using var _ = client;

        Assert.Equal("2026-07-28", client.ProtocolVersion);
        Assert.Equal(new McpImplementation("fixture", "2.0.0"), client.ServerInfo);
        Assert.Equal("modern instructions", client.Instructions);
        Assert.Equal("echo", client.Tools[0].Name);
        Assert.Equal("Echoes its text.", client.Tools[0].Description);
        Assert.DoesNotContain(fixture.Received, m => m["method"]?.GetValue<string>() == "initialize");
        Assert.Equal(["server/discover", "tools/list"], Requests(fixture).Select(m => m["method"]!.GetValue<string>()));
    }

    [Fact]
    public async Task Every_Modern_Request_Carries_The_Version_The_Capabilities_And_The_Client_In_Its_Meta()
    {
        var (client, fixture) = await ConnectAsync();
        await using var _ = client;
        await client.CallToolAsync("echo", Args("""{"text":"hi"}"""));

        var requests = Requests(fixture);
        Assert.Equal(3, requests.Count);
        Assert.All(requests, request =>
        {
            var meta = request["params"]!["_meta"]!;
            Assert.Equal("2026-07-28", meta["io.modelcontextprotocol/protocolVersion"]!.GetValue<string>());
            Assert.Equal("{}", meta["io.modelcontextprotocol/clientCapabilities"]!.ToJsonString());
            Assert.Equal("datalium-strata", meta["io.modelcontextprotocol/clientInfo"]!["name"]!.GetValue<string>());
            Assert.False(string.IsNullOrEmpty(meta["io.modelcontextprotocol/clientInfo"]!["version"]!.GetValue<string>()));
        });
        Assert.All(requests.Select(r => r["id"]!.GetValue<long>()).Order().Zip(requests.Select(r => r["id"]!.GetValue<long>())), p => Assert.Equal(p.First, p.Second));
    }

    [Theory]
    [InlineData("2025-11-25")]
    [InlineData("2025-06-18")]
    [InlineData("2025-03-26")]
    [InlineData("2024-11-05")]
    public async Task A_Legacy_Server_Gets_The_Initialize_Handshake_And_Its_Version_Is_Used(string version)
    {
        var (client, fixture) = await ConnectAsync(new FixtureScript { Discover = "error:-32601", Initialize = version });
        await using var _ = client;
        await client.CallToolAsync("echo", Args("""{"text":"hi"}"""));

        Assert.Equal(version, client.ProtocolVersion);
        Assert.Equal("fixture-legacy", client.ServerInfo!.Name);
        Assert.Equal("legacy instructions", client.Instructions);
        var initialize = fixture.Received.Single(m => m["method"]?.GetValue<string>() == "initialize");
        Assert.Equal("2025-11-25", initialize["params"]!["protocolVersion"]!.GetValue<string>());
        Assert.Equal("{}", initialize["params"]!["capabilities"]!.ToJsonString());
        Assert.Equal("datalium-strata", initialize["params"]!["clientInfo"]!["name"]!.GetValue<string>());

        var methods = fixture.Received.Select(m => m["method"]?.GetValue<string>()).ToList();
        Assert.Equal(["server/discover", "initialize", "notifications/initialized", "tools/list", "tools/call"], methods);
        // Legacy requests carry no modern _meta: the version is the session's.
        Assert.All(Requests(fixture).Skip(1), r => Assert.Null(r["params"]?["_meta"]?["io.modelcontextprotocol/protocolVersion"]));
    }

    [Fact]
    public async Task A_Legacy_Version_Strata_Does_Not_Speak_Fails_Connecting()
    {
        var (server, _) = McpTestServers.InMemory(new FixtureScript { Discover = "error:-32601", Initialize = "2099-01-01" });
        var error = await Assert.ThrowsAsync<McpException>(() => McpClient.ConnectAsync(server));
        Assert.Contains("2099-01-01", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_Server_That_Says_Nothing_To_The_Probe_Is_Taken_For_Legacy()
    {
        var (client, fixture) = await ConnectAsync(
            new FixtureScript { Discover = "silence", Initialize = "2025-06-18" },
            new McpClientOptions { ProbeTimeout = TimeSpan.FromMilliseconds(200) });
        await using var _ = client;

        Assert.Equal("2025-06-18", client.ProtocolVersion);
        Assert.Contains(fixture.Received, m => m["method"]?.GetValue<string>() == "initialize");
    }

    [Fact]
    public async Task An_Unsupported_Version_Answer_Initializes_With_A_Version_It_Supports()
    {
        var (client, fixture) = await ConnectAsync(new FixtureScript { Discover = "unsupported:2025-03-26,2025-06-18", Initialize = "2025-06-18" });
        await using var _ = client;

        var initialize = fixture.Received.Single(m => m["method"]?.GetValue<string>() == "initialize");
        Assert.Equal("2025-06-18", initialize["params"]!["protocolVersion"]!.GetValue<string>());
    }

    [Theory]
    [InlineData("error:-32021")]
    [InlineData("error:-32020")]
    [InlineData("unsupported:2099-01-01")]
    public async Task A_Recognized_Modern_Error_Never_Falls_Back_To_Initialize(string discover)
    {
        var (server, fixture) = McpTestServers.InMemory(new FixtureScript { Discover = discover, Initialize = "2025-11-25" });
        await Assert.ThrowsAsync<McpException>(() => McpClient.ConnectAsync(server));
        Assert.DoesNotContain(fixture.Received, m => m["method"]?.GetValue<string>() == "initialize");
    }

    [Fact]
    public async Task A_Dual_Era_Server_Stays_Modern()
    {
        var (client, fixture) = await ConnectAsync(new FixtureScript { Initialize = "2025-11-25" });
        await using var _ = client;
        Assert.Equal("2026-07-28", client.ProtocolVersion);
        Assert.DoesNotContain(fixture.Received, m => m["method"]?.GetValue<string>() == "initialize");
    }

    [Fact]
    public async Task A_Legacy_Servers_Requests_Are_Answered_Ping_With_An_Empty_Result_The_Rest_Not_Found()
    {
        var (client, _) = await ConnectAsync(new FixtureScript { Discover = "error:-32601", Initialize = "2025-11-25" });
        await using var __ = client;

        var answers = JsonNode.Parse(Text(await client.CallToolAsync("ask", Args("{}"))))!.AsArray();
        Assert.Equal("{}", answers[0]!["result"]!.ToJsonString());
        Assert.Equal(-32601, answers[1]!["error"]!["code"]!.GetValue<int>());
    }

    [Fact]
    public async Task Notifications_From_The_Server_Are_Ignored_And_Logged()
    {
        var log = new ConcurrentQueue<McpLogEntry>();
        var (client, _) = await ConnectAsync(options: McpTestServers.Logging(log));
        await using var __ = client;

        Assert.Equal("progressed", Text(await client.CallToolAsync("progress", Args("{}"))));
        Assert.Contains(log, e => e.Level == McpLogLevel.Debug && e.Message.Contains("notifications/progress", StringComparison.Ordinal));
    }

    [Fact]
    public async Task The_Tool_List_Is_Read_Across_Pages()
    {
        var (client, fixture) = await ConnectAsync(new FixtureScript { PageSize = 3 });
        await using var _ = client;

        var (all, __) = await ConnectAsync();
        await using var ___ = all;
        Assert.Equal(all.Tools.Select(t => t.Name), client.Tools.Select(t => t.Name));
        Assert.True(fixture.Received.Count(m => m["method"]?.GetValue<string>() == "tools/list") > 3);
    }

    [Fact]
    public async Task A_Repeated_Cursor_Fails_Connecting_Rather_Than_Looping()
    {
        var (server, _) = McpTestServers.InMemory(new FixtureScript { PageSize = 2, RepeatCursor = true });
        var error = await Assert.ThrowsAsync<McpException>(() => McpClient.ConnectAsync(server));
        Assert.Contains("cursor", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_Name_Listed_Twice_Keeps_The_First_With_A_Warning()
    {
        var log = new ConcurrentQueue<McpLogEntry>();
        var (client, _) = await ConnectAsync(new FixtureScript { DuplicateTool = true }, McpTestServers.Logging(log));
        await using var __ = client;

        Assert.Single(client.Tools, t => t.Name == "echo");
        Assert.Equal("Echoes its text.", client.Tools.Single(t => t.Name == "echo").Description);
        Assert.Contains(log, e => e.Level == McpLogLevel.Warning && e.Message.Contains("'echo'", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Header_Annotations_Are_Not_Checked_Off_Http()
    {
        var (client, _) = await ConnectAsync(new FixtureScript { BadHeaderTool = true });
        await using var __ = client;
        Assert.Contains(client.Tools, t => t.Name == "bad_header");
    }

    [Fact]
    public async Task A_Tool_Keeps_Its_Title_Annotations_And_Output_Schema()
    {
        var (client, _) = await ConnectAsync();
        await using var __ = client;

        var titled = client.Tools.Single(t => t.Name == "titled");
        Assert.Equal("A titled tool", titled.Title);
        Assert.Null(titled.Description);
        Assert.True(titled.Annotations!.Value.GetProperty("readOnlyHint").GetBoolean());
        Assert.Equal("object", titled.OutputSchema!.Value.GetProperty("type").GetString());
    }

    [Fact]
    public async Task Results_Come_Back_Whole_And_As_Text()
    {
        var (client, _) = await ConnectAsync();
        await using var __ = client;

        var sum = await client.CallToolAsync("add", Args("""{"a":2,"b":3}"""));
        Assert.Equal(5, sum.StructuredContent!.Value.GetProperty("sum").GetInt32());
        Assert.Equal("""{"sum":5}""", sum.ToText());

        var image = await client.CallToolAsync("image", Args("{}"));
        Assert.Equal("iVBORw0KGgo=", image.Content[1].GetProperty("data").GetString());
        Assert.Equal("a picture:\n\n[image/png, 1 KB, not shown]", image.ToText());

        var failed = await client.CallToolAsync("fail", Args("{}"));
        Assert.True(failed.IsError);
        Assert.Equal("""{"error":"it broke"}""", failed.ToText());
    }

    [Fact]
    public async Task A_Json_Rpc_Error_Is_An_McpException_With_Its_Code()
    {
        var (client, _) = await ConnectAsync();
        await using var __ = client;

        var error = await Assert.ThrowsAsync<McpException>(() => client.CallToolAsync("nope", Args("{}")));
        Assert.Equal(-32602, error.Code);
        Assert.Equal("mcp error -32602: Unknown tool: nope", error.Message);
    }

    [Fact]
    public async Task A_Request_For_Input_Fails_The_Call_Naming_What_Was_Asked()
    {
        var (client, _) = await ConnectAsync();
        await using var __ = client;

        var error = await Assert.ThrowsAsync<McpException>(() => client.CallToolAsync("input", Args("{}")));
        Assert.Contains("elicitation/create", error.Message, StringComparison.Ordinal);
        Assert.Null(error.Code);
    }

    [Fact]
    public async Task A_Line_That_Is_Not_Json_Closes_The_Connection()
    {
        var (client, _) = await ConnectAsync();
        await using var __ = client;

        await Assert.ThrowsAsync<IOException>(() => client.CallToolAsync("garbage", Args("{}")));
        // Streams can't be started again: every later call fails too.
        await Assert.ThrowsAsync<IOException>(() => client.CallToolAsync("echo", Args("""{"text":"x"}""")));
    }

    [Fact]
    public async Task A_Message_Bigger_Than_The_Limit_Fails_And_Closes_The_Connection()
    {
        var (client, _) = await ConnectAsync(options: new McpClientOptions { MaxMessageBytes = 8 * 1024 });
        await using var __ = client;

        Assert.Equal(new string('x', 100), Text(await client.CallToolAsync("big", Args("""{"bytes":100}"""))));
        var error = await Assert.ThrowsAsync<McpException>(() => client.CallToolAsync("big", Args("""{"bytes":20000}""")));
        Assert.Contains("8192", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_Server_That_Goes_Away_In_The_Middle_Of_A_Call_Fails_It()
    {
        var (client, _) = await ConnectAsync();
        await using var __ = client;
        await Assert.ThrowsAsync<IOException>(() => client.CallToolAsync("exit", Args("{}")));
    }

    [Fact]
    public async Task Calls_Run_At_Once_On_One_Connection()
    {
        var (client, _) = await ConnectAsync();
        await using var __ = client;

        var slow = client.CallToolAsync("slow", Args("""{"ms":500}"""));
        var quick = await client.CallToolAsync("echo", Args("""{"text":"first"}"""));
        Assert.False(slow.IsCompleted);
        Assert.Equal("first", Text(quick));
        Assert.Equal("done", Text(await slow));
    }

    [Fact]
    public async Task Cancelling_A_Call_Sends_Notifications_Cancelled_With_Its_Id()
    {
        var (client, fixture) = await ConnectAsync();
        await using var __ = client;

        using var cts = new CancellationTokenSource();
        var call = client.CallToolAsync("slow", Args("{}"), cts.Token);
        await fixture.SlowStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();

        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call);
        Assert.Equal(cts.Token, error.CancellationToken);
        var id = Requests(fixture).Single(m => m["params"]?["name"]?.GetValue<string>() == "slow")["id"]!.ToJsonString();
        await WaitUntil(() => fixture.Cancelled.Contains(id));
        var notice = fixture.Received.Single(m => m["method"]?.GetValue<string>() == "notifications/cancelled");
        Assert.Equal("cancelled by the caller", notice["params"]!["reason"]!.GetValue<string>());

        // The connection is still good.
        Assert.Equal("after", Text(await client.CallToolAsync("echo", Args("""{"text":"after"}"""))));
    }

    [Fact]
    public async Task A_Call_Timeout_Is_A_Failure_That_Also_Cancels_The_Request()
    {
        var (client, fixture) = await ConnectAsync(options: new McpClientOptions { CallTimeout = TimeSpan.FromMilliseconds(200) });
        await using var __ = client;

        var error = await Assert.ThrowsAsync<TimeoutException>(() => client.CallToolAsync("slow", Args("{}")));
        Assert.Contains("tools/call", error.Message, StringComparison.Ordinal);
        await WaitUntil(() => !fixture.Cancelled.IsEmpty);
    }

    [Fact]
    public async Task A_Late_Response_To_A_Cancelled_Request_Is_Ignored()
    {
        var (server, fromClient, toClient) = McpTestServers.Manual();
        var serving = Task.Run(async () =>
        {
            // discover, tools/list, then the call this test cancels, its cancellation, and one more call.
            var discover = JsonNode.Parse((await fromClient.ReadLineAsync())!)!;
            await toClient.WriteLineAsync($$$$"""{"jsonrpc":"2.0","id":{{{{discover["id"]}}}},"result":{"supportedVersions":["2026-07-28"],"capabilities":{}}}""");
            var list = JsonNode.Parse((await fromClient.ReadLineAsync())!)!;
            await toClient.WriteLineAsync($$$"""{"jsonrpc":"2.0","id":{{{list["id"]}}},"result":{"tools":[{"name":"t","inputSchema":{"type":"object"}}]}}""");
            var cancelled = JsonNode.Parse((await fromClient.ReadLineAsync())!)!;
            var notice = JsonNode.Parse((await fromClient.ReadLineAsync())!)!;
            Assert.Equal("notifications/cancelled", notice["method"]!.GetValue<string>());
            Assert.Equal(cancelled["id"]!.GetValue<long>(), notice["params"]!["requestId"]!.GetValue<long>());
            await toClient.WriteLineAsync($$$"""{"jsonrpc":"2.0","id":{{{cancelled["id"]}}},"result":{"content":[{"type":"text","text":"late"}]}}""");
            var next = JsonNode.Parse((await fromClient.ReadLineAsync())!)!;
            await toClient.WriteLineAsync($$$"""{"jsonrpc":"2.0","id":{{{next["id"]}}},"result":{"content":[{"type":"text","text":"on time"}]}}""");
        });

        await using var client = await McpClient.ConnectAsync(server);
        using var cts = new CancellationTokenSource();
        var call = client.CallToolAsync("t", Args("{}"), cts.Token);
        await Task.Delay(100);
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call);
        // Bounded: if the cancellation never reached the server, the scripted server stops answering.
        using var patience = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Assert.Equal("on time", Text(await client.CallToolAsync("t", Args("{}"), patience.Token)));
        await serving.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task After_Dispose_A_Call_Fails_With_ObjectDisposedException()
    {
        var (client, _) = await ConnectAsync();
        await client.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.CallToolAsync("echo", Args("{}")));
        await client.DisposeAsync();
    }

    [Fact]
    public async Task A_Call_In_Flight_When_The_Client_Is_Disposed_Fails_With_ObjectDisposedException()
    {
        var (client, fixture) = await ConnectAsync();
        var call = client.CallToolAsync("slow", Args("{}"));
        await fixture.SlowStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await client.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => call);
    }

    internal static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "timed out waiting");
            await Task.Delay(20);
        }
    }
}
