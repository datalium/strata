using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Mcp;
using Datalium.Strata.Tools;

namespace Datalium.Strata.Tests.Mcp;

/// <summary>
/// Streamable HTTP against the fixture behind Kestrel (<c>design/50-MCP-Client.md</c>, "Streamable HTTP",
/// "Authorization"): both eras, JSON and SSE answers, the modern headers and <c>x-mcp-header</c>, the legacy session,
/// the token hook, cancellation, and the era probe's HTTP rows.
/// </summary>
public sealed class McpHttpTests
{
    private static JsonElement Args(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static readonly JsonElement NoArguments = Args("{}");

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_Modern_Server_Gets_Every_Header_And_Answers_In_Json_Or_Sse(bool sse)
    {
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { Sse = sse });
        await using var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint));

        Assert.Equal("2026-07-28", client.ProtocolVersion);
        Assert.Equal("fixture", client.ServerInfo!.Name);
        Assert.Equal("hello", (await client.CallToolAsync("echo", Args("""{"text":"hello"}"""))).ToText());
        Assert.Equal("progressed", (await client.CallToolAsync("progress", NoArguments)).ToText());

        Assert.All(http.Requests, r =>
        {
            Assert.Equal("POST", r.Method);
            Assert.Equal("2026-07-28", r.Headers["MCP-Protocol-Version"]);
            Assert.Equal(r.Body!["method"]!.GetValue<string>(), r.Headers["Mcp-Method"]);
            Assert.Contains("application/json", r.Headers["Accept"], StringComparison.Ordinal);
            Assert.Contains("text/event-stream", r.Headers["Accept"], StringComparison.Ordinal);
            Assert.StartsWith("application/json", r.Headers["Content-Type"], StringComparison.Ordinal);
            Assert.False(r.Headers.ContainsKey("Mcp-Session-Id"));
        });
        var call = http.Requests.First(r => r.Body!["method"]!.GetValue<string>() == "tools/call");
        Assert.Equal("echo", call.Headers["Mcp-Name"]);
    }

    [Fact]
    public async Task Marked_Parameters_Are_Mirrored_Into_Headers_Encoded_As_The_Spec_Says()
    {
        await using var http = await HttpFixture.StartAsync();
        await using var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint));

        var seen = JsonNode.Parse((await client.CallToolAsync("region", Args("""{"region":"東京","limit":5}"""))).ToText())!;
        Assert.Equal("=?base64?" + Convert.ToBase64String(Encoding.UTF8.GetBytes("東京")) + "?=", seen["Mcp-Param-Region"]!.GetValue<string>());
        Assert.Equal("5", seen["Mcp-Param-Limit"]!.GetValue<string>());

        var plain = JsonNode.Parse((await client.CallToolAsync("region", Args("""{"region":"us-west1"}"""))).ToText())!;
        Assert.Equal("us-west1", plain["Mcp-Param-Region"]!.GetValue<string>());
        Assert.Null(plain["Mcp-Param-Limit"]);
    }

    [Fact]
    public async Task A_Tool_Whose_Header_Annotation_Breaks_The_Rules_Is_Left_Out_With_A_Warning()
    {
        var log = new ConcurrentQueue<McpLogEntry>();
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { BadHeaderTool = true });
        await using var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint), McpTestServers.Logging(log));

        Assert.DoesNotContain(client.Tools, t => t.Name == "bad_header");
        Assert.Contains(client.Tools, t => t.Name == "region");
        Assert.Contains(log, e => e.Level == McpLogLevel.Warning && e.Message.Contains("'bad_header'", StringComparison.Ordinal)
            && e.Message.Contains("only string, integer and boolean", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_Header_Mismatch_Lists_The_Tools_Again_And_Retries_Once()
    {
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { MismatchOnce = true });
        await using var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint));

        await client.CallToolAsync("region", Args("""{"region":"eu"}"""));

        var methods = http.Requests.Select(r => r.Body!["method"]!.GetValue<string>()).ToList();
        Assert.Equal(["server/discover", "tools/list", "tools/call", "tools/list", "tools/call"], methods);
    }

    [Fact]
    public async Task A_Legacy_Server_Gets_A_Session_That_Is_Sent_On_Every_Request_And_Deleted_At_The_End()
    {
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { Era = "legacy", LegacyVersion = "2025-06-18" });
        var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint));
        Assert.Equal("2025-06-18", client.ProtocolVersion);
        Assert.Equal("x", (await client.CallToolAsync("echo", Args("""{"text":"x"}"""))).ToText());
        await client.DisposeAsync();

        var requests = http.Requests.ToList();
        Assert.Equal("server/discover", requests[0].Body!["method"]!.GetValue<string>());
        var initialize = requests[1];
        Assert.Equal("initialize", initialize.Body!["method"]!.GetValue<string>());
        Assert.False(initialize.Headers.ContainsKey("MCP-Protocol-Version"));
        Assert.False(initialize.Headers.ContainsKey("Mcp-Method"));
        var afterwards = requests.Skip(2).ToList();
        var session = afterwards[0].Headers["Mcp-Session-Id"];
        Assert.All(afterwards, r =>
        {
            Assert.Equal(session, r.Headers["Mcp-Session-Id"]);
            Assert.Equal("2025-06-18", r.Headers["MCP-Protocol-Version"]);
            Assert.False(r.Headers.ContainsKey("Mcp-Method"));
        });
        Assert.Equal("notifications/initialized", afterwards[0].Body!["method"]!.GetValue<string>());
        Assert.Equal("DELETE", afterwards[^1].Method);
    }

    [Fact]
    public async Task A_Delete_The_Server_Does_Not_Allow_Is_Fine()
    {
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { Era = "legacy", DeleteStatus = 405 });
        var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint));
        await client.DisposeAsync();
        Assert.Equal("DELETE", http.Requests.Last().Method);
    }

    [Fact]
    public async Task A_Session_That_Ended_Is_Started_Again_Once_And_The_Request_Retried()
    {
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { Era = "legacy", ExpireSessionAfter = 4 });
        await using var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint));

        Assert.Equal("one", (await client.CallToolAsync("echo", Args("""{"text":"one"}"""))).ToText());
        Assert.Equal("two", (await client.CallToolAsync("echo", Args("""{"text":"two"}"""))).ToText());
        Assert.Equal(2, http.Requests.Count(r => r.Body?["method"]?.GetValue<string>() == "initialize"));
    }

    [Fact]
    public async Task A_Legacy_Servers_Requests_On_Its_Stream_Are_Answered_With_A_Post()
    {
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { Era = "legacy", Sse = true });
        await using var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint));

        var answers = JsonNode.Parse((await client.CallToolAsync("ask", NoArguments)).ToText())!.AsArray();
        Assert.Equal("{}", answers[0]!["result"]!.ToJsonString());
        Assert.Equal(-32601, answers[1]!["error"]!["code"]!.GetValue<int>());
    }

    [Fact]
    public async Task A_Dual_Era_Server_Stays_Modern_Over_Http()
    {
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { Era = "dual" });
        await using var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint));
        Assert.Equal("2026-07-28", client.ProtocolVersion);
        Assert.DoesNotContain(http.Requests, r => r.Body?["method"]?.GetValue<string>() == "initialize");
    }

    [Fact]
    public async Task The_Token_Hook_Runs_Before_Every_Request_And_A_Token_Can_Change()
    {
        var issued = 0;
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { Authorized = h => h?.StartsWith("Bearer secret-", StringComparison.Ordinal) == true });
        var server = McpServer.Http(http.Endpoint) with
        {
            AccessToken = _ => ValueTask.FromResult<string?>($"secret-{Interlocked.Increment(ref issued)}"),
            Headers = new Dictionary<string, string> { ["X-Tenant"] = "acme" },
        };
        await using var client = await McpClient.ConnectAsync(server);
        await client.CallToolAsync("echo", Args("""{"text":"x"}"""));

        Assert.Equal(["Bearer secret-1", "Bearer secret-2", "Bearer secret-3"], http.Requests.Select(r => r.Headers["Authorization"]));
        Assert.All(http.Requests, r => Assert.Equal("acme", r.Headers["X-Tenant"]));
    }

    [Fact]
    public async Task A_Refused_Token_Fails_With_The_Status_And_Never_Shows_The_Token()
    {
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { Authorized = _ => false });
        var server = McpServer.Http(http.Endpoint) with { AccessToken = _ => ValueTask.FromResult<string?>("very-secret-token") };

        var error = await Assert.ThrowsAsync<HttpRequestException>(() => McpClient.ConnectAsync(server));
        Assert.Equal(HttpStatusCode.Unauthorized, error.StatusCode);
        Assert.Equal("unauthorized (401)", error.Message);
        Assert.DoesNotContain("very-secret-token", error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_Token_Hook_That_Throws_Fails_The_Request()
    {
        await using var http = await HttpFixture.StartAsync();
        var calls = 0;
        var server = McpServer.Http(http.Endpoint) with
        {
            AccessToken = _ => Interlocked.Increment(ref calls) > 2 ? throw new InvalidOperationException("vault is sealed") : ValueTask.FromResult<string?>("t"),
        };
        await using var client = await McpClient.ConnectAsync(server);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => client.CallToolAsync("echo", Args("""{"text":"x"}""")));
        Assert.Equal("vault is sealed", error.Message);
        Assert.Equal(2, http.Requests.Count);
    }

    [Fact]
    public async Task Cancelling_A_Modern_Call_Aborts_The_Request_And_Posts_Nothing()
    {
        await using var http = await HttpFixture.StartAsync();
        await using var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint));
        using var cts = new CancellationTokenSource();

        var call = client.CallToolAsync("slow", NoArguments, cts.Token);
        await http.Fixture.SlowStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call);
        await McpClientTests.WaitUntil(() => !http.Aborted.IsEmpty);
        Assert.DoesNotContain(http.Requests, r => r.Body?["method"]?.GetValue<string>() == "notifications/cancelled");
    }

    [Fact]
    public async Task Cancelling_A_Legacy_Call_Also_Posts_Notifications_Cancelled()
    {
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { Era = "legacy" });
        await using var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint));
        using var cts = new CancellationTokenSource();

        var call = client.CallToolAsync("slow", NoArguments, cts.Token);
        await http.Fixture.SlowStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call);
        await McpClientTests.WaitUntil(() => !http.Fixture.Cancelled.IsEmpty);
        var notice = http.Requests.Single(r => r.Body?["method"]?.GetValue<string>() == "notifications/cancelled");
        Assert.NotNull(notice.Headers["Mcp-Session-Id"]);
    }

    [Fact]
    public async Task A_Server_Error_Status_Fails_Connecting_With_The_Status()
    {
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { Discover = (503, "") });
        var error = await Assert.ThrowsAsync<HttpRequestException>(() => McpClient.ConnectAsync(McpServer.Http(http.Endpoint)));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, error.StatusCode);
    }

    [Fact]
    public async Task A_Modern_Error_In_A_400_Is_Not_Taken_For_Legacy()
    {
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions
        {
            Era = "dual",
            Discover = (400, """{"jsonrpc":"2.0","id":1,"error":{"code":-32021,"message":"Missing required client capability"}}"""),
        });
        var error = await Assert.ThrowsAsync<McpException>(() => McpClient.ConnectAsync(McpServer.Http(http.Endpoint)));
        Assert.Equal(-32021, error.Code);
        Assert.DoesNotContain(http.Requests, r => r.Body?["method"]?.GetValue<string>() == "initialize");
    }

    [Fact]
    public async Task A_Connection_Refused_Fails_Connecting()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => McpClient.ConnectAsync(McpServer.Http(new Uri("http://127.0.0.1:1/mcp"))));
    }

    [Fact]
    public async Task A_Json_Rpc_Error_In_A_400_Keeps_Its_Code()
    {
        await using var http = await HttpFixture.StartAsync();
        await using var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint));
        var error = await Assert.ThrowsAsync<McpException>(() => client.CallToolAsync("nope", NoArguments));
        Assert.Equal(-32602, error.Code);
    }

    [Fact]
    public async Task Http_Tools_Work_Through_A_Registry()
    {
        await using var http = await HttpFixture.StartAsync(new HttpFixtureOptions { Sse = true });
        await using var client = await McpClient.ConnectAsync(McpServer.Http(http.Endpoint));
        var tools = new ToolRegistry().Register(client.ToolProvider(new McpToolOptions { Prefix = "remote." }));
        Assert.Equal("""{"error":"it broke"}""", await tools.DispatchToolAsync("remote.fail", "{}"));
    }
}
