using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Datalium.Strata.Mcp;

namespace Datalium.Strata.Tests.Mcp;

/// <summary>
/// A stdio server as a child process (<c>design/50-MCP-Client.md</c>, "stdio"): starting it, its stderr, its environment,
/// the shutdown that stops its whole process tree, restarting it after it exits, and on Windows a <c>.cmd</c> found
/// through <c>PATHEXT</c>.
/// </summary>
public sealed class McpStdioTests
{
    private static readonly JsonElement NoArguments = JsonDocument.Parse("{}").RootElement.Clone();

    private static JsonElement Args(string json) => JsonDocument.Parse(json).RootElement.Clone();

    [Fact]
    public async Task A_Stdio_Server_Is_Started_Probed_Called_And_Stopped()
    {
        var log = new ConcurrentQueue<McpLogEntry>();
        var client = await McpClient.ConnectAsync(McpTestServers.Stdio("--stderr", "hello from stderr"), McpTestServers.Logging(log));
        var pid = client.ProcessId!.Value;

        Assert.Equal("2026-07-28", client.ProtocolVersion);
        Assert.Equal("hi", (await client.CallToolAsync("echo", Args("""{"text":"hi"}"""))).ToText());
        await client.DisposeAsync();

        Assert.True(Exited(pid));
        Assert.Contains(log, e => e.Level == McpLogLevel.Debug && e.Message == "stderr: hello from stderr");
    }

    [Fact]
    public async Task A_Legacy_Stdio_Server_Gets_The_Handshake()
    {
        await using var client = await McpClient.ConnectAsync(McpTestServers.Stdio("--discover", "error:-32601", "--initialize", "2024-11-05"));
        Assert.Equal("2024-11-05", client.ProtocolVersion);
        Assert.Equal("x", (await client.CallToolAsync("echo", Args("""{"text":"x"}"""))).ToText());
    }

    [Fact]
    public async Task Shutdown_Stops_The_Server_And_The_Processes_It_Started_When_It_Will_Not_Exit()
    {
        var server = McpTestServers.Stdio("--spawn-child", "--ignore-stdin-close") with { ShutdownTimeout = TimeSpan.FromMilliseconds(500) };
        var client = await McpClient.ConnectAsync(server);
        var pid = client.ProcessId!.Value;
        var child = int.Parse((await client.CallToolAsync("child", NoArguments)).ToText(), System.Globalization.CultureInfo.InvariantCulture);
        Assert.False(Exited(child));

        var stopping = Stopwatch.StartNew();
        await client.DisposeAsync();

        Assert.True(stopping.Elapsed >= TimeSpan.FromMilliseconds(400), "it waited for the server first");
        Assert.True(Exited(pid));
        await McpClientTests.WaitUntil(() => Exited(child));
    }

    [Fact]
    public async Task A_Server_That_Exits_Is_Started_Again_On_The_Next_Call()
    {
        var log = new ConcurrentQueue<McpLogEntry>();
        await using var client = await McpClient.ConnectAsync(McpTestServers.Stdio(), McpTestServers.Logging(log));
        var first = client.ProcessId!.Value;

        var error = await Assert.ThrowsAsync<IOException>(() => client.CallToolAsync("exit", NoArguments));
        Assert.Contains("exited", error.Message, StringComparison.Ordinal);
        Assert.Equal("again", (await client.CallToolAsync("echo", Args("""{"text":"again"}"""))).ToText());

        Assert.NotEqual(first, client.ProcessId);
        Assert.Contains(log, e => e.Message == "starting the MCP server again");
    }

    [Fact]
    public async Task Without_Restart_A_Server_That_Exited_Fails_Every_Later_Call()
    {
        await using var client = await McpClient.ConnectAsync(McpTestServers.Stdio(), new McpClientOptions { RestartOnExit = false });
        await Assert.ThrowsAsync<IOException>(() => client.CallToolAsync("exit", NoArguments));
        await Assert.ThrowsAsync<IOException>(() => client.CallToolAsync("echo", Args("""{"text":"x"}""")));
    }

    [Fact]
    public async Task The_Environment_Is_Inherited_With_What_Is_Given_Added_Or_Removed()
    {
        Environment.SetEnvironmentVariable("STRATA_MCP_PARENT", "inherited");
        Environment.SetEnvironmentVariable("STRATA_MCP_REMOVED", "present");
        var server = McpTestServers.Stdio() with
        {
            Environment = new Dictionary<string, string?> { ["STRATA_MCP_GIVEN"] = "given", ["STRATA_MCP_REMOVED"] = null },
        };
        await using (var client = await McpClient.ConnectAsync(server))
        {
            Assert.Equal("inherited", await Env(client, "STRATA_MCP_PARENT"));
            Assert.Equal("given", await Env(client, "STRATA_MCP_GIVEN"));
            Assert.Equal("(unset)", await Env(client, "STRATA_MCP_REMOVED"));
        }

        await using (var cautious = await McpClient.ConnectAsync(server with { InheritEnvironment = false }))
        {
            Assert.Equal("(unset)", await Env(cautious, "STRATA_MCP_PARENT"));
            Assert.Equal("given", await Env(cautious, "STRATA_MCP_GIVEN"));
        }
    }

    [Fact]
    public async Task A_Command_That_Does_Not_Start_Fails_Connecting()
    {
        await Assert.ThrowsAsync<IOException>(() => McpClient.ConnectAsync(McpServer.Stdio("strata-no-such-mcp-server")));
    }

    [Fact]
    public async Task On_Windows_A_Cmd_Is_Found_Through_Pathext_And_Its_Arguments_Arrive_As_Given()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        var directory = Directory.CreateTempSubdirectory("strata-mcp-cmd-");
        try
        {
            await File.WriteAllTextAsync(
                Path.Combine(directory.FullName, "strata-mcp-fixture.cmd"),
                $"@dotnet \"{McpTestServers.FixtureDll()}\" %*\r\n");
            string[] arguments = ["plain", "two words", "q\"uote", "100%", "%PATH%", "amp&er", "caret^", "pipe|", "<angle>", "(paren)", "!bang!", "", @"trail\", @"C:\some dir\"];
            var server = McpServer.Stdio("strata-mcp-fixture", arguments) with
            {
                Environment = new Dictionary<string, string?> { ["PATH"] = directory.FullName + ";" + Environment.GetEnvironmentVariable("PATH") },
            };

            await using var client = await McpClient.ConnectAsync(server);
            var received = JsonSerializer.Deserialize<string[]>((await client.CallToolAsync("args", NoArguments)).ToText());
            Assert.Equal(arguments, received);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    [Theory]
    [InlineData("plain", "plain")]
    [InlineData("two words", "\"two words\"")]
    [InlineData("q\"uote", "\"q\"\"uote\"")]
    [InlineData("100%", "\"100%%cd:~,%\"")]
    [InlineData("", "\"\"")]
    [InlineData(@"trail\", @"""trail\\""")]
    [InlineData(@"C:\a\b.txt", @"C:\a\b.txt")]
    public void A_Batch_Files_Arguments_Are_Quoted_As_Rust_Quotes_Them(string argument, string quoted)
    {
        Assert.Equal($"/e:ON /v:OFF /d /c \"\"C:\\x.cmd\" {quoted}\"", StdioCommand.BatchArguments(@"C:\x.cmd", [argument]));
    }

    [Fact]
    public void A_Batch_File_Argument_With_A_Line_Break_Is_Refused()
    {
        Assert.Throws<ArgumentException>(() => StdioCommand.BatchArguments(@"C:\x.cmd", ["a\nb"]));
    }

    [Fact]
    public void A_Name_Is_Found_Through_Path_And_Pathext()
    {
        var directory = Directory.CreateTempSubdirectory("strata-mcp-path-");
        try
        {
            var cmd = Path.Combine(directory.FullName, "tool.cmd");
            File.WriteAllText(cmd, string.Empty);
            Assert.Equal(cmd, StdioCommand.Resolve("tool", directory.FullName, ".EXE;.CMD"), ignoreCase: true);
            Assert.Equal(cmd, StdioCommand.Resolve("tool.cmd", directory.FullName, ".EXE"), ignoreCase: true);
            Assert.Equal(cmd, StdioCommand.Resolve(Path.Combine(directory.FullName, "tool"), null, ".CMD"), ignoreCase: true);
            Assert.Equal("other", StdioCommand.Resolve("other", directory.FullName, ".EXE;.CMD"));
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    private static async Task<string> Env(McpClient client, string name) =>
        (await client.CallToolAsync("env", JsonDocument.Parse(JsonSerializer.Serialize(new { name })).RootElement)).ToText();

    private static bool Exited(int pid)
    {
        try
        {
            using var process = Process.GetProcessById(pid);
            return process.HasExited;
        }
        catch (ArgumentException)
        {
            return true;
        }
    }
}
