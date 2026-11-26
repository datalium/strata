// Samples for https://stratamesh.dev/guides/tools/mcp-servers/
//
// The samples connect to inventory-mcp, the small MCP server in examples/docs-snippets/inventory-mcp: as a stdio child
// process found on the PATH by its name, as a reader's own server would be, and behind Kestrel on loopback for HTTP.
// A sample that starts with using lines has a namespace block of its own: using directives can only open a namespace,
// so each block's are the ones its sample shows.

namespace Datalium.Strata.DocsSnippets.Guides.Tools.McpServers
{
    // docs:begin guides/tools/mcp-servers/connect-stdio
    using Datalium.Strata.Mcp;

    // docs:hide
    [Collection(McpConsole.Name)]
    public class ConnectOverStdioSample
    {
        public ConnectOverStdioSample() => InventoryMcpOnPath.Ensure();

        [Fact]
        public async Task ConnectOverStdio()
        {
            using var printed = new McpConsole();
            // docs:show
            await using var inventory = await McpClient.ConnectAsync(
                McpServer.Stdio("inventory-mcp", ["--warehouse", "east"]));

            Console.WriteLine($"{inventory.ServerInfo?.Name} speaks MCP {inventory.ProtocolVersion}");
            foreach (var tool in inventory.Tools)
            {
                Console.WriteLine($"{tool.Name}: {tool.Description}");
            }
            // docs:end guides/tools/mcp-servers/connect-stdio

            Assert.Equal(
                [
                    "inventory-mcp speaks MCP 2026-07-28",
                    "lookup_stock: Units in stock for a SKU.",
                    "list_warehouses: The warehouses this server knows.",
                    "reserve_stock: Reserves units of a SKU for an order.",
                ],
                printed.Lines);
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Tools.McpServers
{
    using Datalium.Strata.Mcp;

    [Collection(McpConsole.Name)]
    public class StdioOptionsSamples
    {
        public StdioOptionsSamples() => InventoryMcpOnPath.Ensure();

        [Fact]
        public async Task GiveTheServerItsEnvironment()
        {
            var apiKey = "a-key-from-your-secret-store";
            // docs:begin guides/tools/mcp-servers/environment
            var server = McpServer.Stdio("inventory-mcp", ["--warehouse", "west"]) with
            {
                Environment = new Dictionary<string, string?> { ["INVENTORY_API_KEY"] = apiKey },
            };
            // docs:end guides/tools/mcp-servers/environment

            var log = new List<string>();
            await using var inventory = await McpClient.ConnectAsync(server, new McpClientOptions { Log = entry => Add(log, entry.Message) });
            var result = await inventory.CallToolAsync("lookup_stock", System.Text.Json.JsonDocument.Parse("""{"sku":"AB-12"}""").RootElement);

            Assert.Equal("AB-12: 3 units in west", result.ToText());
            Assert.Equal("stderr: inventory-mcp: serving west, with an API key", await FirstAsync(log, "stderr: "));
        }

        [Fact]
        public async Task SetTheClientsOptions()
        {
            using var printed = new McpConsole();
            // docs:begin guides/tools/mcp-servers/options
            var options = new McpClientOptions
            {
                CallTimeout = TimeSpan.FromSeconds(30),       // tools/call has no time limit of its own
                Log = entry => Console.WriteLine($"[mcp] {entry.Level}: {entry.Message}"),
            };

            await using var inventory = await McpClient.ConnectAsync(
                McpServer.Stdio("inventory-mcp", ["--warehouse", "east"]), options);
            // docs:end guides/tools/mcp-servers/options

            // The server's stderr line arrives on its own reader, so wait for it.
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (!printed.Lines.Contains("[mcp] Debug: stderr: inventory-mcp: serving east, with no API key") && DateTime.UtcNow < deadline)
            {
                await Task.Delay(20);
            }

            Assert.Contains("[mcp] Debug: stderr: inventory-mcp: serving east, with no API key", printed.Lines);
        }

        private static void Add(List<string> log, string message)
        {
            lock (log)
            {
                log.Add(message);
            }
        }

        private static async Task<string?> FirstAsync(List<string> log, string prefix)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline)
            {
                lock (log)
                {
                    if (log.FirstOrDefault(m => m.StartsWith(prefix, StringComparison.Ordinal)) is { } found)
                    {
                        return found;
                    }
                }

                await Task.Delay(20);
            }

            return null;
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Tools.McpServers
{
    using Datalium.Strata.Mcp;

    [Collection(McpConsole.Name)]
    public class ConnectOverHttpSample
    {
        [Fact]
        public async Task ConnectOverHttp()
        {
            var secrets = new SecretStore(new Dictionary<string, string> { ["inventory-mcp-token"] = "token-1" });
            await using var http = await InventoryOverHttp.StartAsync(expectedToken: "token-1");
            var endpoint = http.Endpoint;
            // docs:begin guides/tools/mcp-servers/connect-http
            await using var inventory = await McpClient.ConnectAsync(
                McpServer.Http(endpoint) with
                {
                    // Called before each request, so the code that owns the token can refresh it.
                    AccessToken = cancellationToken => secrets.GetAsync("inventory-mcp-token", cancellationToken),
                });
            // docs:end guides/tools/mcp-servers/connect-http

            Assert.Equal("2026-07-28", inventory.ProtocolVersion);
            Assert.Equal(3, inventory.Tools.Count);
            Assert.NotEmpty(http.Tokens);
            Assert.All(http.Tokens, t => Assert.Equal("Bearer token-1", t));
        }

        [Fact]
        public async Task AWrongTokenFailsConnecting()
        {
            await using var http = await InventoryOverHttp.StartAsync(expectedToken: "token-1");

            var refused = await Assert.ThrowsAnyAsync<Exception>(() => McpClient.ConnectAsync(
                McpServer.Http(http.Endpoint) with { AccessToken = _ => ValueTask.FromResult<string?>("token-2") }));

            Assert.Contains("401", refused.ToString(), StringComparison.Ordinal);
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Tools.McpServers
{
    // docs:begin guides/tools/mcp-servers/register
    using Datalium.Strata.Mcp;
    using Datalium.Strata.Tools;

    // docs:hide
    using Datalium.Strata.DocsSnippets.Guides.Tools.ToolMiddleware;

    [Collection(McpConsole.Name)]
    public class RegisterSamples
    {
        public RegisterSamples() => InventoryMcpOnPath.Ensure();

        [Fact]
        public async Task RegisterCallAndClose()
        {
            await using var inventory = await McpClient.ConnectAsync(McpServer.Stdio("inventory-mcp", ["--warehouse", "east"]));
            var printed = new McpConsole();
            // docs:show
            var tools = new ToolRegistry()
                .Register(inventory.ToolProvider(new McpToolOptions
                {
                    Prefix = "inventory.",
                    Include = ["lookup_stock", "list_warehouses"],   // the server's names, before the prefix
                }))
                .Use(new AuditMiddleware());                         // runs for MCP tools as for any other

            foreach (var tool in tools.Tools)
            {
                Console.WriteLine(tool.Name);
            }
            // docs:end guides/tools/mcp-servers/register

            Assert.Equal(["inventory.lookup_stock", "inventory.list_warehouses"], printed.Lines);
            printed.Dispose();

            printed = new McpConsole();
            // docs:begin guides/tools/mcp-servers/call
            var support = ToolCallContext.None with { CallerId = "support-agent" };

            Console.WriteLine(await tools.DispatchToolAsync("inventory.lookup_stock", """{"sku":"AB-12"}""", support));
            Console.WriteLine(await tools.DispatchToolAsync("inventory.lookup_stock", """{"sku":"ZZ-99"}""", support));
            Console.WriteLine(await tools.DispatchToolAsync("inventory.list_warehouses", "{}", support));
            // docs:end guides/tools/mcp-servers/call

            Assert.Equal(
                [
                    "[audit] support-agent called inventory.lookup_stock: succeeded in N ms",
                    "AB-12: 7 units in east",
                    "[audit] support-agent called inventory.lookup_stock: succeeded in N ms",
                    """{"error":"no SKU 'ZZ-99' in east"}""",
                    "[audit] support-agent called inventory.list_warehouses: succeeded in N ms",
                    """{"warehouses":["east","west"]}""",
                ],
                printed.LinesWithoutDurations);
            printed.Dispose();

            printed = new McpConsole();
            // docs:begin guides/tools/mcp-servers/protocol-error
            try
            {
                await tools.DispatchToolAsync("inventory.lookup_stock", "{}", support);
            }
            catch (McpException failed)
            {
                Console.WriteLine($"failed: {failed.Message} (JSON-RPC code {failed.Code})");
            }
            // docs:end guides/tools/mcp-servers/protocol-error

            Assert.Equal(
                [
                    "[audit] support-agent called inventory.lookup_stock: failed (McpException) in N ms",
                    "failed: mcp error -32602: Invalid params: sku is required (JSON-RPC code -32602)",
                ],
                printed.LinesWithoutDurations);
            printed.Dispose();

            // docs:begin guides/tools/mcp-servers/close
            // At shutdown, when nothing calls the tools any more. await using does the same.
            await inventory.DisposeAsync();
            // docs:end guides/tools/mcp-servers/close

            await Assert.ThrowsAsync<ObjectDisposedException>(
                () => tools.DispatchToolAsync("inventory.lookup_stock", """{"sku":"AB-12"}""", support));
        }

        [Fact]
        public async Task AnIncludedNameTheServerDidNotListThrows()
        {
            await using var inventory = await McpClient.ConnectAsync(McpServer.Stdio("inventory-mcp"));

            Assert.Throws<ToolNotFoundException>(() => inventory.ToolProvider(new McpToolOptions { Include = ["restock"] }));
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Tools.McpServers
{
    // docs:begin guides/tools/mcp-servers/call-directly
    using System.Text.Json;
    using Datalium.Strata.Mcp;

    // docs:hide
    [Collection(McpConsole.Name)]
    public class CallDirectlySample
    {
        public CallDirectlySample() => InventoryMcpOnPath.Ensure();

        [Fact]
        public async Task CallATool()
        {
            await using var inventory = await McpClient.ConnectAsync(McpServer.Stdio("inventory-mcp"));
            using var printed = new McpConsole();
            // docs:show
            var result = await inventory.CallToolAsync("list_warehouses", JsonDocument.Parse("{}").RootElement);

            Console.WriteLine($"error: {result.IsError}, blocks: {result.Content.Count}");
            Console.WriteLine(result.StructuredContent);
            // docs:end guides/tools/mcp-servers/call-directly

            Assert.Equal(["error: False, blocks: 1", """{"warehouses":["east","west"]}"""], printed.Lines);
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Tools.McpServers
{
    using System.Collections.Concurrent;
    using System.Net;
    using System.Text.Json.Nodes;
    using System.Text.RegularExpressions;
    using Datalium.Strata.DocsSnippets.InventoryMcp;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Logging;

    /// <summary>The samples read what they print, so nothing else may write to the console meanwhile.</summary>
    [CollectionDefinition(McpConsole.Name, DisableParallelization = true)]
    public sealed class McpConsoleCollection
    {
    }

    /// <summary>Captures what is written to the console until disposed.</summary>
    public sealed class McpConsole : IDisposable
    {
        public const string Name = "guides/tools/mcp-servers: the console";

        private readonly TextWriter _console = Console.Out;
        private readonly StringWriter _printed = new();

        public McpConsole() => Console.SetOut(TextWriter.Synchronized(_printed));

        public string[] Lines => Split(_printed.ToString());

        /// <summary>What the page shows, with every duration as "N ms": they differ from run to run.</summary>
        public string[] LinesWithoutDurations => Split(Regex.Replace(_printed.ToString(), @"in \d+ ms", "in N ms"));

        public void Dispose() => Console.SetOut(_console);

        private static string[] Split(string text) =>
            text.ReplaceLineEndings("\n").TrimEnd().Split('\n', StringSplitOptions.RemoveEmptyEntries);
    }

    /// <summary>
    /// Puts inventory-mcp's build output on this process's PATH, so McpServer.Stdio("inventory-mcp") finds it by name,
    /// as it would a server you installed. The project reference builds it next to this one, in the same configuration.
    /// </summary>
    public static class InventoryMcpOnPath
    {
        private static readonly Lazy<string> Directory = new(() =>
        {
            var here = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
            var framework = here.Name;
            var configuration = here.Parent!.Name;
            var docsSnippets = here.Parent!.Parent!.Parent!.Parent!.FullName;
            var directory = Path.Combine(docsSnippets, "inventory-mcp", "bin", configuration, framework);
            var executable = OperatingSystem.IsWindows() ? "inventory-mcp.exe" : "inventory-mcp";
            Assert.True(File.Exists(Path.Combine(directory, executable)), $"inventory-mcp isn't built in {directory}");

            // On Linux, Process.Start looks for a bare command in this directory before the PATH. A copy of the apphost
            // here, without its dll, would be started instead and exit at once (the csproj's Private="false" says why).
            Assert.False(
                File.Exists(Path.Combine(AppContext.BaseDirectory, executable)),
                $"a copy of inventory-mcp is next to the tests in {AppContext.BaseDirectory}");
            Environment.SetEnvironmentVariable(
                "PATH", directory + Path.PathSeparator + Environment.GetEnvironmentVariable("PATH"));
            return directory;
        });

        public static void Ensure() => _ = Directory.Value;
    }

    /// <summary>Stands in for your secret store.</summary>
    public sealed class SecretStore(IReadOnlyDictionary<string, string> secrets)
    {
        public ValueTask<string?> GetAsync(string name, CancellationToken cancellationToken) =>
            ValueTask.FromResult(secrets.GetValueOrDefault(name));
    }

    /// <summary>The inventory server behind Kestrel on 127.0.0.1, answering 401 to a request without the token.</summary>
    public sealed class InventoryOverHttp : IAsyncDisposable
    {
        private readonly WebApplication _app;

        private InventoryOverHttp(WebApplication app) => _app = app;

        public Uri Endpoint { get; private set; } = null!;

        /// <summary>The Authorization header of each request.</summary>
        public ConcurrentQueue<string> Tokens { get; } = new();

        public static async Task<InventoryOverHttp> StartAsync(string expectedToken)
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseKestrel(k => k.Listen(IPAddress.Loopback, 0));
            var app = builder.Build();
            var http = new InventoryOverHttp(app);
            var server = new InventoryServer("east");
            app.MapPost("/mcp", async context =>
            {
                var authorization = context.Request.Headers.Authorization.ToString();
                http.Tokens.Enqueue(authorization);
                if (authorization != $"Bearer {expectedToken}")
                {
                    context.Response.StatusCode = 401;
                    return;
                }

                var message = (await JsonNode.ParseAsync(context.Request.Body))!.AsObject();
                if (server.Answer(message) is not { } answer)
                {
                    context.Response.StatusCode = 202;
                    return;
                }

                context.Response.ContentType = "application/json";
                await context.Response.WriteAsync(answer.ToJsonString());
            });
            await app.StartAsync();
            http.Endpoint = new Uri(app.Urls.First().TrimEnd('/') + "/mcp");
            return http;
        }

        public async ValueTask DisposeAsync()
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
    }
}
