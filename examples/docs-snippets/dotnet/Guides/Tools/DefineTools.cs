// Samples for https://stratamesh.dev/guides/tools/define-tools/
//
// A sample that starts with using lines has a namespace block of its own: using directives can only open a
// namespace, so each block's are the ones its sample shows.

namespace Datalium.Strata.DocsSnippets.Guides.Tools.DefineTools
{
    // docs:begin guides/tools/define-tools/write-tool
    using Datalium.Strata.Tools;

    public partial class InventoryTools
    {
        private static readonly Dictionary<string, int> Stock = new() { ["AB-12"] = 7, ["CD-34"] = 0 };

        [StrataTool(Name = "inventory.lookup", Description = "Units in stock for a SKU.")]
        public static int Lookup(string sku, string? warehouse = null) =>
            Stock.GetValueOrDefault(sku);

        [StrataTool(Name = "inventory.reserve", Description = "Reserves units of a SKU for the calling tenant.")]
        public static async Task<string> ReserveAsync(
            string sku, int count, ToolCallContext context, CancellationToken cancellationToken)
        {
            // Stands in for a call to the warehouse system.
            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
            if (count > Stock.GetValueOrDefault(sku))
            {
                throw new InvalidOperationException($"only {Stock.GetValueOrDefault(sku)} x {sku} in stock");
            }

            var tenant = context.Metadata.GetValueOrDefault("tenant", "(no tenant)");
            return $"reserved {count} x {sku} for {tenant}";
        }
    }
    // docs:end guides/tools/define-tools/write-tool
}

namespace Datalium.Strata.DocsSnippets.Guides.Tools.DefineTools
{
    using System.Globalization;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using Datalium.Strata.Graph;
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Storage;
    using Datalium.Strata.Tools;

    /// <summary>The page names billing.refund, "like InventoryTools above", without showing it.</summary>
    public partial class BillingTools
    {
        [StrataTool(Name = "billing.refund", Description = "Refunds part or all of an order.")]
        public static string Refund(string orderId, decimal amount) =>
            string.Create(CultureInfo.InvariantCulture, $"refunded {amount:0.00} on {orderId}");
    }

    public class DefineToolsSamples
    {
        /// <summary>The manifest entry the page shows for inventory.lookup, the same in both engines.</summary>
        private const string LookupEntry = """
            {
              "name": "inventory.lookup",
              "description": "Units in stock for a SKU.",
              "parameters": {
                "type": "object",
                "properties": {
                  "sku": { "type": "string" },
                  "warehouse": { "type": "string" }
                },
                "required": ["sku"]
              }
            }
            """;

        [Fact]
        public void ReadTheManifest()
        {
            // docs:begin guides/tools/define-tools/manifest
            string manifest = new InventoryTools().GetToolsManifest();
            // docs:end guides/tools/define-tools/manifest

            var entries = JsonNode.Parse(manifest)!.AsArray();
            var lookup = entries.Single(entry => entry!["name"]!.GetValue<string>() == "inventory.lookup");
            Assert.True(JsonNode.DeepEquals(JsonNode.Parse(LookupEntry), lookup), lookup!.ToJsonString());

            // The context and the token aren't arguments.
            var reserve = entries.Single(entry => entry!["name"]!.GetValue<string>() == "inventory.reserve");
            Assert.Equal(["sku", "count"], reserve!["parameters"]!["properties"]!.AsObject().Select(p => p.Key));
        }

        [Fact]
        public async Task RegisterAndCallFromANode()
        {
            // docs:begin guides/tools/define-tools/register
            // BillingTools declares billing.refund, like InventoryTools above.
            var tools = new ToolRegistry()
                .Register(new InventoryTools())
                .Register(new BillingTools());

            // One agent's view: only these two tools.
            var support = tools.Subset("inventory.lookup", "inventory.reserve");
            // docs:end guides/tools/define-tools/register

            Assert.Equal(["inventory.lookup", "inventory.reserve", "billing.refund"], tools.Tools.Select(t => t.Name));
            Assert.Equal(["inventory.lookup", "inventory.reserve"], support.Tools.Select(t => t.Name));
            Assert.Throws<DuplicateToolException>(() => tools.Register(new InventoryTools()));

            // docs:begin guides/tools/define-tools/call-from-node
            var graph = new StateGraph<Order>()
                .AddNode("Reserve", async (state, ctx, ct) =>
                {
                    var arguments = JsonSerializer.Serialize(new { sku = state.Sku, count = state.Count });
                    var json = await support.DispatchToolAsync(
                        "inventory.reserve", arguments, ToolCallContext.From(ctx), ct);
                    var result = JsonDocument.Parse(json).RootElement.GetProperty("result").GetString();
                    return state with { Confirmation = result };
                })
                .Compile();

            var run = await graph.RunAsync(
                "order-1001",
                new Order("AB-12", 2),
                new InMemoryCheckpointStore(),
                new RunOptions { Metadata = { ["tenant"] = "acme" } });
            // docs:hide

            var completed = Assert.IsType<GraphExecutionResult<Order>.Completed>(run);
            Assert.Equal("reserved 2 x AB-12 for acme", completed.State.Confirmation);
        }

        // docs:show

        public sealed record Order(string Sku, int Count, string? Confirmation = null);
        // docs:end guides/tools/define-tools/call-from-node

        // docs:begin guides/tools/define-tools/errors
        static async Task<string> AskAsync(
            ToolRegistry tools, string name, string arguments, ToolCallContext context, CancellationToken ct)
        {
            try
            {
                var json = await tools.DispatchToolAsync(name, arguments, context, ct);
                return JsonDocument.Parse(json).RootElement.GetProperty("result").ToString();
            }
            catch (ToolArgumentsException bad)
            {
                return $"invalid arguments: {bad.Problems}";
            }
            catch (ToolNotPermittedException)
            {
                return "not permitted for this agent";
            }
            catch (ToolNotFoundException)
            {
                return "no such tool";
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return $"the tool failed: {ex.Message}";
            }
        }
        // docs:end guides/tools/define-tools/errors

        [Theory]
        [InlineData("inventory.reserve", """{"sku":"AB-12"}""", "invalid arguments: argument 'count' is missing")]
        [InlineData("inventory.reserve", """{"sku":"AB-12","count":9}""", "the tool failed: only 7 x AB-12 in stock")]
        [InlineData("billing.refund", """{"orderId":"A-1","amount":5}""", "not permitted for this agent")]
        [InlineData("inventory.restock", "{}", "no such tool")]
        public async Task HandleToolErrors(string name, string arguments, string expected)
        {
            var support = new ToolRegistry()
                .Register(new InventoryTools())
                .Register(new BillingTools())
                .Subset("inventory.lookup", "inventory.reserve");

            Assert.Equal(expected, await AskAsync(support, name, arguments, ToolCallContext.None, CancellationToken.None));
        }

        [Fact]
        public async Task ACancelledCallNeverRunsTheTool()
        {
            var tools = new ToolRegistry().Register(new InventoryTools());
            using var cancelled = new CancellationTokenSource();
            await cancelled.CancelAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => tools.DispatchToolAsync(
                "inventory.reserve", """{"sku":"AB-12","count":1}""", ToolCallContext.None, cancelled.Token));
        }
    }
}
