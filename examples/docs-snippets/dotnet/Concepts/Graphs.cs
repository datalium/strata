// Samples for https://stratamesh.dev/concepts/graphs/

using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

namespace Datalium.Strata.DocsSnippets.Concepts.Graphs;

public class GraphsSamples
{
    [Theory]
    [InlineData(12, "bulk order, ships via freight")]
    [InlineData(3, "standard shipping")]
    public async Task Branching(int quantity, string expectedShipping)
    {
        // docs:begin concepts/graphs/branching
        var graph = new StateGraph<OrderState>()
            .AddNode("CalculateSubtotal", state => state with { Subtotal = state.Quantity * state.UnitPrice })
            // docs:hide ... ApplyDiscount, TagBulkShipping, TagStandardShipping and FormatReceipt
            .AddNode("ApplyDiscount", state =>
            {
                var subtotal = state.Subtotal!.Value;
                var discount = subtotal >= 100.0 ? subtotal * 0.10 : 0.0;
                return state with { Discount = discount, Total = subtotal - discount };
            })
            .AddNode("TagBulkShipping", state => state with { ShippingNote = "bulk order, ships via freight" })
            .AddNode("TagStandardShipping", state => state with { ShippingNote = "standard shipping" })
            .AddNode("FormatReceipt", state => state with { Receipt = $"{state.Quantity}x {state.Item}: {state.ShippingNote}" })
            // docs:show
            .SetEntryPoint("CalculateSubtotal")
            .AddEdge("CalculateSubtotal", "ApplyDiscount")
            .AddConditionalEdge("ApplyDiscount", "TagBulkShipping", state => state.Quantity >= 10)
            .AddEdge("ApplyDiscount", "TagStandardShipping")
            .AddEdge("TagBulkShipping", "FormatReceipt")
            .AddEdge("TagStandardShipping", "FormatReceipt")
            .Compile();
        // docs:end concepts/graphs/branching

        var initial = new OrderState { Item = "widget", Quantity = quantity, UnitPrice = 9.50 };
        var result = await graph.RunAsync($"order-{quantity}", initial, new InMemoryCheckpointStore());

        var completed = Assert.IsType<GraphExecutionResult<OrderState>.Completed>(result);
        Assert.Equal(expectedShipping, completed.State.ShippingNote);
        Assert.NotNull(completed.State.Receipt);
    }
}

internal sealed record OrderState
{
    public required string Item { get; init; }
    public required int Quantity { get; init; }
    public required double UnitPrice { get; init; }
    public double? Subtotal { get; init; }
    public double? Discount { get; init; }
    public double? Total { get; init; }
    public string? ShippingNote { get; init; }
    public string? Receipt { get; init; }
}
