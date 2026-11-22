// Samples for https://stratamesh.dev/concepts/state/

// docs:begin concepts/state/cross-language
using System.Text.Json.Serialization;

// docs:hide
using System.Text.Json;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

namespace Datalium.Strata.DocsSnippets.Concepts.State;

// docs:show
sealed record WireTransferState(
    [property: JsonPropertyName("invoice_generated")] bool InvoiceGenerated,
    [property: JsonPropertyName("amount")] double Amount,
    [property: JsonPropertyName("currency")] string Currency,
    [property: JsonPropertyName("approved_by")] string? ApprovedBy);

// docs:hide
public class StateSamples
{
    [Fact]
    public async Task CrossLanguage()
    {
        var graph = new StateGraph<WireTransferState>()
            // docs:show
            // A node returns a copy with the fields it changes.
            .AddNode("PrepareWireTransfer", state => state with { InvoiceGenerated = true })
            // docs:hide
            .Compile();

        var result = await graph.RunAsync(
            "state-wire-1",
            new WireTransferState(InvoiceGenerated: false, Amount: 4200.50, Currency: "USD", ApprovedBy: null),
            new InMemoryCheckpointStore());
        var state = Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(result).State;
        Assert.True(state.InvoiceGenerated);

        // The JSON field names are the snake_case ones the Rust struct has.
        Assert.Equal(
            """{"invoice_generated":true,"amount":4200.5,"currency":"USD","approved_by":null}""",
            JsonSerializer.Serialize(state));
    }
}
// docs:show
// docs:end concepts/state/cross-language
