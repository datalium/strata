// Samples for https://stratamesh.dev/guides/graphs/map-fan-out/

using System.Globalization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

namespace Datalium.Strata.DocsSnippets.Guides.Graphs.MapFanOut;

public class MapFanOutSamples
{
    [Fact]
    public async Task FanOut()
    {
        var invoice = Invoice.Of(
            "INV-2207",
            new("L01", "PAP-A4", 40, 4.20m),
            new("L02", "PAP-A3", 10, 8.10m),
            new("L03", "INK-K", 4, 62.00m),
            new("L04", "INK-C", 2, 71.00m),
            new("L05", "PEN-BLU", 200, 0.90m),
            new("L06", "PEN-RED", 50, 0.90m),
            new("L07", "PAD-A5", 60, 2.40m),
            new("L08", "FLD-GRN", 100, 0.35m),
            new("L09", "INK-M", 2, 79.50m),
            new("L10", "SVC-INST", 1, 45.00m),
            new("L11", "PAP-A4", 20, 4.20m),
            new("L12", "SVC-DLV", 1, 12.00m));

        var result = await InvoiceGraph(new PriceList()).Compile()
            .RunAsync("map-fan-out-1", invoice, new InMemoryCheckpointStore());

        var audited = Assert.IsType<GraphExecutionResult<Invoice>.Completed>(result).State;
        Assert.Equal(invoice.Lines.Select(l => l.Id), audited.Audits.Select(a => a.Line.Id));
        Assert.Equal(52, audited.Audits.Single(a => a.Line.Id == "L09").Score);
        Assert.Equal("hold: L09 billed 79.50, list 71.00 (+12%)", audited.Decision);
    }

    [Fact]
    public async Task Options()
    {
        // docs:begin guides/graphs/map-fan-out/options
        var options = new MapOptions<LineAudit>
        {
            MaxConcurrency = 4,                     // at most 4 items at once (default 8)
            MaxItems = 100,                         // a longer list fails the map before any item starts
            ItemTimeout = TimeSpan.FromSeconds(30), // each item's own time limit
            Partial = PartialResults.KeepSucceeded, // reduce over the items that succeeded
        };
        // docs:end guides/graphs/map-fan-out/options

        var invoice = Invoice.Of(
            "INV-2212",
            new("L01", "PAP-A4", 10, 4.20m),
            new("L02", "INK-K", 1, 62.00m),
            new("L03", "INK-X9", 1, 58.00m),
            new("L04", "PEN-BLU", 20, 0.90m));
        var result = await InvoiceGraph(new PriceList(), options).Compile()
            .RunAsync("map-fan-out-options", invoice, new InMemoryCheckpointStore());

        // The unknown SKU fails L03, and the plain reducer gets the other three.
        var audited = Assert.IsType<GraphExecutionResult<Invoice>.Completed>(result).State;
        Assert.Equal(["L01", "L02", "L04"], audited.Audits.Select(a => a.Line.Id));
        Assert.Equal("approve", audited.Decision);
        Assert.Throws<ArgumentOutOfRangeException>(() => new MapOptions<LineAudit> { MaxConcurrency = 0 });
    }

    [Fact]
    public async Task ItemFails()
    {
        var invoice = Invoice.Of(
            "INV-2212",
            new("L01", "PAP-A4", 10, 4.20m),
            new("L02", "INK-K", 1, 62.00m),
            new("L03", "INK-X9", 1, 58.00m),
            new("L04", "PEN-BLU", 20, 0.90m));
        var result = await InvoiceGraph(new PriceList())
            .SetRecoveryPolicy("AuditLines", RecoveryPolicy.Retry(2))
            .Compile()
            .RunAsync("map-fan-out-3a", invoice, new InMemoryCheckpointStore());

        var failed = Assert.IsType<GraphExecutionResult<Invoice>.NodeFailed>(result);
        Assert.Equal(("AuditLines", 1), (failed.Node, failed.Attempts));
        Assert.Equal("item 'L03': failed at 'CheckPrice' after 1 attempt(s): unknown SKU 'INK-X9'", failed.Message);
    }

    [Fact]
    public async Task KeepPartialResults()
    {
        var mixed = Invoice.Of(
            "INV-2214",
            new("L01", "PAP-A4", 10, 4.20m),
            new("L02", "INK-K", 1, 62.00m),
            new("L03", "INK-X9", 1, 58.00m),
            new("L04", "PEN-BLU", 20, 0.90m),
            new("L05", "SVC-INST", 1, 52.00m));
        BranchOutcomes<LineAudit>? reduced = null;

        // docs:begin guides/graphs/map-fan-out/keep-partial
        var keepGraph = new StateGraph<Invoice>()
            .AddMapWithOutcomes(
                "AuditLines",
                inv => inv.Lines,
                line => line.Id,
                LineAuditGraph(new PriceList()),
                (_, line) => LineAudit.For(line),
                (inv, results) =>
                {
                    // docs:hide ...
                    reduced = results;
                    // docs:show
                    return inv with
                    {
                        Audits = [.. results.Succeeded],
                        Review = [.. results.Where(r => r.Status != BranchStatus.Succeeded).Select(r => $"{r.Name} not audited: {r.Error?.Message}")],
                    };
                },
                new MapOptions<LineAudit> { Partial = PartialResults.KeepSucceeded })
            .AddNode("Decide", Decide)
            .AddEdge("AuditLines", "Decide")
            .Compile();
        // docs:end guides/graphs/map-fan-out/keep-partial

        var result = await keepGraph.RunAsync("map-fan-out-4", mixed, new InMemoryCheckpointStore());

        var kept = Assert.IsType<GraphExecutionResult<Invoice>.Completed>(result).State;
        Assert.NotNull(reduced);
        Assert.Equal(
            [BranchStatus.Succeeded, BranchStatus.Succeeded, BranchStatus.Failed, BranchStatus.Succeeded, BranchStatus.Succeeded],
            reduced.Select(o => o.Status));
        Assert.False(reduced.JoinMet);
        Assert.Equal(NodeFailureKind.Fatal, reduced["L03"]!.Error!.Kind);
        Assert.Equal("hold: 2 line(s) need review", kept.Decision);
        Assert.Equal(
            [
                "L03 not audited: failed at 'CheckPrice' after 1 attempt(s): unknown SKU 'INK-X9'",
                "L05 billed 52.00, list 45.00 (+16%)",
            ],
            kept.Review);
    }

    // docs:begin guides/graphs/map-fan-out/parent
    static StateGraph<Invoice> InvoiceGraph(PriceList prices, MapOptions<LineAudit>? options = null) =>
        new StateGraph<Invoice>()
            .AddMap(
                "AuditLines",
                inv => inv.Lines,
                line => line.Id,
                LineAuditGraph(prices),
                (_, line) => LineAudit.For(line),
                (inv, audits) => inv with { Audits = [.. audits] },
                options)
            .AddNode("Decide", Decide)
            .AddEdge("AuditLines", "Decide");
    // docs:end guides/graphs/map-fan-out/parent

    // docs:begin guides/graphs/map-fan-out/child
    static CompiledGraph<LineAudit> LineAuditGraph(PriceList prices) =>
        new StateGraph<LineAudit>()
            .AddNode("Classify", audit => audit with { Category = Category(audit.Line.Sku) })
            .AddFallibleNode("CheckPrice", async (audit, _, ct) => audit with { ListPrice = await prices.LookUpAsync(audit.Line, ct) })
            .AddNode("Score", Score)
            .AddEdge("Classify", "CheckPrice")
            .AddEdge("CheckPrice", "Score")
            .SetRecoveryPolicy("CheckPrice", RecoveryPolicy.Retry(1))
            .Compile();
    // docs:end guides/graphs/map-fan-out/child

    private static string Category(string sku) => sku.Split('-')[0] switch
    {
        "PAP" or "PAD" => "paper",
        "INK" => "toner",
        "PEN" or "FLD" => "stationery",
        "SVC" => "service",
        _ => "other",
    };

    // 100 at or below list price, 4 points off per percent over it.
    private static LineAudit Score(LineAudit audit)
    {
        var list = audit.ListPrice!.Value;
        var over = (int)Math.Round((audit.Line.UnitPrice - list) / list * 100);
        return over <= 0
            ? audit with { Score = 100 }
            : audit with
            {
                Score = Math.Max(0, 100 - (4 * over)),
                Note = string.Create(CultureInfo.InvariantCulture, $"billed {audit.Line.UnitPrice:0.00}, list {list:0.00} (+{over}%)"),
            };
    }

    // Any line scoring under 80 (or not audited at all) holds the invoice.
    private static Invoice Decide(Invoice invoice)
    {
        List<string> review = [.. invoice.Review, .. invoice.Audits.Where(a => a.Score < 80).Select(a => $"{a.Line.Id} {a.Note}")];
        var decision = review.Count switch
        {
            0 => "approve",
            1 when invoice.Review.Count == 0 => $"hold: {review[0]}",
            _ => $"hold: {review.Count} line(s) need review",
        };
        return invoice with { Review = review, Decision = decision };
    }
}

internal sealed record InvoiceLine(string Id, string Sku, int Quantity, decimal UnitPrice);

/// <summary>The parent's state: the lines to audit, the audits that came back, and the decision.</summary>
internal sealed record Invoice(
    string Number,
    IReadOnlyList<InvoiceLine> Lines,
    IReadOnlyList<LineAudit> Audits,
    IReadOnlyList<string> Review,
    string? Decision)
{
    public static Invoice Of(string number, params InvoiceLine[] lines) => new(number, lines, [], [], null);
}

/// <summary>One item's child state: the line, and what Classify, CheckPrice and Score found.</summary>
internal sealed record LineAudit(InvoiceLine Line, string? Category, decimal? ListPrice, int? Score, string? Note)
{
    public static LineAudit For(InvoiceLine line) => new(line, null, null, null, null);
}

/// <summary>The supplier's price list, in memory, so the samples run offline.</summary>
internal sealed class PriceList
{
    private static readonly Dictionary<string, decimal> Prices = new()
    {
        ["PAP-A4"] = 4.20m,
        ["PAP-A3"] = 8.10m,
        ["PAD-A5"] = 2.40m,
        ["INK-K"] = 62.00m,
        ["INK-C"] = 71.00m,
        ["INK-M"] = 71.00m,
        ["PEN-BLU"] = 0.90m,
        ["PEN-RED"] = 0.90m,
        ["FLD-GRN"] = 0.35m,
        ["SVC-INST"] = 45.00m,
        ["SVC-DLV"] = 12.00m,
    };

    public async Task<decimal> LookUpAsync(InvoiceLine line, CancellationToken ct)
    {
        await Task.Yield();
        ct.ThrowIfCancellationRequested();
        return Prices.TryGetValue(line.Sku, out var price)
            ? price
            : throw NodeException.Fatal($"unknown SKU '{line.Sku}'");
    }
}
