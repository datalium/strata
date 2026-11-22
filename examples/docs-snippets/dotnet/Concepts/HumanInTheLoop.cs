// Samples for https://stratamesh.dev/concepts/human-in-the-loop/

using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

namespace Datalium.Strata.DocsSnippets.Concepts.HumanInTheLoop;

public class HumanInTheLoopSamples
{
    [Fact]
    public async Task WireTransfer()
    {
        var bank = new Bank();
        var store = new InMemoryCheckpointStore();

        // docs:begin concepts/human-in-the-loop/wire-transfer
        var graph = new StateGraph<WireTransferState>()
            .AddNode("PrepareWireTransfer", state => state with { InvoiceGenerated = true, Status = "awaiting approval" })
            .AddInterruptBoundary("PrepareWireTransfer", onReject: "CancelWireTransfer")
            .AddNode("ExecuteWireTransfer", (state, ctx, _) =>
            {
                var decision = ctx.Decision ?? throw new InvalidOperationException("a transfer must not run without a decision");
                var reference = bank.Send(state.Beneficiary, state.Amount, state.Currency);
                return ValueTask.FromResult(state with { Status = $"sent ({reference})", ApprovedBy = decision.Reviewer });
            })
            .AddNode("CancelWireTransfer", (state, ctx, _) =>
                ValueTask.FromResult(state with { Status = $"cancelled by {ctx.Decision!.Reviewer}" }))
            .AddEdge("PrepareWireTransfer", "ExecuteWireTransfer")
            .Compile();

        // Runs PrepareWireTransfer, saves the thread, and returns Interrupted.
        var result = await graph.RunAsync("wire-acme-corp", WireTransferState.New("Acme Corp", 4200.50m), store);

        // Later, in this process or another one: approve, and correct the amount.
        var approval = HumanSignal.Approve("jdoe@acme.io").WithOverride("Amount", 4150.25m);
        var resumed = await graph.ResumeAsync("wire-acme-corp", approval, store);
        // docs:end concepts/human-in-the-loop/wire-transfer

        var interrupted = Assert.IsType<GraphExecutionResult<WireTransferState>.Interrupted>(result);
        Assert.Equal("PrepareWireTransfer", interrupted.SuspendedNode);
        Assert.Equal("ExecuteWireTransfer", interrupted.NextNode);
        Assert.True(interrupted.FrozenState.InvoiceGenerated);

        var completed = Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(resumed);
        Assert.Equal(4150.25m, completed.State.Amount);
        Assert.Equal("jdoe@acme.io", completed.State.ApprovedBy);
        Assert.Equal("sent (ref 0001)", completed.State.Status);
        Assert.Equal(["Acme Corp 4150.25 USD"], bank.Sent);

        // A rejection takes the reject route, and the transfer node never runs.
        var paused = await graph.RunAsync("wire-globex", WireTransferState.New("Globex Ltd", 9800.00m), store);
        Assert.IsType<GraphExecutionResult<WireTransferState>.Interrupted>(paused);
        var rejected = await graph.ResumeAsync("wire-globex", HumanSignal.Reject("mlee@acme.io"), store);
        Assert.Equal(
            "cancelled by mlee@acme.io",
            Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(rejected).State.Status);
        Assert.Single(bank.Sent);
    }
}

internal sealed record WireTransferState(
    string Beneficiary,
    decimal Amount,
    string Currency,
    bool InvoiceGenerated,
    string Status,
    string? ApprovedBy)
{
    public static WireTransferState New(string beneficiary, decimal amount) =>
        new(beneficiary, amount, "USD", InvoiceGenerated: false, Status: "new", ApprovedBy: null);
}

// The irreversible side effect, kept in memory so the test can see what was sent.
internal sealed class Bank
{
    public List<string> Sent { get; } = [];

    public string Send(string beneficiary, decimal amount, string currency)
    {
        Sent.Add($"{beneficiary} {amount:F2} {currency}");
        return $"ref {Sent.Count:D4}";
    }
}
