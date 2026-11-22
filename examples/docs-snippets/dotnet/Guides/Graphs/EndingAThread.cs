// Samples for https://stratamesh.dev/guides/graphs/ending-a-thread/

using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

namespace Datalium.Strata.DocsSnippets.Guides.Graphs.EndingAThread;

public class EndingAThreadSamples
{
    [Fact]
    public async Task EndAWaitingThread()
    {
        var transfers = TransferGraph(TimeSpan.Zero);
        var store = new InMemoryCheckpointStore();
        await transfers.RunAsync("transfer-acme", new Transfer("acme", 1200m), store);
        var output = Console.Capture();

        // docs:begin guides/graphs/ending-a-thread/end
        var result = await transfers.EndAsync(
            "transfer-acme",
            ThreadEnd.By("oncall"),
            store,
            new RunOptions { Metadata = { ["ticket"] = "OPS-7" } });

        switch (result)
        {
            case GraphExecutionResult<Transfer>.Cancelled { Reason: CancelReason.Ended } ended:
                Console.WriteLine($"Ended at {ended.Node} ([{string.Join(", ", ended.StoppedAt)}])");
                break;
            case GraphExecutionResult<Transfer>.Failed refused:
                Console.WriteLine($"Refused: {refused.Message}");
                break;
        }

        var checkpoint = store.Load("transfer-acme")!;
        Console.WriteLine(
            $"checkpoint: status {checkpoint.Status}, next {checkpoint.NextNode ?? "null"}, " +
            $"{ThreadEnd.ReasonKey}={checkpoint.Metadata![ThreadEnd.ReasonKey]}, " +
            $"{ThreadEnd.EndedByKey}={checkpoint.Metadata[ThreadEnd.EndedByKey]}, ticket={checkpoint.Metadata["ticket"]}");
        // docs:end guides/graphs/ending-a-thread/end

        Assert.Equal(
            [
                "Ended at Prepare ([Prepare])",
                "checkpoint: status Ended, next null, strata.end_reason=requested, strata.ended_by=oncall, ticket=OPS-7",
            ],
            output);
        Assert.Equal(ThreadEnd.Requested, checkpoint.Metadata[ThreadEnd.ReasonKey]);

        // An answer that arrives after the end is refused, and writes nothing.
        var late = await transfers.ResumeAsync("transfer-acme", HumanSignal.Approve("alice"), store);
        Assert.Equal("Failed: thread 'transfer-acme' was ended: an ended thread can't be resumed", Show(late));

        // Ending it again returns the same result, and the first caller's EndedBy stays.
        var again = await transfers.EndAsync("transfer-acme", ThreadEnd.By("someone-else"), store);
        Assert.Equal("Cancelled at Prepare, reason Ended, stopped at [Prepare]", Show(again));
        Assert.Equal("oncall", store.Load("transfer-acme")!.Metadata![ThreadEnd.EndedByKey]);

        // Under the default Boundaries policy, an approved and finished thread can't be ended.
        await transfers.RunAsync("transfer-globex", new Transfer("globex", 300m), store);
        await transfers.ResumeAsync("transfer-globex", HumanSignal.Approve("alice"), store);
        var endGlobex = Show(await transfers.EndAsync("transfer-globex", store));
        Assert.StartsWith("Failed: thread 'transfer-globex' was resumed at ", endGlobex);
        Assert.EndsWith(" and may still be running: only a waiting, failed or cancelled thread can be ended", endGlobex);
    }

    [Fact]
    public async Task CancelARunInstead()
    {
        var transfers = TransferGraph(TimeSpan.FromSeconds(30));
        var store = new InMemoryCheckpointStore();
        await transfers.RunAsync("transfer-initech", new Transfer("initech", 800m), store);
        var output = Console.Capture();

        // docs:begin guides/graphs/ending-a-thread/cancel
        using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100)))
        {
            // Execute is still running when the token fires.
            var cancelled = await transfers.ResumeAsync(
                "transfer-initech", HumanSignal.Approve("alice"), store, cts.Token);
            Console.WriteLine("cancelled: " + Show(cancelled));
        }

        // Later: run Execute again with ResumeAsync, or give up on the thread.
        Console.WriteLine("end cancelled: " + Show(await transfers.EndAsync("transfer-initech", store)));
        // docs:end guides/graphs/ending-a-thread/cancel

        Assert.Equal(
            [
                "cancelled: Cancelled at Execute, reason Requested, stopped at [Execute]",
                "end cancelled: Cancelled at Execute, reason Ended, stopped at [Execute]",
            ],
            output);
    }

    // Prepare, an interrupt boundary at Prepare, then Execute, which takes `execute` and stops when the run is
    // cancelled.
    private static CompiledGraph<Transfer> TransferGraph(TimeSpan execute) =>
        new StateGraph<Transfer>()
            .AddNode("Prepare", t => t with { Prepared = true })
            .AddNode("Execute", async (t, _, ct) =>
            {
                await Task.Delay(execute, ct);
                return t with { Executed = true };
            })
            .AddEdge("Prepare", "Execute")
            .AddInterruptBoundary("Prepare")
            .Compile();

    private static string Show(GraphExecutionResult<Transfer> result) => result switch
    {
        GraphExecutionResult<Transfer>.Cancelled c =>
            $"Cancelled at {c.Node}, reason {c.Reason}, stopped at [{string.Join(", ", c.StoppedAt)}]",
        GraphExecutionResult<Transfer>.Failed f => $"Failed: {f.Message}",
        GraphExecutionResult<Transfer>.Interrupted i => $"Interrupted at {i.SuspendedNode}",
        GraphExecutionResult<Transfer>.Completed => "Completed",
        _ => result.ToString(),
    };
}

internal sealed record Transfer(string Payee, decimal Amount, bool Prepared = false, bool Executed = false);

/// <summary>Stands in for <see cref="System.Console"/> in the samples, so a test can check what they print.</summary>
internal static class Console
{
    private static readonly AsyncLocal<List<string>?> Lines = new();

    public static List<string> Capture() => Lines.Value = [];

    public static void WriteLine(string line) => Lines.Value?.Add(line);
}
