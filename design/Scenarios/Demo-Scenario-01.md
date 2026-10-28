## Complete End-to-End Demo Scenario

This demo showcases an enterprise Wire Transfer Approval Agent. It chains an LLM call, triggers an automatic Human-in-the-Loop (HITL) Interruption for transfers exceeding a specific dollar threshold, streams diagnostic updates, and leverages type-safe routing.

---

### 🔷 The C# Demo Scenario Implementation

```csharp
using System;
using System.Threading.Tasks;
using Datalium.Strata;
using Datalium.Strata.Streaming;
using Datalium.Strata.Interruption;

// 1. Define a strongly-typed, immutable state contract
public record TransferState(
    string AccountId,
    decimal Amount,
    bool IsPreApproved,
    string ApproverId,
    string ExecutionReceipt,
    bool IsCompleted
);

public class Program
{
    public static async Task Main()
    {
        var initialState = new TransferState("ACC-9921", 15500.00m, false, "", "", false);

        // 2. Build the state graph with a mandatory Human-in-the-Loop boundary
        var graph = new StateGraph<TransferState>(initialState)
            .AddNode("EvaluateRisk", async (state, ctx) =>
            {
                await ctx.Stream.EmitDiagnosticAsync(StreamChunkType.NodeTransition, "Evaluating transaction compliance risk...");

                // Risk-rule: Transfers over $10,000 require manual compliance sign-off
                bool needsHumanApproval = state.Amount > 10000.00m && !state.IsPreApproved;

                return ctx.RouteTo(needsHumanApproval ? "AwaitingCompliance" : "ExecuteTransfer");
            })
            .AddNode("AwaitingCompliance", async (state, ctx) =>
            {
                // This node acts as our frozen registration state marker
                return state;
            })
            .AddInterruptBoundary("AwaitingCompliance") // Pipeline freezes, flushes state, and evicts thread here
            .AddNode("ExecuteTransfer", async (state, ctx) =>
            {
                await ctx.Stream.EmitDiagnosticAsync(StreamChunkType.NodeTransition, "Executing bank ledger wire routing...");

                // Extract human approver metadata safely from context if it bypassed risk
                string finalApprover = string.IsNullOrEmpty(state.ApproverId) ? "SYSTEM_AUTO" : state.ApproverId;
                string receipt = $"TX-WIRE-{Guid.NewGuid().ToString()[..8].ToUpper()}-AUTH({finalApprover})";

                return state with { ExecutionReceipt = receipt, IsCompleted = true };
            })
            .SetEntryPoint("EvaluateRisk");

        // 3. First execution run (Hits the interruption gate)
        var runner = graph.Compile();
        Console.WriteLine("--- Initializing Execution Pipeline ---");

        var runStream = runner.ExecuteWithStreamingAsync(initialState);
        await foreach (var chunk in runStream)
        {
            Console.WriteLine($"[{chunk.NodeName}] Output ({chunk.Type}): {chunk.Payload}");
        }

        // Check if runner halted safely at the interruption boundary
        string activeThreadId = runner.CurrentThreadId;
        Console.WriteLine($"\n⚠️ Execution Suspended! Saved Thread ID: {activeThreadId}");

        // 4. Simulate a Human Approver clicking "Approve" inside a corporate dashboard UI 3 hours later
        Console.WriteLine("\n--- Simulating External Dashboard Human Approval Input ---");
        var humanFeedback = new HumanFeedback(
            Status: InterruptStatus.Approved,
            ApproverId: "USR-COMPLIANCE-OFFICER-7",
            Notes: "High-value transaction audited and cleared.",
            ContextOverrides: new() { { "IsPreApproved", "true" }, { "ApproverId", "USR-COMPLIANCE-OFFICER-7" } }
        );

        // 5. Resume the state graph execution right where it left off
        Console.WriteLine("--- Resuming Execution Pipeline ---");
        var resumeStream = runner.ResumeWithStreamingAsync(activeThreadId, humanFeedback);

        await foreach (var chunk in resumeStream)
        {
            Console.WriteLine($"[{chunk.NodeName}] Output ({chunk.Type}): {chunk.Payload}");
        }

        var finalState = await runner.GetStateAsync(activeThreadId);
        Console.WriteLine($"\n✨ Final Run Status: Completed={finalState.IsCompleted} | Receipt={finalState.ExecutionReceipt}");
    }
}
```
