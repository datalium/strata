// Samples for https://stratamesh.dev/guides/human-in-the-loop/pause-for-approval/
//
// The page shows the using lines of three samples, so the file has a namespace block for each: a using
// directive must come before the members of its block.

// docs:begin guides/human-in-the-loop/pause-for-approval/boundary
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;

// docs:hide
using Datalium.Strata.Storage;

namespace Datalium.Strata.DocsSnippets.Guides.HumanInTheLoop.PauseForApproval
{
    public partial class PauseForApprovalSamples
    {
        private static CompiledGraph<WireTransferState> BuildGraph(Bank bank)
        {
            // docs:show
            const decimal TransferLimit = 10_000m;

            var graph = new StateGraph<WireTransferState>()
                .AddNode("PrepareWireTransfer", state => state with { InvoiceGenerated = true, Status = "awaiting approval" })
                .AddInterruptBoundary("PrepareWireTransfer", onReject: "CancelWireTransfer")
                .AddNode("ExecuteWireTransfer", (state, ctx, _) =>
                {
                    var decision = ctx.Decision ?? throw new InvalidOperationException("a transfer must not run without a decision");

                    // The world may have changed while the thread waited. Check again before the irreversible step.
                    if (state.Amount <= 0 || state.Amount > TransferLimit)
                    {
                        return ValueTask.FromResult(state with { Status = "held: amount outside the limit", ApprovedBy = decision.Reviewer });
                    }

                    var reference = bank.Send(state.Beneficiary, state.Amount, state.Currency);
                    return ValueTask.FromResult(state with { Status = $"sent ({reference})", ApprovedBy = decision.Reviewer });
                })
                .AddNode("CancelWireTransfer", (state, ctx, _) =>
                    ValueTask.FromResult(state with { Status = $"cancelled by {ctx.Decision!.Reviewer}" }))
                .AddEdge("PrepareWireTransfer", "ExecuteWireTransfer")
                .Compile();
            // docs:hide
            return graph;
        }
    }

    // docs:show

    sealed record WireTransferState(
        string Beneficiary, decimal Amount, string Currency, bool InvoiceGenerated,
        string Status, string? ApprovedBy, string? Note)
    {
        public static WireTransferState New(string beneficiary, decimal amount) =>
            new(beneficiary, amount, "USD", InvoiceGenerated: false, Status: "new", ApprovedBy: null, Note: null);
    }
    // docs:end guides/human-in-the-loop/pause-for-approval/boundary

    /// <summary>Stands in for the payment client: it counts what was sent.</summary>
    internal sealed class Bank
    {
        public List<string> Sent { get; } = [];

        public string Send(string beneficiary, decimal amount, string currency)
        {
            Sent.Add($"{beneficiary} {amount:F2} {currency}");
            return $"ref {Sent.Count:D4}";
        }
    }

    /// <summary>A clock stopped at one time, so a test can wake a thread whose timeout is a day away.</summary>
    internal sealed class FixedTime(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.HumanInTheLoop.PauseForApproval
{
    // docs:begin guides/human-in-the-loop/pause-for-approval/open-store
    using Datalium.Strata.Storage.Memory;
    using Microsoft.EntityFrameworkCore;

    // docs:hide
    public partial class PauseForApprovalSamples
    {
        // docs:show
        static EfCoreCheckpointStore OpenStore(string path) =>
            new(new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>()
                .UseSqlite($"Data Source={path}")
                .Options));
        // docs:end guides/human-in-the-loop/pause-for-approval/open-store
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.HumanInTheLoop.PauseForApproval
{
    // docs:begin guides/human-in-the-loop/pause-for-approval/show-reviewer
    using System.Text.Json;

    // docs:hide
    public partial class PauseForApprovalSamples
    {
        private static async Task<WireTransferState> ShowReviewerAsync(ICheckpointStore store)
        {
            // docs:show
            var checkpoint = await store.LoadAsync("wire-acme-corp");
            var pending = checkpoint!.FrozenState.Deserialize<WireTransferState>()!;
            Console.WriteLine($"Review: {pending.Amount} {pending.Currency} to {pending.Beneficiary}");
            // docs:end guides/human-in-the-loop/pause-for-approval/show-reviewer
            return pending;
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.HumanInTheLoop.PauseForApproval
{
    public partial class PauseForApprovalSamples
    {
        [Fact]
        public async Task ApproveAndReject()
        {
            // The page's file name. The test runs in its output folder, and no other page uses this name.
            foreach (var file in new[] { "checkpoints.db", "checkpoints.db-wal", "checkpoints.db-shm" })
            {
                File.Delete(file);
            }

            var bank = new Bank();
            var graph = BuildGraph(bank);

            // docs:begin guides/human-in-the-loop/pause-for-approval/run
            using (var store = OpenStore("checkpoints.db"))
            {
                var result = await graph.RunAsync("wire-acme-corp", WireTransferState.New("Acme Corp", 4200.50m), store);
                if (result is GraphExecutionResult<WireTransferState>.Interrupted paused)
                {
                    Console.WriteLine($"{paused.ThreadId}: paused after {paused.SuspendedNode}, continues at {paused.NextNode}");
                }
                // docs:hide
                var interrupted = Assert.IsType<GraphExecutionResult<WireTransferState>.Interrupted>(result);
                Assert.Equal("PrepareWireTransfer", interrupted.SuspendedNode);
                Assert.Equal("ExecuteWireTransfer", interrupted.NextNode);
                // docs:show
            }
            // docs:end guides/human-in-the-loop/pause-for-approval/run

            using (var store = OpenStore("checkpoints.db"))
            {
                var globex = await graph.RunAsync("wire-globex", WireTransferState.New("Globex Ltd", 9800.00m), store);
                Assert.IsType<GraphExecutionResult<WireTransferState>.Interrupted>(globex);
            }

            Assert.Empty(bank.Sent);

            {
                // docs:begin guides/human-in-the-loop/pause-for-approval/find-paused
                using var store = OpenStore("checkpoints.db");
                var page = await store.ListThreadsAsync(new ThreadQuery
                {
                    States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Interrupted },
                });
                foreach (var thread in page.Threads)
                {
                    Console.WriteLine($"{thread.ThreadId}: waiting at {string.Join("/", thread.WaitingAt)}, continues at {thread.NextNode}");
                }
                // docs:end guides/human-in-the-loop/pause-for-approval/find-paused

                Assert.Equal(
                    new[] { "wire-acme-corp", "wire-globex" },
                    page.Threads.Select(thread => thread.ThreadId).Order(StringComparer.Ordinal));
                Assert.All(page.Threads, thread =>
                {
                    Assert.Equal(new[] { "PrepareWireTransfer" }, thread.WaitingAt);
                    Assert.Equal("ExecuteWireTransfer", thread.NextNode);
                });

                var pending = await ShowReviewerAsync(store);
                Assert.Equal(WireTransferState.New("Acme Corp", 4200.50m) with { InvoiceGenerated = true, Status = "awaiting approval" }, pending);

                // docs:begin guides/human-in-the-loop/pause-for-approval/approve
                var approval = HumanSignal.Approve("jdoe@acme.io").WithOverride("Amount", 4150.25m);
                var approved = await graph.ResumeAsync("wire-acme-corp", approval, store);
                if (approved is GraphExecutionResult<WireTransferState>.Completed done)
                {
                    Console.WriteLine($"{done.State.Status}: {done.State.Amount} {done.State.Currency}, approved by {done.State.ApprovedBy}");
                }
                // docs:end guides/human-in-the-loop/pause-for-approval/approve

                var sent = Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(approved).State;
                Assert.Equal("sent (ref 0001)", sent.Status);
                Assert.Equal(4150.25m, sent.Amount);
                Assert.Equal("jdoe@acme.io", sent.ApprovedBy);
                Assert.Equal(new[] { "Acme Corp 4150.25 USD" }, bank.Sent);

                // docs:begin guides/human-in-the-loop/pause-for-approval/reject
                var rejection = HumanSignal.Reject("mlee@acme.io").WithOverride("Note", "beneficiary not on the vendor list");
                var rejected = await graph.ResumeAsync("wire-globex", rejection, store);
                if (rejected is GraphExecutionResult<WireTransferState>.Completed cancelled)
                {
                    Console.WriteLine($"{cancelled.State.Status}: {cancelled.State.Note}");
                }
                // docs:end guides/human-in-the-loop/pause-for-approval/reject

                var globexState = Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(rejected).State;
                Assert.Equal("cancelled by mlee@acme.io", globexState.Status);
                Assert.Equal("beneficiary not on the vendor list", globexState.Note);
                Assert.Single(bank.Sent);

                // docs:begin guides/human-in-the-loop/pause-for-approval/refused
                var again = await graph.ResumeAsync("wire-acme-corp", HumanSignal.Approve("jdoe@acme.io"), store);
                if (again is GraphExecutionResult<WireTransferState>.Failed refused)
                {
                    Console.WriteLine($"Refused: {refused.Message}");
                }
                // docs:end guides/human-in-the-loop/pause-for-approval/refused

                var failed = Assert.IsType<GraphExecutionResult<WireTransferState>.Failed>(again);
                Assert.Equal("checkpoint for thread 'wire-acme-corp' was already resumed", failed.Message);
                Assert.Single(bank.Sent);

                // The check after the boundary holds a corrected amount above the limit.
                await graph.RunAsync("wire-initech", WireTransferState.New("Initech", 900m), store);
                var tooMuch = HumanSignal.Approve("jdoe@acme.io").WithOverride("Amount", 25_000m);
                var held = Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(
                    await graph.ResumeAsync("wire-initech", tooMuch, store));
                Assert.Equal("held: amount outside the limit", held.State.Status);
                Assert.Single(bank.Sent);

                // A string override for the decimal amount is refused, and the thread keeps waiting.
                await graph.RunAsync("wire-umbrella", WireTransferState.New("Umbrella", 100m), store);
                var wrongType = HumanSignal.Approve("jdoe@acme.io").WithOverride("Amount", "a lot");
                var notRehydrated = Assert.IsType<GraphExecutionResult<WireTransferState>.Failed>(
                    await graph.ResumeAsync("wire-umbrella", wrongType, store));
                Assert.StartsWith("failed to rehydrate state for thread", notRehydrated.Message);
                Assert.Null((await store.LoadAsync("wire-umbrella"))!.ResumedAt);
            }
        }

        [Fact]
        public async Task ApprovalTimeout()
        {
            var bank = new Bank();
            var store = new InMemoryCheckpointStore();
            var graph = new StateGraph<WireTransferState>()
                .AddNode("PrepareWireTransfer", state => state with { InvoiceGenerated = true, Status = "awaiting approval" })
                // docs:begin guides/human-in-the-loop/pause-for-approval/timeout
                .AddInterruptBoundary(
                    "PrepareWireTransfer",
                    onReject: "CancelWireTransfer",
                    timeout: TimeSpan.FromHours(24),
                    onTimeout: "EscalateWireTransfer")
                // docs:hide ...
                .AddNode("ExecuteWireTransfer", state =>
                    state with { Status = $"sent ({bank.Send(state.Beneficiary, state.Amount, state.Currency)})" })
                .AddNode("CancelWireTransfer", state => state with { Status = "cancelled" })
                // docs:show
                .AddNode("EscalateWireTransfer", state => state with { Status = "escalated: no answer within 24 h" })
                // docs:hide
                .AddEdge("PrepareWireTransfer", "ExecuteWireTransfer")
                .Compile();

            var paused = await graph.RunAsync("wire-timeout", WireTransferState.New("Hooli", 500m), store);
            Assert.IsType<GraphExecutionResult<WireTransferState>.Interrupted>(paused);
            // docs:show

            // On a schedule:
            var report = await graph.WakeDueAsync(store);
            foreach (var woken in report.Woken)
            {
                Console.WriteLine($"{woken.ThreadId}: {woken.Reason}");
            }
            // docs:end guides/human-in-the-loop/pause-for-approval/timeout

            // Within the 24 hours, the wake call leaves the thread waiting.
            Assert.Empty(report.Woken);

            // Once they have passed, it continues at the timeout route, with no decision.
            var later = new WakeOptions { Run = new RunOptions { TimeProvider = new FixedTime(DateTimeOffset.UtcNow.AddHours(25)) } };
            var timedOut = Assert.Single((await graph.WakeDueAsync(store, later)).Woken);
            Assert.Equal("wire-timeout", timedOut.ThreadId);
            Assert.Equal(WakeReason.Timeout, timedOut.Reason);
            var escalated = Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(timedOut.Result);
            Assert.Equal("escalated: no answer within 24 h", escalated.State.Status);
            Assert.Empty(bank.Sent);
        }
    }
}
