// Samples for https://stratamesh.dev/guides/graphs/event-waits-and-timers/

using System.Globalization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.DocsSnippets.Guides.Graphs.EventWaitsAndTimers;

public class EventWaitsAndTimersSamples
{
    [Fact]
    public async Task WaitSignalAndWake()
    {
        var orders = OrderGraph();
        var output = Console.Capture();

        // docs:begin guides/graphs/event-waits-and-timers/run
        var clock = new FakeTimeProvider(DateTimeOffset.Parse("2026-11-02T09:00:00Z", CultureInfo.InvariantCulture));
        var run = new RunOptions { TimeProvider = clock };
        var store = new InMemoryCheckpointStore(clock);

        foreach (var id in new[] { "order-1", "order-2" })
        {
            var result = await orders.RunAsync(id, new Order(id, false, null, false), store, run);
            if (result is GraphExecutionResult<Order>.Interrupted waiting)
            {
                Console.WriteLine($"{id}: {waiting.Wait!.Kind} {waiting.Wait.EventType} until {waiting.Wait.WakeAt}");
            }
        }
        // docs:end guides/graphs/event-waits-and-timers/run

        // docs:begin guides/graphs/event-waits-and-timers/signal
        var signal = new EventSignal("payment.received")
            .WithOverride("Paid", true)
            .WithOverride("PaymentRef", "PAY-5531");
        var paid = await orders.SignalAsync("order-1", signal, store, run);
        if (paid is GraphExecutionResult<Order>.Interrupted next)
        {
            Console.WriteLine($"order-1: paid ({next.FrozenState.PaymentRef}), now {next.Wait!.Kind} until {next.Wait.WakeAt}");
        }
        // docs:end guides/graphs/event-waits-and-timers/signal

        Assert.True(Assert.IsType<GraphExecutionResult<Order>.Interrupted>(paid).FrozenState.Paid);

        // docs:begin guides/graphs/event-waits-and-timers/refused
        var wrongType = await orders.SignalAsync("order-2", new EventSignal("refund.issued"), store, run);
        var approve = await orders.ResumeAsync("order-2", HumanSignal.Approve("clerk"), store, run);
        var notWaiting = await orders.SignalAsync("order-1", signal, store, run);
        // docs:end guides/graphs/event-waits-and-timers/refused

        Assert.Equal(
            "thread 'order-2' is not waiting for event 'refund.issued': it is waiting for event 'payment.received' at [AwaitPayment]",
            Refusal(wrongType));
        Assert.Equal(
            "thread 'order-2' is waiting for event 'payment.received' at [AwaitPayment]: only a signal of that event answers it",
            Refusal(approve));
        Assert.Equal(
            "thread 'order-1' is not waiting for event 'payment.received': it is waiting for a timer at [AwaitPickup]",
            Refusal(notWaiting));

        // docs:begin guides/graphs/event-waits-and-timers/wake
        clock.Advance(TimeSpan.FromHours(5));
        var due = await store.ListThreadsAsync(new ThreadQuery { DueBy = clock.GetUtcNow() });
        Console.WriteLine($"Due: {string.Join(", ", due.Threads.Select(t => t.ThreadId))}");
        await Wake();

        clock.Advance(TimeSpan.FromHours(20));
        await Wake();

        async Task Wake()
        {
            var report = await orders.WakeDueAsync(store, new WakeOptions { Run = run });
            foreach (var woken in report.Woken)
            {
                Console.WriteLine($"Woke {woken.ThreadId} ({woken.Reason}): {Outcome(woken.Result)}");
            }
        }
        // docs:end guides/graphs/event-waits-and-timers/wake

        Assert.Equal(
            [
                "order-1: Event payment.received until 2026-11-03T09:00:00Z",
                "order-2: Event payment.received until 2026-11-03T09:00:00Z",
                "order-1: paid (PAY-5531), now Timer until 2026-11-02T13:00:00Z",
                "Due: order-1",
                "Woke order-1 (Timer): completed, shipped = True",
                "Woke order-2 (Timeout): cancelled (Ended) at AwaitPayment",
            ],
            output);
        Assert.Equal(ThreadEnd.TimedOut, store.Load("order-2")!.Metadata![ThreadEnd.ReasonKey]);
    }

    [Fact]
    public async Task WakeOneThread()
    {
        var orders = OrderGraph();
        var clock = new FakeTimeProvider(DateTimeOffset.Parse("2026-11-02T09:00:00Z", CultureInfo.InvariantCulture));
        var run = new RunOptions { TimeProvider = clock };
        var store = new InMemoryCheckpointStore(clock);
        foreach (var id in new[] { "order-1005", "order-3" })
        {
            await orders.RunAsync(id, new Order(id, false, null, false), store, run);
        }

        await orders.SignalAsync("order-1005", new EventSignal("payment.received").WithOverride("Paid", true), store, run);
        clock.Advance(TimeSpan.FromHours(5));

        // docs:begin guides/graphs/event-waits-and-timers/wake-one
        var (chunks, completion) = orders.ResumeStreaming("order-1005", LevelSignals.Due, store, run);
        // docs:end guides/graphs/event-waits-and-timers/wake-one

        var nodes = new List<string>();
        await foreach (var chunk in chunks.ReadAllAsync())
        {
            nodes.Add(chunk.NodeName);
        }

        Assert.True(Assert.IsType<GraphExecutionResult<Order>.Completed>(await completion).State.Shipped);
        Assert.NotEmpty(nodes);
        Assert.All(nodes, node => Assert.Equal("Ship", node));

        // A thread that isn't due yet is refused, with nothing written.
        var early = await orders.ResumeAsync("order-3", LevelSignals.Due, store, run);
        Assert.Equal(
            "thread 'order-3' is not due until 2026-11-03T09:00:00Z: it is waiting for event 'payment.received' at [AwaitPayment]",
            Refusal(early));
    }

    [Fact]
    public void BuildersRefuseBadTimes()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new StateGraph<Order>().AddTimer("AwaitPickup", TimeSpan.Zero));
    }

    private static CompiledGraph<Order> OrderGraph()
    {
        // docs:begin guides/graphs/event-waits-and-timers/graph
        var orders = new StateGraph<Order>()
            .AddNode("Place", o => o)
            .AddWaitForEvent("AwaitPayment", "payment.received", timeout: TimeSpan.FromHours(24))
            .AddNode("Pack", o => o)
            .AddTimer("AwaitPickup", TimeSpan.FromHours(4))
            .AddNode("Ship", o => o with { Shipped = true })
            .AddEdge("Place", "AwaitPayment")
            .AddEdge("AwaitPayment", "Pack")
            .AddEdge("Pack", "AwaitPickup")
            .AddEdge("AwaitPickup", "Ship")
            .Compile();
        // docs:hide
        return orders;
    }
    // docs:show

    sealed record Order(string OrderId, bool Paid, string? PaymentRef, bool Shipped);
    // docs:end guides/graphs/event-waits-and-timers/graph

    // docs:begin guides/graphs/event-waits-and-timers/outcome
    static string Outcome(GraphExecutionResult<Order> result) => result switch
    {
        GraphExecutionResult<Order>.Completed c => $"completed, shipped = {c.State.Shipped}",
        GraphExecutionResult<Order>.Cancelled c => $"cancelled ({c.Reason}) at {c.Node}",
        GraphExecutionResult<Order>.Interrupted i => $"interrupted at {i.SuspendedNode}",
        GraphExecutionResult<Order>.Failed f => $"failed: {f.Message}",
        _ => result.ToString(),
    };
    // docs:end guides/graphs/event-waits-and-timers/outcome

    private static string Refusal(GraphExecutionResult<Order> result) =>
        Assert.IsAssignableFrom<GraphExecutionResult<Order>.Failed>(result).Message;
}

/// <summary>Stands in for <see cref="System.Console"/> in the samples, so a test can check what they print.</summary>
internal static class Console
{
    private static readonly AsyncLocal<List<string>?> Lines = new();

    public static List<string> Capture() => Lines.Value = [];

    public static void WriteLine(string line) => Lines.Value?.Add(line);
}
