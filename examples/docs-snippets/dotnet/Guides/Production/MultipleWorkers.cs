// Samples for https://stratamesh.dev/guides/production/multiple-workers/

using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Scheduling;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Memory;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.DocsSnippets.Guides.Production.MultipleWorkers;

public class MultipleWorkersSamples
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(3);
    private static readonly DateTimeOffset Noon = new(2026, 10, 24, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Handle()
    {
        Console.Lines.Clear();

        // Held: worker-a is in Draft on invoice-7, and worker-b gets the same thread. The store and worker-a's
        // run share a clock, so worker-a's renewals move its lease's expiry.
        var clock = new FakeTimeProvider(Noon);
        var store = new InMemoryCheckpointStore(clock);
        var gate = new Gate();
        var a = new Worker("worker-a", Ttl, clock, store, gate);
        var b = new Worker("worker-b", Ttl, new FakeTimeProvider(Noon), store);
        var aRun = a.HandleAsync("invoice-7");
        await gate.Started;
        await b.HandleAsync("invoice-7");
        gate.Release();
        await aRun;
        Assert.Equal(
            [
                "worker-b: skipped invoice-7: thread 'invoice-7' is held by 'worker-a' until 2026-10-24T12:00:03Z",
                "worker-a: invoice-7 waits at Draft",
            ],
            Console.Take());

        // Lost: worker-a stalls in Draft on invoice-8 on a clock that stands still, so it never renews. The
        // store's clock moves past the ttl, worker-b takes the thread, and worker-a's save is refused.
        var storeClock = new FakeTimeProvider(Noon);
        store = new InMemoryCheckpointStore(storeClock);
        gate = new Gate();
        a = new Worker("worker-a", Ttl, new FakeTimeProvider(Noon), store, gate);
        b = new Worker("worker-b", Ttl, new FakeTimeProvider(Noon), store);
        aRun = a.HandleAsync("invoice-8");
        await gate.Started;
        storeClock.Advance(Ttl + TimeSpan.FromSeconds(1));
        await b.HandleAsync("invoice-8");
        gate.Release();
        await aRun;
        Assert.Equal(
            [
                "worker-b: invoice-8 waits at Draft",
                "worker-a: lost the lease on invoice-8 in Draft",
            ],
            Console.Take());
        var latest = store.Load("invoice-8");
        Assert.Equal("Draft", latest?.SuspendedNode);
        Assert.Equal("worker-b", latest?.FrozenState?["drafted_by"]?.GetValue<string>());
    }

    // docs:begin guides/production/multiple-workers/show-running
    static async Task ShowRunningAsync(EfCoreCheckpointStore store)
    {
        var query = new ThreadQuery
        {
            States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Running },
            GraphId = "invoices",
        };

        do
        {
            var page = await store.ListThreadsAsync(query);
            foreach (var thread in page.Threads)
            {
                var lease = await store.GetLeaseAsync(thread.ThreadId);
                Console.WriteLine(lease is null
                    ? $"{thread.ThreadId}: running, no live lease"
                    : $"{thread.ThreadId}: running, held by '{lease.Owner}' until {lease.ExpiresAt:HH:mm:ss} (token {lease.Token})");
            }

            query = query with { Cursor = page.NextCursor };
        }
        while (query.Cursor is not null);
    }
    // docs:end guides/production/multiple-workers/show-running

    [Fact]
    public async Task ShowRunning()
    {
        Console.Lines.Clear();
        using var store = EfCoreCheckpointStore.CreateInMemory();

        // invoice-7 is in Draft under worker-a's lease, invoice-9 in Draft with no lease, invoice-10 waits for
        // approval, and receipt-1 is a running thread of another graph.
        var held = new Gate();
        var unleased = new Gate();
        var other = new Gate();
        var leased = new RunOptions { Lease = new LeaseOptions("worker-a", TimeSpan.FromSeconds(30)) };
        var runs = new List<Task<GraphExecutionResult<Invoice>>>
        {
            Worker.Invoices("worker-a", held).RunAsync("invoice-7", new Invoice("invoice-7", null, false), store, leased),
        };
        await held.Started;
        runs.Add(Worker.Invoices("worker-a", unleased).RunAsync("invoice-9", new Invoice("invoice-9", null, false), store));
        await unleased.Started;
        var waiting = await Worker.Invoices("worker-b").RunAsync("invoice-10", new Invoice("invoice-10", null, false), store, leased);
        Assert.IsType<GraphExecutionResult<Invoice>.Interrupted>(waiting);
        var receipt = new StateGraph<Invoice>()
            .AddNode("Draft", async (invoice, _, _) =>
            {
                await other.HoldAsync();
                return invoice;
            })
            .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode, GraphId = "receipts" })
            .RunAsync("receipt-1", new Invoice("receipt-1", null, false), store);
        await other.Started;

        await ShowRunningAsync(store);

        var lines = Console.Take().Order(StringComparer.Ordinal).ToList();
        Assert.Equal(2, lines.Count);
        Assert.StartsWith("invoice-7: running, held by 'worker-a' until ", lines[0]);
        Assert.EndsWith(" (token 1)", lines[0]);
        Assert.Equal("invoice-9: running, no live lease", lines[1]);

        held.Release();
        unleased.Release();
        other.Release();
        foreach (var run in runs)
        {
            Assert.IsType<GraphExecutionResult<Invoice>.Interrupted>(await run);
        }

        await receipt;
    }

    [Fact]
    public async Task Scheduler()
    {
        // docs:begin guides/production/multiple-workers/scheduler
        var scheduler = new RunScheduler(new RunSchedulerOptions
        {
            MaxConcurrent = 8,
            TenantLimit = 2,
            Reservations = [new SlotReservation(MinPriority: 5, Slots: 2)],
        });

        var options = new RunOptions
        {
            Lease = new LeaseOptions("worker-a", TimeSpan.FromSeconds(30)),
            Scheduler = scheduler,
            Priority = 5,
            Metadata = { ["tenant"] = "acme" },
        };
        // docs:end guides/production/multiple-workers/scheduler

        // Three of acme's runs at once: two hold slots, and the third waits for one.
        var store = new InMemoryCheckpointStore();
        var open = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var graph = new StateGraph<Invoice>()
            .AddNode("Draft", async (invoice, _, _) =>
            {
                await open.Task;
                return invoice;
            })
            .Compile();
        var runs = Enumerable.Range(1, 3)
            .Select(n => graph.RunAsync($"scheduled-{n}", new Invoice($"scheduled-{n}", null, false), store, options))
            .ToList();

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        var snapshot = scheduler.Snapshot();
        while (snapshot.Active + snapshot.Queued < 3)
        {
            Assert.True(DateTime.UtcNow < deadline, $"{snapshot}");
            await Task.Delay(5);
            snapshot = scheduler.Snapshot();
        }

        Assert.Equal((8, 2, 1), (snapshot.MaxConcurrent, snapshot.Active, snapshot.Queued));
        Assert.Equal((2, 2), (snapshot.Tenants["acme"].Active, snapshot.Tenants["acme"].Limit));

        open.SetResult();
        foreach (var run in runs)
        {
            Assert.IsType<GraphExecutionResult<Invoice>.Completed>(await run);
        }
    }
}

internal sealed record Invoice(
    [property: JsonPropertyName("invoice_id")] string InvoiceId,
    [property: JsonPropertyName("drafted_by")] string? DraftedBy,
    [property: JsonPropertyName("sent")] bool Sent);

/// <summary>
/// The samples print what they find. In this namespace, <c>Console.WriteLine</c> is this class: it keeps the
/// lines, so the tests can check them.
/// </summary>
internal static class Console
{
    public static ConcurrentQueue<string> Lines { get; } = new();

    public static void WriteLine(string line) => Lines.Enqueue(line);

    public static List<string> Take()
    {
        var lines = new List<string>();
        while (Lines.TryDequeue(out var line))
        {
            lines.Add(line);
        }

        return lines;
    }
}

/// <summary>Holds Draft until released, ignoring its token like a paused process.</summary>
internal sealed class Gate
{
    private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Started => _started.Task;

    public async Task HoldAsync()
    {
        _started.TrySetResult();
        await _release.Task;
    }

    public void Release() => _release.TrySetResult();
}

/// <summary>One worker process: its name, its lease's ttl, the clock its renewals run on, and the shared store.</summary>
internal sealed class Worker(string owner, TimeSpan ttl, TimeProvider clock, ICheckpointStore store, Gate? gate = null)
{
    private readonly string _owner = owner;
    private readonly TimeSpan _ttl = ttl;
    private readonly TimeProvider _clock = clock;

    public CompiledGraph<Invoice> Graph { get; } = Invoices(owner, gate);

    public ICheckpointStore Store { get; } = store;

    /// <summary>Draft -> (wait for approval) -> Send, with <c>EveryNode</c> and the graph id <c>invoices</c>.</summary>
    public static CompiledGraph<Invoice> Invoices(string owner, Gate? gate = null) =>
        new StateGraph<Invoice>()
            .AddNode("Draft", async (invoice, _, _) =>
            {
                if (gate is not null)
                {
                    await gate.HoldAsync();
                }

                return invoice with { DraftedBy = owner };
            })
            .AddInterruptBoundary("Draft")
            .AddNode("Send", invoice => invoice with { Sent = true })
            .AddEdge("Draft", "Send")
            .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode, GraphId = "invoices" });

    // docs:begin guides/production/multiple-workers/handle
    public async Task HandleAsync(string threadId)
    {
        var options = new RunOptions
        {
            Lease = new LeaseOptions(_owner, _ttl),
            // docs:hide ...
            TimeProvider = _clock,
            // docs:show
        };

        var result = await Graph.RunAsync(threadId, new Invoice(threadId, null, false), Store, options);

        switch (result)
        {
            case GraphExecutionResult<Invoice>.Failed { Message: var why } when why.Contains(" is held by "):
                // Another worker runs this thread. Nothing was written.
                Console.WriteLine($"{_owner}: skipped {threadId}: {why}");
                break;

            case GraphExecutionResult<Invoice>.Cancelled { Reason: CancelReason.LeaseLost } lost:
                // Another worker took the thread over. This run wrote nothing more.
                Console.WriteLine($"{_owner}: lost the lease on {threadId} in {lost.Node}");
                break;

            case GraphExecutionResult<Invoice>.Interrupted waiting:
                Console.WriteLine($"{_owner}: {threadId} waits at {waiting.SuspendedNode}");
                break;

            default:
                Console.WriteLine($"{_owner}: {threadId} ended {result.GetType().Name}");
                break;
        }
    }
    // docs:end guides/production/multiple-workers/handle
}
