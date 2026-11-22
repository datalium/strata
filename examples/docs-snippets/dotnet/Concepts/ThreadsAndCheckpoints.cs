// Samples for https://stratamesh.dev/concepts/threads-and-checkpoints/

using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

namespace Datalium.Strata.DocsSnippets.Concepts.ThreadsAndCheckpoints;

public class ThreadsAndCheckpointsSamples
{
    [Fact]
    public async Task Query()
    {
        var graph = new StateGraph<Order>()
            .AddNode("Review", state => state)
            .AddInterruptBoundary("Review")
            .AddNode("Ship", state => state)
            .AddEdge("Review", "Ship")
            .Compile();
        var store = new InMemoryCheckpointStore();
        Assert.IsType<GraphExecutionResult<Order>.Interrupted>(await graph.RunAsync("order-1001", new Order(false), store));

        // docs:begin concepts/threads-and-checkpoints/query
        var page = await store.ListThreadsAsync(new ThreadQuery
        {
            States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Interrupted },
        });
        foreach (var thread in page.Threads)
        {
            Console.WriteLine($"{thread.ThreadId} waits at {thread.SuspendedNode}");
        }

        var log = await store.GetLogAsync("order-1001");
        foreach (var entry in log.Entries)
        {
            Console.WriteLine($"#{entry.Sequence} {entry.Checkpoint.Status}");
        }
        // docs:end concepts/threads-and-checkpoints/query

        var waiting = Assert.Single(page.Threads);
        Assert.Equal("order-1001", waiting.ThreadId);
        Assert.Equal("Review", waiting.SuspendedNode);
        Assert.Equal(CheckpointStatus.Interrupted, Assert.Single(log.Entries).Checkpoint.Status);
    }
}

internal sealed record Order(bool Approved);
