using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Memory;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/node_context.rs</c> (<c>design/17-Async-Execution.md</c>,
/// SG-02): what an async handler's <see cref="NodeContext"/> says, at every depth. Keep the two in step.
/// </summary>
public class NodeContextTests
{
    public sealed record Doc([property: JsonPropertyName("steps")] IReadOnlyList<string> Steps);

    private static Doc NewDoc() => new([]);

    private static Doc Completed(GraphExecutionResult<Doc> result) =>
        Assert.IsType<GraphExecutionResult<Doc>.Completed>(result).State;

    /// <summary>A handler that records the context it was given and passes the state on.</summary>
    private static Func<Doc, NodeContext, CancellationToken, ValueTask<Doc>> Record(ConcurrentQueue<NodeContext> seen) =>
        (doc, ctx, _) =>
        {
            seen.Enqueue(ctx);
            return ValueTask.FromResult(doc);
        };

    [Fact]
    public async Task A_Node_Sees_The_Thread_Its_Own_Name_And_Path()
    {
        var seen = new ConcurrentQueue<NodeContext>();
        var graph = new StateGraph<Doc>().AddNode("Load", Record(seen)).Compile();

        Completed(await graph.RunAsync("thread_ctx", NewDoc(), new InMemoryCheckpointStore()));

        var ctx = Assert.Single(seen);
        Assert.Equal(("thread_ctx", "Load", "Load", 1), (ctx.ThreadId, ctx.Node, ctx.NodePath, ctx.Attempt));
    }

    [Fact]
    public async Task The_Attempt_Counts_Retries_From_One()
    {
        var attempts = new ConcurrentQueue<int>();
        var graph = new StateGraph<Doc>()
            .AddFallibleNode("Charge", (doc, ctx, _) =>
            {
                attempts.Enqueue(ctx.Attempt);
                return ctx.Attempt < 3
                    ? ValueTask.FromException<Doc>(NodeException.Transient("timeout"))
                    : ValueTask.FromResult(doc);
            })
            .SetRecoveryPolicy("Charge", RecoveryPolicy.Retry(2))
            .Compile();

        Completed(await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));

        Assert.Equal([1, 2, 3], attempts);
    }

    [Fact]
    public async Task Paths_Qualify_Subgraph_Nodes_And_Branches_And_The_Thread_Is_The_Outermost()
    {
        var seen = new ConcurrentQueue<NodeContext>();
        var child = new StateGraph<Doc>()
            .AddNode("Draft", Record(seen))
            .AddParallelGroup("Gather", new ParallelBranches<Doc>().Branch("Acme", Record(seen)), (entry, _) => entry)
            .AddEdge("Draft", "Gather")
            .Compile();
        var graph = new StateGraph<Doc>()
            .AddNode("Prepare", Record(seen))
            .AddSubgraph("Work", child, doc => doc, (_, result) => result)
            .AddParallelGroup("Quotes", new ParallelBranches<Doc>().Branch("Globex", Record(seen)), (entry, _) => entry)
            .AddEdge("Prepare", "Work")
            .AddEdge("Work", "Quotes")
            .Compile();

        Completed(await graph.RunAsync("thread_outer", NewDoc(), new InMemoryCheckpointStore()));

        Assert.Equal(
            [
                ("Prepare", "Prepare"),
                ("Draft", "Work/Draft"),
                ("Acme", "Work/Gather/Acme"),
                ("Globex", "Quotes/Globex"),
            ],
            seen.Select(c => (c.Node, c.NodePath)));
        Assert.All(seen, c => Assert.Equal("thread_outer", c.ThreadId));
    }

    [Fact]
    public async Task A_Branch_Reports_Its_Groups_Attempt()
    {
        var branchAttempts = new ConcurrentQueue<int>();
        var groupAttempts = 0;
        var branches = new ParallelBranches<Doc>()
            .Branch("Acme", (doc, ctx, _) =>
            {
                branchAttempts.Enqueue(ctx.Attempt);
                return ValueTask.FromResult(doc);
            })
            .Branch("Flaky", doc => Interlocked.Increment(ref groupAttempts) == 1
                ? throw NodeException.Transient("timeout")
                : doc);
        var graph = new StateGraph<Doc>()
            .AddParallelGroup("Gather", branches, (entry, _) => entry)
            .SetRecoveryPolicy("Gather", RecoveryPolicy.Retry(1))
            .Compile();

        Completed(await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));

        // The first attempt's Acme may have been cancelled before it ran; the second always runs.
        Assert.Contains(2, branchAttempts);
        Assert.DoesNotContain(branchAttempts, a => a is not 1 and not 2);
    }

    [Fact]
    public async Task History_And_Services_Come_From_RunOptions_At_Every_Depth()
    {
        var history = new InMemoryHistoryStore();
        var services = new SingleService(new Greeter("hello"));
        var seen = new ConcurrentQueue<NodeContext>();
        var child = new StateGraph<Doc>().AddNode("Inner", Record(seen)).Compile();
        var graph = new StateGraph<Doc>()
            .AddNode("Outer", Record(seen))
            .AddSubgraph("Work", child, doc => doc, (_, result) => result)
            .AddEdge("Outer", "Work")
            .Compile();

        Completed(await graph.RunAsync(
            "t", NewDoc(), new InMemoryCheckpointStore(), new RunOptions { History = history, Services = services }));

        Assert.Equal(2, seen.Count);
        Assert.All(seen, c =>
        {
            Assert.Same(history, c.History);
            Assert.Same(history, ((IExecutionContext)c).History);
            Assert.Equal("hello", ((Greeter)c.Services!.GetService(typeof(Greeter))!).Word);
        });
    }

    [Fact]
    public async Task Without_A_History_Store_The_IExecutionContext_View_Says_So()
    {
        var seen = new ConcurrentQueue<NodeContext>();
        var graph = new StateGraph<Doc>().AddNode("Load", Record(seen)).Compile();

        Completed(await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));

        var ctx = Assert.Single(seen);
        Assert.Null(ctx.History);
        Assert.Null(ctx.Services);
        var ex = Assert.Throws<InvalidOperationException>(() => ((IExecutionContext)ctx).History);
        Assert.Contains("RunOptions.History", ex.Message);
    }

    [Fact]
    public async Task The_Stream_Is_Present_Exactly_When_The_Run_Streams()
    {
        var seen = new ConcurrentQueue<NodeContext>();
        var graph = new StateGraph<Doc>().AddNode("Load", Record(seen)).Compile();

        Completed(await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));
        var (reader, completion) = graph.RunStreaming("t", NewDoc(), new InMemoryCheckpointStore());
        await foreach (var _ in reader.ReadAllAsync())
        {
        }

        Completed(await completion);
        Assert.Equal([false, true], seen.Select(c => c.Stream is not null));
    }

    [Fact]
    public void A_Handler_Can_Be_Unit_Tested_With_A_Context_Built_By_Hand()
    {
        var ctx = new NodeContext { ThreadId = "t", Node = "Draft", NodePath = "Draft" };

        Assert.Equal(1, ctx.Attempt);
        Assert.Null(ctx.Stream);
    }

    private sealed record Greeter(string Word);

    private sealed class SingleService(object service) : IServiceProvider
    {
        public object? GetService(Type serviceType) => serviceType == service.GetType() ? service : null;
    }
}
