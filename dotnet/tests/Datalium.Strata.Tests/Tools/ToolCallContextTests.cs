using System.Text.Json;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Tools;

namespace Datalium.Strata.Tests.Tools;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/tool_context.rs</c> (<c>design/20-Tool-Dispatch-And-Registry.md</c>,
/// SG-21): what a tool call's <see cref="ToolCallContext"/> carries, and how cancellation reaches a tool.
/// Keep the two in step.
/// </summary>
public class ToolCallContextTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Doc(IReadOnlyList<string> Steps);

    private static Doc NewDoc() => new([]);

    private static (ToolRegistry Tools, ContextTools.Probe Probe) Registry()
    {
        var probe = new ContextTools.Probe();
        return (new ToolRegistry().Register(new ContextTools(probe)), probe);
    }

    private static string Result(string json) => JsonDocument.Parse(json).RootElement.GetProperty("result").GetString()!;

    /// <summary>An async node that calls <c>probe</c> as <paramref name="caller"/>.</summary>
    private static Func<Doc, NodeContext, CancellationToken, ValueTask<Doc>> CallsProbe(ToolRegistry tools, string? caller = null) =>
        async (doc, ctx, ct) =>
        {
            await tools.DispatchToolAsync("probe", "{}", ToolCallContext.From(ctx) with { CallerId = caller }, ct);
            return doc;
        };

    private sealed record Marker(string Word);

    private sealed class SingleService(object service) : IServiceProvider
    {
        public object? GetService(Type serviceType) => serviceType.IsInstanceOfType(service) ? service : null;
    }

    [Fact]
    public async Task From_A_Node_The_Context_Carries_The_Run_The_Node_And_The_Services()
    {
        var (tools, probe) = Registry();
        var graph = new StateGraph<Doc>().AddNode("Investigate", CallsProbe(tools, "k8s-investigator")).Compile();

        await graph.RunAsync(
            "thread_tools", NewDoc(), new InMemoryCheckpointStore(),
            new RunOptions { RunId = "run-1", Metadata = { ["tenant"] = "acme" }, Services = new SingleService(new Marker("hi")) });

        var seen = Assert.Single(probe.Seen);
        Assert.Equal("thread_tools", seen.ThreadId);
        Assert.Equal("run-1", seen.RunId);
        Assert.Equal("Investigate", seen.NodePath);
        Assert.Equal(1, seen.Attempt);
        Assert.Equal("acme", seen.Metadata["tenant"]);
        Assert.Equal("k8s-investigator", seen.CallerId);
        Assert.NotNull(seen.CallId);
        Assert.Equal("hi", ((Marker)seen.Services!.GetService(typeof(Marker))!).Word);
    }

    [Fact]
    public async Task Paths_And_Attempts_Are_The_Calling_Nodes_At_Every_Depth()
    {
        var (tools, probe) = Registry();
        var failures = 0;
        var child = new StateGraph<Doc>()
            .AddParallelGroup(
                "Gather",
                new ParallelBranches<Doc>().Branch("Acme", CallsProbe(tools)),
                (entry, _) => entry)
            .Compile();
        var graph = new StateGraph<Doc>()
            .AddSubgraph("Work", child, doc => doc, (_, result) => result)
            .AddFallibleNode("Charge", async (doc, ctx, ct) =>
            {
                await tools.DispatchToolAsync("probe", "{}", ToolCallContext.From(ctx), ct);
                return Interlocked.Increment(ref failures) == 1 ? throw NodeException.Transient("gateway timeout") : doc;
            })
            .SetRecoveryPolicy("Charge", RecoveryPolicy.Retry(1))
            .AddEdge("Work", "Charge")
            .Compile();

        await graph.RunAsync("outer", NewDoc(), new InMemoryCheckpointStore());

        Assert.Equal(
            [("Work/Gather/Acme", 1, "outer"), ("Charge", 1, "outer"), ("Charge", 2, "outer")],
            probe.Seen.Select(s => (s.NodePath, s.Attempt ?? 0, s.ThreadId)).ToList());
    }

    [Fact]
    public async Task A_Call_Id_Is_Generated_When_Missing_And_Kept_When_Given()
    {
        var (tools, probe) = Registry();

        await tools.DispatchToolAsync("probe", "{}", ToolCallContext.None);
        await tools.DispatchToolAsync("probe", "{}", ToolCallContext.None);
        await tools.DispatchToolAsync("probe", "{}", new ToolCallContext { CallId = "call_42" });

        var ids = probe.Seen.Select(s => s.CallId!).ToList();
        Assert.NotEqual(ids[0], ids[1]);
        Assert.Equal(7, Guid.Parse(ids[0]).Version);
        Assert.Equal("call_42", ids[2]);
    }

    [Fact]
    public async Task An_Already_Cancelled_Token_Stops_The_Call_Before_The_Tool_Runs()
    {
        var (tools, probe) = Registry();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => tools.DispatchToolAsync("probe", "{}", ToolCallContext.None, new CancellationToken(canceled: true)));

        Assert.Equal(0, probe.Calls);
    }

    [Fact]
    public async Task A_Tool_That_Throws_For_Its_Cancelled_Token_Is_Cancelled()
    {
        var (tools, probe) = Registry();
        using var cts = new CancellationTokenSource();

        var call = tools.DispatchToolAsync("wait", "{}", ToolCallContext.None, cts.Token);
        await probe.Started.Task.WaitAsync(Patience);
        await cts.CancelAsync();

        var cancelled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call.WaitAsync(Patience));
        Assert.Equal(cts.Token, cancelled.CancellationToken);
    }

    [Fact]
    public async Task Cancelling_The_Run_Reaches_A_Tool_Through_The_Nodes_Token()
    {
        var (tools, probe) = Registry();
        using var cts = new CancellationTokenSource();
        var graph = new StateGraph<Doc>()
            .AddFallibleNode("Investigate", async (doc, ctx, ct) =>
            {
                await tools.DispatchToolAsync("wait", "{}", ToolCallContext.From(ctx), ct);
                return doc;
            })
            .Compile();

        var run = graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore(), cts.Token);
        await probe.Started.Task.WaitAsync(Patience);
        await cts.CancelAsync();
        var result = await run.WaitAsync(Patience);

        Assert.Equal("Investigate", Assert.IsType<GraphExecutionResult<Doc>.Cancelled>(result).Node);
    }

    [Fact]
    public async Task A_Tool_Without_A_Context_Parameter_Runs_Through_Either_Dispatch()
    {
        var (tools, probe) = Registry();

        Assert.Equal("plain", Result(await tools.DispatchToolAsync("plain", "{}")));
        Assert.Equal("plain", Result(await tools.DispatchToolAsync("plain", "{}", new ToolCallContext { CallerId = "a" })));
        // And one that takes a context gets ToolCallContext.None through the two-argument dispatch.
        var direct = new ContextTools(probe);
        Assert.Equal("probed", Result(await direct.DispatchToolAsync("probe", "{}")));
        Assert.Same(ToolCallContext.None, Assert.Single(probe.Seen));
    }

    [Fact]
    public async Task Context_And_Token_Parameters_Are_Passed_In_Any_Position()
    {
        var (tools, _) = Registry();
        using var cts = new CancellationTokenSource();

        var result = await tools.DispatchToolAsync(
            "mixed", """{"sku":"A1","count":3}""", new ToolCallContext { CallerId = "agent" }, cts.Token);

        Assert.Equal("A1 x3 for agent (cancellable)", Result(result));
    }

    [Fact]
    public void A_Context_Built_By_Hand_Carries_What_It_Was_Given()
    {
        var ctx = new ToolCallContext
        {
            ThreadId = "t",
            RunId = "r",
            NodePath = "Work/Send",
            Attempt = 2,
            Metadata = new Dictionary<string, string> { ["tenant"] = "acme" },
            CallerId = "agent",
            CallId = "c",
        };

        Assert.Equal(("t", "r", "Work/Send", 2, "acme", "agent", "c"),
            (ctx.ThreadId, ctx.RunId, ctx.NodePath, ctx.Attempt, ctx.Metadata["tenant"], ctx.CallerId, ctx.CallId));

        var nothing = ToolCallContext.None;
        Assert.Equal<(string?, string?, int?)>((null, null, null), (nothing.ThreadId, nothing.NodePath, nothing.Attempt));
        Assert.Empty(nothing.Metadata);
        Assert.Null(nothing.Services);
    }

    [Fact]
    public void From_A_Hand_Built_Node_Context_The_Run_Id_Is_Absent_Rather_Than_Empty()
    {
        var node = new NodeContext { ThreadId = "t", Node = "Draft", NodePath = "Draft", Attempt = 3 };

        var ctx = ToolCallContext.From(node);

        Assert.Equal(("t", "Draft", 3), (ctx.ThreadId, ctx.NodePath, ctx.Attempt ?? 0));
        Assert.Null(ctx.RunId);
    }
}
