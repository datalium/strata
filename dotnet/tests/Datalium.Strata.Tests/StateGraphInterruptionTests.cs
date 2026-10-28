using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/src/checkpoint.rs</c>'s test module: interrupt a graph, confirm
/// the checkpoint was flushed to the store, resume with injected approval data, confirm the
/// final state matches pre-interruption plus the injected override.
/// </summary>
public class StateGraphInterruptionTests
{
    /// <summary>The STRATA-SPEC-002 wire-transfer example: freeze immediately after preparing the transfer, resume (with human approval) to execute it.</summary>
    public sealed record WireTransferState(
        [property: JsonPropertyName("invoice_generated")] bool InvoiceGenerated,
        [property: JsonPropertyName("amount")] double Amount,
        [property: JsonPropertyName("currency")] string Currency,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy,
        [property: JsonPropertyName("is_completed")] bool IsCompleted);

    internal static CompiledGraph<WireTransferState> WireTransferGraph() =>
        new StateGraph<WireTransferState>()
            .AddNode("PrepareWireTransfer", s => s with { InvoiceGenerated = true })
            .AddInterruptBoundary("PrepareWireTransfer")
            .AddNode("ExecuteWireTransfer", s => s with { IsCompleted = true })
            .AddEdge("PrepareWireTransfer", "ExecuteWireTransfer")
            .Compile();

    internal static WireTransferState InitialState() => new(
        InvoiceGenerated: false,
        Amount: 4200.5,
        Currency: "USD",
        ApprovedBy: null,
        IsCompleted: false);

    [Fact]
    public async Task Run_Freezes_At_The_Interrupt_Boundary_And_Flushes_To_The_Store()
    {
        var graph = WireTransferGraph();
        var store = new InMemoryCheckpointStore();

        var result = await graph.RunAsync("thread_hitl_demo", InitialState(), store);

        var interrupted = Assert.IsType<GraphExecutionResult<WireTransferState>.Interrupted>(result);
        Assert.Equal("thread_hitl_demo", interrupted.ThreadId);
        Assert.Equal("PrepareWireTransfer", interrupted.SuspendedNode);
        Assert.Equal("ExecuteWireTransfer", interrupted.NextNode);
        Assert.True(interrupted.FrozenState.InvoiceGenerated);
        Assert.False(interrupted.FrozenState.IsCompleted);

        // FLUSH: the checkpoint must actually be in the store, independent of the in-process
        // return value asserted above.
        var persisted = store.Load("thread_hitl_demo");
        Assert.NotNull(persisted);
        Assert.Equal(CheckpointStatus.Interrupted, persisted.Status);
        Assert.Equal("PrepareWireTransfer", persisted.SuspendedNode);
        Assert.Equal("ExecuteWireTransfer", persisted.NextNode);
        Assert.Null(persisted.ResumedAt);
    }

    [Fact]
    public async Task Resume_Injects_Field_Overrides_And_Completes_The_Graph()
    {
        var graph = WireTransferGraph();
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync("thread_hitl_demo", InitialState(), store);

        var signal = new HumanSignal(
            IsApproved: true,
            ReviewerToken: "token-abc",
            FieldOverrides: new Dictionary<string, string> { ["approved_by"] = "admin_01" });

        var result = await graph.ResumeAsync("thread_hitl_demo", signal, store);

        var completed = Assert.IsType<GraphExecutionResult<WireTransferState>.Completed>(result);
        Assert.Equal(
            new WireTransferState(
                InvoiceGenerated: true,
                Amount: 4200.5,
                Currency: "USD",
                ApprovedBy: "admin_01",
                IsCompleted: true),
            completed.State);

        // The checkpoint stays in the store (audit trail) but is now marked resumed.
        var persisted = store.Load("thread_hitl_demo");
        Assert.NotNull(persisted?.ResumedAt);
    }

    [Fact]
    public async Task Resume_Without_A_Checkpoint_Fails()
    {
        var graph = WireTransferGraph();
        var store = new InMemoryCheckpointStore();

        var result = await graph.ResumeAsync("does-not-exist", new HumanSignal(false, ""), store);
        Assert.IsType<GraphExecutionResult<WireTransferState>.Failed>(result);
    }

    [Fact]
    public async Task Resume_Of_An_Already_Resumed_Checkpoint_Fails()
    {
        var graph = WireTransferGraph();
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync("thread_hitl_demo", InitialState(), store);
        await graph.ResumeAsync("thread_hitl_demo", new HumanSignal(false, ""), store);

        var result = await graph.ResumeAsync("thread_hitl_demo", new HumanSignal(false, ""), store);
        Assert.IsType<GraphExecutionResult<WireTransferState>.Failed>(result);
    }

    public sealed record RoutingState(int N, List<string> Path);

    private static CompiledGraph<RoutingState> RoutingGraph() =>
        new StateGraph<RoutingState>()
            .AddNode("Classify", s => s with { Path = [.. s.Path, "Classify"] })
            .AddNode("EvenPath", s => s with { Path = [.. s.Path, "EvenPath"] })
            .AddNode("OddPath", s => s with { Path = [.. s.Path, "OddPath"] })
            // The conditional edge is the "if"; the plain edge added after it is the "else" --
            // NextNodeAfter tries them in this order.
            .AddConditionalEdge("Classify", "EvenPath", s => s.N % 2 == 0)
            .AddEdge("Classify", "OddPath")
            .Compile();

    [Fact]
    public async Task Conditional_Edge_Routes_Based_On_Runtime_State()
    {
        var graph = RoutingGraph();
        var store = new InMemoryCheckpointStore();

        var even = await graph.RunAsync("even", new RoutingState(4, []), store);
        var evenCompleted = Assert.IsType<GraphExecutionResult<RoutingState>.Completed>(even);
        Assert.Equal(["Classify", "EvenPath"], evenCompleted.State.Path);

        var odd = await graph.RunAsync("odd", new RoutingState(3, []), store);
        var oddCompleted = Assert.IsType<GraphExecutionResult<RoutingState>.Completed>(odd);
        Assert.Equal(["Classify", "OddPath"], oddCompleted.State.Path);
    }

    [Fact]
    public async Task No_Matching_Conditional_Edge_Fails_At_Runtime()
    {
        var graph = new StateGraph<int>()
            .AddNode("A", s => s)
            .AddNode("B", s => s)
            .AddConditionalEdge("A", "B", _ => false)
            .Compile();

        var store = new InMemoryCheckpointStore();
        var result = await graph.RunAsync("thread", 1, store);

        var failed = Assert.IsType<GraphExecutionResult<int>.Failed>(result);
        Assert.Contains("A", failed.Message);
    }

    public sealed record RetryState(int Attempts);

    [Fact]
    public async Task Guarded_Cycle_Terminates_Via_Its_Condition()
    {
        var graph = new StateGraph<RetryState>()
            .AddNode("Attempt", s => s with { Attempts = s.Attempts + 1 })
            .AddNode("Done", s => s)
            .AddConditionalEdge("Attempt", "Attempt", s => s.Attempts < 3)
            .AddEdge("Attempt", "Done")
            .Compile();

        var store = new InMemoryCheckpointStore();
        var result = await graph.RunAsync("retry", new RetryState(0), store);

        var completed = Assert.IsType<GraphExecutionResult<RetryState>.Completed>(result);
        Assert.Equal(3, completed.State.Attempts);
    }

    public sealed record GreetingState(string Name, bool Greeted);

    private static CompiledGraph<GreetingState> GreetingGraph() =>
        new StateGraph<GreetingState>()
            .AddStreamingNode("Greet", (s, ctx) =>
            {
                ctx.EmitToken("Hello, ");
                ctx.EmitToken(s.Name);
                return s with { Greeted = true };
            })
            .AddNode("Done", s => s)
            .AddEdge("Greet", "Done")
            .Compile();

    [Fact]
    public async Task RunStreaming_Emits_Tokens_And_Lifecycle_Chunks_And_Completes()
    {
        var graph = GreetingGraph();
        var store = new InMemoryCheckpointStore();

        var (reader, completion) = graph.RunStreaming("thread_stream", new GreetingState("World", false), store);

        var chunks = new List<StreamChunk>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            chunks.Add(chunk);
        }

        var result = await completion;
        var completed = Assert.IsType<GraphExecutionResult<GreetingState>.Completed>(result);
        Assert.True(completed.State.Greeted);
        Assert.Equal("World", completed.State.Name);

        // NodeStart/NodeEnd surround both nodes automatically; only the streaming node ("Greet")
        // also emits its own Token chunks, in the order it called EmitToken.
        var summary = chunks.Select(c => (c.Type, Payload: c.Payload.ToString())).ToList();
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Greet"),
                (StreamChunkType.Token, "Hello, "),
                (StreamChunkType.Token, "World"),
                (StreamChunkType.NodeEnd, "Greet"),
                (StreamChunkType.NodeStart, "Done"),
                (StreamChunkType.NodeEnd, "Done"),
            ],
            summary);
        Assert.All(chunks, c => Assert.True(c.NodeName is "Greet" or "Done"));
    }

    [Fact]
    public async Task Plain_RunAsync_Of_A_Graph_With_A_Streaming_Node_Fails_At_That_Node()
    {
        var graph = GreetingGraph();
        var store = new InMemoryCheckpointStore();

        var result = await graph.RunAsync("thread_no_stream", new GreetingState("World", false), store);

        var failed = Assert.IsType<GraphExecutionResult<GreetingState>.Failed>(result);
        Assert.Contains("Greet", failed.Message);
    }
}
