using System.Diagnostics;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/async_execution.rs</c> (<c>design/17-Async-Execution.md</c>,
/// SG-01): every handler kind has an async form that is awaited, not blocked on; sync and async
/// handlers mix freely; async branches don't hold a thread each. Keep the two in step.
/// </summary>
public class AsyncExecutionTests
{
    public sealed record Doc(
        [property: JsonPropertyName("text")] string Text,
        [property: JsonPropertyName("steps")] IReadOnlyList<string> Steps);

    private static Doc NewDoc() => new("", []);

    private static Doc Did(Doc doc, string step) => doc with { Steps = [.. doc.Steps, step] };

    private static Doc Completed(GraphExecutionResult<Doc> result) =>
        Assert.IsType<GraphExecutionResult<Doc>.Completed>(result).State;

    [Fact]
    public async Task Async_Plain_And_Fallible_Nodes_Are_Awaited_And_Mix_With_Sync_Ones()
    {
        var attempts = 0;
        var graph = new StateGraph<Doc>()
            .AddNode("Load", async (doc, _, ct) =>
            {
                await Task.Delay(10, ct);
                return Did(doc, "load");
            })
            .AddNode("Sync", doc => Did(doc, "sync"))
            .AddFallibleNode("Charge", async (doc, _, ct) =>
            {
                await Task.Yield();
                return ++attempts == 1 ? throw NodeException.Transient("gateway timeout") : Did(doc, "charge");
            })
            .SetRecoveryPolicy("Charge", RecoveryPolicy.Retry(1))
            .AddEdge("Load", "Sync")
            .AddEdge("Sync", "Charge")
            .Compile();

        var doc = Completed(await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));

        Assert.Equal(["load", "sync", "charge"], doc.Steps);
        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task An_Async_Fallible_Node_That_Gives_Up_Persists_A_Failed_Checkpoint()
    {
        var graph = new StateGraph<Doc>()
            .AddNode("Load", doc => Did(doc, "load"))
            .AddFallibleNode("Charge", async (_, _, _) =>
            {
                await Task.Yield();
                throw NodeException.Fatal("card declined");
            })
            .AddEdge("Load", "Charge")
            .Compile();
        var store = new InMemoryCheckpointStore();

        var failed = Assert.IsType<GraphExecutionResult<Doc>.NodeFailed>(await graph.RunAsync("t", NewDoc(), store));

        Assert.Equal(("Charge", 1, "card declined"), (failed.Node, failed.Attempts, failed.Message));
        var checkpoint = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Failed, checkpoint.Status);
        Assert.Equal(CheckpointSchema.Version, checkpoint.SchemaVersion);
        Assert.Equal("fatal", checkpoint.Metadata!["error_kind"]);
    }

    [Fact]
    public async Task An_Async_Streaming_Node_Emits_Through_Its_Context()
    {
        var graph = new StateGraph<Doc>()
            .AddStreamingNode("Draft", async (doc, ctx, ct) =>
            {
                foreach (var word in new[] { "hello", " ", "world" })
                {
                    await ctx.Stream!.EmitTokenAsync(word, ct);
                }

                return doc with { Text = "hello world" };
            })
            .Compile();

        var (reader, completion) = graph.RunStreaming("t", NewDoc(), new InMemoryCheckpointStore());
        var chunks = new List<(StreamChunkType, string, string)>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            chunks.Add((chunk.Type, chunk.NodeName, chunk.Payload.ToString()));
        }

        Assert.Equal("hello world", Completed(await completion).Text);
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Draft", "Draft"),
                (StreamChunkType.Token, "Draft", "hello"),
                (StreamChunkType.Token, "Draft", " "),
                (StreamChunkType.Token, "Draft", "world"),
                (StreamChunkType.NodeEnd, "Draft", "Draft"),
            ],
            chunks);
    }

    [Fact]
    public async Task A_Plain_Run_Refuses_An_Async_Streaming_Node_Before_Running_It()
    {
        var ran = false;
        var graph = new StateGraph<Doc>()
            .AddStreamingNode("Draft", (doc, _, _) =>
            {
                ran = true;
                return ValueTask.FromResult(doc);
            })
            .Compile();

        var failed = Assert.IsType<GraphExecutionResult<Doc>.Failed>(
            await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));

        Assert.Contains("is a streaming node", failed.Message);
        Assert.False(ran);
    }

    [Fact]
    public async Task Sync_And_Async_Branches_Mix_In_One_Group_And_Merge_In_Declaration_Order()
    {
        var branches = new ParallelBranches<Doc>()
            .Branch("slow-async", async (doc, _, ct) =>
            {
                await Task.Delay(120, ct);
                return Did(doc, "slow-async");
            })
            .Branch("sync", doc => Did(doc, "sync"))
            .Branch("fast-async", (doc, _, _) => ValueTask.FromResult(Did(doc, "fast-async")));
        var graph = new StateGraph<Doc>()
            .AddParallelGroup("Gather", branches, (entry, outputs) => entry with
            {
                Steps = [.. outputs.SelectMany(o => o.Steps)],
            })
            .Compile();

        var doc = Completed(await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));

        Assert.Equal(["slow-async", "sync", "fast-async"], doc.Steps);
    }

    [Fact]
    public async Task Async_Branches_Do_Not_Hold_A_Thread_Each()
    {
        const int runs = 200;
        const int branchesPerRun = 5;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allWaiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var waiting = 0;

        var branches = new ParallelBranches<Doc>();
        for (var i = 0; i < branchesPerRun; i++)
        {
            branches.Branch($"b{i}", async (doc, _, ct) =>
            {
                if (Interlocked.Increment(ref waiting) == runs * branchesPerRun)
                {
                    allWaiting.SetResult();
                }

                await gate.Task.WaitAsync(ct);
                return doc;
            });
        }

        var graph = new StateGraph<Doc>().AddParallelGroup("Gather", branches, (entry, _) => entry).Compile();
        var store = new InMemoryCheckpointStore();
        var all = Enumerable.Range(0, runs).Select(i => graph.RunAsync($"t{i}", NewDoc(), store)).ToArray();

        // Every branch of every run is in flight at once...
        await allWaiting.Task.WaitAsync(TimeSpan.FromSeconds(30));
        // ...with nowhere near a thread each (a thread per branch would be 1000).
        Assert.True(
            Process.GetCurrentProcess().Threads.Count < 500,
            $"{Process.GetCurrentProcess().Threads.Count} threads for {runs * branchesPerRun} waiting branches");

        gate.SetResult();
        foreach (var result in await Task.WhenAll(all))
        {
            Completed(result);
        }
    }

    [Fact]
    public async Task An_Async_Streaming_Branch_Streams_As_Group_Slash_Branch()
    {
        var branches = new ParallelBranches<Doc>()
            .StreamingBranch("Acme", async (doc, ctx, ct) =>
            {
                await ctx.Stream!.EmitTokenAsync("quote", ct);
                return doc;
            });
        var graph = new StateGraph<Doc>().AddParallelGroup("Gather", branches, (entry, _) => entry).Compile();

        var (reader, completion) = graph.RunStreaming("t", NewDoc(), new InMemoryCheckpointStore());
        var tokens = new List<(string, string)>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            if (chunk.Type == StreamChunkType.Token)
            {
                tokens.Add((chunk.NodeName, chunk.Payload.ToString()));
            }
        }

        Completed(await completion);
        Assert.Equal([("Gather/Acme", "quote")], tokens);
    }

    [Fact]
    public async Task A_NodeException_From_An_Async_Plain_Node_Is_Not_Recovery_Managed()
    {
        var graph = new StateGraph<Doc>()
            .AddNode("Load", async (_, _, _) =>
            {
                await Task.Yield();
                throw NodeException.Transient("not mine to retry");
            })
            .Compile();

        var ex = await Assert.ThrowsAsync<NodeException>(
            () => graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));

        Assert.Equal("not mine to retry", ex.Message);
    }

    [Fact]
    public async Task An_OperationCanceledException_The_Engine_Did_Not_Cause_Is_A_Bug()
    {
        // HttpClient's own timeout looks like this: a cancellation nobody asked the engine for.
        var graph = new StateGraph<Doc>()
            .AddFallibleNode("Fetch", async (_, _, _) =>
            {
                await Task.Yield();
                throw new TaskCanceledException("the request timed out");
            })
            .SetRecoveryPolicy("Fetch", RecoveryPolicy.Retry(3))
            .Compile();

        await Assert.ThrowsAsync<TaskCanceledException>(
            () => graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));
    }

    [Fact]
    public async Task Async_Nodes_Run_Inside_Subgraphs_At_Any_Depth()
    {
        var grandchild = new StateGraph<Doc>()
            .AddNode("Deep", async (doc, _, ct) =>
            {
                await Task.Delay(5, ct);
                return Did(doc, "deep");
            })
            .Compile();
        var child = new StateGraph<Doc>()
            .AddNode("Before", async (doc, _, _) =>
            {
                await Task.Yield();
                return Did(doc, "before");
            })
            .AddSubgraph("Inner", grandchild, doc => doc, (_, result) => result)
            .AddEdge("Before", "Inner")
            .Compile();
        var graph = new StateGraph<Doc>()
            .AddSubgraph("Work", child, doc => doc, (_, result) => result)
            .Compile();

        var doc = Completed(await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));

        Assert.Equal(["before", "deep"], doc.Steps);
    }

    [Fact]
    public async Task An_Interrupt_Boundary_On_An_Async_Node_Freezes_And_Resumes()
    {
        var graph = new StateGraph<Doc>()
            .AddNode("Draft", async (doc, _, _) =>
            {
                await Task.Yield();
                return Did(doc, "draft");
            })
            .AddInterruptBoundary("Draft")
            .AddNode("Send", async (doc, _, _) =>
            {
                await Task.Yield();
                return Did(doc, "send");
            })
            .AddEdge("Draft", "Send")
            .Compile();
        var store = new InMemoryCheckpointStore();

        Assert.IsType<GraphExecutionResult<Doc>.Interrupted>(await graph.RunAsync("t", NewDoc(), store));
        var doc = Completed(await graph.ResumeAsync("t", new HumanSignal(true, "x"), store));

        Assert.Equal(["draft", "send"], doc.Steps);
    }
}
