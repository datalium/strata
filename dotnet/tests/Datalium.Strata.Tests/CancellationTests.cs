using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/cancellation.rs</c> (<c>design/17-Async-Execution.md</c>,
/// SG-03): a cancelled run (token or deadline) ends <c>Cancelled</c> with a <c>cancelled</c>
/// checkpoint that a resume re-enters; a node timeout is a transient failure; the engine never waits
/// for a handler it gave up on; cancellation reaches groups and subgraphs at any depth. Keep the two
/// in step.
/// </summary>
public class CancellationTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Doc(
        [property: JsonPropertyName("steps")] IReadOnlyList<string> Steps,
        [property: JsonPropertyName("note")] string? Note);

    private static Doc NewDoc() => new([], null);

    private static Func<Doc, Doc> Did(string step) => doc => doc with { Steps = [.. doc.Steps, step] };

    private static Doc Completed(GraphExecutionResult<Doc> result) =>
        Assert.IsType<GraphExecutionResult<Doc>.Completed>(result).State;

    private static GraphExecutionResult<Doc>.Cancelled Cancelled(GraphExecutionResult<Doc> result) =>
        Assert.IsType<GraphExecutionResult<Doc>.Cancelled>(result);

    private static HumanSignal Signal(string? note = null) =>
        new(IsApproved: true, ReviewerToken: "x", FieldOverrides: note is null ? null : new() { ["note"] = note });

    /// <summary>
    /// An async handler that waits until it is cancelled — or, once <see cref="Release"/> is set,
    /// records its step and returns. Says when it started and whether it saw its token cancelled.
    /// </summary>
    private sealed class Hang(string step)
    {
        private int _calls;

        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource SawCancel { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool Release { get; set; }

        public int Calls => Volatile.Read(ref _calls);

        public async ValueTask<Doc> Handler(Doc doc, NodeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref _calls);
            if (Release)
            {
                return Did(step)(doc);
            }

            Started.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                SawCancel.TrySetResult();
                throw;
            }

            return doc;
        }
    }

    /// <summary><c>Prepare</c> -> <c>Slow</c> -> <c>Finish</c>, with <c>Slow</c> added by <paramref name="addSlow"/>.</summary>
    private static StateGraph<Doc> Around(Func<StateGraph<Doc>, StateGraph<Doc>> addSlow, Action? onPrepare = null) =>
        addSlow(new StateGraph<Doc>()
                .AddNode("Prepare", doc =>
                {
                    onPrepare?.Invoke();
                    return Did("prepare")(doc);
                }))
            .AddNode("Finish", Did("finish"))
            .AddEdge("Prepare", "Slow")
            .AddEdge("Slow", "Finish");

    [Fact]
    public async Task Cancelling_During_A_Node_Persists_A_Cancelled_Checkpoint_At_That_Node()
    {
        var slow = new Hang("slow");
        var graph = Around(g => g.AddNode("Slow", slow.Handler)).Compile();
        var store = new InMemoryCheckpointStore();
        using var cts = new CancellationTokenSource();

        var run = graph.RunAsync("thread_cancel", NewDoc(), store, cts.Token);
        await slow.Started.Task.WaitAsync(Patience);
        await cts.CancelAsync();
        var cancelled = Cancelled(await run);

        Assert.Equal(("thread_cancel", "Slow", CancelReason.Requested), (cancelled.ThreadId, cancelled.Node, cancelled.Reason));
        Assert.Equal(["prepare"], cancelled.State.Steps);
        Assert.Equal(["Slow"], cancelled.StoppedAt);
        await slow.SawCancel.Task.WaitAsync(Patience);

        var checkpoint = store.Load("thread_cancel")!;
        Assert.Equal(CheckpointStatus.Cancelled, checkpoint.Status);
        Assert.Equal("1.2.0", checkpoint.SchemaVersion);
        Assert.Equal(("Slow", "Slow"), (checkpoint.SuspendedNode, checkpoint.NextNode));
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse("""{ "steps": ["prepare"], "note": null }"""), checkpoint.FrozenState));
        Assert.Equal(new Dictionary<string, string> { ["cancel_reason"] = "requested" }, checkpoint.Metadata);
        Assert.Null(checkpoint.Nested);
    }

    [Fact]
    public async Task A_Run_Started_Already_Cancelled_Runs_Nothing()
    {
        var prepared = false;
        var graph = Around(g => g.AddNode("Slow", Did("slow")), onPrepare: () => prepared = true).Compile();
        var store = new InMemoryCheckpointStore();

        var cancelled = Cancelled(await graph.RunAsync("t", NewDoc(), store, new CancellationToken(canceled: true)));

        Assert.Equal("Prepare", cancelled.Node);
        Assert.Empty(cancelled.State.Steps);
        Assert.False(prepared);
        Assert.Equal("Prepare", store.Load("t")!.NextNode);
    }

    [Fact]
    public async Task Cancelling_Between_Nodes_Stops_Before_The_Next_One_With_The_Committed_State()
    {
        using var cts = new CancellationTokenSource();

        // Edge conditions run once the node has completed: the cancellation lands between nodes.
        var graph = new StateGraph<Doc>()
            .AddNode("Prepare", Did("prepare"))
            .AddNode("Finish", Did("finish"))
            .AddConditionalEdge("Prepare", "Finish", _ =>
            {
                cts.Cancel();
                return true;
            })
            .Compile();

        var cancelled = Cancelled(await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore(), cts.Token));

        Assert.Equal("Finish", cancelled.Node);
        Assert.Equal(["prepare"], cancelled.State.Steps);
    }

    [Fact]
    public async Task The_Deadline_Ends_The_Run_As_Cancelled_With_Reason_Deadline()
    {
        var slow = new Hang("slow");
        var graph = Around(g => g.AddNode("Slow", slow.Handler)).Compile();
        var store = new InMemoryCheckpointStore();

        var result = await graph.RunAsync(
            "t", NewDoc(), store, new RunOptions { Deadline = DateTimeOffset.UtcNow.AddMilliseconds(150) });

        var cancelled = Cancelled(result);
        Assert.Equal(("Slow", CancelReason.Deadline), (cancelled.Node, cancelled.Reason));
        Assert.Equal("deadline", store.Load("t")!.Metadata!["cancel_reason"]);
        await slow.SawCancel.Task.WaitAsync(Patience);
    }

    [Fact]
    public async Task A_Deadline_Already_Passed_Runs_Nothing()
    {
        var graph = Around(g => g.AddNode("Slow", Did("slow"))).Compile();

        var cancelled = Cancelled(await graph.RunAsync(
            "t", NewDoc(), new InMemoryCheckpointStore(), new RunOptions { Deadline = DateTimeOffset.UtcNow.AddSeconds(-1) }));

        Assert.Equal(("Prepare", CancelReason.Deadline), (cancelled.Node, cancelled.Reason));
    }

    [Fact]
    public async Task A_Sync_Node_Is_Abandoned_Not_Waited_For()
    {
        using var gate = new ManualResetEventSlim();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = 0;
        var graph = Around(g => g.AddNode("Slow", doc =>
        {
            started.SetResult();
            gate.Wait(Patience);
            Interlocked.Increment(ref finished);
            return doc;
        })).Compile();
        using var cts = new CancellationTokenSource();

        var run = graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore(), cts.Token);
        await started.Task.WaitAsync(Patience);
        await cts.CancelAsync();

        Assert.Equal("Slow", Cancelled(await run.WaitAsync(Patience)).Node);
        // The run is over while the handler is still blocked.
        Assert.Equal(0, Volatile.Read(ref finished));
        gate.Set();
    }

    [Fact]
    public async Task Cancellation_Is_Not_A_Failure_Nothing_Is_Retried_And_No_Fallback_Runs()
    {
        var slow = new Hang("slow");
        var graph = Around(g => g
                .AddFallibleNode("Slow", slow.Handler)
                .AddNode("Fallback", Did("fallback"))
                .SetRecoveryPolicy("Slow", RecoveryPolicy.Retry(3).WithFallback("Fallback")))
            .Compile();
        using var cts = new CancellationTokenSource();

        var run = graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore(), cts.Token);
        await slow.Started.Task.WaitAsync(Patience);
        await cts.CancelAsync();
        var cancelled = Cancelled(await run);

        Assert.Equal("Slow", cancelled.Node);
        Assert.Equal(1, slow.Calls);
        Assert.DoesNotContain("fallback", cancelled.State.Steps);
    }

    [Fact]
    public async Task A_Timeout_Is_A_Transient_Failure_That_Is_Retried()
    {
        var calls = 0;
        var firstSawCancel = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var graph = Around(g => g
                .AddFallibleNode("Slow", async (doc, ctx, ct) =>
                {
                    Interlocked.Increment(ref calls);
                    if (ctx.Attempt == 1)
                    {
                        try
                        {
                            await Task.Delay(Timeout.Infinite, ct);
                        }
                        catch (OperationCanceledException)
                        {
                            firstSawCancel.SetResult();
                            throw;
                        }
                    }

                    return Did("slow")(doc);
                })
                .SetTimeout("Slow", TimeSpan.FromMilliseconds(100))
                .SetRecoveryPolicy("Slow", RecoveryPolicy.Retry(1)))
            .Compile();

        var doc = Completed(await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));

        Assert.Equal(["prepare", "slow", "finish"], doc.Steps);
        Assert.Equal(2, calls);
        await firstSawCancel.Task.WaitAsync(Patience);
    }

    [Fact]
    public async Task An_Exhausted_Timeout_Fails_Even_A_Plain_Node_With_A_Failed_Checkpoint()
    {
        var slow = new Hang("slow");
        var graph = Around(g => g.AddNode("Slow", slow.Handler).SetTimeout("Slow", TimeSpan.FromMilliseconds(50)))
            .Compile();
        var store = new InMemoryCheckpointStore();

        var failed = Assert.IsType<GraphExecutionResult<Doc>.NodeFailed>(await graph.RunAsync("t", NewDoc(), store));

        Assert.Equal(("Slow", 1, "node 'Slow' timed out after 50 ms"), (failed.Node, failed.Attempts, failed.Message));
        var checkpoint = store.Load("t")!;
        Assert.Equal((CheckpointStatus.Failed, "1.0.0"), (checkpoint.Status, checkpoint.SchemaVersion));
        Assert.Equal("transient", checkpoint.Metadata!["error_kind"]);
    }

    [Fact]
    public async Task A_Timeout_Routes_To_The_Fallback_With_The_Pre_Failure_State()
    {
        var slow = new Hang("slow");
        var graph = Around(g => g
                .AddFallibleNode("Slow", slow.Handler)
                .AddNode("Fallback", Did("fallback"))
                .SetTimeout("Slow", TimeSpan.FromMilliseconds(50))
                .SetRecoveryPolicy("Slow", RecoveryPolicy.Default.WithFallback("Fallback")))
            .Compile();

        var doc = Completed(await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));

        Assert.Equal(["prepare", "fallback"], doc.Steps);
    }

    [Fact]
    public async Task A_Timeout_Abandons_A_Sync_Node()
    {
        using var gate = new ManualResetEventSlim();
        var graph = Around(g => g
                .AddNode("Slow", doc =>
                {
                    gate.Wait(Patience);
                    return doc;
                })
                .SetTimeout("Slow", TimeSpan.FromMilliseconds(50)))
            .Compile();

        var result = await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()).WaitAsync(Patience);

        Assert.Equal("node 'Slow' timed out after 50 ms", Assert.IsType<GraphExecutionResult<Doc>.NodeFailed>(result).Message);
        gate.Set();
    }

    [Fact]
    public void A_Timeout_Must_Be_Positive_And_Name_A_Node()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new StateGraph<Doc>().SetTimeout("A", TimeSpan.Zero));

        var ex = Assert.Throws<InvalidOperationException>(() => new StateGraph<Doc>()
            .AddNode("A", Did("a"))
            .SetTimeout("Missing", TimeSpan.FromSeconds(1))
            .Compile());
        Assert.Equal("timeout set for 'Missing', which does not exist", ex.Message);
    }

    [Fact]
    public async Task Cancelling_A_Group_Cancels_Every_Branch()
    {
        var acme = new Hang("acme");
        var globex = new Hang("globex");
        var branches = new ParallelBranches<Doc>().Branch("Acme", acme.Handler).Branch("Globex", globex.Handler);
        var graph = Around(g => g.AddParallelGroup("Slow", branches, (entry, _) => entry)).Compile();
        using var cts = new CancellationTokenSource();

        var run = graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore(), cts.Token);
        await Task.WhenAll(acme.Started.Task, globex.Started.Task).WaitAsync(Patience);
        await cts.CancelAsync();

        Assert.Equal("Slow", Cancelled(await run).Node);
        await Task.WhenAll(acme.SawCancel.Task, globex.SawCancel.Task).WaitAsync(Patience);
    }

    [Fact]
    public async Task A_Timeout_On_A_Group_Covers_The_Whole_Group()
    {
        var acme = new Hang("acme");
        var branches = new ParallelBranches<Doc>().Branch("Acme", acme.Handler).Branch("Quick", Did("quick"));
        var graph = Around(g => g
                .AddParallelGroup("Slow", branches, (entry, _) => entry)
                .SetTimeout("Slow", TimeSpan.FromMilliseconds(50)))
            .Compile();

        var failed = Assert.IsType<GraphExecutionResult<Doc>.NodeFailed>(
            await graph.RunAsync("t", NewDoc(), new InMemoryCheckpointStore()));

        Assert.Equal("node 'Slow' timed out after 50 ms", failed.Message);
        await acme.SawCancel.Task.WaitAsync(Patience);
    }

    [Fact]
    public async Task A_Cancelled_Thread_Resumes_Once_At_The_Node_It_Stopped()
    {
        var prepares = 0;
        var slow = new Hang("slow");
        var graph = Around(g => g.AddNode("Slow", slow.Handler), onPrepare: () => prepares++).Compile();
        var store = new InMemoryCheckpointStore();
        using var cts = new CancellationTokenSource();

        var run = graph.RunAsync("t", NewDoc(), store, cts.Token);
        await slow.Started.Task.WaitAsync(Patience);
        await cts.CancelAsync();
        Cancelled(await run);

        slow.Release = true;
        var doc = Completed(await graph.ResumeAsync("t", Signal(note: "fixed"), store));

        Assert.Equal(["prepare", "slow", "finish"], doc.Steps);
        Assert.Equal("fixed", doc.Note);
        Assert.Equal(1, prepares);

        var again = Assert.IsType<GraphExecutionResult<Doc>.Failed>(await graph.ResumeAsync("t", Signal(), store));
        Assert.Contains("already resumed", again.Message);
    }

    /// <summary><c>Draft</c> -> <c>Send</c>, a child whose <c>Send</c> is <paramref name="send"/>.</summary>
    private static CompiledGraph<Doc> Child(Hang send, Action onDraft) =>
        new StateGraph<Doc>()
            .AddNode("Draft", doc =>
            {
                onDraft();
                return Did("draft")(doc);
            })
            .AddNode("Send", send.Handler)
            .AddEdge("Draft", "Send")
            .Compile();

    /// <summary><c>Prepare</c> -> <c>Work</c> (the subgraph) -> <c>Finish</c>.</summary>
    private static StateGraph<Doc> Parent(CompiledGraph<Doc> child, Action? onInput = null) =>
        new StateGraph<Doc>()
            .AddNode("Prepare", Did("prepare"))
            .AddSubgraph("Work", child, doc =>
            {
                onInput?.Invoke();
                return new Doc([], doc.Note);
            }, (entry, result) => entry with { Steps = [.. entry.Steps, .. result.Steps], Note = result.Note })
            .AddNode("Finish", Did("finish"))
            .AddEdge("Prepare", "Work")
            .AddEdge("Work", "Finish");

    [Fact]
    public async Task Cancelled_Inside_A_Subgraph_Nests_The_Childs_Checkpoint_And_Resumes_There()
    {
        var drafts = 0;
        var inputs = 0;
        var send = new Hang("send");
        var graph = Parent(Child(send, () => drafts++), onInput: () => inputs++).Compile();
        var store = new InMemoryCheckpointStore();
        using var cts = new CancellationTokenSource();

        var run = graph.RunAsync("t", NewDoc(), store, cts.Token);
        await send.Started.Task.WaitAsync(Patience);
        await cts.CancelAsync();
        var cancelled = Cancelled(await run);

        Assert.Equal("Work", cancelled.Node);
        Assert.Equal(["Work", "Send"], cancelled.StoppedAt);
        var checkpoint = store.Load("t")!;
        Assert.Equal((CheckpointStatus.Cancelled, "1.2.0", "Work", "Work"),
            (checkpoint.Status, checkpoint.SchemaVersion, checkpoint.SuspendedNode, checkpoint.NextNode));
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse("""{ "steps": ["prepare"], "note": null }"""), checkpoint.FrozenState));
        var nested = checkpoint.Nested!;
        Assert.Equal((CheckpointStatus.Cancelled, "1.2.0", "Work", "Send", "Send"),
            (nested.Status, nested.SchemaVersion, nested.ThreadId, nested.SuspendedNode, nested.NextNode));
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse("""{ "steps": ["draft"], "note": null }"""), nested.FrozenState));
        Assert.Equal("requested", nested.Metadata!["cancel_reason"]);

        send.Release = true;
        var doc = Completed(await graph.ResumeAsync("t", Signal(), store));

        Assert.Equal(["prepare", "draft", "send", "finish"], doc.Steps);
        Assert.Equal((1, 1), (drafts, inputs));
    }

    [Fact]
    public async Task Cancellation_Reaches_A_Grandchild()
    {
        var send = new Hang("send");
        var middle = new StateGraph<Doc>()
            .AddSubgraph("Inner", Child(send, () => { }), doc => doc, (_, result) => result)
            .Compile();
        var graph = Parent(middle).Compile();
        var store = new InMemoryCheckpointStore();
        using var cts = new CancellationTokenSource();

        var run = graph.RunAsync("t", NewDoc(), store, cts.Token);
        await send.Started.Task.WaitAsync(Patience);
        await cts.CancelAsync();

        Assert.Equal(["Work", "Inner", "Send"], Cancelled(await run).StoppedAt);
        Assert.Equal(["Work", "Inner", "Send"], store.Load("t")!.WaitingAt());
        await send.SawCancel.Task.WaitAsync(Patience);
    }

    [Fact]
    public async Task A_Subgraph_Nodes_Timeout_Fails_It_And_Nests_Where_The_Child_Stopped()
    {
        var drafts = 0;
        var send = new Hang("send");
        var graph = Parent(Child(send, () => drafts++))
            .SetTimeout("Work", TimeSpan.FromMilliseconds(100))
            .Compile();
        var store = new InMemoryCheckpointStore();

        var failed = Assert.IsType<GraphExecutionResult<Doc>.NodeFailed>(await graph.RunAsync("t", NewDoc(), store));

        Assert.Equal(("Work", "node 'Work' timed out after 100 ms"), (failed.Node, failed.Message));
        var checkpoint = store.Load("t")!;
        Assert.Equal((CheckpointStatus.Failed, "1.2.0"), (checkpoint.Status, checkpoint.SchemaVersion));
        Assert.Equal("transient", checkpoint.Metadata!["error_kind"]);
        var nested = checkpoint.Nested!;
        Assert.Equal((CheckpointStatus.Cancelled, "Send"), (nested.Status, nested.SuspendedNode));
        Assert.Equal("timeout", nested.Metadata!["cancel_reason"]);

        // A resume re-enters the child where it was stopped: Draft doesn't run again.
        send.Release = true;
        var doc = Completed(await graph.ResumeAsync("t", Signal(), store));
        Assert.Equal(["prepare", "draft", "send", "finish"], doc.Steps);
        Assert.Equal(1, drafts);
    }

    [Fact]
    public async Task A_Resume_Started_Cancelled_Keeps_The_Humans_Answer_In_A_Nested_Thread()
    {
        var drafts = 0;
        var child = new StateGraph<Doc>()
            .AddNode("Draft", doc =>
            {
                drafts++;
                return Did("draft")(doc);
            })
            .AddNode("Approve", Did("approve"))
            .AddInterruptBoundary("Approve")
            .AddNode("Send", Did("send"))
            .AddEdge("Draft", "Approve")
            .AddEdge("Approve", "Send")
            .Compile();
        var graph = Parent(child).Compile();
        var store = new InMemoryCheckpointStore();
        Assert.IsType<GraphExecutionResult<Doc>.Interrupted>(await graph.RunAsync("t", NewDoc(), store));

        var cancelled = Cancelled(await graph.ResumeAsync(
            "t", Signal(note: "admin"), store, new CancellationToken(canceled: true)));

        Assert.Equal(["Work", "Send"], cancelled.StoppedAt);
        Assert.Equal("admin", store.Load("t")!.Nested!.FrozenState!["note"]!.GetValue<string>());

        var doc = Completed(await graph.ResumeAsync("t", Signal(), store));
        Assert.Equal(["prepare", "draft", "approve", "send", "finish"], doc.Steps);
        Assert.Equal("admin", doc.Note);
        Assert.Equal(1, drafts);
    }

    [Fact]
    public async Task A_Cancelled_Stream_Ends_With_No_NodeEnd_For_The_Stopped_Node()
    {
        var slow = new Hang("slow");
        var graph = Around(g => g.AddNode("Slow", slow.Handler)).Compile();
        using var cts = new CancellationTokenSource();

        var (reader, completion) = graph.RunStreaming("t", NewDoc(), new InMemoryCheckpointStore(), cts.Token);
        var drained = Task.Run(async () =>
        {
            var chunks = new List<(StreamChunkType, string)>();
            await foreach (var chunk in reader.ReadAllAsync())
            {
                chunks.Add((chunk.Type, chunk.NodeName));
            }

            return chunks;
        });
        await slow.Started.Task.WaitAsync(Patience);
        await cts.CancelAsync();

        Assert.Equal("Slow", Cancelled(await completion).Node);
        Assert.Equal(
            [
                (StreamChunkType.NodeStart, "Prepare"),
                (StreamChunkType.NodeEnd, "Prepare"),
                (StreamChunkType.NodeStart, "Slow"),
            ],
            await drained.WaitAsync(Patience));
    }

    [Fact]
    public async Task A_Handler_Waiting_On_A_Full_Stream_Sees_Its_Cancellation_As_OperationCanceledException()
    {
        var seen = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var graph = Around(g => g.AddNode("Slow", async (Doc doc, NodeContext ctx, CancellationToken ct) =>
        {
            try
            {
                while (true)
                {
                    await ctx.Stream!.EmitTokenAsync("token", ct);
                }
            }
            catch (Exception ex)
            {
                seen.TrySetResult(ex);
                throw;
            }
        })).Compile();
        using var cts = new CancellationTokenSource();

        // Nobody reads, so the channel fills and the handler waits on its next token.
        var (reader, completion) = graph.RunStreaming("t", NewDoc(), new InMemoryCheckpointStore(), cts.Token);
        while (reader.Count < StreamContext.DefaultChannelCapacity)
        {
            await Task.Delay(5);
        }

        await cts.CancelAsync();

        Assert.Equal("Slow", Cancelled(await completion.WaitAsync(Patience)).Node);
        Assert.IsAssignableFrom<OperationCanceledException>(await seen.Task.WaitAsync(Patience));
    }

    [Fact]
    public async Task A_Cancelled_Run_Doesnt_Wait_On_A_Full_Stream_To_Write_Its_Own_Chunks()
    {
        var returned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var graph = Around(g => g.AddNode("Slow", async (Doc doc, NodeContext ctx, CancellationToken ct) =>
        {
            // Prepare's NodeStart and NodeEnd and this node's NodeStart are already in: fill the rest.
            for (var i = 0; i < StreamContext.DefaultChannelCapacity - 3; i++)
            {
                await ctx.Stream!.EmitTokenAsync("token", ct);
            }

            returned.SetResult();
            return Did("slow")(doc);
        })).Compile();
        using var cts = new CancellationTokenSource();

        // The consumer stopped reading and then cancelled: the node's NodeEnd has no room.
        var (_, completion) = graph.RunStreaming("t", NewDoc(), new InMemoryCheckpointStore(), cts.Token);
        await returned.Task.WaitAsync(Patience);
        await cts.CancelAsync();

        var cancelled = Cancelled(await completion.WaitAsync(Patience));
        Assert.Equal("Finish", cancelled.Node);
        Assert.Equal(["prepare", "slow"], cancelled.State.Steps);
    }
}
