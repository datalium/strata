using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/nested_interrupts.rs</c> (<c>design/16-Nested-Interrupts.md</c>):
/// a child graph that freezes at an interrupt boundary freezes the whole run at the subgraph node,
/// with the child's checkpoint nested in the parent's; <c>ResumeAsync</c> re-enters the child where it
/// froze. Keep the two in step.
/// </summary>
public class NestedInterruptTests
{
    /// <summary>The parent's state.</summary>
    public sealed record Order(
        [property: JsonPropertyName("amount")] int Amount,
        [property: JsonPropertyName("log")] IReadOnlyList<string> Log,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy);

    /// <summary>A child's state — a different type from the parent's on purpose.</summary>
    public sealed record Job(
        [property: JsonPropertyName("n")] int N,
        [property: JsonPropertyName("trail")] IReadOnlyList<string> Trail,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy);

    private static Order NewOrder(int amount) => new(amount, [], null);

    private static Func<Job, Job> Step(string label) =>
        job => job with { Trail = [.. job.Trail, label] };

    private static Func<Order, Order> Note(string text) =>
        order => order with { Log = [.. order.Log, text] };

    private static Job ToJob(Order order) => new(order.Amount, [], null);

    private static Order FromJob(Order entry, Job job) =>
        entry with { Amount = job.N, Log = [.. entry.Log, .. job.Trail], ApprovedBy = job.ApprovedBy };

    /// <summary><c>Draft</c> (n * 2) -> <c>Approve</c> (an interrupt boundary) -> <c>Send</c> (n + 1).</summary>
    private static CompiledGraph<Job> ReviewChild() =>
        new StateGraph<Job>()
            .AddNode("Draft", job => job with { N = job.N * 2, Trail = [.. job.Trail, "draft"] })
            .AddNode("Approve", Step("approve"))
            .AddInterruptBoundary("Approve")
            .AddNode("Send", job => job with { N = job.N + 1, Trail = [.. job.Trail, "send"] })
            .AddEdge("Draft", "Approve")
            .AddEdge("Approve", "Send")
            .Compile();

    /// <summary><c>Prepare</c> -> <c>Work</c> (the subgraph) -> <c>Finish</c>.</summary>
    private static StateGraph<Order> ParentAround(CompiledGraph<Job> child) =>
        new StateGraph<Order>()
            .AddNode("Prepare", Note("prepared"))
            .AddSubgraph("Work", child, ToJob, FromJob)
            .AddNode("Finish", Note("finished"))
            .AddEdge("Prepare", "Work")
            .AddEdge("Work", "Finish");

    private static Order Completed(GraphExecutionResult<Order> result) =>
        Assert.IsType<GraphExecutionResult<Order>.Completed>(result).State;

    private static GraphExecutionResult<Order>.Interrupted Interrupted(GraphExecutionResult<Order> result) =>
        Assert.IsType<GraphExecutionResult<Order>.Interrupted>(result);

    private static HumanSignal NoSignal() => new(IsApproved: true, ReviewerToken: "x", FieldOverrides: null);

    private static HumanSignal ApproveAs(string who) =>
        new(IsApproved: true, ReviewerToken: "token", FieldOverrides: new Dictionary<string, string> { ["approved_by"] = who });

    [Fact]
    public async Task A_Child_Boundary_Freezes_The_Whole_Run_At_The_Subgraph_Node()
    {
        var graph = ParentAround(ReviewChild()).Compile();

        var interrupted = Interrupted(await graph.RunAsync("t", NewOrder(5), new InMemoryCheckpointStore()));

        Assert.Equal("Work", interrupted.SuspendedNode);
        // A resume re-enters the subgraph node, so that is where it continues.
        Assert.Equal("Work", interrupted.NextNode);
        // The state the node started with, not the child's.
        Assert.Equal(5, interrupted.FrozenState.Amount);
        Assert.Equal(["prepared"], interrupted.FrozenState.Log);
    }

    [Fact]
    public async Task The_Parent_Checkpoint_Nests_The_Childs()
    {
        var graph = ParentAround(ReviewChild()).Compile();
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync("t", NewOrder(5), store);

        var parent = store.Load("t")!;
        Assert.Equal(CheckpointSchema.NestedVersion, parent.SchemaVersion);
        Assert.Equal(CheckpointStatus.Interrupted, parent.Status);
        Assert.Equal("Work", parent.SuspendedNode);
        Assert.Equal("Work", parent.NextNode);
        Assert.Equal(5, parent.FrozenState!["amount"]!.GetValue<int>());

        var child = parent.Nested!;
        // A nested checkpoint is named after the subgraph node it belongs to.
        Assert.Equal("Work", child.ThreadId);
        Assert.Equal(CheckpointSchema.Version, child.SchemaVersion);
        Assert.Equal(CheckpointStatus.Interrupted, child.Status);
        Assert.Equal("Approve", child.SuspendedNode);
        Assert.Equal("Send", child.NextNode);
        Assert.Equal(10, child.FrozenState!["n"]!.GetValue<int>());
        Assert.Equal("""["draft","approve"]""", child.FrozenState["trail"]!.ToJsonString());
        Assert.Null(child.Nested);
    }

    [Fact]
    public async Task A_Run_With_No_Child_Freeze_Writes_The_Same_Checkpoint_As_Before()
    {
        // A parent's own boundary still writes a plain 1.0.0 checkpoint with no `nested` key at all,
        // so nothing that predates nesting changes.
        var graph = new StateGraph<Order>()
            .AddNode("Prepare", Note("prepared"))
            .AddInterruptBoundary("Prepare")
            .AddNode("Finish", Note("finished"))
            .AddEdge("Prepare", "Finish")
            .Compile();
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync("t", NewOrder(1), store);

        var checkpoint = store.Load("t")!;

        Assert.Equal(CheckpointSchema.Version, checkpoint.SchemaVersion);
        Assert.Null(checkpoint.Nested);
        Assert.False(checkpoint.ToWireJson().ContainsKey("nested"));
    }

    [Fact]
    public async Task Resume_Continues_The_Child_From_Where_It_Froze_Then_The_Parent()
    {
        var inputs = 0;
        var drafts = 0;
        var child = new StateGraph<Job>()
            .AddNode("Draft", job =>
            {
                Interlocked.Increment(ref drafts);
                return job with { N = job.N * 2, Trail = [.. job.Trail, "draft"] };
            })
            .AddNode("Approve", Step("approve"))
            .AddInterruptBoundary("Approve")
            .AddNode("Send", job => job with { N = job.N + 1, Trail = [.. job.Trail, "send"] })
            .AddEdge("Draft", "Approve")
            .AddEdge("Approve", "Send")
            .Compile();
        var graph = new StateGraph<Order>()
            .AddNode("Prepare", Note("prepared"))
            .AddSubgraph(
                "Work",
                child,
                order =>
                {
                    Interlocked.Increment(ref inputs);
                    return ToJob(order);
                },
                FromJob)
            .AddNode("Finish", Note("finished"))
            .AddEdge("Prepare", "Work")
            .AddEdge("Work", "Finish")
            .Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));

        var state = Completed(await graph.ResumeAsync("t", NoSignal(), store));

        Assert.Equal(11, state.Amount);
        Assert.Equal(["prepared", "draft", "approve", "send", "finished"], state.Log);
        // The child picked up after `Approve`: neither its input mapper nor `Draft` ran a second time.
        Assert.Equal(1, inputs);
        Assert.Equal(1, drafts);
    }

    [Fact]
    public async Task Field_Overrides_Reach_The_Childs_State_And_Not_The_Parents()
    {
        var graph = ParentAround(ReviewChild()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));
        var signal = new HumanSignal(
            IsApproved: true,
            ReviewerToken: "token",
            FieldOverrides: new Dictionary<string, string>
            {
                ["approved_by"] = "admin_01",
                // Only the parent has a `log`, and it is not what was reviewed.
                ["log"] = "tampered",
            });

        var state = Completed(await graph.ResumeAsync("t", signal, store));

        // `approved_by` was written into the child's state and mapped up; the parent's own `log` was left alone.
        Assert.Equal("admin_01", state.ApprovedBy);
        Assert.Equal(["prepared", "draft", "approve", "send", "finished"], state.Log);
    }

    [Fact]
    public async Task The_Resumed_Checkpoint_Keeps_Its_Nested_Child_And_Is_Stamped()
    {
        var graph = ParentAround(ReviewChild()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));

        Completed(await graph.ResumeAsync("t", NoSignal(), store));

        var resumed = store.Load("t")!;
        Assert.NotNull(resumed.ResumedAt);
        Assert.NotNull(resumed.Nested);
    }

    [Fact]
    public async Task A_Second_Resume_Of_The_Same_Nested_Checkpoint_Is_Refused()
    {
        var graph = ParentAround(ReviewChild()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));
        Completed(await graph.ResumeAsync("t", NoSignal(), store));

        var again = await graph.ResumeAsync("t", NoSignal(), store);

        Assert.Contains("already resumed", Assert.IsType<GraphExecutionResult<Order>.Failed>(again).Message);
    }

    [Fact]
    public async Task A_Second_Boundary_In_The_Child_Freezes_Again_With_A_Fresh_Nested_Checkpoint()
    {
        var child = new StateGraph<Job>()
            .AddNode("First", Step("first"))
            .AddInterruptBoundary("First")
            .AddNode("Second", Step("second"))
            .AddInterruptBoundary("Second")
            .AddNode("Last", Step("last"))
            .AddEdge("First", "Second")
            .AddEdge("Second", "Last")
            .Compile();
        var graph = ParentAround(child).Compile();
        var store = new InMemoryCheckpointStore();

        Interrupted(await graph.RunAsync("t", NewOrder(1), store));
        Assert.Equal("First", store.Load("t")!.Nested!.SuspendedNode);

        var second = Interrupted(await graph.ResumeAsync("t", NoSignal(), store));
        Assert.Equal("Work", second.SuspendedNode);
        var secondCheckpoint = store.Load("t")!;
        Assert.Null(secondCheckpoint.ResumedAt);
        Assert.Equal("Second", secondCheckpoint.Nested!.SuspendedNode);

        var state = Completed(await graph.ResumeAsync("t", NoSignal(), store));
        Assert.Equal(["prepared", "first", "second", "last", "finished"], state.Log);
    }

    [Fact]
    public async Task Nesting_Goes_To_Any_Depth()
    {
        // Work (subgraph) -> child graph's `Inner` (subgraph) -> grandchild `Review`.
        var child = new StateGraph<Job>()
            .AddNode("Open", Step("open"))
            .AddSubgraph(
                "Inner",
                ReviewChild(),
                job => new Job(job.N, [], null),
                (entry, inner) => entry with { N = inner.N, Trail = [.. entry.Trail, .. inner.Trail], ApprovedBy = inner.ApprovedBy })
            .AddNode("Close", Step("close"))
            .AddEdge("Open", "Inner")
            .AddEdge("Inner", "Close")
            .Compile();
        var graph = ParentAround(child).Compile();
        var store = new InMemoryCheckpointStore();

        var interrupted = Interrupted(await graph.RunAsync("t", NewOrder(5), store));
        Assert.Equal(("Work", "Work"), (interrupted.SuspendedNode, interrupted.NextNode));

        var childCheckpoint = store.Load("t")!.Nested!;
        Assert.Equal("Work", childCheckpoint.ThreadId);
        Assert.Equal("Inner", childCheckpoint.SuspendedNode);
        Assert.Equal(CheckpointSchema.NestedVersion, childCheckpoint.SchemaVersion);
        var grandchild = childCheckpoint.Nested!;
        Assert.Equal("Inner", grandchild.ThreadId);
        Assert.Equal("Approve", grandchild.SuspendedNode);
        Assert.Equal(CheckpointSchema.Version, grandchild.SchemaVersion);

        var state = Completed(await graph.ResumeAsync("t", ApproveAs("admin_02"), store));
        Assert.Equal(["prepared", "open", "draft", "approve", "send", "close", "finished"], state.Log);
        Assert.Equal(11, state.Amount);
        // The override travelled all the way down to the grandchild and back up.
        Assert.Equal("admin_02", state.ApprovedBy);
    }

    [Fact]
    public async Task The_Subgraph_Node_Can_Itself_Be_A_Parent_Boundary_After_Its_Child_Completes()
    {
        var graph = ParentAround(ReviewChild()).AddInterruptBoundary("Work").Compile();
        var store = new InMemoryCheckpointStore();

        // First stop: inside the child.
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));
        Assert.NotNull(store.Load("t")!.Nested);

        // The child completes on resume, so now the parent's own boundary on the node applies: an
        // ordinary checkpoint, with nothing nested.
        var second = Interrupted(await graph.ResumeAsync("t", NoSignal(), store));
        Assert.Equal(("Work", "Finish"), (second.SuspendedNode, second.NextNode));
        Assert.Equal(11, second.FrozenState.Amount);
        var plain = store.Load("t")!;
        Assert.Equal(CheckpointSchema.Version, plain.SchemaVersion);
        Assert.Null(plain.Nested);

        var state = Completed(await graph.ResumeAsync("t", NoSignal(), store));
        Assert.Equal(["prepared", "draft", "approve", "send", "finished"], state.Log);
    }

    [Fact]
    public async Task A_Child_Freeze_Is_Not_A_Failure_So_The_Recovery_Policy_Stays_Out_Of_It()
    {
        var graph = ParentAround(ReviewChild())
            .AddNode("Fallback", Note("fell back"))
            .SetRecoveryPolicy("Work", RecoveryPolicy.Retry(3).WithFallback("Fallback"))
            .Compile();
        var store = new InMemoryCheckpointStore();

        var interrupted = Interrupted(await graph.RunAsync("t", NewOrder(5), store));

        Assert.Equal("Work", interrupted.SuspendedNode);
        // No fallback ran and nothing was retried.
        Assert.Equal(["prepared"], interrupted.FrozenState.Log);
        Assert.Equal(CheckpointStatus.Interrupted, store.Load("t")!.Status);
    }

    [Fact]
    public async Task A_Retry_Of_A_Resumed_Child_Resumes_From_The_Same_Checkpoint_So_Nobody_Is_Asked_Twice()
    {
        var approvals = 0;
        var sends = 0;
        var child = new StateGraph<Job>()
            .AddNode("Approve", job =>
            {
                Interlocked.Increment(ref approvals);
                return job with { Trail = [.. job.Trail, "approve"] };
            })
            .AddInterruptBoundary("Approve")
            .AddFallibleNode("Send", job =>
                Interlocked.Increment(ref sends) == 1
                    ? throw NodeException.Transient("mail server busy")
                    : job with { Trail = [.. job.Trail, "send"] })
            .AddEdge("Approve", "Send")
            .Compile();
        var graph = ParentAround(child).SetRecoveryPolicy("Work", RecoveryPolicy.Retry(1)).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));

        var state = Completed(await graph.ResumeAsync("t", NoSignal(), store));

        // `Send` failed once and was retried; the parent's retry re-entered the child at the same point
        // instead of running `Approve` again.
        Assert.Equal(2, sends);
        Assert.Equal(1, approvals);
        Assert.Equal(["prepared", "approve", "send", "finished"], state.Log);
    }

    [Fact]
    public async Task A_Child_That_Fails_After_Resume_Persists_A_Failed_Checkpoint_At_The_Subgraph_Node()
    {
        var child = new StateGraph<Job>()
            .AddNode("Approve", Step("approve"))
            .AddInterruptBoundary("Approve")
            .AddFallibleNode("Send", _ => throw NodeException.Fatal("mailbox closed"))
            .AddEdge("Approve", "Send")
            .Compile();
        var graph = ParentAround(child).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));

        var result = await graph.ResumeAsync("t", NoSignal(), store);

        var failed = Assert.IsType<GraphExecutionResult<Order>.NodeFailed>(result);
        Assert.Equal("Work", failed.Node);
        Assert.Contains("mailbox closed", failed.Message);

        var checkpoint = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Failed, checkpoint.Status);
        Assert.Equal("Work", checkpoint.SuspendedNode);
        // It keeps the child's own `Failed` checkpoint, so it can be resumed (see "Resuming a failed
        // thread" below). The child had answered its boundary, so it carries that decision
        // (design/21-Typed-Human-Signals.md), and both levels are written as 1.3.0.
        Assert.Equal(CheckpointSchema.DecisionVersion, checkpoint.SchemaVersion);
        var nested = Assert.IsType<Checkpoint>(checkpoint.Nested);
        Assert.Equal(CheckpointStatus.Failed, nested.Status);
        Assert.Equal("Send", nested.SuspendedNode);
    }

    [Fact]
    public async Task A_Checkpoint_With_An_Unsupported_Schema_Version_Is_Refused_On_Resume()
    {
        var graph = ParentAround(ReviewChild()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));
        store.Save(store.Load("t")! with { SchemaVersion = "2.0.0" });

        var result = await graph.ResumeAsync("t", NoSignal(), store);

        Assert.Contains(
            "unsupported schema_version '2.0.0'",
            Assert.IsType<GraphExecutionResult<Order>.Failed>(result).Message);
    }

    [Fact]
    public async Task A_Checkpoint_Nesting_A_Child_Under_A_Node_That_Is_Not_A_Subgraph_Is_Refused()
    {
        var graph = ParentAround(ReviewChild()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));
        store.Save(store.Load("t")! with { NextNode = "Prepare" });

        var result = await graph.ResumeAsync("t", NoSignal(), store);

        var message = Assert.IsType<GraphExecutionResult<Order>.Failed>(result).Message;
        Assert.Contains("'Prepare'", message);
        Assert.Contains("not a subgraph node", message);
    }

    [Fact]
    public async Task RunStreaming_Starts_The_Subgraph_Node_But_Does_Not_End_It_When_Its_Child_Freezes()
    {
        var graph = ParentAround(ReviewChild()).Compile();

        var (reader, completion) = graph.RunStreaming("t", NewOrder(5), new InMemoryCheckpointStore());
        var chunks = new List<(StreamChunkType Type, string Node)>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            chunks.Add((chunk.Type, chunk.NodeName));
        }

        Assert.Equal("Work", Interrupted(await completion).SuspendedNode);
        Assert.Contains((StreamChunkType.NodeStart, "Work"), chunks);
        Assert.DoesNotContain((StreamChunkType.NodeEnd, "Work"), chunks);
    }

    // --- The shared cross-language fixture ---------------------------------------------------------
    //
    // schema/fixtures/checkpoint-lifecycle/nested.json is the interop artifact for a checkpoint nesting a
    // child's. This proves the engine both produces a checkpoint matching it (freeze) and consumes it to
    // resume correctly; rust/strata-core/tests/nested_interrupts.rs proves the same in Rust against the
    // identical file.

    private static Checkpoint LoadFixture(string name = "nested.json", [CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(
            testDir, "..", "..", "..", "schema", "fixtures", "checkpoint-lifecycle", name));
        return Checkpoint.FromWireJson(JsonNode.Parse(File.ReadAllText(path))!);
    }

    /// <summary>
    /// Blanks every level's <c>CreatedAt</c> and decision's <c>DecidedAt</c> (the fixture's are fixed
    /// historical values; a live run uses the real current time).
    /// </summary>
    private static Checkpoint WithoutTimestamps(Checkpoint checkpoint) =>
        checkpoint with
        {
            CreatedAt = string.Empty,
            Decision = checkpoint.Decision is null ? null : checkpoint.Decision with { DecidedAt = string.Empty },
            Nested = checkpoint.Nested is null ? null : WithoutTimestamps(checkpoint.Nested),
        };

    [Fact]
    public async Task Freezing_Matches_The_Shared_Nested_Fixture_At_Every_Level()
    {
        var graph = ParentAround(ReviewChild()).Compile();
        var store = new InMemoryCheckpointStore();

        await graph.RunAsync("thread_nested_demo", NewOrder(5), store);

        var persisted = store.Load("thread_nested_demo")!;
        Assert.True(
            JsonNode.DeepEquals(
                WithoutTimestamps(LoadFixture()).ToWireJson(),
                WithoutTimestamps(persisted).ToWireJson()),
            WithoutTimestamps(persisted).ToWireJson().ToJsonString());
    }

    [Fact]
    public async Task Resuming_The_Shared_Nested_Fixture_Produces_The_Expected_Completed_State()
    {
        var graph = ParentAround(ReviewChild()).Compile();
        var store = new InMemoryCheckpointStore();
        var fixture = LoadFixture();
        store.Save(fixture);

        var state = Completed(await graph.ResumeAsync(fixture.ThreadId, ApproveAs("admin_01"), store));

        Assert.Equal(11, state.Amount);
        Assert.Equal(["prepared", "draft", "approve", "send", "finished"], state.Log);
        Assert.Equal("admin_01", state.ApprovedBy);
    }

    // --- Where a nested thread is waiting (Interrupted.WaitingAt) ------------------------------------

    /// <summary>
    /// <c>Open</c> -> <c>Inner</c> (a subgraph running <paramref name="grandchild"/>) -> <c>Close</c>. Its
    /// <c>output</c> mapper keeps an <c>approved_by</c> this level was given over the grandchild's, so a
    /// signal addressed to this level shows in the result.
    /// </summary>
    private static CompiledGraph<Job> MiddleChild(CompiledGraph<Job> grandchild) =>
        new StateGraph<Job>()
            .AddNode("Open", Step("open"))
            .AddSubgraph(
                "Inner",
                grandchild,
                job => new Job(job.N, [], null),
                (entry, inner) => entry with
                {
                    N = inner.N,
                    Trail = [.. entry.Trail, .. inner.Trail],
                    ApprovedBy = entry.ApprovedBy ?? inner.ApprovedBy,
                })
            .AddNode("Close", Step("close"))
            .AddEdge("Open", "Inner")
            .AddEdge("Inner", "Close")
            .Compile();

    [Fact]
    public async Task A_Flat_Freeze_Reports_The_Boundary_Node_As_Where_It_Is_Waiting()
    {
        var result = await ReviewChild().RunAsync("t", ToJob(NewOrder(5)), new InMemoryCheckpointStore());

        Assert.Equal(["Approve"], Assert.IsType<GraphExecutionResult<Job>.Interrupted>(result).WaitingAt);
    }

    [Fact]
    public async Task A_Nested_Freeze_Reports_One_Node_Per_Level_From_The_Outside_In()
    {
        var graph = ParentAround(ReviewChild()).Compile();
        var store = new InMemoryCheckpointStore();

        var interrupted = Interrupted(await graph.RunAsync("t", NewOrder(5), store));

        Assert.Equal(["Work", "Approve"], interrupted.WaitingAt);
        // `SuspendedNode` still names only the subgraph node...
        Assert.Equal("Work", interrupted.SuspendedNode);
        // ...and the path is what the stored checkpoint says, too.
        Assert.Equal(["Work", "Approve"], store.Load("t")!.WaitingAt());
    }

    [Fact]
    public async Task A_Freeze_Three_Levels_Down_Reports_The_Whole_Path()
    {
        var graph = ParentAround(MiddleChild(ReviewChild())).Compile();

        var interrupted = Interrupted(await graph.RunAsync("t", NewOrder(5), new InMemoryCheckpointStore()));

        Assert.Equal(["Work", "Inner", "Approve"], interrupted.WaitingAt);
    }

    [Fact]
    public async Task A_Second_Boundary_After_Resume_Reports_The_New_Place()
    {
        var child = new StateGraph<Job>()
            .AddNode("First", Step("first"))
            .AddInterruptBoundary("First")
            .AddNode("Second", Step("second"))
            .AddInterruptBoundary("Second")
            .AddNode("Done", Step("done"))
            .AddEdge("First", "Second")
            .AddEdge("Second", "Done")
            .Compile();
        var graph = ParentAround(child).Compile();
        var store = new InMemoryCheckpointStore();

        Assert.Equal(["Work", "First"], Interrupted(await graph.RunAsync("t", NewOrder(5), store)).WaitingAt);
        Assert.Equal(["Work", "Second"], Interrupted(await graph.ResumeAsync("t", NoSignal(), store)).WaitingAt);
    }

    // --- A different signal per level ----------------------------------------------------------------

    /// <summary>A parent state with a field of its own (<c>escalated_by</c>) that its subgraph node's <c>output</c> mapper keeps, unlike <see cref="Order"/>'s.</summary>
    public sealed record Desk(
        [property: JsonPropertyName("amount")] int Amount,
        [property: JsonPropertyName("log")] IReadOnlyList<string> Log,
        [property: JsonPropertyName("escalated_by")] string? EscalatedBy,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy);

    private static CompiledGraph<Desk> DeskAround(CompiledGraph<Job> child) =>
        new StateGraph<Desk>()
            .AddNode("Prepare", desk => desk with { Log = [.. desk.Log, "prepared"] })
            .AddSubgraph(
                "Work",
                child,
                desk => new Job(desk.Amount, [], null),
                (entry, job) => entry with { Amount = job.N, Log = [.. entry.Log, .. job.Trail], ApprovedBy = job.ApprovedBy })
            .AddEdge("Prepare", "Work")
            .Compile();

    private static Desk NewDesk() => new(5, [], null, null);

    private static HumanSignal EscalateAs(string who) =>
        new(IsApproved: true, ReviewerToken: "token", FieldOverrides: new Dictionary<string, string> { ["escalated_by"] = who });

    [Fact]
    public async Task One_Resume_Can_Send_Different_Overrides_To_The_Parent_And_The_Child()
    {
        var graph = DeskAround(ReviewChild());
        var store = new InMemoryCheckpointStore();
        Assert.IsType<GraphExecutionResult<Desk>.Interrupted>(await graph.RunAsync("t", NewDesk(), store));

        var signals = new LevelSignals(ApproveAs("reviewer")).At([], EscalateAs("ops"));
        var desk = Assert.IsType<GraphExecutionResult<Desk>.Completed>(await graph.ResumeAsync("t", signals, store)).State;

        // Each override reached only the level it was addressed to: the parent's reached the state its
        // subgraph node started with (and survived the `output` mapper); the child's was written into
        // the child's state.
        Assert.Equal("ops", desk.EscalatedBy);
        Assert.Equal("reviewer", desk.ApprovedBy);
    }

    [Fact]
    public async Task A_Signal_Can_Address_A_Level_In_The_Middle()
    {
        var graph = ParentAround(MiddleChild(ReviewChild())).Compile();
        var store = new InMemoryCheckpointStore();

        // Without a signal for the middle level the grandchild's answer wins...
        Interrupted(await graph.RunAsync("plain", NewOrder(5), store));
        var plain = Completed(await graph.ResumeAsync("plain", ApproveAs("inner"), store));
        Assert.Equal("inner", plain.ApprovedBy);

        // ...and with one, the middle level's own state carries it instead.
        Interrupted(await graph.RunAsync("levels", NewOrder(5), store));
        var signals = new LevelSignals(ApproveAs("inner")).At(["Work"], ApproveAs("middle"));
        var levelled = Completed(await graph.ResumeAsync("levels", signals, store));
        Assert.Equal("middle", levelled.ApprovedBy);
    }

    [Fact]
    public async Task A_Bare_Human_Signal_Still_Goes_To_The_Innermost_Level_Only()
    {
        var graph = DeskAround(ReviewChild());
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync("t", NewDesk(), store);

        var desk = Assert.IsType<GraphExecutionResult<Desk>.Completed>(
            await graph.ResumeAsync("t", EscalateAs("ops"), store)).State;

        // `escalated_by` is not a field of the child, so nothing was addressed to the parent and
        // nothing arrived there.
        Assert.Null(desk.EscalatedBy);
    }

    [Fact]
    public async Task A_Signal_For_A_Level_The_Checkpoint_Does_Not_Have_Is_Refused_And_Consumes_Nothing()
    {
        var graph = ParentAround(ReviewChild()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));

        var signals = new LevelSignals(ApproveAs("admin_01")).At(["Nope"], ApproveAs("x"));
        var message = Assert.IsType<GraphExecutionResult<Order>.Failed>(await graph.ResumeAsync("t", signals, store)).Message;

        Assert.Contains("[Nope]", message);
        Assert.Contains("[], [Work]", message);

        // Nothing was stamped or run, so the thread can still be resumed.
        Assert.Null(store.Load("t")!.ResumedAt);
        Completed(await graph.ResumeAsync("t", ApproveAs("admin_01"), store));
    }

    [Fact]
    public async Task Addressing_The_Innermost_Level_Twice_Is_Refused()
    {
        var graph = ParentAround(ReviewChild()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));

        // `["Work"]` is the child, which is the innermost level here.
        var signals = new LevelSignals(ApproveAs("a")).At(["Work"], ApproveAs("b"));
        var result = await graph.ResumeAsync("t", signals, store);

        Assert.Contains("innermost", Assert.IsType<GraphExecutionResult<Order>.Failed>(result).Message);
        Assert.Null(store.Load("t")!.ResumedAt);
    }

    [Fact]
    public async Task On_A_Flat_Thread_The_Only_Level_Is_The_Innermost_One()
    {
        var child = ReviewChild();
        var store = new InMemoryCheckpointStore();
        await child.RunAsync("t", ToJob(NewOrder(5)), store);

        var signals = new LevelSignals(ApproveAs("a")).At([], ApproveAs("b"));
        var result = await child.ResumeAsync("t", signals, store);

        Assert.Contains("innermost", Assert.IsType<GraphExecutionResult<Job>.Failed>(result).Message);
    }

    // --- Resuming a failed thread ----------------------------------------------------------------------

    private sealed class Counter
    {
        public int Value { get; set; }
    }

    /// <summary><c>Approve</c> (a boundary) -> <c>Send</c>, which fails fatally for its first <paramref name="failingSends"/> calls. Both are counted.</summary>
    private static CompiledGraph<Job> FlakyChild(Counter approvals, Counter sends, int failingSends) =>
        new StateGraph<Job>()
            .AddNode("Approve", job =>
            {
                approvals.Value++;
                return job with { Trail = [.. job.Trail, "approve"] };
            })
            .AddInterruptBoundary("Approve")
            .AddFallibleNode("Send", job =>
            {
                sends.Value++;
                return sends.Value <= failingSends
                    ? throw NodeException.Fatal("mailbox closed")
                    : job with { N = job.N + 1, Trail = [.. job.Trail, "send"] };
            })
            .AddEdge("Approve", "Send")
            .Compile();

    [Fact]
    public async Task A_Failed_Child_Leaves_The_Childs_Failed_Checkpoint_Nested_In_The_Parents()
    {
        var graph = ParentAround(FlakyChild(new Counter(), new Counter(), failingSends: 1)).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));

        Assert.IsType<GraphExecutionResult<Order>.NodeFailed>(await graph.ResumeAsync("t", ApproveAs("admin_01"), store));

        var failed = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Failed, failed.Status);
        Assert.Equal(CheckpointSchema.DecisionVersion, failed.SchemaVersion);
        Assert.Equal("Work", failed.SuspendedNode);
        Assert.Equal("Work", failed.NextNode);
        // The parent's is the state the subgraph node started with...
        Assert.Equal("""["prepared"]""", failed.FrozenState!["log"]!.ToJsonString());
        Assert.Null(failed.ResumedAt);
        Assert.Contains("mailbox closed", failed.Metadata!["error"]);
        // ...and the child's holds what the human answered, and where it stopped.
        var child = failed.Nested!;
        Assert.Equal("Work", child.ThreadId);
        Assert.Equal(CheckpointStatus.Failed, child.Status);
        Assert.Equal(CheckpointSchema.DecisionVersion, child.SchemaVersion);
        Assert.Equal("Approve", child.Decision!.Boundary);
        Assert.Null(failed.Decision);
        Assert.Equal("Send", child.SuspendedNode);
        Assert.Equal("Send", child.NextNode);
        Assert.Equal("admin_01", child.FrozenState!["approved_by"]!.GetValue<string>());
        Assert.Equal("""["approve"]""", child.FrozenState["trail"]!.ToJsonString());
        Assert.Equal("mailbox closed", child.Metadata!["error"]);
        Assert.Equal(["Work", "Send"], failed.WaitingAt());
    }

    [Fact]
    public async Task Resuming_A_Failed_Nested_Thread_Re_Enters_The_Child_Without_Asking_Again()
    {
        var approvals = new Counter();
        var sends = new Counter();
        var graph = ParentAround(FlakyChild(approvals, sends, failingSends: 1)).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));
        Assert.IsType<GraphExecutionResult<Order>.NodeFailed>(await graph.ResumeAsync("t", ApproveAs("admin_01"), store));

        // No signal this time: the human's earlier answer is in the checkpoint.
        var state = Completed(await graph.ResumeAsync("t", NoSignal(), store));

        Assert.Equal(1, approvals.Value);
        Assert.Equal(2, sends.Value);
        Assert.Equal(6, state.Amount);
        Assert.Equal("admin_01", state.ApprovedBy);
        Assert.Equal(["prepared", "approve", "send", "finished"], state.Log);
        // The failure it recovered from is spent.
        Assert.NotNull(store.Load("t")!.ResumedAt);
        var again = await graph.ResumeAsync("t", NoSignal(), store);
        Assert.Contains("already resumed", Assert.IsType<GraphExecutionResult<Order>.Failed>(again).Message);
    }

    [Fact]
    public async Task A_Signal_On_A_Failed_Resume_Reaches_The_Childs_Committed_State()
    {
        var graph = ParentAround(FlakyChild(new Counter(), new Counter(), failingSends: 1)).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));
        await graph.ResumeAsync("t", ApproveAs("admin_01"), store);

        var state = Completed(await graph.ResumeAsync("t", ApproveAs("admin_02"), store));

        Assert.Equal("admin_02", state.ApprovedBy);
    }

    [Fact]
    public async Task A_Child_That_Fails_Again_On_Resume_Leaves_A_Fresh_Failed_Checkpoint()
    {
        var sends = new Counter();
        var graph = ParentAround(FlakyChild(new Counter(), sends, failingSends: 2)).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));
        await graph.ResumeAsync("t", ApproveAs("admin_01"), store);

        Assert.IsType<GraphExecutionResult<Order>.NodeFailed>(await graph.ResumeAsync("t", NoSignal(), store));

        // Still resumable, and still remembering the answer.
        var again = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Failed, again.Status);
        Assert.Null(again.ResumedAt);
        Assert.Equal("admin_01", again.Nested!.FrozenState!["approved_by"]!.GetValue<string>());
        var state = Completed(await graph.ResumeAsync("t", NoSignal(), store));
        Assert.Equal(3, sends.Value);
        Assert.Equal("admin_01", state.ApprovedBy);
    }

    [Fact]
    public async Task A_Failure_Three_Levels_Down_Nests_A_Failed_Checkpoint_At_Every_Level_And_Resumes()
    {
        var approvals = new Counter();
        var sends = new Counter();
        var graph = ParentAround(MiddleChild(FlakyChild(approvals, sends, failingSends: 1))).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewOrder(5), store));
        Assert.IsType<GraphExecutionResult<Order>.NodeFailed>(await graph.ResumeAsync("t", ApproveAs("admin_01"), store));

        var failed = store.Load("t")!;
        Assert.Equal(["Work", "Inner", "Send"], failed.WaitingAt());
        var middle = failed.Nested!;
        Assert.Equal(CheckpointStatus.Failed, middle.Status);
        Assert.Equal(CheckpointSchema.DecisionVersion, middle.SchemaVersion);
        Assert.Null(middle.Decision);
        var grandchild = middle.Nested!;
        Assert.Equal(CheckpointStatus.Failed, grandchild.Status);
        Assert.Equal(CheckpointSchema.DecisionVersion, grandchild.SchemaVersion);
        Assert.Equal("Approve", grandchild.Decision!.Boundary);

        var state = Completed(await graph.ResumeAsync("t", NoSignal(), store));

        Assert.Equal(1, approvals.Value);
        Assert.Equal(2, sends.Value);
        Assert.Equal("admin_01", state.ApprovedBy);
        Assert.Equal(["prepared", "open", "approve", "send", "close", "finished"], state.Log);
    }

    [Fact]
    public async Task A_Child_Failure_With_No_Checkpoint_Of_Its_Own_Nests_Nothing()
    {
        // A routing gap fails the run without any node failing, so the child persists no `Failed`
        // checkpoint.
        var child = new StateGraph<Job>()
            .AddNode("A", Step("a"))
            .AddNode("B", Step("b"))
            .AddConditionalEdge("A", "B", _ => false)
            .Compile();
        var graph = ParentAround(child).Compile();
        var store = new InMemoryCheckpointStore();

        Assert.IsAssignableFrom<GraphExecutionResult<Order>.Failed>(await graph.RunAsync("t", NewOrder(5), store));

        // The subgraph node fails as a whole and the parent records that, but there is no child
        // checkpoint to nest.
        var failed = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Failed, failed.Status);
        Assert.Equal("Work", failed.SuspendedNode);
        Assert.Null(failed.Nested);
        Assert.Equal(CheckpointSchema.Version, failed.SchemaVersion);
    }

    // --- The shared cross-language fixture for a failed nested thread -------------------------------------
    //
    // schema/fixtures/checkpoint-lifecycle/nested-failed.json, as for nested.json above:
    // rust/strata-core/tests/nested_interrupts.rs proves the same in Rust against the same file.

    /// <summary><see cref="ReviewChild"/>, but <c>Send</c> fails fatally: what the failed fixture records.</summary>
    private static CompiledGraph<Job> ReviewChildWhoseSendFails() =>
        new StateGraph<Job>()
            .AddNode("Draft", job => job with { N = job.N * 2, Trail = [.. job.Trail, "draft"] })
            .AddNode("Approve", Step("approve"))
            .AddInterruptBoundary("Approve")
            .AddFallibleNode("Send", _ => throw NodeException.Fatal("mailbox closed"))
            .AddEdge("Draft", "Approve")
            .AddEdge("Approve", "Send")
            .Compile();

    [Fact]
    public async Task Failing_Matches_The_Shared_Nested_Failed_Fixture_At_Every_Level()
    {
        var graph = ParentAround(ReviewChildWhoseSendFails()).Compile();
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync("thread_nested_failed_demo", NewOrder(5), store);

        await graph.ResumeAsync("thread_nested_failed_demo", ApproveAs("admin_01"), store);

        var persisted = store.Load("thread_nested_failed_demo")!;
        Assert.True(
            JsonNode.DeepEquals(
                WithoutTimestamps(LoadFixture("nested-failed.json")).ToWireJson(),
                WithoutTimestamps(persisted).ToWireJson()),
            WithoutTimestamps(persisted).ToWireJson().ToJsonString());
    }

    [Fact]
    public async Task Resuming_The_Shared_Nested_Failed_Fixture_Completes_From_The_Failed_Node()
    {
        // The same graph, with `Send` working now.
        var graph = ParentAround(ReviewChild()).Compile();
        var store = new InMemoryCheckpointStore();
        var fixture = LoadFixture("nested-failed.json");
        store.Save(fixture);

        var state = Completed(await graph.ResumeAsync(fixture.ThreadId, NoSignal(), store));

        Assert.Equal(11, state.Amount);
        Assert.Equal(["prepared", "draft", "approve", "send", "finished"], state.Log);
        Assert.Equal("admin_01", state.ApprovedBy);
    }
}
