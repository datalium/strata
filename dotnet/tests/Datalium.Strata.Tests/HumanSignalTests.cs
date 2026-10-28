using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Schema;
using Datalium.Strata.Validation;
using Checkpoint = Datalium.Strata.Interruption.Checkpoint;
using CheckpointStatus = Datalium.Strata.Interruption.CheckpointStatus;

namespace Datalium.Strata.Tests;

/// <summary>
/// Typed human signals, recorded decisions and the reject route (<c>design/21-Typed-Human-Signals.md</c>,
/// SG-17). Mirrors <c>rust/strata-core/tests/human_signals.rs</c>; keep the two in step. Both produce and
/// resume the shared fixtures <c>checkpoint-lifecycle/decision-failed.json</c> and
/// <c>nested-decision.json</c>, and both export <c>graph-export/reject-route.json</c>.
/// </summary>
public class HumanSignalTests
{
    public sealed record Plan([property: JsonPropertyName("steps")] IReadOnlyList<string> Steps);

    /// <summary>The flat graph's state; the fixture's <c>frozen_state</c> has exactly these fields.</summary>
    public sealed record Remediation(
        [property: JsonPropertyName("plan")] Plan? Plan,
        [property: JsonPropertyName("replicas")] int Replicas,
        [property: JsonPropertyName("outcome")] string? Outcome);

    /// <summary>The nested fixture's parent state.</summary>
    public sealed record Incident(
        [property: JsonPropertyName("replicas")] int Replicas,
        [property: JsonPropertyName("log")] IReadOnlyList<string> Log,
        [property: JsonPropertyName("outcome")] string? Outcome);

    /// <summary>The nested fixture's child state.</summary>
    public sealed record Change(
        [property: JsonPropertyName("replicas")] int Replicas,
        [property: JsonPropertyName("trail")] IReadOnlyList<string> Trail,
        [property: JsonPropertyName("reason")] string? Reason);

    /// <summary>For the scalar overrides.</summary>
    public sealed record Settings(
        [property: JsonPropertyName("count")] int Count,
        [property: JsonPropertyName("dry_run")] bool DryRun,
        [property: JsonPropertyName("note")] string? Note);

    /// <summary>What each node saw in <see cref="NodeContext.Decision"/>, in the order they ran.</summary>
    private sealed class Seen
    {
        private readonly ConcurrentQueue<(string Path, HumanDecision? Decision)> _entries = new();

        public void Add(NodeContext ctx) => _entries.Enqueue((ctx.NodePath, ctx.Decision));

        public HumanDecision? At(string path) => _entries.Last(e => e.Path == path).Decision;

        public IReadOnlyList<HumanDecision?> All(string path) => [.. _entries.Where(e => e.Path == path).Select(e => e.Decision)];
    }

    private static Func<TState, NodeContext, CancellationToken, ValueTask<TState>> Record<TState>(
        Seen seen, Func<TState, NodeContext, TState>? then = null) =>
        (state, ctx, _) =>
        {
            seen.Add(ctx);
            return ValueTask.FromResult(then is null ? state : then(state, ctx));
        };

    private static HumanSignal Approve(string reviewer) => HumanSignal.Approve(reviewer);

    private static HumanSignal Reject(string reviewer) => HumanSignal.Reject(reviewer);

    private static TState Completed<TState>(GraphExecutionResult<TState> result) =>
        Assert.IsType<GraphExecutionResult<TState>.Completed>(result).State;

    private static void Interrupted<TState>(GraphExecutionResult<TState> result) =>
        Assert.IsType<GraphExecutionResult<TState>.Interrupted>(result);

    private static string Refused<TState>(GraphExecutionResult<TState> result) =>
        Assert.IsType<GraphExecutionResult<TState>.Failed>(result).Message;

    // --- The flat reference graph -------------------------------------------------------------------
    //
    // Plan -> Approve (boundary, rejects to Abort) -> Apply. Apply is fallible and fails fatally while
    // `failures` lasts; Abort and Apply both say who decided.

    private static StateGraph<Remediation> Remediate(Seen seen, int failures = 0, bool rejectRoute = true)
    {
        var left = failures;
        var graph = new StateGraph<Remediation>()
            .AddNode("Plan", s => s with { Plan = new Plan(["drain", "restart"]), Replicas = 2 })
            .AddNode("Approve", s => s)
            .AddFallibleNode("Apply", (s, ctx, _) =>
            {
                seen.Add(ctx);
                return Interlocked.Decrement(ref left) >= 0
                    ? throw NodeException.Fatal("cluster unreachable")
                    : ValueTask.FromResult(s with { Outcome = $"applied, approved by {ctx.Decision?.Reviewer}" });
            })
            .AddNode("Abort", Record<Remediation>(seen, (s, ctx) => s with { Outcome = $"aborted by {ctx.Decision!.Reviewer}" }))
            .AddEdge("Plan", "Approve")
            .AddEdge("Approve", "Apply");
        return rejectRoute
            ? graph.AddInterruptBoundary("Approve", onReject: "Abort")
            : graph.AddInterruptBoundary("Approve").AddEdge("Apply", "Abort");
    }

    private static Remediation Fresh() => new(Plan: null, Replicas: 0, Outcome: null);

    private static HumanSignal ApproveWithEdits() =>
        Approve("alice@acme.io")
            .WithOverride("replicas", 3)
            .WithOverride("plan", JsonSerializer.SerializeToNode(new Plan(["drain", "rollback"])));

    // --- Typed overrides ----------------------------------------------------------------------------

    [Fact]
    public async Task Typed_Overrides_Rehydrate_With_Their_Json_Type()
    {
        var graph = Remediate(new Seen()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", Fresh(), store));

        var state = Completed(await graph.ResumeAsync("t", ApproveWithEdits(), store));

        Assert.Equal(3, state.Replicas);
        Assert.Equal(["drain", "rollback"], state.Plan!.Steps);
        Assert.Equal("applied, approved by alice@acme.io", state.Outcome);
    }

    [Fact]
    public async Task Integer_Boolean_String_And_Null_Overrides_Keep_Their_Type()
    {
        var graph = new StateGraph<Settings>()
            .AddNode("Ask", s => s)
            .AddNode("Done", s => s)
            .AddEdge("Ask", "Done")
            .AddInterruptBoundary("Ask")
            .Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", new Settings(1, DryRun: false, Note: "keep"), store));

        var signal = Approve("alice")
            .WithOverride("count", 7)
            .WithOverride("dry_run", true)
            .WithOverride("note", null);
        var state = Completed(await graph.ResumeAsync("t", signal, store));

        Assert.Equal(new Settings(7, DryRun: true, Note: null), state);
    }

    [Fact]
    public async Task A_String_Override_Still_Writes_A_Json_String()
    {
        var graph = new StateGraph<Settings>()
            .AddNode("Ask", s => s)
            .AddNode("Done", s => s)
            .AddEdge("Ask", "Done")
            .AddInterruptBoundary("Ask")
            .Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", new Settings(1, DryRun: false, Note: null), store));

        var state = Completed(await graph.ResumeAsync(
            "t", new HumanSignal(true, "alice", new Dictionary<string, string> { ["note"] = "42" }), store));

        Assert.Equal("42", state.Note);
    }

    [Fact]
    public async Task A_Value_Can_Be_Reused_Across_Signals()
    {
        var graph = Remediate(new Seen()).Compile();
        var store = new InMemoryCheckpointStore();
        var plan = JsonSerializer.SerializeToNode(new Plan(["drain"]));
        var signal = Approve("alice").WithOverride("plan", plan);

        Interrupted(await graph.RunAsync("a", Fresh(), store));
        Interrupted(await graph.RunAsync("b", Fresh(), store));
        Completed(await graph.ResumeAsync("a", signal, store));
        var state = Completed(await graph.ResumeAsync("b", Approve("bob").WithOverride("plan", plan), store));

        Assert.Equal(["drain"], state.Plan!.Steps);
    }

    [Fact]
    public async Task A_Key_In_Both_Maps_Is_Refused_Before_The_Stamp()
    {
        var graph = Remediate(new Seen()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", Fresh(), store));

        var twice = new HumanSignal(true, "alice", new Dictionary<string, string> { ["outcome"] = "x" })
            .WithOverride("outcome", "y");
        var message = Refused(await graph.ResumeAsync("t", twice, store));

        Assert.Contains("override 'outcome' is set as both a string and a typed value", message);
        Assert.Null(store.Load("t")!.ResumedAt);
        Completed(await graph.ResumeAsync("t", Approve("alice"), store));
    }

    // --- Preflight ----------------------------------------------------------------------------------

    [Fact]
    public async Task An_Override_That_Does_Not_Fit_Is_Refused_And_The_Thread_Stays_Resumable()
    {
        var graph = Remediate(new Seen()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", Fresh(), store));

        var message = Refused(await graph.ResumeAsync("t", Approve("alice").WithOverride("replicas", "three"), store));

        Assert.Contains("failed to rehydrate state for thread 't'", message);
        Assert.Null(store.Load("t")!.ResumedAt);
        Assert.Equal(3, Completed(await graph.ResumeAsync("t", Approve("alice").WithOverride("replicas", 3), store)).Replicas);
    }

    /// <summary>Prepare -> Work (a subgraph: Draft -> Approve (boundary, rejects to Revoke) -> Apply) -> Finish.</summary>
    private static StateGraph<Incident> IncidentAround(CompiledGraph<Change> child, Seen seen) =>
        new StateGraph<Incident>()
            .AddNode("Prepare", s => s with { Log = [.. s.Log, "prepared"] })
            .AddSubgraph(
                "Work",
                child,
                s => new Change(s.Replicas, [], Reason: null),
                (s, c) => s with { Replicas = c.Replicas, Log = [.. s.Log, .. c.Trail] })
            .AddNode("Finish", Record<Incident>(seen, (s, _) => s with { Log = [.. s.Log, "finished"] }))
            .AddEdge("Prepare", "Work")
            .AddEdge("Work", "Finish");

    private static CompiledGraph<Change> ChangeChild(Seen seen, int revokeFailures = 0)
    {
        var left = revokeFailures;
        return new StateGraph<Change>()
            .AddNode("Draft", c => c with { Trail = [.. c.Trail, "draft"] })
            .AddNode("Approve", c => c with { Trail = [.. c.Trail, "approve"] })
            .AddInterruptBoundary("Approve", onReject: "Revoke")
            .AddNode("Apply", Record<Change>(seen, (c, _) => c with { Trail = [.. c.Trail, "apply"] }))
            .AddFallibleNode("Revoke", (c, ctx, _) =>
            {
                seen.Add(ctx);
                return Interlocked.Decrement(ref left) >= 0
                    ? throw NodeException.Fatal("revocation queue full")
                    : ValueTask.FromResult(c with { Trail = [.. c.Trail, $"revoke:{ctx.Decision!.Reviewer}"] });
            })
            .AddEdge("Draft", "Approve")
            .AddEdge("Approve", "Apply")
            .Compile();
    }

    private static Incident NewIncident() => new(Replicas: 2, Log: [], Outcome: null);

    [Fact]
    public async Task A_Bad_Override_For_An_Inner_Level_Is_Refused_Naming_The_Level()
    {
        var graph = IncidentAround(ChangeChild(new Seen()), new Seen()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewIncident(), store));

        var message = Refused(await graph.ResumeAsync("t", Approve("alice").WithOverride("trail", 5), store));

        Assert.Contains("failed to rehydrate state for thread 't' at level [Work]", message);
        Assert.Null(store.Load("t")!.ResumedAt);
        var state = Completed(await graph.ResumeAsync("t", Approve("alice"), store));
        Assert.Equal(["prepared", "draft", "approve", "apply", "finished"], state.Log);
    }

    [Fact]
    public async Task A_Bad_Override_For_An_Outer_Level_Is_Refused_Too()
    {
        var graph = IncidentAround(ChangeChild(new Seen()), new Seen()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewIncident(), store));

        var signals = new LevelSignals(Approve("alice")).At([], Approve("ops").WithOverride("log", "not a list"));
        var message = Refused(await graph.ResumeAsync("t", signals, store));

        Assert.Contains("failed to rehydrate state for thread 't':", message);
        Assert.Null(store.Load("t")!.ResumedAt);
        Completed(await graph.ResumeAsync("t", Approve("alice"), store));
    }

    // --- Decisions ----------------------------------------------------------------------------------

    [Fact]
    public async Task A_Fresh_Run_Has_No_Decision()
    {
        var seen = new Seen();
        var graph = new StateGraph<int>().AddNode("Only", Record<int>(seen)).Compile();

        await graph.RunAsync("t", 0, new InMemoryCheckpointStore());

        Assert.Null(seen.At("Only"));
    }

    [Fact]
    public async Task Every_Node_And_Branch_After_The_Boundary_Sees_The_Decision()
    {
        var seen = new Seen();
        var graph = new StateGraph<int>()
            .AddNode("Before", Record<int>(seen))
            .AddNode("Approve", s => s)
            .AddInterruptBoundary("Approve")
            .AddNode("Next", Record<int>(seen))
            .AddParallelGroup(
                "Fan",
                new ParallelBranches<int>().Branch("Left", Record<int>(seen)).Branch("Right", Record<int>(seen)),
                (s, _) => s)
            .AddNode("Last", Record<int>(seen))
            .AddEdge("Before", "Approve")
            .AddEdge("Approve", "Next")
            .AddEdge("Next", "Fan")
            .AddEdge("Fan", "Last")
            .Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", 0, store));

        Completed(await graph.ResumeAsync("t", Approve("alice").WithOverride("note", "ok"), store));

        Assert.Null(seen.At("Before"));
        var decision = seen.At("Next")!;
        Assert.Equal("Approve", decision.Boundary);
        Assert.True(decision.IsApproved);
        Assert.Equal("alice", decision.Reviewer);
        Assert.Equal("ok", decision.Overrides["note"].GetString());
        Assert.False(string.IsNullOrEmpty(decision.DecidedAt));
        Assert.Same(decision, seen.At("Fan/Left"));
        Assert.Same(decision, seen.At("Fan/Right"));
        Assert.Same(decision, seen.At("Last"));
    }

    [Fact]
    public async Task A_Fresh_Child_Does_Not_Inherit_Its_Parents_Decision()
    {
        var seen = new Seen();
        var child = new StateGraph<int>().AddNode("Inside", Record<int>(seen)).Compile();
        var graph = new StateGraph<int>()
            .AddNode("Approve", s => s)
            .AddInterruptBoundary("Approve")
            .AddSubgraph("Work", child, s => s, (_, c) => c)
            .AddNode("After", Record<int>(seen))
            .AddEdge("Approve", "Work")
            .AddEdge("Work", "After")
            .Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", 0, store));

        Completed(await graph.ResumeAsync("t", Approve("alice"), store));

        Assert.Null(seen.At("Work/Inside"));
        Assert.Equal("alice", seen.At("After")!.Reviewer);
    }

    [Fact]
    public async Task The_Innermost_Level_Gets_The_Decision_And_The_Outer_Level_Does_Not()
    {
        var seen = new Seen();
        var graph = IncidentAround(ChangeChild(seen), seen).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewIncident(), store));

        Completed(await graph.ResumeAsync("t", Approve("alice"), store));

        Assert.Equal("Approve", seen.At("Work/Apply")!.Boundary);
        Assert.Null(seen.At("Finish"));
    }

    [Fact]
    public async Task An_Outer_Level_Keeps_Its_Decision_Across_A_Childs_Freeze()
    {
        var seen = new Seen();
        var child = new StateGraph<int>()
            .AddNode("Check", s => s)
            .AddInterruptBoundary("Check")
            .AddNode("Fix", Record<int>(seen))
            .AddEdge("Check", "Fix")
            .Compile();
        var graph = new StateGraph<int>()
            .AddNode("Approve", s => s)
            .AddInterruptBoundary("Approve")
            .AddSubgraph("Work", child, s => s, (_, c) => c)
            .AddNode("Execute", Record<int>(seen))
            .AddEdge("Approve", "Work")
            .AddEdge("Work", "Execute")
            .Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", 0, store));
        Interrupted(await graph.ResumeAsync("t", Approve("alice"), store));

        // The parent's checkpoint around the child carries the parent's decision.
        Assert.Equal("alice", store.Load("t")!.Decision!.Reviewer);
        Assert.Null(store.Load("t")!.Nested!.Decision);

        Completed(await graph.ResumeAsync("t", Approve("bob"), store));

        Assert.Equal("bob", seen.At("Work/Fix")!.Reviewer);
        Assert.Equal("Check", seen.At("Work/Fix")!.Boundary);
        Assert.Equal("alice", seen.At("Execute")!.Reviewer);
        Assert.Equal("Approve", seen.At("Execute")!.Boundary);
    }

    [Fact]
    public async Task Answering_A_Second_Boundary_Replaces_The_Decision()
    {
        var seen = new Seen();
        var graph = new StateGraph<int>()
            .AddNode("First", s => s)
            .AddNode("Middle", Record<int>(seen))
            .AddNode("Second", s => s)
            .AddNode("End", Record<int>(seen))
            .AddEdge("First", "Middle")
            .AddEdge("Middle", "Second")
            .AddEdge("Second", "End")
            .AddInterruptBoundary("First")
            .AddInterruptBoundary("Second")
            .Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", 0, store));
        Interrupted(await graph.ResumeAsync("t", Approve("alice"), store));

        // The second freeze carries the decision in effect when it was written.
        var frozen = store.Load("t")!;
        Assert.Equal("First", frozen.Decision!.Boundary);
        Assert.Equal(CheckpointSchema.DecisionVersion, frozen.SchemaVersion);

        Completed(await graph.ResumeAsync("t", Approve("bob"), store));

        Assert.Equal("alice", seen.At("Middle")!.Reviewer);
        Assert.Equal("bob", seen.At("End")!.Reviewer);
        Assert.Equal("Second", seen.At("End")!.Boundary);
    }

    [Fact]
    public async Task Every_Retry_Of_A_Resumed_Subgraph_Sees_The_Same_Decision()
    {
        var seen = new Seen();
        var calls = 0;
        var child = new StateGraph<int>()
            .AddNode("Check", s => s)
            .AddInterruptBoundary("Check")
            .AddFallibleNode("Flaky", (s, ctx, _) =>
            {
                seen.Add(ctx);
                return Interlocked.Increment(ref calls) == 1
                    ? throw NodeException.Transient("blip")
                    : ValueTask.FromResult(s);
            })
            .AddEdge("Check", "Flaky")
            .Compile();
        var graph = new StateGraph<int>()
            .AddSubgraph("Work", child, s => s, (_, c) => c)
            .SetRecoveryPolicy("Work", RecoveryPolicy.Retry(1))
            .Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", 0, store));

        Completed(await graph.ResumeAsync("t", Approve("alice"), store));

        var decisions = seen.All("Work/Flaky");
        Assert.Equal(2, decisions.Count);
        Assert.Equal(decisions[0]!.ToWireJson().ToJsonString(), decisions[1]!.ToWireJson().ToJsonString());
    }

    // --- Persistence --------------------------------------------------------------------------------

    [Fact]
    public async Task A_Failure_After_An_Approval_Carries_The_Decision_And_A_Retry_Keeps_It()
    {
        var seen = new Seen();
        var graph = Remediate(seen, failures: 1).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", Fresh(), store));
        Assert.IsType<GraphExecutionResult<Remediation>.NodeFailed>(await graph.ResumeAsync("t", ApproveWithEdits(), store));

        var failed = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Failed, failed.Status);
        Assert.Equal(CheckpointSchema.DecisionVersion, failed.SchemaVersion);
        Assert.Equal("alice@acme.io", failed.Decision!.Reviewer);

        // The operator retries: Apply runs again and still sees Alice's approval.
        var channel = new RunEventChannel();
        var state = Completed(await graph.ResumeAsync("t", Approve("bob@ops"), store, new RunOptions { Observer = channel }));

        Assert.Equal("applied, approved by alice@acme.io", state.Outcome);
        Assert.Equal("alice@acme.io", seen.At("Apply")!.Reviewer);
        var resumed = Assert.Single((await Drain(channel)).OfType<Resumed>());
        Assert.Equal("bob@ops", resumed.Reviewer);
        Assert.Equal(CheckpointStatus.Failed, resumed.From);
        Assert.Null(resumed.RejectedTo);
    }

    [Fact]
    public async Task A_Cancelled_Run_After_An_Approval_Carries_The_Decision()
    {
        var seen = new Seen();
        using var started = new SemaphoreSlim(0);
        var block = true;
        var graph = new StateGraph<int>()
            .AddNode("Approve", s => s)
            .AddInterruptBoundary("Approve")
            .AddNode("Slow", async (s, ctx, ct) =>
            {
                seen.Add(ctx);
                if (Volatile.Read(ref block))
                {
                    started.Release();
                    await Task.Delay(Timeout.Infinite, ct);
                }

                return s;
            })
            .AddEdge("Approve", "Slow")
            .Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", 0, store));

        using var cts = new CancellationTokenSource();
        var run = graph.ResumeAsync("t", Approve("alice"), store, cts.Token);
        await started.WaitAsync();
        await cts.CancelAsync();
        Assert.IsType<GraphExecutionResult<int>.Cancelled>(await run);

        var cancelled = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Cancelled, cancelled.Status);
        Assert.Equal(CheckpointSchema.DecisionVersion, cancelled.SchemaVersion);
        Assert.Equal("alice", cancelled.Decision!.Reviewer);

        Volatile.Write(ref block, false);
        Completed(await graph.ResumeAsync("t", Approve("bob"), store));
        Assert.Equal("alice", seen.At("Slow")!.Reviewer);
    }

    // --- The reject route ---------------------------------------------------------------------------

    [Fact]
    public async Task A_Rejection_Continues_At_The_Reject_Route()
    {
        var seen = new Seen();
        var graph = Remediate(seen).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", Fresh(), store));

        var state = Completed(await graph.ResumeAsync("t", Reject("bob"), store));

        Assert.Equal("aborted by bob", state.Outcome);
        Assert.False(seen.At("Abort")!.IsApproved);
        Assert.Empty(seen.All("Apply"));
    }

    [Fact]
    public async Task An_Approval_Continues_At_The_Next_Node()
    {
        var seen = new Seen();
        var graph = Remediate(seen).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", Fresh(), store));

        var state = Completed(await graph.ResumeAsync("t", Approve("alice"), store));

        Assert.Equal("applied, approved by alice", state.Outcome);
        Assert.Empty(seen.All("Abort"));
    }

    [Fact]
    public async Task A_Rejection_Without_A_Route_Continues_At_The_Next_Node()
    {
        var seen = new Seen();
        var graph = Remediate(seen, rejectRoute: false).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", Fresh(), store));

        Completed(await graph.ResumeAsync("t", Reject("bob"), store));

        Assert.False(seen.At("Apply")!.IsApproved);
    }

    [Fact]
    public async Task Overrides_Apply_On_A_Rejection()
    {
        var graph = Remediate(new Seen()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", Fresh(), store));

        var state = Completed(await graph.ResumeAsync("t", Reject("bob").WithOverride("replicas", 9), store));

        Assert.Equal(9, state.Replicas);
        Assert.Equal("aborted by bob", state.Outcome);
    }

    [Fact]
    public async Task A_Rejection_Inside_A_Child_Routes_Inside_The_Child()
    {
        var seen = new Seen();
        var graph = IncidentAround(ChangeChild(seen), seen).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewIncident(), store));

        var state = Completed(await graph.ResumeAsync("t", Reject("bob"), store));

        Assert.Equal(["prepared", "draft", "approve", "revoke:bob", "finished"], state.Log);
    }

    [Fact]
    public async Task A_Subgraph_Node_That_Is_A_Boundary_Takes_Its_Route_Once_Its_Child_Completes()
    {
        var seen = new Seen();
        var child = new StateGraph<int>().AddNode("Inside", s => s + 1).Compile();
        var graph = new StateGraph<int>()
            .AddSubgraph("Work", child, s => s, (_, c) => c)
            .AddInterruptBoundary("Work", onReject: "Undo")
            .AddNode("Ship", Record<int>(seen))
            .AddNode("Undo", Record<int>(seen))
            .AddEdge("Work", "Ship")
            .Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", 0, store));
        Assert.Null(store.Load("t")!.Nested);

        Completed(await graph.ResumeAsync("t", Reject("bob"), store));

        Assert.Equal("Work", seen.At("Undo")!.Boundary);
        Assert.Empty(seen.All("Ship"));
    }

    [Fact]
    public async Task A_Failed_Checkpoint_Ignores_Is_Approved()
    {
        var seen = new Seen();
        var graph = Remediate(seen, failures: 1).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", Fresh(), store));
        await graph.ResumeAsync("t", Approve("alice"), store);

        var state = Completed(await graph.ResumeAsync("t", Reject("bob"), store));

        Assert.Equal("applied, approved by alice", state.Outcome);
        Assert.Empty(seen.All("Abort"));
    }

    [Fact]
    public async Task A_Boundary_Node_That_Failed_Is_Re_Entered_Even_On_A_Rejection()
    {
        // The boundary itself failed, so its failed checkpoint is suspended at the boundary: a resume
        // re-runs it rather than taking its reject route, and it freezes for the human again.
        var seen = new Seen();
        var calls = 0;
        var graph = new StateGraph<int>()
            .AddFallibleNode("Approve", s => Interlocked.Increment(ref calls) == 1 ? throw NodeException.Fatal("flaky") : s)
            .AddInterruptBoundary("Approve", onReject: "Abort")
            .AddNode("Apply", s => s)
            .AddNode("Abort", Record<int>(seen))
            .AddEdge("Approve", "Apply")
            .Compile();
        var store = new InMemoryCheckpointStore();
        Assert.IsType<GraphExecutionResult<int>.NodeFailed>(await graph.RunAsync("t", 0, store));
        Assert.Equal("Approve", store.Load("t")!.SuspendedNode);

        Interrupted(await graph.ResumeAsync("t", Reject("bob"), store));

        Assert.Equal(2, calls);
        Assert.Empty(seen.All("Abort"));
        Assert.Null(store.Load("t")!.Decision);
    }

    [Fact]
    public async Task Reject_And_Revise_Loops_Back_To_The_Boundary()
    {
        var seen = new Seen();
        var graph = new StateGraph<int>()
            .AddNode("Draft", s => s + 1)
            .AddNode("Approve", s => s)
            .AddInterruptBoundary("Approve", onReject: "Revise")
            .AddNode("Revise", s => s + 10)
            .AddNode("Publish", Record<int>(seen))
            .AddEdge("Draft", "Approve")
            .AddEdge("Approve", "Publish")
            .AddEdge("Revise", "Approve")
            .Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", 0, store));

        Interrupted(await graph.ResumeAsync("t", Reject("bob"), store));
        Assert.Equal("bob", store.Load("t")!.Decision!.Reviewer);
        var state = Completed(await graph.ResumeAsync("t", Approve("alice"), store));

        Assert.Equal(11, state);
        Assert.Equal("alice", seen.At("Publish")!.Reviewer);
    }

    [Fact]
    public async Task A_Boundary_With_No_Outgoing_Edge_Refuses_An_Approval_And_Routes_A_Rejection()
    {
        var seen = new Seen();
        var graph = new StateGraph<int>()
            .AddNode("Last", s => s)
            .AddInterruptBoundary("Last", onReject: "Undo")
            .AddNode("Undo", Record<int>(seen))
            .Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", 0, store));

        Assert.Contains("has no next node to resume from", Refused(await graph.ResumeAsync("t", Approve("alice"), store)));
        Assert.Null(store.Load("t")!.ResumedAt);

        Completed(await graph.ResumeAsync("t", Reject("bob"), store));
        Assert.Equal("bob", seen.At("Undo")!.Reviewer);
    }

    // --- Validation and export ----------------------------------------------------------------------

    private static JsonNode ReadFixture(string relativePath, [CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(testDir, "..", "..", "..", "schema", "fixtures", relativePath));
        return JsonNode.Parse(File.ReadAllText(path))!;
    }

    [Fact]
    public void The_Export_Matches_The_Shared_Reject_Route_Fixture()
    {
        var exported = JsonSerializer.SerializeToNode(Remediate(new Seen()).ToDefinition());
        var expected = ReadFixture(Path.Combine("graph-export", "reject-route.json"));

        Assert.True(JsonNode.DeepEquals(expected, exported), exported!.ToJsonString());
    }

    [Fact]
    public void Reject_Edges_Follow_Fallback_Edges_In_Node_Declaration_Order()
    {
        var graph = new StateGraph<int>()
            .AddFallibleNode("A", s => s)
            .AddNode("B", s => s)
            .AddNode("C", s => s)
            .AddNode("Rescue", s => s)
            .AddNode("NoB", s => s)
            .AddNode("NoC", s => s)
            .AddEdge("A", "B")
            .AddEdge("B", "C")
            .AddInterruptBoundary("C", onReject: "NoC")
            .AddInterruptBoundary("B", onReject: "NoB")
            .SetRecoveryPolicy("A", new RecoveryPolicy().WithFallback("Rescue"));

        var edges = graph.ToDefinition().Edges;

        Assert.Equal(
            ["A->B ", "B->C ", "A->Rescue on_failure", "B->NoB on_reject", "C->NoC on_reject"],
            edges.Select(e => $"{e.From}->{e.To} {e.Condition}"));
        Assert.All(edges.Where(e => e.Condition is not null), e => Assert.True(e.Guarded));
    }

    [Fact]
    public void A_Node_Reachable_Only_By_Rejection_Is_Not_An_Orphan()
    {
        var compiled = Remediate(new Seen()).Compile();

        Assert.True(GraphValidator.Validate(compiled.ToTopology()).IsValid);
        // The executor's own edge list doesn't route over it.
        Assert.DoesNotContain(compiled.Topology.Edges, e => e.Condition == "on_reject");
        Assert.Equal("STRATA_ERR_ORPHAN", Assert.Single(GraphValidator.Validate(compiled.Topology).Errors).Code);
    }

    [Fact]
    public void A_Reject_Route_To_A_Missing_Node_Is_A_Dangling_Edge()
    {
        var ex = Assert.Throws<GraphCompilationException>(() => new StateGraph<int>()
            .AddNode("Approve", s => s)
            .AddInterruptBoundary("Approve", onReject: "Nope")
            .Compile());

        var error = Assert.Single(ex.Result.Errors);
        Assert.Equal("STRATA_ERR_DANGLING_EDGE", error.Code);
        Assert.Equal("Approve", error.Node);
        Assert.Equal("Nope", error.RelatedNode);
    }

    [Fact]
    public void A_Reject_Route_On_A_Name_That_Is_Not_A_Node_Is_A_Dangling_Edge()
    {
        var ex = Assert.Throws<GraphCompilationException>(() => new StateGraph<int>()
            .AddNode("A", s => s)
            .AddNode("Undo", s => s)
            .AddEdge("A", "Undo")
            .AddInterruptBoundary("Ghost", onReject: "Undo")
            .Compile());

        Assert.Equal("STRATA_ERR_DANGLING_EDGE", Assert.Single(ex.Result.Errors).Code);
    }

    [Fact]
    public void Marking_A_Boundary_Again_Without_A_Route_Removes_It()
    {
        var graph = new StateGraph<int>()
            .AddNode("A", s => s)
            .AddNode("B", s => s)
            .AddEdge("A", "B")
            .AddInterruptBoundary("A", onReject: "B")
            .AddInterruptBoundary("A");

        Assert.DoesNotContain(graph.ToDefinition().Edges, e => e.Condition == "on_reject");
    }

    // --- Run events ---------------------------------------------------------------------------------

    private static async Task<List<RunEvent>> Drain(RunEventChannel channel)
    {
        var events = new List<RunEvent>();
        using var patience = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await foreach (var e in channel.Reader.ReadAllAsync(patience.Token))
        {
            events.Add(e);
        }

        return events;
    }

    [Fact]
    public async Task Resumed_Carries_The_Answer_And_The_Route_Taken()
    {
        var graph = Remediate(new Seen()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", Fresh(), store));

        var channel = new RunEventChannel();
        Completed(await graph.ResumeAsync("t", Reject("bob"), store, new RunOptions { Observer = channel }));
        var events = await Drain(channel);

        Assert.Equal("Abort", events.OfType<RunStarted>().Single().Node);
        var resumed = events.OfType<Resumed>().Single();
        Assert.False(resumed.IsApproved);
        Assert.Equal("bob", resumed.Reviewer);
        Assert.Equal("Abort", resumed.RejectedTo);
    }

    [Fact]
    public async Task Resumed_Names_A_Reject_Route_Taken_Inside_A_Child()
    {
        var graph = IncidentAround(ChangeChild(new Seen()), new Seen()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("t", NewIncident(), store));

        var channel = new RunEventChannel();
        Completed(await graph.ResumeAsync("t", Reject("bob"), store, new RunOptions { Observer = channel }));
        var events = await Drain(channel);

        Assert.Equal("Work", events.OfType<RunStarted>().Single().Node);
        Assert.Equal("Revoke", events.OfType<Resumed>().Single().RejectedTo);
    }

    // --- The shared fixtures ------------------------------------------------------------------------

    private static Checkpoint LifecycleFixture(string name) =>
        Checkpoint.FromWireJson(ReadFixture(Path.Combine("checkpoint-lifecycle", name)));

    /// <summary>Blanks every level's <c>CreatedAt</c> and <c>DecidedAt</c>, which a live run sets to now.</summary>
    private static JsonObject WithoutTimestamps(Checkpoint checkpoint) => Strip(checkpoint).ToWireJson();

    private static Checkpoint Strip(Checkpoint checkpoint) =>
        checkpoint with
        {
            CreatedAt = string.Empty,
            Decision = checkpoint.Decision is null ? null : checkpoint.Decision with { DecidedAt = string.Empty },
            Nested = checkpoint.Nested is null ? null : Strip(checkpoint.Nested),
        };

    [Fact]
    public async Task Failing_After_An_Approval_Matches_The_Shared_Decision_Failed_Fixture()
    {
        var graph = Remediate(new Seen(), failures: 1).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("thread_decision_demo", Fresh(), store));
        await graph.ResumeAsync("thread_decision_demo", ApproveWithEdits(), store);

        var expected = WithoutTimestamps(LifecycleFixture("decision-failed.json"));
        var actual = WithoutTimestamps(store.Load("thread_decision_demo")!);

        Assert.True(JsonNode.DeepEquals(expected, actual), actual.ToJsonString());
    }

    [Fact]
    public async Task Resuming_The_Shared_Decision_Failed_Fixture_Keeps_The_Approver()
    {
        var graph = Remediate(new Seen()).Compile();
        var store = new InMemoryCheckpointStore();
        var fixture = LifecycleFixture("decision-failed.json");
        store.Save(fixture);

        var state = Completed(await graph.ResumeAsync(fixture.ThreadId, Approve("bob@ops"), store));

        Assert.Equal(new Remediation(new Plan(["drain", "rollback"]), 3, "applied, approved by alice@acme.io").Outcome, state.Outcome);
        Assert.Equal(3, state.Replicas);
        Assert.Equal(["drain", "rollback"], state.Plan!.Steps);
    }

    [Fact]
    public async Task A_Rejection_Failing_Inside_A_Child_Matches_The_Shared_Nested_Decision_Fixture()
    {
        var graph = IncidentAround(ChangeChild(new Seen(), revokeFailures: 1), new Seen()).Compile();
        var store = new InMemoryCheckpointStore();
        Interrupted(await graph.RunAsync("thread_nested_decision_demo", NewIncident(), store));
        Assert.IsType<GraphExecutionResult<Incident>.NodeFailed>(await graph.ResumeAsync(
            "thread_nested_decision_demo", Reject("bob@acme.io").WithOverride("reason", "wrong replica count"), store));

        var expected = WithoutTimestamps(LifecycleFixture("nested-decision.json"));
        var actual = WithoutTimestamps(store.Load("thread_nested_decision_demo")!);

        Assert.True(JsonNode.DeepEquals(expected, actual), actual.ToJsonString());
    }

    [Fact]
    public async Task Resuming_The_Shared_Nested_Decision_Fixture_Re_Enters_The_Reject_Route()
    {
        var seen = new Seen();
        var graph = IncidentAround(ChangeChild(seen), seen).Compile();
        var store = new InMemoryCheckpointStore();
        var fixture = LifecycleFixture("nested-decision.json");
        store.Save(fixture);

        var state = Completed(await graph.ResumeAsync(fixture.ThreadId, Approve("ops"), store));

        Assert.Equal(["prepared", "draft", "approve", "revoke:bob@acme.io", "finished"], state.Log);
        Assert.Equal("wrong replica count", seen.At("Work/Revoke")!.Overrides["reason"].GetString());
    }

    [Fact]
    public void The_Decision_Fixtures_Round_Trip_Through_The_Interop_Types()
    {
        foreach (var name in new[] { "decision-failed.json", "nested-decision.json" })
        {
            var raw = ReadFixture(Path.Combine("checkpoint-lifecycle", name));
            var typed = JsonSerializer.Deserialize<Storage.Schema.Checkpoint>(raw.ToJsonString())!;

            Assert.True(JsonNode.DeepEquals(raw, JsonSerializer.SerializeToNode(typed)), name);
            Assert.True(JsonNode.DeepEquals(raw, Checkpoint.FromWireJson(raw).ToWireJson()), name);
        }
    }
}
