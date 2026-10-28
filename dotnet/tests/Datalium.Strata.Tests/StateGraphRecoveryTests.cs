using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;
using Datalium.Strata.Validation;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors the error-recovery tests in <c>rust/strata-core/src/checkpoint.rs</c>
/// (<c>design/04-Error-Recovery.md</c>): retry a transient failure, fall back on exhaustion or a
/// fatal error, and persist a <c>Failed</c> checkpoint holding the last committed state when
/// there's no fallback.
/// </summary>
public class StateGraphRecoveryTests
{
    public sealed record PaymentState(
        [property: JsonPropertyName("prepared")] bool Prepared,
        [property: JsonPropertyName("charged")] bool Charged,
        [property: JsonPropertyName("notes")] IReadOnlyList<string> Notes);

    internal static PaymentState InitialState() => new(Prepared: false, Charged: false, Notes: []);

    /// <summary>
    /// A fallible node that counts its calls and, for the first <paramref name="failures"/> of
    /// them, throws <paramref name="error"/> — after building a modified state, to prove that work
    /// never reaches the committed state.
    /// </summary>
    private static Func<PaymentState, PaymentState> Charge(Counter calls, int failures, NodeException error) =>
        state =>
        {
            calls.Value++;
            var partial = state with
            {
                Charged = true,
                Notes = [.. state.Notes, $"partial work in call {calls.Value}"],
            };
            return calls.Value <= failures ? throw error : partial;
        };

    private sealed class Counter
    {
        public int Value { get; set; }
    }

    /// <summary><c>Prepare</c> -> <c>Charge</c>, with no fallback node, so there is no orphan whatever the policy is.</summary>
    internal static CompiledGraph<PaymentState> FallibleOnlyGraph(
        Func<PaymentState, PaymentState> charge, RecoveryPolicy policy) =>
        new StateGraph<PaymentState>()
            .AddNode("Prepare", s => s with { Prepared = true })
            .AddFallibleNode("Charge", charge)
            .AddEdge("Prepare", "Charge")
            .SetRecoveryPolicy("Charge", policy)
            .Compile();

    /// <summary>Same, plus a <c>Fallback</c> node that has no ordinary edge into it.</summary>
    private static StateGraph<PaymentState> GraphWithFallbackNode(Func<PaymentState, PaymentState> charge) =>
        new StateGraph<PaymentState>()
            .AddNode("Prepare", s => s with { Prepared = true })
            .AddFallibleNode("Charge", charge)
            .AddNode("Fallback", s => s with { Notes = [.. s.Notes, "fell back"] })
            .AddEdge("Prepare", "Charge");

    private static NodeException Transient() => NodeException.Transient("gateway timeout");

    [Fact]
    public async Task A_Transient_Failure_Is_Retried_Until_It_Succeeds()
    {
        var calls = new Counter();
        var graph = FallibleOnlyGraph(Charge(calls, failures: 2, Transient()), RecoveryPolicy.Retry(3));

        var result = await graph.RunAsync("t", InitialState(), new InMemoryCheckpointStore());

        var completed = Assert.IsType<GraphExecutionResult<PaymentState>.Completed>(result);
        Assert.True(completed.State.Charged);
        // Only the successful third attempt's work was committed.
        Assert.Equal(["partial work in call 3"], completed.State.Notes);
        Assert.Equal(3, calls.Value);
    }

    [Fact]
    public async Task Exhausted_Retries_Route_To_The_Fallback_With_The_Pre_Failure_State()
    {
        var calls = new Counter();
        var graph = GraphWithFallbackNode(Charge(calls, failures: int.MaxValue, Transient()))
            .SetRecoveryPolicy("Charge", RecoveryPolicy.Retry(2).WithFallback("Fallback"))
            .Compile();

        var result = await graph.RunAsync("t", InitialState(), new InMemoryCheckpointStore());

        var completed = Assert.IsType<GraphExecutionResult<PaymentState>.Completed>(result);
        // `Prepare` committed, `Charge`'s partial work was rolled back, `Fallback` ran on what was left.
        Assert.True(completed.State.Prepared);
        Assert.False(completed.State.Charged);
        Assert.Equal(["fell back"], completed.State.Notes);
        // 1 initial attempt + 2 retries.
        Assert.Equal(3, calls.Value);
    }

    [Fact]
    public async Task A_Fatal_Error_Skips_Retries_And_Goes_Straight_To_The_Fallback()
    {
        var calls = new Counter();
        var graph = GraphWithFallbackNode(Charge(calls, failures: int.MaxValue, NodeException.Fatal("card declined")))
            .SetRecoveryPolicy("Charge", RecoveryPolicy.Retry(5).WithFallback("Fallback"))
            .Compile();

        var result = await graph.RunAsync("t", InitialState(), new InMemoryCheckpointStore());

        Assert.IsType<GraphExecutionResult<PaymentState>.Completed>(result);
        Assert.Equal(1, calls.Value);
    }

    [Fact]
    public async Task Terminal_Failure_Without_A_Fallback_Persists_A_Failed_Checkpoint()
    {
        var graph = FallibleOnlyGraph(Charge(new Counter(), int.MaxValue, Transient()), RecoveryPolicy.Retry(1));
        var store = new InMemoryCheckpointStore();

        var result = await graph.RunAsync("thread_failed", InitialState(), store);

        var failed = Assert.IsType<GraphExecutionResult<PaymentState>.NodeFailed>(result);
        Assert.Equal("Charge", failed.Node);
        Assert.Equal(2, failed.Attempts);
        Assert.Equal("gateway timeout", failed.Message);

        var persisted = store.Load("thread_failed");
        Assert.NotNull(persisted);
        Assert.Equal(CheckpointStatus.Failed, persisted.Status);
        Assert.Equal("Charge", persisted.SuspendedNode);
        Assert.Equal("Charge", persisted.NextNode);
        // Rollback: the last committed state (after `Prepare`), not the failed attempts' partial work.
        Assert.True(JsonNode.DeepEquals(
            JsonNode.Parse("""{ "prepared": true, "charged": false, "notes": [] }"""), persisted.FrozenState));
        Assert.NotNull(persisted.Metadata);
        Assert.Equal("gateway timeout", persisted.Metadata["error"]);
        Assert.Equal("transient", persisted.Metadata["error_kind"]);
        Assert.Equal("2", persisted.Metadata["attempts"]);
        Assert.Null(persisted.ResumedAt);
    }

    [Fact]
    public async Task A_Fallible_Node_With_The_Default_Policy_Fails_On_The_First_Error()
    {
        var calls = new Counter();
        var graph = FallibleOnlyGraph(Charge(calls, int.MaxValue, Transient()), RecoveryPolicy.Default);
        var store = new InMemoryCheckpointStore();

        var result = await graph.RunAsync("t", InitialState(), store);

        var failed = Assert.IsType<GraphExecutionResult<PaymentState>.NodeFailed>(result);
        Assert.Equal(1, failed.Attempts);
        Assert.Equal(1, calls.Value);
        Assert.Equal(CheckpointStatus.Failed, store.Load("t")!.Status);
    }

    [Fact]
    public async Task A_NodeFailed_Result_Is_Also_A_Failed_Result()
    {
        var graph = FallibleOnlyGraph(Charge(new Counter(), int.MaxValue, Transient()), RecoveryPolicy.Default);

        var result = await graph.RunAsync("t", InitialState(), new InMemoryCheckpointStore());

        var failed = Assert.IsAssignableFrom<GraphExecutionResult<PaymentState>.Failed>(result);
        Assert.Equal("gateway timeout", failed.Message);
    }

    private static HumanSignal NoSignal() => new(IsApproved: true, ReviewerToken: "x", FieldOverrides: null);

    [Fact]
    public async Task A_Failed_Checkpoint_Resumes_From_The_Node_That_Failed_With_Its_Committed_State()
    {
        // Only the first call to `Charge` fails; the resume is the second.
        var calls = new Counter();
        var graph = FallibleOnlyGraph(Charge(calls, failures: 1, Transient()), RecoveryPolicy.Default);
        var store = new InMemoryCheckpointStore();
        Assert.IsType<GraphExecutionResult<PaymentState>.NodeFailed>(await graph.RunAsync("t", InitialState(), store));

        var result = await graph.ResumeAsync("t", NoSignal(), store);

        // `Prepare` did not run again; nothing from the failed call leaked.
        var completed = Assert.IsType<GraphExecutionResult<PaymentState>.Completed>(result).State;
        Assert.True(completed.Prepared && completed.Charged);
        Assert.Equal(["partial work in call 2"], completed.Notes);
        Assert.Equal(2, calls.Value);
        Assert.NotNull(store.Load("t")!.ResumedAt);
    }

    [Fact]
    public async Task Resuming_A_Failed_Checkpoint_Applies_The_Field_Overrides()
    {
        var graph = FallibleOnlyGraph(Charge(new Counter(), failures: 1, Transient()), RecoveryPolicy.Default);
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync("t", InitialState(), store);

        // The override is a string in a bool field, so rehydrating refuses it: proof the override
        // reached the failed node's state.
        var signal = new HumanSignal(true, "op", new Dictionary<string, string> { ["prepared"] = "not-a-bool" });
        var result = await graph.ResumeAsync("t", signal, store);

        Assert.Contains("failed to rehydrate", Assert.IsType<GraphExecutionResult<PaymentState>.Failed>(result).Message);
    }

    [Fact]
    public async Task A_Failed_Checkpoint_Can_Only_Be_Resumed_Once_Per_Failure()
    {
        // The first two calls to `Charge` fail; the third succeeds.
        var graph = FallibleOnlyGraph(Charge(new Counter(), failures: 2, Transient()), RecoveryPolicy.Default);
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync("t", InitialState(), store);

        // The resume fails again, which writes a fresh `Failed` checkpoint...
        Assert.IsType<GraphExecutionResult<PaymentState>.NodeFailed>(await graph.ResumeAsync("t", NoSignal(), store));
        var again = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Failed, again.Status);
        Assert.Null(again.ResumedAt);

        // ...which can be resumed in turn. Once that succeeds, the failure it recovered from is spent.
        Assert.IsType<GraphExecutionResult<PaymentState>.Completed>(await graph.ResumeAsync("t", NoSignal(), store));
        var result = await graph.ResumeAsync("t", NoSignal(), store);
        Assert.Contains("already resumed", Assert.IsType<GraphExecutionResult<PaymentState>.Failed>(result).Message);
    }

    [Fact]
    public async Task A_Completed_Checkpoint_Cannot_Be_Resumed()
    {
        var graph = FallibleOnlyGraph(Charge(new Counter(), failures: 0, Transient()), RecoveryPolicy.Default);
        var store = new InMemoryCheckpointStore();
        store.Save(new Checkpoint
        {
            SchemaVersion = CheckpointSchema.Version,
            ThreadId = "t",
            Status = CheckpointStatus.Completed,
            FrozenState = JsonSerializer.SerializeToNode(InitialState()),
            CreatedAt = "2026-09-21T00:00:00Z",
        });

        var result = await graph.ResumeAsync("t", NoSignal(), store);

        Assert.Contains("not resumable", Assert.IsType<GraphExecutionResult<PaymentState>.Failed>(result).Message);
    }

    [Fact]
    public async Task An_Exception_That_Is_Not_A_NodeException_Propagates_Like_A_Plain_Node()
    {
        var graph = FallibleOnlyGraph(_ => throw new InvalidOperationException("bug"), RecoveryPolicy.Retry(3));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => graph.RunAsync("t", InitialState(), new InMemoryCheckpointStore()));
    }

    [Fact]
    public void Compile_Rejects_A_Recovery_Policy_On_A_Node_That_Is_Not_Fallible()
    {
        var graph = new StateGraph<int>()
            .AddNode("A", s => s)
            .SetRecoveryPolicy("A", RecoveryPolicy.Retry(1));

        var ex = Assert.Throws<InvalidOperationException>(graph.Compile);
        Assert.Contains("'A'", ex.Message);
    }

    [Fact]
    public void Compile_Rejects_A_Fallback_That_Names_A_Missing_Node()
    {
        var graph = new StateGraph<int>()
            .AddFallibleNode("A", s => s)
            .SetRecoveryPolicy("A", RecoveryPolicy.Retry(1).WithFallback("Nowhere"));

        var ex = Assert.Throws<InvalidOperationException>(graph.Compile);
        Assert.Contains("Nowhere", ex.Message);
    }

    [Fact]
    public void A_Fallback_Only_Node_Is_Reachable_Only_Through_The_Recovery_Policy()
    {
        // `Exhausted_Retries_Route_To_The_Fallback...` already relies on "Fallback" having no
        // ordinary edge into it; assert the reverse too — without the fallback policy the same
        // node *is* an orphan.
        var charge = Charge(new Counter(), failures: 0, Transient());

        GraphWithFallbackNode(charge)
            .SetRecoveryPolicy("Charge", RecoveryPolicy.Retry(1).WithFallback("Fallback"))
            .Compile();

        var ex = Assert.Throws<GraphCompilationException>(GraphWithFallbackNode(charge).Compile);
        Assert.Contains(ex.Result.Errors, e => e.Code == "STRATA_ERR_ORPHAN" && e.Node == "Fallback");
    }

    [Fact]
    public async Task RunStreaming_Emits_An_Error_Chunk_When_A_Fallible_Node_Gives_Up()
    {
        var graph = GraphWithFallbackNode(Charge(new Counter(), int.MaxValue, Transient()))
            .SetRecoveryPolicy("Charge", RecoveryPolicy.Retry(1).WithFallback("Fallback"))
            .Compile();

        var (reader, completion) = graph.RunStreaming("t", InitialState(), new InMemoryCheckpointStore());
        var errors = new List<(string Node, string Message)>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            if (chunk.Type == StreamChunkType.Error)
            {
                errors.Add((chunk.NodeName, chunk.Payload.ToString()));
            }
        }

        Assert.IsType<GraphExecutionResult<PaymentState>.Completed>(await completion);
        Assert.Equal([("Charge", "gateway timeout")], errors);
    }
}
