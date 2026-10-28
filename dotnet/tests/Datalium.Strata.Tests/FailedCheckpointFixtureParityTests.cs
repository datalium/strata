using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

namespace Datalium.Strata.Tests;

/// <summary>
/// Error-recovery counterpart to <see cref="CheckpointFixtureParityTests"/>:
/// <c>schema/fixtures/checkpoint-lifecycle/failed.json</c> is the shared interop artifact for a
/// fallible node that exhausted its retries with no fallback. This suite proves the C# engine can
/// both produce a checkpoint matching that fixture and that <c>ResumeAsync</c> re-enters the failed
/// node from it; <c>rust/strata-memory/src/checkpoint.rs</c>'s equivalent tests prove the same two
/// things independently in Rust, against the identical file.
/// </summary>
public class FailedCheckpointFixtureParityTests
{
    private static Checkpoint LoadFixture([CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(
            testDir, "..", "..", "..", "schema", "fixtures", "checkpoint-lifecycle", "failed.json"));
        var doc = JsonNode.Parse(File.ReadAllText(path))!.AsObject();

        return new Checkpoint
        {
            SchemaVersion = doc["schema_version"]!.GetValue<string>(),
            ThreadId = doc["thread_id"]!.GetValue<string>(),
            Status = Enum.Parse<CheckpointStatus>(doc["status"]!.GetValue<string>(), ignoreCase: true),
            SuspendedNode = doc["suspended_node"]?.GetValue<string>(),
            NextNode = doc["next_node"]?.GetValue<string>(),
            FrozenState = doc["frozen_state"]?.DeepClone(),
            Metadata = doc["metadata"]?.AsObject().ToDictionary(kv => kv.Key, kv => kv.Value!.GetValue<string>()),
            CreatedAt = doc["created_at"]!.GetValue<string>(),
            ResumedAt = doc["resumed_at"]?.GetValue<string>(),
        };
    }

    private static CompiledGraph<StateGraphRecoveryTests.PaymentState> PaymentGraph() =>
        StateGraphRecoveryTests.FallibleOnlyGraph(
            state =>
            {
                // Partial work that must never reach the committed state.
                _ = state with { Charged = true };
                throw NodeException.Transient("gateway timeout");
            },
            RecoveryPolicy.Retry(1));

    [Fact]
    public async Task A_Failed_Node_Produces_A_Checkpoint_Matching_The_Shared_Fixture_Shape()
    {
        var store = new InMemoryCheckpointStore();

        var result = await PaymentGraph().RunAsync(
            "thread_recovery_demo", StateGraphRecoveryTests.InitialState(), store);
        Assert.IsAssignableFrom<GraphExecutionResult<StateGraphRecoveryTests.PaymentState>.Failed>(result);

        var persisted = store.Load("thread_recovery_demo");
        var fixture = LoadFixture();

        Assert.NotNull(persisted);

        // Every field except the timestamp (the fixture's is a fixed historical value; a live
        // failure uses the real current time).
        Assert.Equal(fixture.SchemaVersion, persisted.SchemaVersion);
        Assert.Equal(fixture.ThreadId, persisted.ThreadId);
        Assert.Equal(CheckpointStatus.Failed, persisted.Status);
        Assert.Equal(fixture.Status, persisted.Status);
        Assert.Equal(fixture.SuspendedNode, persisted.SuspendedNode);
        Assert.Equal(fixture.NextNode, persisted.NextNode);
        Assert.True(JsonNode.DeepEquals(fixture.FrozenState, persisted.FrozenState));
        Assert.Equal(fixture.Metadata, persisted.Metadata);
        Assert.Equal(fixture.ResumedAt, persisted.ResumedAt);
    }

    [Fact]
    public async Task Resume_Re_Enters_The_Failed_Node_Of_The_Shared_Failed_Fixture()
    {
        // The same graph shape, with `Charge` working now.
        var graph = new StateGraph<StateGraphRecoveryTests.PaymentState>()
            .AddNode("Prepare", s => s with { Notes = [.. s.Notes, "prepared"] })
            .AddFallibleNode("Charge", s => s with { Charged = true, Notes = [.. s.Notes, "charged"] })
            .AddEdge("Prepare", "Charge")
            .Compile();
        var store = new InMemoryCheckpointStore();
        var fixture = LoadFixture();
        store.Save(fixture);

        var result = await graph.ResumeAsync(
            fixture.ThreadId, new HumanSignal(IsApproved: true, ReviewerToken: "x", FieldOverrides: null), store);

        // `Prepare` did not run again: the fixture's state is the one it had when `Charge` failed.
        var state = Assert.IsType<GraphExecutionResult<StateGraphRecoveryTests.PaymentState>.Completed>(result).State;
        Assert.True(state.Prepared && state.Charged);
        Assert.Equal(["charged"], state.Notes);
    }
}
