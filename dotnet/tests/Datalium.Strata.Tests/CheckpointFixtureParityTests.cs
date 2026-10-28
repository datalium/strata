using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

namespace Datalium.Strata.Tests;

/// <summary>
/// Proves the Phase 4 exit criterion — a cross-language checkpoint test using the Phase 1
/// shared schema. <c>schema/fixtures/checkpoint-lifecycle/frozen.json</c> is the shared interop
/// artifact: this suite proves the real production store (<see cref="InMemoryCheckpointStore"/>)
/// plus this project's execution engine can both produce a checkpoint matching that fixture and
/// consume it to resume correctly. <c>rust/strata-memory/src/checkpoint.rs</c>'s equivalent
/// suite proves the same two things independently in Rust, against the identical file — that's
/// what "freeze in Rust, resume in C# (or vice versa)" means in a test suite that can't
/// literally pipe one live process into another (the two CI workflows run as separate jobs).
/// </summary>
public class CheckpointFixtureParityTests
{
    private static Checkpoint LoadFixture([CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(
            testDir, "..", "..", "..", "schema", "fixtures", "checkpoint-lifecycle", "frozen.json"));
        var raw = File.ReadAllText(path);
        var doc = JsonNode.Parse(raw)!.AsObject();

        return new Checkpoint
        {
            SchemaVersion = doc["schema_version"]!.GetValue<string>(),
            ThreadId = doc["thread_id"]!.GetValue<string>(),
            Status = Enum.Parse<CheckpointStatus>(doc["status"]!.GetValue<string>(), ignoreCase: true),
            SuspendedNode = doc["suspended_node"]?.GetValue<string>(),
            NextNode = doc["next_node"]?.GetValue<string>(),
            FrozenState = doc["frozen_state"]?.DeepClone(),
            Metadata = null,
            CreatedAt = doc["created_at"]!.GetValue<string>(),
            ResumedAt = doc["resumed_at"]?.GetValue<string>(),
        };
    }

    [Fact]
    public async Task Freezing_The_Wire_Transfer_Graph_Matches_The_Shared_Fixture_Shape()
    {
        var graph = StateGraphInterruptionTests.WireTransferGraph();
        var store = new InMemoryCheckpointStore();

        await graph.RunAsync("thread_hitl_demo", StateGraphInterruptionTests.InitialState(), store);
        var persisted = store.Load("thread_hitl_demo");
        var fixture = LoadFixture();

        Assert.NotNull(persisted);

        // Every field except the timestamps (the fixture's is a fixed historical value; a live
        // freeze uses the real current time).
        Assert.Equal(fixture.SchemaVersion, persisted.SchemaVersion);
        Assert.Equal(fixture.ThreadId, persisted.ThreadId);
        Assert.Equal(fixture.Status, persisted.Status);
        Assert.Equal(fixture.SuspendedNode, persisted.SuspendedNode);
        Assert.Equal(fixture.NextNode, persisted.NextNode);
        Assert.True(JsonNode.DeepEquals(fixture.FrozenState, persisted.FrozenState));
        Assert.Null(persisted.ResumedAt);
    }

    [Fact]
    public async Task Resuming_The_Shared_Fixture_Produces_The_Expected_Completed_State()
    {
        var graph = StateGraphInterruptionTests.WireTransferGraph();
        var store = new InMemoryCheckpointStore();
        var fixture = LoadFixture();
        store.Save(fixture);

        var signal = new HumanSignal(
            IsApproved: true,
            ReviewerToken: "token-abc",
            FieldOverrides: new Dictionary<string, string> { ["approved_by"] = "admin_01" });

        var result = await graph.ResumeAsync(fixture.ThreadId, signal, store);

        var completed = Assert.IsType<GraphExecutionResult<StateGraphInterruptionTests.WireTransferState>.Completed>(result);
        Assert.Equal(
            new StateGraphInterruptionTests.WireTransferState(
                InvoiceGenerated: true, Amount: 4200.5, Currency: "USD", ApprovedBy: "admin_01", IsCompleted: true),
            completed.State);
    }
}
