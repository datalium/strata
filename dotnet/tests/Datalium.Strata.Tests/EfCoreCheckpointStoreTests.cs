using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage.Memory;
using Microsoft.EntityFrameworkCore;

namespace Datalium.Strata.Tests;

/// <summary>
/// Coverage for <see cref="EfCoreCheckpointStore"/>: the same save/load/overwrite contract
/// <see cref="Storage.InMemoryCheckpointStore"/> gets exercised for implicitly via
/// <see cref="StateGraphInterruptionTests"/>, the same shared-fixture parity
/// <see cref="CheckpointFixtureParityTests"/> proves for the in-memory store, and a new test
/// proving this adapter's actual point -- persistence across independent store instances
/// pointed at the same SQLite file, standing in for a process restart.
/// </summary>
public class EfCoreCheckpointStoreTests
{
    private static Checkpoint SampleCheckpoint(string threadId = "thread-1") => new()
    {
        SchemaVersion = "1.0.0",
        ThreadId = threadId,
        Status = CheckpointStatus.Interrupted,
        SuspendedNode = "PrepareWireTransfer",
        NextNode = "ExecuteWireTransfer",
        FrozenState = JsonNode.Parse("""{"invoice_generated":true,"amount":4200.5}"""),
        Metadata = null,
        CreatedAt = "2026-09-17T00:00:00Z",
        ResumedAt = null,
    };

    [Fact]
    public void Save_Then_Load_Round_Trips_The_Checkpoint()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        var checkpoint = SampleCheckpoint();

        store.Save(checkpoint);
        var loaded = store.Load(checkpoint.ThreadId);

        Assert.NotNull(loaded);
        Assert.Equal(checkpoint.SchemaVersion, loaded.SchemaVersion);
        Assert.Equal(checkpoint.ThreadId, loaded.ThreadId);
        Assert.Equal(checkpoint.Status, loaded.Status);
        Assert.Equal(checkpoint.SuspendedNode, loaded.SuspendedNode);
        Assert.Equal(checkpoint.NextNode, loaded.NextNode);
        Assert.True(JsonNode.DeepEquals(checkpoint.FrozenState, loaded.FrozenState));
        Assert.Equal(checkpoint.CreatedAt, loaded.CreatedAt);
        Assert.Null(loaded.ResumedAt);
    }

    [Fact]
    public void Load_Of_A_Missing_Thread_Returns_Null()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        Assert.Null(store.Load("does-not-exist"));
    }

    [Fact]
    public void Save_Overwrites_An_Existing_Checkpoint_For_The_Same_Thread()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        var checkpoint = SampleCheckpoint();
        store.Save(checkpoint);

        var resumed = checkpoint with { Status = CheckpointStatus.Completed, ResumedAt = "2026-09-17T01:00:00Z" };
        store.Save(resumed);

        var loaded = store.Load(checkpoint.ThreadId);
        Assert.NotNull(loaded);
        Assert.Equal(CheckpointStatus.Completed, loaded.Status);
        Assert.Equal("2026-09-17T01:00:00Z", loaded.ResumedAt);
    }

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
        using var store = EfCoreCheckpointStore.CreateInMemory();

        await graph.RunAsync("thread_hitl_demo", StateGraphInterruptionTests.InitialState(), store);
        var persisted = store.Load("thread_hitl_demo");
        var fixture = LoadFixture();

        Assert.NotNull(persisted);
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
        using var store = EfCoreCheckpointStore.CreateInMemory();
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

    [Fact]
    public void Checkpoint_Persists_Across_Independent_Store_Instances_At_The_Same_File()
    {
        var path = Path.Combine(Path.GetTempPath(), $"strata-checkpoint-test-{Guid.NewGuid():N}.db");
        try
        {
            // Pooling=False so Microsoft.Data.Sqlite doesn't keep the underlying file handle
            // open in its connection pool after Dispose() -- otherwise the temp file cleanup
            // below can race a still-open pooled handle on Windows.
            var options = new DbContextOptionsBuilder<CheckpointDbContext>()
                .UseSqlite($"Data Source={path};Pooling=False")
                .Options;

            var checkpoint = SampleCheckpoint("thread-restart");
            using (var writer = new EfCoreCheckpointStore(new CheckpointDbContext(options)))
            {
                writer.Save(checkpoint);
            }

            // A fresh DbContextOptions + EfCoreCheckpointStore pointed at the same file, standing
            // in for a process restart: nothing in-memory survives between the two `using` blocks.
            var reopenedOptions = new DbContextOptionsBuilder<CheckpointDbContext>()
                .UseSqlite($"Data Source={path};Pooling=False")
                .Options;
            using (var reader = new EfCoreCheckpointStore(new CheckpointDbContext(reopenedOptions)))
            {
                var loaded = reader.Load("thread-restart");

                Assert.NotNull(loaded);
                Assert.Equal(checkpoint.ThreadId, loaded.ThreadId);
                Assert.Equal(checkpoint.Status, loaded.Status);
                Assert.Equal(checkpoint.SuspendedNode, loaded.SuspendedNode);
                Assert.Equal(checkpoint.NextNode, loaded.NextNode);
                Assert.True(JsonNode.DeepEquals(checkpoint.FrozenState, loaded.FrozenState));
            }
        }
        finally
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }
}
