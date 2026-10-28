using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

namespace Datalium.Strata.Tests;

/// <summary>
/// Cancellation counterpart to <see cref="FailedCheckpointFixtureParityTests"/>
/// (<c>design/17-Async-Execution.md</c>): <c>schema/fixtures/checkpoint-lifecycle/cancelled.json</c>
/// is the shared interop artifact for a run cancelled while a node was running, and
/// <c>nested-cancelled.json</c> for one cancelled while a subgraph's child was. This suite proves the
/// C# engine produces a checkpoint matching each (timestamps aside, at every level) from a live
/// cancelled run, and resumes each to the expected final state;
/// <c>rust/strata-core/tests/cancellation.rs</c> proves the same in Rust against the identical files.
/// </summary>
public class CancelledCheckpointFixtureParityTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Payment(
        [property: JsonPropertyName("prepared")] bool Prepared,
        [property: JsonPropertyName("charged")] bool Charged,
        [property: JsonPropertyName("notes")] IReadOnlyList<string> Notes);

    public sealed record Order(
        [property: JsonPropertyName("amount")] int Amount,
        [property: JsonPropertyName("log")] IReadOnlyList<string> Log,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy);

    public sealed record Job(
        [property: JsonPropertyName("n")] int N,
        [property: JsonPropertyName("trail")] IReadOnlyList<string> Trail,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy);

    private static JsonNode LoadFixture(string name, [CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(
            testDir, "..", "..", "..", "schema", "fixtures", "checkpoint-lifecycle", name));
        return JsonNode.Parse(File.ReadAllText(path))!;
    }

    /// <summary>Blanks every level's <c>CreatedAt</c>: the fixture's is a fixed value, a live run's the real time.</summary>
    private static Checkpoint WithoutTimestamps(Checkpoint checkpoint) =>
        checkpoint with
        {
            CreatedAt = string.Empty,
            Nested = checkpoint.Nested is null ? null : WithoutTimestamps(checkpoint.Nested),
        };

    private static void AssertMatchesFixture(string name, Checkpoint persisted)
    {
        var expected = WithoutTimestamps(Checkpoint.FromWireJson(LoadFixture(name))).ToWireJson();
        var actual = WithoutTimestamps(persisted).ToWireJson();
        Assert.True(JsonNode.DeepEquals(expected, actual), actual.ToJsonString());
    }

    /// <summary>
    /// An async handler that signals it started and then waits until it is cancelled, or (once
    /// <paramref name="working"/>) returns <paramref name="done"/>'s result.
    /// </summary>
    private static Func<T, NodeContext, CancellationToken, ValueTask<T>> HangUnless<T>(
        Func<bool> working, Func<T, T> done, TaskCompletionSource started) =>
        async (state, _, ct) =>
        {
            if (working())
            {
                return done(state);
            }

            started.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return state;
        };

    /// <summary><c>Prepare</c> -> <c>Charge</c>: what <c>cancelled.json</c> records, cancelled at <c>Charge</c>.</summary>
    private static CompiledGraph<Payment> PaymentGraph(Func<bool> chargeWorks, TaskCompletionSource started) =>
        new StateGraph<Payment>()
            .AddNode("Prepare", p => p with { Prepared = true, Notes = [.. p.Notes, "prepared"] })
            .AddNode("Charge", HangUnless<Payment>(
                chargeWorks, p => p with { Charged = true, Notes = [.. p.Notes, "charged"] }, started))
            .AddEdge("Prepare", "Charge")
            .Compile();

    /// <summary>
    /// <c>Prepare</c> -> <c>Work</c> (child: <c>Draft</c> -> <c>Send</c>) -> <c>Finish</c>: what
    /// <c>nested-cancelled.json</c> records, cancelled at <c>Send</c>. The shapes are those of
    /// <c>nested.json</c>'s graph (<see cref="NestedInterruptTests"/>), without the boundary.
    /// </summary>
    private static CompiledGraph<Order> OrderGraph(Func<bool> sendWorks, TaskCompletionSource started)
    {
        var child = new StateGraph<Job>()
            .AddNode("Draft", j => j with { N = j.N * 2, Trail = [.. j.Trail, "draft"] })
            .AddNode("Send", HangUnless<Job>(
                sendWorks, j => j with { N = j.N + 1, Trail = [.. j.Trail, "send"] }, started))
            .AddEdge("Draft", "Send")
            .Compile();
        return new StateGraph<Order>()
            .AddNode("Prepare", o => o with { Log = [.. o.Log, "prepared"] })
            .AddSubgraph(
                "Work",
                child,
                o => new Job(o.Amount, [], null),
                (entry, job) => entry with { Amount = job.N, Log = [.. entry.Log, .. job.Trail], ApprovedBy = job.ApprovedBy })
            .AddNode("Finish", o => o with { Log = [.. o.Log, "finished"] })
            .AddEdge("Prepare", "Work")
            .AddEdge("Work", "Finish")
            .Compile();
    }

    /// <summary>Runs <paramref name="run"/>, cancels it once <paramref name="started"/>, and returns its result.</summary>
    private static async Task<GraphExecutionResult<T>> CancelWhenStarted<T>(
        Func<CancellationToken, Task<GraphExecutionResult<T>>> run, TaskCompletionSource started)
    {
        using var cts = new CancellationTokenSource();
        var task = run(cts.Token);
        await started.Task.WaitAsync(Patience);
        await cts.CancelAsync();
        return await task.WaitAsync(Patience);
    }

    [Fact]
    public async Task Cancelling_Produces_A_Checkpoint_Matching_The_Shared_Fixture()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var graph = PaymentGraph(() => false, started);
        var store = new InMemoryCheckpointStore();

        var result = await CancelWhenStarted(
            ct => graph.RunAsync("thread_cancelled_demo", new Payment(false, false, []), store, ct), started);

        Assert.IsType<GraphExecutionResult<Payment>.Cancelled>(result);
        AssertMatchesFixture("cancelled.json", store.Load("thread_cancelled_demo")!);
    }

    [Fact]
    public async Task Resuming_The_Shared_Fixture_Re_Enters_The_Node_That_Was_Stopped()
    {
        var graph = PaymentGraph(() => true, new TaskCompletionSource());
        var store = new InMemoryCheckpointStore();
        var fixture = Checkpoint.FromWireJson(LoadFixture("cancelled.json"));
        store.Save(fixture);

        var state = Assert.IsType<GraphExecutionResult<Payment>.Completed>(
            await graph.ResumeAsync(fixture.ThreadId, new HumanSignal(true, "x"), store)).State;

        // `Prepare` did not run again: its note is the fixture's.
        Assert.True(state.Prepared && state.Charged);
        Assert.Equal(["prepared", "charged"], state.Notes);
    }

    [Fact]
    public async Task Cancelling_Inside_A_Subgraph_Matches_The_Shared_Nested_Fixture_At_Every_Level()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var graph = OrderGraph(() => false, started);
        var store = new InMemoryCheckpointStore();

        var result = await CancelWhenStarted(
            ct => graph.RunAsync("thread_nested_cancelled_demo", new Order(5, [], null), store, ct), started);

        Assert.Equal(["Work", "Send"], Assert.IsType<GraphExecutionResult<Order>.Cancelled>(result).StoppedAt);
        AssertMatchesFixture("nested-cancelled.json", store.Load("thread_nested_cancelled_demo")!);
    }

    [Fact]
    public async Task Resuming_The_Shared_Nested_Fixture_Re_Enters_The_Child_Where_It_Was_Stopped()
    {
        var graph = OrderGraph(() => true, new TaskCompletionSource());
        var store = new InMemoryCheckpointStore();
        var fixture = Checkpoint.FromWireJson(LoadFixture("nested-cancelled.json"));
        store.Save(fixture);

        var signal = new HumanSignal(true, "x", new Dictionary<string, string> { ["approved_by"] = "admin_01" });
        var state = Assert.IsType<GraphExecutionResult<Order>.Completed>(
            await graph.ResumeAsync(fixture.ThreadId, signal, store)).State;

        // Draft did not run again (n is 10 from the fixture, +1 by Send), and the override reached the child.
        Assert.Equal(11, state.Amount);
        Assert.Equal(["prepared", "draft", "send", "finished"], state.Log);
        Assert.Equal("admin_01", state.ApprovedBy);
    }

    [Fact]
    public void The_Shared_Fixtures_Round_Trip_Through_The_Interop_Types()
    {
        var raw = LoadFixture("cancelled.json");

        var checkpoint = raw.Deserialize<Datalium.Strata.Storage.Schema.Checkpoint>()!;

        Assert.Equal(Datalium.Strata.Storage.Schema.CheckpointStatus.Cancelled, checkpoint.Status);
        Assert.True(JsonNode.DeepEquals(raw, JsonSerializer.SerializeToNode(checkpoint)));
    }
}
