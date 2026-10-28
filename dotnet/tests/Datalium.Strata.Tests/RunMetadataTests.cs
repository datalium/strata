using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/run_metadata.rs</c> (<c>design/18-Run-Observability.md</c>, SG-14):
/// caller metadata is stamped on every checkpoint a run writes, at every nesting level, beside the
/// engine's own keys; reserved keys are refused before anything is written; a resume carries the
/// stored metadata forward and lays its own over it. The shared fixtures
/// <c>metadata-nested.json</c> and <c>metadata-failed.json</c> are produced and resumed by both engines.
/// Keep the two in step.
/// </summary>
public class RunMetadataTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Order(
        [property: JsonPropertyName("amount")] int Amount,
        [property: JsonPropertyName("log")] IReadOnlyList<string> Log,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy);

    public sealed record Job(
        [property: JsonPropertyName("n")] int N,
        [property: JsonPropertyName("trail")] IReadOnlyList<string> Trail,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy);

    private static Order NewOrder(int amount) => new(amount, [], null);

    private static RunOptions With(params (string Key, string Value)[] metadata) =>
        new() { Metadata = metadata.ToDictionary(m => m.Key, m => m.Value) };

    private static HumanSignal NoSignal() => new(IsApproved: true, ReviewerToken: "x");

    private static HumanSignal ApproveAs(string who) =>
        new(true, "token", new Dictionary<string, string> { ["approved_by"] = who });

    /// <summary>
    /// <c>Draft</c> (n * 2) -> <c>Approve</c> (a boundary) -> <c>Send</c> (n + 1), which records the
    /// metadata it was handed in <paramref name="seen"/>, or fails fatally when <paramref name="sendFails"/>.
    /// </summary>
    private static CompiledGraph<Job> ReviewChild(bool sendFails = false, ConcurrentQueue<IReadOnlyDictionary<string, string>>? seen = null) =>
        new StateGraph<Job>()
            .AddNode("Draft", job => job with { N = job.N * 2, Trail = [.. job.Trail, "draft"] })
            .AddNode("Approve", job => job with { Trail = [.. job.Trail, "approve"] })
            .AddInterruptBoundary("Approve")
            .AddFallibleNode("Send", (job, ctx, _) =>
            {
                seen?.Enqueue(ctx.Metadata);
                return sendFails
                    ? throw NodeException.Fatal("mailbox closed")
                    : ValueTask.FromResult(job with { N = job.N + 1, Trail = [.. job.Trail, "send"] });
            })
            .AddEdge("Draft", "Approve")
            .AddEdge("Approve", "Send")
            .Compile();

    /// <summary><c>Prepare</c> -> <c>Work</c> (the subgraph) -> <c>Finish</c>, the shape of <c>nested.json</c>.</summary>
    private static CompiledGraph<Order> Parent(CompiledGraph<Job> child) =>
        new StateGraph<Order>()
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

    private static JsonNode LoadFixture(string name, [CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(
            testDir, "..", "..", "..", "schema", "fixtures", "checkpoint-lifecycle", name));
        return JsonNode.Parse(File.ReadAllText(path))!;
    }

    private static Checkpoint WithoutTimestamps(Checkpoint checkpoint) =>
        checkpoint with
        {
            CreatedAt = string.Empty,
            Decision = checkpoint.Decision is null ? null : checkpoint.Decision with { DecidedAt = string.Empty },
            Nested = checkpoint.Nested is null ? null : WithoutTimestamps(checkpoint.Nested),
        };

    private static void AssertMatchesFixture(string name, Checkpoint persisted)
    {
        var expected = WithoutTimestamps(Checkpoint.FromWireJson(LoadFixture(name))).ToWireJson();
        var actual = WithoutTimestamps(persisted).ToWireJson();
        Assert.True(JsonNode.DeepEquals(expected, actual), actual.ToJsonString());
    }

    // --- Every checkpoint kind ---------------------------------------------------------------------

    [Fact]
    public async Task An_Interrupted_Checkpoint_Carries_The_Run_Metadata()
    {
        var graph = new StateGraph<Job>()
            .AddNode("Approve", job => job)
            .AddInterruptBoundary("Approve")
            .AddNode("Send", job => job)
            .AddEdge("Approve", "Send")
            .Compile();
        var store = new InMemoryCheckpointStore();

        await graph.RunAsync("t", new Job(1, [], null), store, With(("tenant", "acme")));

        Assert.Equal(new Dictionary<string, string> { ["tenant"] = "acme" }, store.Load("t")!.Metadata);
    }

    [Fact]
    public async Task A_Failed_Checkpoint_Merges_The_Run_Metadata_With_The_Engine_Keys()
    {
        var graph = new StateGraph<Job>()
            .AddFallibleNode("Send", (Func<Job, Job>)(_ => throw NodeException.Transient("gateway timeout")))
            .Compile();
        var store = new InMemoryCheckpointStore();

        await graph.RunAsync("t", new Job(1, [], null), store, With(("tenant", "acme")));

        Assert.Equal(
            new Dictionary<string, string>
            {
                ["tenant"] = "acme",
                ["error"] = "gateway timeout",
                ["error_kind"] = "transient",
                ["attempts"] = "1",
            },
            store.Load("t")!.Metadata);
    }

    [Fact]
    public async Task A_Cancelled_Checkpoint_Merges_The_Run_Metadata_With_The_Cancel_Reason()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var graph = new StateGraph<Job>()
            .AddNode("Send", async (Job job, NodeContext _, CancellationToken ct) =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, ct);
                return job;
            })
            .Compile();
        var store = new InMemoryCheckpointStore();
        using var cts = new CancellationTokenSource();

        var run = graph.RunAsync("t", new Job(1, [], null), store, With(("tenant", "acme")), cts.Token);
        await started.Task.WaitAsync(Patience);
        await cts.CancelAsync();
        Assert.IsType<GraphExecutionResult<Job>.Cancelled>(await run.WaitAsync(Patience));

        Assert.Equal(
            new Dictionary<string, string> { ["tenant"] = "acme", ["cancel_reason"] = "requested" },
            store.Load("t")!.Metadata);
    }

    [Fact]
    public async Task Without_Metadata_An_Interrupted_Checkpoint_Still_Has_None()
    {
        var store = new InMemoryCheckpointStore();

        await Parent(ReviewChild()).RunAsync("t", NewOrder(5), store);

        var checkpoint = store.Load("t")!;
        Assert.Null(checkpoint.Metadata);
        Assert.Null(checkpoint.Nested!.Metadata);
    }

    // --- Reserved keys -----------------------------------------------------------------------------

    [Theory]
    [InlineData("error")]
    [InlineData("error_kind")]
    [InlineData("attempts")]
    [InlineData("cancel_reason")]
    [InlineData("strata.run_id")]
    [InlineData("strata.")]
    public async Task A_Reserved_Key_Is_Refused_Before_Anything_Is_Written(string key)
    {
        var store = new InMemoryCheckpointStore();
        var graph = Parent(ReviewChild());

        var error = await Assert.ThrowsAsync<ArgumentException>(
            () => graph.RunAsync("t", NewOrder(5), store, With((key, "x"))));

        Assert.StartsWith($"run metadata key '{key}' is reserved", error.Message);
        Assert.Null(store.Load("t"));
    }

    [Fact]
    public async Task An_Empty_Key_Is_Refused()
    {
        var error = await Assert.ThrowsAsync<ArgumentException>(
            () => Parent(ReviewChild()).RunAsync("t", NewOrder(5), new InMemoryCheckpointStore(), With(("", "x"))));

        Assert.StartsWith("run metadata keys can't be empty", error.Message);
    }

    [Fact]
    public async Task A_Resume_With_A_Reserved_Key_Is_Refused_Before_The_Checkpoint_Is_Stamped()
    {
        var store = new InMemoryCheckpointStore();
        var graph = Parent(ReviewChild());
        await graph.RunAsync("t", NewOrder(5), store);

        await Assert.ThrowsAsync<ArgumentException>(
            () => graph.ResumeAsync("t", NoSignal(), store, With(("error", "x"))));

        Assert.Null(store.Load("t")!.ResumedAt);
    }

    [Fact]
    public void A_Streaming_Run_With_A_Reserved_Key_Throws_At_The_Call()
    {
        var graph = Parent(ReviewChild());

        Assert.Throws<ArgumentException>(
            () => graph.RunStreaming("t", NewOrder(5), new InMemoryCheckpointStore(), With(("attempts", "x"))));
    }

    // --- Through a resume --------------------------------------------------------------------------

    [Fact]
    public async Task A_Resume_Carries_The_Stored_Metadata_And_Overrides_It_Key_By_Key()
    {
        var seen = new ConcurrentQueue<IReadOnlyDictionary<string, string>>();
        var graph = Parent(ReviewChild(seen: seen));
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync("t", NewOrder(5), store, With(("tenant", "acme"), ("incident", "INC-1")));

        await graph.ResumeAsync("t", NoSignal(), store, With(("incident", "INC-2")));

        Assert.Equal(
            new Dictionary<string, string> { ["tenant"] = "acme", ["incident"] = "INC-2" },
            Assert.Single(seen));
    }

    [Fact]
    public async Task The_Resume_Stamp_Leaves_The_Old_Checkpoint_Metadata_Alone()
    {
        var graph = Parent(ReviewChild());
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync("t", NewOrder(5), store, With(("incident", "INC-1")));

        await graph.ResumeAsync("t", NoSignal(), store, With(("incident", "INC-2")));

        // The run completed, so the latest checkpoint is the stamped one.
        var stamped = store.Load("t")!;
        Assert.NotNull(stamped.ResumedAt);
        Assert.Equal("INC-1", stamped.Metadata!["incident"]);
        Assert.Equal("INC-1", stamped.Nested!.Metadata!["incident"]);
    }

    // --- NodeContext -------------------------------------------------------------------------------

    [Fact]
    public async Task Every_Depth_Sees_The_Run_Metadata()
    {
        var seen = new ConcurrentQueue<(string Path, IReadOnlyDictionary<string, string> Metadata)>();
        ValueTask<Job> Record(Job job, NodeContext ctx, CancellationToken _)
        {
            seen.Enqueue((ctx.NodePath, ctx.Metadata));
            return ValueTask.FromResult(job);
        }

        var child = new StateGraph<Job>().AddNode("Inner", Record).Compile();
        var graph = new StateGraph<Job>()
            .AddNode("Top", Record)
            .AddSubgraph("Work", child, job => job, (_, job) => job)
            .AddParallelGroup("Gather", new ParallelBranches<Job>().Branch("Acme", Record), (s, _) => s)
            .AddEdge("Top", "Work")
            .AddEdge("Work", "Gather")
            .Compile();

        await graph.RunAsync("t", new Job(1, [], null), new InMemoryCheckpointStore(), With(("tenant", "acme")));

        Assert.Equal(["Top", "Work/Inner", "Gather/Acme"], seen.Select(s => s.Path));
        Assert.All(seen, s => Assert.Equal("acme", s.Metadata["tenant"]));
    }

    [Fact]
    public void A_Hand_Built_Context_Has_Empty_Metadata()
    {
        var context = new NodeContext { ThreadId = "t", Node = "n", NodePath = "n" };

        Assert.Empty(context.Metadata);
        Assert.Equal(string.Empty, context.RunId);
        Assert.Null(context.Activity);
    }

    // --- The shared cross-language fixtures --------------------------------------------------------

    [Fact]
    public async Task Freezing_With_Metadata_Matches_The_Shared_Fixture_At_Every_Level()
    {
        var store = new InMemoryCheckpointStore();

        await Parent(ReviewChild()).RunAsync(
            "thread_metadata_demo", NewOrder(5), store, With(("tenant", "acme"), ("incident", "INC-284891")));

        AssertMatchesFixture("metadata-nested.json", store.Load("thread_metadata_demo")!);
    }

    [Fact]
    public async Task Resuming_The_Shared_Fixture_Hands_Its_Metadata_To_The_Resumed_Run()
    {
        var seen = new ConcurrentQueue<IReadOnlyDictionary<string, string>>();
        var store = new InMemoryCheckpointStore();
        var fixture = Checkpoint.FromWireJson(LoadFixture("metadata-nested.json"));
        store.Save(fixture);

        var result = await Parent(ReviewChild(seen: seen)).ResumeAsync(fixture.ThreadId, ApproveAs("admin_01"), store);

        Assert.Equal(11, Assert.IsType<GraphExecutionResult<Order>.Completed>(result).State.Amount);
        Assert.Equal(
            new Dictionary<string, string> { ["tenant"] = "acme", ["incident"] = "INC-284891" },
            Assert.Single(seen));
    }

    [Fact]
    public async Task A_Failed_Resume_With_Metadata_Matches_The_Shared_Fixture_At_Every_Level()
    {
        var graph = Parent(ReviewChild(sendFails: true));
        var store = new InMemoryCheckpointStore();
        await graph.RunAsync(
            "thread_metadata_failed_demo", NewOrder(5), store, With(("tenant", "acme"), ("incident", "INC-284891")));

        await graph.ResumeAsync(
            "thread_metadata_failed_demo", ApproveAs("admin_01"), store, With(("incident", "INC-284892")));

        AssertMatchesFixture("metadata-failed.json", store.Load("thread_metadata_failed_demo")!);
    }

    [Fact]
    public async Task Resuming_The_Shared_Failed_Fixture_Completes_With_Its_Metadata()
    {
        var seen = new ConcurrentQueue<IReadOnlyDictionary<string, string>>();
        var store = new InMemoryCheckpointStore();
        var fixture = Checkpoint.FromWireJson(LoadFixture("metadata-failed.json"));
        store.Save(fixture);

        var result = await Parent(ReviewChild(seen: seen)).ResumeAsync(fixture.ThreadId, NoSignal(), store);

        var state = Assert.IsType<GraphExecutionResult<Order>.Completed>(result).State;
        Assert.Equal(["prepared", "draft", "approve", "send", "finished"], state.Log);

        // The engine's keys stay behind: only the caller's are carried forward.
        Assert.Equal(
            new Dictionary<string, string> { ["tenant"] = "acme", ["incident"] = "INC-284892" },
            Assert.Single(seen));
    }
}
