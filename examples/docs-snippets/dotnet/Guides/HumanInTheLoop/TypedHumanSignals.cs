// Samples for https://stratamesh.dev/guides/human-in-the-loop/typed-human-signals/

// docs:begin guides/human-in-the-loop/typed-human-signals/graph
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;

// docs:hide
using Datalium.Strata.Storage.Memory;
using Microsoft.EntityFrameworkCore;

namespace Datalium.Strata.DocsSnippets.Guides.HumanInTheLoop.TypedHumanSignals;

public partial class TypedHumanSignalsSamples
{
    private static CompiledGraph<ScaleChange> BuildGraph()
    {
        // docs:show
        const int MaxReplicas = 20;

        var graph = new StateGraph<ScaleChange>()
            .AddNode("Propose", state => state with { Status = "awaiting approval" })
            .AddInterruptBoundary("Propose", onReject: "Cancel")
            .AddFallibleNode("Apply", (state, ctx, _) =>
            {
                var decision = ctx.Decision ?? throw NodeException.Fatal("no decision in effect");

                // The overrides are already in the state. Check the rules the type can't express.
                if (state.Replicas is < 1 or > MaxReplicas)
                {
                    throw NodeException.Fatal($"replicas must be between 1 and {MaxReplicas}, got {state.Replicas}");
                }

                if (state.Regions.Count == 0)
                {
                    throw NodeException.Fatal("at least one region is required");
                }

                return ValueTask.FromResult(state with { Status = $"applied, approved by {decision.Reviewer}" });
            })
            .AddNode("Cancel", (state, ctx, _) =>
                ValueTask.FromResult(state with { Status = $"cancelled by {ctx.Decision!.Reviewer}" }))
            .AddEdge("Propose", "Apply")
            .Compile();
        // docs:hide
        return graph;
    }
}

// docs:show

sealed record ScaleChange(
    string Service, int Replicas, bool DryRun, RolloutPlan Plan, List<string> Regions, string? Note, string Status);

sealed record RolloutPlan(int BatchSize, List<string> Steps);
// docs:end guides/human-in-the-loop/typed-human-signals/graph

// docs:begin guides/human-in-the-loop/typed-human-signals/request
/// <summary>What the approval form posts. The reviewer is not in it.</summary>
sealed record ApprovalRequest(bool Approve, int? Replicas, bool? DryRun, List<string>? Regions, string? Note);

static class Approvals
{
    public static HumanSignal ToSignal(ApprovalRequest request, string reviewer)
    {
        var signal = request.Approve ? HumanSignal.Approve(reviewer) : HumanSignal.Reject(reviewer);
        if (request.Replicas is { } replicas) signal = signal.WithOverride("Replicas", replicas);
        if (request.DryRun is { } dryRun) signal = signal.WithOverride("DryRun", dryRun);
        if (request.Regions is { } regions) signal = signal.WithOverride("Regions", JsonSerializer.SerializeToNode(regions));
        if (request.Note is { } note) signal = signal.WithOverride("Note", note);
        return signal;
    }
}

// docs:hide
public partial class TypedHumanSignalsSamples
{
    private static async Task<GraphExecutionResult<ScaleChange>> ResumeFromRequestAsync(
        CompiledGraph<ScaleChange> graph, ICheckpointStore store, string body, string reviewer)
    {
        // docs:show
        // reviewer comes from your authentication, never from the body.
        var request = JsonSerializer.Deserialize<ApprovalRequest>(body, JsonSerializerOptions.Web)!;
        var result = await graph.ResumeAsync("scale-search", Approvals.ToSignal(request, reviewer), store);
        // docs:hide
        return result;
    }
}
// docs:show
// docs:end guides/human-in-the-loop/typed-human-signals/request

public partial class TypedHumanSignalsSamples
{
    /// <summary>The page runs its threads against a SQLite store: a file of this test's own.</summary>
    private static EfCoreCheckpointStore OpenStore() =>
        new(new CheckpointDbContext(new DbContextOptionsBuilder<CheckpointDbContext>()
            .UseSqlite($"Data Source={Path.Combine(Path.GetTempPath(), $"typed-human-signals-{Guid.NewGuid():N}.db")};Pooling=false")
            .Options));

    private static ScaleChange NewChange(string service) =>
        new(service, 4, DryRun: true, new RolloutPlan(1, ["scale"]), ["eu-west"], Note: "first draft", Status: "new");

    [Fact]
    public async Task TypedOverrides()
    {
        var graph = BuildGraph();
        using var store = OpenStore();
        Assert.IsType<GraphExecutionResult<ScaleChange>.Interrupted>(
            await graph.RunAsync("scale-checkout", NewChange("checkout"), store));

        // docs:begin guides/human-in-the-loop/typed-human-signals/typed-overrides
        var approval = HumanSignal.Approve("alice@example.com")
            .WithOverride("Replicas", 6)
            .WithOverride("DryRun", false)
            .WithOverride("Plan", JsonSerializer.SerializeToNode(new RolloutPlan(2, ["drain", "scale", "verify"])))
            .WithOverride("Regions", new JsonArray("eu-west", "us-east"))
            .WithOverride("Note", null);
        var result = await graph.ResumeAsync("scale-checkout", approval, store);
        // docs:end guides/human-in-the-loop/typed-human-signals/typed-overrides

        var applied = Assert.IsType<GraphExecutionResult<ScaleChange>.Completed>(result).State;
        Assert.Equal("applied, approved by alice@example.com", applied.Status);
        Assert.Equal(6, applied.Replicas);
        Assert.False(applied.DryRun);
        Assert.Equal(2, applied.Plan.BatchSize);
        Assert.Equal(new[] { "drain", "scale", "verify" }, applied.Plan.Steps);
        Assert.Equal(new[] { "eu-west", "us-east" }, applied.Regions);
        Assert.Null(applied.Note);
    }

    [Fact]
    public async Task RefuseThenFailThenCorrect()
    {
        var graph = BuildGraph();
        using var store = OpenStore();
        Assert.IsType<GraphExecutionResult<ScaleChange>.Interrupted>(
            await graph.RunAsync("scale-search", NewChange("search"), store));

        var messages = new List<string>();
        // docs:begin guides/human-in-the-loop/typed-human-signals/refused
        var wrongType = HumanSignal.Approve("alice@example.com").WithOverride("Replicas", "six");
        var asString = new HumanSignal(true, "alice@example.com", new Dictionary<string, string> { ["Replicas"] = "6" });
        var inBothMaps = new HumanSignal(true, "alice@example.com", new Dictionary<string, string> { ["Note"] = "a" })
            .WithOverride("Note", "b");

        foreach (var signal in new[] { wrongType, asString, inBothMaps })
        {
            if (await graph.ResumeAsync("scale-search", signal, store) is GraphExecutionResult<ScaleChange>.Failed refused)
            {
                Console.WriteLine($"Refused: {refused.Message}");
                // docs:hide
                messages.Add(refused.Message);
                // docs:show
            }
        }

        var waiting = await store.LoadAsync("scale-search");
        Console.WriteLine($"Still {waiting!.Status}, resumed at: {waiting.ResumedAt ?? "-"}");
        // docs:end guides/human-in-the-loop/typed-human-signals/refused

        Assert.Equal(3, messages.Count);
        Assert.StartsWith(
            "failed to rehydrate state for thread 'scale-search': The JSON value could not be converted to ", messages[0]);
        Assert.Contains("Path: $.Replicas", messages[0]);
        Assert.StartsWith(
            "failed to rehydrate state for thread 'scale-search': The JSON value could not be converted to ", messages[1]);
        Assert.Contains("Path: $.Replicas", messages[1]);
        Assert.Equal(
            "cannot resume thread 'scale-search': override 'Note' is set as both a string and a typed value", messages[2]);
        Assert.Equal(CheckpointStatus.Interrupted, waiting.Status);
        Assert.Null(waiting.ResumedAt);

        // The approval form posts 50 replicas, and Apply fails the thread.
        const string body = """{ "approve": true, "replicas": 50, "note": "peak traffic" }""";
        var failedRun = await ResumeFromRequestAsync(graph, store, body, "alice@example.com");
        var nodeFailed = Assert.IsType<GraphExecutionResult<ScaleChange>.NodeFailed>(failedRun);
        Assert.Equal("Apply", nodeFailed.Node);
        Assert.Contains("replicas must be between 1 and 20, got 50", nodeFailed.Message);

        // docs:begin guides/human-in-the-loop/typed-human-signals/checkpoint-decision
        var checkpoint = await store.LoadAsync("scale-search");
        if (checkpoint?.Decision is { } recorded)
        {
            Console.WriteLine($"{checkpoint.Status}: approved={recorded.IsApproved} by {recorded.Reviewer} at {recorded.Boundary}, {recorded.DecidedAt}");
        }
        // docs:end guides/human-in-the-loop/typed-human-signals/checkpoint-decision

        Assert.Equal(CheckpointStatus.Failed, checkpoint!.Status);
        var decision = Assert.IsType<HumanDecision>(checkpoint.Decision);
        Assert.True(decision.IsApproved);
        Assert.Equal("alice@example.com", decision.Reviewer);
        Assert.Equal("Propose", decision.Boundary);
        Assert.Equal(50, decision.Overrides["Replicas"].GetInt32());
        Assert.Equal("peak traffic", decision.Overrides["Note"].GetString());
        Assert.False(decision.Overrides.ContainsKey("DryRun"), "a field the form didn't send adds no override");

        ScaleChange? corrected = null;
        // docs:begin guides/human-in-the-loop/typed-human-signals/correction
        var correction = HumanSignal.Approve("bob@example.com").WithOverride("Replicas", 12);
        if (await graph.ResumeAsync("scale-search", correction, store) is GraphExecutionResult<ScaleChange>.Completed done)
        {
            Console.WriteLine($"{done.State.Status}, replicas {done.State.Replicas}");
            // docs:hide
            corrected = done.State;
            // docs:show
        }
        // docs:end guides/human-in-the-loop/typed-human-signals/correction

        // A resume of a failed thread records no decision, so Apply still sees Alice's approval.
        Assert.NotNull(corrected);
        Assert.Equal("applied, approved by alice@example.com", corrected.Status);
        Assert.Equal(12, corrected.Replicas);
    }

    [Fact]
    public async Task ReadOverrideInNode()
    {
        using var store = OpenStore();
        int? seenBatchSize = null;
        var graph = new StateGraph<ScaleChange>()
            .AddNode("Propose", state => state with { Status = "awaiting approval" })
            .AddInterruptBoundary("Propose")
            .AddNode("Apply", (state, ctx, _) =>
            {
                var decision = ctx.Decision!;
                // docs:begin guides/human-in-the-loop/typed-human-signals/read-override
                // In Apply. decision.Overrides is an IReadOnlyDictionary<string, JsonElement>.
                if (decision.Overrides.TryGetValue("Plan", out var planJson))
                {
                    var plan = planJson.Deserialize<RolloutPlan>()!;
                    Console.WriteLine($"the reviewer changed the plan: batches of {plan.BatchSize}");
                    // docs:hide
                    seenBatchSize = plan.BatchSize;
                    // docs:show
                }
                // docs:end guides/human-in-the-loop/typed-human-signals/read-override
                return ValueTask.FromResult(state);
            })
            .AddEdge("Propose", "Apply")
            .Compile();

        await graph.RunAsync("scale-read-plan", NewChange("checkout"), store);
        var approval = HumanSignal.Approve("alice@example.com")
            .WithOverride("Plan", JsonSerializer.SerializeToNode(new RolloutPlan(2, ["drain", "scale", "verify"])));
        Assert.IsType<GraphExecutionResult<ScaleChange>.Completed>(
            await graph.ResumeAsync("scale-read-plan", approval, store));
        Assert.Equal(2, seenBatchSize);
    }
}
