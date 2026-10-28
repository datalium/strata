using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Schema;
using Datalium.Strata.Validation;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/group_joins.rs</c> (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>,
/// SG-08): join policies, losers cancelled and reported, the unmet join, a race inside a subgraph,
/// and the join in the exported definition (<c>schema/fixtures/graph-export/joins.json</c>,
/// <c>graph-validation/invalid-quorum.json</c>). Keep the two in step.
/// </summary>
public class GroupJoinTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    /// <summary>A model's answer: <c>N</c> is its confidence in percent.</summary>
    public sealed record Job(int N, IReadOnlyList<string> Trail);

    private static Job NewJob() => new(0, []);

    /// <summary>Answers with <paramref name="confidence"/> after <paramref name="afterMs"/> ms (0: at once).</summary>
    private static Func<Job, NodeContext, CancellationToken, ValueTask<Job>> Answers(
        string name, int confidence, int afterMs = 0, ConcurrentQueue<string>? cancelled = null) =>
        async (job, _, ct) =>
        {
            try
            {
                if (afterMs != 0)
                {
                    await Task.Delay(afterMs, ct);
                }
            }
            catch (OperationCanceledException)
            {
                cancelled?.Enqueue(name);
                throw;
            }

            return job with { N = confidence, Trail = [.. job.Trail, name] };
        };

    /// <summary>Never answers: waits until its token fires.</summary>
    private static Func<Job, NodeContext, CancellationToken, ValueTask<Job>> Hangs(string name, ConcurrentQueue<string> cancelled) =>
        Answers(name, 0, afterMs: Timeout.Infinite, cancelled);

    private static Func<Job, NodeContext, CancellationToken, ValueTask<Job>> Fails(NodeException error) =>
        (_, _, _) => ValueTask.FromException<Job>(error);

    private static async Task<(GraphExecutionResult<Job> Result, List<RunEvent> Events)> Run(
        CompiledGraph<Job> graph, ICheckpointStore? store = null)
    {
        var channel = new RunEventChannel();
        var result = await graph.RunAsync("t", NewJob(), store ?? new InMemoryCheckpointStore(), new RunOptions { Observer = channel })
            .WaitAsync(Patience);
        var events = new List<RunEvent>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            events.Add(e);
        }

        return (result, events);
    }

    /// <summary>The group's branch and join events, one line each, in order.</summary>
    private static List<string> Story(IEnumerable<RunEvent> events) =>
    [
        .. events.Select(e => e switch
        {
            BranchStarted => $"started {e.NodePath}",
            BranchCompleted c => $"completed {e.NodePath} {c.Result}" + (c.Reason is { } r ? $" {r}" : ""),
            JoinDecided j => $"decided {e.NodePath} {j.Policy} {(j.Met ? "met" : "unmet")} [{string.Join(",", j.Deciding)}]",
            _ => null,
        }).OfType<string>(),
    ];

    private static CompiledGraph<Job> Race(
        ParallelBranches<Job> branches,
        ParallelGroupOptions<Job> options,
        ConcurrentQueue<BranchOutcomes<Job>> seen) =>
        new StateGraph<Job>()
            .AddParallelGroupWithOutcomes(
                "Race",
                branches,
                (entry, results) =>
                {
                    seen.Enqueue(results);
                    return entry with { Trail = [.. results.Where(r => results.Deciding.Contains(r.Name)).SelectMany(r => r.State!.Trail)] };
                },
                options)
            .Compile();

    private static Job Completed(GraphExecutionResult<Job> result) =>
        Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State;

    [Fact]
    public async Task First_Success_Reduces_As_Soon_As_One_Branch_Succeeds_And_Cancels_The_Rest()
    {
        var cancelled = new ConcurrentQueue<string>();
        var seen = new ConcurrentQueue<BranchOutcomes<Job>>();
        var graph = Race(
            new ParallelBranches<Job>().Branch("openai", Hangs("openai", cancelled)).Branch("anthropic", Answers("anthropic", 80)).Branch("local", Hangs("local", cancelled)),
            new ParallelGroupOptions<Job> { Join = JoinPolicy<Job>.FirstSuccess },
            seen);

        var (result, events) = await Run(graph);

        Assert.Equal(["anthropic"], Completed(result).Trail);
        var outcomes = Assert.Single(seen);
        Assert.True(outcomes.JoinMet);
        Assert.Equal(["anthropic"], outcomes.Deciding);
        Assert.Equal(
            [BranchStatus.Cancelled, BranchStatus.Succeeded, BranchStatus.Cancelled],
            outcomes.Select(o => o.Status));
        var story = Story(events);
        Assert.Equal(
            [
                "completed Race/anthropic Succeeded",
                "decided Race first_success met [anthropic]",
            ],
            story.Where(line => !line.StartsWith("started", StringComparison.Ordinal)).Take(2));
        Assert.Contains("completed Race/openai Cancelled LostRace", story);
        Assert.Contains("completed Race/local Cancelled LostRace", story);
        await Task.Delay(50);
        Assert.Equal(["local", "openai"], cancelled.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task First_Matching_Waits_For_A_Result_That_Passes_The_Predicate()
    {
        var cancelled = new ConcurrentQueue<string>();
        var seen = new ConcurrentQueue<BranchOutcomes<Job>>();
        var graph = Race(
            new ParallelBranches<Job>()
                .Branch("openai", Answers("openai", 50))
                .Branch("anthropic", Answers("anthropic", 95, afterMs: 30))
                .Branch("local", Hangs("local", cancelled)),
            new ParallelGroupOptions<Job> { Join = JoinPolicy<Job>.FirstMatching(j => j.N >= 90) },
            seen);

        var (result, events) = await Run(graph);

        Assert.Equal(["anthropic"], Completed(result).Trail);
        var outcomes = Assert.Single(seen);
        Assert.Equal(["anthropic"], outcomes.Deciding);
        Assert.Equal(BranchStatus.Succeeded, outcomes["openai"]!.Status);
        Assert.Equal(
            [
                "completed Race/openai Succeeded",
                "completed Race/anthropic Succeeded",
                "decided Race first_matching met [anthropic]",
                "completed Race/local Cancelled LostRace",
            ],
            Story(events).Where(line => !line.StartsWith("started", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task A_Quorum_Is_Met_By_The_First_Branches_To_Succeed()
    {
        var cancelled = new ConcurrentQueue<string>();
        IReadOnlyList<Job>? handed = null;
        var graph = new StateGraph<Job>()
            .AddParallelGroup(
                "Race",
                new ParallelBranches<Job>()
                    .Branch("openai", Answers("openai", 70, afterMs: 30))
                    .Branch("anthropic", Answers("anthropic", 80))
                    .Branch("local", Hangs("local", cancelled)),
                (entry, results) =>
                {
                    handed = results;
                    return entry;
                },
                new ParallelGroupOptions<Job> { Join = JoinPolicy<Job>.Quorum(2) })
            .Compile();

        var (result, events) = await Run(graph);

        Completed(result);
        // The plain reducer gets the deciding states, in declaration order.
        Assert.Equal([70, 80], handed!.Select(j => j.N));
        Assert.Contains("decided Race quorum met [anthropic,openai]", Story(events));
        Assert.Contains("completed Race/local Cancelled LostRace", Story(events));
    }

    [Fact]
    public async Task Branches_Held_Back_By_The_Limit_Never_Start_Once_The_Race_Is_Won()
    {
        var seen = new ConcurrentQueue<BranchOutcomes<Job>>();
        var graph = Race(
            new ParallelBranches<Job>().Branch("openai", Answers("openai", 80)).Branch("anthropic", Answers("anthropic", 90)).Branch("local", Answers("local", 99)),
            new ParallelGroupOptions<Job> { Join = JoinPolicy<Job>.FirstSuccess, MaxConcurrency = 1 },
            seen);

        var (result, events) = await Run(graph);

        Assert.Equal(["openai"], Completed(result).Trail);
        Assert.Equal(["started Race/openai", "completed Race/openai Succeeded", "decided Race first_success met [openai]"], Story(events));
        Assert.Equal([BranchStatus.Succeeded, BranchStatus.Cancelled, BranchStatus.Cancelled], Assert.Single(seen).Select(o => o.Status));
    }

    [Fact]
    public async Task A_Race_Goes_On_After_A_Failure_While_It_Can_Still_Be_Won()
    {
        var seen = new ConcurrentQueue<BranchOutcomes<Job>>();
        var graph = Race(
            new ParallelBranches<Job>().Branch("openai", Fails(NodeException.Transient("500"))).Branch("anthropic", Answers("anthropic", 80, afterMs: 30)),
            new ParallelGroupOptions<Job> { Join = JoinPolicy<Job>.FirstSuccess },
            seen);

        var (result, events) = await Run(graph);

        Assert.Equal(["anthropic"], Completed(result).Trail);
        Assert.Equal(
            ["completed Race/openai Failed", "completed Race/anthropic Succeeded", "decided Race first_success met [anthropic]"],
            Story(events).Where(line => !line.StartsWith("started", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task A_Race_That_Can_No_Longer_Be_Won_Fails_Transiently_If_Any_Failure_Was()
    {
        var store = new InMemoryCheckpointStore();
        var graph = Race(
            new ParallelBranches<Job>().Branch("openai", Fails(NodeException.Transient("500"))).Branch("anthropic", Fails(NodeException.Fatal("refused"))),
            new ParallelGroupOptions<Job> { Join = JoinPolicy<Job>.FirstSuccess },
            new ConcurrentQueue<BranchOutcomes<Job>>());

        var (result, events) = await Run(graph, store);

        Assert.Equal("branch 'openai': 500; branch 'anthropic': refused", Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result).Message);
        Assert.Equal("transient", store.Load("t")!.Metadata!["error_kind"]);
        Assert.Contains("decided Race first_success unmet []", Story(events));
    }

    [Fact]
    public async Task An_Unmet_Quorum_Fails_And_Says_So()
    {
        var store = new InMemoryCheckpointStore();
        var graph = Race(
            new ParallelBranches<Job>()
                .Branch("openai", Answers("openai", 80))
                .Branch("anthropic", Fails(NodeException.Fatal("refused")))
                .Branch("local", Fails(NodeException.Fatal("crashed"))),
            new ParallelGroupOptions<Job> { Join = JoinPolicy<Job>.Quorum(2) },
            new ConcurrentQueue<BranchOutcomes<Job>>());

        var (result, _) = await Run(graph, store);

        Assert.Equal(
            "quorum of 2 not reached: 1 of 3 branches succeeded; branch 'anthropic': refused; branch 'local': crashed",
            Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result).Message);
        Assert.Equal("fatal", store.Load("t")!.Metadata!["error_kind"]);
    }

    [Fact]
    public async Task With_No_Match_Keep_Succeeded_Reduces_Over_Every_Answer()
    {
        var seen = new ConcurrentQueue<BranchOutcomes<Job>>();
        var graph = new StateGraph<Job>()
            .AddParallelGroupWithOutcomes(
                "Race",
                new ParallelBranches<Job>().Branch("openai", Answers("openai", 50)).Branch("anthropic", Answers("anthropic", 70)).Branch("local", Answers("local", 60)),
                (entry, results) =>
                {
                    seen.Enqueue(results);
                    return results.Succeeded.MaxBy(j => j.N) ?? entry;
                },
                new ParallelGroupOptions<Job>
                {
                    Join = JoinPolicy<Job>.FirstMatching(j => j.N >= 90),
                    Partial = PartialResults.KeepSucceeded,
                })
            .Compile();

        var (result, events) = await Run(graph);

        Assert.Equal(["anthropic"], Completed(result).Trail);
        var outcomes = Assert.Single(seen);
        Assert.False(outcomes.JoinMet);
        Assert.Empty(outcomes.Deciding);
        Assert.All(outcomes, o => Assert.Equal(BranchStatus.Succeeded, o.Status));
        Assert.Contains("decided Race first_matching unmet []", Story(events));
    }

    [Fact]
    public async Task With_No_Match_Fail_Group_Fails_The_Group()
    {
        var store = new InMemoryCheckpointStore();
        var graph = Race(
            new ParallelBranches<Job>().Branch("openai", Answers("openai", 50)).Branch("anthropic", Answers("anthropic", 70)),
            new ParallelGroupOptions<Job> { Join = JoinPolicy<Job>.FirstMatching(j => j.N >= 90) },
            new ConcurrentQueue<BranchOutcomes<Job>>());

        var (result, _) = await Run(graph, store);

        Assert.Equal("no branch matched the join (2 succeeded)", Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result).Message);
        Assert.Equal("transient", store.Load("t")!.Metadata!["error_kind"]);
    }

    [Fact]
    public async Task A_Race_Inside_A_Subgraph_Reports_Under_The_Subgraph()
    {
        var cancelled = new ConcurrentQueue<string>();
        var child = Race(
            new ParallelBranches<Job>().Branch("openai", Answers("openai", 95)).Branch("local", Hangs("local", cancelled)),
            new ParallelGroupOptions<Job> { Join = JoinPolicy<Job>.FirstSuccess },
            new ConcurrentQueue<BranchOutcomes<Job>>());
        var parent = new StateGraph<Job>()
            .AddSubgraph("Rca", child, job => job, (_, done) => done)
            .Compile();

        var (result, events) = await Run(parent);

        Assert.Equal(["openai"], Completed(result).Trail);
        var story = Story(events);
        Assert.Contains("decided Rca/Race first_success met [openai]", story);
        Assert.Contains("completed Rca/Race/local Cancelled LostRace", story);
    }

    // The join in the definition.
    private static string Fixtures(string folder, [CallerFilePath] string sourceFilePath = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(sourceFilePath)!, "..", "..", "..", "schema", "fixtures", folder));

    private static ParallelBranches<Job> Named(params string[] names)
    {
        var branches = new ParallelBranches<Job>();
        foreach (var name in names)
        {
            branches.Branch(name, job => job);
        }

        return branches;
    }

    /// <summary>The graph <c>joins.json</c> describes.</summary>
    private static StateGraph<Job> Joins() =>
        new StateGraph<Job>()
            .AddParallelGroupWithOutcomes("Specialists", Named("telemetry", "kubernetes", "database"), (s, _) => s,
                new ParallelGroupOptions<Job> { Partial = PartialResults.KeepSucceeded, BranchTimeout = TimeSpan.FromSeconds(45), MaxConcurrency = 2 })
            .AddParallelGroupWithOutcomes("Race", Named("openai", "anthropic", "local"), (s, _) => s,
                new ParallelGroupOptions<Job>
                {
                    Join = JoinPolicy<Job>.FirstMatching(j => j.N >= 90, "validated, confidence >= 0.9"),
                    Partial = PartialResults.KeepSucceeded,
                })
            .AddParallelGroup("Fastest", Named("primary", "secondary"), (s, _) => s,
                new ParallelGroupOptions<Job> { Join = JoinPolicy<Job>.FirstSuccess, CancelSiblingsOnFailure = false })
            .AddParallelGroup("Majority", Named("openai", "anthropic", "local"), (s, _) => s,
                new ParallelGroupOptions<Job> { Join = JoinPolicy<Job>.Quorum(2) })
            .AddParallelGroup("Plain", Named("a", "b"), (s, _) => s, new ParallelGroupOptions<Job>())
            .AddEdge("Specialists", "Race")
            .AddEdge("Race", "Fastest")
            .AddEdge("Fastest", "Majority")
            .AddEdge("Majority", "Plain")
            .SetEntryPoint("Specialists");

    [Fact]
    public void The_Join_Is_Exported_As_The_Shared_Fixture_Has_It()
    {
        var expected = JsonNode.Parse(File.ReadAllText(Path.Combine(Fixtures("graph-export"), "joins.json")));
        var exported = JsonSerializer.SerializeToNode(Joins().Compile().ToDefinition());

        Assert.True(JsonNode.DeepEquals(expected, exported), $"Expected: {expected}\nActual: {exported}");
    }

    [Fact]
    public void The_Joins_Fixture_Round_Trips_And_Validates()
    {
        var raw = JsonNode.Parse(File.ReadAllText(Path.Combine(Fixtures("graph-export"), "joins.json")))!;
        var definition = raw.Deserialize<GraphDefinition>()!;

        Assert.True(JsonNode.DeepEquals(raw, JsonSerializer.SerializeToNode(definition.ToTopology().ToDefinition())));
        Assert.True(GraphValidator.Validate(definition.ToTopology()).IsValid);
    }

    [Fact]
    public void A_Group_With_Default_Options_Exports_No_Join()
    {
        var definition = new StateGraph<Job>()
            .AddParallelGroup("Plain", Named("a"), (s, _) => s, new ParallelGroupOptions<Job> { MaxConcurrency = 1 })
            .ToDefinition();

        Assert.Equal("1.1.0", definition.SchemaVersion);
        Assert.DoesNotContain("\"join\"", JsonSerializer.Serialize(definition));
    }

    [Fact]
    public void An_Invalid_Quorum_Is_Reported_At_Every_Level()
    {
        var raw = File.ReadAllText(Path.Combine(Fixtures("graph-validation"), "invalid-quorum.json"));
        var result = GraphValidator.Validate(JsonSerializer.Deserialize<GraphDefinition>(raw)!.ToTopology());

        Assert.Equal(
            [("STRATA_ERR_INVALID_QUORUM", "Vote"), ("STRATA_ERR_INVALID_QUORUM", "Review/Ask")],
            result.Errors.Select(e => (e.Code, e.Node)));
    }

    [Fact]
    public void Compile_Refuses_A_Quorum_Above_The_Branch_Count()
    {
        var graph = new StateGraph<Job>()
            .AddParallelGroup("Vote", Named("a", "b"), (s, _) => s, new ParallelGroupOptions<Job> { Join = JoinPolicy<Job>.Quorum(3) });

        var refused = Assert.Throws<GraphCompilationException>(() => graph.Compile());
        Assert.Contains(refused.Result.Errors, e => e.Code == "STRATA_ERR_INVALID_QUORUM" && e.Node == "Vote");
    }

    [Theory]
    [InlineData("1.1.0", "first_success", null, null, "node 'G' has a join, which schema_version 1.1.0 doesn't have")]
    [InlineData("1.2.0", "first_success", 2, null, "node 'G' has a join with a count, which only a quorum has")]
    [InlineData("1.2.0", "quorum", 1, "x", "node 'G' has a join with a condition, which only first_matching has")]
    public void A_Misplaced_Join_Is_Not_A_Graph_Definition(string version, string policy, int? count, string? condition, string message)
    {
        var definition = JsonSerializer.Deserialize<GraphDefinition>($$"""
            {
              "schema_version": "{{version}}",
              "entry_point": "G",
              "nodes": [
                {
                  "name": "G", "metadata": null, "kind": "parallel_group",
                  "branches": [ { "name": "a", "metadata": null } ],
                  "join": { "policy": "{{policy}}", "count": {{count?.ToString() ?? "null"}}, "condition": {{(condition is null ? "null" : $"\"{condition}\"")}}, "partial": "fail_group" }
                }
              ],
              "edges": []
            }
            """)!;

        Assert.Equal(message, Assert.Throws<FormatException>(() => definition.ToTopology()).Message);
    }

    [Fact]
    public void A_Join_On_A_Plain_Node_Is_Not_A_Graph_Definition()
    {
        var definition = JsonSerializer.Deserialize<GraphDefinition>("""
            {
              "schema_version": "1.2.0",
              "entry_point": "G",
              "nodes": [
                { "name": "G", "metadata": null, "join": { "policy": "all", "count": null, "condition": null, "partial": "keep_succeeded" } }
              ],
              "edges": []
            }
            """)!;

        Assert.Equal("node 'G' has a join but is neither a parallel_group nor a map", Assert.Throws<FormatException>(() => definition.ToTopology()).Message);
    }
}
