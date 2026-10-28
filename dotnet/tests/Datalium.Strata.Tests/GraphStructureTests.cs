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
/// Graph structure in definitions (<c>design/22-Graph-Structure-In-Definitions.md</c>, SG-09): node and
/// branch metadata, groups and subgraphs described in the export, recursive validation with qualified
/// names, and run-event paths that resolve in the export. Mirrors
/// <c>rust/strata-core/tests/graph_structure.rs</c>; keep the two in step. Both build the reference
/// graph below and compare its export to <c>schema/fixtures/graph-export/hierarchy.json</c>.
/// </summary>
public class GraphStructureTests
{
    private static int Noop(int s) => s;

    private static ValueTask<int> NoopAsync(int s, NodeContext context, CancellationToken ct) => ValueTask.FromResult(s);

    private static Dictionary<string, string> Meta(params (string Key, string Value)[] pairs) =>
        pairs.ToDictionary(p => p.Key, p => p.Value);

    private static JsonNode? ReadFixture(string fileName, [CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(
            Path.Combine(testDir, "..", "..", "..", "schema", "fixtures", "graph-export", fileName));
        return JsonNode.Parse(File.ReadAllText(path));
    }

    /// <summary><c>Apply</c> -> <c>Verify</c>, two levels down.</summary>
    private static CompiledGraph<int> Fix() =>
        new StateGraph<int>()
            .AddNode("Apply", Noop, metadata: Meta(("tool", "kubectl")))
            .AddNode("Verify", Noop)
            .AddEdge("Apply", "Verify")
            .Compile();

    /// <summary><c>Plan</c> -> <c>Approve</c> (a boundary, rejecting to <c>Abort</c>) -> <c>Fix</c> (a subgraph).</summary>
    private static CompiledGraph<int> Remediation() =>
        new StateGraph<int>()
            .AddNode("Plan", Noop)
            .AddNode("Approve", Noop)
            .AddSubgraph("Fix", Fix(), s => s, (_, child) => child, metadata: Meta(("agent", "fixer")))
            .AddNode("Abort", Noop)
            .AddEdge("Plan", "Approve")
            .AddEdge("Approve", "Fix")
            .AddInterruptBoundary("Approve", onReject: "Abort")
            .Compile();

    /// <summary>
    /// The design doc's incident graph: <c>Triage</c> -> <c>Investigate</c> (a group of three branches) ->
    /// <c>Remediate</c> (the remediation subgraph) -> <c>Report</c>.
    /// </summary>
    private static StateGraph<int> Incident()
    {
        var investigate = new ParallelBranches<int>()
            .Branch("Kubernetes", Noop, metadata: Meta(("agent", "kubernetes-investigator")))
            .Branch("Prometheus", NoopAsync)
            .Branch("GitHub", Noop)
            .SetMetadata("Prometheus", Meta(("agent", "metrics-investigator")));

        // Triage's metadata is given out of order: the export sorts keys.
        return new StateGraph<int>()
            .AddNode("Triage", Noop, metadata: Meta(("model_policy", "ops-reasoning-standard"), ("agent", "triage")))
            .AddParallelGroup("Investigate", investigate, (s, _) => s)
            .AddSubgraph("Remediate", Remediation(), s => s, (_, child) => child)
            .AddNode("Report", Noop)
            .AddEdge("Triage", "Investigate")
            .AddEdge("Investigate", "Remediate")
            .AddEdge("Remediate", "Report")
            .SetNodeMetadata("Remediate", Meta(("agent", "remediator")));
    }

    private static string CompileError(StateGraph<int> graph) =>
        Assert.Throws<InvalidOperationException>(() => graph.Compile()).Message;

    // --- Metadata ---------------------------------------------------------------

    [Fact]
    public void Metadata_Reaches_The_Export_On_Every_Kind_Of_Node_By_Parameter_And_By_Setter()
    {
        var child = new StateGraph<int>().AddNode("Only", Noop).Compile();
        string[] names =
        [
            "Plain", "AsyncPlain", "Streaming", "AsyncStreaming", "Fallible", "AsyncFallible", "Group", "Sub",
        ];
        var graph = new StateGraph<int>()
            .AddNode("Plain", Noop, metadata: Meta(("role", "Plain")))
            .AddNode("AsyncPlain", NoopAsync, metadata: Meta(("role", "AsyncPlain")))
            .AddStreamingNode("Streaming", (s, _) => s, metadata: Meta(("role", "Streaming")))
            .AddStreamingNode("AsyncStreaming", NoopAsync, metadata: Meta(("role", "AsyncStreaming")))
            .AddFallibleNode("Fallible", Noop, metadata: Meta(("role", "Fallible")))
            .AddFallibleNode("AsyncFallible", NoopAsync, metadata: Meta(("role", "AsyncFallible")))
            .AddParallelGroup("Group", new ParallelBranches<int>().Branch("Only", Noop), (s, _) => s, metadata: Meta(("role", "Group")))
            .AddSubgraph("Sub", child, s => s, (_, c) => c, metadata: Meta(("role", "Sub")));
        for (var i = 1; i < names.Length; i++)
        {
            graph.AddEdge(names[i - 1], names[i]);
        }

        var byParameter = graph.ToDefinition();
        Assert.Equal(names, byParameter.Nodes.Select(n => n.Name));
        Assert.All(byParameter.Nodes, n => Assert.Equal(Meta(("role", n.Name)), n.Metadata));

        foreach (var name in names)
        {
            graph.SetNodeMetadata(name, Meta(("set", name)));
        }

        Assert.All(graph.ToDefinition().Nodes, n => Assert.Equal(Meta(("set", n.Name)), n.Metadata));
        graph.Compile();
    }

    [Fact]
    public void Metadata_Set_Again_Is_Replaced_Not_Merged()
    {
        var graph = new StateGraph<int>()
            .AddNode("A", Noop, metadata: Meta(("agent", "first"), ("team", "ops")))
            .SetNodeMetadata("A", Meta(("agent", "second")));

        Assert.Equal(Meta(("agent", "second")), graph.ToDefinition().Nodes[0].Metadata);

        var branches = new ParallelBranches<int>()
            .Branch("B", Noop, metadata: Meta(("agent", "first"), ("team", "ops")))
            .SetMetadata("B", Meta(("agent", "second")));
        var group = new StateGraph<int>().AddParallelGroup("G", branches, (s, _) => s);
        Assert.Equal(Meta(("agent", "second")), group.ToDefinition().Nodes[0].Branches![0].Metadata);
    }

    [Fact]
    public void Re_Adding_A_Node_Without_Metadata_Keeps_It()
    {
        var graph = new StateGraph<int>()
            .AddNode("A", Noop, metadata: Meta(("agent", "triage")))
            .AddFallibleNode("A", Noop);

        Assert.Equal(Meta(("agent", "triage")), graph.ToDefinition().Nodes[0].Metadata);
    }

    [Fact]
    public void Changing_The_Callers_Dictionary_Afterwards_Changes_Nothing()
    {
        var metadata = Meta(("agent", "triage"));
        var graph = new StateGraph<int>().AddNode("A", Noop, metadata: metadata);
        metadata["agent"] = "changed";

        Assert.Equal(Meta(("agent", "triage")), graph.ToDefinition().Nodes[0].Metadata);
    }

    [Fact]
    public void Metadata_Keys_Are_Exported_Sorted()
    {
        var graph = new StateGraph<int>()
            .AddNode("A", Noop)
            .SetNodeMetadata("A", Meta(("zeta", "1"), ("alpha", "2"), ("mid", "3")));

        var text = JsonSerializer.Serialize(graph.ToDefinition());
        Assert.Contains("""
            "metadata":{"alpha":"2","mid":"3","zeta":"1"}
            """, text);
    }

    [Fact]
    public void Compile_Refuses_Metadata_For_A_Name_That_Is_Not_A_Node()
    {
        var graph = new StateGraph<int>().AddNode("A", Noop).SetNodeMetadata("Ghost", Meta(("agent", "x")));

        Assert.Equal("metadata set for 'Ghost', which does not exist", CompileError(graph));
    }

    [Fact]
    public void Compile_Refuses_Metadata_For_A_Branch_The_Group_Does_Not_Have()
    {
        var branches = new ParallelBranches<int>().Branch("Real", Noop).SetMetadata("Ghost", Meta(("agent", "x")));
        var graph = new StateGraph<int>().AddParallelGroup("G", branches, (s, _) => s);

        Assert.Equal(
            "metadata set for branch 'Ghost' of parallel group 'G', which does not exist",
            CompileError(graph));
    }

    [Fact]
    public void Compile_Refuses_An_Empty_Metadata_Key()
    {
        var graph = new StateGraph<int>().AddNode("A", Noop, metadata: Meta(("", "x")));

        Assert.Equal("metadata on node 'A' has an empty key", CompileError(graph));
    }

    [Fact]
    public void Compile_Refuses_A_Reserved_Metadata_Key_On_A_Node_Or_A_Branch()
    {
        var graph = new StateGraph<int>().AddNode("Triage", Noop, metadata: Meta(("strata.x", "y")));
        Assert.Equal("metadata key 'strata.x' on node 'Triage' is reserved", CompileError(graph));

        var branches = new ParallelBranches<int>().Branch("B", Noop, metadata: Meta(("strata.agent", "y")));
        var group = new StateGraph<int>().AddParallelGroup("G", branches, (s, _) => s);
        Assert.Equal(
            "metadata key 'strata.agent' on branch 'B' of parallel group 'G' is reserved",
            CompileError(group));
    }

    [Fact]
    public void The_Export_Leaves_Out_Metadata_For_Names_That_Are_Not_Nodes_Or_Branches()
    {
        var branches = new ParallelBranches<int>().Branch("B", Noop).SetMetadata("Ghost", Meta(("agent", "x")));
        var graph = new StateGraph<int>()
            .AddParallelGroup("G", branches, (s, _) => s)
            .SetNodeMetadata("Ghost", Meta(("agent", "x")));

        var definition = graph.ToDefinition();
        var node = Assert.Single(definition.Nodes);
        var branch = Assert.Single(node.Branches!);
        Assert.Null(branch.Metadata);
    }

    // --- Export -----------------------------------------------------------------

    [Fact]
    public void The_Hierarchy_Export_Matches_The_Shared_Fixture()
    {
        var exported = JsonSerializer.SerializeToNode(Incident().ToDefinition());

        Assert.True(
            JsonNode.DeepEquals(ReadFixture("hierarchy.json"), exported),
            $"Export did not match the fixture.\nExpected: {ReadFixture("hierarchy.json")}\nActual: {exported}");
    }

    [Fact]
    public void A_Plain_Graph_Is_Still_1_0_0_With_No_Hierarchy_Fields()
    {
        var graph = new StateGraph<int>()
            .AddFallibleNode("Charge", Noop)
            .AddNode("Ship", Noop)
            .AddNode("Review", Noop)
            .AddEdge("Charge", "Ship")
            .SetRecoveryPolicy("Charge", new RecoveryPolicy().WithFallback("Review"));

        var definition = graph.ToDefinition();
        Assert.Equal("1.0.0", definition.SchemaVersion);
        var text = JsonSerializer.Serialize(definition);
        Assert.DoesNotContain("\"kind\"", text);
        Assert.DoesNotContain("\"branches\"", text);
        Assert.DoesNotContain("\"graph\"", text);
    }

    [Fact]
    public void A_Group_Exports_Its_Branches_In_Declaration_Order_With_Their_Metadata()
    {
        var group = Incident().ToDefinition().Nodes[1];

        Assert.Equal(GraphNodeKind.ParallelGroup, group.Kind);
        Assert.Null(group.Graph);
        var branches = Assert.IsType<List<GraphBranchDefinition>>(group.Branches);
        Assert.Equal(["Kubernetes", "Prometheus", "GitHub"], branches.Select(b => b.Name));
        Assert.Equal(Meta(("agent", "kubernetes-investigator")), branches[0].Metadata);
        Assert.Equal(Meta(("agent", "metrics-investigator")), branches[1].Metadata);
        Assert.Null(branches[2].Metadata);
    }

    [Fact]
    public void A_Subgraph_Exports_Its_Child_Whole_Recursively_With_Each_Levels_Own_Version()
    {
        var definition = Incident().ToDefinition();
        Assert.Equal("1.1.0", definition.SchemaVersion);

        var remediate = definition.Nodes[2];
        Assert.Equal(GraphNodeKind.Subgraph, remediate.Kind);
        Assert.Null(remediate.Branches);
        var child = remediate.Graph!;
        AssertSameJson(Remediation().ToDefinition(), child);
        // The child's boundary puts its own level at 1.6.0, not its parent's
        // (design/48-Boundaries-In-Definitions.md).
        Assert.Equal("1.6.0", child.SchemaVersion);
        Assert.Contains(child.Edges, e => e.From == "Approve" && e.To == "Abort" && e.Guarded);

        var grandchild = child.Nodes[2].Graph!;
        Assert.Equal("1.0.0", grandchild.SchemaVersion);
        AssertSameJson(Fix().ToDefinition(), grandchild);
    }

    [Fact]
    public void A_Compiled_Graph_Exports_What_Its_Builder_Did()
    {
        AssertSameJson(Incident().ToDefinition(), Incident().Compile().ToDefinition());
    }

    [Fact]
    public void Re_Adding_A_Composite_Node_As_A_Plain_One_Drops_Its_Structure()
    {
        var definition = Incident().AddNode("Remediate", Noop).AddNode("Investigate", Noop).ToDefinition();

        Assert.Equal("1.0.0", definition.SchemaVersion);
        Assert.All(definition.Nodes, n => Assert.Null(n.Kind));
        // ...but keeps its metadata.
        Assert.Equal(Meta(("agent", "remediator")), definition.Nodes[2].Metadata);
    }

    [Fact]
    public void A_Definition_Maps_Back_To_The_Topology_It_Came_From()
    {
        var definition = Incident().ToDefinition();

        var roundTripped = definition.ToTopology().ToDefinition();

        AssertSameJson(definition, roundTripped);
        Assert.True(GraphValidator.Validate(definition.ToTopology()).IsValid);
    }

    [Fact]
    public void The_Hierarchy_Fixture_Round_Trips_Through_The_Definition_Types()
    {
        var raw = ReadFixture("hierarchy.json")!;
        var definition = raw.Deserialize<GraphDefinition>()!;

        Assert.True(JsonNode.DeepEquals(raw, JsonSerializer.SerializeToNode(definition)));
        Assert.True(JsonNode.DeepEquals(raw, JsonSerializer.SerializeToNode(definition.ToTopology().ToDefinition())));
    }

    private static GraphDefinition Hierarchy() => ReadFixture("hierarchy.json")!.Deserialize<GraphDefinition>()!;

    [Fact]
    public void ToTopology_Refuses_A_Version_It_Does_Not_Know_At_Any_Level()
    {
        var top = Hierarchy() with { SchemaVersion = "2.0.0" };
        Assert.Equal(
            "unsupported schema_version '2.0.0': this build reads 1.0.0, 1.1.0, 1.2.0, 1.3.0, 1.4.0, 1.5.0 and 1.6.0",
            Assert.Throws<FormatException>(() => top.ToTopology()).Message);

        var nested = ReadFixture("hierarchy.json")!;
        nested["nodes"]![2]!["graph"]!["nodes"]![2]!["graph"]!["schema_version"] = "1.9.0";
        Assert.Equal(
            "unsupported schema_version '1.9.0' (in 'Remediate/Fix'): this build reads 1.0.0, 1.1.0, 1.2.0, 1.3.0, 1.4.0, 1.5.0 and 1.6.0",
            Assert.Throws<FormatException>(() => nested.Deserialize<GraphDefinition>()!.ToTopology()).Message);
    }

    [Fact]
    public void ToTopology_Refuses_A_Kind_That_Does_Not_Fit()
    {
        var old = Hierarchy() with { SchemaVersion = "1.0.0" };
        Assert.Equal(
            "node 'Investigate' has a kind, which schema_version 1.0.0 doesn't have",
            Assert.Throws<FormatException>(() => old.ToTopology()).Message);

        var noGraph = ReadFixture("hierarchy.json")!;
        noGraph["nodes"]![2]!["graph"]!["nodes"]![2]!.AsObject().Remove("graph");
        Assert.Equal(
            "node 'Remediate/Fix' is a subgraph but has neither branches nor graph",
            Assert.Throws<FormatException>(() => noGraph.Deserialize<GraphDefinition>()!.ToTopology()).Message);

        var plainWithBranches = ReadFixture("hierarchy.json")!;
        plainWithBranches["nodes"]![0]!["branches"] = new JsonArray();
        Assert.Equal(
            "node 'Triage' is a plain node but has branches",
            Assert.Throws<FormatException>(() => plainWithBranches.Deserialize<GraphDefinition>()!.ToTopology()).Message);

        var groupWithGraph = ReadFixture("hierarchy.json")!;
        groupWithGraph["nodes"]![1]!["graph"] = ReadFixture("hierarchy.json")!["nodes"]![2]!["graph"]!.DeepClone();
        Assert.Equal(
            "node 'Investigate' is a parallel_group but has both branches and graph",
            Assert.Throws<FormatException>(() => groupWithGraph.Deserialize<GraphDefinition>()!.ToTopology()).Message);
    }

    [Fact]
    public void An_Unknown_Kind_Is_Not_A_Graph_Definition()
    {
        var unknown = ReadFixture("hierarchy.json")!;
        unknown["nodes"]![1]!["kind"] = "loop";

        Assert.ThrowsAny<JsonException>(() => unknown.Deserialize<GraphDefinition>());
    }

    private static void AssertSameJson(GraphDefinition expected, GraphDefinition actual) =>
        Assert.True(
            JsonNode.DeepEquals(JsonSerializer.SerializeToNode(expected), JsonSerializer.SerializeToNode(actual)),
            $"Expected: {JsonSerializer.Serialize(expected)}\nActual: {JsonSerializer.Serialize(actual)}");

    // --- Paths ------------------------------------------------------------------

    /// <summary>
    /// Whether a run event's node path names something in <paramref name="level"/>: one segment per
    /// level, the last a node, a branch of a group, or a node of a subgraph's child.
    /// </summary>
    private static bool Resolves(GraphDefinition level, string[] path)
    {
        var first = path.FirstOrDefault();
        if (level.Nodes.Find(n => n.Name == first) is not { } node)
        {
            return false;
        }

        var rest = path[1..];
        if (rest.Length == 0)
        {
            return true;
        }

        if (rest.Length == 1 && node.Branches?.Exists(b => b.Name == rest[0]) == true)
        {
            return true;
        }

        return node.Graph is { } child && Resolves(child, rest);
    }

    private static async Task<List<string>> StartedPaths(RunEventChannel channel)
    {
        var paths = new List<string>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token))
        {
            if (e is NodeStarted or BranchStarted)
            {
                paths.Add(e.NodePath!);
            }
        }

        return paths;
    }

    [Fact]
    public async Task Every_Node_And_Branch_Path_A_Run_Reports_Resolves_In_The_Export()
    {
        var definition = Incident().ToDefinition();
        var graph = Incident().Compile();
        var store = new InMemoryCheckpointStore();

        var channel = new RunEventChannel();
        var first = await graph.RunAsync("t", 1, store, new RunOptions { Observer = channel });
        Assert.IsType<GraphExecutionResult<int>.Interrupted>(first);
        var paths = await StartedPaths(channel);

        channel = new RunEventChannel();
        var second = await graph.ResumeAsync("t", HumanSignal.Approve("ops"), store, new RunOptions { Observer = channel });
        Assert.Equal(1, Assert.IsType<GraphExecutionResult<int>.Completed>(second).State);
        paths.AddRange(await StartedPaths(channel));

        foreach (var expected in new[] { "Investigate/Kubernetes", "Investigate/GitHub", "Remediate/Plan", "Remediate/Fix/Apply", "Report" })
        {
            Assert.Contains(expected, paths);
        }

        Assert.All(paths, path => Assert.True(Resolves(definition, path.Split('/')), $"{path} doesn't resolve"));
    }

    // --- Validation -------------------------------------------------------------

    [Fact]
    public void Compile_Validates_The_Whole_Hierarchy_And_Its_Verdicts_Match_The_Export()
    {
        Assert.True(GraphValidator.Validate(Incident().Compile().ToTopology()).IsValid);

        // A group Compile refuses with its own error is flagged by the export with the new codes,
        // which is what the CLI reads.
        StateGraph<int> Empty() => new StateGraph<int>().AddParallelGroup("G", new ParallelBranches<int>(), (s, _) => s);
        Assert.Equal("parallel group 'G' has no branches", CompileError(Empty()));
        var empty = Assert.Single(GraphValidator.Validate(Empty().ToTopology()).Errors);
        Assert.Equal("STRATA_ERR_EMPTY_GROUP", empty.Code);
        Assert.Equal("G", empty.Node);

        StateGraph<int> Duplicate() => new StateGraph<int>().AddParallelGroup(
            "G", new ParallelBranches<int>().Branch("B", Noop).Branch("B", Noop), (s, _) => s);
        Assert.Equal("parallel group 'G' has more than one branch named 'B'", CompileError(Duplicate()));
        var duplicate = Assert.Single(GraphValidator.Validate(Duplicate().ToTopology()).Errors);
        Assert.Equal("STRATA_ERR_DUPLICATE_BRANCH", duplicate.Code);
        Assert.Equal("B", duplicate.RelatedNode);
    }

    [Fact]
    public void A_Child_Error_In_An_Exported_File_Is_Reported_With_Its_Qualified_Name()
    {
        // An edited file: the child's `Abort` loses the reject route into it.
        var definition = Incident().ToDefinition();
        definition.Nodes[2].Graph!.Edges.RemoveAll(e => e.To == "Abort");

        var error = Assert.Single(GraphValidator.Validate(definition.ToTopology()).Errors);
        Assert.Equal("STRATA_ERR_ORPHAN", error.Code);
        Assert.Equal("Remediate/Abort", error.Node);
        Assert.Equal(
            "Node 'Remediate/Abort' is isolated and unreachable from the entry point 'Remediate/Plan'.",
            error.Message);
    }
}
