using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Graph;
using Datalium.Strata.Storage.Schema;
using Datalium.Strata.Validation;

namespace Datalium.Strata.Tests;

/// <summary>
/// <c>StateGraph.ToDefinition()</c> / <c>CompiledGraph.ToDefinition()</c>: the export
/// <c>strata graph validate</c> reads. The Rust twin is
/// <c>rust/strata-core/tests/graph_export.rs</c> — keep the two in step; both pin the same fixture,
/// <c>schema/fixtures/graph-export/</c>.
/// </summary>
public class GraphExportTests
{
    private static int Noop(int s) => s;

    private static JsonNode? ReadFixture(string fileName, [CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(
            Path.Combine(testDir, "..", "..", "..", "schema", "fixtures", "graph-export", fileName));
        return JsonNode.Parse(File.ReadAllText(path));
    }

    /// <summary>Node <c>Charge</c> fails over to <c>ManualReview</c>, which no ordinary edge enters.</summary>
    private static StateGraph<int> FallbackPipeline() =>
        new StateGraph<int>()
            .AddNode("Intake", Noop)
            .AddFallibleNode("Charge", Noop)
            .AddNode("Ship", Noop)
            .AddNode("ManualReview", Noop)
            .AddEdge("Intake", "Charge")
            .AddEdge("Charge", "Ship")
            .AddConditionalEdge("Ship", "Intake", s => s < 3)
            .SetRecoveryPolicy("Charge", RecoveryPolicy.Retry(2).WithFallback("ManualReview"));

    [Fact]
    public void Export_Matches_The_Shared_Fixture()
    {
        var exported = JsonSerializer.SerializeToNode(FallbackPipeline().ToDefinition());

        Assert.True(
            JsonNode.DeepEquals(ReadFixture("fallback-pipeline.json"), exported),
            $"Export did not match the fixture.\nExpected: {ReadFixture("fallback-pipeline.json")}\nActual: {exported}");
    }

    [Fact]
    public void A_Node_Reachable_Only_Through_A_Fallback_Is_Not_An_Orphan_In_The_Export()
    {
        var topology = FallbackPipeline().ToTopology();

        Assert.True(GraphValidator.Validate(topology).IsValid);

        // ...which is what the executor's own edge list would get wrong.
        var routingOnly = FallbackPipeline().Compile().Topology;
        var error = Assert.Single(GraphValidator.Validate(routingOnly).Errors);
        Assert.Equal("STRATA_ERR_ORPHAN", error.Code);
        Assert.Equal("ManualReview", error.Node);
    }

    [Fact]
    public void A_Compiled_Graph_Exports_What_Its_Builder_Did()
    {
        var fromBuilder = FallbackPipeline().ToDefinition();
        var compiled = FallbackPipeline().Compile();

        Assert.True(
            JsonNode.DeepEquals(
                JsonSerializer.SerializeToNode(fromBuilder),
                JsonSerializer.SerializeToNode(compiled.ToDefinition())));
        // The property is what the executor routes over: no fallback edge.
        Assert.Equal(fromBuilder.Edges.Count - 1, compiled.Topology.Edges.Count);
    }

    [Fact]
    public void Fallback_Edges_Follow_Node_Declaration_Order_Whatever_Order_The_Policies_Were_Set_In()
    {
        var graph = new StateGraph<int>()
            .AddFallibleNode("First", Noop)
            .AddFallibleNode("Second", Noop)
            .AddFallibleNode("Third", Noop)
            .AddNode("Rescue", Noop)
            .AddEdge("First", "Second")
            .AddEdge("Second", "Third")
            .AddEdge("Third", "Rescue")
            .SetRecoveryPolicy("Third", new RecoveryPolicy().WithFallback("Rescue"))
            .SetRecoveryPolicy("First", new RecoveryPolicy().WithFallback("Rescue"))
            .SetRecoveryPolicy("Second", new RecoveryPolicy().WithFallback("Rescue"));

        var edges = graph.ToDefinition().Edges;

        Assert.Equal(
            ["First", "Second", "Third"],
            edges.Where(e => e.Condition == "on_failure").Select(e => e.From));
        Assert.All(edges, e => Assert.True(e.Condition is null || e.Guarded));
    }

    /// <summary>One case per way validation can answer. The graph is built twice because reuse is cheap.</summary>
    public static TheoryData<string> CaseNames =>
    [
        "valid chain",
        "guarded retry loop",
        "fallback-only node",
        "empty graph",
        "missing entry point",
        "orphan node",
        "unguarded cycle",
        "orphan and unguarded cycle",
        "edge to a name that is not a node",
        "edge from a name that is not a node",
        "group and subgraph",
    ];

    private static StateGraph<int> Case(string name) => name switch
    {
        "valid chain" => new StateGraph<int>().AddNode("A", Noop).AddNode("B", Noop).AddEdge("A", "B"),
        "guarded retry loop" => new StateGraph<int>()
            .AddNode("A", Noop).AddNode("B", Noop)
            .AddEdge("A", "B").AddConditionalEdge("B", "A", s => s < 3),
        "fallback-only node" => FallbackPipeline(),
        "empty graph" => new StateGraph<int>(),
        "missing entry point" => new StateGraph<int>().AddNode("A", Noop).SetEntryPoint("Nope"),
        "orphan node" => new StateGraph<int>().AddNode("A", Noop).AddNode("B", Noop),
        "unguarded cycle" => new StateGraph<int>()
            .AddNode("A", Noop).AddNode("B", Noop)
            .AddEdge("A", "B").AddEdge("B", "A"),
        "orphan and unguarded cycle" => new StateGraph<int>()
            .AddNode("A", Noop).AddNode("B", Noop).AddNode("Island", Noop)
            .AddEdge("A", "B").AddEdge("B", "A"),
        "edge to a name that is not a node" => new StateGraph<int>()
            .AddNode("A", Noop).AddNode("B", Noop)
            .AddEdge("A", "B").AddEdge("B", "Nope"),
        "edge from a name that is not a node" => new StateGraph<int>()
            .AddNode("A", Noop).AddNode("B", Noop)
            .AddEdge("A", "B").AddEdge("Nope", "B"),
        "group and subgraph" => new StateGraph<int>()
            .AddParallelGroup("G", new ParallelBranches<int>().Branch("X", Noop), (s, _) => s)
            .AddSubgraph("S", new StateGraph<int>().AddNode("C", Noop).Compile(), s => s, (_, c) => c)
            .AddEdge("G", "S"),
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    [Fact]
    public void A_Fallback_To_A_Missing_Node_Is_Flagged_By_The_Export()
    {
        StateGraph<int> Build() => new StateGraph<int>()
            .AddFallibleNode("Charge", Noop)
            .AddNode("Ship", Noop)
            .AddEdge("Charge", "Ship")
            .SetRecoveryPolicy("Charge", new RecoveryPolicy().WithFallback("Nope"));

        // Compile rejects the policy before it validates anything...
        Assert.Throws<InvalidOperationException>(() => Build().Compile());
        // ...and the export, which is what the CLI reads, shows the same fault.
        var error = Assert.Single(GraphValidator.Validate(Build().ToTopology()).Errors);
        Assert.Equal("STRATA_ERR_DANGLING_EDGE", error.Code);
        Assert.Equal("Charge", error.Node);
        Assert.Equal("Nope", error.RelatedNode);
    }

    [Theory]
    [MemberData(nameof(CaseNames))]
    public void Validating_The_Export_Gives_The_Verdict_Compile_Does(string name)
    {
        var cliView = GraphValidator.Validate(Case(name).ToTopology());

        try
        {
            Case(name).Compile();
            Assert.True(cliView.IsValid, $"{name}: Compile accepted, export rejected");
        }
        catch (GraphCompilationException ex)
        {
            Assert.False(cliView.IsValid, $"{name}: Compile rejected, export accepted");
            Assert.Equal(ex.Result.Errors, cliView.Errors);
        }
    }

    [Fact]
    public void A_Graph_Compile_Rejects_Can_Still_Be_Exported()
    {
        var broken = new StateGraph<int>()
            .AddNode("A", Noop).AddNode("B", Noop)
            .AddEdge("A", "B").AddEdge("B", "A");

        var definition = broken.ToDefinition();

        Assert.Equal("A", definition.EntryPoint);
        Assert.Equal(2, definition.Nodes.Count);
        Assert.Equal(2, definition.Edges.Count);
        Assert.Throws<GraphCompilationException>(() => broken.Compile());
    }
}
