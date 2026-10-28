using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Graph;
using Datalium.Strata.Storage.Schema;
using Datalium.Strata.Validation;

namespace Datalium.Strata.Tests;

/// <summary>
/// Interrupt boundaries in exported definitions (<c>design/48-Boundaries-In-Definitions.md</c>, SG-37):
/// <c>interrupt</c> on every boundary node, with its timeout, at <c>schema_version</c> <c>1.6.0</c>; the
/// <c>STRATA_ERR_BOUNDARY_IN_MAP</c> validation code; and the shape checks. Mirrors
/// <c>rust/strata-core/tests/boundary_definitions.rs</c>; keep the two in step. Both build the graph below and
/// compare its export to <c>schema/fixtures/graph-export/boundaries.json</c>.
/// </summary>
public class BoundaryDefinitionTests
{
    private static int Noop(int s) => s;

    private static JsonNode ReadFixture(string relative, [CallerFilePath] string sourceFilePath = "") =>
        JsonNode.Parse(File.ReadAllText(Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(sourceFilePath)!, "..", "..", "..", "schema", "fixtures", relative))))!;

    private static JsonNode Export(GraphDefinition definition) => JsonSerializer.SerializeToNode(definition)!;

    private static void AssertSameJson(JsonNode expected, JsonNode actual) =>
        Assert.True(JsonNode.DeepEquals(expected, actual), actual.ToJsonString());

    private static string ShapeError(JsonNode value) =>
        Assert.Throws<FormatException>(() => value.Deserialize<GraphDefinition>()!.ToTopology()).Message;

    private static void AssertSoundShape(JsonNode value) => value.Deserialize<GraphDefinition>()!.ToTopology();

    /// <summary><c>Draft</c> -> <c>Sign</c> (a boundary with no timeout).</summary>
    private static CompiledGraph<int> SignOff() =>
        new StateGraph<int>()
            .AddNode("Draft", Noop)
            .AddNode("Sign", Noop)
            .AddEdge("Draft", "Sign")
            .AddInterruptBoundary("Sign")
            .Compile();

    /// <summary>What each map item runs: one plain node.</summary>
    private static CompiledGraph<int> Check() =>
        new StateGraph<int>().AddNode("Check", Noop).Compile();

    /// <summary>
    /// The fixture's graph: <c>Plan</c> -> <c>Approve</c> (a boundary with a timeout and both routes) ->
    /// <c>Gather</c> (a group that is a boundary) -> <c>Review</c> (a subgraph that is a boundary, whose child has
    /// its own) -> <c>Fan</c> (a map that is a boundary) -> <c>Ship</c>.
    /// </summary>
    private static StateGraph<int> Release() =>
        new StateGraph<int>()
            .AddNode("Plan", Noop)
            .AddNode("Approve", Noop, metadata: new Dictionary<string, string> { ["role"] = "release-manager" })
            .AddParallelGroup(
                "Gather",
                new ParallelBranches<int>().Branch("Logs", Noop).Branch("Metrics", Noop),
                (s, _) => s)
            .AddSubgraph("Review", SignOff(), s => s, (_, child) => child)
            .AddMap("Fan", _ => Array.Empty<int>(), item => item.ToString(), Check(), (s, _) => s, (s, _) => s)
            .AddNode("Ship", Noop)
            .AddNode("Abort", Noop)
            .AddNode("Escalate", Noop)
            .AddEdge("Plan", "Approve")
            .AddEdge("Approve", "Gather")
            .AddEdge("Gather", "Review")
            .AddEdge("Review", "Fan")
            .AddEdge("Fan", "Ship")
            .AddInterruptBoundary("Approve", onReject: "Abort", timeout: TimeSpan.FromMinutes(30), onTimeout: "Escalate")
            .AddInterruptBoundary("Gather")
            .AddInterruptBoundary("Review", timeout: TimeSpan.FromHours(1))
            .AddInterruptBoundary("Fan", timeout: TimeSpan.FromMinutes(5))
            .SetEntryPoint("Plan");

    private static CompiledGraph<int> CompiledRelease() =>
        Release().Compile(new CompilationOptions { GraphId = "release", GraphVersion = "2" });

    // --- Export ---------------------------------------------------------------

    [Fact]
    public void A_Compiled_Graph_Exports_Its_Boundaries_As_The_Shared_Fixture() =>
        AssertSameJson(ReadFixture(Path.Combine("graph-export", "boundaries.json")), Export(CompiledRelease().ToDefinition()));

    [Fact]
    public void The_Builder_Exports_The_Same_Boundaries_Without_The_Identity()
    {
        var expected = ReadFixture(Path.Combine("graph-export", "boundaries.json"));
        expected.AsObject().Remove("identity");

        // Still 1.6.0: the boundaries alone need it.
        AssertSameJson(expected, Export(Release().ToDefinition()));
    }

    [Fact]
    public void The_Boundaries_Fixture_Round_Trips_And_Has_A_Sound_Shape()
    {
        var fixture = ReadFixture(Path.Combine("graph-export", "boundaries.json"));
        var topology = fixture.Deserialize<GraphDefinition>()!.ToTopology();

        Assert.True(GraphValidator.Validate(topology).IsValid);
        AssertSameJson(fixture, Export(topology.ToDefinition()));
        Assert.Equal(new GraphInterrupt(1_800_000), topology.DetailOf("Approve")!.Interrupt);
    }

    [Fact]
    public void Only_A_Level_With_A_Boundary_Moves_To_1_6_0()
    {
        var plain = new StateGraph<int>().AddNode("A", Noop).AddNode("B", Noop).AddEdge("A", "B").SetEntryPoint("A");
        Assert.Equal("1.0.0", plain.ToDefinition().SchemaVersion);
        Assert.All(plain.ToDefinition().Nodes, node => Assert.Null(node.Interrupt));

        Assert.Equal("1.6.0", plain.AddInterruptBoundary("A").ToDefinition().SchemaVersion);

        // A boundary inside a child moves the child's level only.
        var parent = new StateGraph<int>()
            .AddSubgraph("Review", SignOff(), s => s, (_, child) => child)
            .SetEntryPoint("Review")
            .ToDefinition();
        Assert.Equal("1.1.0", parent.SchemaVersion);
        Assert.Equal("1.6.0", parent.Nodes[0].Graph!.SchemaVersion);
    }

    [Fact]
    public void A_Boundary_Name_That_Is_Not_A_Node_Is_Not_Written_And_Its_Route_Still_Dangles()
    {
        var graph = new StateGraph<int>()
            .AddNode("A", Noop)
            .AddNode("Abort", Noop)
            .AddInterruptBoundary("Elsewhere", onReject: "Abort")
            .SetEntryPoint("A");
        var definition = graph.ToDefinition();

        Assert.All(definition.Nodes, node => Assert.Null(node.Interrupt));
        Assert.Equal("1.0.0", definition.SchemaVersion);
        Assert.Contains(GraphValidator.Validate(graph.ToTopology()).Errors, e => e.Kind == ValidationErrorKind.DanglingEdge);
    }

    [Fact]
    public void A_Timeout_Is_Written_In_Whole_Milliseconds_And_Re_Marking_Drops_It()
    {
        var timed = new StateGraph<int>()
            .AddNode("A", Noop)
            .AddInterruptBoundary("A", timeout: TimeSpan.FromMicroseconds(2_500_700))
            .SetEntryPoint("A");
        Assert.Equal(2_500, timed.ToDefinition().Nodes[0].Interrupt!.TimeoutMs);

        var untimed = timed.AddInterruptBoundary("A");
        Assert.NotNull(untimed.ToDefinition().Nodes[0].Interrupt);
        Assert.Null(untimed.ToDefinition().Nodes[0].Interrupt!.TimeoutMs);
    }

    [Fact]
    public void A_Wait_Node_Marked_A_Boundary_Is_Not_Exported_As_One()
    {
        // Compile refuses the pair; the builder's export doesn't write a shape the CLI would refuse.
        var definition = new StateGraph<int>()
            .AddTimer("Window", TimeSpan.FromMinutes(1))
            .AddInterruptBoundary("Window")
            .SetEntryPoint("Window")
            .ToDefinition();

        Assert.Null(definition.Nodes[0].Interrupt);
        AssertSoundShape(Export(definition));
    }

    // --- Validation -----------------------------------------------------------

    [Fact]
    public void A_Boundary_In_A_Maps_Template_Is_Reported_At_Any_Depth_After_Its_Levels_Waits()
    {
        var topology = ReadFixture(Path.Combine("graph-validation", "boundary-in-map.json"))
            .Deserialize<GraphDefinition>()!.ToTopology();

        var result = GraphValidator.Validate(topology);
        Assert.Equal(
            [
                ("STRATA_ERR_WAIT_IN_MAP", "Fan/*/Window"),
                ("STRATA_ERR_BOUNDARY_IN_MAP", "Fan/*/Read"),
                ("STRATA_ERR_BOUNDARY_IN_MAP", "Fan/*/Inner/Sign"),
            ],
            result.Errors.Select(e => (e.Code, e.Node)));
        Assert.Equal(ValidationErrorKind.BoundaryInMap, result.Errors[1].Kind);
        Assert.Equal(
            "Interrupt boundary 'Fan/*/Read' is inside a map's template; a map item can't freeze.",
            result.Errors[1].Message);
    }

    [Fact]
    public void A_Timeout_Without_A_Route_Is_Valid()
    {
        var definition = new StateGraph<int>()
            .AddNode("A", Noop)
            .AddInterruptBoundary("A", timeout: TimeSpan.FromMinutes(1))
            .SetEntryPoint("A")
            .Compile()
            .ToDefinition();

        Assert.True(GraphValidator.Validate(definition.ToTopology()).IsValid);
    }

    // --- Shape ----------------------------------------------------------------

    [Fact]
    public void An_Interrupt_Needs_1_6_0()
    {
        var old = ReadFixture(Path.Combine("graph-export", "reject-route.json"));
        old["schema_version"] = "1.5.0";
        Assert.Equal("node 'Approve' has an interrupt, which schema_version 1.5.0 doesn't have", ShapeError(old));

        // In a child, named by its path.
        var nested = ReadFixture(Path.Combine("graph-export", "hierarchy.json"));
        nested["nodes"]![2]!["graph"]!["schema_version"] = "1.1.0";
        Assert.Equal(
            "node 'Remediate/Approve' has an interrupt, which schema_version 1.1.0 doesn't have",
            ShapeError(nested));
    }

    [Fact]
    public void A_Wait_Node_Cant_Have_An_Interrupt()
    {
        var both = ReadFixture(Path.Combine("graph-export", "waits.json"));
        both["nodes"]![2]!["interrupt"] = new JsonObject { ["timeout_ms"] = null };
        Assert.Equal("node 'Deploying' is a wait and can't also have an interrupt", ShapeError(both));
    }

    [Fact]
    public void A_Negative_Timeout_Is_Refused()
    {
        var negative = ReadFixture(Path.Combine("graph-export", "waits.json"));
        negative["nodes"]![1]!["interrupt"]!["timeout_ms"] = -1;
        Assert.Equal("node 'Approve' has an interrupt with a negative timeout_ms", ShapeError(negative));
    }

    [Fact]
    public void At_1_6_0_A_Route_Needs_What_Takes_It()
    {
        var unmarked = ReadFixture(Path.Combine("graph-export", "reject-route.json"));
        unmarked["nodes"]![1]!.AsObject().Remove("interrupt");
        unmarked["nodes"]![0]!["interrupt"] = new JsonObject { ["timeout_ms"] = null };
        Assert.Equal("node 'Approve' has an on_reject edge but is not an interrupt boundary", ShapeError(unmarked));

        var untimed = ReadFixture(Path.Combine("graph-export", "waits.json"));
        untimed["nodes"]![1]!["interrupt"]!["timeout_ms"] = null;
        Assert.Equal("node 'Approve' has an on_timeout edge but no timeout", ShapeError(untimed));

        var untimedWait = ReadFixture(Path.Combine("graph-export", "waits.json"));
        untimedWait["nodes"]![2]!["wait"]!["timeout_ms"] = null;
        Assert.Equal("node 'Deploying' has an on_timeout edge but no timeout", ShapeError(untimedWait));

        // An edge from a name that isn't a node is the validator's.
        var stray = ReadFixture(Path.Combine("graph-export", "reject-route.json"));
        stray["edges"]!.AsArray().Add(new JsonObject
        {
            ["from"] = "Nowhere",
            ["to"] = "Abort",
            ["guarded"] = true,
            ["condition"] = "on_reject",
        });
        AssertSoundShape(stray);
    }

    [Fact]
    public void Before_1_6_0_A_Route_From_An_Unmarked_Node_Is_Still_Read()
    {
        var old = ReadFixture(Path.Combine("graph-export", "waits.json"));
        old["schema_version"] = "1.4.0";
        old["nodes"]![1]!.AsObject().Remove("interrupt");

        Assert.True(GraphValidator.Validate(old.Deserialize<GraphDefinition>()!.ToTopology()).IsValid);
    }
}
