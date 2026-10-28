using System.Runtime.CompilerServices;
using System.Text.Json;
using Datalium.Strata.Graph;
using Datalium.Strata.Storage.Schema;
using Datalium.Strata.Validation;

namespace Datalium.Strata.Tests;

/// <summary>
/// Proves the Phase 2 exit criterion: identical fixture graphs (good and bad) compiled against
/// both engines produce equivalent accept/reject verdicts. Reads the same
/// <c>schema/fixtures/graph-validation/*.json</c> files as
/// <c>rust/strata-core/src/validate.rs</c>'s <c>fixture_tests</c> module.
/// </summary>
public class GraphValidationParityTests
{
    private static string FixturesDir([CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        return Path.GetFullPath(Path.Combine(testDir, "..", "..", "..", "schema", "fixtures", "graph-validation"));
    }

    private static GraphTopology LoadTopology(string fileName)
    {
        var raw = File.ReadAllText(Path.Combine(FixturesDir(), fileName));
        return JsonSerializer.Deserialize<GraphDefinition>(raw)!.ToTopology();
    }

    [Fact]
    public void Valid_Graph_Passes_Validation()
    {
        var result = GraphValidator.Validate(LoadTopology("valid.json"));
        Assert.True(result.IsValid);
    }

    [Fact]
    public void Missing_Entry_Point_Is_Rejected()
    {
        var result = GraphValidator.Validate(LoadTopology("missing-entry-point.json"));
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Kind == ValidationErrorKind.MissingEntryPoint);
    }

    [Fact]
    public void Orphan_Node_Is_Rejected()
    {
        var result = GraphValidator.Validate(LoadTopology("orphan-node.json"));
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Kind == ValidationErrorKind.OrphanNode && e.Node == "B");
    }

    [Fact]
    public void Unguarded_Cycle_Is_Rejected()
    {
        var result = GraphValidator.Validate(LoadTopology("unguarded-cycle.json"));
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Kind == ValidationErrorKind.UnguardedCycle);
    }

    [Fact]
    public void Two_Paths_Rejoining_Inside_A_Guarded_Loop_Are_Valid()
    {
        // A -> B -> D and A -> C -> D, with D -> A guarded. Every cycle passes through the
        // guarded edge; C -> D is only a cross edge into D.
        var result = GraphValidator.Validate(LoadTopology("guarded-loop-rejoin.json"));
        Assert.True(result.IsValid, string.Join("; ", result.Errors.Select(e => e.Message)));
    }

    [Fact]
    public void A_Guard_On_The_First_Edge_Of_A_Loop_Is_Valid()
    {
        // A -> B guarded, B -> A plain. Which edge the DFS closes the cycle with must not
        // decide the verdict.
        var result = GraphValidator.Validate(LoadTopology("guarded-first-edge-loop.json"));
        Assert.True(result.IsValid, string.Join("; ", result.Errors.Select(e => e.Message)));
    }

    [Fact]
    public void An_Unguarded_Cycle_Inside_A_Guarded_Loop_Is_Still_Rejected()
    {
        // The outer loop is guarded (C -> A) but B <-> C is a cycle of plain edges.
        var result = GraphValidator.Validate(LoadTopology("unguarded-cycle-in-guarded-loop.json"));
        Assert.False(result.IsValid);
        Assert.Contains(
            result.Errors,
            e => e.Kind == ValidationErrorKind.UnguardedCycle && e.Node == "C" && e.RelatedNode == "B");
    }

    private static List<(string From, string To)> DanglingEdges(ValidationResult result) =>
        result.Errors
            .Where(e => e.Kind == ValidationErrorKind.DanglingEdge)
            .Select(e => (e.Node, e.RelatedNode!))
            .ToList();

    [Fact]
    public void An_Edge_To_A_Name_That_Is_Not_A_Node_Is_Rejected()
    {
        var result = GraphValidator.Validate(LoadTopology("dangling-edge-target.json"));
        Assert.Equal([("B", "Missing")], DanglingEdges(result));
        var error = Assert.Single(result.Errors);
        Assert.Equal("STRATA_ERR_DANGLING_EDGE", error.Code);
    }

    [Fact]
    public void An_Edge_From_A_Name_That_Is_Not_A_Node_Is_Rejected()
    {
        // B is reachable through A, so the dangling edge is the only error.
        var result = GraphValidator.Validate(LoadTopology("dangling-edge-source.json"));
        Assert.Equal([("Ghost", "B")], DanglingEdges(result));
        Assert.Single(result.Errors);
    }

    [Fact]
    public void A_Name_That_Is_Not_A_Node_Does_Not_Bridge_Two_Nodes()
    {
        // A -> Ghost -> B: both edges dangle, and B is not made reachable by a route through a
        // name that is not in the graph.
        var result = GraphValidator.Validate(LoadTopology("dangling-edge-bridge.json"));
        Assert.Equal([("A", "Ghost"), ("Ghost", "B")], DanglingEdges(result));
        Assert.Contains(result.Errors, e => e.Kind == ValidationErrorKind.OrphanNode && e.Node == "B");
    }

    [Fact]
    public void Interop_Sample_Graph_Is_Valid()
    {
        // The Phase 1 checkpoint/HITL sample graph — its guarded retry edge must not be
        // misflagged as an unguarded cycle.
        var raw = File.ReadAllText(Path.Combine(FixturesDir(), "..", "graph-definition.sample.json"));
        var topology = JsonSerializer.Deserialize<GraphDefinition>(raw)!.ToTopology();

        var result = GraphValidator.Validate(topology);
        Assert.True(result.IsValid);
    }

    /// <summary><c>(Code, Node, RelatedNode)</c> of every error, in order.</summary>
    private static List<(string Code, string Node, string? Related)> Summary(ValidationResult result) =>
        [.. result.Errors.Select(e => (e.Code, e.Node, e.RelatedNode))];

    [Fact]
    public void A_Valid_Hierarchy_Passes_At_Every_Level()
    {
        var result = GraphValidator.Validate(LoadTopology("valid-hierarchy.json"));
        Assert.True(result.IsValid, string.Join("; ", result.Errors.Select(e => e.Message)));
    }

    [Fact]
    public void An_Orphan_Inside_A_Subgraph_Is_Named_By_Its_Qualified_Path()
    {
        var result = GraphValidator.Validate(LoadTopology("child-orphan.json"));
        Assert.Equal([("STRATA_ERR_ORPHAN", "Remediate/Abort", null)], Summary(result));
        Assert.Equal(
            "Node 'Remediate/Abort' is isolated and unreachable from the entry point 'Remediate/Plan'.",
            result.Errors[0].Message);
    }

    [Fact]
    public void An_Unguarded_Cycle_Two_Levels_Down_Is_Qualified_At_Both_Levels()
    {
        var result = GraphValidator.Validate(LoadTopology("grandchild-unguarded-cycle.json"));
        Assert.Equal(
            [("STRATA_ERR_UNGUARDED_CYCLE", "Remediate/Fix/Verify", "Remediate/Fix/Apply")],
            Summary(result));
    }

    [Fact]
    public void A_Group_With_No_Branches_Is_Rejected()
    {
        var result = GraphValidator.Validate(LoadTopology("empty-group.json"));
        Assert.Equal([("STRATA_ERR_EMPTY_GROUP", "Investigate", null)], Summary(result));
        Assert.Equal(ValidationErrorKind.EmptyGroup, result.Errors[0].Kind);
    }

    [Fact]
    public void A_Group_With_Two_Branches_Of_One_Name_Is_Rejected()
    {
        var result = GraphValidator.Validate(LoadTopology("duplicate-branch.json"));
        Assert.Equal([("STRATA_ERR_DUPLICATE_BRANCH", "Investigate", "Kubernetes")], Summary(result));
        Assert.Equal(ValidationErrorKind.DuplicateBranch, result.Errors[0].Kind);
    }

    [Fact]
    public void Errors_Come_Level_First_Then_Groups_Then_Children()
    {
        var result = GraphValidator.Validate(LoadTopology("errors-at-every-level.json"));
        Assert.Equal(
            [
                ("STRATA_ERR_ORPHAN", "Island", null),
                ("STRATA_ERR_DUPLICATE_BRANCH", "G", "X"),
                ("STRATA_ERR_ORPHAN", "S/Q", null),
                ("STRATA_ERR_EMPTY_GROUP", "S/H", null),
            ],
            Summary(result));
    }
}
