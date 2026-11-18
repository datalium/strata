using Datalium.Strata.Graph;
using Datalium.Strata.Validation;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/src/graph.rs</c>'s test module: valid graph compiles, each of the
/// three structural error classes is caught and rejected with a specific error/exception, and a
/// guarded cycle is allowed through.
/// </summary>
public class StateGraphTests
{
    private static int Noop(int state) => state;

    [Fact]
    public void Valid_Graph_Compiles()
    {
        var compiled = new StateGraph<int>()
            .AddNode("A", Noop)
            .AddNode("B", Noop)
            .AddEdge("A", "B")
            .Compile();

        Assert.Equal("A", compiled.Topology.EntryPoint);
    }

    [Fact]
    public void Missing_Entry_Point_Is_Rejected()
    {
        var graph = new StateGraph<int>()
            .AddNode("A", Noop)
            .SetEntryPoint("DoesNotExist");

        var ex = Assert.Throws<GraphCompilationException>(() => graph.Compile());
        Assert.Contains(ex.Result.Errors, e => e.Kind == ValidationErrorKind.MissingEntryPoint);
    }

    [Fact]
    public void Orphan_Node_Is_Rejected()
    {
        var graph = new StateGraph<int>()
            .AddNode("A", Noop)
            .AddNode("B", Noop);

        var ex = Assert.Throws<GraphCompilationException>(() => graph.Compile());
        Assert.Contains(ex.Result.Errors, e => e.Kind == ValidationErrorKind.OrphanNode && e.Node == "B");
    }

    [Fact]
    public void Unguarded_Cycle_Is_Rejected()
    {
        var graph = new StateGraph<int>()
            .AddNode("A", Noop)
            .AddNode("B", Noop)
            .AddEdge("A", "B")
            .AddEdge("B", "A");

        var ex = Assert.Throws<GraphCompilationException>(() => graph.Compile());
        Assert.Contains(ex.Result.Errors, e => e.Kind == ValidationErrorKind.UnguardedCycle);
    }

    [Fact]
    public void Guarded_Cycle_Compiles()
    {
        var compiled = new StateGraph<int>()
            .AddNode("A", Noop)
            .AddNode("B", Noop)
            .AddNode("C", Noop)
            .AddEdge("A", "B")
            .AddConditionalEdge("B", "A", _ => false)
            .AddEdge("B", "C")
            .Compile();

        Assert.NotNull(compiled);
    }

    [Fact]
    public void A_Guarded_Loop_With_No_Way_Out_Is_Rejected()
    {
        // Every loop is guarded, but Act's exit to an end was forgotten.
        var graph = new StateGraph<int>()
            .AddNode("Plan", Noop)
            .AddNode("Act", Noop)
            .AddNode("Review", Noop)
            .AddEdge("Plan", "Act")
            .AddConditionalEdge("Act", "Review", s => s > 0)
            .AddConditionalEdge("Act", "Plan", _ => true)
            .AddEdge("Review", "Plan");

        var ex = Assert.Throws<GraphCompilationException>(() => graph.Compile());
        Assert.Equal(
            [
                (ValidationErrorKind.NoTerminal, "Plan"),
                (ValidationErrorKind.NoTerminal, "Act"),
                (ValidationErrorKind.NoTerminal, "Review"),
            ],
            ex.Result.Errors.Select(e => (e.Kind, e.Node)));
        Assert.Equal("STRATA_ERR_NO_TERMINAL", ex.Result.Errors[0].Code);
    }

    [Fact]
    public void A_Boundary_Whose_Only_Edge_Is_Its_Reject_Route_Ends_The_Run()
    {
        // Approved, Approve has no edge to take, so the run ends there.
        var compiled = new StateGraph<int>()
            .AddNode("Draft", Noop)
            .AddNode("Approve", Noop)
            .AddEdge("Draft", "Approve")
            .AddInterruptBoundary("Approve", onReject: "Draft")
            .Compile();

        Assert.NotNull(compiled);
    }
}
