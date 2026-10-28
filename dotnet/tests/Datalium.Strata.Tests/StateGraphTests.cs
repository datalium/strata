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
            .AddEdge("A", "B")
            .AddConditionalEdge("B", "A", _ => false)
            .Compile();

        Assert.NotNull(compiled);
    }
}
