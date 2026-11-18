using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Xunit.Abstractions;

namespace Datalium.Strata.Tests;

/// <summary>Runs the allocation counts alone: they read the whole process's allocations.</summary>
[CollectionDefinition(nameof(AllocationCounts), DisableParallelization = true)]
public sealed class AllocationCounts;

/// <summary>
/// What a node and a branch cost the heap (<c>design/49-Burst-Throughput.md</c>): a burst of runs that
/// really wait is held up by the garbage collector, which does work in proportion to what the runs in
/// flight hold, so bytes a node or a branch allocates for nothing come back as throughput lost. This
/// measures them, so they don't come back unnoticed. Bytes, unlike a timing, are the same on every machine
/// for one runtime. The Rust twin counts allocations: <c>rust/strata-bench/tests/allocations.rs</c>.
/// </summary>
[Collection(nameof(AllocationCounts))]
public sealed class AllocationTests(ITestOutputHelper output)
{
    private sealed record Work(int Step);

    // Each handler completes at once, so the whole run stays on the test's thread and allocates the same on
    // every run and every machine. A handler that yields would add what suspending costs, but how much
    // depends on where the thread pool runs each continuation: on Linux CI a branch varied by 230 bytes
    // between runs. What a node and a branch allocate for nothing (a key, a token source, a closure)
    // they allocate either way.
    private static ValueTask<Work> Step(Work work, NodeContext _, CancellationToken ct) =>
        ValueTask.FromResult(work with { Step = work.Step + 1 });

    private static CompiledGraph<Work> Line(int nodes)
    {
        var graph = new StateGraph<Work>();
        for (var i = 0; i < nodes; i++)
        {
            graph = graph.AddNode($"n{i}", Step);
        }

        for (var i = 1; i < nodes; i++)
        {
            graph = graph.AddEdge($"n{i - 1}", $"n{i}");
        }

        return graph.SetEntryPoint("n0").Compile();
    }

    private static CompiledGraph<Work> Group(int branches)
    {
        var fan = new ParallelBranches<Work>();
        for (var i = 0; i < branches; i++)
        {
            fan = fan.Branch($"b{i}", Step);
        }

        return new StateGraph<Work>()
            .AddParallelGroup("fan", fan, (entry, results) => entry with { Step = entry.Step + results.Count })
            .SetEntryPoint("fan")
            .Compile();
    }

    /// <summary>The bytes one run of <paramref name="graph"/> allocates once the process is warm: the fewest of five.</summary>
    private static async Task<long> Allocated(CompiledGraph<Work> graph)
    {
        var store = new InMemoryCheckpointStore();
        for (var i = 0; i < 50; i++)
        {
            Assert.IsType<GraphExecutionResult<Work>.Completed>(await graph.RunAsync("t", new Work(0), store));
        }

        var fewest = long.MaxValue;
        for (var i = 0; i < 5; i++)
        {
            var before = GC.GetTotalAllocatedBytes(precise: true);
            var end = await graph.RunAsync("t", new Work(0), store);
            fewest = Math.Min(fewest, GC.GetTotalAllocatedBytes(precise: true) - before);
            Assert.IsType<GraphExecutionResult<Work>.Completed>(end);
        }

        return fewest;
    }

    [Fact]
    public async Task A_Node_And_A_Branch_Allocate_Only_What_They_Use()
    {
        var shortLine = await Allocated(Line(11));
        var longLine = await Allocated(Line(101));
        var oneBranch = await Allocated(Group(1));
        var nineBranches = await Allocated(Group(9));

        var perNode = (longLine - shortLine) / 90.0;
        var perBranch = (nineBranches - oneBranch) / 8.0;
        output.WriteLine($"a node: {perNode:0} bytes; a branch: {perBranch:0} bytes; a run of one node: {shortLine - (10 * perNode):0} bytes");

        // On .NET 10 a node allocates 728 bytes and a branch 943 (1,008 and 1,307 before design/49), most of it
        // the engine's state machines, contexts and outcomes, and the same on Windows and Linux. The limits leave
        // room for the runtime to change by a few dozen bytes, not for a token source (64 bytes or more) or an
        // idempotency key built for nobody (about 110).
        Assert.True(perNode <= NodeLimit, $"a node allocated {perNode:0} bytes; the limit is {NodeLimit}");
        Assert.True(perBranch <= BranchLimit, $"a branch allocated {perBranch:0} bytes; the limit is {BranchLimit}");
    }

    private const double NodeLimit = 760;

    private const double BranchLimit = 980;
}
