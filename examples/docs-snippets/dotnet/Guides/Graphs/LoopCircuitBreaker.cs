// Samples for https://stratamesh.dev/guides/graphs/loop-circuit-breaker/

// docs:begin guides/graphs/loop-circuit-breaker/compile
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

// docs:hide
namespace Datalium.Strata.DocsSnippets.Guides.Graphs.LoopCircuitBreaker;

public class LoopCircuitBreakerSamples
{
    [Fact]
    public async Task TripAndGoOn()
    {
        // docs:show
        var store = new InMemoryCheckpointStore();

        var graph = new StateGraph<Essay>()
            .AddNode("Draft", essay => essay with { Revision = essay.Revision + 1 })
            .AddNode("Review", essay => essay with { Score = Reviewer.Score(essay) })
            .AddNode("Publish", essay => essay with { Published = true })
            .AddEdge("Draft", "Review")
            .AddConditionalEdge("Review", "Draft", essay => essay.Score < essay.MinScore)
            .AddEdge("Review", "Publish")
            .Compile(new CompilationOptions { MaxNodeHops = 20 });

        // docs:hide ...
        await GoOn(graph, store);
    }
    // docs:show

    record Essay(int MinScore, int Revision = 0, int Score = 0, bool Published = false);

    static class Reviewer
    {
        // A stand-in for a model call. It never scores above 7.
        public static int Score(Essay essay) => Math.Min(4 + essay.Revision, 7);
    }
    // docs:end guides/graphs/loop-circuit-breaker/compile

    private static async Task GoOn(CompiledGraph<Essay> graph, InMemoryCheckpointStore store)
    {
        var output = Console.Capture();

        // docs:begin guides/graphs/loop-circuit-breaker/detect
        var result = await graph.RunAsync("essay-1", new Essay(MinScore: 8), store);

        switch (result)
        {
            case GraphExecutionResult<Essay>.Completed done:
                Console.WriteLine($"Published after {done.State.Revision} revisions");
                break;
            case GraphExecutionResult<Essay>.HopLimitExceeded tripped:
                Console.WriteLine($"Stopped: node = {tripped.Node}, hops = {tripped.Hops}, limit = {tripped.Limit}");
                Console.WriteLine(tripped.Message);
                break;
            case GraphExecutionResult<Essay>.Failed failed:
                Console.WriteLine($"Failed: {failed.Message}");
                break;
        }
        // docs:end guides/graphs/loop-circuit-breaker/detect

        var trip = Assert.IsType<GraphExecutionResult<Essay>.HopLimitExceeded>(result);
        Assert.Equal(("Draft", 20, 20), (trip.Node, trip.Hops, trip.Limit));

        // docs:begin guides/graphs/loop-circuit-breaker/checkpoint
        var checkpoint = store.Load("essay-1")!;
        Console.WriteLine($"{checkpoint.Status} at {checkpoint.NextNode}");
        Console.WriteLine($"error: {checkpoint.Metadata!["error"]}");
        Console.WriteLine($"error_kind = {checkpoint.Metadata["error_kind"]}, strata.hops = {checkpoint.Metadata["strata.hops"]}");
        Console.WriteLine($"Revision: {checkpoint.FrozenState!["Revision"]}");
        // docs:end guides/graphs/loop-circuit-breaker/checkpoint

        Assert.False(checkpoint.Metadata.ContainsKey("attempts"));

        // docs:begin guides/graphs/loop-circuit-breaker/resume
        // As it is: it trips again before any node runs.
        var again = await graph.ResumeAsync("essay-1", HumanSignal.Approve("editor"), store);
        if (again is GraphExecutionResult<Essay>.HopLimitExceeded stillTripped)
        {
            Console.WriteLine($"Tripped again: {stillTripped.Message}");
        }

        // Lower the bar, and allow ten more hops for this resume.
        var signal = HumanSignal.Approve("editor").WithOverride("MinScore", 7);
        var resumed = await graph.ResumeAsync("essay-1", signal, store, new RunOptions { MaxNodeHops = 30 });
        if (resumed is GraphExecutionResult<Essay>.Completed { State: var essay })
        {
            Console.WriteLine($"Published after {essay.Revision} revisions");
        }
        // docs:end guides/graphs/loop-circuit-breaker/resume

        Assert.IsType<GraphExecutionResult<Essay>.HopLimitExceeded>(again);
        Assert.IsType<GraphExecutionResult<Essay>.Completed>(resumed);
        Assert.Equal(
            [
                "Stopped: node = Draft, hops = 20, limit = 20",
                "hop limit of 20 reached: node 'Draft' would be hop 21",
                "Failed at Draft",
                "error: hop limit of 20 reached: node 'Draft' would be hop 21",
                "error_kind = hop_limit, strata.hops = 20",
                "Revision: 10",
                "Tripped again: hop limit of 20 reached: node 'Draft' would be hop 21",
                "Published after 11 revisions",
            ],
            output);
    }
}

/// <summary>Stands in for <see cref="System.Console"/> in the samples, so a test can check what they print.</summary>
internal static class Console
{
    private static readonly AsyncLocal<List<string>?> Lines = new();

    public static List<string> Capture() => Lines.Value = [];

    public static void WriteLine(string line) => Lines.Value?.Add(line);
}
