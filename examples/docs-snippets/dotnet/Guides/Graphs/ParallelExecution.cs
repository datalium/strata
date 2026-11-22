// Samples for https://stratamesh.dev/guides/graphs/parallel-execution/

// docs:begin guides/graphs/parallel-execution/branches
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

// docs:hide
using System.Diagnostics;

namespace Datalium.Strata.DocsSnippets.Guides.Graphs.ParallelExecution;

public class ParallelExecutionSamples
{
    private static ParallelBranches<Trip> Lookups()
    {
        // docs:show
        var lookups = new ParallelBranches<Trip>()
            .Branch("flights", async (trip, context, ct) =>
            {
                await Task.Delay(300, ct);
                return trip with { Flight = "LH 401, $612" };
            })
            .Branch("hotels", async (trip, context, ct) =>
            {
                await Task.Delay(200, ct);
                return trip with { Hotel = "Hotel Aurora, $148/night" };
            })
            .Branch("weather", async (trip, context, ct) =>
            {
                await Task.Delay(100, ct);
                return trip with { Forecast = "sunny, 24C" };
            });

        // docs:hide
        return lookups;
    }

    // docs:show
    sealed record Trip(
        string Destination,
        string? Flight = null,
        string? Hotel = null,
        string? Forecast = null,
        IReadOnlyList<string>? Gaps = null,
        string? Summary = null);
    // docs:end guides/graphs/parallel-execution/branches

    private static CompiledGraph<Trip> BuildGraph(ParallelBranches<Trip> lookups)
    {
        // docs:begin guides/graphs/parallel-execution/group
        var graph = new StateGraph<Trip>()
            .AddNode("ParseRequest", trip => trip with { Destination = trip.Destination.Trim() })
            .AddParallelGroup("GatherInfo", lookups, Merge)
            .AddNode("BuildItinerary", trip => trip with
            {
                Summary = $"{trip.Destination}: {trip.Flight} | {trip.Hotel} | {trip.Forecast}",
            })
            .AddEdge("ParseRequest", "GatherInfo")
            .AddEdge("GatherInfo", "BuildItinerary")
            .Compile();

        // docs:hide
        return graph;
    }

    // docs:show
    // Each branch fills in one field. Keep the first value found.
    static Trip Merge(Trip entry, IReadOnlyList<Trip> outputs) =>
        outputs.Aggregate(entry, (merged, output) => merged with
        {
            Flight = merged.Flight ?? output.Flight,
            Hotel = merged.Hotel ?? output.Hotel,
            Forecast = merged.Forecast ?? output.Forecast,
        });
    // docs:end guides/graphs/parallel-execution/group

    [Fact]
    public async Task RunTheGraph()
    {
        var graph = BuildGraph(Lookups());

        // docs:begin guides/graphs/parallel-execution/run
        var store = new InMemoryCheckpointStore();
        var clock = Stopwatch.StartNew();
        var result = await graph.RunAsync("trip-1", new Trip("  Lisbon "), store);
        if (result is GraphExecutionResult<Trip>.Completed completed)
        {
            Console.WriteLine($"{completed.State.Summary} ({clock.ElapsedMilliseconds} ms)");
        }
        // docs:end guides/graphs/parallel-execution/run

        var state = Assert.IsType<GraphExecutionResult<Trip>.Completed>(result).State;
        Assert.Equal("Lisbon: LH 401, $612 | Hotel Aurora, $148/night | sunny, 24C", state.Summary);
        // The branches overlap: the group takes about as long as the slowest one, not the 600 ms of all three.
        Assert.True(clock.ElapsedMilliseconds < 600, $"took {clock.ElapsedMilliseconds} ms");
    }

    [Fact]
    public async Task HandleAFailingBranch()
    {
        var store = new InMemoryCheckpointStore();
        var noRooms = new ParallelBranches<Trip>()
            .Branch("flights", async (trip, context, ct) =>
            {
                await Task.Delay(300, ct);
                return trip with { Flight = "LH 401, $612" };
            })
            // docs:begin guides/graphs/parallel-execution/failing-branch
            .Branch("hotels", async (trip, context, ct) =>
            {
                await Task.Delay(200, ct);
                throw NodeException.Fatal("no rooms left on these dates");
            })
            // docs:hide ...
            .Branch("weather", async (trip, context, ct) =>
            {
                await Task.Delay(100, ct);
                return trip with { Forecast = "sunny, 24C" };
            });
        // docs:show

        var failing = new StateGraph<Trip>()
            .AddParallelGroup("GatherInfo", noRooms, Merge)
            .SetRecoveryPolicy("GatherInfo", RecoveryPolicy.Retry(2))
            .Compile();

        var result = await failing.RunAsync("trip-2", new Trip("Madeira"), store);
        if (result is GraphExecutionResult<Trip>.NodeFailed failed)
        {
            Console.WriteLine($"{failed.Node} failed after {failed.Attempts} attempt(s): {failed.Message}");
        }
        // docs:end guides/graphs/parallel-execution/failing-branch

        // The error is fatal, so the policy doesn't retry.
        var nodeFailed = Assert.IsType<GraphExecutionResult<Trip>.NodeFailed>(result);
        Assert.Equal("GatherInfo", nodeFailed.Node);
        Assert.Equal(1, nodeFailed.Attempts);
        Assert.Equal("branch 'hotels': no rooms left on these dates", nodeFailed.Message);
    }

    [Fact]
    public async Task KeepPartialResults()
    {
        var store = new InMemoryCheckpointStore();
        var slow = new ParallelBranches<Trip>()
            .Branch("flights", async (trip, context, ct) =>
            {
                await Task.Delay(300, ct);
                return trip with { Flight = "LH 401, $612" };
            })
            .Branch("hotels", async (trip, context, ct) =>
            {
                await Task.Delay(200, ct);
                throw NodeException.Fatal("no rooms left on these dates");
            })
            // docs:begin guides/graphs/parallel-execution/partial
            .Branch("weather", async (trip, context, ct) =>
            {
                await Task.Delay(Timeout.Infinite, ct); // never answers
                return trip;
            })
            // docs:hide ...
            .SetMetadata("weather", new Dictionary<string, string> { ["service"] = "forecast" });
        // docs:show

        var options = new ParallelGroupOptions<Trip>
        {
            MaxConcurrency = 2,
            BranchTimeout = TimeSpan.FromSeconds(1),
            Partial = PartialResults.KeepSucceeded,
        };

        var partial = new StateGraph<Trip>()
            .AddParallelGroupWithOutcomes(
                "GatherInfo",
                slow,
                (entry, results) => Merge(entry, results.Succeeded) with
                {
                    Gaps = [.. results
                        .Where(r => r.Status != BranchStatus.Succeeded)
                        .Select(r => $"{r.Name}: {r.Status} ({r.Error?.Message})")],
                },
                options)
            .Compile();
        // docs:end guides/graphs/parallel-execution/partial

        var result = await partial.RunAsync("trip-3", new Trip("Madeira"), store);

        var state = Assert.IsType<GraphExecutionResult<Trip>.Completed>(result).State;
        Assert.Equal("LH 401, $612", state.Flight);
        Assert.Equal(
            ["hotels: Failed (no rooms left on these dates)", "weather: TimedOut (branch 'weather' timed out after 1000 ms)"],
            state.Gaps);
    }

    [Fact]
    public async Task RaceBranchesWithAJoin()
    {
        var store = new InMemoryCheckpointStore();
        var forecasts = new ParallelBranches<Trip>()
            .Branch("primary", async (trip, context, ct) =>
            {
                await Task.Delay(300, ct);
                return trip with { Forecast = "sunny, 24C" };
            })
            .Branch("backup", async (trip, context, ct) =>
            {
                await Task.Delay(100, ct);
                return trip with { Forecast = "clear, 23C" };
            });
        BranchOutcomes<Trip>? seen = null;

        // docs:begin guides/graphs/parallel-execution/race
        var race = new StateGraph<Trip>()
            .AddParallelGroupWithOutcomes(
                "Forecast",
                forecasts, // "primary" answers after 300 ms, "backup" after 100 ms
                (entry, results) =>
                {
                    // docs:hide
                    seen = results;
                    // docs:show
                    Console.WriteLine($"Won: {string.Join(", ", results.Deciding)}; " +
                        string.Join(", ", results.Select(r => $"{r.Name} {r.Status}")));
                    return results[results.Deciding[0]]!.State!;
                },
                new ParallelGroupOptions<Trip> { Join = JoinPolicy<Trip>.FirstSuccess })
            .Compile();

        // The other joins:
        var firstSunny = new ParallelGroupOptions<Trip>
        {
            Join = JoinPolicy<Trip>.FirstMatching(t => t.Forecast!.StartsWith("sunny"), "a sunny forecast"),
        };
        var twoAnswers = new ParallelGroupOptions<Trip> { Join = JoinPolicy<Trip>.Quorum(2) };
        // docs:end guides/graphs/parallel-execution/race

        // The first answer wins, and the other branch is cancelled.
        var won = Assert.IsType<GraphExecutionResult<Trip>.Completed>(
            await race.RunAsync("trip-4", new Trip("Lisbon"), store));
        Assert.Equal("clear, 23C", won.State.Forecast);
        Assert.NotNull(seen);
        Assert.Equal(["backup"], seen.Deciding);
        Assert.Equal(
            ["primary Cancelled", "backup Succeeded"],
            seen.Select(r => $"{r.Name} {r.Status}"));

        // The first sunny forecast wins, though it comes second.
        var sunny = new StateGraph<Trip>()
            .AddParallelGroup("Forecast", forecasts, Merge, firstSunny)
            .Compile();
        var matched = Assert.IsType<GraphExecutionResult<Trip>.Completed>(
            await sunny.RunAsync("trip-5", new Trip("Lisbon"), store));
        Assert.Equal("sunny, 24C", matched.State.Forecast);

        // A quorum of 2 waits for both.
        IReadOnlyList<string>? deciding = null;
        var quorum = new StateGraph<Trip>()
            .AddParallelGroupWithOutcomes(
                "Forecast",
                forecasts,
                (entry, results) =>
                {
                    deciding = results.Deciding;
                    return Merge(entry, results.Succeeded);
                },
                twoAnswers)
            .Compile();
        Assert.IsType<GraphExecutionResult<Trip>.Completed>(await quorum.RunAsync("trip-6", new Trip("Lisbon"), store));
        Assert.Equal(["backup", "primary"], deciding);
    }
}
