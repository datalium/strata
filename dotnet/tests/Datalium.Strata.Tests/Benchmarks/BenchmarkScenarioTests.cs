using Datalium.Strata.Benchmarks;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Streaming;
using CheckpointStatus = Datalium.Strata.Interruption.CheckpointStatus;

namespace Datalium.Strata.Tests.Benchmarks;

/// <summary>
/// Mirrors the tests in <c>rust/strata-bench/src/lib.rs</c> and <c>stats.rs</c>
/// (<c>design/37-Benchmarks.md</c>, SG-27): each benchmark scenario does what a number about it claims. The
/// scenarios are compiled into this project from <c>dotnet/benchmarks/Datalium.Strata.Benchmarks</c>
/// (<c>Scenarios.cs</c>, <c>Stats.cs</c>), so the tests don't depend on BenchmarkDotNet. Keep the two in
/// step.
/// </summary>
public class BenchmarkScenarioTests
{
    [Fact]
    public async Task A_line_runs_every_node_once_with_either_kind_of_handler()
    {
        foreach (var handler in Scenarios.Handlers)
        {
            foreach (var nodes in new[] { 1, 10, 100 })
            {
                var graph = Scenarios.Linear(nodes, handler);
                var store = new LatestOnlyStore();

                var end = Scenarios.Completed(await graph.RunAsync("t", Scenarios.NewWork(0), store));

                Assert.Equal(nodes, end.Step);
                Assert.Equal(0, store.Saves);
            }
        }
    }

    [Fact]
    public async Task A_durable_line_saves_a_checkpoint_per_node_and_one_more()
    {
        var store = new LatestOnlyStore();

        var end = Scenarios.Completed(await Scenarios.Durable().RunAsync("t", Scenarios.NewWork(0), store));

        Assert.Equal(Scenarios.RunNodes, end.Step);
        // `running` before the first node and after each node but the last, whose commit is `completed`.
        Assert.Equal(Scenarios.RunNodes + 1, store.Saves);
        Assert.Equal(CheckpointStatus.Completed, store.Load("t")!.Status);
    }

    [Fact]
    public async Task A_group_runs_every_branch_and_a_map_every_item()
    {
        foreach (var branches in new[] { 2, 8, 32 })
        {
            var end = Scenarios.Completed(await Scenarios.Group(branches).RunAsync("t", Scenarios.NewWork(0), new LatestOnlyStore()));
            Assert.Equal(branches, end.Step);
        }

        foreach (var items in new[] { 8, 64 })
        {
            var end = Scenarios.Completed(await Scenarios.Map(items).RunAsync("t", Scenarios.NewWork(0), new LatestOnlyStore()));
            Assert.Equal(items, end.Step);
        }
    }

    [Fact]
    public async Task An_approval_freezes_once_and_a_resume_finishes_it()
    {
        var (graph, store) = (Scenarios.Approval(), new LatestOnlyStore());

        Scenarios.Interrupted(await graph.RunAsync("t", Scenarios.NewWork(Scenarios.PayloadBytes), store));
        Assert.Equal(1, store.Saves);
        var end = Scenarios.Completed(await graph.ResumeAsync("t", HumanSignal.Approve("bench"), store));

        Assert.Equal(3, end.Step);
        // The same thread id runs again from the start: what the round-trip benchmark does.
        Scenarios.Interrupted(await graph.RunAsync("t", Scenarios.NewWork(0), store));
    }

    [Fact]
    public async Task A_streamer_emits_its_tokens()
    {
        var graph = Scenarios.Streamer(1000);

        var (reader, completion) = graph.RunStreaming("t", Scenarios.NewWork(0), new LatestOnlyStore());
        var tokens = 0;
        await foreach (var chunk in reader.ReadAllAsync())
        {
            if (chunk.Type == StreamChunkType.Token)
            {
                tokens++;
            }
        }

        Assert.Equal(1000, tokens);
        Assert.Equal(1, Scenarios.Completed(await completion).Step);
    }

    [Fact]
    public async Task A_timed_line_records_a_gap_for_every_transition()
    {
        foreach (var handler in Scenarios.Handlers)
        {
            var gaps = new Gaps();
            var graph = Scenarios.Timed(100, handler, gaps);

            Scenarios.Completed(await graph.RunAsync("t", Scenarios.NewWork(0), new LatestOnlyStore()));
            Scenarios.Completed(await graph.RunAsync("t", Scenarios.NewWork(0), new LatestOnlyStore()));

            Assert.Equal(2 * 99, gaps.Take().Count);
            Assert.Empty(gaps.Take());
        }
    }

    [Fact]
    public async Task The_sample_checkpoint_is_an_interrupted_one_with_the_payload()
    {
        var checkpoint = await Scenarios.SampleCheckpointAsync();

        Assert.Equal(CheckpointStatus.Interrupted, checkpoint.Status);
        Assert.Equal("approve", checkpoint.SuspendedNode);
        Assert.True(checkpoint.ToWireJson().ToJsonString().Length > Scenarios.PayloadBytes);
    }

    [Fact]
    public void Percentiles_are_nearest_rank()
    {
        var sorted = Enumerable.Range(1, 1000).Select(n => (long)n).ToArray();

        Assert.Equal(500, Stats.Percentile(sorted, 0.50));
        Assert.Equal(900, Stats.Percentile(sorted, 0.90));
        Assert.Equal(990, Stats.Percentile(sorted, 0.99));
        Assert.Equal(999, Stats.Percentile(sorted, 0.999));
        Assert.Equal(1000, Stats.Percentile(sorted, 1.0));
        Assert.Equal(7, Stats.Percentile([7], 0.999));
    }

    [Fact]
    public void A_distribution_sorts_its_samples()
    {
        var distribution = Distribution.Of([30, 10, 20, 40]);

        Assert.Equal(4, distribution.Samples);
        Assert.Equal(25.0, distribution.Mean);
        Assert.Equal(20, distribution.P50);
        Assert.Equal(40, distribution.P999);
        Assert.Equal(40, distribution.Max);
    }
}
