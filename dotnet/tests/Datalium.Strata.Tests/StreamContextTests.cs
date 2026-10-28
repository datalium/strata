using System.Diagnostics;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/src/streaming.rs</c>'s test module: a slow consumer causes the
/// producer to block rather than drop chunks, verified both at a small test capacity (fast,
/// deterministic blocking proof via timing) and at the real 1024 default (end-to-end bound
/// proof).
/// </summary>
public class StreamContextTests
{
    /// <summary>
    /// Runs <paramref name="total"/> <c>EmitTokenAsync</c> calls against a channel bounded at
    /// <paramref name="capacity"/>, only starting to drain after <paramref name="drainDelay"/>.
    /// Returns the tokens received (in order) and how long the producer took overall.
    /// </summary>
    private static async Task<(List<string> Received, TimeSpan Elapsed)> RunBackpressureCaseAsync(
        int capacity, int total, TimeSpan drainDelay)
    {
        var (ctx, reader) = StreamContext.CreateChannel("StreamLLMResponse", capacity);

        var stopwatch = Stopwatch.StartNew();
        var producer = Task.Run(async () =>
        {
            for (var i = 0; i < total; i++)
            {
                await ctx.EmitTokenAsync(i.ToString());
            }
        });

        await Task.Delay(drainDelay);

        var received = new List<string>(total);
        await foreach (var chunk in reader.ReadAllAsync())
        {
            received.Add(chunk.Payload.ToString());
            if (received.Count == total)
            {
                break;
            }
        }

        await producer;
        stopwatch.Stop();

        return (received, stopwatch.Elapsed);
    }

    [Fact]
    public async Task Backpressure_Blocks_Producer_Without_Dropping_Chunks()
    {
        const int total = 20;
        var delay = TimeSpan.FromMilliseconds(50);
        var (received, elapsed) = await RunBackpressureCaseAsync(4, total, delay);

        var expected = Enumerable.Range(0, total).Select(i => i.ToString()).ToList();
        Assert.Equal(expected, received);

        // Capacity 4 with 20 sends and a reader that doesn't start draining
        // until `delay` has passed: the producer necessarily blocks on the
        // 5th write until the reader starts reading, so completion cannot
        // happen before `delay` elapses.
        Assert.True(
            elapsed >= delay,
            $"producer finished in {elapsed}, before the consumer even started draining ({delay}) - backpressure did not pause it");
    }

    [Fact]
    public async Task Default_Capacity_Handles_Overflow_Without_Dropping_Chunks()
    {
        // Exercises the real production bound (1024), not just a small test
        // capacity, proving the "1024 chunks" bound end-to-end.
        var total = StreamContext.DefaultChannelCapacity + 50;
        var (received, _) = await RunBackpressureCaseAsync(
            StreamContext.DefaultChannelCapacity, total, TimeSpan.FromMilliseconds(20));

        var expected = Enumerable.Range(0, total).Select(i => i.ToString()).ToList();
        Assert.Equal(expected, received);
    }

    [Fact]
    public async Task Complete_Lets_A_Readers_ReadAllAsync_Loop_End_Instead_Of_Waiting_Forever()
    {
        var (ctx, reader) = StreamContext.CreateChannel("Node");
        await ctx.EmitTokenAsync("only token");
        ctx.Complete();

        var received = new List<string>();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            received.Add(chunk.Payload.ToString());
        }

        Assert.Equal(["only token"], received);
    }
}
