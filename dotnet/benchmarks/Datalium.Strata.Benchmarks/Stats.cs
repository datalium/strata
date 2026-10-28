using System.Diagnostics;

namespace Datalium.Strata.Benchmarks;

/// <summary>
/// The percentiles of a set of samples, in the samples' unit, by nearest rank: the same rule the Rust
/// profile uses (<c>rust/strata-bench/src/stats.rs</c>), so the two engines' P99.9 mean the same thing.
/// </summary>
public sealed record Distribution(int Samples, double Mean, long P50, long P90, long P99, long P999, long Max)
{
    /// <exception cref="ArgumentException">No samples: a scenario that measured nothing is a bug.</exception>
    public static Distribution Of(IEnumerable<long> samples)
    {
        var sorted = samples.ToArray();
        if (sorted.Length == 0)
        {
            throw new ArgumentException("no samples", nameof(samples));
        }

        Array.Sort(sorted);
        var sum = 0.0;
        foreach (var sample in sorted)
        {
            sum += sample;
        }

        return new Distribution(
            sorted.Length,
            sum / sorted.Length,
            Stats.Percentile(sorted, 0.50),
            Stats.Percentile(sorted, 0.90),
            Stats.Percentile(sorted, 0.99),
            Stats.Percentile(sorted, 0.999),
            sorted[^1]);
    }
}

/// <summary>The arithmetic of the profile.</summary>
public static class Stats
{
    /// <summary>
    /// The nearest-rank percentile of <paramref name="sorted"/>: the smallest sample that at least
    /// <paramref name="fraction"/> of the samples don't exceed.
    /// </summary>
    public static long Percentile(long[] sorted, double fraction)
    {
        var rank = (int)Math.Ceiling(fraction * sorted.Length);
        return sorted[Math.Clamp(rank, 1, sorted.Length) - 1];
    }

    /// <summary>A span of <see cref="Stopwatch"/> ticks in nanoseconds.</summary>
    public static long Nanoseconds(long stopwatchTicks) =>
        (long)(stopwatchTicks * (1_000_000_000.0 / Stopwatch.Frequency));

    /// <summary>The nanoseconds since <paramref name="startedAt"/>, a <see cref="Stopwatch.GetTimestamp"/>.</summary>
    public static long NanosecondsSince(long startedAt) => Nanoseconds(Stopwatch.GetTimestamp() - startedAt);
}
