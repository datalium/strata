using System.Globalization;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Graph;

/// <summary>
/// Options for <see cref="CompiledGraph{TState}.RecoverStaleAsync"/> (<c>design/33-Durable-Execution.md</c>,
/// "Who is dead"): which <c>running</c> threads count as dead, how many to recover, and how each recovery runs.
/// </summary>
public sealed record RecoverOptions
{
    private static readonly IReadOnlyDictionary<string, string> NoMetadata = new Dictionary<string, string>();

    /// <summary>
    /// How old a thread's latest <c>running</c> checkpoint must be before the thread is recovered. A thread a
    /// live lease holds is never recovered, whatever its age.
    ///
    /// <see langword="null"/> (the default) is zero when <see cref="Run"/> has a lease: every worker then runs
    /// under one, and a <c>running</c> thread nobody holds has no run. Without a lease it must be set, to more
    /// than the longest a node can take: the age of the checkpoint is then the only sign of life, a long node
    /// ages it too, and a node slower than this is run twice.
    /// </summary>
    public TimeSpan? StaleAfter { get; init; }

    /// <summary>
    /// The most times a thread is recovered without a node committing in between (<c>strata.recoveries</c> on
    /// its latest checkpoint): a thread already recovered this often is skipped, and stays <c>running</c> for
    /// an operator to look at and end. Stops a node that kills its process from being run forever.
    /// <see langword="null"/> (the default) is no limit. At least 1.
    /// </summary>
    public int? MaxRecoveries { get; init; }

    /// <summary>Only threads whose latest checkpoint's metadata has every pair: a store holds many graphs' threads.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = NoMetadata;

    /// <summary>
    /// Only this graph's threads, when it has an identity (<c>design/31-Graph-Identity.md</c>): its id, and
    /// its version unless it has a migration. <see langword="false"/> lists every graph's, and each recovery
    /// still refuses a thread that isn't this graph's.
    /// </summary>
    public bool MatchGraph { get; init; } = true;

    /// <summary>
    /// The most threads recovered in one call: 1 to <see cref="ThreadQuery.MaxLimit"/>. Threads that are
    /// skipped don't count, so live threads can't crowd out dead ones.
    /// </summary>
    public int Limit { get; init; } = ThreadQuery.DefaultLimit;

    /// <summary>
    /// The options of each recovery, which is a run of its own: its lease (every worker's, or none's), its
    /// metadata, observer and services, and the clock <c>now</c> is read from.
    /// </summary>
    public RunOptions Run { get; init; } = new();

    /// <summary>
    /// When it fires, the call recovers no more threads and returns what it did; the recovery in flight
    /// finishes. Unlike the call's own token, which also cancels that run. How a host stops a driver without
    /// leaving a thread cancelled (<c>design/35-Hosting-Integration.md</c>, "Stopping").
    /// </summary>
    public CancellationToken Stop { get; init; }
}

/// <summary>A thread <see cref="CompiledGraph{TState}.RecoverStaleAsync"/> recovered, and how its run ended.</summary>
/// <param name="At">Where it continued, one node per level from the outermost graph in.</param>
/// <param name="Recoveries">How many times it has now been recovered without a node committing in between.</param>
public sealed record RecoveredThread<TState>(
    string ThreadId, IReadOnlyList<string> At, int Recoveries, GraphExecutionResult<TState> Result);

/// <summary>
/// What one <see cref="CompiledGraph{TState}.RecoverStaleAsync"/> call did: the threads it recovered, each
/// with its run's result, and the <c>running</c> threads it left alone, each with the reason (a live lease
/// holds it, it isn't stale yet, another recovery got there first, …).
/// </summary>
public sealed record RecoveryReport<TState>(
    IReadOnlyList<RecoveredThread<TState>> Recovered, IReadOnlyList<SkippedThread> Skipped);

/// <summary>
/// The count a recovery's claim records (<c>design/33-Durable-Execution.md</c>): how many times the thread
/// has been recovered from the checkpoint it stands at. Only a claim carries it, so it is a count of
/// recoveries since a node last committed.
///
/// <see cref="RecoverOptions.MaxRecoveries"/> limits it for <see cref="CompiledGraph{TState}.RecoverStaleAsync"/>;
/// <c>RecoverAsync</c> and <c>RecoverStreaming</c> always recover, so an application that recovers a thread itself reads <see cref="Of"/> on the thread's
/// latest checkpoint and stops at its own limit.
/// </summary>
public static class RecoveryCount
{
    /// <summary>The checkpoint <c>metadata</c> key, on the outermost level. Mirrors the Rust engine's <c>RECOVERIES_KEY</c>.</summary>
    public const string MetadataKey = "strata.recoveries";

    /// <summary>The count <paramref name="checkpoint"/> records, or 0 when it records none.</summary>
    public static int Of(Checkpoint checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        return checkpoint.Metadata is { } metadata
            && metadata.TryGetValue(MetadataKey, out var stored)
            && int.TryParse(stored, NumberStyles.None, CultureInfo.InvariantCulture, out var count)
            ? count
            : 0;
    }

    /// <summary><paramref name="claim"/> recording <paramref name="recoveries"/>.</summary>
    internal static Checkpoint Counted(Checkpoint claim, int recoveries) => claim with
    {
        Metadata = new Dictionary<string, string>(claim.Metadata ?? [])
        {
            [MetadataKey] = recoveries.ToString(CultureInfo.InvariantCulture),
        },
    };

    /// <summary>Where a run of <paramref name="checkpoint"/> continues: each level's next node, from the outermost in.</summary>
    internal static IReadOnlyList<string> ContinuesAt(Checkpoint checkpoint)
    {
        var path = new List<string>();
        for (var level = checkpoint; level is not null; level = level.Nested)
        {
            if (level.NextNode is { } node)
            {
                path.Add(node);
            }
        }

        return path;
    }
}
