using System.Diagnostics;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Graph;

/// <summary>
/// How a map runs its items and decides it is done (<c>design/27-Map-Fan-Out.md</c>). A map's items
/// are a parallel group's branches, so every option means what it means on
/// <see cref="ParallelGroupOptions{TState}"/> (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>),
/// under item names. Unlike a group's, the concurrency limit is always bounded. Mirrors the Rust
/// engine's <c>MapOptions</c>.
/// </summary>
public sealed class MapOptions<TChild>
{
    /// <summary>The <see cref="MaxConcurrency"/> a map has unless it says otherwise.</summary>
    public const int DefaultMaxConcurrency = 8;

    private readonly int _maxConcurrency = DefaultMaxConcurrency;
    private readonly TimeSpan? _itemTimeout;
    private readonly int? _maxItems;
    private readonly JoinPolicy<TChild> _join = JoinPolicy<TChild>.All;

    /// <summary>
    /// At most this many items run at once; the rest start in list order as running ones settle.
    /// <see cref="DefaultMaxConcurrency"/> by default.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Less than 1.</exception>
    public int MaxConcurrency
    {
        get => _maxConcurrency;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1, nameof(MaxConcurrency));
            _maxConcurrency = value;
        }
    }

    /// <summary>
    /// Each item's own timeout, from when it starts, on the run's clock
    /// (<see cref="RunOptions.TimeProvider"/>): a transient failure of that item only, reported as
    /// <see cref="BranchStatus.TimedOut"/>, its child wound down. <see langword="null"/> (the default): none.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Zero or negative.</exception>
    public TimeSpan? ItemTimeout
    {
        get => _itemTimeout;
        init
        {
            if (value is { } timeout)
            {
                ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero, nameof(ItemTimeout));
            }

            _itemTimeout = value;
        }
    }

    /// <summary>
    /// Under <see cref="PartialResults.FailGroup"/>, when the join can no longer be met: cancel the
    /// items still running and fail at once (the default), or let them finish and fail then.
    /// </summary>
    public bool CancelSiblingsOnFailure { get; init; } = true;

    /// <summary>What a join that can no longer be met does. See <see cref="PartialResults"/>.</summary>
    public PartialResults Partial { get; init; } = PartialResults.FailGroup;

    /// <summary>When the map is done. <see cref="JoinPolicy{TState}.All"/> by default.</summary>
    public JoinPolicy<TChild> Join
    {
        get => _join;
        init => _join = value ?? throw new ArgumentNullException(nameof(Join));
    }

    /// <summary>
    /// More items than this fail the map fatally, before any starts. <see langword="null"/> (the
    /// default): no limit.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Less than 1.</exception>
    public int? MaxItems
    {
        get => _maxItems;
        init
        {
            if (value is { } limit)
            {
                ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1, nameof(MaxItems));
            }

            _maxItems = value;
        }
    }

    /// <summary>Whether these are the defaults in what the definition exports: an <c>All</c> join that fails the map.</summary>
    internal bool HasDefaultJoin => Join.Kind == JoinKind.All && Partial == PartialResults.FailGroup;

    internal GraphJoin? ExportedJoin =>
        HasDefaultJoin ? null : new GraphJoin(Join.Kind, Join.Count, Join.Description, Partial);
}

/// <summary>
/// What a map keeps for <see cref="StateGraph{TState}.Compile"/>'s checks and the exported topology: the
/// child's topology (the template), the join when it isn't the default, and the first interrupt
/// boundary the child could freeze at, which <see cref="StateGraph{TState}.Compile"/> refuses.
/// </summary>
internal sealed record MapShape(GraphTopology Template, GraphJoin? Join, Suspension? Boundary);

/// <summary>
/// Runs a map (<c>design/27-Map-Fan-Out.md</c>): reads its items, checks their keys, and runs a compiled
/// child graph per item through <see cref="FanOutRunner"/>, each at a level of its own, as a subgraph's
/// child runs. A map is registered as a fallible node that winds down (<see cref="NodeEntry{TState}.WindsDown"/>),
/// so retry, fallback and <c>Failed</c> checkpoints work for it, and on cancellation the executor waits
/// for its items to stop.
/// </summary>
internal static class MapRunner
{
    /// <summary>
    /// One attempt of the map <paramref name="name"/>: <paramref name="items"/> from
    /// <paramref name="state"/>, each named by <paramref name="key"/>; then <c>MapExpanded</c>, and the
    /// items, each running <paramref name="child"/> from the state <paramref name="input"/> builds when it
    /// starts; then <paramref name="reducer"/> over their outcomes.
    ///
    /// More items than <see cref="MapOptions{TChild}.MaxItems"/>, an empty key, a key containing
    /// <c>/</c> or two items with one key fail the map fatally before anything starts: the keys come
    /// from the state, so a retry would see them again.
    /// </summary>
    internal static async ValueTask<NodeStep<TState>> RunAsync<TState, TItem, TChild>(
        string name,
        Func<TState, IEnumerable<TItem>> items,
        Func<TItem, string> key,
        CompiledGraph<TChild> child,
        Func<TState, TItem, TChild> input,
        Func<TState, BranchOutcomes<TChild>, TState> reducer,
        MapOptions<TChild> options,
        TState state,
        AttemptInputs inputs)
    {
        List<TItem> list = [.. items(state)];
        if (options.MaxItems is { } max && list.Count > max)
        {
            throw NodeException.Fatal($"map '{name}' has {list.Count} items, more than its limit of {max}");
        }

        var keys = new List<string>(list.Count);
        var seen = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < list.Count; i++)
        {
            var itemKey = key(list[i]);
            if (string.IsNullOrEmpty(itemKey))
            {
                throw NodeException.Fatal($"map '{name}': item {i} has an empty key");
            }

            if (itemKey.Contains('/', StringComparison.Ordinal))
            {
                throw NodeException.Fatal($"map '{name}': item {i} has the key '{itemKey}', which contains '/'");
            }

            if (!seen.TryAdd(itemKey, i))
            {
                throw NodeException.Fatal($"map '{name}': items {seen[itemKey]} and {i} have the same key '{itemKey}'");
            }

            keys.Add(itemKey);
        }

        inputs.Level.Run.Telemetry.MapExpanded(inputs.Context.NodePath, inputs.Context.Attempt, keys, Activity.Current);

        BranchLauncher<TChild> launch = (index, started) =>
            RunItemAsync(name, keys[index], child, input(state, list[index]), inputs, started);
        var outcomes = await FanOutRunner.RunAsync(
            keys,
            launch,
            new FanOutSettings<TChild>(
                options.MaxConcurrency,
                options.ItemTimeout,
                options.CancelSiblingsOnFailure,
                options.Partial,
                options.Join,
                FanOutNoun.Item,
                WindsDown: true),
            inputs).ConfigureAwait(false);
        return new NodeStep<TState>.Done(reducer(state, outcomes));
    }

    /// <summary>
    /// Runs one item: <paramref name="child"/> from <paramref name="childState"/>, at a level of its own
    /// rooted at the item's token and named under its path (<c>"&lt;map&gt;/&lt;key&gt;"</c>), streaming
    /// (when the run is) under that path too. A child stopped by the item's token throws
    /// <see cref="OperationCanceledException"/>; a failed one throws its <see cref="NodeException"/>, as a
    /// subgraph node's failure is derived. Nothing it writes is kept.
    /// </summary>
    private static async Task<TChild> RunItemAsync<TChild>(
        string map, string key, CompiledGraph<TChild> child, TChild childState, AttemptInputs inputs, BranchLaunch started)
    {
        var store = new SubgraphRunner.CapturingStore();
        var stream = inputs.Stream?.ForNode($"{map}/{key}").Scoped();
        var level = inputs.Level.Child(started.Token, started.Path);
        var result = await child.RunInlineAsync(key, childState, store, stream, level).ConfigureAwait(false);
        return result switch
        {
            GraphExecutionResult<TChild>.Completed completed => completed.State,
            GraphExecutionResult<TChild>.Cancelled => throw new OperationCanceledException(started.Token),

            // Compile refuses a child with an interrupt boundary, so this is only a guard.
            GraphExecutionResult<TChild>.Interrupted interrupted =>
                throw NodeException.Fatal($"froze at '{interrupted.SuspendedNode}', which a map item can't do"),
            _ => throw SubgraphRunner.ChildFailure(subgraph: null, result, store.Last),
        };
    }
}
