using System.Collections;
using System.Text.Json.Serialization;

namespace Datalium.Strata.Graph;

/// <summary>
/// How a parallel group runs its branches and decides it is done
/// (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>). A group added without options behaves as
/// one with these defaults, which is how groups behaved before. Mirrors the Rust engine's
/// <c>ParallelGroupOptions</c>.
/// </summary>
public sealed class ParallelGroupOptions<TState>
{
    private readonly int? _maxConcurrency;
    private readonly TimeSpan? _branchTimeout;
    private readonly JoinPolicy<TState> _join = JoinPolicy<TState>.All;

    /// <summary>
    /// At most this many branches run at once; the rest start in declaration order as running ones
    /// settle. <see langword="null"/> (the default): all at once. A branch the group gives up on (timed
    /// out, or failed while the group goes on) settles at once and frees its slot, though its handler,
    /// abandoned rather than awaited, may still be unwinding (<c>design/26</c>, "Timeouts and the
    /// concurrency limit"). A map's items wind down, so <see cref="MapOptions{TItem}.MaxConcurrency"/> is exact.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Less than 1.</exception>
    public int? MaxConcurrency
    {
        get => _maxConcurrency;
        init
        {
            if (value is { } limit)
            {
                ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1, nameof(MaxConcurrency));
            }

            _maxConcurrency = value;
        }
    }

    /// <summary>
    /// Each branch's own timeout, from when it starts, on the run's clock
    /// (<see cref="RunOptions.TimeProvider"/>): a transient failure of that branch only, reported as
    /// <see cref="BranchStatus.TimedOut"/>. <see langword="null"/> (the default): none.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Zero or negative.</exception>
    public TimeSpan? BranchTimeout
    {
        get => _branchTimeout;
        init
        {
            if (value is { } timeout)
            {
                ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero, nameof(BranchTimeout));
            }

            _branchTimeout = value;
        }
    }

    /// <summary>
    /// Under <see cref="PartialResults.FailGroup"/>, when the join can no longer be met: cancel the
    /// branches still running and fail at once (the default), or let them finish and fail then.
    /// </summary>
    public bool CancelSiblingsOnFailure { get; init; } = true;

    /// <summary>What a join that can no longer be met does. See <see cref="PartialResults"/>.</summary>
    public PartialResults Partial { get; init; } = PartialResults.FailGroup;

    /// <summary>When the group is done. <see cref="JoinPolicy{TState}.All"/> by default.</summary>
    public JoinPolicy<TState> Join
    {
        get => _join;
        init => _join = value ?? throw new ArgumentNullException(nameof(Join));
    }

    /// <summary>Whether these are the defaults in what the definition exports: an <c>All</c> join that fails the group.</summary>
    internal bool HasDefaultJoin => Join.Kind == JoinKind.All && Partial == PartialResults.FailGroup;
}

/// <summary>What a parallel group does when its join can no longer be met.</summary>
public enum PartialResults
{
    /// <summary>The group fails. The default, and how groups always behaved.</summary>
    [JsonStringEnumMemberName("fail_group")]
    FailGroup,

    /// <summary>
    /// The group waits for every branch, then reduces over all of them with
    /// <see cref="BranchOutcomes{TState}.JoinMet"/> false. It fails only if no branch succeeded.
    /// </summary>
    [JsonStringEnumMemberName("keep_succeeded")]
    KeepSucceeded,
}

/// <summary>Which join a <see cref="JoinPolicy{TState}"/> is.</summary>
public enum JoinKind
{
    /// <summary>Every branch succeeded.</summary>
    [JsonStringEnumMemberName("all")]
    All,

    /// <summary>One branch succeeded.</summary>
    [JsonStringEnumMemberName("first_success")]
    FirstSuccess,

    /// <summary>A branch succeeded with a result that passes the predicate.</summary>
    [JsonStringEnumMemberName("first_matching")]
    FirstMatching,

    /// <summary><see cref="JoinPolicy{TState}.Count"/> branches succeeded.</summary>
    [JsonStringEnumMemberName("quorum")]
    Quorum,
}

/// <summary>
/// When a parallel group is done (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>). Branches
/// still running when it is met are cancelled: they lost the race.
/// </summary>
public sealed class JoinPolicy<TState>
{
    private JoinPolicy(JoinKind kind, int? count = null, Func<TState, bool>? predicate = null, string? description = null)
    {
        Kind = kind;
        Count = count;
        Predicate = predicate;
        Description = description;
    }

    /// <summary>Done when every branch succeeded. The default.</summary>
    public static JoinPolicy<TState> All { get; } = new(JoinKind.All);

    /// <summary>Done when one branch succeeded: the first to.</summary>
    public static JoinPolicy<TState> FirstSuccess { get; } = new(JoinKind.FirstSuccess);

    public JoinKind Kind { get; }

    /// <summary>A <see cref="JoinKind.Quorum"/>'s count.</summary>
    public int? Count { get; }

    /// <summary>A <see cref="JoinKind.FirstMatching"/> predicate's description, exported as the join's condition.</summary>
    public string? Description { get; }

    internal Func<TState, bool>? Predicate { get; }

    /// <summary>
    /// Done when a branch succeeded with a result that passes <paramref name="predicate"/>: the first
    /// to. The predicate runs once per succeeded branch, on the engine, so it must be cheap.
    /// <paramref name="description"/> is exported as the join's <c>condition</c>.
    /// </summary>
    public static JoinPolicy<TState> FirstMatching(Func<TState, bool> predicate, string? description = null)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return new(JoinKind.FirstMatching, predicate: predicate, description: description);
    }

    /// <summary>
    /// Done when <paramref name="count"/> branches succeeded: the first <paramref name="count"/> to.
    /// <c>Compile</c> refuses a count below 1 or above the group's branch count
    /// (<c>STRATA_ERR_INVALID_QUORUM</c>).
    /// </summary>
    public static JoinPolicy<TState> Quorum(int count) => new(JoinKind.Quorum, count);
}

/// <summary>How one branch of a parallel group ended, as an outcomes reducer sees it.</summary>
public enum BranchStatus
{
    Succeeded,

    /// <summary>It failed with a <see cref="NodeException"/>.</summary>
    Failed,

    /// <summary>It ran over <see cref="ParallelGroupOptions{TState}.BranchTimeout"/>.</summary>
    TimedOut,

    /// <summary>The group was done without it: it lost the race, a sibling failed, or it never started.</summary>
    Cancelled,
}

/// <summary>One branch's result, as an outcomes reducer sees it.</summary>
public sealed record BranchOutcome<TState>
{
    public required string Name { get; init; }

    public required BranchStatus Status { get; init; }

    /// <summary>Its result state: set exactly when it <see cref="BranchStatus.Succeeded"/>.</summary>
    public TState? State { get; init; }

    /// <summary>Why it failed: set when it <see cref="BranchStatus.Failed"/> or <see cref="BranchStatus.TimedOut"/>.</summary>
    public NodeException? Error { get; init; }
}

/// <summary>
/// Every branch's <see cref="BranchOutcome{TState}"/>, in declaration order, and how the join went:
/// what <see cref="StateGraph{TState}.AddParallelGroupWithOutcomes"/>'s reducer receives.
/// </summary>
public sealed class BranchOutcomes<TState> : IReadOnlyList<BranchOutcome<TState>>
{
    private readonly IReadOnlyList<BranchOutcome<TState>> _outcomes;

    internal BranchOutcomes(IReadOnlyList<BranchOutcome<TState>> outcomes, bool joinMet, IReadOnlyList<string> deciding)
    {
        _outcomes = outcomes;
        JoinMet = joinMet;
        Deciding = deciding;
        Succeeded = [.. outcomes.Where(o => o.Status == BranchStatus.Succeeded).Select(o => o.State!)];
    }

    /// <summary>
    /// Whether the join was met. False only under <see cref="PartialResults.KeepSucceeded"/>, when the
    /// group reduces over what it has after the join could no longer be met.
    /// </summary>
    public bool JoinMet { get; }

    /// <summary>
    /// The branches that met the join, in the order they did: a race's winner, a quorum's first
    /// <see cref="JoinPolicy{TState}.Count"/>, every branch for <see cref="JoinKind.All"/>. Empty when
    /// the join wasn't met.
    /// </summary>
    public IReadOnlyList<string> Deciding { get; }

    /// <summary>The succeeded branches' states, in declaration order.</summary>
    public IReadOnlyList<TState> Succeeded { get; }

    public int Count => _outcomes.Count;

    public BranchOutcome<TState> this[int index] => _outcomes[index];

    /// <summary>The outcome of the branch named <paramref name="branch"/>, if the group has one.</summary>
    public BranchOutcome<TState>? this[string branch] => _outcomes.FirstOrDefault(o => o.Name == branch);

    /// <summary>
    /// What a plain reducer gets: the deciding branches' states when the join was met, else every
    /// succeeded branch's, in declaration order.
    /// </summary>
    internal IReadOnlyList<TState> Counted() =>
        JoinMet ? [.. _outcomes.Where(o => Deciding.Contains(o.Name)).Select(o => o.State!)] : Succeeded;

    public IEnumerator<BranchOutcome<TState>> GetEnumerator() => _outcomes.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
