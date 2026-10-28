using System.Globalization;

namespace Datalium.Strata.Graph;

/// <summary>
/// How the engine reacts when a fallible node fails (<c>design/04-Error-Recovery.md</c>).
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>A <see cref="NodeFailureKind.Transient"/> failure is retried up to
/// <paramref name="MaxRetries"/> more times, each attempt starting from the same committed state,
/// after the <see cref="Backoff"/> delay if there is one (<c>design/25-Retry-Backoff.md</c>).</item>
/// <item>Once retries are exhausted — or immediately, for a <see cref="NodeFailureKind.Fatal"/>
/// failure — execution routes to <paramref name="Fallback"/> if set. The fallback node runs
/// against the state as it was <em>before</em> the failed node started, and its
/// <see cref="NodeContext.LastFailure"/> says why the node failed.</item>
/// <item>With no fallback, the engine persists a <c>CheckpointStatus.Failed</c> checkpoint (last
/// committed state + the error) and returns <c>GraphExecutionResult.NodeFailed</c>.</item>
/// </list>
/// The default policy is no retries, no backoff and no fallback. A node timeout
/// (<see cref="StateGraph{TState}.SetTimeout"/>) counts as a transient failure. A cancelled run is
/// not a failure and never reaches the policy. Rollback relies on <c>TState</c> being treated
/// immutably (records and <c>with</c>), the same convention plain nodes already follow.
/// </remarks>
public sealed record RecoveryPolicy(int MaxRetries = 0, string? Fallback = null)
{
    /// <summary>The policy of a fallible node with none set: no retries, no fallback.</summary>
    public static RecoveryPolicy Default { get; } = new();

    /// <summary>
    /// How long to wait before each retry (<c>design/25-Retry-Backoff.md</c>). <see langword="null"/>
    /// (the default): retries start at once, and a failure's <see cref="NodeException.RetryAfter"/> is
    /// ignored.
    /// </summary>
    public RetryBackoff? Backoff { get; init; }

    /// <summary>Retry a transient failure up to <paramref name="maxRetries"/> more times.</summary>
    public static RecoveryPolicy Retry(int maxRetries)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxRetries);
        return new RecoveryPolicy(maxRetries);
    }

    /// <summary>Route to <paramref name="node"/> once the node can't be recovered by retrying.</summary>
    public RecoveryPolicy WithFallback(string node)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(node);
        return this with { Fallback = node };
    }

    /// <summary>
    /// Wait before each retry: the n-th waits <paramref name="initial"/> × 2^(n−1), capped at
    /// <paramref name="max"/> and jittered, or the failure's <see cref="NodeException.RetryAfter"/>
    /// capped at <paramref name="max"/> when it has one. See <see cref="RetryBackoff"/>.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">A negative <paramref name="initial"/>, or a <paramref name="max"/> below it.</exception>
    public RecoveryPolicy WithBackoff(TimeSpan initial, TimeSpan max, BackoffJitter jitter = BackoffJitter.Full) =>
        this with { Backoff = new RetryBackoff(initial, max, jitter) };
}

/// <summary>How a <see cref="RetryBackoff"/> delay is jittered.</summary>
public enum BackoffJitter
{
    /// <summary>The delay is drawn uniformly from [0, d]. The default: it spreads synchronized retries best.</summary>
    Full,

    /// <summary>The delay is drawn uniformly from [d/2, d].</summary>
    Equal,

    /// <summary>The delay is d.</summary>
    None,
}

/// <summary>
/// Exponential backoff between retries (<c>design/25-Retry-Backoff.md</c>): the n-th retry waits
/// d = <see cref="Initial"/> × 2^(n−1), capped at <see cref="Max"/>, jittered by
/// <see cref="Jitter"/>. A failure that carries a <see cref="NodeException.RetryAfter"/> hint waits
/// the hint instead, capped at <see cref="Max"/> and not jittered. Constant backoff is
/// <see cref="Initial"/> = <see cref="Max"/>.
/// </summary>
public sealed record RetryBackoff
{
    // Longest delay a timer takes.
    private static readonly TimeSpan LongestWait = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <exception cref="ArgumentOutOfRangeException">A negative <paramref name="initial"/>, or a <paramref name="max"/> below it.</exception>
    public RetryBackoff(TimeSpan initial, TimeSpan max, BackoffJitter jitter = BackoffJitter.Full)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(initial, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(max, initial);
        if (!Enum.IsDefined(jitter))
        {
            throw new ArgumentOutOfRangeException(nameof(jitter), jitter, "unknown jitter");
        }

        Initial = initial;
        Max = max;
        Jitter = jitter;
    }

    /// <summary>The delay before the first retry, before jitter.</summary>
    public TimeSpan Initial { get; }

    /// <summary>The longest delay, computed or hinted.</summary>
    public TimeSpan Max { get; }

    public BackoffJitter Jitter { get; }

    /// <summary>
    /// The wait before the <paramref name="retry"/>-th retry (1-based) after a failure that carried
    /// <paramref name="hint"/>, drawing jitter from <paramref name="random"/>.
    /// </summary>
    internal TimeSpan DelayBefore(int retry, TimeSpan? hint, Random random)
    {
        var delay = hint is { } after
            ? (after < Max ? after : Max)
            : Jitter switch
            {
                BackoffJitter.Full => Ceiling(retry) * random.NextDouble(),
                BackoffJitter.Equal => (Ceiling(retry) / 2) + ((Ceiling(retry) / 2) * random.NextDouble()),
                _ => Ceiling(retry),
            };
        return delay < LongestWait ? delay : LongestWait;
    }

    /// <summary><see cref="Initial"/> × 2^(<paramref name="retry"/>−1), saturating at <see cref="Max"/>.</summary>
    private TimeSpan Ceiling(int retry)
    {
        var ticks = Initial.Ticks;
        for (var i = 1; i < retry && ticks > 0 && ticks < Max.Ticks; i++)
        {
            ticks = ticks > Max.Ticks / 2 ? Max.Ticks : ticks * 2;
        }

        return TimeSpan.FromTicks(Math.Min(ticks, Max.Ticks));
    }
}

/// <summary>
/// The checkpoint <c>metadata</c> key a <c>failed</c> checkpoint records a hinted final failure under
/// (<c>design/25-Retry-Backoff.md</c>): the hint in whole milliseconds, as a decimal string, so a
/// subgraph node can carry its child's hint and an operator can see it. Mirrors the Rust engine's
/// <c>RETRY_AFTER_KEY</c>.
/// </summary>
internal static class RetryAfterHint
{
    public const string MetadataKey = "strata.retry_after_ms";

    public static string Format(TimeSpan hint) =>
        ((long)hint.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);

    /// <summary>The hint <paramref name="metadata"/> records, if it records a readable one.</summary>
    public static TimeSpan? Read(IReadOnlyDictionary<string, string>? metadata) =>
        metadata is not null
        && metadata.TryGetValue(MetadataKey, out var stored)
        && long.TryParse(stored, NumberStyles.None, CultureInfo.InvariantCulture, out var milliseconds)
            ? TimeSpan.FromMilliseconds(milliseconds)
            : null;
}
