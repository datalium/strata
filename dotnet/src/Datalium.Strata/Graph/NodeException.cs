namespace Datalium.Strata.Graph;

/// <summary>Whether a failed attempt is worth retrying (<c>design/04-Error-Recovery.md</c>'s <c>NodeError::Transient</c> / <c>NodeError::Fatal</c>).</summary>
public enum NodeFailureKind
{
    /// <summary>A timeout, a rate limit — retried under the node's <see cref="RecoveryPolicy"/>.</summary>
    Transient,

    /// <summary>A rejected card, bad input — skips retries and goes straight to the fallback (or a <c>Failed</c> checkpoint).</summary>
    Fatal,
}

/// <summary>
/// How a node registered via <see cref="StateGraph{TState}.AddFallibleNode"/> reports a failure
/// the engine should handle per its <see cref="RecoveryPolicy"/>: throw one of these. Mirrors
/// Rust's <c>Err(NodeError::...)</c>. Any other exception type is not a recovery-managed failure —
/// it propagates out of <c>RunAsync</c> exactly as it does from a plain node.
/// </summary>
public sealed class NodeException(NodeFailureKind kind, string message, Exception? innerException = null)
    : Exception(message, innerException)
{
    public NodeFailureKind Kind { get; } = kind;

    /// <summary>
    /// Only from a subgraph node whose child failed at one of its own nodes: the child's <c>Failed</c>
    /// checkpoint, for the parent to nest in its own so a resume can re-enter the child there
    /// (<c>design/16-Nested-Interrupts.md</c>, "Resuming a failed thread").
    /// </summary>
    internal Interruption.Checkpoint? FailedChild { get; set; }

    /// <summary>
    /// How long the failing service asked to be left alone, if it said (a 429's <c>Retry-After</c>):
    /// a <see cref="RecoveryPolicy"/> with a <see cref="RecoveryPolicy.Backoff"/> waits this long
    /// before the retry, capped by the backoff's maximum, instead of its computed delay
    /// (<c>design/25-Retry-Backoff.md</c>). Only a transient failure has one.
    /// </summary>
    public TimeSpan? RetryAfter { get; internal init; }

    public static NodeException Transient(string message, Exception? innerException = null) =>
        new(NodeFailureKind.Transient, message, innerException);

    /// <summary>
    /// A transient failure whose cause said when to try again: <paramref name="retryAfter"/> becomes
    /// <see cref="RetryAfter"/>. <see langword="null"/> is a transient failure with no hint.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">A negative <paramref name="retryAfter"/>.</exception>
    public static NodeException Transient(string message, TimeSpan? retryAfter, Exception? innerException = null)
    {
        if (retryAfter is { } after)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(after, TimeSpan.Zero, nameof(retryAfter));
        }

        return new(NodeFailureKind.Transient, message, innerException) { RetryAfter = retryAfter };
    }

    public static NodeException Fatal(string message, Exception? innerException = null) =>
        new(NodeFailureKind.Fatal, message, innerException);
}
