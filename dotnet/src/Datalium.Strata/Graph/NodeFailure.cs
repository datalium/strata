namespace Datalium.Strata.Graph;

/// <summary>
/// Why a node failed, as the fallback node its <see cref="RecoveryPolicy"/> routed to sees it
/// (<see cref="NodeContext.LastFailure"/>, <c>design/25-Retry-Backoff.md</c>): the final failure, the
/// one that made the policy give up. Mirrors the Rust engine's <c>NodeFailure</c>.
/// </summary>
public sealed record NodeFailure
{
    /// <summary>The node that failed, by its own name: <c>"rca"</c>.</summary>
    public required string Node { get; init; }

    /// <summary>Its qualified path, the one run events use: <c>"investigate/rca"</c> inside the subgraph node <c>investigate</c>.</summary>
    public required string NodePath { get; init; }

    /// <summary>The final failure's kind: fatal, or transient with no retries left.</summary>
    public required NodeFailureKind Kind { get; init; }

    /// <summary>The final failure's message. For a group or a subgraph, the combined message the node reported.</summary>
    public required string Message { get; init; }

    /// <summary>The final failure's <see cref="NodeException.RetryAfter"/> hint, if it had one.</summary>
    public TimeSpan? RetryAfter { get; init; }

    /// <summary>Attempts made, retries included.</summary>
    public required int Attempts { get; init; }

    /// <summary>The final attempt ran over the node's timeout (<see cref="StateGraph{TState}.SetTimeout"/>).</summary>
    public bool TimedOut { get; init; }
}
