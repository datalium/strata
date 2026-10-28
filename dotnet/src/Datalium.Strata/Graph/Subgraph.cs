using Datalium.Strata.Interruption;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.Graph;

/// <summary>
/// What one attempt of a fallible node produced: a new state, or — only from a subgraph node — the
/// checkpoint of a child that froze at an interrupt boundary
/// (<c>design/16-Nested-Interrupts.md</c>). A freeze is not a failure, so it is not an exception and
/// no recovery policy sees it. Mirrors the Rust engine's <c>Step</c>.
/// </summary>
internal abstract record NodeStep<TState>
{
    private NodeStep()
    {
    }

    internal sealed record Done(TState State) : NodeStep<TState>;

    internal sealed record Interrupted(Checkpoint Child) : NodeStep<TState>;

    /// <summary>
    /// Only from a subgraph node: its child stopped because its token was cancelled (the run was
    /// cancelled, or this node timed out), and left this <c>cancelled</c> checkpoint
    /// (<c>design/17-Async-Execution.md</c>). Which of the two it was is the executor's call.
    /// </summary>
    internal sealed record Cancelled(Checkpoint Child) : NodeStep<TState>;
}

/// <summary>
/// What a subgraph node is given when the run is resuming into it: the child's own checkpoint
/// (frozen or failed), and the signals for the child's level and the levels below it.
/// </summary>
internal sealed record NestedResume(Checkpoint Checkpoint, LevelSignals Signals);

/// <summary>
/// Runs a compiled graph as one node of another (<c>design/15-Subgraph-Composition.md</c>). The
/// subgraph is registered as a fallible node whose handler is <see cref="RunAsync{TParent, TChild}"/>,
/// which is why retry, fallback and <c>Failed</c> checkpoints work for it. A child that freezes at an
/// interrupt boundary is the one thing that is not a failure or a result, so the executor knows
/// subgraph nodes by name and freezes around them (<c>CompiledGraph.FreezeAroundChildAsync</c>).
/// </summary>
internal static class SubgraphRunner
{
    /// <summary>
    /// The store a child run persists to. Nothing the child writes is kept: the child is not a
    /// thread of its own, and the parent persists <i>its</i> failure itself. The last checkpoint is
    /// remembered so the failure kind (<c>transient</c> / <c>fatal</c>) the child recorded can
    /// reach the parent — <see cref="GraphExecutionResult{TState}.NodeFailed"/> doesn't carry it —
    /// and so a child that froze or was cancelled can hand its checkpoint up to be nested in the
    /// parent's.
    /// </summary>
    internal sealed class CapturingStore : ICheckpointStore
    {
        public Checkpoint? Last { get; private set; }

        public void Save(Checkpoint checkpoint) => Last = checkpoint;

        public Checkpoint? Load(string threadId) => null;
    }

    /// <summary>
    /// Runs <paramref name="child"/> against the state <paramref name="input"/> derives from
    /// <paramref name="state"/> — or, when the run is resuming into this node, resumes it from the
    /// checkpoint in <paramref name="inputs"/> without calling <paramref name="input"/> again — and
    /// folds its result back with <paramref name="output"/>.
    ///
    /// If the child freezes at an interrupt boundary the node returns
    /// <see cref="NodeStep{TState}.Interrupted"/> with the child's checkpoint, for the parent to nest
    /// in its own. A retry after a failed resumed attempt resumes from the same checkpoint again, so
    /// the human is not asked twice.
    ///
    /// A child failure becomes a <see cref="NodeException"/> on the subgraph node, carrying the child's
    /// own <c>Failed</c> checkpoint when it left one: fatal if the failing child node's error was fatal, or the failure isn't a node failure at all (a routing
    /// gap, a missing node — retrying can't fix those); transient if the child's own recovery ran
    /// out on a transient error. So the parent's <see cref="RecoveryPolicy"/> can retry a child that
    /// failed for a transient reason, and skips retrying one that didn't. The child's own policies
    /// (retries, fallbacks) have already run by then. A transient failure carries the retry hint the
    /// child's checkpoint recorded (<c>strata.retry_after_ms</c>, <c>design/25-Retry-Backoff.md</c>).
    ///
    /// The node's stream context, when the run is streaming, reaches the child scoped
    /// (<see cref="StreamContext.Scoped"/>), so everything the child streams — lifecycle chunks,
    /// tokens, retries — arrives named <c>"&lt;name&gt;/&lt;child node&gt;"</c>.
    ///
    /// The child runs at a level of its own whose root token is this attempt's
    /// (<c>design/17-Async-Execution.md</c>), so a cancelled run or this node's timeout reaches it at
    /// any depth. It then stops where it is and hands back a <c>cancelled</c> checkpoint as
    /// <see cref="NodeStep{TState}.Cancelled"/>, for the executor to nest.
    /// </summary>
    internal static async ValueTask<NodeStep<TParent>> RunAsync<TParent, TChild>(
        string name,
        CompiledGraph<TChild> child,
        Func<TParent, TChild> input,
        Func<TParent, TChild, TParent> output,
        TParent state,
        AttemptInputs inputs)
    {
        var store = new CapturingStore();
        var childStream = inputs.Stream?.Scoped();
        var level = inputs.Level.Child(inputs.Token, inputs.Context.NodePath);
        var result = inputs.Resume is { } resume
            ? await child.ResumeInlineAsync(name, resume.Checkpoint, resume.Signals, store, childStream, level)
                .ConfigureAwait(false)
            : await child.RunInlineAsync(name, input(state), store, childStream, level).ConfigureAwait(false);

        switch (result)
        {
            case GraphExecutionResult<TChild>.Completed completed:
                return new NodeStep<TParent>.Done(output(state, completed.State));

            case GraphExecutionResult<TChild>.Interrupted interrupted:
                return store.Last is { Status: CheckpointStatus.Interrupted } frozen
                    ? new NodeStep<TParent>.Interrupted(frozen)
                    : throw NodeException.Fatal(
                        $"subgraph '{name}' froze at '{interrupted.SuspendedNode}' but its checkpoint was not captured");

            case GraphExecutionResult<TChild>.Cancelled cancelled:
                return store.Last is { Status: CheckpointStatus.Cancelled } stopped
                    ? new NodeStep<TParent>.Cancelled(stopped)
                    : throw NodeException.Fatal(
                        $"subgraph '{name}' was cancelled at '{cancelled.Node}' but its checkpoint was not captured");

            default:
                throw ChildFailure(name, result, store.Last);
        }
    }

    /// <summary>
    /// The <see cref="NodeException"/> a child run that ended in <paramref name="result"/>, a failure,
    /// becomes on the node that ran it: a subgraph node (<paramref name="subgraph"/> is its name, which
    /// the message starts with) or a map item (<see langword="null"/>; the map names the item). Fatal
    /// if the failing child node's error was fatal, or the failure isn't a node failure at all;
    /// transient, with the retry hint the child's <c>failed</c> checkpoint recorded, if the child's own
    /// recovery ran out on a transient error. It carries that checkpoint (<paramref name="last"/>, when
    /// it is the child's <c>failed</c> one) for a subgraph node to nest.
    /// </summary>
    internal static NodeException ChildFailure<TChild>(
        string? subgraph, GraphExecutionResult<TChild> result, Checkpoint? last)
    {
        string Say(string what) => subgraph is null ? what : $"subgraph '{subgraph}' {what}";
        var failedChild = last is { Status: CheckpointStatus.Failed } ? last : null;

        switch (result)
        {
            case GraphExecutionResult<TChild>.NodeFailed failed:
                var message = Say($"failed at '{failed.Node}' after {failed.Attempts} attempt(s): {failed.Message}");

                // The child persisted a `Failed` checkpoint of its own; keep it, so a resume can
                // re-enter the child at the node that failed.
                var exception = failedChild?.Metadata?.GetValueOrDefault("error_kind") == "transient"
                    ? NodeException.Transient(message, RetryAfterHint.Read(failedChild.Metadata))
                    : NodeException.Fatal(message);
                exception.FailedChild = failedChild;
                return exception;

            case GraphExecutionResult<TChild>.HopLimitExceeded tripped:
                // A fail-safe, not a transient condition: no retry. Keep the child's hop-limit
                // checkpoint, as for any failed child, so a resume re-enters it there.
                return new NodeException(NodeFailureKind.Fatal, Say($"hit its hop limit: {tripped.Message}"))
                {
                    FailedChild = failedChild,
                };

            case GraphExecutionResult<TChild>.Failed failed:
                return NodeException.Fatal(subgraph is null ? failed.Message : $"subgraph '{subgraph}': {failed.Message}");

            default:
                throw new InvalidOperationException("unreachable: not a failed GraphExecutionResult");
        }
    }
}
