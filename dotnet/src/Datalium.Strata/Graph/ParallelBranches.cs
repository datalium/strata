using Datalium.Strata.Streaming;

namespace Datalium.Strata.Graph;

/// <summary>
/// A branch as a group stores it: exactly one of <paramref name="Sync"/> (which takes the run's
/// <see cref="StreamContext"/>, <see langword="null"/> under a plain <c>RunAsync</c>; a plain branch
/// ignores it) and <paramref name="Async"/>.
/// </summary>
internal sealed record BranchEntry<TState>(
    string Name,
    bool Streaming,
    Func<TState, StreamContext?, TState>? Sync,
    Func<TState, NodeContext, CancellationToken, ValueTask<TState>>? Async);

/// <summary>
/// What a group keeps of its branches for <see cref="StateGraph{TState}.Compile"/>'s checks and the
/// exported topology: their names in declaration order, and the metadata set on them (possibly for a
/// name that isn't a branch, which <see cref="StateGraph{TState}.Compile"/> rejects).
/// </summary>
internal sealed record GroupShape(
    IReadOnlyList<string> Branches,
    IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> Metadata)
{
    /// <summary>The group's join, when it isn't the default (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>).</summary>
    public GraphJoin? Join { get; init; }
}

/// <summary>
/// The named branches of a parallel group, in declaration order. Built with
/// <c>Branch</c> and handed to
/// <see cref="StateGraph{TState}.AddParallelGroup"/> (<c>design/14-Parallel-Execution.md</c>).
/// </summary>
/// <remarks>
/// Each branch receives the state as it was when the group started and returns its own result
/// state; it can't see its siblings. Branches share that one input instance, so — as with any
/// handler — they must build a new state (e.g. a <c>with</c> expression) rather than mutate the
/// one they were given. Rust enforces this with a <c>&amp;S</c> borrow; here it's convention.
/// A failing branch throws a <see cref="NodeException"/>; any other exception is a bug and
/// propagates out of the run like it does from a plain node.
/// </remarks>
public sealed class ParallelBranches<TState>
{
    private readonly List<BranchEntry<TState>> _branches = [];
    // Branch name -> its metadata (design/22-Graph-Structure-In-Definitions.md).
    private readonly Dictionary<string, IReadOnlyDictionary<string, string>> _metadata = [];

    /// <summary>
    /// Adds a branch. Names identify the branch in failure messages and must be unique within the
    /// group (<see cref="StateGraph{TState}.Compile"/> rejects duplicates). A sync branch runs on a
    /// thread of its own, so sync branches still run in parallel; it can't see a cancellation, so when
    /// the group gives up on it, it runs on and its result is discarded.
    ///
    /// <paramref name="metadata"/>, when given, is <see cref="SetMetadata"/> for the branch; leaving it
    /// out keeps whatever was set before.
    /// </summary>
    public ParallelBranches<TState> Branch(
        string name, Func<TState, TState> handler, IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(handler);

        _branches.Add(new BranchEntry<TState>(name, Streaming: false, (state, _) => handler(state), Async: null));
        return WithMetadata(name, metadata);
    }

    /// <summary>
    /// Adds an async branch (<c>design/17-Async-Execution.md</c>). It runs as a task, not on a thread
    /// of its own. Its <see cref="NodeContext"/> is named <c>"&lt;group&gt;/&lt;branch&gt;"</c> and
    /// carries the group's attempt; its token is cancelled with the run, by the group's timeout, or
    /// when a sibling fails. Otherwise as <see cref="Branch(string, Func{TState, TState}, IReadOnlyDictionary{string, string}?)"/>.
    /// </summary>
    public ParallelBranches<TState> Branch(
        string name,
        Func<TState, NodeContext, CancellationToken, ValueTask<TState>> handler,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(handler);

        _branches.Add(new BranchEntry<TState>(name, Streaming: false, Sync: null, handler));
        return WithMetadata(name, metadata);
    }

    /// <summary>
    /// Adds a branch that also receives an <see cref="IStreamContext"/>, so it can push tokens as it
    /// runs — concurrently with its siblings, on the one bounded channel the run streams through.
    /// Chunks arrive attributed to <c>"&lt;group&gt;/&lt;branch&gt;"</c>; each branch's own chunks
    /// stay in order, but chunks from different branches interleave however the threads are
    /// scheduled.
    ///
    /// A group with a streaming branch only runs under <c>RunStreaming</c>; a plain <c>RunAsync</c>
    /// fails at the group, as it does at a streaming node. If the group is retried (see
    /// <see cref="RecoveryPolicy"/>) every branch runs again and streams again, and a
    /// <see cref="StreamChunkType.Retry"/> chunk precedes the re-run so a consumer can discard what
    /// it showed. Otherwise as <see cref="Branch(string, Func{TState, TState}, IReadOnlyDictionary{string, string}?)"/>.
    /// </summary>
    public ParallelBranches<TState> StreamingBranch(
        string name, Func<TState, IStreamContext, TState> handler, IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(handler);

        // The engine refuses a streaming group on a run with no context, so the throw is only a guard.
        _branches.Add(new BranchEntry<TState>(
            name,
            Streaming: true,
            (state, context) => handler(state, context ?? throw NoStream()),
            Async: null));
        return WithMetadata(name, metadata);
    }

    /// <summary>
    /// An async <see cref="StreamingBranch(string, Func{TState, IStreamContext, TState}, IReadOnlyDictionary{string, string}?)"/>:
    /// its <see cref="NodeContext.Stream"/> is always present. Otherwise as
    /// <see cref="Branch(string, Func{TState, NodeContext, CancellationToken, ValueTask{TState}}, IReadOnlyDictionary{string, string}?)"/>.
    /// </summary>
    public ParallelBranches<TState> StreamingBranch(
        string name,
        Func<TState, NodeContext, CancellationToken, ValueTask<TState>> handler,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(handler);

        _branches.Add(new BranchEntry<TState>(
            name,
            Streaming: true,
            Sync: null,
            (state, context, ct) => context.Stream is null
                ? ValueTask.FromException<TState>(NoStream())
                : handler(state, context, ct)));
        return WithMetadata(name, metadata);
    }

    /// <summary>
    /// Describes <paramref name="branch"/> in the exported definition
    /// (<c>design/22-Graph-Structure-In-Definitions.md</c>): string keys and values, for a console or a
    /// CI diff to read. The engine never reads it. Setting it again replaces it, and it may come before
    /// or after the branch is added. <see cref="StateGraph{TState}.Compile"/> rejects metadata for a
    /// name that isn't a branch of the group, an empty key, and a key starting <c>strata.</c> (kept for
    /// the engine). Keys are exported sorted.
    /// </summary>
    public ParallelBranches<TState> SetMetadata(string branch, IReadOnlyDictionary<string, string> metadata)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(branch);
        _metadata[branch] = NodeMetadata.Copy(metadata);
        return this;
    }

    private ParallelBranches<TState> WithMetadata(string branch, IReadOnlyDictionary<string, string>? metadata) =>
        metadata is null ? this : SetMetadata(branch, metadata);

    private static NodeException NoStream() =>
        NodeException.Fatal("streaming branch has no StreamContext — use RunStreaming instead of RunAsync");

    internal IReadOnlyList<BranchEntry<TState>> Snapshot() => [.. _branches];

    internal GroupShape Shape() =>
        new([.. _branches.Select(b => b.Name)], new Dictionary<string, IReadOnlyDictionary<string, string>>(_metadata));
}

/// <summary>
/// Runs a parallel group's branches through <see cref="FanOutRunner"/>: a sync branch on a dedicated
/// thread, an async one as a task. A branch the group gives up on is abandoned, never waited for.
/// </summary>
internal static class ParallelGroupRunner
{
    /// <summary>
    /// Runs <paramref name="branches"/> against <paramref name="state"/> with
    /// <paramref name="options"/> (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>); see
    /// <see cref="FanOutRunner.RunAsync{TResult}"/>. An async branch's <see cref="NodeContext"/> is named
    /// <c>"&lt;group&gt;/&lt;branch&gt;"</c>, carries the group's attempt, decision and
    /// <see cref="NodeContext.LastFailure"/>, and its token is the branch's own.
    /// </summary>
    internal static Task<BranchOutcomes<TState>> RunAsync<TState>(
        string group,
        IReadOnlyList<BranchEntry<TState>> branches,
        ParallelGroupOptions<TState> options,
        TState state,
        AttemptInputs inputs)
    {
        BranchLauncher<TState> launch = (index, started) =>
        {
            var branch = branches[index];
            var stream = inputs.Stream?.ForNode($"{group}/{branch.Name}");
            if (branch.Sync is { } sync)
            {
                // LongRunning = a dedicated thread per sync branch. Sync handlers typically block, so
                // borrowing thread-pool threads would let pool starvation quietly serialise the branches
                // on a small machine.
                return Task.Factory.StartNew(
                    () => sync(state, stream),
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default);
            }

            var context = inputs.Level.Run.ContextFor(
                branch.Name,
                started.Path,
                inputs.Context.Attempt,
                stream,
                started.Activity,
                inputs.Level,
                inputs.Context.LastFailure);
            return branch.Async!(state, context, started.Token).AsTask();
        };

        return FanOutRunner.RunAsync(
            [.. branches.Select(b => b.Name)],
            launch,
            new FanOutSettings<TState>(
                options.MaxConcurrency,
                options.BranchTimeout,
                options.CancelSiblingsOnFailure,
                options.Partial,
                options.Join,
                FanOutNoun.Branch,
                WindsDown: false),
            inputs);
    }
}
