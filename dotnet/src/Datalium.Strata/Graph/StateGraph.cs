using Datalium.Strata.Streaming;
using Datalium.Strata.Validation;

namespace Datalium.Strata.Graph;

/// <summary>
/// The minimum graph model everything else (streaming, HITL, tool-calling) hangs off: nodes,
/// edges, and a <see cref="Compile"/> call that runs <see cref="GraphValidator.Validate"/>
/// before handing back a <see cref="CompiledGraph{TState}"/>.
///
/// Every kind of node (plain, fallible, streaming, and a parallel group's branches) takes either a
/// sync handler or an async one, <c>Func&lt;TState, NodeContext, CancellationToken,
/// ValueTask&lt;TState&gt;&gt;</c> (<c>design/17-Async-Execution.md</c>). An async handler gets a
/// <see cref="NodeContext"/> (thread id, node path, attempt, stream, history, services) and a token
/// that is cancelled with the run, by the node's timeout (<see cref="SetTimeout"/>), or for a branch
/// by a failing sibling. A sync handler runs on the thread pool, so it doesn't hold up the executor.
/// See <c>CompiledGraph{TState}</c>'s <c>RunAsync</c>/<c>RunStreaming</c> for the execution loop.
/// </summary>
public sealed class StateGraph<TState>
{
    private readonly List<string> _nodeOrder = [];
    // Every node, whatever kind, in the one shape the executor runs (see NodeEntry).
    private readonly Dictionary<string, NodeEntry<TState>> _nodes = [];
    // Group name -> branch names and branch metadata, kept so `Compile` can reject an empty group or
    // duplicate branch names and the topology can describe the group. The branches themselves live
    // inside the group's handler.
    private readonly Dictionary<string, GroupShape> _parallelGroups = [];
    // Subgraph name -> the child's topology, snapshotted when it was added
    // (design/22-Graph-Structure-In-Definitions.md).
    private readonly Dictionary<string, GraphTopology> _subgraphs = [];
    // Map name -> its template, exported join and the child's first interrupt boundary
    // (design/27-Map-Fan-Out.md).
    private readonly Dictionary<string, MapShape> _maps = [];
    // Node name -> its metadata. Kept apart from the node, so re-adding a name keeps it.
    private readonly Dictionary<string, IReadOnlyDictionary<string, string>> _nodeMetadata = [];
    private readonly Dictionary<string, RecoveryPolicy> _recoveryPolicies = [];
    private readonly Dictionary<string, TimeSpan> _timeouts = [];
    private readonly List<GraphEdge> _edges = [];
    // Parallel to `_edges` (same index = same edge) rather than keyed on the edge itself, since
    // GraphEdge is the lightweight structural shape used for STRATA-SPEC-004 validation and has
    // no room for a delegate.
    private readonly List<Func<TState, bool>?> _edgeConditions = [];
    private readonly HashSet<string> _interruptBoundaries = [];
    // Boundary -> the node a rejection continues at (design/21-Typed-Human-Signals.md).
    private readonly Dictionary<string, string> _rejectRoutes = [];
    // Wait node -> what it waits for (design/29-Event-Waits-And-Timers.md). Kept with the node: re-adding the
    // name as another kind of node drops it, and its timeout route.
    private readonly Dictionary<string, WaitSpec<TState>> _waits = [];
    // Boundary -> how long it waits for a human before timing out.
    private readonly Dictionary<string, TimeSpan> _boundaryTimeouts = [];
    // Boundary or event wait -> the node a timeout continues at.
    private readonly Dictionary<string, string> _timeoutRoutes = [];
    private string? _entryPoint;

    /// <summary>
    /// Registers <paramref name="name"/> as a node (entry point + declaration order) and clears any
    /// handler already stored under it, so re-adding a name replaces its handler whatever kind the
    /// old one was. <paramref name="metadata"/>, when given, is <see cref="SetNodeMetadata"/> for the
    /// node; leaving it out keeps whatever was set before.
    /// </summary>
    private StateGraph<TState> ClaimNodeName(
        string name, NodeEntry<TState> entry, IReadOnlyDictionary<string, string>? metadata)
    {
        if (!_nodes.ContainsKey(name))
        {
            _nodeOrder.Add(name);
        }

        _parallelGroups.Remove(name);
        _subgraphs.Remove(name);
        _maps.Remove(name);
        if (_waits.Remove(name))
        {
            _timeoutRoutes.Remove(name);
        }

        _nodes[name] = entry;
        _entryPoint ??= name;
        return metadata is null ? this : SetNodeMetadata(name, metadata);
    }

    /// <summary>
    /// Adds a node. The first node added becomes the entry point unless overridden via
    /// <see cref="SetEntryPoint"/>. Every adder takes an optional <c>metadata</c>, which is
    /// <see cref="SetNodeMetadata"/> for the node (<c>design/22-Graph-Structure-In-Definitions.md</c>);
    /// leaving it out keeps whatever was set before, so re-adding a name keeps its metadata.
    /// </summary>
    public StateGraph<TState> AddNode(string name, Func<TState, TState> handler, IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(handler);

        return ClaimNodeName(name, new(NodeKind.Plain, NodeHandlers.FromSync(handler), NeedsStream: false, IsSubgraph: false), metadata);
    }

    /// <summary>
    /// Adds a node with an async handler (<c>design/17-Async-Execution.md</c>). As with a sync plain
    /// node, a <see cref="NodeException"/> from it is not recovery-managed (use
    /// <see cref="AddFallibleNode(string, Func{TState, NodeContext, CancellationToken, ValueTask{TState}}, IReadOnlyDictionary{string, string}?)"/>
    /// for that), but a timeout set with <see cref="SetTimeout"/> fails it like a fallible node with
    /// no policy. <see cref="NodeContext.Stream"/> is present whenever the run is streaming.
    /// </summary>
    public StateGraph<TState> AddNode(
        string name,
        Func<TState, NodeContext, CancellationToken, ValueTask<TState>> handler,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(handler);

        return ClaimNodeName(name, new(NodeKind.Plain, NodeHandlers.FromAsync(handler), NeedsStream: false, IsSubgraph: false), metadata);
    }

    /// <summary>
    /// Adds a node whose handler can fail: throwing a <see cref="NodeException"/> (transient or
    /// fatal) hands the failure to the node's <see cref="RecoveryPolicy"/> — see
    /// <see cref="SetRecoveryPolicy"/>; without one, the first failure ends the run with a
    /// persisted <c>Failed</c> checkpoint. Any other exception propagates like it does from a
    /// plain node. Same handler shape as <see cref="AddNode"/>: the engine holds the last
    /// committed state, so a failed attempt's return value is simply never taken.
    /// </summary>
    public StateGraph<TState> AddFallibleNode(string name, Func<TState, TState> handler, IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(handler);

        return ClaimNodeName(name, new(NodeKind.Fallible, NodeHandlers.FromSync(handler), NeedsStream: false, IsSubgraph: false), metadata);
    }

    /// <summary>
    /// Adds a fallible node with an async handler (<c>design/17-Async-Execution.md</c>): throwing a
    /// <see cref="NodeException"/> hands the failure to the node's <see cref="RecoveryPolicy"/>, as
    /// from <see cref="AddFallibleNode(string, Func{TState, TState}, IReadOnlyDictionary{string, string}?)"/>. Each attempt gets its own
    /// <see cref="NodeContext.Attempt"/> and token.
    /// </summary>
    public StateGraph<TState> AddFallibleNode(
        string name,
        Func<TState, NodeContext, CancellationToken, ValueTask<TState>> handler,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(handler);

        return ClaimNodeName(name, new(NodeKind.Fallible, NodeHandlers.FromAsync(handler), NeedsStream: false, IsSubgraph: false), metadata);
    }

    /// <summary>
    /// Adds a parallel fan-out/fan-in node (<c>design/14-Parallel-Execution.md</c>).
    ///
    /// When the group runs, every branch in <paramref name="branches"/> gets the state as it was on
    /// entry and runs concurrently (a sync branch on its own thread, an async one as a task); once
    /// all of them have finished,
    /// <paramref name="reducer"/> merges the results into the state that flows on to the next node.
    /// The reducer receives the entry state and the branch results in *declaration order* (not
    /// completion order), so the merge is deterministic.
    ///
    /// To the rest of the graph the group is a single node: give it ordinary edges, mark it an
    /// interrupt boundary, or attach a <see cref="RecoveryPolicy"/> via <see cref="SetRecoveryPolicy"/>.
    /// If any branch fails the group fails, and the policy applies to the group as a whole — a retry
    /// re-runs <b>every</b> branch, so branches should be safe to repeat. The first branch to fail
    /// fails the group at once, and its siblings are cancelled (<c>design/17-Async-Execution.md</c>).
    ///
    /// A branch added with <see cref="ParallelBranches{TState}.StreamingBranch"/> gets an
    /// <see cref="IStreamContext"/> and can push tokens while it runs; a group with one only runs
    /// under <c>RunStreaming</c> (a plain <c>RunAsync</c> fails at the group, as it does at a
    /// streaming node). Chunks are attributed to <c>"&lt;group&gt;/&lt;branch&gt;"</c>.
    ///
    /// <see cref="Compile"/> rejects a group with no branches or with two branches of the same name.
    /// The topology lists the branches, with any metadata set on them
    /// (<see cref="ParallelBranches{TState}.SetMetadata"/>).
    /// </summary>
    public StateGraph<TState> AddParallelGroup(
        string name,
        ParallelBranches<TState> branches,
        Func<TState, IReadOnlyList<TState>, TState> reducer,
        IReadOnlyDictionary<string, string>? metadata = null) =>
        AddParallelGroup(name, branches, reducer, new ParallelGroupOptions<TState>(), metadata);

    /// <summary>
    /// <see cref="AddParallelGroup(string, ParallelBranches{TState}, Func{TState, IReadOnlyList{TState}, TState}, IReadOnlyDictionary{string, string}?)"/>
    /// with <paramref name="options"/> (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>): a
    /// concurrency limit, a timeout per branch, whether a failure cancels the siblings, partial results,
    /// and a join policy. <paramref name="reducer"/> gets the states of the branches that met the join
    /// (every branch for <c>All</c>), or, when the join wasn't met under
    /// <see cref="PartialResults.KeepSucceeded"/>, of every branch that succeeded, in declaration order.
    /// Use <see cref="AddParallelGroupWithOutcomes"/> to see which branch is which, and how the others ended.
    /// </summary>
    public StateGraph<TState> AddParallelGroup(
        string name,
        ParallelBranches<TState> branches,
        Func<TState, IReadOnlyList<TState>, TState> reducer,
        ParallelGroupOptions<TState> options,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentNullException.ThrowIfNull(reducer);
        return AddGroup(name, branches, (entry, results) => reducer(entry, results.Counted()), options, metadata);
    }

    /// <summary>
    /// A parallel group whose <paramref name="reducer"/> sees every branch's
    /// <see cref="BranchOutcome{TState}"/> (name, status, state when it succeeded, error), in declaration
    /// order, and how the join went (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>). With
    /// <see cref="PartialResults.KeepSucceeded"/> it runs even when some branches failed or timed out.
    /// <paramref name="options"/> default to a group as
    /// <see cref="AddParallelGroup(string, ParallelBranches{TState}, Func{TState, IReadOnlyList{TState}, TState}, IReadOnlyDictionary{string, string}?)"/>
    /// adds it. Otherwise as that method.
    /// </summary>
    public StateGraph<TState> AddParallelGroupWithOutcomes(
        string name,
        ParallelBranches<TState> branches,
        Func<TState, BranchOutcomes<TState>, TState> reducer,
        ParallelGroupOptions<TState>? options = null,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentNullException.ThrowIfNull(reducer);
        return AddGroup(name, branches, reducer, options ?? new ParallelGroupOptions<TState>(), metadata);
    }

    private StateGraph<TState> AddGroup(
        string name,
        ParallelBranches<TState> branches,
        Func<TState, BranchOutcomes<TState>, TState> reducer,
        ParallelGroupOptions<TState> options,
        IReadOnlyDictionary<string, string>? metadata)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(branches);
        ArgumentNullException.ThrowIfNull(options);

        // Snapshot now, so adding to `branches` afterwards can't change a graph already declared.
        var snapshot = branches.Snapshot();

        NodeHandler<TState> run = async (state, inputs) =>
            new NodeStep<TState>.Done(reducer(
                state,
                await ParallelGroupRunner.RunAsync(name, snapshot, options, state, inputs).ConfigureAwait(false)));
        ClaimNodeName(name, new(NodeKind.Fallible, run, NeedsStream: snapshot.Any(b => b.Streaming), IsSubgraph: false), metadata);
        _parallelGroups[name] = branches.Shape() with
        {
            Join = options.HasDefaultJoin
                ? null
                : new GraphJoin(options.Join.Kind, options.Join.Count, options.Join.Description, options.Partial),
        };
        return this;
    }

    /// <summary>
    /// Adds a compiled graph as a single node of this one (<c>design/15-Subgraph-Composition.md</c>).
    ///
    /// When the node runs, <paramref name="input"/> derives the child's initial state from the
    /// parent's, the child runs to completion (its own edges, conditions, recovery policies and
    /// nested subgraphs all apply), and <paramref name="output"/> folds its final state back into the
    /// parent's — it receives the state the node started with and the child's result. The child has
    /// its own state type, so the two mappers are the whole interface between the graphs. One
    /// <see cref="CompiledGraph{TState}"/> can back several nodes.
    ///
    /// To the rest of the graph this is one ordinary node: give it ordinary edges, mark it an
    /// interrupt boundary, or attach a <see cref="RecoveryPolicy"/> via <see cref="SetRecoveryPolicy"/>.
    /// If the child fails, the node fails — transient when the child's own recovery ran out on a
    /// transient error, fatal otherwise — and the policy applies to the child as a whole: a retry
    /// re-runs it from its entry point, so it should be safe to repeat.
    ///
    /// A child may have interrupt boundaries of its own (<c>design/16-Nested-Interrupts.md</c>). When
    /// it freezes at one, the whole run freezes: the parent's checkpoint is suspended at this node, holds
    /// the state the node started with, and nests the child's checkpoint. <c>ResumeAsync</c> then
    /// continues the child from where it froze — <paramref name="input"/> is not called again — applies
    /// the <see cref="Interruption.HumanSignal"/>'s field overrides to the child's own state, and carries
    /// on into the parent when the child completes. Nesting works to any depth.
    ///
    /// A child may stream (<c>design/15-Subgraph-Composition.md</c>): under <c>RunStreaming</c>
    /// everything it emits reaches the same reader, named <c>"&lt;name&gt;/&lt;child node&gt;"</c>,
    /// between this node's own <c>NodeStart</c> and <c>NodeEnd</c>. A parent with a child that streams
    /// only runs under <c>RunStreaming</c>; a plain <c>RunAsync</c> fails at this node, before running
    /// any of the child.
    ///
    /// The child sees cancellation at any depth (<c>design/17-Async-Execution.md</c>). A cancelled run
    /// stops it where it is and the parent's <c>cancelled</c> checkpoint nests the child's; this node's
    /// own timeout fails it transiently, nesting the child's <c>cancelled</c> checkpoint in the
    /// <c>failed</c> one, so a resume re-enters the child where it was stopped.
    ///
    /// The topology describes the node as a subgraph holding the child's own topology
    /// (<see cref="CompiledGraph{TState}.ToTopology"/>), taken now: a compiled graph can't change.
    /// </summary>
    public StateGraph<TState> AddSubgraph<TChild>(
        string name,
        CompiledGraph<TChild> child,
        Func<TState, TChild> input,
        Func<TState, TChild, TState> output,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(child);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);

        NodeHandler<TState> run = (state, inputs) => SubgraphRunner.RunAsync(name, child, input, output, state, inputs);
        ClaimNodeName(
            name,
            new(
                NodeKind.Fallible,
                run,
                NeedsStream: child.NeedsStream(),
                IsSubgraph: true,
                CheckResume: child.CheckResume,
                Boundary: child.InterruptBoundary(),
                Adopt: child.Adopt,
                Claim: child.Claim),
            metadata);
        _subgraphs[name] = child.ToTopology();
        return this;
    }

    /// <summary>
    /// Adds a map (<c>design/27-Map-Fan-Out.md</c>): a node that runs the compiled graph
    /// <paramref name="child"/> once per item of a list read from the state when the node starts, a few
    /// at a time, and merges the children's results.
    ///
    /// When the node runs, <paramref name="items"/> gives the list and <paramref name="key"/> names each
    /// item. More items than <see cref="MapOptions{TChild}.MaxItems"/>, an empty key, a key containing
    /// <c>/</c>, or two items with one key fail the map fatally before anything starts. Then a
    /// <see cref="Observability.MapExpanded"/> event lists the keys, and the items run, at most
    /// <see cref="MapOptions{TChild}.MaxConcurrency"/> (8 by default) at once, in list order: each
    /// builds its child's initial state with <paramref name="input"/> when it starts and runs
    /// <paramref name="child"/> at a level of its own, as a subgraph's child does, its nodes named
    /// <c>"&lt;name&gt;/&lt;key&gt;/&lt;child node&gt;"</c>. A child failure fails its item, transient or
    /// fatal as a subgraph node's failure would be. <paramref name="reducer"/> gets the children's final
    /// states in list order: the deciding items' when the join is met (every item for the default
    /// <c>All</c>, so an empty list gives an empty one), else, under
    /// <see cref="PartialResults.KeepSucceeded"/>, every succeeded item's.
    /// <see cref="AddMapWithOutcomes"/> shows which item is which, and how the others ended.
    ///
    /// The items are a parallel group's branches: <paramref name="options"/> mean what
    /// <see cref="ParallelGroupOptions{TState}"/>'s do, and each item reports
    /// <see cref="Observability.BranchStarted"/> / <see cref="Observability.BranchCompleted"/> at
    /// <c>"&lt;name&gt;/&lt;key&gt;"</c>. An item the map gives up on (a lost race, a failing sibling,
    /// its timeout, a cancelled run) winds down: its child stops at its current node, and the map waits
    /// for that before reporting it.
    ///
    /// To the rest of the graph the map is one node: give it ordinary edges, mark it an interrupt
    /// boundary, or attach a <see cref="RecoveryPolicy"/>, which retries the whole map (every item runs
    /// again). It is one hop (<see cref="CompilationOptions.MaxNodeHops"/>); each item's child counts its
    /// own. Its checkpoints never nest an item's. Under <c>RunStreaming</c> an item's chunks are named
    /// <c>"&lt;name&gt;/&lt;key&gt;/&lt;child node&gt;"</c>; a child that streams makes a plain
    /// <c>RunAsync</c> fail at this node. <see cref="Compile"/> refuses a <paramref name="child"/> with an
    /// interrupt boundary at any depth: a map item can't freeze.
    ///
    /// The topology describes the node as a map holding <paramref name="child"/>'s topology as its
    /// template, and its join when it isn't the default.
    /// </summary>
    public StateGraph<TState> AddMap<TItem, TChild>(
        string name,
        Func<TState, IEnumerable<TItem>> items,
        Func<TItem, string> key,
        CompiledGraph<TChild> child,
        Func<TState, TItem, TChild> input,
        Func<TState, IReadOnlyList<TChild>, TState> reducer,
        MapOptions<TChild>? options = null,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentNullException.ThrowIfNull(reducer);
        return AddMapCore(
            name, items, key, child, input, (entry, results) => reducer(entry, results.Counted()), options, metadata);
    }

    /// <summary>
    /// A map whose <paramref name="reducer"/> sees every item's <see cref="BranchOutcome{TChild}"/>
    /// (<see cref="BranchOutcome{TState}.Name"/> is its key; status, the child's final state when it
    /// succeeded, error), in list order, and how the join went. With
    /// <see cref="PartialResults.KeepSucceeded"/> it runs even when some items failed or timed out.
    /// Otherwise as <see cref="AddMap"/>.
    /// </summary>
    public StateGraph<TState> AddMapWithOutcomes<TItem, TChild>(
        string name,
        Func<TState, IEnumerable<TItem>> items,
        Func<TItem, string> key,
        CompiledGraph<TChild> child,
        Func<TState, TItem, TChild> input,
        Func<TState, BranchOutcomes<TChild>, TState> reducer,
        MapOptions<TChild>? options = null,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentNullException.ThrowIfNull(reducer);
        return AddMapCore(name, items, key, child, input, reducer, options, metadata);
    }

    private StateGraph<TState> AddMapCore<TItem, TChild>(
        string name,
        Func<TState, IEnumerable<TItem>> items,
        Func<TItem, string> key,
        CompiledGraph<TChild> child,
        Func<TState, TItem, TChild> input,
        Func<TState, BranchOutcomes<TChild>, TState> reducer,
        MapOptions<TChild>? options,
        IReadOnlyDictionary<string, string>? metadata)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(child);
        ArgumentNullException.ThrowIfNull(input);
        options ??= new MapOptions<TChild>();

        NodeHandler<TState> run = (state, inputs) =>
            MapRunner.RunAsync(name, items, key, child, input, reducer, options, state, inputs);
        ClaimNodeName(
            name,
            new(NodeKind.Fallible, run, NeedsStream: child.NeedsStream(), IsSubgraph: false, WindsDown: true),
            metadata);
        _maps[name] = new MapShape(child.ToTopology(), options.ExportedJoin, child.InterruptBoundary());
        return this;
    }

    /// <summary>
    /// Attaches a <see cref="RecoveryPolicy"/> to a fallible node (which includes a parallel group —
    /// see <see cref="AddParallelGroup"/> — and a subgraph — see <see cref="AddSubgraph{TChild}"/>). <see cref="Compile"/> rejects a
    /// policy on a node that isn't fallible, or whose fallback names a node that doesn't exist.
    /// The fallback route counts as a guarded edge for validation (STRATA-SPEC-004), so a fallback
    /// node needs no ordinary edge into it to avoid being flagged as an orphan.
    /// </summary>
    public StateGraph<TState> SetRecoveryPolicy(string node, RecoveryPolicy policy)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(node);
        ArgumentNullException.ThrowIfNull(policy);

        _recoveryPolicies[node] = policy;
        return this;
    }

    /// <summary>
    /// Describes <paramref name="node"/> in the exported definition
    /// (<c>design/22-Graph-Structure-In-Definitions.md</c>): string keys and values, such as the agent a
    /// node runs or its model policy, for a console or a CI diff to read. The engine never reads it.
    /// Works for every kind of node, a parallel group or subgraph included (a group's branches take
    /// theirs from <see cref="ParallelBranches{TState}.SetMetadata"/>). The <c>metadata</c> parameter of
    /// every adder calls this.
    ///
    /// Setting it again replaces it, and re-adding the node keeps it. <see cref="Compile"/> rejects
    /// metadata for a name that isn't a node, an empty key, and a key starting <c>strata.</c> (kept for
    /// the engine, as for run metadata). Keys are exported sorted, so the same graph exports the same
    /// bytes every time.
    /// </summary>
    public StateGraph<TState> SetNodeMetadata(string node, IReadOnlyDictionary<string, string> metadata)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(node);
        _nodeMetadata[node] = NodeMetadata.Copy(metadata);
        return this;
    }

    /// <summary>
    /// Adds a streaming node: like <see cref="AddNode"/>, but the handler also receives an
    /// <see cref="IStreamContext"/> scoped to this node so it can push output chunks (e.g. LLM
    /// response tokens) as it runs. Only usable under <c>CompiledGraph{TState}.RunStreaming</c> —
    /// see <see cref="StreamingNodeHandler{TState}"/>.
    /// </summary>
    public StateGraph<TState> AddStreamingNode(string name, StreamingNodeHandler<TState> handler, IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(handler);

        // Runs on the thread pool like any sync handler; its blocking emits are safe there.
        NodeHandler<TState> run = (state, inputs) => new ValueTask<NodeStep<TState>>(
            Task.Run<NodeStep<TState>>(() => new NodeStep<TState>.Done(handler(state, inputs.Stream!))));
        return ClaimNodeName(name, new(NodeKind.Streaming, run, NeedsStream: true, IsSubgraph: false), metadata);
    }

    /// <summary>
    /// Adds a streaming node with an async handler (<c>design/17-Async-Execution.md</c>): its
    /// <see cref="NodeContext.Stream"/> is always present, since a plain <c>RunAsync</c> refuses the
    /// node before running it. Emit with the async methods
    /// (<see cref="IStreamContext.EmitTokenAsync"/>), passing the handler's token.
    /// </summary>
    public StateGraph<TState> AddStreamingNode(
        string name,
        Func<TState, NodeContext, CancellationToken, ValueTask<TState>> handler,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(handler);

        return ClaimNodeName(name, new(NodeKind.Streaming, NodeHandlers.FromAsync(handler), NeedsStream: true, IsSubgraph: false), metadata);
    }

    /// <summary>
    /// Limits each attempt of <paramref name="node"/> to <paramref name="timeout"/>
    /// (<c>design/17-Async-Execution.md</c>). An attempt that runs over is a <b>transient</b> failure,
    /// <c>node '&lt;name&gt;' timed out after &lt;n&gt; ms</c>: a fallible node's
    /// <see cref="RecoveryPolicy"/> retries it (each retry gets a fresh timeout) or falls back, and any
    /// other node fails the run with a <c>Failed</c> checkpoint. The handler's token is cancelled and
    /// the engine stops waiting for it; a sync handler can't see that, so it runs on and its result is
    /// discarded. On a parallel group the timeout covers the whole group, on a subgraph the whole
    /// child. <see cref="Compile"/> rejects a timeout on a node that doesn't exist.
    /// </summary>
    public StateGraph<TState> SetTimeout(string node, TimeSpan timeout)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(node);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);

        _timeouts[node] = timeout;
        return this;
    }

    /// <summary>Adds a direct edge, always taken once <paramref name="from"/> finishes executing. Only guarded edges (see <see cref="AddConditionalEdge"/>) are allowed to close a cycle.</summary>
    public StateGraph<TState> AddEdge(string from, string to) => AddEdgeCore(from, to, condition: null);

    /// <summary>
    /// Adds an edge whose <paramref name="to"/> node is only taken if <paramref name="condition"/>
    /// returns <see langword="true"/> for the state <paramref name="from"/> just produced. Exempt
    /// from the unguarded-cycle check (STRATA_ERR_UNGUARDED_CYCLE), since the runtime predicate —
    /// not the graph's static shape — is what keeps the loop from running forever.
    /// </summary>
    /// <remarks>
    /// When a node has more than one outgoing edge, <c>CompiledGraph.RunAsync</c> tries them in
    /// the order they were added and takes the first whose condition matches; a plain
    /// <see cref="AddEdge"/> always matches, so adding one last acts as a default "else" branch.
    /// </remarks>
    public StateGraph<TState> AddConditionalEdge(string from, string to, Func<TState, bool> condition)
    {
        ArgumentNullException.ThrowIfNull(condition);
        return AddEdgeCore(from, to, condition);
    }

    private StateGraph<TState> AddEdgeCore(string from, string to, Func<TState, bool>? condition)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(from);
        ArgumentException.ThrowIfNullOrWhiteSpace(to);

        _edges.Add(new GraphEdge(from, to, Guarded: condition is not null));
        _edgeConditions.Add(condition);
        return this;
    }

    public StateGraph<TState> SetEntryPoint(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        _entryPoint = name;
        return this;
    }

    /// <summary>
    /// Marks a node as an interrupt boundary (STRATA-SPEC-002): once this node finishes
    /// executing, the engine freezes, persists, and returns control rather than continuing to
    /// the next node. A name that isn't (yet) an added node is accepted but simply never
    /// triggers.
    ///
    /// With <paramref name="onReject"/>, a resume whose <see cref="Interruption.HumanSignal.IsApproved"/>
    /// is <see langword="false"/> continues at that node instead of the next node fixed when the
    /// boundary froze (<c>design/21-Typed-Human-Signals.md</c>). Only a resume of this boundary's own
    /// interrupted checkpoint takes the route: never a failed or cancelled thread's, and never an outer
    /// level's. The route is a guarded <c>on_reject</c> edge to validation and in the exported
    /// definition, so <see cref="Compile"/> rejects one to a node that doesn't exist (or one on a
    /// boundary name that isn't a node) as a dangling edge. Marking the same name again replaces its
    /// route, or removes it when <paramref name="onReject"/> is <see langword="null"/>.
    ///
    /// With <paramref name="timeout"/>, the boundary waits for a human at most that long
    /// (<c>design/29-Event-Waits-And-Timers.md</c>): its checkpoint carries a <c>wait</c> of kind
    /// <c>human</c> whose <c>wake_at</c> is when it times out, and <see cref="CompiledGraph{TState}.WakeDueAsync"/>
    /// times it out once that has passed: it continues at <paramref name="onTimeout"/>, or, with none, the
    /// thread is ended with <c>strata.end_reason</c> <c>timed_out</c>. The route is a guarded
    /// <c>on_timeout</c> edge, like the reject route. Marking the name again replaces its timeout and route.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> isn't positive.</exception>
    /// <exception cref="ArgumentException"><paramref name="onTimeout"/> is given without a timeout.</exception>
    public StateGraph<TState> AddInterruptBoundary(
        string name, string? onReject = null, TimeSpan? timeout = null, string? onTimeout = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        CheckTimeout(timeout, onTimeout);
        if (onReject is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(onReject);
            _rejectRoutes[name] = onReject;
        }
        else
        {
            _rejectRoutes.Remove(name);
        }

        if (timeout is { } limit)
        {
            _boundaryTimeouts[name] = limit;
        }
        else
        {
            _boundaryTimeouts.Remove(name);
        }

        SetTimeoutRoute(name, onTimeout);
        _interruptBoundaries.Add(name);
        return this;
    }

    /// <summary>
    /// Adds a wait node that waits for an external event (<c>design/29-Event-Waits-And-Timers.md</c>): the
    /// node passes its state through, and the run freezes after it, as at an interrupt boundary, with a
    /// <c>wait</c> of kind <c>event</c> for <paramref name="eventType"/>. Only <c>CompiledGraph.SignalAsync</c>
    /// with that event type answers it: its overrides replace fields of the state, and the thread continues at
    /// the edge out of the node. With <paramref name="timeout"/>, <see cref="CompiledGraph{TState}.WakeDueAsync"/>
    /// times it out once that long has passed: it continues at <paramref name="onTimeout"/> (a guarded
    /// <c>on_timeout</c> edge), or, with none, the thread is ended with <c>strata.end_reason</c> <c>timed_out</c>.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> isn't positive.</exception>
    /// <exception cref="ArgumentException"><paramref name="onTimeout"/> is given without a timeout.</exception>
    public StateGraph<TState> AddWaitForEvent(
        string name,
        string eventType,
        TimeSpan? timeout = null,
        string? onTimeout = null,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(eventType);
        CheckTimeout(timeout, onTimeout);
        return AddWait(
            name,
            new WaitSpec<TState>(Interruption.WaitKind.Event, eventType, Delay: null, WakeAt: null, timeout),
            onTimeout,
            metadata);
    }

    /// <summary>
    /// Adds a timer (<c>design/29-Event-Waits-And-Timers.md</c>): a wait node after which the run freezes, as at
    /// an interrupt boundary, with a <c>wait</c> of kind <c>timer</c> due <paramref name="delay"/> after the node
    /// ran, rounded up to a whole second. <see cref="CompiledGraph{TState}.WakeDueAsync"/> wakes it once that has
    /// passed, and it continues at the edge out of the node. Nothing else answers a timer; ending the thread
    /// abandons it. The thread holds nothing while it waits.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="delay"/> isn't positive.</exception>
    public StateGraph<TState> AddTimer(string name, TimeSpan delay, IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(delay, TimeSpan.Zero);
        return AddWait(
            name,
            new WaitSpec<TState>(Interruption.WaitKind.Timer, EventType: null, delay, WakeAt: null, Timeout: null),
            onTimeout: null,
            metadata);
    }

    /// <summary>
    /// <see cref="AddTimer(string, TimeSpan, IReadOnlyDictionary{string, string}?)"/>, due at the time
    /// <paramref name="wakeAt"/> computes from the state when the node runs, rounded up to a whole second. A
    /// time already past is due at once.
    /// </summary>
    public StateGraph<TState> AddTimer(
        string name, Func<TState, DateTimeOffset> wakeAt, IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(wakeAt);
        return AddWait(
            name,
            new WaitSpec<TState>(Interruption.WaitKind.Timer, EventType: null, Delay: null, wakeAt, Timeout: null),
            onTimeout: null,
            metadata);
    }

    private StateGraph<TState> AddWait(
        string name, WaitSpec<TState> wait, string? onTimeout, IReadOnlyDictionary<string, string>? metadata)
    {
        // A wait node does nothing itself: the run freezes after it.
        NodeHandler<TState> passThrough = (state, _) => new ValueTask<NodeStep<TState>>(new NodeStep<TState>.Done(state));
        ClaimNodeName(name, new(NodeKind.Plain, passThrough, NeedsStream: false, IsSubgraph: false), metadata);
        _waits[name] = wait;
        SetTimeoutRoute(name, onTimeout);
        return this;
    }

    private static void CheckTimeout(TimeSpan? timeout, string? onTimeout)
    {
        if (timeout is { } limit)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(limit, TimeSpan.Zero, nameof(timeout));
        }

        if (onTimeout is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(onTimeout);
            if (timeout is null)
            {
                throw new ArgumentException("a timeout route needs a timeout", nameof(onTimeout));
            }
        }
    }

    private void SetTimeoutRoute(string name, string? onTimeout)
    {
        if (onTimeout is not null)
        {
            _timeoutRoutes[name] = onTimeout;
        }
        else
        {
            _timeoutRoutes.Remove(name);
        }
    }

    /// <summary>
    /// The graph's structure as <c>strata graph validate</c> sees it, <b>without validating it</b>
    /// — so a graph <see cref="Compile"/> would reject can still be exported and gated in CI.
    /// Nodes are in declaration order, then the edges in the order they were added, then one
    /// guarded <c>on_failure</c> edge per recovery-policy fallback, then one guarded <c>on_reject</c>
    /// edge per interrupt boundary's reject route, then one guarded <c>on_timeout</c> edge per wait's timeout
    /// route: the same view <see cref="Compile"/> validates, so the CLI and <see cref="Compile"/> agree on a
    /// node reachable only that way. A wait node is described as one, with what it waits for
    /// (<c>design/29-Event-Waits-And-Timers.md</c>).
    ///
    /// <see cref="GraphTopology.Details"/> carries each node's metadata (<see cref="SetNodeMetadata"/>),
    /// a parallel group's branches (name and metadata, in declaration order), and a subgraph's child
    /// topology, recursively (<c>design/22-Graph-Structure-In-Definitions.md</c>). Metadata set for a
    /// name that isn't a node or branch is left out. An interrupt boundary that is a node carries
    /// <see cref="GraphNodeDetail.Interrupt"/>, with its timeout (<c>design/48-Boundaries-In-Definitions.md</c>).
    /// Handlers, edge predicates, node timeouts and retry counts are not part of it. <c>Datalium.Strata.Storage</c>'s <c>ToDefinition()</c> turns
    /// it into the JSON interop shape.
    /// </summary>
    public GraphTopology ToTopology() =>
        PlainTopology()
            .WithFallbackEdges(_recoveryPolicies)
            .WithRejectEdges(_rejectRoutes)
            .WithTimeoutEdges(_timeoutRoutes);

    /// <summary>Nodes and ordinary edges only — what a compiled graph routes over.</summary>
    private GraphTopology PlainTopology()
    {
        var details = new Dictionary<string, GraphNodeDetail>();
        foreach (var name in _nodeOrder)
        {
            if (DetailOf(name) is { } detail)
            {
                details[name] = detail;
            }
        }

        return new(_entryPoint ?? string.Empty, [.. _nodeOrder], [.. _edges]) { Details = details };
    }

    private GraphNodeDetail? DetailOf(string name)
    {
        var detail = KindDetailOf(name);

        // A wait node can't also be a boundary (Compile refuses it), so the export never says it is
        // (design/48-Boundaries-In-Definitions.md).
        if (!_interruptBoundaries.Contains(name) || _waits.ContainsKey(name))
        {
            return detail;
        }

        var interrupt = new GraphInterrupt(
            _boundaryTimeouts.TryGetValue(name, out var timeout) ? (long)timeout.TotalMilliseconds : null);
        return (detail ?? new GraphNodeDetail()) with { Interrupt = interrupt };
    }

    private GraphNodeDetail? KindDetailOf(string name)
    {
        var metadata = _nodeMetadata.TryGetValue(name, out var set) ? NodeMetadata.Sorted(set) : null;
        if (_parallelGroups.TryGetValue(name, out var group))
        {
            return new GraphNodeDetail
            {
                Metadata = metadata,
                Kind = GraphNodeKind.ParallelGroup,
                Join = group.Join,
                Branches =
                [
                    .. group.Branches.Select(branch => new GraphBranch(
                        branch,
                        group.Metadata.TryGetValue(branch, out var m) ? NodeMetadata.Sorted(m) : null)),
                ],
            };
        }

        if (_subgraphs.TryGetValue(name, out var child))
        {
            return new GraphNodeDetail { Metadata = metadata, Kind = GraphNodeKind.Subgraph, Graph = child };
        }

        if (_maps.TryGetValue(name, out var map))
        {
            return new GraphNodeDetail { Metadata = metadata, Kind = GraphNodeKind.Map, Graph = map.Template, Join = map.Join };
        }

        if (_waits.TryGetValue(name, out var wait))
        {
            return new GraphNodeDetail { Metadata = metadata, Kind = GraphNodeKind.Wait, Wait = wait.Exported() };
        }

        return metadata is null ? null : new GraphNodeDetail { Metadata = metadata };
    }

    /// <summary>
    /// <see cref="Compile"/>'s metadata rules: every node metadata entry names a node, every branch
    /// metadata entry a branch of its group, and no key is empty or starts <c>strata.</c>. Nodes are
    /// checked in ordinal name order, then groups in node order, so the first error reported is the
    /// one Rust reports.
    /// </summary>
    private void CheckMetadata()
    {
        foreach (var node in _nodeMetadata.Keys.Order(StringComparer.Ordinal))
        {
            if (!_nodes.ContainsKey(node))
            {
                throw new InvalidOperationException($"metadata set for '{node}', which does not exist");
            }

            NodeMetadata.CheckKeys(_nodeMetadata[node], $"node '{node}'");
        }

        foreach (var groupName in _nodeOrder)
        {
            if (!_parallelGroups.TryGetValue(groupName, out var group))
            {
                continue;
            }

            foreach (var branch in group.Metadata.Keys.Order(StringComparer.Ordinal))
            {
                var owner = $"branch '{branch}' of parallel group '{groupName}'";
                if (!group.Branches.Contains(branch))
                {
                    throw new InvalidOperationException($"metadata set for {owner}, which does not exist");
                }

                NodeMetadata.CheckKeys(group.Metadata[branch], owner);
            }
        }
    }

    /// <summary>
    /// Runs compile-time validation (STRATA-SPEC-004) and, if the graph is structurally sound,
    /// returns a <see cref="CompiledGraph{TState}"/>. Throws <see cref="GraphCompilationException"/>
    /// otherwise — structural errors abort here rather than surfacing at runtime. Throws
    /// <see cref="InvalidOperationException"/> for a misconfigured <see cref="RecoveryPolicy"/>
    /// (Rust's <c>StrataError::CompilationFailed</c>).
    /// </summary>
    public CompiledGraph<TState> Compile() => Compile(new CompilationOptions());

    /// <summary>
    /// <see cref="Compile()"/> with <paramref name="options"/>: a loop circuit breaker
    /// (<see cref="CompilationOptions.MaxNodeHops"/>, <c>design/24-Loop-Circuit-Breaker.md</c>), the graph's
    /// identity and migration (<c>design/31-Graph-Identity.md</c>), and when its runs write checkpoints
    /// (<see cref="CompilationOptions.Checkpointing"/>, <c>design/33-Durable-Execution.md</c>).
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="CompilationOptions.MaxNodeHops"/> is less than 1.</exception>
    /// <exception cref="ArgumentException">
    /// <see cref="CompilationOptions.GraphId"/> or <see cref="CompilationOptions.GraphVersion"/> is blank, or a
    /// version or a migration is given without an id.
    /// </exception>
    public CompiledGraph<TState> Compile(CompilationOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.MaxNodeHops is < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options), options.MaxNodeHops, "MaxNodeHops must be at least 1");
        }

        var identity = IdentityOf(options);

        foreach (var (group, shape) in _parallelGroups)
        {
            var branches = shape.Branches;
            if (branches.Count == 0)
            {
                throw new InvalidOperationException($"parallel group '{group}' has no branches");
            }

            if (branches.GroupBy(b => b).FirstOrDefault(g => g.Count() > 1) is { } duplicate)
            {
                throw new InvalidOperationException(
                    $"parallel group '{group}' has more than one branch named '{duplicate.Key}'");
            }
        }

        foreach (var name in _nodeOrder)
        {
            if (_maps.TryGetValue(name, out var map) && map.Boundary is { } boundary)
            {
                throw new InvalidOperationException(
                    $"map '{name}' runs a graph with {boundary}; a map item can't freeze");
            }

            if (_waits.ContainsKey(name) && _interruptBoundaries.Contains(name))
            {
                throw new InvalidOperationException(
                    $"'{name}' is a wait node and can't also be an interrupt boundary");
            }
        }

        foreach (var (node, policy) in _recoveryPolicies)
        {
            if (!_nodes.TryGetValue(node, out var entry) || entry.Kind != NodeKind.Fallible)
            {
                throw new InvalidOperationException(
                    $"recovery policy set for '{node}', which is not a fallible node");
            }

            if (policy.Fallback is not null && !_nodeOrder.Contains(policy.Fallback))
            {
                throw new InvalidOperationException(
                    $"recovery policy for '{node}' falls back to '{policy.Fallback}', which does not exist");
            }
        }

        foreach (var node in _timeouts.Keys)
        {
            if (!_nodes.ContainsKey(node))
            {
                throw new InvalidOperationException($"timeout set for '{node}', which does not exist");
            }
        }

        CheckMetadata();

        // Validate the exported form, which also carries each fallback route as a guarded edge
        // (taken only on a runtime failure), so a node reachable only through a recovery policy
        // isn't reported as an orphan, and likewise each reject route. The compiled graph keeps just
        // the ordinary edges: neither route is something NextNodeAfter should ever pick.
        var result = GraphValidator.Validate(ToTopology());
        if (!result.IsValid)
        {
            throw new GraphCompilationException(result);
        }

        return new CompiledGraph<TState>(
            PlainTopology(),
            new Dictionary<string, NodeEntry<TState>>(_nodes),
            new Dictionary<string, RecoveryPolicy>(_recoveryPolicies),
            new Dictionary<string, TimeSpan>(_timeouts),
            new HashSet<string>(_interruptBoundaries),
            new Dictionary<string, string>(_rejectRoutes),
            [.. _edgeConditions],
            options.MaxNodeHops)
        {
            Identity = identity,
            Migration = options.Migrate,
            Checkpointing = options.Checkpointing,
            Waits = new Dictionary<string, WaitSpec<TState>>(_waits),
            BoundaryTimeouts = new Dictionary<string, TimeSpan>(_boundaryTimeouts),
            TimeoutRoutes = new Dictionary<string, string>(_timeoutRoutes),
        };
    }

    /// <summary>The identity <paramref name="options"/> give the graph, refusing a blank one or parts without an id.</summary>
    private static Interruption.GraphIdentity? IdentityOf(CompilationOptions options)
    {
        if (options.GraphId is null)
        {
            if (options.GraphVersion is not null)
            {
                throw new ArgumentException("GraphVersion needs a GraphId", nameof(options));
            }

            return options.Migrate is null
                ? null
                : throw new ArgumentException("Migrate needs a GraphId", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.GraphId))
        {
            throw new ArgumentException("GraphId must not be blank", nameof(options));
        }

        if (options.GraphVersion is { } version && string.IsNullOrWhiteSpace(version))
        {
            throw new ArgumentException("GraphVersion must not be blank", nameof(options));
        }

        return new Interruption.GraphIdentity(options.GraphId, options.GraphVersion);
    }
}
