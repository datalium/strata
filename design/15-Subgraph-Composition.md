# Datalium Strata: Subgraph Composition

Before this, every graph was flat: one set of nodes over one state type. There was no way to build a graph out of other graphs, so a "multi-agent" system had to be a single graph with every agent's steps and state fields mixed together. This document specifies the **subgraph**, added while building demo 13 (`design/Scenarios/demo-projects-1.md`). It is a status-first record: what exists, how it behaves, and what was deliberately left out. Unlike `04-Error-Recovery.md`, there was no earlier blueprint to diverge from.

---

## The model

A **subgraph** is a compiled graph, with its own state type, run as one node of another graph. Two mapper functions are the whole interface between them: `input` builds the child's starting state from the parent's, and `output` folds the child's finished state back into the parent's.

```rust
// Rust
let billing = StateGraph::<BillingCase>::new()
    .add_node("LookupInvoice", lookup)
    .add_node("DraftReply", draft)
    .add_edge("LookupInvoice", "DraftReply")
    .compile()?;

StateGraph::<Ticket>::new()
    .add_node("Triage", triage)
    .add_subgraph(
        "BillingAgent",
        billing,                                    // CompiledGraph<BillingCase>, or an Arc of one
        |t: &Ticket| BillingCase::from(t),          // input:  &S -> T
        |t: &Ticket, c: BillingCase| t.with(c),     // output: (&S entry state, T result) -> S
    )
    .add_edge("Triage", "BillingAgent")
    .compile()?;
```

```csharp
// C#
var billing = new StateGraph<BillingCase>()
    .AddNode("LookupInvoice", Lookup)
    .AddNode("DraftReply", Draft)
    .AddEdge("LookupInvoice", "DraftReply")
    .Compile();

new StateGraph<Ticket>()
    .AddNode("Triage", Triage)
    .AddSubgraph(
        "BillingAgent",
        billing,                                    // CompiledGraph<BillingCase>
        t => BillingCase.From(t),                   // input
        (t, c) => t.With(c))                        // output: (entry state, result)
    .AddEdge("Triage", "BillingAgent")
    .Compile();
```

To the rest of the parent graph the subgraph is **one ordinary node**. It has ordinary edges in and out (including conditional ones, which is all a "supervisor" routing to several sub-graphs needs), can be an interrupt boundary, can carry a recovery policy, and shows up in the graph definition as a plain node. That is why nothing else had to change: STRATA-SPEC-004 validation, STRATA-SPEC-002 interruption and the `04-Error-Recovery.md` machinery all see a node and treat it like one. It is the same trick as the parallel group (`14-Parallel-Execution.md`): the subgraph is registered as a *fallible* node.

## Semantics

| Aspect | Behaviour |
|---|---|
| **State** | The child has its own state type. The parent's state never reaches it except through `input`; the child's never reaches the parent except through `output`. `output` also receives the state the node started with, so it can keep parent-only fields. Both types must be serializable (Rust: `serde`), as for any graph. |
| **Execution** | The child runs to completion inside the parent's run, as `run` / `RunAsync` would run it: its edges, conditional edges, loops, fallible nodes, recovery policies, parallel groups and nested subgraphs all apply. Nesting has no limit. Since `17-Async-Execution.md` the child is awaited like any node, and its async handlers see the parent run's thread id, history and services, with node paths qualified as `<subgraph>/<node>`. |
| **Cancellation** | The child shares the run's cancellation (`17-Async-Execution.md`). A cancelled run stops the child where it is, and the parent's `cancelled` checkpoint nests the child's; a resume re-enters it there. A timeout on the subgraph node stops the child the same way, but counts as a transient failure of the node: the `failed` checkpoint nests the child's `cancelled` one. The child's own nodes' timeouts are failures inside the child, handled by its own policies. |
| **Reuse** | One compiled child can back several nodes (Rust: pass an `Arc<CompiledGraph<T>>`, or a plain graph to give it away; C#: it is a reference type). A compiled graph holds no per-run state, so concurrent runs are fine. |
| **Failure kind** | A child failure fails the subgraph node. It is **transient** if the child's own recovery ran out on a transient error, and **fatal** otherwise (a fatal node error, a routing gap, a missing node). So a fatal error is fatal at every level of nesting, and a transient one can be retried by the parent. The child's own retries and fallbacks have already run by then. |
| **Message** | `subgraph 'Work' failed at 'Flaky' after 2 attempt(s): <child's message>`. When subgraphs nest, each level wraps the one below, so the message names the whole path. A child routing gap reads `subgraph 'Work': no outgoing edge condition matched for node 'Start'`. |
| **Recovery** | The parent's `RecoveryPolicy` on the node applies to the child **as a whole**: a retry re-runs it from its entry point (so a child should be safe to repeat), and a fallback node runs on the parent state from *before* the subgraph node started, so none of the child's partial work leaks. With neither, the parent persists a `Failed` checkpoint. |
| **Checkpoints** | A child failure is persisted only as the *parent's* `Failed` checkpoint: the parent's own state (not the child's) as it was before the node, with `error` / `error_kind` / `attempts` in `metadata`. The child does not write checkpoints of its own. |
| **Panics / bugs** | A Rust panic, or a C# exception that is not a `NodeException`, is not a recovery-managed failure. It propagates out of `run` / `RunAsync` exactly as it does from a plain node, and is not retried. |
| **Streaming** | Under `run_streaming` / `RunStreaming` the child's chunks reach the parent's stream, named `<subgraph>/<child node>`. See [Streaming inside a subgraph](#streaming-inside-a-subgraph). |

A child **may** have interrupt boundaries (see `16-Nested-Interrupts.md`) and **may** stream (below). `.compile()` has nothing to reject about a child: each graph is validated by its own `.compile()`. Re-adding a node name replaces a subgraph, and its stream requirement goes with it.

## Streaming inside a subgraph

A child with a streaming node, or a parallel group with a streaming branch, works under `run_streaming` / `RunStreaming`. (Until 2026-09-21 `.compile()` rejected such a child, because the subgraph runner ignored the `StreamContext` the engine hands every fallible node.) The runner now gives the child that context **scoped** to the subgraph node, so every node name the child derives is qualified by it.

| Aspect | Behaviour |
|---|---|
| **Naming** | A child node's chunks carry `node_name = "<subgraph>/<child node>"`: `Work/Talk`. A parallel group in the child adds its own level, `Work/Fan/a`. Names nest to any depth, so a grandchild is `Work/Inner/Talk`. Two nodes backed by one compiled child are told apart by their own node name (`First/Talk`, `Second/Talk`). |
| **What is forwarded** | **Everything** the child emits: its nodes' `NodeStart` / `NodeEnd` (content is the child's own node name, `Talk`), tokens, and its own `Retry` and `Error` chunks. Filtering to tokens was rejected: a retry inside the child re-streams tokens, and dropping its `Retry` chunk would leave a consumer with silent duplicates. |
| **Ordering** | The child's chunks sit between the subgraph node's own `NodeStart` and `NodeEnd`. A plain (non-streaming) child is now visible too, as `Work/Double` start/end pairs; previously only `Work` itself was. |
| **Plain `run`** | A subgraph whose child needs a `StreamContext` (a streaming node, a group with a streaming branch, or a streaming grandchild, to any depth) makes `run` / `RunAsync` fail at the subgraph node **before running any of the child** (`ExecutionAborted` / `Failed`), exactly as a group with a streaming branch does. A recovery policy can't mask it. A child that only *contains* plain branches or nodes is unaffected. |
| **Retry** | The parent's policy re-runs the whole child, and the engine emits a `Retry` chunk on the **subgraph node** (`Work`, content = the child's failure message) before each re-run, so a consumer can discard what it showed for the failed attempt. A retry the child does for itself is announced under the child's node (`Work/Fan`). |
| **Failure** | A child node that gives up emits an `Error` chunk under its own name, then the subgraph node emits one for itself. Neither gets a `NodeEnd`. |
| **Interrupts** | A child that freezes has streamed up to its boundary; the subgraph node has a `NodeStart` and no `NodeEnd` (`16-Nested-Interrupts.md`). A resume can be streamed with `resume_streaming` / `ResumeStreaming` (`16-Nested-Interrupts.md`, "Streaming a resume"), and then the child's chunks arrive named as above. A plain `resume` is not streamed, so the guard above is not applied when resuming into a subgraph that way: a child whose streaming ran *before* its boundary resumes normally, and one that reaches a streaming node after the boundary fails there (`NodeFailed` on the subgraph node), as a flat graph would. |
| **Backpressure** | The child writes to the run's one bounded channel (1024 chunks); a full channel pauses the child, nothing is dropped. |


## Implementation notes

- Rust: `rust/strata-core/src/subgraph.rs` (`run_subgraph`, the mapper aliases) and `StateGraph::add_subgraph` in `graph.rs`, which also holds `CompiledGraph::needs_stream`. The child's state needs `T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static`, the bounds of any running graph since `17-Async-Execution.md`.
- C#: `Graph/Subgraph.cs` (`SubgraphRunner`) and `StateGraph<TState>.AddSubgraph<TChild>`, plus `internal` members on `CompiledGraph<TState>` (`RunInlineAsync`, `ResumeInlineAsync`, `NeedsStream`).
- The child runs against a private store that discards everything it is given but remembers the last checkpoint. That is how the failure kind survives: `StrataError::NodeFailed` / `GraphExecutionResult.NodeFailed` carry the node, attempts and message but not whether the error was transient or fatal, and the child's `Failed` checkpoint records it in `metadata["error_kind"]`. Adding a field to `NodeFailed` would have broken every existing match on it, so the engine reads the kind back from the checkpoint instead.
- The subgraph is stored in the same map as fallible nodes. A subgraph whose child needs a stream is also recorded in `streaming_fallibles` (the set that parallel groups with a streaming branch already used, renamed from `streaming_groups`), which is what a plain run checks. The executor (`execute_from` / `ExecuteFromAsync`) already took an optional stream context; the child's run and resume (`RunInlineAsync`, `resume_checkpoint` / `ResumeInlineAsync`) pass one through. (Since `17-Async-Execution.md` every node kind lives in one map, with a `needs_stream` / `NeedsStream` flag in place of `streaming_fallibles`.)
- `StreamContext` gained a private scope: `for_node` / `ForNode` qualifies the name with it, and `scoped()` / `Scoped()` (crate/assembly-internal) makes the context a subgraph node hands its child. Nothing public changed shape.
- Tests: `rust/strata-core/tests/subgraphs.rs` and `dotnet/tests/Datalium.Strata.Tests/SubgraphTests.cs` mirror each other: mapping, the child's own routing and loops, child self-recovery, the retry / fallback / `Failed`-checkpoint interplay with kind propagation, nesting (including a failure two levels down), reuse, subgraph-as-interrupt-boundary, a supervisor routing to several subgraphs, streaming and panics. They were mutation-checked: making every child failure fatal fails the transient-retry tests (in both engines, then restored). The compile-time rejection of a streaming child, and its tests, were removed when streaming inside a subgraph was built; that has its own mirrored files, `rust/strata-core/tests/subgraph_streaming.rs` and `dotnet/tests/Datalium.Strata.Tests/SubgraphStreamingTests.cs` (15 each: naming, nesting, reuse, the plain-run guard, retry and failure chunks, backpressure, freeze and resume). Mutation-checked in both engines: handing the child an unscoped context fails nine of them, not registering a streaming child as needing a stream fails the two plain-run tests, and applying the guard on resume fails the two resume tests.

## Not built

- ~~Interrupt boundaries inside a subgraph.~~ Built since: see `16-Nested-Interrupts.md`. The original v1 rejected them at compile time.
- ~~Streaming inside a subgraph.~~ Built since: see [Streaming inside a subgraph](#streaming-inside-a-subgraph). The original v1 rejected it at compile time.
- ~~Streaming a resume.~~ Built since: `resume_streaming` / `ResumeStreaming`, see `16-Nested-Interrupts.md`.
- ~~**Structure in the graph definition.**~~ Built with SG-09 (`design/22-Graph-Structure-In-Definitions.md`): a subgraph node exports the child's whole definition under `graph`, recursively, and `strata graph validate` checks it with qualified names (`Remediate/Abort`). Node metadata is settable on every node. A checkpoint still carries no structure.
- **Child checkpoints, resume and replay.** There is no per-child thread. A failure inside a child is recoverable only by re-running the whole child.
- **Choosing the child at runtime**, ~~**or fanning out over a list**~~ (built 2026-10-06, SG-06, `27-Map-Fan-Out.md`: a map runs one compiled child per item, each at a level of its own as a subgraph's child does). The set of children is fixed when the parent is built. Routing *between* them is dynamic (ordinary conditional edges), but there is no "run this subgraph once per item" and no parallel group of subgraphs helper (a parallel-group branch is a closure, so it can call a child graph's `run` by hand, without any of the above).
- **Sharing state by reference.** Everything crosses the boundary by value through the two mappers; there is no shared or read-through state between parent and child.
- **A distinct error type.** A subgraph failure surfaces as the ordinary `NodeFailed` on the subgraph node, with the path in its message. There is no structured "failed at nested path" value to match on.
