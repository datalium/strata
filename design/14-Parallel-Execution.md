# Datalium Strata: Parallel Fan-Out / Fan-In

Before this, the executor was a single sequential loop: run a node, pick one outgoing edge, repeat. There was no way to run two things at once. This document specifies the parallel *group*, added while building demo 10 (`design/Scenarios/demo-projects-1.md`). It is a status-first record: what exists, how it behaves, and what was deliberately left out. Unlike `04-Error-Recovery.md`, there was no earlier blueprint to diverge from.

---

## The model

A **parallel group** is a graph node whose body is several named **branches** run concurrently, followed by a **reducer** that merges their results.

```rust
// Rust
let quotes = ParallelBranches::new()
    .branch("Acme",   |s: &Order| Ok(/* new Order */))
    .branch("Globex", |s: &Order| Ok(/* new Order */));

StateGraph::<Order>::new()
    .add_node("Prepare", prepare)
    .add_parallel_group("Gather", quotes, |entry: &Order, outputs: Vec<Order>| merge(entry, outputs))
    .add_node("Finish", finish)
    .add_edge("Prepare", "Gather")
    .add_edge("Gather", "Finish")
    .compile()?;
```

```csharp
// C#
var quotes = new ParallelBranches<Order>()
    .Branch("Acme",   s => /* new Order */)
    .Branch("Globex", s => /* new Order */);

new StateGraph<Order>()
    .AddNode("Prepare", Prepare)
    .AddParallelGroup("Gather", quotes, (entry, outputs) => Merge(entry, outputs))
    .AddNode("Finish", Finish)
    .AddEdge("Prepare", "Gather")
    .AddEdge("Gather", "Finish")
    .Compile();
```

To the rest of the graph the group is **one ordinary node**. It has ordinary edges in and out, can be an interrupt boundary, can carry a recovery policy, and shows up in the graph definition as a plain node. That is why nothing else had to change: STRATA-SPEC-004 validation, STRATA-SPEC-002 interruption and the `04-Error-Recovery.md` machinery all see a node and treat it like one.

## Semantics

| Aspect | Behaviour |
|---|---|
| **Input** | Every branch gets the state as it was when the group started, and nothing else. Branches cannot see each other. Rust enforces this with a `&S` borrow for a sync branch and a clone of its own for an async one; in C# it is a convention (build a new state, don't mutate the one you were handed). |
| **Concurrency** | An async branch (`async_branch` / an async `Branch` overload, `17-Async-Execution.md`) runs as a task and holds no thread while it waits. A sync branch still gets a thread of its own, because sync handlers typically block and borrowing thread-pool threads could serialise them on a small machine (Rust: tokio's blocking pool, `spawn_blocking`; C#: `TaskCreationOptions.LongRunning`). With no options there is no concurrency limit beyond the number of branches; `MaxConcurrency` / `with_max_concurrency` sets one (`26-Parallel-Group-Controls-And-Joins.md`). |
| **Merge** | The reducer receives the entry state plus every branch result **in declaration order, never completion order**. The merge is deterministic however the branches are scheduled. |
| **Waiting** | The group waits for every branch to succeed. The first branch to fail ends the wait: its siblings' token is cancelled and the group stops waiting for them (`17-Async-Execution.md`). An async sibling sees the cancellation; a sync one can't, so it runs on and its result is discarded (in Rust, one that hadn't started yet never starts). |
| **Failure** | A failing branch fails the group at once: **fatal** if any branch that had failed by then was fatal (retrying would be pointless), otherwise **transient**. The message names those branches in declaration order (`branch 'b': bad input`); a sibling that only stopped because it was cancelled is not named. (Until `17-Async-Execution.md` the group waited for every branch and named every failure.) |
| **Cancellation and timeouts** | A cancelled run cancels every branch, and the group's node ends the run `Cancelled`; branches are not persisted, so a resume re-runs the whole group. A timeout set on the group (`SetTimeout` / `set_timeout`) covers the whole group and is a transient failure of it. |
| **Recovery** | The group is registered as a *fallible node*, so its `RecoveryPolicy` applies to the group as a whole: retry re-runs **every** branch (branches must be safe to repeat), a fallback node runs on the pre-group state, and with neither, a `Failed` checkpoint is persisted. A failed group's partial results are discarded. |
| **Panics / bugs** | A Rust panic, or a C# exception that is not a `NodeException`, is not a recovery-managed failure. It propagates out of `run`/`RunAsync` exactly as it does from a plain node, and is not retried. |
| **Streaming** | Under `run_streaming` / `RunStreaming` the group emits the usual single `NodeStart` / `NodeEnd` pair, like any node. A branch added with `streaming_branch` / `StreamingBranch` can also push tokens; see [Streaming from branches](#streaming-from-branches). |

`.compile()` rejects a group with no branches or with two branches of the same name (`StrataError::CompilationFailed` in Rust, `InvalidOperationException` in C#). Re-adding a node name replaces a group, like it replaces any other node kind.

## Streaming from branches

A branch added with `streaming_branch(name, |state, ctx| ...)` (Rust) / `StreamingBranch(name, (state, context) => ...)` (C#) receives a `StreamContext` and can push tokens while it runs. Plain and streaming branches can be mixed in one group.

| Aspect | Behaviour |
|---|---|
| **Attribution** | Every chunk a branch emits carries `node_name = "<group>/<branch>"`, so a consumer can tell concurrent branches apart and still see which group they belong to. The group's own `NodeStart` / `NodeEnd` (named just `<group>`) still bracket them: branches are joined before `NodeEnd`. Branches are not nodes and get no lifecycle chunks of their own. |
| **Ordering** | One branch's chunks arrive in the order it emitted them. Chunks from *different* branches interleave however the branches are scheduled; there is no cross-branch ordering. (The *state* merge is still in declaration order.) |
| **Backpressure** | All branches write to the run's single bounded channel (1024 chunks). When it fills, every writer blocks until the consumer catches up; nothing is dropped. |
| **Plain `run`** | A group with a streaming branch needs a `StreamContext`, so `run` / `RunAsync` fails at the group *before running any branch* (`ExecutionAborted` / `Failed`), as it does at a streaming node. A recovery policy can't mask this, because it is not a node failure. |
| **Retry** | A retry re-runs every branch, so branches stream again. The engine emits a `Retry` chunk (node = the group, content = the failed attempt's message) before each re-run, so a consumer can discard what it showed for the failed attempt. The final give-up still emits one `Error` chunk, as before. `Retry` is emitted for *any* fallible node that is about to be retried under a streaming run, not only groups. |
| **In a subgraph** | A child containing a group with a streaming branch streams like any other child: its chunks are named `<subgraph>/<group>/<branch>`, and the parent's plain `run` refuses the subgraph node before running any of it (see `15-Subgraph-Composition.md`). |

Under the hood the engine's stored handler for a fallible node now takes the node's `StreamContext` (`None` / `null` under a plain run) as a second argument; `add_fallible_node` / `AddFallibleNode` wrap the user's one-argument closure, so nothing changes for existing callers. The group's runner retargets that context per branch with `for_node` / `ForNode`. Rust's `mpsc` channel was already multi-producer; .NET's bounded channel is now created with `SingleWriter = false`.

---

## Implementation notes

- Rust: `rust/strata-core/src/parallel.rs` (`ParallelBranches`, `run_branches`) and `StateGraph::add_parallel_group` in `graph.rs`. Since `17-Async-Execution.md`, `run_branches` is async and runs every branch in a `tokio::task::JoinSet` (`spawn` for an async branch, `spawn_blocking` for a sync one), aborting the rest on the first failure; it needs the bounds of any running graph, `S: Clone + Send + Sync + 'static`. A sync branch borrows the entry state through an `Arc`; an async one gets its own clone.
- C#: `Graph/ParallelBranches.cs` (`ParallelBranches<TState>`, internal `ParallelGroupRunner`) and `StateGraph<TState>.AddParallelGroup`. `ParallelBranches` is snapshotted at `AddParallelGroup`, so adding to it afterwards changes nothing.
- The group is stored in the same map as fallible nodes, plus a name → branch-names map used only for the compile-time checks. The executor (`execute_from` / `ExecuteFromAsync`) treats it as any fallible node. C#'s `ParallelGroupRunner.RunAsync` waits with `Task.WhenAny` and cancels a linked token on the first failure.
- Tests: `rust/strata-core/tests/parallel_groups.rs` and `dotnet/tests/Datalium.Strata.Tests/ParallelGroupTests.cs` mirror each other; so do `parallel_streaming.rs` and `ParallelStreamingTests.cs` for branch streaming and the `Retry` chunk. The concurrency test asserts that the *peak number of branches in flight* equals the branch count (it would be 1 if run sequentially), rather than asserting on wall-clock time.

## Not built

- **Branches are not graph nodes.** They are closures inside the group, so they can't have edges, their own recovery policies, or interrupt boundaries, and they don't appear in a checkpoint. Since SG-09 (`design/22-Graph-Structure-In-Definitions.md`) the graph definition lists a group's branches by name, with any metadata, and `strata graph validate` checks that there is at least one and that the names are unique; nothing else about a branch is exported. Fan-out expressed as multiple outgoing edges (`AddEdge` from one node to several) is not supported.
- **Per-branch lifecycle chunks and a cross-branch order.** A streaming branch's tokens are attributed to `<group>/<branch>`, but the branch itself gets no `NodeStart` / `NodeEnd`, and chunks from different branches have no defined relative order. A consumer that wants to know when one branch is done has to wait for the group's `NodeEnd`.
- ~~**Partial results**~~ Built 2026-10-05 (SG-07, `26-Parallel-Group-Controls-And-Joins.md`): `PartialResults.KeepSucceeded` and an outcomes reducer. **Per-branch recovery** (a retry or backoff per branch) is still not built: the group's policy retries the whole group.
- ~~**Cancellation** of siblings after a failure~~ and **timeouts** on the whole group: built since, see `17-Async-Execution.md`. ~~A **concurrency limit**, **timeouts per branch**, and opting out of sibling cancellation~~ built 2026-10-05 (SG-07, `26-Parallel-Group-Controls-And-Joins.md`), with join policies other than "every branch" (SG-08: first success, first matching, quorum).
- **Nested groups**: a branch can't hold a group (a map's child graph can). ~~**Dynamic fan-out** (the number of branches decided at runtime, e.g. map over a list).~~ Built 2026-10-06 (SG-06, `27-Map-Fan-Out.md`): `AddMap` / `add_map` runs a compiled child graph per item of a list read from the state, on this runner. A group's own branch set is still fixed when the graph is built.
- **Per-field reducers** (a derive/annotation declaring how each state field merges). The reducer is one function over whole states.
