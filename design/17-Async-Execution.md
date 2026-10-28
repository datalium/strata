# Datalium Strata: Async Execution, Node Context and Cancellation

Until now every node handler was synchronous. `RunAsync` / `ResumeAsync` wrapped a blocking loop in `Task.FromResult`, Rust's `run` / `resume` were plain functions, and a parallel group spent one OS thread per branch. A node doing I/O (an LLM call, an HTTP query, a database read) blocked a thread for as long as the call took. Nodes saw only their state, so they had no thread id, attempt number or history handle, and nothing could stop a run once it had started. This document specifies the three changes that close these gaps together, so the handler signature changes once and not three times:

- **SG-01, async handlers:** async overloads for every handler kind, and an executor that is genuinely asynchronous.
- **SG-02, node context:** a `NodeContext` handed to every async handler.
- **SG-03, cancellation:** a cancellation token per run, per-node timeouts, a run deadline, and a defined `cancelled` outcome with its own checkpoint status.

The SG numbers refer to the gap log of StrataOps (not public yet). Both engines get the capability in one change (strict parity, StrataOps ADR 0003). The APIs are idiomatic in each language, not identical.

It was written as a blueprint before implementation and is now built. "Implementation Status" at the end records where the build diverged from it.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Sync handlers | **Kept**, with the same signatures. They run off the executor, on the .NET thread pool (sync parallel branches keep a dedicated `LongRunning` thread each) or Rust's blocking pool (`spawn_blocking`), so a group's sync branches still run in parallel. |
| .NET compatibility | Source-compatible: every existing call compiles. Behaviour changes are listed under "Migration". |
| Rust compatibility | Breaking: `run` / `resume` become `async fn` and take `Arc<dyn CheckpointStore>`, and running a graph needs `S: Clone + Send + Sync + 'static` and a tokio runtime. |
| Async handler shape | .NET: `Func<TState, NodeContext, CancellationToken, ValueTask<TState>>`. Rust: `Fn(S, NodeContext) -> impl Future<Output = …> + Send + 'static`, with `S` passed by value (a clone; the engine keeps the committed copy). |
| Run-level inputs | A `RunOptions` (.NET, init-only properties) / `RunConfig` (Rust, builder) carrying history, services and deadline (and the token in Rust). New inputs go there, not into more parameters. |
| `NodeContext` | Thread id, node name, qualified node path, attempt, stream, history, services (plus the token in Rust). Extensible: a sealed class with init-only properties (.NET), private fields with accessors (Rust). Implements / builds on `IExecutionContext` / `ExecutionContext`. |
| Cancellation is | Cooperative. The engine signals the token, then **stops waiting**. It never waits for a handler it has given up on, sync or async (see "Abandoned work"). |
| Branch fails | **Siblings are cancelled.** The group fails as soon as one branch fails, instead of after every branch has settled. |
| Node timeout | `SetTimeout` / `set_timeout`, per attempt. A timeout is a **transient** failure for `RecoveryPolicy`. |
| Run deadline | `RunOptions.Deadline` / `RunConfig::with_deadline`, absolute. It ends the run as `Cancelled` with reason `deadline`, which is a cancellation, not a failure. |
| Cancelled run | A new `Cancelled` result plus a checkpoint with status **`cancelled`**, holding the last committed state, with `next_node` set to the node that was running. It is **resumable once, exactly like `failed`**. Recovery policies never see a cancellation. |
| Cancelled inside a subgraph | Nests like `failed`: the parent's `cancelled` checkpoint carries the child's `cancelled` one, and a resume re-enters the child where it stopped. |
| Wire format | `cancelled` added to `status`; `schema_version` **`1.2.0`** for any checkpoint that is, or nests, a `cancelled` one. Everything else is written exactly as before. |
| Not in scope | Async `ICheckpointStore` (SG-11), retry backoff (SG-04), checkpoint metadata (SG-14), tracing (SG-19). |

---

## Async handlers (SG-01)

Every handler kind gets an async counterpart. The sync ones stay, with the same signatures and semantics.

```csharp
// C#: async overloads beside the existing sync ones
new StateGraph<Order>()
    .AddNode("Load", async (s, ctx, ct) => s with { Customer = await crm.GetAsync(s.Id, ct) })
    .AddFallibleNode("Charge", async (s, ctx, ct) =>
    {
        try { return s with { Receipt = await gateway.ChargeAsync(s, ct) }; }
        catch (HttpRequestException ex) { throw NodeException.Transient(ex.Message, ex); }
    })
    .AddStreamingNode("Draft", async (s, ctx, ct) =>
    {
        await foreach (var token in llm.StreamAsync(s.Prompt, ct))
            await ctx.Stream!.EmitTokenAsync(token, ct);
        return s;
    })
    .AddParallelGroup("Gather", new ParallelBranches<Order>()
        .Branch("Acme", async (s, ctx, ct) => s with { Quote = await acme.QuoteAsync(s, ct) })
        .Branch("Globex", s => s with { Quote = GlobexQuote(s) }),            // sync, still fine
        Merge)
    .SetTimeout("Charge", TimeSpan.FromSeconds(10))
    .SetRecoveryPolicy("Charge", RecoveryPolicy.Retry(2));
```

```rust
// Rust: `add_async_*` beside the existing sync methods
StateGraph::<Order>::new()
    .add_async_node("Load", move |s: Order, ctx: NodeContext| {
        let crm = crm.clone();
        async move { Order { customer: crm.get(&s.id).await, ..s } }
    })
    .add_async_fallible_node("Charge", move |s: Order, ctx: NodeContext| {
        let gateway = gateway.clone();
        async move {
            let receipt = gateway.charge(&s).await.map_err(|e| NodeError::Transient(e.to_string()))?;
            Ok(Order { receipt: Some(receipt), ..s })
        }
    })
    .add_async_streaming_node("Draft", |s: Order, ctx: NodeContext| async move {
        let stream = ctx.stream().expect("a streaming node always has a stream");
        stream.emit_token("…").await.ok();
        s
    })
    .add_parallel_group("Gather", ParallelBranches::new()
        .async_branch("Acme", |s: Order, ctx: NodeContext| async move { Ok(s) })
        .branch("Globex", |s: &Order| Ok(s.clone())),                       // sync, still fine
        merge)
    .set_timeout("Charge", Duration::from_secs(10))
    .set_recovery_policy("Charge", RecoveryPolicy::retry(2));
```

| Kind | .NET (new overload) | Rust (new method) |
|---|---|---|
| Plain | `AddNode(name, Func<TState, NodeContext, CancellationToken, ValueTask<TState>>)` | `add_async_node(name, Fn(S, NodeContext) -> Fut<S>)` |
| Fallible | `AddFallibleNode(name, <same delegate>)`, which throws `NodeException` | `add_async_fallible_node(name, Fn(S, NodeContext) -> Fut<Result<S, NodeError>>)` |
| Streaming | `AddStreamingNode(name, <same delegate>)` | `add_async_streaming_node(name, Fn(S, NodeContext) -> Fut<S>)` |
| Branch | `ParallelBranches.Branch(name, <same delegate>)` | `ParallelBranches::async_branch(name, Fn(S, NodeContext) -> Fut<Result<S, NodeError>>)` |
| Streaming branch | `ParallelBranches.StreamingBranch(name, <same delegate>)` | `ParallelBranches::async_streaming_branch(name, …)` |

`Fut<T>` is shorthand for `impl Future<Output = T> + Send + 'static`. The engine boxes it. Rust has no overloading, and an axum-style "handler trait" that lets one `add_node` accept both shapes was rejected for its error messages and closure-inference problems, hence the `async_` names.

Rules that hold for every async kind:

- **The stream lives on the context.** `NodeContext.Stream` / `ctx.stream()` is the node's stream context whenever the run is streaming, for every kind, and null / `None` under a plain run. A *streaming* kind differs only in that a plain run refuses it before running it, as today, so inside one the stream is always present. Async handlers must use the async emits. Rust's `_blocking` emits panic on a runtime worker thread, as before.
- **State ownership (Rust).** An async handler receives `S` **by value**: a clone of the committed state. The engine keeps its own copy, which is what a retry, a fallback, a `failed` checkpoint or a `cancelled` checkpoint needs. That is why async methods, and running a graph at all, require `S: Clone`. A state that is expensive to clone can hold its large parts in `Arc`s. In .NET the handler gets the reference, as today: build a new state, don't mutate the one you were given.
- **Failures** are as today: `NodeException` / `Err(NodeError)` is recovery-managed in a fallible kind. Any other exception, or a panic, is a bug and propagates out of the run. A .NET `OperationCanceledException` is treated as cancellation **only if the token the engine gave the handler was cancelled**. Otherwise (for example `HttpClient`'s own timeout) it is an ordinary exception, so convert it to `NodeException.Transient` if it should be retried.
- **Edge conditions, reducers and subgraph `input` / `output` mappers stay synchronous** and run inline on the executor. They are expected to be cheap and do no I/O.

### The executor

- **.NET:** `RunAsync` / `ResumeAsync` become real `async` methods, using `ConfigureAwait(false)` throughout. `RunStreaming` / `ResumeStreaming` keep their `(Reader, Completion)` shape. The completion is now the async run started with `Task.Run`, and the channel is still completed in a `finally`.
- **Rust:** `run` / `resume` become `async fn`. Subgraph recursion goes through the boxed future each stored handler returns, so no further boxing is needed. `run_streaming` / `resume_streaming` keep their shape, but spawn the run with `tokio::spawn` and return a **`tokio::task::JoinHandle`** (you `.await` it instead of calling `join()`), so they must be called inside a runtime. A run must be polled inside a tokio runtime of either flavour, because the engine uses `spawn_blocking` and `tokio::time`.
- **One internal handler shape per engine.** Every kind is stored as the async fallible shape (state, `NodeContext`, nested resume) → `Step` / `AttemptError`. A sync handler is wrapped: .NET runs it with `Task.Run`, Rust with `spawn_blocking`, moving the state in and handing it back for `&S` handlers. A plain node is a fallible node that never returns `Err`. The executor then has one path, and timeouts and cancellation apply to every kind the same way.
- **Sync handlers run off the executor.** The handoff requires it for parallelism, and it also lets the engine stop waiting on a sync handler that has timed out. In Rust it is also the only place the `_blocking` emits are allowed. It costs one hop to the pool per sync node, which is negligible next to the I/O such a node does. (A Rust sync handler that does no I/O can opt out with `add_inline_node` / `add_inline_fallible_node`, giving up the abandonment and the `_blocking` emits: `design/39-Inline-Sync-Handlers.md`.)
- **The checkpoint store stays synchronous (SG-11 changes that; done in `design/19-Async-Queryable-Store.md`, so the hop described here is gone).** .NET calls it inline. Rust calls it through `spawn_blocking`, because the Postgres adapter uses the sync `postgres` crate, which panics when called from inside a tokio runtime. That is why Rust's `run` / `resume` now take `Arc<dyn CheckpointStore>` (`run_streaming` already did). An `Arc<SqliteCheckpointStore>` coerces at the call site. Saves are rare (a boundary, a failure, a cancellation or a resume), so the hop costs little.
- **Rust history in async handlers.** `HistoryStore` is still synchronous, and its Postgres adapter has the same restriction. Until SG-11, an async handler calls it through `tokio::task::spawn_blocking`. (SG-11 made it async: `design/19-Async-Queryable-Store.md`.) The .NET `IHistoryStore` is already async.

---

## Node context (SG-02)

```csharp
public sealed class NodeContext : IExecutionContext
{
    public required string ThreadId { get; init; }   // the outermost run's thread, at every depth
    public required string Node { get; init; }       // "Draft"
    public required string NodePath { get; init; }   // "Research/Draft", "Gather/Acme"
    public int Attempt { get; init; } = 1;
    public IStreamContext? Stream { get; init; }
    public IHistoryStore? History { get; init; }
    public IServiceProvider? Services { get; init; }

    // For code that takes an IExecutionContext; throws if RunOptions supplied no history.
    IHistoryStore IExecutionContext.History => History ?? throw new InvalidOperationException(…);
}
```

```rust
#[derive(Clone)]
pub struct NodeContext { /* private */ }

impl NodeContext {
    pub fn thread_id(&self) -> &str;
    pub fn node(&self) -> &str;
    pub fn node_path(&self) -> &str;
    pub fn attempt(&self) -> u32;
    pub fn stream(&self) -> Option<&StreamContext>;
    pub fn history(&self) -> Option<Arc<dyn HistoryStore>>;
    pub fn service<T: Send + Sync + 'static>(&self) -> Option<Arc<T>>;
    pub fn cancellation(&self) -> &CancellationToken;
    pub fn execution_context(&self) -> Option<ExecutionContext>;   // thread id + history, for existing code

    pub fn new(thread_id: impl Into<String>, node: impl Into<String>) -> Self;  // for unit-testing a handler
    // + with_attempt / with_stream / with_history / with_service / with_cancellation, for the same purpose
}
```

| Field | Meaning |
|---|---|
| **Thread id** | The thread id the caller passed to the outermost `run` / `resume`, **at every depth**. Inside a subgraph it is *not* the child's internal thread id (the subgraph node's name, which only exists to label nested checkpoints). |
| **Node / path** | `Node` is the local name. `NodePath` uses the streaming names: `"<subgraph>/<node>"` to any depth, and `"<group>/<branch>"` for a branch (`"Work/Gather/Acme"` combines them). It is the same string a streaming chunk carries. |
| **Attempt** | 1-based, counting this node's retries under its `RecoveryPolicy`. A branch reports its group's attempt, since a group retry re-runs every branch. A node inside a subgraph counts its own retries: a parent retry re-runs the child, whose nodes start again at 1. For an idempotency key, use `(ThreadId, NodePath)` and keep `Attempt` for logs. Retries of one logical action should share a key. |
| **Stream** | See "Async handlers". |
| **History, services** | Whatever `RunOptions` / `RunConfig` supplied, unchanged at every depth. Rust services are a type map: `RunConfig::with_service(Arc::new(MyClient))` stores it, and `ctx.service::<MyClient>()` reads it back. |
| **Cancellation (Rust)** | The node's token: cancelled when the run is cancelled, its deadline passes, the node times out, or (for a branch) a sibling fails. In .NET the same token is the handler's `CancellationToken` parameter. |

**Extensibility.** SG-14 (checkpoint metadata) and SG-19 (`Activity` / tracing) will add fields. In .NET a new optional `init` property is non-breaking, which is why this isn't a positional record. In Rust a new accessor is non-breaking because the fields are private. `NodeContext` is cheap to build per node: the run-level parts (thread id, history, services, root token) are shared behind one `Arc`, and only the path, attempt, stream and node token are per node.

Sync handlers don't get a context. Their signatures are unchanged by design.

### Run options

```csharp
public sealed class RunOptions
{
    public IHistoryStore? History { get; init; }
    public IServiceProvider? Services { get; init; }
    public DateTimeOffset? Deadline { get; init; }
}

// Every entry point gets these two shapes; the existing calls bind to the first.
Task<GraphExecutionResult<TState>> RunAsync(string threadId, TState initialState, ICheckpointStore store,
    CancellationToken cancellationToken = default);
Task<GraphExecutionResult<TState>> RunAsync(string threadId, TState initialState, ICheckpointStore store,
    RunOptions options, CancellationToken cancellationToken = default);
// Likewise ResumeAsync (HumanSignal and LevelSignals), RunStreaming, ResumeStreaming.
```

```rust
#[derive(Clone, Default)]
pub struct RunConfig { /* private */ }

impl RunConfig {
    pub fn with_cancellation(self, token: CancellationToken) -> Self;
    pub fn with_deadline(self, deadline: std::time::Instant) -> Self;
    pub fn with_history(self, history: Arc<dyn HistoryStore>) -> Self;
    pub fn with_service<T: Send + Sync + 'static>(self, service: Arc<T>) -> Self;
}

// Every entry point gets a `_with` form taking a RunConfig:
graph.run(thread_id, state, store).await;
graph.run_with(thread_id, state, store, RunConfig::default().with_cancellation(token)).await;
// resume / resume_with, run_streaming / run_streaming_with, resume_streaming / resume_streaming_with
```

`datalium_strata::CancellationToken` re-exports `tokio_util::sync::CancellationToken`, so callers don't add the dependency themselves. A deadline is **absolute**, so an SLA that spans a freeze and a resume can be enforced by passing the same deadline to both calls.

---

## Cancellation, timeouts and deadlines (SG-03)

### Tokens

There is one token tree per run. The run's root token is cancelled by the caller's token or when the deadline passes. Each node attempt gets a child token, which is also cancelled when the node's timeout elapses. Each branch gets a child of the group's token, which is also cancelled when a sibling fails. A subgraph's child run uses the subgraph node's token as its own root, so cancellation reaches every depth.

### Semantics

| Aspect | Behaviour |
|---|---|
| **Where it is noticed** | Before each node starts, and while a node runs: the engine races the handler against its token. A node that has completed is committed. A cancellation that arrives during an interrupt boundary's save lets the save finish, and the run returns `Interrupted`. |
| **Outcome** | `Cancelled` (below), plus a `cancelled` checkpoint. `suspended_node` and `next_node` are the node that was running, or about to start, and `frozen_state` is the last committed state, which is the state that node started with. Metadata: `cancel_reason` is `requested` or `deadline`. |
| **Not a failure** | A cancellation never reaches a `RecoveryPolicy`: nothing is retried, no fallback runs, and no `Error` chunk is emitted. A cancellation between two retry attempts ends the run `Cancelled` at that node, and a resume starts it again at attempt 1. |
| **Run vs node** | If the run's token fires, the outcome is a cancellation, even if the node's timeout also elapsed. |
| **Already cancelled** | A run or resume started with a cancelled token (or a passed deadline) runs nothing and writes a `cancelled` checkpoint at the node it would have run. For a resume, that comes after stamping the old checkpoint `resumed_at`, as any resume does. A resume of a nested thread still re-enters each subgraph level without running anything, so the `cancelled` checkpoint lands at the innermost node, holding the state with the human's field overrides applied: what was answered is kept. |
| **Dropping the Rust future** | This is *not* a cancellation. Dropping the future of `graph.run(…)` (for example with `tokio::time::timeout` around it) stops the run where it is and persists nothing. Use the token or the deadline for a defined outcome. |
| **.NET exceptions** | `RunAsync` returns `Cancelled`. It does not throw `OperationCanceledException` for its own token. A cancelled run is an outcome, like an interrupted one. |

```csharp
public sealed record Cancelled(
    string ThreadId, string Node, CancelReason Reason, TState State, IReadOnlyList<string> StoppedAt)
    : GraphExecutionResult<TState>;          // not a subtype of Failed: a cancellation isn't a failure

public enum CancelReason { Requested, Deadline }
```

```rust
GraphExecutionResult::Cancelled { thread_id, node, reason: CancelReason, state: S, stopped_at: Vec<String> }
```

`StoppedAt` / `stopped_at` is the same per-level path as `waiting_at`, for example `["Work", "Send"]` when a subgraph's child was stopped at `Send`.

### Node timeouts

`SetTimeout(node, TimeSpan)` / `set_timeout(node, Duration)` applies to any node kind, **per attempt**: each retry gets a fresh timeout. A non-positive duration is rejected immediately, and `Compile` / `compile()` rejects a timeout on a node that doesn't exist. An elapsed timeout is a **transient** failure with the message `node '<name>' timed out after <n> ms`, which is the same text in both engines. A fallible node's policy retries it or falls back. A plain or streaming node (which can't carry a policy) ends the run the way a fallible node with the default policy does: a `failed` checkpoint and `NodeFailed`. On a parallel group the timeout covers the whole group. On a subgraph node it covers the whole child (see "Subgraphs"). Per-branch timeouts are SG-07, built since (`26-Parallel-Group-Controls-And-Joins.md`).

### Abandoned work

The engine stops waiting the moment a token fires. **It never waits for a handler it has given up on.** Otherwise a timeout would mean nothing for a sync handler, or for an async one that ignores its token.

- A Rust async handler's future is dropped, which is Rust's own cancellation.
- A .NET async handler is told through its token, and the engine stops awaiting it (`WaitAsync`). If it ignores the token it runs on unobserved. The engine observes its eventual exception, so it doesn't surface as `UnobservedTaskException`.
- A sync handler can't see a token. It keeps running on its pool thread, and its result is discarded.

**The consequence.** A retry, a fallback, or the caller's next step can start while an abandoned attempt is still running. A handler with side effects that must not overlap should be async and honour its token. The stream ends when the run ends, even if an abandoned handler is still emitting: its later emits fail (`ChannelClosed` / `ChannelClosedException`). In .NET, a handler waiting to write to a full channel when its token fires sees `OperationCanceledException`, even when the run has already ended and closed the channel. That has been so since 2026-10-24; before, it usually saw `ChannelClosedException`. In Rust this means the engine closes the channel itself instead of relying on every `Sender` clone being dropped, because an abandoned blocking task still holds one.

The one exception is a **subgraph node**. It is a graph, not a handler, so on cancellation the engine tells the child (through its token) and awaits it. The child returns promptly, because it abandons its own nodes by the same rule, and it hands back the checkpoint described below.

### Parallel groups

| Aspect | Before | Now |
|---|---|---|
| **Concurrency** | A dedicated thread per branch, joined with a blocking wait. | Async branches are tasks (.NET) or spawned futures in a `JoinSet` (Rust): no thread each. Sync branches keep their own thread (.NET `LongRunning`, Rust `spawn_blocking`), so they still run in parallel. |
| **A branch fails** | Every sibling ran to completion, then the group failed, naming every failing branch. | The group's token is cancelled at once and the group fails straight away. The message names the branches that had failed by then (usually one). The kind is still fatal if any of them was fatal, and transient otherwise. |
| **Run cancelled** | Not possible. | Every branch is cancelled, and the group's node ends the run `Cancelled`. Branches are not persisted, so a resume re-runs the whole group. |
| **Merge order** | Declaration order. | Unchanged. |
| **Panics / non-`NodeException`** | Re-raised after all branches settled. | Re-raised as soon as it is seen. Siblings are cancelled first. |

Cancelling siblings is the default. Keeping partial results, opting out of sibling cancellation and the other per-group controls are SG-07, built since (`26-Parallel-Group-Controls-And-Joins.md`).

### Subgraphs

The child run inherits the run scope (thread id, history, services, deadline) and uses the subgraph node's token as its root.

| Cause | What the child does | What the parent records |
|---|---|---|
| **Run cancelled / deadline** | It stops at its running node and returns `Cancelled` with its own `cancelled` checkpoint. | A `cancelled` checkpoint at the subgraph node, holding the state the node started with and **nesting the child's `cancelled` checkpoint**. This mirrors how `failed` nests a failed child. The result's `StoppedAt` is `["Work", "Send"]`. |
| **The subgraph node's timeout** | The same: from inside, the child can't tell, so it stops and returns its checkpoint, with `cancel_reason: "timeout"`. | A **transient failure** of the subgraph node, carrying the child's `cancelled` checkpoint the way `FailedChild` / `AttemptError.child` carries a failed one. If the policy has nothing left, the `failed` checkpoint nests that `cancelled` child, so a resume re-enters the child at the node it was stopped at, **and a human's earlier answer is not asked for again**. |
| **The child's own node times out** | It is a failure *inside* the child, handled by the child's policy. If that runs out, it fails as today (a transient child failure). | As today. |

This is the one place a checkpoint nests one of a **different status**: a `failed` parent may nest a `cancelled` child (because the parent's timeout stopped it). A resume treats a nested `cancelled` child exactly like a nested `failed` one.

### Resuming a cancelled thread

`resume` / `ResumeAsync` accept a `cancelled` checkpoint exactly as they accept a `failed` one (`16-Nested-Interrupts.md`, "Resuming a failed thread"):

- It re-enters `next_node` (the node that was stopped) with `frozen_state`, after applying the signal's `field_overrides`.
- It is resumable **once**: the checkpoint is stamped `resumed_at`, and a second resume is refused.
- A run that stops again writes a new checkpoint (cancelled, failed or interrupted), which can be resumed in turn.
- For a nested one, it re-enters the child where it stopped. `input` is not called again, and `LevelSignals` work as for any nested checkpoint.

A `completed` checkpoint is still the only status that is refused.

### Streaming

The node that was running when the run was cancelled gets no `NodeEnd`, as with a failed node. There is no new chunk type, because the completion's `Cancelled` result says why the stream ended. A timed-out attempt that will be retried emits `Retry` with the timeout message, and a final give-up emits `Error`, as for any failure.

**The engine's own chunks don't wait past cancellation** (since 2026-10-24, both engines). `NodeEnd`, `Retry` and `Error` are written at once when the channel has room. When it is full, the engine waits for room only until the level's token fires, and then drops the chunk; `NodeStart` already worked this way. Before, a consumer that stopped reading and then cancelled the run could leave the run blocked forever on its own chunk, its completion never finishing. An `await foreach` over `ReadAllAsync(token)` with the run's own token does exactly that. A consumer that stops reading **without** cancelling still pauses the run: that is backpressure, by design (STRATA-SPEC-001).

---

## Wire format

`schema/checkpoint.schema.json`:

- **`status`** gains **`"cancelled"`**. `suspended_node` and `next_node` are both the node that was stopped. `metadata.cancel_reason` is `"requested"`, `"deadline"` or (only on a checkpoint nested in a `failed` one) `"timeout"`.
- **`schema_version`** gains **`"1.2.0"`**. It is written **only** for a checkpoint whose status is `cancelled`, or which nests a `cancelled` checkpoint at any depth. Everything else keeps the version it has today (`1.0.0`, or `1.1.0` with `nested`), so existing fixtures and logs are untouched. The schema's `allOf` requires `1.2.0` with `cancelled`. The rule that a checkpoint with `nested` has `status` `interrupted` or `failed` widens to `interrupted`, `failed` or `cancelled`.
- Both engines' `is_supported_checkpoint_version` / `CheckpointSchema.IsSupported` accept `1.2.0`. A runtime from before this change already refuses to resume an unknown version, so it can't half-resume a cancelled thread.

`schema/fixtures/checkpoint-lifecycle/` gains two fixtures. As with `failed.json` and `nested-failed.json`, each is **produced** by a live run in both engines (compared at every level, timestamps aside) and **resumed** by both:

- `cancelled.json`: a flat run cancelled while a node was running (`cancel_reason: "requested"`).
- `nested-cancelled.json`: a run cancelled while a subgraph's child was at `Send`, with the parent at `Work` (`1.2.0` at both levels).

### Storage and the CLI

`status` is `TEXT` in every SQL layout, so there is **no DDL change and no migration**. The status string mappings gain `"cancelled"` in `strata-memory` (SQLite, Postgres), `EfCoreCheckpointStore`, `PostgresCheckpointStore` (.NET), the .NET interop `Schema.Checkpoint` enum and the CLI. Redis and in-memory hold the JSON. Each store's contract test round-trips a cancelled checkpoint, flat and nested, and the SQLite row fixtures gain `cancelled.row.json`.

The CLI shows a cancelled thread the way it shows a failed one:

- `thread list`: status `cancelled`, and `--status cancelled` filters for it.
- `thread inspect`: `Cancelled at: Work/Send` and `Reason: deadline`.
- `checkpoint log`: `[cancelled] node=Work/Send`.
- `checkpoint replay`: `Cancelled inside: Work/Send`.

The `--json` output carries the stored fields as they are. `thread inspect --json` also gains `metadata` (the parsed object), which carries `cancel_reason` here, and `error` / `error_kind` / `attempts` for a failed thread.

---

## Migration

**.NET** (every existing call compiles, but there are behaviour changes):

- Sync handlers run on thread-pool threads, not on the thread that called `RunAsync`, and `RunAsync` no longer completes synchronously. Code that relied on thread identity or `[ThreadStatic]` state changes behaviour; `AsyncLocal` still flows.
- A failing branch now cancels its siblings, and the group fails as soon as one branch fails. Sync siblings are abandoned, not awaited. The failure message names only the branches that had failed by then.
- `RunAsync` / `ResumeAsync` can return the new `Cancelled`. A `switch` that only knows `Completed` / `Interrupted` / `Failed` falls through for it. `Cancelled` deliberately isn't a `Failed`.
- `ResumeAsync` accepts a `cancelled` checkpoint.

**Rust** (breaking):

- `run` / `resume` are `async fn`: add `.await`, and call them inside a tokio runtime (for example `#[tokio::main]` / `#[tokio::test]`).
- They take `Arc<dyn CheckpointStore>`, not `&dyn CheckpointStore`.
- Running a graph requires `S: Clone + Send + Sync + Serialize + DeserializeOwned + 'static`.
- `run_streaming` / `resume_streaming` return a `tokio::task::JoinHandle`: `.await` it instead of calling `join()`.
- `GraphExecutionResult` has a `Cancelled` variant, so exhaustive matches need an arm for it.
- Sync handlers are unchanged. Sync streaming nodes keep using the `_blocking` emits, which still work because those nodes run on the blocking pool.
- `strata-core` gains the tokio features `rt`, `time` and `macros`, plus a `tokio-util` dependency.

**Downstream.**

- `strata-demos-rust` needs the Rust changes above.
- `strata-demos-dotnet` should keep compiling.
- StrataOps bumps `strata.lock` in its own PR and removes its `SyncBridge`, its `AsyncLocal` context and its per-call timeouts.

---

## Tests

Mirrored per engine, as for every feature so far:

- `async_execution.rs` / `AsyncExecutionTests.cs`: each async kind; sync and async mixed in one graph and one group; a sync group still peaks at N branches in flight. Many concurrent runs of async branches complete on a two-worker runtime (Rust) or with a bounded thread count (.NET), which proves branches no longer hold a thread each. Also: panics and non-`NodeException` errors still propagate, and subgraph recursion works.
- `node_context.rs` / `NodeContextTests.cs`: thread id at depth, paths for subgraph nodes, branches and both together, attempts across retries, history and services passed through, and the stream present exactly when streaming.
- `cancellation.rs` / `CancellationTests.cs`: cancel before, during and between nodes; the deadline; timeouts per kind (retried, fallen back, failed); a cancelled node never reaching its recovery policy; sibling cancellation; abandoned sync work; cancelled checkpoints, flat and nested, a subgraph timeout nesting a `cancelled` child, and resuming each; resume once only; streaming end-of-stream with no `NodeEnd`.
- The fixture parity tests for `cancelled.json` and `nested-cancelled.json`, the store contracts, and the CLI test seeding both.
- Every existing test, ported to the async APIs in Rust and unchanged in .NET except where the behaviour above changed. These are the "all branches run to completion" tests in the group suites, which now assert cancellation.

---

## Implementation Status

Built 2026-10-01 in both engines, in one change, as specified above. These are the differences and details.

**Where it diverged, and why:**

- **A queued Rust sync branch may never start.** When a group gives up (a sibling failed, the run was cancelled, or the group timed out), `JoinSet::abort_all` cancels any `spawn_blocking` task that hasn't started yet, so that branch never runs. A branch that is already running is abandoned, as specified. .NET gives every sync branch its own `LongRunning` thread up front, so all of them start. Neither engine waits for a sync branch it gave up on, and not starting work nobody will use is the better outcome, so the engines were left to differ. One Rust test counted a healthy sibling's calls across group retries; it now asserts a range (`parallel_groups.rs`, `a_transient_failure_retries_the_whole_group`).
- **A zero Rust timeout is rejected at `compile()`**, not by `set_timeout`. The Rust builder returns `Self` and can't fail mid-chain, and it already reports its other configuration errors at compile time. .NET's `SetTimeout` throws `ArgumentOutOfRangeException` at once. Both engines reject a timeout on a node that doesn't exist at compile time, with the same message.
- **A nested resume started already cancelled** re-enters every level before stopping (see "Already cancelled"). Stopping at the outer subgraph node instead would have nested the old interrupted child under a `cancelled` parent and lost the human's overrides.
- **The CLI's `checkpoint log` format** is `[cancelled] node=…`, the format it already used for every status, not `status=cancelled` as this document first said.
- **Two .NET tests have Rust stand-ins rather than twins.** A Rust plain node can't return an error, and Rust has no `OperationCanceledException`. So `A_NodeException_From_An_Async_Plain_Node_Is_Not_Recovery_Managed` and `An_OperationCanceledException_The_Engine_Did_Not_Cause_Is_A_Bug` are replaced in Rust by `a_panic_in_an_async_node_unwinds_through_the_run` and `dropping_the_run_future_is_not_a_cancellation_and_persists_nothing`.
- **No test races run cancellation against a timeout**, because no such test can be deterministic. The rule is one branch in `RunAttemptsAsync` / `run_attempts`, which checks the level's token before treating a stop as a timeout. `Cancellation_Is_Not_A_Failure…` covers a cancelled node that has a recovery policy.

**How it was built:**

- **.NET:**
  - `Graph/NodeContext.cs` and `Graph/RunOptions.cs` hold the public types.
  - `Graph/Execution.cs` holds the plumbing: the stored `NodeHandler` shape, `NodeEntry`, `AttemptInputs`, the `RunScope` / `LevelScope` / `RootScope` token tree, and the sync-to-async wrappers.
  - The executor in `CompiledGraph.Interruption.cs` has one path for every node kind (`RunAttemptsAsync`, `AttemptAsync`, `CancelAt`).
  - `ParallelGroupRunner.RunAsync` waits with `Task.WhenAny` and cancels a linked token on the first failure.
  - `SubgraphRunner.RunAsync` runs the child at a `LevelScope` rooted at the attempt's token.
  - `GraphExecutionResult.Cancelled` and `CancelReason` sit beside the other results.
- **Rust:**
  - `context.rs` holds `NodeContext`, `RunConfig` and the run, level and root scopes. A deadline is a spawned timer that the root aborts when it is dropped.
  - `node.rs` holds the boxed-future `NodeHandler`, `NodeEntry` and `on_blocking_pool`.
  - `execute_from` and `resume_checkpoint` return a boxed `Send` future, which breaks the subgraph recursion.
  - `attempt` races the handler against its token and timeout with `tokio::select!` and drops it; a subgraph is awaited instead.
  - `run_branches` uses a `JoinSet`.
  - `CancellationToken` is re-exported from `tokio-util`.
- **The Rust stream closes with the run.** `StreamContext` keeps its sender in one shared slot, which the engine empties when a streaming run ends (`close`). Otherwise an abandoned blocking handler, which still holds a clone of its context, would keep the receiver open.
- **Rust stores are called on the blocking pool**, as specified. This was confirmed against a live Postgres: calling `save` directly from the engine makes the sync `postgres` crate panic ("Cannot start a runtime from within a runtime"). The store contract suite (`checkpoint_store_contract.rs`) drives the engine through a small local runtime, so that its own direct store calls stay outside a runtime.
- **Status mapping:** `cancelled` was added to every SQL store in both engines, the .NET interop `Schema.CheckpointStatus` and the CLI. Schema `1.2.0` is chosen by `checkpoint_version_for` (Rust) / `CheckpointSchema.For` (.NET).
- **Examples:** both example agents' `ClassifyTicket` became an async node. That removed their sync-over-async bridges: `block_in_place` + `block_on` in Rust, `GetAwaiter().GetResult()` in .NET.

**Tests:**

- **New suites:**
  - `AsyncExecutionTests.cs` / `async_execution.rs`: 11 each.
  - `NodeContextTests.cs` / `node_context.rs`: 8 each.
  - `CancellationTests.cs` (20) / `cancellation.rs` (25). The Rust file has the 20 twins plus the 5 shared-fixture tests, which .NET keeps in `CancelledCheckpointFixtureParityTests.cs`.
- **Storage and CLI:**
  - Every SQL writer is tested against the row fixture `checkpoint-log/cancelled.row.json`: both SQLite stores, both Postgres stores and the CLI.
  - The store contracts round-trip a flat cancelled checkpoint, a nested one, and a `failed` parent nesting a `cancelled` child.
  - The CLI test seeds both lifecycle fixtures and drives every command.
- **Totals when this landed:** .NET 294 (all backends live), `rust/` 268, `cli/` 35, `examples/rust-agent` 1.
- **Mutation checks, in both engines:**
  - Awaiting a handler instead of abandoning it fails 2 tests in .NET and 10 in Rust. Every Rust run in `cancellation.rs` has a time limit, so it fails rather than hangs.
  - Dropping the child's checkpoint when a subgraph is cancelled fails 3 in each.
  - Not cancelling a failed branch's siblings fails 3 in .NET and 1 in Rust.

## Not built (deliberately out of scope)

- **An async checkpoint store and an async Rust history store** (SG-11). Stores are called synchronously, through `spawn_blocking` in Rust. Since built: `design/19-Async-Queryable-Store.md`.
- ~~**Retry backoff and `Retry-After`** (SG-04). This change adds the async wait it needs.~~ Built 2026-10-05 (`25-Retry-Backoff.md`), with the wait on the run's clock: .NET's `RunOptions.TimeProvider` now also drives node timeouts and the deadline.
- **Checkpoint metadata on every checkpoint** (SG-14) **and tracing** (SG-19). `NodeContext` leaves room for both.
- ~~**Per-group controls** (SG-07): keeping partial results, per-branch timeouts, a concurrency limit, and opting out of sibling cancellation.~~ Built 2026-10-05 (`26-Parallel-Group-Controls-And-Joins.md`), with join policies (SG-08).
- **A `Cancelled` stream chunk.** The completion result carries the outcome.
