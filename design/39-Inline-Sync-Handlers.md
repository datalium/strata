# Datalium Strata: Inline Sync Handlers (Rust)

A sync node handler in the Rust engine runs on tokio's blocking pool (`design/17-Async-Execution.md`). That costs a thread hand-off each way: about 1.1 µs a node when both threads stay awake and 4 to 5.5 µs when one of them sleeps, so the sync benchmarks vary from run to run. An async node costs 0.4 µs. `design/38-Rust-Executor-Overhead.md` left the question open (its review decision 1): should a sync handler be able to run on the executor instead, giving up what the blocking pool is for?

This document answers it. It was written before implementation and reviewed (every recommendation in "Review decisions" was accepted); "Implementation Status" at the end records the build and the figures measured on it.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Can a sync handler run on the executor today? | **Yes, already.** `add_async_node(name, \|s, _ctx\| async move { f(s) })` is a sync handler on the executor: its future is ready the first time it is polled. It costs what an async node costs. Nothing in the engine has to change for the speed. |
| Then what is built? | **Two methods that name it:** `add_inline_node` and `add_inline_fallible_node`, with the same handler signatures as `add_node` and `add_fallible_node`, so moving a node is a rename. Their documentation carries the contract, and `.compile()` refuses a timeout on one. |
| Which handler kinds? | **Plain and fallible nodes.** No inline streaming node (its emits could only block or drop) and no inline branch (a branch is already a task; see "Branches"). |
| What does an inline handler give up? | **Being abandoned, and the right to block.** A timeout can't fire during it, so `.compile()` refuses one. A cancellation or deadline takes effect when it returns: its result is kept, as if the token had fired just after the node. Blocking is documented, not detected. It gets no stream context, so it can't reach a `_blocking` emit. |
| .NET? | **Rust only.** An inline node is a plain or fallible node to everything outside the Rust executor: definitions, checkpoints, events and spans don't change. |
| What is it worth? | **A sync node costs 4.4 to 5.2 µs, an inline one about 0.4 µs, and inline doesn't vary from run to run.** Throughput rises from about 0.43 M to about 11 M transitions a second at 64 runs (five runs each; see "Expected gain"). Only for small CPU-only handlers. |

---

## What the blocking pool is for

`design/17` runs sync handlers on the blocking pool for three reasons:

1. **A handler can't hold up a runtime worker.** A sync handler may do I/O or wait on a lock. On the pool it occupies a thread of its own; on a worker it stops every task queued behind it (on a current-thread runtime, every task there is).
2. **The engine can abandon it.** On a timeout or a cancellation the engine stops waiting for the `spawn_blocking` task and drops its handle. The handler runs on, and its result is discarded. On the executor there is nothing to stop waiting for: the handler is the code running.
3. **Its `_blocking` emits work there.** `StreamContext::emit_token_blocking` calls tokio's `blocking_send`, which panics on a runtime thread.

None of these is a cost to a handler that does a few microseconds of computation and returns: no I/O, no lock contention, nothing to time out. For those, the pool is two thread hand-offs a node for nothing.

## The shape

### Two methods

```rust
StateGraph::<Work>::new()
    .add_inline_node("normalize", |work: Work| normalize(work))
    .add_inline_fallible_node("parse", |work: &Work| parse(work))
```

- `add_inline_node(name, Fn(S) -> S)`: a plain node.
- `add_inline_fallible_node(name, Fn(&S) -> Result<S, NodeError>)`: a fallible node; its `RecoveryPolicy` applies as to any fallible node (retries, backoff, fallback).

Each stores its handler in the one internal shape (`NodeHandler`), as a future that runs the handler when first polled, inside its attempt's span and the executor's panic catch. One allocation per node (the boxed future), like an async node; a sync node makes two.

**Why methods, and not the alternatives:**

- **An option on the node** (`set_inline("parse")`, beside `set_timeout`): it separates the promise ("this doesn't block") from the handler that has to keep it, and it would have to be refused on every kind it doesn't fit (async nodes, streaming nodes, groups, subgraphs, maps, waits). The person writing the handler is the one who knows.
- **A compile option** (every sync node of the graph inline): one handler that blocks stalls a worker, and the option is set by whoever compiles the graph, who may not have written every node. It would also have to say whether a subgraph's child follows its parent.
- **Only documenting the async idiom**: it is what the methods do, and it stays valid. But nothing tells the engine the node is meant to be inline, so a timeout set on it is silently never honoured, and the idiom doesn't say at the call site what the handler promises. The methods cost two short functions.

### Branches

No `inline_branch`. A group's branch is already a task in a `JoinSet` (about 1.5 µs and 11 allocations, most of them tokio's), so the hand-off is a smaller share of a branch than of a node. Sync branches today run in parallel on threads of their own; inline ones would run one after another on a current-thread runtime and share the workers on a multi-thread one, which is a different promise to make in a group. The async idiom (`async_branch(name, |s, _ctx| async move { f(&s) })`) is there for a branch that wants it. If a sync-branch benchmark later shows a gain worth having, it can be added the same way.

### Streaming

No inline streaming node. A sync streaming handler emits with the `_blocking` emits, which panic on the executor. The only emit that doesn't block is one that drops the token or grows the buffer when the channel is full, and STRATA-SPEC-001 says backpressure pauses the node rather than dropping. A streaming handler that wants to avoid the pool is an async one, whose emits wait.

## What an inline handler gives up

| | On the blocking pool | Inline |
|---|---|---|
| Its node's timeout | Fires; the handler is abandoned and the attempt fails as timed out (transient) | **Refused by `.compile()`** |
| The run is cancelled, or its deadline passes, while it runs | The engine stops waiting at once; the handler's result is discarded; `cancelled` checkpoint at that node | **Takes effect when it returns:** its result is committed, and the run goes on exactly as if the token had fired between this node and the next (normally a `cancelled` checkpoint at the next node) |
| A subgraph's timeout cancels the child while the handler runs | As a cancellation | As a cancellation: the child stops after this node |
| It blocks | Holds a pool thread | **Holds a runtime worker** (on a current-thread runtime, the whole runtime: the lease renewal, the observer queue and the stream's reader wait too). Not detected |
| `_blocking` emits | Allowed | **Unreachable:** the handler signature has no stream context, and there is no inline streaming node |
| A panic | Resumes on the executor | Caught the same way (it runs inside the attempt's panic catch) |
| Its span, events, metrics, recovery policy | | The same |

How each is held:

- **The timeout: refused at compile.** `.compile()` fails with `node 'parse' runs inline and can't be timed out: add it with add_node or add_fallible_node to have its timeout honoured`. The alternative, checking the elapsed time after the handler returns and failing the attempt if it ran over, honours the outcome but not the point of a timeout (the run waited anyway), and throws away a result already computed. A run-level `RunConfig::with_deadline` isn't per node and stays allowed: it takes effect at the next node.
- **Cancellation: documented.** It is the rule every async handler that doesn't await already lives by: a token is cooperative, and code between two awaits runs to the end.
- **Blocking: documented.** The method's documentation says what "doesn't block" means (no I/O, no sleeping, no waiting on a lock another thread may hold for long, no sync store call, no `block_on`) and gives tokio's rule of thumb: tens of microseconds, not milliseconds. Detection would need a clock read on each side of every handler, and the node span and `strata.node.duration` already show a slow node.
- **`_blocking` emits: by the types.** A plain or fallible inline handler gets the state and nothing else.

## Parity

**Rust only.** The question is Rust's: the hand-off is tokio's blocking pool, and the run-to-run spread is the cost of its sleeping threads. .NET's sync node runs through `Task.Run` on a thread pool that keeps its threads spinning, costs about 1.4 µs (0.5 µs async) and doesn't vary the same way.

Nothing outside the Rust executor sees an inline node: it exports as the plain or fallible node it is (no `kind`, no `schema_version`), checkpoints and events are unchanged, and the CLI has nothing to show. So the engines' outputs stay the same, and a .NET `AddInlineNode` that did the same as `AddNode` would be API with nothing behind it. If .NET ever wants to skip `Task.Run` for a sync handler, it is a real option of its own, decided on its own figures.

## Expected gain

**What stood in for the inline node** when this was written: no inline node had been prototyped. Inline adds no executor path of its own: to `attempt()` it is a boxed future that is ready on its first poll, which is what the scenarios' async handlers are (`|work, _ctx| async move { stepped(work) }`, no await). The async rows are therefore the inline estimate, and the sync rows are what it replaces. The build added that scenario, and its figures matched the async ones (see "Implementation Status").

Measured on 2026-10-14 on the machine `benchmarks/RESULTS.md` names (Windows, x86_64, 24 logical cores), five runs of each, one at a time. Each cell gives the range over the five runs.

**Traversal** (criterion medians):

| Nodes | Sync | Async (inline estimate) |
|---:|---:|---:|
| 1 | 6.51 – 6.86 µs | 1.29 – 2.03 µs |
| 10 | 42.2 – 57.2 µs | 4.96 – 8.37 µs |
| 100 | 437 – 525 µs | 40.9 – 42.1 µs |

**Transition latency** (the profile, 198,000 samples a run):

| | Sync | Async |
|---|---:|---:|
| P50 | 1.2 µs once, 4.4 – 5.4 µs four times | 0.4 µs every run |
| P99 | 1.5 µs once, 8.1 – 8.7 µs four times | 0.5 – 0.6 µs |
| P99.9 | 10.3 µs once, 28 – 34 µs four times | 0.7 – 1.6 µs |

**Throughput** (the profile, transitions a second over two seconds):

| Concurrent runs | Sync | Async |
|---:|---:|---:|
| 1 | 0.13 – 0.14 M (0.39 M once) | 1.83 – 1.93 M |
| 4 | 0.31 – 0.33 M | 5.73 – 5.96 M |
| 16 | 0.43 – 0.44 M | 9.71 – 10.6 M |
| 64 | 0.40 – 0.42 M | 10.6 – 11.5 M |

What this says:

- **A sync node costs 4.4 to 5.2 µs when the hand-off sleeps, and on this day it nearly always did.** In criterion all five runs were in the slow mode. The profile found the fast mode once in five (P50 1.2 µs, and one-run throughput tripled). An inline node is 0.4 µs every time: **about 11 times less per node, and the same from run to run** (the 100-node line varied by 3% over five runs, against 20% for sync).
- **The tail goes too.** Sync P99.9 is 28 to 34 µs; inline would be about 1 µs.
- **Throughput is where it shows most:** sync stops at about 0.43 M transitions a second however many runs are added, because every node goes through the blocking pool. Async keeps scaling to 11 M at 64 runs. That is **about 26 times more** for a graph whose sync nodes all move inline.
- **What it doesn't change:** a node that waits on a model or a tool for hundreds of milliseconds. It should stay on the blocking pool, or be async. The gain is for graphs with many small, CPU-only nodes (routing, parsing, merging state, validation), where the engine's own cost was the cost.

## Tests

- **Allocations** (`rust/strata-bench/tests/allocations.rs`): an inline node may make 2 (it should make 1, like an async node).
- **Behaviour**, beside the existing node tests: an inline node and an inline fallible node run, retry under a policy, fall back, and panic like the others; `.compile()` refuses a timeout on each; a run cancelled while an inline handler runs commits that node's result and stops before the next one, with a `cancelled` checkpoint at the next node; an inline handler runs on the thread that polls the run (on a current-thread runtime, the test's own thread; a blocking-pool thread has a different id), inside its attempt's span.
- **Benchmarks:** `Handler::Inline` in `strata-bench`'s scenarios, so `traversal/inline/*`, `transition_latency` and `throughput` measure it from then on. It is a Rust-only row; `benchmarks/README.md` says so, and the .NET profile has no such row.

## Wire format

Unchanged.

## Review decisions

1. **Build it as two named methods, rather than only documenting the async idiom?** Recommended: yes, for the compile-time timeout refusal and the contract at the call site. **Accepted.**
2. **Refuse a timeout on an inline node, rather than check it after the fact?** Recommended: refuse. **Accepted.**
3. **Leave out inline branches and inline streaming nodes?** Recommended: yes (above). **Accepted.**
4. **Rust only?** Recommended: yes. **Accepted.**

---

## Implementation Status

Built 2026-10-14, Rust engine only, as designed.

**How it was built:**

- **`node.rs`:** `on_executor`, beside `on_blocking_pool`: the handler runs in the boxed future when it is first polled, so inside its attempt's span and the executor's panic catch. `NodeEntry.inline`, false for every other kind.
- **`graph.rs`:** `add_inline_node`, `add_inline_fallible_node`, their contract in the doc comments; `.compile()` refuses a timeout on an inline node, after the existing timeout checks; pointers from `add_node`, `add_fallible_node` and `set_timeout`. Re-adding the node with `add_node` clears the flag, since the entry is replaced.
- **Tests:** `tests/inline_nodes.rs` (8: the thread an inline handler runs on, retry, fallback, failure with its `failed` checkpoint, panic, the timeout refusal and re-adding, a cancellation that arrives during the handler, the export), and `an_inline_handler_runs_inside_its_attempt` in `tests/telemetry.rs`.
- **`rust/strata-bench`:** `Handler::Inline` in `Handler::ALL`, so `traversal/inline/*` and the profile's `inline` rows exist; `allocations.rs` counts it (limit 2; it makes 1, like an async node; sync makes 2); `profile_loop` takes `inline`.
- **Documents:** this one; pointers in `design/17` and `design/38`; `benchmarks/README.md`; `CHANGELOG.md`; `CLAUDE.md`.

**Measured on the build** (same machine, 2026-10-14, five runs of each, one at a time; ranges over the five):

| | Sync | Async | Inline |
|---|---:|---:|---:|
| Traversal, 1 node | 4.93 – 7.38 µs | 1.26 – 1.52 µs | 1.28 – 1.32 µs |
| Traversal, 10 nodes | 16.3 – 59.8 µs | 5.01 – 5.40 µs | 4.82 – 5.09 µs |
| Traversal, 100 nodes | 510 – 571 µs | 41.3 – 43.1 µs | 40.2 – 44.5 µs |
| Transition latency, P50 | 1.1 – 1.2 µs | 0.4 µs | 0.4 µs |
| Transition latency, P99 | 1.4 – 3.2 µs | 0.5 µs | 0.5 – 0.7 µs |
| Transition latency, P99.9 | 11.5 – 14.8 µs | 0.7 – 1.2 µs | 0.7 – 1.5 µs |
| Throughput, 1 run | 0.27 – 0.44 M/s | 1.96 – 2.05 M/s | 1.95 – 2.04 M/s |
| Throughput, 16 runs | 0.42 – 0.44 M/s | 9.74 – 10.4 M/s | 9.88 – 10.4 M/s |
| Throughput, 64 runs | 0.41 – 0.42 M/s | 10.9 – 11.6 M/s | 11.1 – 11.7 M/s |

Inline is async within the noise, as "Expected gain" assumed. This time the profile's sync transitions were in the fast mode in all five runs (P50 1.1 µs, against 4.4 to 5.4 µs in four runs of five earlier the same day) while criterion's sync traversals were in the slow one: which mode a run lands in still decides the sync figure, and inline is 0.4 µs either way. Against sync's fast mode it is about 3 times less per node at P50 and 8 to 20 times less at P99.9.

`benchmarks/RESULTS.md` was not rerun: it is a full run of both engines, replaced by hand, and the inline rows will be in the next one.

**Tests:** `rust/` 789 (780 before, plus the nine above), `cli/` 80, `examples/rust-agent` 1, without Postgres or Redis running. `cargo fmt` and `clippy --all-targets --all-features -D warnings` are clean in `rust/`.

## Not built (deliberately out of scope)

- **An inline branch** and **an inline streaming node** (review decision 3).
- **Detecting a handler that blocks.**
- **Anything in .NET.**
