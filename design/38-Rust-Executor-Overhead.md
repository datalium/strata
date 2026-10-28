# Datalium Strata: The Rust Executor's Per-Node Overhead

The first benchmark run (`design/37-Benchmarks.md`, SG-27) found the .NET engine faster than the Rust engine in every benchmark without I/O, by between 2.6 and 7.5 times. A node cost the Rust engine 1.8 µs and the .NET engine 0.4 µs. That run could not say why: it was a measurement suite, and the Rust hot path had never been profiled.

This document is the follow-up: what the time was spent on, what was changed, and what a node costs now. It covers the Rust engine only. The .NET engine is unchanged.

It is written after the work, as a record. "Review decisions" lists what the build chose and what it left for review.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Why was Rust slower? | **No single hot spot.** The Rust executor was written as a line-by-line twin of the .NET one, and did for every node what is nearly free in .NET and is not in Rust: allocate small strings, clone a cancellation token, box a future. |
| What was a node made of? | **28 heap allocations and 8.5 KB**, about fifteen lock operations inside cancellation tokens, three string formats, five hash lookups, and a scan of every edge in the graph. |
| What is a node made of now? | **One allocation of 104 bytes** (the future its handler returns), one hash lookup, and no formatting. |
| What does a node cost now? | **0.4 µs** at the median on the machine that measured 1.8 µs (the .NET engine: 0.4 µs). A hundred async nodes take 41 µs (.NET: 50 µs). |
| Does anything an application sees change? | **No API, no wire format, no event, span or metric.** One corner is different: a handler that cancels its own token (see "The attempt's token"). |
| What keeps it from coming back? | **A test that counts allocations per node.** A count is the same on every machine; a timing is not. |
| What is still slower than .NET? | **Sync handlers, parallel groups, maps, streaming and a run's own setup.** Each has a cause outside the per-node path; see "What is left". |

---

## How it was measured

No sampling profiler was installed on the machine the benchmarks ran on, and Windows needs an elevated shell for one. Three things stood in:

1. **A counting allocator.** A global allocator that counts calls and sizes while a flag is on, around one run of a 1-node, an 11-node and a 101-node line. The difference between two lengths is what a node allocates.
2. **Another allocator.** The same loop under mimalloc. What it saves is a lower bound on what allocation costs.
3. **`perf` in a Linux container** (Debian, glibc, the WSL2 kernel's `cpu-clock` at 10 kHz, frame pointers on) over `rust/strata-bench/examples/profile_loop.rs`, which runs one scenario in a loop.

### What they showed, before any change

| | Windows | Linux (container) |
|---|---:|---:|
| Async node, system allocator | 1,848 ns | 1,380 ns |
| Async node, mimalloc | 1,235 ns | 1,192 ns |
| Sync node | 5,702 ns | 29,409 ns |
| Allocations per node | 28 | |
| Bytes allocated per node | 8,512 | |

So the gap was not a Windows artefact: a third of a node was the Windows heap, and the other two thirds were there on Linux too.

The sampled profile of the async loop, by where the time was (self time, Linux, glibc):

| Share | Where |
|---:|---|
| 44% | `libc`: `malloc`, `free` and `realloc` (14% by name), the allocator's internals and memory copies (the rest; `libc` has no frame pointers, so the profile can't split them further) |
| 15% | `tokio_util`'s cancellation token: `decrease_handle_refcount`, `increase_handle_refcount`, `child_node`, `is_cancelled`. Each locks a mutex. |
| 13% | Building and dropping the node's `NodeContext`, string formatting included (inclusive) |
| 5% | Cloning and dropping the `LevelScope` |
| 2% | `next_node_after`: finding the edge to follow |
| 2% | Hashing metric keys nobody was listening for |

The sync loop was a different picture: **70% of it was in the kernel**, waking threads. A sync handler runs on tokio's blocking pool, so every node is a hand-off to another thread and one back.

### Where the 28 allocations came from

- **Two boxed futures, 6,144 and 1,544 bytes.** The executor boxed the node's future and the attempt's future so it could catch a panic in each. The node's future was that large because it held the whole failure path (a `failed` checkpoint, its save, the retry wait) whether or not the node failed.
- **About twelve copies of the node's name and path**, as `String` and as `Arc<str>`: for the loop variable, the telemetry scope, the node's context, and the labels of two metrics.
- **The idempotency key**, three formats and five allocations, for every node, though few handlers read it.
- **Metric labels.** `metrics::counter!(name, "label" => value.to_string())` builds its key (a `Vec` of labels, the strings, a hash) before it looks for a recorder, so it costs the same with nobody listening.
- **A child cancellation token per attempt:** its node, and a slot in its parent that the parent frees again when the child goes.
- **The handler's own future**, boxed. This one is the price of storing handlers of any type in one map, and is the one that remains.

---

## What changed

All of it is inside `rust/strata-core`. Nothing public changed.

### A plan per node, made when the graph compiles

`NodeEntry` gained a `NodePlan`, filled by `.compile()`: the node's name as an `Arc<str>`, its recovery policy (or the default), its timeout, whether a run freezes after it, its outgoing edges in declaration order (each with its target's name as an `Arc<str>`), and the metric series the node records to.

The executor now looks a node up **once**, and gets everything from the plan. Before, it made five lookups by name (the node, its policy, its timeout, the interrupt boundaries, the waits) and then scanned every edge of the graph for the ones that leave the node, so a line of N nodes did N² string comparisons.

The loop carries the node's name as that `Arc<str>` from node to node, so following an edge copies nothing.

### Metric series made once

A compiled graph makes the `strata.node.duration` series of each node committing and the `strata.graph.transition` series of each edge, and records to them through `metrics::with_recorder`, which is what the macros expand to. With no recorder that is now two pointer checks. The run's own two series (`strata.graphs.started`, `strata.graph.duration`) are made once per process.

The series are the ones the macros made: same name, same labels, in the same order. `tests/telemetry.rs` reads them back through a recorder.

They are made for the outermost level, where a node's path is its name. A node inside a subgraph or a map has a path that depends on where the child is used, and still builds its key each time.

### A level's shared part behind one `Arc`

The executor makes a `LevelScope` for every node, to stamp the node's hop number. That was a copy of a cancellation token (a mutex) and up to seven `Arc`s. The parts every node of a level shares are now a `LevelCore` behind one `Arc`, and the copy is a reference count and a few integers.

### The node's context

`NodeContext` takes the name and path the executor already has, instead of copying them. Its idempotency key is made the first time `idempotency_key()` is called, from the hop numbers it holds, so a handler that doesn't ask doesn't pay. The key is the same as before; a context renamed with `with_node_path` or `with_run_id` settles its key first, so the key stays the one of the node it was made for.

### Futures

- **A panic is caught without a box.** `CatchUnwind` holds its future in place (through `pin-project-lite`, which tokio already brings).
- **The failure path is a future of its own.** What follows a failed attempt (the retry's wait, or the `failed` checkpoint and its save) moved to `after_failure`, boxed when a node fails. The loop's other uncommon exits (a cancellation, a tripped hop limit, a freeze around a child) are boxed the same way.
- **What a subgraph node resumes from is boxed**, so the inputs of every other node's attempt are several hundred bytes smaller.

The future a node runs in is now part of the run's one allocation.

### The attempt's token

Every attempt had a child token of its own. Only three things ever cancel it apart from the level: the node's timeout, and a group, subgraph or map, which cancel what they started.

A node with no timeout that is none of those now runs against its level's token, and its context makes a child token **if the handler asks for one** (`ctx.cancellation()`). The handler still gets a token of its own, cancelled when the run is.

**The one corner that differs.** A handler that cancelled its own token and then went on waiting used to be stopped by the engine and reported as timed out (`node 'X' timed out after 0 ms`). The engine no longer watches a token it didn't make, so that handler now runs on. And two copies of a context, made before either was asked for its token, each make their own child of the level's token: both are cancelled with the run, but cancelling one by hand no longer cancels the other. Nothing documented either, and in .NET a handler can't cancel its token at all.

### A run's own setup

A run made 18 allocations besides its nodes' and makes 10 now: the run and the resume aren't boxed, the thread id is shared instead of copied twice, the run id is written into one buffer instead of two, and an empty service map, metadata map and hop chain are shared statics.

### Branches

Three small things in the fan-out runner: a branch's path is one shared string instead of three, the words of its timeout are built only if it has one, and its future is boxed once instead of twice. A branch made 16 allocations and makes 11.

---

## What a node costs now

Measured on the machine `benchmarks/RESULTS.md` names, on 2026-10-13. The .NET column is the 2026-10-11 run; the .NET engine did not change, and the Rust baseline measured again on the 13th came within 2% of the 11th.

| Benchmark | Rust before | Rust now | .NET |
|---|---:|---:|---:|
| Traversal, async, 1 node | 2.77 µs | 1.28 µs | 0.71 µs |
| Traversal, async, 10 nodes | 17.8 µs | 5.22 µs | 4.11 µs |
| Traversal, async, 100 nodes | 185 µs | 41.0 µs | 49.9 µs |
| Traversal, sync, 1 node | 8.61 µs | 4.49 µs | 2.06 µs |
| Traversal, sync, 10 nodes | 75.2 µs | 13.2 µs | 14.7 µs |
| Traversal, sync, 100 nodes | 766 µs | 459 µs | 143 µs |
| State size, 10 async nodes, 16 KiB state | 24.6 µs | 7.94 µs | 4.12 µs |
| Parallel group, 8 branches | 37.4 µs | 15.1 µs | 5.65 µs |
| Parallel group, 32 branches | 99.6 µs | 51.3 µs | 20.3 µs |
| Map, 64 items | 160 µs | 144 µs | 61.1 µs |
| Interrupt and resume | 12.6 µs | 7.64 µs | 4.64 µs |
| 10 nodes, `EveryNode` | 34.2 µs | 20.5 µs | 12.3 µs |
| Streaming, per token | 363 ns | 352 ns | 111 ns |
| Transition latency, async, P50 | 1.80 µs | 0.40 µs | 0.40 µs |
| Transition latency, async, P99.9 | 9.00 µs | 1.00 µs | 1.50 µs |
| Throughput, async, 64 runs at once | 6.4 M/s | 11.3 M/s | 20.1 M/s |

| | Before | Now |
|---|---:|---:|
| Allocations per async node | 28 | 1 |
| Bytes per async node | 8,512 | 104 |
| Allocations per sync node | 29 | 2 |
| Allocations per run, besides its nodes' | 18 | 10 |
| Allocations of a one-node run, in all | 38 | 11 |
| Allocations per group branch | 16.4 | 11.4 |

**The sync figures move between runs.** Five runs of the sync traversals gave 4.3 to 7.0 µs for one node, 13 to 54 µs for ten and 134 to 539 µs for a hundred; the table has one of them. See the next section for why.

---

## What is left

Each of these is slower in Rust than in .NET, and none is in the per-node path this work covered.

- **Sync handlers hand off to another thread, twice a node.** That is the design (`design/17-Async-Execution.md`): a sync handler runs on tokio's blocking pool so it can't hold up the executor and so the engine can stop waiting for it. A hand-off costs a thread wake each way. When the two threads stay awake between nodes a sync node costs about 1.1 µs; when either goes to sleep it costs 4 to 5.5 µs, and which happens depends on timing, so the benchmark gives both. In the Linux container it was 29 µs before this work. The .NET thread pool keeps its threads spinning for work, and runs what follows on the thread that finished. Sync throughput still stops at about 0.4 million transitions a second. **The only large gain left here is running a sync handler on the executor, which gives up what the blocking pool is for; it is a decision, not a fix** (review decision 1). Decided yes, as an opt-in per node: `design/39-Inline-Sync-Handlers.md`.
- **Parallel groups and maps spawn a task per branch or item.** A branch costs about 1.5 µs (2.7 before; .NET 0.6) and still makes 11 allocations, most of them tokio's task and the branch's own token. A map's item is a child graph run of its own (about 27 allocations).
- **Streaming is the channel.** A token makes one allocation (its text). The rest is `tokio::sync::mpsc` and waking the reader.
- **A run's setup is about 0.9 µs**: ten allocations and a UUID v7.
- **A large state is cloned into every handler.** That is the handler signature: Rust handlers take the state by value. 16 KiB adds about 0.3 µs a node.
- **`EveryNode` serializes the state to JSON for every checkpoint**, in both engines; Rust builds a `serde_json::Value` first.

---

## Wire format

Unchanged. Checkpoints, graph definitions, events, spans and metric series are what they were.

---

## Migration

Nothing to migrate. `datalium-strata` gains one dependency, `pin-project-lite`, which `tokio` already brings.

A compiled graph now reads its edges once, at `.compile()`. `CompiledGraph::definition` is a public field; changing its edges after compiling never worked (the edge conditions sit beside them by index), and now has no effect at all.

---

## Tests

- **Allocations are counted** (`rust/strata-bench/tests/allocations.rs`): an async node may make 2 (it makes 1), a sync node 4 (it makes 2), and a run 16 besides its nodes' (it makes 10). The limits leave room for a dependency to change by one, not for a string per node. `-- --nocapture` prints the counts.
- **A handler's token is its own** (`tests/cancellation.rs`): a handler that cancels the token it was given doesn't cancel the run, and the next node's token is live.
- **The idempotency key is the node's when asked for late** (`tests/node_context.rs`): a context kept past its run gives the key its node had, for a node and for a branch, and renaming the context doesn't change it.
- Everything else is the existing suite, which is what says the behaviour is the same: retries, fallbacks, routing, timeouts, cancellation at every depth, events in order, spans, and the metric series read back through a recorder.

---

## Review decisions

1. **Should a sync handler be allowed to run on the executor?** **Not decided here** (since decided yes, as `add_inline_node` and `add_inline_fallible_node`: `design/39-Inline-Sync-Handlers.md`). It would be an option on the node or the graph (`add_inline_node`, or a compile option), off by default. An inline handler can't be abandoned when its node times out or its run is cancelled, must not block, and can't use the `_blocking` emits. In exchange a sync node would cost what an async one does. .NET needs no such option. It is the largest gain left and it changes `design/17`'s guarantee, so it wants a yes from review first.
2. **Is the lazily made token acceptable?** **The build says yes.** It removes two allocations and about six lock round trips from every node. The behaviour that changes was never promised and can't be reached from .NET.
3. **Gate on a count, not a time?** **Yes.** `design/37` decided against gating CI on performance because a shared runner varies. An allocation count doesn't.
4. **Keep the metric macros?** **Only where the labels are literals.** Per-node and per-run series are made once. The rarer metrics (retries, hop limits, leases, tools) still use the macros.

---

## Implementation Status

Built 2026-10-13, Rust engine only.

**How it was built:**

- **`node.rs`:** `NodePlan`, `EdgePlan`; `NodeEntry.plan` and `.composite`; `AttemptInputs.resume` boxed, `.token` replaced by `token()`.
- **`graph.rs`:** `.compile()` fills the plans; `CompiledGraph.timeouts` is gone (the plan has it).
- **`checkpoint.rs`:** `execute_from` takes the starting node by reference and carries an `Arc<str>`; `next_edge` replaces `next_node_after`; `run_attempts` is the common path and `after_failure` the rest; `observed` takes any future.
- **`context.rs`:** `LevelCore`; `LevelScope::path_arc`; the lazy key (`KeyHops`) and token in `NodeContext`.
- **`observability.rs`:** `transition_key`, `node_committed_key`, `RunSeries`; `NodeScope::committed`; `CatchUnwind` generic.
- **`parallel.rs`:** `Launch.path` shared; one box per branch.
- **`rust/strata-bench`:** `tests/allocations.rs`, `examples/profile_loop.rs`.
- **Documents:** this one; `benchmarks/RESULTS.md` (the Rust columns), `benchmarks/README.md`, the README's sentence, `CHANGELOG.md`, a pointer in `design/37`.

**Where it diverged, or filled a gap, and why:**

- **The profiler ran on Linux, the benchmarks on Windows.** The profile ranked the causes; every before-and-after number is from Windows.
- **Linux was not measured after the change.** The container run that would have done it took Docker Desktop down (its script copied the 68 GB `rust/target` folder into the container), and no further container was run.
- **Postgres and Redis were not measured again** for the same reason. Their rows in `RESULTS.md` are the 2026-10-11 run's; a store round trip is hundreds of microseconds, so the engine's share doesn't show there.
- **The .NET benchmarks were not run again.** The .NET engine didn't change.

**Tests:** `rust/` 780 (777 before, plus the three above), `cli/` 80, `examples/rust-agent` 1, without Postgres or Redis running, so the tests that need them returned early. `cargo fmt` and `clippy -D warnings` are clean in all three projects.

## Not built (deliberately out of scope)

- **Sync handlers on the executor** (review decision 1; since built, `design/39-Inline-Sync-Handlers.md`).
- **Anything in the .NET engine.**
- **Fewer tasks in a parallel group or a map,** a faster channel for streaming, or a cheaper run id.
- **Handlers that borrow the state** instead of taking a clone.
- **A connection pool for the Rust Postgres store**, the other follow-up `design/37` named.
- **A sampling profiler in CI**, and Linux results.
