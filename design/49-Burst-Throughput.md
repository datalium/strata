# Datalium Strata: Burst Throughput

Issue #37 reported that a burst of runs whose handlers really wait gets through 3 to 5 times fewer runs a second than the same waits in plain async code. Each run was `Plan`, then a parallel group of 8 branches, then `Answer`, and every node and branch waited 20–80 ms, so ten waits a run, against the in-memory store. Without the waits, the same graph ran half a million times a second, so the engine's own work per node wasn't the explanation.

This document is the follow-up: what the time went on in both engines, what was changed, and what is left. The work is in two parts. The first part, the small changes, is built. The second, a new wait for a .NET group's branches and smaller Rust futures, is described under "What is left".

It is written after the first part, as a record. "Review decisions" lists what the build chose and what it left for review.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Why is a burst slower than plain code? | **Memory per run in flight, not CPU.** .NET spent 55–90% of the burst's wall time in GC pauses; Rust's burst was held up by Windows' default allocator. The engine's own code was about 1% of the CPU samples per method. |
| Is it the engine or the runtime? | **Both.** The runtime's default throttles: .NET's DATAS kept the server GC at one to three heaps of 24, and Windows' heap serialized Rust's allocations. What it throttles on is what each run holds: in .NET 14.8 KB per run waiting in its branches, against 3.9 KB for plain code; in Rust 25.7 KB against 3.3 KB. |
| What changed? | **The bytes a node and a branch allocate for nothing.** In .NET: the idempotency key is built when a handler reads it, an attempt has a token of its own only when it has a timeout, and the subgraph-only closure is made only for a subgraph. In both: a branch without a timeout watches its siblings' token, and a group's branch names are made once. In Rust: a branch without a timeout has no timer in its task. |
| What does it save? | **In .NET, a node allocates about 1,390 bytes instead of 1,860, and a branch 995 instead of 1,425; in Rust a branch makes 9 allocations instead of 12.** The burst allocates 24.2 KB a run instead of 29.5 in .NET, and 138.9 allocations instead of 159.9 in Rust; with DATAS off, .NET's 100,000-run burst went from 66,000–81,000 to 92,000 runs a second. |
| Does anything an application sees change? | **No API, no wire format, no event, span or metric.** The idempotency key is the same string, read when it's read. A branch's token can't be cancelled from .NET, and a Rust handler's token is a child of the branch's, so sharing the siblings' token can't be told apart. |
| What keeps it from coming back? | **A test in each engine that counts what a node and a branch allocate.** Bytes in .NET, allocations in Rust: the same on every machine, unlike a timing. |
| What should a host that runs bursts do? | **Turn DATAS off (.NET) or use a faster allocator (Rust)**, after measuring: "Hosting a burst". The engine can't choose either for the application. |
| What is left? | **The .NET group's wait for its branches, and the size of Rust's futures**: "What is left". |

---

## How it was measured

On the machine issue #37 names: an Intel Core i9-12900KF (24 logical cores) with 128 GB of RAM, Windows 11 Pro 10.0.26300, .NET 10.0.15 with server GC, Rust 1.91.1 with tokio 1.54.1, on 2026-11-18.

The burst is the issue's program, built against the source rather than the packages, in three versions per engine:

- **strata:** the graph.
- **plain:** the same waits in plain async code, with a cancellable token per run and a linked or child one per group.
- **mimic:** plain code shaped like the engine's fan-out. In .NET it has a linked token and a `WaitAsync` for each node and branch, a wrapper task per branch and a `Task.WhenAny` loop. In Rust it has a child token and a `JoinSet` task per branch.

Each version ran at 10,000, 100,000 and 1,000,000 runs started at once, after a warm-up of 1,000.

In .NET the counters were in the process: CPU time, `GC.GetTotalAllocatedBytes`, GC counts and pause time, lock contention, thread count. Allocations were sampled by type through an `EventListener` on the runtime's allocation tick. `dotnet-gcdump` took the heap with 100,000 runs all waiting in their branches. `dotnet-trace` recorded CPU samples and verbose GC events, read with TraceEvent for allocation call sites and each GC's work per heap.

In Rust a counting allocator measured allocations and live bytes, swapped over mimalloc 0.1.52 to see what the allocator costs. A sampling allocator recorded call sites: it samples about 1 in 32 blocks by address, so a free knows without a lookup whether its block was sampled, and symbolizes at the end. dhat's backtraces don't resolve on Windows. No CPU profile was taken: on Windows it needs an elevated shell, and with mimalloc the engine added only about 40 µs of CPU a run.

### What they showed, before any change

**.NET.** Runs a second; in brackets, the share of the wall time spent in GC pauses:

| Runs at once | plain | mimic | strata |
|---:|---:|---:|---:|
| 10,000 | 35,595 (19%) | 34,243 (46%) | 13,476 (76%) |
| 100,000 | 113,000–134,000 (34–47%) | 52,733 (62%) | 13,700–39,500 (55–90%), 12 runs |
| 1,000,000 | 186,138 (31%) | 60,181 (73%) | 61,944 (56%) |

The 100,000-run burst was bimodal: most runs got about 14,000 runs a second with some 320 gen0 collections, a few got 26,000–39,000 with about 50. The published `0.9.0-preview.2` packages did the same, so it was no regression. In every run the rate followed the pause.

A larger gen0 budget (`DOTNET_GCgen0size`) cut the collections from 321 to 12 and the pause only from about 6 s to 3.8 s: each collection then took about 300 ms. So the cost followed what the runs in flight held, not how fast they allocated. Almost all of a gen0 collection's mark time was cards (`MarkOlder`): older runs still waiting, pointing at what newer ones had just allocated.

The burst used 4 to 5 of the 24 cores: 28 s of CPU in 7 s. The GC trace said why. Every collection ran on one heap, and the bytes it promoted were spread over two or three. Since .NET 9, server GC adapts its heap count to the application's size (DATAS, Dynamic Adaptation To Application Sizes), starting from one. With it off (`DOTNET_GCDynamicAdaptationMode=0`):

| Runs at once | plain | strata | Gap, DATAS on → off |
|---:|---:|---:|---:|
| 10,000 | 38,149 | 38,173 | 2.7× → none |
| 100,000 | 174,353 | 66,000–81,000, 5 runs | about 9× → 2.4× |
| 1,000,000 | 239,041 | 85,600 | 3× → 2.8× |

The rate stopped varying from run to run too. Linux has the same default, which is why the issue's Linux container hit the same ceiling.

**Rust.** Runs a second, with Windows' allocator and with mimalloc:

| Runs at once | plain | mimic | strata |
|---:|---:|---:|---:|
| 100,000, system | 125,375 | 106,672 | 65,907 |
| 100,000, mimalloc | 129,010 | 114,528 | 93,732 |
| 1,000,000, system | 131,022 | 100,806 | 34,040 |
| 1,000,000, mimalloc | 140,432 | 124,764 | 97,976 |

At a million runs the strata burst used about 6 cores with Windows' allocator; mimalloc almost tripled its rate and barely changed plain code's. A run made 159.9 allocations and 30.1 KB, against plain code's 6 and 3.3 KB.

**What a run holds while it waits.** Every wait set to the same few seconds, and the heap read while every run waits in the same step:

| Waiting in | .NET plain | .NET strata | Rust plain | Rust strata |
|---|---:|---:|---:|---:|
| `Plan` | 0.9 KB | 4.1 KB | 0.7 KB | 11.8 KB |
| the 8 branches | 3.9 KB | 14.8 KB | 3.3 KB | 25.7 KB |
| `Answer` | 1.8 KB | 4.6 KB | 0.8 KB | 12.2 KB |

### Where the bytes came from

**.NET, allocated per run (29.7 KB sampled):**

| Where | Bytes | What |
|---|---:|---|
| `LevelScope.KeyFor` | 2,580 | The idempotency key of each of the 11 contexts, as two strings each; most handlers never read it |
| The fan-out, per branch | about 5,700 | The `Settle` state machine, `WaitAsync`'s promise and callbacks, a linked token source, the path, the outcome, the branch's scope and context |
| The fan-out's `WhenAny` loop | about 3,600 | A promise and a copy of the running tasks on every pass, and a dictionary that grows |
| Each node | about 4,500 | The executor's state machines, the context, and `RunAttemptsAsync`'s closure, made for every node though only a checkpointing subgraph uses it |
| The workload | about 7,400 | The timers and waits, the branch lambdas, the state |

A run waiting in its branches held mostly cancellation callbacks (27 per run, 2.2 KB), the eight `Settle` state machines and `WaitAsync` promises, and linked token sources.

**Rust, held per run while it waits in its branches (25.4 KB sampled):**

| Where | Bytes | What |
|---|---:|---|
| The run's future | 5,594 | `graph.run(...)`'s future is 5,720 bytes; plain code's is 336 |
| `start`'s boxed future | 3,416 | |
| `execute_node` → `run_attempts` | 2,317 | The boxed attempt future of the group's node |
| `run_fan_out` | 1,608 | |
| Each of the 8 branches | about 1,370 | The handler's boxed future (about 570 bytes, holding its `NodeContext`), the `JoinSet` task (about 535, its `select!` keeping room for a timer even with no timeout), a child token (112), the `CatchUnwind` box (80), the `JoinSet` entry (56) |

Allocated and not held: each run cloned the group's branch names into a new `Vec<String>`.

The CPU profile, in .NET, added two things. The engine's own methods were about 1% of the samples each, and 42% of the samples were threads waiting for a collection to end. The lock waits were the timer queue's, which `Task.Delay` takes in plain code too, and the `WhenAny` loop's: it puts a continuation on every running branch, and takes it off the others each time one finishes.

---

## What changed

### .NET: the idempotency key, when it is read

`NodeContext.IdempotencyKey` was built for every node and branch as its context was made, as two strings when the run isn't durable. The context now holds the level it was made at, internally, and builds the key the first time it is read, as one string (`string.Create`). It is the same string it was, whenever it is read, including after the run has ended. The `init` setter is unchanged, so a context built by hand still takes a key, and is still empty without one. The Rust engine has built its key this way since `design/38`.

### .NET: an attempt's token only with a timeout

Every attempt of every node linked a token source to its level's. Nothing cancels an attempt apart from its level and its timeout, so an attempt now links one only when the node has a timeout, and otherwise watches the level's token. The Rust engine makes one only for a timeout or a node that cancels what it started (`design/38`, "The attempt's token"). In .NET nothing cancels a group's, a subgraph's or a map's attempt token, so the timeout is the only case.

### .NET: the subgraph's progress closure only for a subgraph

`RunAttemptsAsync` builds the closure through which a checkpointing subgraph's child hands its running checkpoints up. It used the method's own parameters. C# allocates a lambda's captured parameters on entry to the method that declares them, so every node paid for it. It is now built in a method of its own, `WithChildProgress`, which only a checkpointing subgraph node calls.

### Both engines: a branch without a timeout watches its siblings' token

Each branch had a token of its own: in .NET a token source linked to the group's siblings' token, in Rust a child token. Only the branch's timeout ever cancels it on its own, so a branch now has one only when the group has a `BranchTimeout` / `branch_timeout`. Without one, it watches the siblings' token, which the group cancels when it gives up. A .NET handler can't cancel a token. A Rust handler's `ctx.cancellation()` is already a child of the branch's token, made when the handler asks for it, so cancelling it doesn't reach the siblings. The same holds for a map's items with no `ItemTimeout`.

### Rust: a branch's task without a timer

A branch's task raced its work against its token and an `expiry` future. With no timeout that future never resolved, but the task kept room for its timer all the same. A branch without a timeout is now spawned without it. The two paths share `as_settled` and `as_wound_down`, which turn a branch's result into how it settled, so the rules are written once.

### Both engines: a group's names, made once

Each run of a group made the list of its branch names: in Rust a new `Vec<String>`, in .NET an array and a `FanOutSettings` record. A group's options are immutable once it is added, so both are now made in `add_parallel_group` / `AddParallelGroup`. A map's item keys still come from each run's items.

---

## What a run costs now

| | Before | Now |
|---|---:|---:|
| .NET, a node (async handler that yields) | about 1,860 bytes | about 1,390 bytes |
| .NET, a branch (async handler that yields) | about 1,425 bytes | about 995 bytes |
| Rust, an async branch | 11.9 allocations | 9.2 allocations |
| .NET burst, allocated a run | 29.5 KB | 24.2 KB |
| .NET burst, held a run waiting in its branches | 14.8 KB | 12.6 KB |
| Rust burst, allocated a run | 159.9 allocations, 30.1 KB | 138.9 allocations, 26.8 KB |
| Rust burst, held a run waiting in its branches | 25.7 KB | 22.6 KB |
| .NET burst of 100,000 runs, DATAS off | 66,000–81,000 runs/s | 91,000–92,000 runs/s |
| Rust burst of 100,000 runs, Windows' allocator | 65,300–65,900 runs/s | 67,700–71,100 runs/s |

A Rust node already made one allocation, and makes one still. With DATAS on, .NET's burst is still bimodal, so a single number would say little; the gain shows in the stable configuration.

---

## Hosting a burst

What the measurements say for an application that starts many runs at once and has them wait on I/O. Measure your own workload before changing either default.

- **.NET: DATAS.** Server GC's dynamic heap count is on by default since .NET 9. It keeps a process's memory small by starting with one heap and adding heaps as it judges they're needed. A burst of 100,000 runs in flight didn't get it past three, and spent most of its time in pauses. To turn it off, set `<GarbageCollectionAdaptationMode>0</GarbageCollectionAdaptationMode>` in the application's project, or `DOTNET_GCDynamicAdaptationMode=0` in its environment. Server GC then keeps a heap per core, which costs memory. This is the application's setting: a library can't set the GC mode, so neither `Datalium.Strata` nor `Datalium.Strata.Hosting` does.
- **Rust: the allocator.** On Windows the system allocator held a burst of a million runs to about 6 cores; mimalloc tripled its rate. A library shouldn't choose the global allocator, so no Strata crate does. An application chooses it with `#[global_allocator]`.

---

## What is left

The second part of #37, not built yet:

- **.NET: one wait for a group's branches.** The fan-out waits on its branches with a `Task.WhenAny` loop over the ones still running, and each branch through a `Settle` wrapper with a `WaitAsync` on its token. Each branch would instead report how it settled to a queue the group reads, with one cancellation registration for the run's token and, only for a branch with a timeout, one on its timer. A branch that ignores its token must still settle when it is cancelled or times out, and a late result from a branch given up on must still have its exception observed. That would remove about 5–6 KB allocated and 2.5 KB held per run of the issue's graph, and the lock waits on the branches' continuation lists. Rust needs no change: `JoinSet::join_next` already works that way.
- **Rust: smaller futures.** The run's future is 5,720 bytes, and `start`'s and an attempt's are boxed at 3.4 and up to 2.3 KB; `RunConfig` is only 256 bytes of it. The rest is room held for paths a plain run doesn't take: a scheduler's admission, resume, recovery, failure. Boxing those paths would shrink what every run holds, and a test would keep the run's future under a size. This is where most of the 19 KB a Rust run holds beyond plain code's is. .NET needs no change: an async method doesn't keep room for paths it doesn't take.

Considered and not proposed: running a Rust group's async branches inside the group's own task instead of spawning one each, which would save the most but stop CPU-heavy branches from running on several threads at once; passing `NodeContext` by reference, which changes the API; pooling objects between runs.

---

## Wire format

None. No checkpoint, definition, history record or memory record changes.

## Migration

None. The API is the same in both engines.

## Tests

- **What a node and a branch allocate is measured** (`dotnet/tests/Datalium.Strata.Tests/AllocationTests.cs`, alone in its collection because it reads the process's allocations). The fewest bytes of five warm runs of a line of 11 and of 101 nodes, and of a group of 1 and of 9 branches, whose handlers yield. A node may allocate 1,500 bytes (it allocates about 1,390), a branch 1,100 (about 995). Building the key eagerly again fails it (1,523 bytes a node), and so do a linked token source per attempt (1,616) and per branch (1,280).
- **What a branch allocates is counted** (`rust/strata-bench/tests/allocations.rs`): an async branch may make 10 allocations (it makes 9.2). A child token per branch again fails it (10.9).
- **The idempotency key is the node's when it is read late** (`NodeContextTests`, the twin of `rust/strata-core/tests/node_context.rs`): read after the run, a node's and a branch's keys are what they were. A context built by hand has an empty key, or the one it was given.
- Everything else is the existing suite, which is what says the behaviour is the same: branch timeouts, sibling cancellation, joins, maps winding down, retries, durable execution's keys, events and spans.

## Review decisions

1. **Is the change to DATAS a default the engine should make?** **No: it can't.** The GC mode is the application's runtime configuration. The engine documents it ("Hosting a burst").
2. **Should a host crate or `Datalium.Strata.Hosting` offer a faster allocator, or turn DATAS off?** **Not decided here.** Both change the whole process, not just Strata's part of it.
3. **Two parts or one?** **Two**, so the rewrite of the .NET group's wait is reviewed on its own.
4. **Should `benchmarks/RESULTS.md` be run again for this?** **Not for this part.** None of its scenarios is a burst of runs that wait, and the next full run will include both parts.

## Implementation Status

The first part built 2026-11-18, in both engines.

**How it was built:**

- **.NET, `NodeContext.cs`:** `IdempotencyKey` reads through `KeyLevel`, built on first read; `init` still sets it.
- **.NET, `Execution.cs`:** `RunScope.ContextFor` sets `KeyLevel`; `LevelScope.KeyFor` makes one string.
- **.NET, `CompiledGraph.Interruption.cs`:** `RunAttemptsAsync` links an attempt's token only with a timeout, and `AttemptOnceAsync` takes a token; `WithChildProgress` builds the subgraph's progress closure.
- **.NET, `FanOutRunner.cs`:** a branch's token is the siblings' unless the group has a branch timeout; the list of sources to dispose is made only when there is one.
- **.NET, `ParallelBranches.cs`, `StateGraph.cs`:** `ParallelGroupRunner.Plan` and `GroupPlan`, made in `AddGroup`.
- **Rust, `parallel.rs`:** `GroupRun::start` shares the siblings' token and spawns without a timer when there is no timeout; `as_settled`, `as_wound_down`; `run_branches` takes the names.
- **Rust, `graph.rs`:** the group's names, made once.
- **Tests:** `AllocationTests.cs`, a test in `NodeContextTests.cs`, the branch count in `allocations.rs`.
- **Documents:** this one; `design/26`'s note on a branch's token; `design/38`'s count of what a branch allocates; `benchmarks/README.md`; `CHANGELOG.md`.

**Where it diverged, or filled a gap, and why:**

- **The .NET test measures bytes; the Rust test counts allocations.** .NET has no count of allocations, only bytes, which are the same on every machine for one runtime.
- **No CPU profile of the Rust engine**, for the reason in "How it was measured".
- **The burst's numbers are from repro programs kept outside the repository.** They're the issue's program and its variants, not part of `strata-bench`.

**Tests,** with Postgres and Redis from `docker-compose.yml` and `STRATA_REQUIRE_BACKENDS=1`:

- **`dotnet/`:** 1,345 passed and 12 skipped, the live cloud and model tests, which need credentials; the example's 1 passed.
- **`rust/`:** 1,148 passed and 1 ignored. One failed, `postgres_settings_open_a_memory_store_that_is_migrated_or_only_checked`, on main too: the local database was reused, so pgvector's type was already in `public` and the test's own `search_path` didn't see it. CI starts from an empty database.
- **`cli/`:** 86 passed. **`examples/rust-agent`:** 1 passed.

`cargo fmt`, `clippy -D warnings` and `dotnet format` are clean.

## Not built (deliberately out of scope)

- **The second part**: "What is left".
- **A burst scenario in `strata-bench` and the .NET benchmarks.** The repro programs measured this one; a scenario of its own would have to wait seconds per sample.
- **Changing DATAS, or the allocator, for the application.**
