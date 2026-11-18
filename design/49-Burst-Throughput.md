# Datalium Strata: Burst Throughput

Issue #37 reported that a burst of runs whose handlers really wait gets through 3 to 5 times fewer runs a second than the same waits in plain async code. Each run was `Plan`, then a parallel group of 8 branches, then `Answer`, and every node and branch waited 20–80 ms, so ten waits a run, against the in-memory store. Without the waits, the same graph ran half a million times a second, so the engine's own work per node wasn't the explanation.

This document is the follow-up: what the time went on in both engines, what was changed, and what is left. The work was in three parts, all built: the small changes first (#46); then a new wait for a .NET group's branches (#47), which also tried smaller Rust futures and didn't keep them ("Tried and not kept"); then a CPU profile of the Rust engine, and the two allocations a branch made for nothing that it found ("The Rust CPU profile").

It is written after the work, as a record. "Review decisions" lists what the build chose and what it left for review.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Why is a burst slower than plain code? | **Memory per run in flight, not CPU.** .NET spent 55–90% of the burst's wall time in GC pauses; Rust's burst was held up by Windows' default allocator. The engine's own code was about 1% of the CPU samples per method. |
| Is it the engine or the runtime? | **Both.** The runtime's default throttles: .NET's DATAS kept the server GC at one to three heaps of 24, and Windows' heap serialized Rust's allocations. What it throttles on is what each run holds: in .NET 14.8 KB per run waiting in its branches, against 3.9 KB for plain code; in Rust 25.7 KB against 3.3 KB. |
| What changed? | **The bytes a node and a branch allocate for nothing, and how a .NET group waits.** In .NET: the idempotency key is built when a handler reads it, an attempt has a token of its own only when it has a timeout, the subgraph-only closure is made only for a subgraph, and a group's branches report to one inbox instead of being waited on through `Task.WhenAny`. In both: a branch without a timeout watches its siblings' token, and a group's branch names are made once. In Rust: a branch without a timeout has no timer in its task, and a group borrows its attempt's inputs. |
| What does it save? | **In .NET, a node allocates 728 bytes instead of 1,008, and a branch 707 instead of 1,307; in Rust a branch makes 9 allocations instead of 12.** .NET's burst allocates 19.5 KB a run instead of 29.5, and with DATAS off its 100,000-run burst went from 66,000–81,000 runs a second to 132,000–145,000 (plain code: 174,000). Rust's burst allocates 25.9 KB a run instead of 30.1. |
| Did smaller Rust futures help? | **No.** Boxing the paths a plain run doesn't take shrank a run's future from 5,720 bytes to 1,344 and what a waiting run holds by a quarter to a third, but the burst ran about 10% slower with the default allocator on Windows and on Linux. Giving each task its old size back restored the rate, so it was the task's size, not more work. Not kept. |
| Does anything an application sees change? | **No API, no wire format, no event, span or metric.** The idempotency key is the same string, read when it's read. A branch's token can't be cancelled from .NET, and a Rust handler's token is a child of the branch's, so sharing the siblings' token can't be told apart. A .NET group reports, times out and gives up on its branches as before. |
| What keeps it from coming back? | **A test in each engine that counts what a node and a branch allocate.** Bytes in .NET, allocations in Rust: the same on every machine, unlike a timing. |
| What should a host that runs bursts do? | **Turn DATAS off (.NET) or use a faster allocator (Rust)**, after measuring: "Hosting a burst". The engine can't choose either for the application. |
| What limits Rust's burst? | **The C allocator, then tokio.** On Linux, glibc's `malloc` is about 40% of the engine's CPU in the burst: its own code, `mprotect` as it grows its heaps, and contention on its locks. mimalloc makes the burst 1.6 times faster. The rest is spread over a task per branch, the engine's own code, `tracing` and tokens, with no hotspot. Plain code is bound by tokio's timer lock, at about 10 of 24 cores. |
| What is left? | **Nothing for #37.** A task per branch and a smaller `NodeContext` were considered and not pursued: "What is left". |

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

The sections above are the first part (#46); the two below are the second.

### .NET: one inbox for a group's branches

The fan-out waited on its branches with a `Task.WhenAny` loop over the ones still running, each pass copying them into an array, and each branch through a `Settle` wrapper that awaited `WaitAsync` on its token. Every branch thus held a state machine, a cancellation promise and its callbacks, and every pass put a continuation on every running branch and took it off the others, under their locks.

Now each branch reports how it settled to one `Inbox` per fan-out, once, and the loop reads it:

- **A branch's watcher** (`Branch`) settles it when its task completes, by taking the task's result, so a late exception from a branch the group gave up on is observed. A task that completed at once, as a synchronous handler's has, is settled on the spot, so the fan-out sees such branches in the order they started.
- **A branch with a timeout** that is abandoned rather than waited for also registers on its timer, so it settles as timed out on time even if its handler ignores its token. A map's item winds down instead, as before. The first of the two reports wins.
- **The loop** waits on a `ManualResetValueTaskSourceCore` it resets each time, so a wait allocates nothing. One registration on the attempt's token wakes it. That wake goes through the thread pool, not inside whatever cancelled the token, since the loop goes on to end the node. When the executor has already stopped waiting for the group, the wake still lets the group give up on its branches and end their spans.
- **What the group does with the reports is unchanged:** the join, the branches already settled when it gives up (recorded in declaration order), a map's items waited for until they stop, and the events and spans in the same order.

Rust needs no such change: `JoinSet::join_next` already works this way.

### Rust: the group borrows its attempt's inputs

`run_branches` took the attempt's inputs (328 bytes) by value, and its launcher cloned the group's `NodeContext` (248 bytes) to make each branch's from. An async function keeps room for an argument it moves, so its future held the inputs twice, and the launcher a third copy of the context. It now borrows the inputs, and the launcher borrows the context and the stream: the future is 848 bytes instead of 1,768.

### Tried and not kept: smaller Rust futures

A run's future was 5,720 bytes, measured with `-Zprint-type-sizes`. Most of it was room for paths a plain run doesn't take, and one held twice:

- `run_with` handed `start`'s future (2,192 bytes) to `observed` unboxed. `observed` kept it both as its argument and inside the wrapper it awaited.
- `observed` kept room for `fail_running` (2,072 bytes), the save a durable run makes when it fails.
- `start` kept room for `save_checkpoint` (2,008 bytes), a durable run's first save.

Boxing the three, as the streaming entry already boxed `start`, made a run's future 1,344 bytes and `start`'s 320. A run waiting in its branches then held 17.7 KB instead of 22.6, and in `Plan` 7.8 instead of 11.8.

The burst got slower all the same:

| Burst of 100,000 runs, spawned as `tokio::spawn(graph.run(...))` | before | with the boxes |
|:--|--:|--:|
| Windows, system allocator | 69,000–74,000 runs/s, P50 560–620 ms | 63,000–67,000 runs/s, P50 810–830 ms |
| Linux container, glibc | 57,000–58,000 runs/s, P50 1,050–1,090 ms | 51,000–53,000 runs/s, P50 1,510–1,540 ms |
| A million runs, Linux | 48,800 runs/s | 45,900 runs/s |

Under mimalloc it was faster (a million runs: 98,000 to 107,000 runs a second). Undoing any one of the three boxes didn't bring the rate back. Adding 4,400 bytes of padding to each task of the boxed build did: 71,000–72,000 runs a second on Windows, 57,000 on Linux. So the slowdown came from each run's task being smaller, through how the allocator and the scheduler handled it, not from work the engine did. The boxes aren't kept: #37 is about the rate, and what a Rust run holds isn't what limits it. Measured 2026-11-18; the numbers are in "What a run costs now".

### The Rust CPU profile, and a branch's two allocations

A CPU profile of the Rust burst, taken after the second part with `perf` (`--call-graph dwarf`, 999 Hz) in a Linux container: Rust 1.91, glibc 2.36. The container had the `PERFMON`, `SYS_PTRACE` and `SYSLOG` capabilities, so it needed no elevated shell on the host. Each sample was attributed to the nearest meaningful frame on its stack: tokio's timers, tokio's scheduler, a cancellation token, the engine, or the workload's handlers.

- **Plain code is bound by tokio's timer driver.** A third of its samples are a contended mutex, and 45% are timer handling (`Sleep::poll`, `clear_entry`, `reregister`). In the kernel that is futex spinning. It keeps about 10 of the 24 cores busy, at about 92 µs of CPU a run.
- **The engine's burst is bound by the allocator.** About 22% of its samples are in glibc itself (`malloc`, `free`). In the kernel, 10% are `mprotect`, which glibc calls as it grows a thread's heap, contending on the process's memory-map lock (`osq_lock`, `rwsem_spin_on_owner`). About 7% are futex calls on glibc's arena locks (`__lll_lock_wake_private`), and 3.5% are page faults on new memory. Together that is about 40% of the engine's CPU, which is about 190 µs a run.
- **The rest is spread out.** Of the engine's samples, about 22% are tokio's scheduler and the task each branch runs as (creating, waking, joining it), 16% `tracing`'s wrappers and other code, 5% cancellation tokens, and the engine's own code the rest, with no function above a few percent.

The allocator, swapped on the same build (100,000 runs at once, three runs each):

| Allocator | strata | plain |
|:--|--:|--:|
| glibc | 50,000–55,500 runs/s, 180–200 µs of CPU a run | 111,000–116,000 runs/s |
| glibc, `GLIBC_TUNABLES=glibc.malloc.top_pad=268435456:glibc.malloc.trim_threshold=1073741824` | 57,000 runs/s, 170–175 µs | 113,000–116,000 runs/s |
| mimalloc | 73,000–83,000 runs/s, 130–150 µs | 136,000–138,000 runs/s |

A million runs: 49,600 a second on glibc, 82,800 on mimalloc.

Counted by call site, a branch made two allocations for nothing:

- **A second box.** An async branch's handler returns a boxed future. The fan-out wrapped it to catch its panic and boxed the wrapper again, because a branch's work was a boxed future of any kind. It is now an enum: the handler's future held in place with its panic caught around it, or a sync branch's boxed wait on the blocking pool. A map's item takes the first form too.
- **A copy of each deciding branch's name.** `BranchOutcomes` held the names of the branches that met the join as a `Vec<String>`, copied from the group's names on every run; under `All` that is every branch. It now holds their indexes, and builds the names the first time `deciding()` is read, as the idempotency key is built. A reducer that reads them sees the same list.

That made a run 120.9 allocations instead of 137.9, and a branch 7.2 instead of 9.2. The rate barely moved. On Windows' allocator it rose by about 1–2% (72,500–73,700 runs a second, against 69,700–74,000), and on Linux it stayed within run-to-run noise with either allocator: 17 allocations of about 120 don't decide how fast glibc serves them.

---

## What a run costs now

| | Before | After the first part | Now |
|---|---:|---:|---:|
| .NET, a node (async handler that completes at once) | 1,008 bytes | 728 bytes | 728 bytes |
| .NET, a branch (async handler that completes at once) | 1,307 bytes | 943 bytes | 707 bytes |
| Rust, an async branch | 11.9 allocations | 9.2 allocations | 9.2 allocations |
| .NET burst, allocated a run | 29.5 KB | 24.2 KB | 19.5 KB |
| .NET burst, held a run waiting in its branches | 14.8 KB | 12.6 KB | 10.9 KB |
| .NET burst of 100,000 runs, DATAS off | 66,000–81,000 runs/s | 91,000–92,000 runs/s | 132,000–145,000 runs/s |
| .NET burst of 100,000 runs, DATAS on | 13,700–39,500 runs/s | | 53,000–62,000 runs/s |
| .NET burst of a million runs, DATAS on / off | 61,900 / 85,600 runs/s | | 84,900 / 140,300 runs/s |
| Rust burst, allocated a run | 159.9 allocations, 30.1 KB | 138.9 allocations, 26.8 KB | 137.9 allocations, 25.9 KB |
| Rust burst, held a run waiting in its branches | 25.7 KB | 22.6 KB | 21.7 KB |
| Rust burst of 100,000 runs, Windows' allocator | 65,300–65,900 runs/s | 69,000–74,000 runs/s | 68,500–72,500 runs/s |
| Rust burst of 100,000 runs, Linux container | | 52,300–52,600 runs/s | 54,600–55,500 runs/s |
| Rust, an async branch, after the CPU profile | | | 7.2 allocations |
| Rust burst, allocated a run, after the CPU profile | | | 120.9 allocations, 26.1 KB |

A Rust node already made one allocation, and makes one still. With DATAS on, .NET's burst after the first part was still bimodal, so a single number would say little; after the second it isn't, and the GC pauses for 6–7% of a 100,000-run burst with DATAS off, against 30% before. The Rust bursts from the second part on are spawned as an application would (`tokio::spawn(graph.run(...))`), one allocation fewer than the first part's harness, which boxed each run; the Linux numbers are from one session, compared within it.

---

## Hosting a burst

What the measurements say for an application that starts many runs at once and has them wait on I/O. Measure your own workload before changing either default.

- **.NET: DATAS.** Server GC's dynamic heap count is on by default since .NET 9. It keeps a process's memory small by starting with one heap and adding heaps as it judges they're needed. A burst of 100,000 runs in flight didn't get it past three, and spent most of its time in pauses. To turn it off, set `<GarbageCollectionAdaptationMode>0</GarbageCollectionAdaptationMode>` in the application's project, or `DOTNET_GCDynamicAdaptationMode=0` in its environment. Server GC then keeps a heap per core, which costs memory. This is the application's setting: a library can't set the GC mode, so neither `Datalium.Strata` nor `Datalium.Strata.Hosting` does.
- **Rust: the allocator.** On Windows the system allocator held a burst of a million runs to about 6 cores; mimalloc tripled its rate. On Linux, glibc's `malloc` is about 40% of the engine's CPU in a burst, and mimalloc made it 1.6 times faster ("The Rust CPU profile"). Without changing allocator, letting glibc grow its heaps in larger steps (`GLIBC_TUNABLES=glibc.malloc.top_pad=268435456:glibc.malloc.trim_threshold=1073741824`) gave about 10%. A library shouldn't choose the global allocator, so no Strata crate does. An application chooses it with `#[global_allocator]`.

---

## What is left

- **Rust: a task per branch.** About 22% of the engine's samples are tokio's work for the task each branch runs as. Running a group's async branches inside the group's own task would save most of it, and two allocations a branch, but CPU-heavy async branches would then no longer run on several threads at once. Not pursued.
- **Rust: the allocator.** The largest single cost, and the application's choice ("Hosting a burst").
- **Rust: a smaller `NodeContext`.** It is 248 bytes, moved into every async handler's future, and part of it is per run, not per node: the run id, the metadata, the telemetry. Those could live in the part it already shares, about 32 bytes less per context. Not pursued: the profile puts the context's cost (building and dropping it) at a few percent, and the futures' result says bytes held aren't what limits the rate.
- **Rust: a cancelled run can end failed.** Found by a test this work added for .NET. If a run is cancelled while a parallel group runs, Rust can end it `NodeFailed` with `no branches to run`, where .NET ends it `Cancelled`. The executor's `select!` polls the group before the token, and the group fails first once its branches have settled as cancelled. It was so before this work. Since fixed in #48, with the test's Rust twin (`design/17`, "Implementation Status").

Considered and not proposed: running a Rust group's async branches inside the group's own task instead of spawning one each, which would save the most but stop CPU-heavy branches from running on several threads at once; passing `NodeContext` by reference, which changes the API; pooling objects between runs.

---

## Wire format

None. No checkpoint, definition, history record or memory record changes.

## Migration

None. The API is the same in both engines.

## Tests

- **What a node and a branch allocate is measured** (`dotnet/tests/Datalium.Strata.Tests/AllocationTests.cs`, alone in its collection because it reads the process's allocations). The fewest bytes of five warm runs of a line of 11 and of 101 nodes, and of a group of 1 and of 9 branches, whose handlers complete at once, so the run stays on the test's thread. A node may allocate 760 bytes (it allocates 728), a branch 740 (707), the same on Windows and Linux. Building the key eagerly again fails it (840 bytes a node), and so do a linked token source per attempt (792) and, before the inbox, per branch (1,117).
- **A branch that ignores its token is still given up on** (`GroupControlsTests`, and the first two in `group_controls.rs`): it times out on time, it is cancelled when a sibling fails, and, in .NET, a run cancelled while it runs ends at once and the group still ends the branch's span. Taking the timer's registration out of the watcher makes the first hang; making the inbox ignore the token makes the last fail. All of them pass on main too: they say what the inbox must keep.
- **A late failure of a branch given up on is observed** (`GroupControlsTests`, .NET only, since Rust drops such a branch's future): it never reaches `TaskScheduler.UnobservedTaskException`. A watcher that stops taking results once the group has given up fails it.
- **What a branch allocates is counted** (`rust/strata-bench/tests/allocations.rs`): an async branch may make 8 allocations (it makes 7.2; 9.2 after the first part). A child token per branch again fails it, and so do a second box of the handler's future and a copy of the deciding names on every run (8.2 each).
- **The idempotency key is the node's when it is read late** (`NodeContextTests`, the twin of `rust/strata-core/tests/node_context.rs`): read after the run, a node's and a branch's keys are what they were. A context built by hand has an empty key, or the one it was given.
- Everything else is the existing suite, which is what says the behaviour is the same: branch timeouts, sibling cancellation, joins, maps winding down, retries, durable execution's keys, events and spans.

## Review decisions

1. **Is the change to DATAS a default the engine should make?** **No: it can't.** The GC mode is the application's runtime configuration. The engine documents it ("Hosting a burst").
2. **Should a host crate or `Datalium.Strata.Hosting` offer a faster allocator, or turn DATAS off?** **No.** Both change the whole process, not just Strata's part of it: a library can't set the GC mode (decision 1), and the allocator was decided after the profile (decision 8).
3. **Two parts or one?** **Two**, so the rewrite of the .NET group's wait is reviewed on its own.
4. **Should `benchmarks/RESULTS.md` be run again for this?** **Not for these parts.** None of its scenarios is a burst of runs that wait, and the next full run will include both.
5. **Keep the smaller Rust futures?** **No** (decided in review, 2026-11-18). They held a quarter to a third less, and ran about 10% slower with the default allocators.
6. **Fix the Rust cancellation bug here?** **No** (decided in review): an issue and a change of its own, so this one stays about #37.
7. **Should the inbox's cancellation wake run inline, as `WaitAsync`'s did?** **On the thread pool.** Inline, the loop would go on to end the node, and maybe the run, inside whatever cancelled the token. The observer sees the same events either way: 20 runs of a cancelled run with a stuck branch each gave the same events on main and on this change.
8. **Should a host crate offer mimalloc?** **No** (decided in review, 2026-11-18, after the profile). The profile is recorded, and "Hosting a burst" names the choice and its numbers; the application makes it.
9. **Is #37 done?** **Yes**, with the third part. .NET's burst runs at about 0.8 of plain code's rate with DATAS off. What is left in Rust is the allocator, which is the application's, and a task per branch, which is a design decision.

## Implementation Status

The first part built 2026-11-18 (#46), in both engines; the second (#47) and the third the same day.

**How the third part was built:**

- **Rust, `parallel.rs`:** `BranchWork` is an enum, `Caught` (the handler's future in place, `CatchUnwind` around it) or `Boxed` (a sync branch's wait); `BranchOutcomes` holds `deciding_at` and builds `deciding` in a `OnceLock`.
- **Rust, `map.rs`:** an item's work is `BranchWork::caught`.
- **Tests:** `allocations.rs`'s per-branch limit is 8 (it makes 7.2). Boxing the handler's future again, or copying the deciding names on every run again, makes it 8.2 and fails it. The join tests read `deciding()` as before.
- **Documents:** this one; `design/38`'s count of what a branch allocates; `benchmarks/README.md`; `CHANGELOG.md`.

**How the second part was built:**

- **.NET, `FanOutRunner.cs`:** `Inbox` (an `IValueTaskSource` over `ManualResetValueTaskSourceCore`, a fixed buffer of reports, one registration on the attempt's token) and `Branch` (the watcher) replace `Settle`, `Task.WhenAny` and the dictionary of running tasks; `Settled` is a struct.
- **Rust, `parallel.rs`, `graph.rs`:** `run_branches` takes `&AttemptInputs`, and its launcher borrows the context and the stream.
- **Tests:** four in `GroupControlsTests.cs`, two in `group_controls.rs`; `AllocationTests.cs`'s branch limit.
- **Documents:** this one; `design/14` and `design/17`, which named `Task.WhenAny`; `CHANGELOG.md`.

**How the first part was built:**

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
- **The .NET test's handlers complete at once.** It first had them yield, as a handler that awaits real work does. That measured what suspending costs too, but not the same way twice: on Linux a branch varied between 922 and 1,177 bytes from run to run, depending on where the thread pool ran each continuation. What a node and a branch allocate for nothing they allocate either way.
- **No CPU profile of the Rust engine**, for the reason in "How it was measured".
- **The burst's numbers are from repro programs kept outside the repository.** They're the issue's program and its variants, not part of `strata-bench`.
- **The second part dropped what it set out to do in Rust**, smaller futures, after measuring them ("Tried and not kept").
- **The .NET cancelled-run test has no Rust twin yet**, because of the bug in "What is left".

**Tests of the first part,** with Postgres and Redis from `docker-compose.yml` and `STRATA_REQUIRE_BACKENDS=1`:

- **`dotnet/`:** 1,345 passed and 12 skipped, the live cloud and model tests, which need credentials; the example's 1 passed.
- **`rust/`:** 1,148 passed and 1 ignored. One failed, `postgres_settings_open_a_memory_store_that_is_migrated_or_only_checked`, on main too: the local database was reused, so pgvector's type was already in `public` and the test's own `search_path` didn't see it. CI starts from an empty database.
- **`cli/`:** 86 passed. **`examples/rust-agent`:** 1 passed.

`cargo fmt`, `clippy -D warnings` and `dotnet format` are clean.

**Tests of the second part,** the same way: `dotnet/` 1,349 passed and 12 skipped, twice; `rust/` 1,150 passed and 1 ignored, with the same local `postgres_settings_…` failure as above, which CI passed in #46; `cli/` 86 and `examples/rust-agent` 1 passed. The formatters and linters are clean.

## Not built (deliberately out of scope)

- **What is left**: see "What is left".
- **A burst scenario in `strata-bench` and the .NET benchmarks.** The repro programs measured this one; a scenario of its own would have to wait seconds per sample.
- **Changing DATAS, or the allocator, for the application.**
