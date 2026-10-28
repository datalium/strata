# Datalium Strata: Benchmarks

Strata says it is fast and has never measured it. The README promises "sub-millisecond graph traversals". The 2026-09-21 evaluation answers: "There are no benchmarks in the repo. The copy should be softened or backed with measurements", and makes benchmarks one of the four gates for 0.9. The only numbers anywhere are two lines in `design/33-Durable-Execution.md`, "Cost", taken once by hand.

That leaves four questions nobody can answer:

- **What does the engine cost per node?** An application's nodes call models and tools that take hundreds of milliseconds, so the engine's share is small. How small, and how it is distributed, is unknown.
- **What does it do under load?** How many transitions a second, with how many runs at once.
- **What does a suspended thread hold?** STRATA-SPEC-002 says "threads consume zero resources while suspended". Nothing checks it.
- **What does durability cost on a real store?** A checkpoint per node (`design/33`) is one store append per node. Which store, and how long, decides whether an application can afford it.

This document closes the gap in the engine: **SG-27**. The SG numbers refer to the gap log of StrataOps (not public yet). The gap asks for BenchmarkDotNet and `criterion` measuring per-node scheduling overhead (P50 to P99.9), transitions per second, memory per suspended thread, checkpoint write throughput per store, and fan-out cost. Under strict parity (StrataOps ADR 0003) both engines measure the same scenarios.

It is written as a blueprint before implementation. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged and what the first run found.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| The tools | **Two in each engine.** Microbenchmarks with `criterion` (Rust) and BenchmarkDotNet (.NET), for what a run costs on average. A **profile**, a small program of the repository's own, for what those two can't report: latency percentiles to P99.9, throughput under concurrency, memory, and the stores. |
| The scenarios | **One catalogue, the same in both engines:** same graphs, same state, same parameters, same names. A number in one engine is about the same thing as the number beside it. |
| What the engine's cost is measured against | **A store that costs nothing** (a thread's latest checkpoint in a dictionary) and **handlers that do nothing**. What is left is the engine. |
| Handler kinds | **Both.** A sync handler runs off the executor (tokio's blocking pool, the .NET thread pool) and an async one on it, and the difference is one of the things worth knowing. |
| Per-node overhead | **Measured from inside the handlers:** the gap between one handler returning and the next one starting, a sample per transition, reported P50 to P99.9. |
| Memory per suspended thread | **Heap growth over ten thousand suspended threads**, against the in-memory store (where the bytes are the checkpoint) and against SQLite (where the process should keep nothing). |
| The stores | **Every built-in store**: in-memory, SQLite, Postgres, Redis. Saves and loads a second, and a ten-node run that checkpoints every node. Postgres and Redis are the repository's `docker-compose.yml`, through the variables the tests read. |
| Where the code lives | **`rust/strata-bench`** (a workspace crate, `publish = false`) and **`dotnet/benchmarks/Datalium.Strata.Benchmarks`** (in the solution, not a package). |
| Where the numbers live | **`benchmarks/RESULTS.md`**, one full run on one named machine, replaced by hand. `benchmarks/README.md` says how to run and how to read them. |
| CI | **Every pull request runs every benchmark once**, with small counts, to prove they still run. **Nothing is compared:** a shared runner is too noisy to gate on. |
| The README's claim | **Backed or changed by the first run** (below). |
| Wire format | **None.** |

---

## Usage

```bash
# The microbenchmarks
(cd rust && cargo bench -p strata-bench)
dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- --filter '*'

# The profile (with Postgres and Redis from docker-compose.yml)
(cd rust && cargo run --release -p strata-bench -- --out ../rust-profile.json)
dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- profile --out dotnet-profile.json

# One scenario, or everything once with small counts
(cd rust && cargo run --release -p strata-bench -- --only throughput)
dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- profile --quick
```

---

## Semantics

### Two tools, and why

`criterion` and BenchmarkDotNet are the right tools for "how long does this take": they warm up, pick the iteration count, and report a mean with its confidence interval (and in .NET the bytes allocated). They are the wrong tools for three of the gap's five measurements:

- **Percentiles of single operations.** Both report statistics over iterations of many operations. A P99.9 of one transition needs every transition timed.
- **Memory held, not allocated.** A suspended thread's cost is what stays on the heap after the run returned.
- **Throughput under concurrency, and I/O.** Many runs at once for a fixed time; thousands of saves to a database, each a millisecond.

So each engine has a **profile**: a program that times every operation itself, keeps the samples, and reports nearest-rank percentiles. It is the same program in both engines, with the same scenarios, variants and metric names.

### The scenarios

All of them run on one state, `Work`: a counter each node adds one to, and a payload string that only gives the state a size. The graphs:

| Graph | What it is |
|---|---|
| `linear(nodes, handler)` | `nodes` no-op nodes in a line, sync or async. |
| `durable()` | `linear(10, async)` compiled with `EveryNode`. |
| `group(branches)` | One parallel group of no-op async branches. |
| `map(items)` | One map over `items` items, each through a one-node child, at the default concurrency of 8. |
| `approval()` | `prepare`, `approve` (an interrupt boundary), `finish`. |
| `streamer(tokens)` | One streaming node that emits `tokens` tokens. |
| `timed(nodes, handler)` | `linear`, whose handlers record the gap between them. |

The store is `LatestOnlyStore`: a dictionary from thread id to its latest checkpoint, which also counts saves. A thread can be run a million times without a log growing.

### Microbenchmarks

| Group | Parameters | What it says |
|---|---|---|
| Traversal | async and sync; 1, 10 and 100 nodes | The cost of a run, and so of a node. One node shows the fixed cost of a run. |
| State size | 0 and 16 KiB | What a state's size costs a run that doesn't checkpoint. |
| Fan-out | a group of 2, 8, 32 branches; a map of 8, 64 items | The cost of a branch, and of an item (a child graph run). |
| Interrupt | a round trip | Freeze, flush, evict, rehydrate: a run to a boundary and its resume, with a 1 KiB state. |
| Checkpointing | `Boundaries`, `EveryNode` | What a checkpoint per node costs the engine before any store's I/O: building eleven checkpoints of a 1 KiB state. |
| Streaming | 1,000 tokens | The cost of a token through the bounded channel to a reader. |

### The profile

| Scenario | Method | Metrics |
|---|---|---|
| `transition_latency` | Runs of `timed(100, handler)`. Each handler reads the clock when it starts and when it returns; the gap from one node's return to the next node's start is a sample. 2,000 runs, so 198,000 samples. | `p50_ns`, `p90_ns`, `p99_ns`, `p999_ns`, `max_ns`, `mean_ns` |
| `run_latency` | `linear(10, handler)`, one run after another, each timed: 100,000 async, 20,000 sync. | the same |
| `throughput` | 1, 4, 16 and 64 tasks, each running `linear(10, handler)` in a loop for two seconds. | `transitions_per_sec`, `runs_per_sec` |
| `suspended_memory` | 10,000 runs of `approval()` with a 1 KiB state, each to its boundary, on distinct threads. The heap before and after. | `heap_bytes_per_thread`, `checkpoint_json_bytes`, and for SQLite `disk_bytes_per_thread` |
| `store_save` | An `interrupted` checkpoint with a 1 KiB state, saved under one thread id after another: by one writer, each save timed; then by sixteen writers at once. | `ops_per_sec`; for one writer `p50_ns`, `p99_ns` |
| `store_load` | The latest checkpoint of each thread the one writer saved. | the same |
| `durable_run` | `durable()` with a 1 KiB state against the store, each run timed. | `mean_ns`, `p50_ns`, `p99_ns`, `checkpoints_per_run` (11) |

The store scenarios run 20,000 operations and 2,000 runs against the in-memory store, and 2,000 and 200 against the others.

**Warm-up.** Before anything is sampled, a latency scenario runs its graph for a second and discards what it recorded, and a store scenario makes fifty saves and loads and three durable runs. The .NET JIT compiles a method again once it has run for a while, and until then the code is several times slower; a store's first calls open, prepare and cache. The Rust profile warms up the same way so the two stay alike.

**The transition gap** is the engine's whole share of a transition: committing the state, choosing the edge, the hop count and the cancellation check, the node's events and spans, and dispatching the next handler. It includes one read of the clock. The first node of a run has no gap; a run's fixed cost shows in `run_latency` and in the one-node traversal.

**The heap** is, in Rust, the bytes the global allocator handed out and didn't get back while the threads were suspended (a counting allocator, switched on for this scenario only), and in .NET the managed heap's growth after a full collection. Neither sees SQLite's own page cache, which is bounded and not per thread.

**The stores.** Postgres and Redis are measured when `STRATA_TEST_POSTGRES_URL` / `STRATA_TEST_REDIS_URL` are set, in a schema and under a key prefix of the profile's own (`strata_bench_<pid>`), removed afterwards. Unset, the store is left out and the report says so. SQLite is a file in the system's temporary folder.

### What the numbers are, and aren't

- **One machine's.** `RESULTS.md` names it. A run is comparable with another run on the same machine.
- **Not a race between the engines.** The scenarios are identical so that a difference can be explained. The runtimes still differ: Rust hands each async handler a clone of the state; .NET shares an immutable record and allocates on a collected heap.
- **A floor for the stores.** Postgres and Redis in a container on the same machine have no network between them and the engine.
- **Not an application.** A node that calls a model takes a hundred thousand times longer than the engine's share of it.

### CI

Both workflows run every benchmark once on every pull request: the criterion benchmarks in test mode, BenchmarkDotNet with its `Dry` job, and each profile with `--quick`, against the workflow's Postgres and Redis. A benchmark that no longer compiles, or that fails when it runs, fails the build. No number is recorded or compared.

---

## Wire format

Unchanged. The profile's `--out` file is not a wire format: it is a report (`engine`, `version`, `quick`, `started_at`, `machine`, `results` of `scenario` / `variant` / `metrics`, `notes`), the same shape from both engines so the two can be read side by side.

---

## Migration

Nothing to migrate. A new crate in the Rust workspace and a new project in the .NET solution, neither published.

**StrataOps** measures end to end and keeps doing so; engine-level numbers now come from here (the gap's "Why StrataOps needs it").

---

## Tests

- **The scenarios do what a number about them claims**, in both engines: a line of N nodes ends at step N and saves nothing; the durable line saves eleven checkpoints and ends `completed`; a group runs every branch and a map every item; an approval freezes once and a resume finishes it, and the same thread id runs again; the streamer emits its tokens; the timed line records a gap per transition and none for a run's first node.
- **Percentiles** are nearest-rank, and the same in both engines.
- **Every benchmark runs** in CI, once.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

1. **A profile of our own beside the two libraries?** **Yes.** Three of the gap's five measurements are not means of a loop.
2. **Where do the results live?** **In the repository, one file, replaced by hand.** A claim in the README needs something to point at. A history of runs, or a page generated from the JSON, is "Not built".
3. **Should CI gate on performance?** **No.** A shared runner varies by more than any regression worth catching. CI proves the benchmarks run.
4. **Reuse the tests' variables for Postgres and Redis?** **Yes.** One compose file and one pair of variables, and a sandbox of the profile's own so nothing else is touched.
5. **Does the benchmark crate join the workspace, and the project the solution?** **Yes.** Then every build compiles them and every lint covers them: a benchmark that rots is worse than none.
6. **Server or workstation GC in .NET?** **Server.** It is what a hosted service runs with, and the report says which was used.
7. **Compare against LangGraph?** **No.** `design/07` wants it for the launch; it is a different piece of work with its own fairness problems, and belongs with the website, not the engine.
8. **Does the README change?** **Only the sentence that makes the claim**, to say what was measured and where.

---

## Implementation Status

Built 2026-10-11, as specified above, in both engines. The first full run is in `benchmarks/RESULTS.md`. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**What the first run found.** `RESULTS.md` has the tables; these are the findings that change what someone should believe.

- **The README's claim holds.** A hundred async nodes traverse in 50 µs in .NET and 185 µs in Rust. The slowest case measured, a hundred sync nodes in Rust, takes 0.77 ms. The engine's share of a transition is 0.4 µs at the median in .NET (P99.9 1.5 µs) and 1.8 µs in Rust (P99.9 9 µs). The README's sentence now says that and links to the results; "optimal CPU utilization", which nothing measures, is gone from it.
- **The .NET engine is faster than the Rust engine**, by between 2.6 and 7.5 times in every benchmark without I/O, and 4.3 times on a plain ten-node traversal. That is the opposite of what the project's pitch assumes, and this work does not explain it: it is a measurement suite, and the Rust hot path was not profiled. Two contributors are visible in the numbers. Rust clones the state into every async handler (a 16 KiB state adds 36% to a ten-node run; in .NET it adds nothing). A sync handler's trip to the blocking pool and back costs about 5 µs in Rust, against about 1 µs for the thread pool in .NET, and sync throughput in Rust stops at about 0.4 million transitions a second however many runs are going. Neither accounts for an async node costing 1.8 µs with an empty state. **Profiling the Rust executor's per-node path is the follow-up this suite exists to make possible.** (Done 2026-10-13, `38-Rust-Executor-Overhead.md`: the engine made 28 heap allocations a node. It makes one now, a node costs 0.4 µs, and `RESULTS.md` has the Rust figures from after it.)
- **A suspended thread holds nothing in the process**, as STRATA-SPEC-002 says: against SQLite, ten thousand frozen threads grew the heap by nothing in either engine. The in-memory store holds its copy of the checkpoint: 1.7 KB in .NET and 3.6 KB in Rust for 1.25 KB of JSON.
- **A checkpoint per node costs a store round trip per node.** A ten-node `EveryNode` run takes 3 ms on Redis, 15 to 20 ms on Postgres and 26 ms on SQLite, against 12 µs (.NET) or 34 µs (Rust) when the store costs nothing. For nodes that call a model that is noise; for a graph of cheap nodes it is the whole cost.
- **The Rust Postgres store has one connection, so its writers queue:** 593 saves a second from sixteen writers against 551 from one. The .NET store pools and reaches 6,215. A pool for the Rust store is a storage follow-up.
- **SQLite saves are bound by the disk** (about 2.3 ms each, one transaction per save) and do not scale with writers, in either engine.
- **`design/33`'s cost figures for .NET were about ten times too high** (4.1 µs a node; it is 0.4 µs). The Rust figures there hold. That section now points here.

**Where it diverged, or filled a gap, and why:**

- **A durable ten-node run writes eleven checkpoints:** `running` before the first node and after each node but the last, whose commit is the `completed` one (`design/33`). The build first assumed twelve; a test in each engine pins eleven.
- **The profile warms up by time, not by count.** The first full run of the .NET profile sampled code the JIT had not finished with: the median transition was 0.6 µs and P90 3.1 µs. A second of runs first gives 0.4 µs and 0.5 µs. The Rust profile does the same.
- **The .NET SQLite store is opened with pooling on**, as an application opens it. The first run opened it with `Pooling=False` (the tests' setting, which lets them delete the file), and every call paid for opening the file: a load took 1.05 ms instead of 0.14 ms.
- **The .NET scenario tests compile the scenarios in by link** (`Scenarios.cs` and `Stats.cs` are `Compile` items of the test project). A project reference would have brought BenchmarkDotNet, and its Roslyn, into the project that drives the source generator with a pinned Roslyn.
- **Rust CI runs the benchmarks unoptimized.** `cargo test --benches` runs each criterion benchmark once in the build the tests already made, and the profile runs with `--quick` from the same build (it says it is a debug build and goes on). An optimized build only to run each benchmark once would cost minutes per pull request.
- **Streaming is reported per token in .NET** (`OperationsPerInvoke`) and per run of a thousand tokens in Rust, where criterion also prints tokens a second. `RESULTS.md` gives both per token.
- **Rust benchmarks clone a state built outside the loop.** A run takes its state by value, so each iteration needs one; .NET passes the same immutable record every time.
- **The Rust memory scenario counts through a global allocator that is off except while it runs**, so the other scenarios share the binary at the cost of one flag read per allocation.
- **`rust/Cargo.lock` is not tracked**, so the new crate changes no lock file in the repository.

**How it was built:**

- **Rust:** `rust/strata-bench` (workspace member, `publish = false`): `src/lib.rs` (the scenarios, `Gaps`, `LatestOnlyStore`, `completed` / `interrupted`, `sample_checkpoint`), `src/stats.rs` (`Distribution`, `percentile`), `benches/engine.rs` (criterion 0.8, without its default features), `src/main.rs` (the `strata-bench` profile).
- **.NET:** `dotnet/benchmarks/Datalium.Strata.Benchmarks` (in the solution under `benchmarks`, server GC, BenchmarkDotNet 0.15.8): `Scenarios.cs`, `Stats.cs`, `EngineBenchmarks.cs` (one class per criterion group, `[MemoryDiagnoser]`), `Profile.cs`, `Program.cs` (`profile …` runs the profile; anything else goes to BenchmarkDotNet, and a failed benchmark is exit code 1).
- **Documents:** `benchmarks/README.md`, `benchmarks/RESULTS.md`; the README's sentence and a line in its "Status"; `CHANGELOG.md`; a pointer in `design/33`, "Cost".
- **Workflows:** a "Run Each Benchmark Once" step in both, after the tests.

**Tests:**

- **Scenarios:** 9 in `rust/strata-bench` (7 scenarios, 2 percentiles) and 9 in `dotnet/tests/Datalium.Strata.Tests/Benchmarks/BenchmarkScenarioTests.cs`, the same ones.
- **Totals when this landed:** .NET 842, with Postgres and Redis running, and 2 skipped (the opt-in Cloud live tests), plus the example's 1. `rust/` 777 and `cli/` 80, with `STRATA_REQUIRE_BACKENDS=1` against live Postgres and Redis, and `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` and `dotnet format --verify-no-changes` are clean; `versions.sh`, `pack-smoke.sh` and `cargo package` pass with the new crate and project in place (the packaging check now leaves the benchmark crate out: `cargo package --workspace --exclude strata-bench`).
- **Every benchmark ran:** 17 criterion benchmarks and 17 BenchmarkDotNet ones, and both profiles in full against all four stores. The profiles left no schema in Postgres, no key in Redis and no folder in the temporary directory.

## Not built (deliberately out of scope)

- **Any fix for what the first run found.** ~~Profiling the Rust executor's per-node path~~ (built 2026-10-13, `38-Rust-Executor-Overhead.md`), and a connection pool for the Rust Postgres store, are follow-ups of their own.
- **A regression gate in CI**, and a history of runs.
- **A results page generated from the profiles' JSON.** `RESULTS.md` is assembled by hand.
- **A comparison with LangGraph or any other framework.**
- **Tool dispatch, subgraphs, retries and the drivers** as benchmarks of their own. Tool dispatch is the next candidate.
- **Load, soak and fuzz tests** (the evaluation's §8 lists them separately).
- **A network between the engine and its store,** and Strata Cloud.
- **Linux numbers.** The first run is on Windows; CI runs the benchmarks on Linux but records nothing.
