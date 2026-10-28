# Benchmarks

What Strata costs, measured in both engines with the same scenarios. The design is `design/37-Benchmarks.md`; the numbers from the last full run are in [`RESULTS.md`](RESULTS.md).

There are two tools in each engine, and they answer different questions.

| | Rust | .NET | Answers |
|---|---|---|---|
| **Microbenchmarks** | `rust/strata-bench/benches/engine.rs` (criterion) | `dotnet/benchmarks/Datalium.Strata.Benchmarks/EngineBenchmarks.cs` (BenchmarkDotNet) | What a run costs on average when the handlers do nothing and the store costs nothing, and in .NET what it allocates. |
| **Profile** | `rust/strata-bench/src/main.rs` | `dotnet/benchmarks/Datalium.Strata.Benchmarks/Profile.cs` | How latency is distributed (P50 to P99.9), throughput under concurrency, memory per suspended thread, and what each store does. |

The scenarios themselves (the graphs, the state, the store that costs nothing) are in `rust/strata-bench/src/lib.rs` and `dotnet/benchmarks/Datalium.Strata.Benchmarks/Scenarios.cs`, and are the same in both.

## Running them

Close what you can first: a benchmark measures the machine as much as the code.

```bash
# The microbenchmarks
(cd rust && cargo bench -p strata-bench)
dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- --filter '*'

# One group of them
(cd rust && cargo bench -p strata-bench -- traversal)
dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- --filter '*Traversal*'

# The profile, with Postgres and Redis
docker compose up -d --wait
export STRATA_TEST_POSTGRES_URL=postgres://strata:strata@localhost:5432/strata
export STRATA_TEST_REDIS_URL=redis://localhost:6379
(cd rust && cargo run --release -p strata-bench -- --out ../rust-profile.json)
dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- profile --out dotnet-profile.json

# One scenario of the profile
(cd rust && cargo run --release -p strata-bench -- --only throughput)
dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- profile --only throughput
```

`--only` takes `transition_latency`, `run_latency`, `throughput`, `suspended_memory`, `stores` and `large_state` (the last two each run several scenarios of the table below), separated by commas.

The profile prints a Markdown table, and with `--out` also writes the same results as JSON. Without the two variables the Postgres and Redis rows are left out, and the profile says so. It makes a schema (`strata_bench_<pid>`) and a key prefix of its own and removes them when it is done.

`--quick` (profile) and `-- --test` / `--job Dry` (microbenchmarks) run everything once with small counts. That proves the benchmarks still run, which is what CI does on every pull request. It measures nothing.

## The microbenchmarks

Every one runs against a store that keeps a thread's latest checkpoint in a dictionary, so the time is the engine's.

| Group | Rust id | .NET | What runs |
|---|---|---|---|
| Traversal | `traversal/{async,sync,inline}/{1,10,100}` | `Traversal.Run` | A line of no-op nodes. The per-node cost is the time over the node count. |
| State size | `state_size/async/{0,16384}` | `StateSize.Run` | The ten-node line with an empty state and a 16 KiB one. |
| Fan-out | `fan_out/group/{2,8,32}`, `fan_out/map/{8,64}` | `FanOutGroup.Run`, `FanOutMap.Run` | One parallel group of no-op async branches; one map over no-op one-node children at the default concurrency of 8. |
| Interrupt | `interrupt/round_trip` | `Interrupt.RoundTrip` | A run to an interrupt boundary and the resume that finishes it, with a 1 KiB state. |
| Checkpointing | `checkpointing/{boundaries,every_node}` | `Checkpointing.Boundaries`, `.EveryNode` | The ten-node line with a 1 KiB state, without and with a checkpoint after every node (eleven saves). |
| Streaming | `streaming/tokens/1000` | `Streaming.Tokens` | One node emitting a thousand tokens to a reader that drains them. |

A sync handler runs off the executor (tokio's blocking pool, the .NET thread pool) and an async one on it, which is why traversal is measured with both. In Rust that is a thread hand-off each way, and the sync figures move from run to run with whether the threads stayed awake (`design/38-Rust-Executor-Overhead.md`, "What is left"): run them more than once before believing one. Rust also measures `inline`, a sync handler on the executor (`add_inline_node`, `design/39-Inline-Sync-Handlers.md`); .NET has no such handler, so those rows are Rust's alone.

## Allocations, and a loop for a profiler (Rust)

Two things that are not benchmarks, from the work that reduced what a node costs the Rust engine (`design/38-Rust-Executor-Overhead.md`):

```bash
# What a node and a run allocate, counted. It is a test, so it also fails if a node starts allocating again.
(cd rust && cargo test -p strata-bench --test allocations -- --nocapture)

# One scenario in a loop, for a sampling profiler to watch: [async|sync|inline] [nodes] [seconds].
(cd rust && cargo run --release -p strata-bench --example profile_loop -- async 100 10)
```

A count is the same on every machine, which is why it can be a test and a timing can't.

## The profile

| Scenario | Variants | Metrics |
|---|---|---|
| `transition_latency` | `async`, `sync`, `inline` (Rust) | The gap between one handler returning and the next starting, over runs of a hundred-node line: `p50_ns` to `p999_ns`, `max_ns`. This is the engine's scheduling overhead per transition. |
| `run_latency` | `async/10_nodes`, `sync/10_nodes`, `inline/10_nodes` (Rust) | A whole run of the ten-node line, one after another: the same percentiles. |
| `throughput` | `{async,sync,inline}/{1,4,16,64}_concurrent` (`inline`: Rust) | That many ten-node runs going at once for two seconds: `transitions_per_sec`, `runs_per_sec`. |
| `suspended_memory` | `memory`, `sqlite` | Ten thousand threads frozen at an interrupt boundary with a 1 KiB state: `heap_bytes_per_thread`, the checkpoint's size as JSON, and for SQLite `disk_bytes_per_thread`. |
| `store_save` | `<store>/1_writer`, `<store>/16_writers`, `/64_writers`, `/256_writers`, and each with `_batched` for the stores with group commit (SQLite, Postgres; `design/41`) | Saves of a 1 KiB checkpoint, a thread each: `ops_per_sec`, and for one writer without group commit `p50_ns` and `p99_ns`. With 64 and 256 writers, at least 32 saves each. |
| `store_load` | `<store>/1_reader` | The latest checkpoint of each of those threads: the same. |
| `durable_run` | `<store>/10_nodes` | The ten-node line checkpointing after every node against a real store: `mean_ns`, `p50_ns`, `p99_ns`. |
| `durable_throughput` | `<store>/64_concurrent`, and `_batched` with group commit | 64 workers each running that line one run after another: `runs_per_sec`, `saves_per_sec`. |
| `store_save_large` | `<store>/whole`, `<store>/delta`, `<store>/offload`, `<store>/delta_offload` (SQLite, Postgres, Redis; `design/41`) | A state whose 64 KiB payload never changes and whose step does, saved as a durable run would: a `running` checkpoint per node, twenty to a thread, one writer. Stored whole, by a store with deltas on (a whole row every 16), by one that offloads the payload to its own database (64 KiB is the default threshold), and by one with both. `ops_per_sec`, and `stored_bytes`, the mean JSON a save stored, its blobs included (`strata.store.checkpoint.size` and `strata.store.blob.size`). 400 saves against SQLite, 200 against the others. |
| `store_load_chain` | `<store>/whole`, `<store>/15_deltas`, `<store>/offloaded`, `<store>/15_deltas_offloaded` | Loading the latest checkpoint of a thread of 16 such rows: whole; with deltas, where it is 15 deltas from a whole row (the longest chain a load reads); with the payload offloaded (the load reads its blob and checks it); and both. `ops_per_sec`, `p50_ns`, `p99_ns`. |

`<store>` is `memory`, `sqlite` (a file), `postgres` and `redis`. The in-memory store keeps objects and has no deltas or offload, so the last two scenarios leave it out.

A metric's name ends with its unit. Percentiles are nearest-rank. Timers on Windows tick every 100 ns, so the smallest numbers there are multiples of that.

## Reading the numbers

- **They are one machine's.** Compare a run with another run on the same machine, not with `RESULTS.md`.
- **The engines are not racing each other.** The scenarios are the same so that a difference can be explained, and the two runtimes differ in ways no benchmark removes: Rust hands each async handler a clone of the state, .NET shares an immutable record; .NET allocates on a garbage-collected heap, under the server collector here.
- **Postgres and Redis are a container on the same machine.** The numbers are a floor for the store's own cost and say nothing about a network.
- **SQLite saves wait for the disk.** Each save is a transaction of its own, so its rate is the disk's.
- **A 64 KiB command to a container is slow on Docker Desktop.** On the machine `RESULTS.md` names, a single Postgres or Redis command carrying 32 to 64 KB takes about 44 ms however it is sent (the port forwarding and TCP's delayed acknowledgement; `design/41`, "Implementation Status"). `store_save_large`'s whole rows against Postgres and Redis measure that, not the store.

## Updating `RESULTS.md`

Run all four commands on a quiet machine, with Postgres and Redis up, and replace the tables and the "Measured on" block. (The file as it stands mixes two days, and says which figure is from which: the next full run should make it one again.) Say what the machine is and what else it was doing. Nothing compares runs automatically: a shared CI runner is too noisy to gate on.
