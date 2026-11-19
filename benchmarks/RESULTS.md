# Benchmark results

The suite in `design/37-Benchmarks.md`, measured on one machine. How to run it and how to read it: [`README.md`](README.md).

## Measured on

- **When:** 2026-11-19, one run of the four commands in `README.md`, on `main` at `6ba5421`, which is released as `0.9.0-preview.3`. The profiles print `0.9.0-preview.2` in their headings: the manifests had not been bumped yet. The same afternoon, some scenarios were measured again on `6ba5421` and on `v0.9.0-preview.2`, alternating the two (findings 4 and 5): in .NET the traversal, group and map microbenchmarks and the profile's latency and throughput, twice each; in Rust the sync and group microbenchmarks and the profile's latency, twice each, and its throughput three to five times each.
- **Version:** `0.9.0-preview.3`.
- **Machine:** Intel Core i9-12900KF (8 performance and 8 efficiency cores, 24 logical), Windows 11 Pro (build 26300), the Balanced power plan.
- **Runtimes:** .NET 10.0.15 (SDK 10.0.204, x64 RyuJIT, server GC), BenchmarkDotNet 0.15.8; Rust 1.91.1, `--release`, tokio's multi-threaded runtime.
- **Stores:** SQLite as a file on the local disk. Postgres 16 (`pgvector/pgvector:pg16`, on `tmpfs`) and Redis 8 from the repository's `docker-compose.yml`, in Docker Desktop on the same machine.
- **A quiet machine, nearly.** Other applications were closed; only VS Code, with a Claude Code session in it, was running. The slowest percentiles and the maxima still carry some noise.

Timers on Windows tick every 100 ns.

## What it shows

1. **The engine's share of a node is about 0.4 µs in both engines.** The median gap between one async handler returning and the next starting is 0.3 µs in .NET (P99.9 1.4 µs) and 0.4 µs in Rust (P99.9 0.9 µs).
2. **Traversals are sub-millisecond at every size measured.** A hundred async nodes take 40 µs in .NET and 41 µs in Rust. The slowest case, a hundred sync nodes in Rust, takes 0.57 ms.
3. **Rust is level with .NET on a line of async nodes and behind it elsewhere.** Where it is behind, the cost is outside the per-node path:
   - A run's own setup is about 0.9 µs in Rust and 0.2 µs in .NET, which is the whole difference on a one-node run.
   - A branch of a parallel group costs about 1.1 µs in Rust and 0.3 µs in .NET, and a map's item 1.7 µs and 0.6 µs: each is a task of its own.
   - A streamed token costs 397 ns in Rust and 85 ns in .NET, nearly all of it the channel.
   - Rust clones the state into every async handler, so a 16 KiB state adds 52% to a ten-node run; in .NET it adds nothing.
4. **A sync handler in Rust costs a thread hand-off each way, and on this machine that day each hand-off woke a sleeping thread.** A sync node cost about 4.6 to 4.8 µs between handlers and 5.6 µs a node in the microbenchmark, and sync throughput stayed between 168,000 and 309,000 transitions a second however many runs were going. `v0.9.0-preview.2`, measured the same afternoon alternately with `6ba5421`, gave the same figures (sync transition P50 4.6 to 4.8 µs on both; ten sync nodes 47.6 to 51.2 µs on `6ba5421` and 51.4 µs on `v0.9.0-preview.2`), so the figure is the machine's, not a change in the engine. On days when both threads stayed awake between nodes, a sync node measured about 1.1 µs; why this day's didn't was not chased. The same wake-up shows in the Rust parallel groups: a group of 2 branches costs about what a group of 8 does. Sync throughput with 16 and 64 runs at once is the one figure the comparison left open (#53): `6ba5421` gave 301,000 to 371,000 transitions a second in six runs, `v0.9.0-preview.2` 303,000 to 447,000 in five, reaching about 400,000 in three of them.
5. **Against `0.9.0-preview.2`, measured alternately the same afternoon, .NET is faster everywhere and Rust's parallel groups are faster** (`design/49-Burst-Throughput.md`).
   - **.NET:** a group of 32 branches takes 9.8 µs against 20.3 µs and allocates 24 KB against 45 KB; a map of 64 items 42 µs against 61 to 63 µs (106 KB against 157 KB); ten async nodes 3.2 µs against 4.2 µs (8.3 KB against 11.3 KB); a hundred sync nodes 121 µs against 136 µs. A ten-node async run's P50 is 2.6 to 2.7 µs against 3.6 µs. With 64 runs at once, async throughput is 29.2 to 29.4 million transitions a second against 19.6 to 20.8 million, and sync 10.2 to 10.3 million against 8.8 million.
   - **Rust:** a group of 32 branches takes 37 to 39 µs against 48 to 50 µs, and a group of 2 takes 11 to 14 µs against 17 µs. An async branch makes 7 allocations, counted by `rust/strata-bench/tests/allocations.rs`. Async and inline nodes, their latency and their throughput, are the same in both versions.
6. **Async throughput reaches about 10.5 million transitions a second in Rust and 26.7 million in .NET** with 64 runs at once. .NET sync handlers reach 9.9 million.
7. **A suspended thread holds nothing in the process.** Against SQLite, ten thousand frozen threads grew the heap by 0 bytes each in Rust and by nothing measurable in .NET, and took 1.6 KB each on disk. The in-memory store holds 1.7 KB (.NET) or 3.6 KB (Rust) each, which is its copy of a 1.25 KB checkpoint.
8. **A checkpoint per node costs a store round trip per node.** A ten-node run under `EveryNode` (eleven saves) takes 3 ms on Redis, 12 to 13 ms on Postgres and 27 ms on SQLite, against 11 µs (.NET) or 20 µs (Rust) when the store costs nothing.
9. **Without group commit, SQLite saves are bound by the disk** (about 2.4 ms each, one sync per save, about 400 a second however many write) **and Postgres stops scaling at about 7,500 saves a second.**
10. **Group commit multiplies what many writers save.** With 64 writers: SQLite 13 times in Rust (5,354 a second against 410) and 11 in .NET (4,543 against 401), Postgres 3.1 times in Rust (23,223 against 7,537) and 2.2 in .NET (16,355 against 7,590). 64 durable ten-node runs at once write 17,984 checkpoints a second to Postgres in Rust (7,194 without) and 14,088 in .NET (9,439). A writer alone gains nothing and pays a little in Rust: 943 saves a second against 1,014.
11. **Group commit with many writers varies from run to run.** Rust Postgres with 256 writers measured 9,397, below its own 64-writer figure; it was not repeated. When `design/41-Checkpoint-Efficiency.md` was built, six .NET runs gave 5,900 to 16,200 saves a second with 64 writers and 5,100 to 20,800 with 256, and four Rust runs 17,900 to 21,600 and 26,400 to 36,000; the cause isn't known.
12. **Deltas store a large state's unchanged payload once in sixteen rows.** A running checkpoint with a 64 KiB payload that never changes stores about 100 bytes as a delta against 65.6 KB whole; over a thread of twenty such rows (two of them whole: the first, and the sixteenth) a save stores 6.6 KB on average, a tenth. SQLite saves gain 2 to 11% (its rate is the disk's sync), Postgres and Redis 8 to 9 times, but there the whole rows are slow for a reason of this machine's (see the section). Loading a row fifteen deltas from a whole one costs 11 to 18% more than a whole one in Postgres, 1.3 to 1.8 times in Redis, where it is a second read, and 1.7 to 1.9 times in SQLite.
13. **Offload stores a large unchanged value once per thread, and Postgres and Redis save faster for it.** With the 64 KiB payload offloaded, a save stores 3.4 to 3.7 KB on average over twenty rows, against 6.6 KB as deltas and 65.6 KB whole. Postgres saves 271 to 298 a second and Redis 344 to 366, against 170 to 205 with deltas and 22 whole, because only the first save of a thread carries a 64 KB command. A load reads the blob too: about 0.9 ms in Postgres against 0.4 to 0.6 ms whole, and about 0.54 ms in Redis against 0.36 to 0.49 ms.

## Microbenchmarks

The mean time of one run. Every benchmark uses a store that costs nothing. The .NET column on the right is the managed memory one run allocates. `inline` runs a sync handler on the engine's own task, and exists in Rust only.

| Benchmark | Rust | .NET | .NET allocated |
|---|---:|---:|---:|
| Traversal, async, 1 node | 1.29 µs | 0.63 µs | 1.84 KB |
| Traversal, async, 10 nodes | 5.19 µs | 3.15 µs | 8.31 KB |
| Traversal, async, 100 nodes | 41.1 µs | 40.4 µs | 73.00 KB |
| Traversal, sync, 1 node | 6.21 µs | 1.79 µs | 3.15 KB |
| Traversal, sync, 10 nodes | 60.1 µs | 18.3 µs | 15.21 KB |
| Traversal, sync, 100 nodes | 567 µs | 122 µs | 134.61 KB |
| Traversal, inline, 1 node | 1.30 µs | | |
| Traversal, inline, 10 nodes | 5.05 µs | | |
| Traversal, inline, 100 nodes | 40.2 µs | | |
| State size, 10 async nodes, empty state | 5.17 µs | 3.16 µs | 8.31 KB |
| State size, 10 async nodes, 16 KiB state | 7.86 µs | 3.12 µs | 8.31 KB |
| Parallel group, 2 branches | 12.1 µs | 1.73 µs | 5.02 KB |
| Parallel group, 8 branches | 12.5 µs | 3.37 µs | 8.80 KB |
| Parallel group, 32 branches | 38.6 µs | 9.81 µs | 24.24 KB |
| Map, 8 items | 27.4 µs | 6.30 µs | 17.01 KB |
| Map, 64 items | 125 µs | 41.9 µs | 106.46 KB |
| Interrupt and resume, 1 KiB state | 7.24 µs | 4.35 µs | 12.52 KB |
| 10 nodes, 1 KiB state, `Boundaries` | 5.43 µs | 3.07 µs | 8.31 KB |
| 10 nodes, 1 KiB state, `EveryNode` | 20.0 µs | 11.0 µs | 29.78 KB |
| Streaming, per token (1,000 tokens) | 397 ns | 84.9 ns | 64 B |

The Rust sync rows and the Rust groups of 2 and 8 branches carry a thread's wake-up (finding 4).

Per node, an async traversal costs about 0.4 µs in both engines; a sync one costs 1.2 µs in .NET and 5.6 µs in Rust here. Per branch, a group costs about 1.1 µs in Rust and 0.3 µs in .NET; per item, a map costs about 1.7 µs and 0.6 µs.

Rust allocates on the heap only what it is told to, and `rust/strata-bench/tests/allocations.rs` counts it: one allocation for an async or inline node, two for a sync one, ten for a run besides its nodes', and seven for an async branch of a group.

## Profile

### Transition latency

The gap between one handler returning and the next starting, 198,000 transitions.

| Handler | Engine | P50 | P90 | P99 | P99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| async | Rust | 0.40 µs | 0.50 µs | 0.60 µs | 0.90 µs | 146 µs |
| async | .NET | 0.30 µs | 0.50 µs | 0.70 µs | 1.40 µs | 181 µs |
| inline | Rust | 0.40 µs | 0.50 µs | 0.60 µs | 1.60 µs | 163 µs |
| sync | Rust | 4.80 µs | 9.70 µs | 14.7 µs | 41.4 µs | 442 µs |
| sync | .NET | 1.20 µs | 1.50 µs | 2.40 µs | 8.40 µs | 10.9 ms |

### Run latency

A whole ten-node run: 100,000 async and inline runs, 20,000 sync.

| Handler | Engine | P50 | P90 | P99 | P99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| async | Rust | 4.80 µs | 5.00 µs | 6.40 µs | 15.6 µs | 475 µs |
| async | .NET | 2.90 µs | 3.50 µs | 5.60 µs | 24.3 µs | 1.36 ms |
| inline | Rust | 4.80 µs | 5.00 µs | 6.20 µs | 19.2 µs | 222 µs |
| sync | Rust | 47.3 µs | 69.0 µs | 101 µs | 237 µs | 380 µs |
| sync | .NET | 10.6 µs | 12.3 µs | 17.9 µs | 190 µs | 409 µs |

### Throughput

Transitions a second with that many ten-node runs going at once, for two seconds.

| Handler | Engine | 1 | 4 | 16 | 64 |
|---|---|---:|---:|---:|---:|
| async | Rust | 2,014,000 | 5,781,000 | 9,835,000 | 10,474,000 |
| async | .NET | 3,358,000 | 12,167,000 | 26,019,000 | 26,693,000 |
| inline | Rust | 1,985,000 | 5,247,000 | 9,903,000 | 10,815,000 |
| sync | Rust | 168,000 | 240,000 | 301,000 | 309,000 |
| sync | .NET | 854,000 | 2,556,000 | 6,828,000 | 9,894,000 |

### Memory per suspended thread

Ten thousand threads frozen at an interrupt boundary with a 1 KiB state. The checkpoint is 1,251 bytes of JSON.

| Store | Engine | Heap per thread | Disk per thread |
|---|---|---:|---:|
| in-memory | Rust | 3,608 B | |
| in-memory | .NET | 1,697 B | |
| SQLite | Rust | 0 B | 1,596 B |
| SQLite | .NET | −4 B | 1,596 B |

The −4 is the measurement's noise: the managed heap after a full collection, before and after.

### Stores

Saves and loads of a 1 KiB checkpoint, a thread each, without group commit. 20,000 operations against the in-memory store, 2,000 against the others; with 64 and 256 writers, at least 32 saves each.

| Store | Engine | Saves/s, 1 writer | Save P50 | Save P99 | 16 writers | 64 writers | 256 writers | Loads/s | Load P50 | Load P99 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| in-memory | Rust | 1,699,000 | 0.2 µs | 0.5 µs | 955,000 | 948,000 | 1,089,000 | 1,465,000 | 0.7 µs | 1.1 µs |
| in-memory | .NET | 5,324,000 | 0.1 µs | 0.5 µs | 4,627,000 | 3,439,000 | 16,323,000 | 6,973,000 | 0.1 µs | 0.5 µs |
| SQLite | Rust | 405 | 2.37 ms | 3.67 ms | 404 | 410 | 398 | 16,508 | 54 µs | 127 µs |
| SQLite | .NET | 401 | 2.37 ms | 3.90 ms | 396 | 401 | 398 | 11,558 | 79 µs | 176 µs |
| Postgres | Rust | 1,014 | 949 µs | 1.66 ms | 6,703 | 7,537 | 7,358 | 3,425 | 272 µs | 711 µs |
| Postgres | .NET | 903 | 1.06 ms | 1.75 ms | 6,942 | 7,590 | 5,036 | 1,876 | 518 µs | 859 µs |
| Redis | Rust | 3,912 | 245 µs | 441 µs | 32,733 | 61,717 | 55,622 | 4,065 | 234 µs | 427 µs |
| Redis | .NET | 3,371 | 282 µs | 561 µs | 16,204 | 59,962 | 60,709 | 3,961 | 237 µs | 481 µs |

The .NET in-memory figures with many writers are the machine's scheduling more than the store's: 3.4 million a second with 64 writers and 16.3 million with 256, from one dictionary.

### Group commit

The same saves through a store with group commit on (`design/41`, defaults: up to 256 saves a flush, 4 flushes at once in Postgres, 1 in SQLite). Redis has none: built and measured, it was no faster than the store without it (`design/41`, "Implementation Status").

| Store | Engine | 1 writer | 16 writers | 64 writers | 256 writers |
|---|---|---:|---:|---:|---:|
| SQLite | Rust | 382 | 2,064 | 5,354 | 7,095 |
| SQLite | .NET | 389 | 1,953 | 4,543 | 5,008 |
| Postgres | Rust | 943 | 7,834 | 23,223 | 9,397 |
| Postgres | .NET | 924 | 9,529 | 16,355 | 21,228 |

The Rust Postgres figure with 256 writers is one run, not repeated (finding 11).

### A durable run

The ten-node line with a 1 KiB state under `EveryNode`: eleven checkpoints a run. 2,000 runs against the in-memory store, 200 against the others.

| Store | Engine | Mean | P50 | P99 |
|---|---|---:|---:|---:|
| in-memory | Rust | 29.0 µs | 23.8 µs | 81.2 µs |
| in-memory | .NET | 21.3 µs | 11.8 µs | 27.9 µs |
| SQLite | Rust | 26.8 ms | 25.9 ms | 62.2 ms |
| SQLite | .NET | 26.6 ms | 25.6 ms | 32.3 ms |
| Postgres | Rust | 12.3 ms | 12.1 ms | 15.9 ms |
| Postgres | .NET | 13.2 ms | 13.0 ms | 16.5 ms |
| Redis | Rust | 2.92 ms | 2.82 ms | 3.88 ms |
| Redis | .NET | 3.03 ms | 2.96 ms | 4.61 ms |

### Concurrent durable runs

64 workers each running that durable line one run after another: checkpoints written a second, without and with group commit.

| Store | Engine | Without | With group commit |
|---|---|---:|---:|
| in-memory | Rust | 916,405 | |
| in-memory | .NET | 717,390 | |
| SQLite | Rust | 411 | 5,201 |
| SQLite | .NET | 404 | 4,918 |
| Postgres | Rust | 7,194 | 17,984 |
| Postgres | .NET | 9,439 | 14,088 |
| Redis | Rust | 56,883 | |
| Redis | .NET | 67,458 | |

### A large state: whole, as deltas, offloaded

`design/41`, parts 2 and 3. A running checkpoint whose state has a 64 KiB payload that never changes and a step that does, saved as a durable run would: one writer, twenty rows to a thread. Four stores of each kind:

- **whole:** no deltas, no offload.
- **delta:** deltas, with a whole row every 16, so a thread's first and sixteenth rows are whole.
- **offload:** the payload (a value of 64 KiB or more) is offloaded to the store's own database. The payload's blob is written by a thread's first save and by no other.
- **delta + offload:** both.

400 saves and 500 loads against SQLite, 200 and 200 against Postgres and Redis. The loads read the latest row of a thread of sixteen: whole, or fifteen deltas from a whole row.

Stored bytes are the mean JSON a save stored, the blobs it wrote included (`strata.store.checkpoint.size` and `strata.store.blob.size`). They are the same in both engines: the encoded form is the same JSON, and the blob is the payload's canonical JSON.

| Store | Whole | Delta | Offload | Delta + offload |
|---|---:|---:|---:|---:|
| SQLite, Postgres | 65,579 B | 6,650 B | 3,489 B | 3,390 B |
| Redis | 65,774 B | 6,845 B | 3,686 B | 3,593 B |

A delta row here is about 100 bytes (the envelope, the step, the hop count), and an offloaded row about 150 (the same and the blob's reference); the means are the whole rows, or the blob, spread over twenty.

Saves a second:

| Store | Engine | Whole | Delta | Offload | Delta + offload |
|---|---|---:|---:|---:|---:|
| SQLite | Rust | 374 | 415 | 436 | 424 |
| SQLite | .NET | 362 | 371 | 405 | 426 |
| Postgres | Rust | 22 | 177 | 298 | 290 |
| Postgres | .NET | 22 | 170 | 271 | 277 |
| Redis | Rust | 22 | 205 | 366 | 344 |
| Redis | .NET | 22 | 201 | 363 | 366 |

Load P50:

| Store | Engine | Whole | 15 deltas | Offloaded | 15 deltas, offloaded |
|---|---|---:|---:|---:|---:|
| SQLite | Rust | 103 µs | 198 µs | 162 µs | 210 µs |
| SQLite | .NET | 165 µs | 272 µs | 223 µs | 282 µs |
| Postgres | Rust | 420 µs | 496 µs | 926 µs | 962 µs |
| Postgres | .NET | 641 µs | 710 µs | 892 µs | 892 µs |
| Redis | Rust | 364 µs | 649 µs | 544 µs | 832 µs |
| Redis | .NET | 488 µs | 655 µs | 540 µs | 824 µs |

**The whole rows against Postgres and Redis measure the machine, not the store.** A save of 64 KB to either container takes about 45 ms, however small the rest of the save: the large-command latency of Docker Desktop's port forwarding with TCP's delayed acknowledgement (`design/41`, "Implementation Status"). The delta rates are then mostly the two whole rows of each twenty (2 × 45 ms against 18 × about 1 ms), and offload is faster again because a thread's first save is the only one that sends 64 KB. On a network without that delay the three would be closer, and closer to SQLite's, whose rate is the disk's sync.

**A load of an offloaded row reads the blob as well as the row**: one more query in Postgres, one more command in Redis, and the SHA-256 of 64 KiB to check it. In SQLite, in process, it costs little. The load takes the blob's hash as the value's fingerprint, so it doesn't hash the payload a second time for the cache.

Several loads' P99 against Postgres and Redis reached 8 to 14 ms, a few slow commands among 200; the medians are steady.
