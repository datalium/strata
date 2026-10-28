# Benchmark results

The suite in `design/37-Benchmarks.md`, measured on one machine. How to run it and how to read it: [`README.md`](README.md).

## Measured on

- **When:** three days.
  - **2026-10-11**, the `benchmarks` branch on top of `dd6def5`: the .NET figures above "Stores".
  - **2026-10-13**, after `design/38-Rust-Executor-Overhead.md` reduced what a node costs the Rust engine: the Rust figures above "Stores". The Rust engine as it was on the 11th, measured again on the 13th before the change, came within 2% of the 11th's figures, so the two days compare.
  - **2026-10-15**, the `sg-15-checkpoint-efficiency` branch on top of `3c93f53` (`design/41-Checkpoint-Efficiency.md`, part 1: group commit and the Rust Postgres pool): every figure from "Stores" to "Concurrent durable runs", both engines, one run each (`--only stores`).
  - **2026-10-15, later**, the `sg-15-delta-checkpoints` branch on top of `67ede5a` (`design/41`, part 2: delta checkpoints): "A large state, whole and as deltas", both engines, two runs each (`--only large_state`), shown as ranges where they differ.
  - **2026-10-15, last**, the `sg-15-value-offload` branch on top of `9f390b9` (`design/41`, part 3: value offload): "A large state, offloaded", both engines, two runs each (`--only large_state`), shown as ranges where they differ. The part 2 section is left as it was measured.
- **Version:** `0.9.0-preview.1`.
- **Machine:** Intel Core i9-12900KF (8 performance and 8 efficiency cores, 24 logical), Windows 11.
- **Runtimes:** .NET 10.0.8 (x64 RyuJIT, server GC); Rust 1.91.1, `--release`, tokio's multi-threaded runtime.
- **Stores:** SQLite as a file on the local disk. Postgres 16 and Redis 7 from the repository's `docker-compose.yml` (Docker Desktop on the same machine, Postgres on `tmpfs`).
- **Not a quiet machine.** A development session and an editor were running on every day, and several other containers on the 11th. The slowest percentiles and the maxima carry that noise; the profile's medians agree with the microbenchmarks' means.

Timers on Windows tick every 100 ns.

## What it shows

1. **The engine's share of a node is about 0.4 µs in both engines.** The median gap between one async handler returning and the next starting is 0.4 µs in .NET (P99.9 1.5 µs) and 0.4 µs in Rust (P99.9 1.0 µs). Before `design/38` it was 1.8 µs in Rust.
2. **Traversals are sub-millisecond at every size measured.** A hundred async nodes take 50 µs in .NET and 41 µs in Rust. The slowest case, a hundred sync nodes in Rust, takes 0.46 ms.
3. **Rust is level with .NET on a line of nodes and behind it elsewhere.** Where it is behind, the cost is outside the per-node path:
   - A run's own setup is about 0.9 µs in Rust and 0.2 µs in .NET, which is the whole difference on a one-node run.
   - A branch of a parallel group costs about 1.5 µs in Rust and 0.6 µs in .NET, and a map's item 2.1 µs and 0.9 µs: each is a task of its own.
   - A streamed token costs 352 ns in Rust and 111 ns in .NET, nearly all of it the channel.
   - Rust clones the state into every async handler, so a 16 KiB state adds 54% to a ten-node run; in .NET it adds nothing.
4. **A sync handler in Rust costs a thread hand-off each way, and the figure moves with it.** When both threads stay awake between nodes a sync node costs about 1.1 µs, which is what the profile measured; when one sleeps it costs 4 to 5.5 µs, which is what the hundred-node microbenchmark measured. Five runs of the sync traversals gave 4.3 to 7.0 µs for one node, 13 to 54 µs for ten and 134 to 539 µs for a hundred. Sync throughput stops at about 0.4 million transitions a second however many runs are going; async reaches 11 million in Rust and 20 million in .NET.
5. **A suspended thread holds nothing in the process.** Against SQLite, ten thousand frozen threads grew the heap by 0 bytes each in Rust and by nothing measurable in .NET, and took 1.6 KB each on disk. The in-memory store holds 1.7 KB (.NET) or 3.6 KB (Rust) each, which is its copy of a 1.25 KB checkpoint.
6. **A checkpoint per node costs a store round trip per node.** A ten-node run under `EveryNode` (eleven saves) takes 3 ms on Redis, 12 to 14 ms on Postgres and 25 ms on SQLite, against 12 µs (.NET) or 20 µs (Rust) when the store costs nothing.
7. **Both Postgres stores now pool connections.** Sixteen writers save 6,849 checkpoints a second in Rust and 6,339 in .NET; before `design/41` the Rust store had one connection and reached 593. Preparing each statement once per connection took one Rust writer from 551 saves a second to 1,010.
8. **Without group commit, SQLite saves are bound by the disk** (about 2.3 ms each, one sync per save, about 415 a second however many write) **and Postgres stops scaling at about 7,000 saves a second.**
9. **Group commit multiplies what many writers save.** With 64 writers: SQLite about 15 times (6,433 a second in Rust, 6,362 in .NET, against 415 and 410), Postgres 2.5 times in Rust (17,882 against 7,097) and 1.8 in .NET (12,619 against 7,092). 64 durable ten-node runs at once write 19,527 checkpoints a second to Postgres in Rust (6,952 without) and 15,438 in .NET (8,860). A writer alone gains nothing and pays a little: 773 saves a second against 1,010 in Rust, 733 against 868 in .NET, for the batched statement's extra work.
10. **The .NET group commit varies from run to run; the Rust one doesn't.** Six runs of .NET Postgres with 64 writers gave 5,900 to 16,200 saves a second, and 5,100 to 20,800 with 256; four Rust runs gave 17,900 to 21,600 and 26,400 to 36,000. The cause is not known.
11. **Deltas store a large state's unchanged payload once in sixteen rows.** A running checkpoint with a 64 KiB payload that never changes stores about 100 bytes as a delta against 65.6 KB whole; over a thread of twenty such rows (two of them whole: the first, and the sixteenth) a save stores 6.6 KB on average, a tenth. SQLite saves gain 13 to 22% (its rate is the disk's sync), Postgres and Redis 8 to 9 times, but there the whole rows are slow for a reason of this machine's (see the section). Loading a row fifteen deltas from a whole one costs up to a quarter more than loading a whole one in SQLite and Postgres, where it is still one statement, and about twice as much in Redis, where it is a second read.
12. **Offload stores a large unchanged value once per thread, and Postgres and Redis save faster for it.** With the 64 KiB payload offloaded, a save stores 3.4 to 3.7 KB on average over twenty rows (the payload once, in the blob store, and rows of about 150 bytes), against 6.6 KB as deltas and 65.6 KB whole. Postgres and Redis save 272 to 399 a second, against 170 to 217 with deltas and 22 whole, because only the first save of a thread now carries a 64 KB command (see the section). SQLite gains nothing more: its rate is the disk's sync. A load reads the blob too: about 1.1 ms in Postgres against 0.5 to 0.9 ms whole, about as much as a delta chain in Redis.

## Microbenchmarks

The mean time of one run. Every benchmark uses a store that costs nothing. The .NET column on the right is the managed memory one run allocates.

| Benchmark | Rust | .NET | .NET allocated |
|---|---:|---:|---:|
| Traversal, async, 1 node | 1.28 µs | 0.71 µs | 2.21 KB |
| Traversal, async, 10 nodes | 5.22 µs | 4.11 µs | 11.28 KB |
| Traversal, async, 100 nodes | 41.0 µs | 49.9 µs | 101.99 KB |
| Traversal, sync, 1 node | 4.49 µs | 2.06 µs | 3.53 KB |
| Traversal, sync, 10 nodes | 13.2 µs | 14.7 µs | 19.37 KB |
| Traversal, sync, 100 nodes | 459 µs | 143 µs | 177.32 KB |
| State size, 10 async nodes, empty state | 5.16 µs | 4.08 µs | 11.28 KB |
| State size, 10 async nodes, 16 KiB state | 7.94 µs | 4.12 µs | 11.28 KB |
| Parallel group, 2 branches | 6.65 µs | 2.33 µs | 6.69 KB |
| Parallel group, 8 branches | 15.1 µs | 5.65 µs | 14.09 KB |
| Parallel group, 32 branches | 51.3 µs | 20.3 µs | 45.01 KB |
| Map, 8 items | 28.4 µs | 8.97 µs | 24.27 KB |
| Map, 64 items | 144 µs | 61.1 µs | 157.05 KB |
| Interrupt and resume, 1 KiB state | 7.64 µs | 4.64 µs | 13.53 KB |
| 10 nodes, 1 KiB state, `Boundaries` | 5.60 µs | 4.11 µs | 11.28 KB |
| 10 nodes, 1 KiB state, `EveryNode` | 20.5 µs | 12.3 µs | 31.58 KB |
| Streaming, per token (1,000 tokens) | 352 ns | 111 ns | 59 B |

The sync rows in Rust are one run of five that differed widely (finding 4).

Per node, an async traversal costs about 0.4 µs in Rust and 0.5 µs in .NET; a sync one costs 1.4 µs in .NET and between 1.1 and 5.5 µs in Rust. Per branch, a group costs about 1.5 µs in Rust and 0.6 µs in .NET; per item, a map costs about 2.1 µs and 0.9 µs.

Rust allocates on the heap only what it is told to, and `rust/strata-bench/tests/allocations.rs` counts it: one allocation of 104 bytes for an async node, two for a sync one, and ten for a run besides its nodes'.

## Profile

### Transition latency

The gap between one handler returning and the next starting, 198,000 transitions.

| Handler | Engine | P50 | P90 | P99 | P99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| async | Rust | 0.40 µs | 0.50 µs | 0.50 µs | 1.00 µs | 190 µs |
| async | .NET | 0.40 µs | 0.50 µs | 0.90 µs | 1.50 µs | 296 µs |
| sync | Rust | 1.10 µs | 1.20 µs | 1.50 µs | 10.4 µs | 286 µs |
| sync | .NET | 1.30 µs | 1.60 µs | 2.40 µs | 7.50 µs | 323 µs |

### Run latency

A whole ten-node run: 100,000 async runs, 20,000 sync.

| Handler | Engine | P50 | P90 | P99 | P99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| async | Rust | 4.80 µs | 5.00 µs | 7.20 µs | 21.1 µs | 225 µs |
| async | .NET | 3.50 µs | 4.10 µs | 4.90 µs | 20.4 µs | 673 µs |
| sync | Rust | 11.7 µs | 12.7 µs | 49.0 µs | 182 µs | 578 µs |
| sync | .NET | 12.7 µs | 14.4 µs | 22.6 µs | 213 µs | 405 µs |

### Throughput

Transitions a second with that many ten-node runs going at once, for two seconds.

| Handler | Engine | 1 | 4 | 16 | 64 |
|---|---|---:|---:|---:|---:|
| async | Rust | 1,982,000 | 5,569,000 | 10,289,000 | 11,348,000 |
| async | .NET | 2,766,000 | 10,189,000 | 19,627,000 | 20,056,000 |
| sync | Rust | 390,000 | 290,000 | 437,000 | 417,000 |
| sync | .NET | 773,000 | 2,052,000 | 5,265,000 | 4,853,000 |

### Memory per suspended thread

Ten thousand threads frozen at an interrupt boundary with a 1 KiB state. The checkpoint is 1,251 bytes of JSON.

| Store | Engine | Heap per thread | Disk per thread |
|---|---|---:|---:|
| in-memory | Rust | 3,608 B | |
| in-memory | .NET | 1,697 B | |
| SQLite | Rust | 0 B | 1,596 B |
| SQLite | .NET | −7 B | 1,596 B |

The −7 is the measurement's noise: the managed heap after a full collection, before and after.

### Stores

Saves and loads of a 1 KiB checkpoint, a thread each, without group commit. 20,000 operations against the in-memory store, 2,000 against the others; with 64 and 256 writers, at least 32 saves each.

| Store | Engine | Saves/s, 1 writer | Save P50 | Save P99 | 16 writers | 64 writers | 256 writers | Loads/s | Load P50 | Load P99 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| in-memory | Rust | 1,898,000 | 0.2 µs | 0.5 µs | 981,000 | 875,000 | 952,000 | 1,433,000 | 0.7 µs | 1.1 µs |
| in-memory | .NET | 2,071,000 | 0.1 µs | 2.1 µs | 5,516,000 | | | 3,174,000 | 0.3 µs | 1.7 µs |
| SQLite | Rust | 415 | 2.29 ms | 3.49 ms | 421 | 415 | 415 | 18,879 | 49 µs | 111 µs |
| SQLite | .NET | 410 | 2.34 ms | 3.92 ms | 403 | 410 | 414 | 6,472 | 148 µs | 292 µs |
| Postgres | Rust | 1,010 | 950 µs | 1.52 ms | 6,849 | 7,097 | 7,039 | 1,487 | 654 µs | 1.08 ms |
| Postgres | .NET | 868 | 1.11 ms | 1.83 ms | 6,339 | 7,092 | 3,543 | 2,310 | 418 µs | 781 µs |
| Redis | Rust | 4,133 | 228 µs | 479 µs | 32,259 | 67,785 | 63,448 | 4,607 | 202 µs | 455 µs |
| Redis | .NET | 3,402 | 282 µs | 495 µs | 18,172 | 43,095 | 61,131 | 4,377 | 221 µs | 449 µs |

Two rows are the machine's, not the code's. The .NET in-memory figures came out two to four times slower than on the 11th, and its 64- and 256-writer runs (378,000 and 12.7 million) are left out as noise, though neither the store nor the engine changed (the `EveryNode` microbenchmark measured 11.9 µs, against 12.3 on the 11th). And .NET Redis with 16 writers measured 31,787 and 33,477 in two earlier runs that day.

### Group commit

The same saves through a store with group commit on (`design/41`, defaults: up to 256 saves a flush, 4 flushes at once in Postgres, 1 in SQLite). Redis has none: built and measured, it was no faster than the store without it (`design/41`, "Implementation Status").

| Store | Engine | 1 writer | 16 writers | 64 writers | 256 writers |
|---|---|---:|---:|---:|---:|
| SQLite | Rust | 409 | 1,985 | 6,433 | 8,517 |
| SQLite | .NET | 421 | 2,287 | 6,362 | 7,664 |
| Postgres | Rust | 773 | 7,728 | 17,882 | 35,978 |
| Postgres | .NET | 733 | 7,327 | 12,619 | 17,090 |

### A durable run

The ten-node line with a 1 KiB state under `EveryNode`: eleven checkpoints a run. 2,000 runs against the in-memory store, 200 against the others.

| Store | Engine | Mean | P50 | P99 |
|---|---|---:|---:|---:|
| in-memory | Rust | 26.1 µs | 20.4 µs | 84.8 µs |
| in-memory | .NET | 104 µs | 76.6 µs | 203 µs |
| SQLite | Rust | 25.7 ms | 24.8 ms | 36.6 ms |
| SQLite | .NET | 25.4 ms | 24.8 ms | 35.6 ms |
| Postgres | Rust | 12.4 ms | 12.2 ms | 14.8 ms |
| Postgres | .NET | 13.9 ms | 13.8 ms | 16.3 ms |
| Redis | Rust | 2.82 ms | 2.73 ms | 3.72 ms |
| Redis | .NET | 2.87 ms | 2.77 ms | 4.13 ms |

The .NET in-memory row is the machine's, as in "Stores": on the 11th it measured 21.6 µs.

### Concurrent durable runs

64 workers each running that durable line one run after another: checkpoints written a second, without and with group commit.

| Store | Engine | Without | With group commit |
|---|---|---:|---:|
| in-memory | Rust | 859,501 | |
| in-memory | .NET | 734,794 | |
| SQLite | Rust | 431 | 5,819 |
| SQLite | .NET | 424 | 7,352 |
| Postgres | Rust | 6,952 | 19,527 |
| Postgres | .NET | 8,860 | 15,438 |
| Redis | Rust | 67,135 | |
| Redis | .NET | 63,608 | |

### A large state, whole and as deltas

`design/41`, part 2. A running checkpoint whose state has a 64 KiB payload that never changes and a step that does, saved as a durable run would: one writer, twenty rows to a thread. "Whole" is a store without deltas; "delta" one with them (a whole row every 16, so a thread's first and sixteenth rows are whole). Stored bytes are the mean JSON a save stored (`strata.store.checkpoint.size`). The loads read the latest row of a thread of sixteen: whole, or fifteen deltas from a whole row. 400 saves and 500 loads against SQLite, 200 and 200 against Postgres and Redis. Two runs each; a range where they differed.

| Store | Engine | Saves/s whole | Saves/s delta | Stored whole | Stored delta | Load P50 whole | Load P50, 15 deltas |
|---|---|---:|---:|---:|---:|---:|---:|
| SQLite | Rust | 370–385 | 434–443 | 65,579 B | 6,650 B | 77–204 µs | 145–180 µs |
| SQLite | .NET | 344–353 | 400–419 | 65,579 B | 6,650 B | 294 µs | 327–340 µs |
| Postgres | Rust | 22 | 174–178 | 65,579 B | 6,650 B | 521–541 µs | 659–669 µs |
| Postgres | .NET | 22 | 169–173 | 65,579 B | 6,650 B | 821–835 µs | 0.90–1.01 ms |
| Redis | Rust | 22 | 205–206 | 65,774 B | 6,845 B | 279–288 µs | 583–588 µs |
| Redis | .NET | 23 | 184–192 | 65,774 B | 6,845 B | 372–384 µs | 698–721 µs |

The two engines store the same bytes: the encoded form is the same JSON. A delta row here is about 100 bytes (the envelope, the step, the hop count); the mean is the two whole rows of twenty.

**The whole rows against Postgres and Redis measure the machine, not the store.** A save of 64 KB to either container takes about 45 ms, however small the rest of the save: the large-command latency of Docker Desktop's port forwarding with TCP's delayed acknowledgement that part 1 found with Redis (`design/41`, "Implementation Status"), which Postgres shows too. The delta rates are then mostly the two whole rows of each twenty (2 × 45 ms against 18 × about 1 ms); with the stores on a network without that delay, the gain would be smaller and closer to SQLite's. Not chased further.

### A large state, offloaded

`design/41`, part 3. The same state and the same saves and loads as the section above, with two more stores: "offload" offloads the payload (a value of 64 KiB or more) to the store's own database, and "delta + offload" stores deltas as well. Stored bytes are the mean JSON a save stored, the blobs it wrote included (`strata.store.checkpoint.size` and `strata.store.blob.size`); the payload's blob is written by a thread's first save and by no other. Two runs each; a range where they differed. The "whole" and "delta" columns are this day's runs of the same stores as the section above.

| Store | Engine | Saves/s whole | Saves/s delta | Saves/s offload | Saves/s delta + offload | Stored offload | Stored delta + offload |
|---|---|---:|---:|---:|---:|---:|---:|
| SQLite | Rust | 351–384 | 404–436 | 363–429 | 301–388 | 3,489 B | 3,390 B |
| SQLite | .NET | 339 | 383–389 | 318–345 | 356–363 | 3,489 B | 3,390 B |
| Postgres | Rust | 22 | 177–178 | 291–296 | 284 | 3,489 B | 3,390 B |
| Postgres | .NET | 22 | 170–175 | 272 | 267–273 | 3,489 B | 3,390 B |
| Redis | Rust | 23 | 214–217 | 394–399 | 380–395 | 3,686 B | 3,593 B |
| Redis | .NET | 22–23 | 198–199 | 345 | 339 | 3,686 B | 3,593 B |

| Store | Engine | Load P50 whole | Load P50, 15 deltas | Load P50 offloaded | Load P50, 15 deltas offloaded |
|---|---|---:|---:|---:|---:|
| SQLite | Rust | 89–90 µs | 168–177 µs | 119–129 µs | 167–237 µs |
| SQLite | .NET | 296–298 µs | 349–363 µs | 229–304 µs | 256–268 µs |
| Postgres | Rust | 501–532 µs | 649–800 µs | 1.06–1.08 ms | 1.11–1.14 ms |
| Postgres | .NET | 818–877 µs | 929 µs | 1.02–1.08 ms | 1.07–1.08 ms |
| Redis | Rust | 334–358 µs | 585–668 µs | 543–547 µs | 814–825 µs |
| Redis | .NET | 467–478 µs | 652–668 µs | 525–576 µs | 789–819 µs |

The two engines store the same bytes again: the same rows, the same blob (the payload's canonical JSON). Over a thread of twenty rows the payload is written once, so a save stores its share of 64 KiB and a row of about 150 bytes (the envelope, the step, the hop count, the blob's reference). Deltas add little here because the payload is the only large value and offload already leaves it out of every row.

**Offload is the faster save against Postgres and Redis on this machine for the reason the section above gives**: a thread's first save is the only one that sends 64 KB, where deltas send it twice in twenty (the first row, and the sixteenth). On a network without Docker Desktop's delay the two would be closer.

**A load reads the blob as well as the row**: one more query in Postgres (about half a millisecond here), one more command in Redis, and the SHA-256 of 64 KiB to check it. In SQLite, in process, it costs little. The load takes the blob's hash as the value's fingerprint, so it doesn't hash the payload a second time for the cache.

