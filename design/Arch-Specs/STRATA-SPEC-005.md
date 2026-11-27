## 📋 Architectural Spec: The Cross-Language Checkpoint Schema

**Component ID:** STRATA-SPEC-005  
**Status:** IMPLEMENTED  
**Relates to:** FR-XL-02 (implements), FR-XL-01 (states it normatively), FR-XL-03 (tests it)  
**Introduces:** none

This spec makes the wire formats in `schema/` normative. `schema/README.md` is the reference: each record's fields, the SQL, SQLite and Redis layouts, and the fixtures. This document says what a conforming engine, store or tool **must** and **must not** do with them. Where the two disagree, the JSON Schema files are the source of truth for a record's shape, and this spec for the rules around it. "Must", "must not" and "may" are used as in RFC 2119.

---

### 1. System Overview & The Cross-Language Challenge

Strata has two engines, Rust and .NET, and a CLI. A thread one of them starts may be resumed, inspected or recovered by another: a .NET worker and a Rust worker share one Postgres database, the `strata` CLI opens a SQLite file a .NET service wrote, and Strata Cloud stores checkpoints from both. A thread can also outlive the build that started it, so it may be read by a newer version of either engine.

Without one versioned format, three things break:

- **Silent drift.** Two serializers that disagree on a timestamp's form, a key's name or an absent-versus-null field each read their own output and quietly misread the other's. A resumed thread then continues from the wrong state, and nothing reports an error.
- **Guessing at the unknown.** An older runtime that reads a record with a field it doesn't know, and drops it, resumes a nested thread at its parent or a rejected decision down the approval path.
- **Databases that move under a reader.** A newer store adds a table and an older reader opens the database it changed.

OI-005 (the core brief) records the gap: `schema/` versioned every record, but nothing declared it normative, and no test migrated a database across a real schema change from one engine to the other. This spec closes both.

---

### 2. Where a Checkpoint Goes

```text
 [ Engine A ] ── Checkpoint (wire JSON, version 1.0.0–1.7.0) ──► [ Store A ]
                                                                 │ may encode it: 1.8.0 (delta), 1.9.0 (offload)
                                                                 ▼
                                    [ SQL row / Redis element / Cloud API ]
                                     database at checkpoints schema version N
                                                                 │
                ┌────────────────────────────────────────────────┼───────────────────────────┐
                ▼                                                ▼                           ▼
          [ Store B ]                                      [ strata CLI ]               [ Store A' ]
   migrates the database if behind,                  decodes, refuses a newer       (a newer build of A)
   refuses it if ahead; decodes 1.8.0/1.9.0          database or record
                │
                ▼
 [ Engine B ] ◄── Checkpoint (wire JSON, 1.0.0–1.7.0): refused if its version is unknown
```

The engine only ever writes and reads **engine-visible** versions. Encodings a store adds for its own storage never reach an engine.

---

### 3. The Records

Four records cross the boundary. Each is a JSON Schema 2020-12 file in `schema/`, and each carries a `schema_version`.

| Record | Schema | Versions | Written by |
|:--|:--|:--|:--|
| Checkpoint | `checkpoint.schema.json` | `1.0.0`–`1.7.0` engine-visible; `1.8.0`, `1.9.0` store-only | the engine (through its store) |
| Graph definition | `graph-definition.schema.json` | `1.0.0`–`1.6.0` | `ToDefinition()` / `to_definition()`, read by the CLI and `ToTopology()` |
| History record | `history-record.schema.json` | `1.0.0` | the history stores |
| Memory record | `memory-record.schema.json` | `1.0.0` | the long-term memory stores (`design/45`) |

`schema/README.md`, "Records", lists each record's Rust and C# types.

**What each checkpoint version adds.** Each is written only when a checkpoint's content needs it (`schema/README.md`, "Versioning", has the full pairings, which the schema enforces):

| Version | Written for | Design |
|:--|:--|:--|
| `1.0.0` | a plain checkpoint: interrupted, completed, failed | STRATA-SPEC-002 |
| `1.1.0` | a `nested` child | `design/16` |
| `1.2.0` | a `cancelled` status, at any depth | `design/17` |
| `1.3.0` | a `decision`, at any depth | `design/21` |
| `1.4.0` | an `ended` status | `design/28` |
| `1.5.0` | a `wait` (an event or a timer) | `design/29` |
| `1.6.0` | a `graph` identity | `design/31` |
| `1.7.0` | a `running` status (a checkpoint per node) | `design/33` |
| `1.8.0` | **store-only:** a running row stored as a delta of the row before | `design/41` |
| `1.9.0` | **store-only:** a row whose large values are offloaded to blobs | `design/41` |

---

### 4. Versioning Rules

These are `design/36`'s "Compatibility" rules, made normative.

1. **A writer must write the lowest version that can say what it has to say.** A graph that uses nothing new must write what it always wrote, byte for byte: a plain checkpoint is still `1.0.0`.
2. **Every version is a superset of the one before.** A reader that reads version N must read every version below it, and a writer must not change the meaning of a field an earlier version had.
3. **A reader must refuse what it doesn't know.** A record whose `schema_version` it doesn't know, a status or `kind` it can't parse, or a database newer than the build is an error, never a guess. A reader must not drop a field it doesn't know and carry on.
4. **A version of the packages must read everything every earlier version wrote.** Dropping support for a schema version is a major version of the packages.
5. **A record's shape changes only through a new version.** A new field gets a new minor `schema_version`, a new fixture, and the same change in both engines in the same pull request (`schema/README.md`, "Adding or changing a record").
6. **Timestamps are RFC 3339 strings at the boundary**, treated as opaque text; each engine parses them at its own edges.

---

### 5. Store-Only Encodings

`1.8.0` and `1.9.0` exist only inside a store. The engine never writes or sees one.

- **A store that writes them must decode them on every read**, whatever its own options: a store opened without deltas or offload still reads a row another store wrote with them. Every built-in Postgres, SQLite and Redis store, in both engines, and the CLI do.
- **A store must decode a row to the version its envelope's `checkpoint_version` names** before the engine sees it, and must check each blob against its hash (`schema/README.md`, "Offloaded values").
- **A conforming store may refuse the store-only encodings it doesn't write.** A store with an API of its own, which the engine hands only engine-visible checkpoints, need not accept `1.8.0` or `1.9.0`. **Strata Cloud** stores `1.0.0` to `1.7.0` and refuses `1.8.0` and `1.9.0`; the Cloud store adapter has no deltas or offload (`design/41`, "Where").
- A runtime built before `1.8.0` or `1.9.0` refuses such a row by its version, as rule 3 requires.

---

### 6. Database Schema Versions

Each SQL database records the layout it holds in `strata_schema_version`, one row per component (`schema/README.md`, "Schema versions"; `design/34`, "Migrations").

| Component | Tables | Current version | Steps |
|:--|:--|:--|:--|
| `checkpoints` | the log, its head table, the leases; `checkpoint_blobs` from 2 | **2** | 0 → 1 the baseline (creates the tables, or adds what an older layout lacks); 1 → 2 creates `checkpoint_blobs` |
| `history` | the history log (Postgres) | **1** | 0 → 1 the baseline |
| `memory` | the memory store's table (`design/45`) | **1** | created at 1; in Redis, the key `{prefix}:memory:schema` |

The rules:

1. **Databases move forward only, one recorded step at a time.** A step is written once in each engine, with identical DDL, and bumps the number in `fixtures/checkpoint-log/schema-versions.json`.
2. **A store must migrate a database that is behind when it opens it**, in one transaction under the lock it takes for DDL (Postgres's advisory lock `0x535452415441`, SQLite's immediate transaction), so two stores opening one database at once don't interleave. A store told not to migrate must refuse a database that is behind.
3. **A store must refuse a database newer than its build** and leave it unchanged: `database is at checkpoints schema version 3, and this build knows 2: upgrade the runtime`.
4. **A current store applies every step when it opens a database, whether or not it uses what the step adds.** So a database a current store has opened is at the current version, and an older runtime refuses it.
5. **Redis has no migrations.** Every writer to a key prefix must be current, because the scripts that keep the index are part of the writer.

---

### 7. How Each Engine Reads and Writes

Both engines and the CLI share one version (`design/36`). For `0.9.0`:

| | Writes up to | Reads |
|:--|:--|:--|
| Checkpoint | `1.7.0`; `1.8.0` from a store with deltas on; `1.9.0` from a store with offload on | `1.0.0`–`1.9.0` (a store decodes `1.8.0` and `1.9.0`) |
| Graph definition | `1.6.0` | `1.0.0`–`1.6.0` |
| History record | `1.0.0` | `1.0.0` |
| Memory record | `1.0.0` | `1.0.0` |
| Database, `checkpoints` | 2 | 0, 1 (migrated on open), 2 |
| Database, `history` | 1 | 0 (migrated on open), 1 |
| Database, `memory` | 1 | 1 |

Each release's `CHANGELOG.md` entry has this table, under "Wire formats".

**The upgrade order: every reader before any writer.** Deploy the new version everywhere a thread can be read (the workers, the API, the `strata` CLI), and only then turn on what writes a new schema version or opens a shared database with a new step: a graph identity, a wait, `EveryNode`, deltas, offload, or a current store on a database an older runtime still reads.

---

### 8. The Stability Promise for 0.9.0

- **No wire-format change from `0.9.0` until `1.0.0-rc.1`.** No version in between adds a record schema version, a database step or a change to a store's layout. A change that needs one waits for `1.0.0-rc.1`.
- **Everything `0.9.0` writes stays readable** by every later version (rule 4 in section 4). Dropping a schema version is a major version.
- Previews before `0.9.0` promised nothing between previews. A database or record a preview wrote is still readable by `0.9.0`.

---

### 9. Conformance

Two kinds of test prove an implementation conforms. Both run in CI on every pull request.

**The shared fixtures** (`schema/fixtures/`) are read directly by both engines' tests and the CLI's. They prove that each engine, alone, writes the same bytes and reads the same meaning:

- a deserialize-then-serialize round trip of each record reproduces its fixture exactly;
- a run of the same graph in either engine writes the checkpoint a `checkpoint-lifecycle` fixture holds, one for every engine-visible version, and either engine resumes or recovers a thread a fixture holds;
- each store's tables match the column fixtures, its encoded rows the `delta.rows.json` and `offloaded.rows.json` fixtures, and a fresh database the versions in `schema-versions.json`;
- a version-1 database gains `checkpoint_blobs` when a current store opens it, in each engine.

**The cross-engine migration test** (`schema/fixtures/cross-engine/scenario.json`; `rust/strata-memory/tests/cross_engine.rs`, `dotnet/tests/Datalium.Strata.Tests/Backends/CrossEngineTests.cs`, `cli/tests/cross_engine.rs`; `.github/workflows/cross-engine-ci.yml`) proves what the fixtures can't: that what one engine's stores leave in a real database, the other engine's current stores read and continue. Each engine writes, in SQLite and in Postgres:

- **an older database**, at the `checkpoints` component's version 1, holding every `checkpoint-lifecycle` fixture (`1.0.0` to `1.7.0`), and
- **a current database**, written by a store with deltas and offload on, holding a run that died mid-way: rows at `1.9.0`, `1.8.0` and `1.8.0`.

Then the other engine opens them with its current stores. It must migrate the older database to version 2, load every checkpoint exactly as its fixture, list every thread, and resume or recover a thread at `1.0.0`, `1.6.0` and `1.7.0` to completion. For the current database, it must decode the other engine's deltas and offloaded values and recover the run, with the node in flight running again under the idempotency key the writer's hop count gives it. Last, the CLI reads the .NET engine's databases after the Rust engine has continued them. Without the job's variables, each engine runs the same test against databases it wrote itself, so the code runs in every build.

A new record version, database step or store encoding must come with fixtures for both kinds: a shared fixture, and a case in the cross-engine scenario if it changes what a store leaves in a database.

**The idempotency-key fixtures** (`schema/fixtures/idempotency-key/cases.json`; `rust/strata-core/tests/durable_execution.rs`, `dotnet/tests/Datalium.Strata.Tests/DurableExecutionTests.cs`) pin section 10. Each case is a graph, a policy, and either a `running` checkpoint to recover or a fresh run, with every handler execution it must make: its path, its key and its `IsRecovery`. Each engine's test saves the checkpoint and recovers the thread, or runs it, with handlers that record what their `NodeContext` says, and compares that with the case. The cases cover a node at the outermost level, a level without `strata.hops`, a node inside a subgraph, a subgraph not yet entered, a parallel group, a map, a durable run's keys, and runs that aren't durable, one of them with a hop limit.

---

### 10. Crash Recovery and the Idempotency Key

A thread one engine wrote may be recovered by the other, so what a recovery runs again, and the key it runs it under, must be the same in both. This section is normative for an engine that runs or recovers a durable thread, and for a store or tool that derives a key from a checkpoint. It describes what both engines do from checkpoint `1.7.0` (`design/33`), and adds no field: it needs only `status`, `next_node`, `nested` and the `strata.hops` metadata key.

A **durable run** is one whose outermost graph is compiled with `CheckpointPolicy.EveryNode` (Rust `CheckpointPolicy::EveryNode`). The outermost graph's policy is the run's, at every depth. Any other run, including one under the default, `Boundaries`, is not durable.

#### 10.1 The Crash Contract: At Least Once

1. **A checkpoint names a node before the node runs.** In a durable run, before a node's handler starts, the engine must have saved a `running` checkpoint whose `next_node` is that node at its level, and the store must have acknowledged the save. A store that commits saves in groups acknowledges a save only after its group commits (`design/41`). The checkpoint is:
   - at the start of a fresh run, after its lease and before its first node: `suspended_node` null, `next_node` the entry point, the initial state;
   - after a node commits and an edge leads on: `suspended_node` the node, `next_node` the edge's target, the committed state;
   - inside a subgraph, one checkpoint built from the outside in: each enclosing level `running`, suspended at its subgraph node with `next_node` the subgraph node itself and the state that node started with, and `nested` in it the child's level, as above.
2. **A parallel group and a map are one node.** Nothing is saved while their branches or items run. The checkpoint before them names the group or the map; the next is saved after the reducer.
3. **A live process never leaves a thread `running`.** Every way a durable run returns writes another status: `completed`, `interrupted`, `failed` or `cancelled`. A run that would end without one (an edge with no matching condition, a node it can't run, an exception out of a handler) writes `failed`, at the node that was to run, with the state before it and `error_kind` `fatal`. So a latest checkpoint that is `running` means its run is still going, or its process died.
4. **A recovery runs the node in flight again.** It loads the thread's latest checkpoint, which must be `running`, and continues it from `next_node` at every level: it re-enters the subgraph nodes on the way down, and starts the innermost level's `next_node` with that level's stored state. That node runs again from the state it started with. If it is a group or a map, every branch or item runs again. The nodes before it don't. Each level keeps its decision and its hop count.
5. **So execution is at least once.** A node runs again if its process died while it ran, or after it finished and before the checkpoint after it was acknowledged. Whatever it did outside the state happens twice unless the node prevents it. No engine can tell whether a call to the outside world went out, and a conforming engine must not claim more than this.
6. **One recovery at a time.** Before it runs anything, a recovery saves the loaded checkpoint again, with `created_at` now and `strata.recoveries` one higher, only if the thread's latest sequence is still the one it loaded. If it isn't, nothing runs. With leases, a recovery acquires the thread's lease, and a run whose lease was taken over can't save again (`design/32`). Leases and the conditional save protect the checkpoint log. They don't stop a process that was thought dead from finishing a call it had started.
7. **`IsRecovery` means "this may have run before".** It is true for the node a recovery starts with at each level, on every attempt of it, and for its fallback if it fails again; and for every node of every branch or item of a group or map that a recovery runs again. It is false at a level once a node commits there. It is never true outside a recovery.
8. **A run that isn't durable is not recovered.** It writes no `running` checkpoint, an engine must refuse to recover a thread with a graph compiled without `EveryNode`, and a process that dies loses what it did since its last checkpoint.

#### 10.2 The Idempotency Key

Every execution of a node has a key: `NodeContext.IdempotencyKey` in .NET, `NodeContext::idempotency_key()` in Rust. A node passes it to whatever it changes outside its state, so that system can tell a repeat from a new request.

```text
key   = thread "/" path "#" hops [ "@" run-id ]
path  = segment *( "/" segment )
hops  = hop *( "." hop )
hop   = a decimal integer of 1 or more, in ASCII digits, without sign or leading zeros
```

1. **`thread`** is the run's thread id, as the caller gave it. At every depth it is the outermost thread's id, not a nested level's `thread_id`.
2. **`path`** names the node from the outermost level in:
   - a node at the outermost level: its name;
   - a node inside subgraph node `S`: `S`'s path, `/`, its name;
   - branch `b` of parallel group `G`: `G`'s path, `/`, `b`. A branch runs at its group's level: it adds a segment, not a hop;
   - node `N` inside the item with key `k` of map `M`: `M`'s path, `/`, `k`, `/`, `N`. An item is a level of its own: it adds two segments and a hop.
3. **`hops`** has one number per level, from the outermost in: the outermost level, then one for each subgraph or map item the node is inside. A level's number for a node is the hops that level made before the node, plus 1. A hop is a node started at the level (`design/24`, "What counts"): a fallback is a hop of its own, a retry is not, and a group, a map or a subgraph node is one hop at its level. A new level counts from 0. A resume or a recovery counts from the level's `strata.hops`.
4. **`@` and the run id** follow when, and only when, the run isn't durable. Without stored hop counts a later resume could repeat a key, so the run id keeps it from doing so. A durable run's key has no run id, so a recovery in another process, or in the other engine, repeats it.
5. **What stays the same.** A node's retries have its key. In a durable run, so does the node's run again in a recovery, and in a resume that re-enters the node a checkpoint stopped at (a failed or cancelled node, `design/24`, "Persistence"). The same node reached again later in the thread, by a loop or after a resume continues past a boundary, has another.

**Deriving a key from a checkpoint.** A store or tool may compute the key of the node a recovery of a `running` checkpoint would run, without running anything:

1. Let level 0 be the checkpoint, and level *i* + 1 the `nested` checkpoint of level *i*, down to the innermost.
2. For each level, its hop is its `strata.hops` value plus 1. A level without `strata.hops` counts from 0, so its hop is 1. A value that isn't a decimal integer is an error: a recovery refuses it, and so must a tool.
3. The path is the levels' `next_node` values, outermost first, joined with `/`, and the hops are the levels' hops, joined with `.`.
4. If the innermost `next_node` is a subgraph node, its child starts afresh: the key is that of the child's entry point, which adds `/` and the entry point's name to the path and a hop of `1`. If it is a parallel group, each branch's key adds `/` and the branch's name to the path, with the same hops. If it is a map, an item's first node adds `/`, the item key, `/` and the node's name, and a hop of `1`. Entry points and branch names come from the graph, and item keys from the state, so a tool needs the graph, or the state's items, to derive these.

For example, `checkpoint-lifecycle/nested-running.json` (thread `thread_nested_running_demo`, outermost level `next_node` `Work` with `strata.hops` `1`, nested level `next_node` `Y` with `strata.hops` `1`) is recovered by running `Y` under the key `thread_nested_running_demo/Work/Y#2.2`. `checkpoint-lifecycle/running.json` runs `B` under `thread_running_demo/B#2`.

#### 10.3 What the Key Promises, and What It Doesn't

- **A system that receives a key must treat it as an opaque string**, compared for equality, and must not parse it. Its format is fixed here so that both engines and every tool derive the same key, not so that callers read meaning into it. A node with several effects derives one key for each by adding a suffix of its own, for example `{key}:notify`.
- **Exactly once needs the other side.** An effect happens once only when the system the node calls ignores a request whose key it has already seen. Nothing in the engine, the store or the lease makes an outside effect exactly once.
- **A key is unique only for distinct ids and names.** Two executions get the same key if a fresh run reuses a thread id (hops start again from 0), or if a thread id, a node name or a branch name contains `/`. For example, thread `t/G` with node `b` at its first hop and branch `b` of group `G` in thread `t` both get `t/G/b#1`, and so does a node named `G/b` at the first hop of thread `t`. Map item keys can't contain `/` (`design/27`); the engines don't refuse the others. An application that relies on the key must not reuse thread ids, and must not use `/` in thread ids, node names or branch names.

---

### 11. Cross-Language Edge Cases & Mitigations

- **Absent versus null.** A key a version doesn't have must be absent, not `null`, so an older version's bytes are unchanged (`nested`, `decision`, `wait`, `graph`). Both engines' serializers are pinned to the fixtures byte for byte.
- **Numbers and key order in blobs.** An offloaded value's blob is its JSON with keys sorted at every depth, compact, and named by the SHA-256 of those bytes; a reader checks the hash, not the formatting, so either engine reads the other's blobs.
- **Idempotency keys across engines.** A recovered node's key comes from the checkpoint's `strata.hops` metadata (section 10.2), so a thread one engine wrote and the other recovers runs its node under the same key. The cross-engine test checks this.
- **CLI implications.** None new. `strata checkpoint log`, `replay` and `thread list` / `inspect` read every version and both databases' layouts, and refuse a newer database (FR-XL-03).
- **Failure and rollback.** A migration is one transaction: a failure leaves the database at its old version. A process that dies mid-run leaves its last `running` checkpoint, which either engine recovers (section 10, `design/33`).

---

### 12. Requirement Changes

| ID | Change |
|:---|:-------|
| FR-XL-02 | `Partial` → `Implemented`: this spec is written, and the cross-engine migration test runs in CI |
| FR-XL-01 | None. This spec states its rules normatively, the crash contract and the idempotency key's format (section 10) included |
| OI-005 | Resolved |

---

## ✅ Implementation Status (2026-11-20)

Written after the build: it describes what `0.9.0-preview.3` writes and reads, and changes no wire format.

**Added with this spec:**

- **The cross-engine migration test.** `schema/fixtures/cross-engine/scenario.json` sets out what each engine writes and what the other must find. Each engine's twin test is one test whose step comes from `STRATA_INTEROP_DIR` and `STRATA_INTEROP_STEP` (`write` its own databases, `read` the other's). Without them it writes into a directory of its own and reads it back, with Postgres when `STRATA_TEST_POSTGRES_URL` is set, so it runs in `rust-ci.yml` and `dotnet-ci.yml` too. `cross-engine-ci.yml` runs Rust write, .NET write, Rust read, .NET read, then the CLI read, against one SQLite directory and one Postgres service. The CLI step skips outside that job.
- **The older database is made the way the existing migration tests make one:** a current store writes it, then the test drops `checkpoint_blobs` and records version 1. That is exactly version 1's layout ("Version 1 is the layout described in this file before value offload"). No published release wrote a version-1 database: `checkpoint_blobs` shipped in `0.9.0-preview.1`, the first public release. So a test can't take one from an old package.
- **What a resume writes:** the reader's graphs checkpoint every node, so each resumed thread ends with a `completed` row the reader wrote on top of the writer's log. The CLI then reads logs that both engines wrote into.
- **Checked by breaking it, then restoring it:** a hop count changed in a .NET-written row fails the Rust reader on the recovered node's key (`C#4`, not `C#3`), and a `wait` removed from a Rust-written row fails the .NET reader on that fixture.
- **`schema/README.md`:** links here; "Records" lists the memory record; "Versioning" no longer says every record is at `1.0.0`; "Schema versions" lists the `memory` component.

**Added 2026-11-26: section 10**, the crash contract and the idempotency key's format, from `design/33` and checked against both engines' code. It changes no wire format and no behaviour. Writing it down found one wording error, in `design/33`, now fixed: the run id ends a key whenever the run isn't durable, not only "when hops aren't stored". A `Boundaries` level with a hop limit stores its hops and still adds the run id. The `idempotency-key` fixtures (section 9) come with it.

**Not covered:** Redis has no migrations to test (section 6, rule 5), and the Cloud store's wire contract is Cloud's (`design/30`, `design/43`).
