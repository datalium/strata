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

---

### 10. Cross-Language Edge Cases & Mitigations

- **Absent versus null.** A key a version doesn't have must be absent, not `null`, so an older version's bytes are unchanged (`nested`, `decision`, `wait`, `graph`). Both engines' serializers are pinned to the fixtures byte for byte.
- **Numbers and key order in blobs.** An offloaded value's blob is its JSON with keys sorted at every depth, compact, and named by the SHA-256 of those bytes; a reader checks the hash, not the formatting, so either engine reads the other's blobs.
- **Idempotency keys across engines.** A recovered node's key comes from the checkpoint's `strata.hops` metadata, so a thread one engine wrote and the other recovers runs its node under the same key. The cross-engine test checks this.
- **CLI implications.** None new. `strata checkpoint log`, `replay` and `thread list` / `inspect` read every version and both databases' layouts, and refuse a newer database (FR-XL-03).
- **Failure and rollback.** A migration is one transaction: a failure leaves the database at its old version. A process that dies mid-run leaves its last `running` checkpoint, which either engine recovers (`design/33`).

---

### 11. Requirement Changes

| ID | Change |
|:---|:-------|
| FR-XL-02 | `Partial` → `Implemented`: this spec is written, and the cross-engine migration test runs in CI |
| FR-XL-01 | None. This spec states its rules normatively |
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

**Not covered:** Redis has no migrations to test (section 6, rule 5), and the Cloud store's wire contract is Cloud's (`design/30`, `design/43`).
