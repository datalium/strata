# Strata Interop Schema

Canonical source of truth for the wire formats `CLAUDE.md`'s cross-language
constraint requires: the Rust `strata` CLI/runtime and the C# runtime must be
able to read each other's checkpoints, graph definitions, and history
records.

## Wire format

**JSON**, not MessagePack. The design docs (`design/03-Architecture.md`,
`design/06-Project-Structure.md`) leave the choice open ("JSON or
MessagePack"); JSON is chosen for Phase 1 because:

- It's what both `serde_json` and `System.Text.Json` support with zero extra
  crates/packages, keeping this phase's dependency footprint minimal.
- Fixtures are human-readable and diff cleanly in git and PR review.
- Nothing in Phases 1-4 has a demonstrated performance requirement that JSON
  can't meet; STRATA-SPEC-001's ring buffer (the actual hot path) carries
  raw token chunks, not these structures.

A binary MessagePack transport can be layered on top of the same Rust/C#
types later (both `rmp-serde` and `MessagePack-CSharp` can serialize the
exact types defined here) if a real throughput need shows up. Don't add it
speculatively.

**Timestamps are plain RFC3339 strings**, not native `DateTime`/`chrono`
types, deliberately. `chrono::DateTime<Utc>`'s default serde impl and
`System.Text.Json`'s `DateTimeOffset` handling don't agree on trailing `Z`
vs. `+00:00` or fractional-second trimming, which is exactly the kind of
silent cross-language drift this schema exists to prevent. Treating
timestamps as opaque strings at the interop boundary sidesteps it; each
runtime's richer domain types (e.g. the `ChatMessage.timestamp:
DateTime<Utc>` shown in `design/03-Architecture.md`) can parse/format at
their edges however they like.

## Layout

- `schema/*.schema.json` — canonical JSON Schema (2020-12) definitions. One
  file per record type.
- `schema/fixtures/*.sample.json` — hand-written sample instances, one per
  schema. These are read directly (via relative path, not copied) by both
  language test suites and must stay byte-identical to what a
  deserialize-then-reserialize round trip produces.
- `schema/fixtures/graph-validation/` (Phase 2) — hand-written
  `graph-definition`-shaped fixtures, one per accept/reject case
  (`valid`, `missing-entry-point`, `orphan-node`, `unguarded-cycle`, and the
  guarded-loop cases `guarded-loop-rejoin`, `guarded-first-edge-loop`,
  `unguarded-cycle-in-guarded-loop`, and the dangling-edge cases
  `dangling-edge-target`, `dangling-edge-source`, `dangling-edge-bridge`), read by
  both languages' compile-time graph validators to prove they produce
  equivalent verdicts on identical input. The hierarchy cases
  (`design/22-Graph-Structure-In-Definitions.md`) are `valid-hierarchy`,
  `child-orphan` (one error, `Remediate/Abort`), `grandchild-unguarded-cycle`
  (qualified two levels down), `empty-group`, `duplicate-branch`, and
  `errors-at-every-level`, which pins the error order (a level's own, then its
  groups', then each child's); the parity tests compare every error's code,
  node and related node, and the CLI tests read the same files.
  `invalid-quorum` (`design/26-Parallel-Group-Controls-And-Joins.md`) has a
  quorum above its group's branch count and, inside a subgraph, one of 0: two
  `STRATA_ERR_INVALID_QUORUM` errors, `Vote` and `Review/Ask`.
  `map-template-errors` (`design/27-Map-Fan-Out.md`) pins how a map's template
  is validated: a map quorum of 0 (`Vote`), an orphan in a template
  (`Investigate/*/Stray`, the `*` standing for the item), and an unguarded
  cycle in the template of a map inside a subgraph (`Review/Fan/*/Loop`).
  `wait-in-map` (`design/29-Event-Waits-And-Timers.md`) has a wait node at the
  top level (allowed), one in a map's template (`Fan/*/Window`) and one in a
  subgraph inside a map's template inside a subgraph
  (`Review/Batch/*/Inner/Hold`): two `STRATA_ERR_WAIT_IN_MAP` errors, in that
  order. `boundary-in-map` (`design/48-Boundaries-In-Definitions.md`) has an
  interrupt boundary at the top level (allowed), and, in a map's template, a
  boundary (`Fan/*/Read`), a wait node (`Fan/*/Window`) and a subgraph whose
  child has a boundary (`Fan/*/Inner/Sign`): a `STRATA_ERR_WAIT_IN_MAP` error,
  then two `STRATA_ERR_BOUNDARY_IN_MAP` errors, in that order (a level's wait
  errors, then its boundary errors, then each child's).
- `schema/fixtures/graph-export/` (demo 14) — what `StateGraph::to_definition()`
  (Rust) and `StateGraph<T>.ToDefinition()` (.NET) must write for one fixed
  graph, including the guarded `on_failure` edge a recovery-policy fallback
  adds. Both engines' `graph_export` tests build the same graph and compare
  their export to it, so the two runtimes write identical JSON; the CLI test
  `graph_export_gate.rs` feeds it to `strata graph validate`.
  `reject-route.json` (`design/21-Typed-Human-Signals.md`) is the same for a
  graph whose interrupt boundary `Approve` has a reject route to `Abort`: the
  guarded `on_reject` edge comes after any `on_failure` edges, and is all that
  keeps `Abort` from being an orphan. Since
  `design/48-Boundaries-In-Definitions.md`, `Approve` carries
  `"interrupt": { "timeout_ms": null }` and the level is `1.6.0`. `hierarchy.json`
  (`design/22-Graph-Structure-In-Definitions.md`) is the design doc's incident
  graph: node metadata (keys sorted), a parallel group whose branches carry
  metadata, and a subgraph whose child has a boundary with a reject route and a
  subgraph of its own, so the file has a `1.6.0` level (the boundary's,
  `design/48-Boundaries-In-Definitions.md`) inside a `1.1.0` level and a
  `1.0.0` one inside that. Both engines' `graph_structure` tests export it, and it
  round-trips through both engines' definition types. `joins.json`
  (`design/26-Parallel-Group-Controls-And-Joins.md`) is a `1.2.0` level: one
  parallel group per join policy (`all` with `keep_succeeded`, `first_matching`
  with a condition, `first_success`, a `quorum` of 2) and a group with the
  default join, which writes no `join`. Both engines' `group_joins` tests export
  it and round-trip it, and the CLI validates and views it. `map.json`
  (`design/27-Map-Fan-Out.md`) is a `1.3.0` level: a map whose template (a
  `1.1.0` level) holds a group and a subgraph, a map with a `first_matching`
  join, and a group with a join beside them (`1.3.0` is a superset of
  `1.2.0`). Both engines' `map` tests export it and round-trip it, and the CLI
  validates and views it. `waits.json` (`design/29-Event-Waits-And-Timers.md`)
  is a `1.6.0` level (`1.4.0` until `design/48-Boundaries-In-Definitions.md`
  marked its boundary): a boundary with a 30-minute timeout, a reject route and
  a timeout route, an
  event wait (`kind: "wait"`) with a timeout route, and a timer in a guarded
  loop, with node metadata; the guarded `on_timeout` edges come after the
  `on_reject` edge, and are all that keep `Escalate` and `Rollback` from being
  orphans. Both engines' `event_waits` tests export it and round-trip it, and
  the CLI validates and views it. `identity.json` (`design/31-Graph-Identity.md`)
  is what a compiled graph with an identity exports: a `1.5.0` level with an
  `identity` (`incident` version `1`) whose subgraph's child has its own
  (`remediation`, unversioned, also `1.5.0`) and a grandchild with none, which
  stays `1.0.0`. Both engines' `graph_identity` tests export it from compiled
  graphs and round-trip it, and the CLI validates and views it.
  `boundaries.json` (`design/48-Boundaries-In-Definitions.md`) is a compiled
  graph with an identity and a boundary on every kind of node that can be one:
  a plain node with a timeout and both routes, a parallel group with no
  timeout, a subgraph node with a timeout and no route (whose child has a
  boundary of its own, so the child is `1.6.0` too), and a map node with a
  timeout. Both engines' `boundary_definitions` tests export it from the
  builder (without the identity) and the compiled graph and round-trip it, and
  the CLI validates and views it.
- `schema/fixtures/checkpoint-lifecycle/` (Phase 4) — a single frozen
  `checkpoint`-shaped fixture (`frozen.json`) representing a graph
  interrupted mid-run. Read by both languages' HITL execution engines: each
  proves it can both produce a checkpoint matching this shape (freeze) and
  consume it to resume to the expected final state. This is the "freeze in
  Rust, resume in C#" proof in a test suite that can't literally pipe one
  live process into another — the two CI workflows run as separate jobs.
  `failed.json` (error recovery, `design/04-Error-Recovery.md`) is the
  counterpart for a `status: "failed"` checkpoint: a fallible node that
  exhausted its retries with no fallback. Each engine proves it can produce
  a checkpoint matching this shape (last committed state, `error` /
  `error_kind` / `attempts` metadata) and that `resume` re-enters the failed
  node from it.
  `nested.json` (`design/16-Nested-Interrupts.md`) is the counterpart for a
  checkpoint nesting a child's: a parent suspended at the subgraph node `Work`
  whose child is frozen at its own boundary `Approve`, written with
  `schema_version` `1.1.0`. Each engine proves it can freeze to exactly this
  shape at every level (timestamps aside) and resume from it to the expected
  final state, with the human's field override reaching the child's state.
  `cancelled.json` and `nested-cancelled.json` (`design/17-Async-Execution.md`)
  are the counterparts for a `status: "cancelled"` checkpoint (`schema_version`
  `1.2.0`, `cancel_reason` metadata): a run cancelled while `Charge` was
  running, and one cancelled while the child of the subgraph node `Work` was at
  `Send`. Each engine produces exactly these from a live cancelled run (at every
  level, timestamps aside) and resumes them to the expected final state
  (`rust/strata-core/tests/cancellation.rs`,
  `CancelledCheckpointFixtureParityTests.cs`).
  `metadata-nested.json` and `metadata-failed.json` (`design/18-Run-Observability.md`)
  carry caller metadata (`tenant`, `incident`) beside the engine's own keys, at
  every level: a run that froze inside the subgraph node `Work`, and the same
  thread resumed with an overridden `incident` whose child then failed at
  `Send`. Nothing in the format changes; each engine produces exactly these from
  live runs and resumes them, handing the stored metadata to the resumed run
  (`rust/strata-core/tests/run_metadata.rs`, `RunMetadataTests.cs`).
  `decision-failed.json` and `nested-decision.json`
  (`design/21-Typed-Human-Signals.md`) carry a recorded `decision`
  (`schema_version` `1.3.0`): a flat thread whose `Apply` failed after Alice
  approved `Approve` with typed overrides (an integer and an object), and a
  thread whose child, inside the subgraph node `Work`, took `Approve`'s reject
  route on Bob's rejection and failed at `Revoke`. Each engine produces exactly
  these from live runs (every level, `created_at` and `decided_at` aside) and
  resumes them, and the resumed nodes see the stored decision
  (`rust/strata-core/tests/human_signals.rs`, `HumanSignalTests.cs`). Since the
  same change, `nested-failed.json` and `metadata-failed.json` carry the
  child's decision too, and are `1.3.0`: they are produced by a failure after
  the child's boundary was answered, and that answer is now recorded.
  `ended.json` and `nested-ended.json` (`design/28-Ending-A-Thread.md`) are
  threads ended where they stood (`status: "ended"`, `schema_version`
  `1.4.0`, no `next_node`, `strata.end_reason` metadata): `frozen.json`'s
  thread ended by `oncall` while waiting at `PrepareWireTransfer`, and
  `nested.json`'s ended while its child waited at `Approve` inside `Work`,
  every level ended and carrying the caller's `tenant`. Each engine produces
  exactly these from a live end (every level, timestamps aside) and refuses to
  resume them (`rust/strata-core/tests/end_thread.rs`, `EndThreadTests.cs`).
  `waiting-event.json`, `waiting-approval.json` and `nested-timer.json`
  (`design/29-Event-Waits-And-Timers.md`) carry a `wait` (`schema_version`
  `1.5.0`): a deployment waiting for `deployment.finished` with an hour's
  timeout, an approval boundary with a thirty-minute timeout, and a verification
  window's five-minute timer inside the subgraph node `Verify` (only the
  innermost level waits). Each engine freezes to exactly these (`created_at`
  aside, and `wake_at` too in Rust, which has no clock seam), then signals, wakes
  or times them out (`rust/strata-core/tests/event_waits.rs`,
  `EventWaitTests.cs`), and every store round-trips them.
  `identified.json` and `nested-identified.json` (`design/31-Graph-Identity.md`)
  carry a `graph` (`schema_version` `1.6.0`): `frozen.json`'s thread written by
  `wire-transfer` version `2`, and `nested.json`'s with `incident` version `1`
  outside and `approval` version `3` on the level inside `Work`. Each engine
  freezes to exactly these (`created_at` aside) and refuses to resume them under
  another graph (`rust/strata-core/tests/graph_identity.rs`,
  `GraphIdentityTests.cs`), and every store round-trips them.
  `running.json`, `nested-running.json`, `claimed.json` and `completed.json`
  (`design/33-Durable-Execution.md`) are what a graph compiled with
  `CheckpointPolicy.EveryNode` writes: a thread past its first node
  (`status: "running"`, `schema_version` `1.7.0`, `strata.hops` metadata), one
  inside the subgraph node `Work` (running at both levels), the claim an
  approval with an override makes of an interrupted thread (running, with the
  `decision` and the overridden state, and no `resumed_at`), and a finished
  thread (`status: "completed"`, `1.0.0`, no `next_node`). Each engine writes
  exactly these (timestamps aside) and recovers the three running ones, the
  node in flight running again under the idempotency key their hop counts give
  it (`rust/strata-core/tests/durable_execution.rs`,
  `DurableExecutionTests.cs`).
- `schema/fixtures/checkpoint-log/` (demo 09) — the on-disk SQLite layout of
  the checkpoint log (next section): `columns.json` is the expected
  `PRAGMA table_info(checkpoints)`, and `frozen.row.json` is the expected raw
  row after saving `checkpoint-lifecycle/frozen.json`. Read by all three
  writers' tests (`strata-memory`, `Datalium.Strata.Tests`, the CLI).
  `nested.row.json` is the same for `checkpoint-lifecycle/nested.json`: its
  `nested` column holds the child's checkpoint as JSON. `cancelled.row.json` is
  the same for `checkpoint-lifecycle/cancelled.json` (`status` `'cancelled'`),
  and `decision.row.json` for `checkpoint-lifecycle/decision-failed.json`: its
  `decision` column holds the decision as JSON. `ended.row.json` is the row for
  `checkpoint-lifecycle/ended.json` (`status` `'ended'`), and `waiting.row.json`
  for `checkpoint-lifecycle/waiting-event.json`: its `wait` column holds the
  wait as JSON and `wake_at` its time. `identified.row.json` is the row for
  `checkpoint-lifecycle/identified.json`: `graph_id` and `graph_version` hold
  the outermost level's identity; every other row has them `NULL`.
  `running.row.json` is the row for `checkpoint-lifecycle/running.json`
  (`status` `'running'`, `design/33-Durable-Execution.md`): a status, and
  nothing new in the layout.
  `delta.rows.json` (`design/41-Checkpoint-Efficiency.md`) is one thread's rows
  as a store with deltas stores them (see "Delta checkpoints" below) and the
  checkpoint each decodes to: a plain row; a delta changing and adding keys at
  two levels; a delta of that delta removing a key, with a nested level that
  is a delta beside one that is whole. Every adapter in both engines stores the
  rows as they are, a row at a time, and loads, logs and lists them
  (`tests/deltas.rs`, `DeltaCheckpointTests.cs`); the CLI replays, diffs and
  logs them (`cli/tests/checkpoint_log_layout.rs`).
  `offloaded.rows.json` (`design/41-Checkpoint-Efficiency.md`, "Offload") is
  one thread's rows as a store with offload and deltas stores them (see
  "Offloaded values" below), the blobs they refer to, as the text a store
  holds, and the checkpoint each row decodes to: a whole row with a value
  offloaded; a delta that inherits it, with a nested level that appears whole
  with a value of its own offloaded; a delta of that delta whose value changes
  to another blob, its nested level a delta inheriting its blob; an interrupted
  whole row referring to a blob already there. Every adapter in both engines
  stores the rows and the blobs as they are and loads, logs and lists them
  (`tests/offload.rs`, `OffloadTests.cs`; SQLite also with the blobs in a
  filesystem blob store); the CLI replays, logs and inspects them, from the
  database and with `--blobs` (`cli/tests/checkpoint_log_layout.rs`).
  `threads-columns.json` / `postgres-threads-columns.json`
  (`design/19-Async-Queryable-Store.md`) are the expected columns of the
  `checkpoint_threads` head table, and `threads.rows.json` is a sequence of raw
  log rows (one arriving out of order) with the head rows the trigger must leave
  behind: every SQL store in both engines inserts them as plain SQL, so the
  test proves the trigger, not the writer. `redis-index.json` is the Redis
  index sets after saving the query seed below.
- `schema/fixtures/checkpoint-query/cases.json` (`design/19-Async-Queryable-Store.md`)
  — the query port's shared cases: a seed log of fourteen threads (every state,
  nested checkpoints, a resume stamp, two threads updated in the same second,
  and five waiting threads for the due cases: a timer, an event wait with a
  timeout and one without, a resumed timer, and a timer inside a subgraph;
  four of them written by graphs with an identity, for the graph cases of
  `design/31-Graph-Identity.md`),
  the summary each must list as, and list and log queries with their expected
  pages. Every store in both engines saves the seed and answers every case
  (`checkpoint_store_contract.rs`, `CheckpointQueryContractTests.cs`); the CLI
  runs the same seed through `--storage` against SQLite, Postgres and Redis
  (`cli/tests/storage_backends.rs`).
- `schema/fixtures/checkpoint-log/postgres-columns.json` and
  `schema/fixtures/history-log/postgres-columns.json` (demo 12) — the expected
  `information_schema` columns of the Postgres `checkpoints` and
  `history_records` tables, asserted by both runtimes' Postgres tests (which
  also hold the Postgres row to `checkpoint-log/frozen.row.json`).
- `schema/fixtures/checkpoint-log/leases-columns.json`,
  `postgres-leases-columns.json`, `leases.rows.json` and `redis-leases.json`
  (`design/32-Thread-Leases.md`) — where thread leases are kept (see "Thread
  leases"): the `checkpoint_leases` table in SQLite and Postgres, three raw
  rows with the lease every SQL store must report for each and the token its
  next acquisition gets, and the Redis keys an acquisition, a release and a
  second acquisition leave. Read by both runtimes' store tests
  (`rust/strata-memory/tests/thread_leases.rs`,
  `CheckpointLeaseTableTests.cs`) and the CLI's layout test.
- `schema/fixtures/checkpoint-log/schema-versions.json`,
  `schema-versions-columns.json` and `postgres-schema-versions-columns.json`
  (`design/34-Storage-Hardening.md`) — the `strata_schema_version` table every
  SQL database has (see "Schema versions"): its name, the version each
  component is at in a database a current store has prepared (`checkpoints` 2
  since value offload), and its columns in SQLite and in Postgres. Read by
  both runtimes' hardening tests (`rust/strata-memory/tests/storage_hardening.rs`,
  `StorageHardeningTests.cs`) and the CLI's layout test.
- `schema/fixtures/checkpoint-log/blobs-columns.json` and
  `postgres-blobs-columns.json` (`design/41-Checkpoint-Efficiency.md`,
  "Offload") — the `checkpoint_blobs` table, the `checkpoints` component's
  schema version 2 (see "Offloaded values"), in SQLite and in Postgres. Read
  by both runtimes' offload tests, after migrating a version-1 database, and
  the CLI's layout test.
- `schema/fixtures/cloud/` (`design/30-Strata-Cloud-Store.md`) — Strata Cloud's
  SDK wire, one recorded exchange per file: the `operation` (`save`, `load` or
  `decisions`; since SG-36, `design/43-Cloud-Leases-And-Listing.md`, also
  `acquire`, `renew`, `release`, `get_lease`, `save_under_lease`, `list` and
  `log`) and its input (a `checkpoint` path, an `expected_sequence` for a
  conditional save, a `thread_id`, a decisions `query`, an `owner` and
  `ttl_ms`, a `lease`, a thread `query`, or `after_sequence` and `limit`), the
  `exchanges` (each the `request` the adapter must send: method, path, exact
  query, a list value standing for a repeated key, headers that must be
  present, with `{api_key}` standing for the test key, `present_headers` that
  must be sent with any value, `absent_headers`, and `"checkpoint"` for a body
  equal by value to the one sent, or a JSON object it must equal; and the
  `response` the fake gives, or `"lost": true` for one that never arrives), and
  what the call must `expect` (`saved`, a `checkpoint` and `sequence`, a
  decisions `page`, `acquired` and a `lease`, a `lease` or `null`, `released`,
  a `leased_save` of `saved`, `not_latest` or `lease_lost`, a thread `page`,
  log `sequences`, `checkpoints` and `next_after_sequence`, or an `error` with
  its `kind`, `status`, `code`, `attempts` and `retry_after_ms`;
  `min_waits_ms` for the waits between retries; `same_header` for a header
  every attempt must send with the same value). Both adapters run every file against a fake server
  (`rust/strata-cloud/tests/fixtures.rs`, `Cloud/CloudFixtureTests.cs`).
- `schema/fixtures/scheduler/cases.json` (`design/40-Run-Scheduler.md`) — not
  a wire format: the run scheduler's admission rules, as scripts of arrivals
  (with a priority and a tenant), releases, cancellations, tenant limit changes
  and a close, against a scheduler with given options, and who holds a slot,
  who waits and who is refused (with the reason) after each step. Both engines
  replay every case through the public API
  (`rust/strata-core/tests/scheduler.rs`, `Scheduling/RunSchedulerTests.cs`).
- `schema/fixtures/tools/argument-problems.json` (`design/20-Tool-Dispatch-And-Registry.md`) —
  not a wire format: what a tool call's arguments got wrong against the
  tool's `parameters` schema (not JSON, not an object, a required argument
  missing, a value of the wrong type), in the words an agent sends back to
  the model. Both engines replay every case
  (`rust/strata-core/tests/argument_problems.rs`,
  `Tools/ToolArgumentProblemTests.cs`).
- `schema/fixtures/ai/agent-cases.json` (`design/44-LLM-Integration.md`) —
  not a wire format: the agent's loop, as a scripted model's turns (text, tool
  calls, faults), a registry (tools that answer, fail, are left out of the
  agent's subset or are denied by middleware) and options, with the
  declarations the model sees, every tool call and its result, what each tool
  turn sent back, the repair requests, the run's model-call and tool-call
  events, and the output or the node's failure. Both engines replay every case
  through an agent in a graph run (`rust/strata-ai/tests/agent_cases.rs`,
  `AI/AgentCaseTests.cs`).

## SQLite checkpoint log

Both runtimes' SQLite `CheckpointStore`s and the `strata` CLI read and write
one table, so a database file produced by any of them can be read — and a
frozen thread resumed — by the others (`CLAUDE.md`'s cross-language
constraint, proven end to end by demo `09-cross-language-interop`).

```sql
CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id      TEXT NOT NULL,
    sequence       INTEGER NOT NULL,
    schema_version TEXT NOT NULL,
    status         TEXT NOT NULL,   -- 'interrupted' | 'completed' | 'failed' | 'cancelled' | 'ended' | 'running'
    suspended_node TEXT,
    next_node      TEXT,
    frozen_state   TEXT NOT NULL,   -- JSON text
    metadata       TEXT,            -- JSON object text, or NULL
    created_at     TEXT NOT NULL,
    resumed_at     TEXT,
    nested         TEXT,            -- JSON text of the child's checkpoint, or NULL
    decision       TEXT,            -- JSON text of the level's decision, or NULL
    wait           TEXT,            -- JSON text of the level's wait, or NULL
    wake_at        TEXT,            -- the innermost level's wait.wake_at (derived), or NULL
    graph_id       TEXT,            -- the outermost level's graph.id, or NULL
    graph_version  TEXT,            -- the outermost level's graph.version, or NULL
    PRIMARY KEY (thread_id, sequence)
)
```

It is an **append-only log**: `save` inserts a row at the next `sequence` for
its `thread_id` (counting from 0), and `load` returns the highest-sequence
row. Resuming a thread therefore appends a second row (the same checkpoint
with `resumed_at` set) rather than overwriting the first, which is what
`strata checkpoint log`/`diff`/`replay` show as history. Each column is one
`checkpoint` field, so the JSON Schema above still defines the values.

The DDL is a literal string in two places — `strata-memory`
(`CHECKPOINT_LOG_DDL`) and `Datalium.Strata.Storage` (`EfCoreCheckpointStore.CheckpointLogDdl`)
— and each language's tests compare the created table against
`fixtures/checkpoint-log/columns.json`. The CLI reads and creates files through
`strata-memory`'s store (`design/19-Async-Queryable-Store.md`), so it has no copy.

- **Single writer.** `save` computes the next sequence in the statement that
  inserts it; two processes racing on one thread collide on the primary key
  rather than corrupting the log. No retry.
- **The conditional save (2026-10-06).** `save_if_latest` /
  `SaveIfLatestAsync` (`design/28-Ending-A-Thread.md`, "Races") inserts at
  `expected + 1` only `WHERE` the thread's `MAX(sequence)` is still `expected`,
  in one statement; a primary-key collision counts as losing. `load_latest` /
  `LoadLatestAsync` returns the latest row with its `sequence`. Ending a thread
  and a resume's stamp use them, so the two can't both win. Postgres does the
  same under its per-thread advisory lock; Redis with `SAVE_IF_LATEST_SCRIPT`.
- **Ended checkpoints (2026-10-06).** `status` gains `'ended'`
  (`design/28-Ending-A-Thread.md`): a thread ended where it stood, at every
  level, with `schema_version` `1.4.0` and no `next_node`. A `TEXT` column, so
  no DDL change and no migration; the head-table trigger derives the `ended`
  state from it as it is.
- **Running and completed checkpoints (2026-10-10).** `status` gains
  `'running'` (`design/33-Durable-Execution.md`): what a graph compiled with
  `CheckpointPolicy.EveryNode` writes before its first node and after every
  node that commits, at every level, with `schema_version` `1.7.0`. Such a
  graph also writes `'completed'` when it finishes, which the column always
  allowed and nothing wrote. A `TEXT` column, so no DDL change and no
  migration; the head-table trigger derives the `running` and `completed`
  states as they are. A durable run writes a row per node: each holds the whole
  state.
- **Waits (2026-10-06).** `wait` holds the level's `wait`
  (`design/29-Event-Waits-And-Timers.md`), the JSON of `checkpoint.schema.json`'s
  `wait` object, or `NULL`; a nested level carries its own inside `nested`, like
  `decision`. `wake_at` is **derived**: the innermost level's `wait.wake_at`,
  written by the store when it inserts the row so the head-table trigger can
  copy it without parsing nested JSON; readers ignore it. A row with a `wait` at
  any depth has `schema_version` `1.5.0`. Every SQL store (and the CLI) adds both
  columns when it opens a log created before them (`JSONB` and `TEXT` in
  Postgres), and adds the head table's `wake_at` and replaces its trigger; old
  rows keep `NULL`, which is right: nothing waited with a time before. Redis
  keeps a `waking` index set (below). Older runtimes refuse to resume a `1.5.0`
  checkpoint.
- **Graph identity (2026-10-09).** `graph_id` and `graph_version` hold the
  outermost level's `graph` (`design/31-Graph-Identity.md`), the identity of
  the graph that wrote it, or `NULL`; a nested level carries its own `graph`
  inside `nested`. A reader builds `graph` from them (`graph_id` `NULL` is
  none). A row with a `graph` at any depth has `schema_version` `1.6.0`. Every
  SQL store (and the CLI) adds both columns when it opens a log created before
  them, adds them to the head table and replaces its trigger; old rows keep
  `NULL`: nothing had an identity before. Redis needs nothing. Older runtimes
  refuse to resume a `1.6.0` checkpoint.
- **User state is not normalised.** `frozen_state` is the graph's own state
  serialised by whichever runtime froze it, so two runtimes resuming each
  other's threads must serialise their state types to the same JSON — in
  practice, `snake_case` field names (`[JsonPropertyName]` in C#). A
  `HumanSignal` field override is keyed by that JSON name.
- **Nested checkpoints.** `nested` holds the child's whole checkpoint, exactly
  the JSON of `checkpoint.schema.json` (which may itself carry a `nested`), or
  `NULL` for the overwhelming majority of rows. A row with one has
  `schema_version` `1.1.0` (`1.2.0` if anything in it is cancelled). It is one column rather than a row per level because
  a nested checkpoint is not a thread of its own: only the outermost is
  `load`ed, resumed and stamped `resumed_at`.
- **The `nested` column (2026-09-21).** It was added to all of the layouts in
  this document. A SQLite or Postgres database created before it has no such
  column; every store (and the CLI) adds it when it opens the file, so old rows
  keep `NULL` and nothing is lost (`design/16-Nested-Interrupts.md`, "Upgrading
  an old log"). Redis needs nothing — its elements are the JSON, and `nested`
  is simply absent when there is no child.
- **Failed checkpoints nest too.** A `failed` checkpoint whose failed node is a
  subgraph carries the child's own `failed` checkpoint in `nested`
  (`schema_version` `1.1.0`), and can be resumed
  (`fixtures/checkpoint-lifecycle/nested-failed.json`).
- **Cancelled checkpoints (2026-10-01).** `status` gains `'cancelled'`
  (`design/17-Async-Execution.md`). It is a `TEXT` column in every layout, so
  there is no DDL change and no migration; every store maps the new value. A
  cancelled checkpoint nests a cancelled child the same way, and a `failed`
  one may nest a `cancelled` child (its subgraph node timed out).
- **Caller metadata (2026-10-02).** A run's metadata (`RunOptions.Metadata` /
  `RunConfig::with_metadata`, `design/18-Run-Observability.md`) goes in the
  existing `metadata` column, beside the engine's keys, on every checkpoint it
  writes. No DDL change, no migration, no new `schema_version`.
- **The thread head table (2026-10-02).** See "Thread head table" below.
- **The `decision` column (2026-10-04).** `decision` holds the decision in
  effect at the checkpoint's level (`design/21-Typed-Human-Signals.md`), the
  JSON of `checkpoint.schema.json`'s `decision` object, or `NULL`. A row with
  one, at any depth, has `schema_version` `1.3.0`. A nested checkpoint carries
  its own level's decision inside the `nested` JSON. Every SQL store (and the
  CLI) adds the column when it opens a log created before it, as for `nested`
  (`JSONB` in Postgres); Redis needs nothing. Older runtimes refuse to resume a
  `1.3.0` checkpoint.
- **Breaking change (2026-09-20).** Earlier `SqliteCheckpointStore` /
  `EfCoreCheckpointStore` databases (`checkpoints(thread_id, checkpoint_json)`
  and `Checkpoints(ThreadId, CheckpointJson)`) are not readable by the current
  stores, and there is no migration. Nothing outside demo scratch files used
  them.

## Postgres checkpoint log

`PostgresCheckpointStore` in both runtimes writes the SQLite log above with
Postgres types, so a database written by either can be resumed by the other
(demo `12-pluggable-storage`).

```sql
CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id      TEXT NOT NULL,
    sequence       BIGINT NOT NULL,
    schema_version TEXT NOT NULL,
    status         TEXT NOT NULL,   -- 'interrupted' | 'completed' | 'failed' | 'cancelled' | 'ended' | 'running'
    suspended_node TEXT,
    next_node      TEXT,
    frozen_state   JSONB NOT NULL,
    metadata       JSONB,           -- JSON object, or NULL
    created_at     TEXT NOT NULL,
    resumed_at     TEXT,
    nested         JSONB,           -- the child's checkpoint, or NULL
    decision       JSONB,           -- the level's decision, or NULL
    wait           JSONB,           -- the level's wait, or NULL
    wake_at        TEXT,            -- the innermost level's wait.wake_at (derived), or NULL
    graph_id       TEXT,            -- the outermost level's graph.id, or NULL
    graph_version  TEXT,            -- the outermost level's graph.version, or NULL
    PRIMARY KEY (thread_id, sequence)
)

CREATE TABLE IF NOT EXISTS history_records (
    session_id TEXT NOT NULL,
    sequence   BIGINT NOT NULL,
    role       TEXT NOT NULL,       -- 'system' | 'user' | 'assistant' | 'tool'
    content    TEXT NOT NULL,
    timestamp  TEXT NOT NULL,       -- RFC 3339
    PRIMARY KEY (session_id, sequence)
)
```

The DDL is a literal string in two places — `strata-memory`
(`POSTGRES_CHECKPOINT_LOG_DDL`, `POSTGRES_HISTORY_DDL`) and
`Datalium.Strata.Storage` (`PostgresCheckpointStore.CheckpointLogDdl`,
`PostgresHistoryStore.HistoryLogDdl`) — and both runtimes' tests compare the
created tables against the two `postgres-columns.json` fixtures.

- **Concurrent writers are supported** (unlike SQLite). `save`/`append` runs in
  a transaction that first takes `pg_advisory_xact_lock(hashtextextended(
  '<scope>:<thread or session id>', 0))` — `scope` is `checkpoints` or
  `history` — then inserts at `MAX(sequence) + 1`. The hash is computed
  server-side, so both runtimes contend on the same lock. Creating a table
  takes a database-wide advisory lock (key `0x535452415441`) because
  `CREATE TABLE IF NOT EXISTS` from two sessions at once can fail on
  `pg_type`'s unique index. Both are covered by concurrency tests in each
  language, which fail if the lock is removed.
- **`JSONB` does not round-trip bytes.** `frozen_state` and `metadata` come back
  with normalised whitespace and key order. State is read by field name, so
  this is harmless, but the stored text is not what was saved.
- **Search path.** Tables are created in the connection's first `search_path`
  schema (`public` by default); the tests give each test its own schema this
  way.
- **NUL.** Postgres `TEXT` and `JSONB` cannot hold `U+0000`; a message or state
  containing one is rejected by the server.
- **Head table.** Postgres has the same `checkpoint_threads` table and trigger
  as SQLite (below), with `BIGINT` / `JSONB`, the `"C"` collation on
  `thread_id` and `updated_at` (so threads sort byte-wise, as in every other
  store), a GIN index on `metadata` for `@>` filters, and a `plpgsql` trigger
  function `strata_checkpoints_head()`. `CREATE OR REPLACE TRIGGER` needs
  Postgres 14 or later.
- **Readable by the `strata` CLI** with `--storage postgres://…`.

## Thread head table

Listing threads (`ICheckpointQuery` / `CheckpointQuery`,
`design/19-Async-Queryable-Store.md`) reads one row per thread instead of
scanning the log. Every SQL store creates it beside `checkpoints`, with a
trigger that keeps it current **whoever writes the log** — either runtime, a
runtime older than this table, or raw SQL:

```sql
CREATE TABLE IF NOT EXISTS checkpoint_threads (
    thread_id  TEXT NOT NULL PRIMARY KEY,
    sequence   INTEGER NOT NULL,   -- the latest checkpoint's
    state      TEXT NOT NULL,      -- 'interrupted' | 'failed' | 'cancelled' | 'resumed' | 'completed' | 'ended' | 'running'
    updated_at TEXT NOT NULL,      -- COALESCE(resumed_at, created_at)
    metadata      TEXT,            -- the latest checkpoint's, for filtering
    wake_at       TEXT,            -- the latest checkpoint's derived wake_at
    graph_id      TEXT,            -- the latest checkpoint's graph_id
    graph_version TEXT             -- the latest checkpoint's graph_version
);
-- + indexes on (state, updated_at, thread_id), (updated_at, thread_id), (wake_at)
--   and (graph_id, updated_at, thread_id)
-- + AFTER INSERT ON checkpoints: upsert the row, WHERE excluded.sequence > checkpoint_threads.sequence
```

- **`state`** is the thread's state, not the raw status: a checkpoint with
  `resumed_at` set is `resumed`, whatever its status, because the resume stamp
  is a re-save of the checkpoint being resumed and a resumed run that completes
  writes nothing.
- **`updated_at`** is `resumed_at` when set, because the resume stamp keeps the
  original `created_at`.
- **`wake_at`** (`design/29-Event-Waits-And-Timers.md`) is copied from the row
  the head points at, whatever its state. A due listing (`ThreadQuery.DueBy` /
  `due_by`) is `state = 'interrupted' AND wake_at <= ?`, so a resumed timer, which
  keeps its `wake_at`, is not due.
- **`graph_id` and `graph_version`** (`design/31-Graph-Identity.md`) are copied
  from the row the head points at. `ThreadQuery.GraphId` / `graph_id` is
  `h.graph_id = ?`, and a `GraphVersion` adds `h.graph_version = ?`.
- **Migration.** A store opening a log without the table creates it, its
  indexes and the trigger, and backfills it from the log, once, in one
  transaction (`BEGIN IMMEDIATE` in SQLite; under the DDL advisory lock in
  Postgres, where creating the trigger also blocks inserts until the backfill
  commits).
- The DDL is a literal string in `strata-memory` (`CHECKPOINT_THREADS_DDL`,
  `POSTGRES_CHECKPOINT_THREADS_DDL`, and the `…_BACKFILL`s) and
  `Datalium.Strata.Storage` (`CheckpointThreadsDdl` / `CheckpointThreadsBackfill`
  on both SQL stores), pinned by `threads-columns.json`,
  `postgres-threads-columns.json` and `threads.rows.json`.

## Thread leases

A run given a lease (`RunOptions.Lease` / `RunConfig::with_lease`,
`design/32-Thread-Leases.md`) holds its thread in the store the thread's
checkpoints are in, so one owner at a time works on it and a stale owner's
saves are refused. Both engines write the same rows and keys, so a lease one
runtime takes is honoured by the other. A checkpoint carries nothing about
leases: no field, no `schema_version`.

```sql
-- SQLite                                          -- Postgres
CREATE TABLE IF NOT EXISTS checkpoint_leases (     CREATE TABLE IF NOT EXISTS checkpoint_leases (
    thread_id  TEXT NOT NULL PRIMARY KEY,              thread_id  TEXT NOT NULL PRIMARY KEY,
    owner      TEXT,                                   owner      TEXT,
    token      INTEGER NOT NULL,                       token      BIGINT NOT NULL,
    expires_at INTEGER                                 expires_at TIMESTAMPTZ
)                                                  )
```

- **One row per thread that ever had a lease**, kept until the thread is
  purged (see "Retention"), so **`token`** only grows: 1 for a thread's first lease, one more for every acquisition
  after. The token is what fences a save, not the expiry.
- **Held** means `expires_at` is not null and after now. A release sets
  `owner` and `expires_at` to null and keeps the token.
- **`expires_at`** is Unix milliseconds in SQLite, on the adapter's clock (the
  system clock unless a test gives it another), and a `TIMESTAMPTZ` in
  Postgres, compared with `clock_timestamp()`: the server's clock, read when
  the statement runs, not when its transaction began.
- **Acquiring** is one upsert: insert at token 1, or on conflict set the
  owner, `token + 1` and the expiry `WHERE` the row isn't held, `RETURNING` the
  token. No row back means the thread is held. In Postgres it runs under the
  thread's advisory lock, the one a save takes.
- **A save under a lease** is the checkpoint insert with one more condition,
  `(SELECT token FROM checkpoint_leases WHERE thread_id = ?) = ?`: the same
  statement in SQLite, a read under the advisory lock in Postgres. A save made
  with no lease doesn't look at the table.
- **Migration.** A store opening a database without the table creates it.
- The DDL is a literal string in `strata-memory` (`CHECKPOINT_LEASES_DDL`,
  `POSTGRES_CHECKPOINT_LEASES_DDL`) and `Datalium.Strata.Storage`
  (`CheckpointLeasesDdl` on both SQL stores), pinned by
  `fixtures/checkpoint-log/leases-columns.json` and
  `postgres-leases-columns.json`. `leases.rows.json` holds three rows as any
  writer leaves them (held, expired, released), the lease every SQL store must
  report for each, and the token its next acquisition gets.

In Redis a lease is two keys (see "Redis layout"): a hash that exists while
the thread is held and expires with the lease, and a counter that never
expires. `fixtures/checkpoint-log/redis-leases.json` pins them through an
acquisition, a release and a second acquisition.

## Schema versions

Each SQL database says what it holds (`design/34-Storage-Hardening.md`,
"Migrations"), in a table both runtimes and the CLI read and write:

```sql
-- SQLite                                          -- Postgres
CREATE TABLE IF NOT EXISTS strata_schema_version ( CREATE TABLE IF NOT EXISTS strata_schema_version (
    component TEXT NOT NULL PRIMARY KEY,               component TEXT NOT NULL PRIMARY KEY,
    version   INTEGER NOT NULL                         version   BIGINT NOT NULL
)                                                  )
```

- **One row per component:** `checkpoints` (the log, its head table and the
  leases) and `history` (the history log). No row means version 0: a database
  from before the table, or an empty one.
- **Version 1 is the layout described in this file before value offload.** A
  store that finds a component at 0 runs its baseline, which creates the
  tables or adds whatever an older layout lacks (every "added when an older log
  is opened" above), and records 1. A later change to a table is a numbered
  step, written once in each runtime, and bumps the number in
  `fixtures/checkpoint-log/schema-versions.json`.
- **The `checkpoints` component's version 2** (`design/41-Checkpoint-Efficiency.md`,
  "Wire format") is the first step: it creates `checkpoint_blobs` (see
  "Offloaded values"). It runs when a current store opens a database, whether
  or not that store offloads, so a database a current store has opened is at
  2 and an older runtime refuses it.
- **A database newer than the build is refused**, unchanged: `database is at
  checkpoints schema version 3, and this build knows 2: upgrade the runtime`.
  A Postgres store told not to migrate refuses one that is behind: `… and this
  build needs 2: run its migration`.
- **One transaction.** A store reads the version, runs what is missing and
  records the version it reached under the lock it takes for DDL (Postgres's
  advisory lock `0x535452415441`, SQLite's immediate transaction), so two
  stores opening one database at once don't interleave.
- The SQLite history stores, whose files the runtimes don't share, record
  nothing. A runtime from before this table ignores it.
- Identical DDL in both runtimes (`SCHEMA_VERSION_DDL` /
  `POSTGRES_SCHEMA_VERSION_DDL` in `strata-memory`'s `schema_versions`,
  `StoreSchemaVersions.SqliteDdl` / `PostgresDdl`), pinned by
  `schema-versions-columns.json` and `postgres-schema-versions-columns.json`.

## Retention

Nothing is deleted unless an application asks
(`design/34-Storage-Hardening.md`, "Retention"). `PurgeAsync` / `purge`
deletes threads whole, picked by their **latest** checkpoint: its state (by
default `completed` and `ended`) and its `updated_at`, before a cutoff, oldest
first.

- **SQL:** the thread's rows in `checkpoints`, its row in `checkpoint_threads`,
  its row in `checkpoint_leases` and its blobs in `checkpoint_blobs`, in one
  transaction. A purged id starts over: sequence 0, lease token 1.
- **Redis:** `PURGE_SCRIPT` / `RedisCheckpointStore.PurgeScript` (identical in
  both engines) deletes the thread's list, its lease-token counter and its blob
  hash, and takes it out of every index set, deciding from the list's own
  latest element in the same atomic step.
- **Blobs in another blob store** (see "Offloaded values") are deleted after
  the thread's rows, through the blob store's `DeleteThreadAsync` /
  `delete_thread`; a failure there is the purge's, and the rows are already
  gone.
- **A thread a live lease holds is never purged**, in any store.
- Any other writer may do the same by hand; the head table has no `DELETE`
  trigger, so a writer that deletes a thread's rows deletes its head row too.

## Delta checkpoints

A store given deltas (`design/41-Checkpoint-Efficiency.md`: .NET
`DeltaOptions` on `PostgresStoreOptions`, `RedisStoreOptions` and
`SqliteStoreOptions`; Rust `Deltas` and `with_deltas`) stores a `running`
checkpoint as the changes to the thread's previous row, when it knows that row
and is still fewer than `FullEvery` (16) rows from a whole one; every other
checkpoint, and every `FullEvery`-th row, is stored as it always was. Such a
row is the checkpoint's wire JSON with `schema_version` `1.8.0` and each
level's `frozen_state` an envelope (`checkpoint.schema.json`, `$defs`):

```json
{ "checkpoint_version": "1.7.0", "delta_of": 41, "depth": 3, "set": { "step": 12 }, "unset": ["draft"] }
```

`checkpoint_version` is the version a load restores; `delta_of` the row just
before it; `depth` how many rows back the nearest whole one is. A level is
`state` (whole) or `set` and `unset` (its top-level keys that changed or are
new, and those that went, against the same level of row `delta_of`); a nested
level's envelope has only those. **There is no layout change**: in SQL the
row's `schema_version` column holds `1.8.0`, `frozen_state` the outermost
envelope and `nested` the nested levels with theirs, every other column as for
any checkpoint; in Redis the list element is the encoded JSON, and the index
and purge scripts read only fields the encoding doesn't touch.

A delta is appended only if `delta_of` is still the thread's latest, decided in
the same atomic step as the append: under the thread's lock in Postgres, in the
insert's own `WHERE` in SQLite, in the save script in Redis (each save script
takes the row it relies on and returns `-2`, having written nothing, when it
moved). **"Still the latest" is the row's sequence and its token**: in SQL the
row at that sequence must have the `created_at` the store remembers, in Redis
the latest element's SHA-1 must be the one the store remembers (since value
offload, which closed the gap a purged and re-run thread id left). Otherwise
the store writes the checkpoint whole. A load reads the latest row and the
`depth` rows before it (one SQL statement; in Redis a second, range read) and
applies them in order. **Every store decodes every row it reads, whatever its
own options**, the CLI included, so the engine never sees `1.8.0`; a runtime
that predates it refuses an encoded row by its version. Upgrade every reader
before a writer turns deltas on (see "Compatibility").

## Offloaded values

A store given offload (`design/41-Checkpoint-Efficiency.md`, "Offload": .NET
`OffloadOptions` on `PostgresStoreOptions`, `RedisStoreOptions` and
`SqliteStoreOptions`; Rust `Offload` and `with_offload`) stores each top-level
value of a level's state whose JSON is at least `Threshold` bytes (64 KiB)
once, in a blob store, and names it in the level's envelope. Such a row is
`1.8.0`'s encoded form with `schema_version` `1.9.0`, a delta or not, of any
status:

```json
{ "checkpoint_version": "1.7.0", "delta_of": null, "depth": 0, "state": { "step": 1 },
  "blobs": { "evidence": { "hash": "sha256:9f2c…", "bytes": 1048576 } } }
```

- **The blob** is the value's JSON text, UTF-8, keys sorted at every depth and
  compact, and `hash` is `sha256:` and its lowercase hex SHA-256. A key in
  `blobs` is absent from `state` and `set`. In a delta level only a value that
  changed is named; one that didn't is inherited from the row before,
  offloaded or not.
- **Where blobs go:** the checkpoint store's own database by default, or any
  `ICheckpointBlobStore` / `BlobStore` its options name (built in: in memory,
  and the filesystem, `<root>/<thread id as lowercase hex of its UTF-8>/<the
  hash's hex digest>`). A store reads blobs from where its options say,
  whatever its threshold, and from nowhere else.
- **In the database**, blobs are kept per thread:

  ```sql
  CREATE TABLE IF NOT EXISTS checkpoint_blobs (
      thread_id TEXT NOT NULL,
      hash      TEXT NOT NULL,
      content   BLOB NOT NULL,   -- BYTEA in Postgres
      PRIMARY KEY (thread_id, hash)
  )
  ```

  the `checkpoints` component's schema version 2 (see "Schema versions"),
  identical in both runtimes (`CHECKPOINT_BLOBS_DDL` / `POSTGRES_CHECKPOINT_BLOBS_DDL`,
  `EfCoreCheckpointStore.CheckpointBlobsDdl` / `PostgresCheckpointStore.CheckpointBlobsDdl`),
  pinned by `blobs-columns.json` and `postgres-blobs-columns.json`; in Redis,
  `{prefix}:blobs:{thread}` (see "Redis layout"). A blob goes in with its row,
  in the same statement (Postgres), immediate transaction (SQLite) or script
  (Redis), and only if the row does. In another blob store it is written before
  the row; a row whose save then fails leaves a blob nobody refers to, which
  goes when the thread is purged.
- **A store doesn't write a blob twice:** it remembers which blobs the thread's
  latest row refers to, and a save that leaves one out relies on that row
  still being the latest, checked like a delta's base. Otherwise the save is
  written whole, its blobs with it.
- **A load** fetches each blob the decoded rows refer to once, and checks it
  against its hash: `checkpoint '<thread>' #<sequence> refers to blob <hash>
  at '<key>', which the blob store doesn't have` (or `…, whose content doesn't
  match`). A decoded offloaded value of a whole level comes after the level's
  other keys, and its objects' keys in the blob's (sorted) order: state is read
  by name.
- **A listing** reads no state, so it reads no blob.

An older runtime refuses a `1.9.0` row by its version, and a database at
version 2 by its schema version: upgrade every reader, the CLI included, before
a store opens a shared database (see "Compatibility").

## Thread listing cursor

A listing page's `next_cursor` / `NextCursor` is opaque to callers, but both
engines encode it the same way, so a cursor from one decodes in the other (and
in the CLI): **base64url without padding, of the JSON array
`[updated_at, thread_id]`** — the last returned thread's stored `updated_at`
text and id. Listing order is `updated_at` descending, then `thread_id`
descending, both compared byte-wise (ordinally); a page continues with the
threads that sort strictly after the cursor. Redis builds the `updated_at` of
its cursors from the index score (below), which is the same text for
timestamps written the way both engines write them.

## Redis layout

`RedisCheckpointStore` / `RedisHistoryStore` in both runtimes use Redis lists
and sorted sets; there is no schema to create. The default prefix is `strata`.

| Key | Content |
|---|---|
| `{prefix}:checkpoints:{thread_id}` | list: one JSON `checkpoint` per save — exactly the shape of `checkpoint.schema.json` and `fixtures/checkpoint-lifecycle/*.json`, a nested child included |
| `{prefix}:threads` | sorted set: every thread, scored by its latest checkpoint's `updated_at` |
| `{prefix}:threads:{state}` | sorted set, one per state (`interrupted`, `failed`, `cancelled`, `resumed`, `completed`, `ended`, `running`): the threads whose latest checkpoint is in it |
| `{prefix}:threads:waking` | sorted set: the threads whose latest checkpoint is interrupted, not resumed, with an innermost `wait.wake_at` (`design/29-Event-Waits-And-Timers.md`), scored by `updated_at` like the others |
| `{prefix}:threads:indexed` | string: present once the prefix has been indexed |
| `{prefix}:leases:{thread_id}` | hash, `owner` and `token`, with a `PX` expiry: present while the thread is held (`design/32-Thread-Leases.md`) |
| `{prefix}:lease-tokens:{thread_id}` | string: the thread's latest lease token, a counter that never expires |
| `{prefix}:blobs:{thread_id}` | hash: the thread's offloaded values, hash to content (`design/41-Checkpoint-Efficiency.md`, "Offload"), kept, expired and deleted with its list |
| `{prefix}:history:{session_id}` | list: one JSON `{"role", "content", "timestamp"}` (`role` lowercase, `timestamp` RFC 3339) per `append` |

- **The thread index** (`design/19-Async-Queryable-Store.md`). A score is the
  `updated_at` text's digits as a number (`2026-10-02T09:15:00Z` scores
  `20261002091500`), so it sorts like the text. A save is one Lua script
  (`SAVE_SCRIPT` / `RedisCheckpointStore.SaveScript`, identical in both
  engines) that `RPUSH`es and moves the thread to the right sets atomically;
  `INDEX_SCRIPT` / `IndexScript` indexes one thread from its own latest element
  (used when a prefix is indexed); `CANDIDATES_SCRIPT` / `CandidatesScript`
  positions a listing after its cursor. `SAVE_IF_LATEST_SCRIPT` /
  `SaveIfLatestScript` is the save script behind an `LLEN` check, the
  conditional save (`design/28-Ending-A-Thread.md`). The save and index scripts
  loop over however many state sets they're given (six since `ended`, seven
  since `running`, `design/33-Durable-Execution.md`; the index script names
  them, so it changed). Since
  waits (`design/29-Event-Waits-And-Timers.md`) the `waking` set is `KEYS[3]`, the
  state sets follow, and the save scripts take a fifth argument, `1` when the
  thread is waking (the conditional save's expected sequence moves to the
  sixth); the index script walks to the innermost level to decide it. Since
  delta checkpoints (`design/41-Checkpoint-Efficiency.md`) the three save
  scripts take one more argument, after the ttl: the sequence the checkpoint
  is a delta of, empty for none (`ARGV[7]`, `ARGV[8]`, `ARGV[9]`), and return
  `-2` without writing when it is no longer the thread's latest; the fenced
  save script returns the list's length when it saves, where it returned 1.
  Since value offload the three save scripts take the thread's blob hash as a
  key (last, or before the token counter in the fenced script), the SHA-1 of
  the row relied on right after its sequence (`-2` when the latest element
  isn't that row), and then any number of pairs of a blob's hash and content,
  which they `HSETNX` once every check has passed; they `PEXPIRE` and
  `PERSIST` the blob hash with the list. `PURGE_SCRIPT` takes the blob hash
  last and deletes it with the list. A due
  listing reads only the `waking` set and applies every other filter to each
  candidate's latest checkpoint. A store indexes an unindexed prefix the
  first time it connects, and `rebuild_index` / `RebuildIndexAsync` does it on
  demand. Pinned by `fixtures/checkpoint-log/redis-index.json`.
- **Every writer to one prefix must maintain the index.** A runtime from before
  the index only `RPUSH`es, so its threads don't appear in listings until a
  rebuild. Redis has no trigger to do it for them.
- **Leases** (`design/32-Thread-Leases.md`). Four scripts, identical in both
  engines, take, extend, give up and read a thread's lease
  (`ACQUIRE_LEASE_SCRIPT` / `AcquireLeaseScript`, `RENEW_LEASE_SCRIPT`,
  `RELEASE_LEASE_SCRIPT`, `GET_LEASE_SCRIPT`): acquiring `INCR`s the token
  counter and sets the hash with `PEXPIRE`, unless the hash exists; renewing
  and releasing compare the counter with the lease's token first. An expiry is
  Redis's own `TIME` plus the ttl. A save under a lease is
  `SAVE_UNDER_LEASE_SCRIPT` / `SaveUnderLeaseScript`: the conditional save
  behind a first line that compares the token counter with the lease's token
  and returns `-1`, having written nothing, when they differ. Its `KEYS` are
  the save script's with the token counter last (so its state-set loop stops
  one short), and it takes the expected sequence (empty for none) and the token
  as `ARGV[6]` and `ARGV[7]`. `SAVE_SCRIPT`, `SAVE_IF_LATEST_SCRIPT` and
  `INDEX_SCRIPT` are unchanged, so a writer from before leases keeps working,
  without them.
- **Expiry of finished threads** (`design/34-Storage-Hardening.md`). The three
  save scripts take one more argument, last: a ttl in milliseconds (`ARGV[6]`,
  `ARGV[7]` after the expected sequence, `ARGV[8]` after the lease token; 0 or
  absent for none). A save that leaves the thread `completed` or `ended`
  (state positions 5 and 6) `PEXPIRE`s its list by it, and under a lease its
  token counter too; a save that leaves it in any other state `PERSIST`s them,
  so a thread id that runs again doesn't expire under the run. A store sends 0
  unless it was given a `FinishedThreadTtl` / `with_finished_thread_ttl`. A
  writer from before this argument neither sets nor clears an expiry, so every
  writer to a prefix with a ttl must be current.
- **A thread whose list is gone leaves the index** when a listing next comes
  across it, or with a rebuild: `INDEX_SCRIPT`, finding no list, removes the
  thread from every set it was given. `PURGE_SCRIPT` does the same.
- **Filtered listings are bounded per call.** Metadata filters are applied to
  each candidate's latest checkpoint, which a listing reads anyway; one call
  examines at most 10 × the page size, so a filtered page can come back short
  with a cursor. A graph filter (`design/31-Graph-Identity.md`) is applied the
  same way, to each candidate's outermost `graph`: there is no index set per
  graph, so the save and index scripts are unchanged by identities.

- A record's `sequence` is its **position in the list**, so it is not stored
  (`history-record.schema.json` requires `sequence`; the Redis element omits
  it). `load` is `LINDEX key -1`; `get_history` is `LRANGE key -limit -1`
  (with `limit == 0` special-cased — `LRANGE key -0 -1` is the whole list).
- The list is the log: resuming a thread appends a second element, exactly as
  it appends a second row in SQL.
- Every write is one atomic command or script, so concurrent writers from
  either runtime are safe with no lock.
- **Nothing expires unless a store is told to let finished threads go** (see
  "Expiry of finished threads"). Otherwise a list lives until it is purged or
  you `DEL` it; whether it survives a Redis restart is Redis's RDB/AOF
  configuration. History lists never expire.
- **Redis Cluster is not supported**: one save's keys hash to different slots.
- **Readable by the `strata` CLI** with `--storage redis://…` (another prefix:
  `?prefix=<name>`).

## Running the backend tests

The Postgres and Redis tests need live servers. `docker compose up -d --wait`
(the repo-root `docker-compose.yml`) starts throwaway ones, and the tests read
`STRATA_TEST_POSTGRES_URL` (`postgres://strata:strata@localhost:5432/strata`)
and `STRATA_TEST_REDIS_URL` (`redis://localhost:6379`) — the same variables in
both runtimes. With a variable unset those tests are skipped (Rust prints
`SKIPPED` and returns; xUnit reports them skipped), so a machine without the
services still gets a green run. **CI sets `STRATA_REQUIRE_BACKENDS=1`**, which
turns an unset variable into a failure so a misconfigured job can't quietly
stop testing the adapters; both workflows start Postgres and Redis service
containers. Each test creates its own Postgres schema or Redis key prefix and
removes it afterwards, so tests are independent and re-runnable.

## Records

| Schema | Corresponds to | Rust types | C# types |
|---|---|---|---|
| `checkpoint.schema.json` | Freeze/Flush/Evict persisted state (STRATA-SPEC-002) | `rust/strata-core/src/schema.rs` | `dotnet/src/Datalium.Strata.Storage/Schema/Checkpoint.cs` |
| `graph-definition.schema.json` | Compiled graph topology (STRATA-SPEC-004) | `rust/strata-core/src/schema.rs` | `dotnet/src/Datalium.Strata.Storage/Schema/GraphDefinition.cs` |
| `history-record.schema.json` | Persisted `ChatMessage` (design/03-Architecture.md) | `rust/strata-memory/src/schema.rs` | `dotnet/src/Datalium.Strata.Storage/Schema/HistoryRecord.cs` |

All three C# types live in `Datalium.Strata.Storage` per the persistence
layer split in `design/06-Project-Structure.md`, regardless of which crate
owns the equivalent Rust type — the Rust side splits `checkpoint` /
`graph-definition` into `strata-core` (graph engine concerns) and
`history-record` into `strata-memory` (persistence concerns), matching
`STRATA-SPEC-004` and `design/03-Architecture.md` respectively.

`frozen_state` in a checkpoint is intentionally untyped (`object` /
`serde_json::Value` / `JsonElement`) — it's the user-defined graph state
`S`, which doesn't exist as a concrete type until a specific graph is
compiled (Phase 2).

## Versioning

Every record carries a `schema_version` field (currently `"1.0.0"`,
`const`-pinned in each schema). A breaking change to a record's shape bumps
this value, adds a new fixture, and both language test suites must handle
the version bump explicitly rather than silently accepting old and new
shapes interchangeably.

`graph-definition` has seven versions, by the same rule, each a superset of
the one before: `"1.6.0"` for a level with an interrupt boundary, a node with
an `interrupt` (`design/48-Boundaries-In-Definitions.md`), else `"1.5.0"` for
a level with an `identity`
(`design/31-Graph-Identity.md`), else `"1.4.0"` for a level with a `wait` node
(`design/29-Event-Waits-And-Timers.md`), else `"1.3.0"` for a level with a
`map` node (`design/27-Map-Fan-Out.md`), else `"1.2.0"` for a level with a group `join`
(`design/26-Parallel-Group-Controls-And-Joins.md`), else `"1.1.0"` for a level
with a node that has a `kind` (a parallel group or subgraph,
`design/22-Graph-Structure-In-Definitions.md`), else `"1.0.0"`, written byte
for byte as before (no `kind`, `branches` or `graph` key). A nested `graph`
carries its own version. The schema enforces the pairings (a `map` requires
`"1.3.0"`, a `join` `"1.2.0"` or later and a group or map, `kind` `"1.1.0"` or
later, an `identity` `"1.5.0"` or later, an `interrupt` `"1.6.0"` and no wait
node; a `parallel_group` has `branches` and no `graph`, a `subgraph` and a
`map` the reverse, a plain node neither). At a `"1.6.0"` level the readers
also check that an `on_reject` edge comes from a node with `interrupt` and an
`on_timeout` edge from a node with a timeout, which the schema can't express. `strata graph validate` / `view` and .NET's
`GraphDefinition.ToTopology()` refuse a version they don't know, and a
mismatched `kind`, at any level, as not a graph definition (CLI exit `2`); an
older CLI has no such check and validates only the top level of a `1.1.0`
file.

`checkpoint` is the record with the most versions, each written only when
it is needed, so everything that existed before a version is byte-for-byte
unchanged:

- `"1.9.0"` only for a row as a store with offload stores it
  (`design/41-Checkpoint-Efficiency.md`; see "Offloaded values"): `"1.8.0"`'s
  envelope, a delta or not, of any status, at least one level of which names
  a value in `blobs`. Never a checkpoint the engine writes or sees.
- Otherwise `"1.8.0"` only for a row as a store with deltas stores it
  (`design/41-Checkpoint-Efficiency.md`; see "Delta checkpoints"): a running
  checkpoint at least one level of which is a delta, every level's
  `frozen_state` an envelope. It is never a checkpoint the engine writes or
  sees: every store decodes it to the version its envelope's
  `checkpoint_version` names.
- Otherwise `"1.7.0"` for a checkpoint whose `status` is `"running"`, which every level
  of it then is (`design/33-Durable-Execution.md`): a run of a graph that
  checkpoints every node is going on, or its process died. A superset of
  everything below: a running level may carry a `graph` and a `decision`, and
  nest. It never has a `wait` or a `resumed_at`, and always a `next_node`.
- Otherwise `"1.6.0"` for a checkpoint a level of which carries a `graph`
  (`design/31-Graph-Identity.md`): the identity of the graph that wrote it. A
  superset of everything below: an identified checkpoint may be ended, wait,
  carry decisions, be cancelled or nest.
- Otherwise `"1.5.0"` for a checkpoint a level of which carries a `wait`
  (`design/29-Event-Waits-And-Timers.md`).
- Otherwise `"1.4.0"` for a checkpoint whose `status` is `"ended"`, which every level of
  it then is (`design/28-Ending-A-Thread.md`). A superset of `"1.3.0"`: an
  ended level may carry a `decision`.
- Otherwise `"1.3.0"` for a checkpoint that carries a `decision` at any depth
  (`design/21-Typed-Human-Signals.md`).
- Otherwise `"1.2.0"` for a checkpoint whose `status` is `"cancelled"`, or that nests a
  cancelled one at any depth (`design/17-Async-Execution.md`).
- Otherwise `"1.1.0"` for a checkpoint that carries a `nested` child
  (`design/16-Nested-Interrupts.md`).
- Otherwise `"1.0.0"`, with no `nested` or `decision` key.

The schema enforces the pairings (`nested` requires `"1.1.0"` or later;
`"cancelled"` requires `"1.2.0"`, `"1.3.0"` or `"1.6.0"`; `decision` requires
`"1.3.0"` or later; `"ended"` requires `"1.4.0"` or `"1.6.0"` and a null
`next_node`; `wait` requires `"1.5.0"` or `"1.6.0"`; `graph` requires
`"1.6.0"`, `"1.7.0"` or `"1.8.0"`; `"running"` requires `"1.7.0"` or
`"1.8.0"`, a `next_node`, a null `resumed_at` and no `wait`; `"1.8.0"`
requires `"running"` and an envelope as `frozen_state`; `"1.9.0"` an envelope
as `frozen_state`, whatever the status, and is allowed wherever a level's
content needs a version below it). A `"completed"` checkpoint, which a graph that
checkpoints every node writes when it finishes, takes the version its content
needs (`"1.0.0"` for a plain one) and has a null `next_node`. Both runtimes refuse to
`resume` a checkpoint whose `schema_version` they don't know, rather than
resuming at the parent and silently skipping the rest of the child, or treating
a status they don't know as something else. A runtime built before `1.1.0`
doesn't have that check, so don't resume a nested thread with one; one built
before `1.2.0` refuses a cancelled thread, and one built before `1.3.0` a
thread carrying a decision (it would drop the decision and could resume a
rejection down the approval path), and one built before `1.4.0` an ended
thread (it can't parse the status), which is the intended outcome. One built
before `1.6.0` refuses an identified thread: it couldn't tell another graph's
thread from its own. One built before `1.7.0` refuses a running thread: it
can't parse the status. One built before `1.8.0` refuses a row a store with
deltas wrote: it can't read the envelope, and its stores don't decode it. One
built before `1.9.0` refuses a row with offloaded values the same way, and a
database a current store has opened by its schema version.

## Compatibility

What a version of Strata may change in these formats, and what it may not
(`design/36-Packaging-And-Versioning.md`, "Compatibility"). The packages, the
crates and the CLI share one version; `CHANGELOG.md` lists, for each, the
highest versions it writes and reads.

1. **A record's `schema_version` changes only when its content needs it.** A
   writer writes the lowest version that can say what it has to say. A graph
   that uses nothing new writes what it always wrote, byte for byte.
2. **Every schema version is a superset of the one before.** A runtime that
   reads version N reads every version below it.
3. **A runtime refuses what it doesn't know.** A checkpoint, a definition or a
   database newer than the build is an error, never a guess.
4. **A version of the packages reads everything every earlier version wrote.**
   Dropping support for a schema version is a major version of the packages.
5. **Databases move forward only**, one recorded step at a time (see "Schema
   versions"). A store migrates an older database when it opens it, or refuses
   it if told not to migrate.
6. **Redis has no migrations.** Every writer to a prefix must be current: the
   scripts that keep the index are part of the writer.

So the order to upgrade in is: **every reader before any writer starts using
something new.** Deploy the new version everywhere a thread can be read (the
workers, the API, the `strata` CLI), and only then turn on what writes a new
schema version: a graph identity, a wait, `EveryNode`, deltas, offload.

## Adding or changing a record

1. Edit the `.schema.json` file first — it's the source of truth.
2. Update (or add) the matching `fixtures/*.sample.json`.
3. Update both the Rust and C# types together, in the same change.
4. Both round-trip test suites (`cargo test` in `rust/strata-core` /
   `rust/strata-memory`, `dotnet test` in `Datalium.Strata.Tests`) must pass
   before merging — that's what actually proves the interop constraint
   holds, not just that the schema doc looks right.
