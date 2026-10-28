# Datalium Strata: Interrupt Boundaries Inside a Subgraph

`15-Subgraph-Composition.md` shipped subgraphs with one deliberate hole: a child graph that froze at an interrupt boundary was rejected when the parent was compiled. That ruled out the most natural multi-agent shape, an agent (a subgraph) that stops for a human halfway through its own work. This document specifies **nested interrupts**, which close it. It is a status-first record: what exists, how it behaves, and what was left out.

---

## The model

A child graph can have `add_interrupt_boundary` nodes like any other graph. When one is hit while the child runs as a subgraph node, the **whole run** freezes:

- The parent's checkpoint is suspended **at the subgraph node** (`suspended_node`, and also `next_node`, is that node's name: a resume re-enters it).
- The parent's `frozen_state` is the state the subgraph node **started with**, not the child's, because the node's `output` mapper needs it once the child finishes.
- The child's own checkpoint travels inside the parent's, in a new `nested` field. It has the shape of any checkpoint, its `thread_id` is the **name of the subgraph node**, and it may itself nest a grandchild's.

```jsonc
{
  "schema_version": "1.1.0",
  "thread_id": "thread_nested_demo",
  "status": "interrupted",
  "suspended_node": "Work",          // the subgraph node
  "next_node": "Work",               // a resume re-enters it
  "frozen_state": { "amount": 5, "log": ["prepared"], "approved_by": null },   // the state Work started with
  "nested": {
    "schema_version": "1.0.0",
    "thread_id": "Work",             // the subgraph node's name
    "status": "interrupted",
    "suspended_node": "Approve",     // where the child is actually waiting
    "next_node": "Send",
    "frozen_state": { "n": 10, "trail": ["draft", "approve"], "approved_by": null },
    ...
  },
  ...
}
```

`resume` on such a checkpoint re-enters the subgraph node, hands it the child's checkpoint, and the child continues from its own `next_node`. `input` is **not** called again (the child's state comes from the checkpoint), and the nodes before the boundary do not re-run. When the child completes, `output` runs against the parent's saved entry state and the parent carries on. Everything is the ordinary machinery: the shared fixture `schema/fixtures/checkpoint-lifecycle/nested.json` is frozen and resumed by both engines.

`add_subgraph` / `AddSubgraph` takes the same arguments; a child with boundaries simply compiles now. `run` / `resume` return `Interrupted { thread_id, suspended_node, next_node, frozen_state, waiting_at }`, and for a nested freeze `suspended_node` is the **subgraph node** (`"Work"`), not the inner one. Where the child is actually waiting is `waiting_at` (below), and in the checkpoint's `nested` part, which the CLI shows.

## Semantics

| Aspect | Behaviour |
|---|---|
| **Human signal** | `resume`'s `field_overrides` apply to the **innermost** frozen state, the one a human reviewed. The parent's own state is left alone, even for a key only the parent has. The signal travels down through every level. To send different overrides to outer levels in the same resume, see "A different signal per level". |
| **Several boundaries** | A child with a second boundary just freezes again on resume: a fresh parent checkpoint (not stamped `resumed_at`) with a fresh nested one. Any number of stops, at any depth. |
| **Depth** | Unlimited. A grandchild's freeze nests two levels; `Work` -> `Inner` -> `Review` gives `nested.nested`, with thread ids `Work` and `Inner`. |
| **Parent boundary on the subgraph node** | Works, and applies once the child has **completed**: first stop inside the child, then (after resume) an ordinary 1.0.0 checkpoint with `next_node` after the node and nothing nested. |
| **Recovery policy** | A child freezing is **not a failure**, so the node's policy is never consulted: nothing is retried and no fallback runs. |
| **Retry after resume** | If the resumed child then fails transiently and the node has a retry policy, each attempt resumes from the **same nested checkpoint**, not from the child's start, so the human is not asked twice and the work before the boundary is not redone. |
| **Failure after resume** | A child that fails for good after a resume persists a `Failed` checkpoint at the subgraph node that **carries the child's own `Failed` checkpoint** in `nested`, so the human's earlier answers survive and the thread can be resumed (see "Resuming a failed thread"). A child that fails without leaving a checkpoint of its own (a routing gap, a missing node) is recorded without `nested`; resuming that one re-runs the child from its start. |
| **Resume bookkeeping** | Only the outermost checkpoint is loaded, stamped `resumed_at` and saved by `resume`; the nested ones are not threads of their own. The append-only log gets a second row, exactly like an ordinary resume, with `nested` intact. |
| **Streaming** | Under `run_streaming` / `RunStreaming` a child freeze emits the subgraph node's `NodeStart` and no `NodeEnd` (the node did not complete), like a failed node. A child may stream (`15-Subgraph-Composition.md`): it has streamed up to its boundary when it freezes. A plain `resume` is not streamed and completes normally unless it reaches a streaming node after the boundary, which fails there; `resume_streaming` streams it (below). |

## Wire format

`schema/checkpoint.schema.json` gains one optional property and a second version:

- **`nested`**: a checkpoint (`$ref: "#"`), **absent** (not `null`) when there is no child. A checkpoint that predates nesting serializes byte-for-byte as before. On a `failed` checkpoint it is the child's own `failed` one (see "Resuming a failed thread").
- **`schema_version`** may be `"1.0.0"` or `"1.1.0"`. `1.1.0` is written **only** for a checkpoint that carries `nested`; the schema requires it, together with `status: "interrupted"` or `"failed"`. Every other checkpoint is still `1.0.0`, so nothing that existed changes, and every existing fixture is untouched.
- Both engines now **refuse to resume a checkpoint whose `schema_version` they don't recognise** (`unsupported schema_version '2.0.0'`), so a runtime that predates a version can't silently resume the parent and skip the rest of the child. (A runtime built *before* this change has no such check; don't resume a nested thread with one. `schema/README.md` says so.)

Storage, one new nullable column in each SQL layout, and nothing for Redis (its elements are the JSON):

| Store | Column | Notes |
|---|---|---|
| SQLite (`strata-memory`, `EfCoreCheckpointStore`, the CLI) | `nested TEXT` | JSON text of the child's checkpoint, or `NULL` |
| Postgres (both runtimes) | `nested JSONB` | same, as `JSONB` |
| Redis (both runtimes) | none | the element is the whole nested JSON |
| In-memory | none | holds the record |

It is **one column, not a row per level**, because a nested checkpoint is not a thread of its own. The DDL is a literal string in each place it already was, and the layout fixtures (`columns.json`, `postgres-columns.json`, `frozen.row.json`) gained the column, with a new `nested.row.json` for a nested row. A SQLite or Postgres database created before this has no `nested` column. It is upgraded when a store opens it (see "Upgrading an old log"); it did fail on the next `save` between the nested-interrupt change and that one, and the 2026-09-20 layout change (`schema/README.md`) is still not migrated.

## The CLI

`strata` reads the new column. The wire fields are shown as stored; the CLI adds a rendering of where the thread is waiting, one node per level from the outside in:

- `thread list`: **Current Node** is `Work/Approve` for a nested thread (the JSON `suspended_node` stays `Work`).
- `thread inspect`: a `Waiting at:` line, and `--json` gains a `nested` object (the child's checkpoint).
- `checkpoint log`: `node=Work/Approve`.
- `checkpoint replay`: `Waiting inside: Work/Approve`, and `--json` gains `nested`.

A thread that **failed** inside a subgraph says so: `Failed at:  Work/Send` and `Failed inside: Work/Send`, since nobody is waiting for it. A thread with no child is displayed exactly as before (`nested` is `null` in the JSON). The CLI opens a log through the same upgrade the runtimes use ("Upgrading an old log").

## Implementation notes

- Rust: `Checkpoint.nested: Option<Box<Checkpoint>>` (`#[serde(default, skip_serializing_if = "Option::is_none")]`) in `strata-core/src/schema.rs`, with `NESTED_CHECKPOINT_SCHEMA_VERSION` and `is_supported_checkpoint_version`. C#: `Checkpoint.Nested`, `CheckpointSchema`, and `ToWireJson` / `FromWireJson` on the core `Checkpoint` (the one place the JSON shape is written, now shared by the Redis store; EF Core and Postgres use it for the `nested` column).
- The stored fallible-node handler now returns a **`Step`** (`Done(state)` or `Interrupted(child checkpoint)`) and takes, besides the state and stream context, an optional **`NestedResume`** (the child's checkpoint plus the human signal). `add_fallible_node` and parallel groups wrap theirs to `Done`; only a subgraph node ever returns `Interrupted`. `run_fallible` returns on `Interrupted` without consulting the policy, and the executor's `freeze_around_child` / `FreezeAroundChild` writes the parent checkpoint. The subgraph node is tracked by name (`subgraph_nodes`) so a checkpoint nesting a child under any other node is refused (`... which is not a subgraph node`).
- A failing subgraph hands its child's `Failed` checkpoint up next to the error: Rust's stored fallible-node handler now fails with an `AttemptError { error: NodeError, child }` (`add_fallible_node` and parallel groups convert a plain `NodeError`), C# sets an `internal` `FailedChild` on the `NodeException`; `run_fallible` / `RunFallible` nests it in the `Failed` checkpoint it writes. `NestedResume` carries `LevelSignals` (which `descend`s by node name) instead of one signal.
- `resume` was split: `resume` loads and calls `resume_checkpoint` / `ResumeCheckpoint`, which the subgraph node also calls for its child with the checkpoint it was given. The subgraph runner runs the child against a capturing store; on `Interrupted` it hands the captured checkpoint up.
- `subgraph_blocker` / `SubgraphBlocker` no longer looked at boundaries, only at streaming; it has since been removed altogether (`15-Subgraph-Composition.md`, streaming inside a subgraph).
- Tests: `rust/strata-core/tests/nested_interrupts.rs` and `dotnet/tests/Datalium.Strata.Tests/NestedInterruptTests.cs` mirror each other (19 each), including the shared-fixture freeze and resume. The store contracts (`checkpoint_store_contract.rs`, `StoreContracts.cs`) round-trip a two-level nested checkpoint through every backend; each SQL store, Postgres and Redis compares its raw row or element to `nested.row.json` / `nested.json`; the CLI test seeds the fixture and drives every command. Mutation-checked in both engines: a subgraph node that ignores the resume payload fails ten tests, and writing a nested freeze as `1.0.0` fails three. The two old "a child with a boundary is rejected" tests in `subgraphs.rs` / `SubgraphTests.cs` were replaced by the streaming equivalents, which went in turn when streaming inside a subgraph was built.

## Streaming a resume

`resume_streaming(thread_id, signal, store)` (Rust; like `run_streaming` it takes `self: Arc<Self>` and an `Arc<dyn CheckpointStore>`) / `ResumeStreaming(threadId, signal, store)` (C#) resumes exactly as `resume` does and streams the resumed run. It returns what `run_streaming` returns: a join handle / task for the result, and a receiver / reader for the chunks. Added 2026-09-21; until then a streaming node reached after an interrupt boundary could not run at all, in a flat graph or a subgraph.

| Aspect | Behaviour |
|---|---|
| **Starts at** | The node the run resumes into. Nodes that ran before the freeze emit nothing (including the boundary node itself). |
| **Nested** | The parent's subgraph node emits a **new** `NodeStart` (the one from the run that froze never got a `NodeEnd`), then the child's chunks follow, named `<subgraph>/<child node>` to any depth. The child re-enters at the node after its own boundary, so the boundary node is not repeated. |
| **Human signal** | Applied exactly as in `resume`: field overrides reach the innermost state before any node runs, so a streaming node sees them. |
| **Can't resume** | No checkpoint for the thread, already resumed, unsupported version, a completed one, a signal for a level it doesn't have, or a state that won't rehydrate: the stream ends with **no chunks** and the result is the same `Failed` that `resume` would return. Nothing hangs. |
| **Freezes again** | A resumed run that reaches another boundary (its own, or one deeper in a child) returns `Interrupted` and can be resumed again, with `resume` or `resume_streaming`. As in a fresh run, the subgraph node has no `NodeEnd` when its child froze. |
| **Retry** | Unchanged from a plain resume: each attempt re-enters the child at the same nested checkpoint, so the human is not asked twice, and the engine emits a `Retry` chunk on the subgraph node before each re-run. |
| **Plain `resume`** | Unchanged, and still not streamed. A streaming node it reaches fails there (a flat graph: `ExecutionAborted` at that node; a subgraph: a fatal `NodeFailed` on the subgraph node), and the plain-run refusal for streaming subgraphs is skipped for it. |
| **Backpressure** | The same single bounded channel (1024 chunks); a full channel pauses the node, nothing is dropped. |

Implementation: `resume` was split again. It and `resume_streaming` both call a private `resume_with_stream` / `ResumeFrom` that loads the checkpoint and passes an optional stream context to `resume_checkpoint` / `ResumeCheckpoint`; the executor and the subgraph runner already carried a stream context, so no new engine path was needed. `resume_streaming` runs it on a background task, as `run_streaming` does (originally a plain `std::thread` in Rust; a `tokio::spawn`ed task since `17-Async-Execution.md`, which returns a tokio `JoinHandle`, and `Task.Run` in C#), and both sides close the channel however the run ends, so a failed resume still ends the reader. Tests: `rust/strata-core/tests/resume_streaming.rs` and `dotnet/tests/Datalium.Strata.Tests/ResumeStreamingTests.cs` (11 each: flat and nested, overrides, no checkpoint, resumed twice, a second boundary, a retry after resume, three levels, backpressure). Mutation-checked in both engines: dropping the stream in `resume_checkpoint` fails ten, and dropping it only in the subgraph's child resume fails five (the nested ones).

## Where a thread is waiting

`Interrupted` carries `waiting_at` (Rust `Vec<String>`, C# `IReadOnlyList<string>`): one node name per level from the outermost graph in, the last being where the thread actually stopped. `["Approve"]` for a boundary in the graph you ran, `["Work", "Approve"]` for one at `Approve` inside the subgraph node `Work` (`suspended_node` stays `"Work"`), `["Work", "Inner", "Approve"]` three levels down. It is the same path as `Checkpoint::waiting_at()` / `Checkpoint.WaitingAt()` on the stored checkpoint, and the one the CLI joins with `/`.

It is a new field on an existing variant, so a Rust pattern that names every field of `Interrupted` (`Interrupted { thread_id, suspended_node, next_node, frozen_state }`) no longer compiles: add `..` or bind `waiting_at`. In C# it is a fifth positional parameter of the `Interrupted` record, so construction and positional deconstruction change and property access does not. Both engines are pre-1.0.

## A different signal per level

`resume` takes a `HumanSignal`, which goes to the innermost graph. To answer more than one level in the same resume, pass `LevelSignals` (Rust `LevelSignals::new(inner).at(&["Work"], outer)`; C# `new LevelSignals(inner).At(["Work"], outer)`). `resume` / `ResumeAsync` / `resume_streaming` / `ResumeStreaming` all accept either, so a bare `HumanSignal` keeps working (C# has one overload for each).

| Aspect | Behaviour |
|---|---|
| **Addressing** | A level is named by the subgraph nodes leading to it from the graph you call `resume` on: `[]` is that graph itself, `["Work"]` the child running inside its `Work` node, `["Work", "Inner"]` the grandchild. |
| **The innermost level** | Takes the signal passed to the constructor. Addressing it again with `at` / `At` is refused. On a flat thread the only level is `[]`, which is the innermost, so `at(&[], ..)` is refused there too. |
| **An outer level** | Its `field_overrides` apply to the state that level's subgraph node **started with** (the parent's `frozen_state`), before the node re-enters its child. The child's `input` is not called again, so an outer override can only reach the `output` mapper that folds the child's result back into the parent, and the nodes after the subgraph node. |
| **Only overrides matter** | An outer level isn't waiting at a boundary of its own, so its signal's `is_approved` and `reviewer_token` are not used there: it records no decision and takes no reject route. The innermost signal does both when the innermost level is waiting at its own boundary (`21-Typed-Human-Signals.md`). |
| **A level that isn't there** | The resume is refused (`Failed`) before anything is stamped or run, naming the levels the checkpoint has (`... levels are [], [Work]`), so the thread can still be resumed correctly. |

## Resuming a failed thread

`resume` now accepts a `Failed` checkpoint as well as an `Interrupted` one. It re-enters the node that failed (`next_node`, the node a replay would re-run) with the last committed state (`frozen_state`), applies the signal's `field_overrides` to it, and carries on. Like an interrupted one it is resumable **once**: the checkpoint is stamped `resumed_at`, a second resume is refused, and if the resume fails again a new `Failed` checkpoint replaces it, which can be resumed in turn. A `Completed` checkpoint is still refused (`is not resumable`). This reverses the "not built" of `04-Error-Recovery.md`: `resume` used to refuse anything not `interrupted`. A `Cancelled` checkpoint (`17-Async-Execution.md`) is resumed exactly the same way, and nests a cancelled child the way a failed one nests a failed child.

**Only the signal's overrides count here.** A `failed` or `cancelled` level isn't waiting at a boundary, so its signal records no decision and takes no reject route (`21-Typed-Human-Signals.md`); the level keeps the decision it had. The signal's `is_approved` and `reviewer_token` mean nothing, and by convention it is `HumanSignal.Approve(who)` / `HumanSignal::approve(who)` with the overrides. A neutral factory for this case was considered (2026-10-24, from the .NET demo 08) and left out: the type would gain a member whose only job is to say "this field is ignored", and the docs on `HumanSignal` now say so.

For a subgraph node the failure carries the child. When the child fails at one of its own nodes, the parent's `Failed` checkpoint nests **the child's `Failed` checkpoint** (`schema_version` `1.1.0`, `status: "failed"`; it nests a grandchild's the same way, to any depth):

```jsonc
{
  "schema_version": "1.1.0",
  "thread_id": "thread_nested_failed_demo",
  "status": "failed",
  "suspended_node": "Work",          // the subgraph node
  "next_node": "Work",               // a resume re-enters it
  "frozen_state": { "amount": 5, "log": ["prepared"], "approved_by": null },   // the state Work started with
  "metadata": { "error": "subgraph 'Work' failed at 'Send' after 1 attempt(s): mailbox closed", "error_kind": "fatal", "attempts": "1" },
  "nested": {
    "schema_version": "1.0.0",
    "thread_id": "Work",
    "status": "failed",
    "suspended_node": "Send",        // where the child actually failed
    "next_node": "Send",
    "frozen_state": { "n": 10, "trail": ["draft", "approve"], "approved_by": "admin_01" },   // what the human answered is kept
    ...
  }
}
```

Resuming it re-enters the child at `Send` with that state: `input` is not called again, and the nodes before the failure (including the boundary the human already answered) do not run again, so **nobody is asked twice**. A signal on that resume applies to the child's committed state, so the operator can also correct a field before retrying. The parent's recovery policy still applies to the resumed run: each retry re-enters the child from the same nested checkpoint.

A child that fails without leaving a checkpoint of its own (a routing gap: no edge condition matched; a missing node) is recorded without `nested`, and resuming that re-runs the child from its start. Nothing about an ordinary flat failure changes except that it is now resumable: `next_node` was already the failed node.

The shared fixture `schema/fixtures/checkpoint-lifecycle/nested-failed.json` is produced by a live failure in both engines (each compares its checkpoint to it at every level) and resumed by both.

## Upgrading an old log

`SqliteCheckpointStore`, `EfCoreCheckpointStore`, `PostgresCheckpointStore` (both runtimes) and the CLI's `store::open` add the `nested` column when they open a log created before nested checkpoints: SQLite checks `pragma_table_info('checkpoints')` and runs `ALTER TABLE checkpoints ADD COLUMN nested TEXT`; Postgres runs `ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS nested JSONB` in the same advisory-locked transaction as the `CREATE TABLE`. Every existing row keeps `NULL`, which is right (none has a child), and opening a current log changes nothing. If two processes race on an old SQLite file the loser's `ALTER` fails, which is fine so long as the column is there afterwards.

Only this one column is migrated. The layout from before the shared append-only log (2026-09-20, `schema/README.md`) is still not readable and not upgraded, and there is no general migration framework: a future layout change gets its own step. Tests: an old-layout file seeded by hand is opened by each store (and the CLI), its row still loads, a nested checkpoint then saves, and the table matches the shared column fixture. The CLI sets its schema up on one connection before creating its pool: a pooled connection that had read the old schema kept describing `SELECT *` with the old columns and returned rows one column short, which sqlx reported as an empty list.

## Not built

- ~~Streaming inside a subgraph.~~ Built since: see `15-Subgraph-Composition.md`.
- ~~A structured "waiting at" value~~, ~~a different signal per level~~, ~~resuming a failed nested thread~~ and ~~migration~~. Built since: see above.
- **Resuming a thread from the CLI, nested or not.** This was listed as a nested-specific gap and is not one: no `strata` command can run a graph, because a generic binary has no way to load your graph's closures. `thread`/`checkpoint` are read-only by design, and that is a decision, not a to-do. Something that resumes from the command line has to be your own agent binary (it has the graph); a `strata thread resume --host <your-binary>` that launches it would be a new feature with a protocol of its own.
- ~~**A per-level `is_approved`.**~~ Built since, as recorded decisions and the reject route: see `21-Typed-Human-Signals.md`. The innermost level's answer is recorded and acted on; an outer level's signal still only overrides.
- **Migration beyond the `nested` column.** See "Upgrading an old log".
