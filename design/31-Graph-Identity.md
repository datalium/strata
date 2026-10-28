# Datalium Strata: Graph Identity in Checkpoints

A store holds many graphs' threads, and a checkpoint doesn't say which graph wrote it. Every operation that loads a thread through a compiled graph (a resume, a signal, a wake, an end) trusts the caller to have picked the right graph. When the caller picks the wrong one, the engine notices only by accident: a node name that doesn't exist, a state that doesn't deserialize. When the wrong graph has the same node names and a compatible state (version 2 of the same graph, say), nothing notices at all, and the thread runs on under a graph that never wrote it.

StrataOps works around this today. Its `execution` table records each thread's graph id and version, and its `GraphRunner` refuses a resume under another one. design/29's wake driver and design/30's decision worker each need the caller to filter (a `graph` metadata pair, an `Owns` predicate), and design/29 says plainly that its own check "catches a missing filter, not a graph with the same node names".

This document closes the gap in the engine: **SG-13**. The SG numbers refer to the gap log of StrataOps (not public yet). It is the first of the four durability gaps StrataOps' M5 depends on (SG-13, SG-12, SG-10, SG-30), and the smallest; SG-10's recovery driver lists running threads by graph through what this document adds.

SG-13 lands in both engines at one commit (strict parity, StrataOps ADR 0003): the same capability, in each language's idiom.

It was written as a blueprint before implementation and is now built. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Where identity lives | **On the compiled graph**, from **`CompilationOptions.GraphId` / `GraphVersion`** (Rust: `CompileOptions::with_graph_id` / `with_graph_version`). Optional: an unnamed graph works exactly as before. `CompiledGraph.Identity` / `identity()` reads it back. |
| The shape | **`GraphIdentity(Id, Version)`**: a non-blank id and an optional version, both free text compared exactly. No ordering of versions: "2" and "10" are just different. |
| On the checkpoint | A **`graph` object** (`id`, `version`) on **every level written by a graph that has an identity**: the outermost level carries the outer graph's, a nested level its own subgraph's. **`schema_version` `1.6.0`**, a superset of `1.5.0`, when any level carries one; a checkpoint with none is byte for byte as before. |
| Resuming under another graph | **Refused at every level, before anything is written**: a different id always (`thread 't' was written by graph 'x' version 2, not 'y' version 2`), and a graph with no identity resuming a thread that has one. |
| A different version | **Refused by default**, accepted by a **migration**: `CompilationOptions.Migrate`, a `Checkpoint → Checkpoint?` delegate that may rename nodes or reshape state, and returns `null` to refuse. It runs per level, in the resume's preflight, before the conditional stamp; the stamp then records the new identity. Automatic migration is "Not built". |
| Checkpoints without identity | **Unknown, resumable by any graph**, as before: everything written before `1.6.0`, and everything an unnamed graph writes. A graph with an identity resuming one writes its own identity on every checkpoint from then on. |
| Which operations check it | **Every one that loads a thread through a compiled graph**: `ResumeAsync`, `ResumeStreaming`, `SignalAsync`, `WakeDueAsync`, `EndAsync`, and their Rust twins. They share one rule. |
| Listing by graph | **`ThreadQuery.GraphId`** and **`GraphVersion`** (version only with an id), on the outermost level's identity. **SQL**: `graph_id` and `graph_version` columns on `checkpoints` and on the head table, copied by the trigger, indexed. **Redis**: filtered from the candidates like metadata, with no new keys or script change. `ThreadSummary.Graph` carries it. |
| The drivers | **`WakeDueAsync` lists only its own graph's threads** when the graph has an identity (by id, and by version too unless it has a migration). `WakeOptions.MatchGraph = false` lists as before, which also wakes threads written before the graph had one. **`DecisionWorker` reports another graph's thread as `NotMine`**, without running anything. `Owns` stays as an extra filter, no longer the default guard. |
| Definitions | A top-level **`identity`** object (`id`, `version`) in the exported definition, at every level that has one (a subgraph's or a map's child exports its own), at definition **`schema_version` `1.5.0`**. `identity`, not `graph`: a subgraph node's `graph` already holds its child's definition. |
| Run events and telemetry | **`RunStarted.Graph`**, and the run span's **`strata.graph.id`** / **`strata.graph.version`**. No new event. |
| The CLI | **Read-only.** `thread list --graph <id> [--graph-version <v>]`, `graph` in `--json`; `thread inspect` prints `Graph:`; `graph view` prints the definition's identity. |

---

## Usage

```csharp
// StrataOps' versioned graphs:
var investigationV2 = BuildInvestigation().Compile(new CompilationOptions
{
    GraphId = "incident-investigation",
    GraphVersion = "2",
    MaxNodeHops = 40,
});

// A thread v1 wrote can't be resumed by v2, and nothing is written:
var result = await investigationV2.ResumeAsync(threadId, HumanSignal.Approve("alice"), store);
// Failed("thread 't-17' was written by graph 'incident-investigation' version 1, not version 2, and this graph has no migration")

// Which graph wrote a thread, without resuming it:
var latest = await store.LoadLatestAsync(threadId);
var runner = latest?.Checkpoint.Graph?.Version switch { "1" => investigationV1, _ => investigationV2 };

// Or let v2 take v1's threads, renaming a node it no longer has:
var investigation = BuildInvestigation().Compile(new CompilationOptions
{
    GraphId = "incident-investigation",
    GraphVersion = "2",
    Migrate = checkpoint => checkpoint.Graph?.Version == "1"
        ? checkpoint with { NextNode = checkpoint.NextNode == "Rollback" ? "Revert" : checkpoint.NextNode }
        : null,   // any other version is refused
});

// The approvals queue lists only this graph's threads:
var waiting = await query.ListThreadsAsync(new ThreadQuery
{
    GraphId = "incident-investigation",
    States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Interrupted },
});

// The wake driver needs no metadata filter any more:
var report = await investigation.WakeDueAsync(store);
```

```rust
let investigation = build_investigation().compile_with(
    CompileOptions::new()
        .with_graph_id("incident-investigation")
        .with_graph_version("2")
        .with_migration(|mut checkpoint: Checkpoint| {
            if checkpoint.graph.as_ref()?.version.as_deref() != Some("1") {
                return None; // refused
            }
            if checkpoint.next_node.as_deref() == Some("Rollback") {
                checkpoint.next_node = Some("Revert".into());
            }
            Some(checkpoint)
        }),
)?;

let threads = store
    .list_threads(&ThreadQuery::default().with_graph("incident-investigation"))
    .await?;
```

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Interruption;

/// Which graph wrote a checkpoint level: its id and optional version, compared exactly.
public sealed record GraphIdentity(string Id, string? Version);

public sealed record Checkpoint
{
    // … as before …
    public GraphIdentity? Graph { get; init; }   // the graph that wrote this level
}

public static class CheckpointSchema { public const string GraphVersion = "1.6.0"; /* … */ }

public sealed record ThreadQuery
{
    public string? GraphId { get; init; }        // only threads whose outermost graph has this id
    public string? GraphVersion { get; init; }   // … and this version; needs GraphId
}

public sealed record ThreadSummary(/* … */) { public GraphIdentity? Graph { get; init; } }

namespace Datalium.Strata.Graph;

public sealed class CompilationOptions
{
    public string? GraphId { get; init; }
    public string? GraphVersion { get; init; }           // needs GraphId
    public Func<Checkpoint, Checkpoint?>? Migrate { get; init; }   // needs GraphId
}

public sealed partial class CompiledGraph<TState>
{
    public GraphIdentity? Identity { get; }

    /// Whether this graph takes a thread whose latest checkpoint is `checkpoint`: it has no identity,
    /// or this graph's id with this graph's version (or any version, when this graph has a migration).
    public bool Owns(Checkpoint checkpoint);
}

public sealed record WakeOptions { public bool MatchGraph { get; init; } = true; /* … */ }

public sealed record GraphTopology { public GraphIdentity? Identity { get; init; } /* … */ }

// Observability.RunStarted gains `GraphIdentity? Graph { get; init; }`.
```

### Rust

```rust
// schema
pub struct GraphIdentity { pub id: String, pub version: Option<String> }
pub const GRAPH_CHECKPOINT_SCHEMA_VERSION: &str = "1.6.0";
pub const IDENTITY_GRAPH_SCHEMA_VERSION: &str = "1.5.0";
// Checkpoint gains `pub graph: Option<GraphIdentity>`; GraphDefinition gains `pub identity: Option<GraphIdentity>`.

// graph
impl CompileOptions {
    pub fn with_graph_id(self, id: impl Into<String>) -> Self;
    pub fn with_graph_version(self, version: impl Into<String>) -> Self;
    pub fn with_migration(self, migrate: impl Fn(Checkpoint) -> Option<Checkpoint> + Send + Sync + 'static) -> Self;
}
impl<S> CompiledGraph<S> {
    pub fn identity(&self) -> Option<&GraphIdentity>;
    pub fn owns(&self, checkpoint: &Checkpoint) -> bool;
}

// store: ThreadQuery gains `graph_id`, `graph_version` (builders `with_graph`, `with_graph_version`);
// ThreadSummary gains `graph`.
// wait: WakeOptions gains `with_match_graph(bool)`.
// observability: RunEventKind::RunStarted gains `graph: Option<GraphIdentity>`.
```

- The migration is synchronous and must be pure: it may be called while a resume is being checked, and a resume it refuses writes nothing.
- An id or version that is empty or only whitespace is refused at compile time, as are a version or a migration without an id (.NET `ArgumentException`, Rust `CompilationFailed`).

---

## Semantics

### What is written

Every checkpoint a graph with an identity writes carries it on its own level: interrupted (a boundary or a wait), failed (a node failure, the hop limit), cancelled, and the frozen level around a subgraph's child. A subgraph's child writes its own graph's identity, or none, on the nested level, so a checkpoint can say "the outer graph is `incident-investigation` version 2, and the level frozen inside its `Remediate` node is `remediation` version 5".

What is written from a stored checkpoint keeps that checkpoint's identity: the resume stamp re-saves the checkpoint being resumed, and an ended checkpoint (`design/28`) keeps each level's. After a migration both carry the migrated identity.

A map's items run a child graph, but an item's checkpoint is never nested (`design/27`), so a map's child's identity is never written; it is exported in the template's definition.

### The rule

A level of a stored checkpoint is checked against the graph that runs that level (the outer graph for the outermost level, the subgraph node's child for a nested one), top-down:

| Stored `graph` | This graph | Result |
|---|---|---|
| none | anything | **accepted** as it is: unknown, as before `1.6.0` |
| `x`, any version | no identity | **refused**: `… was written by graph 'x' version 2, and this graph has no identity` |
| `x` | `y` | **refused**: `… was written by graph 'x' version 2, not 'y' version 2` |
| `x` version `v` | `x` version `v` | **accepted** |
| `x` version `v` | `x` version `w`, no migration | **refused**: `… was written by graph 'x' version 1, not version 2, and this graph has no migration` |
| `x` version `v` | `x` version `w`, a migration | the migration's checkpoint, **accepted**; or **refused** when it returns null: `… not version 2, and this graph's migration refused it` |

A missing version is written `unversioned` in the messages, and compares equal only to another missing one. For a nested level, the message says which: `thread 't' at level [Remediate] was written by …`.

A migrated level keeps its stored `thread_id`, and gets this graph's identity and the `schema_version` its content needs. Everything else is what the migration returned: it may rename the node it resumes at, rename the nodes recorded in its decision, reshape its state, drop metadata. It should leave `nested` alone: the child's own graph migrates the child's level next.

### Where it is checked

- **A resume** (and so a signal and a wake, which are resumes): after the checks on the stored checkpoint (a known version, not ended or completed, not resumed), and before every other check of the preflight (`design/21`), which then runs on the migrated checkpoint. A refusal writes nothing and leaves the thread resumable by the right graph. When a subgraph node is resumed into, its child checks its own level again; that level already carries the child's identity, so its migration doesn't run twice.
- **An end** checks it before deciding anything, ending the migrated checkpoint. An ended thread of another graph is refused like any other, not returned as the idempotent result.
- **`WakeDueAsync`** checks it when it decides each listed thread again; a refusal skips the thread with the message as the reason.
- **A fresh run** with a thread id another graph used isn't refused: a run never reads the store (`design/28`, "Resuming an ended thread").

### Listing by graph

`ThreadQuery.GraphId` keeps the threads whose latest checkpoint's **outermost** level was written by a graph with that id; with `GraphVersion` too, at that version. A thread with no identity never matches. Both compose with every other filter, in the usual order. A `GraphVersion` without a `GraphId` is refused, like a bad limit.

`ThreadSummary.Graph` is the outermost level's identity, `null` for an unknown thread.

### The drivers

**`WakeDueAsync`** lists due threads with `GraphId` = its identity's id, and `GraphVersion` = its version unless it has a migration (which may accept other versions). Each thread is then decided again with the rule above. A thread of another graph is no longer listed at all, so the metadata filter design/29 recommended is no longer needed for that (it still works). `WakeOptions.MatchGraph = false` lists by `WakeOptions.Metadata` alone, as before this document: that is how a deployment that just gave its graph an identity wakes the timers its unnamed predecessor left.

**`DecisionWorker`** loads the decision's thread before resuming it (the resume then uses what it loaded, so it costs no extra request) and reports a thread its graph doesn't own (`CompiledGraph.Owns`) as **`NotMine`**, having run nothing. Another worker, running the right graph, handles it. `ThreadIdPrefix` and `Owns` still filter first, without a load.

---

## Run events and telemetry

| | |
|---|---|
| `RunStarted` | gains **`Graph`**: the compiled graph's identity, `null` when it has none. For a resume, the graph resuming (which, after a migration, is not the one that wrote the thread). |
| Run span | **`strata.graph.id`** and **`strata.graph.version`** (when set), on every run's span. |

No new event: a refusal is a `Failed` run like any other refused resume. `RunEvent` is non-exhaustive in both engines; .NET's member is an `init` property, Rust's a field, so a pattern on `RunStarted` without `..` needs it.

---

## Wire format

### The checkpoint (`1.6.0`)

```json
"graph": { "id": "incident-investigation", "version": "2" }
```

- **`graph`** is optional on every level, absent (not null) when the level's graph has no identity. Both keys are always written; `version` is `null` for an unversioned graph.
- **`schema_version` `"1.6.0"`** is what a checkpoint with a `graph` at any level is written with: a superset of `1.5.0`, so it may carry a `wait`, be `ended`, be `cancelled`, nest, carry decisions. Without one, every checkpoint keeps the version it had.
- A reader built before `1.6.0` refuses the checkpoint as an unsupported version, which is right: it can't check the identity, and would resume another graph's thread.

### Storage

- **SQL `checkpoints`** gains **`graph_id`** and **`graph_version`** (`TEXT`): the outermost level's identity, like `decision` and `wait`; nested levels carry theirs inside `nested`. A reader builds `graph` from them (`graph_id` `NULL` is none).
- **SQL `checkpoint_threads`** gains the same two columns, copied by the trigger from the row it points at, and an index on `(graph_id, updated_at, thread_id)`. The filter is `h.graph_id = ? [AND h.graph_version = ?]`.
- **Migration:** a store opening an older log adds the two columns to `checkpoints` and to the head table, and replaces the trigger, in the same transaction that creates everything else, as for `wake_at` (`design/29`). Existing rows keep `NULL`: nothing had an identity before.
- **Redis** writes `graph` inside each list element, as every other field, and filters on it from the candidates a listing reads anyway, like metadata (`design/19`, "Filtered listings are bounded per call"). No new key and no script change, so writers from before this document keep maintaining the index correctly.
- **Shared fixtures:**
  - `checkpoint-lifecycle/identified.json`: `frozen.json`'s thread written by `wire-transfer` version `2`. `nested-identified.json`: `nested.json`'s thread with `incident` version `1` outside and `approval` version `3` inside. What a live freeze writes, `created_at` aside, in both engines.
  - `checkpoint-log/identified.row.json`: the SQLite row for `identified.json`.
  - `columns.json`, `postgres-columns.json`, `threads-columns.json`, `postgres-threads-columns.json`: the new columns. Every `*.row.json` gains `graph_id` and `graph_version` (`null`). `threads.rows.json`: an identified thread and its head row.
  - `checkpoint-query/cases.json`: identified threads, and cases filtering by id and by id and version.

### Definitions (`1.5.0`)

```json
{ "schema_version": "1.5.0", "identity": { "id": "incident", "version": "1" },
  "entry_point": "Plan", "nodes": [ … ], "edges": [ … ] }
```

- **`identity`** is optional at every level, absent (not null) when the graph has none; both keys are always written. A level with one is **`1.5.0`**, a superset of `1.4.0`. The builder's export (`StateGraph.ToDefinition()` / `to_definition()`) has none: identity is given at compile time, so only a compiled graph's export carries it, and a subgraph's or map's child is exported as compiled.
- **`check_shape` / `ToTopology`** refuse an `identity` below `1.5.0` and an empty id (exit 2). The validator doesn't read it.
- Fixture: `graph-export/identity.json`, a parent and its subgraph child, each with an identity.

---

## The CLI

Read-only:

- **`strata thread list --graph <id>`** and **`--graph-version <version>`** (only with `--graph`) filter through the query port; `--json` gains a `graph` key.
- **`strata thread inspect`** prints `Graph: incident-investigation version 2` (`unversioned` when it has no version, nothing for an unknown thread) and, for a nested thread, the inner levels' as `Graph at Remediate: remediation version 5`; `--json` gains `graph`.
- **`strata graph view`** prints the definition's identity under its title; **`graph validate`** reads `1.5.0`.

---

## Migration

**.NET** (source-compatible): `GraphIdentity` is new. `Checkpoint.Graph`, `ThreadQuery.GraphId` / `GraphVersion`, `ThreadSummary.Graph`, `CompilationOptions.GraphId` / `GraphVersion` / `Migrate`, `WakeOptions.MatchGraph`, `GraphTopology.Identity`, `GraphDefinition.Identity` and `RunStarted.Graph` are `init` properties, so positional construction and deconstruction are unchanged.

**Rust:** `Checkpoint` gains a public `graph` field and `GraphDefinition` an `identity` field (struct literals need them, or `..`). `RunEventKind::RunStarted` gains `graph`, so a pattern that names every field needs `..`. `ThreadQuery`, `ThreadSummary`, `CompileOptions` and `WakeOptions` are non-exhaustive or builders already.

**Stores:** every SQL store migrates on open (above). Redis needs nothing.

**Strata Cloud:** Cloud must accept `schema_version` `1.6.0` before a graph with an identity can save to it (another catch-up phase in Strata Cloud's API, as Phase 9 was for `1.5.0`): until then such a save is refused (`422`). Cloud stores the checkpoint JSON as it is, so nothing else changes for it; a listing by graph on Cloud would need Cloud to index `graph` (Cloud has no query port yet).

**StrataOps:** its versioned graphs compile with `GraphId` / `GraphVersion` (`incident-investigation` 1 and 2, `rca`, `remediation`). `GraphRunner` picks the graph by `LoadLatestAsync(…).Checkpoint.Graph` instead of its `execution` table, and the engine refuses a mismatch on its own. Published versions never change, so it needs no migration in M5. Its Worker's `WakeDueAsync` drops the `graph` metadata filter, and its decision worker the `Owns` lookup.

---

## Tests

Mirrored in both engines, in new suites `graph_identity.rs` / `GraphIdentityTests.cs` unless noted:

- **Identity on every level:** a flat freeze writes `graph` and `1.6.0` (it matches `identified.json`); a freeze inside a subgraph writes each level's own identity (it matches `nested-identified.json`), and a child without one writes none on its level; a failed, a cancelled and a hop-limited checkpoint carry it; an unnamed graph writes no `graph` and keeps its old version.
- **Refusals, writing nothing:** a resume under another id; under another version without a migration; by a graph with no identity; a nested level of another graph or version (`at level [Work]`); an end and a signal under another graph. Each leaves the thread resumable by its own graph.
- **Migration:** accepted (a renamed node and a reshaped state; the stamp and the next checkpoint carry the new identity); refused by a migration that returns null; a nested level migrated by its child's graph; the migration called once per level.
- **Unknown threads:** a checkpoint without `graph` (the `frozen.json` fixture, `1.0.0`) resumes under a named graph, and what it writes next carries the identity.
- **`Owns`:** unknown, own, another id, another version with and without a migration.
- **Compile:** an empty id, a blank version, a version or a migration without an id are refused.
- **The query in every store** (`checkpoint_store_contract.rs`, `CheckpointQueryContractTests.cs`, the CLI): the shared graph cases in `cases.json`; `ThreadSummary.Graph`; a version without an id refused; the head table's `graph_id` from raw SQL (`threads.rows.json`); migrating a log from before identity in SQLite and Postgres.
- **The drivers:** `WakeDueAsync` doesn't list another graph's due thread, nor an unknown one unless `MatchGraph` is false; with a migration, it wakes an older version's thread. The decision worker reports another graph's thread `NotMine` and runs nothing (`worker.rs`, `DecisionWorkerTests.cs`).
- **Fixtures:** each lifecycle fixture is what a live freeze writes and round-trips through every store; `identified.row.json` is the SQLite row.
- **Definitions:** `identity.json` is what both engines export from compiled graphs and round-trips; the CLI validates and views it; an `identity` below `1.5.0` is refused (exit 2).
- **Run events:** `RunStarted.Graph`; the run span's tags.
- **Mutation checks**, both engines: not checking the id; not checking the version; checking only the outermost level; not copying `graph_id` to the head table.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

### The questions as asked (the handoff brief)

Recommendations are in bold.

1. **Where identity lives.** **`CompilationOptions.GraphId` / `GraphVersion`**, optional. The builder is the graph's shape; identity is a promise about the compiled artifact, given where its other compile-time options (the hop limit) are.
2. **On the checkpoint.** **A `graph` object on every level that has one, at `1.6.0`.** A subgraph is a compiled graph with its own identity, and a migration of the outer graph says nothing about its child's.
3. **Resuming under another graph.** **A different id is always refused; a different version is refused unless a migration accepts it**, and the migration is **a delegate** that can rename nodes and reshape state, run in the preflight before the stamp. An `AcceptsVersion(from)` predicate alone couldn't rename the node a thread waits at. Automatic migration is "Not built".
4. **Filters.** **`ThreadQuery.GraphId` / `GraphVersion`**, indexed in the SQL stores. **The drivers filter by the graph's own identity** when it has one.
5. **Checkpoints before `1.6.0`.** **Unknown, resumable by any graph**, today's behaviour. Refusing them would strand every thread in flight when a deployment adds identities.

### Added by this document

6. **A graph without an identity resuming a thread with one.** **Refused.** An unknown thread is unknown because nobody could tell; a thread with an identity can tell, and an unnamed graph can't show it is that graph. A deployment rolling back to an unnamed build would be refused by its older reader anyway (`1.6.0` is unsupported there).
7. **Free-text versions, compared exactly.** **Yes.** Versions mean whatever the application publishes ("2", "2026-10-01", a git SHA); the engine has no business ordering them. A migration decides what it accepts.
8. **The migration's scope.** **One level at a time, by that level's graph.** The outer graph doesn't know its child's state type, and the child is compiled on its own.
9. **Ending under another graph.** **The same rule.** Ending writes in the graph's schema and rehydrates its state type for the result, so another graph's thread is refused, and a migration applies.
10. **Identity in the definition: `identity`, not `graph`.** A subgraph node's `graph` already holds its child's definition, and `nodes[2].graph.graph` would be unreadable.
11. **Redis: an index set per graph, or a filter in code?** **A filter in code**, like metadata. A per-graph set would need the save script to find and leave the previous graph's set when a thread id is reused by another graph: either a key the script computes (not declared, so not cluster-safe, which Redis support already isn't) or a read of the previous checkpoint on every save. A listing reads each candidate's latest checkpoint anyway. A per-graph index set is "Not built".
12. **The decision worker: load first, or resume and interpret the refusal?** **Load first.** A refused resume is a failed run: observers would report a run for a decision this worker was never meant to handle. The load replaces the resume's own, so it costs nothing.
13. **The wake driver and legacy threads.** **`WakeOptions.MatchGraph`**, default true. Filtering by identity is the point; the switch is the way to drain an unnamed predecessor's timers once.

---

## Implementation Status

Built 2026-10-09 in both engines, every store adapter, both decision workers and the CLI, in one change, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **Refusals follow each engine's habit.** .NET's `Compile` throws `ArgumentException` (`GraphId must not be blank`, `GraphVersion needs a GraphId`, `Migrate needs a GraphId`); Rust's `compile_with` returns `CompilationFailed` with the snake_case names (`graph_id must not be blank`, `a migration needs a graph_id`). A `ThreadQuery` with a version and no id is refused by `Validate()` / `validate()`, so every store refuses it the same way (.NET `ArgumentException`, Rust a `CheckpointStoreError`).
- **The checks that come first stay first.** A resume still refuses an unsupported version, an ended or completed thread and an already-resumed one before it looks at identity, so those messages are unchanged for any graph; identity is checked next, then the rest of the preflight on the migrated checkpoint. An end checks identity after its own refusals the same way.
- **`RunStarted.Node` is the stored checkpoint's next node,** as before: the event comes before the preflight, so after a migration that renames the node a thread resumes at, the event names the old one and the run starts at the new one.
- **The .NET interop type is `Storage.Schema.Identity`,** not `GraphIdentity` (the engine's type) or `Graph` (which would shadow the `Datalium.Strata.Graph` namespace inside `Datalium.Strata.Storage`). It serves both the checkpoint's `graph` and the definition's `identity`.
- **The decision worker's load** is inside the worker's own error handling: a transient or unauthorized Cloud error still fails the round (the decision is tried again), and any other load failure is reported `Failed`, as the resume's own load failure was.
- **The run span's tags are tested in Rust's `telemetry.rs`,** which owns that test binary's process-wide subscriber; .NET checks them in `GraphIdentityTests` with an `ActivityListener`.

**How it was built:**

- **.NET:**
  - `Interruption/GraphIdentity.cs` is new. `Checkpoint` gained `Graph` (wire `graph`, absent when null) and `CheckpointSchema` `GraphVersion` (`1.6.0`), which `For` returns first when any level has one. `ThreadQuery.GraphId` / `GraphVersion` (checked in `Validate`, applied in `Matches`), `ThreadSummary.Graph`.
  - `CompilationOptions.GraphId` / `GraphVersion` / `Migrate`; `StateGraph.Compile` checks them (`IdentityOf`). `CompiledGraph.Identity`, the internal `Migration`, `Owns`, and `Adopt`, which takes a level and recurses through each subgraph node's `NodeEntry.Adopt` (a `LevelAdopter`, returning an `Adopted`). `ToTopology()` carries the identity, so `AddSubgraph` and `AddMap` snapshot their child's with it.
  - The executor writes `Graph = Identity` on every checkpoint it builds (`FreezeAroundChildAsync`, `CancelAtAsync` and `HopLimitAtAsync` became instance methods for it). `ResumeCheckpointAsync`, `EndFromAsync` and `WakeOneAsync` adopt before deciding; `WakeDueAsync` lists by identity unless `WakeOptions.MatchGraph` is false.
  - `RunStarted.Graph` and the run span's `strata.graph.id` / `strata.graph.version` (`RunTelemetry.RunStarted(node, graph)`).
  - Storage: the two columns, the head table's columns, index and trigger, and the migrations in the EF Core and Postgres stores; the interop `Identity`, `Checkpoint.Graph`, `GraphDefinition.Identity` and `IdentityVersion` (`1.5.0`), `ToDefinition` / `ToTopology`. Redis and in-memory filter through `ThreadQuery.Matches`.
  - `Datalium.Strata.Cloud`: `DecisionWorker` loads first, reports `NotMine` when `CompiledGraph.Owns` says no, and its `PinnedStore` serves that load to the resume.
- **Rust:**
  - `schema.rs`: `GraphIdentity`, `Checkpoint::graph`, `GRAPH_CHECKPOINT_SCHEMA_VERSION`, `versioned` (moved here from `checkpoint.rs`), `GraphDefinition::identity`, `IDENTITY_GRAPH_SCHEMA_VERSION`, `graph_version_of(identity, nodes)`, and `check_shape`. `store.rs`: `ThreadQuery::graph_id` / `graph_version` and their builders, `ThreadSummary::graph`.
  - `graph.rs`: `CompileOptions::with_graph_id` / `with_graph_version` / `with_migration` (the options lost `#[derive(Debug)]` for a hand-written one, since a migration is a closure), `CompiledGraph::identity`, `owns` and `adopt`, the compiled `definition` carrying the identity. `node.rs`: `NodeEntry::adopt`, `LevelAdopter`.
  - `checkpoint.rs`: `graph: self.identity.clone()` at every write, `graph` kept by `ended_checkpoint`, and adoption in `resume_checkpoint_at`, `end_from_store` and `wake_one` (which hands the adopted checkpoint to `end_timed_out`); `wake_due_with` lists by identity. `wait.rs`: `WakeOptions::with_match_graph`. `observability.rs`: `RunEventKind::RunStarted { graph }`, the span fields.
  - `strata-memory`: the same columns, head table, trigger and migrations in SQLite and Postgres; Redis unchanged (it filters with `ThreadQuery::matches`). `strata-cloud`: the worker as in .NET.
- **CLI:** `thread list --graph` / `--graph-version` and `graph` in JSON, `thread inspect`'s `Graph:` lines (`store::graphs`, `store::identity_text`) and `graph` in JSON, `graph view`'s `Graph:` header and each child's identity under "Composite nodes".
- **Schema:** `checkpoint.schema.json` (`graph`, `1.6.0`, and the version pairings widened to admit it) and `graph-definition.schema.json` (`identity`, `1.5.0`). Every fixture validates against them, and the schemas refuse a `graph` below `1.6.0` and an `identity` below `1.5.0`.

**Tests:**

- **New suites:** `graph_identity.rs` (24) and `GraphIdentityTests.cs` (24), test for test, plus Rust's span test in `telemetry.rs`.
- **Stores:** the identified row fixture and the nested one's columns, and opening a log from before identities, in SQLite and Postgres, both engines; the shared graph cases in `cases.json` against every store in both engines and the CLI (SQLite, Postgres, Redis); `threads.rows.json` with an identified and an unversioned thread, through every SQL store's trigger.
- **Workers:** a thread another graph wrote is `NotMine` and runs nothing, and its own graph's worker resumes it (`worker.rs`, `DecisionWorkerTests.cs`).
- **CLI:** `--graph` and `--graph-version` listings and `inspect`'s graph lines against SQLite, Postgres and Redis; `graph validate` and `view` of `identity.json`, and an identity refused below `1.5.0` or with an empty id (exit 2); the identified row through the CLI's store.
- **Totals when this landed:** .NET 708, with Postgres and Redis running, and 2 skipped (the opt-in Cloud live tests), plus the example's 1. `rust/` 631 and `cli/` 75, with `STRATA_REQUIRE_BACKENDS=1` against live Postgres and Redis, and `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` and `dotnet format --verify-no-changes` are clean.
- **Mutation checks, in both engines** (each broken, the suites run, then restored):
  - Comparing only versions (not checking the id) fails 3 tests in each.
  - Comparing only ids (not checking the version) fails 5 in each.
  - Checking only the outermost level fails 2 in each.
  - Not copying `graph_id` to the SQLite head table fails 4 in each.

## Not built (deliberately out of scope)

- **Automatic migration** (by a version order, or a schema diff of the state type): a migration is the application's code.
- **A Redis index set per graph**: Redis filters by graph from the candidates, like metadata.
- **Graph identity on Strata Cloud's listings**: Cloud has no query port; it only has to accept `1.6.0`.
- **Refusing a fresh run on a thread id another graph used**: a run doesn't read the store.
- **Labelling metrics with the graph**: spans carry it; metric attributes would multiply series by graph and version.
- **Identity in the builder's export** (`StateGraph.ToDefinition()`): only a compiled graph has one.
