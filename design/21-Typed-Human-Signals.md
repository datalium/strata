# Datalium Strata: Typed Human Signals, Recorded Decisions and a Reject Route

A `HumanSignal` is how a human answers an interrupt boundary (STRATA-SPEC-002, `design/16-Nested-Interrupts.md`). Today three things are wrong with it:

- **Overrides are strings.** `.NET` `HumanSignal(bool IsApproved, string ReviewerToken, Dictionary<string, string>? FieldOverrides)` and Rust's `field_overrides: HashMap<String, String>` are written into the frozen state as JSON strings (`ApplyFieldOverrides` in `CompiledGraph.Interruption.cs`, `apply_field_overrides` in `checkpoint.rs`). A number, a boolean or an object can't be overridden with its real type: rehydration fails, or a string ends up where a structured value belonged.
- **The graph can't see the answer.** `IsApproved` and `ReviewerToken` are carried and then dropped at every level (`design/16`, "Only `field_overrides` matter"). The node after a boundary can't tell whether the human approved, or who they were, so a downstream product encodes the decision as string fields in state and parses them in a node of its own.
- **A rejection can't route.** The next node is fixed when the boundary freezes (`NextNodeAfter` at the boundary). Routing on the human's answer needs an extra node after the boundary.

This document specifies SG-17, which fixes all three in one change. The SG numbers refer to the gap log of StrataOps (not public yet). Both engines get the capability in one change (strict parity, StrataOps ADR 0003), each in its own idiom.

It was written as a blueprint before implementation, reviewed, and is now built. "Review decisions" settles the questions the review was asked, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Typed overrides, .NET | A new `init` property, **`HumanSignal.Overrides`**, typed `IReadOnlyDictionary<string, JsonNode?>?`. `JsonNode` has implicit conversions from `string`, `int`, `bool`, `double` and the rest, so `["replicas"] = 3` just works, and `JsonSerializer.SerializeToNode(plan)` covers objects. `FieldOverrides` is unchanged and still writes JSON strings. |
| Typed overrides, Rust | A new field, **`HumanSignal::overrides: HashMap<String, serde_json::Value>`**, next to the unchanged `field_overrides`. Constructors and a builder are added: `HumanSignal::approve(reviewer)`, `HumanSignal::reject(reviewer)`, `.with_override(key, impl Into<Value>)`. This is a **small breaking change** for Rust: a struct literal that names every field needs the new field or `..Default::default()` (see "Migration"). |
| Both maps set | Allowed, with **disjoint keys**. A key in both is refused before anything is changed. |
| What the graph sees | A **`HumanDecision`**: the boundary it answered, `is_approved`, the reviewer, every override applied (typed, string ones as JSON strings), and when. It is the signal as recorded, not the signal itself. .NET: `NodeContext.Decision`. Rust: `ctx.decision()`. |
| Which signal becomes a decision | Only one that **answers an interrupt boundary**: the signal delivered to a level whose checkpoint is `interrupted` at that level's own boundary. Resuming a `failed` or `cancelled` checkpoint, or a signal addressed to an outer level with `LevelSignals`, applies overrides and records no decision. |
| How long it is visible | To **every node of that level** (branches included) for the rest of the level's life, until another boundary of the same level is answered. A subgraph's child doesn't inherit its parent's decision. |
| Persisted | **Yes.** Every checkpoint a level writes while a decision is in effect carries it, in a new optional **`decision`** field. A resume restores it. So a `Failed` checkpoint written after an approval still knows who approved, and the node it re-enters sees it. |
| Wire format | Checkpoint `schema_version` **`1.3.0`** for a checkpoint that carries a `decision` at any depth. A new nullable **`decision` column** in each SQL layout, added when a store opens an old log, the way `nested` was. Redis and in-memory need nothing. |
| Reject route | **`AddInterruptBoundary(name, onReject: "Abort")`** in .NET and **`add_interrupt_boundary_with_reject(name, "Abort")`** in Rust. A resume of that boundary with `is_approved = false` continues at the reject route instead of the next node fixed at freeze time. See "Exactly when the reject route applies". |
| Reject route in definitions | A synthetic guarded edge **`boundary → route`, condition `on_reject`**, appended after the `on_failure` edges, like `GraphTopology.WithFallbackEdges`. `.Compile()`, `to_definition()` / `ToDefinition()` and `strata graph validate` all see it. No graph-definition schema change. |
| A bad override | **Refused before the checkpoint is stamped**, at every level. Today a resume stamps `resumed_at` first and rehydrates after, so an override that doesn't fit the state uses up the thread. Typed overrides make that mistake easier, so a preflight check is part of this change. |
| .NET `JsonNode` or `JsonElement` | **`JsonNode` in, `JsonElement` out.** `HumanSignal.Overrides` takes `JsonNode` because it is easy to build. `HumanDecision.Overrides` is `IReadOnlyDictionary<string, JsonElement>`, because one decision is shared by every node of a level, including concurrent branches, and a `JsonNode` could be mutated by any of them. |
| Run events | `Resumed` gains `IsApproved`, `Reviewer` and `RejectedTo` (the reject route, when it was taken). Overrides stay out of events, like state (`design/18`, "State in events"). |
| A typed `TSignal` generic | **No.** See "Why not a typed signal". |
| Not in scope | Who may approve (StrataOps policy owns it; Strata carries the reviewer's identity and doesn't judge it), deep or patch-style overrides, the decision on `ToolCallContext`, inheritance of a parent's decision by a child, persisting the reject route, anything in SG-09. |

---

## Usage

```csharp
var remediation = new StateGraph<Remediation>()
    .AddNode("Plan", PlanAsync)
    .AddNode("Approve", s => s)
    .AddNode("Apply", async (s, ctx, ct) =>
    {
        // The run resumed at Approve, so the decision is set. s.Plan and s.Replicas already hold
        // the human's edits: overrides are applied to state before any node runs.
        var decision = ctx.Decision!;
        await audit.RecordAsync(decision.Reviewer, decision.Boundary, decision.DecidedAt, ct);
        return await ApplyAsync(s, ct);
    })
    .AddNode("Abort", (s, ctx, ct) =>
        ValueTask.FromResult(s with { Outcome = $"rejected by {ctx.Decision!.Reviewer}" }))
    .AddEdge("Plan", "Approve")
    .AddEdge("Approve", "Apply")
    .AddInterruptBoundary("Approve", onReject: "Abort")
    .SetEntryPoint("Plan");

// The approval console: an approval with an edited plan and a changed replica count.
await graph.ResumeAsync(threadId,
    new HumanSignal(IsApproved: true, ReviewerToken: "alice@acme.io")
    {
        Overrides = new Dictionary<string, JsonNode?>
        {
            ["replicas"] = 3,
            ["plan"] = JsonSerializer.SerializeToNode(editedPlan),
        },
    },
    store);

// A rejection continues at Abort instead of Apply.
await graph.ResumeAsync(threadId, new HumanSignal(IsApproved: false, ReviewerToken: "bob@acme.io"), store);
```

```rust
let graph = StateGraph::<Remediation>::new()
    .add_async_node("Plan", plan)
    .add_node("Approve", |s| s)
    .add_async_node("Apply", |s: Remediation, ctx: NodeContext| async move {
        let decision = ctx.decision().expect("resumed at Approve");
        record_audit(&decision.reviewer, &decision.boundary, &decision.decided_at).await;
        apply(s).await
    })
    .add_node("Abort", |s| s)
    .add_edge("Plan", "Approve")
    .add_edge("Approve", "Apply")
    .add_interrupt_boundary_with_reject("Approve", "Abort")
    .set_entry_point("Plan")
    .compile()?;

graph
    .resume(
        thread_id,
        HumanSignal::approve("alice@acme.io")
            .with_override("replicas", 3)
            .with_override("plan", serde_json::to_value(&edited_plan)?),
        store.clone(),
    )
    .await;
```

---

## Typed overrides

### .NET

```csharp
public sealed record HumanSignal(
    bool IsApproved,
    string ReviewerToken,
    Dictionary<string, string>? FieldOverrides = null)          // unchanged: each value is written as a JSON string
{
    /// Overrides with their JSON type. A JSON null sets the field to null.
    public IReadOnlyDictionary<string, JsonNode?>? Overrides { get; init; }

    public static HumanSignal Approve(string reviewer);           // new HumanSignal(true, reviewer)
    public static HumanSignal Reject(string reviewer);            // new HumanSignal(false, reviewer)
    public HumanSignal WithOverride(string key, JsonNode? value); // a copy with one more typed override
}
```

`Overrides` is an `init` property, not a fourth positional parameter, so positional construction and deconstruction of `HumanSignal` are unchanged. Every existing `new HumanSignal(true, "x", new Dictionary<string, string> { ... })` keeps compiling and keeps its meaning.

`JsonNode` and not `JsonElement`, for the input: it is what the frozen state already is (`Checkpoint.FrozenState`), so merging is a direct assignment, and it has implicit conversions from the primitives, so a typical override needs no `JsonSerializer` call at all. The engine deep-clones every value it applies, because a `JsonNode` can only have one parent: the caller can reuse a signal or a value without "the node already has a parent".

### Rust

```rust
#[derive(Debug, Clone, Default)]
pub struct HumanSignal {
    pub is_approved: bool,
    pub reviewer_token: String,
    pub field_overrides: HashMap<String, String>,      // unchanged: each value is written as a JSON string
    pub overrides: HashMap<String, serde_json::Value>, // new: written with its JSON type
}

impl HumanSignal {
    pub fn approve(reviewer: impl Into<String>) -> Self;
    pub fn reject(reviewer: impl Into<String>) -> Self;
    pub fn with_override(self, key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self;
    pub fn with_field_override(self, key: impl Into<String>, value: impl Into<String>) -> Self;
}
```

`impl Into<Value>` covers `&str`, `String`, numbers, `bool`, `Vec<_>`, `Option<_>` and `Value` itself, so `.with_override("approved_by", "alice")` still writes a JSON string.

A new public field can't be added to a struct with all-public fields without breaking struct literals that name every field. All 19 `HumanSignal { .. }` literals in this repository (`examples/rust-agent` among them) do. The alternatives are worse: a second signal type would double every `resume` overload, and making the struct `#[non_exhaustive]` would break every literal for good instead of once. The constructors make the next field addition non-breaking for code that uses them.

### Semantics, both engines

| Aspect | Behaviour |
|---|---|
| **Where** | Top-level fields of the state's JSON object, as today. An override replaces the field's whole value: overriding `plan` replaces the plan. Patching inside a value (JSON Pointer, merge patch) is not built. |
| **String form** | `FieldOverrides` / `field_overrides` keep their exact meaning: the value is written as a JSON string. |
| **Both maps** | Both apply. A key present in both is refused (`cannot resume thread 't': override 'replicas' is set as both a string and a typed value`), before anything is stamped or run. |
| **JSON null** | Sets the field to `null`. It doesn't remove it. |
| **A non-object state** | Overrides are ignored, as today. |
| **A key the state doesn't have** | Inserted, as today. Each engine's deserializer then decides: by default both ignore an unknown field. Refusing unknown keys is not built, because a Rust field skipped while `None` (`skip_serializing_if`) is absent from the JSON but is still a valid override target. |
| **A value that doesn't fit** | Refused before the checkpoint is stamped, at any level. See "Preflight". |
| **Levels** | Unchanged from `design/16`: the innermost signal's overrides apply to the innermost frozen state, and a `LevelSignals` entry for an outer level applies to the state its subgraph node started with. Typed overrides work at every level. |

---

## Recorded decisions

### Shape

```csharp
public sealed record HumanDecision
{
    public required string Boundary { get; init; }     // the interrupt boundary it answered, at its own level
    public required bool IsApproved { get; init; }
    public required string Reviewer { get; init; }     // HumanSignal.ReviewerToken
    public IReadOnlyDictionary<string, JsonElement> Overrides { get; init; }   // every override applied, sorted by key
    public required string DecidedAt { get; init; }    // RFC 3339: when the resume began
}

// NodeContext gains:
public HumanDecision? Decision { get; init; }
```

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HumanDecision {
    pub boundary: String,
    pub is_approved: bool,
    pub reviewer: String,
    pub overrides: serde_json::Map<String, serde_json::Value>,
    pub decided_at: String,
}

// NodeContext gains:
pub fn decision(&self) -> Option<&HumanDecision>;
pub fn with_decision(self, decision: HumanDecision) -> Self;   // for handlers tested by hand
```

`Overrides` merges both of the signal's maps, string ones as JSON strings. It is the one shape whether the decision came from the resume that is running or was read back from a checkpoint, so a node doesn't have to look in two places. It's also exactly what is persisted.

`HumanSignal.ReviewerToken` becomes `Reviewer` here because that is what it is: the engine **persists it** and shows it to every node and observer. It must be an identity (a user id, an email address), never a credential. The XML docs and rustdoc of `ReviewerToken` / `reviewer_token` say so. Renaming the signal's field would break every caller for no behavioural gain.

### Which signal becomes a decision

A decision answers an interrupt boundary. A resume records one at **at most one level**: the level whose checkpoint is `interrupted` at its own boundary, with nothing nested. That is always the innermost level, and its signal is always the innermost one.

| Resume of… | Decision recorded | Overrides applied |
|---|---|---|
| A flat `interrupted` checkpoint | Yes, from the signal | Yes |
| A nested `interrupted` checkpoint | At the innermost level, from the innermost signal | Innermost, and outer levels addressed with `LevelSignals` |
| A `failed` or `cancelled` checkpoint, flat or nested | **No.** The decision stored in the checkpoint, if any, stays in effect | Yes, as a correction |
| An outer level addressed with `LevelSignals.At` | **No.** Its `IsApproved` and reviewer are not used, as today | Yes |

The `failed` row is what makes the decision useful after a failure. Alice approves at `Approve`, `Apply` fails, and an operator, Bob, resumes the failed thread to retry it (perhaps correcting a field). `Apply` runs again and `ctx.Decision` still says Alice approved. Bob's resume shows up in the `Resumed` run event, which carries the resuming signal's reviewer, so both facts reach an audit trail. If retries replaced the decision, an approval would be lost every time an operator retried.

### Where it is visible

- **Every node of the level** sees the level's decision, from the node the resume continues at until the level ends: that is, until the run completes, freezes again, fails or is cancelled. Parallel branches see their group's level. `NodeContext.Decision` is null in a run that has never been resumed at a boundary of that level.
- **Answering another boundary of the same level replaces it.** A graph that asks twice (`Draft → Approve → Revise → Approve`) sees the latest answer.
- **A subgraph's child doesn't inherit its parent's.** A child level starts with no decision, and gets one only by answering one of its own boundaries. Each level sees the signal addressed to it, as the gap asks. If a child needs its parent's decision, the parent passes it in through the subgraph's `input` mapper, which is the subgraph contract for everything else too (`design/15`). Inheritance is listed under "Not built".
- **An outer level keeps its own across a child's freeze.** `Approve` (parent boundary) → `Work` (subgraph, with its own boundary) → `Execute`: the parent's checkpoint around `Work` carries the parent's decision, so when the child completes after the second resume, `Execute` still sees the parent-level answer.
- **Retries:** every attempt of a node sees the same decision. A subgraph retried after a resumed attempt re-enters the same nested checkpoint, and its child gets the same decision, `DecidedAt` included.

### Persistence

Every checkpoint a level writes while a decision is in effect carries it in `decision`: `failed`, `cancelled`, `interrupted` at a later boundary, and the parent's `interrupted` / `failed` / `cancelled` checkpoint around a child. A nested checkpoint carries its own level's decision, so each level's decision is stored where that level's state is. A resume restores the decision of every level from its checkpoint, unless this resume answers that level's boundary.

The `resumed_at` stamp row is unchanged: a copy of the checkpoint it resumes, plus the timestamp. It doesn't record the answer, because `decision` on a checkpoint has one meaning: the decision in effect when it was written. The answer itself reaches the store with the next checkpoint the level writes, and observers through `Resumed`. If the run completes after the answer, nothing more is written: completed runs write no checkpoint today. A product that needs every answer in its own audit log records it from `Resumed` or from the node, as StrataOps' Incidents module does.

---

## The reject route

```csharp
public StateGraph<TState> AddInterruptBoundary(string name, string? onReject = null);
```

```rust
pub fn add_interrupt_boundary(self, name: impl Into<String>) -> Self;                     // unchanged
pub fn add_interrupt_boundary_with_reject(
    self, name: impl Into<String>, on_reject: impl Into<String>) -> Self;
```

Rust's existing per-node setters (`set_recovery_policy`, `set_timeout`) would suggest `set_reject_route(boundary, node)`. A separate setter could name a node that isn't a boundary, though, which would then need its own compile error. The combined method can't be misused.

### Exactly when the reject route applies

"The next node is fixed when the boundary freezes" stays the rule, with one exception. A resume continues at the reject route instead of the checkpoint's `next_node` when **all** of these are true:

1. The level being resumed is waiting at **its own** boundary: its checkpoint is `interrupted` and has no `nested` child. At a nested level, that is the child's checkpoint, and the child's graph is the one whose boundary is looked up.
2. That level's graph declares a reject route for the checkpoint's `suspended_node`.
3. The signal delivered to that level (always the innermost signal, by rule 1) has `is_approved = false`.

So:

- **An approval**, or a boundary with no reject route, continues at `next_node`, exactly as today. A rejection at a boundary with no route still continues at `next_node`; the node there can read `ctx.Decision.IsApproved`. Existing graphs don't change behaviour.
- **A `failed` or `cancelled` checkpoint** never takes the reject route: rule 1. The failed node is re-entered, whatever `is_approved` says.
- **Outer levels** never take it during the same resume: their checkpoints nest a child, rule 1.
- **A subgraph node that is itself a boundary** (`design/16`, "Parent boundary on the subgraph node") takes its route like any other boundary, once the child has completed and the parent has frozen at the node in the ordinary way.
- **A boundary with no outgoing edge** freezes with `next_node: null`, as today. An approval of it is refused (`has no next node to resume from`). A rejection, if it has a route, continues at the route. So the "no next node" check moves after the reject decision.
- **Overrides still apply** on a rejection. The state the reject route starts with is the frozen state plus the overrides, which is where a rejection reason can go.
- **The decision is recorded** on a rejection too, with `IsApproved = false`, so the reject route's nodes can read who rejected.

The route is **not persisted** in the checkpoint. It's a static property of the graph, like a recovery-policy fallback (which isn't persisted either), and the resume reads it from the compiled graph. `next_node` is persisted because it depends on the state at freeze time, and a reject route doesn't. A graph changed between freeze and resume uses its current route. Graph identity in checkpoints is SG-13.

### Validation and export

The reject route is a synthetic edge `boundary → route`, guarded, with `condition: "on_reject"`, added by `GraphTopology.WithRejectEdges` (.NET) and its Rust twin, next to `WithFallbackEdges`. The executor doesn't route over it. The full edge order in the topology and the export is: ordinary edges in insertion order, then `on_failure` edges in node declaration order, then `on_reject` edges in node declaration order.

Because the edge is part of what `.Compile()` validates:

- **A node reachable only through a reject route** isn't an orphan.
- **Reject and try again** (`Approve` rejects to `Revise`, which leads back to `Approve`) is a guarded loop, which is valid.
- **A reject route to a node that doesn't exist** fails with `STRATA_ERR_DANGLING_EDGE`. So does a reject route on a boundary name that isn't a node. A plain boundary on an unknown name is still accepted and never triggers, as today.

The graph-definition schema doesn't change: `condition` is already free text. Its description gains `on_reject` beside `on_failure`. `strata graph validate` needs no code change: it sees one more guarded edge.

---

## Preflight

Today `ResumeCheckpointAsync` / `resume_checkpoint` stamps `resumed_at` first, then applies the overrides and rehydrates the state. When rehydration fails the resume returns `Failed("failed to rehydrate state …")`, and the thread can never be resumed again: its checkpoint is already stamped. At a nested level it's worse: the child's failure becomes a fatal failure of the subgraph node with no nested checkpoint, and resuming that re-runs the child from its start, asking the human again.

With strings this happened only to an override aimed at a non-string field. With typed overrides, a wrong type is an ordinary mistake. So, before anything is stamped, the resume checks every level the signals reach:

1. Refuse a key set in both maps of the same signal.
2. Apply the level's overrides to a copy of its frozen state.
3. Deserialize that copy into the level's state type.
4. At a level whose checkpoint nests a child, ask the subgraph node to check the child the same way, with the child's state type. A subgraph node knows its `TChild`, so its entry gains an internal `CheckResume(childCheckpoint, signals)` / `check_resume(..)` beside its handler, which recurses.

The first problem refuses the whole resume with `Failed` and the level's path (`cannot resume thread 't': failed to rehydrate state at level [Work]: …`). Nothing is stamped or run, so the thread stays resumable with a corrected signal. The message keeps the words "failed to rehydrate", which existing tests and callers match on. A resume that passes the preflight applies the same overrides again for real, so the check costs one extra deserialization per level that has overrides.

---

## Run events and telemetry

`Resumed` (`design/18`) gains three fields:

| Field | Value |
|---|---|
| `IsApproved` / `is_approved` | The innermost signal's: what the person resuming said. |
| `Reviewer` / `reviewer` | The innermost signal's `ReviewerToken`: who resumed. On a failed-thread retry this is the operator, while `NodeContext.Decision` keeps the approver. |
| `RejectedTo` / `rejected_to` | The reject route, when this resume took it; otherwise null / `None`. |

Overrides are not in the event: they are state-like values, possibly personal data, and design/18 keeps state out of events.

**Spans and metrics: no change.** The reviewer is an identity, and the run span doesn't get it. An observer that wants it has `Resumed`. `strata.graph.transition` isn't recorded for the jump to a reject route, since a resume is not a transition between two nodes of one run.

`RunStarted`'s node, for a resume, becomes the node the outermost level actually continues at (the reject route, when it is taken at a flat level).

---

## Wire format

### Checkpoint

`schema/checkpoint.schema.json` gains one optional property and a fourth version:

```jsonc
{
  "schema_version": "1.3.0",
  "thread_id": "thread_decision_demo",
  "status": "failed",
  "suspended_node": "Apply",
  "next_node": "Apply",
  "frozen_state": { "plan": { "steps": ["drain", "rollback"] }, "replicas": 3, "outcome": null },
  "metadata": { "error": "cluster unreachable", "error_kind": "fatal", "attempts": "1" },
  "created_at": "2026-10-04T10:15:02Z",
  "resumed_at": null,
  "decision": {
    "boundary": "Approve",
    "is_approved": true,
    "reviewer": "alice@acme.io",
    "overrides": { "plan": { "steps": ["drain", "rollback"] }, "replicas": 3 },
    "decided_at": "2026-10-04T10:15:00Z"
  }
}
```

- **`decision`**: an object with exactly those five required properties; `overrides` is an object of any JSON values, keys sorted by ordinal (Rust's default `serde_json::Map` is a `BTreeMap`; .NET sorts when writing). It is **absent**, not `null`, when no decision is in effect, so every checkpoint that predates this serializes byte for byte as before.
- **`schema_version`** may also be `"1.3.0"`, written for a checkpoint that carries `decision` **at any depth** (itself or any checkpoint nested in it). Otherwise the existing rule applies: `1.2.0` if anything in it is cancelled, `1.1.0` if it nests a child, `1.0.0` otherwise. `CheckpointSchema.For` / `schema::version_for` gain the check. The schema requires `1.3.0` on a checkpoint that has `decision`. The cancelled rule loosens from `const "1.2.0"` to `enum ["1.2.0", "1.3.0"]`, since a cancelled checkpoint can now also carry a decision.
- **Older runtimes refuse `1.3.0`** (`unsupported schema_version`), which is right: they would drop the decision and silently re-route a rejection. Both engines and the CLI accept it.

### Storage

| Store | Column | Notes |
|---|---|---|
| SQLite (`strata-memory`, `EfCoreCheckpointStore`, the CLI) | `decision TEXT` | JSON text of the decision, or `NULL` |
| Postgres (both runtimes) | `decision JSONB` | same, as `JSONB` |
| Redis (both runtimes) | none | the element is the whole checkpoint JSON |
| In-memory | none | holds the record |

A log created before this has no `decision` column. Each store adds it when it opens the log, by the same steps as `nested` (`design/16`, "Upgrading an old log"): `pragma_table_info` plus `ALTER TABLE … ADD COLUMN decision TEXT` for SQLite, `ADD COLUMN IF NOT EXISTS decision JSONB` inside the existing advisory-locked transaction for Postgres. Existing rows keep `NULL`, which is correct. The `checkpoint_threads` head table and the Redis index don't change: listing threads by reviewer isn't asked for, and a decision is per level, not per thread.

### The CLI

- `thread inspect` gains a `Decision:` line per level that has one (`Decision:   rejected by bob@acme.io at Work/Approve, 2026-10-04T10:15:00Z`). `--json` gains `decision` (null when absent), and the nested object carries its own.
- `checkpoint replay` likewise, and its `--json` too.
- `thread list` and `checkpoint log` are unchanged.

---

## Why not a typed signal

A `HumanSignal<TSignal>` or a `ResumeAsync<TSignal>` would have to flow through `LevelSignals` (whose levels each have a different state type), through `NestedResume` into every subgraph node, onto `NodeContext` (which isn't generic), into the checkpoint (serialized, then deserialized by a runtime that may be the other engine), and through the CLI, which knows no types at all. The JSON-valued version gives the same result at the two ends that matter. The console builds the override from a typed object with `JsonSerializer.SerializeToNode(plan)` / `serde_json::to_value(&plan)`. A node reads it back typed from the state, which already has the override applied, or from `ctx.Decision.Overrides["plan"].Deserialize<Plan>()` / `serde_json::from_value(decision.overrides["plan"].clone())`. A typed accessor such as `decision.Get<T>(key)` can be added later without breaking anything.

---

## Implementation sketch

- **.NET:**
  - `Interruption/HumanSignal.cs` gains `Overrides`, `Approve`, `Reject`, `WithOverride`. `Interruption/HumanDecision.cs` is new, with `ToWireJson` / `FromWireJson` helpers that `Checkpoint` uses.
  - `Checkpoint` gains `Decision`. `CheckpointSchema` gains `DecisionVersion = "1.3.0"`, and `For` becomes `For(status, nested, decision)`.
  - `LevelScope` gains the level's `Decision`. `ContextFor` copies it onto `NodeContext`, and every checkpoint the level writes copies it into `Decision`. `SubgraphRunner` resets it for the child's level.
  - `ResumeCheckpointAsync` runs the preflight, then stamps, then decides the start node (reject route or `NextNode`), then builds the level with its decision: the new one if this level answered its boundary, otherwise the checkpoint's own.
  - `StateGraph` keeps `_rejectRoutes`, `GraphTopology.WithRejectEdges` adds the edges, and `CompiledGraph` gets the routes.
  - The subgraph `NodeEntry` gains the internal `CheckResume` delegate.
  - Storage: the `decision` column and its upgrade in `EfCoreCheckpointStore` and `PostgresCheckpointStore`. The Redis store already goes through `ToWireJson`.
- **Rust:**
  - `checkpoint.rs`: `HumanSignal` gains its field and builders. `HumanDecision` lives in `schema.rs` (it is wire shape). `Checkpoint.decision: Option<HumanDecision>` uses `#[serde(default, skip_serializing_if = "Option::is_none")]`. `DECISION_CHECKPOINT_SCHEMA_VERSION` is added.
  - `LevelScope` and `NodeContext` change as in .NET. `resume_checkpoint` follows the same order.
  - The graph builder gains `add_interrupt_boundary_with_reject` and `reject_routes`, and `to_definition` adds the edges.
  - The subgraph's stored handler gains `check_resume`.
  - Storage: `strata-memory`'s SQLite and Postgres stores get the column and its upgrade, and so does the CLI's `store::open`.
- **Docs, once built:** `design/16`'s "Only `field_overrides` matter" row and its "Not built: a per-level `is_approved`" bullet are updated to point here, as are STRATA-SPEC-002's Implementation Status, `schema/README.md` (the new fixtures and version), and a section 13 in `CLAUDE.md`.

---

## Migration

- **.NET: source-compatible.**
  - `HumanSignal` keeps its constructor and deconstruction.
  - `AddInterruptBoundary` gains an optional parameter. That is binary-breaking only, and nothing ships as a binary yet.
  - `NodeContext` and `Checkpoint` gain `init` properties.
  - The `Resumed` record gains three positional parameters. That breaks code that constructs or positionally deconstructs a `Resumed`, which observers don't do: they read properties.
- **Rust: two small breaks.**
  - Every `HumanSignal { is_approved, reviewer_token, field_overrides }` literal needs `overrides: HashMap::new()` or `..Default::default()`, or can switch to `HumanSignal::approve(..)`. That is 19 literals in this repository, `examples/rust-agent` among them, plus `strata-demos-rust`, which still awaits its design/17 port anyway.
  - A pattern that names every field of `RunEventKind::Resumed` needs `..`.
- **Behaviour changes:**
  - A resume whose overrides don't fit the state is now refused **before** the checkpoint is stamped, so the thread stays resumable. Before, the same resume used the thread up. The error text still says "failed to rehydrate".
  - A key in both override maps is refused.
  - Nothing else changes for a graph without reject routes.
- **Wire:** `1.3.0` checkpoints are refused by older runtimes and the older CLI. Old logs gain the `decision` column when opened. StrataOps pins this repo, so its bump brings both together.

---

## Tests

Twin suites, `rust/strata-core/tests/human_signals.rs` and `dotnet/tests/Datalium.Strata.Tests/HumanSignalTests.cs`, mirroring each other:

- **Typed overrides:** an integer, a boolean, an object and a JSON null each rehydrate with their real type. The string form still writes a JSON string. A value reused across two resumes works (.NET parent issue). A key in both maps is refused before the stamp.
- **Preflight:** a value that doesn't fit is refused and the thread stays resumable. A corrected resume then succeeds. This is checked flat, at an inner level, and at an outer level addressed with `LevelSignals`. The existing "failed to rehydrate" tests keep passing, now with the checkpoint left unstamped.
- **Decisions:**
  - Null in a fresh run.
  - Set after a boundary resume, for the next node, a later node and every branch of a parallel group after it.
  - Not visible in a fresh child.
  - The innermost level of a nested resume gets its own decision.
  - An outer level keeps its earlier decision across a child's freeze.
  - A second boundary of the same level replaces it.
  - The same decision on every retry, `DecidedAt` included.
- **Persistence:**
  - `Apply` fails after an approval, and its `failed` checkpoint carries the decision.
  - The operator's retry doesn't replace it: `Apply` sees Alice, and `Resumed` carries Bob.
  - The same for `cancelled`.
  - A nested failure carries the child's decision in `nested`.
- **Reject route:**
  - Rejected continues at the route; approved continues at `next_node`.
  - Rejected with no route continues at `next_node`.
  - A rejection inside a child routes inside the child.
  - A subgraph node that is a boundary routes once its child completes.
  - A `failed` checkpoint ignores `is_approved`.
  - Reject and revise loops back to the boundary.
  - A boundary with no outgoing edge: an approval is refused, and a rejection routes.
  - Overrides apply on a rejection.
- **Validation and export:**
  - The `on_reject` edge appears after the `on_failure` edges.
  - A node reachable only by rejection is not an orphan.
  - A route to an unknown node, or on an unknown boundary name, is a dangling edge.
  - Reject and revise validates.
- **Events:** `Resumed` carries `IsApproved`, `Reviewer` and `RejectedTo`.
- **Shared fixtures**, produced live by both engines (timestamps aside) and resumed by both:
  - `checkpoint-lifecycle/decision-failed.json`: flat, approved with typed overrides, then a fatal failure.
  - `checkpoint-lifecycle/nested-decision.json`: a rejection inside `Work` that routes to the child's reject route, which then fails, so the decision is nested.
- **Graph export:** `graph-export/reject-route.json` is written byte for byte by both engines. `cli/tests/graph_export_gate.rs` feeds it to `strata graph validate`.
- **Storage:**
  - `checkpoint-log/columns.json` and `postgres-columns.json` gain `decision`, and a new `decision.row.json` is added.
  - The store contracts (`checkpoint_store_contract.rs`, `StoreContracts.cs`) round-trip a decision through every backend.
  - The old-layout upgrade tests now also cover a log without `decision`.
  - The CLI test seeds the fixture and checks `thread inspect` and `checkpoint replay`, human output and `--json`.
- **Mutation checks**, as for design/17 to 20. Each of these should fail tests in both engines:
  - Writing typed overrides as strings.
  - Ignoring the reject route.
  - Taking the reject route from a `failed` checkpoint.
  - Not persisting the decision on a `failed` checkpoint.
  - Letting a failed-thread retry replace the decision.
  - Stamping before the preflight.

---

## Review decisions

Reviewed 2026-10-04. Every recommendation below was accepted as asked.

### The questions as asked

Recommendations are in bold.

1. **What becomes a decision.** Only a signal that answers a boundary, so that a failed-thread retry keeps the approval (as above)? Or simply the latest signal delivered to a level, which is easier to state but loses the approver on every retry unless the operator repeats it? **Only an answer.**
2. **Where the decision is stored.** A typed `decision` field with its own column and a `1.3.0` version? Or reserved `strata.decision.*` metadata keys, which need no column and no migration but put the overrides into a string-to-string map as JSON text? **A field.**
3. **The Rust break.** Accept that `HumanSignal` literals need the new field (19 in the repo), with constructors added so it's the last such break? **Accept.**
4. **Preflight in this change.** It fixes an existing hazard, but one that typed overrides make much more likely. Include it, or leave it for a follow-up? **Include.**
5. **Rust reject-route API.** `add_interrupt_boundary_with_reject(name, route)`, or `set_reject_route(boundary, route)` with a compile error for a non-boundary? **The former.**
6. **A typed `TSignal`.** **No**, for the reasons above.

---

## Implementation Status

Built 2026-10-04 in both engines, in one change, as specified above. These are the differences and details.

**Where it diverged, and why:**

- **Two produced fixtures changed.** `nested-failed.json` and `metadata-failed.json` come from a live failure *after* the child's boundary was answered, so the child now records that answer: both levels carry `1.3.0` and the child has a `decision`. "Every checkpoint that predates this serializes byte for byte as before" still holds for reading a stored checkpoint; what changed is what a resume writes. The `1.1.0` failed-nested shape is no longer produced by any live test, only read (round-trip and old-row tests).
- **One more test, found by a mutation check.** "Take the reject route from a failed checkpoint" survived the first run: the failure in the tests was at `Apply`, which has no route, so rule 1 was never exercised. The case where it matters is a fallible **boundary node** that fails: its failed checkpoint is suspended at the boundary itself. `A_Boundary_Node_That_Failed_Is_Re_Entered_Even_On_A_Rejection` / `a_boundary_node_that_failed_is_re_entered_even_on_a_rejection` now pin that a rejection re-runs the boundary (which freezes again) instead of routing.
- **The version rule is computed from content.** The blueprint had `CheckpointSchema.For(status, nested, decision)`. Built as `CheckpointSchema.For(Checkpoint)` / `schema::checkpoint_version_of(&Checkpoint)`, and every writer wraps its checkpoint in `Versioned` / `versioned`, so no writer can pick a version that disagrees with what it wrote.
- **The resume check takes the thread id.** `ResumeChecker` / `check_resume` takes the outermost thread id beside the level path, so a refusal at any depth names the thread the caller resumed.
- **Error wording stays each engine's own.** A missing next node is "has no next node to resume from" in .NET and "has no next_node to resume from" in Rust, as before; both name the level (`at level [Work]`) when it is nested.
- **Postgres migration.** One `ALTER TABLE` with two `ADD COLUMN IF NOT EXISTS` actions, so the shared migration constant stays a single statement, identical in both engines.
- **Rust literal count.** 17 `HumanSignal { .. }` literals in this repository needed `..Default::default()` (the blueprint's 19 counted two function signatures). `strata-demos-rust` will need the same, along with its design/17 port.
- **Re-marking a boundary.** In both engines, marking a boundary again without a route (`AddInterruptBoundary(name)` / `add_interrupt_boundary(name)`) removes its route, and marking it with one replaces it.
- **.NET `HumanDecision` equality.** It is a record whose `Overrides` is a dictionary, so two equal decisions aren't `Equals`; tests compare `ToWireJson()`. Rust's derives `PartialEq` on the map and compares by value.

**How it was built:**

- **.NET:**
  - `Interruption/HumanSignal.cs` gains `Overrides`, `Approve`, `Reject`, `WithOverride` and an internal `EffectiveOverrides` (both maps as JSON, sorted, the clash refused). `Interruption/HumanDecision.cs` is new, with `ToWireJson` / `FromWireJson`; `Checkpoint` gains `Decision` and `CheckpointSchema.DecisionVersion`.
  - `LevelScope` gains `Decision`, and `Child(token, path)` resets it for a subgraph's child. `RunScope.ContextFor` copies it onto `NodeContext`, branches included. `LevelSignals` carries the resume's `DecidedAt`.
  - `CompiledGraph.Interruption.cs`: `ResumeCheckpointAsync` checks the signals, runs `CheckResume` (every level, recursing through the subgraph entry's `CheckResume` delegate), stamps, picks the start node and the decision, applies overrides and runs. `RejectRouteFor` holds the three-rule test. Every checkpoint writer sets `Decision = level.Decision`.
  - `StateGraph` keeps `_rejectRoutes`; `GraphTopology.WithRejectEdges` adds the `on_reject` edges after `WithFallbackEdges`.
  - Storage: the `decision` column (DDL, upgrade on open, write, both read paths) in `EfCoreCheckpointStore` and `PostgresCheckpointStore`; the interop `Storage.Schema.Checkpoint` gains `Decision`. Redis already writes the wire JSON.
- **Rust:**
  - `checkpoint.rs`: `HumanSignal` gains `overrides` and the builders; `resume_checkpoint`, `check_resume`, `reject_route_for`, `answers_boundary` and `versioned` mirror .NET. `HumanDecision` lives in `schema.rs` with `Checkpoint::decision`.
  - `context.rs`: `LevelScope` holds an `Arc<HumanDecision>`; `NodeContext` gains `decision()` and `with_decision`. `node.rs`: `NodeEntry::check_resume`, `ResumeChecker`, `ResumeCheck`.
  - `graph.rs`: `add_interrupt_boundary_with_reject`, `reject_routes`, `with_reject_edges`.
  - `observability.rs`: `RunEventKind::Resumed` gains `is_approved`, `reviewer`, `rejected_to`.
  - `strata-memory`: the column in the SQLite and Postgres stores (`add_missing_column` now handles both upgrades).
- **CLI:** `store::decisions` renders each level's decision; `thread inspect` prints a `Decision:` line per level and `checkpoint replay` a `Decision in effect:` line, and both `--json` outputs gain `decision`.

**Tests:**

- **New suites:** `rust/strata-core/tests/human_signals.rs` (40) / `HumanSignalTests.cs` (40), twins.
- **Added to existing suites:** a decision row test in each SQL layout suite (SQLite in both engines and the CLI, Postgres in both engines), decision round-trips in both store contracts (every backend), the old-layout upgrade saving a decision, a CLI command test over both new fixtures, and a CLI gate test for `reject-route.json`.
- **Updated:** the nested failure tests now expect `1.3.0` and the child's decision; the timestamp-stripping helpers blank `decided_at` too.
- **Fixtures:** `checkpoint-lifecycle/decision-failed.json`, `nested-decision.json`, `checkpoint-log/decision.row.json`, `graph-export/reject-route.json` (new); `columns.json` / `postgres-columns.json` gain the column and the existing row fixtures gain `"decision": null`. Every checkpoint fixture validates against the updated JSON Schema, and a decision under `1.2.0` is rejected by it.
- **Totals when this landed** (all backends live): .NET 432 plus the example's 1, `rust/` 401, `cli/` 43, `examples/rust-agent` 1.
- **Mutation checks, in both engines,** each restored afterwards:

| Mutation | .NET failures | Rust failures |
|---|---|---|
| Typed overrides written as strings | 9 | 9 |
| Reject route ignored | 9 | 9 |
| Reject route taken from a failed checkpoint | 1 | 1 |
| Decision not persisted on a failed checkpoint | 9 | 9 |
| A failed-thread retry replaces the decision | 5 | 6 |
| Stamped before the preflight | 5 | 5 |

**Downstream, checked against this branch:** `strata-demos-dotnet` builds with no warnings. `strata-demos-rust` still awaits design/17's async port, and will also need its `HumanSignal` literals migrated.

## Not built (deliberately out of scope)

- **Authorization of who may approve.** StrataOps' Incidents module and policy own it. Strata records the reviewer's identity and doesn't judge it.
- **Patch-style overrides** (JSON Pointer, merge patch) inside a field's value. An override replaces a top-level field.
- **Refusing override keys the state doesn't have.** See "Semantics".
- **The decision on `ToolCallContext`.** A tool that requires an approved decision is a natural SG-22 middleware. `ToolCallContext` is open for extension, so adding it later isn't breaking.
- **A child inheriting its parent's decision.** Pass it through `input`.
- **Persisting the reject route,** and graph identity in checkpoints (SG-13).
- **Exporting interrupt boundaries themselves** in the graph definition. Only the reject edge is exported. Boundaries as node properties belong with SG-09's node metadata.
- **A record of the answer on the `resumed_at` stamp row.** See "Persistence".
- **A typed accessor** on `HumanDecision` (`Get<T>(key)`).
