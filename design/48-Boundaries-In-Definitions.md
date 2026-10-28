# Datalium Strata: Interrupt Boundaries in Exported Definitions

An exported definition (`ToDefinition()` / `to_definition()`) describes a graph's structure: its nodes, their kinds, what a parallel group, subgraph or map holds, what a wait node waits for, and every edge, including the synthetic routes (`on_failure`, `on_reject`, `on_timeout`). It doesn't say which nodes are **interrupt boundaries**. A boundary's routes are there, as guarded `on_reject` and `on_timeout` edges, but the boundary's own node is exported as a plain node, so nothing in the file says that the run freezes after it, and a boundary's **timeout** (design/29) isn't anywhere in it. A wait node carries its timeout (`wait.timeout_ms`); a boundary's is lost.

design/29 chose this (review decision 15: "Boundaries themselves aren't exported, so neither is their timeout") and listed "A boundary's timeout in the exported definition" as not built. StrataOps now needs it:

- It pins a published graph by its exported definition: the snapshots in `config/graphs` and its CI graph gate. Remediation v3's 30-minute approval timeout isn't in v3's snapshot, so changing it wouldn't change the published definition. Its interim guard is a structure test asserting `RemediationGraphV3.ApprovalTimeout` beside the snapshot.
- Its console draws the graph from the export, and can't show which node waits for a human, or for how long.

This document closes the gap: **SG-37** (the SG numbers refer to StrataOps' gap log, not public yet). It lands in both engines and the CLI at one commit (strict parity, StrataOps ADR 0003), with shared fixtures read by both.

It was written as a blueprint before implementation, agreed with Nick on 2026-10-22 as written, and is now built. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| The shape | An optional node property **`interrupt`**, an object with one key, **`timeout_ms`** (an integer, or `null` for a boundary without a timeout). Written for every interrupt boundary that is a node at its level, and for nothing else. Every key always written, like `wait` and `join`. |
| `kind` or a property | **A property beside `kind`.** A boundary is a node with a handler that runs, and the freeze comes after it, so it can be any kind but a wait: a plain node, a parallel group, a subgraph or a map node can each be a boundary at its own level. A wait node already freezes and can't also be a boundary (`.compile()` refuses it), so `interrupt` beside `kind: "wait"` is a shape error. |
| The version | A level with an `interrupt` node is definition **`schema_version` `1.6.0`**, a superset of `1.5.0`. The rule stays "the lowest version that can express the level": `1.6.0` when a node at this level has `interrupt`, else `1.5.0` when the level has an `identity`, else as before. A nested `graph` carries its own. |
| Compatibility | **Every level with a boundary changes**: it gains `interrupt` and moves to `1.6.0`. Every level without one is **byte for byte as before**. Older readers refuse `1.6.0`; every earlier definition stays valid and is read as before. |
| What isn't exported | The reject route and the timeout route stay edges (`on_reject`, `on_timeout`), as now: `interrupt` doesn't repeat them. A boundary name that isn't a node is still not exported (it never triggers); a route on one is still a dangling edge. |
| A map's template | **`STRATA_ERR_BOUNDARY_IN_MAP`**, a new code in the shared validator, for every `interrupt` node in a map's template at any depth, named by its qualified path (`Investigate/*/Assess`), beside `STRATA_ERR_WAIT_IN_MAP`. `.compile()` already refuses such a child; the validator can now see it. |
| A timeout with no route | **Not an error.** It ends the thread `timed_out` (design/29, review decision 7), on purpose. `graph view` says which: `times out after 30m to Escalate` or `times out after 30m, then the thread ends`. |
| Shape checks | `check_shape` / `ToTopology` refuse (exit 2 in the CLI): `interrupt` below `1.6.0`; `interrupt` on a wait node; a negative `timeout_ms` (.NET; Rust's `u64` already refuses it); and, **at a `1.6.0` level only**, an `on_reject` edge from a node without `interrupt`, or an `on_timeout` edge from a node with neither `interrupt.timeout_ms` nor `wait.timeout_ms`. The engines never write any of these. An edge from a name that isn't a node is left to the validator (a dangling edge), as now. |
| The CLI | `graph validate` reads `1.6.0` and reports `STRATA_ERR_BOUNDARY_IN_MAP`. `graph view` annotates each boundary: `Approve: interrupt boundary, rejects to Abort, times out after 30m to Escalate`; a composite node that is one gets `; interrupt boundary…` on its own line. |
| API | **None new.** `ToDefinition()` / `to_definition()` on a builder and on a compiled graph, and `ToTopology()` / `GraphTopology` in .NET, carry it. .NET `GraphNodeDetail.Interrupt` (`GraphInterrupt(long? TimeoutMs)`, milliseconds like `GraphWait`); Rust `GraphNodeDef.interrupt: Option<GraphInterruptDef { timeout_ms: Option<u64> }>`. |
| Checkpoints, events, telemetry | **Unchanged.** |

---

## Usage

```csharp
var remediation = new StateGraph<Remediation>()
    .AddNode("Plan", Plan)
    .AddNode("Approve", PresentPlan)
    .AddNode("Apply", Apply)
    .AddNode("Abort", Abort)
    .AddNode("Escalate", Escalate)
    .AddEdge("Plan", "Approve")
    .AddEdge("Approve", "Apply")
    .AddInterruptBoundary("Approve", onReject: "Abort",
        timeout: TimeSpan.FromMinutes(30), onTimeout: "Escalate")
    .SetEntryPoint("Plan")
    .Compile(new CompilationOptions { GraphId = "remediation", GraphVersion = "3" });

var definition = remediation.ToDefinition();   // Datalium.Strata.Storage.Schema
```

```rust
let remediation = StateGraph::<Remediation>::new()
    // …
    .add_interrupt_boundary_with(
        "Approve",
        BoundaryOptions::new()
            .with_reject("Abort")
            .with_timeout(Duration::from_secs(30 * 60))
            .with_on_timeout("Escalate"),
    )
    .compile_with(CompileOptions::new().with_graph_id("remediation").with_graph_version("3"))?;

let definition = remediation.to_definition();
```

Both export:

```json
{
  "schema_version": "1.6.0",
  "identity": { "id": "remediation", "version": "3" },
  "entry_point": "Plan",
  "nodes": [
    { "name": "Plan", "metadata": null },
    { "name": "Approve", "metadata": null, "interrupt": { "timeout_ms": 1800000 } },
    { "name": "Apply", "metadata": null },
    { "name": "Abort", "metadata": null },
    { "name": "Escalate", "metadata": null }
  ],
  "edges": [
    { "from": "Plan", "to": "Approve", "guarded": false, "condition": null },
    { "from": "Approve", "to": "Apply", "guarded": false, "condition": null },
    { "from": "Approve", "to": "Abort", "guarded": true, "condition": "on_reject" },
    { "from": "Approve", "to": "Escalate", "guarded": true, "condition": "on_timeout" }
  ]
}
```

A boundary without a timeout is `"interrupt": { "timeout_ms": null }`.

```
$ strata graph view --path remediation.json
Graph: remediation version 3
…
Approve: interrupt boundary, rejects to Abort, times out after 30m to Escalate
```

---

## The API

Nothing to call that isn't there today. What changes is what the existing exports carry.

### .NET

- `Datalium.Strata.Graph.GraphNodeDetail` gains **`Interrupt`** (`GraphInterrupt?`), and a new `public sealed record GraphInterrupt(long? TimeoutMs)`, in milliseconds like `GraphWait`. `StateGraph.ToTopology()` and `CompiledGraph.ToTopology()` set it for every boundary that is a node and isn't a wait node, so a subgraph's or map's child topology (taken by `AddSubgraph` / `AddMap` from `child.ToTopology()`) carries its own. A boundary on a plain node with no metadata gains a `Details` entry it didn't have.
- `Datalium.Strata.Storage.Schema.GraphNodeDefinition` gains **`Interrupt`** (`GraphInterruptDefinition?`, `[JsonPropertyName("interrupt")]`, omitted when null) with `TimeoutMs` (`long?`, `timeout_ms`, always written). `GraphDefinitionExport.ToDefinition` copies it (the builder truncated the timeout to whole milliseconds, as `wait.timeout_ms` is), and computes the level's version. `GraphDefinition.BoundaryVersion = "1.6.0"`, and an internal `GraphDefinition.IsAtLeast(version, least)` the shape checks use.
- `GraphDefinition.ToTopology()` maps it back and throws `FormatException` for the shape errors below.

### Rust

- `schema::GraphNodeDef` gains **`interrupt: Option<GraphInterruptDef>`** (`#[serde(default, skip_serializing_if = "Option::is_none")]`), and `pub struct GraphInterruptDef { pub timeout_ms: Option<u64> }`, always serialized with its key. Struct literals of `GraphNodeDef` already use `..Default::default()`.
- `schema::BOUNDARY_GRAPH_SCHEMA_VERSION = "1.6.0"`; `is_supported_graph_version` and `graph_version_of` know it.
- `StateGraph::to_definition` and `CompiledGraph::to_definition` write it: the builder from `interrupt_boundaries` and `boundary_timeouts`, the compiled graph from the same sets it keeps. `CompiledGraph` keeps `boundary_timeouts` already (the freeze reads it).
- `GraphDefinition::check_shape` refuses the shape errors below.
- `validate::ValidationErrorKind::BoundaryInMap`.

---

## Semantics

### What is written

- **Which nodes.** Every name marked a boundary (`AddInterruptBoundary` / `add_interrupt_boundary*`) that is a node at that level and isn't a wait node. A boundary name that isn't a node never triggers and isn't written (a route on it is still exported as a dangling edge, and the validator still reports it). A name both a wait and a boundary is refused by `.compile()`; a builder's export leaves `interrupt` off the wait node, so it never writes a shape the CLI refuses.
- **The timeout** in whole milliseconds, truncated, like `wait.timeout_ms` (`.compile()` refuses a zero timeout); `null` without one.
- **Where in the node:** after `metadata` and the kind's own fields, last (`name`, `metadata`, `kind`, `branches` | `graph`, `join`, `wait`, `interrupt`). Rust's field order and .NET's property order give the same bytes.
- **The version** per level, by the rule above. A subgraph node that is a boundary puts `interrupt` on the parent's level (the parent freezes after the child completes), and so moves the parent to `1.6.0`; a boundary inside the child moves the child's level. In `graph-export/hierarchy.json` the child `Remediate` has the boundary `Approve`: the child's level becomes `1.6.0`, the outermost stays `1.1.0`.

### The version rule

| A level with … | `schema_version` |
|---|---|
| an `interrupt` node | `1.6.0` |
| else an `identity` | `1.5.0` |
| else a wait node | `1.4.0` |
| else a map | `1.3.0` |
| else a group with a `join` | `1.2.0` |
| else a node with a `kind` | `1.1.0` |
| else | `1.0.0` |

Each version is a superset of the one before, so a `1.6.0` level may hold everything earlier ones may, and a level with only a boundary sitting "above" identity's `1.5.0` is just the lowest version that can say it.

### Validation

- **`STRATA_ERR_BOUNDARY_IN_MAP`**: `Interrupt boundary 'Investigate/*/Assess' is inside a map's template; a map item can't freeze.`, one per `interrupt` node of every level inside a map's template, in node order, reported where `STRATA_ERR_WAIT_IN_MAP` is (a level's own errors, after its group errors; wait errors then boundary errors). `node` is the qualified path.
- Nothing else is new. A timeout without a route is valid: the thread ends `timed_out`. An `interrupt` node with no edges in or out is judged like any node (orphan, cycles).

### Shape checks

`check_shape` (Rust, the CLI) and `ToTopology` (.NET) refuse, naming the node by its qualified path:

| Refused | Message |
|---|---|
| `interrupt` at a level below `1.6.0` | `node 'X' has an interrupt, which schema_version 1.5.0 doesn't have` |
| `interrupt` on a wait node | `node 'X' is a wait and can't also have an interrupt` |
| a negative `timeout_ms` (.NET) | `node 'X' has an interrupt with a negative timeout_ms` |
| at `1.6.0`, an `on_reject` edge from a node without `interrupt` | `node 'X' has an on_reject edge but is not an interrupt boundary` |
| at `1.6.0`, an `on_timeout` edge from a node without a timeout | `node 'X' has an on_timeout edge but no timeout` |

The two route checks apply only at `1.6.0` levels: before it, a boundary had no `interrupt`, so an `on_reject` edge from an unmarked node was what every engine wrote, and must stay readable. An edge whose `from` isn't a node is left to the validator.

---

## Wire format

### Definitions (`1.6.0`)

`schema/graph-definition.schema.json`:

- `schema_version` gains `"1.6.0"`; its description and the `if`/`then` chain at the end put a level with an `interrupt` node first.
- A node's **`interrupt`**: `{ "type": "object", "additionalProperties": false, "required": ["timeout_ms"], "properties": { "timeout_ms": { "type": ["integer", "null"], "minimum": 0 } } }`.
- The node's `allOf` gains: `interrupt` with `kind: "wait"` is refused.
- The `on_reject` / `on_timeout` edge rules aren't expressible in JSON Schema without a cross-reference; they are the readers' shape checks.

Checkpoints are unchanged: a boundary's timeout is already on the checkpoint (`wait` of kind `human`, `1.5.0`).

### Fixtures

- `graph-export/reject-route.json` (`1.0.0` → `1.6.0`, `Approve` gains `interrupt` with no timeout), `graph-export/waits.json` (`1.4.0` → `1.6.0`, `Approve` with its timeout, which the fixture's builder already sets), `graph-export/hierarchy.json` (its child level `1.1.0` → `1.6.0`). They are what both engines export, so they change with the export.
- New `graph-export/boundaries.json`: a boundary on a plain node with a timeout and both routes, a parallel group that is a boundary with no timeout, a subgraph node that is one with a timeout and no route (its child has its own, so the child is `1.6.0` too), a map node that is one with a timeout, and an identity at the top.
- New `graph-validation/boundary-in-map.json`: a boundary at the top level (allowed), and in a map's template a boundary, a wait node and a subgraph whose child has a boundary: `STRATA_ERR_WAIT_IN_MAP` (`Fan/*/Window`), then `STRATA_ERR_BOUNDARY_IN_MAP` twice (`Fan/*/Read`, `Fan/*/Inner/Sign`).
- The shape cases are inline in each engine's tests and the CLI's, edits of the fixtures above (one case per row of the table).
- The `graph-validation` fixtures with `on_reject` edges and no `interrupt` (`valid-hierarchy.json`) stay as they are: they are earlier versions, and prove they still read.

---

## The CLI

- `graph validate` reads `1.6.0` definitions (through `strata-core`), and reports `STRATA_ERR_BOUNDARY_IN_MAP` with exit 1 like any validation error. Shape errors exit 2.
- `graph view` gives each boundary a line, in node order, among the composite and wait lines it prints now:
  - a plain node: `Approve: interrupt boundary`, then `, rejects to Abort` when it has an `on_reject` edge, then `, times out after 30m to Escalate` / `, times out after 30m, then the thread ends` when it has a timeout;
  - a composite node that is a boundary: its existing line, then `; interrupt boundary…` with the same suffixes.
- `--json` prints the definition as read, `interrupt` included.
- Nothing else changes. `thread inspect` already prints a waiting boundary's `Times out at:`.

---

## Compatibility and migration

- **Exports change only where a boundary is.** A graph with a boundary exports `interrupt` and a `1.6.0` level (the outermost, or a child's, wherever the boundary is). Every other level is byte for byte as before.
- **Readers.** A CLI or runtime from before this refuses a `1.6.0` file (`unsupported schema_version '1.6.0'`, exit 2). Upgrade the CLI that gates definitions before exporting from the new runtime (`schema/README.md`, "Compatibility").
- **StrataOps.** On the pin bump, its snapshots of graphs with boundaries change (remediation v3 gains `"interrupt": { "timeout_ms": 1800000 }` on its approval node), which is the point; its `GraphStructureTests` assertion can go. The demo repos' exported definitions change the same way when they move their pin.
- **No checkpoint, store, event or API break.** The new fields are optional; Rust code that builds `GraphNodeDef` with `..Default::default()` keeps compiling, and so does .NET code building `GraphNodeDetail` with an initializer.

---

## Tests

Both engines, the same cases:

- **Export:** the three changed fixtures and `boundaries.json` are exactly what the builder and the compiled graph export, and round-trip (read, write, same bytes). A graph with no boundary exports as before (the existing fixtures that don't change prove it).
- **Version:** a boundary-only level is `1.6.0`; with an identity too, `1.6.0`; a boundary in a child moves only the child.
- **What is written:** a boundary name that isn't a node isn't written (its route is still a dangling edge); a timeout is written in whole milliseconds; a re-marked boundary without a timeout writes `null`; a group, a subgraph node and a map node that are boundaries carry it.
- **Validation:** `boundary-in-map.json` gives `STRATA_ERR_BOUNDARY_IN_MAP` twice, in both validators and the CLI, after any `STRATA_ERR_WAIT_IN_MAP` at the same level.
- **Shape:** one case per row of the table, in `check_shape` and `ToTopology`; a `1.5.0` file with `on_reject` from an unmarked node is still accepted.
- **CLI:** `graph validate` on `boundaries.json` (valid) and `boundary-in-map.json` (exit 1); `graph view`'s boundary lines for each suffix; an old CLI's refusal is the existing unknown-version test.
- The graph-export gate test (`cli/tests/graph_export_gate.rs`) and the .NET export tests run over the changed fixtures.

---

## Review decisions

### The questions as asked (the handoff brief)

1. **The version.** **`1.6.0`** for a level with an `interrupt` node, above identity's `1.5.0`. The rule is still "the lowest version that can express the level", and each version is a superset, so a boundary-only level at `1.6.0` reads cleanly.
2. **Compatibility.** Every level with a boundary changes bytes and version; everything else is byte for byte as before. Older readers refuse `1.6.0`, which the compatibility policy already expects. StrataOps' snapshots change on its pin bump, intended.
3. **`kind` or a property.** **A property**, because a boundary is a node with a handler of any kind but a wait. A group, a subgraph node and a map node may each be a boundary (the engine allows it today, and tests cover a group and a subgraph node). A wait node can't be (`.compile()` refuses it; the shape check refuses it). A map's template can't have one: **`STRATA_ERR_BOUNDARY_IN_MAP`**, mirroring `STRATA_ERR_WAIT_IN_MAP`.
4. **Validation.** **No error for a timeout without a route**: it ends the thread on purpose. The one new validation code is the map one. Shape checks refuse what no engine writes, the two route checks only at `1.6.0`, so earlier files stay readable.
5. **`graph view`.** One line per boundary, saying its routes and whether a timeout routes or ends the thread, which is the "on purpose" check the gaps doc asked for, made visible rather than enforced.

### Added by this document

6. **`timeout_ms` always written, `null` without a timeout**, not left out as the gaps doc sketched. `wait` and `join` always write every key; one rule for every object in the definition keeps readers simple and the shape strict. `"interrupt": { "timeout_ms": null }` is a boundary with no timeout.
7. **The routes aren't repeated in `interrupt`.** They are edges, which the validator already walks; a second copy could disagree with them.
8. **A boundary name that isn't a node stays unexported**, since it never triggers. Its route (if any) is still a dangling edge, as today.
9. **Truncated milliseconds**, as `wait.timeout_ms`. A sub-millisecond timeout exports as `0`; the schema allows `0` for that reason, and nobody sets one.

---

## Implementation Status

Built 2026-10-22 in both engines and the CLI, as written, on the branch `sg-37-boundaries-in-definitions`.

- **Rust:** `schema.rs` (`GraphInterruptDef`, `BOUNDARY_GRAPH_SCHEMA_VERSION`, `graph_version_of`, `check_shape` rewritten to compare versions by rank through `graph_version_rank`, which also makes an `identity` legal at every version from `1.5.0` up, and `check_routes`), `graph.rs` (`node_definition` writes `interrupt`, so both the builder's export and the compiled graph's stored definition carry it), `validate.rs` (`BoundaryInMap`). Tests: `tests/boundary_definitions.rs` (14); `graph_structure.rs` and `event_waits.rs` adjusted for the changed fixtures.
- **.NET:** `GraphTopology.cs` (`GraphInterrupt`, `GraphNodeDetail.Interrupt`), `StateGraph.cs` (`DetailOf` adds it over `KindDetailOf`), `GraphValidator.cs` / `ValidationError.cs` (`BoundaryInMap`), Storage `GraphDefinition.cs` (`GraphInterruptDefinition`, `BoundaryVersion`, `IsAtLeast`) and `GraphDefinitionExport.cs` (export, `CheckInterruptFits`, `CheckRoutes`, the version checks through `IsAtLeast`). Tests: `BoundaryDefinitionTests.cs` (14, the Rust suite's twins); `GraphStructureTests.cs` and `EventWaitTests.cs` adjusted.
- **CLI:** `commands/graph.rs` (`composite_nodes` over `kind_annotation` and `boundary_annotation`); `tests/graph_commands.rs` gained four tests (validate the fixture, `STRATA_ERR_BOUNDARY_IN_MAP`, the shape refusals with exit 2, the view lines).
- **Schema:** `graph-definition.schema.json` (`1.6.0`, `interrupt`, no `interrupt` on a wait node, `1.6.0` first in the version chain). Every definition fixture validates against it.
- **The unknown-version message** now lists seven versions (`…, 1.5.0 and 1.6.0`), in both engines.
- **Version numbers are per format:** definition `1.6.0` (boundaries) is unrelated to checkpoint `1.6.0` (graph identity), as definition `1.5.0` (identity) already was to checkpoint `1.5.0` (waits).

---

## Not built (deliberately out of scope)

- **A boundary's reviewer, role or policy** in the definition: who may answer is the application's (StrataOps' plan policy), not the engine's. Node metadata can describe it.
- **Validating a timeout against a route.** A timeout without a route is valid; `graph view` shows which way it goes.
- **A boundary name that isn't a node** in the export.
- **Run-time options in the definition** (a node's timeout, retry counts, concurrency limits) remain unexported, as design/22 and design/26 decided.
