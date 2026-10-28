# Datalium Strata: Graph Structure in Definitions

The exported graph definition (`schema/graph-definition.schema.json`, `to_definition()` / `ToDefinition()`, read by `strata graph validate`) describes a graph one level deep, and only by name:

- **Groups and subgraphs are opaque.** A parallel group's branches are closures (`design/14`, "Not built"), and a subgraph's child graph is invisible to its parent's definition (`design/15`, "Not built"). Neither reaches the export, so neither reaches `strata graph validate`.
- **Node metadata is unused.** The schema has a nullable string-to-string `metadata` map on every node, but neither engine can set it. .NET's `GraphTopology` has no slot for it, and Rust's `GraphNodeDef.metadata` is always `None`.
- **So a product that draws a graph keeps its own copy.** StrataOps' console needs a group's branches and a subgraph's nodes to draw a live graph. Today it keeps a parallel description of every graph (`GraphCatalog`), which can drift from the code.

This document specifies SG-09, which closes all three. It also closes the rest of SG-31: `strata graph validate` checks group and subgraph internals. The SG numbers refer to the gap log of StrataOps (not public yet). Both engines get the capability in one change (strict parity, StrataOps ADR 0003), each in its own idiom.

Run events already describe branches and subgraph nodes as they run (`BranchStarted`, `NodePath` `"Gather/Acme"`, SG-20, `design/18`). What is missing is the **static** structure those paths belong to.

It was reviewed and approved, and is built in both engines and the CLI (2026-10-04). "Review decisions" records what the review settled, and "Implementation Status" records where the build diverges from the blueprint.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Node metadata API | A per-node setter in both engines, beside `SetRecoveryPolicy` / `SetTimeout`: **`SetNodeMetadata(node, metadata)`** / **`set_node_metadata(node, metadata)`**. .NET also takes an optional **`metadata:`** parameter on every `Add*` method, as the handoff sketched. Rust has no optional parameters, so it has the setter only. |
| Branch metadata | **`ParallelBranches.Branch(name, handler, metadata: ...)`** (every overload) plus `SetMetadata(branch, metadata)` in .NET; **`ParallelBranches::set_metadata(branch, metadata)`** in Rust. |
| Metadata rules | String to string, as the schema already says. Keys are non-empty, and the **`strata.` prefix is reserved** for the engine, as for run metadata (`design/18`). Metadata on a name that isn't a node (or branch) fails `.compile()`. Exported with **keys sorted**, in both engines. |
| Where it's exported | The existing `metadata` field of each node, and a new one on each branch. |
| Hierarchy shape | A node gains an optional **`kind`**: `"parallel_group"` or `"subgraph"`. A group carries **`branches`** (each `name` + `metadata`). A subgraph carries **`graph`**: the child's **whole definition**, recursively, so a subgraph inside a subgraph is described too. A plain node has none of the three. |
| Room for map (SG-06) | A future `kind: "map"` would reuse `graph` for its per-item template. Nothing about it is designed here. Because `kind` is an enum, adding a value is a new `schema_version`, so a reader that doesn't know it refuses the file rather than misreading it. |
| `schema_version` | **`1.1.0`** for a definition with a group or subgraph node at its own level. Every other definition is **`1.0.0`** and serializes byte for byte as today, so the existing export fixture is untouched. A nested `graph` carries its own version, by the same rule. The schema accepts both versions. |
| A `1.0.0` reader | `strata graph validate` (and `view`) now **refuses a `schema_version` it doesn't know** (exit `2`). An older CLI has no such check. It would validate a `1.1.0` file's top level and ignore the children, which is today's behaviour, so nothing gets worse. |
| Validating children | The shared validator (Rust `validate`, .NET `GraphValidator.Validate`) **recurses**. Each child graph is validated as a graph in its own right, and each group must have at least one branch, with unique names. Child errors name nodes by their **qualified path** (`Remediate/Plan`), the same path run events use. There are two new error codes: `STRATA_ERR_EMPTY_GROUP` and `STRATA_ERR_DUPLICATE_BRANCH`. |
| Malformed hierarchy in a file | A `kind` without its field (or a field without its `kind`) is not a graph definition: the CLI refuses the file with exit `2`, like a missing `edges`. The engines can't produce one. |
| Paths line up with events | Every `NodePath` a run reports (`Investigate/Kubernetes`, `Remediate/Plan`, `Remediate/Fix/Apply`) resolves in the exported definition, one segment per level. Tests pin this, because it's what a live console relies on. |
| Rust `metadata` type | `GraphNodeDef.metadata` becomes `Option<BTreeMap<String, String>>`, so exported files don't reorder keys from run to run (a small breaking change, see "Migration"). |
| Checkpoints | **No change.** Structure belongs to the graph, not to a run. Graph identity and version in checkpoints is SG-13. |
| Not in scope | Branches as real graph nodes, dynamic fan-out (SG-06), group controls (SG-07), graph identity (SG-13), per-branch lifecycle events (already built by SG-20), exporting interrupt boundaries or node kinds (fallible, streaming), and metadata on `NodeContext`, spans or events. |

---

## Usage

```csharp
var investigate = new ParallelBranches<Incident>()
    .Branch("Kubernetes", K8sAsync, metadata: Agent("kubernetes-investigator"))
    .Branch("Prometheus", MetricsAsync, metadata: Agent("metrics-investigator"))
    .Branch("GitHub", ChangesAsync, metadata: Agent("change-investigator"));

var incident = new StateGraph<Incident>()
    .AddNode("Triage", TriageAsync, metadata: new Dictionary<string, string>
    {
        ["agent"] = "triage",
        ["model_policy"] = "ops-reasoning-standard",
    })
    .AddParallelGroup("Investigate", investigate, MergeEvidence)
    .AddSubgraph("Remediate", remediation, ToPlan, FromPlan, metadata: Agent("remediator"))
    .AddEdge("Triage", "Investigate")
    .AddEdge("Investigate", "Remediate");

// The console's live graph, and the CI gate, both read this.
File.WriteAllText("incident.json", JsonSerializer.Serialize(incident.ToDefinition()));

static Dictionary<string, string> Agent(string name) => new() { ["agent"] = name };
```

```rust
let investigate = ParallelBranches::new()
    .async_branch("Kubernetes", k8s)
    .async_branch("Prometheus", metrics)
    .async_branch("GitHub", changes)
    .set_metadata("Kubernetes", [("agent", "kubernetes-investigator")])
    .set_metadata("Prometheus", [("agent", "metrics-investigator")])
    .set_metadata("GitHub", [("agent", "change-investigator")]);

let incident = StateGraph::<Incident>::new()
    .add_async_node("Triage", triage)
    .add_parallel_group("Investigate", investigate, merge_evidence)
    .add_subgraph("Remediate", remediation, to_plan, from_plan)
    .add_edge("Triage", "Investigate")
    .add_edge("Investigate", "Remediate")
    .set_node_metadata("Triage", [("agent", "triage"), ("model_policy", "ops-reasoning-standard")])
    .set_node_metadata("Remediate", [("agent", "remediator")]);

std::fs::write("incident.json", serde_json::to_string_pretty(&incident.to_definition())?)?;
```

Either engine writes:

```jsonc
{
  "schema_version": "1.1.0",
  "entry_point": "Triage",
  "nodes": [
    { "name": "Triage", "metadata": { "agent": "triage", "model_policy": "ops-reasoning-standard" } },
    {
      "name": "Investigate",
      "metadata": null,
      "kind": "parallel_group",
      "branches": [
        { "name": "Kubernetes", "metadata": { "agent": "kubernetes-investigator" } },
        { "name": "Prometheus", "metadata": { "agent": "metrics-investigator" } },
        { "name": "GitHub", "metadata": { "agent": "change-investigator" } }
      ]
    },
    {
      "name": "Remediate",
      "metadata": { "agent": "remediator" },
      "kind": "subgraph",
      "graph": {
        "schema_version": "1.0.0",            // the child has no group or subgraph of its own
        "entry_point": "Plan",
        "nodes": [ { "name": "Plan", "metadata": null }, { "name": "Approve", "metadata": null }, ... ],
        "edges": [ ..., { "from": "Approve", "to": "Abort", "guarded": true, "condition": "on_reject" } ]
      }
    }
  ],
  "edges": [
    { "from": "Triage", "to": "Investigate", "guarded": false, "condition": null },
    { "from": "Investigate", "to": "Remediate", "guarded": false, "condition": null }
  ]
}
```

---

## Node metadata

### API

```csharp
// StateGraph<TState>
public StateGraph<TState> SetNodeMetadata(string node, IReadOnlyDictionary<string, string> metadata);

// ...and an optional last parameter on every adder, which calls it:
public StateGraph<TState> AddNode(string name, Func<TState, TState> handler, IReadOnlyDictionary<string, string>? metadata = null);
// likewise both AddNode overloads, both AddFallibleNode, both AddStreamingNode, AddParallelGroup, AddSubgraph

// ParallelBranches<TState>
public ParallelBranches<TState> Branch(string name, Func<TState, TState> handler, IReadOnlyDictionary<string, string>? metadata = null);
// likewise the async Branch and both StreamingBranch overloads
public ParallelBranches<TState> SetMetadata(string branch, IReadOnlyDictionary<string, string> metadata);
```

```rust
// StateGraph<S>
pub fn set_node_metadata<K, V>(self, node: impl Into<String>, metadata: impl IntoIterator<Item = (K, V)>) -> Self
where K: Into<String>, V: Into<String>;

// ParallelBranches<S>
pub fn set_metadata<K, V>(self, branch: impl Into<String>, metadata: impl IntoIterator<Item = (K, V)>) -> Self
where K: Into<String>, V: Into<String>;
```

A setter is the codebase's idiom for per-node configuration in both engines (`SetRecoveryPolicy`, `SetTimeout`, `set_recovery_policy`, `set_timeout`). It works for every node kind with one method, and needs no change to any existing Rust signature. The .NET `metadata:` parameter is the shape the handoff sketched, and reads well at the call site. It is sugar for the setter, not a second mechanism.

### Rules

| Aspect | Behaviour |
|---|---|
| **Values** | Strings, as the schema has always said. Structured data belongs in your own configuration, keyed by the node name or a metadata value. |
| **Replace, not merge** | Setting metadata again for a node replaces it. An `Add*` call without `metadata:` leaves what's there, so re-adding a node name (which replaces its handler) keeps its metadata. |
| **Keys** | Non-empty. A key starting `strata.` is refused when the graph compiles (`metadata key 'strata.x' on node 'Triage' is reserved`), leaving the prefix free for engine keys later, as with run metadata. |
| **Unknown names** | Metadata for a name that isn't a node fails `.compile()` (`metadata set for 'Ghost', which does not exist`), as a timeout does. Metadata for a branch the group doesn't have fails `.compile()` as well. `to_definition()` / `ToDefinition()` doesn't validate: metadata is attached only to nodes and branches that exist, so a stray entry is simply left out of the export. |
| **Order** | Keys are written sorted, in both engines, so the same graph exports the same bytes every time, and a CI diff of exported files shows only real changes. |
| **What it's for** | Description only. The engine doesn't read it. It doesn't reach `NodeContext`, spans or run events (see "Not built"). |

---

## Hierarchy in the definition

### Wire format

`schema/graph-definition.schema.json`, node object:

| Field | Type | When |
|---|---|---|
| `name` | string | always, as today |
| `metadata` | object of strings, or `null` | always, as today; now settable |
| `kind` | `"parallel_group"` \| `"subgraph"` | **absent** for a plain node; present for the two composite kinds |
| `branches` | array of `{ "name": string, "metadata": object \| null }` | exactly when `kind` is `"parallel_group"`, in declaration order |
| `graph` | a graph definition (`$ref: "#"`) | exactly when `kind` is `"subgraph"` |

- **Absent, not null.** A plain node writes no `kind`, `branches` or `graph`, so a graph with no group or subgraph writes exactly what it writes today.
- **Why a whole definition under `graph`.** It carries `schema_version`, `entry_point`, `nodes` and `edges`, and nests further, so a child can be cut out and validated or drawn on its own. It also has the same shape as a nested checkpoint (`design/16`), so readers already know the pattern.
- **Why not one generic `children` field.** The gap suggested `children`. A group's children are branches (a name and metadata, nothing to route), while a subgraph's child is a graph with an entry point and edges. One field holding either would push every reader into a type switch on the contents. `kind` is that switch, explicit, and it leaves `graph` free for a map's template later.
- **The edges a child exports** are whatever its own `to_definition()` gives: ordinary edges, then `on_failure`, then `on_reject` (`design/21`). Group branches have no edges: they all start when the group starts.
- **Versions.** `schema_version` is `"1.1.0"` when a node *at that level* has a `kind`, and `"1.0.0"` otherwise. A nested `graph` computes its own. The schema's `schema_version` becomes `enum ["1.0.0", "1.1.0"]`, with `1.1.0` required when a node has `kind`. `1.0.0` files stay valid under the new schema. This is the checkpoint rule (`design/16`, `design/19`, `design/21`): a version is written only when it's needed.

### Paths

A node path in a run event or stream chunk names one node per level: `Investigate/Kubernetes` (the branch `Kubernetes` of the group `Investigate`), `Remediate/Plan` (the node `Plan` of the subgraph `Remediate`'s child), `Remediate/Fix/Apply` (two levels down). Each segment resolves in the definition: look the first segment up in `nodes`, then descend into its `branches` or `graph` with the rest. That's what lets a console light up the right box when `BranchStarted` arrives. Tests pin it in both engines: every `NodeStarted` and `BranchStarted` path in a run of the reference graph resolves in that graph's export.

### How the engines hold it

- **.NET.** `GraphTopology` (core, `Graph/GraphTopology.cs`) keeps its positional shape and gains an `init` property, `Details: IReadOnlyDictionary<string, GraphNodeDetail>`. A detail holds the node's metadata, its kind, a group's branches (`GraphBranch(Name, Metadata)`), and a subgraph's child `GraphTopology`. Positional construction and `Nodes` are unchanged, so the validator's existing passes and every existing caller keep working. `AddSubgraph` snapshots `child.ToTopology()` when it's called: a compiled graph can't change, so the snapshot is exact. `Datalium.Strata.Storage`'s `GraphNodeDefinition` gains `Kind`, `Branches` and `Graph` (`JsonIgnore` when null). `GraphDefinitionExport` maps both ways: `ToDefinition()` as today, and a new `GraphDefinition.ToTopology()`, so a .NET program can validate a definition file in-process with the same validator `.Compile()` uses.
- **Rust.** `GraphNodeDef` gains `kind: Option<GraphNodeKind>`, `branches: Option<Vec<GraphBranchDef>>` and `graph: Option<Box<GraphDefinition>>`, each `#[serde(default, skip_serializing_if = "Option::is_none")]`. `add_subgraph` snapshots `child.to_definition()`. `parallel_groups` keeps branch names and metadata instead of names only.

---

## Validation

The shared validator recurses, so `.compile()`, `strata graph validate` and the .NET validator stay one function per engine and agree:

1. Validate this level as today: entry point, dangling edges, orphans, unguarded cycles.
2. For each `parallel_group` node: at least one branch (`STRATA_ERR_EMPTY_GROUP`, `node` = the group), and unique branch names (`STRATA_ERR_DUPLICATE_BRANCH`, `node` = the group, `related_node` = the branch).
3. For each `subgraph` node: validate its `graph` as a graph in its own right, recursively. Its errors are reported with every node name qualified by the subgraph path: an orphan `Abort` inside `Remediate` is `node: "Remediate/Abort"`. Codes and kinds are unchanged, so the error shape (`kind`, `code`, `node`, `related_node`, `message`) is the same as today.

Errors come in this order: this level's (as today), then group errors in node order, then each child's in node order.

**In the engines.** A child graph was already validated by its own `.compile()`, so step 3 never finds anything there: it costs one walk of an already-valid child. `.compile()` keeps its existing group checks, which run first with their existing errors (.NET `InvalidOperationException`, Rust `CompilationFailed`), so nothing about compiling changes. Step 2 is what the CLI uses for a hand-written or edited file.

**In the CLI.** `strata graph validate` gets the recursion for free. Directory mode is unchanged: a hierarchical file is one file, valid only if every level is. Human output prints child errors with their qualified names, and `--json` has the same shape. Two read-time checks come before validation, both exit `2` ("not a graph definition"):

- a `schema_version` the CLI doesn't know;
- a node whose `kind` and fields don't match (a `subgraph` without `graph`, `branches` on a node that isn't a group, and so on).

`strata graph view` adds one annotation line per composite node under its diagram: `Investigate: parallel group [Kubernetes, Prometheus, GitHub]`, and `Remediate: subgraph, entry Plan, 5 nodes`. It doesn't draw the child: `--json` prints the whole definition.

---

## Fixtures

- **`graph-export/hierarchy.json`.** Both engines build the same graph and compare their export to it, as for `fallback-pipeline.json`. The graph has node metadata, a parallel group with branch metadata, and a subgraph whose child has a subgraph of its own (and a reject route, so `design/21`'s edge appears at depth). `fallback-pipeline.json` and `reject-route.json` don't change: they stay `1.0.0`.
- **`graph-validation/`.** These are read by both engines' validators (the parity tests) and by the CLI:
  - `valid-hierarchy.json`: passes.
  - `child-orphan.json`: one error, `Remediate/Abort`.
  - `grandchild-unguarded-cycle.json`: qualified two levels down.
  - `empty-group.json`
  - `duplicate-branch.json`
- **`graph-definition.sample.json`** is unchanged, and round-trips under the new types. `hierarchy.json` round-trips through both engines' definition types too.

---

## Migration

- **.NET: source-compatible.**
  - `Add*` methods and `Branch` gain an optional last parameter. That's binary-breaking only, and nothing ships as a binary yet.
  - `GraphTopology` gains an `init` property.
  - `GraphNodeDefinition` gains properties.
- **Rust: two small breaks.**
  - `GraphNodeDef` literals need the three new fields (or `..Default::default()`, which `GraphNodeDef` gains).
  - `GraphNodeDef.metadata` changes from `HashMap` to `BTreeMap`. The engine builds these, so code that constructs one by hand changes `HashMap::from` to `BTreeMap::from`.
- **Behaviour:**
  - `.compile()` now refuses metadata on unknown names and reserved keys. Neither could be set before.
  - The CLI refuses unknown `schema_version`s. Every file it accepted before still says `1.0.0`.
- **Files:** a graph with a group or subgraph now exports `1.1.0` with its children. A CI gate that runs an older `strata` binary over it still passes or fails on the top level as before, but doesn't check the children. StrataOps pins this repo, so its CLI and engine move together.

---

## Tests

Twin suites, `rust/strata-core/tests/graph_structure.rs` and `dotnet/tests/Datalium.Strata.Tests/GraphStructureTests.cs`:

- **Metadata:**
  - On every node kind, by setter (and in .NET by parameter).
  - Replaced, not merged.
  - Kept when a node name is re-added.
  - Keys sorted in the export.
  - Refused at compile for an unknown node, an unknown branch, an empty key, and a `strata.` key.
- **Export:**
  - A plain graph is still `1.0.0` and byte-identical to `fallback-pipeline.json`.
  - A group exports its branches in declaration order with their metadata.
  - A subgraph exports its child whole, recursively, with each level's own version.
  - `hierarchy.json` matches in both engines.
  - A compiled graph exports what its builder did.
- **Paths:** every `NodeStarted` / `BranchStarted` path from a run of the hierarchy graph resolves in its export.
- **Validation:**
  - Recursion, qualified names and error order.
  - The two new codes.
  - The `graph-validation/` fixtures agree across engines.
  - `.compile()` verdicts are unchanged for every existing case.
- **.NET:** `GraphDefinition.ToTopology()` round-trips with `ToDefinition()`.
- **CLI** (`graph_commands.rs`, `graph_export_gate.rs`):
  - A hierarchical file is validated at every level, with qualified errors in human output and `--json`.
  - Directory mode with hierarchical files.
  - An unknown version, or a mismatched `kind`, exits `2`.
  - `view` annotates composite nodes.
- **Mutation checks**, as for design/17 to 21. Each of these should fail tests in both engines:
  - Not recursing into a child.
  - Not qualifying child error names.
  - Writing `1.1.0` for a plain graph.
  - Dropping branch metadata.

---

## Review decisions

Reviewed 2026-10-04. Every recommendation below was accepted as asked.

### The questions as asked

Recommendations are in bold.

1. **Hierarchy shape.** `kind` plus kind-specific fields (`branches`, `graph`), or one generic `children` field as the gap suggested? **`kind` plus fields.**
2. **Versioning.** Bump to `1.1.0` only for a definition that needs it, keep `1.0.0` files byte-identical and valid, and have the CLI refuse unknown versions? **Yes.**
3. **Metadata API.** A setter in both engines plus the .NET `metadata:` parameter as sugar? Or the parameter only in .NET and the setter only in Rust, which is closer to the handoff's sketch but leaves .NET with no way to set metadata on a node after adding it? **Both in .NET, the setter in Rust.**
4. **Rust `metadata` as `BTreeMap`.** It's a small break, in exchange for exports that are deterministic byte for byte. **Yes.**
5. **Exporting interrupt boundaries and node kinds (fallible, streaming) now.** Cheap, and a console could draw approval gates. But it's not asked for, and it would make every graph with a boundary export `1.1.0`. **Not now**: StrataOps already knows its boundaries, and it can be added later with a version bump.
6. **A mismatched `kind` in a file.** A read error (exit `2`), or a new validation code (exit `1`)? **A read error**: the engines can't produce one, so it means a broken file, not a broken graph.

---

## Implementation Status

Built 2026-10-04 in both engines, the CLI, the schema and the fixtures, in one change. Everything above holds as written, except for the points below, which the blueprint left open or which the build settled differently.

### Where the build diverges or fills a gap

- **A level whose entry point is missing reports only that.** As before, the missing-entry error returns alone for its level, and that level's groups and children are not checked either. A child with a missing entry point reports one qualified `STRATA_ERR_MISSING_ENTRY` (`Remediate/Ghost`), and its parent's other errors are still reported.
- **Messages are qualified too, not just `node` and `related_node`.** An orphan inside `Remediate` reads `Node 'Remediate/Abort' is isolated and unreachable from the entry point 'Remediate/Plan'.` Both engines build every message from the qualified names, so the two validators still produce identical errors, messages included. A top-level message is unchanged.
- **One duplicate-branch error per repeated name**, at the branch where it first repeats: `[A, B, A, A, B]` gives one error for `A` and one for `B`.
- **The read-time checks live in the engines, not only the CLI.** Rust has `GraphDefinition::check_shape()` (`schema.rs`), which `strata graph validate` and `view` call after parsing. .NET's `GraphDefinition.ToTopology()` makes the same checks with the same messages and throws `FormatException`, since it can't map a mismatched node into a topology. Both also refuse a `kind` in a level that says `1.0.0`, which the JSON Schema forbids. The error names the node by its qualified path (`node 'Remediate/Fix' is a subgraph but has neither branches nor graph`), and an unknown version names the level (`unsupported schema_version '1.9.0' (in 'Remediate/Fix')`).
- **An unknown `kind` value fails to parse** in both engines (serde's unknown variant, `System.Text.Json`'s `JsonException`), so the CLI reports it as a file it couldn't parse, still exit `2`. That is how a reader refuses a future `kind: "map"`.
- **Version constants.** Rust: `schema::COMPOSITE_GRAPH_SCHEMA_VERSION` and `is_supported_graph_version`. .NET: `GraphDefinition.Version`, `GraphDefinition.CompositeVersion` and `GraphDefinition.IsSupportedVersion` in `Datalium.Strata.Storage.Schema`.
- **One `GraphNodeKind` enum in .NET.** It lives in core (`Datalium.Strata.Graph`, with `JsonStringEnumMemberName` values), and Storage's `GraphNodeDefinition.Kind` reuses it, rather than a second enum in Storage that would clash in any file importing both namespaces. `GraphNodeDetail` is an init-only record (`Metadata`, `Kind`, `Branches`, `Graph`), and `GraphTopology.DetailOf(node)` reads it. Rust's `GraphNodeDef` and `GraphBranchDef` derive `Default`, as the migration note planned.
- **Compiled graphs carry the structure too.** Rust's `CompiledGraph::definition` and .NET's `CompiledGraph<T>.Topology` (the routing views, without fallback or reject edges) now hold node metadata, kinds, branches and children as well. The executor reads only their entry point and edges, so nothing about running changed, and `to_definition()` / `ToTopology()` still add the two kinds of synthetic edge.
- **Metadata rules, in detail.**
  - `.compile()` checks metadata after the existing group, recovery-policy and timeout checks: node metadata in name order (ordinal), then each group's branch metadata, groups in node order, so both engines report the same first error. The messages are `metadata set for 'Ghost', which does not exist`, `metadata set for branch 'Ghost' of parallel group 'G', which does not exist`, `metadata on node 'A' has an empty key`, and `metadata key 'strata.x' on node 'Triage' is reserved` (or `on branch 'B' of parallel group 'G'`).
  - .NET copies the dictionary it is given, so changing it afterwards changes nothing, and a `null` value is refused when it is set (`ArgumentException`); Rust can't express one.
  - An empty map is exported as `{}`, not `null`: it is what was set.
  - Re-adding a group or subgraph name as a plain node drops its branches or child (and so its `kind`) but keeps its metadata, like any other re-add.
- **CLI output.** A duplicate branch's human line names the branch by its path (`[STRATA_ERR_DUPLICATE_BRANCH] Investigate/Kubernetes: ...`), because the usual `node -> related` reads as an edge. `--json` is unchanged. `view`'s annotation says `1 node` for a one-node child. A file with a malformed hierarchy in directory mode counts as unreadable (`unreadable_count`), like any other file that isn't a graph definition.

### Fixtures and tests

- **`graph-export/hierarchy.json`**: the incident graph from "Usage". Its child `Remediate` (`1.1.0`) has `Approve`'s reject route to `Abort` and a subgraph `Fix` (`1.0.0`, `Apply` -> `Verify`, with metadata on `Apply`), and the branch `GitHub` has no metadata (`null`). Both engines export it, compare it, and round-trip it through their definition types; the CLI's `view --json` prints it unchanged.
- **`graph-validation/`**: the five planned fixtures, plus **`errors-at-every-level.json`**, which pins the error order across a level, a group and a child that itself has an orphan and an empty group (`Island`, `G`/`X`, `S/Q`, `S/H`). The parity tests in both engines compare every error's code, node and related node, not just the verdict. .NET's parity tests now load every fixture through `GraphDefinition.ToTopology()`; before, they rebuilt a topology by hand and dropped `condition`.
- **Twin suites** `rust/strata-core/tests/graph_structure.rs` (19 tests) and `dotnet/tests/Datalium.Strata.Tests/GraphStructureTests.cs` (24) cover everything in "Tests". The .NET suite also covers setting metadata by parameter and by setter on all eight adders, and the `ToTopology()` refusals. The path test runs the incident graph to `Approve`'s boundary, resumes it approved, and checks every `NodeStarted` / `BranchStarted` path from both runs, including `Investigate/Kubernetes`, `Remediate/Plan` and `Remediate/Fix/Apply`, against the export.
- **The existing cases** in `graph_export.rs` / `GraphExportTests.cs` gained `group and subgraph`, so `.compile()` and the export agree on a hierarchical graph as well. Every existing case is unchanged.
- **CLI**: `graph_commands.rs` gained 12 tests (qualified errors in human and `--json` output, error order, directory mode with hierarchical files, unknown versions and mismatched kinds for both `validate` and `view`, a malformed file in directory mode, `view`'s annotations), and `graph_export_gate.rs` gained 2 (a hierarchy built in code passes the binary; one edited inside its child fails with `Remediate/Abort`).
- **Schema**: every fixture validates against the new `graph-definition.schema.json` (checked with Python's `jsonschema`, Draft 2020-12), and seven malformed variants are refused: `1.0.0` with a kind, at the top and in a child; an unknown version; a subgraph without `graph`; a plain node with `branches`; a group with `graph`; an unknown `kind`.

### Verification

- Every CI command in `CLAUDE.md` passed with live Postgres and Redis (`docker compose up`, `STRATA_REQUIRE_BACKENDS=1`): `cargo fmt --check`, `cargo clippy --all-targets --all-features -D warnings` and `cargo test --all-features` in `rust/`, `cli/` and `examples/rust-agent/`; `cargo build --release --bin strata`; `dotnet format --verify-no-changes`, `dotnet build -c Release` and `dotnet test -c Release`.
- **Mutation checks**: each of the four mutations, applied to one engine at a time, fails that engine's tests:

  | Mutation | Rust (core + CLI) | .NET |
  |---|---|---|
  | Not recursing into a child | 9 failing | 4 failing |
  | Not qualifying child error names | 9 failing | 4 failing |
  | Writing `1.1.0` for a plain graph | 6 failing | 6 failing |
  | Dropping branch metadata | 3 failing | 3 failing |

## Not built (deliberately out of scope)

- **Branches as graph nodes**, with their own edges, recovery policies or interrupt boundaries (`design/14`).
- ~~**Dynamic fan-out** (SG-06, map). Its definition shape is only kept free (`kind: "map"`, `graph`).~~ Built 2026-10-06 (`27-Map-Fan-Out.md`): `kind: "map"` with its template under `graph`, at `schema_version` `1.3.0`; validation names template nodes with a `*` segment (`Investigate/*/Assess`).
- ~~**Group controls** (SG-07), and anything about them in the definition.~~ Built 2026-10-05 (`26-Parallel-Group-Controls-And-Joins.md`): a group's join and partial mode are exported as a `join` object, and a level with one is `schema_version` `1.2.0`; limits and timeouts are not exported.
- **Graph identity and version** in definitions or checkpoints (SG-13).
- **Node metadata at run time:** on `NodeContext`, on spans, or on run events. A console joins events to the definition by path, which is enough to show an agent's name. Putting `agent` on the node span would help a trace UI, and can come with SG-23's agent node.
- **Recovery policies, timeouts and node kinds** (fallible, streaming) in the definition (review question 5). ~~Interrupt boundaries~~ are exported since SG-37 (`48-Boundaries-In-Definitions.md`): `interrupt`, with the boundary's timeout, at definition `1.6.0`.
- **Structure in checkpoints.** A checkpoint names where a thread is (`waiting_at`), and the definition says what that is.
