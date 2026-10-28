# Datalium Strata: Dynamic Fan-Out (the Map)

A parallel group (`design/14`, `design/26`) runs branches fixed when the graph is built. A subgraph (`design/15`) runs one compiled child graph. Both docs list the missing piece as not built: **"fanning out over a list"**, a child graph run once per item of a list that only exists at run time.

StrataOps needs it for its M2 (real models):

- **SG-06, the hypothesis fan-out.** `generate-hypotheses` produces three to five hypotheses at run time. `investigate-hypotheses` runs a child graph per hypothesis (`assess`, then whatever the child does), a few at a time, then reduces the findings.
- **The console draws "branches as they appear"** (StrataOps plan §6.11). It joins run events to the exported definition by qualified path, so each item needs a stable, readable path segment (the hypothesis id, not an index), and an event when the map expands.

This document specifies **SG-06**. It is built on `design/26`'s runner, options and results: a map is a parallel group whose branches are the items of a list, each running the same compiled child graph. The SG numbers refer to the gap log of StrataOps (not public yet).

It lands in both engines at one commit (strict parity, StrataOps ADR 0003): the same capability, in each language's idiom.

It was written as a blueprint before implementation and is now built. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| The API | **`AddMap` / `AddMapWithOutcomes`** (.NET) and **`add_map` / `add_map_with` / `add_map_with_outcomes`** (Rust): a name, the items (read from the state when the node starts), a **required key** per item, a **compiled child graph**, an input mapper `(state, item) → child state`, and a reducer over the children's final states, as the plain or the outcomes reducer of `design/26`. |
| What a map is | **One fallible node**, like a group or a subgraph: ordinary edges, a `RecoveryPolicy`, a timeout, metadata, and one hop (`design/24`). Its items are its branches. |
| Options | **`MapOptions<TChild>`**: `design/26`'s options under item names (`MaxConcurrency`, `ItemTimeout`, `CancelSiblingsOnFailure`, `Partial`, `Join`) plus `MaxItems`. **`MaxConcurrency` defaults to 8**, never unbounded. A separate type, not `ParallelGroupOptions`, so a default can differ without one type meaning two things. |
| Joins | **A map takes a join**, as a group does: the runner is the same, and "the first hypothesis confirmed" is a race. `STRATA_ERR_INVALID_QUORUM` refuses a quorum below 1 at compile time; above the item count it is a join that can't be met, decided at run time. |
| Keys | **Required**, a function of the item. A key names the item in paths, events, outcomes and errors. **An empty key, a key containing `/`, or two items with one key fail the map fatally** before any item starts: they come from the state, so a retry would see the same keys. |
| An empty list | **The runner's rules, unchanged.** An `All` map with no items is met at once: the reducer runs with no results. A race or a quorum with no items can't be met: it fails (fatally, since a retry sees the same empty list). |
| `MaxItems` | Optional. **More items than `MaxItems` fail the map fatally** before any starts. Unset: no limit (the concurrency limit bounds the work in flight either way). |
| An item | **Runs the child at a level of its own**, as a subgraph's child does: its own hops, decision and failures, its nodes at **`<map>/<key>/<child node>`**. A child failure fails the item with the child's kind and retry hint, as for a subgraph (`design/15`, `design/25`). |
| Winding down | **An item is a graph, not a handler, so it winds down** when it is given up on (lost race, failing sibling, its timeout, a cancelled run): the map tells it through its token and waits for it to stop, as the executor does for a subgraph. Every `NodeStarted` inside it still gets its `NodeCompleted`, before the item's `BranchCompleted`. |
| Interrupt boundaries | **Refused at compile time**: `.Compile()` / `.compile()` fails if the child, or any subgraph inside it, has an interrupt boundary. Freezing one item of a running map isn't built. |
| Run events | **`MapExpanded { Keys }`** (new), once per attempt at the map's path, before any item starts. Items reuse **`BranchStarted` / `BranchCompleted`** at `<map>/<key>` (with `design/26`'s reasons) and **`JoinDecided`**. The child's nodes report as ever, under the item's path. |
| Streaming | Item chunks are attributed to **`<map>/<key>/<child node>`**. A plain run of a parent whose map's child streams fails at the map node. |
| Export | **`kind: "map"`** with the child's definition as a template under **`graph`**, and a `join` when it isn't the default. A level with a map is **`schema_version` `1.3.0`**, a superset of `1.2.0`. Validation recurses into the template with a **`*` segment**: `investigate-hypotheses/*/assess`. |
| Checkpoints | **Unchanged.** A map is one node: it never nests a child checkpoint, and a retry or a resume re-runs every item. |
| Retries | **The map's policy retries the whole map**, which re-reads the items and re-runs every one, as a retried group re-runs every branch. No per-item retry (an item's child can retry its own nodes). |

---

## Usage

```csharp
// SG-06: one child graph per hypothesis, four at a time, keep what succeeded.
CompiledGraph<HypothesisState> assess = new StateGraph<HypothesisState>()
    .AddFallibleNode("assess", AssessAsync)
    .AddNode("summarize", Summarize)
    .AddEdge("assess", "summarize")
    .Compile();

graph.AddMapWithOutcomes(
    "investigate-hypotheses",
    items: s => s.Hypotheses,
    key: h => h.Id,
    child: assess,
    input: (s, h) => new HypothesisState(s.IncidentId, h),
    reducer: (s, results) => s with
    {
        Findings = [.. results.Succeeded.Select(r => r.Finding)],
        Unassessed = [.. results.Where(r => r.Status != BranchStatus.Succeeded).Select(r => r.Name)],
    },
    new MapOptions<HypothesisState>
    {
        MaxConcurrency = 4,
        ItemTimeout = TimeSpan.FromSeconds(60),
        Partial = PartialResults.KeepSucceeded,
        MaxItems = 10,
    });

// The plain reducer gets the children's final states, in list order.
graph.AddMap("score", s => s.Candidates, c => c.Id, scoring, (s, c) => new Score(c), (s, scores) => s with { Scores = [.. scores] });
```

```rust
let assess = Arc::new(
    StateGraph::new()
        .add_async_fallible_node("assess", assess)
        .add_node("summarize", summarize)
        .add_edge("assess", "summarize")
        .compile()?,
);

graph.add_map_with_outcomes(
    "investigate-hypotheses",
    |s: &Investigation| s.hypotheses.clone(),
    |h: &Hypothesis| h.id.clone(),
    assess,
    |s: &Investigation, h: Hypothesis| HypothesisState::new(&s.incident_id, h),
    |s: &Investigation, results: BranchOutcomes<HypothesisState>| merge(s, results),
    MapOptions::new()
        .with_max_concurrency(4)
        .with_item_timeout(Duration::from_secs(60))
        .keep_succeeded()
        .with_max_items(10),
);
```

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Graph;

/// How a map runs its items and decides it is done. The items are a parallel group's branches,
/// so every option means what it means on ParallelGroupOptions (design/26), under item names.
public sealed class MapOptions<TChild>
{
    public const int DefaultMaxConcurrency = 8;

    /// At most this many items run at once; the rest start in list order. Always bounded.
    /// At least 1 (ArgumentOutOfRangeException from the setter).
    public int MaxConcurrency { get; init; } = DefaultMaxConcurrency;

    /// Each item's own timeout, from when it starts, on the run's clock: a transient failure of the
    /// item (status TimedOut). Null: none. Positive.
    public TimeSpan? ItemTimeout { get; init; }

    public bool CancelSiblingsOnFailure { get; init; } = true;
    public PartialResults Partial { get; init; } = PartialResults.FailGroup;
    public JoinPolicy<TChild> Join { get; init; } = JoinPolicy<TChild>.All;

    /// More items than this fail the map fatally before any starts. Null: no limit. At least 1.
    public int? MaxItems { get; init; }
}

public sealed class StateGraph<TState>
{
    /// The plain reducer: the children's final states — the deciding items' when the join is met
    /// (every item for All), else every succeeded item's — in list order.
    public StateGraph<TState> AddMap<TItem, TChild>(
        string name,
        Func<TState, IEnumerable<TItem>> items,
        Func<TItem, string> key,
        CompiledGraph<TChild> child,
        Func<TState, TItem, TChild> input,
        Func<TState, IReadOnlyList<TChild>, TState> reducer,
        MapOptions<TChild>? options = null,
        IReadOnlyDictionary<string, string>? metadata = null);

    /// The outcomes reducer: every item's BranchOutcome (Name is its key), in list order.
    public StateGraph<TState> AddMapWithOutcomes<TItem, TChild>(
        string name,
        Func<TState, IEnumerable<TItem>> items,
        Func<TItem, string> key,
        CompiledGraph<TChild> child,
        Func<TState, TItem, TChild> input,
        Func<TState, BranchOutcomes<TChild>, TState> reducer,
        MapOptions<TChild>? options = null,
        IReadOnlyDictionary<string, string>? metadata = null);
}

/// A map expanded its items, once per attempt, before any started.
public sealed record MapExpanded(IReadOnlyList<string> Keys) : RunEvent;

public enum GraphNodeKind { ParallelGroup, Subgraph, Map }   // Map is new: "map"
```

### Rust

```rust
pub struct MapOptions<T> { /* private */ }

impl<T> MapOptions<T> {
    pub const DEFAULT_MAX_CONCURRENCY: usize = 8;
    pub fn new() -> Self;
    pub fn with_max_concurrency(self, limit: usize) -> Self;       // .compile() refuses 0
    pub fn with_item_timeout(self, timeout: Duration) -> Self;     // .compile() refuses zero
    pub fn with_cancel_siblings_on_failure(self, cancel: bool) -> Self;
    pub fn with_partial(self, partial: PartialResults) -> Self;
    pub fn keep_succeeded(self) -> Self;
    pub fn with_join(self, join: Join<T>) -> Self;
    pub fn with_max_items(self, limit: usize) -> Self;             // .compile() refuses 0
}

impl<S> StateGraph<S> {
    pub fn add_map<I, T, Items, Key, Input, R>(self, name, items: Items, key: Key,
        child: impl Into<Arc<CompiledGraph<T>>>, input: Input, reducer: R) -> Self
    where
        I: Send + 'static,
        T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
        Items: Fn(&S) -> Vec<I> + Send + Sync + 'static,
        Key: Fn(&I) -> String + Send + Sync + 'static,
        Input: Fn(&S, I) -> T + Send + Sync + 'static,
        R: Fn(&S, Vec<T>) -> S + Send + Sync + 'static;

    pub fn add_map_with<…>(…, reducer: R, options: MapOptions<T>) -> Self;            // plain reducer
    pub fn add_map_with_outcomes<…>(…, reducer: R, options: MapOptions<T>) -> Self    // R: Fn(&S, BranchOutcomes<T>) -> S
}

RunEventKind::MapExpanded { keys: Vec<String> }    // new
GraphNodeKind::Map                                   // new
```

- The names follow the group's: Rust has no overloading, so `add_map_with` is .NET's `AddMap(…, options)`.
- Items are read with a borrow and consumed by `input` (Rust), so an item needn't be `Clone`.

---

## Semantics

### Expanding

When the map node starts an attempt, it reads `items(state)` and every item's `key`, then checks, in this order, before anything starts:

1. **`MaxItems`**: more items fail the map fatally, `map '<name>' has 12 items, more than its limit of 10`.
2. **Every key**: an empty key (`item 3 has an empty key`), a key containing `/` (`item 1 has the key 'a/b', which contains '/'`), or a key seen before (`items 0 and 2 have the same key 'h1'`) fails the map fatally, prefixed `map '<name>': `. Indexes are positions in the list, from 0.

Then it records **`MapExpanded`** with the keys in list order and starts the items. An exception from `items`, `key` or `input` is a bug, and propagates like a reducer's.

A fatal failure here is the same on every retry, so the map's policy doesn't retry it (it may still fall back).

### Running the items

Each item is a branch of `design/26`'s runner, named by its key:

- At most `MaxConcurrency` run at once, **starting in list order**. `input(state, item)` is called when the item starts.
- The item **runs the child graph at a level of its own** (`design/15`): its root token is the item's own (the map's attempt token, plus `ItemTimeout`), its nodes are named `<map path>/<key>/<node>`, it counts its own hops against the child's own limit, and it sees no decision and no `LastFailure` (both belong to the parent level).
- Its result is the child's final state. **A child failure fails the item**: transient if the child's own recovery ran out on a transient error, with the retry hint its `failed` checkpoint recorded (`strata.retry_after_ms`), fatal otherwise, exactly as a subgraph node's failure is derived (`design/15`, `design/25`). The child's own policies have run by then.
- **The join, `Partial` and the failure kind are `design/26`'s, unchanged.** A failure matters only once the join can't be met; then `FailGroup` fails the map (at once, cancelling the rest, unless `CancelSiblingsOnFailure` is off) and `KeepSucceeded` waits for every item and reduces over all of them, flagged unmet, failing only if nothing succeeded. The map fails transiently iff a retry could meet the join, with the longest retry hint among its failures. Messages say `item`, not `branch`: `item 'h2': failed at 'assess' after 1 attempt(s): …`, `no item matched the join (3 succeeded)`, `quorum of 2 not reached: 1 of 3 items succeeded`.
- **An item winds down** when the map is done with it before it finished (a lost race, a failing sibling under `FailGroup`, its `ItemTimeout`, the map's own timeout, a cancelled run): its token fires, its child stops at its current node as a cancelled subgraph does, and the map waits for that before reporting the item `cancelled` (or `TimedOut`). A sync handler inside the child is abandoned by the child's own executor, as always, so winding down is prompt.

### The reducer

- **`AddMap` / `add_map_with`**: the children's final states, in list order: the deciding items' when the join was met (every item for `All`), else every succeeded item's.
- **`AddMapWithOutcomes` / `add_map_with_outcomes`**: a `BranchOutcomes<TChild>` with one `BranchOutcome` per item, `Name` its key, in list order, plus `JoinMet` and `Deciding` (keys, completion order). Items that never started are `Cancelled`.

### An empty list

No special case: the runner's own rules decide. An `All` map needs every item, which no items satisfy at once, so the reducer runs with an empty list (outcomes: empty, `JoinMet` true). A race (`FirstSuccess`, `FirstMatching`) or a `Quorum(n)` needs at least one success, which can't happen: under `FailGroup` the map fails, and under `KeepSucceeded` nothing succeeded, so it fails too. That failure is fatal, since a retry reads the same empty list. A `Quorum(n)` with fewer than n items is decided the same way before anything starts: `FailGroup` fails at once, `KeepSucceeded` runs every item and reduces unmet.

### Interrupt boundaries

`.Compile()` / `.compile()` refuses a map whose child graph has an interrupt boundary, or holds a subgraph that has one, at any depth: `map 'investigate-hypotheses' runs a graph with an interrupt boundary at 'review/approve'; a map item can't freeze`. (A boundary name that isn't a node never triggers, so it isn't counted.) The map node itself may be a boundary of its own graph, like any node.

### Retries, timeouts, cancellation

- **Retries**: the map's `RecoveryPolicy` retries the whole map, with its backoff: it reads the items again (from the same committed state) and re-runs every one, emitting a new `MapExpanded`. Items must be safe to repeat, as branches must. A fallback runs on the pre-map state, with `LastFailure` saying why the map failed.
- **The map's own timeout** (`SetTimeout`) covers the whole map: every item winds down and the map fails transiently, discarding every result. `ItemTimeout` keeps what finished.
- **A cancelled run** winds every item down and ends the run `Cancelled` at the map node; the checkpoint nests nothing, and a resume re-runs the map.

### Hops

A map is **one hop** at its level, however many items it runs (`design/24`). Each item's child counts its own nodes against the child's compiled limit, as a subgraph's child does; a child that trips fails its item fatally.

---

## Run events and telemetry

| | |
|---|---|
| `MapExpanded` | **New**: `Keys` in list order, at the map's path, with its attempt, once per attempt, after the keys are checked and before any item starts. A console draws the items from it. Not emitted when the keys or `MaxItems` fail the map. |
| `BranchStarted` / `BranchCompleted` | Per item, at `<map>/<key>`, with the map's attempt, exactly as for a branch: `BranchCompleted` comes after every event of the item's child, and carries `TimedOut`, `LostRace` or `SiblingFailed` as `design/26` defines them. |
| `JoinDecided` | For a map whose join isn't `All`, as for a group: `Deciding` holds keys. |
| The child's events | `NodeStarted`, `RetryScheduled`, `NodeCompleted`, tool calls, `HopLimitExceeded`, … at `<map>/<key>/<node>`. A child never writes to the caller's store, so it reports no `CheckpointSaved`. |
| Spans | An item runs in a `strata.branch` span named by its path; its child's `strata.node` spans nest inside it. The map's attempt span gets a `map_expanded` span event with `strata.map.item_count` and `strata.map.keys`. |
| Metrics | Unchanged: the map's `strata.node.duration` covers its items, and the child's nodes record their own. |

Event order for a three-hypothesis map, `MaxConcurrency = 2` and `KeepSucceeded`, `h2` failing:

```text
NodeStarted investigate-hypotheses#1
MapExpanded investigate-hypotheses [h1, h2, h3]
BranchStarted investigate-hypotheses/h1, investigate-hypotheses/h2
NodeStarted investigate-hypotheses/h1/assess …
NodeCompleted investigate-hypotheses/h2/assess Failed
BranchCompleted investigate-hypotheses/h2 Failed          (KeepSucceeded: h3 starts)
BranchStarted investigate-hypotheses/h3
…
BranchCompleted investigate-hypotheses/h1 Succeeded
BranchCompleted investigate-hypotheses/h3 Succeeded
NodeCompleted investigate-hypotheses#1 Committed
```

---

## Wire format

### The definition

A map exports `kind: "map"`, the child's whole definition as a template under `graph`, and a `join` when its join or `Partial` isn't the default:

```json
{
  "name": "investigate-hypotheses",
  "metadata": { "items": "hypotheses" },
  "kind": "map",
  "graph": { "schema_version": "1.0.0", "entry_point": "assess", "nodes": [ … ], "edges": [ … ] },
  "join": { "policy": "all", "count": null, "condition": null, "partial": "keep_succeeded" }
}
```

- **A level with a map is `schema_version` `1.3.0`**, the lowest version that can express it. `1.3.0` is a superset of `1.2.0`: a group at that level may have a `join` too. Readers accept `1.0.0` to `1.3.0`. `check_shape` / `ToTopology` refuse a `map` below `1.3.0`, a map without `graph` or with `branches`, and a `join` on anything but a `parallel_group` or a `map` (exit 2 in the CLI).
- What the item key means is description, so it goes in the node's metadata (`SetNodeMetadata`), not a field of its own. `MaxConcurrency`, `ItemTimeout`, `CancelSiblingsOnFailure` and `MaxItems` are runtime tuning and aren't exported, as for a group.
- **Validation recurses into the template** as into a subgraph's child, naming its nodes with a `*` segment for the item: `investigate-hypotheses/*/assess`. Errors are ordered as `design/22` says (the level's own, its groups' and maps', then each child's and template's, in node order). **`STRATA_ERR_INVALID_QUORUM`** covers a map's quorum below 1 (or missing); there is no upper bound, since the items are counted at run time.

New fixtures: `schema/fixtures/graph-export/map.json` (a map with a template holding a group and a subgraph, a map with a `join`, exported by both engines and validated by the CLI) and `schema/fixtures/graph-validation/map-template-errors.json` (an orphan inside a template and an invalid map quorum). `strata graph view` annotates a map with its template's entry point and its join.

### Checkpoints

**Unchanged.** A map is one node to the graph around it: items, their outcomes and their children's checkpoints exist only inside one attempt. A failed map's checkpoint nests no child (unlike a subgraph's), so a resume re-runs every item.

---

## Migration

**.NET** (source-compatible): `MapOptions<TChild>`, `AddMap`, `AddMapWithOutcomes`, the `MapExpanded` event, `GraphNodeKind.Map` and `GraphDefinition.MapVersion` are new. A `switch` over `GraphNodeKind` without a default needs the new case.

**Rust:** `MapOptions`, `add_map`, `add_map_with`, `add_map_with_outcomes`, `RunEventKind::MapExpanded` (the enum is non-exhaustive) and `MAP_GRAPH_SCHEMA_VERSION` are new. `GraphNodeKind` gains `Map`, so an exhaustive `match` on it needs the arm.

**StrataOps:** `investigate-hypotheses` becomes `AddMapWithOutcomes` over `generate-hypotheses`' list, keyed by hypothesis id, with `KeepSucceeded`; the console draws items from `MapExpanded` and joins `<map>/<key>/<node>` events to the template's `<map>/*/<node>`.

---

## Tests

Mirrored per engine, in new suites `MapTests.cs` / `map.rs`, on a controlled clock where time matters:

- **An empty list, one item, many items:** an empty `All` map reduces with nothing; one item runs its child; many items respect `MaxConcurrency` (the peak in flight equals the limit, starts in list order) and reduce in list order. The default limit is 8.
- **Keys are checked before anything starts:** a duplicate, an empty key and a key with `/` fail the map fatally with no `MapExpanded` and no item started; `MaxItems` likewise.
- **Paths and events per item:** `MapExpanded` before the first `BranchStarted`; the child's nodes at `<map>/<key>/<node>`; each item's `BranchCompleted` after its child's events.
- **Partial results and failures:** under `KeepSucceeded` the outcomes reducer sees every item with its status and error; a child's transient failure makes a transient map failure carrying its hint; under `FailGroup` the siblings wind down with `SiblingFailed` and their children's nodes complete `Cancelled`.
- **Item timeouts** time out only that item (`TimedOut`), and its child winds down.
- **Joins:** `FirstSuccess` cancels the losers (`LostRace`); an empty race fails fatally.
- **A map inside a subgraph:** paths are `<subgraph>/<map>/<key>/<node>`.
- **Streaming:** item chunks are named `<map>/<key>/<node>`; a plain run of a map whose child streams fails at the map node.
- **A retried map re-runs its items**, with a second `MapExpanded`.
- **An interrupt inside an item is refused at compile time**, directly and through a nested subgraph.
- **Hops:** a map is one hop; an item's child trips its own limit, failing the item fatally.
- **Export and validation:** `map.json` from both engines and round-tripped; `1.3.0` only on a level with a map; `check_shape` / `ToTopology` refuse a misplaced map; the template's errors are named with `*`; an invalid map quorum is refused.
- **Mutation checks:** not checking for duplicate keys, starting items without the concurrency limit, and reporting an item before its child wound down should each fail tests in both engines.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

### The questions as asked

Recommendations are in bold.

1. **Is `key` required?** **Yes.** It names the item in paths, events, outcomes and errors, and a console needs a stable segment (the hypothesis id) rather than an index that shifts when the list does. Duplicates, empty keys and keys containing `/` **fail the map fatally**, before any item starts: they come from the state, so a retry would see them again.
2. **What does an empty list do?** **The runner's rules decide**: an `All` map reduces with no results (the handoff's recommendation); a race or quorum, which needs a success, fails fatally. A special case ("always reduce") would hand a `FirstSuccess` reducer a met join with no winner.
3. **A maximum item count?** **Optional (`MaxItems`), unset by default**, failing the map fatally when exceeded. The bounded concurrency already caps the work in flight; `MaxItems` guards against a runaway list (a model asked for 5 hypotheses that returned 500).
4. **A compiled graph or a plain function per item?** **A compiled graph**, so items have nodes, events and a definition template. A branch-style function per item isn't built: a one-node child graph does the same, and keeps one model for consoles.
5. **Paths and events:** **`<map>/<key>/<node>`; `MapExpanded` once per attempt; `BranchStarted` / `BranchCompleted` reused at `<map>/<key>`**, so a console that already draws branches draws items.
6. **Options:** **`MapOptions<TChild>`, a type of its own** with `design/26`'s fields under item names, `MaxConcurrency` **bounded at 8 by default**, plus `MaxItems`. **A map takes a join**, so `1.3.0` is a superset of `1.2.0`.
7. **Export:** **`kind: "map"` with the template under `graph`, at `1.3.0`**; the key's meaning goes in node metadata; validation names template nodes with `*`.
8. **Interrupts inside an item:** **refused at compile time**, listed under "Not built".
9. **Streaming:** **`<map>/<key>/<node>`.**
10. **Hops:** **a map is one hop; each item is its own level**, as a subgraph's child is.

### Added by this document

11. **Do items wind down or get abandoned?** **They wind down**, as a subgraph's child does: an item is a graph, its child's executor stops promptly at its token, and waiting keeps the event contract (every `NodeStarted` gets its `NodeCompleted`, before the item's `BranchCompleted`). A group's branches are still abandoned, as `design/26` says.
12. **Does a failed map nest its item's checkpoint, as a failed subgraph does?** **No.** A map is one node and a resume re-runs it; which item's checkpoint would it nest when several failed? Partial progress across a resume is SG-10's.

---

## Implementation Status

Built 2026-10-06 in both engines, the shared validator, the schema and the CLI, in one change, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **The group runner became a fan-out runner.** `design/26`'s runner took fixed branch closures over one state type. It now takes a list of names and a launcher (index → the branch's work), is generic over the branch's result type, and is told what to call its branches (`branch` / `item`) and whether they wind down. A group is a launcher over its closures; a map is a launcher that runs the child. Group behaviour is unchanged: every existing group test passes untouched.
- **Two decisions now happen before anything starts**, which a group never needed (its quorum is validated against its branch count): a join that needs nothing (an `All` map with no items) is met at once, and one that needs more successes than there are items can't be met (under `FailGroup` nothing starts; under `KeepSucceeded` every item runs and the map reduces unmet). A failure with no failed items to name says `no items to run`.
- **Option validation is each engine's idiom**, as for groups: .NET's `MapOptions` setters throw `ArgumentOutOfRangeException` for a `MaxConcurrency` or `MaxItems` below 1 or a non-positive `ItemTimeout`; Rust's builder can't fail, so `.compile()` refuses them (`CompilationFailed`).
- **Winding down, in the executor.** A map node is marked as winding down (`NodeEntry.WindsDown` / `winds_down`), so the executor waits for it when its token fires instead of abandoning it, as it does for a subgraph. The map watches the token itself and returns once its items have stopped. In Rust the executor's first version also raced the token, and lost to the map's own error in the same poll, reporting a cancelled run as a failure (the cancellation test caught it); it now treats an error returned after the token fired as the node being stopped. In .NET the map throws `OperationCanceledException`, which the executor already reads that way.
- **An item that didn't succeed after its token fired counts as cancelled** (or timed out, when its own timer fired), whatever its child reported, and a panic or bug raised while it winds down is discarded: by then the map has given up on it. An item that succeeded anyway counts as succeeded.
- **The child-failure rule is shared.** `SubgraphRunner.ChildFailure` / `subgraph::child_failure` derive a failed child's kind, retry hint and `failed` checkpoint for both a subgraph node and a map item; an item's message drops the `subgraph '<name>'` prefix (`failed at 'assess' after 1 attempt(s): …`, `hit its hop limit: …`) because the map prefixes `item '<key>': `.
- **Finding a boundary at any depth** is `CompiledGraph.InterruptBoundary()` / `interrupt_boundary()`: a level's own boundaries that are nodes, then each subgraph node's (`NodeEntry.Boundary`), in node order, qualified (`Review/assess`).
- **`graph view`** writes a map as `Investigate: map over a template, entry Assess, 3 nodes`, plus `, join …` as for a group.
- **`design/17` lists no SG-06 entry**, so nothing was struck there; `design/14`, `15`, `18` and `22` were.

**How it was built:**

- **.NET:**
  - `Graph/FanOutRunner.cs` is new: `FanOutRunner.RunAsync` (the runner, moved out of `ParallelBranches.cs`), `BranchLauncher`, `BranchLaunch`, `FanOutNoun`, `FanOutSettings`. `ParallelGroupRunner.RunAsync` is now the group's launcher.
  - `Graph/Map.cs` is new: `MapOptions<TChild>`, `MapShape`, `MapRunner` (keys, `MapExpanded`, each item's child run).
  - `StateGraph.cs`: `AddMap`, `AddMapWithOutcomes`, `_maps`, the compile-time boundary check, the map's topology detail; subgraph entries record their child's boundary. `BranchOutcomes.Counted()` is the plain reducer's input for groups and maps.
  - `Execution.cs`: `NodeEntry.WindsDown` and `Boundary`; `CompiledGraph.Interruption.cs` awaits a winding-down node; `Subgraph.cs`: `ChildFailure`, and `CapturingStore` is internal.
  - `GraphTopology.cs`: `GraphNodeKind.Map`. `Validation`: template recursion with `*`, the map quorum check. `Observability`: `MapExpanded` and `RunTelemetry.MapExpanded`.
  - `Datalium.Strata.Storage/Schema`: `GraphDefinition.MapVersion`, the version rule in `ToDefinition`, and the map checks in `ToTopology`.
- **Rust:**
  - `parallel.rs`: `run_fan_out` with `Launch`, `Launcher`, `Noun` and `FanOut`, `GroupRun` generic over the result and able to wind down (`give_up` waits, `give_up_now` doesn't); `run_branches` is the group's launcher.
  - `map.rs` is new: `MapOptions`, `MapShape`, `MapSpec`, `run_map`.
  - `graph.rs`: `add_map`, `add_map_with`, `add_map_with_outcomes` (the last two `#[allow(clippy::too_many_arguments)]`), `maps`, the compile checks, `node_definition`, `CompiledGraph::interrupt_boundary`. `node.rs`: `winds_down`, `boundary`. `checkpoint.rs`: the winding-down attempt. `subgraph.rs`: `child_failure`.
  - `schema.rs`: `GraphNodeKind::Map`, `MAP_GRAPH_SCHEMA_VERSION`, the version rule, `check_shape`. `validate.rs`: template recursion with `*`, `add_map_errors`. `observability.rs`: `RunEventKind::MapExpanded`, `RunTelemetry::map_expanded`.
- **CLI:** `graph view` annotates a map; `graph validate` reads `1.3.0` through `check_shape`.
- **Schema:** `graph-definition.schema.json` gains `map`, `1.3.0` and the pairings; new fixtures `graph-export/map.json` and `graph-validation/map-template-errors.json`. Every definition fixture validates against the JSON Schema, and `map.json` at `1.2.0` is refused by it.

**Tests:**

- **New suites:** `MapTests.cs` (27, its key check a theory of three cases) and `map.rs` (25, the same three cases in one test). Both run the same cases: an empty list, one item, the limit and list order, the default limit of 8, the plain reducer, bad keys, `MaxItems`, out-of-range options, `KeepSucceeded` outcomes, a transient child failure's kind and hint, siblings winding down, an item timeout, a `FirstSuccess` race, an empty race, a map inside a subgraph, item streaming and the plain-run refusal, a retried map, a cancelled run, hops, the compile-time boundary and quorum refusals, and the definition (export, round trip, misplaced maps, `*` paths). The CLI gained three tests (`map.json` and `map-template-errors.json` validated, a map below `1.3.0` refused, the map annotated by `view`).
- **Changed tests:** the "unknown kind" tests in both engines and the CLI used `map`, which is now known; they use `loop`. The version-list and join-placement messages gained `1.3.0` and "or a map".
- **Timing:** Rust's suite runs on tokio's paused clock. .NET's timeout test runs on a `FakeTimeProvider`; its concurrency tests use short real delays, and the suite passed five repeated runs.
- **Totals when this landed:** .NET 541 passed, with 38 Postgres/Redis tests skipped because those services weren't running locally (this change touches no store), plus the example's 1. `rust/` 533, `cli/` 64, `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` (all three projects) and `dotnet format --verify-no-changes` are clean.
- **Mutation checks, in both engines:**
  - Not checking for duplicate keys fails 1 test in each.
  - Starting items without the concurrency limit fails 2 in each.
  - Reporting an item before its child wound down (items abandoned, as a group's branches are) fails 2 in .NET and 4 in Rust. .NET catches fewer because cancelling a token runs the child's continuations inline, so in the race and cancelled-run tests the child often finishes winding down before the item is reported even when nothing waits for it.

## Not built (deliberately out of scope)

- **Interrupt boundaries inside an item**, and so resuming one item of a map. `.compile()` refuses them.
- **Keeping an item's progress across a retry, a crash or a resume.** A retry or a resume re-runs every item (SG-10 for crash-safe progress).
- **Per-item retry policies.** The map retries as a whole; an item's child can retry its own nodes.
- **A plain function per item** instead of a compiled child graph.
- **Choosing the child graph per item** (a map over heterogeneous children). Route to several maps with conditional edges, or branch inside the child.
- **Exporting `MaxConcurrency`, `ItemTimeout`, `MaxItems`**, or the key function itself.
