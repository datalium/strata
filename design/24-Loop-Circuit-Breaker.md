# Datalium Strata: Loop Circuit Breaker (`MaxNodeHops`)

STRATA-SPEC-004 §5 ("Loop Escape Guards") promises a global circuit breaker on a compiled graph:

```csharp
// Abort the transaction automatically if any runtime thread processes more than 25 total node hops
var runner = graph.Compile(new CompilationOptions { MaxNodeHops = 25 });
```

Until now neither engine had it. `Compile()` / `compile()` took no options and nothing counted node hops, so a guarded loop was bounded only by its own predicate. A predicate bug in an agent loop means unbounded LLM spend.

StrataOps' investigation graph loops on purpose: `correlate → expand → generate-hypotheses → investigate → correlate` while more evidence is needed, and `remediation → expand → …` when verification shows the service didn't recover. `remediation` is a subgraph with a nested human-approval interrupt, so every pass through the second loop freezes and resumes the thread, possibly in another process. The planned workaround was a `Hop` / `MaxHops` counter in StrataOps' graph state, with a guard on every back edge. This document specifies **SG-05**, which makes that counter unnecessary. The SG numbers refer to the gap log of StrataOps (not public yet).

SG-05 lands in both engines at one commit (strict parity, StrataOps ADR 0003): the same capability, in each language's idiom.

It was written as a blueprint before implementation and is now built. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| What is a hop | **Every node started at a level**, including a fallback node. Retries of the same node are not hops. A parallel group and a subgraph are each **one hop** at their level. |
| Which graph's limit | **Each compiled graph enforces its own limit over its own hops** (per level). A subgraph's child counts its own nodes against the limit it was compiled with, and a parent counts the subgraph node once per run of it. See "Per level, not per run". |
| When it trips | **Before starting the node that would be hop N+1.** That node gets no `NodeStarted`, no `NodeStart` chunk, and its handler never runs. A loop that ends at hop N completes. |
| Cancellation first | The cancellation check runs before the hop check: a cancelled run at the limit ends `Cancelled`, not failed. |
| The result | A new result that **refines `Failed`**, as `NodeFailed` does: .NET **`GraphExecutionResult<TState>.HopLimitExceeded(Node, Hops, Limit)`**, Rust **`GraphExecutionResult::Failed(StrataError::HopLimitExceeded { node, hops, limit })`**. Code matching `Failed` still catches it. |
| The checkpoint | A **`Failed` checkpoint** at the node that would have run (`suspended_node` = `next_node` = that node), with the last committed state, `error` (the message), **`error_kind` = `hop_limit`**, and `strata.hops`. No `attempts`: nothing was attempted. No recovery policy sees it. |
| How the count persists | An engine-reserved checkpoint metadata key, **`strata.hops`**: the hops the level made before the checkpoint's next node, which is the count a resume starts from. **No schema change.** Callers can't write `strata.*` keys, and every store and the CLI already carry metadata. |
| When it is written | On every checkpoint a level writes **while it has a limit in effect**. A graph without a limit writes exactly the checkpoints it wrote before. |
| Resume | Reads `strata.hops` at every level the checkpoint nests, and **counts the resume's first node as a hop like any other**. So a resume of a hop-limit checkpoint trips again at once, unless the resume raises the limit. |
| The API | .NET `Compile(CompilationOptions)` with `Compile()` unchanged; Rust `compile_with(CompileOptions)` with `compile()` unchanged. No limit by default. A limit of 0 (or less, in .NET) is refused at compile. |
| Per-run override | **Yes**: `RunOptions.MaxNodeHops` / `RunConfig::with_max_node_hops`. It replaces the compiled limit (higher or lower) **for the outermost graph only**. |
| Telemetry | A new run event **`HopLimitExceeded { hops, limit }`** on the node that would have run; a new counter **`strata.graph.hop_limit_exceeded`**; the run span gets **`error.type` = `hop_limit`** when the run ends with this result. |
| Exported definition | **Not now.** `ToDefinition()` / `to_definition()` don't carry the limit (see "Not built"). |
| Not in scope | Token, cost and time budgets (StrataOps' Model Fabric owns budgets; deadlines exist), per-node visit limits, SPEC-004's "missing terminal paths" check. |

---

## Usage

```csharp
var investigation = new StateGraph<Investigation>()
    .AddNode("correlate", Correlate)
    .AddNode("expand", Expand)
    .AddNode("hypothesize", Hypothesize)
    .AddNode("investigate", Investigate)
    .AddSubgraph("remediation", remediationGraph, ToRemediation, FromRemediation)   // nested approval
    .AddEdge("correlate", "expand")
    .AddEdge("expand", "hypothesize")
    .AddEdge("hypothesize", "investigate")
    .AddConditionalEdge("investigate", "correlate", s => s.NeedsMoreEvidence)
    .AddEdge("investigate", "remediation")
    .AddConditionalEdge("remediation", "expand", s => !s.Recovered)
    .SetEntryPoint("correlate")
    .Compile(new CompilationOptions { MaxNodeHops = 40 });

var result = await investigation.RunAsync(incidentId, state, store, options, ct);
// … every remediation pass freezes for approval; each ResumeAsync carries the count on …

if (result is GraphExecutionResult<Investigation>.HopLimitExceeded tripped)
{
    // tripped.Node == the node that would have been hop 41; a Failed checkpoint with
    // error_kind "hop_limit" is in the store.
}

// An operator decides the loop was making progress: let the thread go on, 20 hops further.
await investigation.ResumeAsync(incidentId, HumanSignal.Approve("oncall"), store,
    new RunOptions { MaxNodeHops = 60 });
```

```rust
let investigation = StateGraph::new()
    .add_async_node("correlate", correlate)
    // … as above …
    .set_entry_point("correlate")
    .compile_with(CompileOptions::new().with_max_node_hops(40))?;

match investigation.run_with(incident_id, state, store.clone(), config).await {
    GraphExecutionResult::Failed(StrataError::HopLimitExceeded { node, hops, limit }) => { /* … */ }
    GraphExecutionResult::Failed(other) => { /* any other failure */ }
    _ => {}
}

investigation
    .resume_with(incident_id, HumanSignal::approve("oncall"), store,
        RunConfig::new().with_max_node_hops(60))
    .await;
```

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Graph;

/// Options for StateGraph.Compile (STRATA-SPEC-004).
public sealed class CompilationOptions
{
    /// The most nodes one thread may start at this graph's level, across every run and resume of it.
    /// Null (the default): no limit. Must be at least 1.
    public int? MaxNodeHops { get; init; }
}

public sealed class StateGraph<TState>
{
    public CompiledGraph<TState> Compile();                              // = Compile(new CompilationOptions())
    public CompiledGraph<TState> Compile(CompilationOptions options);    // ArgumentOutOfRangeException for MaxNodeHops < 1
}

public sealed partial class CompiledGraph<TState>
{
    public int? MaxNodeHops { get; }          // the compiled limit
}

public sealed class RunOptions
{
    /// Replaces the compiled limit for this run or resume, at the outermost graph only. Must be at
    /// least 1: the run throws ArgumentException before it starts otherwise.
    public int? MaxNodeHops { get; init; }
}

public abstract record GraphExecutionResult<TState>
{
    /// The level was about to start Node, which would have been hop Hops + 1 with a limit of Limit.
    public sealed record HopLimitExceeded(string Node, int Hops, int Limit) : Failed(…);
}
```

### Rust

```rust
/// Options for `StateGraph::compile_with` (STRATA-SPEC-004).
#[derive(Debug, Clone, Default)]
pub struct CompileOptions { /* private */ }

impl CompileOptions {
    pub fn new() -> Self;
    /// The most nodes one thread may start at this graph's level, across every run and resume.
    /// 0 is refused by `compile_with` (`CompilationFailed`).
    pub fn with_max_node_hops(self, limit: u32) -> Self;
    pub fn max_node_hops(&self) -> Option<u32>;
}

impl<S> StateGraph<S> {
    pub fn compile(self) -> Result<CompiledGraph<S>, StrataError>;            // = compile_with(CompileOptions::default())
    pub fn compile_with(self, options: CompileOptions) -> Result<CompiledGraph<S>, StrataError>;
}

impl<S> CompiledGraph<S> {
    pub fn max_node_hops(&self) -> Option<u32>;
}

impl RunConfig {
    /// Replaces the compiled limit for this run or resume, at the outermost graph only.
    /// 0 is refused: the run returns `Failed(ExecutionAborted)` before it starts.
    pub fn with_max_node_hops(self, limit: u32) -> Self;
}

pub enum StrataError {
    // …
    /// The level was about to start `node`, which would have been hop `hops + 1` with a limit of `limit`.
    HopLimitExceeded { node: String, hops: u32, limit: u32 },
}
```

- `CompileOptions` is a builder with private fields, like `RunConfig`, so later compile options don't break anyone. `.NET`'s `CompilationOptions` is an `init`-only class, like `RunOptions`, and keeps the spec's name.
- Rust takes a `u32`, so the only invalid limit is 0. .NET takes an `int`, so it refuses 0 and below.
- The message is the same in both engines: `hop limit of 25 reached: node 'expand' would be hop 26`.

---

## Semantics

### What counts

A **hop is one node started at a level**: one pass through the executor's step loop that starts a node.

| What happens | Hops |
|---|---|
| A node runs (any kind: plain, fallible, streaming, async) | 1 |
| The same node is retried by its recovery policy | 0 more: retries belong to the hop |
| A node fails and its policy falls back | 1 for the failed node, 1 for the fallback node |
| A parallel group runs every branch | 1 |
| A subgraph runs its child | 1 at the parent's level; the child's nodes count at the child's level |
| A resume re-enters the node a checkpoint stopped at (failed, cancelled, frozen inside a subgraph) | the same hop again, not a new one (see "Persistence") |
| A resume continues past an interrupt boundary, or takes its reject route | 1 for that next node |

### When it trips

At the top of the step loop, after the cancellation check and before anything about the node starts:

```text
loop:
    if cancelled: write a cancelled checkpoint, return Cancelled              (unchanged)
    if limit is set and hops >= limit:
        write a Failed checkpoint at `current`, error_kind "hop_limit"
        return HopLimitExceeded(current, hops, limit)
    hops += 1
    NodeStart chunk, NodeStarted, run the node …                             (unchanged)
```

So with a limit of N the N-th node runs and completes normally, and only the node that would be hop N+1 is refused. A loop whose N-th node is its last completes. Nothing extra runs, and the refused node leaves no trace in telemetry but the hop-limit event: no node span, no `NodeStarted` / `NodeCompleted`, no stream chunks.

The trip is not a node failure: no recovery policy sees it (there is no attempt to retry, and a fallback would itself be a hop past the limit), and the node's `RecoveryPolicy` is not consulted.

### Per level, not per run

Each compiled graph enforces its own limit over its own hops:

- The limit belongs to the compiled graph (SPEC-004: "configured per compiled graph"), and a subgraph's child is compiled separately, with its own options. StrataOps' catalog versions the investigation graph and the remediation graph separately, so each version owns its own limit.
- A parent's limit can be reasoned about from the parent's own shape: a subgraph is one node in it, as it is to validation, recovery and telemetry (`design/15`). A parent limit that counted its children's nodes would change meaning whenever a child changed.
- The total is still bounded: at most `parent limit × child limit` nodes, and a child with no loop needs no limit.

A child's count starts at 0 each time its subgraph node runs it fresh, and lives in the child's nested checkpoint across a freeze (`design/16`), so a child frozen mid-loop resumes counting where it stopped. A retry of the subgraph node runs the child fresh, from 0.

When a child trips, the subgraph node fails **fatally** (a hop limit is a fail-safe, not a transient condition), with the message `subgraph 'remediation' hit its hop limit: hop limit of 10 reached: node 'apply' would be hop 11`, and keeps the child's hop-limit checkpoint nested in its own `Failed` checkpoint, as it keeps any failed child's (`design/16`). The parent's result is then `NodeFailed` for the subgraph node, not `HopLimitExceeded`: the parent's own limit wasn't reached. Its recovery policy applies as usual.

### Persistence

`strata.hops` is **the number of hops the level made before the checkpoint's next node**: exactly the count a resume from that checkpoint starts from. With the rule "a resume counts its first node as a hop like any other", that one definition covers every checkpoint:

| Checkpoint | `next_node` | `strata.hops` |
|---|---|---|
| `interrupted` at the level's own boundary | the node after the boundary | hops including the boundary node |
| `interrupted` around a frozen child | the subgraph node itself | hops before the subgraph node |
| `failed` (a node's policy gave up) | the node that failed | hops before it |
| `cancelled` | the node running, or about to start | hops before it |
| `failed` with `error_kind` `hop_limit` | the node that would have run | hops before it (= the limit, unless a resume lowered the limit) |

So a node re-entered by a resume gets the hop number it had the first time, and the thread's total is never double-counted. A resume of a hop-limit checkpoint counts its first node as hop `limit + 1`, which trips again at once: it writes a new hop-limit checkpoint (the old one is stamped `resumed_at`, as for any resume), and returns `HopLimitExceeded` again. Only a resume with a higher `RunOptions.MaxNodeHops` / `with_max_node_hops` goes on.

- **Every level.** A checkpoint that nests a child's carries one `strata.hops` per level, each in its own `metadata` map, and a resume reads each level's from its own.
- **Only with a limit.** A level writes `strata.hops` only when it has a limit in effect (compiled, or the run's override at the outermost level). A graph without a limit writes the same checkpoints as before, byte for byte.
- **A missing key is 0.** A checkpoint written before SG-05, or while the level had no limit, resumes counting from 0. Adding a limit to a graph whose threads are mid-flight therefore bounds what they do *from then on*.
- **A malformed key refuses the resume** in preflight (`design/21`, "Preflight"), before anything is stamped: `checkpoint for thread 't' has an invalid 'strata.hops' value 'x'`. The engine writes only non-negative integers, so this means someone edited the log; resetting the breaker silently would defeat it.
- **Not caller metadata.** Every `strata.` key is already refused from callers (`design/18`), and a resume already drops reserved keys when it carries stored metadata forward, so `strata.hops` never leaks into `NodeContext.Metadata` or a later run's metadata.

A trip at a subgraph node that a resume is re-entering (possible only when the resume lowered the limit) keeps the child checkpoint it was resuming nested in the hop-limit checkpoint, so nothing a human answered, or has yet to answer, is lost.

### The per-run override

`RunOptions.MaxNodeHops` / `RunConfig::with_max_node_hops` **replaces** the compiled limit for one run or resume, at the outermost graph only:

- It can raise or lower the limit, and it sets one on a graph compiled without. A test can run a production graph with a tight limit; an operator can let a tripped thread go further.
- It doesn't reach subgraphs. A child's limit is its compiled one: a per-run value for "every level" would mean something different at each level, and naming a level would need a path-keyed map (see "Not built").
- It is not stored. The next resume uses the compiled limit again unless it passes the override again. `strata.hops` is a count, not a limit.

---

## Telemetry

| | |
|---|---|
| Run event | **`HopLimitExceeded { hops, limit }`** (.NET `HopLimitExceeded(int Hops, int Limit)`), with `node_path` the qualified path of the node that would have run and no `attempt`. Recorded at whichever level trips, so a child's trip is visible (`"remediation/apply"`). It comes after the previous node's `NodeCompleted` and before the hop-limit checkpoint's `CheckpointSaved`. |
| Metric | Counter **`strata.graph.hop_limit_exceeded`** (`{run}`), labelled `strata.node.path` (the node that would have run). Node paths are a graph's shape, so the label is bounded. |
| Run span | When the run's result is `HopLimitExceeded`: status error with the message, as for every failed run, and **`error.type` = `hop_limit`**. A span event (.NET `ActivityEvent` `hop_limit_exceeded`; Rust a `warn` event under the run span) records `strata.node.path`, `strata.hops` and `strata.hop_limit` at whichever level trips. |
| Subgraph node span | A child's trip fails the subgraph node with `error.type` `fatal`, its own failure kind; the event and the metric say why. |
| Streams | No chunk: the refused node never started. The stream ends with the run, and the completion says why. |
| Checkpoint | `error_kind` = `hop_limit` and `strata.hops`, as above; the CLI's `thread inspect` prints `Hops:` when a checkpoint has `strata.hops`. |

`RunEvent` is already non-exhaustive in both engines (`design/18`), so the new kind breaks no `match`.

---

## Wire format

**No schema change.** The count rides in the checkpoint's existing `metadata` map, under the `strata.` prefix design/18 reserved for exactly this; the hop-limit failure is an ordinary `failed` checkpoint with a new `error_kind` value. No `schema_version` bump, no DDL, and no new fixtures in `schema/`. Each engine reads the other's `strata.hops` because both write it as a decimal string in the same map. The graph-definition format is unchanged (the limit isn't exported).

---

## Migration

**.NET** (source-compatible): `Compile()` is unchanged; `CompilationOptions`, `Compile(CompilationOptions)`, `CompiledGraph.MaxNodeHops`, `RunOptions.MaxNodeHops`, `GraphExecutionResult<TState>.HopLimitExceeded` and the `HopLimitExceeded` run event are new. A `switch` over `GraphExecutionResult` that matches `Failed` catches the new result.

**Rust:** `compile()` is unchanged. `StrataError` gains `HopLimitExceeded`; it isn't `#[non_exhaustive]`, so an exhaustive `match` on it needs an arm (as design/20 and design/23's variants did).

**StrataOps:** compiles the investigation graph with `MaxNodeHops` and drops the planned `Hop` / `MaxHops` state fields and back-edge guards. Its execution events map `HopLimitExceeded` (or a `failed` checkpoint whose `error_kind` is `hop_limit`) to `FAILED` with that reason.

---

## Tests

Mirrored per engine, in new suites `hop_limit.rs` / `HopLimitTests.cs`:

- **No limit:** a guarded loop that runs 50 hops completes, and its checkpoints carry no `strata.hops`.
- **Stopped at exactly N:** a loop that never exits, limit 5: five nodes run, the sixth never starts (its handler counter stays put), the result is `HopLimitExceeded(node, 5, 5)` and matches `Failed`, and the store holds a `failed` checkpoint at that node with `error_kind` `hop_limit`, `strata.hops` `5`, no `attempts`, and the last committed state.
- **Exits at N:** a loop that ends at hop N (and one at N−1) completes untouched.
- **Fallbacks count, retries don't:** a node retried three times is one hop; a node that falls back is one hop and its fallback another.
- **A parallel group and a subgraph count as one**, whatever is inside them; a child's nodes count only against the child's own limit.
- **Interrupt boundary inside the loop:** freeze, resume (the count carries on from `strata.hops`), freeze again, and trip at the exact hop a run without interrupts would.
- **Interrupt nested in a subgraph inside the loop** (StrataOps' shape): every pass freezes inside the child; the parent's count survives each resume, the subgraph node isn't counted twice, and the parent trips at the same hop.
- **A child's own limit:** a child that loops trips at its limit; the parent's result is `NodeFailed` for the subgraph node, fatal, with the child's hop-limit checkpoint nested.
- **Resume of the hop-limit checkpoint:** trips again at once (no node runs, a new hop-limit checkpoint); with a higher per-run limit it continues and counts on from the stored hops.
- **Cancellation wins** over a trip at the same moment; a cancelled checkpoint's `strata.hops` excludes the node it re-enters.
- **A malformed `strata.hops`** refuses the resume without stamping the checkpoint.
- **Per-run override:** lower than the compiled limit trips earlier; on a graph compiled without a limit it sets one; it doesn't reach a subgraph's child.
- **Validation:** `Compile` refuses 0 (and, in .NET, a negative limit); a run refuses a per-run 0.
- **Telemetry and events:** the `HopLimitExceeded` event (path, hops, limit) between the last `NodeCompleted` and `CheckpointSaved`; no `NodeStarted` for the refused node; the run span's `error.type` is `hop_limit`; the counter is incremented with the node path.
- **CLI:** `thread inspect` prints `Hops:` for a checkpoint with `strata.hops`, and nothing for one without. No cross-engine fixture is needed: the count is a metadata string, and the stores and the CLI already round-trip metadata in both directions.
- **Mutation checks:** tripping one hop late, not reading `strata.hops` on resume, and recording a re-entered node's own hop in its checkpoint should each fail tests in both engines.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

### The questions as asked

Recommendations are in bold.

1. **What counts as a hop?** **Every node started at a level, fallbacks included; retries not; a group or subgraph is one.** A retry is the same node's recovery, which `MaxRetries` already bounds; a fallback is a different node, so it is a step of the walk.
2. **Per level or per run?** **Per level.** See "Per level, not per run".
3. **Trip before hop N+1, or after hop N?** **Before hop N+1.** Tripping after hop N would fail a loop that was about to end at N, and checking after the node would mean either running one node too many or refusing a graph's last node.
4. **Metadata key or checkpoint field?** **A metadata key (`strata.hops`).** No schema version, no fixtures, no DDL, every store and the CLI carry it already, and `strata.` was reserved for this in design/18. A field would be cleaner to type, and could be added later as a 1.x bump if the count needs to be queryable.
5. **What does the key mean on each checkpoint?** **Hops before its next node**, so "a resume counts its first node" holds everywhere. The alternative (hops including the node a failed checkpoint stopped at, with a re-entry not counted) needs a rule per checkpoint kind.
6. **Write the key without a limit?** **No.** A graph without a limit stays byte-for-byte as before; the cost is that a limit added later counts mid-flight threads from their next resume.
7. **A new result or `NodeFailed`?** **A new result refining `Failed`.** `NodeFailed` means a node ran and its policy gave up; here no node ran, and `attempts` would be a lie. `error_kind` = `hop_limit` says so on the wire.
8. **A child's trip in the parent.** **The subgraph node fails fatally and nests the child's checkpoint;** the parent's result is `NodeFailed`, not `HopLimitExceeded`. Retrying a child that hit a fail-safe would just spend it again.
9. **A per-run override?** **Yes, outermost level only, replacing (not capping) the compiled limit.** Without it a tripped thread could never go on, and tests couldn't tighten a production graph's limit.
10. **Export the limit in the definition?** **Not now** (see "Not built").

---

## Implementation Status

Built 2026-10-04 in both engines and the CLI, in one change, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **The CLI's `Hops:` line is the outermost level's.** `thread inspect` prints the latest checkpoint's `strata.hops`; a nested level's count is in `--json` (`nested.metadata`). `strata.hops` was already hidden from the caller-metadata line, since the CLI treats every `strata.` key as the engine's.
- **Refusing a bad limit is each engine's idiom.** .NET throws `ArgumentOutOfRangeException` from `Compile` and from the run (it is an `ArgumentException`, like a reserved metadata key). Rust returns `CompilationFailed("max_node_hops must be at least 1")` from `compile_with`, and `Failed(ExecutionAborted("a run's max_node_hops must be at least 1"))` from the run, as it does for a reserved metadata key.
- **Twins that live in different files.** The span and counter check is `HopLimitTests.The_Run_Span_Says_Hop_Limit_And_The_Counter_Names_The_Node` in .NET and `telemetry.rs`'s `a_hop_limit_marks_the_run_span_and_counts_the_refused_node` in Rust, because the Rust tracing subscriber and metrics recorder are process-wide and that binary installs them.

**How it was built:**

- **.NET:**
  - `Graph/CompilationOptions.cs` is new. `StateGraph.Compile()` calls `Compile(new CompilationOptions())`, which checks the limit and passes it to `CompiledGraph` (`MaxNodeHops`).
  - `Graph/HopLimit.cs` holds the `strata.hops` key and its parser (`TryRead`: missing is 0, anything but a non-negative integer is invalid).
  - `LevelScope` gains `Hops`, and its own `CheckpointMetadata` adds `strata.hops` when it is set. Every checkpoint writer now goes through the level's, not the run's. `Child` resets `Hops`, so a child counts its own.
  - `ExecuteFromAsync` takes the starting count. Each pass of its loop derives the node's level (`level with { Hops = hops }`, only when a limit is in effect), checks cancellation, then the limit (`HopLimitAtAsync` writes the checkpoint), then counts the node. The boundary checkpoint records the count including the boundary node.
  - `ResumeCheckpointAsync` passes the stored count; `CheckResume` refuses a malformed one.
  - `SubgraphRunner` maps a child's `HopLimitExceeded` to a fatal `NodeException` that keeps the child's checkpoint.
  - `RunTelemetry.HopLimitExceeded` records the event, the counter and a span event. `RunCompleted` takes an `errorType`, which `ObserveAsync` sets to `hop_limit` for this result.
- **Rust:**
  - `graph.rs`: `CompileOptions` and `compile_with`; `compile()` calls it with the default. `CompiledGraph` gains `max_node_hops` and its accessor.
  - `checkpoint.rs`: `HOPS_KEY`, `stored_hops`, `hop_limit_at` and `hop_limit_at_node`; `execute_from` takes the starting count, and rebinds `run` to a level carrying the count on each pass of its loop.
  - `context.rs`: `RunConfig::with_max_node_hops` (refused at 0 in `validate`), `RunScope::max_node_hops`, and `LevelScope::hops` with `with_hops`; `checkpoint_metadata` adds the key.
  - `lib.rs`: `StrataError::HopLimitExceeded`. `observability.rs`: `RunEventKind::HopLimitExceeded`, `metric_names::HOP_LIMIT_EXCEEDED`, the run span's new `error.type` field, and the message in `error_message`. `subgraph.rs` keeps a tripped child's checkpoint, like .NET.
- **CLI:** `store::hops` and the `Hops:` line in `thread inspect`.

**Tests:**

- **New suites:** `hop_limit.rs` (16) / `HopLimitTests.cs` (19). The counts differ only in shape: .NET's two theories (`A_Loop_That_Ends_At_Or_Before_The_Limit_Is_Untouched`, and `A_Limit_Below_One_Is_Refused`, which also tries -1) are two cases each where Rust loops in one test, and Rust's span and counter test is in `telemetry.rs`.
- **Added elsewhere:** `telemetry.rs`'s `a_hop_limit_marks_the_run_span_and_counts_the_refused_node`, and the CLI's `inspect_shows_the_hop_count_of_a_graph_with_a_hop_limit`.
- **Totals when this landed:** .NET 467 passed, with 38 Postgres/Redis tests skipped because those services weren't running locally (this change touches no store), plus the example's 1. `rust/` 464, `cli/` 58, `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` (all three projects) and `dotnet format --verify-no-changes` are clean.
- **Mutation checks, in both engines:**
  - Tripping one hop late (`hops > limit`) fails 12 Rust and 13 .NET tests.
  - Not reading `strata.hops` on resume (starting from 0) fails 5 in each.
  - Recording a re-entered node's own hop in its checkpoint (`hops + 1`) fails 11 in each.

## Not built (deliberately out of scope)

- **The limit in the exported definition.** `ToDefinition()` / `to_definition()` and `strata graph validate` don't see it. Adding it is a definition `schema_version` change and a validator rule ("a loop with no limit") nobody has asked for yet.
- **A per-run override for nested levels**, or one keyed by subgraph path. The override reaches the outermost graph only.
- **A queryable hop count.** `strata.hops` is in `metadata`, so `ListThreads` can't filter or sort on it, and `NodeContext` doesn't expose it.
- **Per-node visit limits** ("`expand` at most 3 times"). SPEC-004 describes a total per level.
- **Token, cost or time budgets.** StrataOps keeps budgets in its Model Fabric, and `RunOptions.Deadline` / `RunConfig::with_deadline` already bound time.
- **SPEC-004's "missing terminal paths" check**, a separate validation gap.
