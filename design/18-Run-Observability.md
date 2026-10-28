# Datalium Strata: Run Metadata, Run Events and Telemetry

A run is a black box until it returns. The caller can't attach a tenant or an incident to the checkpoints it writes. Nothing reports what is happening during a run, except the `StreamChunk`s of a streaming run, and those carry no run id, timestamp, sequence number or attempt. The engine has no spans or metrics, so a downstream product has to wrap every handler to get them, and from there it can't see retries, checkpoint time or branches. This document specifies three changes. They hook into the same places in the executor and add run-level inputs to the same types, so they are designed and built together:

- **SG-14, run metadata:** caller-supplied key/value pairs, stamped on every checkpoint the run writes and visible to every node.
- **SG-20, run events:** a typed event stream with observer hooks that works under every run mode.
- **SG-19, telemetry:** OpenTelemetry-compatible spans and metrics from the engine itself. .NET uses `ActivitySource` and `Meter`. Rust uses the `tracing` and `metrics` crates.

The SG numbers refer to the gap log of StrataOps (not public yet). Both engines get every capability in one change (strict parity, StrataOps ADR 0003). The APIs are idiomatic in each language, not identical.

It was written as a blueprint before implementation, reviewed, and is now built. "Review decisions" settles the questions the review was asked, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Where metadata lives on the wire | **In the existing `metadata` map**, beside the engine's keys. The engine reserves the names `error`, `error_kind`, `attempts`, `cancel_reason`, and every key that starts with `strata.`. A run whose metadata uses a reserved name is refused before it starts. **No wire-format change, no `schema_version` bump and no DDL change.** |
| Which checkpoints carry it | Every checkpoint the run writes (interrupted, failed, cancelled) at every nesting level. A resume carries forward the metadata of the checkpoint it resumes, and the resume's own metadata overrides it key by key. |
| Run id | New. Every `run` / `resume` call gets one, shared by every depth of that call. It is a UUID v7 by default, or the caller supplies it. It goes on events, spans and `NodeContext`, **not** on checkpoints. |
| Observer shape | One sink in `RunOptions` / `RunConfig`: `IRunObserver` / `RunObserver`, with a single synchronous `OnEvent` / `on_event` method. A provided channel adapter turns it into an `IAsyncEnumerable` / a tokio receiver. No new entry points. |
| Can an observer stall or fail the run? | **No.** Events go into an unbounded per-run queue and are delivered by a separate pump, never on the executor. An exception or panic from the observer is caught, counted, and delivery carries on. |
| Ordering | Every event has a per-run sequence number, starting at 1. Events are delivered one at a time in sequence order. `RunCompleted` is always the last event. |
| Events vs `StreamChunk`s | **Separate channels.** Chunks are unchanged. Events exist in every run mode. Chunks still exist only when streaming, and still carry tokens, which events never do. |
| State diff or summary hook | **Not built.** Events carry no state. See "State in events". |
| Span tree | Run → node → attempt. A branch is a span under its group's attempt. A subgraph's nodes nest under the subgraph node's attempt. Handler code is the current span (`Activity.Current` / the current `tracing` span) while it runs. |
| How SG-19 relates to SG-20 | One internal hook layer drives both, at the same call sites. Spans can't be derived from the event stream, because a span has to be current *on the executor* for handler spans to nest under it. |
| Metrics | Eight instruments, named so that OpenTelemetry's Prometheus exporter produces the names in StrataOps Arch §16 where Strata has the concept. See "Metrics". |
| Logs | .NET: none in the core, because `ILogger` isn't BCL. Activity events cover it. Rust: `tracing` events, which are logs in that ecosystem. |
| Cost when nothing listens | No observer, no listener, no subscriber and no recorder means no event objects are allocated and no spans are recorded. What remains is a few branch-predictable checks per node. |
| Not in scope | Graph identity (SG-13), retry backoff (SG-04, though `RetryScheduled` has the field it will fill), dynamic fan-out (SG-06, though the event set leaves room for `MapExpanded`), an async queryable store (SG-11), tool dispatch (SG-21), hosting extensions (SG-28). |

---

## Run metadata (SG-14)

```csharp
var result = await graph.RunAsync("inc-284891", state, store, new RunOptions
{
    Metadata = { ["tenant"] = "acme", ["incident"] = "INC-284891" },
});
```

```rust
let config = RunConfig::default()
    .with_metadata("tenant", "acme")
    .with_metadata("incident", "INC-284891");
graph.run_with("inc-284891", state, store, config).await;
```

| .NET | Rust |
|---|---|
| `RunOptions.Metadata`: a `Dictionary<string, string>` that starts empty, with `init`. The collection-initializer syntax above works. The engine copies it when the run starts, so changing the dictionary afterwards has no effect. | `RunConfig::with_metadata(key, value)`, which can be repeated. Setting a key again replaces its value. |
| `NodeContext.Metadata`: `IReadOnlyDictionary<string, string>`, empty when there is none. | `NodeContext::metadata() -> &HashMap<String, String>`, plus `with_metadata` for hand-built test contexts. |

### Reserved keys

The engine already writes four keys into `metadata`: `error`, `error_kind` and `attempts` on `failed` checkpoints, and `cancel_reason` on `cancelled` ones. Caller keys share the map with them.

- **Reserved names:** the four keys above, and **any key that starts with `strata.`**. The prefix keeps room for future engine keys, such as a trace parent or SG-13's graph identity, so adding one never collides with a caller's key. The four existing keys keep their unprefixed names because they are already on the wire.
- **Also refused:** an empty key.
- **When it is checked:** when a run or resume starts, before anything is loaded or written. .NET throws `ArgumentException` naming the key. Rust returns `GraphExecutionResult::Failed` with the message `run metadata key '<key>' is reserved`, because `RunConfig`'s builder returns `Self` and can't fail mid-chain. That is the same reason Rust reports an invalid timeout at `compile()` (design/17).
- **On the checkpoint:** the caller's keys and the engine's keys for that checkpoint are merged. They can't collide.
- **No metadata:** an interrupted checkpoint written without caller metadata still has `metadata: null`, as today. Every existing fixture and log stays valid.

Why not a separate field? A separate field (for example `labels`) would be cleaner to read, but it costs a `schema_version` bump, a new column and migration in four SQL stores, new fixtures, and a CLI change. And a runtime from before this change would refuse to resume any checkpoint that has it. The shared map costs none of that. An older runtime can still resume such a checkpoint, and simply doesn't carry the metadata forward. A store that later filters by metadata (SG-11) filters the same JSON in either layout.

### Which checkpoints carry it

- **Every checkpoint the run writes:** an interrupt boundary, a freeze around a subgraph's child, a `failed` checkpoint and a `cancelled` one. This holds at **every nesting level**, because each level builds its checkpoint from the same run scope. A nested child's checkpoint carries the same caller keys as its parent, plus its own engine keys.
- **The `resumed_at` stamp** re-saves the checkpoint being resumed unchanged apart from `resumed_at`. It keeps the metadata it was written with.
- **Through a resume:** a resume reads the caller keys from the outermost checkpoint it resumes (every key that isn't reserved). It then applies its own `RunOptions.Metadata` / `RunConfig` metadata over them, key by key. The result is the run's metadata: what `NodeContext` sees and what the resumed run's checkpoints carry. A resume can add or change keys but can't remove one. So a caller that passes `tenant` once, on the first run, has it on every checkpoint of the thread from then on.

### Storage and the CLI

`metadata` is already `TEXT` (JSON) in every SQL layout, and JSON in Redis and in memory. **No store changes.** The CLI shows caller metadata and can filter by it:

- `thread inspect` prints `Metadata: incident=INC-284891, tenant=acme`, with caller keys only, sorted. The engine's keys are left out (a cancelled thread keeps its `Reason:` line). `--json` already carries the whole map.
- `thread list --meta key=value` filters by the latest checkpoint's metadata. The flag can be repeated, and every pair must match.

---

## Run events (SG-20)

```csharp
public interface IRunObserver
{
    /// Called once per event, in sequence order, never concurrently for one run, and never on the
    /// executor. Must be thread-safe if it is shared between runs. Exceptions are caught and counted.
    void OnEvent(RunEvent runEvent);
}

var events = new RunEventChannel();                       // one per run
var completion = graph.RunAsync(id, state, store, new RunOptions { Observer = events });
await foreach (var e in events.Reader.ReadAllAsync())     // ends after RunCompleted
    timeline.Add(e);
```

```rust
pub trait RunObserver: Send + Sync + 'static {
    /// As in .NET. Called on a pump task, so it must not block; hand slow work to a channel.
    fn on_event(&self, event: &RunEvent);
}

let (observer, mut events) = RunEventChannel::new();     // events: tokio UnboundedReceiver<RunEvent>
let run = graph.run_with(id, state, store, RunConfig::default().with_observer(observer));
```

`RunOptions.Observer` / `RunConfig::with_observer` takes a single observer. A caller that needs several composes them. The **channel adapter** (`RunEventChannel` in both engines) is an observer whose reader completes once it has delivered `RunCompleted`, so `await foreach` / `while let Some(e) = events.recv().await` ends with the run. It serves one run. Events from a second run are refused, and that refusal is counted as an observer error. A long-lived observer that publishes every run's events (to Kafka, for example) implements the interface directly.

### Every event carries

| Field | Meaning |
|---|---|
| **Thread id** | The caller's thread id, at every depth, as `NodeContext.ThreadId` gives it. |
| **Run id** | This `run` / `resume` call's id (see "Run id"). |
| **Sequence** | 1, 2, 3, … per run, with no gaps. It is assigned when the engine records the event, under the same lock that queues it, so delivery order is sequence order even with concurrent branches. |
| **Timestamp** | When the engine recorded the event (UTC). `DateTimeOffset` / `chrono::DateTime<Utc>`. |
| **Node path** | The qualified path (`"Work/Send"`, `"Gather/Acme"`), the same string `NodeContext.NodePath` and stream chunks use. It is absent on run-level events. |
| **Attempt** | The node's attempt at that moment (see the table below). It is absent on run-level events. A branch reports its group's attempt. |

### The events

| Event | When | Payload |
|---|---|---|
| `RunStarted` | First, every run. For a resume, it comes after the checkpoint is loaded, so it can carry the effective metadata. | Mode (`Run` / `Resume`), the node the run starts at, the run's metadata. |
| `Resumed` | A resume only, after the checkpoint has been stamped `resumed_at`. | The status resumed from (`interrupted` / `failed` / `cancelled`), and its `waiting_at` path. Since `21-Typed-Human-Signals.md`, also the innermost signal's `is_approved` and reviewer, and the reject route if this resume took one. |
| `NodeStarted` | A node is about to run (attempt 1). | None. |
| `RetryScheduled` | An attempt failed transiently and will run again. | Error message, whether it was a timeout, and the delay before the next attempt (the policy's backoff since SG-04, `design/25`; zero without one). Attempt is the one that failed. |
| `BranchStarted` | A parallel group launches a branch. | None. |
| `BranchCompleted` | That branch settles, or the group gives up on it. | Result (`Succeeded` / `Failed` / `Cancelled`), duration, the error if it failed, and since SG-07/08 (`design/26`) a reason when the result alone doesn't say: `TimedOut`, `LostRace` or `SiblingFailed`. |
| `JoinDecided` | A group whose join isn't `All` decided it (`design/26`). | The policy, whether it was met, and the deciding branches in completion order. |
| `NodeCompleted` | A node is finished with, however it ended. | Result (`Committed` / `Failed` / `Interrupted` / `Cancelled`), duration (including every attempt), the error if it failed, and the fallback node if the policy routes to one. Attempt is the last one made. |
| `CheckpointSaved` | After every successful save to the caller's store. | Status, the `waiting_at` path, whether it was the `resumed_at` stamp, and the save's duration. |
| `Interrupted` | After the frozen checkpoint is saved. | The `waiting_at` path and the next node. |
| `RunCompleted` | Last, every run. | Outcome (`Completed` / `Interrupted` / `Failed` / `Cancelled`), the run's duration, the error message for `Failed`, and the reason for `Cancelled`. |

Rules:

- **Pairs always close.** Every `NodeStarted` gets exactly one `NodeCompleted`, and every `BranchStarted` exactly one `BranchCompleted`, including for a node that failed, was cancelled or froze around its child, and for a branch that was abandoned. This is deliberately more regular than the chunks, where a failed node gets no `NodeEnd`. A branch the group gives up on is `Cancelled` at that moment, even if its sync handler is still running. The only exception is a Rust run future that is dropped (design/17: not a cancellation): its events stop where they are, with no `RunCompleted`.
- **Fallbacks:** a node whose policy falls back ends `NodeCompleted(Failed)`, naming the fallback node, and the fallback's own `NodeStarted` follows.
- **Subgraphs** add no event types. The subgraph node gets `NodeStarted` / `NodeCompleted` at its own path, and its child's nodes appear inside that pair with paths like `"Work/Send"`, to any depth. A child level never emits `RunStarted`, `RunCompleted`, `CheckpointSaved` or `Interrupted`. Its saves go to the subgraph's capturing store, which is not the caller's store. The run's single `Interrupted` names the innermost boundary in `waiting_at`.
- **Refused nodes:** a node the run refuses (a streaming node under a plain run, or a node that doesn't exist) gets `NodeStarted` and `NodeCompleted(Failed)`.
- **Bugs:** an exception or panic from a handler that propagates out of the run still produces `NodeCompleted(Failed)` and `RunCompleted(Failed)` before it propagates. .NET rethrows the exception unchanged. Rust resumes the unwind.

A flat run with one retry and a boundary:

```text
 1 RunStarted       mode=Run node=Classify metadata={tenant: acme}
 2 NodeStarted      Classify#1
 3 RetryScheduled   Classify#1 "gateway timeout" delay=0
 4 NodeCompleted    Classify#2 Committed 1.84 s
 5 NodeStarted      Approve#1
 6 NodeCompleted    Approve#1 Committed 0.01 s
 7 CheckpointSaved  interrupted waiting_at=[Approve] 3 ms
 8 Interrupted      waiting_at=[Approve] next=Execute
 9 RunCompleted     Interrupted 1.86 s
```

### Delivery

- **Off the executor.** Recording an event is a lock, an increment and an enqueue. A per-run pump (.NET: one `Task`; Rust: one spawned task) delivers events to the observer. The pump only exists when an observer is set. In .NET it starts with `ExecutionContext` flow suppressed, so observer code doesn't run inside the run's `Activity`.
- **Unbounded.** A slow observer makes its own queue grow. It never makes the run wait. A run records roughly 2 to 4 events per node, so an unbounded queue is a deliberate trade: completeness over a memory bound. A bounded queue would have had to either block the run or drop events, and either one breaks a guarantee above.
- **Isolated.** Each call is wrapped: `try/catch` in .NET, `catch_unwind` in Rust. A failure increments `strata.observer.errors`, records a span event on the run span, and delivery continues with the next event.
- **Late events are dropped.** An event recorded after `RunCompleted` is never delivered, by any observer or `RunEventChannel`: the run's queue is closed. Only work that outlives its run can record one, and in .NET that happens: an abandoned handler (design/17) that goes on to call a tool or a model, and **a model route's hedge losers**. A route's winner doesn't wait for its losers (design/46), so a loser's `ModelCallCompleted` can come after `RunCompleted` and be missing. The route's `ModelRouted` event, recorded before the call returns, lists every attempt with its outcome (`LostRace`), and is the one to read. In Rust a loser's future is dropped before the route returns, so its events always arrive; an abandoned sync handler can still record late.
- **The run doesn't wait for delivery.** `RunAsync` / `run_with` return as soon as the run is over. The observer may still be receiving its last events. A caller that needs "every event delivered" waits for `RunCompleted` on the observer side. That is what the channel adapter's completion is for.

### Run id

A string. By default a fresh UUID v7 per `run` / `resume` call (.NET `Guid.CreateVersion7()`, Rust the `uuid` crate with the `v7` feature), so ids sort by start time. `RunOptions.RunId` / `RunConfig::with_run_id` supplies one instead, for example StrataOps' execution id. The engine doesn't check that it is unique. A subgraph's child shares its parent's run id, because it is the same run. A resume is a new run with a new id, and the thread id is what connects them.

The run id is not written to checkpoints. It would make every produced fixture differ between runs, and nothing reads it there yet. The reserved `strata.` prefix keeps room to add `strata.run_id` later without a format change.

### State in events

Events carry no state, no diff and no summary. A diff of an arbitrary `TState` means serializing it twice per node, which is the dominant cost of a cheap node, and it would push domain state (evidence, PII) into every telemetry pipeline by default. StrataOps' per-incident summary (tokens, cost, tool calls) is known to the handlers that make those calls, not to a state diff. When it is needed, the likely shape is a node annotation on `NodeContext`, carried on `NodeCompleted` and the attempt span, together with SG-21 / SG-23. It is not built here. `RunEvent` is open for extension, so adding it later is not breaking.

### Room for `MapExpanded` (SG-06)

`RunEvent` is an abstract record that other code can't subclass (.NET), and `RunEventKind` is `#[non_exhaustive]` (Rust). So consumers already need a default arm, and adding `MapExpanded(items)` with per-item paths (`"<map>/<index>"`) is not a breaking change. The branch events are the model for a map item's lifecycle.

### Shapes

```csharp
public abstract record RunEvent
{
    private protected RunEvent() { }                          // closed: only the engine's events exist
    public required string ThreadId { get; init; }
    public required string RunId { get; init; }
    public required long Sequence { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public string? NodePath { get; init; }
    public int? Attempt { get; init; }
}
public sealed record RunStarted(RunMode Mode, string Node, IReadOnlyDictionary<string, string> Metadata) : RunEvent;
public sealed record NodeCompleted(NodeResult Result, TimeSpan Duration, string? Error, string? Fallback) : RunEvent;
// … one sealed record per row of the table above
```

```rust
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct RunEvent {
    pub thread_id: Arc<str>,
    pub run_id: Arc<str>,
    pub sequence: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub node_path: Option<Arc<str>>,
    pub attempt: Option<u32>,
    pub kind: RunEventKind,
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum RunEventKind {
    RunStarted { mode: RunMode, node: String, metadata: HashMap<String, String> },
    NodeCompleted { result: NodeResult, duration: Duration, error: Option<String>, fallback: Option<String> },
    // … one variant per row of the table above
}
```

---

## Telemetry (SG-19)

### Why it shares SG-20's hooks rather than consuming its events

For an LLM client's or `HttpClient`'s own span to land under the right Strata span, the attempt span has to be **current on the executor's flow when the handler is invoked**: `Activity.Current` (an `AsyncLocal`) in .NET, and the instrumented future or entered span in Rust. An observer runs later, on a pump, so it can't do that. Instead, one internal per-run component (`RunTelemetry`) has a method for each lifecycle point. The executor calls it once at each point, and each call records the event (if there is an observer), starts or ends the span, and updates the metrics. Events, spans and metrics come from the same call, so they can't disagree on what happened or how many times.

### Spans

| Span | .NET `OperationName` / Rust span name | Display name (`DisplayName` / `otel.name`) | Parent |
|---|---|---|---|
| Run | `strata.run` | `strata.run` or `strata.resume` | Whatever is current when `RunAsync` / `run_with` is called. |
| Node | `strata.node` | the node path | Run span, or the enclosing subgraph's attempt span. |
| Attempt | `strata.attempt` | `<node path> #<n>` | Node span. |
| Branch | `strata.branch` | the branch path (`Gather/Acme`) | The group's attempt span. |

- **Subgraphs** need no span kind of their own. The child's node spans are children of the subgraph node's attempt span, which is the subgraph. That gives the Arch §17 tree: incident (caller) → `strata.run` → `Kubernetes Branch` → … .
- **Handler code runs inside its attempt span** (or branch span). .NET: the activity is started on the executor's flow before the handler is invoked, and `Task.Run` / `LongRunning` carry it to sync handlers. Rust: async handler futures are `.instrument`ed, and sync handlers enter the span inside `spawn_blocking`. Spawned branch futures are instrumented explicitly, because `JoinSet::spawn` doesn't carry the current span.
- **Exposed on the context:** `NodeContext.Activity` (`Activity?`, null when nothing listens) and `NodeContext::span() -> &tracing::Span`.
- **Abandoned work:** a span ends when the engine stops waiting, not when an abandoned handler finally returns. Spans the handler starts afterwards still parent correctly, but start after their parent ends. OTel allows that.
- **Streaming runs:** the run span is created before the background task or `tokio::spawn` starts and is carried into it, so `RunStreaming` / `run_streaming` parent like `RunAsync`.

**Attributes.** Names follow OpenTelemetry conventions. `error.type` is the semantic-convention attribute.

| Attribute | On | Value |
|---|---|---|
| `strata.thread_id`, `strata.run_id` | every span | |
| `strata.run.mode` | run | `run` / `resume` |
| `strata.metadata.<key>` (.NET) / `strata.metadata` (Rust) | run | The run's metadata. Each key is its own attribute in .NET. Rust has one attribute holding the map as JSON, because `tracing` span fields are fixed when the span is created. |
| `strata.outcome` | run | `completed` / `interrupted` / `failed` / `cancelled` |
| `strata.cancel_reason` | run | when cancelled |
| `strata.node.path` | node, attempt, branch | |
| `strata.attempt` | attempt, branch | 1-based |
| `strata.retry_count` | node | attempts − 1 |
| `strata.node.result` | node | `committed` / `failed` / `interrupted` / `cancelled` |
| `error.type` | failed node, attempt, branch | `transient` / `fatal` / `timeout`, or the exception type for a bug |

A failed attempt, node, branch or run has status `Error` with the message as its description. A cancelled one doesn't: a cancellation isn't an error. **Span events:** `retry` on the node span (message, delay) and `checkpoint.saved` on the run span (status, duration). In Rust these are `tracing` events inside the span. In .NET they are `ActivityEvent`s.

Graph id and version (Arch §16) wait for SG-13, which will add `strata.graph.id` / `strata.graph.version`. Queue latency waits for a scheduler (SG-25).

### Metrics

There are eight instruments, each `strata.*`. Through OpenTelemetry's Prometheus exporter, the dotted name, the unit and the `_total` suffix produce the names in the right-hand column. Four of them are exactly the names StrataOps Arch §16 already uses.

| Instrument | Type, unit | Attributes | Prometheus | Why |
|---|---|---|---|---|
| `strata.graphs.started` | counter, `{run}` | `strata.run.mode` | `strata_graphs_started_total` | Arch §16. Throughput. |
| `strata.graphs.active` | up-down counter, `{run}` | | `strata_graphs_active` | Arch §16. Concurrency in the process. Suspended threads aren't active, so this is the "zero resources while suspended" claim, measured. |
| `strata.graph.duration` | histogram, `s` | `strata.run.mode`, `strata.outcome` | `strata_graph_duration_seconds` | Run latency by outcome. |
| `strata.node.duration` | histogram, `s` | `strata.node.path`, `strata.node.result` | `strata_node_duration_seconds` | Where the time goes. |
| `strata.node.retries` | counter, `{retry}` | `strata.node.path`, `error.type` | `strata_node_retries_total` | Flaky dependencies. |
| `strata.graph.transition` | counter, `{transition}` | `strata.node.path` (from), `strata.next_node.path` | `strata_graph_transition_total` | Arch §16. Edge traffic, which drives the live graph's heat map. |
| `strata.checkpoint.latency` | histogram, `s` | `strata.checkpoint.operation` (`save` / `load`), `strata.checkpoint.status` | `strata_checkpoint_latency_seconds` | Arch §16. Store health: the wait the run actually sees. Since SG-11 (`design/19-Async-Queryable-Store.md`) the store is awaited directly, so in Rust it no longer includes a hop to the blocking pool, except inside the SQLite adapter, where the hop is part of the store's own cost. |
| `strata.observer.errors` | counter, `{error}` | | `strata_observer_errors_total` | Makes a broken observer visible, since it can't fail the run. |

- **No thread id, run id or metadata in metric attributes.** They are unbounded. A node path is bounded by the size of the graph. Tenant as a metric dimension is an open question below.
- **The rest of the Arch §16 list** belongs to work that doesn't exist yet: agent tasks, scheduler latency and queue depth (SG-25), model and tool requests (SG-21 / SG-23), worker saturation and backpressure (SG-25 / SG-26).
- **.NET:** one static `Meter("Datalium.Strata", <assembly version>)` and one static `ActivitySource` with the same name and version. Both are BCL (`System.Diagnostics`). Listening is enabled with `AddSource("Datalium.Strata")` / `AddMeter("Datalium.Strata")`. An `AddStrataInstrumentation()` extension that does exactly that is SG-28.
- **Rust:** the `metrics` facade (`counter!` / `gauge!` / `histogram!`, with `describe_*` for units and descriptions). It does nothing until the application installs a recorder. Instruments use the same dotted names and units as .NET. How they are finally rendered depends on the installed exporter, which is an open question below.

### Logs

- **.NET:** no `ILogger` in the core. `Microsoft.Extensions.Logging.Abstractions` isn't BCL, and span events plus run events already carry what a log would. A hosting package (SG-28) can bridge events to `ILogger`.
- **Rust:** `tracing` events under target `datalium_strata`, which in that ecosystem are both logs and span events:
  - `info`: run started and run completed
  - `warn`: retry scheduled, node failed, observer failed
  - `debug`: checkpoint saved

---

## Run options and node context, after this change

```csharp
public sealed class RunOptions
{
    public IHistoryStore? History { get; init; }
    public IServiceProvider? Services { get; init; }
    public DateTimeOffset? Deadline { get; init; }
    public Dictionary<string, string> Metadata { get; init; } = new();   // SG-14
    public string? RunId { get; init; }                                   // SG-20
    public IRunObserver? Observer { get; init; }                          // SG-20
}

public sealed class NodeContext : IExecutionContext
{
    // … existing members …
    public string RunId { get; init; } = "";                                         // SG-20
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = Empty;     // SG-14
    public Activity? Activity { get; init; }                                          // SG-19
}
```

```rust
impl RunConfig {
    pub fn with_metadata(self, key: impl Into<String>, value: impl Into<String>) -> Self;
    pub fn with_run_id(self, run_id: impl Into<String>) -> Self;
    pub fn with_observer(self, observer: Arc<dyn RunObserver>) -> Self;
}

impl NodeContext {
    pub fn run_id(&self) -> &str;
    pub fn metadata(&self) -> &HashMap<String, String>;
    pub fn span(&self) -> &tracing::Span;
    // + with_run_id / with_metadata / with_span for hand-built test contexts
}
```

The new `NodeContext` members are optional (`init` with defaults) in .NET, so `new NodeContext { ThreadId = …, Node = …, NodePath = … }` in existing tests still compiles. The run id, metadata and observer belong to the run scope, which `NodeContext` already shares behind one reference, so a node's context costs no more to build than it does today.

---

## Wire format and fixtures

There are **no schema changes**: the `metadata` map, `schema_version` and every column are unchanged. `schema/checkpoint.schema.json`'s `metadata` description is rewritten to document the reserved keys and caller metadata. Its current mention of "approver id, notes, field overrides injected on resume" is dropped, because nothing writes those.

The handoff asks for shared fixtures for anything that touches what checkpoints carry. These two are added to `schema/fixtures/checkpoint-lifecycle/`, and like the existing ones, each is **produced** by a live run in both engines (compared at every level, timestamps aside) and **resumed** by both:

- `metadata-nested.json`: a run with `tenant` and `incident` that froze inside a subgraph. The caller keys are at both levels, and `metadata` is no longer `null` on an interrupted checkpoint. The resume test also checks that both keys reach the resumed run's `NodeContext` and its next checkpoint.
- `metadata-failed.json`: a failed subgraph child. Caller keys merge with `error` / `error_kind` / `attempts` at both levels.

Run events and telemetry are not a wire format. They are in-process objects and spans, and StrataOps serializes events into its own Kafka contract. So they get no schema. The cross-language check for them is the mirrored event-sequence tests (below), which assert the same sequence for the same graph in both engines.

---

## Migration

**.NET** (additive, source-compatible):

- New members on `RunOptions` and `NodeContext`, and new types `IRunObserver`, `RunEvent` and its records, and `RunEventChannel`.
- The engine now emits activities and metrics. An application already listening to every source (`AddSource("*")`) will see them.

**Rust** (additive):

- New `RunConfig` and `NodeContext` methods, and new types `RunObserver`, `RunEvent`, `RunEventKind` and `RunEventChannel`. `GraphExecutionResult` is unchanged.
- New dependencies: `tracing`, `metrics`, and `uuid` (features `v7`). `tracing::Span` appears in `NodeContext`'s public API, so `tracing` 0.1 becomes a public dependency.
- An application with a `tracing` subscriber now sees Strata's spans and events. It can filter them with `datalium_strata=warn`.

**Downstream:**

- StrataOps bumps `strata.lock` in its own PR. Its `ExecutionEventPublisher` becomes an `IRunObserver`, its `StrataInstrumentation` becomes `AddSource` / `AddMeter`, and its thread-id convention and side table give way to `RunOptions.Metadata`.
- The demo repos keep compiling. A follow-up demo could show the event timeline.

---

## Tests

Mirrored per engine, as for every feature so far:

- `run_metadata.rs` / `RunMetadataTests.cs`:
  - metadata on every checkpoint kind, flat and nested, merged with the engine keys
  - reserved names and the `strata.` prefix refused before anything is written
  - carried through a resume, and overridden key by key
  - `NodeContext.Metadata` at every depth
  - the `resumed_at` stamp leaves metadata alone
- `run_events.rs` / `RunEventsTests.cs`:
  - the exact sequence for a reference graph: a retry, a fallback, a parallel group with one failing branch, a subgraph with a nested boundary, then its resume
  - the same assertions under the plain and streaming modes
  - sequence numbers gapless and ordered under concurrent branches
  - pairs always close: failure, cancellation, timeout, abandoned sync branch, refused streaming node
  - `RunCompleted` last
  - a throwing or panicking observer doesn't affect the result and is counted
  - a blocked observer doesn't delay the run, which completes while the observer is still parked
  - the channel adapter completes
  - a caller-supplied run id
- `telemetry.rs` / `TelemetryTests.cs`:
  - with an `ActivityListener` / a test `tracing` subscriber layer: the span tree for the reference graph, parenting of a handler's own span under its attempt (async, sync and branch), and attributes and statuses
  - with a `MeterListener` / a test `metrics` recorder: each instrument's values and attributes for the reference graph
  - nothing allocated per event when no observer is set (.NET: allocation count around a run; Rust: the event path not taken)
- Fixture parity for `metadata-nested.json` and `metadata-failed.json`.
- CLI: `thread inspect` metadata line and `thread list --meta`, seeded from the new fixtures.
- **Mutation checks to run:**
  - emit events inline on the executor instead of via the pump (the blocked-observer test fails)
  - drop `NodeCompleted` on the cancellation path (the pairs test fails)
  - stamp metadata only at the top level (the nested fixture fails)

---

## Review decisions

Reviewed 2026-10-02. Every recommendation above was accepted:

1. **Rust metrics:** the `metrics` facade, with the same dotted names and units as .NET (`observability::metric_names`). How a recorder renders them is the exporter's business.
2. **Tenant on metrics:** not now. Metadata stays off metric attributes.
3. **The `resumed_at` stamp:** reported as `CheckpointSaved`, with `ResumeStamp` / `resume_stamp` set.
4. **Attempt span names:** `<node path> #<n>`, with the number also in `strata.attempt`.
5. **Trace links across a freeze:** not now.

---

## Implementation Status

Built 2026-10-02 in both engines, in one change, as specified above. These are the differences and details.

**Where it diverged, and why:**

- **A failing observer is counted, not recorded on the run span.** By the time an observer fails, the run span has often ended, because delivery runs behind the run. .NET only increments `strata.observer.errors`. Rust increments it and also logs a `warn` event ("a run observer panicked") with no parent span.
- **Rust spans end when their last handle drops.** A `tracing` span closes only when every clone of it is gone. An abandoned sync handler still holds its attempt's span, through `in_scope` and its `NodeContext`. So in Rust that span ends when the handler finally returns, not when the engine gives up on it. Events and metrics are unaffected, because they come from the hook calls, not from span lifetimes. .NET ends the `Activity` when the engine stops waiting, as specified.
- **Two Rust events use the module's default target.** `tracing`'s event macros can't combine `target:` and `parent:` with dotted field names. So the "checkpoint saved" and "retry scheduled" events keep their explicit `parent:` (so they attach to the run span and the node span) and log under `datalium_strata::observability`. A `datalium_strata` filter still covers them.
- **`thread inspect` has no `Error:` line.** That line never existed. The `Metadata:` line sits beside `Reason:`, the only engine key the human-readable output shows. `--json` carries the whole map.
- **The internal `NodeOutcome` record was not renamed.** The public enum is `NodeResult`, so nothing clashes and there was nothing to rename. The Rust `NodeScope` also carries the node's local name, so `run_attempts` stays within clippy's argument limit.
- **No allocation-count test.** An allocation count around an async run isn't deterministic in .NET (thread-pool and timer allocations land on the same thread). The guarantee is structural instead: every event is built behind an `Observed` check (`events.record(|| …)` in Rust), and the inline-delivery mutation below shows that events are only recorded when an observer is set.

**How it was built:**

- **.NET:**
  - `Observability/RunEvent.cs` and `IRunObserver.cs` (with `RunEventChannel`) hold the public types.
  - `StrataTelemetry.cs` holds the `ActivitySource`, the `Meter`, the eight instruments and the public `StrataTelemetry.Name`.
  - `RunTelemetry.cs` holds the per-run hook layer (`RunTelemetry`, `NodeScope`, `BranchScope`) and `ObservedStore`, which wraps the caller's store at the entry point so that only real writes are timed and reported. A subgraph's capturing store is never wrapped.
  - `RunEventQueue.cs` is the unbounded channel and its pump, started with `ExecutionContext` flow suppressed.
  - `RunMetadata.cs` holds the reserved keys, validation, the resume merge and the checkpoint merge.
  - In the executor, `ObserveAsync` ends every entry point's run. `RunAttemptsAsync` opens an attempt span around `AttemptOnceAsync` (the old attempt `switch`, now returning an `AttemptResult`). `ExecuteFromAsync` opens a `NodeScope` per node.
  - `ParallelGroupRunner` starts each branch's span, launches the branch inside it, restores the group's `Activity.Current`, and settles every branch's scope when the group sees it end or gives up on it.
- **Rust:**
  - `observability.rs` holds the public types (`RunEvent`, `RunEventKind`, `RunObserver`, `RunEventChannel`, `metric_names`) and the crate-private hook layer (`RunTelemetry`, `NodeScope`, `BranchScope`, `run_metadata`).
  - `CatchUnwind` is a small future adapter. With it, a panic ends the attempt, the node and the run before the unwind resumes, whoever drives the future. Tokio would otherwise catch a spawned task's panic before any drop guard could tell.
  - `RunTelemetry`'s `Drop` stops counting a dropped run as active, without recording anything.
  - `save_checkpoint` / `load_checkpoint` time the store calls. Only the root level reports `CheckpointSaved`.
  - `on_blocking_pool` enters the current span inside `spawn_blocking`, so a sync handler's own spans nest under its attempt. The span-tree test found this gap.
  - `run_branches` catches each branch's panic inside its task, so it knows which branch failed. A `BranchScope` dropped unfinished, because the group gave up or its future was dropped, ends as `Cancelled`.
  - `run_streaming_with` / `resume_streaming_with` spawn with `.in_current_span()`, so the run span parents to the caller's.
  - New dependencies are `tracing`, `metrics` and `uuid` (`v7`). The new dev-dependencies are `tracing-subscriber` (`registry`) and `metrics-util` (`debugging`).
- **CLI:** `thread inspect` prints the caller's metadata. `thread list --meta key=value`, which can be repeated, filters on it. The CLI keeps its own copy of the reserved-key rule.
- **Schema:** the `metadata` description in `checkpoint.schema.json` documents caller metadata and the reserved keys. `schema/README.md` lists the two new fixtures.

**Tests:**

- **New suites, twins in each engine:**
  - `RunMetadataTests.cs` (21) / `run_metadata.rs` (16). Rust tests the six reserved names in one loop, where .NET has a six-case theory.
  - `RunEventsTests.cs` (16) / `run_events.rs` (16). Both describe events as the same strings, so the asserted sequences are identical line for line. Rust's panic test replaces .NET's bug-exception test.
  - `TelemetryTests.cs` (5) / `telemetry.rs` (5). .NET uses an `ActivityListener` and a `MeterListener`. Rust uses a capturing `tracing` layer and `metrics-util`'s `DebuggingRecorder`, installed once for the test binary. Its two metric tests take turns, because snapshots drain the recorder.
- **CLI:** `a_thread_shows_its_caller_metadata_and_can_be_listed_by_it` seeds both new fixtures.
- **Totals when this landed:** .NET 336, with 28 needing live Postgres/Redis and skipped here. `rust/` 305, `cli/` 36, `examples/rust-agent` 1.
- **Mutation checks, in both engines:**
  - Delivering events inline on the executor fails 4 tests in .NET and 10 in Rust (the blocked-observer test among them).
  - Dropping `NodeCompleted` on the cancellation path fails 1 in each.
  - Stamping metadata only at the top level fails 3 in each (the nested fixtures and the resume-stamp test).

## Not built (deliberately out of scope)

- ~~**Graph identity** (SG-13), for `strata.graph.id` / `strata.graph.version`~~ Built with SG-13 (`design/31-Graph-Identity.md`): the run span carries both, and `RunStarted` a `Graph`. A graph label on metrics is still not built.
- ~~**Retry backoff** (SG-04). `RetryScheduled.Delay` / `delay` is always zero until then.~~ Built 2026-10-05 (`design/25-Retry-Backoff.md`): the delay and `strata.retry.delay_ms` are the real wait.
- ~~**`MapExpanded`** (SG-06). `RunEvent` / `RunEventKind` leave room for it.~~ Built 2026-10-06 (`27-Map-Fan-Out.md`): `MapExpanded { Keys }` at the map's path, once per attempt; items are named by key (`<map>/<key>`), not by index, and reuse `BranchStarted` / `BranchCompleted`.
- **A state diff or node annotations on events.** See "State in events".
- **`AddStrataInstrumentation()`** (SG-28). Use `AddSource(StrataTelemetry.Name)` / `AddMeter(StrataTelemetry.Name)`.
- **`ILogger` in the .NET core**, and the Arch §16 metrics that need a scheduler or tool layer (SG-21, SG-23, SG-25).
- **The run id on checkpoints, and trace links across a freeze.** The `strata.` prefix keeps room for both.
