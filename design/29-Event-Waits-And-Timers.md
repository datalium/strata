# Datalium Strata: Event Waits and Durable Timers

A thread can stop and wait in exactly one way today: at an interrupt boundary, for a human (`design/16`, `design/21`). It waits there forever. Nothing in the engine can say "wait at most thirty minutes for this approval", "sleep five minutes, then check again", or "wait until the deployment finishes". Each of those either holds a worker for as long as it waits (a node that polls) or lives outside the engine (a due-table and a service that resumes threads).

StrataOps needs all three in M4, governed remediation:

- **The approval timeout:** an approval that nobody answers within, say, thirty minutes either takes a route (`escalate`, `reject`) or ends the thread.
- **The timed verification window:** "monitor the error rate for five minutes" (Brief §8). Today the verification agent polls inside one node. A durable timer lets the thread wait without holding a worker: a check, a timer, a check, in a loop.
- **Event waits:** `deployment.finished`, `metric.normalized` (StrataOps Arch §6.4, its `WAITING_FOR_EVENT` state). Something outside signals the thread, with a payload, and it continues.

The workaround StrataOps planned (its plan §6.2, `WaitScheduler`) is a Postgres due-table and a hosted service that resumes threads with overrides. This document closes the gap in the engine instead: **SG-16**. The SG numbers refer to the gap log of StrataOps (not public yet).

It builds on `design/28-Ending-A-Thread.md` (SG-34): a wait that times out with nowhere to go **ends** the thread, and waking uses its **conditional save**, so two drivers never wake one thread twice.

SG-16 lands in both engines at one commit (strict parity, StrataOps ADR 0003): the same capability, in each language's idiom.

It was written as a blueprint before implementation and is now built. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| The model | **A wait is an interrupt boundary with a wake condition**, not a new execution model. The thread checkpoints as `interrupted`, holds no executor, and leaves the wait through a resume, as every interrupted thread does. |
| The builders | **`AddWaitForEvent(name, eventType, timeout?, onTimeout?)`** and **`AddTimer(name, delay)`** / **`AddTimer(name, state => wakeAt)`** add a *wait node*: a node that does nothing, after which the run freezes. **`AddInterruptBoundary(name, onReject, timeout, onTimeout)`** gives a human boundary a timeout. Rust: `add_wait_for_event(_with)`, `add_timer`, `add_timer_until`, `add_interrupt_boundary_with`. |
| Why it waits | The checkpoint level that waits carries a **`wait` object**: `kind` (`human`, `event`, `timer`), `event_type`, and **`wake_at`**: when the timer fires, or when the wait times out. **`schema_version` `1.5.0`**, a superset of `1.4.0`. A boundary with no timeout writes no `wait`, so it is byte for byte as before. |
| Who answers | **Each kind of wait has one answer.** A human boundary: `ResumeAsync`. An event wait: **`SignalAsync(threadId, EventSignal, store)`**. A timer: **`WakeDueAsync(store)`** when it is due. Any wait with a timeout: `WakeDueAsync` when it times out. A resume of an event or timer wait is refused, and so is a signal for any other wait. `EndAsync` ends any of them. |
| A signal's payload | **Typed overrides**, as a human's (`design/21`): `EventSignal.Overrides` replace top-level fields of the waiting level's state, so the payload is in the state, which every later node and checkpoint carries. A signal records **no decision**: the level keeps the decision it had. |
| The wrong signal | **Refused**, before anything is written: a signal for an event type the thread isn't waiting for, or for a thread that isn't waiting for an event at all. Buffering a signal that arrives early is "Not built". |
| Due threads | **From the query port**, not a separate timer store: **`ThreadQuery.DueBy`** lists waiting threads whose `wake_at` is at or before a time. Every store indexes `wake_at`: a column on the SQL head table, maintained by the trigger, and a Redis index set. Checkpoints stay the single source of truth. |
| Waking | **`WakeDueAsync(store, WakeOptions)`**, a library call (no hosted service: that is SG-28, a scheduler SG-25). It wakes every due timer and times out every due wait, one thread at a time, each as a resume run (or, for a timeout with nowhere to go, an end). It returns a **`WakeReport`**: the threads it woke, with each run's result, and the ones it skipped. |
| Two drivers | **Waking is a resume**, so it goes through `design/28`'s conditional stamp. Two drivers that both list a due thread both try; one wins, and the other skips it. |
| A timeout | **Takes `onTimeout` if given; otherwise ends the thread** (`design/28`) with `strata.end_reason` **`timed_out`**. The end's result is `Cancelled(Ended)`, as for any end; the reason is in the metadata, where `ThreadQuery` can filter on it. No new `CancelReason`. |
| Which graph's threads | A store holds many graphs' threads, and graph identity is SG-13. **`WakeOptions.Metadata`** filters the threads a driver wakes (put `graph=investigation` in the run metadata), and the engine **skips a thread whose wait isn't one of this graph's**. |
| Precision and clock | **`wake_at` at whole seconds, rounded up**, so a timer never fires early, and compared as text like every other timestamp. **.NET: `RunOptions.TimeProvider`**, both for `wake_at` and for "now" in `WakeDueAsync`. **Rust: `chrono::Utc::now()`** for `wake_at`, and an explicit **`now`** argument to `wake_due`, since tokio's clock is monotonic, not wall time. |
| Nesting | A wait inside a subgraph is the innermost level's. The head table, the Redis index, `ThreadSummary.Wait` and `WakeDueAsync` all use the **innermost** level's `wait`, the only level that can have one. |
| Run events | **`Interrupted` gains `Wait`**, and **`Resumed` gains `Wake`** (`human`, `event`, `timer`, `timeout`), `EventType` and `TimedOutTo`. No new event type. |
| Definitions | A wait node is **`kind: "wait"`** with a **`wait`** object, at definition **`schema_version` `1.4.0`**. A timeout route is a guarded **`on_timeout`** edge, like `on_reject`. The shared validator refuses a wait inside a map's template (**`STRATA_ERR_WAIT_IN_MAP`**), and `.Compile()` refuses one in a map's child, as for boundaries. |
| The CLI | **Read-only.** `thread list --due` (now) and `--due-by <time>`; `thread inspect` shows what a thread waits for and when it wakes or times out; `graph view` annotates wait nodes. |

---

## Usage

```csharp
// StrataOps: the investigation waits for approval, at most 30 minutes, then escalates.
var investigation = new StateGraph<Investigation>()
    .AddNode("Plan", PlanAsync)
    .AddNode("Approve", PresentAsync)
    .AddInterruptBoundary("Approve", onReject: "Abort", timeout: TimeSpan.FromMinutes(30), onTimeout: "Escalate")
    .AddNode("Remediate", RemediateAsync)
    // The verification window: check, wait five minutes, check again, up to an hour.
    .AddNode("Check", CheckErrorRateAsync)
    .AddTimer("Window", TimeSpan.FromMinutes(5))
    // Wait for the deployment to finish, at most an hour; time out to a rollback.
    .AddWaitForEvent("Deploying", "deployment.finished", timeout: TimeSpan.FromHours(1), onTimeout: "Rollback")
    // … edges …
    .Compile();

// The Worker's cadence, e.g. every 15 seconds:
var report = await investigation.WakeDueAsync(store, new WakeOptions
{
    Metadata = new Dictionary<string, string> { ["graph"] = "investigation" },
    Run = new RunOptions { Observer = timeline },
});
foreach (var woken in report.Woken)
{
    // woken.Reason: Timer, or Timeout; woken.Result: what the resumed (or ended) run returned.
}

// A deployment webhook:
var result = await investigation.SignalAsync(
    threadId,
    new EventSignal("deployment.finished").WithOverride("deployment", JsonSerializer.SerializeToNode(deployment)),
    store);
// A thread that isn't waiting for deployment.finished: Failed("thread '…' is not waiting for event …").
```

```rust
let investigation = StateGraph::<Investigation>::new()
    .add_async_node("Approve", present)
    .add_interrupt_boundary_with(
        "Approve",
        BoundaryOptions::new()
            .with_reject("Abort")
            .with_timeout(Duration::from_secs(30 * 60))
            .with_on_timeout("Escalate"),
    )
    .add_async_node("Check", check_error_rate)
    .add_timer("Window", Duration::from_secs(5 * 60))
    .add_wait_for_event_with(
        "Deploying",
        "deployment.finished",
        WaitOptions::new().with_timeout(Duration::from_secs(3600)).with_on_timeout("Rollback"),
    )
    // … edges …
    .compile()?;

let report = investigation
    .wake_due_with(store.clone(), Utc::now(), WakeOptions::new().with_metadata("graph", "investigation"))
    .await?;

let result = investigation
    .signal(&thread_id, EventSignal::new("deployment.finished").with_override("deployment", json!(deployment)), store.clone())
    .await;
```

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Interruption;

public enum WaitKind { Human, Event, Timer }

/// Why a level waits (the checkpoint's `wait`).
public sealed record CheckpointWait(WaitKind Kind, string? EventType, string? WakeAt);

public sealed record Checkpoint
{
    // … as before …
    public CheckpointWait? Wait { get; init; }           // this level's
    public CheckpointWait? InnermostWait();              // the innermost level's
}

public static class CheckpointSchema { public const string WaitVersion = "1.5.0"; /* … */ }

/// An external event for a thread waiting for it.
public sealed record EventSignal(string EventType)
{
    public IReadOnlyDictionary<string, JsonNode?>? Overrides { get; init; }
    public EventSignal WithOverride(string key, JsonNode? value);
}

public enum WakeReason { Human, Event, Timer, Timeout }

public sealed record ThreadEnd { public const string TimedOut = "timed_out"; /* … */ }

public sealed record ThreadQuery { public DateTimeOffset? DueBy { get; init; } /* … */ }
public sealed record ThreadSummary(/* … */) { public CheckpointWait? Wait { get; init; } }

namespace Datalium.Strata.Graph;

public sealed class StateGraph<TState>
{
    public StateGraph<TState> AddInterruptBoundary(
        string name, string? onReject = null, TimeSpan? timeout = null, string? onTimeout = null);
    public StateGraph<TState> AddWaitForEvent(
        string name, string eventType, TimeSpan? timeout = null, string? onTimeout = null,
        IReadOnlyDictionary<string, string>? metadata = null);
    public StateGraph<TState> AddTimer(string name, TimeSpan delay, IReadOnlyDictionary<string, string>? metadata = null);
    public StateGraph<TState> AddTimer(string name, Func<TState, DateTimeOffset> wakeAt, IReadOnlyDictionary<string, string>? metadata = null);
}

public sealed record WakeOptions
{
    public IReadOnlyDictionary<string, string> Metadata { get; init; }   // only threads whose metadata has every pair
    public int Limit { get; init; } = 100;                               // most threads woken per call
    public RunOptions Run { get; init; } = new();                        // for every run; its TimeProvider is "now"
}

public sealed record WokenThread<TState>(string ThreadId, WakeReason Reason, GraphExecutionResult<TState> Result);
public sealed record SkippedThread(string ThreadId, string Reason);
public sealed record WakeReport<TState>(IReadOnlyList<WokenThread<TState>> Woken, IReadOnlyList<SkippedThread> Skipped);

public sealed partial class CompiledGraph<TState>
{
    public Task<GraphExecutionResult<TState>> SignalAsync(string threadId, EventSignal signal, ICheckpointStore store, CancellationToken ct = default);
    public Task<GraphExecutionResult<TState>> SignalAsync(string threadId, EventSignal signal, ICheckpointStore store, RunOptions options, CancellationToken ct = default);
    public Task<WakeReport<TState>> WakeDueAsync<TStore>(TStore store, WakeOptions? options = null, CancellationToken ct = default)
        where TStore : ICheckpointStore, ICheckpointQuery;
}

// GraphExecutionResult<TState>.Interrupted gains `CheckpointWait? Wait { get; init; }`.
// Observability.Interrupted gains `Wait`; Observability.Resumed gains `Wake`, `EventType`, `TimedOutTo`.
```

### Rust

```rust
// schema
pub enum WaitKind { Human, Event, Timer }
pub struct CheckpointWait { pub kind: WaitKind, pub event_type: Option<String>, pub wake_at: Option<String> }
pub const WAIT_CHECKPOINT_SCHEMA_VERSION: &str = "1.5.0";
impl Checkpoint { pub fn innermost_wait(&self) -> Option<&CheckpointWait>; }   // + field `wait`

// checkpoint
pub struct EventSignal { pub event_type: String, pub overrides: HashMap<String, Value> }
impl EventSignal { pub fn new(event_type: impl Into<String>) -> Self; pub fn with_override(self, key, value) -> Self; }
pub enum WakeReason { Human, Event, Timer, Timeout }
pub const END_TIMED_OUT: &str = "timed_out";
pub struct WakeOptions { /* metadata, limit, config */ }
pub struct WokenThread<S> { pub thread_id: String, pub reason: WakeReason, pub result: GraphExecutionResult<S> }
pub struct SkippedThread { pub thread_id: String, pub reason: String }
pub struct WakeReport<S> { pub woken: Vec<WokenThread<S>>, pub skipped: Vec<SkippedThread> }

impl<S> StateGraph<S> {
    pub fn add_interrupt_boundary_with(self, name, options: BoundaryOptions) -> Self;
    pub fn add_wait_for_event(self, name, event_type) -> Self;
    pub fn add_wait_for_event_with(self, name, event_type, options: WaitOptions) -> Self;
    pub fn add_timer(self, name, delay: Duration) -> Self;
    pub fn add_timer_until(self, name, wake_at: impl Fn(&S) -> DateTime<Utc>) -> Self;
}

impl<S> CompiledGraph<S> {
    pub async fn signal(&self, thread_id, signal: EventSignal, store: Arc<dyn CheckpointStore>) -> GraphExecutionResult<S>;
    pub async fn signal_with(&self, thread_id, signal: EventSignal, store, config: RunConfig) -> GraphExecutionResult<S>;
    pub async fn wake_due<T: CheckpointStore + CheckpointQuery + 'static>(&self, store: Arc<T>, now: DateTime<Utc>)
        -> Result<WakeReport<S>, StrataError>;
    pub async fn wake_due_with<T: …>(&self, store: Arc<T>, now: DateTime<Utc>, options: WakeOptions)
        -> Result<WakeReport<S>, StrataError>;
}

// ThreadQuery gains `due_by` (builder `due_by(time)`), ThreadSummary `wait`.
// GraphExecutionResult::Interrupted gains `wait`; RunEventKind::Interrupted gains `wait`,
// RunEventKind::Resumed gains `wake`, `event_type`, `timed_out_to`.
```

- **No token on `WakeDueAsync`'s store calls**, as everywhere (`design/19`). Its cancellation token (Rust: `WakeOptions`' `RunConfig`) reaches every run it starts and is checked between threads.
- **One thread at a time.** A woken run runs to its next freeze or to completion before the next due thread is woken. Concurrency is the caller's: run several drivers, each filtered to its share of threads, or call it more often.

---

## Semantics

### Waiting

A **wait node** is a node with no handler of its own: it passes its state through, counts as a hop (`design/24`), and streams `NodeStart` / `NodeEnd` like any node. After it the run freezes exactly as at an interrupt boundary: an `interrupted` checkpoint at the wait node, `next_node` the edge out of it (evaluated on the state, as for a boundary), and a `wait`:

| Wait | `kind` | `event_type` | `wake_at` |
|---|---|---|---|
| `AddInterruptBoundary(…, timeout)` | `human` | `null` | now + timeout |
| `AddInterruptBoundary` with no timeout | — no `wait` — | | |
| `AddWaitForEvent(…, timeout)` | `event` | the type | now + timeout |
| `AddWaitForEvent` with no timeout | `event` | the type | `null`: never due |
| `AddTimer(name, delay)` | `timer` | `null` | now + delay |
| `AddTimer(name, state => t)` | `timer` | `null` | `t`, rounded up |

"Now" is the run's clock (.NET `RunOptions.TimeProvider`, Rust `Utc::now()`), and `wake_at` is rounded **up** to a whole second. A timer whose `wake_at` is already past still freezes; the next `WakeDueAsync` wakes it.

`Compile()` refuses a delay or timeout of zero or less, an `onTimeout` with no timeout, an `onTimeout` (or `onReject`) naming a node that doesn't exist (a dangling edge, through validation), and a name that is both a wait node and an interrupt boundary. Re-adding a wait node's name as another kind of node makes it that kind.

### Answering a wait

Each wait has one answer, and a resume checks it before anything is stamped (`design/21`'s preflight, at the innermost level):

| The innermost level waits for | `ResumeAsync` (human) | `SignalAsync(type)` | `WakeDueAsync`, due |
|---|---|---|---|
| a human, no timeout (no `wait`) | **resumes** | refused | never due |
| a human, with a timeout | **resumes** | refused | **times out** |
| event `type` | refused | **resumes** (same type only) | times out, if it has a timeout |
| a timer | refused | refused | **wakes** |

A failed or cancelled thread has no `wait`: `ResumeAsync` resumes it as before, a signal is refused, and it is never due.

What each answer does at the waiting level:

- **A human resume** is unchanged: overrides, a recorded decision, the reject route.
- **A signal** applies its overrides to the waiting level's state (as a human signal's apply to the innermost level) and continues at `next_node`. It records no decision: the level keeps the decision it had, as a failed thread's resume does.
- **A due timer** continues at `next_node`, with no overrides and no decision.
- **A timeout** continues at the wait's `onTimeout` route, with no overrides and no decision. With no route, the thread is **ended** where it stands (`design/28`), every level, with `strata.end_reason` `timed_out` and no `strata.ended_by`.

Refusals (`Failed`, nothing written):

- `ResumeAsync` of an event wait: `thread 't' is waiting for event 'deployment.finished' at [Deploying]: only a signal of that event answers it`.
- `ResumeAsync` of a timer: `thread 't' is waiting for a timer at [Window] until 2026-10-06T12:05:00Z: it wakes when due`.
- `SignalAsync` of the wrong type, or of a thread waiting for something else: `thread 't' is not waiting for event 'x': it is waiting for event 'y' at [Deploying]` (or `… for a human at [Approve]`, `… for a timer at [Window]`, `… its latest checkpoint is failed`).
- Everything a resume already refuses: no checkpoint, an ended or completed thread, one already resumed.

### Due threads and the query port

A thread is **due at `t`** when its latest checkpoint is `interrupted`, not resumed, and its innermost `wait.wake_at` is at or before `t` (both at whole seconds). `ThreadQuery.DueBy = t` lists exactly those threads, composing with every other filter (metadata, states, time window, cursor) and in the usual order (most recently updated first). `ThreadSummary.Wait` is the innermost level's wait, so a listing says what each thread waits for and until when.

### Waking

`WakeDueAsync(store, options)`:

1. Fixes **now** once (`options.Run.TimeProvider`, Rust's `now` argument).
2. Lists due threads (`DueBy = now`, `options.Metadata`), a page at a time, until it has acted on `options.Limit` threads or the listing ends.
3. For each, loads the latest checkpoint with its sequence and **decides again**: it must still be due (a listing can be stale), and its wait must be one of this graph's (a wait node of the right kind, or a boundary with a timeout, at that node of the innermost graph). Otherwise the thread is **skipped**, with the reason.
4. A due **timer** is resumed with `WakeReason.Timer`. A due **human or event wait** is timed out: resumed at its `onTimeout` route with `WakeReason.Timeout`, or, with no route at the innermost level, **ended** with `timed_out`.
5. Each is one observed run: a resume (`RunMode.Resume`, `Resumed { Wake = Timer | Timeout }`), or an end (`RunMode.End`). The run's result goes in the report's `Woken`.
6. **Two drivers.** The resume's stamp and the end's save are conditional on the sequence loaded in step 3 (`design/28`, "Races"). If another driver, a signal, a resume or an end got there first, this one writes nothing and the thread goes in `Skipped` (`another driver woke or ended it first`). The resume that lost still reports its run to the observer, failed with the usual message.

A woken run may freeze again at another wait (a timer in a loop), with a new `wake_at`; it is not woken again in the same call, because it now sorts as newer than the listing's cursor.

**A woken thread that finishes doesn't read `completed`** unless its graph checkpoints every node. Under the default `CheckpointPolicy.Boundaries`, a run that completes writes nothing (design/33), so the thread's latest checkpoint stays the resume's stamp. It is listed as `resumed` (`ThreadState.Resumed`), and the stamped row still carries the `wait` and `wake_at` it was woken from. It isn't due again, because it is resumed. The same goes for any resume, signal or recovery that runs to the end. To see `completed` in the store (a dashboard, a test that waits for it), compile with `CheckpointPolicy.EveryNode`; otherwise read the run's own result, which the wake report and the run's `RunCompleted` event carry. Two of the .NET demos (28 and 29) expected `completed` and switched to `EveryNode`.

Exceptions from a woken run (a plain node's bug) propagate, as from `ResumeAsync`; the thread it was running has already been stamped `resumed`, so it is not due any more, and the next call carries on with the others.

### Graph identity

`WakeDueAsync` is a method of one compiled graph, but a store holds many graphs' threads, and the store doesn't record which graph wrote a thread (SG-13). Two guards, recommended together:

- **The caller's filter:** `WakeOptions.Metadata`. Put the graph's name in every run's metadata (`RunOptions.Metadata["graph"] = "investigation"`) and wake with the same pair. This is the real guard.
- **The engine's check:** a due thread whose innermost waiting node isn't a wait of this graph (or whose nesting doesn't follow this graph's subgraph nodes) is skipped, with nothing written. It catches a missing filter, not a graph with the same node names.

Since SG-13 (`design/31-Graph-Identity.md`, "The drivers"), a graph compiled with an identity lists only its own threads, and refuses another graph's even when the node names match, so neither guard is needed for it. `WakeOptions.MatchGraph = false` lists by metadata alone, as described here.

### Nesting

A wait inside a subgraph freezes the whole thread, as a boundary does (`design/16`): the innermost level carries the `wait`, every outer level is suspended at its subgraph node. A signal or a wake goes down to the innermost level, which is the only one that waits. A timeout's route is the innermost graph's; with none, the whole thread is ended, every level. A map's child can't wait: `Compile()` refuses it, naming the qualified wait (`map 'Fan' runs a graph with a wait at 'Window'; a map item can't freeze`).

---

## Run events and telemetry

| | |
|---|---|
| `Interrupted` | gains **`Wait`**: the innermost level's `wait`, `null` for a boundary with no timeout. |
| `Resumed` | gains **`Wake`** (`Human` for `ResumeAsync`, `Event`, `Timer`, `Timeout`), **`EventType`** (for `Event`) and **`TimedOutTo`** (the `onTimeout` route a timeout took). For a non-human wake, `IsApproved` is `true` and `Reviewer` is empty. |
| A timeout that ends | an end run, as `design/28`: `RunStarted(End)`, `CheckpointSaved(Ended)`, `RunCompleted(Cancelled, Ended)`. |
| Run span | `strata.wake.reason` on a resume's span; `strata.end_reason` is in the ended checkpoint's metadata. |
| Metrics | **`strata.thread.wakes`** counter, labelled **`strata.wake.reason`** (`event`, `timer`, `timeout`), one per thread a signal or `WakeDueAsync` wakes or ends. |

`RunEvent` is non-exhaustive in both engines; the new members are properties (.NET `init`, not positional) and fields (Rust; a pattern without `..` needs them, see "Migration").

---

## Wire format

### The checkpoint (`1.5.0`)

```json
"wait": { "kind": "event", "event_type": "deployment.finished", "wake_at": "2026-10-06T13:00:00Z" }
```

- **`wait`** is optional on every level, and absent (not null) when the level doesn't wait or has nothing new to say (a boundary with no timeout). Every key inside is always written. `kind` is `human`, `event` or `timer`; `event_type` is set exactly for `event`; `wake_at` is RFC 3339 at whole seconds, or `null` for an event wait with no timeout.
- **`schema_version` `"1.5.0"`** is what a checkpoint with a `wait` at any level is written with. It is a superset of `1.4.0`. A resume stamp keeps it. An ended checkpoint drops every level's `wait` (it will never wake) and stays `1.4.0`.
- Only an `interrupted` level carries one, and only the innermost level of a checkpoint can.

### Storage

- **SQL `checkpoints`** gains two columns: **`wait`** (`TEXT` / `JSONB`: the outermost level's `wait`, like `decision`; nested levels carry theirs inside `nested`) and **`wake_at`** (`TEXT`): the **innermost** level's `wait.wake_at`, derived when the row is written, so the trigger can copy it without parsing nested JSON. Readers ignore `wake_at`; the checkpoint is read from the other columns.
- **SQL `checkpoint_threads`** gains **`wake_at`** (`TEXT`, `"C"` collation in Postgres), copied by the trigger from the row it points at, and an index on it. The due filter is `state = 'interrupted' AND wake_at <= ?`.
- **Migration:** a store opening an older log adds the two `checkpoints` columns, adds `wake_at` to the head table, and replaces the trigger (and backfill) with the new one, in the same transaction that creates everything else. Existing rows keep `NULL`: nothing waited with a time before.
- **Redis** gains **`{prefix}:threads:waking`**: the threads whose latest checkpoint is interrupted, not resumed, with an innermost `wake_at`, **scored by `updated_at`** like every other index set, so a due listing reads only threads that can be due and keeps the listing order and cursor of every other query; the time filter is applied to each candidate's latest checkpoint. The save, conditional-save and index scripts maintain it (it is `KEYS[3]`; the state sets follow).
- **Shared fixtures:**
  - `checkpoint-lifecycle/waiting-event.json` (a flat event wait with a timeout), `waiting-approval.json` (a boundary with a timeout and a reject route), `nested-timer.json` (a timer inside the subgraph node `Verify`): what a live freeze writes, `created_at` and `wake_at` aside, in both engines.
  - `checkpoint-log/waiting.row.json`: the SQLite row for `waiting-event.json`, with `wait` and `wake_at`.
  - `columns.json`, `postgres-columns.json`, `threads-columns.json`, `postgres-threads-columns.json`: the new columns. `threads.rows.json`: a timed wait and its resume, with the head's `wake_at`.
  - `checkpoint-query/cases.json`: three waiting threads (a timer, an event wait with a timeout, one with none) and the due cases; `redis-index.json`: the `waking` set.

### Definitions (`1.4.0`)

```json
{ "name": "Deploying", "metadata": null, "kind": "wait",
  "wait": { "kind": "event", "event_type": "deployment.finished", "delay_ms": null, "timeout_ms": 3600000 } }
```

- **`kind: "wait"`** with a **`wait`** object (every key always written): `kind` `event` or `timer`, `event_type` for an event, `delay_ms` for a timer with a fixed delay (`null` when it is computed from the state), `timeout_ms` for an event wait with a timeout.
- A level with a wait node is **`schema_version` `1.4.0`**, a superset of `1.3.0`.
- A timeout route is a guarded edge with `condition` **`on_timeout`**, after the `on_failure` and `on_reject` edges, in node order. A boundary's timeout itself isn't exported (boundaries aren't); its route is. *(Since `48-Boundaries-In-Definitions.md`, both are: the boundary node carries `interrupt` with its `timeout_ms`.)*
- **`check_shape` / `ToTopology`** refuse a `wait` on a node that isn't `kind: "wait"`, a wait node without one, and either below `1.4.0` (exit 2).
- **The shared validator** reports **`STRATA_ERR_WAIT_IN_MAP`** for every wait node in a map's template, at any depth, named by its qualified path (`Fan/*/Window`). Fixtures: `graph-export/waits.json`, `graph-validation/wait-in-map.json`.

---

## The CLI

Read-only:

- **`strata thread list --due`** lists threads due now; **`--due-by <rfc3339>`** at a given time. A `Waiting For` column shows each thread's wait (`event deployment.finished until …`, `timer until …`), and `--json` a `wait` key.
- **`strata thread inspect`** prints `Waiting for: event 'deployment.finished'` (or `a human`, `a timer`), and `Times out at:` / `Wakes at:`. An ended thread whose reason is `timed_out` shows it under `Reason:`.
- **`strata graph validate`** reads `1.4.0` definitions and reports `STRATA_ERR_WAIT_IN_MAP`; **`graph view`** annotates a wait node (`Deploying: wait for event deployment.finished, times out after 1h`, `Window: timer, 5m`).
- **No `strata thread signal` or `wake`**: writing to a thread is the application's job, through the graph that owns it.

---

## Migration

**.NET** (source-compatible): `AddInterruptBoundary` gains two optional parameters. `CheckpointWait`, `WaitKind`, `EventSignal`, `WakeReason`, `WakeOptions`, `WakeReport`, `WokenThread`, `SkippedThread`, `SignalAsync`, `WakeDueAsync`, `AddWaitForEvent` and `AddTimer` are new. `Checkpoint.Wait`, `ThreadQuery.DueBy`, `ThreadSummary.Wait`, `Interrupted.Wait` (result and event) and `Resumed.Wake` / `EventType` / `TimedOutTo` are `init` properties, so positional construction and deconstruction are unchanged. `GraphNodeKind.Wait` is a new enum value.

**Rust:** `Checkpoint` gains a public `wait` field (struct literals need it, or `..`), as does `GraphNodeDef` (`Default` covers it). `GraphExecutionResult::Interrupted`, `RunEventKind::Interrupted` and `RunEventKind::Resumed` gain fields, so a pattern that names every field needs `..`. `ThreadQuery` and `ThreadSummary` are non-exhaustive already. `GraphNodeKind` gains `Wait`, and `ValidationErrorKind` `WaitInMap`, so an exhaustive `match` on either needs an arm.

**Stores:** every SQL store migrates on open (above); Redis builds the `waking` set when a thread is next saved or indexed. Every writer to a Redis prefix must be current, as `design/19` requires: an older writer passes the state sets where the script now expects the `waking` set. Run `rebuild_index` after upgrading the last writer.

**StrataOps:** its approval boundary gets `timeout` / `onTimeout`; the verification agent's polling node becomes `Check` → `Window` (a timer) → `Check`; its `WAITING_FOR_EVENT` state is a wait node, signalled from its webhooks with `SignalAsync`; its Worker calls `WakeDueAsync` on its own cadence with `graph` metadata. The planned `WaitScheduler` due-table is not built.

---

## Tests

Mirrored in both engines, in new suites `event_waits.rs` / `EventWaitTests.cs` unless noted:

- **A timer wakes on the clock:** a run freezes at the timer with `wait { timer, wake_at = now + delay }`; `WakeDueAsync` before it is due wakes nothing; after (a fake `TimeProvider` advanced; Rust's `now` argument) it continues at the next node; a timer in a loop freezes again with a new `wake_at`; a timer from the state wakes at the state's time, rounded up.
- **An event wait resumes with its payload:** `SignalAsync` with typed overrides; the next node sees them in the state; no decision is recorded, the level's earlier one is kept.
- **Refusals:** a signal of the wrong type, a signal to a human boundary, to a timer, to a failed thread; a human resume of an event wait and of a timer. Each writes nothing and leaves the thread waiting.
- **Timeouts:** an event wait with `onTimeout` takes the route (`Resumed { Wake = Timeout, TimedOutTo }`); one without ends the thread with `timed_out`; an approval boundary with a timeout and `onTimeout`, and without (ended, `ThreadQuery` finds it by `strata.end_reason`); a human answer before the timeout makes it not due.
- **Nesting:** a timer inside a subgraph is due through the head, wakes into the child, and the parent completes; a nested event wait is signalled; a nested timeout with no route ends every level. Matches `nested-timer.json`.
- **Two drivers wake a thread once:** two `WakeDueAsync` calls on a store that lets both list it (a wrapper that pauses the first after its load): one wakes it, the other skips it; the log has one resume stamp.
- **Graph identity:** the metadata filter leaves other threads alone; a due thread of another graph is skipped with nothing written.
- **The due query in every store** (`checkpoint_store_contract.rs`, `CheckpointQueryContractTests.cs`, the CLI): the shared due cases, the head-table `wake_at` from raw SQL (`threads.rows.json`), the Redis `waking` set (`redis-index.json`), migration of a pre-`1.5.0` log.
- **Fixtures:** each lifecycle fixture is what a live freeze writes; each round-trips through every store; `waiting.row.json` is the SQLite row.
- **Definitions:** `waits.json` is what both engines export, round-trips, and the CLI validates and views it; `wait-in-map.json` gives `STRATA_ERR_WAIT_IN_MAP` in both validators and the CLI; `.Compile()` refuses a map child with a wait.
- **Run events:** `Interrupted.Wait`; `Resumed.Wake` for each reason; the end run of a timeout.
- **Mutation checks**, both engines: waking without the due re-check; an unconditional stamp in the wake path; not refusing the wrong signal; not ending on a timeout without a route; dropping `wake_at` from the head.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

### The questions as asked (the handoff brief)

Recommendations are in bold.

1. **A new execution model, or a boundary with a wake condition?** **A boundary with a wake condition.** The thread is `interrupted`, so listing, ending, nesting, the conditional stamp and the CLI all work as they do.
2. **Where the reason for waiting lives.** **A `wait` object on the waiting level, at `1.5.0`.** A typed field rather than metadata, because it drives the engine (what may answer the thread, when it is due), and the head table and the Redis index need `wake_at` without parsing anything.
3. **A separate timer store, or the query port?** **The query port** (`ThreadQuery.DueBy`), with `wake_at` indexed in every store. Checkpoints stay the single source of truth, and there is nothing to keep in step.
4. **The signal's payload: overrides, `NodeContext.Signal`, or both?** **Overrides only.** The state is what survives: a payload in the context would be lost the moment a later node fails and the thread is resumed, unless it were persisted like a decision. With overrides it is in the state, in every checkpoint after it, and in the reducer's hands.
5. **A signal for an event the thread isn't waiting for: refuse or buffer?** **Refuse**, before anything is written. Buffering is "Not built".
6. **`WakeDueAsync` as a library call or a hosted service?** **A library call.** Hosting is SG-28 and scheduling SG-25; StrataOps' Worker calls it on its own cadence.
7. **A timeout with no `onTimeout`: end or fail?** **End** (`design/28`), with `strata.end_reason` `timed_out`: nothing failed, and the thread must not be resumable.
8. **Run events:** a new `Waiting` event, or members on existing ones? **Members on `Interrupted` and `Resumed`.** One event per suspension, and observers that report suspensions and resumes already handle them.
9. **Definitions:** **`kind: "wait"` and `on_timeout` edges, at `1.4.0`**, validated; waits refused in a map.

### Raised while building SG-34 (the second handoff)

10. **Indexing `wake_at`.** **A `wake_at` column on `checkpoint_threads`**, copied by the trigger from a derived `wake_at` column on `checkpoints` (the innermost level's), and a `wait` column holding the outermost level's `wait` like `decision`. Redis: a `waking` set scored by `updated_at`, not by `wake_at`, so due listings keep the listing order and cursor every store shares; ordering due threads by wake time is "Not built".
11. **Which graph's threads.** **Both guards:** the caller's metadata filter (the real one) and the engine skipping a wait that isn't this graph's. Graph identity in the store is SG-13.
12. **Precision.** **Whole seconds, rounded up**, compared as text. Due-ness on the caller's clock: .NET `RunOptions.TimeProvider`, Rust an explicit `now`.
13. **A wait inside a subgraph.** **The innermost level's `wait` is the thread's**: only the innermost level can wait, so "earliest at any level" and "innermost" are the same.
14. **Signals and decisions.** **No decision:** a decision records a human's answer to a boundary; an event isn't one. The level keeps its decision. What arrived is in the state (the overrides) and in the `Resumed` event (`Wake`, `EventType`).
15. **The approval timeout in the definition.** **Its route only**, as a guarded `on_timeout` edge, which the validator checks like any edge. Boundaries themselves aren't exported, so neither is their timeout. *(Superseded by `48-Boundaries-In-Definitions.md`, SG-37: every boundary is now exported with `interrupt`, its timeout included, at definition `1.6.0`.)*
16. **Map children.** **Refused at compile time**, and by the shared validator for wait nodes it can see.

### Added by this document

17. **Can a human resume a timer or an event wait?** **No.** Each wait has one answer, so an operator can't skip a timer by accident and an approval UI can't answer an event wait. To abandon a wait, end the thread.
18. **What a wait node is.** **A node with no handler.** A wait is a point in the graph, and a node before it does the work. That keeps the handler out of the waiting step: a wait node never fails, never retries, and never needs a policy.
19. **The `Interrupted` result.** **Gains `Wait`**, so the caller that started a run learns what it waits for (and until when) without a query.

---

## Implementation Status

Built 2026-10-06 in both engines, every store adapter and the CLI, in one change, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **Argument checks follow each engine's habit.** .NET refuses a non-positive delay or timeout, and a timeout route with no timeout, when the builder is called (`ArgumentOutOfRangeException`, `ArgumentException`), as `SetTimeout` does. Rust refuses them at `.compile()` (`CompilationFailed`), as `set_timeout` does. Both refuse a name that is a wait node and an interrupt boundary at compile time, and a dangling timeout route through validation.
- **Rust's `wake_due` returns `Result<WakeReport<S>, StrataError>`**, not a store error: an out-of-range limit or a refused config is `ExecutionAborted`, like a refused run, and so is a store error while listing. .NET throws, as its stores do.
- **One store argument.** .NET's `WakeDueAsync<TStore>` constrains `TStore : ICheckpointStore, ICheckpointQuery`; Rust's takes an `Arc<T>` with `T: CheckpointStore + CheckpointQuery`. Every built-in adapter is both.
- **`ThreadSummary` gained `Wait`** (the innermost wait), so the shared `cases.json` summaries carry a `wait` key, null for threads that don't wait.
- **The definition's wait `kind`** reuses `WaitKind` in Rust (`check_shape` refuses `human` there); .NET's interop type keeps it a string, and `ToTopology` refuses anything but `event` or `timer`.
- **`thread list` gained a `Waiting For` column** (`timer until <t>`, `event deployment.finished until <t>`), and `--json` a `wait` key.
- **Telling a lost race apart:** the run scope records that its conditional write lost (a `Superseded` flag on the run), so the wake driver reports the thread skipped; the run itself still ends `Failed` with the usual message.

**How it was built:**

- **.NET:**
  - `Interruption/CheckpointWait.cs` is new: `WaitKind`, `CheckpointWait`, `EventSignal`, `WakeReason`. `Checkpoint` gained `Wait` and `InnermostWait()`, and `CheckpointSchema` `WaitVersion`. `ThreadEnd.TimedOut` and an internal `Reason`. `ThreadQuery.DueBy` and `IsDue`, `ThreadSummary.Wait`. `LevelSignals` carries the wake (`ForWake`).
  - `StateGraph`: `AddWaitForEvent`, both `AddTimer`s, `AddInterruptBoundary`'s `timeout` / `onTimeout`, the `wait` node detail and `on_timeout` edges (`GraphTopology.WithTimeoutEdges`), and the compile checks. `Graph/Waits.cs` is new: `WaitSpec`, `WakeTimes` (rounding up), `WakeOptions`, `WokenThread`, `SkippedThread`, `WakeReport`.
  - `CompiledGraph.Interruption.cs`: wait nodes freeze like boundaries, with their `wait`; `RouteFor` (reject and timeout routes) and `WakeProblem` (what may answer a wait) in the resume preflight; a decision only for a human's answer. `CompiledGraph.Waits.cs` is new: `SignalAsync`, `WakeDueAsync`, and the timeout's end. `CompiledGraph.End.cs`: the end's reason, and no `wait` on an ended level.
  - Events: `Interrupted.Wait`; `Resumed.Wake`, `EventType` and `TimedOutTo`; the run span's `strata.wake.reason`; the `strata.thread.wakes` counter. Validation: `ValidationErrorKind.WaitInMap`.
  - Storage: the `wait` and `wake_at` columns, the head table's `wake_at`, its index and trigger, and the migrations, in the EF Core and Postgres stores; the Redis scripts' `waking` set; the interop `Wait` and `GraphWaitDefinition`, version `1.4.0`, and `ToTopology`'s checks.
- **Rust:**
  - `schema.rs`: `WaitKind`, `CheckpointWait`, `Checkpoint::wait` and `innermost_wait`, `WAIT_CHECKPOINT_SCHEMA_VERSION`, `GraphNodeKind::Wait`, `GraphWaitDef`, `WAIT_GRAPH_SCHEMA_VERSION`, and `check_shape`. `store.rs`: `ThreadQuery::due_by`, `is_due`, `ThreadSummary::wait`.
  - `wait.rs` is new: `WakeReason`, `EventSignal`, `BoundaryOptions`, `WaitOptions`, `WaitSpec`, `wake_text`, `WakeOptions`, `WokenThread`, `SkippedThread`, `WakeReport`.
  - `graph.rs`: the builders, the compile checks, the export (`with_routes`, for `on_reject` and `on_timeout`), `wait_after`. `checkpoint.rs`: the freeze, `route_for`, `wake_problem`, `signal` / `signal_with`, `wake_due` / `wake_due_with`, `end_timed_out`, `END_TIMED_OUT`. `observability.rs`: the event fields, the span field, `THREAD_WAKES`. `validate.rs`: `WaitInMap`. `node.rs`: `Suspension`, so a map's refusal names a wait or a boundary.
  - `strata-memory`: the same columns, head table, trigger and migrations in SQLite and Postgres, and the Redis scripts.
- **CLI:** `thread list --due` / `--due-by`, the `Waiting For` column and `wait` in JSON, `thread inspect`'s `Waiting for:` / `Wakes at:` / `Times out at:` lines, and `graph view`'s wait annotation. It reads `1.4.0` definitions and `1.5.0` checkpoints through the shared types.
- **Schema:** `checkpoint.schema.json` (`wait`, `1.5.0`) and `graph-definition.schema.json` (`kind: wait`, `wait`, `1.4.0`, `on_timeout`). Every fixture validates against them.

**Tests:**

- **New suites:** `event_waits.rs` (22) and `EventWaitTests.cs` (22), test for test. One is a thread another driver woke between the listing and the load: it is skipped, and nothing is written.
- **Store contracts:**
  - A freeze-and-wake contract against every adapter in both engines: the three waiting fixtures round-trip, a timer is listed as due by the store and woken once, and an event wait is signalled.
  - The shared due cases in `cases.json`, against every store and the CLI.
  - The waiting row fixture in all three SQLite writers and both Postgres stores, with the nested wait's derived `wake_at`.
  - Migrating a log and head table from before waits, in SQLite and Postgres, both engines.
  - The head table's `wake_at` from raw SQL (`threads.rows.json`), and the Redis `waking` set (`redis-index.json`), rebuilt from the lists alone.
- **CLI:** the due listings, `inspect` of an event wait and a nested timer, `graph validate` and `view` of `waits.json` and `wait-in-map.json`, a wait refused below `1.4.0` (exit 2), and the waiting row.
- **Totals when this landed:** .NET 635, with Postgres and Redis running and nothing skipped, plus the example's 1. `rust/` 579 and `cli/` 72, with `STRATA_REQUIRE_BACKENDS=1` against live Postgres and Redis, and `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` and `dotnet format --verify-no-changes` are clean.
- **Mutation checks, in both engines** (each broken, the suite run, then restored):
  - Waking without deciding again that the thread is due fails 1 test in each.
  - Stamping a wake's resume unconditionally fails 1 in each: two drivers.
  - Accepting a signal of the wrong event type fails 1 in each.
  - Resuming a timeout with no route instead of ending the thread fails 3 in each.
  - Not copying `wake_at` to the SQLite head table fails 4 in each.

## Not built (deliberately out of scope)

- **Buffered signals:** a signal that arrives before the thread reaches its wait is refused, not kept for later.
- **Child-completion waits** ("wait until another thread completes") and a general event bus: StrataOps signals its threads itself.
- ~~**A hosted wake service** (SG-28).~~ Built 2026-10-10 (`35-Hosting-Integration.md`): `AddWakeDriver<TState>()` in .NET and `Host::wake` in Rust call `WakeDueAsync` on a cadence. A **scheduler** (SG-25) is still not built.
- ~~**Graph identity in the store** (SG-13): the metadata filter stands in for it.~~ Built with SG-13 (`design/31-Graph-Identity.md`).
- ~~**Streaming a signal or a wake:** a woken or signalled run that reaches a streaming node fails there, as a plain resume does. `ResumeStreaming` still works for human waits.~~ Built with SG-35 (`42-Waking-Through-A-Resume.md`): `ResumeStreaming` with `LevelSignals.Due` or `LevelSignals.Event(signal)` streams a woken or signalled run. `WakeDueAsync` and `SignalAsync` themselves still don't stream.
- **Outer-level overrides with a signal** (`LevelSignals` for events): a signal's overrides go to the waiting level only.
- **Due threads ordered by wake time**, and a Redis index scored by `wake_at`: due listings use the usual order.
- **Waking threads concurrently** inside one `WakeDueAsync` call. Since SG-35 (`42-Waking-Through-A-Resume.md`), an application that wants concurrency lists due threads itself and resumes each with `LevelSignals.Due`.
- **Cancelling or moving a timer** other than ending the thread.
- ~~**A boundary's timeout in the exported definition** (only its route is).~~ Built with SG-37 (`48-Boundaries-In-Definitions.md`): a boundary node carries `"interrupt": { "timeout_ms": … }` at definition `1.6.0`, and `graph view` shows whether its timeout routes or ends the thread.
