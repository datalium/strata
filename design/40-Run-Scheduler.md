# Datalium Strata: The Run Scheduler

Every run starts the moment it is called. `RunAsync`, a resume, a signal, a wake or a recovery each runs at once, on the caller's task, however many are already running. The engine has no answer to "how many runs at once", "SEV-1 first", "no tenant takes the whole process" or "what happens when more work arrives than the process can do". Its one bound is the 1,024-chunk stream channel, which slows a run's tokens, not its admission.

StrataOps works around this today with a `SemaphoreSlim` (`InvestigationOptions.MaxConcurrent`, 4 by default, in `InvestigationRuns`). A request beyond it waits for a slot, which holds back its Kafka partition. Its architecture describes more: priority lanes with SEV-1 capacity reserved (Arch §19), per-tenant runtime quotas (Arch §20), and a backpressure controller that queues, sheds low-priority work and rejects (Arch §6.7). None of that is built, and all of it is listed as Strata's to provide. M6, the incident storm, is gated on it for published numbers: "Overload the runtime deliberately. Demonstrate graceful queueing" (Brief §13) has to show Strata's backpressure, not a shim's.

This document closes the gap in the engine: **SG-25**. The SG numbers refer to the gap log of StrataOps (not public yet). SG-25 lands in both engines at one commit (strict parity, StrataOps ADR 0003).

It was written as a blueprint before implementation and is now built. "Review decisions" lists the questions for review with the answers the build follows (the first four were decided before the blueprint was written), and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Where | **In the engine**: a `RunScheduler` in `Datalium.Strata` (`Datalium.Strata.Scheduling`) and `strata-core` (`scheduler`), given to a run through `RunOptions.Scheduler` / `RunConfig::with_scheduler`. Every run mode and both drivers wait for a slot before they load or write anything. The hosting packages register one and put it in the run defaults. |
| What a slot is | **One run call**: a run, resume, signal or recovery, streaming or not, from before it loads its checkpoint to its return. A subgraph's child, a parallel group's branches and a map's items are part of the run and take no slot. An interrupted run gives its slot back when it returns: a waiting thread holds nothing. Ending a thread (`EndAsync`) takes no slot. |
| Priority | **Strict**: an integer per run (`RunOptions.Priority`, default 0, higher first), first come first served within a priority. **Reserved slots** give higher priorities headroom: a reservation `(MinPriority 3, Slots 2)` keeps two slots that only runs at priority 3 or more may take. No aging and no weighted shares. |
| Tenants | **Concurrency only**: at most `TenantLimit` runs of one tenant at once, with per-tenant overrides, changeable while running (0 pauses a tenant). The tenant is a key of the run's metadata (`tenant` by default). A tenant at its limit waits without holding back other tenants. Rate limits (model and tool requests per second) stay with the application. |
| Overload | **A bounded queue that sheds.** Runs that can't start wait, up to `MaxQueued`. When the queue is full, a newcomer that outranks the lowest-ranked waiting run sheds it; otherwise the newcomer is refused. `MaxQueueTime`, the run's deadline and its token also end a wait. |
| A run that isn't admitted | **A new result, `NotAdmitted`** (refines `Failed`; Rust `StrataError::NotAdmitted`), with the reason and how long it waited. Nothing was loaded or written, no run event or span was made: the thread is exactly as it was, and the call can be made again. |
| Wire format | **None.** No checkpoint, definition, database or fixture-format change. A shared case file pins the scheduling rules across the two engines. |

---

## Usage

```csharp
var scheduler = new RunScheduler(new RunSchedulerOptions
{
    MaxConcurrent = 32,
    MaxQueued = 2_000,
    MaxQueueTime = TimeSpan.FromMinutes(2),
    Reservations = [new SlotReservation(MinPriority: 3, Slots: 4)],   // SEV-1 always finds 4 slots
    TenantLimit = 8,
    TenantLimits = new Dictionary<string, int> { ["acme"] = 16 },
});

var result = await graph.RunAsync(threadId, initial, store, new RunOptions
{
    Scheduler = scheduler,
    Priority = incident.Severity switch { 1 => 3, 2 => 2, 3 => 1, _ => 0 },
    Metadata = { ["tenant"] = incident.TenantId, ["incident"] = incident.Id },
});

if (result is GraphExecutionResult<InvestigationState>.NotAdmitted refused)
{
    // refused.Reason: QueueFull, Shed, QueueTimeout, Cancelled, Deadline or Closed. Nothing was written.
}
```

```rust
let scheduler = RunScheduler::new(
    SchedulerOptions::new(32)
        .with_max_queued(2_000)
        .with_max_queue_time(Duration::from_secs(120))
        .with_reservation(3, 4)
        .with_tenant_limit(8)
        .with_tenant_override("acme", 16),
)?;

let config = RunConfig::new()
    .with_scheduler(scheduler.clone())
    .with_priority(3)
    .with_metadata("tenant", "acme");
match graph.run_with("inc-42", initial, store, config).await {
    GraphExecutionResult::Failed(StrataError::NotAdmitted { reason, waited }) => { /* nothing written */ }
    other => { /* as before */ }
}
```

Hosting (.NET):

```csharp
builder.Services.AddStrata()
    .AddPostgresCheckpoints()
    .AddScheduler()                       // from Strata:Scheduler, then an optional delegate
    .AddGraph(sp => Investigation.Build())
    .AddWakeDriver<InvestigationState>()
    .AddRecoveryDriver<InvestigationState>();

// StrataRunner<TState>.CreateOptions(priority: 3, metadata: …) carries the scheduler.
```

```json
{
  "Strata": {
    "Scheduler": {
      "MaxConcurrent": 32, "MaxQueued": 2000, "MaxQueueTime": "00:02:00",
      "Reservations": [ { "MinPriority": 3, "Slots": 4 } ],
      "TenantLimit": 8, "TenantLimits": { "acme": 16 }
    }
  }
}
```

---

## The API

### .NET (`Datalium.Strata.Scheduling`)

| Member | |
|---|---|
| `RunScheduler(RunSchedulerOptions options)` | Validates the options (`ArgumentException` / `ArgumentOutOfRangeException`). |
| `ValueTask<Admission> AdmitAsync(AdmissionRequest request, CancellationToken cancellationToken = default)` | Waits for a slot. What the engine calls; public so an application can schedule work of its own beside its runs. |
| `void SetTenantLimit(string tenant, int? limit)` | Overrides one tenant's limit (0 pauses it), or with `null` returns it to `TenantLimit`. Takes effect at once: raising it admits waiting runs, lowering it stops none that are running. |
| `SchedulerSnapshot Snapshot()` | `Active`, `Queued`, `MaxConcurrent`, `MaxQueued`, `IsClosed`, and per tenant `Active`, `Queued`, `Limit`. |
| `void Close()` / `bool IsClosed` | Admits nothing more: every waiting run and every later one is refused with `Closed`. Runs holding slots are not touched. |

`RunSchedulerOptions` (init-only): `Name` (`"default"`, a metric attribute), `MaxConcurrent` (16, at least 1), `MaxQueued` (1,024, at least 0), `MaxQueueTime` (`null`: no limit), `Reservations` (`IReadOnlyList<SlotReservation>`, empty; each `Slots` ≥ 1, `MinPriority`s distinct, and all of them together fewer than `MaxConcurrent`), `TenantKey` (`"tenant"`), `TenantLimit` (`null`: none), `TenantLimits` (`IReadOnlyDictionary<string, int>`, each ≥ 0), `TenantMaxQueued` (`null`: none).

`AdmissionRequest` (record): `Priority` (int), `Tenant` (string?), `Deadline` (DateTimeOffset?), `TimeProvider` (TimeProvider?, the clock for `MaxQueueTime`, the deadline and `Waited`).

`Admission` (`IDisposable`): `IsAdmitted`, `Refusal` (`NotAdmittedReason?`), `Waited` (TimeSpan). Disposing an admitted one gives its slot back, once.

`NotAdmittedReason`: `QueueFull`, `Shed`, `QueueTimeout`, `Cancelled`, `Deadline`, `Closed`.

In the engine:

- `RunOptions.Scheduler` (`RunScheduler?`) and `RunOptions.Priority` (int, 0).
- `GraphExecutionResult<TState>.NotAdmitted(NotAdmittedReason Reason, TimeSpan Waited) : Failed(message)`.
- `RunStarted.QueuedFor` (`TimeSpan?`): how long the run waited for its slot, `null` without a scheduler.

### Rust (`datalium_strata::scheduler`)

- `RunScheduler` (`Clone`, one shared scheduler behind an `Arc`): `new(SchedulerOptions) -> Result<Self, StrataError>`, `admit(AdmissionRequest) -> impl Future<Output = Result<RunSlot, NotAdmitted>>`, `set_tenant_limit(tenant, Option<u32>)`, `snapshot() -> SchedulerSnapshot`, `close()`, `is_closed()`, `options()`. `admit` takes the run's place in the queue when it is called, not when the future is first polled; dropping the future while it waits withdraws the run (`Cancelled`), and dropping it once admitted gives the slot back.
- `SchedulerOptions::new(max_concurrent)` with `with_name`, `with_max_queued`, `with_max_queue_time`, `with_reservation(min_priority, slots)`, `with_tenant_key`, `with_tenant_limit`, `with_tenant_override(tenant, limit)`, `with_tenant_max_queued`. Invalid options are `StrataError::ExecutionAborted` from `new`.
- `AdmissionRequest::new()` with `with_priority(i32)`, `with_tenant`, `with_deadline(Instant)`, `with_cancellation(CancellationToken)`.
- `RunSlot` gives its slot back when dropped; `waited()`.
- `NotAdmitted { reason: NotAdmittedReason, waited: Duration }`, `NotAdmittedReason` as in .NET.
- `RunConfig::with_scheduler(RunScheduler)`, `with_priority(i32)`; `StrataError::NotAdmitted { reason, waited }`; `RunEventKind::RunStarted` gains `queued_for: Option<Duration>`.

The clock is tokio's, as for every other wait (a paused runtime in tests).

---

## Semantics

### Admission

A slot is free **for a run at priority `p`** when

```
active < MaxConcurrent − (slots reserved for priorities above p)
```

where a reservation `(MinPriority m, Slots n)` is reserved for priorities above `p` when `m > p`. So with 32 slots and `(3, 4)`, `(2, 4)`: priority 0 and 1 runs may fill 24, priority 2 runs 28, priority 3 and above all 32. A reservation is headroom, not a separate pool: priority 3 runs use the unreserved slots first like anyone, and a low-priority run already running is never stopped.

A run is **admissible** when a slot is free for its priority and its tenant (if it has one and a limit applies) has fewer runs active than its limit.

Waiting runs are kept in rank order: higher priority first, then earlier arrival. Whenever something changes (a run arrives, a slot is given back, a limit changes, a wait ends) the scheduler walks the queue in rank order and admits every admissible run, skipping one whose tenant is at its limit, and stopping at the first one no slot is free for (no run after it can have one: equal or lower priority means equal or less headroom). A run never overtakes a waiting run of higher or equal priority unless that run's tenant is what holds it.

### Overload

When a newcomer can't be admitted it joins the queue, and then:

1. If its tenant now has more than `TenantMaxQueued` waiting, the newcomer is refused (`QueueFull`).
2. If the queue now holds more than `MaxQueued`, the lowest-ranked waiting run (lowest priority, latest arrival) leaves: refused with `Shed` if it is not the newcomer, with `QueueFull` if it is. So a newcomer sheds only a run it strictly outranks, and `MaxQueued = 0` means "start now or not at all".

A waiting run also leaves the queue when its token fires (`Cancelled`), its deadline passes (`Deadline`), it has waited `MaxQueueTime` (`QueueTimeout`), or the scheduler is closed (`Closed`). If the run was admitted at the same moment, it keeps its slot and starts: the engine's own checks see the token or the deadline first thing, as they would for any run.

### What a run does with its slot

| Call | Waits for its slot | Gives it back |
|---|---|---|
| `RunAsync` / `run_with`, `RunStreaming` | before the lease, before its first save | when the call returns, however it ends |
| `ResumeAsync`, `ResumeStreaming`, `SignalAsync`, `RecoverAsync`, `RecoverStreaming` | before it loads the checkpoint | when the call returns |
| `WakeDueAsync` / `RecoverStaleAsync` | per thread, after it is listed and before it is loaded (a wake that turns out to be a timeout with no route ends the thread in that slot) | when that thread's run returns |
| `EndAsync` / `end` | **never**: ending runs no node | |
| A subgraph's child, a group's branch, a map's item | **never**: part of the run | |

The run's **tenant** is its metadata's `TenantKey` value: `RunOptions.Metadata` for a direct call, which is all that is known before the checkpoint is loaded; for a driver's thread, the listed thread's metadata with the driver's laid over it. A resume that should count against a tenant passes the tenant in its metadata, as StrataOps already does.

A run that waits in a retry's backoff, or a handler that waits on a model, still holds its slot: a slot is a run in flight, whatever it is doing. A durable run's crash leaves no slot behind, because a slot lives in the process.

The **stop token** of the drivers (`WakeOptions.Stop`, `RecoverOptions.Stop`) also ends a driver's wait for a slot: nothing has started, so stopping there loses nothing, and the round returns what it did.

### Not admitted

The call returns `NotAdmitted` / `StrataError::NotAdmitted`:

| Reason | Message |
|---|---|
| `QueueFull` | `run not admitted: the scheduler's queue is full` |
| `Shed` | `run not admitted: shed from the scheduler's queue for a higher-priority run` |
| `QueueTimeout` | `run not admitted: no slot within the scheduler's longest wait` |
| `Cancelled` | `run not admitted: cancelled while waiting for a slot` |
| `Deadline` | `run not admitted: its deadline passed while it waited for a slot` |
| `Closed` | `run not admitted: the scheduler is closed` |

Streaming calls return their reader at once; a run that isn't admitted ends it with no chunks and completes with `NotAdmitted`, as a resume that can't be resumed does. The drivers skip the thread (`it was not admitted: <message>`); the wake driver's next round finds it due again, the recovery driver's finds it still running. Both decision workers leave the decision for their next round, as for a held thread (`design/32`): a `Failed` decision is remembered and never tried again, and nothing was tried.

### Closing

`Close()` refuses every waiting run and every later one with `Closed`, and touches no run that holds a slot. The hosting packages close their scheduler when the host stops, so a queued run doesn't hold the shutdown up; with the drivers' stop tokens, a stopping host starts nothing new and lets what is running finish (`design/35`).

---

## Hosting

**.NET.** `IStrataBuilder.AddScheduler(Action<StrataSchedulerOptions>? configure = null)` binds `Strata:Scheduler` (`Name`, `MaxConcurrent`, `MaxQueued`, `MaxQueueTime`, `Reservations`, `TenantKey`, `TenantLimit`, `TenantLimits`, `TenantMaxQueued`) and then the delegate, and registers one `RunScheduler` singleton, built at start-up so bad options stop the host from starting. `StrataRunner<TState>.CreateOptions` gains `priority` and sets `Scheduler` to the registered one; the wake and recovery drivers' options gain `Priority` (`Strata:Wake:Priority`, `Strata:Recovery:Priority`, 0). When the host stops, the scheduler is closed after the drivers' stop tokens fire.

**Rust.** `StrataSettings.scheduler: Option<SchedulerSettings>` (the same fields, `snake_case`, durations in the settings' forms) with `open() -> Result<RunScheduler, StrataError>`; `RunSettings::config_with(&scheduler)` adds it to a run's config; `WakeDriver` and `RecoveryDriver` take `with_priority`. `Host::close_on_shutdown(scheduler)` closes it when `run`'s shutdown fires.

---

## Telemetry

| | |
|---|---|
| **`strata.scheduler.admissions`** | Counter, `{run}`: every admission decision, by `strata.scheduler.name`, `strata.scheduler.outcome` (`admitted`, `queue_full`, `shed`, `queue_timeout`, `cancelled`, `deadline`, `closed`) and `strata.run.priority`. |
| **`strata.scheduler.wait`** | Histogram, `s`: how long a run waited, admitted or not, by name and outcome. Next to nothing for a run admitted on arrival. |
| **`strata.scheduler.queued`** | Up-down counter, `{run}`: runs waiting, by name. Rust: a gauge. |
| **`strata.scheduler.active`** | Up-down counter, `{run}`: slots held, by name. Rust: a gauge. |
| Run span | `strata.run.priority` and `strata.scheduler.wait_ms` on an admitted run's span. A run that isn't admitted has no span. |
| Run event | `RunStarted.QueuedFor` / `queued_for`. A run that isn't admitted has no events. |

The tenant is not a metric attribute (unbounded cardinality); `Snapshot()` answers per-tenant questions. .NET instruments are on the engine's `Datalium.Strata` meter; Rust uses the `metrics` facade, names in `observability::metric_names`.

---

## Wire format

None. No checkpoint, definition, database or Redis change. A new shared case file, **`schema/fixtures/scheduler/cases.json`**, scripts arrivals, releases, limit changes, closes and expired waits against a scheduler with given options, and states who is admitted, waiting or refused after each step. Both engines replay it, so the rules above are one rule.

---

## Migration

Additive. A run with no scheduler behaves exactly as before. Rust's `RunEventKind::RunStarted` gains a field, which breaks a pattern that names every field without `..` (as `graph` did in `design/31`).

**StrataOps:** `InvestigationRuns`' semaphore becomes `AddScheduler()` with `MaxConcurrent` from `Investigations:MaxConcurrent`; the run's priority comes from the incident's severity, and its metadata already carries `tenant`. The consumer still waits for the run to be under way before it acknowledges the message, so it keeps holding its partition back. A `NotAdmitted` request is published as a refused investigation instead of being left on the partition. The tenant quotas in `config/tenants` become `Strata:Scheduler:TenantLimits`.

---

## Tests

Mirrored in both engines (`RunSchedulerTests` / `tests/scheduler.rs`):

- **The shared cases**: `schema/fixtures/scheduler/cases.json`, every case, through the public API.
- **Ordering**: higher priority first, arrival order within one; a reservation keeps its headroom from lower priorities and lends nothing back; a tenant at its limit doesn't hold back another tenant; raising a tenant's limit admits waiting runs, 0 pauses a tenant and running runs go on.
- **Overload**: a full queue refuses an equal-priority newcomer, sheds the lowest-ranked waiting run for a higher one, `MaxQueued = 0`, `TenantMaxQueued`.
- **Waits**: `MaxQueueTime`, a deadline and a token each end a wait with their reason, on a fake clock (.NET `FakeTimeProvider`, Rust a paused runtime); an admission racing a timeout keeps its slot; a slot given back twice counts once.
- **Close**: waiting runs refused, later runs refused, running runs untouched.
- **The engine**: every run mode waits for a slot before it loads or writes (a store that records calls sees nothing until the slot is free); a run not admitted writes nothing, emits no event and returns `NotAdmitted`; an interrupted, failed, cancelled or thrown run gives its slot back; a subgraph, a group and a map take one slot; ending takes none; streaming ends with no chunks; `RunStarted.QueuedFor` and the span tags; the drivers skip a thread not admitted and stop waiting on their stop token; the decision workers leave a decision not admitted for the next round.
- **Hosting**: options bound from configuration and validated at start-up; `StrataRunner` carries the scheduler and the priority; drivers' priority; the scheduler closed on stop.
- **Mutation checks**: a scheduler that ignores reservations, admits past a tenant's limit, sheds an equal-priority run, or holds a slot after a run throws, fails a test.

---

## Review decisions

Decided by Nick before the blueprint (2026-10-14): 1 to 4. The rest are this document's; the build follows each recommendation.

1. **Engine or hosting?** **Engine.** Every caller, including one that uses `CompiledGraph` directly and both drivers, gets the same backpressure, and the queue wait is part of the run's own telemetry.
2. **Priority model?** **Strict priority with reserved slots.** No aging, no weighted shares: the point is that SEV-1 goes first and always finds capacity; starvation of low priority under sustained overload is what shedding and `MaxQueueTime` make visible.
3. **Overload?** **A bounded queue that sheds the lowest-ranked run for a newcomer that strictly outranks it, refusing the newcomer otherwise; a `NotAdmitted` result, not an exception.**
4. **Tenant quotas?** **Concurrency only**, by a metadata key, with overrides changeable at runtime.
5. **Does ending a thread need a slot?** **No.** It runs no node and writes one checkpoint; making a cancellation wait behind the work it is cancelling would be the wrong way round. (A wake that ends a timed-out thread does so in the slot it already waited for: see "Implementation Status".)
6. **When does a resume wait: before or after it loads?** **Before.** A checkpoint loaded and then held while queued is stale by the time the run starts; waiting first means the resume decides on what is there when it can act. The cost is that a resume's tenant is what the caller passes, not what is stored.
7. **What a `NotAdmitted` run leaves behind.** **Nothing: no checkpoint, no event, no span; only the metrics.** It never started; a `failed` checkpoint would put a thread that never ran into a state a human has to look at.
8. **A token cancelled while queued: `Cancelled` or `NotAdmitted(Cancelled)`?** **`NotAdmitted(Cancelled)`.** `Cancelled` promises a `cancelled` checkpoint and a state; there is neither.
9. **One scheduler for the process, or one per graph?** **Whatever the application passes.** A scheduler is an object; hosting registers one, and an application that wants pools (general, high-priority) makes two and names them.
10. **Changing `MaxConcurrent` while running?** **Not built.** Tenant limits change at runtime because pausing a tenant is an operation; the process's capacity is configuration.

---

## Implementation Status

Built 2026-10-14 in both engines, both hosting packages and both decision workers, in one change, as specified above. Decisions 1 to 4 were Nick's; 5 to 10 follow the recommendations and are open to review.

**Where it diverged, or filled a gap, and why:**

- **A wake's timeout end runs in the slot the wake waited for.** A wake waits for its slot before it loads the thread (decision 6), so it can't know yet whether the thread will be resumed or ended; a timeout with no route then ends the thread in that slot. Only `EndAsync` / `end` takes none.
- **The `QueueTimeout` message doesn't name the time** (`no slot within the scheduler's longest wait`): the result is built where the options aren't known, and `Waited` says how long anyway.
- **Rust's `admit` takes its place when called.** It returns a future whose queue entry exists from the call, so two calls are queued in call order however their futures are polled, and the shared cases replay deterministically. The future's withdrawal guard is made at the call too: a future dropped before its first poll leaves no dead entry behind (the first build made it inside the `async` block, and a test of exactly that failed). A slot decided for a future nobody polls any more is given back once the scheduler's lock is released.
- **Waits share the run's clock.** .NET's `AdmissionRequest.TimeProvider` is the run's `RunOptions.TimeProvider`; Rust uses tokio's, so a paused runtime moves `MaxQueueTime` and the deadline with the run's other waits.
- **A driver's stop token in Rust drops the wait** (`select!` between the admission and the token) rather than going into the request, which takes one token; dropping withdraws the run, reported `Cancelled`.
- **Rust's drivers take an optional priority** (`WakeDriver::with_priority`, `priority` in settings; unset: the config's own), because their config is already the place a scheduler and a priority go. .NET's driver options have `Priority` (0).
- **Hosting closes the scheduler in `StoppingAsync`** (.NET `IHostedLifecycleService`), before any hosted service's `StopAsync`, so a run waiting for a slot is refused before the drivers are asked to stop. Rust's `Host::close_on_shutdown` closes it when the token fires.
- **`RunScheduler.Options` / `options()`** are public: the engine reads the tenant key from them, and hosting and tests check what was bound.
- **Rust's `set_tenant_limit` takes any string**; .NET refuses a blank tenant (`ArgumentException`).
- **Rust's metric test lives in `tests/scheduler_engine.rs`**, with that binary's own recorder: the debugging recorder drains histograms on every snapshot, and a snapshot in `telemetry.rs` emptied another test's node-duration histogram (it failed once, in the full run). `telemetry.rs` checks the span only.

**Tests.** .NET: `Scheduling/RunSchedulerTests.cs` (the 13 shared cases and 7 more), `Scheduling/SchedulerEngineTests.cs` (13), four in `Hosting/HostingTests.cs`, one in `Cloud/DecisionWorkerTests.cs`. Rust: `strata-core/tests/scheduler.rs` (the shared cases and 8 more), `tests/scheduler_engine.rs` (13), one in `tests/telemetry.rs`, two in `strata-host/tests/drivers.rs` and one in `settings.rs`, one in `strata-cloud/tests/worker.rs`. Mutations, every one caught in both engines: tenant limits ignored, reservations ignored (Rust; .NET checked the same by hand), LIFO within a priority (so an equal-priority run is shed), a slot kept after the run returns, a driver's stop token not reaching its wait. Verified locally: `dotnet test` 799 passed (84 skipped: the Postgres and Redis tests, whose servers weren't configured; nothing here touches a store); `cargo test --all-features` 816 in `rust/`, 80 in `cli/`, 1 in `examples/rust-agent/`; `dotnet format --verify-no-changes`, `cargo fmt --check` and `cargo clippy -D warnings` clean.

---

## Not built (deliberately out of scope)

- **Rate limits** (runs, model calls or tool calls per second) and cost budgets: the application's (StrataOps' Model Fabric and Tool Gateway).
- **Aging and weighted fair shares.**
- **Changing `MaxConcurrent` at runtime**, and adaptive capacity from CPU, memory or downstream latency (Arch §6.7's other inputs).
- **Scheduling inside a run**: a group's or a map's `MaxConcurrency` already bounds its branches.
- **Preemption**: a running run is never stopped to make room.
- **A distributed scheduler** (SG-26): one scheduler bounds one process. Several replicas each have their own.
- **A health check** for saturation: `Snapshot()` is there for an application that wants one.
