# Datalium Strata: Waking a Thread Through Its Own Resume

`design/29-Event-Waits-And-Timers.md` (SG-16) gave threads timers and timeouts, and one way to act on them when they come due: `WakeDueAsync` / `wake_due` (and the hosted wake drivers that call it). It lists due threads, decides each again, and wakes them one at a time, every run with the same `WakeOptions.Run`, none of them streaming. A resume with a `HumanSignal` is refused at a timer ("it wakes when due"), and the signals a wake uses (`LevelSignals.ForWake`, Rust `for_wake`) are internal.

StrataOps' M4 adds an approval timeout and a timed verification window to its remediation subgraph, and runs every investigation through its own runner: `ResumeStreaming` so agents' output reaches the console, an observer per run that turns events into the incident's milestones, a claim per incident so a decision waits for the run before it, and the scheduler at the incident's priority. A run woken under `WakeDueAsync` would stream nothing, publish nothing of its own, and hold every other due thread until it ended. StrataOps can list due threads itself (`ThreadQuery.DueBy` is public); what it can't do is wake one through its own resume.

This document closes that gap: **SG-35**. The SG numbers refer to the gap log of StrataOps (not public yet). It lands in both engines at one commit (strict parity, StrataOps ADR 0003).

It was written as a blueprint before implementation and is now built. "Review decisions" lists the questions for review with the answer the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| The shape | **Public wake signals that the existing resumes accept**: `LevelSignals.Due` (Rust `LevelSignals::due()`, and `due_at(now)`). `ResumeAsync`, `ResumeStreaming`, `resume_with` and `resume_streaming_with` take it like any `LevelSignals`. No new entry point. |
| What it does | **What `WakeDueAsync` does for one thread**, decided at the resume's own load: a due timer continues at the edge out of it, a due timeout takes its route, and a due timeout with no route ends the thread `timed_out` (`Cancelled(Ended)`). |
| "Now" | **.NET: the run's `RunOptions.TimeProvider`**, as for `WakeDueAsync`. **Rust: `Utc::now()`** for `due()`, or the caller's time for `due_at(now)`, as `wake_due` takes `now`: tokio's clock is monotonic, not wall time. |
| Refusals | **`Failed`, nothing written**, as every resume refusal: the resume's existing refusals first (no checkpoint, running, ended, completed, already resumed), then **not due** (with why), then another graph's thread, then a wait this graph doesn't have. |
| Races | **Unchanged**: the resume stamp (a durable graph's claim) and the end are conditional on the sequence the resume loaded, so this resume, a `WakeDueAsync` driver, a human, a signal or an end can't both act on one thread. |
| Streaming | **A streaming resume of a wake streams** like a human resume does, which lifts design 29's "Streaming a signal or a wake" for wakes. |
| Signals, too | **`LevelSignals.Event(EventSignal)`** (Rust `LevelSignals::event(signal)`) answers an event wait through the same resumes, so a signal can stream as well. `SignalAsync` / `signal` are unchanged. |
| Outer levels | **A wake or event signal takes no signals for outer levels** (`.At(path, …)`): refused before anything is written, as design 29 left outer-level overrides with a signal unbuilt. |
| An ending timeout's run | **The resume it was called as**: `RunStarted(Resume)`, `CheckpointSaved(Ended)`, `RunCompleted(Cancelled, Ended)`, and `strata.thread.wakes` counts it. `WakeDueAsync` still reports its own ends as `RunMode.End`. |
| `WakeDueAsync` | **Keeps its contract and its own loop**; it shares the "due for what" decision and the timed-out end with the new path. |
| Wire format | **None.** |

---

## Usage

```csharp
// StrataOps' Worker, every 5 s: list due investigation threads, wake each through the runner.
var due = await store.ListThreadsAsync(new ThreadQuery { DueBy = clock.GetUtcNow(), GraphId = "investigation" });
foreach (var thread in due.Threads)
{
    var (chunks, completion) = investigation.ResumeStreaming(
        thread.ThreadId, LevelSignals.Due, store, runner.OptionsFor(thread), ct);
    await console.PumpAsync(chunks, ct);
    var result = await completion;
    // Completed / Interrupted (the next timer) / Cancelled(Ended) for a timeout with no route /
    // Failed: not due any more, another driver got there first, held by another owner, …
}

// A webhook that wants the signalled run's tokens:
var (tokens, done) = investigation.ResumeStreaming(
    threadId, LevelSignals.Event(new EventSignal("deployment.finished").WithOverride("outcome", "ok")), store);
```

```rust
let result = graph
    .resume_with(&thread_id, LevelSignals::due(), store.clone(), config)
    .await;
let (stream, completion) = graph
    .clone()
    .resume_streaming_with(&thread_id, LevelSignals::due(), store.clone(), config);
// Tests, or a driver that fixed its time once:
let result = graph.resume(&thread_id, LevelSignals::due_at(now), store.clone()).await;
```

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Interruption;

public sealed class LevelSignals
{
    // … as before …

    /// Wakes the thread if it is due at the run's clock: a timer, or a wait whose timeout passed.
    public static LevelSignals Due { get; }

    /// Answers a thread waiting for signal's event, as SignalAsync does.
    /// Throws ArgumentException for a blank event type.
    public static LevelSignals Event(EventSignal signal);
}
```

### Rust

```rust
impl LevelSignals {
    /// Wakes the thread if it is due at `Utc::now()`.
    pub fn due() -> Self;
    /// Wakes the thread if it is due at `now`.
    pub fn due_at(now: DateTime<Utc>) -> Self;
    /// Answers a thread waiting for `signal`'s event, as `signal` does.
    pub fn event(signal: EventSignal) -> Self;
}
```

Rust can't refuse a blank event type when the signals are made (the constructor returns `Self`), so the resume refuses it, before anything is loaded: `a signal needs an event type`, as `signal` says.

---

## Semantics

### Deciding again

A resume given `Due` loads the thread's latest checkpoint with its sequence, as every resume does, and then, at the outermost level:

1. The resume's existing refusals apply first, with their messages: no checkpoint, a running thread, an ended one, a completed one, one already resumed.
2. **It must be due at now**: the latest checkpoint is `interrupted`, not resumed, and its innermost `wait.wake_at` is at or before now (`ThreadQuery.IsDue` / `is_due`, both at whole seconds). What it is due for is the stored wait: a timer's wake, or a timeout for a human or event wait.
3. **It must be this graph's**: the identity check and migration (`design/31`), then the resume preflight (`design/21`), which refuses a wait this graph doesn't have at that node (`waits at [Deploying], which has no timeout in this graph`).
4. Then it is that wake, exactly as `WakeDueAsync` would perform it: a timer or a timeout with a route continues through the resume's conditional stamp (or a durable graph's claim, `design/33`), with `Resumed { Wake = Timer | Timeout, TimedOutTo }`; a timeout with no route, at the innermost level, **ends** the thread with `strata.end_reason` `timed_out`, through the end's conditional save, and returns `Cancelled(Ended)`.

### Refusals

`Failed`, nothing written, the thread exactly as it was:

| The thread | Message |
|---|---|
| failed or cancelled | `thread 't' is not due: its latest checkpoint is failed` |
| waiting with no time (a human with no timeout, an event wait with none) | `thread 't' is not due: it is waiting for a human at [Approve], with no timeout` |
| waiting, not yet | `thread 't' is not due until 2026-10-06T12:05:00Z: it is waiting for a timer at [Window]` |
| another graph's | the identity refusal (`thread 't' was written by graph 'x' …`) |
| a wait this graph doesn't have | the preflight's (`thread 't' waits at [Deploying], which has no timeout in this graph`) |
| a wake or event signal with outer-level signals | `cannot resume thread 't': a wake or an event signal answers the innermost level only, and takes no signals for outer levels` |
| ended, completed, already resumed, running, none | the resume's own messages |
| another resume, a driver or an end got there first | `checkpoint for thread 't' changed before it could be resumed: …`, or for an end `thread 't' changed before its timeout could end it: …` |
| held by another owner's lease | `thread 't' is held by 'worker-2' until …` (`design/32`) |

### What behaves as for any resume

Admission to the scheduler (before the load, at `RunOptions.Priority`, with the tenant from the run's metadata), the lease (acquired after the preflight and before the stamp or the end, renewed, released when the call returns), the observer and the span, metadata carried from the checkpoint and overridden key by key, the deadline and the token, the hop count, a durable graph's claim and per-node checkpoints, nesting (the wake reaches the innermost level; a timeout's route is the innermost graph's; a timeout with no route ends every level).

### Streaming

`ResumeStreaming(threadId, LevelSignals.Due, …)` streams the woken run's chunks, from the node it continues at, as a human resume's do; inside a subgraph the parent's subgraph node emits a new `NodeStart` and the child's chunks follow, named `<subgraph>/<child node>`. A refusal or an end ends the stream with no chunks, and the completion says what happened. `LevelSignals.Event` streams a signalled run the same way.

---

## Review decisions

Recommendations are in bold; the build follows each.

1. **A wake signal, a per-thread callback in `WakeOptions`, or a public `WakeOneAsync`?** **A wake signal.** A callback (`Func<ThreadSummary, RunOptions>` and concurrency) fixes the options but not the claim and milestones an application keeps around its own runs, and leaves the driver's loop in charge. `WakeOneAsync` is equivalent but a second entry point beside the resumes, with its own streaming variant. The signal reuses both resumes, streaming included.
2. **A static property or a factory?** **.NET `LevelSignals.Due`, a property**, as the handoff asked: the clock is the run's `TimeProvider`, so there is nothing to pass. **Rust `due()` and `due_at(now)`**, since its wall clock isn't injectable, as `wake_due` takes `now`.
3. **Where the decision happens.** **At the resume's own load**, not before the call: the application's listing may be stale, and only the load the stamp is conditional on can say the thread is still due.
4. **The order of refusals.** **The resume's own first**, so an ended or already-resumed thread says so, then not due, then identity, then the preflight; the same order as `WakeDueAsync`'s skips, whose "due for what" is the stored wait, whatever a migration makes of it.
5. **A timeout with no route: end inside the resume, or refuse?** **End**: the handoff's semantics are `WakeDueAsync`'s, and an application waking its own threads has no other way to end one `timed_out` (`EndAsync` writes `requested`).
6. **That end's run mode.** **`Resume`, the call that started it.** `RunMode` is fixed when the run starts (its span, its `strata.graphs.started` count), before the checkpoint is loaded; an observer tells the end by `RunCompleted(Cancelled, Ended)` and the `CheckpointSaved(Ended)` before it. Changing the mode mid-run would make the span and the counter disagree with the event.
7. **Rebuild `WakeDueAsync` on the new path?** **No.** It would turn every skip into an observed failed run and every timed-out end into a `Resume`-mode run, which is its contract changing. It shares the decision of what a thread is due for and the timed-out end instead, so the two can't drift.
8. **Streaming signals.** **`LevelSignals.Event(EventSignal)`**, the same mechanism, rather than a `SignalStreaming` method: no new entry point, and it lifts the rest of design 29's "Streaming a signal or a wake".
9. **Outer-level signals with a wake.** **Refused**: a wake overrides nothing, and design 29 left outer-level overrides with an event signal unbuilt.

---

## Tests

Mirrored in both engines, in new suites `wake_through_resume.rs` / `WakeThroughResumeTests.cs`:

- A timer and then a **streaming** node, woken through `ResumeStreaming(Due)`: it streams and completes, and the run events say `Resumed { Wake = Timer }`.
- The same inside a subgraph: the wake reaches the nested level, and the outer level's streaming node after the subgraph streams.
- A boundary timeout with a route (escalates, `TimedOutTo`), and one without (ended `timed_out`, `Cancelled(Ended)`, the end's events in `Resume` mode, `strata.end_reason` found by the query port).
- **Not due** (one second early), **a human wait with no timeout**, **a failed thread**, **another graph's thread**, **an ended thread**, and **outer-level signals**: each refused, nothing written.
- **Two concurrent wakes** of one thread, one through `Due` and one through `WakeDueAsync`: exactly one wins, the other refused (`changed before it could be resumed`) or skipped.
- **A lease**: a thread another owner holds is refused with nothing written; the woken run acquires the lease, saves under it and releases it.
- **The scheduler**: the woken run waits for a slot at the run's priority.
- **A durable graph**: the wake is claimed with a `running` checkpoint, not a stamp.
- **Streaming a signal**: `ResumeStreaming(LevelSignals.Event(…))` streams the signalled run; a blank event type is refused.
- **Mutation checks**: skipping the due decision; stamping unconditionally; resuming an ending timeout instead of ending it.

---

## Wire format

None: no checkpoint, definition, database or fixture change. A `Due` resume writes what `WakeDueAsync` writes for the same thread.

---

## Implementation Status

Built 2026-10-20 in both engines, in one change, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **Rust refuses a blank event type in `resume_with` / `resume_streaming_with`**, with the config's own refusals and before admission, since `LevelSignals::event` returns `Self`. .NET throws `ArgumentException` from `LevelSignals.Event`, as `SignalAsync` does.
- **`RunStarted.Node` for a `Due` resume that ends the thread is the node it waits at** (`StartsAt` / `starts_at`), as `WakeDueAsync`'s end reports it, not the next node a resume would have continued at.
- **`strata.thread.wakes` for a timed-out end is now counted inside the end itself** (`EndOnTimeoutAsync` / `end_on_timeout`) when its save succeeds, for both the driver and a `Due` resume. The driver counted it after the run, with the same conditions (not superseded, not held, the lease not lost), so its count is unchanged.

**How it was built:**

- **.NET:** `LevelSignals` gained `Due`, `Event(EventSignal)`, and internally `WhenDue`, `DueFor` (shared with `WakeOneAsync`), `WhenDueAt` and `NotDue`; `Check` refuses outer-level signals with a wake. `CompiledGraph.Interruption.cs`: `ResumeLoadedAsync` decides the wake at the load on `RunScope.Clock` and reports `RunStarted` through `StartsAt`; `ResumeCheckpointAsync` refuses an undecided `Due` after its own refusals, and ends the thread at the outermost level when the preflight says a timeout ends it. `CompiledGraph.Waits.cs`: `EndTimedOutAsync` (the driver's, which starts its telemetry) calls the new `EndOnTimeoutAsync`, which both paths share.
- **Rust:** `LevelSignals` gained the `due` field (`DueAt::Now` / `At`), `due()`, `due_at(now)`, `event(signal)`, and privately `validate`, `due_for` (shared with `wake_one`), `when_due`, `not_due`; `check` refuses outer-level signals with a wake. `resume_loaded`, `resume_checkpoint_at`, `starts_at`, and `end_timed_out` / `end_on_timeout`, as in .NET.

**Tests:**

- **New suites:** `rust/strata-core/tests/wake_through_resume.rs` (11) and `dotnet/tests/Datalium.Strata.Tests/WakeThroughResumeTests.cs` (11), test for test, covering every case in "Tests" above. `EventWaitTests` / `event_waits.rs` pass unchanged.
- **Mutation checks, in both engines** (each broken, the suites run, then restored):
  - Deciding the wake from the stored wait without checking it is due fails 1 test in each (the not-due refusals).
  - Stamping a resume unconditionally fails the `Due`-against-driver race in each, and design 29's two-drivers test.
  - Resuming a timeout with no route instead of ending the thread fails 1 test in each.
- **Totals when this landed:** .NET 853 passed and 128 skipped (Postgres and Redis weren't running), plus the example's 1. Rust: `rust/` 928, `cli/` 82 and `examples/rust-agent` 1, also without Postgres or Redis. `cargo fmt`, `cargo +1.99.0 clippy --all-targets --all-features -D warnings` and `dotnet format --verify-no-changes` are clean.

## Not built (deliberately out of scope)

- **Waking several threads concurrently inside `WakeDueAsync`.** An application that wants concurrency lists due threads and resumes them with `Due` itself.
- **Outer-level overrides with a wake or an event signal.**
- **A per-thread options callback in `WakeOptions`** (review decision 1).
