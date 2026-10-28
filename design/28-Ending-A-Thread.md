# Datalium Strata: Ending a Waiting Thread

A thread that stops (at an interrupt boundary, a failed node or a cancellation) leaves a checkpoint, and the only way out of it is a resume (`design/16`, `design/17`, `design/21`). Nothing says "this thread is finished and must never run again". A thread whose approval will never come stays `interrupted` forever, in every store, in every listing and for the CLI.

StrataOps hits this in M4, governed remediation:

- **Cancelling an investigation that waits for approval** (`POST /v1/incidents/{id}/cancel`). The incident is cancelled, but its thread stays `interrupted`. StrataOps refuses later decisions itself (its ADR 0020). Without a fix, its approvals queue (M4) and recovery sweeper (M5) would each have to filter dead threads out.
- **Approval timeouts** (M4) end a wait nobody answered. That's `design/29`'s timer firing into this document's ending.

This document specifies **SG-34**: one call that ends a thread where it stands. The SG numbers refer to the gap log of StrataOps (not public yet). It also adds the **conditional save** that this call and `design/29`'s wake driver both need, and that closes the double-resume race.

SG-34 lands in both engines at one commit (strict parity, StrataOps ADR 0003): the same capability, in each language's idiom.

It was written as a blueprint before implementation and is now built. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| The API | **`CompiledGraph.EndAsync(threadId, store, …)`** (.NET) and **`CompiledGraph::end` / `end_with`** (Rust). It's on the compiled graph, not the store: it writes a checkpoint in the graph's schema, rehydrates the graph's state for the result, and runs under a run's telemetry and observer. "End", not "cancel": a run's cancellation is resumable, an ending isn't. |
| What it writes | A **new terminal status, `ended`**, at **`schema_version` `1.4.0`**. A thread that may never resume looks different from a resumable cancellation in every store, the query port and the CLI. |
| What can be ended | A thread whose latest checkpoint is **`interrupted`, `failed` or `cancelled` and not resumed**. **Ending an ended thread returns the same result and writes nothing.** A completed thread, a resumed one, or one with no checkpoint is refused. |
| A resumed thread | **Refused.** Its resumed run may still be running, or may have died with its process. Ending it safely needs leases (SG-12) or a checkpoint per node (SG-10). See "Not built". |
| Nesting | **A nested wait ends the whole thread, every level**: each level of the checkpoint becomes `ended`, keeping where it stood, its state and its decision. |
| The result | **`GraphExecutionResult.Cancelled`** with the new **`CancelReason.Ended`**. The node is where the thread stood, the state its frozen state, `StoppedAt` / `stopped_at` its path. Refusals are **`Failed`**. |
| Who and why | An optional **`EndedBy`** (an operator, a service) and the end reason are recorded in the checkpoint's metadata as **`strata.ended_by`** and **`strata.end_reason`**. The reason is `requested` here; `design/29` adds `timed_out`. The call's `RunOptions.Metadata` is stamped as a resume stamps it. |
| A later resume | **Returns `Failed`** (`thread 't' was ended: an ended thread can't be resumed`), and writes nothing. |
| Races | A **conditional save** in every store: **`LoadLatestAsync`** returns the latest checkpoint with its sequence, and **`SaveIfLatestAsync(checkpoint, expectedSequence)`** appends only if nothing was appended since. Ending and resuming both use it, so an end and a resume racing on one thread can't both win, and neither can two resumes. |
| Stores that don't have it | The port's **default implementations** load without a sequence and save unconditionally, which is the old behavior and the old race. Every built-in adapter overrides them. |
| Observers | **`RunCompleted`** with outcome `Cancelled` and reason **`Ended`**, after **`RunStarted`** with the new **`RunMode.End`**. No new event type, so consumers that report cancellations already handle it. |
| The query port | A new thread state, **`Ended`** (`ended`). The SQL head-table trigger derives it from the status, unchanged; the Redis scripts gain a sixth state set. |
| The CLI | **Read-only.** `thread list --status ended`, and `thread inspect` shows the ending. No `strata thread end`. |

---

## Usage

```csharp
// StrataOps: POST /v1/incidents/{id}/cancel, while the investigation waits for approval.
var result = await investigation.EndAsync(
    incident.ThreadId,
    ThreadEnd.By(operatorId),
    store,
    new RunOptions { Metadata = { ["incident"] = incident.Id }, Observer = timeline });

switch (result)
{
    case GraphExecutionResult<Investigation>.Cancelled { Reason: CancelReason.Ended } ended:
        // ended.Node / ended.StoppedAt: where it stood, e.g. ["remediation", "approve"].
        break;
    case GraphExecutionResult<Investigation>.Failed refused:
        // Completed already, or resumed and possibly still running: report it, don't retry.
        break;
}

// Later, an approval for it arrives anyway:
var late = await investigation.ResumeAsync(incident.ThreadId, HumanSignal.Approve("alice"), store);
// late is Failed("thread '…' was ended: an ended thread can't be resumed")

// The approvals queue lists only live waits:
var waiting = await ((ICheckpointQuery)store).ListThreadsAsync(new ThreadQuery
{
    States = new HashSet<CheckpointThreadState> { CheckpointThreadState.Interrupted },
});
```

```rust
match investigation
    .end_with(&thread_id, ThreadEnd::by(operator_id), store.clone(), RunConfig::new())
    .await
{
    GraphExecutionResult::Cancelled { reason: CancelReason::Ended, stopped_at, .. } => { /* … */ }
    GraphExecutionResult::Failed(refused) => { /* completed, resumed, or no checkpoint */ }
    _ => unreachable!("ending returns Cancelled or Failed"),
}

// Without who or metadata:
investigation.end(&thread_id, store.clone()).await;
```

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Interruption;

/// Who ends a thread (design/28-Ending-A-Thread.md).
public sealed record ThreadEnd
{
    /// Recorded as `strata.ended_by` when set: an operator, a service, a reviewer.
    public string? EndedBy { get; init; }

    public static ThreadEnd By(string endedBy);
}

public enum CheckpointStatus { Interrupted, Completed, Failed, Cancelled, Ended }   // + Ended

public enum CancelReason { Requested, Deadline, Ended }                             // + Ended

public enum CheckpointThreadState { Interrupted, Failed, Cancelled, Resumed, Completed, Ended }   // + Ended

/// The latest checkpoint of a thread, with its sequence in the thread's log: null when the store
/// can't tell (it doesn't override LoadLatestAsync).
public sealed record LatestCheckpoint(Checkpoint Checkpoint, long? Sequence);

public interface ICheckpointStore
{
    // … Save, Load, SaveAsync, LoadAsync as before …

    /// Default: LoadAsync, with no sequence.
    ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken ct = default);

    /// Appends `checkpoint` only if its thread's latest checkpoint is still the one at
    /// `expectedSequence`, and says whether it did. A null sequence saves unconditionally.
    /// Default: SaveAsync, then true.
    ValueTask<bool> SaveIfLatestAsync(Checkpoint checkpoint, long? expectedSequence, CancellationToken ct = default);
}

public sealed partial class CompiledGraph<TState>
{
    public Task<GraphExecutionResult<TState>> EndAsync(string threadId, ICheckpointStore store);
    public Task<GraphExecutionResult<TState>> EndAsync(string threadId, ThreadEnd end, ICheckpointStore store);
    public Task<GraphExecutionResult<TState>> EndAsync(
        string threadId, ThreadEnd end, ICheckpointStore store, RunOptions options);
}

public enum RunMode { Run, Resume, End }   // + End
```

### Rust

```rust
/// Who ends a thread (`design/28-Ending-A-Thread.md`).
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct ThreadEnd {
    /// Recorded as `strata.ended_by` when set.
    pub ended_by: Option<String>,
}

impl ThreadEnd {
    pub fn new() -> Self;
    pub fn by(ended_by: impl Into<String>) -> Self;
}

pub enum CheckpointStatus { Interrupted, Completed, Failed, Cancelled, Ended }   // + Ended
pub enum CancelReason { Requested, Deadline, Ended }                             // + Ended
pub enum ThreadState { Interrupted, Failed, Cancelled, Resumed, Completed, Ended }   // + Ended (non_exhaustive)

pub struct LatestCheckpoint {
    pub checkpoint: Checkpoint,
    /// `None` when the store can't tell.
    pub sequence: Option<i64>,
}

pub trait CheckpointStore: Send + Sync {
    // … save, load as before …

    /// Default: `load`, with no sequence.
    fn load_latest<'a>(&'a self, thread_id: &'a str)
        -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>>;

    /// Default: `save`, then `true`.
    fn save_if_latest(&self, checkpoint: Checkpoint, expected_sequence: Option<i64>)
        -> BoxFuture<'_, Result<bool, CheckpointStoreError>>;
}

pub trait SyncCheckpointStore {
    // … save, load …
    fn load_latest(&self, thread_id: &str) -> Result<Option<LatestCheckpoint>, CheckpointStoreError>;   // default
    fn save_if_latest(&self, checkpoint: Checkpoint, expected_sequence: Option<i64>)
        -> Result<bool, CheckpointStoreError>;                                                     // default
}

impl<S> CompiledGraph<S> {
    pub async fn end(&self, thread_id: impl Into<String>, store: Arc<dyn CheckpointStore>)
        -> GraphExecutionResult<S>;
    pub async fn end_with(&self, thread_id: impl Into<String>, end: ThreadEnd,
        store: Arc<dyn CheckpointStore>, config: RunConfig) -> GraphExecutionResult<S>;
}

pub enum RunMode { Run, Resume, End }   // + End
```

- `ThreadEnd` is a record (a struct) and not a bare string, so `design/29` can end a thread with a reason a caller can't pass, and later fields don't change the signature. Callers pass only who ends it; the reason is always `requested` from a caller.
- **No cancellation token or deadline.** Ending is one load and one save, and store calls never take a run's token (`design/19`). `RunOptions` / `RunConfig` contribute the metadata, the run id and the observer; their deadline, token, hop limit, history and services are ignored.

---

## Semantics

### What can be ended

The call loads the thread's latest checkpoint with its sequence, then decides:

| Latest checkpoint | Result | Writes |
|---|---|---|
| none | `Failed`: `no checkpoint found for thread 't'` | nothing |
| unsupported `schema_version` | `Failed`, as a resume says it | nothing |
| `interrupted`, `failed` or `cancelled`, not resumed | **`Cancelled(Ended)`** | the `ended` checkpoint |
| `ended` | **`Cancelled(Ended)`**, the same as the first call returned | nothing |
| any status with `resumed_at` set | `Failed`: `thread 't' was resumed at <time> and may still be running: only a waiting, failed or cancelled thread can be ended` | nothing |
| `completed` | `Failed`: `thread 't' has completed: only a waiting, failed or cancelled thread can be ended` | nothing |

Then the frozen state must rehydrate as the graph's state type, for the result. If it doesn't, the call is refused (`failed to rehydrate state for thread 't': …`) and nothing is written.

### What is written

One checkpoint, appended to the thread's log like any other:

| Field | Value |
|---|---|
| `schema_version` | `1.4.0` |
| `status` | `ended`, at every level |
| `suspended_node` | unchanged at every level: where the thread stood |
| `next_node` | `null` at every level: there is nothing to continue at |
| `frozen_state` | unchanged at every level |
| `metadata` | at every level: the call's run metadata (the stored caller keys with the call's laid over them, exactly as a resume settles them, `design/18`), plus `strata.end_reason` (`requested`) and, when given, `strata.ended_by` |
| `created_at` | now, at every level |
| `resumed_at` | `null` |
| `nested` | the stored chain, each level converted as above |
| `decision` | unchanged at every level: who approved what, before the end |

The engine keys of the checkpoint it ends (`error`, `error_kind`, `attempts`, `cancel_reason`, `strata.hops`, `strata.retry_after_ms`) are not carried over. They describe a stop the thread no longer resumes from, and the ended checkpoint's predecessor in the log still has them.

So `waiting_at()` on an ended checkpoint says where it stood when it ended, every listing shows it at that node, and the result's `StoppedAt` is the same path.

### Nesting

A thread frozen inside a subgraph (`design/16`) is one thread. Ending it ends every level: the outermost checkpoint and every nested one become `ended`. Nothing below is left to resume, and a child is never a thread of its own. `ended` is the one status every level of a checkpoint shares without exception. A `failed` parent may nest a `cancelled` child, but an ended parent's child is always ended.

### Idempotence

Ending an ended thread is a success: it returns `Cancelled(Ended)` built from the stored ended checkpoint, and writes nothing. The first call's `EndedBy` stands; a second caller's is ignored. Callers that retry on a timeout, or two operators who both press cancel, see the same answer.

### Resuming an ended thread

A resume of an `ended` checkpoint is refused before anything is stamped: `Failed("thread 't' was ended: an ended thread can't be resumed")`. It is checked where the resume checks for `completed` today, so it covers `ResumeAsync`, `ResumeStreaming` and `resume_with` / `resume_streaming_with` alike, and `LevelSignals` change nothing.

A fresh **run** with an ended thread's id isn't refused. A run never reads the store (it starts at the entry point and appends), and a caller reusing a thread id is starting something new. That is today's behavior for every thread.

### Races: the conditional save

Without a compare-and-set, an end and a resume racing on one thread could both read `interrupted`. Both would append: the resume its stamp, the end its `ended` checkpoint. The resumed run would then go on writing checkpoints after the thread had "ended". Two resumes racing (two workers handling one approval) have the same problem today.

The store port gains two members (shapes above):

- **`LoadLatestAsync` / `load_latest`** returns the latest checkpoint with its sequence in the thread's log, the same sequence `GetLogAsync` / `get_log` and the head table use.
- **`SaveIfLatestAsync` / `save_if_latest`** appends the checkpoint only if the thread's latest sequence is still the expected one, and returns whether it did. Each built-in adapter does it atomically:

| Adapter | How |
|---|---|
| In-memory (both engines) | under the log's lock: compare the log's length − 1, then append |
| SQLite (EF Core, rusqlite) | one `INSERT … SELECT … WHERE (SELECT MAX(sequence) …) = expected`, at sequence `expected + 1`; a primary-key collision with another process counts as losing |
| Postgres (Npgsql, tokio-postgres) | in the save's transaction, after the per-thread advisory lock: compare `MAX(sequence)`, insert only if equal |
| Redis (both engines) | a new script, `SAVE_IF_LATEST_SCRIPT` / `SaveIfLatestScript`: `LLEN − 1` compared with the expected sequence, then the save script's body, in one atomic step |

**Who uses it:**

- **Ending:** it saves the `ended` checkpoint conditionally. On losing, it loads again and decides again, so a thread ended meanwhile returns the idempotent result, and a thread resumed meanwhile is refused. It does this at most three times, then gives up with `Failed("thread 't' kept changing while it was being ended")`.
- **Resuming** (the outermost level): the `resumed_at` stamp is saved conditionally on the sequence the resume loaded. On losing, the resume returns `Failed("checkpoint for thread 't' changed before it could be resumed: another resume or end got there first")` and runs nothing. A child level's stamp goes to its capturing store, as before.
- **`design/29`'s wake driver**, through the resume.

**A store that doesn't override them** gets defaults that load with no sequence and save unconditionally, returning `true`. That is the behavior before this document, race included. A custom store keeps compiling and working; it gets the guarantee by overriding both members.

---

## Run events and telemetry

Ending is observed as a run of its own, like a resume:

| | |
|---|---|
| `RunStarted` | `Mode` = **`End`** (new), `Node` = where the thread stood (its outermost `suspended_node`), `Metadata` = the call's run metadata. |
| `CheckpointSaved` | `Status` = `Ended`, the path, `ResumeStamp` = false. Not on the idempotent call, which writes nothing. |
| `RunCompleted` | `Outcome` = `Cancelled`, `CancelReason` = **`Ended`**. A refusal is `Failed` with its message, as any refused run. |
| Run span | Named **`strata.end`** (beside `strata.run` and `strata.resume`), `strata.run.mode` = `end`. It ends with `strata.run.outcome` = `cancelled` and `strata.cancel_reason` = `ended`, and isn't an error. |
| Metrics | The run counter and duration histogram, as for every run, labelled with mode `end` and outcome `cancelled`; `strata.checkpoint.latency` for the load and the save. |

No `NodeStarted`, `Interrupted` or `Resumed`: no node runs, and nothing is resumed.

`RunEvent` is already non-exhaustive in both engines (`design/18`). `RunMode` and `CancelReason` gain a value, which a `switch` with a default arm handles. In Rust, `RunMode` and `CancelReason` aren't `#[non_exhaustive]`, so an exhaustive `match` needs a new arm (see "Migration").

---

## Wire format

### The checkpoint (`1.4.0`)

- **`status`** gains **`"ended"`**.
- **`schema_version` `"1.4.0"`** is what an ended checkpoint is written with. It is a superset of `1.3.0`: an ended level may carry a `decision`, and its predecessors may be anything. Since an ended checkpoint is ended at every level, "anything in it is ended" and "it is ended" are the same test. `"ended"` requires `1.4.0`.
- **No new field.** The reason and who ended it are metadata (`strata.end_reason`, `strata.ended_by`), under the prefix `design/18` reserved for the engine. So they need no column, and `ThreadQuery.Metadata` can filter on them in every store (`strata.end_reason = timed_out`, once `design/29` writes it).
- A reader built before `1.4.0` refuses the checkpoint (an unsupported version), which is right: it would otherwise see a status it can't parse.

### Storage

- **SQLite and Postgres:** no DDL change. `status` is text, and the head-table trigger already derives the thread state from it (`CASE WHEN resumed_at IS NOT NULL THEN 'resumed' ELSE status END`), so an ended thread's state is `ended` with no migration.
- **Redis:** a sixth state set, `{prefix}:threads:ended`. `SAVE_SCRIPT` and `INDEX_SCRIPT` loop over however many state keys they're given (`#KEYS - 2`) instead of a fixed five, and `INDEX_SCRIPT`'s state list gains `ended`. Every writer to a prefix must be current, as `design/19` already requires. A writer from before this document never writes `ended`, but it doesn't know the sixth set either.
- **Shared fixtures:**
  - `schema/fixtures/checkpoint-lifecycle/ended.json`: `frozen.json`'s wire-transfer thread, ended by `oncall` while waiting at `PrepareWireTransfer`.
  - `nested-ended.json`: `nested.json`'s thread, ended while its child waits at `Approve` inside `Work`.

  Each engine produces exactly these from a live end (timestamps aside), refuses to resume them, and ends them again idempotently. Each store round-trips them.
  - `schema/fixtures/checkpoint-log/ended.row.json`: the SQLite row for `ended.json`.
  - `checkpoint-query/cases.json` gains an ended thread and a case listing state `ended`.

### Definitions

Unchanged: ending is an operation on a thread, not part of a graph.

---

## The CLI

Read-only, as recommended:

- **`strata thread list --status ended`** works through `ThreadState::parse`, with no change but the new state.
- **`strata thread inspect`** prints `Status: ended`, `Thread state: ended`, `Ended at: Work/Approve` (the path, when nested), `Reason: requested`, and `Ended by: oncall` when set. `strata.` keys stay hidden from the caller-metadata line, as before.
- **`strata checkpoint log` / `replay`** print the ended checkpoint like any other; replay says `Ended inside: <path>` for a nested one.
- **No `strata thread end`.** The CLI reads stores; writing to a thread is the application's job, through the graph that owns it (ending needs the graph's state type for its result). A store-level "end" that needs no graph is listed under "Not built".

---

## Migration

**.NET** (source-compatible): `CheckpointStatus.Ended`, `CancelReason.Ended`, `CheckpointThreadState.Ended` and `RunMode.End` are new enum values; a `switch` with a default arm (or a `switch` expression that throws on unknown values) is unaffected, but one that is exhaustive by listing every value now misses one. `ICheckpointStore` gains two default interface members, so existing stores compile. `ThreadEnd`, `LatestCheckpoint` and `EndAsync` are new.

**Rust:** `CheckpointStatus` and `CancelReason` aren't `#[non_exhaustive]`, so an exhaustive `match` on either needs an `Ended` arm. `ThreadState` is non-exhaustive already. `RunMode` gains `End`. The `CheckpointStore` and `SyncCheckpointStore` traits gain default methods, so implementations compile unchanged.

**Stores and files:** none to migrate. Existing logs have no `ended` rows; a Redis prefix gets its sixth set the first time something is ended.

**StrataOps:** its cancel endpoint calls `EndAsync` with the operator id and incident metadata. Its approvals queue lists `interrupted` threads, which no longer include ended ones. Its ADR 0020 "the thread stays interrupted" note is retired, along with the code that refuses decisions for cancelled incidents (a resume of an ended thread is refused by the engine). Its execution events map `RunCompleted(Cancelled, Ended)` to `CANCELLED`.

---

## Tests

Mirrored in both engines, in new suites `end_thread.rs` / `EndThreadTests.cs` unless noted:

- **Ending an interrupted thread:** `Cancelled(Ended)` at the boundary, with the frozen state and the path; the store's latest checkpoint is `ended` at `1.4.0` with `next_node` null, `strata.end_reason` `requested`, `strata.ended_by`, the caller metadata, and the decision kept.
- **Ending a failed thread and a cancelled thread:** the same, and the old engine keys (`error`, `attempts`, `cancel_reason`) are not carried over.
- **A nested wait:** every level `ended`, `StoppedAt` `["Work", "Approve"]`; it matches `nested-ended.json`.
- **Idempotence:** a second end returns the same result and writes nothing (the log's length is unchanged), whoever calls it.
- **Resume of an ended thread** fails with the message above and writes nothing, both flat and nested, `ResumeAsync` and `ResumeStreaming`.
- **Refusals:** a resumed thread, a completed one, and an unknown one are refused with `Failed`, and nothing is written.
- **The conditional save losing a race:** a store that appends a resume stamp just before the end's save (a wrapper) makes the end reload and refuse; one that appends an `ended` checkpoint makes it return the idempotent result. A resume whose stamp loses returns `Failed` and runs no node.
- **The conditional save in every adapter** (store contract suites, both engines, Postgres and Redis behind their env gates): it appends when the sequence matches, refuses a stale sequence without writing, and `LoadLatestAsync` reports the sequence `GetLogAsync` does. A custom store with only `Save` / `Load` still resumes and ends (the defaults).
- **The observer:** `RunStarted(End)`, `CheckpointSaved(Ended)`, `RunCompleted(Cancelled, Ended)`; the idempotent call has no `CheckpointSaved`; the run span is `strata.end`.
- **The query state:** an ended thread lists under `Ended` and not under `Interrupted`, in every store (the shared `checkpoint-query/cases.json`), and the Redis index moves it between sets.
- **Fixtures:** `ended.json` and `nested-ended.json` are what a live end produces (both engines), they round-trip through every store, and `ended.row.json` is the SQLite row.
- **The CLI:** `thread list --status ended` and `thread inspect` on a log holding an ended thread (written by the Rust runtime, and from the fixture as the .NET runtime would write it).
- **Mutation checks:** saving unconditionally in the end path, not refusing a resume of `ended`, and not converting nested levels should each fail tests in both engines.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

### The questions as asked

Recommendations are in bold.

1. **The name, and graph or store?** **`End` on the compiled graph.** "Cancel" already means a run's token, and a cancelled thread is resumable. On the graph, the call can rehydrate the state for a typed result, fire the observer, and write in the graph's schema. A graph-free store operation is under "Not built".
2. **A new status, or `cancelled` with a marker?** **A new status, `ended`, at `1.4.0`.** A marker would leave every store's head table, the Redis index and the CLI showing a resumable state for a thread that isn't, and every reader would need to know the marker.
3. **What can be ended?** **Interrupted, failed and cancelled; ended again idempotently; resumed refused.** See the table under "What can be ended".
4. **Races: conditional save, or document the race?** **A conditional save in every store**, keyed on the log's sequence, with defaults that keep custom stores working without the guarantee. The resume's stamp uses it too, which closes the double-resume race as a side effect.
5. **Observers: reuse `RunCompleted`, or a new event?** **Reuse it**: `RunCompleted(Cancelled, Ended)`, with `RunMode.End` on `RunStarted`.
6. **The CLI: `thread end`?** **No, read-only.** `thread list --status ended` and `inspect`.

### Added by this document

7. **Reason and identity: fields or metadata?** **Metadata** (`strata.end_reason`, `strata.ended_by`). Every store and the CLI carry metadata already, and the query port can filter on it. A typed field would need a column in every SQL layout for two strings.
8. **The result type.** **`Cancelled` with `CancelReason.Ended`**, so code that handles a cancellation (StrataOps' timeline, its progress map) handles an ending, and code that needs the difference matches the reason.
9. **Carry the old engine keys?** **No** (see "What is written"). The log keeps them.
10. **`next_node` on the ended checkpoint.** **`null`**: nothing continues from it, and a resumable-looking `next_node` on a terminal checkpoint invites a hand-rolled resume.

---

## Implementation Status

Built 2026-10-06 in both engines, every store adapter and the CLI, in one change, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **The metadata key names are public constants.** .NET `ThreadEnd.ReasonKey`, `ThreadEnd.EndedByKey` and `ThreadEnd.Requested`; Rust `END_REASON_KEY`, `ENDED_BY_KEY` and `END_REQUESTED` in `checkpoint`. That way StrataOps can filter on them without retyping the strings.
- **A refusal's error type in Rust** is `ExecutionAborted`, as for every refused resume, except a missing checkpoint, which is `NodeNotFound`, the error a resume returns for one.
- **One insert statement where it was natural.** Rust's SQLite store and both Postgres stores now insert with `INSERT … SELECT … FROM (SELECT COALESCE(MAX(sequence), -1) …) latest WHERE expected IS NULL OR latest.sequence = expected`, so their `save` is the conditional save with no expected sequence. .NET's EF Core store keeps its `save` statement and adds a second one for the conditional save. The rows written are unchanged: the shared row fixtures still pass.
- **Redis's `LoadLatestAsync` / `load_latest`** reads the list's last element and its length in one `MULTI` / atomic pipeline, so the sequence and the checkpoint can't disagree.

**How it was built:**

- **.NET:**
  - `Interruption/Checkpoint.cs`: `CheckpointStatus.Ended`, `CheckpointSchema.EndedVersion`, and the wire name.
  - `ICheckpointStore.cs`: `LoadLatestAsync` and `SaveIfLatestAsync` as default interface members, and `LatestCheckpoint`.
  - `ICheckpointQuery.cs`: `CheckpointThreadState.Ended`.
  - `ThreadEnd.cs` is new, and so is `Graph/CompiledGraph.End.cs`: `EndAsync`, the refusals, and `Ended`, which converts every level.
  - `CompiledGraph.Interruption.cs`: `ResumeFromAsync` loads with the sequence, and `ResumeCheckpointAsync` refuses an ended checkpoint and stamps the outermost level through `SaveIfLatestAsync`.
  - `ObservedStore` forwards both new members, reporting `CheckpointSaved` only when the save wrote.
  - `RunTelemetry`: mode `end` and span `strata.end`.
  - Storage: each of the four adapters overrides both members. `RedisCheckpointStore.SaveIfLatestScript` is new, and the interop `Schema/Checkpoint.cs` gains `ended`.
- **Rust:**
  - `schema.rs`: `CheckpointStatus::Ended` and `ENDED_CHECKPOINT_SCHEMA_VERSION`.
  - `store.rs`: `LatestCheckpoint`, the two default trait methods on `CheckpointStore` and `SyncCheckpointStore` (`BlockingCheckpointStore` forwards them), and `ThreadState::Ended`.
  - `checkpoint.rs`: `CancelReason::Ended`, `ThreadEnd`, `end` / `end_with`, `save_checkpoint_if_latest`, and `resume_checkpoint_at`, which carries the sequence to the stamp.
  - `observability.rs`: `RunMode::End`.
  - `strata-memory`: each of the four adapters overrides both methods. `SAVE_IF_LATEST_SCRIPT` is new.
- **CLI:** `store::end_reason` and `store::ended_by`, the `Ended` verb, and the `Reason:` / `Ended by:` lines in `thread inspect`.

**Tests:**

- **New suites:** `end_thread.rs` (16) and `EndThreadTests.cs` (16), test for test.
- **The store contracts:** a conditional-save contract and a freeze-and-end contract, run against every adapter in both engines (`checkpoint_store_contract.rs`, `StoreContracts` / `StoreContractTests`), plus a defaults test for a store with only save and load (Rust's `SlowStore` and blocking `LegacyStore`).
- **Fixture checks:** `ended.row.json` in all three SQLite writers and both Postgres stores. The ended thread in the shared query cases and the Redis index fixture, with the thread counts in `storage_backends.rs`, `redis_stores.rs` and `CheckpointHeadTableTests` raised from 8 to 9.
- **CLI:** `an_ended_thread_says_where_it_stood_why_and_by_whom` (from the fixtures), and `cli_reads_a_thread_the_rust_runtime_ended` (a log the Rust runtime wrote).
- **Totals when this landed:** .NET 605, with Postgres and Redis running and nothing skipped, plus the example's 1. `rust/` 552 and `cli/` 67, with `STRATA_REQUIRE_BACKENDS=1` against live Postgres and Redis, and `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` and `dotnet format --verify-no-changes` are clean.
- **Mutation checks, in both engines:**
  - Ending with an unconditional save fails 2 tests in each.
  - Not refusing a resume of `ended` fails 2 in each.
  - Not converting nested levels fails 1 in each, the shared fixture.
  - Stamping a resume unconditionally fails 1 in each.

## Not built (deliberately out of scope)

- **Ending a resumed thread.** A thread whose resume began may still be running, or may have died. Ending it needs a lease to fence the running worker (SG-12) or a checkpoint after every node so the end can be ordered after the run's last write (SG-10).
- **A store-level end**, without the compiled graph (for an operator tool or `strata thread end`). It would write the same checkpoint with no result state and no observer; easy to add once someone needs it.
- **Ending a running thread** in the process that runs it: that is the run's cancellation token (`design/17`), which leaves a resumable `cancelled` checkpoint that this call can then end. Since 2026-10-10 (`design/33-Durable-Execution.md`) a thread whose latest checkpoint is `running`, which a graph compiled with `CheckpointPolicy.EveryNode` leaves when its process dies, is ended by this call unless a live lease holds it.
- **Refusing a fresh run on an ended thread id.** A run doesn't read the store.
- **Deleting or archiving ended threads.** Ended threads stay in the log and the listings (under `ended`); retention is the store owner's.
