# Datalium Strata: Durable Execution (a Checkpoint per Node, and Crash Recovery)

A run writes a checkpoint only when it stops: at an interrupt boundary or a wait, on a failure, on a cancellation, on an end. Between those points nothing is saved. A process that dies mid-run takes the thread with it:

- A fresh run that dies leaves no checkpoint at all.
- A resumed run that dies leaves a checkpoint stamped `resumed_at`, which says only that a resume began. `CheckpointThreadState.Resumed` documents the hole: the run "may still be running, may have completed (which writes nothing), or may have died with its process".
- The human's answer is lost with it. The stamp doesn't hold the decision or the overrides, and a second resume is refused as already resumed.

StrataOps works around this today. Its `execution` table marks a thread "running" from claim to return, and its recovery sweeper restarts stuck runs from the beginning.

This document closes the gap in the engine: **SG-10**. The SG numbers refer to the gap log of StrataOps (not public yet). It is the third of the four durability gaps StrataOps' M5 depends on (SG-13, SG-12, SG-10, SG-30), and the one M5 is gated on: *the process crashes → restart → restore the workflow → execute the rollback*, with no investigation lost and no remediation run twice. It builds on `design/32-Thread-Leases.md`: a `running` thread whose lease is live is somebody's, and one with no live lease is nobody's and can be recovered.

SG-10 lands in both engines at one commit (strict parity, StrataOps ADR 0003): the same capability, in each language's idiom.

It was written as a blueprint before implementation and is now built. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| The policy | **`CompilationOptions.Checkpointing = CheckpointPolicy.EveryNode`** (Rust `CompileOptions::with_checkpointing`). The default, `Boundaries`, is today's behaviour, byte for byte. The outermost graph's policy is the run's, at every depth. |
| What is written | **A `running` checkpoint** before the first node and after every node that commits: the node that committed (`suspended_node`), the edge taken (`next_node`), the committed state. Inside a subgraph, the parent's checkpoint nests the child's, as an interrupt does. `schema_version` **`1.7.0`**. |
| Groups and maps | **One checkpoint, at the join.** A parallel group or a map is one node; its branches and items aren't checkpointed and run again on recovery. |
| A run that finishes | **Writes a `completed` checkpoint.** A durable run never leaves its thread `running` when it returns: it is `completed`, `interrupted`, `failed` or `cancelled`. Only a process that died (or lost its lease) leaves `running`. |
| A resume | **Its claim is a `running` checkpoint**, saved only if the thread hasn't changed, holding the decision, the state with the overrides applied and the route taken. It replaces the `resumed_at` stamp under `EveryNode`, so a crash after an approval can't lose the approval. |
| Thread state | **`CheckpointThreadState.Running`** (`running`), and `Completed` is now reachable. Listed like any other state. |
| Recovering one thread | **`RecoverAsync(threadId, store, options)`** continues a `running` thread at its `next_node`, at every nesting level. Its claim is conditional too, so of two recoveries one runs. A thread a live lease holds is refused. |
| Recovering all of them | **`RecoverStaleAsync(store, options)`**, a library driver like `WakeDueAsync`: lists `running` threads, skips the ones a live lease holds, recovers the rest one at a time. |
| Who is dead | **A `running` thread with no live lease.** A deployment without leases must say how old a `running` checkpoint has to be (`StaleAfter`); with a lease on the recovery run it defaults to zero. |
| The crash contract | **At least once.** The node in flight when the process died runs again. `NodeContext.IdempotencyKey` is the same on that re-run, and `NodeContext.IsRecovery` says the node may have run before. |
| A node that crashes every time | **`RecoverOptions.MaxRecoveries`**: the driver skips a thread recovered that many times without a node committing (`strata.recoveries`). An operator ends it. The count is public in both engines (`RecoveryCount.Of` / `recoveries_of`), so an application that recovers threads itself limits its own. |
| Ending a running thread | **Allowed when no live lease holds it.** Resume, signal and wake refuse a `running` thread. |
| Run events and telemetry | **`Recovered`**, `RunStarted(Mode = Recover)`, a `strata.thread.recoveries` counter. `CheckpointSaved` already reports every save. |
| The CLI | **Read-only.** `strata thread list --status running` (and `completed`); `thread inspect` says where a running thread's run is and how often it was recovered. |
| Strata Cloud | **Works for the store port, at a price**: every checkpoint is metered. No query port, so no `RecoverStaleAsync`. Cloud must accept `1.7.0`. Recorded as follow-ups. *Since SG-36 (`43-Cloud-Leases-And-Listing.md`) Cloud accepts `1.7.0`, lists running threads and has leases.* |

---

## Usage

```csharp
// Compile once, with a checkpoint per node:
var remediation = graph.Compile(new CompilationOptions
{
    GraphId = "remediation",
    GraphVersion = "1",
    Checkpointing = CheckpointPolicy.EveryNode,
});

// Every worker runs under a lease, so a live run is never mistaken for a dead one:
var options = new RunOptions { Lease = new LeaseOptions($"worker-{Environment.ProcessId}", TimeSpan.FromSeconds(30)) };
await remediation.RunAsync(threadId, state, store, options);

// A node makes its side effect idempotent with the key, which a recovery repeats:
graph.AddNode("Execute", async (state, ctx, ct) =>
{
    await rollbacks.ExecuteAsync(state.Plan, idempotencyKey: ctx.IdempotencyKey, ct);
    return state with { Executed = true };
});

// On a timer, in every worker: recover the threads whose owner died.
var report = await remediation.RecoverStaleAsync(store, new RecoverOptions { Run = options });
foreach (var recovered in report.Recovered) { /* recovered.ThreadId, recovered.Result */ }

// Or one thread, by hand:
var result = await remediation.RecoverAsync(threadId, store, options);
```

```rust
let remediation = graph.compile_with(
    CompileOptions::new()
        .with_graph_id("remediation")
        .with_graph_version("1")
        .with_checkpointing(CheckpointPolicy::EveryNode),
)?;

let config = RunConfig::new()
    .with_lease(LeaseOptions::new(format!("worker-{}", std::process::id()), Duration::from_secs(30)));
remediation.run_with(thread_id, state, store.clone(), config.clone()).await;

graph.add_async_node("Execute", |state: Remediation, ctx: NodeContext| async move {
    rollbacks.execute(&state.plan, ctx.idempotency_key()).await;
    Remediation { executed: true, ..state }
});

let report = remediation
    .recover_stale(store.clone(), Utc::now(), RecoverOptions::new().with_config(config.clone()))
    .await?;

let result = remediation.recover_with(thread_id, store, config).await;
```

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Graph;

public enum CheckpointPolicy { Boundaries, EveryNode }

public sealed class CompilationOptions { public CheckpointPolicy Checkpointing { get; init; } /* … */ }

public sealed partial class CompiledGraph<TState>
{
    public CheckpointPolicy Checkpointing { get; }

    public Task<GraphExecutionResult<TState>> RecoverAsync(
        string threadId, ICheckpointStore store, CancellationToken cancellationToken = default);
    public Task<GraphExecutionResult<TState>> RecoverAsync(
        string threadId, ICheckpointStore store, RunOptions options, CancellationToken cancellationToken = default);

    // Recovers and streams the recovered run's chunks, as ResumeStreaming does.
    public (ChannelReader<StreamChunk> Reader, Task<GraphExecutionResult<TState>> Completion) RecoverStreaming(
        string threadId, ICheckpointStore store, CancellationToken cancellationToken = default);
    public (ChannelReader<StreamChunk> Reader, Task<GraphExecutionResult<TState>> Completion) RecoverStreaming(
        string threadId, ICheckpointStore store, RunOptions options, CancellationToken cancellationToken = default);

    public Task<RecoveryReport<TState>> RecoverStaleAsync<TStore>(
        TStore store, RecoverOptions? options = null, CancellationToken cancellationToken = default)
        where TStore : ICheckpointStore, ICheckpointQuery;
}

public sealed record RecoverOptions
{
    public TimeSpan? StaleAfter { get; init; }          // null: zero when Run.Lease is set, else refused
    public int? MaxRecoveries { get; init; }            // null: no limit
    public IReadOnlyDictionary<string, string> Metadata { get; init; }
    public bool MatchGraph { get; init; } = true;
    public int Limit { get; init; } = ThreadQuery.DefaultLimit;   // the most threads recovered in one call
    public RunOptions Run { get; init; } = new();
}

public sealed record RecoveredThread<TState>(string ThreadId, IReadOnlyList<string> At, int Recoveries, GraphExecutionResult<TState> Result);
public sealed record RecoveryReport<TState>(IReadOnlyList<RecoveredThread<TState>> Recovered, IReadOnlyList<SkippedThread> Skipped);

public static class RecoveryCount
{
    public const string MetadataKey = "strata.recoveries";
    public static int Of(Checkpoint checkpoint);   // 0 when it records none
}

public sealed class NodeContext
{
    public string IdempotencyKey { get; init; }
    public bool IsRecovery { get; init; }
    /* … */
}

namespace Datalium.Strata.Interruption;

public enum CheckpointStatus { Interrupted, Completed, Failed, Cancelled, Ended, Running }        // + Running
public enum CheckpointThreadState { Interrupted, Failed, Cancelled, Resumed, Completed, Ended, Running }   // + Running
public static class CheckpointSchema { public const string RunningVersion = "1.7.0"; /* … */ }

// Observability: RunMode.Recover, and the Recovered(At, Recoveries) run event.
```

### Rust

```rust
pub enum CheckpointPolicy { Boundaries, EveryNode }

impl CompileOptions { pub fn with_checkpointing(self, policy: CheckpointPolicy) -> Self; }

impl<S> CompiledGraph<S> {
    pub fn checkpointing(&self) -> CheckpointPolicy;

    pub async fn recover(&self, thread_id: impl Into<String>, store: Arc<dyn CheckpointStore>) -> GraphExecutionResult<S>;
    pub async fn recover_with(&self, thread_id: impl Into<String>, store: Arc<dyn CheckpointStore>, config: RunConfig)
        -> GraphExecutionResult<S>;
    pub fn recover_streaming(self: Arc<Self>, thread_id: impl Into<String>, store: Arc<dyn CheckpointStore>) -> StreamingRun<S>;
    pub fn recover_streaming_with(self: Arc<Self>, thread_id: impl Into<String>, store: Arc<dyn CheckpointStore>, config: RunConfig)
        -> StreamingRun<S>;

    pub async fn recover_stale<T>(&self, store: Arc<T>, now: DateTime<Utc>, options: RecoverOptions)
        -> Result<RecoveryReport<S>, StrataError>
    where T: CheckpointStore + CheckpointQuery + 'static;
}

pub struct RecoverOptions { /* stale_after, max_recoveries, metadata, match_graph, limit, config */ }
impl RecoverOptions {
    pub fn new() -> Self;
    pub fn with_stale_after(self, age: Duration) -> Self;
    pub fn with_max_recoveries(self, limit: u32) -> Self;
    pub fn with_metadata(self, key: impl Into<String>, value: impl Into<String>) -> Self;
    pub fn with_match_graph(self, match_graph: bool) -> Self;
    pub fn with_limit(self, limit: usize) -> Self;
    pub fn with_config(self, config: RunConfig) -> Self;
}

pub struct RecoveredThread<S> { pub thread_id: String, pub at: Vec<String>, pub recoveries: u32, pub result: GraphExecutionResult<S> }
pub struct RecoveryReport<S> { pub recovered: Vec<RecoveredThread<S>>, pub skipped: Vec<SkippedThread> }

impl NodeContext {
    pub fn idempotency_key(&self) -> &str;
    pub fn is_recovery(&self) -> bool;
    pub fn with_idempotency_key(self, key: impl Into<Arc<str>>) -> Self;   // for a hand-built context
    pub fn with_recovery(self, is_recovery: bool) -> Self;
}

pub enum CheckpointStatus { /* … */ Running }
pub enum ThreadState { /* … */ Running }
pub const RUNNING_CHECKPOINT_SCHEMA_VERSION: &str = "1.7.0";

pub const RECOVERIES_KEY: &str = "strata.recoveries";   // recovery
pub fn recoveries_of(checkpoint: &Checkpoint) -> u32;

// observability: RunMode::Recover, RunEventKind::Recovered { at, recoveries }.
```

`now` is the caller's clock in Rust, as for `wake_due`; .NET reads `RecoverOptions.Run.TimeProvider`.

---

## Semantics

### What a durable run writes

With `EveryNode`, at the outermost level:

| When | Checkpoint |
|---|---|
| A fresh run starts, after it has its lease and before its first node | `running`: `suspended_node` null, `next_node` the entry point, the initial state |
| A node commits and an edge leads on | `running`: `suspended_node` the node, `next_node` the edge's target, the committed state |
| A node commits at an interrupt boundary or a wait | `interrupted`, as before |
| The last node commits | **`completed`**: `suspended_node` the node, `next_node` null, the final state |
| A node fails for good, the run is cancelled, the hop limit trips | `failed` / `cancelled`, as before |
| The run can't go on for a reason that wrote nothing (an edge with no matching condition, a node it can't run, an exception out of a handler) | **`failed`**, at the node that was to run, with the state before it and `error_kind` `fatal` |

So a 15-node run writes 16 rows: 15 `running` and one `completed`. With `Boundaries` it writes none.

**A thread is never left `running` by a process that is alive.** Every way a run returns ends in another status. That is what makes `running` mean something: the run is still going, or its process died.

The last row of the table is new behaviour, and only under `EveryNode`. Today a run that returns `Failed` for a routing gap, or throws out of a plain node, writes nothing. Under `EveryNode` that would leave the thread `running` with nobody running it, and the recovery driver would re-run it into the same wall on every sweep. So the run writes a `failed` checkpoint first: built from its last `running` one, suspended at the node that was about to run, resumable like any failed thread. The result (or the exception) is what it was.

A fallback isn't a commit, so nothing is written between a failed node and its fallback.

### Subgraphs, groups and maps

A subgraph's child writes nothing to the store, as always: its checkpoints nest in its parent's. When a child node commits and the child goes on, the run saves **one** checkpoint, built from the outside in:

- the parent's level: `running`, suspended at the subgraph node with `next_node` the node itself (a recovery re-enters it), holding the state the node started with;
- nested in it, the child's: `running`, with the node that committed, the edge taken and the child's committed state.

That is the shape an interrupt inside a subgraph has (`design/16-Nested-Interrupts.md`), to any depth, with `running` at every level. A child's last node writes nothing by itself: the child completes, the subgraph node commits, and the parent's checkpoint follows.

A parallel group and a map are one node each. Nothing is written while their branches or items run, and one checkpoint follows the reducer. A recovery of a thread that died inside one runs every branch, or every item, again.

The outermost graph's policy decides for the whole run. A child compiled with `EveryNode` inside a `Boundaries` parent writes nothing (it has no store), and a `Boundaries` child inside an `EveryNode` parent is checkpointed per node like the rest.

### A resume under `EveryNode`

A resume (so a signal and a wake) checks everything a resume checks, acquires its lease, and then, instead of stamping the old checkpoint `resumed_at`, **appends a `running` checkpoint, only if the thread's latest sequence is still the one it loaded**. That checkpoint is the resume, in one atomic step:

- every level is `running`;
- each level's state has that level's overrides applied;
- the innermost level has the decision this resume recorded, and `next_node` is the route it took (the reject route, the timeout route, or the edge out of the boundary);
- the `wait` is gone.

Of two resumes that loaded the same checkpoint one saves and the other is refused, exactly as with the stamp. A process that dies right after an approval leaves a thread that recovery continues with the approval in it. `resumed_at` is never written by an `EveryNode` graph, and `CheckpointThreadState.Resumed` is never seen for one.

A `running` thread refuses a resume, a signal and a wake: `thread 't' is running: only a waiting, failed or cancelled thread can be resumed`. The decision workers report that as `AlreadyHandled`.

### Recovering a thread

`RecoverAsync` loads the thread's latest checkpoint and continues it. In order:

1. **Refusals, with nothing written.** No checkpoint; an unsupported version; a checkpoint that isn't `running` (`thread 't' is not running: its latest checkpoint is interrupted`); a graph compiled without `EveryNode`; another graph's thread, or another version's without a migration (`design/31-Graph-Identity.md`); a state or hop count that doesn't read; a nested level under a node that isn't a subgraph.
2. **The lease.** A run with `Lease` acquires it, and a held thread is refused as for any run. A run without one reads the thread's lease (`GetLeaseAsync`) and refuses a live one the same way: somebody is running the thread.
3. **The claim**: the loaded checkpoint again, `created_at` now, the run's metadata laid over the stored, and `strata.recoveries` one higher, saved only if the thread's latest sequence is still the one loaded. If it isn't, the run that was thought dead saved something, or another recovery got there first: `thread 't' changed before it could be recovered`, nothing runs.
4. **The run**, from `next_node` with the stored state, at every level: the subgraph nodes on the way down are re-entered, and the innermost level starts the node that was in flight. Each level keeps its decision and its hop count.

From there it is an ordinary durable run: it checkpoints every node, and ends `completed`, `interrupted`, `failed` or `cancelled`.

### The crash contract

**At least once.** The checkpoint before a node says the node is next. If the process dies while the node runs, or after it finished and before its checkpoint was saved, a recovery runs the node again from the state it started with. Whatever it did outside the state happens twice unless the node prevents it.

Two things on `NodeContext` are for that:

- **`IdempotencyKey`** names one execution of one node in one thread: the thread id, the node's path, and the node's hop number at every level, for example `inc-42/Remediate/Execute#3.2`. A retry of the node has the same key. So does the node's re-run in a recovery, because the hop count is in every checkpoint. The same node reached again later in the thread (a loop) has another.
- **`IsRecovery`** is true for the node a recovery starts with at each level, on every attempt, and for its fallback if it fails again; and for every node of every branch or item of a group or map a recovery re-runs. It is false again once a node commits at that level. It means "this may have run before", not "this ran before".

A node passes the key to whatever it changes: StrataOps' rollback write carries it in its authorization, so a recovered `Execute` never rolls back twice. A node with several effects derives one key per effect (`$"{ctx.IdempotencyKey}:notify"`).

Under `Boundaries` nothing is recovered, and hop counts aren't stored unless the level has a hop limit, so the key ends with the run id (`…#3@018f…`): the same across a node's retries, never repeated by a later resume. A fresh run on a thread id that was used before starts counting hops again under either policy: don't reuse thread ids and rely on the key.

Hop counts (`strata.hops`, `design/24-Loop-Circuit-Breaker.md`) are written on every checkpoint level of an `EveryNode` run, limit or no limit.

### Who is dead: `RecoverStaleAsync`

A library call, on the application's cadence. It:

1. **Lists** `running` threads (`ThreadQuery.States`), this graph's by identity as `WakeDueAsync` does, filtered by `RecoverOptions.Metadata`, and older than `StaleAfter` when that isn't zero (`ThreadQuery.UpdatedBefore`).
2. **Decides each again** from its latest checkpoint: still `running`; still stale; this graph's; not recovered `MaxRecoveries` times already; not held by a live lease. Otherwise it is skipped with the reason, and nothing is written.
3. **Recovers** it, as `RecoverAsync` does, with `RecoverOptions.Run`. A recovery whose claim lost (`another run or recovery got there first`) or that found the thread held is a skip.

It reads the whole listing, and stops once `Limit` threads have been recovered. An exception from a recovered run propagates, as from `WakeDueAsync`.

**Staleness.** A lease is the only reliable sign of life:

- **With leases** (every worker runs with `RunOptions.Lease`): a run holds its lease from before its first save until after its last. A `running` thread with no live lease has no run. `StaleAfter` defaults to zero when `RecoverOptions.Run` has a lease, and a crashed worker's threads are recovered as soon as its lease expires: one ttl.
- **Without leases**: the only sign is the age of the latest checkpoint, which a long node ages too. `StaleAfter` must be set, to more than the longest a node can take, and a node slower than that will be run twice. `RecoverStaleAsync` with neither a lease nor `StaleAfter` is refused.

**A node that kills its process** would be recovered forever. The claim counts recoveries since the last commit (`strata.recoveries`); a node that commits writes a checkpoint without the key, so the count is of consecutive recoveries that got nowhere. With `MaxRecoveries = 3` the driver skips a thread at 3 (`it was recovered 3 times without a node completing`), and it stays `running` for an operator to look at and end. `RecoverAsync` / `recover` (and the streaming forms) always recover, so an application that recovers each thread itself, not through the driver, reads the count on the thread's latest checkpoint (`RecoveryCount.Of` / `recoveries_of`, both public) and stops at its own limit.

### Ending a running thread

`EndAsync` ends a `running` thread unless a live lease holds it: with `Lease` it acquires (and is refused a held thread); without, it reads the lease and refuses a live one. Every level is ended where it stood, as for any other thread.

Without leases nothing stops a run that is alive after all from appending its next checkpoint after the `ended` one. That is the same rule as everywhere: every worker on a thread uses leases, or none of them is protected.

### What a lease now fences

A completion. `design/32` left it open: a completed run wrote nothing, so a stale owner that finished its last node returned `Completed`. Under `EveryNode` the `completed` checkpoint is saved under the lease, and a stale owner gets `Cancelled(LeaseLost)` like anywhere else.

---

## Run events and telemetry

| | |
|---|---|
| **`RunStarted(Mode = Recover)`** | A recovery's first event; `Node` is the outermost `next_node`. |
| **`Recovered { At, Recoveries }`** | After the claim is saved: `At` is where the thread continues, one node per level from the outermost in; `Recoveries` the count the claim recorded. Not on a refusal. |
| `CheckpointSaved` | One per save, as before: `Running` and `Completed` are new values of `Status`. A resume's claim has `ResumeStamp` false. |
| `Resumed` | Unchanged, after the claim. |
| Run span | Named `strata.recover`, with `strata.run.mode` `recover`; `strata.recoveries` once claimed. |
| Metrics | **`strata.thread.recoveries`**, one per claim saved. |

`RunEvent` is non-exhaustive in both engines.

---

## Wire format

### The checkpoint: `1.7.0`

`status` gains **`running`**. A checkpoint with `running` at any level is written as `schema_version` `1.7.0`, a superset of `1.6.0`. A reader that predates it can't parse the status, and must refuse the checkpoint.

A `running` level:

| Field | Value |
|---|---|
| `suspended_node` | The node that last committed at this level; null before the first. On a level that nests a child, the subgraph node. On a claim, what the checkpoint it claims had. |
| `next_node` | Where the level continues: never null. On a level that nests a child, the subgraph node again. |
| `frozen_state` | The state `next_node` starts with. |
| `nested` | The child's `running` level, when the thread is inside a subgraph. |
| `decision`, `graph` | As on any level. |
| `wait`, `resumed_at` | Never. |
| `metadata` | The run's, `strata.hops`, and on a recovery's claim `strata.recoveries` (the outermost level only). |

Every level of a `running` checkpoint is `running`.

`completed` was in the schema from `1.0.0` and nothing wrote it. It is now written, with the version its content needs (`1.0.0` for a plain one). `next_node` is null, and it never nests.

`strata.recoveries` is a decimal count, like `strata.hops`. Both are engine keys under the reserved `strata.` prefix.

### Stores

- **SQL**: nothing changes in the layout. `checkpoints.status` and `checkpoint_threads.state` are text, and the head-table trigger already copies the status as the state (`running`, `completed`). No migration.
- **Redis**: a seventh index set, `{prefix}:threads:running`, beside the six. The save and index scripts loop over the state sets, so they gain a key; a writer from before this document doesn't know the set, and as before every writer to a prefix must be current (`rebuild_index` repairs).
- **In-memory**: the state is derived from the checkpoint.

No new query field: the driver lists by state, graph, metadata and `UpdatedBefore`, and reads each candidate's lease.

### Shared fixtures

- `checkpoint-lifecycle/running.json`: a thread past its first node.
- `checkpoint-lifecycle/nested-running.json`: a thread inside a subgraph, `running` at both levels.
- `checkpoint-lifecycle/claimed.json`: the `running` claim an approval with an override makes of an interrupted thread, with its decision.
- `checkpoint-lifecycle/completed.json`: a finished thread.
- `checkpoint-log/running.row.json`: the row every SQL store writes for `running.json`.
- `checkpoint-query/cases.json`: a running thread that was recovered once (`running-initech`), listed by state, by age and by `strata.recoveries`; it already had a completed one.
- `checkpoint-log/redis-index.json`: the seventh state set.

Each engine writes exactly the four lifecycle fixtures (timestamps aside) and recovers the three running ones, so a thread one runtime leaves is recovered by the other.

### Definitions

Unchanged. The policy isn't exported, like the hop limit: it is how a graph is run, not what it is.

---

## The CLI

Read-only:

- **`strata thread list --status running`** and **`--status completed`**.
- **`strata thread inspect`** on a running thread prints `Running: after '<node>', next '<node>'`, with the subgraph nodes it is inside first (`Running: in 'Work', after 'X', next 'Y'`) and `next '<node>'` alone before its first node; `Recoveries: n` when it was recovered; and the `Lease:` line `design/32` added. `--json` carries the checkpoint as it is.
- `strata checkpoint log` and `replay` show `running` and `completed` rows like any other.

No `strata thread recover`: the CLI has no graph to run.

---

## Migration

**.NET** (source-compatible): `CheckpointPolicy`, `RecoverOptions`, `RecoveredThread`, `RecoveryReport` and the `Recovered` event are new. `CompilationOptions.Checkpointing`, `NodeContext.IdempotencyKey` and `IsRecovery` are `init` properties. `CheckpointStatus.Running`, `CheckpointThreadState.Running` and `RunMode.Recover` are new enum values: a `switch` with a default arm is unaffected.

**Rust:** `CheckpointStatus` gains `Running`, and an exhaustive `match` on it needs an arm; so does one on `RunMode` (`Recover`). `ThreadState` (which also gains `Running`, making `ThreadState::ALL` seven long) and `RunEventKind` are non-exhaustive.

**Behaviour:** nothing changes for a graph compiled without the option. With it:

- a run writes a row per node, and its state is serialized once per node;
- a run that completes leaves a `completed` thread, where it left the last checkpoint it happened to write;
- a resume appends a `running` checkpoint and doesn't stamp `resumed_at`;
- a run that returns `Failed` without a failed node, or throws, leaves a `failed` checkpoint.

**Custom stores** need nothing new. A store that validates statuses must accept `running`.

**Stores:** Redis indexes gain the `running` set; a deployment upgrades every writer to a prefix together, as for `ended` and the waking set.

**Strata Cloud:** it must accept `schema_version` `1.7.0` (another catch-up in Strata Cloud's API, as for `1.6.0`). The Cloud store has the store port, so a durable run and `RecoverAsync` work against it, but it has no query port and no leases: `RecoverStaleAsync` can't run against it, and a recovery there can't tell a live run from a dead one. Every checkpoint is metered, so `EveryNode` on Cloud is a pricing question, to flag and not to decide here.

**StrataOps:** its graphs compile with `EveryNode`, its Worker's `RecoverySweeper` calls `RecoverStaleAsync` with the Worker's lease, the rollback's write authorization carries `ctx.IdempotencyKey`, and the `execution` table's "running" mark goes away.

---

## Cost

`EveryNode` trades a write per node for the ability to continue. Measured by `DurableExecutionTests` / `durable_execution.rs` on a 15-node graph:

- **Rows per run:** 16 against 0.
- **Latency per node:** one state serialization and one store append. Against the in-memory stores, with a three-field state and release builds, a node costs about 4.1 µs with `Boundaries` and 5.4 µs with `EveryNode` in .NET, and 2.1 µs and 3.8 µs in Rust: the engine's own share is between one and two microseconds. Against a real store the append is the cost: one round trip per node.
- **Measured properly since** (`design/37-Benchmarks.md`, `benchmarks/RESULTS.md`): the Rust figures above hold; the .NET ones are about ten times too high (it is 0.4 µs a node with `Boundaries` and 1.2 µs with `EveryNode`), most likely because they were taken before the JIT had finished. Against real stores a ten-node run takes 3 ms on Redis, 15 to 20 ms on Postgres and 26 ms on SQLite.

Each row holds the whole state. A thread with a large state and many nodes writes it many times; deltas and blob offload are SG-15, and retention is SG-30's `PurgeAsync`.

---

## Tests

Mirrored in both engines, in new suites `durable_execution.rs` / `DurableExecutionTests.cs` unless noted:

- **What is written:** a checkpoint per node with the right nodes and states, then `completed`; nothing between nodes under `Boundaries`; one checkpoint for a parallel group and for a map; the nested chain inside a subgraph, two levels down; an interrupted checkpoint at a boundary and no `running` one after it; `strata.hops` at every level.
- **Never left running:** a failed node, a cancellation and a hop limit leave their own checkpoint; a routing gap and an exception (in Rust, a panic) out of a node leave `failed`, and the routing gap's is resumed.
- **A resume's claim:** an approval with overrides appends one `running` checkpoint and no stamp; the decision, the state and the reject route are in it; of two resumes one runs; a resume of a `running` thread is refused.
- **A crash recovered:** a store that goes dark mid-node (nothing after it is written, the lease isn't released), then `RecoverAsync` in a "new process": the node in flight runs again once with the same idempotency key and `IsRecovery`, the nodes before it don't, the next node has another key and no `IsRecovery`, and the thread completes.
- **A crash after an approval:** the claim is all that was written; recovery continues with the decision and the overrides.
- **A crash inside a subgraph:** recovery re-enters the child at its node in flight, and the child's earlier nodes don't run again; a crash in a child's first node re-enters the child from its input.
- **A crash inside a group and a map:** every branch and item runs again with `IsRecovery`.
- **A crash while waiting at a boundary:** nothing to recover (`interrupted`), and a resume in a new process completes.
- **Refusals:** not running, a graph without `EveryNode`, another graph's thread, a live lease (with and without a lease of its own), nothing written by any.
- **Two recoverers:** one recovers, the other is refused, and the node runs once.
- **`RecoverStaleAsync`:** skips a thread with a live lease and recovers it once the lease expired (fake clock); `StaleAfter` without leases; the metadata and graph filters; `MaxRecoveries`; `Limit` counts recoveries; refused with neither a lease nor `StaleAfter`.
- **Ending:** a running thread with no lease is ended; one a live lease holds is refused.
- **Leases:** a stale owner's `completed` checkpoint is fenced.
- **Events and telemetry:** `RunStarted(Recover)`, `Recovered`, a `CheckpointSaved` per node, the counter.
- **Stores:** `running` and `completed` round-trip, list by state and count in every adapter (`StoreContracts`, `checkpoint_store_contract.rs`; Postgres and Redis behind their env gates); a durable run and a recovery against every adapter; the Redis `running` set.
- **Fixtures:** each engine writes what `running.json`, `nested-running.json` and `resume-claim.json` hold, and recovers from them; the raw row; the query cases.
- **The decision workers:** a decision for a `running` thread is `AlreadyHandled`.
- **The CLI:** `thread list --status running`, `inspect` on a running and a recovered thread, against SQLite, Postgres and Redis.
- **Cost:** the row count for a 15-node graph under each policy, and the time per node.
- **Mutation checks**, both engines: not writing the checkpoint after a node; stamping instead of claiming on resume; an unconditional recovery claim; ignoring a live lease in recovery; not writing the `completed` checkpoint; a decision worker that doesn't know the `running` refusal.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

### The questions as asked (the handoff brief)

Recommendations are in bold.

1. **The policy.** **`CompilationOptions.Checkpointing`, `Boundaries` (default) or `EveryNode`.** No `EveryNNodes`.
2. **What's written.** **A `running` checkpoint after each commit, at `1.7.0`; groups and maps at their join.**
3. **Thread state.** **`Running`; not resumable; endable unless a live lease holds it.**
4. **Recovery.** **`RecoverAsync` and the `RecoverStaleAsync` driver, skipping live leases.**
5. **The crash contract.** **At least once, with `IdempotencyKey` and `IsRecovery`.**
6. **Cost.** **Measured, and said plainly**; deltas are SG-15, Cloud metering is flagged.
7. **Run events.** **`Recovered`.** The brief's `{ FromNode, Hop }` became `{ At, Recoveries }`: `At` names the node at every level, and the hop is in the key.
8. **The CLI.** **`thread list --status running`** (the existing flag is `--status`), and `inspect`.

### Added by this document

9. **A checkpoint before the first node.** **Yes.** Without it a fresh run that dies in its first node leaves nothing, and the thread is lost.
10. **A `completed` checkpoint.** **Yes, under `EveryNode`.** Otherwise a finished thread's last checkpoint is `running` and it is recovered forever. It is also what finally answers "did it finish?".
11. **A resume's claim replaces the stamp.** **Yes.** A stamp and then a checkpoint are two writes, and a crash between them loses the approval with no way to give it again. One conditional append can't be half done.
12. **A `failed` checkpoint for a run that gives up without one, or throws.** **Yes, under `EveryNode`.** A live process must not leave `running` behind. The alternative is a thread the driver retries into the same exception on every sweep.
13. **Whose policy applies inside a subgraph.** **The outermost graph's.** The thread is its, and only it has the store.
14. **The idempotency key's shape.** **Thread, node path, hop numbers**; the run id is appended when hops aren't stored. It needs nothing new on the wire but `strata.hops` on every level.
15. **What `IsRecovery` covers.** **The node in flight at each level, until a commit; everything inside a re-run group or map.** Narrower would miss an effect that may have happened; wider would cry wolf on every node of a recovered run.
16. **Staleness without a new query.** **List `running`, read each lease.** One round trip per running thread per sweep, which is nothing at StrataOps' scale. A store-side "not held" filter is "Not built".
17. **`StaleAfter` has no default without a lease.** **Refuse the call.** Any default would recover live threads in some deployment; the caller has to know its longest node.
18. **A recovery limit.** **`MaxRecoveries` on the driver, counting recoveries without a commit.** Cheap, and the alternative is an endless crash loop.
19. **Does recovery need a graph compiled with `EveryNode`?** **Yes.** A `Boundaries` graph would recover the thread, then leave it `running` again when it completed.
20. **`RecoverStreaming`.** **Yes, for one thread**, so a graph with streaming nodes can be recovered. The driver runs plain, like `WakeDueAsync`.
21. **A policy per run** (`RunOptions.Checkpointing`). **No.** A thread's checkpoints should not depend on which caller ran it.

---

## Implementation Status

Built 2026-10-10 in both engines, every store adapter, both decision workers and the CLI, in one change, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **A store that goes dark ends a run in each engine's habit.** .NET's store exception propagates out of the run; Rust's store error comes back as `Failed`. Either way the run then tries its `failed` checkpoint, that save fails too, and the thread stays `running` for a recovery. The crash tests in both suites are a store wrapper that stops answering (`Process`), which is what a dead process looks like from the store.
- **A subgraph's child stops at the first save that fails.** A child hands each `running` checkpoint up through its level (`LevelScope.Progress` / `progress`), and the outermost level saves it. In Rust the hand-off returns the store's error, so the child ends there as the parent would; in .NET the exception does that by itself.
- **The `failed` checkpoint of a run that gave up** is made from the last `running` checkpoint the run itself saved (`RunScope.LastRunning` / `last_running`), at every level, with the result's message (or the exception's, or the panic's) as `error`, `error_kind` `fatal`, and no `attempts`. A run that lost its lease writes nothing, as always.
- **A plain `RecoverAsync` of a graph with a streaming node** fails at that node like any plain run, and so leaves the thread `failed`, not `running`. Recover such a graph with `RecoverStreaming`. The driver runs plain, as `WakeDueAsync` does.
- **Ending a `running` thread records the node that was to run** as each level's `suspended_node`, as a cancelled thread's checkpoint does, so `Cancelled.Node`, `StoppedAt` and `inspect` say where the run was, not what it last finished.
- **The driver reads a thread's lease before it starts a run for it,** so a sweep over live threads costs one read each and no events or spans. The recovery itself checks again when it claims: with a lease of its own by acquiring, without one by reading.
- **The driver pages by the default page size (100)** and `Limit` counts recoveries only. A recovered thread moves to the front of the listing, behind the cursor, so it isn't listed twice.
- **`MaxRecoveries` is the driver's.** `RecoverAsync` always recovers: it is what an operator uses on a thread the driver gave up on. An application that recovers each thread itself (one of its own runs, with its own options) limits its own recoveries by the count: **since SG-39 (2026-10-23) .NET's `RecoveryCount` (`MetadataKey`, `Of`) is public**, as Rust's `RECOVERIES_KEY` and `recoveries_of` always were (it was `internal`, so an application had to read `strata.recoveries` by its literal name). Making it and stopping at it are still the engine's alone (`Counted` and `continues_at` stay internal). A limit on `RecoverAsync` itself wasn't added: the count is enough, and the sweep's skip is the driver's own.
- **A lease that can't be read** (a store error other than "no leases") skips the thread in Rust (`its lease can't be read: …`) and propagates in .NET, as a load error does in each.
- **`strata.hops` is on every checkpoint a durable run writes,** the `interrupted`, `failed`, `cancelled` and `completed` ones included, at every level, not only on the `running` ones. A thread written without it (by a graph compiled before, or with `Boundaries`) counts from zero.
- **A branch's key** is its group's hop under its own path (`t/Group/b1#1`). In Rust the group's context makes it (`NodeContext::for_branch`); in .NET the group's level does.
- **Rust keeps a copy of the last `running` checkpoint** it saved (a clone per save), where .NET keeps a reference: the price of taking the checkpoint by value in the store port.
- **`Recovered` and the claim's `CheckpointSaved` come in that order after `RunStarted`:** `RunStarted`, `CheckpointSaved(Running)`, `Recovered`, then the nodes.

**How it was built:**

- **.NET:**
  - `Graph/CompilationOptions.cs`: `Checkpointing`, `CheckpointPolicy`. `CompiledGraph.Checkpointing`.
  - `Interruption/Checkpoint.cs`: `CheckpointStatus.Running`, `CheckpointSchema.RunningVersion`. `ICheckpointQuery.cs`: `CheckpointThreadState.Running`.
  - `Graph/Execution.cs`: `RunScope.Durable` / `LastRunning`; `LevelScope.Hop`, `HopChain`, `Recovering`, `Progress`, `ChildProgress`, `Checkpoints`, `KeyFor`; `NodeEntry.Claim`; `RootScope.Start` takes the policy.
  - `Graph/CompiledGraph.Interruption.cs`: the start checkpoint (`StartAsync`), `RunningAt`, the save after each commit and the `completed` one (`ExecuteFromAsync`), a subgraph's `ChildProgress` (`RunAttemptsAsync`), the claim in `ResumeCheckpointAsync` (which also takes a recovery), `FailRunningAsync` / `Failing` (`ObserveAsync`).
  - `Graph/CompiledGraph.Recovery.cs` is new: `RecoverAsync`, `RecoverStreaming`, `Claim`, `RecoverStaleAsync`. `Graph/Recovery.cs` is new: `RecoverOptions`, `RecoveredThread`, `RecoveryReport`, `RecoveryCount`.
  - `Graph/CompiledGraph.Leases.cs`: `PersistAsync` notes what was saved; `HeldByAnotherAsync`. `CompiledGraph.End.cs`: `Standing`, and the lease check for a running thread.
  - `NodeContext.IdempotencyKey` / `IsRecovery`; `RunMode.Recover`, the `Recovered` event, `RunTelemetry.Recovered`, `StrataTelemetry.Recoveries`.
  - Storage: the `running` status name, and `'running'` in the Redis index script. `Datalium.Strata.Cloud`: the worker's `SomeoneElseActed` knows the `running` refusal.
- **Rust:**
  - `graph.rs`: `CheckpointPolicy`, `CompileOptions::with_checkpointing`, `CompiledGraph::checkpointing`. `schema.rs`: `CheckpointStatus::Running`, `RUNNING_CHECKPOINT_SCHEMA_VERSION`. `store.rs`: `ThreadState::Running`.
  - `context.rs`: `RunScope::durable` / `saved` / `last_running`; `LevelScope`'s `hop`, `hop_chain`, `recovering`, `progress`, `child_progress`, `checkpoints`, `at_hop`; `NodeContext::idempotency_key` / `is_recovery`; `RootScope::start` takes `durable`.
  - `checkpoint.rs`: `running_checkpoint`, `fail_running` / `failing`, `standing`, `held_by_another`, `claim`; `start`, `execute_from`, `run_attempts`, `resume_checkpoint_at` and `observed` (which now takes the store) as in .NET; `recover`, `recover_with`, `recover_streaming`, `recover_streaming_with`, `recover_stale`.
  - `recovery.rs` is new: `RecoverOptions`, `RecoveredThread`, `RecoveryReport`, `RECOVERIES_KEY`, `recoveries_of`. `node.rs`: `LevelClaimer`. `observability.rs`: `RunMode::Recover`, `RunEventKind::Recovered`, `metric_names::THREAD_RECOVERIES`.
  - `strata-memory`: the `running` status name, the index script. `strata-cloud`: the worker as in .NET.
- **CLI:** `store::running_text` and `recoveries`, the `Running:` and `Recoveries:` lines of `thread inspect`; `--status running` needed nothing (it parses `ThreadState`).
- **Schema:** `checkpoint.schema.json` (`running`, `1.7.0`, and the pairings); the fixtures above; `schema/README.md`.

**Tests:**

- **New suites:** `durable_execution.rs` in `strata-core` (32) and `DurableExecutionTests.cs` (32), test for test (SG-39 later added `an_application_limits_its_own_recoveries_by_the_public_count` to both, which reads the count through the public API only), with the recovery test in `telemetry.rs`. The Rust suite runs on `tests/lease_store`.
- **Stores:** `run_durable_run` / `StoreContracts.DurableRunContractAsync` against all four adapters in both engines (a durable run, a dead thread a lease protects, the driver recovering it); the `running` row in SQLite and Postgres; the shared query cases, which every adapter and the CLI answer, with the running thread; the Redis index with its seventh set.
- **Workers:** a decision for a thread that is `running` is `AlreadyHandled` (`worker.rs`, `DecisionWorkerTests.cs`).
- **CLI:** `thread list --status running` and `completed`, and `inspect` on the four fixtures and on the seeded running thread against SQLite, Postgres and Redis.
- **Totals when this landed:** .NET 785, with Postgres and Redis running, and 2 skipped (the opt-in Cloud live tests), plus the example's 1. `rust/` 708 and `cli/` 77, with `STRATA_REQUIRE_BACKENDS=1` against live Postgres and Redis, and `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` and `dotnet format --verify-no-changes` are clean.
- **Mutation checks, in both engines** (each broken, the durable suites, the store contract and the worker suite run, then restored). The counts are the same in .NET and in Rust:
  - Not writing the checkpoint after a node fails 26 tests.
  - A resume that stamps instead of claiming fails 4.
  - An unconditional recovery claim fails 1.
  - A recovery that ignores a live lease fails 1.
  - Not writing the `completed` checkpoint fails 17.
  - A worker that doesn't know the `running` refusal fails 1.

## Not built (deliberately out of scope)

- **Per-branch and per-item progress** inside a parallel group or a map: they checkpoint at the join and re-run whole.
- **`EveryNNodes`**, or a policy per node or per run.
- **Checkpoint deltas and blob offload** (SG-15): every row holds the whole state.
- **Exactly-once effects.** The engine gives a stable key; the node uses it.
- **A key per tool call.** `ToolCallContext` doesn't carry the node's key: a node making several calls derives one each.
- **A store-side filter for threads no lease holds**, and listing oldest first: the driver reads each running thread's lease.
- **Failing a thread at `MaxRecoveries`.** The driver skips it; an operator ends it.
- **Recovering a `resumed` thread** written without `EveryNode`: its decision was never stored. Those threads are as answerable as they were.
- **A sequence-conditional save per node**, which would stop a lease-less run from writing after an end. Leases do that.
- ~~**A hosted recovery service.**~~ Built 2026-10-10 (`35-Hosting-Integration.md`): `AddRecoveryDriver<TState>()` in .NET and `Host::recover` in Rust call `RecoverStaleAsync` on a cadence. A scheduler (SG-25) and distributed execution (SG-26) are still not built.
- **`strata thread recover`**: the CLI has no graph.
- ~~**Running-thread listing and leases on Strata Cloud**~~ (built 2026-10-20, `43-Cloud-Leases-And-Listing.md`, SG-36), and a decision on metering per-node checkpoints there (still Cloud's, its Q19).
