# Datalium Strata: Thread Leases with Fencing

Two processes can work on one thread. Since `design/28` only one of two resumes wins the `resumed_at` stamp, but that is all the engine guarantees: nothing says who is running a thread now, a fresh run on a thread id another worker is running isn't noticed at all, and a worker that stalled (a long GC pause, a partitioned node, a Kafka rebalance that handed its partition to someone else) comes back and writes its checkpoints over whatever the worker that replaced it has written since.

StrataOps works around this today. A run claims its thread in its `execution` table with an owner and an expiring lease, renews it, and releases it. That gives it one owner at a time, but nothing fences the saves a stale owner makes: the claim and the checkpoint log are two tables with no shared rule.

This document closes the gap in the engine: **SG-12**. The SG numbers refer to the gap log of StrataOps (not public yet). It is the second of the four durability gaps StrataOps' M5 depends on (SG-13, SG-12, SG-10, SG-30). SG-10's crash recovery is built on it: a `running` thread whose lease is live is somebody's, and one whose lease has expired is nobody's and can be recovered.

SG-12 lands in both engines at one commit (strict parity, StrataOps ADR 0003): the same capability, in each language's idiom.

It was written as a blueprint before implementation and is now built. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Where leases live | **On the checkpoint store itself**: five new members with defaults, so a custom store still compiles. A separate lease store couldn't fence a save atomically. |
| The lease | **`ThreadLease(ThreadId, Owner, Token, ExpiresAt)`**. The **token** is a counter per thread, 1, 2, 3, …: every acquisition gets the next one, and it never goes back. The owner is a label for people. |
| Acquiring | **`TryAcquireLeaseAsync(threadId, owner, ttl)`** returns the new lease, or the live lease that holds the thread. A thread is held while its lease hasn't expired and hasn't been released, **whoever asks**, the same owner name included. |
| Renewing and releasing | **`RenewLeaseAsync(lease, ttl)`** extends the lease while its token is still the thread's latest, and says so; **`ReleaseLeaseAsync(lease)`** gives it up. Both do nothing for a lease that was taken over. |
| Fencing | **`SaveUnderLeaseAsync(checkpoint, lease, expectedSequence)`** saves only if the lease's token is still the thread's latest, in the same atomic step as the save. Otherwise nothing is written and the save reports the lease lost (.NET `LeaseLostException`, Rust `LeasedSave::LeaseLost`). **Only the token decides**: expiry decides who may acquire, never whether a save is accepted. |
| Saves without a lease | **Unchanged.** `SaveAsync` and `SaveIfLatestAsync` don't look at leases. Leases are opt-in, and the guarantee holds among the writers that use them. |
| The engine | **`RunOptions.Lease = new LeaseOptions(owner, ttl)`** (Rust `RunConfig::with_lease`). Run, resume, signal, wake and end acquire before their first save, renew every `ttl / 3` on the run's clock, save under the lease, and release when they return. |
| A held thread | **Refused, with nothing written**: `Failed("thread 't' is held by 'worker-2' until 2026-10-09T12:00:05Z")`. The wake driver skips it; the decision worker leaves the decision for its next round. |
| A lost lease | **The run stops at once** and returns `Cancelled` with the new **`CancelReason.LeaseLost`**. It writes nothing more: not even a `cancelled` checkpoint, which would be the stale write fencing exists to stop. |
| The clock | **The store's, where it has one**: Postgres `clock_timestamp()`, Redis's own expiry. The in-memory and SQLite stores take one (.NET `TimeProvider`, Rust `with_clock`). The engine's renewal timer runs on the run's clock. |
| Storage | **No checkpoint change**, so no `schema_version`. SQL: a `checkpoint_leases` table. Redis: a lease hash with a `PX` expiry and a token counter, and one new save script. Existing scripts and tables are untouched. |
| Run events and telemetry | **`LeaseAcquired`** and **`LeaseLost`** events; `strata.lease.owner` / `strata.lease.token` on the run span; `strata.lease.acquisitions` and `strata.lease.losses` counters. |
| The CLI | **Read-only.** `strata thread inspect` prints `Lease:`, and `lease` in `--json`. |
| Strata Cloud | **No leases**: Cloud has no lease route. A run with `Lease` against the Cloud store is refused. Recorded as a Cloud follow-up. *Built 2026-10-20 by SG-36 (`43-Cloud-Leases-And-Listing.md`).* |

---

## Usage

```csharp
// Every worker runs with a lease. The owner names this process, not the service:
var options = new RunOptions
{
    Lease = new LeaseOptions(Owner: $"worker-{Environment.MachineName}-{Environment.ProcessId}", Ttl: TimeSpan.FromSeconds(30)),
    Metadata = { ["incident"] = incidentId },
};

var result = await investigation.ResumeAsync(threadId, HumanSignal.Approve("alice"), store, options);

switch (result)
{
    // Another worker is on it. Nothing was written; leave it alone.
    case GraphExecutionResult<Incident>.Failed { Message: var why } when why.Contains("is held by"):
        break;

    // This worker lost the thread while running it (it stalled past its ttl and another took over).
    // Whatever it was doing isn't recorded: the new owner's checkpoints are the thread's history.
    case GraphExecutionResult<Incident>.Cancelled { Reason: CancelReason.LeaseLost }:
        break;
}

// Who holds a thread, without touching it:
var lease = await store.GetLeaseAsync(threadId);   // null when nobody does
```

```rust
let config = RunConfig::new()
    .with_lease(LeaseOptions::new(format!("worker-{}", std::process::id()), Duration::from_secs(30)));

match investigation.resume_with(thread_id, HumanSignal::approve("alice"), store.clone(), config).await {
    GraphExecutionResult::Cancelled { reason: CancelReason::LeaseLost, .. } => { /* another owner has it */ }
    other => { /* … */ }
}

let lease = store.get_lease(thread_id).await?;   // None when nobody holds it
```

The store members are public, so an application can hold a lease around work of its own:

```csharp
var attempt = await store.TryAcquireLeaseAsync(threadId, owner, TimeSpan.FromSeconds(30));
if (attempt.Lease is { } lease)
{
    try { await store.SaveUnderLeaseAsync(checkpoint, lease); }
    finally { await store.ReleaseLeaseAsync(lease); }
}
```

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Interruption;

/// A thread's lease: who holds it, the token that fences its saves, and when it expires unless renewed.
public sealed record ThreadLease(string ThreadId, string Owner, long Token, DateTimeOffset ExpiresAt);

/// What TryAcquireLeaseAsync found: the lease it acquired, or the live lease that holds the thread.
public sealed record LeaseAttempt(ThreadLease? Lease, ThreadLease? HeldBy) { public bool Acquired { get; } }

/// A save under a lease whose token is no longer its thread's latest. Nothing was written.
public sealed class LeaseLostException : Exception { public ThreadLease Lease { get; } }

public interface ICheckpointStore
{
    // … as before …

    ValueTask<LeaseAttempt> TryAcquireLeaseAsync(string threadId, string owner, TimeSpan ttl, CancellationToken ct = default);
    ValueTask<ThreadLease?> RenewLeaseAsync(ThreadLease lease, TimeSpan ttl, CancellationToken ct = default);   // null: lost
    ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken ct = default);                       // false: it was lost
    ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken ct = default);                     // the live lease

    /// Appends only if the lease's token is the thread's latest (else LeaseLostException) and, with an
    /// expected sequence, only if that is still the thread's latest (else false).
    ValueTask<bool> SaveUnderLeaseAsync(Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken ct = default);
}

public enum CancelReason { Requested, Deadline, Ended, LeaseLost }   // + LeaseLost

namespace Datalium.Strata.Graph;

public sealed record LeaseOptions(string Owner, TimeSpan Ttl);

public sealed class RunOptions { public LeaseOptions? Lease { get; init; } /* … */ }

// Observability: LeaseAcquired(Owner, Token, ExpiresAt) and LeaseLost(Owner, Token) run events.
```

### Rust

```rust
// store (re-exported from checkpoint)
pub struct ThreadLease { pub thread_id: String, pub owner: String, pub token: i64, pub expires_at: DateTime<Utc> }
pub enum LeaseAttempt { Acquired(ThreadLease), Held(ThreadLease) }
pub enum LeasedSave { Saved, NotLatest, LeaseLost }

pub trait CheckpointStore: Send + Sync {
    // … as before …
    fn try_acquire_lease<'a>(&'a self, thread_id: &'a str, owner: &'a str, ttl: Duration)
        -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>>;
    fn renew_lease<'a>(&'a self, lease: &'a ThreadLease, ttl: Duration)
        -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>>;
    fn release_lease<'a>(&'a self, lease: &'a ThreadLease) -> BoxFuture<'a, Result<bool, CheckpointStoreError>>;
    fn get_lease<'a>(&'a self, thread_id: &'a str) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>>;
    fn save_under_lease<'a>(&'a self, checkpoint: Checkpoint, lease: &'a ThreadLease, expected_sequence: Option<i64>)
        -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>>;
}
// SyncCheckpointStore gains the same five, and BlockingCheckpointStore forwards them.

// lease
pub struct LeaseOptions { /* owner, ttl */ }
impl LeaseOptions { pub fn new(owner: impl Into<String>, ttl: Duration) -> Self; }
impl RunConfig { pub fn with_lease(self, lease: LeaseOptions) -> Self; }

pub enum CancelReason { Requested, Deadline, Ended, LeaseLost }   // + LeaseLost

// observability: RunEventKind::LeaseAcquired { owner, token, expires_at } and LeaseLost { owner, token }.
```

- **The defaults.** A store that doesn't override the five members has no leases: .NET's throw `NotSupportedException`, Rust's return a `CheckpointStoreError` ("this checkpoint store has no leases"). Nothing calls them unless a run has `Lease` set, so a custom store keeps working as it did.
- **A ttl below one millisecond is refused**, by the stores (`ArgumentOutOfRangeException`, a `CheckpointStoreError`) and by the run (as a reserved metadata key is: .NET throws `ArgumentException` before it starts, Rust returns `Failed`). So is a blank owner.
- **A lease for another thread than the checkpoint's** is a caller's bug: `ArgumentException` / a `CheckpointStoreError`.

---

## Semantics

### The store's rules

Every adapter implements the same five rules. "Now" is the store's clock.

| Call | Rule |
|---|---|
| **Acquire** | If the thread has a lease that was not released and expires after now, it is **held**: return that lease, change nothing. Otherwise issue the thread's next token (1 for its first lease), with `expires_at` = now + ttl. |
| **Renew** | If the lease's token is the thread's latest, set its owner and `expires_at` = now + ttl, and return the lease as it now is. Otherwise return nothing: it was taken over. |
| **Release** | If the lease's token is the thread's latest, the thread is no longer held; say true. Otherwise change nothing; say false. The token is kept, so the next acquisition still gets a higher one. |
| **Get** | The thread's lease if it is held now, else nothing. |
| **Save under a lease** | If the lease's token is not the thread's latest: write nothing, report the lease lost. Otherwise save as `SaveIfLatestAsync` does (unconditionally when there is no expected sequence). The check and the append are one atomic step. |

Three consequences worth stating:

- **An expired lease nobody took can still be renewed and saved under.** Its token is still the thread's latest, so no other owner exists whose work it could overwrite. A worker that paused past its ttl and woke before anyone took the thread simply carries on. Expiry is permission for someone else to acquire, not a verdict on the holder.
- **Tokens are what make a late save safe.** A worker that stalls for ten minutes with token 5 and then saves is refused if anyone acquired meanwhile, however confident it is that it still holds the lease. That is the property a lease alone can't give.
- **Acquisition is refused for the same owner name too.** Two processes configured with one name would otherwise take the thread from each other in turn. An owner names a process; give each a distinct one.

### What a run does

With `RunOptions.Lease` set, at the outermost level:

1. **Acquire**, before the first save:
   - a fresh run, before its first node;
   - a resume (so a signal and a wake), after everything that can refuse it has been checked and just before the `resumed_at` stamp;
   - an end, after its own refusals and just before the `ended` checkpoint.

   A refused resume or end never touches a lease.
2. **A held thread is refused**: `Failed("thread 't' is held by 'worker-2' until <time>")`, the time rounded up to the second. Nothing is written and no node runs.
3. **Every save is under the lease**: the stamp, an interrupted, failed, cancelled, hop-limit or ended checkpoint. The stamp and the end keep their expected sequence, so they are fenced and conditional in one step. A subgraph's child writes nothing to the store (its checkpoints are nested in its parent's), so only the outermost level saves.
4. **Renew every `ttl / 3`**, on the run's clock (.NET `RunOptions.TimeProvider`, Rust tokio's), in the background.
5. **Release when the run returns**, however it ends: completed, interrupted, failed, cancelled. A thread waiting at a boundary holds nothing, as always. A release that fails is ignored: the lease expires on its own.

### Losing a lease

A run has lost its lease when:

- a **renewal says so** (another owner acquired);
- a **save is refused** as fenced; or
- **no renewal has succeeded for a whole ttl**. A renewal that throws (the store is unreachable) doesn't lose the lease by itself: the run keeps trying every `ttl / 3`. But once a ttl has passed on the run's clock since the last one that worked, the run can no longer show it holds the thread, and stops.

Then:

- The run's token is cancelled, so every node, branch and subgraph stops as for any cancellation (`design/17`): async handlers see their token, sync ones are abandoned, a subgraph's child and a map's items wind down.
- **Nothing more is written.** The `cancelled` checkpoint a cancellation normally writes is not saved, and neither is the checkpoint whose save was refused.
- The result is **`Cancelled(thread, node, CancelReason.LeaseLost, state, stoppedAt)`**: the node that was running or about to start, and the last committed state, as for any cancellation. When a save was refused, it is the node and state that checkpoint held. So a run whose `failed` checkpoint is fenced returns `Cancelled(LeaseLost)`, not `NodeFailed`: what it concluded isn't the thread's history.
- It is not resumable *from this run*: the thread is the new owner's, and its latest checkpoint is whatever they wrote.

A run that **completes** writes nothing, so nothing fences a completion: a stale owner that finishes its last node between two renewals returns `Completed`. Side effects are the handlers' to make idempotent, and SG-10's checkpoint after every node narrows the window to one node.

### What leases do not do

- **They don't stop a writer that doesn't use them.** An unleased save is accepted while a lease is live. Every worker on a thread must run with `Lease`, or none.
- **They don't make a run's side effects exactly-once.** A stale owner's node may run alongside the new owner's until its next renewal or save. Fencing protects the checkpoint log; `design/33`'s idempotency key is for the side effects.
- **They don't queue.** A held thread is refused, not waited for.

### The drivers

**`WakeDueAsync`** passes `WakeOptions.Run` to each wake, so its lease applies. A due thread that is held is **skipped** with the refusal as its reason (`it is held by 'worker-2' until …`), not reported woken with a failed result.

**`DecisionWorker`**'s resume carries the per-resume `RunOptions` / `RunConfig`, so its lease applies. A decision whose thread is held is **left for the next round**: it is not reported, and not remembered as handled. Whoever holds the thread either handles the decision (the next round then finds it `AlreadyHandled` or gone) or lets go.

---

## Run events and telemetry

| | |
|---|---|
| **`LeaseAcquired { Owner, Token, ExpiresAt }`** | After a run acquired its lease, before its first save. Not on a refusal. |
| **`LeaseLost { Owner, Token }`** | When the run finds out, once. `RunCompleted(Cancelled, LeaseLost)` follows. |
| `RunCompleted` | `CancelReason` gains **`LeaseLost`**. A held thread is a `Failed` run with the message. |
| Run span | **`strata.lease.owner`** and **`strata.lease.token`** once acquired; `strata.cancel_reason` = `lease_lost`; a span event `lease_lost`. |
| Metrics | **`strata.lease.acquisitions`** (attribute `strata.lease.outcome`: `acquired` or `held`) and **`strata.lease.losses`**. |

Renewals are not events: one every few seconds per run would drown the rest. `RunEvent` is non-exhaustive in both engines.

---

## Wire format

### The checkpoint

Unchanged. A lease is not part of a checkpoint: it is about who may write the next one. No `schema_version` is taken; SG-10's `running` status takes `1.7.0`.

### SQL: `checkpoint_leases`

```sql
-- SQLite                                          -- Postgres
CREATE TABLE IF NOT EXISTS checkpoint_leases (     CREATE TABLE IF NOT EXISTS checkpoint_leases (
    thread_id  TEXT NOT NULL PRIMARY KEY,              thread_id  TEXT NOT NULL PRIMARY KEY,
    owner      TEXT,                                   owner      TEXT,
    token      INTEGER NOT NULL,                       token      BIGINT NOT NULL,
    expires_at INTEGER                                 expires_at TIMESTAMPTZ
)                                                  )
```

- One row per thread that ever had a lease. A release doesn't delete it, so `token` only grows. Purging the thread (`34-Storage-Hardening.md`) deletes it with the thread's log, and the id then starts over.
- **Held** means `expires_at` is not null and after now. A release sets `owner` and `expires_at` to null.
- SQLite's `expires_at` is Unix milliseconds on the adapter's clock; Postgres's is a `TIMESTAMPTZ` compared with `clock_timestamp()` (not `now()`, which is the transaction's start).
- **Acquire** is one upsert: insert at token 1, or on conflict set the owner, `token + 1` and the expiry `WHERE` the row isn't held, `RETURNING token`. No row back means held, and a second statement reads who holds it. In Postgres it runs under the thread's advisory lock, the one a save takes, so an acquisition waits for a save in flight.
- **The fenced save** is the existing insert with one more condition, `AND (SELECT token FROM checkpoint_leases WHERE thread_id = …) = …`. When it inserts nothing, the token is read again to tell a lost lease from a stale sequence: tokens never go back, so a token that still matches means the sequence was the reason.
- **Migration:** a store opening an older database creates the table (`CREATE TABLE IF NOT EXISTS`). Nothing else changes.

### Redis

| Key | Content |
|---|---|
| `{prefix}:leases:{thread_id}` | a hash, `owner` and `token`, with a `PX` expiry: present while the thread is held |
| `{prefix}:lease-tokens:{thread_id}` | the thread's latest token, a counter that never expires |

Four small scripts (`AcquireLeaseScript`, `RenewLeaseScript`, `ReleaseLeaseScript`, `GetLeaseScript`; Rust `ACQUIRE_LEASE_SCRIPT` and so on) and one save script, **`SaveUnderLeaseScript`**: `SaveIfLatestScript` with a first line that compares the token counter with the lease's token and returns `-1` when they differ. Its `KEYS` are `SaveScript`'s with the token key last, and it takes the expected sequence (empty for none) and the token as `ARGV[6]` and `ARGV[7]`. `ExpiresAt` is Redis's `TIME` plus the ttl.

`SaveScript`, `SaveIfLatestScript` and `IndexScript` are unchanged, so writers from before this document keep working, without leases.

### Shared fixtures

- `checkpoint-log/leases-columns.json`, `postgres-leases-columns.json`: the table.
- `checkpoint-log/leases.rows.json`: three rows as raw SQL would insert them (held far into the future, expired, released) with the lease each store must report for them, and the token the next acquisition gets. A lease one runtime takes is honoured by the other.
- `checkpoint-log/redis-leases.json`: the keys and hash fields an acquisition leaves, and after a release.

### Definitions

Unchanged.

---

## The CLI

Read-only:

- **`strata thread inspect`** prints `Lease: held by 'worker-2' until 2026-10-09T12:00:05Z (token 7)` when the thread is held, and nothing when it isn't; `--json` gains `lease` (`owner`, `token`, `expires_at`, or `null`).
- No `strata thread release`: the CLI doesn't write. A stuck lease expires.

---

## Migration

**.NET** (source-compatible): `ThreadLease`, `LeaseAttempt`, `LeaseLostException` and `LeaseOptions` are new. `ICheckpointStore` gains five default interface members, so existing stores compile. `RunOptions.Lease` is an `init` property. `CancelReason.LeaseLost` is a new enum value: a `switch` with a default arm is unaffected. `InMemoryCheckpointStore` and `EfCoreCheckpointStore` gain an optional `TimeProvider`.

**Rust:** `CancelReason` isn't `#[non_exhaustive]`, so an exhaustive `match` needs a `LeaseLost` arm. The `CheckpointStore` and `SyncCheckpointStore` traits gain default methods, so implementations compile unchanged. `RunEventKind` gains two variants (it is non-exhaustive).

**A store that wraps another** (a decorator) must forward the five members, or runs with a lease are refused through it. The engine's own wrappers and both decision workers' do.

**Stores:** the SQL stores create `checkpoint_leases` on open. Redis needs nothing.

**Strata Cloud:** no change. A run with `Lease` against the Cloud store is refused (see "Not built"); Cloud needs a lease route and a fenced save for leases in Cloud mode, to record as a follow-up in Strata Cloud's API.

**StrataOps:** its runs pass `RunOptions.Lease` with an owner per Worker process, and its `execution` table's claim, renewal and release go away. A Kafka rebalance that hands a partition to another Worker while the first is mid-run is then safe: the second acquires when the first's lease expires, and the first's next save or renewal stops it. Its handling of `Cancelled` gains `LeaseLost` (nothing to record: the thread moved on).

---

## Tests

Mirrored in both engines, in new suites `thread_leases.rs` / `ThreadLeaseTests.cs` unless noted:

- **The store contract, in every adapter** (`checkpoint_store_contract.rs`, `StoreContracts`; Postgres and Redis behind their env gates):
  - acquire gives token 1, then a held thread is refused with the holder, the same owner name included;
  - renew extends, and `GetLease` reports it;
  - release frees the thread, and the next acquisition's token is higher;
  - an expired lease is taken over with the token incremented;
  - the stale owner's save is refused and writes nothing, its renewal returns nothing and its release changes nothing;
  - an expired lease nobody took still saves and renews;
  - a save under a lease with a stale expected sequence returns false, and with the right one appends;
  - an unleased save is unaffected by a live lease.
- **Leases on a fake clock:** the in-memory and SQLite stores expire and renew by the clock they are given, with no waiting.
- **The engine:**
  - a run acquires before its first node, saves its interrupted checkpoint under the lease, and has released when it returns;
  - a held thread refuses a run, a resume, a signal and an end, with nothing written and no node run; the thread stays resumable;
  - a resume's refusals (already resumed, a bad override) never acquire;
  - renewal: a run longer than its ttl keeps its lease (fake clock);
  - a lease taken over mid-run: the run returns `Cancelled(LeaseLost)` at the node running, its handler's token fires, and the log has nothing from it;
  - a fenced save: an interrupted, a failed and a cancelled checkpoint each become `Cancelled(LeaseLost)` with nothing written;
  - an unreachable store: renewals failing for a whole ttl lose the lease; failing for less doesn't;
  - a lease lost inside a subgraph: one lease for the run, the child's nodes stop, and the result names both levels;
  - a store without leases refuses a run with `Lease`.
- **The drivers:** `WakeDueAsync` skips a held due thread and wakes it once released; the decision worker leaves a held thread's decision for the next round (`worker.rs`, `DecisionWorkerTests.cs`).
- **Events and telemetry:** `LeaseAcquired` then the run's events; `LeaseLost` then `RunCompleted(Cancelled, LeaseLost)`; the span's tags and the counters.
- **Fixtures:** the table layout in every SQL store; the raw rows of `leases.rows.json` read by every SQL store and the CLI; the Redis keys.
- **The CLI:** `thread inspect` on a held and an unheld thread, against SQLite, Postgres and Redis.
- **Mutation checks**, both engines: not checking the token in the fenced save; not incrementing the token on takeover; acquiring over a live lease; writing the `cancelled` checkpoint after a lost lease.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

### The questions as asked (the handoff brief)

Recommendations are in bold.

1. **The port.** **Lease members on the checkpoint store.** Fencing has to be decided in the same atomic step as the append, which only the store that owns the log can do.
2. **Fencing.** **A per-thread monotonic token, checked by every save made under a lease; saves with no lease unchanged.**
3. **The engine.** **`RunOptions.Lease`; acquire before the first save, renew every `ttl / 3` on the run's clock, release on return; a held thread refused; a lost lease ends the run `Cancelled(LeaseLost)` with nothing more written.**
4. **Store mechanics.** **As the brief's table**: a `checkpoint_leases` row under the advisory lock in Postgres, the same table in SQLite, a key with `PX` and a token checked in a save script in Redis, under the lock in memory.
5. **The clock.** **The store's where there is one**, else a clock the adapter is given.
6. **The Cloud store.** **No leases**, a follow-up for Cloud.

### Added by this document

7. **One fenced save member, not two.** **`SaveUnderLeaseAsync(checkpoint, lease, expectedSequence = null)`.** A null expected sequence already means "unconditionally" in `SaveIfLatestAsync`, so one member covers both saves, and a store has one fenced statement to get right.
8. **What an acquisition returns.** **The lease, or the lease that holds the thread** (`LeaseAttempt`), not a bare nullable: the refusal has to say who holds the thread and until when, and a second call to ask would race.
9. **Does expiry refuse a save?** **No: only the token.** Refusing an expired holder nobody replaced would fail work no one else is doing, and the holder's clock and the store's never agree exactly. The token says whether anyone took over, which is the only thing that matters to the log.
10. **Can the same owner name re-acquire a live lease?** **No.** Reentrancy would let two processes with one name alternate, each fencing the other. A restarted process waits out its old lease, at most one ttl.
11. **How a lost lease is reported by a store, in Rust.** **`LeasedSave::LeaseLost`, an outcome, not an error.** `CheckpointStoreError` is a public tuple struct holding a string, so a `LeaseLost` variant would break every store and caller that builds one. .NET throws `LeaseLostException`, as the brief says.
12. **Where a resume acquires.** **After the preflight, before the stamp.** A resume that would be refused anyway shouldn't take a thread from anyone, and the stamp is conditional on the sequence loaded, so a change between the load and the acquisition is still caught.
13. **A renewal that fails for the store's reasons.** **Not a loss until a ttl has passed without one succeeding.** One dropped connection shouldn't cancel every run in the process; a ttl without contact means the lease may be someone else's.
14. **A fenced `failed` checkpoint: `NodeFailed` or `Cancelled(LeaseLost)`?** **`Cancelled(LeaseLost)`.** The failure isn't recorded, so reporting it as the thread's outcome would be reporting something the log contradicts.
15. **A completed run that had lost its lease.** **`Completed`.** A completion writes nothing, so the store can't refuse it, and a check at the end would only move the race. SG-10 closes most of it.
16. **Reading a lease.** **`GetLeaseAsync`**, on the store. SG-10's recovery driver needs it, an operator needs it, and tests need it.
17. **The decision worker and a held thread.** **Left for the next round**, not `Failed`: a `Failed` decision is remembered and never tried again, which would drop an approval whose thread was only briefly held.

---

## Implementation Status

Built 2026-10-09 in both engines, every store adapter, both decision workers and the CLI, in one change, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **A store without leases refuses in each engine's habit.** .NET's default members throw `NotSupportedException`, which propagates out of the run like any store exception. Rust's return a `CheckpointStoreError` (`NO_LEASES`, "this checkpoint store has no leases"), which the run returns as `Failed`. Either way the run is refused when it acquires, before any node runs: the options alone can't tell.
- **A resume rehydrates its state before the stamp, not after.** A lost lease at the stamp has to report a state, so the resume now builds it first. The preflight had already checked that it rehydrates, so no refusal moved; the lost result carries the node the resume was about to start and the state with the signal's overrides applied.
- **A fenced end** returns what the end would have returned (where the thread stood, its state), with `LeaseLost` instead of `Ended`.
- **A handler that returns when its token fires has finished.** The engine abandons a handler that hangs, and the lost result then carries the state its node started with. A handler that notices its token and returns promptly commits; the save after it is what is refused, and the result carries the committed state. Both write nothing.
- **The renewal's timer is always set.** .NET sets the first one when the lease is acquired, before the run goes on, and each next one before the renewal it follows is made. So time that passes on the run's clock while a renewal is in flight, or while the thread pool is busy, still counts towards the next one. The first build started the loop on the pool and set each timer after its renewal; a fake clock advanced in between was missed, which is what failed `Renewals_Failing_For_Less_Than_A_Ttl_Lose_Nothing` in CI. Rust's loop sleeps on tokio's clock, which a paused runtime only moves once every task is waiting.
- **A dropped Rust run can't release.** Dropping the future of `run` / `resume` isn't a cancellation (`design/17`): it stops the renewal task and leaves the lease to expire. .NET has no such case.
- **The checks every store makes are public,** so a custom store refuses the same things with the same words: .NET `ThreadLeases` (`Check`, `CheckTtl`, `CheckThread`, `Milliseconds`, `MinimumTtl`, `NotSupported`), Rust `store::check_lease`, `check_lease_ttl`, `check_lease_thread`, `lease_millis`, `MINIMUM_LEASE_TTL`, `NO_LEASES`.
- **The clock seams.** .NET: an optional `TimeProvider` on `InMemoryCheckpointStore`, `EfCoreCheckpointStore` and `EfCoreCheckpointStore.CreateInMemory`. Rust: `with_clock(Clock)` on `InMemoryCheckpointStore` and `SqliteCheckpointStore`, where `Clock` is `Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>`, exported from `datalium_strata_memory`.
- **An acquisition looks again, at most three times,** when the upsert says the thread is held and the read that follows finds nobody (it was released or expired between the two statements); then it fails with `the lease of thread 't' kept changing while it was being acquired`. Redis and the in-memory stores decide in one step and never retry.
- **Rust's Postgres store reads an expiry as Unix milliseconds** (`floor(extract(epoch from expires_at) * 1000)::bigint`), so `tokio-postgres` needs no date feature. .NET reads the `TIMESTAMPTZ`.
- **The wake driver** doesn't count a timeout's end as a wake (`strata.thread.wakes`) when the thread was held or the lease was lost.
- **The lost-lease span event** is an `ActivityEvent` named `lease_lost` in .NET and a `tracing` warning, `lease lost`, under the run span in Rust, as for the hop limit.
- **A map isn't tested on its own.** Its items stop through the same token a subgraph's child does, and `ThreadLeaseTests` / `thread_leases.rs` cover the subgraph.

**How it was built:**

- **.NET:**
  - `Interruption/ThreadLease.cs` is new: `ThreadLease`, `LeaseAttempt`, `LeaseLostException`, `ThreadLeases`. `ICheckpointStore` gained the five members; `CancelReason.LeaseLost`.
  - `Graph/LeaseOptions.cs` is new: `LeaseOptions`, and the internal `RunLease` (acquire, the renewal loop on the run's `TimeProvider`, the fenced save, release) and `LeasedSave`. `RunOptions.Lease`, checked in `RunMetadata.Validate`.
  - `Graph/CompiledGraph.Leases.cs` is new: `AcquireLeaseAsync`, `PersistAsync`, `PersistIfLatestAsync`, `LeaseLostAt`. Every save in `CompiledGraph.Interruption.cs`, `.End.cs` and `.Waits.cs` goes through them; a fresh run starts in `StartAsync`, which acquires first; `ObserveAsync` releases.
  - `Execution.cs`: `RootScope` links the lease's loss into the run's token; `RunScope.Lease`, `Held`, and `Reason`, which says `LeaseLost` first.
  - `RunTelemetry`: `LeaseAcquired`, `LeaseHeld`, `LeaseLost`; `ObservedStore` forwards the five members and reports `CheckpointSaved` only for a save that wrote. `StrataTelemetry`: the two counters. `RunEvent.cs`: the two events.
  - Storage: each of the four adapters implements the five members; `CheckpointLeasesDdl` on both SQL stores; the five Redis scripts.
  - `Datalium.Strata.Cloud`: `DecisionWorker` leaves a held thread's decision unhandled, and its `PinnedStore` forwards the lease members.
- **Rust:**
  - `store.rs`: `ThreadLease`, `LeaseAttempt`, `LeasedSave`, the five default methods on `CheckpointStore` and `SyncCheckpointStore` (`BlockingCheckpointStore` forwards them), and the checks. `lease.rs` is new: `LeaseOptions`, and the internal `RunLease` with its renewal task.
  - `context.rs`: `RunConfig::with_lease`, `RunScope::lease` / `held`, `reason()`; dropping a `RootScope` stops the renewal.
  - `checkpoint.rs`: `CancelReason::LeaseLost`; `save_checkpoint` returns whether it wrote and `save_checkpoint_if_latest` a `LeasedSave`; `acquire_lease`, `lease_lost_at`, `start`; `observed` takes the `RootScope` and releases; `wake_one` skips a held thread.
  - `observability.rs`: `RunEventKind::LeaseAcquired` / `LeaseLost`, `lease_acquired` / `lease_held` / `lease_lost`, the span fields, `metric_names::LEASE_ACQUISITIONS` / `LEASE_LOSSES`. `RunTelemetry::resumed` takes the status and path instead of the checkpoint.
  - `strata-memory`: the five methods in each of the four adapters, `CHECKPOINT_LEASES_DDL` and `POSTGRES_CHECKPOINT_LEASES_DDL`, the five Redis scripts, `Clock`. `strata-cloud`: the worker as in .NET.
- **CLI:** `Storage::lease` (and `hold`, for tests), `store::lease_text` / `lease_json`, the `Lease:` line and the `lease` key in `thread inspect`.
- **Schema:** the four fixtures above; `schema/README.md` gained "Thread leases" and the Redis keys and scripts. No JSON Schema changed.

**Tests:**

- **New suites:** `thread_leases.rs` in `strata-core` (17) with `telemetry.rs`'s lease test, and `ThreadLeaseTests.cs` (18, the telemetry test among them), test for test. The Rust suite runs on a store of its own (`tests/lease_store`), since `strata-core`'s tests can't depend on `strata-memory`; its clock is tokio's paused one.
- **Stores:** the lease contract and a leased run against every adapter in both engines, on a fake clock in memory and in SQLite and really waiting in Postgres and Redis (`strata-memory`'s `thread_leases.rs`, `StoreContracts` / `StoreContractTests`); the table layouts, the raw rows of `leases.rows.json`, a database from before leases, and the Redis keys (the same file, `CheckpointLeaseTableTests.cs`).
- **Workers:** a held thread's decision is left for the next round and resumed once the holder lets go; a leased run against the Cloud store is refused (`worker.rs`, `DecisionWorkerTests.cs`).
- **CLI:** `thread inspect` on a held and an unheld thread, in text and JSON, against SQLite, Postgres and Redis (`storage_backends.rs`); the lease table's layout (`checkpoint_log_layout.rs`).
- **Totals when this landed:** .NET 746, with Postgres and Redis running, and 2 skipped (the opt-in Cloud live tests), plus the example's 1. `rust/` 668 and `cli/` 75, with `STRATA_REQUIRE_BACKENDS=1` against live Postgres and Redis, and `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` and `dotnet format --verify-no-changes` are clean.
- **Mutation checks, in both engines** (each broken, the lease suites run, then restored). The three store mutations were made in each engine's in-memory store; .NET's engine suite runs on it and Rust's doesn't, which is the difference in the counts:
  - Not checking the token in the fenced save fails 6 tests in .NET and 1 in Rust.
  - Not incrementing the token on takeover fails 11 and 2.
  - Acquiring over a live lease fails 7 and 2.
  - Writing the checkpoint after a lost lease fails 7 and 6.

## Not built (deliberately out of scope)

- ~~**Leases on Strata Cloud.**~~ Built 2026-10-20 (`43-Cloud-Leases-And-Listing.md`, SG-36). What this said: Cloud's SDK contract has no lease route and no fenced save. `StrataCloudCheckpointStore` keeps the defaults, so a run with `Lease` against it is refused. A follow-up for Strata Cloud's API.
- **Refusing unleased saves while a lease is live.** Leases are opt-in; a deployment that wants the guarantee runs every worker with one.
- **Waiting for a held thread**, or a queue of owners: a held thread is refused.
- **Reentrant leases** (the same owner acquiring again), and **handing a lease from one run to the next** (a resume right after a run acquires afresh).
- **A lease per nesting level or per map item**: the run holds one lease, on the thread.
- **Fencing a completion**: a completed run writes nothing. Since 2026-10-10 a graph compiled with `CheckpointPolicy.EveryNode` writes a `completed` checkpoint under the lease, so its completion is fenced (`design/33-Durable-Execution.md`).
- **A `strata thread release` command**, or stealing a live lease: a stuck lease expires.
- **Renewal events**, and a configurable renewal interval: it is `ttl / 3`.
- **Listing threads by lease** (held, expired). SG-10's recovery driver (`design/33-Durable-Execution.md`) didn't need it: it lists `running` threads and reads each one's lease.
