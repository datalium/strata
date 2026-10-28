# Datalium Strata: Leases and the Query Port on the Strata Cloud Store

`design/30-Strata-Cloud-Store.md` (SG-18) built the Cloud store with the store port only: save, load and the conditional save. Two later features left it behind. Thread leases (`design/32`, SG-12) added five members to the store port, and the Cloud store kept their defaults, so a run with a lease against it was refused. The drivers (`WakeDueAsync`, `RecoverStaleAsync`, the hosted wake and recovery drivers of `design/35`) list threads through the query port (`design/19`), which the Cloud store didn't have. So a Cloud-mode runtime could fence no saves, wake no timers, time out no approvals and recover no dead runs.

Strata Cloud closed its side in Phase 10b (its SDK wire contract §11, §12 and §8): lease routes, a fenced save, `GET /v1/sdk/threads` and `GET /v1/sdk/checkpoints`. This document is the adapter's side: **SG-36**. The SG numbers refer to the gap log of StrataOps (not public yet). It lands in both engines at one commit (strict parity, StrataOps ADR 0003).

It was written with the build and records it; "Implementation Status" at the end says what is verified and what isn't yet.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Where | **The existing `StrataCloudCheckpointStore`** in both engines, on its transport: the same timeouts, retry policy, `Retry-After` handling, error types, telemetry and fixtures as SG-18. No new type. |
| Leases | **The five lease members, mapped as contract §8's table says.** Acquire is `POST /v1/sdk/leases`, renew `PUT`, release `DELETE`, get `GET`; the fenced save is today's save with `Strata-Lease-Token`. "Held" and "taken over" are `200`s and map to the port's answers, not to errors. |
| A retried acquisition | **An `Idempotency-Key` per call**, a UUID v7, the same on each of the call's attempts: a retry after a lost answer gets back the lease the first attempt took. Without it Cloud would answer "held by your own owner name", which the port can't tell from a real holder. |
| A lost lease | **`423 STRATA_CLOUD_API_LEASE_LOST` is `LeaseLostException` / `LeasedSave::LeaseLost`**, never retried. A `423` on a save without a lease is an error (`Rejected`), not a lost lease. |
| A leased `412` | **`false` / `NotLatest` with no look-up.** Cloud records the token each checkpoint was saved under and answers a retry of our own committed fenced save `200`, so under a lease a `412` is always another writer. The unleased look-up after an ambiguous attempt (`design/30`) is unchanged. |
| The listing | **One `GET /v1/sdk/threads` per `ListThreadsAsync` / `list_threads` call**, every `ThreadQuery` filter on the query string, Cloud's page and cursor as they come. A page may be short while more follow (Cloud looks at 5,000 threads a request for a metadata or graph filter); the port allows that, and the engine's drivers already follow the cursor until it is null. |
| The cursor | **Cloud's, opaque, passed back as it came.** `ThreadQuery.Validate` decodes the shared cursor encoding, so the store checks the limit and the version-needs-an-id rule itself and leaves the cursor alone; a cursor Cloud didn't issue is Cloud's `400`. |
| The log | **Followed until the page is full or the log ends.** Cloud may stop a page at about 4 MB of stored state with more to come; the port's log page is full or the end, so `GetLogAsync` / `get_log` asks again after the last entry for the rest of the limit. |
| Retention | **None.** Cloud doesn't serve a purge: retention is Cloud's own. The store implements no `ICheckpointRetention` / `CheckpointRetention`, so the retention driver still refuses it. |
| Hosting | **No code.** `AddCloudCheckpoints` registers a store under every port its type implements, so it is now `ICheckpointQuery` too, and the wake and recovery drivers' start-up checks pass. Rust's `Host::wake` / `recover` take any `CheckpointStore + CheckpointQuery`. |
| Wire format | **None.** No checkpoint, definition or database change, no `schema_version`. The shared Cloud fixtures grow from 23 to 41. |

---

## Usage

Nothing new to call: a leased run, a wake and a recovery take the Cloud store as they take any other.

```csharp
using var store = new StrataCloudCheckpointStore(cloudOptions);
var lease = new RunOptions { Lease = new LeaseOptions("worker-1", TimeSpan.FromSeconds(30)) };

await graph.RunAsync(threadId, state, store, lease);                       // fenced on Cloud
await graph.WakeDueAsync(store, new WakeOptions { Run = lease });          // lists Cloud's due threads
await graph.RecoverStaleAsync(store, new RecoverOptions { Run = lease });  // lists Cloud's running threads

// Hosting: the drivers now run on Cloud; retention still doesn't.
services.AddStrata()
    .AddCloudCheckpoints()
    .AddGraph(_ => investigation)
    .AddWakeDriver<Investigation>()
    .AddRecoveryDriver<Investigation>();
```

```rust
let store = Arc::new(StrataCloudCheckpointStore::new(cloud_options)?);
let lease = RunConfig::new().with_lease(LeaseOptions::new("worker-1", Duration::from_secs(30)));

graph.run_with(&thread_id, state, store.clone(), lease.clone()).await;
graph.wake_due(store.clone(), Utc::now()).await?;
graph.recover_stale(store.clone(), Utc::now(), RecoverOptions::new().with_config(lease)).await?;

Host::new()
    .wake(graph.clone(), store.clone(), WakeDriver::new())
    .recover(graph, store, RecoveryDriver::new().with_config(lease))
    .run(shutdown)
    .await?;
```

A lease's ttl must be within Cloud's limits, **1 s to 1 h**, and an owner at most 256 bytes: Cloud refuses anything else with a `422` (`Rejected`), and the run then fails with that store error. The adapter doesn't repeat Cloud's limits (`design/30` leaves client-side validation of Cloud's rules out, and an additive widening on Cloud's side would then be refused here).

---

## The API

Additions only.

### .NET (`Datalium.Strata.Cloud`)

```csharp
public sealed class StrataCloudCheckpointStore : ICheckpointStore, ICheckpointQuery, IDisposable
{
    // … as before, plus the five lease members of ICheckpointStore and:
    public ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default);
    public ValueTask<CheckpointLogPage> GetLogAsync(
        string threadId, long? afterSequence = null, int limit = ThreadQuery.DefaultLimit, CancellationToken cancellationToken = default);
}

public static class StrataCloudWire
{
    public const string LeaseTokenHeader = "Strata-Lease-Token";
    public const string IdempotencyKeyHeader = "Idempotency-Key";
    public const string LeaseLostCode = "STRATA_CLOUD_API_LEASE_LOST";
}
```

### Rust (`datalium-strata-cloud`, `datalium-strata`)

```rust
impl CheckpointStore for StrataCloudCheckpointStore { /* … plus the five lease methods */ }
impl CheckpointQuery for StrataCloudCheckpointStore { /* list_threads, get_log */ }

pub mod wire {
    pub const LEASE_TOKEN_HEADER: &str = "strata-lease-token";
    pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
    pub const LEASE_LOST_CODE: &str = "STRATA_CLOUD_API_LEASE_LOST";
}

// datalium-strata (strata-core): ThreadSummary is #[non_exhaustive], so a store in another crate that reads
// summaries from somewhere other than a checkpoint needs a constructor.
impl ThreadSummary {
    pub fn new(thread_id, sequence: i64, state: ThreadState, status: CheckpointStatus,
               updated_at: DateTime<Utc>, schema_version) -> Self;   // the other fields empty; set them after
}
```

The Rust store's typed methods stay `save_checkpoint` and `load_checkpoint`; the lease and query methods are the ports', whose error is the string `CheckpointStoreError` carrying the `CloudError`'s message, as for the save and the load. `datalium-strata-cloud` gains the dependency `uuid` (v7, the version `datalium-strata` already uses).

---

## The wire

### Routes

| Port member | Request | Answer |
|---|---|---|
| `TryAcquireLeaseAsync` / `try_acquire_lease` | `POST /v1/sdk/leases` `{ thread_id, owner, ttl_ms }`, `Idempotency-Key` | `{ acquired, lease }`: `Acquired(lease)` / `Held(lease)` (.NET `LeaseAttempt(lease, null)` / `(null, lease)`) |
| `RenewLeaseAsync` / `renew_lease` | `PUT /v1/sdk/leases` `{ thread_id, owner, token, ttl_ms }` | `{ lease }`: the lease as it now is, or `null` when taken over |
| `ReleaseLeaseAsync` / `release_lease` | `DELETE /v1/sdk/leases?thread_id=&token=` | `{ released }` |
| `GetLeaseAsync` / `get_lease` | `GET /v1/sdk/leases?thread_id=` | `{ lease }` or `null` |
| `SaveUnderLeaseAsync` / `save_under_lease` | `POST /v1/sdk/checkpoints` with `Strata-Lease-Token`, and `Strata-If-Sequence` for an expected sequence | `201`/`200` saved; `412` `false` / `NotLatest`; `423 LEASE_LOST` `LeaseLostException` / `LeaseLost` |
| `ListThreadsAsync` / `list_threads` | `GET /v1/sdk/threads?state=…&due_by=&updated_after=&updated_before=&meta.<k>=<v>&graph_id=&graph_version=&limit=&cursor=` | `{ threads: [ThreadSummary], next_cursor }` |
| `GetLogAsync` / `get_log` | `GET /v1/sdk/checkpoints?thread_id=&after_sequence=&limit=`, repeated | `{ entries: [{ sequence, checkpoint }], next_after_sequence }` |

`ttl_ms` is the ttl in whole milliseconds, rounded up (`ThreadLeases.Milliseconds` / `lease_millis`, what every store counts in). `expires_at` is parsed as RFC 3339 into a UTC time: Cloud's clock, as the port says (`design/32`, "Clock"). A thread id is always in the body or the query string, never the path (ids may contain `/`).

**The listing's query string.** Each state of `ThreadQuery.States` is a `state` parameter of its own; `due_by`, `updated_after` and `updated_before` are sent as the engines write timestamps, RFC 3339 at whole seconds (`CheckpointTimestamps.Text` / `timestamp_text`), the text every other store compares, so a sub-second bound means the same on Cloud; each metadata pair is `meta.<key>=<value>`; `limit` is always sent. A row maps to `ThreadSummary` field for field: `state` by its name, `status` as the checkpoint's, `metadata` `{}` when absent, `updated_at` (the runtime's clock, `resumed_at ?? created_at`, contract §12), `wait` and `graph` `null` when absent.

**Checked before sending**, as every store checks: a blank owner and a ttl below 1 ms (`ThreadLeases.Check` / `check_lease`), a ttl on renewal (`CheckTtl` / `check_lease_ttl`), a lease for another thread than the checkpoint's (`CheckThread` / `check_lease_thread`), a listing or log limit outside 1 to 1,000, and a `GraphVersion` without a `GraphId`. Nothing is sent for any of them.

### Retries

The transport's policy (`design/30`, "Retries"), per call:

| Call | Retried on | Never on |
|---|---|---|
| A save, leased or not | network errors, timeouts, `408`, `409`, `429`, `500`, `502`, `503`, `504` | `412`, `423`, any other `4xx` |
| Acquire, renew, release, get, list, log | network errors, timeouts, `408`, `429`, `500`, `502`, `503`, `504` | any `4xx` |

Every one of them is safe to repeat after a lost answer (contract §11, "Retrying after a lost answer"): renewing, releasing and getting twice are the same as once; an acquisition carries its key; a fenced save that went in is answered `200` on its retry, conditional or not; a listing and a log page are reads.

---

## Telemetry

As SG-18's: a `strata.cloud.request` span per attempt, `strata.cloud.retries` by route and reason, `strata.cloud.request.duration` by route and status. The new routes are `leases.acquire`, `leases.renew`, `leases.release`, `leases.get`, `threads.list` and `checkpoints.log`; a fenced save is `checkpoints.save`, like any save. `strata.thread.id` is on every span that names one thread (all but the listing). The lease token, the owner and the idempotency key are not recorded; the engine's own lease telemetry (`design/32`) is unchanged.

---

## Hosting and the drivers

- **.NET:** `AddCloudCheckpoints` registers through the same `Checkpoints<TStore>` as every store, which adds `ICheckpointQuery` when the type implements it. The start-up checks of `AddWakeDriver` and `AddRecoveryDriver` (a registered `ICheckpointQuery`) now pass with Cloud; `AddRetention` still refuses it (`the retention driver needs a checkpoint store that purges (ICheckpointRetention), and the registered one doesn't`). Its doc comment says so.
- **Rust:** `Host::wake` and `Host::recover` take `Arc<T>` with `T: CheckpointStore + CheckpointQuery`, which the Cloud store now is. `Host::retain` needs `CheckpointRetention`, which it isn't.
- **The decision worker** resumes through its `PinnedStore`, which already forwards the lease members (`design/32`). A resume reads no listing, so the pinned store needs no query port. A decision whose thread another owner holds on Cloud is left for the next round, as against any store.
- **SG-35's "list and wake"** (`design/42`): `ListThreadsAsync(new ThreadQuery { DueBy, GraphId })` then `ResumeAsync(threadId, LevelSignals.Due, …)` per thread now works on Cloud.

---

## Tests

Test for test in both engines.

- **Shared fixtures** (`schema/fixtures/cloud/`, 23 → 41; `CloudFixtureTests`, `tests/fixtures.rs`): an acquisition acquired and held; acquired after a lost answer, and after a `503`, with the same `Idempotency-Key` on both attempts; renewed and taken over; released; get, held and not; a fenced save that is `423` (one request); a fenced conditional save retried after a lost answer that is `200` (no look-up); a fenced conditional save retried into a `412` (`NotLatest`, no look-up); a listing page with `cases.json`'s own rows; every filter on the query string (repeated `state`, whole-second timestamps, `meta.` keys, Cloud's cursor); a short page with a cursor; a refused cursor (`400`, one attempt); a log page that stops early, and one that reaches the end. The fixture format grew: a request's `query` value may be a list (a repeated key), its `body` may be a JSON object (compared by value), `present_headers` names headers that must be sent with any value, and `expect.same_header` names one every attempt must send with the same value.
- **Against the emulated Cloud** (`CloudLeaseAndListingTests.cs`, `tests/leases_and_listing.rs`): the test fakes' small Cloud gained leases (with the idempotency key and the per-checkpoint token), the fenced save, the listing (filtered and ordered by the engine's own `ThreadQuery.Matches` / `matches`, with a cursor of its own that the adapter must pass back) and the log, with switches for short listing pages and early-stopping log pages. On it: a leased durable run saves every checkpoint under its token and releases; a held thread is refused with nothing written; a takeover during a node makes the next save `Cancelled(LeaseLost)` with nothing more written; `WakeDueAsync` follows short pages (the first one empty) and wakes every due timer; a `LevelSignals.Due` resume wakes a timer; `RecoverStaleAsync` skips a running thread a live lease holds and recovers it once released; the listing maps each row to exactly the summary the engine makes from the checkpoint (nested, identified, running, ended, waiting); the log follows early stops; each acquisition sends a key of its own; a `423` without a lease is `Rejected`; what a store would refuse is refused with nothing sent; and **`checkpoint-query/cases.json` through the Cloud store**, every `list` case page by page and every `log` case. .NET also runs the store contracts through it (`FreezeResume`, `ConditionalSave`, `FreezeEnd`, `FreezeWake`, `LeasedRun`, `DurableRun`); Rust's live in `strata-memory`'s tests, which can't depend on `strata-cloud`, and its Cloud tests cover the same paths.
- **Telemetry:** a lease call retried after a `503` is two `leases.get` spans with the thread, and a retry counted under its route; a listing is a `threads.list` span with no thread.
- **Workers and hosting:** the worker's held-thread test now holds the thread on the emulated Cloud itself, where it used a lease store beside it; the test that a leased run against Cloud is refused is gone. The hosting test registers the Cloud store as `ICheckpointQuery` and refuses retention with it.
- **Live tests** (opt-in, `STRATA_CLOUD_URL`, `STRATA_CLOUD_KEY`; `CloudLiveTests.cs`, `tests/live.rs`): a leased durable run completes and releases; a held thread is refused with nothing written; a save after a takeover is `Cancelled(LeaseLost)` (the run's renewals are answered by a test decorator and never sent, so its 1 s lease runs out while `Draft` waits, and another owner takes the thread); a due timer is woken by `WakeDueAsync` and another through a `LevelSignals.Due` resume; `RecoverStaleAsync` lists and recovers a running thread. Every thread id and every graph id is new per run (`sg36-live-…`), so the drivers, which list by graph id, see only this run's threads in a dev account that keeps what earlier runs left.
- **Mutation checks, in both engines** (each broken, the suites run, then restored): retrying a `423`; sending no `Idempotency-Key`; the same key for every call; ending a listing at a short page; stopping the log at its first page; looking up a leased `412`; not mapping a `423` to a lost lease. Each fails at least one test in each engine.

---

## Review decisions

1. **Fill a short listing page in the adapter, or pass it through?** **Through.** The port says a page may be short while more follow, every engine caller already follows the cursor, and filling would make one call cost an unbounded number of 5,000-thread scans. The log is the other way round because the port's log page has no such allowance: a short log page means the end.
2. **Decode Cloud's cursor?** **No.** It is Cloud's, and `ThreadQuery.Validate`'s decoding of the shared encoding would refuse it, so the store checks the rest of what `Validate` checks itself.
3. **Check Cloud's ttl and owner limits in the adapter?** **No**, as `design/30` decided for Cloud's other rules: Cloud's `422` says it, with the field's path, and a limit Cloud later widens would otherwise stay refused here until a release.
4. **Treat "held by my own owner name" after an ambiguous acquisition as an error?** **No: send the key instead.** With the key Cloud answers our own lease `acquired: true`; a holder of the same name is then really another process configured with one name, which the port reports as held, as every store does.
5. **A Rust host test with the Cloud store?** **No.** It needs a dev-dependency of `datalium-strata-cloud` on `datalium-strata-host`, which crates.io would need published first, and the release workflow publishes `strata-cloud` before `strata-host`. The host's drivers call `wake_due_with` and `recover_stale` with the same trait bounds the Cloud tests use.

6. **Run `checkpoint-query/cases.json` against the live stack?** **No** (Nick, 2026-10-20). Its seed has fixed thread ids and Cloud has no purge, so a second run against the same dev account would collide with the first. Cloud ran it in its own e2e suite on a fresh database (25 of 25 `list` cases, 7 of 7 `log` cases), and both adapters run it through the emulated Cloud on every test run.

---

## Wire format

None. The fixtures under `schema/fixtures/cloud/` are recorded exchanges, not a wire format of ours, and `schema/README.md` describes their new fields.

---

## Implementation Status

Built 2026-10-20 in both engines, in one change, as specified above.

**Where it diverged from the handoff, and why:**

- **`ThreadSummary::new` in `strata-core`**, an additive constructor: `ThreadSummary` is `#[non_exhaustive]`, so `strata-cloud` couldn't build one from Cloud's row.
- **No Rust host test** (review decision 5).
- **`StoreContracts.FreezeResumeContractAsync` loads with `LoadAsync`**, not the synchronous `Load`, so the async-only Cloud store can run it; for the other stores the default `LoadAsync` calls `Load`, so nothing changes. **`CheckpointQueryContractTests.RunCasesAsync` takes `opaqueCursor`**, which leaves out its check that a malformed shared cursor is an `ArgumentException`.

**Live:** every live test passed against a local Strata Cloud stack on 2026-10-20, 7 of 7 in each engine (SG-18's two and the five above), with a key minted from the dev token. Everything else above passed too.

## Not built (deliberately out of scope)

- **Retention on Cloud.** Cloud doesn't serve a purge; its own retention applies.
- **A cursor on the last page of decisions** and **`LevelSignals` in decisions**, as `design/30` left them.
- **An `Idempotency-Key` on unleased conditional saves** (Cloud's Q16): Cloud hasn't built it, so an unleased conditional save that lost its answer still looks the thread up (`design/30`).
