# Datalium Strata: The Strata Cloud Checkpoint Store and Decisions Client

Every store so far is one the customer runs: in-memory, SQLite, Postgres, Redis (`design/19`). Strata Cloud is the hosted one. A runtime saves its checkpoints to it over HTTPS, a reviewer approves or rejects an interrupted thread in the Strata Cloud console, and the runtime learns of the decision and resumes the thread. The wire is Cloud's: its SDK wire contract (v1) and its OpenAPI description. The contract names the adapter "`StrataCloudCheckpointStore` in Rust and C#", owned by this repo.

StrataOps needs it for M4's Cloud mode, the headline demo scene: the investigation's run saves to Strata Cloud, the rollback is approved in the console, and the StrataOps Worker resumes the thread.

This document specifies **SG-18**: the store, a client for the decision feed, and a worker loop that resumes decided threads. The SG numbers refer to the gap log of StrataOps (not public yet). StrataOps' gap doc lists SG-18 as ".NET only" and its ADR 0003 calls it "a .NET catch-up"; neither engine had any Cloud code, so it lands in both engines at one commit (strict parity, StrataOps ADR 0003): the same capability, in each language's idiom.

It was written as a blueprint before implementation and is now built. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Packaging | A new .NET project **`Datalium.Strata.Cloud`** (BCL `HttpClient` and `System.Text.Json`, referencing only `Datalium.Strata`) and a new Rust crate **`rust/strata-cloud`** (`datalium-strata-cloud`: `reqwest` with `rustls`, `flate2`). HTTP and TLS stay out of `strata-core`, `Datalium.Strata` and the storage packages. |
| The store | **`StrataCloudCheckpointStore`**: `ICheckpointStore` / `impl CheckpointStore`. Async only: .NET's synchronous `Save` / `Load` throw `NotSupportedException`, as the port allows. |
| The conditional save | **Detected at runtime.** A load whose response carries `Strata-Sequence` reports the sequence; a conditional save sends `Strata-If-Sequence`, and Cloud's `412` is `false`. Against a Cloud that doesn't have it yet, the load reports no sequence and the save is unconditional, which is the port's documented default, race included. The adapter gains the guarantee the day Cloud ships it, with no new release. |
| Retries | **Contract §8 exactly**: 4 attempts, backoff from 200 ms with full jitter capped at 5 s, never shorter than `Retry-After`, on network errors, timeouts, `409`, `429`, `500`, `502`, `503`, `504`. Never on `401`, `403`, `413`, `422` or any other `4xx`, and never on `412`. A save is retried at once, before anything else is saved to its thread (the store holds the call until it is done). |
| Errors | A **`StrataCloudException`** (.NET) / **`CloudError`** (Rust) with a kind, the HTTP status and the envelope's `code`, never a body's values. **`403 ENTITLEMENT_EXCEEDED`** is its own type, `StrataCloudEntitlementException` / `CloudError::EntitlementExceeded`, so a host can refuse new executions and let running ones carry on. |
| Round-trip fidelity | **Nothing to normalise on our side.** The engines already write whole-second UTC timestamps; Cloud's JSONB key order and `1.0` → `1` don't change what a state deserializes to. A test saves and loads through a fake that normalises like Cloud, then resumes. |
| The decisions client | **`StrataCloudDecisions`** / **`Decisions`**: one request per call, a page of `CloudDecision`s, each with an engine `HumanSignal` (typed `overrides` mapped when Cloud sends them). The long-poll's read timeout is the wait plus 10 s. It doesn't retry: the worker loop does. |
| The worker | **`DecisionWorker`**, a library loop, not a hosted service: it polls the feed (long-poll), resumes each decided thread through the store, one at a time, and reports every decision it handled. Hosting is SG-28's. |
| Which threads | A **filter** (`Owns`, or a thread-id prefix) for "is this thread mine", since checkpoints carry no graph identity until SG-13, and an optional **single thread id** to wait on. A foreign or unresumable decision is remembered and paged past, never re-read forever. |
| A stale decision | The worker resumes **the checkpoint the decision answered, or nothing**: its sequence is pinned for the resume's conditional stamp, so a decision for an earlier boundary can't approve a later one. |
| Telemetry | A `strata.cloud.request` span per HTTP attempt (route, status, attempt), a `strata.cloud.retries` counter and a `strata.cloud.request.duration` histogram, in an `ActivitySource` / `Meter` named `Datalium.Strata.Cloud` and Rust `tracing` / `metrics`. A decisions poll gets a span only when it returned items or failed, so idle long-polls don't flood traces. No key, no state and no override value in any attribute or message. |
| Due threads | **Not on Cloud.** The SDK surface has no thread listing, so the Cloud store implements no query port, and `WakeDueAsync` / `wake_due` (timers, approval timeouts) can't run against it. A Cloud follow-up (`GET /v1/sdk/threads?due_by=`) would close it additively. *Closed 2026-10-20 by SG-36 (`43-Cloud-Leases-And-Listing.md`): Cloud lists threads, and the store implements the query port and the lease members.* |
| The CLI | **Unchanged.** It reads self-hosted logs; reading Cloud is the console's job. |

---

## Usage

```csharp
using Datalium.Strata.Cloud;

var cloud = new StrataCloudOptions
{
    BaseUrl = new Uri("https://api.strata.datalium.com"),
    ApiKey = configuration["Strata:CloudKey"]!,           // a secret store, never state or metadata
};

// The store: a drop-in ICheckpointStore.
using var store = new StrataCloudCheckpointStore(cloud);
var result = await investigation.RunAsync(incident.ThreadId, initial, store, new RunOptions
{
    Metadata = { ["tenant"] = tenant.Id },
});

// Interrupted at the approval boundary: the save opened an approval in the console.

// The worker, under StrataOps' own BackgroundService.
using var decisions = new StrataCloudDecisions(cloud);
var worker = new DecisionWorker<Investigation>(investigation, store, decisions, new DecisionWorkerOptions<Investigation>
{
    ThreadIdPrefix = "inc-",                                   // or Owns = (decision, ct) => lookup
    RunOptions = decision => new RunOptions { Metadata = { ["approval"] = decision.ApprovalId } },
    OnHandled = (handled, ct) => audit.RecordAsync(handled.Decision.ThreadId, handled.Outcome, ct),
});
await worker.RunAsync(stoppingToken);                          // until the host stops

// A new execution refused by the plan's thread limit:
try { await investigation.RunAsync(threadId, initial, store); }
catch (StrataCloudEntitlementException) { /* refuse new incidents; running ones carry on */ }
```

```rust
use datalium_strata_cloud::{Decisions, DecisionWorker, StrataCloudCheckpointStore, StrataCloudOptions};

let cloud = StrataCloudOptions::new("https://api.strata.datalium.com", api_key);
let store = Arc::new(StrataCloudCheckpointStore::new(cloud.clone())?);
let result = investigation.run(&thread_id, initial, store.clone()).await;

let mut worker = DecisionWorker::new(investigation.clone(), store.clone(), Decisions::new(cloud)?)   // Arc<CompiledGraph<_>>
    .with_thread_id_prefix("inc-")
    .with_run_config(|decision| RunConfig::new().with_metadata("approval", &decision.approval_id))
    .with_on_handled(move |handled| {
        let _ = audit.send((handled.decision.thread_id.clone(), handled.outcome));  // a channel: the callback is sync
    });
worker.run(shutdown_token).await?;

// A new execution refused by the plan's thread limit:
if let GraphExecutionResult::Failed(error) = &result {
    if datalium_strata_cloud::is_entitlement_exceeded(error) { /* refuse new incidents */ }
}
```

---

## The API

### .NET (`Datalium.Strata.Cloud`)

```csharp
namespace Datalium.Strata.Cloud;

/// Where Strata Cloud is and how to talk to it. A class, not a record, so nothing prints the key.
public sealed class StrataCloudOptions
{
    public required Uri BaseUrl { get; init; }                 // the host; routes are /v1/sdk/… under it
    public required string ApiKey { get; init; }               // strata_live_…, sent as Authorization: Bearer
    public TimeSpan ConnectTimeout { get; init; }              // 3 s (only for the HttpClient the store creates)
    public TimeSpan RequestTimeout { get; init; }              // 30 s, per attempt
    public StrataCloudRetryPolicy Retry { get; init; }         // StrataCloudRetryPolicy.Default
    public int GzipThresholdBytes { get; init; }               // 64 KiB: larger request bodies are gzipped
    public TimeProvider TimeProvider { get; init; }            // backoff, timeouts and the worker's waits
}

public sealed record StrataCloudRetryPolicy
{
    public static StrataCloudRetryPolicy Default { get; }      // 4 attempts, 200 ms → 5 s, full jitter, Retry-After ≤ 30 s
    public int MaxAttempts { get; init; }                      // ≥ 1, the first attempt included
    public RetryBackoff Backoff { get; init; }                 // the engine's type (design/25)
    public TimeSpan MaxRetryAfter { get; init; }               // a longer Retry-After fails at once instead of waiting
}

public sealed class StrataCloudCheckpointStore : ICheckpointStore, IDisposable
{
    public StrataCloudCheckpointStore(StrataCloudOptions options, HttpClient? httpClient = null);

    // ICheckpointStore: SaveAsync, LoadAsync, LoadLatestAsync, SaveIfLatestAsync.
    // Save and Load throw NotSupportedException.
}

public sealed class StrataCloudDecisions : IDisposable
{
    public StrataCloudDecisions(StrataCloudOptions options, HttpClient? httpClient = null);

    /// One request: the decisions waiting now, or within `wait` (at most 25 s on Cloud's side).
    public Task<CloudDecisionPage> GetAsync(
        string? threadId = null, int? limit = null, string? cursor = null, TimeSpan? wait = null,
        CancellationToken cancellationToken = default);
}

public sealed record CloudDecisionPage(IReadOnlyList<CloudDecision> Items, string? NextCursor);
public sealed record CloudDecision(
    string ApprovalId, string ThreadId, long Sequence, DateTimeOffset DecidedAt, HumanSignal Signal)
{
    public string? DecidedBy { get; init; }        // SG-38: who decided, as the console verified it
    public string? DecidedByEmail { get; init; }   // the email the IdP vouched for, or null
    public string? Notes { get; init; }            // the reviewer's notes, or null
}

public enum StrataCloudErrorKind { Unauthorized, EntitlementExceeded, Rejected, Unavailable, InvalidResponse }

public class StrataCloudException : Exception
{
    public StrataCloudErrorKind Kind { get; }
    public int? StatusCode { get; }                            // null for a network error or a timeout
    public string? Code { get; }                               // the envelope's code, e.g. STRATA_CLOUD_API_RATE_LIMITED
    public TimeSpan? RetryAfter { get; }
    public int Attempts { get; }
    public bool IsTransient { get; }                           // Unavailable
}

public sealed class StrataCloudEntitlementException : StrataCloudException;

public static class StrataCloudWire                            // SequenceHeader, IfSequenceHeader, the codes
public static class StrataCloudCheckpoints { public static bool AreEquivalent(Checkpoint left, Checkpoint right); }
public static class StrataCloudTelemetry { public const string Name = "Datalium.Strata.Cloud"; public const string RequestSpan = "strata.cloud.request"; }

public sealed class DecisionWorker<TState>
{
    public DecisionWorker(CompiledGraph<TState> graph, ICheckpointStore store, StrataCloudDecisions decisions,
        DecisionWorkerOptions<TState>? options = null);

    /// Polls and resumes until the token is cancelled. Throws for a failure no wait can fix (401, 403, 422).
    public Task RunAsync(CancellationToken cancellationToken);

    /// One round: every page from the start of the feed, each new decision handled. No retries.
    public Task<IReadOnlyList<HandledDecision<TState>>> PollAsync(TimeSpan wait, CancellationToken cancellationToken = default);
}

public sealed record DecisionWorkerOptions<TState>
{
    public string? ThreadId { get; init; }                     // wait on one thread only
    public string? ThreadIdPrefix { get; init; }               // mine = starts with it
    public Func<CloudDecision, CancellationToken, ValueTask<bool>>? Owns { get; init; }   // mine = true (with the prefix, both)
    public Func<CloudDecision, RunOptions>? RunOptions { get; init; }                    // per resume
    public Func<HandledDecision<TState>, CancellationToken, ValueTask>? OnHandled { get; init; }
    public TimeSpan Wait { get; init; }                        // 25 s
    public int? Limit { get; init; }                           // Cloud's default, 25
    public TimeSpan IdleDelay { get; init; }                   // 2 s, after a page of nothing new
    public RetryBackoff ErrorBackoff { get; init; }            // 1 s → 30 s, full jitter
}

public enum DecisionOutcome { Resumed, Failed, AlreadyHandled, NotMine }

public sealed record HandledDecision<TState>(
    CloudDecision Decision, DecisionOutcome Outcome, GraphExecutionResult<TState>? Result);
```

### Rust (`datalium-strata-cloud`)

```rust
#[derive(Clone)]                                   // Debug prints the key as "<redacted>"
pub struct StrataCloudOptions { /* private */ }
impl StrataCloudOptions {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self;
    pub fn with_connect_timeout(self, d: Duration) -> Self;     // 3 s
    pub fn with_request_timeout(self, d: Duration) -> Self;     // 30 s
    pub fn with_retry(self, retry: RetryPolicy) -> Self;
    pub fn with_gzip_threshold(self, bytes: usize) -> Self;     // 64 KiB
    pub fn with_client(self, client: reqwest::Client) -> Self;  // else one is built
    pub fn with_sleep(self, sleep: Sleep) -> Self;              // tokio::time::sleep by default
}
pub type Sleep = Arc<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>;
pub fn tokio_sleep() -> Sleep;

pub struct RetryPolicy { pub max_attempts: u32, pub backoff: Backoff, pub max_retry_after: Duration }  // Default: §8

pub struct StrataCloudCheckpointStore { /* private */ }
impl StrataCloudCheckpointStore {
    pub fn new(options: StrataCloudOptions) -> Result<Self, CloudError>;
    pub async fn save_checkpoint(&self, cp: &Checkpoint, expected: Option<i64>) -> Result<SaveOutcome, CloudError>;
    pub async fn load_checkpoint(&self, thread_id: &str) -> Result<Option<LatestCheckpoint>, CloudError>;
}
impl CheckpointStore for StrataCloudCheckpointStore { /* save, load, load_latest, save_if_latest */ }

pub struct Decisions { /* private */ }
impl Decisions {
    pub fn new(options: StrataCloudOptions) -> Result<Self, CloudError>;
    pub async fn get(&self, query: &DecisionQuery) -> Result<DecisionPage, CloudError>;
}
pub struct DecisionQuery { pub thread_id: Option<String>, pub limit: Option<u32>, pub cursor: Option<String>, pub wait: Duration }
pub struct DecisionPage { pub items: Vec<CloudDecision>, pub next_cursor: Option<String> }
pub struct CloudDecision { pub approval_id: String, pub thread_id: String, pub sequence: i64,
                           pub decided_at: DateTime<Utc>, pub signal: HumanSignal,
                           pub decided_by: Option<String>,        // SG-38
                           pub decided_by_email: Option<String>,
                           pub notes: Option<String> }

#[non_exhaustive]
pub enum CloudError {
    Unauthorized { code: Option<String>, attempts: u32 },
    EntitlementExceeded { code: Option<String>, attempts: u32 },
    Rejected { status: u16, code: Option<String>, attempts: u32 },
    Unavailable { status: Option<u16>, code: Option<String>, retry_after: Option<Duration>, attempts: u32, cause: Option<String> },
    InvalidResponse { status: u16, message: String },
    Config(String),
}
impl CloudError { pub fn is_transient(&self) -> bool; pub fn status(&self) -> Option<u16>; pub fn code(&self) -> Option<&str>;
                  pub fn retry_after(&self) -> Option<Duration>; pub fn attempts(&self) -> u32; }
pub fn is_entitlement_exceeded(error: &StrataError) -> bool;   // through the engine's string store error
pub fn is_unavailable(error: &StrataError) -> bool;            // the same, for Unavailable
pub fn is_unauthorized(error: &StrataError) -> bool;           // the same, for Unauthorized
pub fn equivalent(left: &Checkpoint, right: &Checkpoint) -> bool;
pub mod wire;                                                   // SEQUENCE_HEADER, IF_SEQUENCE_HEADER, the codes

pub struct DecisionWorker<S> { /* private */ }
impl<S> DecisionWorker<S> {
    pub fn new(graph: Arc<CompiledGraph<S>>, store: Arc<dyn CheckpointStore>, decisions: Decisions) -> Self;
    pub fn with_thread_id(self, id) / with_thread_id_prefix(self, prefix) / with_owns(self, f) / with_owns_async(self, f)
        / with_run_config(self, f) / with_on_handled(self, f) / with_wait / with_limit / with_idle_delay / with_error_backoff;
    pub async fn poll(&mut self, wait: Duration) -> Result<Vec<HandledDecision<S>>, CloudError>;
    pub async fn run(&mut self, shutdown: CancellationToken) -> Result<(), CloudError>;
}
pub enum DecisionOutcome { Resumed, Failed, AlreadyHandled, NotMine }
pub struct HandledDecision<S> { pub decision: CloudDecision, pub outcome: DecisionOutcome, pub result: Option<GraphExecutionResult<S>> }   // Debug without the result
```

- **The Rust port's error is a string** (`CheckpointStoreError(String)`), so a store error reaches a run's result as `Failed(ExecutionAborted(message))`. The message carries the envelope's code, and `is_entitlement_exceeded` matches it. The store's own methods (`save_checkpoint`, `load_checkpoint`) return the typed `CloudError` for callers outside the engine. .NET's engine lets a store's exception propagate, so the typed exception reaches the host as is.
- **The clock.** .NET takes a `TimeProvider` (tests use `FakeTimeProvider`), like `RunOptions.TimeProvider` (`design/25`). Rust takes a `Sleep` function for every wait the client and worker make (tests record the delays). Tokio's paused clock would auto-advance past a real socket's latency and fire request timeouts early, so it can't drive tests that talk to a local server.

---

## The wire

### Routes

| Port member | Request | Answers |
|---|---|---|
| `SaveAsync` / `save` | `POST /v1/sdk/checkpoints`, the checkpoint's JSON as the body | `201`, `200` → done |
| `SaveIfLatestAsync` / `save_if_latest` | the same, plus `Strata-If-Sequence: <n>` when a sequence is expected | `201`, `200` → `true`; `412` → `false` |
| `LoadAsync` / `load` | `GET /v1/sdk/checkpoints/latest?thread_id=` | `200` → the checkpoint; `404` → `null` / `None` |
| `LoadLatestAsync` / `load_latest` | the same | `200` → the checkpoint, with the `Strata-Sequence` header's value as its sequence, or none without the header |
| `GetAsync` / `get` | `GET /v1/sdk/decisions?thread_id=&limit=&cursor=&wait=` (each only when set; `wait` in whole seconds, only when above 0) | `200` → a page |

Every request carries `Authorization: Bearer <key>` and `Accept: application/json`; a body carries `Content-Type: application/json`, and `Content-Encoding: gzip` when it is larger than the threshold (64 KiB). Responses are decompressed by the `HttpClient` / `reqwest::Client` the adapter builds; a caller passing its own client chooses.

`200` on a save is Cloud's idempotent retry (the checkpoint is already the thread's latest). Nothing distinguishes it for the engine.

### Status mapping

| Status | Save | Load | Decisions |
|---|---|---|---|
| `200`, `201` | success | the checkpoint | the page |
| `401` | `Unauthorized` | `Unauthorized` | `Unauthorized` |
| `403 …ENTITLEMENT_EXCEEDED` | **`EntitlementExceeded`** | `Rejected` | `Rejected` |
| `404 …THREAD_NOT_FOUND` | `Rejected` | **`null` / `None`** | `Rejected` |
| `409 …SEQUENCE_CONFLICT` | retried | `Rejected` | `Rejected` |
| `412` | `false` (conditional save), not retried | `Rejected` | `Rejected` |
| other `4xx` (`400`, `413`, `422`) | `Rejected` | `Rejected` | `Rejected` |
| `408`, `429`, `500`, `502`, `503`, `504` | retried | retried | `Unavailable` (the worker waits) |
| a network error, a timeout | retried | retried | `Unavailable` |
| retries exhausted | `Unavailable`, with the last status and `Retry-After` | the same | — |
| a `2xx` body that isn't what the contract says | `InvalidResponse` | `InvalidResponse` | `InvalidResponse` |

An error's message names the route, the status and the code, and nothing from the body: `strata cloud: save refused (HTTP 403 STRATA_CLOUD_API_ENTITLEMENT_EXCEEDED): the plan's limit of new threads is reached`.

### Retries

- **Attempts:** 4, the first included.
- **The wait before retry *n*:** `min(200 ms × 2^(n−1), 5 s)`, jittered uniformly in `[0, d]` (the engine's `Jitter::Full`), and **never shorter than `Retry-After`** when the response had one (seconds or an HTTP date). A `Retry-After` above `MaxRetryAfter` (30 s) fails at once with `Unavailable`, carrying it, rather than holding a save that long.
- **Timeouts:** connect 3 s (on the client the adapter builds), each attempt 30 s, the decisions long-poll the wait plus 10 s.
- **A save is retried at once**, inside the same call, so nothing else is saved to its thread in between: the engine saves one checkpoint at a time per thread and awaits each. Cloud answers the retry of a save that did commit with `200` (contract §3), so retrying after an ambiguous failure (a timeout, a reset connection, a `500`, `502` or `504`) never appends twice.
- **`409 SEQUENCE_CONFLICT`** is retried without the reload the contract suggests: the reload informs a caller choosing what to save, and the store saves the same checkpoint either way.
- **A conditional save that was ambiguous, then refused.** If an attempt of a conditional save failed ambiguously and a later one is answered `412`, the first may have committed: the `412` may be our own write. The adapter then loads the latest checkpoint; if it equals the one sent (numbers by value, timestamps as instants) at the expected sequence plus one, the save succeeded and returns `true`. Otherwise `false`.
- **Never the run's token.** The engine never passes one to a store call (`design/19`); the store's timeouts bound it.

### The conditional save (what we assume from Cloud)

Cloud's catch-up (the companion brief to Strata Cloud's API) is asked for:

| | Request | Assumed here |
|---|---|---|
| A sequence on the load | `Strata-Sequence: <n>` on `GET …/latest`'s `200` | Parsed as the sequence; absent → `null` / `None` |
| An expected sequence on the save | `Strata-If-Sequence: <n>` on `POST /v1/sdk/checkpoints` | A mismatch is `412 STRATA_CLOUD_API_PRECONDITION_FAILED`, with nothing stored; an older Cloud ignores the header and appends, which is the unconditional default |
| **The precondition first** | A save with `Strata-If-Sequence` whose expected sequence isn't the latest is `412`, **even when the body is identical to the latest checkpoint** | Required. Two workers that resume the same decision in the same second write byte-identical stamps; if Cloud answered the second with the idempotent `200` before checking the precondition, both would run the remediation. The adapter's own retry of a conditional save that did commit then gets `412`, and the ambiguous-then-`412` check (above) recognises its write |

The header names are the companion brief's proposals; they are constants in each adapter (`StrataCloudWire` / `wire`), changed in one place if Cloud settles on others.

**What the companion session must be told** (through Nick): the precondition-first ordering above. The companion brief doesn't say which check comes first, and the natural implementation (the idempotent comparison, then the new header) reopens the double-resume race for stamps made in the same second.

**Without them** the store keeps the port's defaults in effect: no sequence on load, unconditional saves. That reopens on Cloud the race `design/28` closed: two workers that receive the same decision (contract §5, "several workers can receive the same decision") both stamp `resumed_at` and both run the approved remediation. Until Cloud ships the headers, a deployment must give its workers disjoint threads. The XML docs and rustdoc say so.

### Decisions

- `GetAsync` / `get` sends one request: `thread_id`, `limit` and `cursor` only when given, `wait` in whole seconds when above zero.
- Each item becomes a `CloudDecision`. `signal` becomes the engine's `HumanSignal`: `is_approved`, `reviewer_token`, `field_overrides`, and **`overrides`** (typed values, SG-17) when Cloud sends them. A missing `field_overrides` is empty.
- **Who decided, and the notes** (SG-38, contract §5 since 2026-10-22): the item's `decided_by`, `decided_by_email` and `notes` become `CloudDecision.DecidedBy` / `DecidedByEmail` / `Notes` (Rust `decided_by` / `decided_by_email` / `notes`), as sent, each null (`None`) when Cloud sends `null` or, being older, nothing. `decided_by` is the identity the console verified (an IdP user id, `idp-apikey:<key id>`, `idp-system:<account id>`, `dev-console` for the development token) and `decided_by_email` the email the IdP vouched for (null for anything but a person, compare it case-insensitively). **`HumanSignal.ReviewerToken` is still the reviewer's own choice and is not who decided:** an application that lets only some people approve checks `DecidedBy` or `DecidedByEmail`. The signal, the recorded `HumanDecision` and the worker's behaviour are unchanged; `HandledDecision` carries the whole `CloudDecision`, so `OnHandled` sees the three. Who may decide what stays the application's (design/21).
- No retries: a `429` or a `5xx` is `Unavailable` with its `Retry-After`, and the caller (the worker) waits.

### Normalisation

Cloud stores the checkpoint as JSONB and normalises timestamps (contract §3):

- **Timestamps** come back as `YYYY-MM-DDTHH:MM:SSZ`. Both engines already write that form for `created_at` and `resumed_at`, so nothing changes.
- **Key order** in `frozen_state` and `metadata` isn't kept. Nothing in the engine depends on it.
- **`1.0` comes back as `1`.** A state's `f64` / `double` deserializes from `1` unchanged; a `serde_json::Value` field sees an integer where it wrote a float, which compares unequal with `==` but holds the same number. The adapter doesn't rewrite numbers.

---

## The worker

```
loop until stopped:
    cursor = none
    repeat:                                         # one round: the feed from its start
        page = decisions.get(thread_id?, limit?, cursor, wait if cursor is none else 0)
        for each item, oldest first:
            if seen before: skip
            remember it
            if not mine:              report NotMine
            else resume it (below) and report Resumed, Failed or AlreadyHandled
        cursor = page.next_cursor
    until cursor is none
    if the round had items but nothing new: wait IdleDelay (2 s)
on Unavailable: wait Retry-After (429), else back off 1 s → 30 s; reset after a good page
on anything else (401, 403, 400, 422): stop with the error
```

**Resuming a decision.** `ResumeAsync(threadId, decision.Signal, store, runOptions)` / `resume_with(…)`, through a thin wrapper of the store that **pins the decision's sequence**: the resume's `LoadLatestAsync` returns the latest checkpoint with the decision's sequence as its own, so the resume's conditional stamp (`design/28`) only writes if the thread is still at the checkpoint the reviewer decided. If the thread moved on (another worker resumed it and reached a later boundary), the stamp is refused and nothing runs. Without the Cloud headers the stamp is unconditional (above), so the pin protects nothing.

**Outcomes.**

| Outcome | When | `Result` |
|---|---|---|
| `Resumed` | The resume ran: completed, interrupted again, cancelled | the result |
| `AlreadyHandled` | The resume returned `Failed` because the checkpoint was already resumed, or changed before the stamp ("another resume or end got there first"). Someone else acted on the decision | the result |
| `Failed` | Any other `Failed`: refused before anything was written (an override that doesn't fit the state, not this graph's state), or the resumed run failed | the result |
| `NotMine` | The filter said no. Never resumed | none |

A store exception during the resume (`Unavailable` after its retries) is reported as `Failed`, its message in a `Failed` result, in .NET too.

**Paging past.** Cloud offers a decision until something is saved to its thread (contract §5): a decision the worker can't or mustn't act on stays in the feed. So the worker remembers every approval id it has handled (the last 10,000) and never handles one twice; a page holding only remembered decisions is paged past with the cursor. Cloud issues a cursor only when more items follow, so a short page of remembered decisions would make every long-poll answer at once: after a round with nothing new, the worker waits `IdleDelay` (2 s) before asking again. A new decision behind a stuck one is then seen within 2 s instead of tens of milliseconds. A cursor on the last page (a Cloud follow-up, "Not built") would remove the delay.

**One at a time.** A worker resumes one decision at a time, as `WakeDueAsync` does. Callers scale with several workers on disjoint threads (a prefix each, or one thread each).

**Stopping.** The token stops the long-poll at once. A resume in progress is not cancelled: stopping mid-remediation would leave a `cancelled` thread nobody's decision resumes. In .NET the resume runs with no token; in Rust the worker adds none to the caller's `RunConfig`. Bound a resume with `RunOptions.Deadline` / `RunConfig::with_deadline`.

**Nested waits.** A decision carries one `HumanSignal`, which applies to the innermost level (contract §10); `LevelSignals` aren't sent ("Not built").

---

## Telemetry

| | .NET | Rust |
|---|---|---|
| Source | `ActivitySource` and `Meter` named `Datalium.Strata.Cloud` (`StrataCloudTelemetry.Name`) | `tracing` target `datalium_strata_cloud`, `metrics` facade |
| Span per attempt | `strata.cloud.request`, child of the current activity (the engine's run, node or attempt span) | `strata.cloud.request`, child of the current span |
| Attributes | `strata.cloud.route` (`checkpoints.save`, `checkpoints.load`, `decisions`), `http.request.method`, `http.response.status_code`, `strata.cloud.attempt`, `strata.thread.id` (save and load), `error.type` (the status, `timeout` or `network`) | the same fields |
| Decisions polls | one span per poll that returned items (`strata.cloud.items`) or failed, started when the request was (backdated) | the same, created when the answer arrives, with `strata.cloud.duration_ms` |
| `strata.cloud.retries` | counter, `route` and `reason` (`status_503`, `timeout`, `network`, …) | the same |
| `strata.cloud.request.duration` | histogram in seconds, `route` and `status` | the same |

No attribute, log or exception message carries the key, a state, metadata values or an override value. Thread ids are recorded, as the engine's spans already do.

---

## Tests

Shared fixtures in **`schema/fixtures/cloud/`**: one recorded exchange per case (the request the adapter must send, the response the fake gives, and what the call must return), read by **both** engines' tests against a fake server. That's the parity check. .NET's fake is an `HttpMessageHandler`; Rust's is a local plain-HTTP `hyper` server, so `reqwest`'s real request path (headers, gzip, a dropped connection, a timeout) is exercised.

| Fixture | Pins |
|---|---|
| `save-201`, `save-200-identical` | a save, and the idempotent answer |
| `save-gzip` | a body over the threshold is gzipped |
| `save-401`, `save-403-entitlement`, `save-413`, `save-422-unsupported-version` | one attempt, the error kind, status and code |
| `save-409-then-201`, `save-429-then-201`, `save-503-then-201` | retried; the wait honours `Retry-After` |
| `save-503-exhausted` | four attempts, then `Unavailable` |
| `save-lost-then-200` | a lost response, retried; Cloud's `200` |
| `save-conditional-201`, `save-conditional-412` | `Strata-If-Sequence`; `412` is `false`, not retried |
| `save-conditional-lost-then-412-ours` | the ambiguous-then-`412` check finds our own write |
| `load-200`, `load-200-no-sequence`, `load-200-normalized`, `load-404` | the sequence header, its absence, Cloud's normalised form, `null` |
| `decisions-page`, `decisions-cursor`, `decisions-empty-long-poll`, `decisions-429` | paging, typed overrides, who decided and the notes (all three, the development token's, and an item without them), the empty long-poll, `Retry-After` |

Mirrored tests in `rust/strata-cloud/tests/` and `dotnet/tests/Datalium.Strata.Tests/Cloud/`, test for test:

- **Every fixture**, through the fake: the request matches (method, path, query, headers, body), the result matches.
- **Retry and backoff on a fake clock:** the recorded waits are at least each `Retry-After`, and computed waits stay under the cap; no retry on `401`, `403`, `413`, `422`, `412`.
- **Timeouts:** an attempt that hangs is retried after the request timeout.
- **The key** is on every request, and in no span attribute, exception message or `ToString` / `Debug`.
- **Round-trip fidelity:** a run interrupted on the fake Cloud (which normalises like Cloud), then resumed from it, ends in the same state as on the in-memory store.
- **The conditional save:** with the headers, a stale sequence is refused and nothing runs; without them, the save is unconditional.
- **The decisions client:** paging, the cursor, an empty long-poll, `429` with `Retry-After`, typed overrides.
- **The worker**, against the fake feed and the in-memory store:
  - resumes a decided thread and reports `Resumed`
  - reports `AlreadyHandled` for a thread already resumed
  - pages past a foreign thread (`NotMine`) and a refused one (`Failed`), handling each once
  - a stale decision (the thread moved on) runs nothing
  - **two workers, one decision:** one run with the conditional save, two without it (a store with only the port's defaults). The second is the documented race.
  - backs off on `Unavailable`, waits `Retry-After` on `429`, stops on `401`
- **Live tests**, opt-in: `STRATA_CLOUD_URL`, `STRATA_CLOUD_KEY` and, for the decision, `STRATA_CLOUD_CONSOLE_TOKEN` (the local dev token). A save, a load, an interrupt, a decision through the console API, and the worker resuming it. Skipped without the variables, and not in default CI. They need Cloud's catch-up (schema `1.3.0` and later).

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

### The questions as asked (the handoff brief)

1. **Packaging.** **A new project and a new crate**, so HTTP and TLS reach only the hosts that use Cloud.
2. **Sync members in .NET.** **`NotSupportedException`.** The engine never calls them, and sync-over-async on HTTP invites deadlocks.
3. **`LoadLatestAsync` / `SaveIfLatestAsync`.** **Detected at runtime**: the response header decides, and an older Cloud ignores the request header. No configuration switch, no release when Cloud ships it.
4. **The worker: library loop or hosted service?** **A library loop** (`RunAsync` / `run`), plus `PollAsync` / `poll` for hosts that drive it themselves. Hosting is SG-28.
5. **Telemetry for idle polls.** **A span only for a poll that returned items or failed.**
6. **The CLI against Cloud.** **No.**

### Added by this document

7. **One thread id, not a set.** Cloud's long-poll takes one `thread_id`. A worker for several known threads would need one held request per thread, against the account's 100. A host waiting on a handful of threads runs a worker each; anything more uses the account feed with a filter.
8. **The stale-decision pin.** **Pin the decision's sequence for the resume's stamp.** Otherwise a decision read from a page, then acted on after another worker had moved the thread to a later boundary, would answer a boundary the reviewer never saw.
9. **Classifying "someone else got there".** By the engine's two refusal messages for the thread (`… was already resumed`, `… changed before it could be resumed: …`), matched exactly with the thread id. Both are pinned by the worker tests in each engine, so a change to either fails them.
10. **`409` without the reload.** See "Retries".
11. **A `Retry-After` beyond 30 s fails the call.** A save held for minutes is a run stalled for minutes; the host sees `Unavailable` with the hint and decides.
12. **Rust's clock seam is a sleep function, not tokio's paused clock**, see "The API".
13. **The key's type.** A plain string in an options type that never prints it (.NET: a class, not a record; Rust: a redacting `Debug`), and a sensitive header value in `reqwest`.

14. **Which check Cloud makes first.** **The precondition, then the idempotent comparison** (see "The conditional save"). Found while writing the two-workers test: the emulated Cloud answered the second worker's identical stamp `200`, and both ran.
15. **A transient store failure during a resume** fails the worker's round instead of being reported `Failed`: the decision isn't remembered, so the next round (after the backoff) tries it again. The store's problem, not the decision's. A `401` stops the worker the same way.
---

## Implementation Status

Built 2026-10-09 in both engines, in one change, as specified above, against fakes. The review questions are still open: the build follows each recommendation. **The live tests haven't run**: they wait for Cloud's catch-up (the companion brief), since today's Cloud accepts only schema `1.0.0` and `1.1.0` and has no sequence headers.

**Where it diverged, or filled a gap, and why:**

- **Cloud's check order is a requirement, not a detail** (review decision 14). The companion brief must be amended through Nick: a conditional save's precondition is checked before the identical-to-latest `200`.
- **Rust's `CloudError` variants carry `attempts`**, so every error says how many attempts the call made, as .NET's `StrataCloudException.Attempts` does; `Unavailable.cause` is optional (only a network error or a timeout has one).
- **Rust has `is_unavailable` and `is_unauthorized`** beside `is_entitlement_exceeded`: the worker needs them to tell a store failure from a refused resume through the engine's string error. Each matches the fixed start of the `CloudError` message (`strata cloud: unavailable after …`), never a thread id.
- **A public equivalence check**, `StrataCloudCheckpoints.AreEquivalent` / `equivalent`: the ambiguous-then-`412` check needs it, and so does anyone comparing Cloud's copy with their own.
- **A `404` that isn't `THREAD_NOT_FOUND` is an error**, not an empty thread, so a wrong base URL (a proxy's 404 page) can't make every resume say "no checkpoint".
- **The Rust worker's callback is synchronous** (`Fn(&HandledDecision<S>)`; send to a channel for anything that awaits), where .NET's is async: `HandledDecision<S>` borrows the run's result, and a `'static` future can't hold the borrow. Both `poll` methods also return the handled list.
- **A refused resume's message differs by engine** (Rust says `invalid type: string "lots", expected i64`; .NET names the field), so the worker tests assert the `Failed` outcome, not the text.

**How it was built:**

- **.NET** (`dotnet/src/Datalium.Strata.Cloud/`): `StrataCloudOptions` (and `StrataCloudRetryPolicy`), `StrataCloudException` / `StrataCloudEntitlementException`, `StrataCloudWire`, `StrataCloudTelemetry`, `CloudTransport` (internal: retries, gzip, attempt timeouts on the `TimeProvider`, spans, metrics, the error mapping), `StrataCloudCheckpointStore`, `StrataCloudCheckpoints`, `StrataCloudDecisions`, `DecisionWorker<TState>` with its internal `PinnedStore`. Added to the solution; the test project references it.
- **Rust** (`rust/strata-cloud/`, `datalium-strata-cloud`, a workspace member): `options` (`StrataCloudOptions`, `RetryPolicy`, `Sleep`), `error`, `wire`, `transport` (`reqwest` with `rustls-tls` and `gzip`, request gzip with `flate2`, percent-encoded queries so a space is `%20` as in .NET), `store`, `equivalence`, `decisions`, `worker`.
- **Shared fixtures:** `schema/fixtures/cloud/` (23 cases, listed under "Tests"), generated once and checked in.
- **No engine change.** Nothing in `strata-core`, `Datalium.Strata` or the storage packages changed.

**Tests:**

- **.NET** `Cloud/`: `CloudFixtureTests` (the 23 fixtures and a count), `CloudStoreTests` (11), `DecisionWorkerTests` (9), `CloudLiveTests` (2, skipped without the variables), all in one xUnit collection since the span and meter listeners are global. The fakes are `ScriptedCloud`, `EmulatedCloud` (normalises like Cloud, the decision feed, the conditional save, a hook that holds a load's answer) and `RecordingTimeProvider` (records each wait under 31 s and fires it at once).
- **Rust** `strata-cloud/tests/`: `fixtures.rs` (the 23 fixtures and a count), `store.rs` (8), `telemetry.rs` (1, its own binary for the global recorders), `worker.rs` (9), `live.rs` (2, returning early without the variables). The fakes in `common/` are the same three, on a local `hyper` server.
- **Totals when this landed:** .NET 632 passed and 49 skipped (Postgres, Redis and the Cloud live tests, none of which ran on this machine), plus the example's 1. `rust/` 601, with the Postgres and Redis tests skipping. `cargo fmt`, `clippy -D warnings` and `dotnet format --verify-no-changes` are clean; the CLI and `examples/rust-agent` are untouched.
- **Mutation checks, in both engines:**
  - Not pinning the decision's sequence fails the stale-decision test in each.
  - Dropping the ambiguous-then-`412` check fails the `save-conditional-lost-then-412-ours` fixture in each.

**SG-38 (2026-10-22): who decided.** For StrataOps, which relays decisions from `GetAsync` to an API that checks the decider's roles by the email the IdP vouched for, and so couldn't trust `ReviewerToken`. `CloudDecision` gained `DecidedBy`, `DecidedByEmail` and `Notes` (.NET init-only properties, so the positional constructor is unchanged; Rust three `Option<String>` fields, so a struct literal of it needs them). `decisions-page` now has an item with all three and one without, `decisions-cursor` the development token's (`dev-console`, no email, no notes); both engines' emulated Clouds send them, and the worker tests assert them on `HandledDecision`. The live decision test sends notes and asserts `decided_by` is `dev-console`. Nothing else changed.

## Not built (deliberately out of scope)

- ~~**Who decided.**~~ Built 2026-10-22 (SG-38, see "Decisions" and "Implementation Status"). What was missing: a decision carried only `signal.reviewer_token`, which the reviewer may set, so an application couldn't tell who decided.

- ~~**The query port on Cloud.**~~ Built 2026-10-20 (`43-Cloud-Leases-And-Listing.md`, SG-36): `GET /v1/sdk/threads` and `GET /v1/sdk/checkpoints`, every `ThreadQuery` filter, so the wake and recovery drivers run on Cloud. What this said: the SDK surface has no thread listing, so the Cloud store implements no `ICheckpointQuery` / `CheckpointQuery`. **SG-16's `WakeDueAsync` / `wake_due` (timers, approval timeouts) can't run against a Cloud store.** A Cloud follow-up, an SDK `GET /v1/sdk/threads?due_by=`, would close it additively; the adapter would then implement the port's `ListThreads` for `DueBy` alone.
- **A cursor on the last page of decisions** (or an `after=` parameter), so a worker can long-poll past decisions it has set aside without the `IdleDelay`. A Cloud follow-up.
- **`LevelSignals`** (per-level answers; contract §10 and Cloud's DB-5), webhooks and push delivery.
- **`ETag` revalidation** on the load (`If-None-Match`, `304`). The engine loads a thread once per resume, so a cache would rarely hit.
- ~~**Leases on Cloud.**~~ Built 2026-10-20 (`43-Cloud-Leases-And-Listing.md`, SG-36): Cloud's lease routes and fenced save (`423 LEASE_LOST`), so a leased run against the Cloud store is no longer refused. What this said: thread leases (SG-12) are built in the engine and every other store (`design/32-Thread-Leases.md`), but the SDK contract has no lease route and no fenced save, so the Cloud store has none: a run with `RunOptions.Lease` / `RunConfig::with_lease` against it is refused (.NET `NotSupportedException`, Rust `Failed`). A Cloud follow-up: lease routes, and a save that carries the token.
- **The CLI against Cloud.** ~~Hosting (SG-28).~~ Built 2026-10-10 (`35-Hosting-Integration.md`): `AddCloudCheckpoints()` and `AddDecisionWorker<TState>()` in .NET; in Rust the worker's `run` goes in `Host::task`. Graph identity (SG-13) is built (`design/31-Graph-Identity.md`): the worker loads a decision's thread first and reports another graph's as `NotMine`, so `Owns` is no longer the default guard. Cloud must accept `schema_version` `1.6.0` for a graph with an identity to save to it.
- **Durable execution on Cloud** (`design/33-Durable-Execution.md`). *Since SG-36 (`43-Cloud-Leases-And-Listing.md`) Cloud accepts `1.7.0`, lists running threads and has leases, so `RecoverStaleAsync` runs against it; metering per-node checkpoints is still Cloud's decision (its Q19).* A graph compiled with `CheckpointPolicy.EveryNode` saves a `running` checkpoint per node, at `schema_version` `1.7.0`, which Cloud must accept, and every one of them is metered. `RecoverAsync` / `recover` works against the Cloud store (it needs only the store port), but `RecoverStaleAsync` / `recover_stale` doesn't (no query port), and with no leases there nothing tells a live run from a dead one. Cloud follow-ups: accept `1.7.0`, list running threads, lease routes, and a decision on metering per-node checkpoints.
- **Client-side validation** of what Cloud refuses (`thread_id` over 1024 bytes, NUL, lone surrogates): Cloud's `422` says it, and the adapter surfaces it as `Rejected`.
