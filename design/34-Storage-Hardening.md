# Datalium Strata: Storage Hardening

The Postgres and Redis stores work when the network does. They have four weaknesses that a production deployment, and StrataOps' chaos suite, will find:

- **.NET still blocks on the database when a store is created.** `PostgresCheckpointStore`'s constructor creates and migrates its tables with synchronous Npgsql calls, `PostgresHistoryStore`'s does the same, and `RedisCheckpointStore`'s checks and builds its index synchronously.
- **One dropped connection fails a run.** No store retries. A Postgres failover, an admin shutdown, a deadlock or a Redis reconnect surfaces as an exception (or, in Rust, a `Failed` run) on whichever save hit it. Rust's Postgres and Redis stores hold one connection and never replace it: once it drops, every later call fails.
- **Schema changes are ad hoc.** Each store "adds a column when an older log is opened", by looking for it. Nothing records what a database has, so nothing can refuse a database written by a newer build, and every new column adds another probe.
- **Nothing is ever deleted.** A finished thread's log stays forever. With a checkpoint per node (`design/33-Durable-Execution.md`) that is a row per node per run.

This document closes the gap in the engine: **SG-30**. The SG numbers refer to the gap log of StrataOps (not public yet). It is the last of the four durability gaps StrataOps' M5 depends on (SG-13, SG-12, SG-10, SG-30). StrataOps' ADR 0003 calls it ".NET storage hardening"; the Rust stores have the same weaknesses, so it lands in both engines at one commit (strict parity).

It was written as a blueprint before implementation. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Async everywhere (.NET) | **No store's constructor touches the network.** The Postgres and Redis stores prepare themselves on first use, asynchronously, or when asked: `MigrateAsync` (Postgres), `OpenAsync` / `ConnectAsync` factories. |
| Transient faults | **A retry policy inside the Postgres and Redis stores**, in both engines: 4 attempts, 50 ms doubling to 2 s, full jitter. `StoreRetryPolicy.None` turns it off. |
| What is transient | **Postgres:** a broken or refused connection (SQLSTATE class `08`), `57P01`-`57P03` (shutdown, starting up), `40001` (serialization), `40P01` (deadlock), `53300` (too many connections), and I/O errors and timeouts under them. **Redis:** connection loss, timeouts, `LOADING`, `BUSY`, `TRYAGAIN`. Nothing else. |
| A save that may have happened | **Checked, not blindly repeated.** A save whose commit (or Redis script) failed ambiguously is looked up: if the checkpoint is in the log where it would be, it was saved; if not, it is tried again. A conditional or fenced save keeps its answer: saved, lost the race, or lease lost. |
| The run's cancellation token | **Never reaches a retry.** The engine passes no token to its stores (`design/19`); a caller's own token on a store call stops the waiting between attempts. |
| Rust connections | **Reconnect.** The Postgres store reconnects when its connection has dropped (a store built with `connect`; one built from a caller's client can't). The Redis stores use a connection that reconnects by itself. |
| Migrations | **A `strata_schema_version` table** in each SQL database: one row per component (`checkpoints`, `history`) with the version it is at. Version 1 is today's layout, reached from any older one. Later changes are ordered steps. A database newer than the build is refused. |
| When migrations run | **When a store opens** (the default), or only by `MigrateAsync` / `migrate` (`AutoMigrate = false`): a store then refuses a database that is behind instead of changing it. |
| Retention | **`PurgeAsync(olderThan, states)`**, on a new `ICheckpointRetention` / `CheckpointRetention` port every built-in store implements: deletes the whole log of each thread whose latest checkpoint is in one of `states` (by default `completed` and `ended`) and older than `olderThan`. Never automatic. A thread a live lease holds is kept. |
| Redis TTL | **`FinishedThreadTtl`**, optional and off by default: a thread's keys expire that long after it completes or ends, and stop expiring if it runs again. |
| Telemetry | **`strata.store.retries`** (by store and operation), and a warning per retry. |
| Wire format | **No checkpoint change.** New: the `strata_schema_version` table; the Redis save scripts set or clear an expiry. |
| Strata Cloud | **Unchanged.** It has its own retry policy (`design/30`), and retention is Cloud's own. |

---

## Usage

```csharp
// Created without touching the network; prepared on first use, or here:
await using var store = await PostgresCheckpointStore.OpenAsync(connectionString);

// Or with options:
var store = new PostgresCheckpointStore(dataSource, new PostgresStoreOptions
{
    Retry = new StoreRetryPolicy { MaxAttempts = 6, MaxDelay = TimeSpan.FromSeconds(5) },
    AutoMigrate = false,          // refuse a database that is behind; a deploy step runs MigrateAsync
});
await store.MigrateAsync();

// On a schedule the application owns:
var purged = await store.PurgeAsync(olderThan: DateTimeOffset.UtcNow.AddDays(-30));

// Redis: finished threads expire by themselves.
var redis = await RedisCheckpointStore.ConnectAsync(url, new RedisStoreOptions { FinishedThreadTtl = TimeSpan.FromDays(30) });
```

```rust
let store = PostgresCheckpointStore::connect_with(
    url,
    PostgresStoreOptions::new().with_retry(StoreRetry::new().with_max_attempts(6)),
)
.await?;

let purged = store
    .purge(Utc::now() - chrono::Duration::days(30), &PurgeStates::finished(), MAX_PAGE_SIZE)
    .await?;

let redis = RedisCheckpointStore::connect_with(
    url,
    RedisStoreOptions::new().with_finished_thread_ttl(Duration::from_secs(30 * 24 * 3600)),
)
.await?;
```

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Interruption;

/// Deleting finished threads' logs. Separate from ICheckpointStore, like ICheckpointQuery.
public interface ICheckpointRetention
{
    /// Deletes the log of every thread whose latest checkpoint is in one of `states` (null: completed and
    /// ended) and was written before `olderThan`, up to `limit` threads. Returns how many threads it purged.
    ValueTask<int> PurgeAsync(
        DateTimeOffset olderThan,
        IReadOnlySet<CheckpointThreadState>? states = null,
        int limit = ThreadQuery.MaxLimit,
        CancellationToken cancellationToken = default);
}

namespace Datalium.Strata.Storage;

public sealed record StoreRetryPolicy
{
    public int MaxAttempts { get; init; } = 4;                              // 1: no retries
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromMilliseconds(50);
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(2);
    public static StoreRetryPolicy None { get; }
}

namespace Datalium.Strata.Storage.Postgres;

public sealed record PostgresStoreOptions
{
    public StoreRetryPolicy Retry { get; init; } = new();
    public bool AutoMigrate { get; init; } = true;
    public TimeProvider? TimeProvider { get; init; }      // the waits between attempts
}

public sealed class PostgresCheckpointStore : ICheckpointStore, ICheckpointQuery, ICheckpointRetention, IDisposable, IAsyncDisposable
{
    public PostgresCheckpointStore(string connectionString, PostgresStoreOptions? options = null);
    public PostgresCheckpointStore(NpgsqlDataSource dataSource, PostgresStoreOptions? options = null, bool ownsDataSource = false);
    public static Task<PostgresCheckpointStore> OpenAsync(string connectionString, PostgresStoreOptions? options = null, CancellationToken ct = default);
    public Task MigrateAsync(CancellationToken ct = default);
}
// PostgresHistoryStore: the same constructors, OpenAsync and MigrateAsync.

namespace Datalium.Strata.Storage.Redis;

public sealed record RedisStoreOptions
{
    public string Prefix { get; init; } = RedisConnection.DefaultPrefix;
    public StoreRetryPolicy Retry { get; init; } = new();
    public TimeSpan? FinishedThreadTtl { get; init; }
    public TimeProvider? TimeProvider { get; init; }
}

public sealed class RedisCheckpointStore : ICheckpointStore, ICheckpointQuery, ICheckpointRetention
{
    public RedisCheckpointStore(IDatabase database, RedisStoreOptions options);
    public static Task<RedisCheckpointStore> ConnectAsync(string url, RedisStoreOptions? options = null);
}

// EfCoreCheckpointStore and InMemoryCheckpointStore implement ICheckpointRetention too.
// StoreSchemaVersions: the component names and the version this build knows.
```

### Rust

```rust
// datalium_strata::store
pub trait CheckpointRetention: Send + Sync {
    fn purge<'a>(&'a self, older_than: DateTime<Utc>, states: &'a PurgeStates, limit: usize)
        -> BoxFuture<'a, Result<usize, CheckpointStoreError>>;
}
pub struct PurgeStates(/* a set of ThreadState */);
impl PurgeStates { pub fn finished() -> Self; pub fn of(states: impl IntoIterator<Item = ThreadState>) -> Self; }

// datalium_strata_memory
pub struct StoreRetry { /* max_attempts, initial_delay, max_delay */ }
impl StoreRetry { pub fn new() -> Self; pub fn none() -> Self; pub fn with_max_attempts(self, n: u32) -> Self;
                  pub fn with_initial_delay(self, d: Duration) -> Self; pub fn with_max_delay(self, d: Duration) -> Self; }

pub struct PostgresStoreOptions { /* retry, auto_migrate */ }   // new(), with_retry, with_auto_migrate
pub struct RedisStoreOptions { /* prefix, retry, finished_thread_ttl */ }   // new(), with_prefix, with_retry, with_finished_thread_ttl

impl PostgresCheckpointStore {
    pub async fn connect(url: &str) -> Result<Self, CheckpointStoreError>;                    // as before, with the defaults
    pub async fn connect_with(url: &str, options: PostgresStoreOptions) -> Result<Self, CheckpointStoreError>;
    pub async fn from_client(client: Client) -> Result<Self, CheckpointStoreError>;           // as before: can't reconnect
    pub async fn from_client_with(client: Client, options: PostgresStoreOptions) -> Result<Self, CheckpointStoreError>;
    pub async fn migrate(&self) -> Result<(), CheckpointStoreError>;
}
// PostgresHistoryStore: the same five.
impl RedisCheckpointStore {
    pub async fn connect_with(url: &str, options: RedisStoreOptions) -> Result<Self, CheckpointStoreError>;
}
// RedisHistoryStore: connect_with too.
// Every checkpoint store implements CheckpointRetention; SqliteCheckpointStore gains the version table.
// datalium_strata_memory::schema_versions: the component names and the version this build knows.
```

---

## Semantics

### Retries

Every call a Postgres or Redis store makes to its server runs under the store's policy. An attempt that fails for a **transient** reason is made again after a wait: `InitialDelay × 2^(n−1)`, capped at `MaxDelay`, jittered over `[0, d]`. After `MaxAttempts` the last failure is the call's failure, as before. Any other failure is the call's failure at once.

What an attempt repeats depends on whether doing it twice is doing it once:

| Call | On a transient failure |
|---|---|
| Loads, listings, the log, reading a lease | Repeated. |
| Renewing and releasing a lease | Repeated: both are the same whether done once or twice. |
| Acquiring a lease | Repeated only when the failure came before the commit. After it, the failure is the call's: the lease, if it was taken, runs out by itself. |
| A save (plain, conditional, under a lease) | See below. |
| Creating and migrating tables | Repeated: every step can be run again. |

**A save that may have happened.** A save's attempt can fail before its transaction commits (nothing was written: repeat it), or while it commits, when the connection drops between the store sending the commit and hearing the answer. In Redis, where a save is one script, any transient failure is of the second kind. The store then **looks**:

- A **conditional** save (expected sequence `n`) reads the thread's entry at `n + 1`. If it is this checkpoint, the save happened: `true`. If it is another, somebody else won: `false`. If there is none, nothing was written: the save is tried again.
- A **plain** save reads the thread's latest checkpoint. If it is this checkpoint, the save happened. Otherwise it is tried again.
- A save **under a lease** does the same, and a repeat that finds the lease lost says so, as always.

"Is this checkpoint" compares the wire JSON, keys in any order. So an ambiguous failure never turns into a second copy of a checkpoint, and never into a lost answer.

A plain save repeated after an ambiguous failure can, in one case, append a checkpoint twice: when another writer appended to the same thread in between, so the latest is no longer this one. Two writers on one thread without conditional saves or leases was already a race; the engine's own writers use both.

The retry is inside the store call. A run sees a slower save, not a failed one. The engine gives its stores no token, so a cancelled run still waits for the save it is in; a caller that passes its own token to a store call stops the wait between attempts.

### Reconnecting (Rust)

- **Postgres.** A store built with `connect` / `connect_with` knows its URL. When a call finds the connection closed, the store connects again (a new client and connection task) and the attempt is made on the new one, under the same policy. A store built with `from_client` has only the caller's client: a dropped connection fails every later call, as before, and the docs say so.
- **Redis.** The stores hold a connection that reconnects by itself (`redis`'s connection manager), and the retry policy covers the calls made while it does.

.NET needs neither: Npgsql's pool and StackExchange.Redis's multiplexer replace broken connections. What it lacked was the retry.

### Migrations

Each SQL database has a table that says what it holds:

```sql
CREATE TABLE IF NOT EXISTS strata_schema_version (
    component TEXT NOT NULL PRIMARY KEY,   -- 'checkpoints' | 'history'
    version   BIGINT NOT NULL               -- INTEGER in SQLite
)
```

A store brings its component up to date like this, under the lock it already takes for DDL (Postgres's advisory lock, SQLite's immediate transaction):

1. Create the version table if it isn't there, and read the component's version (no row: 0).
2. **Newer than this build knows: refuse**, changing nothing: `database is at checkpoints schema version 3, and this build knows 1: upgrade the runtime`.
3. At 0: run the **baseline**, which is what stores did before this document: create the tables, or add whatever an older layout lacks. It is safe on any database, old or current. Record version 1.
4. Run each later step in order, recording its version as it commits.

Version 1 is today's layout, so every existing database, at whatever age, reaches it through the baseline the first time a current store opens it. From here on a schema change is a numbered step, written once in each engine, not another probe.

With `AutoMigrate` off, a store does steps 1 and 2 only, and refuses a database that is behind (`database is at checkpoints schema version 0, and this build needs 1: run its migration`). `MigrateAsync` / `migrate` does all four whenever it is called.

The versions are shared: both engines and the CLI read and write the same rows. `schema/fixtures/checkpoint-log/schema-versions.json` pins the table and each component's current version.

### Retention

`PurgeAsync` deletes threads, whole: every checkpoint of the thread, its row in the head table, and its lease row. It picks threads by their **latest** checkpoint: in one of `states`, and written before `olderThan` (at second precision, exclusive, like `ThreadQuery.UpdatedBefore`), oldest first, up to `limit`.

- **Default states: `completed` and `ended`.** Those threads will not run again. `failed` and `cancelled` can be named (an abandoned failure is still resumable until it is purged). `interrupted`, `resumed` and `running` can be named too, and the caller then owns the consequence: a waiting approval is gone.
- **A thread a live lease holds is skipped.**
- **A thread that changes while it is being purged is kept.** Each thread is deleted only if its head is still the one that was picked (Postgres: under the thread's advisory lock; Redis: in one script; SQLite and the in-memory store: under their own lock).
- It returns the number of threads purged. Call it again while it returns `limit`.

Nothing calls it. An application decides its retention and runs it on its own schedule.

**Redis `FinishedThreadTtl`.** When set, a save that leaves a thread `completed` or `ended` gives the thread's list that time to live, and a save that leaves it in any other state clears it (a thread id run again doesn't expire). The save scripts do it, so it is atomic with the save. An expired thread's entries in the index sets go when a listing next finds them empty, or with `rebuild_index`. Off by default.

History stores are not purged here: a session's messages are the application's data. `Not built`.

---

## Telemetry

| | |
|---|---|
| **`strata.store.retries`** | Counter: attempts repeated after a transient failure, by `strata.store` (`postgres`, `redis`) and `strata.store.operation` (`save`, `load`, `list`, `lease`, `migrate`, `purge`). .NET meter `Datalium.Strata.Storage`; Rust the `metrics` facade. |
| A warning per retry | .NET `ActivityEvent` `store_retry` on the current activity, Rust `tracing::warn!`: the operation, the attempt and the error's class, never the connection string. |

---

## Wire format

- **Checkpoints, definitions:** unchanged.
- **SQL:** the `strata_schema_version` table, created on open. The checkpoint and history tables are what they were.
- **Redis:** the three save scripts take one more argument, the finished-thread ttl in milliseconds (0 or absent: none). A save that leaves the thread `completed` or `ended` gives its list that long to live (and, under a lease, its token counter); a save that leaves it in any other state clears the expiry, whatever the ttl. A writer from before this document keeps working, and neither sets nor clears an expiry, so every writer to a prefix with a ttl should be current. The index script takes a thread whose list is gone out of every set. A new script, `PURGE_SCRIPT` / `PurgeScript`. No new keys.
- **Fixtures:** `checkpoint-log/schema-versions.json`, `postgres-schema-versions-columns.json`, `schema-versions-columns.json`.

---

## Migration

**.NET** (breaking, in ways a compiler finds):

- `PostgresCheckpointStore` and `PostgresHistoryStore` no longer create their tables in the constructor. Code that constructed one and then read the tables with its own SQL calls `MigrateAsync` (or uses `OpenAsync`) first. Code that only uses the store needs nothing.
- `RedisCheckpointStore`'s constructor no longer builds the index; the first call does. The `(database, prefix)` constructor and `Connect` remain.
- The stores gain an optional options argument; `ownsDataSource` moves after it.
- `ICheckpointRetention` is new, on every built-in store.

**Rust:** `connect` / `connect_with_prefix` keep their signatures. `CheckpointRetention` and `PurgeStates` are new. The Redis stores' connection type changed (not public).

**Databases:** the version table appears the first time a current store opens a database. An older runtime ignores it. A runtime older than this document opening a database a later one has moved past version 1 would not know to refuse; from this document on, one does.

**StrataOps:** its chaos suite drops Postgres connections and restarts the broker; the stores now ride through both. Its retention job calls `PurgeAsync`. Nothing in its `execution` schema depended on this gap.

---

## Tests

Mirrored in both engines:

- **Retries, in the abstract** (`StoreRetryTests` / `retry.rs`): the delays, the cap, the attempt count, a non-transient failure not repeated, `None`, the caller's token stopping a wait, the classification of each SQLSTATE and Redis error.
- **A dropped Postgres connection:** every backend of the store's database is terminated (`pg_terminate_backend`) between two saves of a durable run; the run completes and its log is whole. The same for a load and a listing.
- **A dropped Redis connection:** the store's clients are killed (`CLIENT KILL`) between two saves; the run completes.
- **An ambiguous save:** a fault injected after the commit (and after the script ran): a plain save isn't duplicated; a conditional save answers `true`; one that lost its race answers `false`; a fenced one keeps its lease.
- **A failure before the commit** is repeated and writes once.
- **Rust reconnects:** a store built with `connect` works again after its connection is terminated; one built with `from_client` reports the error.
- **Migrations:** a fresh database is at version 1 with the fixture's table; every older layout the stores could upgrade before (no `nested`, no `decision`, no head table, no `wait`, no `graph`, no leases) ends at version 1 with its data readable; a database at a newer version is refused and unchanged; `AutoMigrate = false` refuses a database that is behind and accepts a current one; later steps run once, in order (with steps of the test's own); two stores opening one empty database at once both succeed.
- **Purge, in every adapter:** old `completed` and `ended` threads go, with their head and lease rows; a newer one, a `failed` one, an `interrupted` one and one a live lease holds stay; named states are honoured; `limit`; the count; a thread that changed while it was being purged stays.
- **Redis ttl:** a completed thread's list has the ttl, a thread run again has none, and a listing drops an expired thread from the index.
- **.NET construction:** constructing a Postgres or Redis store against an unreachable server doesn't throw or block; the first call fails.
- **Mutation checks**, both engines: retrying a non-transient failure; not re-checking an ambiguous save; purging a thread a lease holds; accepting a newer database.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

### The questions as asked (the handoff brief)

Recommendations are in bold.

1. **Async everywhere.** **Constructors stop touching the network; preparation is lazy, or explicit and async.**
2. **Transient faults.** **A policy inside the Postgres and Redis stores of both engines, with the list above; an ambiguous conditional save is re-checked; Rust reconnects.**
3. **Migrations.** **A `strata_schema_version` table and ordered steps, at open or by `MigrateAsync`.**
4. **Retention.** **`PurgeAsync(olderThan, states)`; an optional Redis ttl; never automatic.**
5. **Rust parity.** **All of it.**

### Added by this document

6. **Version 1 is a baseline, not a replay of history.** **Yes.** The stores already know how to bring every older layout up to date; rewriting that as seven historical steps would risk every existing database to gain nothing. The baseline is that code, run once and recorded. Steps start at 2.
7. **Lazy preparation, not an async factory only.** **Both.** Constructors are how stores are registered in a container; a factory alone would break that. So a constructed store prepares itself on first use, and `OpenAsync` is there for code that wants the failure at startup.
8. **A plain save's ambiguity.** **Compare with the latest and repeat if it differs.** The alternative, never repeating a plain save, would fail a run for a dropped connection, which is the thing this gap is about.
9. **Acquiring a lease is not re-checked.** **The failure stands.** The store can't tell its own acquisition from another process's with the same owner name, and the cost of being wrong is a thread nobody can touch for one ttl either way.
10. **Which states can be purged.** **Any the caller names; finished ones by default.** A hard refusal of `interrupted` would force operators with abandoned approvals to delete rows by hand.
11. **Purge on its own port.** **`ICheckpointRetention`**, like the query port: a custom store stays complete without it.
12. **The Redis ttl in the save scripts.** **Yes.** Setting it in a second command could leave a thread that runs again with an expiry on it.
13. **No retry in the SQLite and in-memory stores.** **Right.** They have no network. SQLite's busy handling is the driver's.

---

## Implementation Status

Built 2026-10-10 in both engines, every store adapter and the CLI's tests, in one change, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **A migration is one transaction, not one per step.** The baseline and every later step run, and the version is recorded, in the transaction that holds the DDL lock. A failure leaves the database where it was, at the version it had. "Recording its version as it commits" above is the whole migration's commit.
- **`AutoMigrate` is the Postgres stores' only.** The SQLite stores always migrate when they open a file, as they always did: a file has one application. The SQLite history stores, which the runtimes don't share, record no version. In Rust the Postgres stores still connect and prepare when they are created (`connect` was always `async`), so a refusal comes from `connect_with`; in .NET it comes from the first call, or from `OpenAsync` / `MigrateAsync`.
- **Rust reconnects before an attempt, not after a failure.** Each attempt of a call first looks at the client, and makes a new one if its connection has closed. So a store built with `connect` replaces a dropped connection even with `StoreRetry::none()`, and a call counts as repeated only when the drop interrupted it. A store built with `from_client` fails every later call with `the connection is closed, and a store built from a client can't reconnect`.
- **The Rust Redis stores make their first connection themselves, once,** under the store's policy, and only then hand the URL to the connection manager. The manager tries an unreachable server several times before it says so, which made a wrong URL take over a minute to fail. While the manager reconnects, a call waits for it; the call the drop interrupted fails and is repeated.
- **`RebuildIndexAsync` / `rebuild_index` also drops threads whose lists are gone.** A scan of the lists can't find a list that expired, so the rebuild then walks the set of every thread. The count it returns is still the threads it indexed.
- **A held thread takes no place in a purge's limit.** The SQL stores and the in-memory ones leave held threads out of the candidates. The Redis store asks its script thread by thread, remembers the ones the script kept, and looks past them for more.
- **A purged id starts over, lease tokens included.** The lease row (in Redis, the token counter) goes with the thread, as the blueprint says, so the id's next lease is token 1 again. A worker that acquired the thread before the purge and has been stalled ever since could then hold a token that matches. Purge takes threads that are finished, not held, and older than a cutoff counted in days, while a lease lasts seconds, so the two don't meet; an application that purges with a cutoff shorter than its longest stall should not reuse thread ids.
- **A purge repeated after an ambiguous failure can count one thread too few.** The repeat finds the thread already gone. Nothing is purged twice, and nothing is left behind.
- **In Postgres a refused or lost call rolls its transaction back before it returns** (Rust). `tokio-postgres` otherwise leaves the rollback for the connection to send later, and until then the transaction holds the DDL lock, or the thread's. .NET's `await using` already did.
- **"Is this checkpoint" is the parsed wire JSON.** A number that a `JSONB` column gives back in another form (an exponent written out) still compares equal once parsed, in both engines' own JSON types. If a store ever gave a number back with a different value, the save would be made again; nothing here does.
- **Rust's Postgres errors now say what went wrong.** `tokio-postgres` prints only an error's kind (`db error`); the stores add the causes beneath it (the server's message, the I/O error). Neither holds the connection string.
- **The retry warning** carries the store, the operation, the attempt and the failure's class: a SQLSTATE or `closed` / `io` in Rust's Postgres store, the error kind in its Redis one, the exception's type in .NET.
- **The fault seam.** Both engines' Postgres and Redis options take a hook the tests use to fail a call at a named point (`save:before-commit`, `save:commit`, `save:after-commit`, `save:recheck`, `lease:after-commit`, `append:after-commit`; Redis `save:before`, `save:after`, `save:recheck`). It is `internal` in .NET and `#[doc(hidden)]` in Rust, like the later schema steps a test adds.
- **A test-only race from SG-12, fixed here.** `A_Lease_Taken_Over_Mid_Run_Cancels_The_Run_And_Writes_Nothing` asserted that the handler had seen its token the moment the run returned. The engine doesn't wait for a handler it has given up on, so the test now waits for it.

**How it was built:**

- **.NET:**
  - `Interruption/ICheckpointRetention.cs` is new: `ICheckpointRetention`, `CheckpointRetention.Finished` / `Takes`.
  - `Datalium.Strata.Storage/StoreRetry.cs` is new: `StoreRetryPolicy`, `StoreRetrier`, `SaveFate` and `StoredCheckpoints`, `StorageTelemetry`, `StoreSchemaVersions`, `SchemaStep`.
  - `Postgres/PostgresSchema.cs`: `PostgresStoreOptions`, `PostgresFaults`, `PostgresSchema.MigrateAsync`, `PostgresPreparation` (once, on first use). `PostgresCheckpointStore`: no I/O in the constructors, `OpenAsync`, `MigrateAsync`, every call under the retrier, `InsertAsync` / `FateAsync` / `InsertOnceAsync`, `PurgeAsync`, `IAsyncDisposable`. `PostgresHistoryStore`: the same preparation, an append repeated only before its commit.
  - `Redis/RedisConnection.cs`: `RedisStoreOptions`, `RedisFaults`, `ConnectAsync`. `RedisCheckpointStore`: the index built on first use, `SaveOnceAsync`, the ttl argument and `PurgeScript`, a listing and a rebuild that drop a gone thread, `PurgeAsync`. `RedisHistoryStore`: options, reads repeated, an append only if it was never sent.
  - `InMemoryCheckpointStore` and `EfCoreCheckpointStore`: `PurgeAsync`; the SQLite store's `Migrate` and the version table.
- **Rust:**
  - `strata-core/src/store.rs`: `CheckpointRetention`, `PurgeStates`, re-exported from `checkpoint`.
  - `strata-memory/src/retry.rs` is new: `StoreRetry`, `Retrier` (`again`, `run`), `Failure`, `SaveFate` / `fate_of`, the metric's names. `schema_versions.rs` is new.
  - `postgres_store.rs`: `PostgresStoreOptions`, `is_transient_sqlstate`, `Link` (the client, the URL to reconnect with, the retrier), `migrate_component`, `connect_with` / `from_client_with` / `migrate` on both stores, `insert_row` / `insert_attempt` / `fate` / `insert_once`, `purge_rows`.
  - `redis_store.rs`: `ConnectionManager` (the `connection-manager` feature), `RedisStoreOptions`, `is_transient_reply`, `connect_with`, `append` / `append_attempt` / `append_once`, `index_one`, the ttl argument and `PURGE_SCRIPT`, `purge_now`.
  - `checkpoint.rs`: `purge` on the in-memory and SQLite stores; `migrate` and the version table.
- **CLI:** no code. It opens every store through `strata-memory`, so it migrates a file it opens and refuses one a newer build has moved on.
- **Schema:** the three fixtures above; `schema/README.md`.

**Tests:**

- **Mirrored suites:** `StorageHardeningTests.cs` (27) and, in `strata-memory`, `tests/storage_hardening.rs` (24) with the policy's and the classification's unit tests in `retry.rs` and `redis_store.rs` (14): the policy, a dropped Postgres connection (`pg_terminate_backend`) and a dropped Redis one (`CLIENT KILL`) under a durable run, a save that may have happened (written and not, plain, conditional, fenced, and one that lost its race), an acquisition and an append that are not repeated, the version table and its fixtures, a database from before it, a newer one refused and left alone, `AutoMigrate` off, later steps run once in order (Postgres and SQLite), the purge contract against all four adapters, the Redis ttl, and a gone thread leaving the index.
- **What differs between the suites.** The caller's token stopping a wait is .NET's (Rust store calls take no token; dropping the future stops them). Reconnecting is Rust's: a store built with `connect` works again after its connection is terminated, with and without retries, and one built with `from_client` reports why it can't. .NET's "without retries a dropped connection is the call's failure" has no Rust twin for the reason above; both have "without retries a transient failure is the call's".
- **Not written:** a thread that changes while it is being purged. Each store deletes under the lock that makes it impossible to observe from a test without a seam of its own; the held thread and the limit are tested in all four.
- **CLI:** a file it creates has the version table of the fixture, at the shared version; one at a newer version is refused with the message above and left alone.
- **Totals when this landed:** .NET 812, with Postgres and Redis running, and 2 skipped (the opt-in Cloud live tests), plus the example's 1. `rust/` 746 and `cli/` 78, with `STRATA_REQUIRE_BACKENDS=1` against live Postgres and Redis, and `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` and `dotnet format --verify-no-changes` are clean. The Redis scripts and the version table's DDL are the same text in both engines.
- **Mutation checks, in both engines** (each broken, the hardening suites run, then restored). The counts are the same in .NET and in Rust:
  - Retrying a failure that isn't transient fails 1 test.
  - Not looking before a save in doubt is made again fails 4.
  - Purging a thread a live lease holds fails 4, one per adapter.
  - Accepting a newer database fails 3.

## Not built (deliberately out of scope)

- **Retention for history stores**, and purging by metadata or graph.
- **Archiving before deletion.** ~~A background sweeper.~~ Built 2026-10-10 (`35-Hosting-Integration.md`), as something an application opts into: `AddRetention()` in .NET and `Host::retain` in Rust call `PurgeAsync` on a cadence, with an age the application must give. Nothing purges by default.
- **Checkpoint deltas and blob offload** (SG-15): purge deletes whole threads; it doesn't shrink a live one.
- **Connection pooling in Rust's Postgres store**: still one connection, now replaced when it drops.
- **Redis Cluster and Sentinel**, and TLS in the Rust adapters beyond what they had.
- **Down migrations.** A database moves forward only.
- **A `strata migrate` or `strata purge` CLI command**: the CLI stays read-only, apart from the migration any store runs when it opens a database.
- **Retries in the Cloud store**: it has its own (`design/30`).
- **Circuit breaking**: after `MaxAttempts` the call fails, and the next call starts again.
