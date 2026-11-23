# Datalium Strata: Opening a Store Read-Only

No checkpoint store in either engine can be opened without changing it. Opening a store is how it gets ready to write, so today:

| Store | What opening does today |
|---|---|
| **SQLite**, both engines | A missing file is created (the drivers' default mode is read-write-create). Then, inside a `BEGIN IMMEDIATE` transaction, the store creates `strata_schema_version` if it isn't there, and brings an older file up to date: tables, columns, indexes, the head trigger and its backfill. It does this every time, even when the file is current. .NET does it in the `EfCoreCheckpointStore` constructor, Rust in `SqliteCheckpointStore::open`. There is no option to skip it. |
| **PostgreSQL**, both engines | Takes the DDL advisory lock and runs `CREATE TABLE IF NOT EXISTS strata_schema_version`, then migrates an older database. With `AutoMigrate = false` / `with_auto_migrate(false)` it refuses a database that is behind instead, but it still takes the lock and runs the `CREATE`, so it fails for a role without `CREATE` and in a read-only session or on a standby. .NET does this on the first call (any read), Rust in `connect_with`. |
| **Redis**, both engines | If `{prefix}:threads:indexed` is missing, it scans the prefix, builds the index sets and sets the marker. .NET does this on the first call, Rust in `connect_with`. Listing also writes: a listed thread whose list is gone is taken out of the index. |
| **`strata` CLI** | Opens every store through `strata-memory` with the defaults, so it creates a missing SQLite file, migrates an older database and indexes a Redis prefix. Its module doc says so: a SQLite path is "created if missing, and brought up to date like any store opening it". |

For a runtime that's right: a store has one application, and that application should bring it up to date. For a tool that only looks, it's a trap. A developer who points an inspector at their app's database, or at a shared staging Postgres, with a newer inspector would have it migrated ahead of their app by a tool that's supposed to only look. A mistyped SQLite path leaves an empty database behind. A read-only database role can't be used at all.

This document adds **FR-MP-12**: every SQL and Redis checkpoint store can be opened read-only. It is **DK-1** of Strata Desktop's brief (`27-desktop-product-brief.md` §6, in `strata-specs`), whose preview reads stores and must never change them, and the `strata` CLI moves to it too. It lands in both engines and the CLI at one commit (strict parity).

It is a blueprint, written before implementation (2026-11-23). "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end will record where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| What read-only promises | **Four things.** Opening creates nothing: no file, table, index, trigger or key. It never migrates. Reads never write. Every write is refused. |
| How it's asked for | **A factory per store**, in both engines: `OpenReadOnlyAsync` / `ConnectReadOnlyAsync` in .NET, `open_read_only` / `connect_read_only` in Rust. Not a flag on the existing options, because read-only is decided by how the connection is opened (SQLite's open mode, a Postgres session setting), which a store handed a caller's connection can't control. |
| When it checks | **At open, in both engines.** The factory reads the schema version before it returns, so a bad store fails at open, not on the first list. |
| Which versions it reads | **Any version from 1 to the one the build knows.** A newer database is refused with the existing "upgrade the runtime" message. Version 0 (a log from before the version table) is refused, because reading it needs the head table that only a migration creates. |
| Enforced by the database too | **Yes, where the database can.** SQLite opens with its read-only flag and `PRAGMA query_only`. Postgres sessions run with `default_transaction_read_only = on`, so the server itself refuses a write. Redis has no such switch; a read-only ACL user works if the read scripts are marked read-only (see "Redis"). |
| A missing or wrong store | **A clear error, and nothing created.** A missing SQLite file, a database with no Strata log, a Redis prefix with no thread index, a missing blob directory. The messages are shared by both engines and pinned by a fixture. |
| Writes on a read-only store | **Refused** with `the store was opened read-only: <operation> is refused`. That covers saves, lease changes and purge. Reads, `GetLeaseAsync` / `get_lease` included, work. |
| The CLI | **Opens every store read-only.** A missing SQLite file becomes an error instead of an empty new database, and a database that is behind is no longer migrated by an inspector. |
| Wire format | **No change.** No table, column, key, version or checkpoint encoding changes. So it can land in `1.0.0-rc.1` under STRATA-SPEC-005's freeze. |
| History and memory stores | **Not included.** Desktop and the CLI read checkpoints only. |

---

## Usage

**.NET**

```csharp
// A SQLite file. Fails if the file is missing, isn't a Strata store, or is newer than this build.
using var sqlite = await EfCoreCheckpointStore.OpenReadOnlyAsync("app.db");

// PostgreSQL. Works with a role that can only SELECT, and on a standby.
await using var postgres = await PostgresCheckpointStore.OpenReadOnlyAsync(
    "Host=staging;Database=app;Username=reader");

// Redis, with a prefix and offloaded values in a blob directory.
using var redis = await RedisCheckpointStore.ConnectReadOnlyAsync(
    "redis://localhost:6379",
    new RedisStoreOptions
    {
        Prefix = "billing",
        Offload = new OffloadOptions { Blobs = new FileSystemCheckpointBlobStore("/var/app/blobs") },
    });

ICheckpointQuery query = sqlite;
var page = await query.ListThreadsAsync(new ThreadQuery { States = [CheckpointThreadState.Failed] });
var log = await query.GetLogAsync(page.Threads[0].ThreadId);

await sqlite.SaveAsync(checkpoint); // InvalidOperationException: the store was opened read-only: save is refused
```

**Rust**

```rust
let sqlite = SqliteCheckpointStore::open_read_only("app.db", SqliteStoreOptions::new())?;
let postgres = PostgresCheckpointStore::connect_read_only(
    "postgres://reader@staging/app", PostgresStoreOptions::new()).await?;
let redis = RedisCheckpointStore::connect_read_only(
    "redis://localhost:6379",
    RedisStoreOptions::new()
        .with_prefix("billing")
        .with_offload(Offload::new().with_blobs(Arc::new(FileSystemBlobStore::new("/var/app/blobs")))),
).await?;
```

**CLI.** Nothing changes in how it's called. `strata thread list --storage app.db` reads `app.db` and changes nothing; on a missing file it fails instead of creating one.

---

## The API

### .NET (`Datalium.Strata.Storage`)

| Store | New | Takes |
|---|---|---|
| `EfCoreCheckpointStore` | `static Task<EfCoreCheckpointStore> OpenReadOnlyAsync(string path, SqliteStoreOptions? options = null, CancellationToken cancellationToken = default)` | A file path |
| `PostgresCheckpointStore` | `static Task<PostgresCheckpointStore> OpenReadOnlyAsync(string connectionString, PostgresStoreOptions? options = null, CancellationToken cancellationToken = default)` | A connection string |
| `RedisCheckpointStore` | `static Task<RedisCheckpointStore> ConnectReadOnlyAsync(string url, RedisStoreOptions? options = null, CancellationToken cancellationToken = default)` | A `redis://` URL or a StackExchange.Redis configuration string, as `ConnectAsync` takes |

- Each returns the same store type, with a new `bool IsReadOnly { get; }` that is `true`.
- From the options, a read-only store uses `Offload.Blobs` (where offloaded values are read from), `Retry`, `TimeProvider` and Redis's `Prefix`. Write options (`GroupCommit`, `Deltas`, `AutoMigrate`, `FinishedThreadTtl`, the offload `Threshold`) have no effect.
- Errors at open are `InvalidOperationException`, with the messages under "Errors". A missing blob directory is a `DirectoryNotFoundException`.

### Rust (`datalium-strata-memory`)

| Store | New |
|---|---|
| `SqliteCheckpointStore` | `pub fn open_read_only(path: impl AsRef<Path>, options: SqliteStoreOptions) -> Result<Self, CheckpointStoreError>` |
| `PostgresCheckpointStore` | `pub async fn connect_read_only(url: &str, options: PostgresStoreOptions) -> Result<Self, CheckpointStoreError>` |
| `RedisCheckpointStore` | `pub async fn connect_read_only(url: &str, options: RedisStoreOptions) -> Result<Self, CheckpointStoreError>` |

- Each store gets `pub fn is_read_only(&self) -> bool`.
- The same options are used and ignored as in .NET. `open_read_only` is synchronous like `open`, since SQLite has no network.
- Every write method returns `CheckpointStoreError` with the shared message.

---

## Semantics

### What every read-only store does

1. **Opens without creating.** The connection is opened in a way that can't create anything (below, per store).
2. **Checks what it opened, and refuses what it can't read.** It reads the `checkpoints` component's version without creating the version table. It accepts versions 1 to the build's known version (2 today). It refuses a newer one (the existing `Newer` message) and version 0. It refuses a database that has no Strata log at all.
3. **Reads exactly as a writable store reads.** `ListThreadsAsync` / `list_threads`, `GetLogAsync` / `get_log`, `LoadAsync` / `load`, `LoadLatestAsync` / `load_latest` and `GetLeaseAsync` / `get_lease` return what the writable store would. The CLI's and Desktop's agreement with the runtimes depends on it.
4. **Refuses every write**, before it reaches the database.

A database at version 1 has no `checkpoint_blobs` table, and none of its rows can be offloaded (offload came with version 2). So a read-only store at version 1 reads offloaded values from nowhere: it never queries `checkpoint_blobs`. An external blob store given in the options is still used.

### SQLite

- **Opening.** .NET opens with `SqliteOpenMode.ReadOnly`. Rust uses `OpenFlags::SQLITE_OPEN_READ_ONLY` (no `SQLITE_OPEN_CREATE`). Both then set `PRAGMA query_only = ON`, so a bug that tries to write fails in SQLite.
- **A missing file** is checked before the driver opens it, so the error names the path instead of SQLite's "unable to open database file".
- **The check.** It reads `sqlite_master` for `strata_schema_version`, `checkpoints` and `checkpoint_threads`. No `checkpoints` table means "no Strata log". `checkpoints` without the version table means version 0. Otherwise it reads the version row.
- **No transaction is begun for the check.** Today's `BEGIN IMMEDIATE` takes a write lock, which a read-only connection can't and shouldn't take.
- **A hot journal.** A file whose writer crashed mid-write has a hot rollback journal. A read-only connection can't roll it back, and SQLite refuses to read it. The store reports that the file has an unfinished write and should be opened once by the application that writes it.
- **WAL.** The framework's stores don't set a journal mode, so their files use the rollback journal. An application that switches its own connection to WAL makes reading depend on the `-wal` and `-shm` files. Review decision 8 asks for a test of what SQLite does then.

### PostgreSQL

- **Opening.** The connection string gets `Options=-c default_transaction_read_only=on` (.NET, Npgsql). In Rust, `tokio_postgres::Config::options` gets the same. Every transaction in the session is read-only, so the server refuses any write. That is also what makes a standby and a `SELECT`-only role work.
- **The check.** No advisory lock and no DDL. One query, `SELECT to_regclass('strata_schema_version'), to_regclass('checkpoints'), to_regclass('checkpoint_threads')`, resolved through the connection's `search_path` like every other statement the store runs. Then the version row.
- **When.** In the factory, before it returns, in both engines. A read-only .NET store doesn't use `PostgresPreparation` at all.
- **A caller's `NpgsqlDataSource` / client** isn't accepted (review decision 6).

### Redis

- **Opening.** It connects as today, then checks that `{prefix}:threads:indexed` exists. If it doesn't, the store refuses: either nothing was ever written under the prefix (a mistyped prefix is the common case), or no runtime from after the index (`design/19`) has connected to it. It never scans, indexes or sets the marker.
- **No schema version.** The Redis checkpoint store has none (`schema/README.md`, "Compatibility", rule 6). Each checkpoint's own `schema_version` still decides whether it can be read. A version key is a layout change, out of scope here.
- **Listing.** A listed thread whose list is gone is skipped. The read-only store doesn't take it out of the index, as the writable store does.
- **Scripts on read paths.** Two reads run Lua scripts: the lease lookup (`GetLeaseScript` / `GET_LEASE`) and the listing's candidate script (`CANDIDATES`). A Redis ACL user limited to read commands can't run `EVAL`. On Redis 7 and later these two scripts declare `#!lua flags=no-writes` and run with `EVAL_RO` / `EVALSHA_RO`, so such a user works. On older Redis, read-only is the store's promise, not the server's (review decision 9).

### Blob directory

- A `FileSystemCheckpointBlobStore` / `FileSystemBlobStore` given to a read-only store must exist at open. Today a missing root reads as "blob not found" on every get, which would surface later as a misleading missing-blob error on the first offloaded value.
- A read-only store never calls `put` or `delete_thread`.

### Errors

The same messages in both engines and the CLI, pinned by `schema/fixtures/checkpoint-log/read-only-errors.json`.

| Case | Message |
|---|---|
| The SQLite file doesn't exist | `no SQLite store at '<path>': the file doesn't exist` |
| No Strata log in the database | `'<store>' has no Strata checkpoint log` |
| Newer than the build | (unchanged) `database is at checkpoints schema version <found>, and this build knows <known>: upgrade the runtime` |
| Version 0 | `database is at checkpoints schema version 0, and a read-only store reads 1 to <known>: open it once with a runtime to migrate it` |
| A hot SQLite journal | `'<path>' has an unfinished write: open it once with the application that writes it` |
| Redis prefix with no index | `no Strata thread index under prefix '<prefix>': nothing was written there, or no current runtime has connected to it` |
| Missing blob directory | `blob directory '<dir>' doesn't exist` |
| A write | `the store was opened read-only: <operation> is refused` (`save`, `acquire lease`, `renew lease`, `release lease`, `purge`) |

`<store>` is the SQLite path, or the Postgres database name. Connection failures (a refused connection, bad credentials) keep the driver's own error.

---

## The CLI

- `cli::store::open_with` opens each kind with the read-only constructor. `--blobs` is passed as the read-only store's blob directory.
- `Storage::insert` and `Storage::hold` exist only to seed tests. They move behind `#[cfg(test)]`, or into the tests, and open a writable store themselves.
- The module doc's "created if missing, and brought up to date" goes. `design/34`'s note "the CLI stays read-only, apart from the migration any store runs when it opens a database" becomes simply "the CLI is read-only".
- **For users, three changes**, listed in `CHANGELOG.md`:
  - A missing SQLite path is an error (it used to create an empty database and list nothing).
  - A database at version 0 is refused, not migrated.
  - A Redis prefix no current runtime has indexed is refused, not indexed.

---

## Telemetry

Nothing new. Read-only stores retry transient faults on reads as writable ones do, with the same `strata.store.retries` counter.

---

## Wire format

None. No table, column, index, trigger, key, schema version or checkpoint encoding changes. Writers are untouched.

One new test fixture: `schema/fixtures/checkpoint-log/read-only-errors.json`, the messages above, read by both engines' tests and the CLI's.

---

## Migration

- **Runtimes:** nothing changes. Only the new factories are read-only.
- **The CLI:** the three changes under "The CLI". A script that relied on `strata thread list` creating a SQLite file must create it another way. None of the repo's examples do.
- **Release:** `1.0.0-rc.1`. It has no wire change, so it fits STRATA-SPEC-005's rule of no wire-format change from `0.9.0` until `1.0.0-rc.1`. Desktop's M2 (Jan 2027) builds on that release.

---

## Tests

Mirrored in both engines, and against all three backends where CI has them (Postgres and Redis run in the `dotnet-ci` and `rust-ci` service containers today).

**Nothing changes.** The central test is per store kind. Seed a store with `checkpoint-query/cases.json`, an offloaded log and a held lease. Snapshot it, open it read-only, run every read, and compare.

- **SQLite.** The file's SHA-256 is unchanged, and no `-journal`, `-wal` or `-shm` file appears. Run once with the file and its directory writable and once with both read-only (Unix CI only).
- **Postgres.** Every row of every `strata`/`checkpoint` table and the version row are unchanged. Run as a role granted only `CONNECT` and `SELECT`, so a write would fail anyway.
- **Redis.** Every key under the prefix keeps its type, value and TTL, and no key is added.

**Refusals.** Each one changes nothing:

- A missing SQLite file is refused, and no file is created.
- A file or database with no Strata log is refused.
- Version 0 is refused, with the `checkpoints` table present and the version table absent.
- A newer version is refused, with the version left as it was.
- A Redis prefix without `threads:indexed` is refused, and the marker isn't created.
- A missing blob directory is refused, and it isn't created.

**Reads.**

- A read-only store answers every `checkpoint-query/cases.json` case exactly as the writable store does.
- A version 1 database (from `write_older` in the cross-engine tests) is read, including its logs.
- An offloaded log is read from the database's own `checkpoint_blobs` and from a blob directory.
- In Redis, a listed thread whose list was deleted is skipped, and the index sets are unchanged.

**Writes.** Each write method on each read-only store returns the shared message.

**Database-enforced.** With `query_only` (SQLite) and `default_transaction_read_only` (Postgres), a write issued straight on the store's connection fails. This proves the guard doesn't rest only on the store's own checks.

**CLI.**

- Every command (`thread list`, `thread inspect`, `checkpoint log`, `checkpoint diff`, `checkpoint replay`) runs against a SQLite file and directory made read-only. Each succeeds, and the file's SHA-256 is unchanged.
- `thread list --storage missing.db` fails with the shared message and creates nothing.
- `a_log_the_cli_creates_matches_the_shared_column_fixtures` creates its file through `strata-memory` instead of the CLI.

**Cross-engine.** `cross-engine-ci.yml` gains a step in which the CLI reads the older (version 1) databases read-only, before any engine migrates them. Today it only reads them after Rust has migrated them.

**Mutation checks**, in both engines. Each is broken, the suites run, then restored:

- Let a Redis list index a missing thread.
- Drop `query_only`.
- Take the advisory lock in the Postgres check.
- Run the version-table DDL in the SQLite check.

The "nothing changes" tests must fail for each.

---

## Review decisions

Decision 3 was decided on 2026-11-23, as recommended. The others aren't reviewed yet. The build follows each recommendation below; a different answer from review changes the build before merge.

Recommendations are in bold.

1. **Factories or an option flag.** **Factories.** Read-only is decided by how the connection is opened, and the existing constructors take a caller's connection, DbContext, data source or client. A `ReadOnly` flag next to them would be a promise they can't keep.
2. **The same store type or a reader interface.** **The same store type, with writes refused at run time, and `IsReadOnly`.** A narrower `ICheckpointReader` (query, load, get lease) would let a compiler catch a write. It's a public API addition, so it belongs in the 1.0 API review, and it can be added later without breaking this.
3. **Older databases.** **Read versions 1 to known; refuse 0.** *Decided 2026-11-23.* DK-1's wording in the Desktop brief is "older or newer than the reader knows". Refusing every older version would stop a newer Desktop or CLI reading the store of an app one framework release behind, which is the normal case for an inspector. Version 1 differs from 2 only by `checkpoint_blobs`, which a version 1 database can't need. Version 0 lacks the head table that listing reads.
4. **A schema version for Redis checkpoint stores.** **Not now.** The index marker is the check. A version key is a layout change, so it waits for after `1.0.0-rc.1`.
5. **The CLI moves in the same commit.** **Yes.** An inspector that changes what it inspects is the same bug in the CLI as in Desktop, and moving both keeps them reading through the same code.
6. **A caller's connection, data source or client.** **Not accepted, for now.** The session setting and open mode can't be guaranteed on a connection someone else opened. Desktop and the CLI only need a path, a connection string or a URL.
7. **Eager checking in .NET.** **Yes, in the factories.** Writable .NET stores prepare lazily so they can be registered in a container (`design/34`, review decision 7). A read-only store is opened by a tool that wants the failure at open, and both engines then behave alike.
8. **WAL files written by an application.** **Test it, then document what SQLite does.** If reading a WAL file read-only needs write access to its directory for `-shm`, say so in the error and in Desktop's docs; don't work around it.
9. **Redis scripts on read paths.** **Mark them `no-writes` and call them with `EVAL_RO` on Redis 7 and later.** To check in the build: whether StackExchange.Redis and the `redis` crate expose `EVAL_RO`/`EVALSHA_RO` directly, or need a raw command.

---

## Implementation Status

Not built. This section records the build when it lands.

---

## Not built (deliberately out of scope)

- **Read-only history and memory stores.** Nothing reads them without writing yet.
- **Strata Cloud.** `StrataCloudCheckpointStore` reads over HTTP with an API key that can also write; read-only keys are Cloud's question (Desktop brief §11, item 7).
- **A `strata migrate` command**, now that the CLI never migrates. An application's own runtime migrates its store.
- **A Redis checkpoint schema version** (review decision 4).
- **Writing through Desktop** (approvals and resume, DK-2): it waits for the write-path decision (family vision §12, item 5).
