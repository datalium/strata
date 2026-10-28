# Datalium Strata: Hosting Integration

Strata is a library of parts. An application that uses it in a service has to put the parts together itself:

- **Nothing registers Strata in a container.** StrataOps registers the Postgres store three times by hand (the type, `ICheckpointStore`, `ICheckpointQuery`), opens it in a start-up service of its own so the tables exist before the first run, and wires Strata's telemetry source and meter into OpenTelemetry in a helper of its own (`StrataOps.Runtime/RuntimeModule.cs`, `StrataInstrumentation.cs`, both marked `SG-28`).
- **Nothing binds configuration.** Every store option (retries, migration, ttl) and every run default (the lease owner and ttl) is set in code.
- **Nothing answers "is the store reachable?"** for a readiness probe.
- **Four loops have no home.** Timers and approval timeouts fire only when something calls `WakeDueAsync` (`design/29`). A dead run is recovered only when something calls `RecoverStaleAsync` (`design/33`). Finished threads are deleted only when something calls `PurgeAsync` (`design/34`). Cloud decisions are handled only while something runs the `DecisionWorker` (`design/30`). Each of those documents says "a library call: the application calls it on its own cadence". Every application then writes the same loop, with the same questions: how often, what happens when it throws, and what happens to a run in flight when the process is asked to stop.

This document closes the gap in the engine: **SG-28**. The SG numbers refer to the gap log of StrataOps (not public yet). The gap names .NET (`Datalium.Strata.Hosting`). Under strict parity (StrataOps ADR 0003) it needs a Rust counterpart or a stated reason there is none, so this document gives each part one or the other.

It is written as a blueprint before implementation. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Packaging (.NET) | A new project **`Datalium.Strata.Hosting`**, referencing `Datalium.Strata`, `Datalium.Strata.Storage` and `Datalium.Strata.Cloud`, and the `Microsoft.Extensions.*` abstractions (DI, options, hosting, health checks, logging) plus `OpenTelemetry.Api.ProviderBuilderExtensions`. The engine and the stores gain no dependency. |
| Packaging (Rust) | A new crate **`rust/strata-host`** (`datalium-strata-host`), depending on `datalium-strata` and `datalium-strata-memory`. |
| Registration (.NET) | **`services.AddStrata()`** returns a builder; `AddPostgresCheckpoints`, `AddRedisCheckpoints`, `AddSqliteCheckpoints`, `AddInMemoryCheckpoints`, `AddCloudCheckpoints` and the matching `Add…History` each register one singleton store under every port it implements. |
| Registration (Rust) | **None.** Rust has no container to register in, and `RunConfig::with_service` already carries services to nodes. Settings structs open the stores instead (below). |
| Configuration | **.NET:** options classes bound from the `Strata` configuration section (`Strata:Postgres`, `Strata:Redis`, `Strata:Runs`, `Strata:Wake`, …), then a delegate. **Rust:** the same settings as `serde::Deserialize` structs, read from whatever format the application uses, with `open()` on each store's. |
| Start-up | **.NET:** a hosted service migrates the SQL stores and compiles every registered graph when the host starts, so a database that is behind, or a graph with a structural error, stops the host from starting. **Rust:** `open()` and `compile()` already fail where they are called. |
| Graphs and runs (.NET) | **`AddGraph<TState>(factory)`** registers the compiled graph and a **`StrataRunner<TState>`**: the graph, the store and the host's run defaults (services, clock, history, observer, lease) in one injectable object. Composition, not a `StrataWorker<TState>` base class. |
| The four loops | **Drivers.** .NET: `AddWakeDriver<TState>()`, `AddRecoveryDriver<TState>()`, `AddRetention()`, `AddDecisionWorker<TState>()`, each a hosted service. Rust: a **`Host`** that runs the same loops as tasks until a token fires. |
| A driver that fails | **Logs, counts, waits and goes on.** It never stops the host. The wait doubles from the interval up to a cap while it keeps failing. |
| Stopping | **A driver starts nothing more and lets the run in flight finish.** It does not cancel it: a cancelled thread is nobody's until someone resumes it, while a thread whose process died is recovered. Both engines' wake and recovery options gain a **stop token** for this. |
| Several replicas | **Safe by construction.** Every driver may run on every replica: waking and recovering claim conditionally (and under leases), and purging twice is purging once. |
| Health | **.NET:** two `IHealthCheck`s, `strata-checkpoints` (a load of a thread that doesn't exist) and `strata-drivers` (degraded while a driver's last round failed). **Rust:** `check_store` and the host's driver status, for whatever endpoint the application has. |
| Instrumentation | **.NET:** `AddInstrumentation()` adds Strata's sources and meters (`Datalium.Strata`, `.Storage`, `.Cloud`, `.Hosting`) to the application's OpenTelemetry providers. **Rust: none applies.** `tracing` and `metrics` are global facades: the application installs a subscriber and a recorder and Strata's spans and metrics arrive. |
| Wire format | **None.** No checkpoint, definition or database change. |

---

## Usage

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddStrata()
    .AddPostgresCheckpoints()                       // Strata:Postgres, or a delegate, or an NpgsqlDataSource from the container
    .AddInstrumentation()
    .AddHealthChecks()
    .AddGraph(sp => Investigation.Build(sp))        // CompiledGraph<InvestigationState>, compiled at start-up
    .AddWakeDriver<InvestigationState>()            // timers and approval timeouts
    .AddRecoveryDriver<InvestigationState>()        // runs whose process died
    .AddRetention(o => o.OlderThan = TimeSpan.FromDays(30));

// Anywhere a run is started or answered:
public sealed class Investigations(StrataRunner<InvestigationState> runner)
{
    public Task<GraphExecutionResult<InvestigationState>> Start(string id, InvestigationState state, CancellationToken ct) =>
        runner.RunAsync(id, state, cancellationToken: ct);
}
```

```json
{
  "Strata": {
    "Postgres": { "ConnectionStringName": "strata", "Retry": { "MaxAttempts": 6 } },
    "Runs": { "Lease": { "Ttl": "00:00:30" } },
    "Recovery": { "Interval": "00:00:15" },
    "Retention": { "Interval": "01:00:00" }
  }
}
```

```rust
let settings: StrataSettings = config.get("strata")?;            // any serde source
let store = Arc::new(settings.postgres.open().await?);
let graph = Arc::new(investigation::build()?);
let runs = settings.runs.config();                               // the lease, when it has a ttl

Host::new()
    .wake(graph.clone(), store.clone(), settings.wake.clone().with_config(runs.clone()))
    .recover(graph.clone(), store.clone(), settings.recovery.clone().with_config(runs))
    .retain(store.clone(), settings.retention.clone())
    .run(shutdown)                                               // until the token fires
    .await?;                                                     // Err: a driver lacks what it needs
```

---

## The API

### .NET (`Datalium.Strata.Hosting`)

```csharp
public static class StrataServiceCollectionExtensions
{
    public static IStrataBuilder AddStrata(this IServiceCollection services);   // idempotent
}
public interface IStrataBuilder { IServiceCollection Services { get; } }

// Stores. One checkpoint store and one history store per container; a second registration replaces the first.
public static class StrataStoreExtensions
{
    public static IStrataBuilder AddInMemoryCheckpoints(this IStrataBuilder builder);
    public static IStrataBuilder AddSqliteCheckpoints(this IStrataBuilder builder, Action<StrataSqliteOptions>? configure = null);
    public static IStrataBuilder AddPostgresCheckpoints(this IStrataBuilder builder, Action<StrataPostgresOptions>? configure = null);
    public static IStrataBuilder AddRedisCheckpoints(this IStrataBuilder builder, Action<StrataRedisOptions>? configure = null);
    public static IStrataBuilder AddCloudCheckpoints(this IStrataBuilder builder, Action<StrataCloudHostOptions>? configure = null);
    // A store of the application's own:
    public static IStrataBuilder AddCheckpointStore<TStore>(this IStrataBuilder builder, Func<IServiceProvider, TStore> factory)
        where TStore : class, ICheckpointStore;
    // …and AddInMemoryHistory, AddSqliteHistory, AddPostgresHistory, AddRedisHistory, AddHistoryStore<TStore>.
}

public sealed class StrataPostgresOptions            // Strata:Postgres
{
    public string? ConnectionString { get; set; }
    public string? ConnectionStringName { get; set; }      // ConnectionStrings:<name>
    public string? DataSourceKey { get; set; }             // a keyed NpgsqlDataSource from the container
    public bool UseRegisteredDataSource { get; set; }      // the unkeyed one
    public bool AutoMigrate { get; set; } = true;
    public StoreRetryOptions Retry { get; } = new();       // MaxAttempts, InitialDelay, MaxDelay
}
// StrataRedisOptions (ConnectionString | ConnectionStringName | a registered IConnectionMultiplexer, Prefix,
// FinishedThreadTtl, Retry), StrataSqliteOptions (Path), StrataCloudHostOptions (BaseUrl, ApiKey, timeouts).

// Graphs and runs.
public static class StrataGraphExtensions
{
    public static IStrataBuilder AddGraph<TState>(this IStrataBuilder builder, Func<IServiceProvider, CompiledGraph<TState>> factory);
}
public sealed class StrataRunOptions                 // Strata:Runs
{
    public StrataLeaseOptions Lease { get; } = new();      // Owner (default "<machine>/<pid>"), Ttl (null: no lease)
}
public sealed class StrataRunner<TState>
{
    public CompiledGraph<TState> Graph { get; }
    public ICheckpointStore Store { get; }
    public RunOptions CreateOptions(IReadOnlyDictionary<string, string>? metadata = null, string? runId = null,
        DateTimeOffset? deadline = null, IRunObserver? observer = null);
    public Task<GraphExecutionResult<TState>> RunAsync(string threadId, TState initial, RunOptions? options = null, CancellationToken cancellationToken = default);
    public Task<GraphExecutionResult<TState>> ResumeAsync(string threadId, HumanSignal signal, RunOptions? options = null, CancellationToken cancellationToken = default);
    public Task<GraphExecutionResult<TState>> SignalAsync(string threadId, EventSignal signal, RunOptions? options = null, CancellationToken cancellationToken = default);
    public Task<GraphExecutionResult<TState>> RecoverAsync(string threadId, RunOptions? options = null, CancellationToken cancellationToken = default);
    public Task<GraphExecutionResult<TState>> EndAsync(string threadId, ThreadEnd? end = null, RunOptions? options = null);
}

// Drivers.
public static class StrataDriverExtensions
{
    public static IStrataBuilder AddWakeDriver<TState>(this IStrataBuilder builder, Action<WakeDriverOptions>? configure = null);
    public static IStrataBuilder AddRecoveryDriver<TState>(this IStrataBuilder builder, Action<RecoveryDriverOptions>? configure = null);
    public static IStrataBuilder AddRetention(this IStrataBuilder builder, Action<RetentionOptions>? configure = null);
    public static IStrataBuilder AddDecisionWorker<TState>(this IStrataBuilder builder, Func<IServiceProvider, DecisionWorkerOptions<TState>>? options = null);
}
public sealed class WakeDriverOptions      { Interval = 5 s;  BatchSize = 100; Metadata; MatchGraph = true; }                          // Strata:Wake
public sealed class RecoveryDriverOptions  { Interval = 30 s; BatchSize = 100; StaleAfter; MaxRecoveries; Metadata; MatchGraph = true; } // Strata:Recovery
public sealed class RetentionOptions       { Interval = 1 h;  BatchSize = 1000; OlderThan (required); States = completed, ended; }      // Strata:Retention

// Health and instrumentation.
public static class StrataHostExtensions
{
    public static IStrataBuilder AddHealthChecks(this IStrataBuilder builder, IEnumerable<string>? tags = null, TimeSpan? timeout = null);
    public static IStrataBuilder AddInstrumentation(this IStrataBuilder builder);
}
public static class StrataInstrumentation { ActivitySourceNames; MeterNames; }   // for wiring by hand
public interface IStrataDriverStatus { IReadOnlyList<DriverState> Drivers { get; } }   // name, rounds, failures in a row, last round, last failure's type
```

### The engine, both languages

```csharp
// WakeOptions and RecoverOptions
public CancellationToken Stop { get; init; }   // when it fires, the driver starts no more threads; the one in flight finishes
```

```rust
// WakeOptions and RecoverOptions
pub fn with_stop(self, stop: CancellationToken) -> Self;
```

### Rust (`datalium-strata-host`)

```rust
pub struct Host { /* drivers */ }
impl Host {
    pub fn new() -> Self;
    pub fn wake<S, T>(self, graph: Arc<CompiledGraph<S>>, store: Arc<T>, driver: WakeDriver) -> Self
        where T: CheckpointStore + CheckpointQuery + 'static;
    pub fn recover<S, T>(self, graph: Arc<CompiledGraph<S>>, store: Arc<T>, driver: RecoveryDriver) -> Self
        where T: CheckpointStore + CheckpointQuery + 'static;
    pub fn retain<T>(self, store: Arc<T>, driver: RetentionDriver) -> Self where T: CheckpointRetention + 'static;
    /// A loop of the application's own, such as the Cloud decision worker's `run`. Started again after an error.
    pub fn task<F, Fut>(self, name: impl Into<String>, task: F) -> Self
        where F: FnMut(CancellationToken) -> Fut + Send + 'static, Fut: Future<Output = Result<(), String>> + Send + 'static;
    pub fn with_clock(self, clock: Clock) -> Self;                // what "now" is; the waits are tokio's
    pub fn status(&self) -> DriverStatus;                         // cloneable; for a health endpoint
    /// Returns when every driver has stopped. Err, having started nothing, when a driver lacks what it needs.
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), HostError>;
}

pub struct WakeDriver { /* interval, batch_size, metadata, match_graph, config */ }        // Deserialize, Default, with_*
pub struct RecoveryDriver { /* interval, batch_size, stale_after, max_recoveries, …, config */ }
pub struct RetentionDriver { /* interval, batch_size, older_than, states */ }

pub struct StrataSettings { pub postgres: PostgresSettings, pub redis: RedisSettings, pub sqlite: SqliteSettings,
                            pub runs: RunSettings, pub wake: WakeDriver, pub recovery: RecoveryDriver, pub retention: RetentionDriver }
impl PostgresSettings { pub async fn open(&self) -> Result<PostgresCheckpointStore, CheckpointStoreError>; /* open_history */ }
impl RedisSettings    { pub async fn open(&self) -> Result<RedisCheckpointStore, CheckpointStoreError>;    /* open_history */ }
impl SqliteSettings   { pub fn open(&self) -> Result<SqliteCheckpointStore, CheckpointStoreError>; }
impl RunSettings      { pub fn config(&self) -> RunConfig; }      // the lease, when it has a ttl

pub async fn check_store(store: &dyn CheckpointStore, timeout: Duration) -> StoreHealth;   // Healthy { latency } | Unhealthy { error }
```

Durations in settings are a number of seconds or a string with a unit (`"250ms"`, `"30s"`, `"5m"`, `"1h"`, `"30d"`).

---

## Semantics

### Stores

Each `Add…Checkpoints` registers the store as a singleton under its own type and under every port it has: `ICheckpointStore`, and `ICheckpointQuery` and `ICheckpointRetention` for all but the Cloud store (which, since SG-36, `43-Cloud-Leases-And-Listing.md`, is an `ICheckpointQuery` too, so the wake and recovery drivers run on it; it still has no retention). Nothing is opened at registration (`design/34`: no constructor touches the network).

Where a Postgres store gets its connection, in this order: a `DataSourceKey` (a keyed `NpgsqlDataSource` the application registered, as Aspire does), `UseRegisteredDataSource`, `ConnectionString`, `ConnectionStringName` (default `strata`, read from `ConnectionStrings`). A store built on the container's data source doesn't own it. Redis is the same with `IConnectionMultiplexer`.

An application's own store goes in with `AddCheckpointStore<TStore>`. It is registered under the ports its type implements, found once at registration.

### Start-up

`AddStrata()` adds one hosted service that runs before the drivers start. It:

1. migrates each registered SQL store (`MigrateAsync`), unless its `AutoMigrate` is off, in which case it only checks, and a database that is behind stops the host with the store's message (`design/34`);
2. resolves every registered graph, which compiles it. A structural error stops the host, as `Compile()` promises ("abort at start-up, not at run time").

So a host that started has stores that are current and graphs that are valid.

### Run defaults

`StrataRunner<TState>.CreateOptions` builds a `RunOptions` from the container: `Services` is the container, `TimeProvider`, `IHistoryStore` and `IRunObserver` are the registered ones if any, and `Lease` is `Strata:Runs:Lease` when it has a `Ttl`. Its `RunAsync`, `ResumeAsync`, `SignalAsync`, `RecoverAsync` and `EndAsync` use those defaults when the caller passes no options, and the caller's options, untouched, when it does. The drivers run with the same defaults.

### Drivers

A driver is a loop: do one round, wait `Interval`, again. A round is one call:

| Driver | A round |
|---|---|
| Wake | `WakeDueAsync` with `Limit = BatchSize`, again while a call fills its batch and woke something. |
| Recovery | `RecoverStaleAsync` with `Limit = BatchSize`, again while a call recovers a whole batch. Needs a graph compiled with `EveryNode`, and a lease in the run defaults or a `StaleAfter`: without either the host doesn't start. |
| Retention | `PurgeAsync(now − OlderThan, States, BatchSize)`, again while a call returns `BatchSize`. `OlderThan` has no default: an application says how long it keeps its threads. |
| Decisions | `DecisionWorker.RunAsync` (it has its own polling and back-off, `design/30`) |

- **A round that throws** (the store is down, a woken run threw) is logged, counted and recorded in the driver's status. The driver then waits and tries again: the wait is `Interval`, doubled for each failure in a row, up to 5 minutes, and back to `Interval` after a round that works. A driver never ends the host.
- **Stopping.** When the host stops, a driver finishes the wait it is in at once, or lets the round it is in wind down: the round's `Stop` token fires, so it starts no more threads, and the thread it is running finishes. Nothing is cancelled. If the run outlasts the host's shutdown timeout the process exits with it in flight, which is a crash as far as the thread is concerned, and what `EveryNode` and the recovery driver are for.
- **Replicas.** Every replica may run every driver. Two wake drivers that list the same thread: one stamps it, the other skips it. Two recovery drivers: one claims, the other skips, and with leases a live run is nobody else's. Two purges delete once.
- **What a finished thread reads.** A thread a driver woke and ran to the end reads `completed` only if its graph is compiled with `CheckpointPolicy.EveryNode`. Under the default `Boundaries` it stays `resumed`, with the old `wait` / `wake_at` on its row (design/29, "Waking"). The recovery driver needs `EveryNode` anyway.
- **Several graphs.** A driver is registered per state type. Each lists only its own graph's threads when the graph has an identity (`design/31`); graphs without one share a store only if their drivers are given distinguishing `Metadata`.

Rust's `Host` is the same loops as tasks on the runtime `run` is awaited on, with the same waits, the same back-off and the same stop: `run` returns when every driver has let its round finish.

### Health

- **`strata-checkpoints`** loads the latest checkpoint of a thread id nobody uses (`strata.health`). An answer, which is "none", is healthy. A failure, or no answer within the timeout (5 s by default), is unhealthy. The description is the failure's type; the exception is attached for the application to render or not.
- **`strata-drivers`** is degraded while any driver's last round failed, and names them. A host with no drivers is healthy.

Rust: `check_store` does the same load and returns the latency or the error; `Host::status()` is the drivers' last rounds.

### Instrumentation

`AddInstrumentation()` calls `ConfigureOpenTelemetryTracerProvider` / `ConfigureOpenTelemetryMeterProvider` to add every Strata source and meter. It configures providers the application creates; it creates none, exports nothing, and depends on the OpenTelemetry API package only.

---

## Telemetry

| | |
|---|---|
| **`strata.host.driver.rounds`** | Counter: driver rounds, by `strata.driver` (`wake`, `recovery`, `retention`, `decisions`, or a task's name) and `strata.outcome` (`ok`, `error`). .NET meter `Datalium.Strata.Hosting`; Rust the `metrics` facade. |
| **`strata.host.driver.threads`** | Counter: threads a round woke, recovered or purged, by `strata.driver`. |
| Logs | .NET `ILogger` (category `Datalium.Strata.Hosting.<driver>`), Rust `tracing`: a round that did something at information, a round that failed at warning with the failure's type, start and stop at information. A round that found nothing logs nothing. |

The runs a driver starts are ordinary runs: their spans, events and metrics are the engine's.

---

## Wire format

Unchanged. No checkpoint, definition, database or Redis change, and no fixture.

---

## Migration

Additive. The engine's only change is the `Stop` token on the two drivers' options, which defaults to "never".

**StrataOps:** `RuntimeModule`'s three store registrations and `RuntimeInitializer`'s store opening become `AddStrata().AddPostgresCheckpoints(o => o.DataSourceKey = "strata")`; `StrataInstrumentation.AddStrataInstrumentation` becomes `.AddInstrumentation()`; `RuntimeOptions.WorkerId` and `LeaseDuration` become `Strata:Runs:Lease`; the M5 recovery loop and the retention job become `AddRecoveryDriver` and `AddRetention`.

---

## Tests

Mirrored where both engines have the part:

- **Drivers** (`HostingDriverTests` / `strata-host/tests/drivers.rs`), on the in-memory store and a fake clock: a due timer is woken within one interval; a dead `running` thread is recovered; old finished threads are purged, in batches; a round that throws is counted, the driver backs off (doubling, capped) and recovers; a stop during a wait ends the driver at once; a stop during a run lets the run finish and starts no other; two hosts on one store wake a thread once; a recovery driver with neither a lease nor a `StaleAfter` refuses to start.
- **The stop token** in the engines' own suites: `WakeDueAsync` and `RecoverStaleAsync` with a fired `Stop` start nothing; fired mid-batch they finish the thread in flight and return what they did.
- **Settings:** every option bound from configuration (.NET `IConfiguration`, Rust JSON through serde), defaults, a delegate over configuration, durations in each accepted form (Rust).
- **.NET only:** each `Add…` registers one instance under every port; a store on a registered data source doesn't dispose it; start-up migrates, and refuses a database that is behind or a graph that doesn't compile; `StrataRunner` defaults (services reach a node, the lease is taken, caller options win); both health checks (healthy, a store that fails, a store that hangs, a failing driver); `AddInstrumentation` makes a run's span and a metric reach an in-memory exporter.
- **Rust only:** `open()` on each settings struct against the live backends; `check_store`; `Host::task`.
- **Mutation checks**, both engines: a driver that cancels the run in flight on stop; a driver that stops after a failed round; a driver that ignores its interval's back-off; an engine that ignores the stop token.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

1. **One hosting package that references the stores and Cloud, or registration split across packages?** **One.** `Datalium.Strata.Storage` already carries every backend, so the hosting package adds no driver an application using the stores doesn't have. Splitting would put DI abstractions into `Datalium.Strata.Cloud`, which `design/30` keeps BCL-only.
2. **OpenTelemetry in the hosting package, or a package of its own?** **In it.** The dependency is the API's provider-builder extensions, not the SDK. `StrataInstrumentation.ActivitySourceNames` / `MeterNames` are there for anyone who wires it by hand.
3. **A `StrataWorker<TState>` base class?** **No: `StrataRunner<TState>`.** What an application's worker loops over (a queue, a topic, HTTP) is its own; what it needs from Strata is the graph, the store and the run defaults in one place.
4. **Do drivers cancel runs when the host stops?** **No.** A `cancelled` thread waits for someone to resume it and no driver does; a thread whose run finishes, or whose process exits, needs nothing or is recovered.
5. **What a Rust counterpart is.** **The drivers, the settings and the health probe; no registration and no instrumentation call**, for the reasons in the table at the top.
6. **Options types of the hosting package's own, or binding straight onto the stores' option records?** **Its own.** The stores' records are immutable (`init`) and hold things configuration can't (a clock, a fault hook); binding onto them leans on reflection setting `init` properties.
7. **Does retention have a default age?** **No.** Deleting data on a default nobody chose is the wrong failure.
8. **One checkpoint store per container?** **Yes.** An application with several registers the others itself, keyed.

---

## Implementation Status

Built 2026-10-10 in both engines, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **A round is more than one call when there is more to do.** A driver calls again at once while a call fills its batch: the wake driver while it also woke something (a batch of threads it can only skip would otherwise be listed again and again without a wait), the recovery driver while it recovered a whole batch, the retention driver while a purge deleted a whole batch. The interval is the wait once a round has caught up.
- **The first round runs when the host starts,** not an interval later: a host that comes up after a crash recovers at once.
- **Rust's `Host::run` returns a `Result`.** .NET refuses a misconfigured driver by failing the host's start; Rust has no start to fail, so `run` returns `HostError`, having started nothing. The messages are the same in substance and name each engine's own setting.
- **Rust's `Host` has a clock** (`with_clock`): "now" for what is due, what is old and when a round finished. The engine's `wake_due` and `recover_stale` take `now` as an argument, so the host needs one to give. The waits between rounds are tokio's.
- **`Host::task` takes a function it can call again** (`FnMut`), not once: a task that returns an error is started again after a wait that doubles while it keeps failing, as the .NET decision worker driver is.
- **`AddDecisionWorker` takes a function that returns the worker's options,** not a delegate that changes them: `DecisionWorkerOptions<TState>` is an immutable record. The worker's runs use the host's run defaults unless the options give their own.
- **One graph per state type** in a container, and one driver of each kind per state type. A driver's options are named by the state type, so two graphs' wake drivers share `Strata:Wake` and differ by their delegates.
- **A driver's failure is recorded by its type** (.NET: the exception's type name; Rust: the error's variant), in the log, the metric's absence of a message, and the status. A store's message can hold a connection string. `check_store` in Rust returns the store's own message, and says not to put it on a public endpoint; the .NET health check attaches the exception and describes it by type.
- **The engine's drivers want one object with the store port and the query port,** and a container hands them out separately. The hosting package wraps the two in a forwarding store (every member, the lease ones included) instead of adding overloads to the engine.
- **A Redis store given a connection string connects when it is first resolved,** which is the host's start-up, and that call blocks its thread for as long as the connection takes (five seconds at most). A registered `IConnectionMultiplexer` avoids it. A container's factory can't be awaited.
- **`Strata:Postgres` and `Strata:Redis` serve both of their stores.** The checkpoint and the history store share the connection settings, and for Redis the multiplexer.
- **Configuration is bound by reflection** (`ConfigurationBinder`), once, when the options are first read. Not trimming-safe; an application that publishes with trimming sets the options in a delegate.
- **`RunConfig::has_lease` is public now** (Rust), so the host can refuse a recovery driver with neither a lease nor an age before it starts.
- **Not written:** a functional test of `AddDecisionWorker` against the fake Cloud. Its registration, its refusal without the decisions client and the loop it shares with the other drivers are tested; the worker itself is `design/30`'s.
- **The examples are unchanged.** `examples/dotnet-agent` still hosts its one run by hand. *Since 2026-10-24 it doesn't:* it registers its stores and graph with `AddStrata()` (`AddInMemoryCheckpoints`, `AddInMemoryHistory`, `AddGraph`), its worker runs through the `StrataRunner<TicketState>` the host gives, streaming with `runner.CreateOptions()`, and its test starts a real host. `examples/rust-agent` is unchanged: `strata-host` has no registration to show.

**How it was built:**

- **Engine, both languages:** `WakeOptions.Stop` / `RecoverOptions.Stop` and `with_stop`: the two drivers' loops check it between threads, beside the token that also cancels the run.
- **.NET:** `dotnet/src/Datalium.Strata.Hosting/`, in the solution.
  - `StrataBuilder.cs`: `AddStrata`, `IStrataBuilder`, the options binding (`StrataConfiguration.Bind`, once per options type and name), the start-up steps and the hosted service that runs them.
  - `StrataStoreOptions.cs`, `StrataStoreExtensions.cs`: the settings and the `Add…Checkpoints` / `Add…History` registrations; `StrataRedisConnection`.
  - `StrataRunner.cs`: `StrataRunOptions`, `StrataRunner<TState>`, `AddGraph<TState>`.
  - `StrataDriver.cs`: the loop (`StrataDriver`, `WaitAfter`), `StrataDriverStatus`, `StrataHostingTelemetry`, `QueryableStore`. `StrataDrivers.cs`: the options, the four drivers and their checks.
  - `StrataHostExtensions.cs`: the two health checks, `AddInstrumentation`, `StrataInstrumentation`.
- **Rust:** `rust/strata-host/` (`datalium-strata-host`), a workspace member.
  - `host.rs`: `Host`, `WakeDriver`, `RecoveryDriver`, `RetentionDriver`, `DriverStatus`, `wait_after`, the metric names.
  - `settings.rs`: `StrataSettings` and its parts, with `open()` / `open_history()` / `config()`. `duration.rs`: durations as seconds or with a unit.
  - `health.rs`: `check_store`, `StoreHealth`, `PROBE_THREAD_ID`.
- **Not in the release workflows.** The publish jobs name their packages, and neither names these: that is SG-29's.

**Tests:**

- **Engine:** `A_Stop_Token_Wakes_No_More_Threads…` / `A_Stop_Token_Recovers_No_More_Threads…` in `EventWaitTests` and `DurableExecutionTests`, and the same two in `event_waits.rs` and `durable_execution.rs`.
- **.NET:** `Hosting/HostingTests.cs` (19), on a real host: registration and replacement, settings from configuration and their defaults, start-up refusals (a graph that doesn't compile, a store with no connection, each driver without what it needs), the SQLite, Postgres and Redis stores through the container, the runner's defaults, the wake, recovery and retention drivers, a failing round and its back-off, both ways of stopping, two hosts on one store, both health checks, and instrumentation reaching an in-memory exporter.
- **Rust:** `strata-host` (20): `tests/drivers.rs` (10) mirrors the driver tests on tokio's paused clock, with `Host::task` and `check_store`; `tests/settings.rs` (7) reads the settings through serde and opens each store; 3 unit tests (the back-off, durations, a failure's kind).
- **Totals when this landed:** .NET 833, with Postgres and Redis running, and 2 skipped (the opt-in Cloud live tests), plus the example's 1. `rust/` 768 and `cli/` 78, with `STRATA_REQUIRE_BACKENDS=1` against live Postgres and Redis, and `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` and `dotnet format --verify-no-changes` are clean.
- **Mutation checks, in both engines** (each broken, the hosting and stop-token tests run, then restored):
  - A driver that cancels the run in flight on stop fails 1 test in each.
  - A driver that stops after a failed round fails 1 in .NET and 2 in Rust (the second is `Host::task`'s, which has no .NET twin).
  - A driver that ignores its back-off fails 1 in each.
  - An engine that ignores the stop token fails 3 in each: its own two, and the host's "stopping lets the run in flight finish".

## Not built (deliberately out of scope)

- ~~**Packaging and versioning** (SG-29).~~ Built 2026-10-11 (`36-Packaging-And-Versioning.md`): `Datalium.Strata.Hosting` and `datalium-strata-host` are packages at the shared version, and the release workflows name them.
- **A run scheduler** (SG-25): global concurrency, priority, quotas, admission. Drivers run one thread at a time, as the library calls do.
- **Distributed execution** (SG-26): drivers on several replicas don't share work, they just don't collide.
- **A `StrataWorker<TState>`** that consumes a queue.
- **Several checkpoint stores in one container**, and named options per graph.
- **A Rust container, or an `axum` / `tower` integration.** `check_store` and `Host::status()` are plain values for the application's own endpoint.
- **Rust `Host` for the Cloud decision worker as a built-in driver:** `Host::task` takes its `run`. `strata-host` doesn't depend on `strata-cloud`.
- **Leader election for drivers.** None is needed for correctness; an application that wants one replica to do the sweeping runs the drivers on one.
- **Metrics descriptions and units in Rust** (`metrics::describe_*`).
