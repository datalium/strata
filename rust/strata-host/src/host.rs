//! The drivers (`design/35-Hosting-Integration.md`, "Drivers"): the loops
//! that wake due threads, recover dead runs, purge finished threads and old
//! memories (`design/45-Long-Term-Memory.md`), as tasks a [`Host`] runs until
//! a token fires.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use datalium_strata::checkpoint::{
    CheckpointQuery, CheckpointRetention, CheckpointStore, MAX_PAGE_SIZE, PurgeStates, ThreadState,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CheckpointPolicy, CompiledGraph};
use datalium_strata::memory::{MAX_LIMIT, MemoryPurge, MemoryRetention};
use datalium_strata::recovery::RecoverOptions;
use datalium_strata::scheduler::RunScheduler;
use datalium_strata::wait::WakeOptions;
use datalium_strata::{BoxFuture, CancellationToken};
use datalium_strata_memory::Clock;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::duration;

/// The counter of driver rounds, by `strata.driver` (`wake`, `recovery`,
/// `retention`, `memory-retention`, or a task's name) and `strata.outcome`
/// (`ok`, `error`).
pub const ROUNDS_METRIC: &str = "strata.host.driver.rounds";

/// The counter of threads a driver's rounds woke, recovered or purged, by
/// `strata.driver`.
pub const THREADS_METRIC: &str = "strata.host.driver.threads";

/// The longest a failing driver waits between rounds, unless its interval is
/// longer.
pub const MAX_BACKOFF: Duration = Duration::from_secs(5 * 60);

/// The wait after `failures` failed rounds in a row: the interval, doubled
/// for each, capped at [`MAX_BACKOFF`] (or the interval, if that is longer: a
/// failing driver is never faster than a healthy one).
pub fn wait_after(interval: Duration, failures: u32) -> Duration {
    if failures == 0 {
        return interval;
    }
    let cap = interval.max(MAX_BACKOFF);
    let doubled = interval.as_secs_f64() * 2f64.powi(failures.min(30) as i32);
    if doubled >= cap.as_secs_f64() {
        cap
    } else {
        Duration::from_secs_f64(doubled)
    }
}

/// Why a [`Host`] didn't start: a driver without what it needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostError(pub String);

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for HostError {}

/// The wake driver's settings (`design/29-Event-Waits-And-Timers.md`): how
/// often it looks for due timers and timeouts, and how it runs them.
#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct WakeDriver {
    /// The wait between two rounds. A timer or a timeout fires at most this
    /// long after it is due. 5 seconds by default.
    #[serde(deserialize_with = "duration::required")]
    pub interval: Duration,
    /// The most threads one listing looks at: 1 to [`MAX_PAGE_SIZE`].
    pub batch_size: usize,
    /// Only threads whose latest checkpoint's metadata has every pair.
    pub metadata: BTreeMap<String, String>,
    /// Whether a graph with an identity wakes only the threads it wrote.
    pub match_graph: bool,
    /// The priority of the runs it wakes, for the run scheduler
    /// (`design/40-Run-Scheduler.md`). Not set: the config's.
    pub priority: Option<i32>,
    #[serde(skip)]
    config: RunConfig,
}

impl Default for WakeDriver {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            batch_size: 100,
            metadata: BTreeMap::new(),
            match_graph: true,
            priority: None,
            config: RunConfig::default(),
        }
    }
}

impl WakeDriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_match_graph(mut self, match_graph: bool) -> Self {
        self.match_graph = match_graph;
        self
    }

    /// The config of every run the driver starts: its lease, services,
    /// observer, metadata and scheduler.
    pub fn with_config(mut self, config: RunConfig) -> Self {
        self.config = config;
        self
    }

    /// The priority of the runs it wakes, for the run scheduler.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    fn options(&self, stop: &CancellationToken) -> WakeOptions {
        let config = match self.priority {
            Some(priority) => self.config.clone().with_priority(priority),
            None => self.config.clone(),
        };
        let mut options = WakeOptions::new()
            .with_limit(self.batch_size)
            .with_match_graph(self.match_graph)
            .with_config(config)
            .with_stop(stop.clone());
        for (key, value) in &self.metadata {
            options = options.with_metadata(key, value);
        }
        options
    }
}

impl fmt::Debug for WakeDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WakeDriver")
            .field("interval", &self.interval)
            .field("batch_size", &self.batch_size)
            .field("metadata", &self.metadata)
            .field("match_graph", &self.match_graph)
            .field("priority", &self.priority)
            .finish_non_exhaustive()
    }
}

/// The recovery driver's settings (`design/33-Durable-Execution.md`): how
/// often it looks for runs that died, which count as dead, and how it runs
/// them.
#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct RecoveryDriver {
    /// The wait between two rounds. 30 seconds by default.
    #[serde(deserialize_with = "duration::required")]
    pub interval: Duration,
    /// The most threads recovered before the driver lists again: 1 to
    /// [`MAX_PAGE_SIZE`].
    pub batch_size: usize,
    /// How old a `running` checkpoint must be before its thread is recovered.
    /// Required when the driver's runs take no lease.
    #[serde(deserialize_with = "duration::optional")]
    pub stale_after: Option<Duration>,
    /// The most times a thread is recovered without a node committing in
    /// between.
    pub max_recoveries: Option<u32>,
    /// Only threads whose latest checkpoint's metadata has every pair.
    pub metadata: BTreeMap<String, String>,
    /// Whether a graph with an identity recovers only the threads it wrote.
    pub match_graph: bool,
    /// The priority of the runs it recovers, for the run scheduler
    /// (`design/40-Run-Scheduler.md`). Not set: the config's.
    pub priority: Option<i32>,
    #[serde(skip)]
    config: RunConfig,
}

impl Default for RecoveryDriver {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            batch_size: 100,
            stale_after: None,
            max_recoveries: None,
            metadata: BTreeMap::new(),
            match_graph: true,
            priority: None,
            config: RunConfig::default(),
        }
    }
}

impl RecoveryDriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_stale_after(mut self, age: Duration) -> Self {
        self.stale_after = Some(age);
        self
    }

    pub fn with_max_recoveries(mut self, limit: u32) -> Self {
        self.max_recoveries = Some(limit);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_match_graph(mut self, match_graph: bool) -> Self {
        self.match_graph = match_graph;
        self
    }

    /// The config of every recovery: its lease (every worker's, or none's),
    /// services, observer, metadata and scheduler.
    pub fn with_config(mut self, config: RunConfig) -> Self {
        self.config = config;
        self
    }

    /// The priority of the runs it recovers, for the run scheduler.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    fn options(&self, stop: &CancellationToken) -> RecoverOptions {
        let config = match self.priority {
            Some(priority) => self.config.clone().with_priority(priority),
            None => self.config.clone(),
        };
        let mut options = RecoverOptions::new()
            .with_limit(self.batch_size)
            .with_match_graph(self.match_graph)
            .with_config(config)
            .with_stop(stop.clone());
        if let Some(age) = self.stale_after {
            options = options.with_stale_after(age);
        }
        if let Some(limit) = self.max_recoveries {
            options = options.with_max_recoveries(limit);
        }
        for (key, value) in &self.metadata {
            options = options.with_metadata(key, value);
        }
        options
    }
}

impl fmt::Debug for RecoveryDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveryDriver")
            .field("interval", &self.interval)
            .field("batch_size", &self.batch_size)
            .field("stale_after", &self.stale_after)
            .field("max_recoveries", &self.max_recoveries)
            .field("metadata", &self.metadata)
            .field("match_graph", &self.match_graph)
            .finish_non_exhaustive()
    }
}

/// The retention driver's settings (`design/34-Storage-Hardening.md`,
/// "Retention"): how often it purges, and what.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct RetentionDriver {
    /// The wait between two rounds. An hour by default.
    #[serde(deserialize_with = "duration::required")]
    pub interval: Duration,
    /// The most threads one purge deletes: 1 to [`MAX_PAGE_SIZE`]. A round
    /// purges until one comes back short.
    pub batch_size: usize,
    /// How long a thread is kept after its latest checkpoint. No default: an
    /// application says how long it keeps its threads, and the host doesn't
    /// start without it.
    #[serde(deserialize_with = "duration::optional")]
    pub older_than: Option<Duration>,
    /// The thread states purged, by name. Empty (the default): `completed`
    /// and `ended`.
    pub states: Vec<String>,
}

impl Default for RetentionDriver {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3600),
            batch_size: MAX_PAGE_SIZE,
            older_than: None,
            states: Vec::new(),
        }
    }
}

impl RetentionDriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_older_than(mut self, age: Duration) -> Self {
        self.older_than = Some(age);
        self
    }

    pub fn with_states(mut self, states: impl IntoIterator<Item = ThreadState>) -> Self {
        self.states = states
            .into_iter()
            .map(|state| state.as_str().to_string())
            .collect();
        self
    }

    fn purge_states(&self) -> Result<PurgeStates, String> {
        if self.states.is_empty() {
            return Ok(PurgeStates::finished());
        }
        let states = self
            .states
            .iter()
            .map(|name| {
                ThreadState::parse(name).ok_or_else(|| {
                    format!("the retention driver doesn't know the thread state '{name}'")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PurgeStates::of(states))
    }
}

/// The memory retention driver's settings (`design/45-Long-Term-Memory.md`,
/// "Hosting"): how often it purges long-term memory, and what. Mirrors the
/// .NET `MemoryRetentionOptions`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct MemoryRetentionDriver {
    /// The wait between two rounds. An hour by default.
    #[serde(deserialize_with = "duration::required")]
    pub interval: Duration,
    /// The most records one purge deletes: 1 to [`MAX_LIMIT`]. A round
    /// purges until one comes back short.
    pub batch_size: usize,
    /// How long a memory is kept after it was last written. No default: an
    /// application says how long it keeps its memories, and the host doesn't
    /// start without it.
    #[serde(deserialize_with = "duration::optional")]
    pub older_than: Option<Duration>,
    /// Only this namespace's records; every namespace's when `None`.
    pub namespace: Option<String>,
    /// Only this tenant's records (`""`: the tenant-less ones); every
    /// tenant's when `None`.
    pub tenant: Option<String>,
}

impl Default for MemoryRetentionDriver {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3600),
            batch_size: MAX_LIMIT,
            older_than: None,
            namespace: None,
            tenant: None,
        }
    }
}

impl MemoryRetentionDriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_older_than(mut self, age: Duration) -> Self {
        self.older_than = Some(age);
        self
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// The purge of records last written before `older_than`.
    fn purge(&self, older_than: DateTime<Utc>) -> MemoryPurge {
        let mut purge = MemoryPurge::new(older_than).limit(self.batch_size);
        if let Some(namespace) = &self.namespace {
            purge = purge.namespace(namespace);
        }
        if let Some(tenant) = &self.tenant {
            purge = purge.tenant(tenant);
        }
        purge
    }
}

/// How a driver stands: its rounds so far, and its last failure while it
/// keeps failing. Mirrors the .NET `DriverState`.
#[derive(Debug, Clone, PartialEq)]
pub struct DriverState {
    /// The driver: `wake:<state type>`, `recovery:<state type>`, `retention`,
    /// `memory-retention`, or a task's name.
    pub name: String,
    /// Rounds finished, failed ones included.
    pub rounds: u64,
    /// How many rounds have failed since the last one that worked. 0: healthy.
    pub failures_in_a_row: u32,
    /// When the last round finished.
    pub last_round_at: Option<DateTime<Utc>>,
    /// The kind of the last failed round's failure, while
    /// `failures_in_a_row` isn't 0: an error's variant, never its message.
    pub last_failure: Option<String>,
}

/// The drivers of a [`Host`] and how each stands, for a health endpoint.
/// Cheap to clone; it keeps answering after the host has stopped.
#[derive(Debug, Clone, Default)]
pub struct DriverStatus(Arc<Mutex<BTreeMap<String, DriverState>>>);

impl DriverStatus {
    /// Every driver that has started, by name.
    pub fn drivers(&self) -> Vec<DriverState> {
        self.0.lock().unwrap().values().cloned().collect()
    }

    /// The drivers whose last round failed.
    pub fn failing(&self) -> Vec<DriverState> {
        self.drivers()
            .into_iter()
            .filter(|driver| driver.failures_in_a_row > 0)
            .collect()
    }

    fn started(&self, name: &str) {
        self.0
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert_with(|| DriverState {
                name: name.to_string(),
                rounds: 0,
                failures_in_a_row: 0,
                last_round_at: None,
                last_failure: None,
            });
    }

    fn finished(&self, name: &str, at: DateTime<Utc>, failure: Option<&str>) {
        let mut drivers = self.0.lock().unwrap();
        let Some(driver) = drivers.get_mut(name) else {
            return;
        };
        driver.rounds += 1;
        driver.last_round_at = Some(at);
        match failure {
            None => {
                driver.failures_in_a_row = 0;
                driver.last_failure = None;
            }
            Some(kind) => {
                driver.failures_in_a_row += 1;
                driver.last_failure = Some(kind.to_string());
            }
        }
    }
}

/// A failed round: the failure's kind, for the log, the metric and the
/// status. Never its message, which can hold a connection string.
struct Failure(String);

impl Failure {
    /// An error's variant or type name, from its `Debug` form.
    fn of(error: &impl fmt::Debug) -> Self {
        let debug = format!("{error:?}");
        let end = debug
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(debug.len());
        Self(debug[..end].to_string())
    }
}

type Round =
    Box<dyn FnMut(CancellationToken, Clock) -> BoxFuture<'static, Result<usize, Failure>> + Send>;

struct Driver {
    /// The metric's label: `wake`, `recovery`, `retention`, or a task's name.
    kind: String,
    name: String,
    interval: Duration,
    round: Round,
}

/// Runs the drivers as tasks until a token fires
/// (`design/35-Hosting-Integration.md`, "Drivers"). Each does a round, waits
/// its interval, and goes again. A round that fails is logged, counted and
/// recorded, and the driver waits longer ([`wait_after`]) before the next:
/// it never ends the host. When the token fires, a driver that is waiting
/// stops at once, and one that is in a round starts nothing more and lets
/// the run in flight finish: nothing is cancelled. Every replica may run
/// every driver.
pub struct Host {
    drivers: Vec<Driver>,
    refusals: Vec<String>,
    status: DriverStatus,
    clock: Clock,
    schedulers: Vec<RunScheduler>,
}

impl Default for Host {
    fn default() -> Self {
        Self::new()
    }
}

impl Host {
    pub fn new() -> Self {
        Self {
            drivers: Vec::new(),
            refusals: Vec::new(),
            status: DriverStatus::default(),
            clock: Arc::new(Utc::now),
            schedulers: Vec::new(),
        }
    }

    /// Closes `scheduler` when [`Self::run`]'s token fires
    /// (`design/40-Run-Scheduler.md`, "Closing"): a run still waiting for a
    /// slot is refused instead of holding the shutdown up, and nothing that
    /// holds one is touched.
    pub fn close_on_shutdown(mut self, scheduler: RunScheduler) -> Self {
        self.schedulers.push(scheduler);
        self
    }

    /// Decides what is due, what is old and when a round finished on `clock`
    /// instead of the system clock: how a test makes time pass. The waits
    /// between rounds are tokio's.
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Wakes `graph`'s due threads in `store`: timers whose time has come
    /// and waits whose timeout has passed.
    pub fn wake<S, T>(
        mut self,
        graph: Arc<CompiledGraph<S>>,
        store: Arc<T>,
        driver: WakeDriver,
    ) -> Self
    where
        S: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
        T: CheckpointStore + CheckpointQuery + 'static,
    {
        self.check_pacing("wake", driver.interval, driver.batch_size);
        let interval = driver.interval;
        self.add("wake", format!("wake:{}", short_name::<S>()), interval, {
            move |stop: CancellationToken, clock: Clock| {
                let (graph, store, driver) = (graph.clone(), store.clone(), driver.clone());
                Box::pin(async move {
                    let mut woken = 0;
                    loop {
                        let report = graph
                            .wake_due_with(store.clone(), clock(), driver.options(&stop))
                            .await
                            .map_err(|error| Failure::of(&error))?;
                        woken += report.woken.len();
                        // More may be due behind a full batch. A batch that
                        // woke nothing would be listed again as it is.
                        let full = report.woken.len() + report.skipped.len() >= driver.batch_size;
                        if !full || report.woken.is_empty() || stop.is_cancelled() {
                            return Ok(woken);
                        }
                    }
                })
            }
        })
    }

    /// Recovers `graph`'s `running` threads in `store` whose run died. Needs a
    /// graph compiled with [`CheckpointPolicy::EveryNode`], and either a lease
    /// in the driver's config or a `stale_after`.
    pub fn recover<S, T>(
        mut self,
        graph: Arc<CompiledGraph<S>>,
        store: Arc<T>,
        driver: RecoveryDriver,
    ) -> Self
    where
        S: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
        T: CheckpointStore + CheckpointQuery + 'static,
    {
        self.check_pacing("recovery", driver.interval, driver.batch_size);
        if graph.checkpointing() != CheckpointPolicy::EveryNode {
            self.refusals.push(format!(
                "the recovery driver for {} needs a graph compiled with CheckpointPolicy::EveryNode: only such a graph leaves a running thread to recover",
                short_name::<S>()
            ));
        }
        if driver.stale_after.is_none() && !driver.config.has_lease() {
            self.refusals.push(
                "the recovery driver needs a lease in its run config or a stale_after: without leases only the age of a running checkpoint says its run died"
                    .to_string(),
            );
        }
        let interval = driver.interval;
        self.add(
            "recovery",
            format!("recovery:{}", short_name::<S>()),
            interval,
            {
                move |stop: CancellationToken, clock: Clock| {
                    let (graph, store, driver) = (graph.clone(), store.clone(), driver.clone());
                    Box::pin(async move {
                        let mut recovered = 0;
                        loop {
                            let report = graph
                                .recover_stale(store.clone(), clock(), driver.options(&stop))
                                .await
                                .map_err(|error| Failure::of(&error))?;
                            recovered += report.recovered.len();
                            if report.recovered.len() < driver.batch_size || stop.is_cancelled() {
                                return Ok(recovered);
                            }
                        }
                    })
                }
            },
        )
    }

    /// Purges `store`'s threads that are done with. Needs an `older_than`.
    pub fn retain<T>(mut self, store: Arc<T>, driver: RetentionDriver) -> Self
    where
        T: CheckpointRetention + 'static,
    {
        self.check_pacing("retention", driver.interval, driver.batch_size);
        if driver.older_than.is_none_or(|age| age.is_zero()) {
            self.refusals.push(
                "the retention driver needs an older_than: how long a thread is kept after its latest checkpoint"
                    .to_string(),
            );
        }
        let states = match driver.purge_states() {
            Ok(states) => states,
            Err(refusal) => {
                self.refusals.push(refusal);
                PurgeStates::finished()
            }
        };
        let (interval, batch_size) = (driver.interval, driver.batch_size);
        let older_than = driver.older_than.unwrap_or_default();
        self.add("retention", "retention".to_string(), interval, {
            move |stop: CancellationToken, clock: Clock| {
                let (store, states) = (store.clone(), states.clone());
                Box::pin(async move {
                    let mut purged = 0;
                    loop {
                        let batch = store
                            .purge(clock() - older_than, &states, batch_size)
                            .await
                            .map_err(|error| Failure::of(&error))?;
                        purged += batch;
                        if batch < batch_size || stop.is_cancelled() {
                            return Ok(purged);
                        }
                    }
                })
            }
        })
    }

    /// Purges `store`'s long-term memories nobody has written for a while
    /// (`design/45-Long-Term-Memory.md`, "Retention"): each round deletes
    /// records last written before now − `older_than`, of the driver's
    /// namespace and tenant when set, oldest first, a batch at a time until a
    /// batch comes back short. Needs an `older_than`, and a namespace and
    /// tenant the store accepts.
    pub fn retain_memory<T>(mut self, store: Arc<T>, driver: MemoryRetentionDriver) -> Self
    where
        T: MemoryRetention + ?Sized + 'static,
    {
        self.check_pacing_up_to(
            "memory retention",
            driver.interval,
            driver.batch_size,
            MAX_LIMIT,
        );
        if driver.older_than.is_none_or(|age| age.is_zero()) {
            self.refusals.push(
                "the memory retention driver needs an older_than: how long a memory is kept after it was last written"
                    .to_string(),
            );
        }
        // A namespace or tenant the store would refuse fails here, not every
        // round.
        if let Err(refusal) = driver.purge(Utc::now()).validate() {
            self.refusals.push(refusal.0);
        }
        let interval = driver.interval;
        let older_than = driver.older_than.unwrap_or_default();
        self.add(
            "memory-retention",
            "memory-retention".to_string(),
            interval,
            {
                move |stop: CancellationToken, clock: Clock| {
                    let (store, driver) = (store.clone(), driver.clone());
                    Box::pin(async move {
                        let mut purged = 0;
                        loop {
                            let batch = store
                                .purge(&driver.purge(clock() - older_than))
                                .await
                                .map_err(|error| Failure::of(&error))?;
                            purged += batch;
                            if batch < driver.batch_size || stop.is_cancelled() {
                                return Ok(purged);
                            }
                        }
                    })
                }
            },
        )
    }

    /// A loop of the application's own, run beside the drivers and stopped
    /// with them, such as the Cloud decision worker's `run`. `task` is called
    /// with the host's token and should return when it fires. A task that
    /// returns an error is started again after a wait that doubles while it
    /// keeps failing; one that returns `Ok` before the token fires is started
    /// again after a second.
    pub fn task<F, Fut>(self, name: impl Into<String>, mut task: F) -> Self
    where
        F: FnMut(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        let name = name.into();
        self.add(&name.clone(), name, Duration::from_secs(1), {
            move |stop: CancellationToken, _clock: Clock| {
                let running = task(stop);
                Box::pin(async move {
                    running
                        .await
                        .map(|()| 0)
                        .map_err(|_| Failure("TaskFailed".to_string()))
                })
            }
        })
    }

    /// How each driver stands, for a health endpoint. Take it before
    /// [`Self::run`], which consumes the host.
    pub fn status(&self) -> DriverStatus {
        self.status.clone()
    }

    /// Runs every driver until `shutdown` fires, and returns once each has
    /// let its round finish. Refuses, having started nothing, when a driver
    /// lacks what it needs.
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), HostError> {
        if let Some(refusal) = self.refusals.into_iter().next() {
            return Err(HostError(refusal));
        }
        let schedulers = self.schedulers;
        let closing = {
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                shutdown.cancelled().await;
                for scheduler in schedulers {
                    scheduler.close();
                }
            })
        };
        let tasks: Vec<_> = self
            .drivers
            .into_iter()
            .map(|driver| {
                tokio::spawn(drive(
                    driver,
                    self.status.clone(),
                    self.clock.clone(),
                    shutdown.clone(),
                ))
            })
            .collect();
        for task in tasks {
            if let Err(error) = task.await {
                tracing::error!(error.type = "panic", "a Strata driver ended: {error}");
            }
        }
        let _ = closing.await;
        Ok(())
    }

    fn add(
        mut self,
        kind: &str,
        name: String,
        interval: Duration,
        round: impl FnMut(CancellationToken, Clock) -> BoxFuture<'static, Result<usize, Failure>>
        + Send
        + 'static,
    ) -> Self {
        self.drivers.push(Driver {
            kind: kind.to_string(),
            name,
            interval,
            round: Box::new(round),
        });
        self
    }

    fn check_pacing(&mut self, driver: &str, interval: Duration, batch_size: usize) {
        self.check_pacing_up_to(driver, interval, batch_size, MAX_PAGE_SIZE);
    }

    fn check_pacing_up_to(
        &mut self,
        driver: &str,
        interval: Duration,
        batch_size: usize,
        most: usize,
    ) {
        if interval.is_zero() {
            self.refusals.push(format!(
                "the {driver} driver's interval must be more than zero"
            ));
        }
        if !(1..=most).contains(&batch_size) {
            self.refusals.push(format!(
                "the {driver} driver's batch_size must be between 1 and {most}"
            ));
        }
    }
}

/// One driver's loop.
async fn drive(
    mut driver: Driver,
    status: DriverStatus,
    clock: Clock,
    shutdown: CancellationToken,
) {
    status.started(&driver.name);
    tracing::info!(
        strata.driver = driver.name.as_str(),
        "Strata driver started, every {:?}",
        driver.interval
    );
    let mut failures = 0;
    while !shutdown.is_cancelled() {
        match (driver.round)(shutdown.clone(), clock.clone()).await {
            Ok(threads) => {
                failures = 0;
                status.finished(&driver.name, clock(), None);
                metrics::counter!(ROUNDS_METRIC, "strata.driver" => driver.kind.clone(), "strata.outcome" => "ok")
                    .increment(1);
                if threads > 0 {
                    metrics::counter!(THREADS_METRIC, "strata.driver" => driver.kind.clone())
                        .increment(threads as u64);
                    tracing::info!(
                        strata.driver = driver.name.as_str(),
                        "Strata driver handled {threads} thread(s)"
                    );
                }
            }
            Err(Failure(kind)) => {
                failures += 1;
                status.finished(&driver.name, clock(), Some(&kind));
                metrics::counter!(ROUNDS_METRIC, "strata.driver" => driver.kind.clone(), "strata.outcome" => "error")
                    .increment(1);
                tracing::warn!(
                    strata.driver = driver.name.as_str(),
                    error.type = kind.as_str(),
                    "Strata driver failed a round; {failures} in a row"
                );
            }
        }
        tokio::select! {
            () = shutdown.cancelled() => break,
            () = tokio::time::sleep(wait_after(driver.interval, failures)) => {}
        }
    }
    tracing::info!(
        strata.driver = driver.name.as_str(),
        "Strata driver stopped"
    );
}

/// A type's name without its path.
fn short_name<S>() -> &'static str {
    let name = std::any::type_name::<S>();
    name.rsplit("::").next().unwrap_or(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failing_driver_waits_twice_as_long_each_time_up_to_a_cap() {
        let five = Duration::from_secs(5);
        assert_eq!(wait_after(five, 0), five);
        assert_eq!(wait_after(five, 1), Duration::from_secs(10));
        assert_eq!(wait_after(five, 2), Duration::from_secs(20));
        assert_eq!(wait_after(five, 5), Duration::from_secs(160));
        assert_eq!(wait_after(five, 6), MAX_BACKOFF);
        assert_eq!(wait_after(five, 1000), MAX_BACKOFF);
        // An interval longer than the cap is its own cap: a failing driver is
        // never faster than a healthy one.
        let hour = Duration::from_secs(3600);
        assert_eq!(wait_after(hour, 3), hour);
    }

    #[test]
    fn a_failure_is_named_by_its_kind_never_its_message() {
        #[derive(Debug)]
        #[allow(dead_code)]
        enum Example {
            Refused(String),
            Lost { secret: String },
            Plain,
        }
        assert_eq!(
            Failure::of(&Example::Refused("password=hunter2".to_string())).0,
            "Refused"
        );
        assert_eq!(
            Failure::of(&Example::Lost {
                secret: "hunter2".to_string()
            })
            .0,
            "Lost"
        );
        assert_eq!(Failure::of(&Example::Plain).0, "Plain");
        assert_eq!(short_name::<Example>(), "Example");
    }
}
