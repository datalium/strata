//! Strata's settings as `serde` structs
//! (`design/35-Hosting-Integration.md`, "Configuration"): read them from
//! whatever format the application uses (a TOML file, JSON, the environment
//! through a config crate), then `open()` the stores they describe. The
//! counterpart of the .NET package's options bound from the `Strata`
//! configuration section; the keys here are `snake_case`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{CheckpointStoreError, LeaseOptions};
use datalium_strata::context::RunConfig;
use datalium_strata::history::HistoryStoreError;
use datalium_strata::memory::{InMemoryMemoryStore, MAX_DIMENSIONS, MemoryStoreError};
use datalium_strata::scheduler::{RunScheduler, SchedulerOptions};
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use datalium_strata_memory::history::SqliteHistoryStore;
use datalium_strata_memory::postgres_store::{
    DEFAULT_POOL_SIZE, PostgresCheckpointStore, PostgresHistoryStore,
};
use datalium_strata_memory::redis_store::{
    DEFAULT_PREFIX, RedisCheckpointStore, RedisHistoryStore,
};
use datalium_strata_memory::{
    Deltas, FileSystemBlobStore, GroupCommit, MemoryIndex, Offload, PostgresMemoryOptions,
    PostgresMemoryStore, PostgresStoreOptions, RedisMemoryOptions, RedisMemoryStore,
    RedisStoreOptions, SqliteMemoryOptions, SqliteMemoryStore, SqliteStoreOptions, StoreRetry,
};
use serde::Deserialize;

use crate::duration;
use crate::host::{MemoryRetentionDriver, RecoveryDriver, RetentionDriver, WakeDriver};

/// Everything a host is configured with. Every part has defaults, so a
/// source need only say what differs.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct StrataSettings {
    pub postgres: PostgresSettings,
    pub redis: RedisSettings,
    pub sqlite: SqliteSettings,
    /// The defaults of every run the host starts.
    pub runs: RunSettings,
    /// The run scheduler (`design/40-Run-Scheduler.md`), when the source has
    /// a `scheduler` section.
    pub scheduler: Option<SchedulerSettings>,
    pub wake: WakeDriver,
    pub recovery: RecoveryDriver,
    pub retention: RetentionDriver,
    /// Long-term memory (`design/45-Long-Term-Memory.md`): what each store's
    /// `open_memory` opens, and the memory retention driver.
    pub memory: MemorySettings,
}

/// How a Postgres or Redis store repeats a call that failed for a transient
/// reason (`design/34-Storage-Hardening.md`, "Retries"). The defaults are
/// [`StoreRetry`]'s.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct RetrySettings {
    /// How many times a call is made at most, the first included.
    pub max_attempts: u32,
    /// The wait before the first repeat, doubled before each later one.
    #[serde(deserialize_with = "duration::required")]
    pub initial_delay: Duration,
    /// The longest wait between two attempts.
    #[serde(deserialize_with = "duration::required")]
    pub max_delay: Duration,
}

impl Default for RetrySettings {
    fn default() -> Self {
        let policy = StoreRetry::new();
        Self {
            max_attempts: policy.max_attempts(),
            initial_delay: policy.initial_delay(),
            max_delay: policy.max_delay(),
        }
    }
}

impl RetrySettings {
    pub fn policy(&self) -> StoreRetry {
        StoreRetry::new()
            .with_max_attempts(self.max_attempts)
            .with_initial_delay(self.initial_delay)
            .with_max_delay(self.max_delay)
    }
}

/// Group commit as settings say it (`design/41-Checkpoint-Efficiency.md`):
/// off unless `enabled`; the other defaults are [`GroupCommit`]'s.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct GroupCommitSettings {
    /// Whether the checkpoint store flushes concurrent saves together.
    pub enabled: bool,
    /// The most saves one flush takes.
    pub max_batch: usize,
    /// How many flushes may run at once (the SQLite store always runs one).
    pub max_concurrent_flushes: usize,
}

impl Default for GroupCommitSettings {
    fn default() -> Self {
        let defaults = GroupCommit::new();
        Self {
            enabled: false,
            max_batch: defaults.max_batch(),
            max_concurrent_flushes: defaults.max_concurrent_flushes(),
        }
    }
}

impl GroupCommitSettings {
    /// The group commit to run, or `None` when it is off.
    pub fn group_commit(&self) -> Option<GroupCommit> {
        self.enabled.then(|| {
            GroupCommit::new()
                .with_max_batch(self.max_batch)
                .with_max_concurrent_flushes(self.max_concurrent_flushes)
        })
    }
}

/// Delta checkpoints as settings say them
/// (`design/41-Checkpoint-Efficiency.md`, "Deltas"): off unless `enabled`;
/// the other defaults are [`Deltas`]'.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct DeltaSettings {
    /// Whether the checkpoint store stores running checkpoints as the changes
    /// to the row before.
    pub enabled: bool,
    /// Every this many rows of a thread one is stored whole.
    pub full_every: u32,
    /// How many threads' latest rows the store remembers.
    pub cached_threads: usize,
}

impl Default for DeltaSettings {
    fn default() -> Self {
        let defaults = Deltas::new();
        Self {
            enabled: false,
            full_every: defaults.full_every(),
            cached_threads: defaults.cached_threads(),
        }
    }
}

impl DeltaSettings {
    /// The deltas to write, or `None` when they are off.
    pub fn deltas(&self) -> Option<Deltas> {
        self.enabled.then(|| {
            Deltas::new()
                .with_full_every(self.full_every)
                .with_cached_threads(self.cached_threads)
        })
    }
}

/// Value offload as settings say it (`design/41-Checkpoint-Efficiency.md`,
/// "Offload"): off unless `enabled`; the blobs go to a
/// [`FileSystemBlobStore`] at `path`, or to the store's own database. Another
/// blob store is given in code, with the store's options.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct OffloadSettings {
    /// Whether the checkpoint store stores large values once, in a blob store,
    /// and refers to them.
    pub enabled: bool,
    /// The bytes of a value's JSON from which it is offloaded.
    pub threshold: usize,
    /// The directory of a filesystem blob store, instead of the store's own
    /// database.
    pub path: Option<String>,
}

impl Default for OffloadSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            threshold: Offload::new().threshold(),
            path: None,
        }
    }
}

impl OffloadSettings {
    /// The offload to do, or `None` when it is off.
    pub fn offload(&self) -> Option<Offload> {
        self.enabled.then(|| {
            let offload = Offload::new().with_threshold(self.threshold);
            match &self.path {
                Some(path) => offload.with_blobs(Arc::new(FileSystemBlobStore::new(path))),
                None => offload,
            }
        })
    }
}

/// The Postgres stores' settings.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct PostgresSettings {
    /// `postgres://user:pass@host:port/db`.
    pub url: Option<String>,
    /// Whether a store brings its tables up to date when it is opened (the
    /// default). When `false` it only checks them, and refuses a database
    /// that is behind.
    pub auto_migrate: bool,
    pub retry: RetrySettings,
    /// How many connections each store keeps at most.
    pub pool_size: usize,
    pub group_commit: GroupCommitSettings,
    pub deltas: DeltaSettings,
    pub offload: OffloadSettings,
}

impl Default for PostgresSettings {
    fn default() -> Self {
        Self {
            url: None,
            auto_migrate: true,
            retry: RetrySettings::default(),
            pool_size: DEFAULT_POOL_SIZE,
            group_commit: GroupCommitSettings::default(),
            deltas: DeltaSettings::default(),
            offload: OffloadSettings::default(),
        }
    }
}

impl PostgresSettings {
    pub fn options(&self) -> PostgresStoreOptions {
        let mut options = PostgresStoreOptions::new()
            .with_retry(self.retry.policy())
            .with_auto_migrate(self.auto_migrate)
            .with_pool_size(self.pool_size);
        if let Some(group_commit) = self.group_commit.group_commit() {
            options = options.with_group_commit(group_commit);
        }
        if let Some(deltas) = self.deltas.deltas() {
            options = options.with_deltas(deltas);
        }
        if let Some(offload) = self.offload.offload() {
            options = options.with_offload(offload);
        }
        options
    }

    /// Connects, and migrates or checks the checkpoint tables: a database
    /// that is behind, or can't be reached, is the error.
    pub async fn open(&self) -> Result<PostgresCheckpointStore, CheckpointStoreError> {
        let url = self.url.as_deref().ok_or_else(|| {
            CheckpointStoreError("Strata's Postgres store has no url: set postgres.url".to_string())
        })?;
        PostgresCheckpointStore::connect_with(url, self.options()).await
    }

    /// [`Self::open`] for the history store.
    pub async fn open_history(&self) -> Result<PostgresHistoryStore, HistoryStoreError> {
        let url = self.url.as_deref().ok_or_else(|| {
            HistoryStoreError("Strata's Postgres store has no url: set postgres.url".to_string())
        })?;
        PostgresHistoryStore::connect_with(url, self.options()).await
    }

    /// The memory store `memory` describes on this database
    /// (`design/45-Long-Term-Memory.md`), with this section's `auto_migrate`,
    /// `retry` and `pool_size`: connects, and creates or checks its table
    /// (`auto_migrate` off: only checks). A database that is behind or newer,
    /// a table of other dimensions, or no pgvector is the error.
    pub async fn open_memory(
        &self,
        memory: &MemorySettings,
    ) -> Result<PostgresMemoryStore, MemoryStoreError> {
        let options = self.memory_options(memory)?;
        let url = self.url.as_deref().ok_or_else(|| {
            MemoryStoreError("Strata's Postgres store has no url: set postgres.url".to_string())
        })?;
        PostgresMemoryStore::connect(url, options).await
    }

    /// The options [`Self::open_memory`] opens the store with.
    pub fn memory_options(
        &self,
        memory: &MemorySettings,
    ) -> Result<PostgresMemoryOptions, MemoryStoreError> {
        let mut options = PostgresMemoryOptions::new(memory.required_dimensions()?)
            .with_index(memory.index.index()?)
            .with_retry(self.retry.policy())
            .with_auto_migrate(self.auto_migrate)
            .with_pool_size(self.pool_size);
        if let Some(table) = &memory.table {
            options = options.with_table(table);
        }
        Ok(options)
    }
}

/// The Redis stores' settings.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct RedisSettings {
    /// `redis://[:password@]host[:port][/db]`.
    pub url: Option<String>,
    /// The key namespace: keys are `{prefix}:checkpoints:…`.
    pub prefix: String,
    /// Lets a thread's keys expire this long after it completes or ends
    /// (`design/34-Storage-Hardening.md`, "Retention"). Off unless set.
    #[serde(deserialize_with = "duration::optional")]
    pub finished_thread_ttl: Option<Duration>,
    pub retry: RetrySettings,
    pub deltas: DeltaSettings,
    pub offload: OffloadSettings,
}

impl Default for RedisSettings {
    fn default() -> Self {
        Self {
            url: None,
            prefix: DEFAULT_PREFIX.to_string(),
            finished_thread_ttl: None,
            retry: RetrySettings::default(),
            deltas: DeltaSettings::default(),
            offload: OffloadSettings::default(),
        }
    }
}

impl RedisSettings {
    pub fn options(&self) -> RedisStoreOptions {
        let mut options = RedisStoreOptions::new()
            .with_prefix(&self.prefix)
            .with_retry(self.retry.policy());
        if let Some(ttl) = self.finished_thread_ttl {
            options = options.with_finished_thread_ttl(ttl);
        }
        if let Some(deltas) = self.deltas.deltas() {
            options = options.with_deltas(deltas);
        }
        if let Some(offload) = self.offload.offload() {
            options = options.with_offload(offload);
        }
        options
    }

    /// Connects, and indexes a prefix an older runtime wrote to.
    pub async fn open(&self) -> Result<RedisCheckpointStore, CheckpointStoreError> {
        let url = self.url.as_deref().ok_or_else(|| {
            CheckpointStoreError("Strata's Redis store has no url: set redis.url".to_string())
        })?;
        RedisCheckpointStore::connect_with(url, self.options()).await
    }

    /// [`Self::open`] for the history store.
    pub async fn open_history(&self) -> Result<RedisHistoryStore, HistoryStoreError> {
        let url = self.url.as_deref().ok_or_else(|| {
            HistoryStoreError("Strata's Redis store has no url: set redis.url".to_string())
        })?;
        RedisHistoryStore::connect_with(url, self.options()).await
    }

    /// The memory store `memory` describes on this server
    /// (`design/45-Long-Term-Memory.md`): under `memory.prefix` (when set and
    /// not blank), else this section's `prefix`, with its `retry`. Connects
    /// and opens the store: a prefix a newer build moved on or made with
    /// other dimensions, or a server without the Query Engine, is the error.
    pub async fn open_memory(
        &self,
        memory: &MemorySettings,
    ) -> Result<RedisMemoryStore, MemoryStoreError> {
        let options = self.memory_options(memory)?;
        let url = self.url.as_deref().ok_or_else(|| {
            MemoryStoreError("Strata's Redis store has no url: set redis.url".to_string())
        })?;
        RedisMemoryStore::connect(url, options).await
    }

    /// The options [`Self::open_memory`] opens the store with.
    pub fn memory_options(
        &self,
        memory: &MemorySettings,
    ) -> Result<RedisMemoryOptions, MemoryStoreError> {
        let prefix = memory
            .prefix
            .as_deref()
            .filter(|prefix| !prefix.trim().is_empty())
            .unwrap_or(&self.prefix);
        Ok(RedisMemoryOptions::new(memory.required_dimensions()?)
            .with_prefix(prefix)
            .with_index(memory.index.index()?)
            .with_retry(self.retry.policy()))
    }
}

/// The SQLite stores' settings.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct SqliteSettings {
    /// The database file. The checkpoint and the history store share it.
    pub path: Option<String>,
    pub group_commit: GroupCommitSettings,
    pub deltas: DeltaSettings,
    pub offload: OffloadSettings,
}

impl SqliteSettings {
    fn file(&self) -> Result<&str, String> {
        self.path
            .as_deref()
            .ok_or_else(|| "Strata's SQLite store has no file: set sqlite.path".to_string())
    }

    /// Opens the file, bringing its checkpoint tables up to date.
    pub fn open(&self) -> Result<SqliteCheckpointStore, CheckpointStoreError> {
        let mut options = SqliteStoreOptions::new();
        if let Some(group_commit) = self.group_commit.group_commit() {
            options = options.with_group_commit(group_commit);
        }
        if let Some(deltas) = self.deltas.deltas() {
            options = options.with_deltas(deltas);
        }
        if let Some(offload) = self.offload.offload() {
            options = options.with_offload(offload);
        }
        SqliteCheckpointStore::open_with(self.file().map_err(CheckpointStoreError)?, options)
    }

    /// [`Self::open`] for the history store.
    pub fn open_history(&self) -> Result<SqliteHistoryStore, HistoryStoreError> {
        SqliteHistoryStore::open(self.file().map_err(HistoryStoreError)?)
    }

    /// The memory store `memory` describes in this file, the checkpoint
    /// store's (`design/45-Long-Term-Memory.md`): opens it and creates or
    /// checks its tables. A table of other dimensions, or a file a newer
    /// build has moved on, is the error. sqlite-vec is compiled in; the index
    /// settings don't apply (SQLite always searches exactly).
    pub fn open_memory(
        &self,
        memory: &MemorySettings,
    ) -> Result<SqliteMemoryStore, MemoryStoreError> {
        let dimensions = memory.required_dimensions()?;
        memory.index.index()?;
        let mut options = SqliteMemoryOptions::new(dimensions);
        if let Some(table) = &memory.table {
            options = options.with_table(table);
        }
        SqliteMemoryStore::open(self.file().map_err(MemoryStoreError)?, options)
    }
}

/// Long-term memory's settings (`design/45-Long-Term-Memory.md`, "Hosting"):
/// the store each section's `open_memory` opens (`postgres`, `sqlite`,
/// `redis`; [`Self::open_in_memory`] for none), and the memory retention
/// driver ([`Host::retain_memory`](crate::Host::retain_memory)). The
/// counterpart of the .NET `Strata:Memory` section, without three of its
/// keys: `extension` (sqlite-vec is compiled into the Rust store), and
/// `model` and `tenant_key` (the memory helper is `datalium-strata-ai`'s
/// `Memory`, which this crate doesn't depend on: the application builds it
/// over the store, with `with_model` and `with_tenant_key`).
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct MemorySettings {
    /// The vectors' length. Required to open a store: it must match the
    /// embedding model's.
    pub dimensions: Option<usize>,
    /// How Postgres and Redis find the nearest records; exact by default.
    pub index: MemoryIndexSettings,
    /// The Postgres or SQLite table; the store's default (`memory_records`)
    /// when not set.
    pub table: Option<String>,
    /// The Redis key namespace; `redis.prefix` when not set or blank.
    pub prefix: Option<String>,
    pub retention: MemoryRetentionDriver,
}

impl MemorySettings {
    /// An in-memory store of these dimensions: for tests and development.
    pub fn open_in_memory(&self) -> Result<InMemoryMemoryStore, MemoryStoreError> {
        let dimensions = self.required_dimensions()?;
        self.index.index()?;
        if !(1..=MAX_DIMENSIONS).contains(&dimensions) {
            return Err(MemoryStoreError(format!(
                "a memory store has 1 to {MAX_DIMENSIONS} dimensions, not {dimensions}"
            )));
        }
        Ok(InMemoryMemoryStore::new(dimensions))
    }

    /// The dimensions, or the refusal every `open_memory` makes without them
    /// (0 counts as none).
    fn required_dimensions(&self) -> Result<usize, MemoryStoreError> {
        self.dimensions
            .filter(|&dimensions| dimensions > 0)
            .ok_or_else(|| {
                MemoryStoreError(
                    "Strata's memory store has no dimensions: set memory.dimensions".to_string(),
                )
            })
    }
}

/// A memory store's index (`design/45-Long-Term-Memory.md`, "Index"):
/// `kind` `none` (exact, the default) or `hnsw` with its build parameters
/// (pgvector's defaults, 16 and 64). Postgres and Redis use it; SQLite and
/// the in-memory store always search exactly.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct MemoryIndexSettings {
    pub kind: String,
    pub m: u32,
    pub ef_construction: u32,
}

impl Default for MemoryIndexSettings {
    fn default() -> Self {
        Self {
            kind: "none".to_string(),
            m: 16,
            ef_construction: 64,
        }
    }
}

impl MemoryIndexSettings {
    /// The index, or the refusal of a kind that isn't `none` or `hnsw` (in
    /// any case, as .NET's enum binds).
    pub fn index(&self) -> Result<MemoryIndex, MemoryStoreError> {
        match self.kind.to_ascii_lowercase().as_str() {
            "none" => Ok(MemoryIndex::None),
            "hnsw" => Ok(MemoryIndex::Hnsw {
                m: self.m,
                ef_construction: self.ef_construction,
            }),
            other => Err(MemoryStoreError(format!(
                "Strata's memory index kind '{other}' is unknown: use none or hnsw"
            ))),
        }
    }
}

/// The defaults of every run the host starts.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct RunSettings {
    /// The lease every run holds its thread under
    /// (`design/32-Thread-Leases.md`), when it has a ttl.
    pub lease: LeaseSettings,
}

impl RunSettings {
    /// A run config with these defaults: the lease, when it has a ttl. Add
    /// services, an observer and metadata to it as the application needs.
    pub fn config(&self) -> RunConfig {
        match self.lease.ttl {
            Some(ttl) => {
                RunConfig::new().with_lease(LeaseOptions::new(self.lease.owner.clone(), ttl))
            }
            None => RunConfig::new(),
        }
    }

    /// [`Self::config`], with every run waiting for a slot of `scheduler`
    /// (`design/40-Run-Scheduler.md`).
    pub fn config_with(&self, scheduler: &RunScheduler) -> RunConfig {
        self.config().with_scheduler(scheduler.clone())
    }
}

/// The run scheduler's settings (`design/40-Run-Scheduler.md`, "Hosting"):
/// [`SchedulerOptions`] as settings can say them. The defaults are its.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct SchedulerSettings {
    /// Names the scheduler on its metrics.
    pub name: String,
    /// How many runs may hold a slot at once.
    pub max_concurrent: u32,
    /// How many runs may wait for a slot.
    pub max_queued: u32,
    /// How long a run may wait for a slot. Not set: as long as it takes.
    #[serde(deserialize_with = "duration::optional")]
    pub max_queue_time: Option<Duration>,
    /// Slots kept for higher priorities.
    pub reservations: Vec<ReservationSettings>,
    /// The metadata key that names a run's tenant.
    pub tenant_key: String,
    /// How many runs of one tenant may hold slots at once. Not set: no limit.
    pub tenant_limit: Option<u32>,
    /// Limits of particular tenants, in place of `tenant_limit`. 0 pauses one.
    pub tenant_limits: HashMap<String, u32>,
    /// How many runs of one tenant may wait. Not set: no limit.
    pub tenant_max_queued: Option<u32>,
}

/// A reservation of slots for a priority and above, as settings can say it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
pub struct ReservationSettings {
    pub min_priority: i32,
    pub slots: u32,
}

impl Default for SchedulerSettings {
    fn default() -> Self {
        let options = SchedulerOptions::default();
        Self {
            name: options.name().to_string(),
            max_concurrent: options.max_concurrent(),
            max_queued: options.max_queued(),
            max_queue_time: options.max_queue_time(),
            reservations: Vec::new(),
            tenant_key: options.tenant_key().to_string(),
            tenant_limit: None,
            tenant_limits: HashMap::new(),
            tenant_max_queued: None,
        }
    }
}

impl SchedulerSettings {
    pub fn options(&self) -> SchedulerOptions {
        let mut options = SchedulerOptions::new(self.max_concurrent)
            .with_name(&self.name)
            .with_max_queued(self.max_queued)
            .with_tenant_key(&self.tenant_key);
        if let Some(time) = self.max_queue_time {
            options = options.with_max_queue_time(time);
        }
        for reservation in &self.reservations {
            options = options.with_reservation(reservation.min_priority, reservation.slots);
        }
        if let Some(limit) = self.tenant_limit {
            options = options.with_tenant_limit(limit);
        }
        for (tenant, limit) in &self.tenant_limits {
            options = options.with_tenant_override(tenant, *limit);
        }
        if let Some(max) = self.tenant_max_queued {
            options = options.with_tenant_max_queued(max);
        }
        options
    }

    /// The scheduler: invalid settings are the error.
    pub fn open(&self) -> Result<RunScheduler, StrataError> {
        RunScheduler::new(self.options())
    }
}

/// A run's lease, as settings can say it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct LeaseSettings {
    /// Who holds the threads this process runs. The host name and the
    /// process id by default.
    pub owner: String,
    /// How long a lease lasts unless renewed. Not set (the default): runs
    /// take no lease. Every worker on a thread uses leases, or none does.
    #[serde(deserialize_with = "duration::optional")]
    pub ttl: Option<Duration>,
}

impl Default for LeaseSettings {
    fn default() -> Self {
        let host = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "host".to_string());
        Self {
            owner: format!("{host}/{}", std::process::id()),
            ttl: None,
        }
    }
}
