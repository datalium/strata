//! Hosting Datalium Strata in a service (`design/35-Hosting-Integration.md`):
//! the loops every application would otherwise write itself, the settings
//! that configure them, and a probe for a readiness endpoint. The Rust
//! counterpart of the .NET `Datalium.Strata.Hosting` package.
//!
//! - [`Host`] runs the **drivers** as tasks until a token fires: waking due
//!   timers and timeouts ([`Host::wake`], `design/29`), recovering runs whose
//!   process died ([`Host::recover`], `design/33`), purging finished threads
//!   ([`Host::retain`], `design/34`), purging old long-term memories
//!   ([`Host::retain_memory`], `design/45`), and any loop of the application's own
//!   ([`Host::task`], such as the Cloud decision worker's `run`). A driver
//!   that fails a round logs, counts, backs off and goes on. When the token
//!   fires a driver starts nothing more and lets the run in flight finish:
//!   nothing is cancelled.
//! - [`StrataSettings`] is the same configuration as `serde` structs, read
//!   from whatever format the application uses, with `open()` on each
//!   store's (and `open_memory()` for long-term memory).
//! - [`check_store`] says whether a checkpoint store answers, and
//!   [`Host::status`] how each driver stands.
//!
//! What .NET has and this crate doesn't, because Rust needs neither: a
//! container to register in ([`RunConfig::with_service`] already carries
//! services to nodes), and an instrumentation call (`tracing` and `metrics`
//! are global facades: install a subscriber and a recorder and Strata's spans
//! and metrics arrive).
//!
//! [`RunConfig::with_service`]: datalium_strata::context::RunConfig::with_service

mod duration;
mod health;
mod host;
mod settings;

pub use health::{PROBE_THREAD_ID, StoreHealth, check_store};
pub use host::{
    DriverState, DriverStatus, Host, HostError, MAX_BACKOFF, MemoryRetentionDriver, ROUNDS_METRIC,
    RecoveryDriver, RetentionDriver, THREADS_METRIC, WakeDriver, wait_after,
};
pub use settings::{
    DeltaSettings, GroupCommitSettings, LeaseSettings, MemoryIndexSettings, MemorySettings,
    OffloadSettings, PostgresSettings, RedisSettings, ReservationSettings, RetrySettings,
    RunSettings, SchedulerSettings, SqliteSettings, StrataSettings,
};
