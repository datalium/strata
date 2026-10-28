//! Running a thread under a lease (`design/32-Thread-Leases.md`, SG-12):
//! [`LeaseOptions`], what a caller passes to
//! [`RunConfig::with_lease`](crate::context::RunConfig::with_lease), and the
//! engine's side of it. The lease itself lives in the checkpoint store
//! ([`CheckpointStore::try_acquire_lease`] and its neighbours), so a save and
//! the check that fences it are one atomic step.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::observability::RunTelemetry;
use crate::schema::Checkpoint;
use crate::store::{
    CheckpointStore, CheckpointStoreError, LeaseAttempt, LeasedSave, MINIMUM_LEASE_TTL, ThreadLease,
};

/// Runs a thread under a lease, so one owner at a time works on it and a
/// stale owner's saves are refused. The run acquires the thread's lease
/// before its first save, renews it every third of the ttl on tokio's clock,
/// saves under it, and releases it when it returns. A thread another owner
/// holds is refused; a run that loses its lease stops with
/// [`CancelReason::LeaseLost`](crate::checkpoint::CancelReason::LeaseLost)
/// and writes nothing more. Mirrors the .NET engine's `LeaseOptions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseOptions {
    owner: String,
    ttl: Duration,
}

impl LeaseOptions {
    /// `owner` says who holds the thread, for people and logs: name the
    /// process, not the service, since a thread held under one name is
    /// refused to another process using the same name. `ttl` is how long the
    /// lease lasts unless renewed: how long a thread stays held after its
    /// owner dies. A blank owner or a ttl below one millisecond refuses the
    /// run.
    pub fn new(owner: impl Into<String>, ttl: Duration) -> Self {
        Self {
            owner: owner.into(),
            ttl,
        }
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// What refuses the run before it starts, if anything.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.owner.trim().is_empty() {
            return Err("a run's lease needs an owner".to_string());
        }
        if self.ttl < MINIMUM_LEASE_TTL {
            return Err("a run's lease ttl must be at least 1 millisecond".to_string());
        }
        Ok(())
    }
}

/// What the run and its renewal task share.
struct Held {
    /// The lease as last acquired or renewed; `None` until acquired.
    current: Mutex<Option<ThreadLease>>,
    lost: AtomicBool,
    /// The run's root token: losing the lease cancels the run.
    run: CancellationToken,
    telemetry: Arc<RunTelemetry>,
}

impl Held {
    fn current(&self) -> Option<ThreadLease> {
        self.current
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn set(&self, lease: Option<ThreadLease>) {
        *self
            .current
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = lease;
    }

    /// The lease is gone: say so once, and stop the run.
    fn lose(&self, lease: &ThreadLease) {
        if !self.lost.swap(true, Ordering::SeqCst) {
            self.telemetry.lease_lost(lease);
            self.run.cancel();
        }
    }
}

/// One run's lease on its thread (`design/32-Thread-Leases.md`, "What a run
/// does"): acquired before the run's first save, renewed in the background,
/// carried by every save, released when the run returns. When it is lost (a
/// renewal says so, a save is refused, or no renewal succeeded for a whole
/// ttl) the run's token is cancelled, and every later save writes nothing.
pub(crate) struct RunLease {
    options: LeaseOptions,
    held: Arc<Held>,
    /// Stops the renewal task.
    stop: CancellationToken,
    renewal: Mutex<Option<Renewal>>,
}

/// The task renewing an acquired lease, and the store it was acquired from.
struct Renewal {
    store: Arc<dyn CheckpointStore>,
    task: JoinHandle<()>,
}

impl RunLease {
    pub(crate) fn new(
        options: LeaseOptions,
        run: CancellationToken,
        telemetry: Arc<RunTelemetry>,
    ) -> Self {
        Self {
            options,
            held: Arc::new(Held {
                current: Mutex::new(None),
                lost: AtomicBool::new(false),
                run,
                telemetry,
            }),
            stop: CancellationToken::new(),
            renewal: Mutex::new(None),
        }
    }

    pub(crate) fn is_lost(&self) -> bool {
        self.held.lost.load(Ordering::SeqCst)
    }

    /// Takes `thread_id`'s lease from `store` and starts renewing it. Returns
    /// the lease that holds the thread when it is held, `None` once this run
    /// holds it. A second call changes nothing.
    pub(crate) async fn acquire(
        &self,
        store: &Arc<dyn CheckpointStore>,
        thread_id: &str,
    ) -> Result<Option<ThreadLease>, CheckpointStoreError> {
        if self.held.current().is_some() {
            return Ok(None);
        }
        let attempt = store
            .try_acquire_lease(thread_id, &self.options.owner, self.options.ttl)
            .await?;
        let lease = match attempt {
            LeaseAttempt::Acquired(lease) => lease,
            LeaseAttempt::Held(holder) => {
                RunTelemetry::lease_held();
                return Ok(Some(holder));
            }
        };
        self.held.telemetry.lease_acquired(&lease);
        self.held.set(Some(lease));
        let task = tokio::spawn(renew(
            store.clone(),
            self.held.clone(),
            self.options.ttl,
            self.stop.clone(),
        ));
        *self
            .renewal
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(Renewal {
            store: store.clone(),
            task,
        });
        Ok(None)
    }

    /// Saves `checkpoint` under the lease, conditionally when there is an
    /// `expected` sequence. Once the lease is lost nothing is written, by this
    /// save or any later one.
    pub(crate) async fn save(
        &self,
        store: &Arc<dyn CheckpointStore>,
        checkpoint: Checkpoint,
        expected: Option<i64>,
    ) -> Result<LeasedSave, CheckpointStoreError> {
        if self.is_lost() {
            return Ok(LeasedSave::LeaseLost);
        }
        let Some(lease) = self.held.current() else {
            return Err(CheckpointStoreError(
                "a run with a lease saved before acquiring it".to_string(),
            ));
        };
        let saved = store.save_under_lease(checkpoint, &lease, expected).await?;
        if saved == LeasedSave::LeaseLost {
            self.held.lose(&lease);
        }
        Ok(saved)
    }

    /// Stops renewing and gives the lease up, when the run returns. The
    /// renewal in flight, if any, is waited for first: a renewal landing
    /// after the release would hold the thread again with nobody to release
    /// it. A release that fails is ignored: the lease expires by itself.
    pub(crate) async fn release(&self) {
        self.stop.cancel();
        let renewal = self
            .renewal
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        let Some(Renewal { store, task }) = renewal else {
            return;
        };
        let _ = task.await;
        if self.is_lost() {
            return;
        }
        if let Some(lease) = self.held.current() {
            self.held.set(None);
            // The run's result stands; the thread is free again once the
            // lease expires.
            let _ = store.release_lease(&lease).await;
        }
    }

    /// Stops renewing without releasing: all a dropped run can do. The lease
    /// then expires by itself.
    pub(crate) fn stop(&self) {
        self.stop.cancel();
    }
}

/// Renews every third of the ttl on tokio's clock until stopped. A renewal the
/// store refuses loses the lease; one that fails doesn't, until a whole ttl
/// has passed since the last that succeeded.
async fn renew(
    store: Arc<dyn CheckpointStore>,
    held: Arc<Held>,
    ttl: Duration,
    stop: CancellationToken,
) {
    let interval = (ttl / 3).max(MINIMUM_LEASE_TTL);
    let mut renewed_at = tokio::time::Instant::now();
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        let Some(lease) = held.current() else { return };
        match store.renew_lease(&lease, ttl).await {
            Ok(Some(renewed)) => {
                held.set(Some(renewed));
                renewed_at = tokio::time::Instant::now();
            }
            Ok(None) => return held.lose(&lease),
            Err(_) if renewed_at.elapsed() >= ttl => return held.lose(&lease),
            Err(_) => {}
        }
    }
}
