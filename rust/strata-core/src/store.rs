//! The checkpoint persistence ports (`design/19-Async-Queryable-Store.md`):
//! [`CheckpointStore`], which the engine saves to and loads from, and
//! [`CheckpointQuery`], which lists threads and reads a thread's log. Concrete
//! adapters live in `strata-memory`; every one implements both.
//!
//! Both traits return a [`BoxFuture`], so they stay object-safe and the engine
//! can hold an `Arc<dyn CheckpointStore>`. A store built on a blocking client
//! implements [`SyncCheckpointStore`] instead and is wrapped in a
//! [`BlockingCheckpointStore`], which runs it on tokio's blocking pool — what
//! the engine did for every store before SG-11.
//!
//! Ending a thread and a resume's `resumed_at` stamp go through
//! [`CheckpointStore::load_latest`] and [`CheckpointStore::save_if_latest`], a
//! compare-and-set on the thread's log, so an end and a resume (or two
//! resumes) racing on one thread can't both win
//! (`design/28-Ending-A-Thread.md`, "Races"). Their default implementations
//! load with no sequence and save unconditionally, which is what the engine
//! did before they existed, race included; every `strata-memory` adapter
//! overrides both.
//!
//! A run given a lease ([`crate::context::RunConfig::with_lease`]) holds its
//! thread through the five lease methods (`design/32-Thread-Leases.md`): it
//! acquires before its first save, renews, saves through
//! [`CheckpointStore::save_under_lease`], which refuses a save from an owner
//! another has replaced, and releases when it returns. Their defaults return
//! an error ([`NO_LEASES`]): a store without them works as before for every
//! run that has no lease. Every `strata-memory` adapter implements them.
//!
//! [`CheckpointRetention`] deletes the logs of threads that are done with
//! (`design/34-Storage-Hardening.md`, "Retention"). It is a port of its own,
//! like the query port, and nothing in the engine calls it.
//!
//! Everything here is re-exported from [`crate::checkpoint`], where the store
//! trait has always lived.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, SecondsFormat, Utc};

use crate::schema::{Checkpoint, CheckpointStatus, CheckpointWait, GraphIdentity};

/// A boxed, `Send` future: what every store method returns, so the traits stay
/// object-safe.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The store the engine persists checkpoints to.
///
/// The engine never cancels a store call: a cancelled run must still write its
/// `cancelled` checkpoint (`design/17-Async-Execution.md`), so `save` and `load`
/// are always polled to completion. A store that can hang is bounded by its own
/// timeouts.
pub trait CheckpointStore: Send + Sync {
    /// Persists `checkpoint` as its thread's latest.
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>>;

    /// The thread's latest checkpoint, or `None` if it has none.
    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>>;

    /// The thread's latest checkpoint with its sequence in the thread's log
    /// (the sequence [`CheckpointQuery::get_log`] reports), or `None` if it
    /// has none. The default is [`Self::load`] with no sequence, for a store
    /// that can't tell.
    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        Box::pin(async move {
            Ok(self
                .load(thread_id)
                .await?
                .map(|checkpoint| LatestCheckpoint {
                    checkpoint,
                    sequence: None,
                }))
        })
    }

    /// Appends `checkpoint` only if its thread's latest checkpoint is still
    /// the one at `expected_sequence`, atomically, and says whether it did:
    /// `false` means something was appended since, and nothing was written. A
    /// `None` sequence (from a store that can't tell) saves unconditionally.
    /// The default is [`Self::save`], then `true`.
    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        let _ = expected_sequence;
        Box::pin(async move {
            self.save(checkpoint).await?;
            Ok(true)
        })
    }

    /// Takes `thread_id`'s lease for `owner`, expiring after `ttl` unless
    /// renewed (`design/32-Thread-Leases.md`). A thread whose lease was not
    /// released and has not expired is held, whoever asks: the attempt is
    /// then [`LeaseAttempt::Held`] with that lease, and nothing changes.
    /// Otherwise it is [`LeaseAttempt::Acquired`] with the thread's next
    /// token. The default has no leases.
    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        let _ = (thread_id, owner, ttl);
        Box::pin(std::future::ready(Err(no_leases())))
    }

    /// Extends `lease` to `ttl` from now, if its token is still its thread's
    /// latest, and returns it as it now is; `None` when another owner acquired
    /// the thread since. An expired lease nobody took over can still be
    /// renewed.
    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        let _ = (lease, ttl);
        Box::pin(std::future::ready(Err(no_leases())))
    }

    /// Gives `lease` up, if its token is still its thread's latest, so the
    /// thread can be acquired at once; says whether it did. The token is
    /// kept: the next acquisition gets a higher one.
    fn release_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
    ) -> BoxFuture<'a, Result<bool, CheckpointStoreError>> {
        let _ = lease;
        Box::pin(std::future::ready(Err(no_leases())))
    }

    /// The lease `thread_id` is held under now, or `None` when nobody holds it.
    fn get_lease<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        let _ = thread_id;
        Box::pin(std::future::ready(Err(no_leases())))
    }

    /// Appends `checkpoint` only if `lease`'s token is still its thread's
    /// latest, decided in the same atomic step as the append: otherwise
    /// nothing is written and the save is [`LeasedSave::LeaseLost`]. Only the
    /// token decides, not the expiry. With an `expected_sequence` it is also
    /// conditional as [`Self::save_if_latest`] is, and is
    /// [`LeasedSave::NotLatest`] when something was appended since.
    fn save_under_lease<'a>(
        &'a self,
        checkpoint: Checkpoint,
        lease: &'a ThreadLease,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>> {
        let _ = (checkpoint, lease, expected_sequence);
        Box::pin(std::future::ready(Err(no_leases())))
    }
}

/// A thread's lease (`design/32-Thread-Leases.md`): who holds the thread, the
/// token that fences its saves, and when it expires unless renewed. `token`
/// is a counter per thread: every acquisition gets the next one, and
/// [`CheckpointStore::save_under_lease`] refuses a save whose token is no
/// longer the thread's latest. `owner` is a label for people; it decides
/// nothing. Mirrors the .NET engine's `ThreadLease`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadLease {
    pub thread_id: String,
    pub owner: String,
    pub token: i64,
    /// On the store's clock.
    pub expires_at: DateTime<Utc>,
}

/// What [`CheckpointStore::try_acquire_lease`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseAttempt {
    /// The thread is the caller's, under this lease.
    Acquired(ThreadLease),
    /// The thread is held under this live lease; nothing changed.
    Held(ThreadLease),
}

impl LeaseAttempt {
    /// The lease, if it was acquired.
    pub fn acquired(self) -> Option<ThreadLease> {
        match self {
            LeaseAttempt::Acquired(lease) => Some(lease),
            LeaseAttempt::Held(_) => None,
        }
    }

    /// The lease that holds the thread, if it wasn't acquired.
    pub fn held_by(&self) -> Option<&ThreadLease> {
        match self {
            LeaseAttempt::Acquired(_) => None,
            LeaseAttempt::Held(lease) => Some(lease),
        }
    }
}

/// How [`CheckpointStore::save_under_lease`] ended. A lost lease is an
/// outcome here, not an error: `CheckpointStoreError` holds only a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeasedSave {
    Saved,
    /// Something was appended since the expected sequence. Nothing was written.
    NotLatest,
    /// The lease's token is no longer its thread's latest: another owner
    /// acquired the thread. Nothing was written.
    LeaseLost,
}

/// The message a store with no leases refuses with.
pub const NO_LEASES: &str = "this checkpoint store has no leases";

/// The shortest ttl a lease can have.
pub const MINIMUM_LEASE_TTL: Duration = Duration::from_millis(1);

fn no_leases() -> CheckpointStoreError {
    CheckpointStoreError(NO_LEASES.to_string())
}

/// Refuses a blank owner and a ttl below [`MINIMUM_LEASE_TTL`]: what every
/// store's `try_acquire_lease` checks first.
pub fn check_lease(owner: &str, ttl: Duration) -> Result<(), CheckpointStoreError> {
    if owner.trim().is_empty() {
        return Err(CheckpointStoreError("a lease needs an owner".to_string()));
    }
    check_lease_ttl(ttl)
}

/// Refuses a ttl below [`MINIMUM_LEASE_TTL`].
pub fn check_lease_ttl(ttl: Duration) -> Result<(), CheckpointStoreError> {
    if ttl < MINIMUM_LEASE_TTL {
        return Err(CheckpointStoreError(
            "a lease's ttl must be at least 1 millisecond".to_string(),
        ));
    }
    Ok(())
}

/// Refuses a lease for another thread than `checkpoint`'s: what every store's
/// `save_under_lease` checks first.
pub fn check_lease_thread(
    checkpoint: &Checkpoint,
    lease: &ThreadLease,
) -> Result<(), CheckpointStoreError> {
    if lease.thread_id != checkpoint.thread_id {
        return Err(CheckpointStoreError(format!(
            "the lease is for thread '{}', not the checkpoint's thread '{}'",
            lease.thread_id, checkpoint.thread_id
        )));
    }
    Ok(())
}

/// A ttl in the whole milliseconds a store counts in, rounded up.
pub fn lease_millis(ttl: Duration) -> i64 {
    let millis = ttl.as_millis() + u128::from(!ttl.subsec_nanos().is_multiple_of(1_000_000));
    i64::try_from(millis).unwrap_or(i64::MAX)
}

/// A thread's latest checkpoint, with its sequence in the thread's log: what
/// [`CheckpointStore::save_if_latest`] compares against.
#[derive(Debug, Clone, PartialEq)]
pub struct LatestCheckpoint {
    pub checkpoint: Checkpoint,
    /// `None` when the store can't tell (it doesn't override
    /// [`CheckpointStore::load_latest`]).
    pub sequence: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointStoreError(pub String);

impl std::fmt::Display for CheckpointStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CheckpointStoreError {}

/// A checkpoint store with a blocking API: the shape [`CheckpointStore`] had
/// before SG-11. Wrap one in [`BlockingCheckpointStore`] to hand it to the
/// engine.
pub trait SyncCheckpointStore: Send + Sync + 'static {
    fn save(&self, checkpoint: Checkpoint) -> Result<(), CheckpointStoreError>;
    fn load(&self, thread_id: &str) -> Result<Option<Checkpoint>, CheckpointStoreError>;

    /// As [`CheckpointStore::load_latest`]; the default is [`Self::load`] with
    /// no sequence.
    fn load_latest(
        &self,
        thread_id: &str,
    ) -> Result<Option<LatestCheckpoint>, CheckpointStoreError> {
        Ok(self.load(thread_id)?.map(|checkpoint| LatestCheckpoint {
            checkpoint,
            sequence: None,
        }))
    }

    /// As [`CheckpointStore::save_if_latest`]; the default is [`Self::save`],
    /// then `true`.
    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> Result<bool, CheckpointStoreError> {
        let _ = expected_sequence;
        self.save(checkpoint)?;
        Ok(true)
    }

    /// As [`CheckpointStore::try_acquire_lease`]; the default has no leases.
    fn try_acquire_lease(
        &self,
        thread_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<LeaseAttempt, CheckpointStoreError> {
        let _ = (thread_id, owner, ttl);
        Err(no_leases())
    }

    /// As [`CheckpointStore::renew_lease`].
    fn renew_lease(
        &self,
        lease: &ThreadLease,
        ttl: Duration,
    ) -> Result<Option<ThreadLease>, CheckpointStoreError> {
        let _ = (lease, ttl);
        Err(no_leases())
    }

    /// As [`CheckpointStore::release_lease`].
    fn release_lease(&self, lease: &ThreadLease) -> Result<bool, CheckpointStoreError> {
        let _ = lease;
        Err(no_leases())
    }

    /// As [`CheckpointStore::get_lease`].
    fn get_lease(&self, thread_id: &str) -> Result<Option<ThreadLease>, CheckpointStoreError> {
        let _ = thread_id;
        Err(no_leases())
    }

    /// As [`CheckpointStore::save_under_lease`].
    fn save_under_lease(
        &self,
        checkpoint: Checkpoint,
        lease: &ThreadLease,
        expected_sequence: Option<i64>,
    ) -> Result<LeasedSave, CheckpointStoreError> {
        let _ = (checkpoint, lease, expected_sequence);
        Err(no_leases())
    }
}

/// Adapts a [`SyncCheckpointStore`] to [`CheckpointStore`] by running each call
/// on tokio's blocking pool, so a blocking client (the sync `postgres` crate,
/// say, which panics on a runtime thread) never runs on the executor.
pub struct BlockingCheckpointStore<T>(Arc<T>);

impl<T: SyncCheckpointStore> BlockingCheckpointStore<T> {
    pub fn new(store: T) -> Self {
        Self(Arc::new(store))
    }

    /// Wraps a store the caller keeps a handle to.
    pub fn from_arc(store: Arc<T>) -> Self {
        Self(store)
    }

    /// The wrapped store.
    pub fn inner(&self) -> &Arc<T> {
        &self.0
    }
}

/// Runs `work` on the blocking pool, re-raising a panic from it here.
pub(crate) async fn on_blocking_pool<R, E>(
    work: impl FnOnce() -> Result<R, E> + Send + 'static,
    shut_down: impl FnOnce() -> E,
) -> Result<R, E>
where
    R: Send + 'static,
    E: Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(_) => Err(shut_down()),
    }
}

fn runtime_shut_down() -> CheckpointStoreError {
    CheckpointStoreError("the runtime shut down during a checkpoint store call".to_string())
}

impl<T: SyncCheckpointStore> CheckpointStore for BlockingCheckpointStore<T> {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        let store = self.0.clone();
        Box::pin(on_blocking_pool(
            move || store.save(checkpoint),
            runtime_shut_down,
        ))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        let (store, thread_id) = (self.0.clone(), thread_id.to_string());
        Box::pin(on_blocking_pool(
            move || store.load(&thread_id),
            runtime_shut_down,
        ))
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        let (store, thread_id) = (self.0.clone(), thread_id.to_string());
        Box::pin(on_blocking_pool(
            move || store.load_latest(&thread_id),
            runtime_shut_down,
        ))
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        let store = self.0.clone();
        Box::pin(on_blocking_pool(
            move || store.save_if_latest(checkpoint, expected_sequence),
            runtime_shut_down,
        ))
    }

    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        let (store, thread_id, owner) = (self.0.clone(), thread_id.to_string(), owner.to_string());
        Box::pin(on_blocking_pool(
            move || store.try_acquire_lease(&thread_id, &owner, ttl),
            runtime_shut_down,
        ))
    }

    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        let (store, lease) = (self.0.clone(), lease.clone());
        Box::pin(on_blocking_pool(
            move || store.renew_lease(&lease, ttl),
            runtime_shut_down,
        ))
    }

    fn release_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
    ) -> BoxFuture<'a, Result<bool, CheckpointStoreError>> {
        let (store, lease) = (self.0.clone(), lease.clone());
        Box::pin(on_blocking_pool(
            move || store.release_lease(&lease),
            runtime_shut_down,
        ))
    }

    fn get_lease<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        let (store, thread_id) = (self.0.clone(), thread_id.to_string());
        Box::pin(on_blocking_pool(
            move || store.get_lease(&thread_id),
            runtime_shut_down,
        ))
    }

    fn save_under_lease<'a>(
        &'a self,
        checkpoint: Checkpoint,
        lease: &'a ThreadLease,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>> {
        let (store, lease) = (self.0.clone(), lease.clone());
        Box::pin(on_blocking_pool(
            move || store.save_under_lease(checkpoint, &lease, expected_sequence),
            runtime_shut_down,
        ))
    }
}

/// The most threads or log entries one query page may ask for.
pub const MAX_PAGE_SIZE: usize = 1000;

/// The page size a [`ThreadQuery`] starts with.
pub const DEFAULT_PAGE_SIZE: usize = 100;

/// Lists threads and reads a thread's checkpoint log
/// (`design/19-Async-Queryable-Store.md`, "Query port"). Separate from
/// [`CheckpointStore`] so a store that can only save and load stays complete;
/// every built-in adapter implements both.
pub trait CheckpointQuery: Send + Sync {
    /// One page of threads matching `query`, most recently updated first. A
    /// page may hold fewer than `query.limit` threads even when more follow:
    /// only a `None` [`ThreadPage::next_cursor`] means the end.
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>>;

    /// Up to `limit` (1 to [`MAX_PAGE_SIZE`]) of `thread_id`'s checkpoints
    /// with a sequence above `after_sequence`, oldest first. An unknown thread
    /// gives an empty page.
    fn get_log<'a>(
        &'a self,
        thread_id: &'a str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>>;
}

/// Where a thread stands, derived from its latest checkpoint
/// (`design/19-Async-Queryable-Store.md`, "`ThreadState`").
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ThreadState {
    /// Waiting for a human: the latest checkpoint is `interrupted` and was not
    /// resumed.
    Interrupted,
    /// Stopped by a node failure, not resumed.
    Failed,
    /// Stopped by cancellation or a deadline, not resumed.
    Cancelled,
    /// A resume began from the latest checkpoint, by a graph that doesn't
    /// checkpoint every node. The resumed run may still be running, may have
    /// completed (which writes nothing), or may have died with its process:
    /// the store can't tell which. A graph compiled with
    /// `CheckpointPolicy::EveryNode` never leaves this state: its resume makes
    /// the thread [`ThreadState::Running`] (`design/33-Durable-Execution.md`).
    Resumed,
    /// The latest checkpoint is `completed`: a run of a graph compiled with
    /// `CheckpointPolicy::EveryNode` finished
    /// (`design/33-Durable-Execution.md`). Other graphs write nothing when
    /// they complete.
    Completed,
    /// The thread was ended where it stood (`design/28-Ending-A-Thread.md`):
    /// it will not run again.
    Ended,
    /// The latest checkpoint is `running` (`design/33-Durable-Execution.md`):
    /// a run of a graph compiled with `CheckpointPolicy::EveryNode` is going
    /// on, or its process died. With leases, a running thread no live lease
    /// holds is dead, and `CompiledGraph::recover_stale` recovers it.
    Running,
}

impl ThreadState {
    /// Every state, in the order the Redis index script expects its sets.
    pub const ALL: [ThreadState; 7] = [
        ThreadState::Interrupted,
        ThreadState::Failed,
        ThreadState::Cancelled,
        ThreadState::Resumed,
        ThreadState::Completed,
        ThreadState::Ended,
        ThreadState::Running,
    ];

    /// The state of a thread whose latest checkpoint is `checkpoint`.
    pub fn of(checkpoint: &Checkpoint) -> Self {
        if checkpoint.resumed_at.is_some() {
            return ThreadState::Resumed;
        }
        match checkpoint.status {
            CheckpointStatus::Interrupted => ThreadState::Interrupted,
            CheckpointStatus::Failed => ThreadState::Failed,
            CheckpointStatus::Cancelled => ThreadState::Cancelled,
            CheckpointStatus::Completed => ThreadState::Completed,
            CheckpointStatus::Ended => ThreadState::Ended,
            CheckpointStatus::Running => ThreadState::Running,
        }
    }

    /// The name stored in the SQL head table and the Redis index keys.
    pub fn as_str(self) -> &'static str {
        match self {
            ThreadState::Interrupted => "interrupted",
            ThreadState::Failed => "failed",
            ThreadState::Cancelled => "cancelled",
            ThreadState::Resumed => "resumed",
            ThreadState::Completed => "completed",
            ThreadState::Ended => "ended",
            ThreadState::Running => "running",
        }
    }

    /// The inverse of [`Self::as_str`].
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|state| state.as_str() == name)
    }
}

/// Deletes the logs of threads that are done with
/// (`design/34-Storage-Hardening.md`, "Retention"). Separate from
/// [`CheckpointStore`], like [`CheckpointQuery`], so a store that can only
/// save and load stays complete; every `strata-memory` adapter implements it.
/// Mirrors the .NET `ICheckpointRetention`.
pub trait CheckpointRetention: Send + Sync {
    /// Deletes every checkpoint of each thread whose **latest** checkpoint is
    /// in one of `states` and was written before `older_than` (at second
    /// precision, exclusive, like [`ThreadQuery::updated_before`]), oldest
    /// first, up to `limit` threads (1 to [`MAX_PAGE_SIZE`]), with the
    /// thread's lease record. Returns how many threads it purged: call it
    /// again while it returns `limit`.
    ///
    /// [`PurgeStates::finished`] names the threads that will not run again.
    /// Naming another state purges threads that could still be resumed,
    /// recovered or answered: a waiting approval is then gone. A thread a live
    /// lease holds is never purged, and neither is one that changed while it
    /// was being purged.
    ///
    /// Nothing in the engine calls this: retention is the application's to
    /// decide and schedule.
    fn purge<'a>(
        &'a self,
        older_than: DateTime<Utc>,
        states: &'a PurgeStates,
        limit: usize,
    ) -> BoxFuture<'a, Result<usize, CheckpointStoreError>>;
}

/// The thread states a [`CheckpointRetention::purge`] takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeStates(BTreeSet<ThreadState>);

impl PurgeStates {
    /// [`ThreadState::Completed`] and [`ThreadState::Ended`]: threads that
    /// will not run again.
    pub fn finished() -> Self {
        Self::of([ThreadState::Completed, ThreadState::Ended])
    }

    /// These states, and only they.
    pub fn of(states: impl IntoIterator<Item = ThreadState>) -> Self {
        Self(states.into_iter().collect())
    }

    pub fn contains(&self, state: ThreadState) -> bool {
        self.0.contains(&state)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The states, in [`ThreadState`]'s order.
    pub fn iter(&self) -> impl Iterator<Item = ThreadState> + '_ {
        self.0.iter().copied()
    }

    /// The states' stored names ([`ThreadState::as_str`]).
    pub fn names(&self) -> Vec<String> {
        self.iter()
            .map(|state| state.as_str().to_string())
            .collect()
    }

    /// Whether a thread whose latest checkpoint is `checkpoint` is one a
    /// purge of these states older than `older_than` (stored text) takes. For
    /// stores that decide in code.
    pub fn takes(&self, checkpoint: &Checkpoint, older_than: &str) -> bool {
        self.contains(ThreadState::of(checkpoint)) && updated_at_text(checkpoint) < older_than
    }
}

impl Default for PurgeStates {
    fn default() -> Self {
        Self::finished()
    }
}

/// Which threads [`CheckpointQuery::list_threads`] returns.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ThreadQuery {
    /// Only threads in one of these states; empty means any state.
    pub states: Vec<ThreadState>,
    /// Only threads whose latest checkpoint's `metadata` has every pair.
    pub metadata: BTreeMap<String, String>,
    /// Only threads updated strictly after this, at second precision.
    pub updated_after: Option<DateTime<Utc>>,
    /// Only threads updated strictly before this, at second precision.
    pub updated_before: Option<DateTime<Utc>>,
    /// Only threads due at this time (`design/29-Event-Waits-And-Timers.md`):
    /// waiting (interrupted, not resumed), with an innermost `wait.wake_at` at
    /// or before it, at second precision.
    pub due_by: Option<DateTime<Utc>>,
    /// Only threads whose latest checkpoint's outermost level was written by
    /// a graph with this id (`design/31-Graph-Identity.md`). A thread with no
    /// identity never matches.
    pub graph_id: Option<String>,
    /// Only threads written by this version of [`Self::graph_id`], which it
    /// needs.
    pub graph_version: Option<String>,
    /// How many threads a page holds at most: 1 to [`MAX_PAGE_SIZE`].
    pub limit: usize,
    /// Where to continue: the previous page's [`ThreadPage::next_cursor`].
    pub cursor: Option<String>,
}

impl Default for ThreadQuery {
    fn default() -> Self {
        Self {
            states: Vec::new(),
            metadata: BTreeMap::new(),
            updated_after: None,
            updated_before: None,
            due_by: None,
            graph_id: None,
            graph_version: None,
            limit: DEFAULT_PAGE_SIZE,
            cursor: None,
        }
    }
}

impl ThreadQuery {
    pub fn with_states(mut self, states: impl IntoIterator<Item = ThreadState>) -> Self {
        self.states.extend(states);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn updated_after(mut self, time: DateTime<Utc>) -> Self {
        self.updated_after = Some(time);
        self
    }

    pub fn updated_before(mut self, time: DateTime<Utc>) -> Self {
        self.updated_before = Some(time);
        self
    }

    /// Only threads due at `time` (see [`Self::due_by`]).
    pub fn due_by(mut self, time: DateTime<Utc>) -> Self {
        self.due_by = Some(time);
        self
    }

    /// Only threads written by a graph with this id (see [`Self::graph_id`]).
    pub fn with_graph(mut self, id: impl Into<String>) -> Self {
        self.graph_id = Some(id.into());
        self
    }

    /// Only threads written by this version of the graph
    /// [`Self::with_graph`] names (see [`Self::graph_version`]).
    pub fn with_graph_version(mut self, version: impl Into<String>) -> Self {
        self.graph_version = Some(version.into());
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    /// Refuses a limit outside 1 to [`MAX_PAGE_SIZE`], a cursor that isn't
    /// one, and a [`Self::graph_version`] without a [`Self::graph_id`];
    /// returns the decoded cursor.
    pub fn validate(&self) -> Result<Option<ThreadCursor>, CheckpointStoreError> {
        check_limit(self.limit)?;
        if self.graph_version.is_some() && self.graph_id.is_none() {
            return Err(CheckpointStoreError(
                "a thread query's graph_version needs a graph_id".to_string(),
            ));
        }
        self.cursor.as_deref().map(ThreadCursor::decode).transpose()
    }

    /// `updated_after` as stored text, for comparing with `updated_at`.
    pub fn updated_after_text(&self) -> Option<String> {
        self.updated_after.map(timestamp_text)
    }

    /// `updated_before` as stored text, for comparing with `updated_at`.
    pub fn updated_before_text(&self) -> Option<String> {
        self.updated_before.map(timestamp_text)
    }

    /// `due_by` as stored text, for comparing with `wake_at`.
    pub fn due_by_text(&self) -> Option<String> {
        self.due_by.map(timestamp_text)
    }

    /// Whether a thread whose latest checkpoint is `checkpoint` matches every
    /// filter (not the cursor). For adapters that filter in code.
    pub fn matches(&self, checkpoint: &Checkpoint) -> bool {
        if !self.states.is_empty() && !self.states.contains(&ThreadState::of(checkpoint)) {
            return false;
        }
        if !self.metadata.is_empty() {
            let Some(metadata) = &checkpoint.metadata else {
                return false;
            };
            if self
                .metadata
                .iter()
                .any(|(key, value)| metadata.get(key) != Some(value))
            {
                return false;
            }
        }
        if self
            .due_by_text()
            .is_some_and(|due| !is_due(checkpoint, &due))
        {
            return false;
        }
        if let Some(id) = &self.graph_id {
            let written = checkpoint.graph.as_ref().is_some_and(|graph| {
                &graph.id == id
                    && self
                        .graph_version
                        .as_ref()
                        .is_none_or(|version| graph.version.as_ref() == Some(version))
            });
            if !written {
                return false;
            }
        }
        let updated = updated_at_text(checkpoint);
        if self
            .updated_after_text()
            .is_some_and(|after| updated <= after.as_str())
        {
            return false;
        }
        if self
            .updated_before_text()
            .is_some_and(|before| updated >= before.as_str())
        {
            return false;
        }
        true
    }
}

/// Whether a thread whose latest checkpoint is `checkpoint` is due at `due`
/// (stored text): waiting, not resumed, with an innermost `wake_at` at or
/// before it (`design/29-Event-Waits-And-Timers.md`).
pub fn is_due(checkpoint: &Checkpoint, due: &str) -> bool {
    ThreadState::of(checkpoint) == ThreadState::Interrupted
        && checkpoint
            .innermost_wait()
            .and_then(|wait| wait.wake_at.as_deref())
            .is_some_and(|wake_at| wake_at <= due)
}

/// Refuses a page size outside 1 to [`MAX_PAGE_SIZE`].
pub fn check_limit(limit: usize) -> Result<(), CheckpointStoreError> {
    if (1..=MAX_PAGE_SIZE).contains(&limit) {
        Ok(())
    } else {
        Err(CheckpointStoreError(format!(
            "a page size must be between 1 and {MAX_PAGE_SIZE}, not {limit}"
        )))
    }
}

/// When a thread whose latest checkpoint is `checkpoint` was last written, as
/// stored: `resumed_at` if set (the resume stamp keeps the original
/// `created_at`), else `created_at`.
pub fn updated_at_text(checkpoint: &Checkpoint) -> &str {
    checkpoint
        .resumed_at
        .as_deref()
        .unwrap_or(&checkpoint.created_at)
}

/// `time` the way the engines write timestamps: RFC 3339, whole seconds, `Z`.
pub fn timestamp_text(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Parses a stored timestamp.
pub fn parse_timestamp(text: &str) -> Result<DateTime<Utc>, CheckpointStoreError> {
    DateTime::parse_from_rfc3339(text)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|e| CheckpointStoreError(format!("bad timestamp '{text}' in the log: {e}")))
}

/// A listed thread: enough to act on without loading its checkpoint.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadSummary {
    pub thread_id: String,
    /// The latest checkpoint's sequence in the thread's log.
    pub sequence: i64,
    pub state: ThreadState,
    /// The latest checkpoint's own status.
    pub status: CheckpointStatus,
    pub suspended_node: Option<String>,
    pub next_node: Option<String>,
    /// As [`Checkpoint::waiting_at`].
    pub waiting_at: Vec<String>,
    /// The latest checkpoint's whole `metadata` map, engine keys included.
    pub metadata: HashMap<String, String>,
    pub updated_at: DateTime<Utc>,
    pub schema_version: String,
    /// What the latest checkpoint's innermost level waits for, and until
    /// when (`design/29-Event-Waits-And-Timers.md`):
    /// [`Checkpoint::innermost_wait`].
    pub wait: Option<CheckpointWait>,
    /// Which graph wrote the latest checkpoint's outermost level
    /// (`design/31-Graph-Identity.md`), or `None` when it has no identity.
    pub graph: Option<GraphIdentity>,
}

impl ThreadSummary {
    /// A summary with no nodes, no waiting path, no metadata, no wait and no
    /// graph, for a store that reads a thread's summary from somewhere other
    /// than its checkpoint (the Strata Cloud store reads Cloud's rows): set
    /// the other fields on it.
    pub fn new(
        thread_id: impl Into<String>,
        sequence: i64,
        state: ThreadState,
        status: CheckpointStatus,
        updated_at: DateTime<Utc>,
        schema_version: impl Into<String>,
    ) -> Self {
        Self {
            thread_id: thread_id.into(),
            sequence,
            state,
            status,
            suspended_node: None,
            next_node: None,
            waiting_at: Vec::new(),
            metadata: HashMap::new(),
            updated_at,
            schema_version: schema_version.into(),
            wait: None,
            graph: None,
        }
    }

    /// The summary of a thread whose latest checkpoint, at `sequence`, is
    /// `checkpoint`.
    pub fn of(sequence: i64, checkpoint: &Checkpoint) -> Result<Self, CheckpointStoreError> {
        Ok(Self {
            thread_id: checkpoint.thread_id.clone(),
            sequence,
            state: ThreadState::of(checkpoint),
            status: checkpoint.status,
            suspended_node: checkpoint.suspended_node.clone(),
            next_node: checkpoint.next_node.clone(),
            waiting_at: checkpoint.waiting_at(),
            metadata: checkpoint.metadata.clone().unwrap_or_default(),
            updated_at: parse_timestamp(updated_at_text(checkpoint))?,
            schema_version: checkpoint.schema_version.clone(),
            wait: checkpoint.innermost_wait().cloned(),
            graph: checkpoint.graph.clone(),
        })
    }
}

/// One page of [`CheckpointQuery::list_threads`].
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadPage {
    pub threads: Vec<ThreadSummary>,
    /// Pass this as [`ThreadQuery::cursor`] for the next page; `None` at the end.
    pub next_cursor: Option<String>,
}

/// One checkpoint of a thread's log, with its sequence.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointLogEntry {
    pub sequence: i64,
    pub checkpoint: Checkpoint,
}

/// One page of [`CheckpointQuery::get_log`].
#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointLogPage {
    pub entries: Vec<CheckpointLogEntry>,
    /// The last entry's sequence when the page was full (more may follow);
    /// `None` otherwise.
    pub next_after_sequence: Option<i64>,
}

impl CheckpointLogPage {
    /// The page for `entries` read with `limit`.
    pub fn new(entries: Vec<CheckpointLogEntry>, limit: usize) -> Self {
        let next_after_sequence = if entries.len() == limit {
            entries.last().map(|entry| entry.sequence)
        } else {
            None
        };
        Self {
            entries,
            next_after_sequence,
        }
    }
}

/// The position a thread listing continues from: the last returned thread's
/// stored `updated_at` and id. Encoded as base64url (no padding) of the JSON
/// array `[updated_at, thread_id]`, the same in both engines
/// (`schema/README.md`, "Thread listing cursor").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadCursor {
    pub updated_at: String,
    pub thread_id: String,
}

impl ThreadCursor {
    pub fn new(updated_at: impl Into<String>, thread_id: impl Into<String>) -> Self {
        Self {
            updated_at: updated_at.into(),
            thread_id: thread_id.into(),
        }
    }

    pub fn encode(&self) -> String {
        let json = serde_json::json!([self.updated_at, self.thread_id]).to_string();
        URL_SAFE_NO_PAD.encode(json)
    }

    pub fn decode(cursor: &str) -> Result<Self, CheckpointStoreError> {
        let bad = || CheckpointStoreError(format!("'{cursor}' is not a thread listing cursor"));
        let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| bad())?;
        let (updated_at, thread_id): (String, String) =
            serde_json::from_slice(&bytes).map_err(|_| bad())?;
        Ok(Self::new(updated_at, thread_id))
    }

    /// Whether a thread at `(updated_at, thread_id)` comes after this cursor
    /// in listing order (newest first, then thread id descending), so belongs
    /// on a page that continues from it.
    pub fn admits(&self, updated_at: &str, thread_id: &str) -> bool {
        (updated_at, thread_id) < (self.updated_at.as_str(), self.thread_id.as_str())
    }
}
