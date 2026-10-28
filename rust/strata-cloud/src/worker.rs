//! Resumes the threads a reviewer decided in the Strata Cloud console
//! (`design/30-Strata-Cloud-Store.md`, "The worker"). Mirrors the .NET
//! adapter's `DecisionWorker<TState>`.

use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{Backoff, CompiledGraph, Jitter};
use datalium_strata::schema::Checkpoint;
use datalium_strata::store::{
    CheckpointStore, CheckpointStoreError, LatestCheckpoint, LeaseAttempt, LeasedSave, ThreadLease,
};
use datalium_strata::{BoxFuture, CancellationToken, StrataError};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::decisions::{CloudDecision, DecisionQuery, Decisions};
use crate::error::{self, CloudError};
use crate::options::backoff_delay;

/// How many approval ids a worker remembers having handled.
const REMEMBERED: usize = 10_000;

/// What a [`DecisionWorker`] did with a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DecisionOutcome {
    /// The worker resumed the thread, and the resume ran (completed,
    /// interrupted again, cancelled).
    Resumed,
    /// The resume returned `Failed` for a reason of its own: refused before
    /// anything was written (an override that doesn't fit the state, a thread
    /// of another graph), or the resumed run failed. The decision is not
    /// handled again.
    Failed,
    /// Someone else acted on the decision first: the checkpoint was already
    /// resumed, or changed before this worker's resume could stamp it
    /// (another worker, an end, a later boundary). Nothing ran here.
    AlreadyHandled,
    /// The decision is for a thread this worker doesn't own: one the prefix
    /// or [`DecisionWorker::with_owns`] leaves out, or one another graph wrote
    /// (`CompiledGraph::owns`, `design/31-Graph-Identity.md`). It was not
    /// resumed.
    NotMine,
}

/// A decision a [`DecisionWorker`] handled, and what came of it.
pub struct HandledDecision<S> {
    pub decision: CloudDecision,
    pub outcome: DecisionOutcome,
    /// The resume's result; `None` for [`DecisionOutcome::NotMine`].
    pub result: Option<GraphExecutionResult<S>>,
}

impl<S> fmt::Debug for HandledDecision<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandledDecision")
            .field("decision", &self.decision)
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}

type OwnsFn = Arc<dyn Fn(&CloudDecision) -> BoxFuture<'static, bool> + Send + Sync>;
type RunConfigFn = Arc<dyn Fn(&CloudDecision) -> RunConfig + Send + Sync>;
type OnHandledFn<S> = Arc<dyn Fn(&HandledDecision<S>) + Send + Sync>;

/// Resumes the threads a reviewer decided in the Strata Cloud console. A
/// library loop, not a hosted service: a host runs [`run`](Self::run) on a
/// task of its own, or drives [`poll`](Self::poll) itself.
///
/// Each round reads the feed from its start (long-polling while it is empty)
/// and handles each decision it hasn't handled before, oldest first, one at a
/// time: a decision for a thread it doesn't own is reported
/// [`DecisionOutcome::NotMine`]; any other is resumed through the store with
/// the decision's signal. The resume is pinned to the checkpoint the decision
/// answered, so a decision read before the thread moved on can't approve a
/// later boundary. Every decision is handled once per worker (the last 10,000
/// are remembered): one Cloud keeps offering because nothing was saved to its
/// thread is paged past.
///
/// One worker resumes one thread at a time. Scale with several workers on
/// disjoint threads.
pub struct DecisionWorker<S> {
    graph: Arc<CompiledGraph<S>>,
    store: Arc<dyn CheckpointStore>,
    decisions: Decisions,
    thread_id: Option<String>,
    thread_id_prefix: Option<String>,
    owns: Option<OwnsFn>,
    run_config: Option<RunConfigFn>,
    on_handled: Option<OnHandledFn<S>>,
    wait: Duration,
    limit: Option<u32>,
    idle_delay: Duration,
    error_backoff: Backoff,
    seen: HashSet<String>,
    seen_order: VecDeque<String>,
    last_round_stale: bool,
}

impl<S> DecisionWorker<S>
where
    S: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    /// A worker resuming `graph`'s threads in `store` (a
    /// [`StrataCloudCheckpointStore`](crate::StrataCloudCheckpointStore)) as
    /// `decisions` delivers them.
    pub fn new(
        graph: Arc<CompiledGraph<S>>,
        store: Arc<dyn CheckpointStore>,
        decisions: Decisions,
    ) -> Self {
        Self {
            graph,
            store,
            decisions,
            thread_id: None,
            thread_id_prefix: None,
            owns: None,
            run_config: None,
            on_handled: None,
            wait: Duration::from_secs(25),
            limit: None,
            idle_delay: Duration::from_secs(2),
            error_backoff: Backoff {
                initial: Duration::from_secs(1),
                max: Duration::from_secs(30),
                jitter: Jitter::Full,
            },
            seen: HashSet::new(),
            seen_order: VecDeque::new(),
            last_round_stale: false,
        }
    }

    /// Wait on this one thread only (Cloud's long-poll takes one). Otherwise
    /// the account's whole feed.
    pub fn with_thread_id(mut self, thread_id: impl Into<String>) -> Self {
        self.thread_id = Some(thread_id.into());
        self
    }

    /// A thread is this worker's only if its id starts with `prefix`.
    pub fn with_thread_id_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.thread_id_prefix = Some(prefix.into());
        self
    }

    /// A thread is this worker's only if `owns` says so (combined with the
    /// prefix when both are set), checked before the thread is loaded.
    /// Default: every thread. Whatever it says, a thread another graph wrote
    /// is not this worker's (`CompiledGraph::owns`,
    /// `design/31-Graph-Identity.md`).
    pub fn with_owns(self, owns: impl Fn(&CloudDecision) -> bool + Send + Sync + 'static) -> Self {
        self.with_owns_async(move |decision| {
            let mine = owns(decision);
            async move { mine }
        })
    }

    /// As [`with_owns`](Self::with_owns), for a lookup that awaits (a
    /// database).
    pub fn with_owns_async<F, Fut>(mut self, owns: F) -> Self
    where
        F: Fn(&CloudDecision) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        self.owns = Some(Arc::new(move |decision| Box::pin(owns(decision))));
        self
    }

    /// The run config for each resume (its metadata, observer, deadline). The
    /// worker adds no cancellation token: stopping it doesn't cancel a resume
    /// in progress.
    pub fn with_run_config(
        mut self,
        config: impl Fn(&CloudDecision) -> RunConfig + Send + Sync + 'static,
    ) -> Self {
        self.run_config = Some(Arc::new(config));
        self
    }

    /// Called once for every decision handled, in order, before the next one.
    /// StrataOps audits here (send it to a channel for anything that awaits).
    pub fn with_on_handled(
        mut self,
        on_handled: impl Fn(&HandledDecision<S>) + Send + Sync + 'static,
    ) -> Self {
        self.on_handled = Some(Arc::new(on_handled));
        self
    }

    /// How long each long-poll is held by [`run`](Self::run). Default 25 s,
    /// Cloud's maximum.
    pub fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }

    /// The page size. Default Cloud's (25).
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// The wait after a round that found decisions but nothing new, since a
    /// long-poll answers at once while any decision is waiting. Default 2 s.
    pub fn with_idle_delay(mut self, delay: Duration) -> Self {
        self.idle_delay = delay;
        self
    }

    /// The wait after the feed or the store was unavailable. Default 1 s
    /// doubling to 30 s, full jitter.
    pub fn with_error_backoff(mut self, backoff: Backoff) -> Self {
        self.error_backoff = backoff;
        self
    }

    /// Polls and resumes until `shutdown` is cancelled, then returns `Ok`.
    /// Waits out an unavailable feed or store (`Retry-After` on a `429`, else
    /// the error backoff), and returns a failure no wait can fix: a `401`,
    /// any other refusal of the feed. Cancelling stops a long-poll or a wait
    /// at once; a resume in progress runs to its end (bound it with
    /// `RunConfig::with_deadline`).
    pub async fn run(&mut self, shutdown: CancellationToken) -> Result<(), CloudError> {
        let sleep = self.decisions.options().sleep.clone();
        let mut failures = 0;
        while !shutdown.is_cancelled() {
            let delay = match self.round(self.wait, Some(&shutdown)).await {
                Ok(_) => {
                    failures = 0;
                    if self.last_round_stale {
                        self.idle_delay
                    } else {
                        Duration::ZERO
                    }
                }
                Err(err) if err.is_transient() => {
                    failures += 1;
                    let backoff = backoff_delay(&self.error_backoff, failures);
                    match (err.status(), err.retry_after()) {
                        (Some(429), after) => after.unwrap_or(Duration::from_secs(5)),
                        (_, Some(after)) if after > backoff => after,
                        _ => backoff,
                    }
                }
                Err(err) => return Err(err),
            };
            if !delay.is_zero() {
                tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = sleep(delay) => {}
                }
            }
        }
        Ok(())
    }

    /// One round: reads the feed from its start, holding the first request
    /// up to `wait` while it is empty, follows every cursor, and handles each
    /// decision not handled before. Returns them in order. Fails with the
    /// feed's error, or with a store error a wait may fix (the decision is
    /// then not remembered, so the next round tries it again).
    pub async fn poll(&mut self, wait: Duration) -> Result<Vec<HandledDecision<S>>, CloudError> {
        self.round(wait, None).await
    }

    async fn round(
        &mut self,
        wait: Duration,
        shutdown: Option<&CancellationToken>,
    ) -> Result<Vec<HandledDecision<S>>, CloudError> {
        let mut handled = Vec::new();
        let mut saw_items = false;
        let mut cursor: Option<String> = None;
        loop {
            let query = DecisionQuery {
                thread_id: self.thread_id.clone(),
                limit: self.limit,
                cursor: cursor.clone(),
                wait: if cursor.is_none() {
                    wait
                } else {
                    Duration::ZERO
                },
            };
            // Only the request races the shutdown, never a resume.
            let page = match shutdown {
                Some(shutdown) => tokio::select! {
                    _ = shutdown.cancelled() => break,
                    page = self.decisions.get(&query) => page,
                },
                None => self.decisions.get(&query).await,
            };
            let page = match page {
                Ok(page) => page,
                // A cursor Cloud no longer takes: the next round starts over.
                Err(CloudError::Rejected { status: 400, .. }) if cursor.is_some() => break,
                Err(err) => return Err(err),
            };
            saw_items |= !page.items.is_empty();
            for decision in page.items {
                if self.seen.contains(&decision.approval_id) {
                    continue;
                }
                // `None`: its thread is held by another owner's lease, or its
                // resume wasn't admitted. It is left for the next round, not
                // remembered.
                let Some(outcome) = self.handle(decision).await? else {
                    continue;
                };
                self.remember(outcome.decision.approval_id.clone());
                if let Some(on_handled) = &self.on_handled {
                    on_handled(&outcome);
                }
                handled.push(outcome);
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        self.last_round_stale = saw_items && handled.is_empty();
        Ok(handled)
    }

    /// Resumes `decision`'s thread and says how it went; `None` when the
    /// thread is held by another owner's lease
    /// (`design/32-Thread-Leases.md`), which is theirs to finish with, or the
    /// run's scheduler didn't admit the resume (`design/40-Run-Scheduler.md`).
    async fn handle(
        &self,
        decision: CloudDecision,
    ) -> Result<Option<HandledDecision<S>>, CloudError> {
        if !self.owns(&decision).await {
            return Ok(Some(HandledDecision {
                decision,
                outcome: DecisionOutcome::NotMine,
                result: None,
            }));
        }
        // Loaded before resuming, so another graph's thread is left to the
        // worker running that graph, with nothing run here
        // (design/31-Graph-Identity.md). The resume goes on from this load, so
        // it costs no request.
        let result = match self.store.load_latest(&decision.thread_id).await {
            Ok(Some(latest)) if !self.graph.owns(&latest.checkpoint) => {
                return Ok(Some(HandledDecision {
                    decision,
                    outcome: DecisionOutcome::NotMine,
                    result: None,
                }));
            }
            Ok(loaded) => {
                let config = self
                    .run_config
                    .as_ref()
                    .map(|config| config(&decision))
                    .unwrap_or_default();
                let pinned: Arc<dyn CheckpointStore> = Arc::new(PinnedStore {
                    inner: self.store.clone(),
                    sequence: decision.sequence,
                    loaded,
                });
                self.graph
                    .resume_with(
                        decision.thread_id.clone(),
                        decision.signal.clone(),
                        pinned,
                        config,
                    )
                    .await
            }
            Err(err) => GraphExecutionResult::Failed(StrataError::ExecutionAborted(err.0)),
        };
        let outcome = match &result {
            GraphExecutionResult::Failed(err) => {
                // The store's problem, not the decision's: the round fails and
                // the decision is tried again.
                if error::is_unavailable(err) {
                    return Err(CloudError::Unavailable {
                        status: None,
                        code: None,
                        retry_after: None,
                        attempts: 1,
                        cause: match err {
                            StrataError::ExecutionAborted(message) => Some(message.clone()),
                            other => Some(format!("{other:?}")),
                        },
                    });
                }
                if error::is_unauthorized(err) {
                    return Err(CloudError::Unauthorized {
                        code: None,
                        attempts: 1,
                    });
                }
                if held_by_another(&decision.thread_id, err) {
                    return Ok(None);
                }
                // The run's scheduler had no slot for it
                // (design/40-Run-Scheduler.md): nothing was tried, so the
                // decision is left for the next round like a held thread's.
                if matches!(err, StrataError::NotAdmitted { .. }) {
                    return Ok(None);
                }
                if someone_else_acted(&decision.thread_id, err) {
                    DecisionOutcome::AlreadyHandled
                } else {
                    DecisionOutcome::Failed
                }
            }
            _ => DecisionOutcome::Resumed,
        };
        Ok(Some(HandledDecision {
            decision,
            outcome,
            result: Some(result),
        }))
    }

    async fn owns(&self, decision: &CloudDecision) -> bool {
        if let Some(prefix) = &self.thread_id_prefix
            && !decision.thread_id.starts_with(prefix.as_str())
        {
            return false;
        }
        match &self.owns {
            Some(owns) => owns(decision).await,
            None => true,
        }
    }

    fn remember(&mut self, approval_id: String) {
        if self.seen.insert(approval_id.clone()) {
            self.seen_order.push_back(approval_id);
            if self.seen_order.len() > REMEMBERED
                && let Some(oldest) = self.seen_order.pop_front()
            {
                self.seen.remove(&oldest);
            }
        }
    }
}

/// The engine's refusals that mean another resume or an end acted on the
/// thread first (`design/28-Ending-A-Thread.md`, "Races"), matched exactly for
/// this thread: the checkpoint was stamped, it changed before this resume's
/// stamp, or the thread is running, which a durable graph's resume makes it
/// (`design/33-Durable-Execution.md`).
fn someone_else_acted(thread_id: &str, error: &StrataError) -> bool {
    let StrataError::ExecutionAborted(message) = error else {
        return false;
    };
    *message == format!("checkpoint for thread '{thread_id}' was already resumed")
        || *message
            == format!(
                "checkpoint for thread '{thread_id}' changed before it could be resumed: another resume or end got there first"
            )
        || *message
            == format!(
                "thread '{thread_id}' is running: only a waiting, failed or cancelled thread can be resumed"
            )
}

/// The engine's refusal of a thread another owner's lease holds
/// (`design/32-Thread-Leases.md`), matched for this thread.
fn held_by_another(thread_id: &str, error: &StrataError) -> bool {
    matches!(error, StrataError::ExecutionAborted(message)
        if message.starts_with(&format!("thread '{thread_id}' is held by '")))
}

/// The store a decision's resume goes through: the latest checkpoint, the one
/// the worker already `loaded`, is reported at the decision's sequence, so the
/// resume's conditional stamp writes only if the thread is still at the
/// checkpoint the reviewer decided (with a store that has the conditional
/// save).
struct PinnedStore {
    inner: Arc<dyn CheckpointStore>,
    sequence: i64,
    loaded: Option<LatestCheckpoint>,
}

impl CheckpointStore for PinnedStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.inner.save(checkpoint)
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        self.inner.load(thread_id)
    }

    fn load_latest<'a>(
        &'a self,
        _thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        let latest = self.loaded.clone().map(|latest| LatestCheckpoint {
            sequence: Some(self.sequence),
            ..latest
        });
        Box::pin(std::future::ready(Ok(latest)))
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        self.inner.save_if_latest(checkpoint, expected_sequence)
    }

    // A resume given `RunConfig::with_lease` holds the thread through the
    // store (design/32-Thread-Leases.md).
    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        self.inner.try_acquire_lease(thread_id, owner, ttl)
    }

    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        self.inner.renew_lease(lease, ttl)
    }

    fn release_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
    ) -> BoxFuture<'a, Result<bool, CheckpointStoreError>> {
        self.inner.release_lease(lease)
    }

    fn get_lease<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        self.inner.get_lease(thread_id)
    }

    fn save_under_lease<'a>(
        &'a self,
        checkpoint: Checkpoint,
        lease: &'a ThreadLease,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>> {
        self.inner
            .save_under_lease(checkpoint, lease, expected_sequence)
    }
}
