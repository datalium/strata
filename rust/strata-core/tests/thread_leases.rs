//! Running a thread under a lease (`design/32-Thread-Leases.md`, SG-12),
//! through the public API only. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/ThreadLeaseTests.cs` — keep the two in step.
//!
//! Every test runs on a paused tokio clock, which both the run's renewals and
//! the store's leases follow, so a ttl passes without waiting.

mod lease_store;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use datalium_strata::checkpoint::{
    CancelReason, CheckpointStore, CheckpointStoreError, EventSignal, GraphExecutionResult,
    HumanSignal, LatestCheckpoint, LeaseAttempt, LeaseOptions, ThreadEnd, ThreadLease,
};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, NodeError, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind, RunOutcome};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::store::LeasedSave;
use datalium_strata::wait::WakeOptions;
use datalium_strata::{BoxFuture, CancellationToken, StrataError};
use lease_store::{LeaseStore, TTL};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

const PATIENCE: Duration = Duration::from_secs(600);

/// Forwards everything to a [`LeaseStore`] but what `acquire` and `renew`
/// override: what a store that wraps another must do to keep its leases.
struct Wrapping {
    inner: Arc<LeaseStore>,
    /// While set, `worker-2` takes the thread the moment a run has acquired it.
    usurp: AtomicBool,
    /// While set, renewals fail, as against a store that can't be reached.
    renewals_fail: AtomicBool,
    renewals: AtomicU32,
}

impl Wrapping {
    fn new(inner: Arc<LeaseStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            usurp: AtomicBool::new(false),
            renewals_fail: AtomicBool::new(false),
            renewals: AtomicU32::new(0),
        })
    }
}

impl CheckpointStore for Wrapping {
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
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        self.inner.load_latest(thread_id)
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        self.inner.save_if_latest(checkpoint, expected_sequence)
    }

    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        Box::pin(async move {
            let attempt = self.inner.try_acquire_lease(thread_id, owner, ttl).await?;
            if self.usurp.load(Ordering::SeqCst) && matches!(attempt, LeaseAttempt::Acquired(_)) {
                self.inner.take_over(thread_id);
            }
            Ok(attempt)
        })
    }

    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        self.renewals.fetch_add(1, Ordering::SeqCst);
        if self.renewals_fail.load(Ordering::SeqCst) {
            return Box::pin(std::future::ready(Err(CheckpointStoreError(
                "the store is unreachable".to_string(),
            ))));
        }
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

/// Only `save` and `load`: the trait's defaults supply the rest, leases not
/// among them.
#[derive(Default)]
struct PlainStore(Mutex<HashMap<String, Checkpoint>>);

impl CheckpointStore for PlainStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.0
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
        Box::pin(std::future::ready(Ok(())))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(self
            .0
            .lock()
            .unwrap()
            .get(thread_id)
            .cloned())))
    }
}

// --- Graphs ------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct WireTransfer {
    invoice_generated: bool,
    amount: f64,
    currency: String,
    approved_by: Option<String>,
    is_completed: bool,
}

fn wire_transfer() -> WireTransfer {
    WireTransfer {
        invoice_generated: false,
        amount: 4200.5,
        currency: "USD".to_string(),
        approved_by: None,
        is_completed: false,
    }
}

/// What `Prepare` does before it commits.
type During = Arc<dyn Fn(NodeContext) -> BoxFuture<'static, ()> + Send + Sync>;

fn during<F, Fut>(hook: F) -> Option<During>
where
    F: Fn(NodeContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Some(Arc::new(move |context| Box::pin(hook(context))))
}

/// How many times each node of [`wire_transfer_graph`] ran.
#[derive(Clone, Default)]
struct Ran {
    prepared: Arc<AtomicU32>,
    executed: Arc<AtomicU32>,
}

impl Ran {
    fn counts(&self) -> (u32, u32) {
        (
            self.prepared.load(Ordering::SeqCst),
            self.executed.load(Ordering::SeqCst),
        )
    }
}

/// `Prepare` (a boundary, which runs `hook` first) -> `Execute`, and how many
/// times each ran.
fn wire_transfer_graph(hook: Option<During>) -> (Arc<CompiledGraph<WireTransfer>>, Ran) {
    let ran = Ran::default();
    let (prepared, executed) = (ran.prepared.clone(), ran.executed.clone());
    let graph = StateGraph::<WireTransfer>::new()
        .add_async_node("Prepare", move |mut s: WireTransfer, context| {
            prepared.fetch_add(1, Ordering::SeqCst);
            let hook = hook.clone();
            async move {
                if let Some(hook) = hook {
                    hook(context).await;
                }
                s.invoice_generated = true;
                s
            }
        })
        .add_interrupt_boundary("Prepare")
        .add_node("Execute", move |mut s: WireTransfer| {
            executed.fetch_add(1, Ordering::SeqCst);
            s.is_completed = true;
            s
        })
        .add_edge("Prepare", "Execute")
        .compile()
        .unwrap();
    (Arc::new(graph), ran)
}

// --- Helpers -----------------------------------------------------------------

fn leased(owner: &str) -> RunConfig {
    RunConfig::new().with_lease(LeaseOptions::new(owner, TTL))
}

fn refused<S>(result: GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => message,
        GraphExecutionResult::Failed(error) => format!("{error:?}"),
        _ => panic!("expected a refusal"),
    }
}

/// The `(node, state, stopped_at)` of a run that lost its lease; panics on
/// anything else.
fn lease_lost<S>(result: GraphExecutionResult<S>) -> (String, S, Vec<String>) {
    match result {
        GraphExecutionResult::Cancelled {
            reason: CancelReason::LeaseLost,
            node,
            state,
            stopped_at,
            ..
        } => (node, state, stopped_at),
        GraphExecutionResult::Cancelled { reason, .. } => {
            panic!("expected a lost lease, got a run cancelled as {reason:?}")
        }
        GraphExecutionResult::Failed(error) => panic!("expected a lost lease, got: {error:?}"),
        GraphExecutionResult::Interrupted { .. } => panic!("expected a lost lease, got a freeze"),
        GraphExecutionResult::Completed(_) => panic!("expected a lost lease, got a completion"),
    }
}

/// `time` rounded up to the second, as a refusal writes it.
fn text(time: DateTime<Utc>) -> String {
    let floored = time.with_nanosecond(0).unwrap();
    let rounded = if floored == time {
        floored
    } else {
        floored + chrono::Duration::seconds(1)
    };
    rounded.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn holder(lease: Option<ThreadLease>) -> Option<(String, i64)> {
    lease.map(|lease| (lease.owner, lease.token))
}

async fn events_of(mut events: tokio::sync::mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        all.push(event);
    }
    all
}

fn name(event: &RunEvent) -> &'static str {
    match event.kind {
        RunEventKind::RunStarted { .. } => "RunStarted",
        RunEventKind::LeaseAcquired { .. } => "LeaseAcquired",
        RunEventKind::LeaseLost { .. } => "LeaseLost",
        RunEventKind::Resumed { .. } => "Resumed",
        RunEventKind::NodeStarted => "NodeStarted",
        RunEventKind::NodeCompleted { .. } => "NodeCompleted",
        RunEventKind::CheckpointSaved { .. } => "CheckpointSaved",
        RunEventKind::Interrupted { .. } => "Interrupted",
        RunEventKind::RunCompleted { .. } => "RunCompleted",
        _ => "other",
    }
}

fn names(events: &[RunEvent]) -> Vec<&'static str> {
    events.iter().map(name).collect()
}

// --- Holding -----------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_run_holds_its_lease_while_it_runs_and_releases_it_when_it_freezes() {
    let store = LeaseStore::new();
    let seen: Arc<Mutex<Option<ThreadLease>>> = Arc::default();
    let (graph, _) = wire_transfer_graph(during({
        let (store, seen) = (store.clone(), seen.clone());
        move |context| {
            *seen.lock().unwrap() = store.held(context.thread_id());
            std::future::ready(())
        }
    }));
    let (observer, events) = RunEventChannel::new();
    let started = store.now();

    let result = graph
        .run_with(
            "t",
            wire_transfer(),
            store.clone(),
            leased("worker-1").with_observer(observer),
        )
        .await;

    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    let held = seen.lock().unwrap().clone().expect("held while it ran");
    assert_eq!(
        (held.thread_id.as_str(), held.owner.as_str(), held.token),
        ("t", "worker-1", 1)
    );
    assert_eq!(held.expires_at, started + chrono::Duration::seconds(30));
    assert_eq!(store.held("t"), None);
    assert_eq!(store.latest("t").status, CheckpointStatus::Interrupted);

    // The lease is the first thing the run takes, before any node starts.
    let events = events_of(events).await;
    assert_eq!(
        names(&events),
        [
            "RunStarted",
            "LeaseAcquired",
            "NodeStarted",
            "NodeCompleted",
            "CheckpointSaved",
            "Interrupted",
            "RunCompleted"
        ]
    );
    match &events[1].kind {
        RunEventKind::LeaseAcquired {
            owner,
            token,
            expires_at,
        } => assert_eq!(
            (owner.as_str(), *token, *expires_at),
            ("worker-1", 1, held.expires_at)
        ),
        other => panic!("{other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn a_held_thread_refuses_a_run_and_runs_nothing() {
    let store = LeaseStore::new();
    let (graph, ran) = wire_transfer_graph(None);
    let holder_lease = store.acquire("t", "worker-2").unwrap();

    let result = graph
        .run_with("t", wire_transfer(), store.clone(), leased("worker-1"))
        .await;

    assert_eq!(
        refused(result),
        format!(
            "thread 't' is held by 'worker-2' until {}",
            text(holder_lease.expires_at)
        )
    );
    assert_eq!(ran.counts(), (0, 0));
    assert!(store.log("t").is_empty());
    assert_eq!(store.held("t"), Some(holder_lease));

    // The same owner name is refused too: an owner names a process.
    let same = graph
        .run_with("t", wire_transfer(), store.clone(), leased("worker-2"))
        .await;
    assert!(refused(same).starts_with("thread 't' is held by 'worker-2' until "));

    // A streaming run is refused the same way, with no chunks.
    let (handle, mut chunks) =
        graph
            .clone()
            .run_streaming_with("t", wire_transfer(), store.clone(), leased("worker-1"));
    assert!(refused(handle.await.unwrap()).starts_with("thread 't' is held by 'worker-2' until "));
    assert!(chunks.recv().await.is_none());
    assert_eq!(ran.counts(), (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_held_thread_refuses_a_resume_a_signal_and_an_end_and_stays_resumable() {
    let store = LeaseStore::new();
    let (graph, ran) = wire_transfer_graph(None);
    let waiting = StateGraph::<i32>::new()
        .add_wait_for_event("Deploying", "deployment.finished")
        .add_node("After", |n: i32| n + 1)
        .add_edge("Deploying", "After")
        .compile()
        .unwrap();
    graph
        .run_with("t", wire_transfer(), store.clone(), leased("worker-1"))
        .await;
    waiting
        .run_with("w", 1, store.clone(), leased("worker-1"))
        .await;
    let on_t = store.acquire("t", "worker-2").unwrap();
    let on_w = store.acquire("w", "worker-2").unwrap();
    let held = "is held by 'worker-2' until ";

    let resumed = graph
        .resume_with(
            "t",
            HumanSignal::approve("alice"),
            store.clone(),
            leased("worker-1"),
        )
        .await;
    assert!(refused(resumed).contains(held));
    let ended = graph
        .end_with(
            "t",
            ThreadEnd::by("oncall"),
            store.clone(),
            leased("worker-1"),
        )
        .await;
    assert!(refused(ended).contains(held));
    let signalled = waiting
        .signal_with(
            "w",
            EventSignal::new("deployment.finished"),
            store.clone(),
            leased("worker-1"),
        )
        .await;
    assert!(refused(signalled).contains(held));

    // Nothing was written, and nothing ran.
    assert_eq!((store.log("t").len(), store.log("w").len()), (1, 1));
    assert_eq!(store.latest("t").resumed_at, None);
    assert_eq!(store.latest("w").resumed_at, None);
    assert_eq!(ran.counts(), (1, 0));

    // Once the holder lets go, both go on.
    assert!(store.release(&on_t));
    assert!(store.release(&on_w));
    assert!(matches!(
        graph
            .resume_with(
                "t",
                HumanSignal::approve("alice"),
                store.clone(),
                leased("worker-1")
            )
            .await,
        GraphExecutionResult::Completed(_)
    ));
    match waiting
        .signal_with(
            "w",
            EventSignal::new("deployment.finished"),
            store.clone(),
            leased("worker-1"),
        )
        .await
    {
        GraphExecutionResult::Completed(n) => assert_eq!(n, 2),
        _ => panic!("the signal goes through once the thread is free"),
    }
}

#[tokio::test(start_paused = true)]
async fn a_refused_resume_never_takes_the_lease() {
    let store = LeaseStore::new();
    let (graph, _) = wire_transfer_graph(None);
    graph
        .run_with("t", wire_transfer(), store.clone(), leased("worker-1"))
        .await;
    let observed = || {
        let (observer, events) = RunEventChannel::new();
        (leased("worker-1").with_observer(observer), events)
    };

    // An override that doesn't fit the state, a thread that doesn't exist,
    // and an end of one.
    let misfit = HumanSignal::approve("alice").with_override("amount", "not a number");
    let (config, first) = observed();
    refused(graph.resume_with("t", misfit, store.clone(), config).await);
    let (config, second) = observed();
    refused(
        graph
            .resume_with(
                "missing",
                HumanSignal::approve("alice"),
                store.clone(),
                config,
            )
            .await,
    );
    let (config, third) = observed();
    refused(
        graph
            .end_with("missing", ThreadEnd::by("oncall"), store.clone(), config)
            .await,
    );

    for events in [first, second, third] {
        assert!(!names(&events_of(events).await).contains(&"LeaseAcquired"));
    }
    // Only the first run ever acquired: the thread's next token is 2, and
    // "missing" never had one.
    assert_eq!(store.acquire("t", "probe").unwrap().token, 2);
    assert_eq!(store.acquire("missing", "probe").unwrap().token, 1);
}

#[tokio::test(start_paused = true)]
async fn a_resume_and_an_end_run_under_the_lease_and_release_it() {
    let store = LeaseStore::new();
    let (graph, ran) = wire_transfer_graph(None);
    for thread in ["resumed", "ended"] {
        graph
            .run_with(thread, wire_transfer(), store.clone(), leased("worker-1"))
            .await;
    }
    let (resume_observer, resume_events) = RunEventChannel::new();
    let (end_observer, end_events) = RunEventChannel::new();

    let resumed = graph
        .resume_with(
            "resumed",
            HumanSignal::approve("alice"),
            store.clone(),
            leased("worker-1").with_observer(resume_observer),
        )
        .await;
    let ended = graph
        .end_with(
            "ended",
            ThreadEnd::by("oncall"),
            store.clone(),
            leased("worker-1").with_observer(end_observer),
        )
        .await;

    assert!(matches!(resumed, GraphExecutionResult::Completed(_)));
    assert!(matches!(
        ended,
        GraphExecutionResult::Cancelled {
            reason: CancelReason::Ended,
            ..
        }
    ));
    assert!(store.latest("resumed").resumed_at.is_some());
    assert_eq!(store.latest("ended").status, CheckpointStatus::Ended);
    assert_eq!(ran.counts(), (2, 1));
    assert_eq!(store.held("resumed"), None);
    assert_eq!(store.held("ended"), None);

    // Each took the thread's second lease, before its first save.
    for events in [resume_events, end_events] {
        let events = events_of(events).await;
        let tokens: Vec<i64> = events
            .iter()
            .filter_map(|event| match event.kind {
                RunEventKind::LeaseAcquired { token, .. } => Some(token),
                _ => None,
            })
            .collect();
        assert_eq!(tokens, [2]);
        let names = names(&events);
        let at = |name: &str| names.iter().position(|n| *n == name).unwrap();
        assert!(at("LeaseAcquired") < at("CheckpointSaved"), "{names:?}");
    }

    // An ended thread, ended again, writes nothing, so it takes no lease.
    graph.end("ended", store.clone()).await;
    assert_eq!(store.acquire("ended", "probe").unwrap().token, 3);
}

// --- Renewing ----------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_run_longer_than_its_ttl_keeps_its_lease_by_renewing() {
    let store = LeaseStore::new();
    let (started, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (graph, _) = wire_transfer_graph(during({
        let (started, release) = (started.clone(), release.clone());
        move |_| {
            let (started, release) = (started.clone(), release.clone());
            async move {
                started.notify_one();
                release.notified().await;
            }
        }
    }));
    let acquired_at = store.now();

    let run = tokio::spawn({
        let (graph, store) = (graph.clone(), store.clone());
        async move {
            graph
                .run_with("t", wire_transfer(), store, leased("worker-1"))
                .await
        }
    });
    started.notified().await;

    // More than two ttls go by: only renewing keeps the thread held.
    tokio::time::sleep(TTL * 7 / 3 + Duration::from_secs(1)).await;
    let held = store.held("t").expect("still held");
    assert_eq!((held.owner.as_str(), held.token), ("worker-1", 1));
    assert!(held.expires_at > acquired_at + chrono::Duration::seconds(90));
    assert_eq!(store.acquire("t", "worker-2"), None);

    release.notify_one();
    assert!(matches!(
        tokio::time::timeout(PATIENCE, run).await.unwrap().unwrap(),
        GraphExecutionResult::Interrupted { .. }
    ));
    assert_eq!(store.held("t"), None);

    // Nothing renews a lease the run has given up.
    tokio::time::sleep(TTL).await;
    assert_eq!(store.held("t"), None);
}

#[tokio::test(start_paused = true)]
async fn renewals_failing_for_a_whole_ttl_lose_the_lease() {
    let inner = LeaseStore::new();
    let store = Wrapping::new(inner.clone());
    let started = Arc::new(Notify::new());
    let (graph, _) = wire_transfer_graph(during({
        let started = started.clone();
        move |_| {
            let started = started.clone();
            async move {
                started.notify_one();
                std::future::pending::<()>().await;
            }
        }
    }));

    let run = tokio::spawn({
        let (graph, store) = (graph.clone(), store.clone());
        async move {
            graph
                .run_with("t", wire_transfer(), store, leased("worker-1"))
                .await
        }
    });
    started.notified().await;
    store.renewals_fail.store(true, Ordering::SeqCst);

    // Two failed renewals aren't a loss: the run goes on.
    tokio::time::sleep(TTL * 2 / 3 + Duration::from_secs(1)).await;
    assert_eq!(store.renewals.load(Ordering::SeqCst), 2);
    assert!(!run.is_finished());

    // The third makes a whole ttl without one: the run can't show it holds
    // the thread any more.
    let (node, _, _) = lease_lost(tokio::time::timeout(PATIENCE, run).await.unwrap().unwrap());
    assert_eq!(node, "Prepare");
    assert_eq!(store.renewals.load(Ordering::SeqCst), 3);
    assert!(inner.log("t").is_empty());
}

#[tokio::test(start_paused = true)]
async fn renewals_failing_for_less_than_a_ttl_lose_nothing() {
    let inner = LeaseStore::new();
    let store = Wrapping::new(inner.clone());
    let (started, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (graph, _) = wire_transfer_graph(during({
        let (started, release) = (started.clone(), release.clone());
        move |_| {
            let (started, release) = (started.clone(), release.clone());
            async move {
                started.notify_one();
                release.notified().await;
            }
        }
    }));

    let run = tokio::spawn({
        let (graph, store) = (graph.clone(), store.clone());
        async move {
            graph
                .run_with("t", wire_transfer(), store, leased("worker-1"))
                .await
        }
    });
    started.notified().await;
    store.renewals_fail.store(true, Ordering::SeqCst);
    tokio::time::sleep(TTL * 2 / 3 + Duration::from_secs(1)).await;
    assert_eq!(store.renewals.load(Ordering::SeqCst), 2);

    // The store is back: the next renewal succeeds, and the count of time
    // without one starts over.
    store.renewals_fail.store(false, Ordering::SeqCst);
    tokio::time::sleep(TTL * 4 / 3).await;
    assert_eq!(store.renewals.load(Ordering::SeqCst), 6);
    assert!(!run.is_finished());

    release.notify_one();
    assert!(matches!(
        tokio::time::timeout(PATIENCE, run).await.unwrap().unwrap(),
        GraphExecutionResult::Interrupted { .. }
    ));
    assert_eq!(inner.latest("t").status, CheckpointStatus::Interrupted);
}

// --- Losing ------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_lease_taken_over_mid_run_cancels_the_run_and_writes_nothing() {
    let store = LeaseStore::new();
    let started = Arc::new(Notify::new());
    let handler_token: Arc<Mutex<Option<CancellationToken>>> = Arc::default();
    let (graph, ran) = wire_transfer_graph(during({
        let (started, handler_token) = (started.clone(), handler_token.clone());
        move |context: NodeContext| {
            let started = started.clone();
            *handler_token.lock().unwrap() = Some(context.cancellation().clone());
            // A handler that never finishes on its own: the engine stops
            // waiting for it when its token fires.
            async move {
                started.notify_one();
                std::future::pending::<()>().await;
            }
        }
    }));
    let (observer, events) = RunEventChannel::new();

    let run = tokio::spawn({
        let (graph, store) = (graph.clone(), store.clone());
        async move {
            graph
                .run_with(
                    "t",
                    wire_transfer(),
                    store,
                    leased("worker-1").with_observer(observer),
                )
                .await
        }
    });
    started.notified().await;

    // The worker stalls past its ttl, another takes the thread, and the
    // worker's next renewal finds out.
    let usurper = store.take_over("t");
    let result = tokio::time::timeout(PATIENCE, run).await.unwrap().unwrap();
    let (node, state, stopped_at) = lease_lost(result);

    assert_eq!(node, "Prepare");
    assert_eq!(stopped_at, ["Prepare"]);
    assert_eq!(state, wire_transfer());
    let token = handler_token.lock().unwrap().clone().unwrap();
    assert!(token.is_cancelled());
    assert_eq!(ran.counts(), (1, 0));

    // Not even a cancelled checkpoint: the thread's history is the new
    // owner's, who still holds it.
    assert!(store.log("t").is_empty());
    assert_eq!(store.held("t"), Some(usurper));

    let events = events_of(events).await;
    let lost = events
        .iter()
        .position(|event| {
            matches!(&event.kind, RunEventKind::LeaseLost { owner, token }
                if owner == "worker-1" && *token == 1)
        })
        .expect("a LeaseLost event");
    assert!(!names(&events).contains(&"CheckpointSaved"));
    assert!(matches!(
        events.last().unwrap().kind,
        RunEventKind::RunCompleted {
            outcome: RunOutcome::Cancelled,
            cancel_reason: Some(CancelReason::LeaseLost),
            ..
        }
    ));
    assert!(lost < events.len() - 1);
}

/// A hook that lets `worker-2` take the thread while `Prepare` runs. The run's
/// own clock doesn't move, so no renewal tells it: its save is what is
/// refused.
fn taken_over_meanwhile(store: &Arc<LeaseStore>) -> Option<During> {
    let store = store.clone();
    during(move |context: NodeContext| {
        store.take_over(context.thread_id());
        std::future::ready(())
    })
}

#[tokio::test(start_paused = true)]
async fn a_fenced_interrupted_checkpoint_becomes_a_lost_lease() {
    let store = LeaseStore::new();
    let (graph, _) = wire_transfer_graph(taken_over_meanwhile(&store));

    let result = graph
        .run_with("t", wire_transfer(), store.clone(), leased("worker-1"))
        .await;
    let (node, state, stopped_at) = lease_lost(result);

    assert_eq!(node, "Prepare");
    assert_eq!(stopped_at, ["Prepare"]);
    assert!(state.invoice_generated);
    assert!(store.log("t").is_empty());
    assert_eq!(holder(store.held("t")), Some(("worker-2".to_string(), 2)));
}

#[tokio::test(start_paused = true)]
async fn a_fenced_failed_checkpoint_becomes_a_lost_lease() {
    let store = LeaseStore::new();
    let graph = StateGraph::<WireTransfer>::new()
        .add_async_fallible_node("Charge", {
            let store = store.clone();
            move |_: WireTransfer, context: NodeContext| {
                store.take_over(context.thread_id());
                async move { Err(NodeError::Fatal("gateway refused".to_string())) }
            }
        })
        .compile()
        .unwrap();

    // Not a node failure: the failure isn't recorded, so it isn't the
    // thread's outcome.
    let result = graph
        .run_with("t", wire_transfer(), store.clone(), leased("worker-1"))
        .await;
    let (node, state, _) = lease_lost(result);

    assert_eq!(node, "Charge");
    assert_eq!(state, wire_transfer());
    assert!(store.log("t").is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_fenced_cancelled_checkpoint_becomes_a_lost_lease() {
    let store = LeaseStore::new();
    let cancel = CancellationToken::new();
    let (graph, _) = wire_transfer_graph(during({
        let (store, cancel) = (store.clone(), cancel.clone());
        move |context: NodeContext| {
            store.take_over(context.thread_id());
            cancel.cancel();
            std::future::pending::<()>()
        }
    }));

    // The caller cancelled, but the run finds its lease gone when it goes to
    // say so.
    let result = graph
        .run_with(
            "t",
            wire_transfer(),
            store.clone(),
            leased("worker-1").with_cancellation(cancel),
        )
        .await;
    let (node, _, _) = lease_lost(result);

    assert_eq!(node, "Prepare");
    assert!(store.log("t").is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_fenced_resume_stamp_runs_nothing_and_a_fenced_end_ends_nothing() {
    let inner = LeaseStore::new();
    let store = Wrapping::new(inner.clone());
    let (graph, ran) = wire_transfer_graph(None);
    graph
        .run_with("t", wire_transfer(), store.clone(), leased("worker-1"))
        .await;
    store.usurp.store(true, Ordering::SeqCst);

    let resumed = graph
        .resume_with(
            "t",
            HumanSignal::approve("alice"),
            store.clone(),
            leased("worker-1"),
        )
        .await;
    let (node, _, _) = lease_lost(resumed);

    assert_eq!(node, "Execute");
    assert_eq!(ran.counts(), (1, 0));
    assert_eq!(inner.latest("t").resumed_at, None);
    assert_eq!(inner.log("t").len(), 1);

    inner.advance(TTL + Duration::from_millis(1));
    let ended = graph
        .end_with(
            "t",
            ThreadEnd::by("oncall"),
            store.clone(),
            leased("worker-1"),
        )
        .await;
    let (node, _, _) = lease_lost(ended);

    assert_eq!(node, "Prepare");
    assert_eq!(inner.latest("t").status, CheckpointStatus::Interrupted);
    assert_eq!(inner.log("t").len(), 1);

    // The thread is still resumable by whoever holds it next.
    store.usurp.store(false, Ordering::SeqCst);
    inner.advance(TTL + Duration::from_millis(1));
    assert!(matches!(
        graph
            .resume_with(
                "t",
                HumanSignal::approve("alice"),
                store.clone(),
                leased("worker-1")
            )
            .await,
        GraphExecutionResult::Completed(_)
    ));
}

#[tokio::test(start_paused = true)]
async fn a_lease_lost_inside_a_subgraph_stops_the_child() {
    let store = LeaseStore::new();
    let started = Arc::new(Notify::new());
    let after = Arc::new(AtomicU32::new(0));
    let child = StateGraph::<i32>::new()
        .add_async_node("Charge", {
            let started = started.clone();
            move |n: i32, _| {
                let started = started.clone();
                async move {
                    started.notify_one();
                    std::future::pending::<()>().await;
                    n
                }
            }
        })
        .add_node("Settle", {
            let after = after.clone();
            move |n: i32| {
                after.fetch_add(1, Ordering::SeqCst);
                n
            }
        })
        .add_edge("Charge", "Settle")
        .compile()
        .unwrap();
    let graph = Arc::new(
        StateGraph::<i32>::new()
            .add_subgraph("Work", child, |n: &i32| *n, |_: &i32, n: i32| n)
            .add_node("Finish", {
                let after = after.clone();
                move |n: i32| {
                    after.fetch_add(1, Ordering::SeqCst);
                    n
                }
            })
            .add_edge("Work", "Finish")
            .compile()
            .unwrap(),
    );

    let run = tokio::spawn({
        let (graph, store) = (graph.clone(), store.clone());
        async move { graph.run_with("t", 7, store, leased("worker-1")).await }
    });
    started.notified().await;
    store.take_over("t");
    let (node, state, stopped_at) =
        lease_lost(tokio::time::timeout(PATIENCE, run).await.unwrap().unwrap());

    // One lease covers every level of the run.
    assert_eq!(node, "Work");
    assert_eq!(stopped_at, ["Work", "Charge"]);
    assert_eq!(state, 7);
    assert_eq!(after.load(Ordering::SeqCst), 0);
    assert!(store.log("t").is_empty());
}

// --- Options and stores ------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_store_without_leases_refuses_a_run_with_one() {
    let (graph, ran) = wire_transfer_graph(None);
    let store = Arc::new(PlainStore::default());

    let result = graph
        .run_with("t", wire_transfer(), store.clone(), leased("worker-1"))
        .await;

    assert_eq!(refused(result), "this checkpoint store has no leases");
    assert_eq!(ran.counts(), (0, 0));
    assert_eq!(store.load("t").await.unwrap(), None);

    // Without a lease it runs as it always did.
    assert!(matches!(
        graph.run("t", wire_transfer(), store).await,
        GraphExecutionResult::Interrupted { .. }
    ));
}

#[tokio::test(start_paused = true)]
async fn bad_lease_options_are_refused_before_the_run_starts() {
    let (graph, ran) = wire_transfer_graph(None);
    let store = LeaseStore::new();

    for (lease, why) in [
        (LeaseOptions::new(" ", TTL), "a run's lease needs an owner"),
        (
            LeaseOptions::new("worker-1", Duration::ZERO),
            "a run's lease ttl must be at least 1 millisecond",
        ),
    ] {
        let config = || RunConfig::new().with_lease(lease.clone());
        assert_eq!(
            refused(
                graph
                    .run_with("t", wire_transfer(), store.clone(), config())
                    .await
            ),
            why
        );
        assert_eq!(
            refused(
                graph
                    .resume_with("t", HumanSignal::approve("a"), store.clone(), config())
                    .await
            ),
            why
        );
        assert_eq!(
            refused(
                graph
                    .end_with("t", ThreadEnd::by("a"), store.clone(), config())
                    .await
            ),
            why
        );
    }

    assert_eq!(ran.counts(), (0, 0));
    assert_eq!(store.acquire("t", "probe").unwrap().token, 1);
}

// --- The wake driver ---------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn the_wake_driver_skips_a_held_thread_and_wakes_it_once_released() {
    let store = LeaseStore::new();
    let graph = StateGraph::<i32>::new()
        .add_timer("Window", Duration::from_secs(60))
        .add_node("After", |n: i32| n + 1)
        .add_edge("Window", "After")
        .compile()
        .unwrap();
    graph
        .run_with("t", 1, store.clone(), leased("worker-1"))
        .await;
    let due = Utc::now() + chrono::Duration::minutes(2);
    let holder_lease = store.acquire("t", "worker-2").unwrap();
    let options = || WakeOptions::new().with_config(leased("worker-1"));

    let skipped = graph
        .wake_due_with(store.clone(), due, options())
        .await
        .unwrap();

    assert!(skipped.woken.is_empty());
    let reasons: Vec<(&str, &str)> = skipped
        .skipped
        .iter()
        .map(|skipped| (skipped.thread_id.as_str(), skipped.reason.as_str()))
        .collect();
    let expected = format!(
        "it is held by 'worker-2' until {}",
        text(holder_lease.expires_at)
    );
    assert_eq!(reasons, [("t", expected.as_str())]);
    assert_eq!(store.latest("t").resumed_at, None);

    assert!(store.release(&holder_lease));
    let woken = graph
        .wake_due_with(store.clone(), due, options())
        .await
        .unwrap();

    assert!(woken.skipped.is_empty());
    assert_eq!(woken.woken.len(), 1);
    match &woken.woken[0].result {
        GraphExecutionResult::Completed(n) => assert_eq!(*n, 2),
        _ => panic!("the timer wakes once the thread is free"),
    }
    assert_eq!(store.held("t"), None);
}
