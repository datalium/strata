//! Waking a due thread through the application's own `resume` /
//! `resume_streaming` with `LevelSignals::due`, and an event signal through
//! them with `LevelSignals::event` (`design/42-Waking-Through-A-Resume.md`,
//! SG-35), through the public API only. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/WakeThroughResumeTests.cs` — keep the
//! two in step. Wake times come from `Utc::now()` at the freeze, so a test
//! reads them back and wakes with `LevelSignals::due_at`.

mod lease_store;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use datalium_strata::checkpoint::{
    CancelReason, CheckpointLogPage, CheckpointQuery, CheckpointStore, CheckpointStoreError,
    END_REASON_KEY, END_TIMED_OUT, EventSignal, GraphExecutionResult, HumanSignal,
    LatestCheckpoint, LeaseOptions, LevelSignals, ThreadPage, ThreadQuery, WakeReason,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{
    CheckpointPolicy, CompileOptions, CompiledGraph, NodeError, StateGraph,
};
use datalium_strata::observability::{RunEvent, RunEventKind, RunMode, RunObserver, RunOutcome};
use datalium_strata::scheduler::{AdmissionRequest, RunScheduler, SchedulerOptions};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::streaming::{StreamChunk, StreamChunkType, StreamContext};
use datalium_strata::wait::BoundaryOptions;
use datalium_strata::{BoxFuture, StrataError};
use lease_store::LeaseStore;
use serde::{Deserialize, Serialize};
use serde_json::json;

const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    log: Vec<String>,
}

fn job() -> Job {
    Job { log: Vec::new() }
}

fn note(label: &'static str) -> impl Fn(Job) -> Job + Send + Sync + 'static {
    move |mut job| {
        job.log.push(label.to_string());
        job
    }
}

fn talk(
    label: &'static str,
    tokens: &'static [&'static str],
) -> impl Fn(Job, &StreamContext) -> Job + Send + Sync + 'static {
    move |mut job, ctx| {
        for token in tokens {
            ctx.emit_token_blocking(token).unwrap();
        }
        job.log.push(label.to_string());
        job
    }
}

// --- Graphs ------------------------------------------------------------------

/// `Start` -> `Window` (a five-minute timer) -> `Talk` (streams) -> `Finish`.
fn timer_then_talk() -> StateGraph<Job> {
    StateGraph::new()
        .add_node("Start", note("start"))
        .add_timer("Window", Duration::from_secs(5 * 60))
        .add_streaming_node("Talk", talk("talk", &["Hel", "lo"]))
        .add_node("Finish", note("finish"))
        .add_edge("Start", "Window")
        .add_edge("Window", "Talk")
        .add_edge("Talk", "Finish")
}

/// `Remediate` -> `Verify` ([`timer_then_talk`] as a subgraph) -> `Report`
/// (streams).
fn nested() -> Arc<CompiledGraph<Job>> {
    Arc::new(
        StateGraph::new()
            .add_node("Remediate", note("remediate"))
            .add_subgraph(
                "Verify",
                timer_then_talk().compile().unwrap(),
                |job: &Job| job.clone(),
                |_: &Job, child: Job| child,
            )
            .add_streaming_node("Report", talk("report", &["done"]))
            .add_edge("Remediate", "Verify")
            .add_edge("Verify", "Report")
            .compile()
            .unwrap(),
    )
}

/// `Approve` (a boundary that times out after 30 minutes, to `Escalate` when
/// routed; no timeout at all without `timeout`) -> `Execute`.
fn approval(route: bool, timeout: bool) -> Arc<CompiledGraph<Job>> {
    let mut options = BoundaryOptions::new();
    if timeout {
        options = options.with_timeout(Duration::from_secs(30 * 60));
    }
    if route {
        options = options.with_on_timeout("Escalate");
    }
    let mut graph = StateGraph::new()
        .add_node("Approve", note("approve"))
        .add_interrupt_boundary_with("Approve", options)
        .add_node("Execute", note("execute"))
        .add_edge("Approve", "Execute");
    if route {
        graph = graph.add_node("Escalate", note("escalate"));
    }
    Arc::new(graph.compile().unwrap())
}

/// `Deploying` (waits for `deployment.finished`) -> `Talk` (streams).
fn deploy() -> Arc<CompiledGraph<Job>> {
    Arc::new(
        StateGraph::new()
            .add_wait_for_event("Deploying", "deployment.finished")
            .add_streaming_node("Talk", talk("talk", &["deployed"]))
            .add_edge("Deploying", "Talk")
            .compile()
            .unwrap(),
    )
}

// --- Helpers -----------------------------------------------------------------

/// Every event of every run, in arrival order.
#[derive(Default)]
struct Collector(Mutex<Vec<RunEvent>>);

impl RunObserver for Collector {
    fn on_event(&self, event: &RunEvent) {
        self.0.lock().unwrap().push(event.clone());
    }
}

impl Collector {
    async fn after(&self, runs: usize) -> Vec<RunEvent> {
        tokio::time::timeout(PATIENCE, async {
            loop {
                let events = self.0.lock().unwrap().clone();
                let completed = events
                    .iter()
                    .filter(|event| matches!(event.kind, RunEventKind::RunCompleted { .. }))
                    .count();
                if completed >= runs {
                    return events;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the runs' events")
    }
}

/// A [`LeaseStore`] whose `load_latest` holds each caller until a second one
/// has loaded too, once armed: two wakes that both decided a thread is due,
/// racing to wake it.
struct RacingStore {
    inner: Arc<LeaseStore>,
    armed: AtomicBool,
    barrier: tokio::sync::Barrier,
}

impl CheckpointStore for RacingStore {
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
        Box::pin(async move {
            let latest = self.inner.load_latest(thread_id).await;
            if self.armed.load(Ordering::SeqCst) {
                self.barrier.wait().await;
            }
            latest
        })
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        self.inner.save_if_latest(checkpoint, expected_sequence)
    }
}

impl CheckpointQuery for RacingStore {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        self.inner.list_threads(query)
    }

    fn get_log<'a>(
        &'a self,
        thread_id: &'a str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        self.inner.get_log(thread_id, after_sequence, limit)
    }
}

async fn drain(mut receiver: tokio::sync::mpsc::Receiver<StreamChunk>) -> Vec<StreamChunk> {
    let mut chunks = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push(chunk);
    }
    chunks
}

async fn stream(
    (handle, receiver): (
        tokio::task::JoinHandle<GraphExecutionResult<Job>>,
        tokio::sync::mpsc::Receiver<StreamChunk>,
    ),
) -> (Vec<StreamChunk>, GraphExecutionResult<Job>) {
    let chunks = tokio::time::timeout(PATIENCE, drain(receiver))
        .await
        .expect("the stream");
    let result = tokio::time::timeout(PATIENCE, handle)
        .await
        .expect("the run")
        .unwrap();
    (chunks, result)
}

/// Runs `graph` to its first wait, streaming, since some of these graphs'
/// children stream.
async fn freeze(
    graph: &Arc<CompiledGraph<Job>>,
    thread_id: &str,
    store: &Arc<dyn CheckpointStore>,
) {
    let (_, result) = stream(graph.clone().run_streaming(thread_id, job(), store.clone())).await;
    assert!(
        matches!(result, GraphExecutionResult::Interrupted { .. }),
        "expected the run to freeze"
    );
}

fn tokens_of(chunks: &[StreamChunk], node: &str) -> Vec<String> {
    chunks
        .iter()
        .filter(|c| c.chunk_type == StreamChunkType::Token && &*c.node_name == node)
        .map(|c| c.content.to_string())
        .collect()
}

fn refused<S>(result: GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => message,
        GraphExecutionResult::Failed(error) => format!("{error:?}"),
        _ => panic!("expected Failed"),
    }
}

fn completed(result: GraphExecutionResult<Job>) -> Job {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => panic!("expected Completed, got Failed({err:?})"),
        GraphExecutionResult::Interrupted { .. } => panic!("expected Completed, got Interrupted"),
        GraphExecutionResult::Cancelled { node, .. } => {
            panic!("expected Completed, got Cancelled at {node}")
        }
    }
}

/// `thread_id`'s innermost wake time, as a time.
fn wake_at(store: &LeaseStore, thread_id: &str) -> DateTime<Utc> {
    let latest = store.latest(thread_id);
    let text = latest.innermost_wait().unwrap().wake_at.clone().unwrap();
    DateTime::parse_from_rfc3339(&text)
        .unwrap()
        .with_timezone(&Utc)
}

fn later(minutes: i64) -> DateTime<Utc> {
    Utc::now() + chrono::Duration::minutes(minutes)
}

fn observed(collector: &Arc<Collector>) -> RunConfig {
    RunConfig::new().with_observer(collector.clone())
}

// --- Waking ------------------------------------------------------------------

#[tokio::test]
async fn a_due_timer_woken_through_a_streaming_resume_streams_and_completes() {
    let leases = LeaseStore::new();
    let store: Arc<dyn CheckpointStore> = leases.clone();
    let graph = Arc::new(timer_then_talk().compile().unwrap());
    freeze(&graph, "t", &store).await;
    let due = wake_at(&leases, "t");
    let collector = Arc::new(Collector::default());

    let (chunks, result) = stream(graph.clone().resume_streaming_with(
        "t",
        LevelSignals::due_at(due),
        store.clone(),
        observed(&collector),
    ))
    .await;

    assert_eq!(completed(result).log, ["start", "talk", "finish"]);
    assert_eq!(tokens_of(&chunks, "Talk"), ["Hel", "lo"]);
    assert_eq!(&*chunks[0].node_name, "Talk");
    // What `wake_due` writes: one resume stamp, no decision.
    let log = leases.log("t");
    assert_eq!(log.len(), 2);
    assert!(log[1].resumed_at.is_some());
    assert_eq!(log[1].decision, None);
    let events = collector.after(1).await;
    let started: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::RunStarted { mode, node, .. } => Some((*mode, node.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(started, [(RunMode::Resume, "Talk".to_string())]);
    let resumed: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::Resumed {
                wake, timed_out_to, ..
            } => Some((*wake, timed_out_to.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(resumed, [(WakeReason::Timer, None)]);
    // Woken, it isn't due any more.
    let report = graph.wake_due(leases.clone(), later(60)).await.unwrap();
    assert!(report.woken.is_empty());
}

#[tokio::test]
async fn a_due_timer_inside_a_subgraph_streams_the_child_and_then_the_outer_level() {
    let leases = LeaseStore::new();
    let store: Arc<dyn CheckpointStore> = leases.clone();
    let graph = nested();
    freeze(&graph, "n", &store).await;
    let due = wake_at(&leases, "n");

    let (chunks, result) = stream(graph.clone().resume_streaming(
        "n",
        LevelSignals::due_at(due),
        store.clone(),
    ))
    .await;

    assert_eq!(
        completed(result).log,
        ["remediate", "start", "talk", "finish", "report"]
    );
    assert_eq!(tokens_of(&chunks, "Verify/Talk"), ["Hel", "lo"]);
    assert_eq!(tokens_of(&chunks, "Report"), ["done"]);
}

#[tokio::test]
async fn a_due_timeout_takes_its_route() {
    let leases = LeaseStore::new();
    let store: Arc<dyn CheckpointStore> = leases.clone();
    let graph = approval(true, true);
    freeze(&graph, "a", &store).await;
    let due = wake_at(&leases, "a");
    let collector = Arc::new(Collector::default());

    let result = graph
        .resume_with(
            "a",
            LevelSignals::due_at(due),
            store.clone(),
            observed(&collector),
        )
        .await;

    assert_eq!(completed(result).log, ["approve", "escalate"]);
    let events = collector.after(1).await;
    assert!(events.iter().any(|e| matches!(
        &e.kind,
        RunEventKind::RunStarted { node, .. } if node == "Escalate"
    )));
    assert!(events.iter().any(|e| matches!(
        &e.kind,
        RunEventKind::Resumed { wake: WakeReason::Timeout, timed_out_to: Some(to), rejected_to: None, .. }
            if to == "Escalate"
    )));
    // A timeout isn't a human's answer: no decision.
    assert!(leases.log("a").iter().all(|c| c.decision.is_none()));
}

#[tokio::test]
async fn a_due_timeout_with_no_route_ends_the_thread() {
    let leases = LeaseStore::new();
    let store: Arc<dyn CheckpointStore> = leases.clone();
    let graph = approval(false, true);
    freeze(&graph, "a", &store).await;
    let due = wake_at(&leases, "a") + chrono::Duration::minutes(1);
    let collector = Arc::new(Collector::default());

    let (chunks, result) = stream(graph.clone().resume_streaming_with(
        "a",
        LevelSignals::due_at(due),
        store.clone(),
        observed(&collector),
    ))
    .await;

    assert!(chunks.is_empty());
    match result {
        GraphExecutionResult::Cancelled { reason, node, .. } => {
            assert_eq!((reason, node.as_str()), (CancelReason::Ended, "Approve"));
        }
        _ => panic!("expected Cancelled"),
    }
    let checkpoint = leases.latest("a");
    assert_eq!(checkpoint.status, CheckpointStatus::Ended);
    assert_eq!(
        checkpoint.metadata.as_ref().unwrap()[END_REASON_KEY],
        END_TIMED_OUT
    );
    assert_eq!(leases.log("a").len(), 2);
    // The run is the resume it was called as, and says it ended the thread.
    let events = collector.after(1).await;
    assert!(events.iter().any(|e| matches!(
        &e.kind,
        RunEventKind::RunStarted { mode: RunMode::Resume, node, .. } if node == "Approve"
    )));
    assert!(events.iter().any(|e| matches!(
        &e.kind,
        RunEventKind::CheckpointSaved {
            status: CheckpointStatus::Ended,
            ..
        }
    )));
    assert!(
        !events
            .iter()
            .any(|e| matches!(e.kind, RunEventKind::Resumed { .. }))
    );
    assert!(events.iter().any(|e| matches!(
        &e.kind,
        RunEventKind::RunCompleted {
            outcome: RunOutcome::Cancelled,
            cancel_reason: Some(CancelReason::Ended),
            ..
        }
    )));
    // Ended, it is neither due nor resumable.
    let again = graph
        .resume("a", LevelSignals::due_at(due), store.clone())
        .await;
    assert!(refused(again).contains("was ended"));
}

#[tokio::test]
async fn a_thread_that_is_not_due_is_refused_and_nothing_is_written() {
    let leases = LeaseStore::new();
    let store: Arc<dyn CheckpointStore> = leases.clone();
    let timers = Arc::new(timer_then_talk().compile().unwrap());
    let approvals = approval(false, false);
    let deploys = deploy();
    freeze(&timers, "timer", &store).await;
    freeze(&approvals, "human", &store).await;
    freeze(&deploys, "event", &store).await;
    let failing = StateGraph::<Job>::new()
        .add_fallible_node("Fail", |_: &Job| Err(NodeError::Fatal("boom".to_string())))
        .compile()
        .unwrap();
    assert!(matches!(
        failing.run("failed", job(), store.clone()).await,
        GraphExecutionResult::Failed(_)
    ));

    // One second early.
    let due = wake_at(&leases, "timer");
    let early = due - chrono::Duration::seconds(1);
    assert_eq!(
        refused(
            timers
                .resume("timer", LevelSignals::due_at(early), store.clone())
                .await
        ),
        format!(
            "thread 'timer' is not due until {}: it is waiting for a timer at [Window]",
            due.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        )
    );
    assert_eq!(
        refused(
            approvals
                .resume("human", LevelSignals::due(), store.clone())
                .await
        ),
        "thread 'human' is not due: it is waiting for a human at [Approve], with no timeout"
    );
    assert_eq!(
        refused(
            deploys
                .resume("event", LevelSignals::due(), store.clone())
                .await
        ),
        "thread 'event' is not due: it is waiting for event 'deployment.finished' at [Deploying], with no timeout"
    );
    assert_eq!(
        refused(
            failing
                .resume("failed", LevelSignals::due(), store.clone())
                .await
        ),
        "thread 'failed' is not due: its latest checkpoint is failed"
    );
    for thread in ["timer", "human", "event", "failed"] {
        assert_eq!(leases.log(thread).len(), 1, "{thread}");
    }

    // A second later it is.
    let (_, result) = stream(timers.clone().resume_streaming(
        "timer",
        LevelSignals::due_at(due),
        store.clone(),
    ))
    .await;
    assert_eq!(completed(result).log, ["start", "talk", "finish"]);
}

#[tokio::test]
async fn another_graphs_thread_an_ended_thread_and_outer_signals_are_refused_and_nothing_is_written()
 {
    let leases = LeaseStore::new();
    let store: Arc<dyn CheckpointStore> = leases.clone();

    // Another graph's, by identity.
    let named = Arc::new(
        timer_then_talk()
            .compile_with(CompileOptions::new().with_graph_id("verify"))
            .unwrap(),
    );
    let other = timer_then_talk()
        .compile_with(CompileOptions::new().with_graph_id("triage"))
        .unwrap();
    freeze(&named, "named", &store).await;

    // Another graph's, by its waits: this graph has no timer at [Window].
    let timers = Arc::new(timer_then_talk().compile().unwrap());
    freeze(&timers, "timer", &store).await;
    let approvals = approval(true, true);

    // Ended.
    freeze(&timers, "ended", &store).await;
    assert!(matches!(
        timers.end("ended", store.clone()).await,
        GraphExecutionResult::Cancelled { .. }
    ));

    // Outer-level signals with a wake.
    let nested = nested();
    freeze(&nested, "nested", &store).await;

    let now = later(60);
    assert!(
        refused(
            other
                .resume("named", LevelSignals::due_at(now), store.clone())
                .await
        )
        .contains("was written by graph 'verify'")
    );
    assert_eq!(
        refused(
            approvals
                .resume("timer", LevelSignals::due_at(now), store.clone())
                .await
        ),
        "thread 'timer' waits at [Window], which is not a timer in this graph"
    );
    assert_eq!(
        refused(
            timers
                .resume("ended", LevelSignals::due_at(now), store.clone())
                .await
        ),
        "thread 'ended' was ended: an ended thread can't be resumed"
    );
    assert_eq!(
        refused(
            nested
                .resume(
                    "nested",
                    LevelSignals::due_at(now).at(&[], HumanSignal::approve("alice")),
                    store.clone(),
                )
                .await
        ),
        "cannot resume thread 'nested': a wake or an event signal answers the innermost level only, and takes no signals for outer levels"
    );
    for thread in ["named", "timer", "nested"] {
        assert_eq!(leases.log(thread).len(), 1, "{thread}");
    }
    assert_eq!(leases.log("ended").len(), 2);
}

#[tokio::test]
async fn a_due_resume_and_a_wake_driver_wake_a_thread_once() {
    let racing = Arc::new(RacingStore {
        inner: LeaseStore::new(),
        armed: AtomicBool::new(false),
        barrier: tokio::sync::Barrier::new(2),
    });
    let store: Arc<dyn CheckpointStore> = racing.clone();
    let graph = Arc::new(timer_then_talk().compile().unwrap());
    freeze(&graph, "t", &store).await;
    let due = wake_at(&racing.inner, "t");
    racing.armed.store(true, Ordering::SeqCst);

    // Both load the thread before either wakes it.
    let resume = stream(graph.clone().resume_streaming(
        "t",
        LevelSignals::due_at(due),
        store.clone(),
    ));
    let driver = graph.wake_due(racing.clone(), due);
    let ((_, resumed), report) =
        tokio::time::timeout(PATIENCE, async { tokio::join!(resume, driver) })
            .await
            .expect("both wakes");
    let report = report.unwrap();

    match resumed {
        GraphExecutionResult::Completed(_) => {
            assert!(report.woken.is_empty());
            assert_eq!(report.skipped.len(), 1);
            assert_eq!(
                report.skipped[0].reason,
                "another driver woke or ended it first"
            );
        }
        other => {
            assert_eq!(
                refused(other),
                "checkpoint for thread 't' changed before it could be resumed: another resume or end got there first"
            );
            assert_eq!(report.woken.len(), 1);
        }
    }
    let stamps = racing
        .inner
        .log("t")
        .iter()
        .filter(|c| c.resumed_at.is_some())
        .count();
    assert_eq!(stamps, 1);
}

#[tokio::test]
async fn a_thread_another_owner_holds_is_refused_and_a_woken_run_holds_the_lease() {
    let leases = LeaseStore::new();
    let store: Arc<dyn CheckpointStore> = leases.clone();
    let during = Arc::new(Mutex::new(None));
    let graph = {
        let leases = leases.clone();
        let during = during.clone();
        Arc::new(
            StateGraph::<Job>::new()
                .add_timer("Window", Duration::from_secs(5 * 60))
                .add_node("Check", move |mut job: Job| {
                    *during.lock().unwrap() = leases.held("t");
                    job.log.push("check".to_string());
                    job
                })
                .add_edge("Window", "Check")
                .compile()
                .unwrap(),
        )
    };
    freeze(&graph, "t", &store).await;
    let due = wake_at(&leases, "t");
    let leased = || RunConfig::new().with_lease(LeaseOptions::new("worker-1", lease_store::TTL));

    let holder = leases.acquire("t", "worker-2").unwrap();
    let message = refused(
        graph
            .resume_with("t", LevelSignals::due_at(due), store.clone(), leased())
            .await,
    );
    assert!(
        message.starts_with("thread 't' is held by 'worker-2' until "),
        "{message}"
    );
    assert_eq!(leases.log("t").len(), 1);

    assert!(leases.release(&holder));
    let result = graph
        .resume_with("t", LevelSignals::due_at(due), store.clone(), leased())
        .await;
    assert_eq!(completed(result).log, ["check"]);
    assert_eq!(
        during
            .lock()
            .unwrap()
            .as_ref()
            .map(|lease| lease.owner.clone()),
        Some("worker-1".to_string())
    );
    assert!(leases.held("t").is_none());
}

#[tokio::test]
async fn a_due_resume_waits_for_a_slot_at_its_priority() {
    let leases = LeaseStore::new();
    let store: Arc<dyn CheckpointStore> = leases.clone();
    let order = Arc::new(Mutex::new(Vec::new()));
    let graph = {
        let order = order.clone();
        Arc::new(
            StateGraph::<Job>::new()
                .add_node("Start", |job: Job| job)
                .add_timer("Window", Duration::from_secs(5 * 60))
                .add_async_node("Work", move |job: Job, ctx| {
                    let order = order.clone();
                    async move {
                        order.lock().unwrap().push(ctx.thread_id().to_string());
                        tokio::task::yield_now().await;
                        job
                    }
                })
                .add_edge("Start", "Window")
                .add_edge("Window", "Work")
                .compile()
                .unwrap(),
        )
    };
    freeze(&graph, "low", &store).await;
    freeze(&graph, "high", &store).await;
    let due = later(60);
    let scheduler = RunScheduler::new(SchedulerOptions::new(1)).unwrap();
    let holder = scheduler.admit(AdmissionRequest::new()).await.unwrap();

    let config = |priority| {
        RunConfig::new()
            .with_scheduler(scheduler.clone())
            .with_priority(priority)
    };
    let low = tokio::spawn({
        let (graph, store, config) = (graph.clone(), store.clone(), config(0));
        async move {
            graph
                .resume_with("low", LevelSignals::due_at(due), store, config)
                .await
        }
    });
    let high = tokio::spawn({
        let (graph, store, config) = (graph.clone(), store.clone(), config(5));
        async move {
            graph
                .resume_with("high", LevelSignals::due_at(due), store, config)
                .await
        }
    });
    tokio::time::timeout(PATIENCE, async {
        while scheduler.snapshot().queued < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("both resumes waiting for a slot");

    // Nothing was loaded or written while they waited.
    assert_eq!(leases.log("low").len(), 1);
    assert_eq!(leases.log("high").len(), 1);
    drop(holder);
    let (low, high) = tokio::time::timeout(PATIENCE, async { tokio::join!(low, high) })
        .await
        .expect("both resumes");
    completed(low.unwrap());
    completed(high.unwrap());

    assert_eq!(*order.lock().unwrap(), ["high", "low"]);
}

#[tokio::test]
async fn a_durable_graph_claims_a_due_thread_with_a_running_checkpoint() {
    let leases = LeaseStore::new();
    let store: Arc<dyn CheckpointStore> = leases.clone();
    let graph = Arc::new(
        timer_then_talk()
            .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
            .unwrap(),
    );
    freeze(&graph, "t", &store).await;
    let due = wake_at(&leases, "t");
    let frozen = leases.log("t").len();

    let (_, result) = stream(graph.clone().resume_streaming(
        "t",
        LevelSignals::due_at(due),
        store.clone(),
    ))
    .await;
    assert_eq!(completed(result).log, ["start", "talk", "finish"]);

    let log = leases.log("t");
    let claim = &log[frozen];
    assert_eq!(claim.status, CheckpointStatus::Running);
    assert_eq!(claim.next_node.as_deref(), Some("Talk"));
    assert_eq!(claim.wait, None);
    assert!(log.iter().all(|c| c.resumed_at.is_none()));
    assert_eq!(leases.latest("t").status, CheckpointStatus::Completed);
}

// --- Event signals -----------------------------------------------------------

#[tokio::test]
async fn an_event_signal_through_a_streaming_resume_streams_the_signalled_run() {
    let leases = LeaseStore::new();
    let store: Arc<dyn CheckpointStore> = leases.clone();
    let graph = deploy();
    freeze(&graph, "d", &store).await;
    let collector = Arc::new(Collector::default());

    let (_, wrong) = stream(graph.clone().resume_streaming(
        "d",
        LevelSignals::event(EventSignal::new("deployment.started")),
        store.clone(),
    ))
    .await;
    assert_eq!(
        refused(wrong),
        "thread 'd' is not waiting for event 'deployment.started': it is waiting for event 'deployment.finished' at [Deploying]"
    );
    let (chunks, result) = stream(graph.clone().resume_streaming_with(
        "d",
        LevelSignals::event(
            EventSignal::new("deployment.finished").with_override("log", json!(["deployed"])),
        ),
        store.clone(),
        observed(&collector),
    ))
    .await;

    assert_eq!(completed(result).log, ["deployed", "talk"]);
    assert_eq!(tokens_of(&chunks, "Talk"), ["deployed"]);
    let events = collector.after(1).await;
    assert!(events.iter().any(|e| matches!(
        &e.kind,
        RunEventKind::Resumed { wake: WakeReason::Event, event_type: Some(event), .. }
            if event == "deployment.finished"
    )));
    // A blank event type is refused before anything is loaded.
    assert_eq!(
        refused(
            graph
                .resume(
                    "d",
                    LevelSignals::event(EventSignal::new(" ")),
                    store.clone()
                )
                .await
        ),
        "a signal needs an event type"
    );
    assert_eq!(leases.log("d").len(), 2);
}
