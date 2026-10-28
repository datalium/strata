//! The engine under a run scheduler (`design/40-Run-Scheduler.md`, "What a
//! run does with its slot"): every run mode and both drivers wait for a slot
//! before they touch the store, a run that isn't admitted leaves nothing
//! behind, and every way a run ends gives its slot back. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/Scheduling/SchedulerEngineTests.cs` —
//! keep the two in step. The span and the metrics are in `telemetry.rs`.

mod lease_store;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::checkpoint::{
    CancelReason, CheckpointLogPage, CheckpointQuery, CheckpointStore, CheckpointStoreError,
    EventSignal, GraphExecutionResult, HumanSignal, LatestCheckpoint, ThreadEnd, ThreadPage,
    ThreadQuery,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{
    CheckpointPolicy, CompileOptions, CompiledGraph, NodeError, StateGraph,
};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind, RunObserver};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::recovery::RecoverOptions;
use datalium_strata::scheduler::{
    AdmissionRequest, NotAdmittedReason, RunScheduler, RunSlot, SchedulerOptions,
};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::wait::WakeOptions;
use datalium_strata::{BoxFuture, CancellationToken, StrataError};
use lease_store::LeaseStore;
use serde::{Deserialize, Serialize};

const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
struct Work {
    trail: Vec<String>,
    items: Vec<String>,
}

fn work(items: &[&str]) -> Work {
    Work {
        trail: Vec::new(),
        items: items.iter().map(|i| i.to_string()).collect(),
    }
}

fn step(label: &'static str) -> impl Fn(Work) -> Work + Send + Sync + 'static {
    move |mut w: Work| {
        w.trail.push(label.to_string());
        w
    }
}

/// `Prepare` → `Approve` (a boundary) → `Finish`.
fn approval(options: CompileOptions) -> Arc<CompiledGraph<Work>> {
    Arc::new(
        StateGraph::new()
            .add_node("Prepare", step("prepare"))
            .add_node("Approve", step("approve"))
            .add_node("Finish", step("finish"))
            .add_edge("Prepare", "Approve")
            .add_edge("Approve", "Finish")
            .add_interrupt_boundary("Approve")
            .compile_with(options)
            .unwrap(),
    )
}

fn scheduler(max_concurrent: u32, max_queued: u32) -> RunScheduler {
    RunScheduler::new(SchedulerOptions::new(max_concurrent).with_max_queued(max_queued)).unwrap()
}

/// Takes a slot the way a run elsewhere in the process would.
async fn hold(scheduler: &RunScheduler, tenant: Option<&str>) -> RunSlot {
    let mut request = AdmissionRequest::new();
    if let Some(tenant) = tenant {
        request = request.with_tenant(tenant);
    }
    scheduler.admit(request).await.unwrap()
}

fn refused(result: GraphExecutionResult<Work>) -> (NotAdmittedReason, Duration) {
    match result {
        GraphExecutionResult::Failed(StrataError::NotAdmitted { reason, waited }) => {
            (reason, waited)
        }
        _ => panic!("expected NotAdmitted"),
    }
}

/// A store that counts every call made to it.
struct Watched {
    inner: Arc<LeaseStore>,
    calls: AtomicUsize,
}

impl Watched {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: LeaseStore::new(),
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn count(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

impl CheckpointStore for Watched {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.count();
        self.inner.save(checkpoint)
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        self.count();
        self.inner.load(thread_id)
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        self.count();
        self.inner.load_latest(thread_id)
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        self.count();
        self.inner.save_if_latest(checkpoint, expected_sequence)
    }
}

impl CheckpointQuery for Watched {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        self.count();
        self.inner.list_threads(query)
    }

    fn get_log<'a>(
        &'a self,
        thread_id: &'a str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        self.count();
        self.inner.get_log(thread_id, after_sequence, limit)
    }
}

/// Every event of every run.
#[derive(Default)]
struct Events(Mutex<Vec<RunEvent>>);

impl RunObserver for Events {
    fn on_event(&self, event: &RunEvent) {
        self.0.lock().unwrap().push(event.clone());
    }
}

async fn queued(scheduler: &RunScheduler, waiting: u32) {
    tokio::time::timeout(PATIENCE, async {
        while scheduler.snapshot().queued < waiting {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("runs waiting for a slot");
}

#[tokio::test]
async fn a_run_and_a_resume_wait_for_a_slot_before_they_touch_the_store() {
    let scheduler = scheduler(1, 1024);
    let store = Watched::new();
    let graph = approval(CompileOptions::new());
    let config = RunConfig::new().with_scheduler(scheduler.clone());

    let holder = hold(&scheduler, None).await;
    let run = {
        let (graph, store, config) = (graph.clone(), store.clone(), config.clone());
        tokio::spawn(async move { graph.run_with("t", work(&[]), store, config).await })
    };
    queued(&scheduler, 1).await;
    assert_eq!(store.calls(), 0);
    assert!(!run.is_finished());

    drop(holder);
    let result = tokio::time::timeout(PATIENCE, run).await.unwrap().unwrap();
    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    let after_run = store.calls();
    assert!(after_run > 0);

    let holder = hold(&scheduler, None).await;
    let resume = {
        let (graph, store, config) = (graph.clone(), store.clone(), config.clone());
        tokio::spawn(async move {
            graph
                .resume_with("t", HumanSignal::approve("alice"), store, config)
                .await
        })
    };
    queued(&scheduler, 1).await;
    assert_eq!(store.calls(), after_run);

    drop(holder);
    match tokio::time::timeout(PATIENCE, resume)
        .await
        .unwrap()
        .unwrap()
    {
        GraphExecutionResult::Completed(done) => {
            assert_eq!(done.trail, ["prepare", "approve", "finish"]);
        }
        _ => panic!("expected Completed"),
    }
    assert_eq!(scheduler.snapshot().active, 0);
}

#[tokio::test]
async fn every_run_mode_not_admitted_returns_not_admitted_and_leaves_nothing_behind() {
    let scheduler = scheduler(1, 0);
    let store = Watched::new();
    let graph = approval(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode));
    graph.run("waiting", work(&[]), store.inner.clone()).await;
    let events = Arc::new(Events::default());
    let config = RunConfig::new()
        .with_scheduler(scheduler.clone())
        .with_observer(events.clone());
    let _holder = hold(&scheduler, None).await;
    let as_store: Arc<dyn CheckpointStore> = store.clone();

    let mut results = vec![
        graph
            .run_with("fresh", work(&[]), as_store.clone(), config.clone())
            .await,
        graph
            .resume_with(
                "waiting",
                HumanSignal::approve("alice"),
                as_store.clone(),
                config.clone(),
            )
            .await,
        graph
            .signal_with(
                "waiting",
                EventSignal::new("payment"),
                as_store.clone(),
                config.clone(),
            )
            .await,
        graph
            .recover_with("waiting", as_store.clone(), config.clone())
            .await,
    ];
    for (handle, mut chunks) in [
        graph
            .clone()
            .run_streaming_with("fresh", work(&[]), as_store.clone(), config.clone()),
        graph.clone().resume_streaming_with(
            "waiting",
            HumanSignal::approve("alice"),
            as_store.clone(),
            config.clone(),
        ),
        graph
            .clone()
            .recover_streaming_with("waiting", as_store.clone(), config.clone()),
    ] {
        results.push(handle.await.unwrap());
        assert!(chunks.recv().await.is_none(), "a stream with no chunks");
    }

    for result in results {
        assert_eq!(refused(result).0, NotAdmittedReason::QueueFull);
    }
    assert_eq!(store.calls(), 0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(events.0.lock().unwrap().is_empty());
    assert!(store.inner.log("fresh").is_empty());
    assert_eq!(
        store.inner.latest("waiting").status,
        CheckpointStatus::Interrupted
    );
}

#[tokio::test]
async fn every_way_a_run_ends_gives_its_slot_back() {
    let scheduler = scheduler(1, 0);
    let config = RunConfig::new().with_scheduler(scheduler.clone());
    let store: Arc<dyn CheckpointStore> = LeaseStore::new();
    let first = |w: &Work| w.items.first().cloned().unwrap_or_default();
    let graph = Arc::new(
        StateGraph::new()
            .add_fallible_node("Work", move |w: &Work| match first(w).as_str() {
                "fail" => Err(NodeError::Fatal("no".to_string())),
                "panic" => panic!("bug"),
                "wait" => Ok(w.clone()),
                _ => Ok(step("work")(w.clone())),
            })
            .add_node("Wait", step("wait"))
            .add_node("Done", step("done"))
            .add_conditional_edge("Work", "Wait", move |w: &Work| first(w) == "wait")
            .add_edge("Work", "Done")
            .add_edge("Wait", "Done")
            .add_interrupt_boundary("Wait")
            .compile()
            .unwrap(),
    );

    let run = |id: &'static str, items: &'static [&'static str], config: RunConfig| {
        let (graph, store) = (graph.clone(), store.clone());
        async move { graph.run_with(id, work(items), store, config).await }
    };
    assert!(matches!(
        run("a", &[], config.clone()).await,
        GraphExecutionResult::Completed(_)
    ));
    assert!(matches!(
        run("b", &["wait"], config.clone()).await,
        GraphExecutionResult::Interrupted { .. }
    ));
    assert!(matches!(
        run("c", &["fail"], config.clone()).await,
        GraphExecutionResult::Failed(StrataError::NodeFailed { .. })
    ));
    // A handler's panic comes out of the run, and the slot is given back on the way.
    let panicked = tokio::spawn(run("d", &["panic"], config.clone())).await;
    assert!(panicked.is_err_and(|err| err.is_panic()));
    // Admitted on arrival with a free slot, so it starts, and stops at its first check.
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        run("e", &[], config.clone().with_cancellation(cancelled)).await,
        GraphExecutionResult::Cancelled { .. }
    ));

    assert_eq!(scheduler.snapshot().active, 0);
    assert!(matches!(
        run("f", &[], config.clone()).await,
        GraphExecutionResult::Completed(_)
    ));
}

#[tokio::test]
async fn a_subgraph_a_group_and_a_map_run_in_their_runs_one_slot() {
    let scheduler = scheduler(1, 0);
    let child = Arc::new(
        StateGraph::new()
            .add_node("Inner", step("inner"))
            .compile()
            .unwrap(),
    );
    let graph = StateGraph::new()
        .add_parallel_group(
            "Group",
            ParallelBranches::new()
                .branch("b1", |w: &Work| Ok(step("b1")(w.clone())))
                .branch("b2", |w: &Work| Ok(step("b2")(w.clone()))),
            |w: &Work, done: Vec<Work>| {
                let mut w = w.clone();
                w.trail.extend(done.into_iter().flat_map(|d| d.trail));
                w
            },
        )
        .add_subgraph("Sub", child.clone(), |w: &Work| w.clone(), |_, done| done)
        .add_map(
            "Map",
            |w: &Work| w.items.clone(),
            |item: &String| item.clone(),
            child,
            |_, item| work(&[item.as_str()]),
            |w: &Work, done: Vec<Work>| {
                let mut w = w.clone();
                w.trail.push(format!("mapped {}", done.len()));
                w
            },
        )
        .add_edge("Group", "Sub")
        .add_edge("Sub", "Map")
        .compile()
        .unwrap();

    let result = graph
        .run_with(
            "t",
            work(&["x", "y", "z"]),
            LeaseStore::new(),
            RunConfig::new().with_scheduler(scheduler),
        )
        .await;

    match result {
        GraphExecutionResult::Completed(done) => {
            assert_eq!(done.trail, ["b1", "b2", "inner", "mapped 3"]);
        }
        _ => panic!("expected Completed"),
    }
}

#[tokio::test]
async fn ending_a_thread_takes_no_slot() {
    let scheduler = scheduler(1, 0);
    let store: Arc<dyn CheckpointStore> = LeaseStore::new();
    let graph = approval(CompileOptions::new());
    graph.run("t", work(&[]), store.clone()).await;
    let _holder = hold(&scheduler, None).await;

    let ended = graph
        .end_with(
            "t",
            ThreadEnd::by("alice"),
            store,
            RunConfig::new().with_scheduler(scheduler),
        )
        .await;

    assert!(matches!(
        ended,
        GraphExecutionResult::Cancelled {
            reason: CancelReason::Ended,
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn a_token_or_a_deadline_ends_a_wait_with_nothing_written() {
    let scheduler = scheduler(1, 1024);
    let store = Watched::new();
    let graph = approval(CompileOptions::new());
    let _holder = hold(&scheduler, None).await;

    let token = CancellationToken::new();
    let cancelled = {
        let (graph, store) = (graph.clone(), store.clone());
        let config = RunConfig::new()
            .with_scheduler(scheduler.clone())
            .with_cancellation(token.clone());
        tokio::spawn(async move { graph.run_with("a", work(&[]), store, config).await })
    };
    queued(&scheduler, 1).await;
    token.cancel();
    assert_eq!(
        refused(cancelled.await.unwrap()).0,
        NotAdmittedReason::Cancelled
    );

    let deadline = (tokio::time::Instant::now() + Duration::from_secs(5)).into_std();
    let late = {
        let (graph, store) = (graph.clone(), store.clone());
        let config = RunConfig::new()
            .with_scheduler(scheduler.clone())
            .with_deadline(deadline);
        tokio::spawn(async move { graph.run_with("b", work(&[]), store, config).await })
    };
    queued(&scheduler, 1).await;
    tokio::time::advance(Duration::from_secs(5)).await;
    let (reason, waited) = refused(late.await.unwrap());
    assert_eq!(reason, NotAdmittedReason::Deadline);
    assert!(waited >= Duration::from_secs(5) && waited < Duration::from_secs(6));

    assert_eq!(store.calls(), 0);
}

#[tokio::test]
async fn the_tenant_is_the_runs_metadata_and_a_full_tenant_holds_back_only_its_own() {
    let scheduler = RunScheduler::new(SchedulerOptions::new(4).with_tenant_limit(1)).unwrap();
    let store: Arc<dyn CheckpointStore> = LeaseStore::new();
    let graph = approval(CompileOptions::new());
    let holder = hold(&scheduler, Some("acme")).await;
    let spawn = |thread: &'static str, tenant: &'static str| {
        let (graph, store) = (graph.clone(), store.clone());
        let config = RunConfig::new()
            .with_scheduler(scheduler.clone())
            .with_metadata("tenant", tenant);
        tokio::spawn(async move { graph.run_with(thread, work(&[]), store, config).await })
    };

    let acme = spawn("acme-1", "acme");
    let globex = spawn("globex-1", "globex");
    let result = tokio::time::timeout(PATIENCE, globex)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    assert!(!acme.is_finished());

    drop(holder);
    let result = tokio::time::timeout(PATIENCE, acme).await.unwrap().unwrap();
    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
}

#[tokio::test]
async fn a_higher_priority_run_starts_first() {
    let scheduler = scheduler(1, 1024);
    let order = Arc::new(Mutex::new(Vec::new()));
    let graph = {
        let order = order.clone();
        Arc::new(
            StateGraph::new()
                .add_node("Work", move |w: Work| {
                    order.lock().unwrap().push(w.items[0].clone());
                    w
                })
                .compile()
                .unwrap(),
        )
    };
    let store: Arc<dyn CheckpointStore> = LeaseStore::new();
    let holder = hold(&scheduler, None).await;
    let spawn = |thread: &'static str, priority: i32| {
        let (graph, store) = (graph.clone(), store.clone());
        let config = RunConfig::new()
            .with_scheduler(scheduler.clone())
            .with_priority(priority);
        tokio::spawn(async move { graph.run_with(thread, work(&[thread]), store, config).await })
    };

    let low = spawn("low", 0);
    queued(&scheduler, 1).await;
    let high = spawn("high", 5);
    queued(&scheduler, 2).await;
    drop(holder);
    tokio::time::timeout(PATIENCE, low).await.unwrap().unwrap();
    tokio::time::timeout(PATIENCE, high).await.unwrap().unwrap();

    assert_eq!(*order.lock().unwrap(), ["high", "low"]);
}

#[tokio::test(start_paused = true)]
async fn the_run_says_how_long_it_queued_on_its_first_event() {
    let scheduler = scheduler(1, 1024);
    let graph = approval(CompileOptions::new());
    let store: Arc<dyn CheckpointStore> = LeaseStore::new();
    let holder = hold(&scheduler, None).await;
    let (events, mut received) = RunEventChannel::new();

    let run = {
        let (graph, store) = (graph.clone(), store.clone());
        let config = RunConfig::new()
            .with_scheduler(scheduler.clone())
            .with_priority(2)
            .with_observer(events);
        tokio::spawn(async move { graph.run_with("queued", work(&[]), store, config).await })
    };
    queued(&scheduler, 1).await;
    tokio::time::advance(Duration::from_secs(3)).await;
    drop(holder);
    run.await.unwrap();

    match received.recv().await.unwrap().kind {
        RunEventKind::RunStarted { queued_for, .. } => {
            let waited = queued_for.expect("a run with a scheduler says how long it queued");
            assert!(waited >= Duration::from_secs(3) && waited < Duration::from_secs(4));
        }
        other => panic!("expected RunStarted, got {other:?}"),
    }

    // Without a scheduler there is no queue to have waited in.
    let (events, mut received) = RunEventChannel::new();
    graph
        .run_with(
            "plain",
            work(&[]),
            store,
            RunConfig::new().with_observer(events),
        )
        .await;
    match received.recv().await.unwrap().kind {
        RunEventKind::RunStarted { queued_for, .. } => assert_eq!(queued_for, None),
        other => panic!("expected RunStarted, got {other:?}"),
    }
}

/// A timer graph with one thread `t` due: `Start` → `Later` (a minute) → `Finish`.
async fn due_timer(store: &Arc<LeaseStore>) -> Arc<CompiledGraph<Work>> {
    let graph = Arc::new(
        StateGraph::new()
            .add_node("Start", step("start"))
            .add_timer("Later", Duration::from_secs(60))
            .add_node("Finish", step("finish"))
            .add_edge("Start", "Later")
            .add_edge("Later", "Finish")
            .compile()
            .unwrap(),
    );
    graph
        .run_with(
            "t",
            work(&[]),
            store.clone(),
            RunConfig::new().with_metadata("tenant", "acme"),
        )
        .await;
    graph
}

#[tokio::test]
async fn the_wake_driver_skips_a_thread_not_admitted_and_wakes_it_next_round() {
    let scheduler = scheduler(1, 0);
    let store = LeaseStore::new();
    let graph = due_timer(&store).await;
    let later = chrono::Utc::now() + chrono::Duration::minutes(2);
    let options =
        || WakeOptions::new().with_config(RunConfig::new().with_scheduler(scheduler.clone()));

    let holder = hold(&scheduler, None).await;
    let report = graph
        .wake_due_with(store.clone(), later, options())
        .await
        .unwrap();
    drop(holder);

    assert!(report.woken.is_empty());
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(
        report.skipped[0].reason,
        "it was not admitted: run not admitted: the scheduler's queue is full"
    );
    assert_eq!(store.latest("t").status, CheckpointStatus::Interrupted);

    let report = graph
        .wake_due_with(store.clone(), later, options())
        .await
        .unwrap();
    assert_eq!(report.woken.len(), 1);
    assert!(matches!(
        report.woken[0].result,
        GraphExecutionResult::Completed(_)
    ));
}

#[tokio::test]
async fn a_drivers_stop_token_ends_its_wait_for_a_slot() {
    let scheduler = scheduler(1, 1024);
    let store = LeaseStore::new();
    let graph = due_timer(&store).await;
    let later = chrono::Utc::now() + chrono::Duration::minutes(2);
    let stop = CancellationToken::new();
    let _holder = hold(&scheduler, None).await;

    let round = {
        let (graph, store) = (graph.clone(), store.clone());
        let options = WakeOptions::new()
            .with_config(RunConfig::new().with_scheduler(scheduler.clone()))
            .with_stop(stop.clone());
        tokio::spawn(async move { graph.wake_due_with(store, later, options).await })
    };
    queued(&scheduler, 1).await;
    stop.cancel();
    let report = tokio::time::timeout(PATIENCE, round)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert!(report.woken.is_empty());
    assert_eq!(report.skipped.len(), 1);
    assert!(
        report.skipped[0]
            .reason
            .contains("cancelled while waiting for a slot")
    );
    assert_eq!(scheduler.snapshot().queued, 0);
    assert_eq!(store.latest("t").status, CheckpointStatus::Interrupted);
}

#[tokio::test]
async fn the_recovery_driver_skips_a_thread_not_admitted() {
    let scheduler = scheduler(1, 0);
    let store = Watched::new();
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/checkpoint-lifecycle/running.json");
    let running: Checkpoint =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    store.inner.save(running).await.unwrap();
    let graph = approval(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode));
    let _holder = hold(&scheduler, None).await;

    let report = graph
        .recover_stale(
            store.clone(),
            chrono::Utc::now() + chrono::Duration::days(3650),
            RecoverOptions::new()
                .with_stale_after(Duration::from_secs(1))
                .with_match_graph(false)
                .with_config(RunConfig::new().with_scheduler(scheduler.clone())),
        )
        .await
        .unwrap();

    assert!(report.recovered.is_empty());
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(
        report.skipped[0].reason,
        "it was not admitted: run not admitted: the scheduler's queue is full"
    );
    // Listed, and nothing more: the thread was never loaded.
    assert_eq!(store.calls(), 1);
}

/// The scheduler's metrics (`design/40-Run-Scheduler.md`, "Telemetry"): every
/// decision counted by outcome, every wait recorded, and the gauges back to
/// zero once nothing waits or runs. The recorder is this binary's own: no
/// other test here takes a snapshot, which drains histograms.
#[tokio::test]
async fn admissions_and_waits_are_counted_by_outcome() {
    use datalium_strata::observability::metric_names;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    static SNAPSHOTTER: std::sync::LazyLock<metrics_util::debugging::Snapshotter> =
        std::sync::LazyLock::new(|| {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            recorder.install().unwrap();
            snapshotter
        });
    std::sync::LazyLock::force(&SNAPSHOTTER);

    let scheduler = RunScheduler::new(
        SchedulerOptions::new(1)
            .with_max_queued(0)
            .with_name("metrics-scheduler"),
    )
    .unwrap();
    let graph = approval(CompileOptions::new());
    let store: Arc<dyn CheckpointStore> = LeaseStore::new();
    let config = || RunConfig::new().with_scheduler(scheduler.clone());
    graph
        .run_with("a", work(&[]), store.clone(), config())
        .await;
    {
        let _busy = hold(&scheduler, None).await;
        let result = graph
            .run_with("b", work(&[]), store.clone(), config())
            .await;
        assert_eq!(refused(result).0, NotAdmittedReason::QueueFull);
    }

    let ours: Vec<_> = SNAPSHOTTER
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| {
            key.key()
                .labels()
                .any(|l| l.key() == "strata.scheduler.name" && l.value() == "metrics-scheduler")
        })
        .collect();
    let label = |key: &metrics_util::CompositeKey, name: &str| {
        key.key()
            .labels()
            .find(|l| l.key() == name)
            .map(|l| l.value().to_string())
    };
    let counted = |outcome: &str| -> u64 {
        ours.iter()
            .filter(|(key, ..)| {
                key.key().name() == metric_names::SCHEDULER_ADMISSIONS
                    && label(key, "strata.scheduler.outcome").as_deref() == Some(outcome)
            })
            .map(|(.., value)| match value {
                DebugValue::Counter(n) => *n,
                _ => 0,
            })
            .sum()
    };
    // The run, and the test's own slot; then the run refused.
    assert_eq!(counted("admitted"), 2);
    assert_eq!(counted("queue_full"), 1);
    let waits: usize = ours
        .iter()
        .filter(|(key, ..)| key.key().name() == metric_names::SCHEDULER_WAIT)
        .map(|(.., value)| match value {
            DebugValue::Histogram(values) => values.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(waits, 3);
    for gauge in [
        metric_names::SCHEDULER_ACTIVE,
        metric_names::SCHEDULER_QUEUED,
    ] {
        for (key, .., value) in ours.iter().filter(|(key, ..)| key.key().name() == gauge) {
            assert!(
                matches!(value, DebugValue::Gauge(v) if v.into_inner() == 0.0),
                "{key:?} back to zero"
            );
        }
    }
}
