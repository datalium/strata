//! Mirrors the .NET `DecisionWorkerTests` (`design/30-Strata-Cloud-Store.md`,
//! "The worker"): the decision worker against the emulated Cloud. Keep the two
//! in step.

mod common;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{
    EmulatedCloud, RecordingSleep, Reply, Rollout, Server, envelope, initial, rollout_graph,
    rollout_graph_with,
};
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{Backoff, CompileOptions, CompiledGraph, Jitter};
use datalium_strata::lease::LeaseOptions;
use datalium_strata::schema::{Checkpoint, CheckpointStatus, RUNNING_CHECKPOINT_SCHEMA_VERSION};
use datalium_strata::store::{CheckpointStore, CheckpointStoreError, LatestCheckpoint};
use datalium_strata::{BoxFuture, CancellationToken, StrataError};
use datalium_strata_cloud::{
    CloudError, DecisionOutcome, DecisionWorker, Decisions, StrataCloudCheckpointStore,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde_json::json;

struct Setup {
    cloud: EmulatedCloud,
    sleep: RecordingSleep,
    store: Arc<dyn CheckpointStore>,
    graph: Arc<CompiledGraph<Rollout>>,
    executed: Arc<std::sync::atomic::AtomicU32>,
}

impl Setup {
    async fn new(conditional: bool) -> Self {
        let cloud = EmulatedCloud::start().await;
        cloud.set_conditional(conditional);
        let sleep = RecordingSleep::default();
        let store: Arc<dyn CheckpointStore> =
            Arc::new(StrataCloudCheckpointStore::new(cloud.server.options(&sleep)).unwrap());
        let (graph, executed) = rollout_graph();
        Self {
            cloud,
            sleep,
            store,
            graph,
            executed,
        }
    }

    fn worker(&self, store: Option<Arc<dyn CheckpointStore>>) -> DecisionWorker<Rollout> {
        DecisionWorker::new(
            self.graph.clone(),
            store.unwrap_or_else(|| self.store.clone()),
            Decisions::new(self.cloud.server.options(&self.sleep)).unwrap(),
        )
    }

    async fn interrupt(&self, thread_id: &str) {
        let result = self
            .graph
            .run(thread_id, initial(), self.store.clone())
            .await;
        assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    }

    fn executed(&self) -> u32 {
        self.executed.load(Ordering::SeqCst)
    }
}

type Before = Box<dyn FnOnce(Arc<dyn CheckpointStore>) -> BoxFuture<'static, ()> + Send>;

/// A store that does something else to the thread just before its first load.
struct FirstLoad {
    inner: Arc<dyn CheckpointStore>,
    before: Mutex<Option<Before>>,
}

impl FirstLoad {
    fn wrap(inner: Arc<dyn CheckpointStore>, before: Before) -> Arc<dyn CheckpointStore> {
        Arc::new(Self {
            inner,
            before: Mutex::new(Some(before)),
        })
    }
}

impl CheckpointStore for FirstLoad {
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
            let first = self.before.lock().unwrap().take();
            if let Some(first) = first {
                first(self.inner.clone()).await;
            }
            self.inner.load_latest(thread_id).await
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

#[tokio::test]
async fn a_decision_whose_thread_is_held_is_left_for_the_next_round() {
    let setup = Setup::new(true).await;
    setup.interrupt("inc-held").await;
    setup.cloud.decide("inc-held", true, "alice", None);
    let store = setup.store.clone();
    let reported = Arc::new(Mutex::new(0));
    let r = reported.clone();
    let mut worker = setup
        .worker(None)
        .with_run_config(|_| {
            RunConfig::new().with_lease(LeaseOptions::new("worker-1", Duration::from_secs(60)))
        })
        .with_on_handled(move |_| *r.lock().unwrap() += 1);

    // Another owner holds the thread on Cloud (design/32-Thread-Leases.md,
    // design/43-Cloud-Leases-And-Listing.md): the decision is theirs to act on
    // for now.
    let holder = store
        .try_acquire_lease("inc-held", "worker-2", Duration::from_secs(60))
        .await
        .unwrap()
        .acquired()
        .unwrap();
    assert!(worker.poll(Duration::ZERO).await.unwrap().is_empty());
    assert_eq!((0, 0), (*reported.lock().unwrap(), setup.executed()));
    assert!(setup.cloud.latest("inc-held")["resumed_at"].is_null());

    // It wasn't remembered as handled, so the next round resumes it, under
    // the worker's own lease.
    assert!(store.release_lease(&holder).await.unwrap());
    let handled = worker.poll(Duration::ZERO).await.unwrap();
    assert_eq!(1, handled.len());
    assert_eq!(DecisionOutcome::Resumed, handled[0].outcome);
    assert_eq!((1, 1), (*reported.lock().unwrap(), setup.executed()));
    assert_eq!(None, store.get_lease("inc-held").await.unwrap());
}

#[tokio::test]
async fn a_decision_whose_resume_is_not_admitted_is_left_for_the_next_round() {
    use datalium_strata::scheduler::{AdmissionRequest, RunScheduler, SchedulerOptions};

    let setup = Setup::new(true).await;
    setup.interrupt("inc-busy").await;
    setup.cloud.decide("inc-busy", true, "alice", None);
    let scheduler = RunScheduler::new(SchedulerOptions::new(1).with_max_queued(0)).unwrap();
    let reported = Arc::new(Mutex::new(0));
    let r = reported.clone();
    let s = scheduler.clone();
    let mut worker = setup
        .worker(None)
        .with_run_config(move |_| RunConfig::new().with_scheduler(s.clone()))
        .with_on_handled(move |_| *r.lock().unwrap() += 1);

    // The process is busy (design/40-Run-Scheduler.md): nothing was tried,
    // so nothing is remembered.
    {
        let _busy = scheduler.admit(AdmissionRequest::new()).await.unwrap();
        assert!(worker.poll(Duration::ZERO).await.unwrap().is_empty());
        assert_eq!((0, 0), (*reported.lock().unwrap(), setup.executed()));
        assert!(setup.cloud.latest("inc-busy")["resumed_at"].is_null());
    }

    let handled = worker.poll(Duration::ZERO).await.unwrap();
    assert_eq!(1, handled.len());
    assert_eq!(DecisionOutcome::Resumed, handled[0].outcome);
    assert_eq!((1, 1), (*reported.lock().unwrap(), setup.executed()));
}

#[tokio::test]
async fn resumes_a_decided_thread_and_reports_it() {
    let setup = Setup::new(true).await;
    setup.interrupt("inc-1").await;
    let approval = setup
        .cloud
        .decide("inc-1", true, "alice", Some(json!({ "replicas": 3 })));
    let reported = Arc::new(Mutex::new(Vec::new()));
    let configs = Arc::new(Mutex::new(Vec::new()));
    let (r, c) = (reported.clone(), configs.clone());
    let mut worker = setup
        .worker(None)
        .with_run_config(move |decision| {
            c.lock().unwrap().push(decision.approval_id.clone());
            RunConfig::new().with_metadata("approval", &decision.approval_id)
        })
        .with_on_handled(move |handled| {
            r.lock()
                .unwrap()
                .push((handled.decision.approval_id.clone(), handled.outcome));
        });

    let handled = worker.poll(Duration::ZERO).await.unwrap();

    assert_eq!(1, handled.len());
    assert_eq!(DecisionOutcome::Resumed, handled[0].outcome);
    assert_eq!(approval, handled[0].decision.approval_id);
    assert_eq!(
        Some("user-alice"),
        handled[0].decision.decided_by.as_deref()
    );
    assert_eq!(
        Some("alice@example.com"),
        handled[0].decision.decided_by_email.as_deref()
    );
    assert_eq!(None, handled[0].decision.notes);
    match &handled[0].result {
        Some(GraphExecutionResult::Completed(state)) => {
            assert_eq!(vec!["prepared", "scaled to 3"], state.log)
        }
        _ => panic!("expected Completed"),
    }
    assert_eq!(1, setup.executed());
    assert_eq!(
        vec![(approval.clone(), DecisionOutcome::Resumed)],
        *reported.lock().unwrap()
    );
    assert_eq!(vec![approval], *configs.lock().unwrap());

    // The resume's stamp acknowledged it: the feed no longer offers it.
    assert!(worker.poll(Duration::ZERO).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_thread_resumed_by_someone_else_first_is_already_handled() {
    let setup = Setup::new(true).await;
    setup.interrupt("inc-2").await;
    setup.cloud.decide("inc-2", true, "alice", None);
    let store = FirstLoad::wrap(
        setup.store.clone(),
        Box::new(|inner| {
            Box::pin(async move {
                let mut latest = inner
                    .load_latest("inc-2")
                    .await
                    .unwrap()
                    .unwrap()
                    .checkpoint;
                latest.resumed_at = Some("2026-10-09T12:00:00Z".to_string());
                inner.save(latest).await.unwrap();
            })
        }),
    );

    let handled = setup
        .worker(Some(store))
        .poll(Duration::ZERO)
        .await
        .unwrap();

    assert_eq!(1, handled.len());
    assert_eq!(DecisionOutcome::AlreadyHandled, handled[0].outcome);
    assert_eq!(0, setup.executed());
}

#[tokio::test]
async fn a_decision_for_a_thread_that_is_running_is_already_handled() {
    // design/33-Durable-Execution.md: a durable graph's resume makes its
    // thread running, so a worker that was handed the decision just before
    // finds a thread somebody is already running.
    let setup = Setup::new(true).await;
    setup.interrupt("inc-running").await;
    setup.cloud.decide("inc-running", true, "alice", None);
    let store = FirstLoad::wrap(
        setup.store.clone(),
        Box::new(|inner| {
            Box::pin(async move {
                let mut running = inner
                    .load_latest("inc-running")
                    .await
                    .unwrap()
                    .unwrap()
                    .checkpoint;
                running.schema_version = RUNNING_CHECKPOINT_SCHEMA_VERSION.to_string();
                running.status = CheckpointStatus::Running;
                inner.save(running).await.unwrap();
            })
        }),
    );

    let handled = setup
        .worker(Some(store))
        .poll(Duration::ZERO)
        .await
        .unwrap();

    assert_eq!(1, handled.len());
    assert_eq!(DecisionOutcome::AlreadyHandled, handled[0].outcome);
    assert_eq!(0, setup.executed());
}

#[tokio::test]
async fn a_stale_decision_runs_nothing_once_the_thread_has_moved_on() {
    let setup = Setup::new(true).await;
    setup.interrupt("inc-3").await;
    setup.cloud.decide("inc-3", false, "alice", None);

    // Before the worker's resume, the thread moves on to a later checkpoint of
    // the same kind: the reviewer's decision answered the earlier one.
    let store = FirstLoad::wrap(
        setup.store.clone(),
        Box::new(|inner| {
            Box::pin(async move {
                let latest = inner
                    .load_latest("inc-3")
                    .await
                    .unwrap()
                    .unwrap()
                    .checkpoint;
                let mut stamp = latest.clone();
                stamp.resumed_at = Some("2026-10-09T12:00:00Z".to_string());
                inner.save(stamp).await.unwrap();
                let mut later = latest;
                later.created_at = "2026-10-09T12:00:01Z".to_string();
                inner.save(later).await.unwrap();
            })
        }),
    );

    let handled = setup
        .worker(Some(store))
        .poll(Duration::ZERO)
        .await
        .unwrap();

    assert_eq!(1, handled.len());
    assert_eq!(DecisionOutcome::AlreadyHandled, handled[0].outcome);
    assert_eq!(0, setup.executed());
    assert!(setup.cloud.latest("inc-3")["resumed_at"].is_null());
}

#[tokio::test]
async fn pages_past_a_foreign_thread_and_a_refused_decision_handling_each_once() {
    let setup = Setup::new(true).await;
    setup.interrupt("other-1").await;
    setup.interrupt("inc-4").await;
    setup.cloud.decide("other-1", true, "alice", None);
    setup
        .cloud
        .decide("inc-4", true, "alice", Some(json!({ "replicas": "lots" })));
    let mut worker = setup
        .worker(None)
        .with_thread_id_prefix("inc-")
        .with_limit(1);

    let first = worker.poll(Duration::ZERO).await.unwrap();

    let outcomes: Vec<_> = first.iter().map(|h| h.outcome).collect();
    assert_eq!(
        vec![DecisionOutcome::NotMine, DecisionOutcome::Failed],
        outcomes
    );
    assert!(first[0].result.is_none());
    assert!(matches!(
        first[1].result,
        Some(GraphExecutionResult::Failed(StrataError::ExecutionAborted(
            _
        )))
    ));
    assert_eq!(0, setup.executed());

    // Cloud still offers both: neither is handled again, and a new decision
    // behind them is.
    setup.interrupt("inc-5").await;
    setup.cloud.decide("inc-5", true, "bob", None);
    let second = worker.poll(Duration::ZERO).await.unwrap();
    assert_eq!(1, second.len());
    assert_eq!("inc-5", second[0].decision.thread_id);
    assert_eq!(DecisionOutcome::Resumed, second[0].outcome);
    assert!(worker.poll(Duration::ZERO).await.unwrap().is_empty());
}

#[tokio::test]
async fn owns_decides_which_threads_are_resumed() {
    let setup = Setup::new(true).await;
    setup.interrupt("inc-6").await;
    setup.cloud.decide("inc-6", true, "alice", None);
    let mut worker = setup.worker(None).with_owns_async(|_| async { false });

    let handled = worker.poll(Duration::ZERO).await.unwrap();

    assert_eq!(DecisionOutcome::NotMine, handled[0].outcome);
    assert_eq!(0, setup.executed());
}

#[tokio::test]
async fn a_thread_another_graph_wrote_is_not_mine_and_runs_nothing() {
    let setup = Setup::new(true).await;
    let version = |v: &str| {
        rollout_graph_with(
            CompileOptions::new()
                .with_graph_id("rollout")
                .with_graph_version(v),
        )
    };
    let (v1, executed_by_v1) = version("1");
    let (v2, executed_by_v2) = version("2");
    assert!(matches!(
        v1.run("inc-8", initial(), setup.store.clone()).await,
        GraphExecutionResult::Interrupted { .. }
    ));
    setup.cloud.decide("inc-8", true, "alice", None);
    let worker_for = |graph: Arc<CompiledGraph<Rollout>>| {
        DecisionWorker::new(
            graph,
            setup.store.clone(),
            Decisions::new(setup.cloud.server.options(&setup.sleep)).unwrap(),
        )
    };

    // design/31-Graph-Identity.md: v2's worker leaves v1's thread alone, with
    // no owns lookup.
    let handled = worker_for(v2).poll(Duration::ZERO).await.unwrap();
    assert_eq!(handled.len(), 1);
    assert_eq!(DecisionOutcome::NotMine, handled[0].outcome);
    assert!(handled[0].result.is_none());
    assert_eq!(0, executed_by_v2.load(Ordering::SeqCst));
    assert!(setup.cloud.latest("inc-8")["resumed_at"].is_null());

    // v1's worker resumes it.
    let handled = worker_for(v1).poll(Duration::ZERO).await.unwrap();
    assert_eq!(handled.len(), 1);
    assert_eq!(DecisionOutcome::Resumed, handled[0].outcome);
    assert_eq!(1, executed_by_v1.load(Ordering::SeqCst));
}

async fn two_workers_one_decision(conditional: bool) -> (u32, Vec<DecisionOutcome>) {
    let setup = Setup::new(conditional).await;
    setup.interrupt("inc-7").await;
    setup.cloud.decide("inc-7", true, "alice", None);

    // Both workers' resumes load the decided checkpoint before either stamps it.
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    setup.cloud.set_before_load(Some(Arc::new(move || {
        let barrier = barrier.clone();
        Box::pin(async move {
            tokio::time::timeout(Duration::from_secs(10), barrier.wait())
                .await
                .expect("both workers load");
        })
    })));

    let (mut first, mut second) = (setup.worker(None), setup.worker(None));
    let (a, b) = tokio::join!(first.poll(Duration::ZERO), second.poll(Duration::ZERO));
    setup.cloud.set_before_load(None);

    let mut outcomes: Vec<_> = a
        .unwrap()
        .into_iter()
        .chain(b.unwrap())
        .map(|handled| handled.outcome)
        .collect();
    outcomes.sort();
    (setup.executed(), outcomes)
}

#[tokio::test]
async fn two_workers_one_decision_run_once_with_the_conditional_save() {
    let (runs, outcomes) = two_workers_one_decision(true).await;
    assert_eq!(1, runs);
    assert_eq!(
        vec![DecisionOutcome::Resumed, DecisionOutcome::AlreadyHandled],
        outcomes
    );
}

#[tokio::test]
async fn two_workers_one_decision_run_twice_without_the_conditional_save() {
    // The documented race (design/30, "The conditional save"): without Cloud's
    // headers, both stamps land.
    let (runs, outcomes) = two_workers_one_decision(false).await;
    assert_eq!(2, runs);
    assert_eq!(
        vec![DecisionOutcome::Resumed, DecisionOutcome::Resumed],
        outcomes
    );
}

#[tokio::test]
async fn run_waits_out_an_unavailable_feed_honours_retry_after_and_stops_on_401() {
    let feed = Server::scripted(vec![
        Reply::json(503, envelope(503, "STRATA_CLOUD_API_DATABASE_ERROR")),
        Reply::json(429, envelope(429, "STRATA_CLOUD_API_RATE_LIMITED"))
            .with_header("Retry-After", "5"),
        Reply::json(401, envelope(401, "STRATA_CLOUD_API_TOKEN_REVOKED")),
    ])
    .await;
    let sleep = RecordingSleep::default();
    let (graph, _) = rollout_graph();
    let mut worker = DecisionWorker::new(
        graph,
        Arc::new(InMemoryCheckpointStore::new()),
        Decisions::new(feed.options(&sleep)).unwrap(),
    )
    .with_error_backoff(Backoff {
        initial: Duration::from_secs(1),
        max: Duration::from_secs(30),
        jitter: Jitter::None,
    });

    let stopped = worker.run(CancellationToken::new()).await.unwrap_err();

    assert!(matches!(stopped, CloudError::Unauthorized { .. }));
    assert_eq!(
        vec![Duration::from_secs(1), Duration::from_secs(5)],
        sleep.waits()
    );
    for request in feed.requests() {
        assert_eq!("25", request.query["wait"]);
    }
}

#[tokio::test]
async fn run_waits_the_idle_delay_after_a_round_with_nothing_new_and_stops_when_cancelled() {
    let setup = Setup::new(true).await;
    setup.interrupt("other-2").await;
    setup.cloud.decide("other-2", true, "alice", None);
    let mut worker = setup.worker(None).with_thread_id_prefix("inc-");
    let stop = CancellationToken::new();

    let sleep = setup.sleep.clone();
    let stopper = stop.clone();
    let watcher = tokio::spawn(async move {
        while sleep
            .waits()
            .iter()
            .filter(|w| **w == Duration::from_secs(2))
            .count()
            < 3
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        stopper.cancel();
    });
    tokio::time::timeout(Duration::from_secs(10), worker.run(stop))
        .await
        .expect("the worker stops")
        .unwrap();
    watcher.await.unwrap();

    // Every round after the first found only the foreign decision, and waited
    // 2 s before the next.
    assert!(
        setup
            .sleep
            .waits()
            .iter()
            .all(|w| *w == Duration::from_secs(2))
    );
    assert_eq!(0, setup.executed());
}
