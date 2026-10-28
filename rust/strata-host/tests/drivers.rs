//! The host's drivers (`design/35-Hosting-Integration.md`, "Drivers"), on the
//! in-memory store, a clock the test sets and tokio's paused time: waking,
//! recovering, purging, a failing round and its back-off, stopping, replicas,
//! what a driver needs, and the health probe. The xUnit twin is
//! `HostingTests`.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use datalium_strata::checkpoint::{
    CheckpointLogPage, CheckpointQuery, CheckpointStore, CheckpointStoreError,
    GraphExecutionResult, LatestCheckpoint, LeaseOptions, ThreadPage, ThreadQuery, ThreadState,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::schema::{Checkpoint, CheckpointStatus, SCHEMA_VERSION};
use datalium_strata::{BoxFuture, CancellationToken};
use datalium_strata_host::{
    Host, HostError, MAX_BACKOFF, PROBE_THREAD_ID, ROUNDS_METRIC, RecoveryDriver, RetentionDriver,
    StoreHealth, THREADS_METRIC, WakeDriver, check_store,
};
use datalium_strata_memory::Clock;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Notify;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Tick {
    ticks: u32,
    done: bool,
}

fn tick() -> Tick {
    Tick {
        ticks: 0,
        done: false,
    }
}

type Hook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

fn no_hook() -> Hook {
    Arc::new(|| Box::pin(async {}))
}

/// `Wait` (a five-minute timer) -> `After`, which runs `while_after` first.
fn timer_graph(while_after: Hook) -> Arc<CompiledGraph<Tick>> {
    Arc::new(
        StateGraph::<Tick>::new()
            .add_timer("Wait", Duration::from_secs(5 * 60))
            .add_async_node("After", move |mut tick: Tick, _ctx| {
                let while_after = while_after.clone();
                async move {
                    while_after().await;
                    tick.ticks += 1;
                    tick.done = true;
                    tick
                }
            })
            .add_edge("Wait", "After")
            .compile()
            .unwrap(),
    )
}

/// `First` -> `Second`, with a checkpoint per node.
fn durable_graph() -> Arc<CompiledGraph<Tick>> {
    Arc::new(
        StateGraph::<Tick>::new()
            .add_node("First", |mut tick: Tick| {
                tick.ticks += 1;
                tick
            })
            .add_node("Second", |mut tick: Tick| {
                tick.done = true;
                tick
            })
            .add_edge("First", "Second")
            .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
            .unwrap(),
    )
}

/// A clock the test sets: what the host calls "now".
#[derive(Clone)]
struct TestClock(Arc<Mutex<DateTime<Utc>>>);

impl TestClock {
    fn at(now: DateTime<Utc>) -> Self {
        Self(Arc::new(Mutex::new(now)))
    }

    fn set(&self, now: DateTime<Utc>) {
        *self.0.lock().unwrap() = now;
    }

    fn clock(&self) -> Clock {
        let now = self.0.clone();
        Arc::new(move || *now.lock().unwrap())
    }
}

/// Lets `seconds` of tokio's paused time pass, a second at a time, so every
/// timer in between fires in order.
async fn pass(seconds: u64) {
    for _ in 0..seconds {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Waits, in tokio's paused time, for `condition`.
async fn until(what: &str, mut condition: impl AsyncFnMut() -> bool) {
    for _ in 0..2000 {
        if condition().await {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("{what}");
}

async fn log(store: &impl CheckpointQuery, thread_id: &str) -> Vec<Checkpoint> {
    store
        .get_log(thread_id, None, 100)
        .await
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| entry.checkpoint)
        .collect()
}

fn finished(thread_id: &str, created_at: &str) -> Checkpoint {
    Checkpoint {
        status: CheckpointStatus::Completed,
        ..waiting(thread_id, created_at)
    }
}

fn waiting(thread_id: &str, created_at: &str) -> Checkpoint {
    Checkpoint {
        schema_version: SCHEMA_VERSION.to_string(),
        thread_id: thread_id.to_string(),
        status: CheckpointStatus::Interrupted,
        suspended_node: Some("Review".to_string()),
        next_node: Some("Send".to_string()),
        frozen_state: json!({ "ticks": 0, "done": false }),
        metadata: None,
        created_at: created_at.to_string(),
        resumed_at: None,
        nested: None,
        decision: None,
        wait: None,
        graph: None,
    }
}

/// A counter's name, its labels and a count it reported.
type Series = (String, Vec<(String, String)>, u64);

/// Counts the host's metrics on this thread while it lives: a
/// `#[tokio::test]` runs the host's tasks on the test's own thread.
struct Metrics {
    snapshotter: Snapshotter,
    seen: Cell<Vec<Series>>,
    _guard: metrics::LocalRecorderGuard<'static>,
}

impl Metrics {
    fn new() -> Self {
        let recorder: &'static DebuggingRecorder = Box::leak(Box::new(DebuggingRecorder::new()));
        Self {
            snapshotter: recorder.snapshotter(),
            seen: Cell::new(Vec::new()),
            _guard: metrics::set_default_local_recorder(recorder),
        }
    }

    /// The total of `metric` over the series with every one of `labels`.
    fn total(&self, metric: &str, labels: &[(&str, &str)]) -> u64 {
        let mut seen = self.seen.take();
        // A snapshot takes the counts with it.
        for (key, _, _, value) in self.snapshotter.snapshot().into_vec() {
            if let DebugValue::Counter(count) = value {
                let series = key
                    .key()
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect();
                seen.push((key.key().name().to_string(), series, count));
            }
        }
        let total = seen
            .iter()
            .filter(|(name, series, _)| {
                name == metric
                    && labels
                        .iter()
                        .all(|(key, value)| series.iter().any(|(k, v)| k == key && v == value))
            })
            .map(|(_, _, count)| count)
            .sum();
        self.seen.set(seen);
        total
    }
}

/// The in-memory store behind a switch: while `failing`, listing fails;
/// while `hanging`, a load never answers.
#[derive(Default)]
struct FlakyStore {
    inner: InMemoryCheckpointStore,
    failing: AtomicBool,
    hanging: AtomicBool,
    listings: AtomicUsize,
}

const UNREACHABLE: &str = "postgres://ops:hunter2@db.internal is unreachable";

impl CheckpointStore for FlakyStore {
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
            if self.hanging.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self.failing.load(Ordering::SeqCst) {
                return Err(CheckpointStoreError(UNREACHABLE.to_string()));
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

impl CheckpointQuery for FlakyStore {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        self.listings.fetch_add(1, Ordering::SeqCst);
        if self.failing.load(Ordering::SeqCst) {
            return Box::pin(std::future::ready(Err(CheckpointStoreError(
                UNREACHABLE.to_string(),
            ))));
        }
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

fn noon() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 6, 12, 0, 0).unwrap()
}

// --- The drivers ---

#[tokio::test(start_paused = true)]
async fn the_wake_driver_wakes_a_due_timer() {
    let store = Arc::new(InMemoryCheckpointStore::new());
    let graph = timer_graph(no_hook());
    assert!(matches!(
        graph.run("t", tick(), store.clone()).await,
        GraphExecutionResult::Interrupted { .. }
    ));
    let clock = TestClock::at(Utc::now());
    let metrics = Metrics::new();
    let host = Host::new()
        .with_clock(clock.clock())
        .wake(graph, store.clone(), WakeDriver::new());
    let status = host.status();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(host.run(shutdown.clone()));

    // Not due for five minutes: rounds come and go and nothing is written.
    pass(30).await;
    assert_eq!(log(&*store, "t").await.len(), 1);

    // Due: within an interval the driver has resumed it.
    clock.set(Utc::now() + chrono::Duration::minutes(6));
    until("the due timer was woken", async || {
        log(&*store, "t").await.len() == 2
    })
    .await;
    assert!(log(&*store, "t").await[1].resumed_at.is_some());

    let drivers = status.drivers();
    assert_eq!(drivers.len(), 1);
    assert_eq!(
        (drivers[0].name.as_str(), drivers[0].failures_in_a_row),
        ("wake:Tick", 0)
    );
    assert!(drivers[0].rounds >= 2);
    assert!(
        metrics.total(
            ROUNDS_METRIC,
            &[("strata.driver", "wake"), ("strata.outcome", "ok")]
        ) >= 2
    );
    assert_eq!(
        metrics.total(THREADS_METRIC, &[("strata.driver", "wake")]),
        1
    );
    assert_eq!(
        metrics.total(ROUNDS_METRIC, &[("strata.outcome", "error")]),
        0
    );

    shutdown.cancel();
    assert_eq!(running.await.unwrap(), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn the_recovery_driver_recovers_a_run_that_died() {
    let store = Arc::new(InMemoryCheckpointStore::new());
    let graph = durable_graph();
    let leased =
        || RunConfig::new().with_lease(LeaseOptions::new("worker-1", Duration::from_secs(30)));

    // A thread whose process died after First, as its last checkpoint left it.
    assert!(matches!(
        graph
            .run_with("seed", tick(), store.clone(), leased())
            .await,
        GraphExecutionResult::Completed(_)
    ));
    let mut dead = log(&*store, "seed").await.remove(1);
    dead.thread_id = "dead".to_string();
    store.save(dead).await.unwrap();

    let shutdown = CancellationToken::new();
    let running = tokio::spawn(
        Host::new()
            .recover(
                graph,
                store.clone(),
                RecoveryDriver::new().with_config(leased()),
            )
            .run(shutdown.clone()),
    );

    until("the dead run was recovered", async || {
        log(&*store, "dead").await.last().unwrap().status == CheckpointStatus::Completed
    })
    .await;
    let recovered = log(&*store, "dead").await;
    assert_eq!(
        recovered.iter().map(|c| c.status).collect::<Vec<_>>(),
        [
            CheckpointStatus::Running,
            CheckpointStatus::Running,
            CheckpointStatus::Completed
        ]
    );
    assert_eq!(
        recovered[1].metadata.as_ref().unwrap()["strata.recoveries"],
        "1"
    );
    shutdown.cancel();
    assert_eq!(running.await.unwrap(), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn the_retention_driver_purges_old_finished_threads_batch_after_batch() {
    let store = Arc::new(InMemoryCheckpointStore::new());
    for old in ["a", "b", "c", "d", "e"] {
        store
            .save(finished(old, "2026-01-01T00:00:00Z"))
            .await
            .unwrap();
    }
    store
        .save(finished("recent", "2026-10-01T00:00:00Z"))
        .await
        .unwrap();
    store
        .save(waiting("waiting", "2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    let host = Host::new()
        .with_clock(TestClock::at(noon()).clock())
        .retain(
            store.clone(),
            RetentionDriver::new()
                .with_older_than(Duration::from_secs(30 * 86_400))
                .with_batch_size(2),
        );
    let status = host.status();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(host.run(shutdown.clone()));

    // One round, three purges of two: everything old and finished is gone
    // before the driver waits.
    until("the first round finished", async || {
        status
            .drivers()
            .iter()
            .any(|d| d.name == "retention" && d.rounds == 1)
    })
    .await;
    let mut left: Vec<String> = store
        .list_threads(&ThreadQuery::default())
        .await
        .unwrap()
        .threads
        .into_iter()
        .map(|thread| thread.thread_id)
        .collect();
    left.sort();
    assert_eq!(left, ["recent", "waiting"]);
    shutdown.cancel();
    assert_eq!(running.await.unwrap(), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn a_round_that_fails_is_counted_the_driver_backs_off_and_goes_on_when_the_store_is_back() {
    let store = Arc::new(FlakyStore::default());
    store.failing.store(true, Ordering::SeqCst);
    let metrics = Metrics::new();
    let host = Host::new().wake(timer_graph(no_hook()), store.clone(), WakeDriver::new());
    let status = host.status();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(host.run(shutdown.clone()));

    // The first round fails, and the host is still up.
    tokio::time::sleep(Duration::from_millis(1)).await;
    let failing = status.failing();
    assert_eq!(failing.len(), 1);
    assert_eq!(
        (failing[0].name.as_str(), failing[0].failures_in_a_row),
        ("wake:Tick", 1)
    );
    // The failure's kind, never its message: that one holds a password.
    let kind = failing[0].last_failure.clone().unwrap();
    assert!(!kind.is_empty() && !kind.contains("hunter2"), "{kind}");
    assert!(!running.is_finished());

    // A minute of the clock. Rounds come at 0, 10 and 30 seconds, where a
    // driver that didn't back off would make thirteen.
    pass(60).await;
    assert_eq!(store.listings.load(Ordering::SeqCst), 3);
    assert_eq!(status.failing()[0].failures_in_a_row, 3);
    assert_eq!(
        metrics.total(
            ROUNDS_METRIC,
            &[("strata.driver", "wake"), ("strata.outcome", "error")]
        ),
        3
    );

    // The store comes back: the next round works, and the driver is healthy.
    store.failing.store(false, Ordering::SeqCst);
    until("the driver recovered", async || status.failing().is_empty()).await;
    assert_eq!(status.drivers()[0].last_failure, None);
    shutdown.cancel();
    assert_eq!(running.await.unwrap(), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn stopping_ends_a_waiting_driver_at_once() {
    let store = Arc::new(InMemoryCheckpointStore::new());
    let host = Host::new().retain(
        store,
        RetentionDriver::new().with_older_than(Duration::from_secs(30 * 86_400)),
    );
    let status = host.status();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(host.run(shutdown.clone()));
    until("the first round finished", async || {
        status.drivers().iter().any(|d| d.rounds == 1)
    })
    .await;

    // An hour's wait ahead of it.
    let asked = tokio::time::Instant::now();
    shutdown.cancel();
    assert_eq!(running.await.unwrap(), Ok(()));

    assert!(asked.elapsed() < Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn stopping_lets_the_run_in_flight_finish_and_starts_no_other() {
    let store = Arc::new(InMemoryCheckpointStore::new());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let graph = timer_graph({
        let (entered, release, started, finished) = (
            entered.clone(),
            release.clone(),
            started.clone(),
            finished.clone(),
        );
        Arc::new(move || {
            let (entered, release, started, finished) = (
                entered.clone(),
                release.clone(),
                started.clone(),
                finished.clone(),
            );
            Box::pin(async move {
                started.fetch_add(1, Ordering::SeqCst);
                entered.notify_one();
                release.notified().await;
                finished.fetch_add(1, Ordering::SeqCst);
            })
        })
    });
    graph.run("a", tick(), store.clone()).await;
    graph.run("b", tick(), store.clone()).await;
    let clock = TestClock::at(Utc::now() + chrono::Duration::minutes(6));
    let shutdown = CancellationToken::new();
    let mut running = tokio::spawn(
        Host::new()
            .with_clock(clock.clock())
            .wake(graph, store.clone(), WakeDriver::new())
            .run(shutdown.clone()),
    );
    entered.notified().await;

    // The host is asked to stop, and waits for the run.
    shutdown.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(30), &mut running)
            .await
            .is_err(),
        "the host waits for the run in flight"
    );

    release.notify_one();
    let stopped = tokio::time::timeout(Duration::from_secs(600), running)
        .await
        .expect("the host stops once the run in flight has finished");
    assert_eq!(stopped.unwrap(), Ok(()));

    // That run finished, not cancelled, and the other due thread was left for
    // the next host.
    assert_eq!(
        (
            started.load(Ordering::SeqCst),
            finished.load(Ordering::SeqCst)
        ),
        (1, 1)
    );
    let logs = [log(&*store, "a").await, log(&*store, "b").await];
    assert!(
        logs.iter()
            .flatten()
            .all(|checkpoint| checkpoint.status != CheckpointStatus::Cancelled)
    );
    let mut lengths: Vec<usize> = logs.iter().map(Vec::len).collect();
    lengths.sort();
    assert_eq!(lengths, [1, 2]);
}

#[tokio::test(start_paused = true)]
async fn two_hosts_on_one_store_wake_a_thread_once() {
    let store = Arc::new(InMemoryCheckpointStore::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let graph = timer_graph({
        let runs = runs.clone();
        Arc::new(move || {
            runs.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {})
        })
    });
    graph.run("t", tick(), store.clone()).await;
    let clock = TestClock::at(Utc::now() + chrono::Duration::minutes(6));
    let shutdown = CancellationToken::new();
    let replica = || {
        tokio::spawn(
            Host::new()
                .with_clock(clock.clock())
                .wake(graph.clone(), store.clone(), WakeDriver::new())
                .run(shutdown.clone()),
        )
    };
    let (first, second) = (replica(), replica());

    until("the timer was woken", async || {
        runs.load(Ordering::SeqCst) > 0
    })
    .await;
    pass(60).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(log(&*store, "t").await.len(), 2);
    shutdown.cancel();
    assert_eq!(first.await.unwrap(), Ok(()));
    assert_eq!(second.await.unwrap(), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn a_driver_without_what_it_needs_stops_the_host_from_starting() {
    let store = Arc::new(InMemoryCheckpointStore::new());
    let refusal = async |host: Host| -> String {
        // Refused before anything starts: no token has to fire.
        let HostError(refusal) = host.run(CancellationToken::new()).await.unwrap_err();
        refusal
    };
    let month = Duration::from_secs(30 * 86_400);

    assert_eq!(
        refusal(Host::new().wake(
            timer_graph(no_hook()),
            store.clone(),
            WakeDriver::new().with_interval(Duration::ZERO)
        ))
        .await,
        "the wake driver's interval must be more than zero"
    );
    assert_eq!(
        refusal(Host::new().wake(
            timer_graph(no_hook()),
            store.clone(),
            WakeDriver::new().with_batch_size(0)
        ))
        .await,
        "the wake driver's batch_size must be between 1 and 1000"
    );

    // Only a graph that checkpoints every node leaves a running thread, and
    // without leases only its age says its run died.
    assert!(
        refusal(Host::new().recover(
            timer_graph(no_hook()),
            store.clone(),
            RecoveryDriver::new().with_stale_after(month)
        ))
        .await
        .starts_with(
            "the recovery driver for Tick needs a graph compiled with CheckpointPolicy::EveryNode"
        )
    );
    assert!(
        refusal(Host::new().recover(durable_graph(), store.clone(), RecoveryDriver::new()))
            .await
            .starts_with("the recovery driver needs a lease in its run config or a stale_after")
    );

    assert!(
        refusal(Host::new().retain(store.clone(), RetentionDriver::new()))
            .await
            .starts_with("the retention driver needs an older_than")
    );
    let mut unknown = RetentionDriver::new().with_older_than(month);
    unknown.states = vec!["finished".to_string()];
    assert_eq!(
        refusal(Host::new().retain(store.clone(), unknown)).await,
        "the retention driver doesn't know the thread state 'finished'"
    );

    // Named states are taken by name.
    let named = RetentionDriver::new()
        .with_older_than(month)
        .with_states([ThreadState::Failed]);
    assert_eq!(named.states, ["failed"]);
}

#[tokio::test(start_paused = true)]
async fn a_task_of_the_applications_own_runs_until_the_host_stops_and_is_started_again_after_an_error()
 {
    let starts = Arc::new(AtomicUsize::new(0));
    let host = Host::new().task("decisions", {
        let starts = starts.clone();
        move |stop: CancellationToken| {
            let attempt = starts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    return Err("the feed said no".to_string());
                }
                stop.cancelled().await;
                Ok(())
            }
        }
    });
    let status = host.status();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(host.run(shutdown.clone()));

    // It failed once, and two seconds on (a second, doubled) it runs again.
    pass(1).await;
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        status.failing()[0].last_failure.as_deref(),
        Some("TaskFailed")
    );
    pass(2).await;
    assert_eq!(starts.load(Ordering::SeqCst), 2);

    shutdown.cancel();
    assert_eq!(running.await.unwrap(), Ok(()));
    let decisions = &status.drivers()[0];
    assert_eq!(
        (
            decisions.name.as_str(),
            decisions.rounds,
            decisions.failures_in_a_row
        ),
        ("decisions", 2, 0)
    );
    assert_eq!(MAX_BACKOFF, Duration::from_secs(300));
}

// --- Health ---

#[tokio::test(start_paused = true)]
async fn the_probe_says_whether_the_store_answers() {
    let store = FlakyStore::default();
    let timeout = Duration::from_millis(200);

    assert!(check_store(&store, timeout).await.is_healthy());
    assert!(
        check_store(&InMemoryCheckpointStore::new(), timeout)
            .await
            .is_healthy()
    );
    // The probe's thread is nobody's.
    assert_eq!(store.inner.load(PROBE_THREAD_ID).await.unwrap(), None);

    store.failing.store(true, Ordering::SeqCst);
    assert_eq!(
        check_store(&store, timeout).await,
        StoreHealth::Unhealthy {
            error: UNREACHABLE.to_string()
        }
    );

    // So is a store that doesn't answer.
    store.failing.store(false, Ordering::SeqCst);
    store.hanging.store(true, Ordering::SeqCst);
    assert_eq!(
        check_store(&store, timeout).await,
        StoreHealth::Unhealthy {
            error: "no answer within 200ms".to_string()
        }
    );
}

// --- The run scheduler (design/40-Run-Scheduler.md) ---

#[tokio::test(start_paused = true)]
async fn the_wake_drivers_runs_wait_for_the_scheduler_at_the_drivers_priority() {
    use datalium_strata::scheduler::{AdmissionRequest, RunScheduler, SchedulerOptions};

    let store = Arc::new(InMemoryCheckpointStore::new());
    let graph = timer_graph(no_hook());
    graph.run("t", tick(), store.clone()).await;
    // Two slots, one of them only for priority 5 and up, and a run elsewhere holds the other.
    let scheduler = RunScheduler::new(
        SchedulerOptions::new(2)
            .with_max_queued(0)
            .with_reservation(5, 1),
    )
    .unwrap();
    let _elsewhere = scheduler.admit(AdmissionRequest::new()).await.unwrap();
    let clock = TestClock::at(Utc::now() + chrono::Duration::minutes(6));
    let config = RunConfig::new().with_scheduler(scheduler.clone());

    // At the config's priority (0) there is no slot: every round skips the thread.
    let shutdown = CancellationToken::new();
    let low = Host::new().with_clock(clock.clock()).wake(
        graph.clone(),
        store.clone(),
        WakeDriver::new().with_config(config.clone()),
    );
    let running = tokio::spawn(low.run(shutdown.clone()));
    pass(30).await;
    assert_eq!(log(&*store, "t").await.len(), 1);
    shutdown.cancel();
    assert_eq!(running.await.unwrap(), Ok(()));

    // At priority 5 the reserved slot is its.
    let shutdown = CancellationToken::new();
    let high = Host::new().with_clock(clock.clock()).wake(
        graph,
        store.clone(),
        WakeDriver::new().with_config(config).with_priority(5),
    );
    let running = tokio::spawn(high.run(shutdown.clone()));
    until("the due timer was woken", async || {
        log(&*store, "t").await.len() == 2
    })
    .await;
    shutdown.cancel();
    assert_eq!(running.await.unwrap(), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn the_host_closes_its_scheduler_when_it_stops() {
    use datalium_strata::scheduler::{
        AdmissionRequest, NotAdmittedReason, RunScheduler, SchedulerOptions,
    };

    let scheduler = RunScheduler::new(SchedulerOptions::new(1)).unwrap();
    let holder = scheduler.admit(AdmissionRequest::new()).await.unwrap();
    let waiting = tokio::spawn(scheduler.admit(AdmissionRequest::new()));
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(
        Host::new()
            .close_on_shutdown(scheduler.clone())
            .run(shutdown.clone()),
    );
    pass(5).await;
    assert!(!scheduler.is_closed());

    shutdown.cancel();
    assert_eq!(running.await.unwrap(), Ok(()));
    assert!(scheduler.is_closed());
    assert_eq!(
        waiting.await.unwrap().unwrap_err().reason,
        NotAdmittedReason::Closed
    );
    // The run holding a slot is not touched.
    assert_eq!(scheduler.snapshot().active, 1);
    drop(holder);
}
