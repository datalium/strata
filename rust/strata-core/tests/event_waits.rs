//! Event waits and durable timers (`design/29-Event-Waits-And-Timers.md`,
//! SG-16), through the public API only. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/EventWaitTests.cs` — keep the two in step.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use datalium_strata::checkpoint::{
    CancelReason, CheckpointLogPage, CheckpointQuery, CheckpointStore, CheckpointStoreError,
    END_REASON_KEY, END_TIMED_OUT, EventSignal, GraphExecutionResult, HumanSignal,
    LatestCheckpoint, ThreadCursor, ThreadPage, ThreadQuery, ThreadSummary, WakeReason,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CompiledGraph, StateGraph};
use datalium_strata::map::MapOptions;
use datalium_strata::observability::{RunEvent, RunEventKind, RunObserver};
use datalium_strata::schema::{
    Checkpoint, CheckpointStatus, CheckpointWait, GraphDefinition, WAIT_CHECKPOINT_SCHEMA_VERSION,
    WaitKind,
};
use datalium_strata::store::updated_at_text;
use datalium_strata::validate::validate;
use datalium_strata::wait::{BoundaryOptions, WaitOptions, WakeOptions, WakeReport};
use datalium_strata::{BoxFuture, CancellationToken};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const PATIENCE: Duration = Duration::from_secs(10);

/// Keeps every thread's whole log, offers the conditional save, and lists
/// threads in code, as the `strata-memory` in-memory store does.
#[derive(Default)]
struct LogStore {
    logs: Mutex<HashMap<String, Vec<Checkpoint>>>,
}

impl LogStore {
    fn log(&self, thread_id: &str) -> Vec<Checkpoint> {
        self.logs
            .lock()
            .unwrap()
            .get(thread_id)
            .cloned()
            .unwrap_or_default()
    }

    fn latest(&self, thread_id: &str) -> Checkpoint {
        self.log(thread_id).last().cloned().expect("a checkpoint")
    }
}

impl CheckpointStore for LogStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.logs
            .lock()
            .unwrap()
            .entry(checkpoint.thread_id.clone())
            .or_default()
            .push(checkpoint);
        Box::pin(std::future::ready(Ok(())))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(self.log(thread_id).last().cloned())))
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        let log = self.log(thread_id);
        let latest = log.last().map(|checkpoint| LatestCheckpoint {
            checkpoint: checkpoint.clone(),
            sequence: Some(log.len() as i64 - 1),
        });
        Box::pin(std::future::ready(Ok(latest)))
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        let mut logs = self.logs.lock().unwrap();
        let log = logs.entry(checkpoint.thread_id.clone()).or_default();
        let saved = expected_sequence.is_none_or(|expected| log.len() as i64 - 1 == expected);
        if saved {
            log.push(checkpoint);
        }
        Box::pin(std::future::ready(Ok(saved)))
    }
}

impl CheckpointQuery for LogStore {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        let page = (|| {
            let cursor = query.validate()?;
            let logs = self.logs.lock().unwrap();
            let mut latest: Vec<(i64, Checkpoint)> = logs
                .values()
                .filter_map(|log| Some((log.len() as i64 - 1, log.last()?.clone())))
                .filter(|(_, checkpoint)| query.matches(checkpoint))
                .filter(|(_, checkpoint)| {
                    cursor.as_ref().is_none_or(|cursor| {
                        cursor.admits(updated_at_text(checkpoint), &checkpoint.thread_id)
                    })
                })
                .collect();
            latest.sort_by(|(_, a), (_, b)| {
                (updated_at_text(b), &b.thread_id).cmp(&(updated_at_text(a), &a.thread_id))
            });
            let more = latest.len() > query.limit;
            latest.truncate(query.limit);
            let next_cursor = more
                .then(|| latest.last())
                .flatten()
                .map(|(_, c)| ThreadCursor::new(updated_at_text(c), &c.thread_id).encode());
            Ok(ThreadPage {
                threads: latest
                    .iter()
                    .map(|(sequence, checkpoint)| ThreadSummary::of(*sequence, checkpoint))
                    .collect::<Result<_, _>>()?,
                next_cursor,
            })
        })();
        Box::pin(std::future::ready(page))
    }

    fn get_log<'a>(
        &'a self,
        _thread_id: &'a str,
        _after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(CheckpointLogPage::new(
            Vec::new(),
            limit,
        ))))
    }
}

/// A [`LogStore`] whose `load_latest` holds each caller until a second one has
/// loaded too, once armed: two wake drivers that both decided a thread is due,
/// racing to wake it.
struct RacingStore {
    inner: LogStore,
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

/// Every event of every run, in arrival order.
#[derive(Default)]
struct Collector(Mutex<Vec<RunEvent>>);

impl RunObserver for Collector {
    fn on_event(&self, event: &RunEvent) {
        self.0.lock().unwrap().push(event.clone());
    }
}

impl Collector {
    /// The events once `runs` runs have completed (delivery is off the
    /// executor, so it may trail the call).
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

// --- Graphs ------------------------------------------------------------------

/// `waiting-event.json`'s graph: `Start` -> `Deploying` (waits for
/// `deployment.finished`, at most an hour, then `Rollback`) -> `Verify`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Deploy {
    release: String,
    started: bool,
    finished: bool,
    outcome: Option<String>,
    rolled_back: bool,
    verified: bool,
}

fn deploy() -> Deploy {
    Deploy {
        release: "v2.4.1".to_string(),
        started: false,
        finished: false,
        outcome: None,
        rolled_back: false,
        verified: false,
    }
}

fn deploy_graph(route: bool) -> CompiledGraph<Deploy> {
    let mut options = WaitOptions::new().with_timeout(Duration::from_secs(3600));
    if route {
        options = options.with_on_timeout("Rollback");
    }
    let mut graph = StateGraph::<Deploy>::new()
        .add_node("Start", |mut s: Deploy| {
            s.started = true;
            s
        })
        .add_wait_for_event_with("Deploying", "deployment.finished", options)
        .add_node("Verify", |mut s: Deploy| {
            s.verified = true;
            s
        })
        .add_edge("Start", "Deploying")
        .add_edge("Deploying", "Verify");
    if route {
        graph = graph.add_node("Rollback", |mut s: Deploy| {
            s.rolled_back = true;
            s
        });
    }
    graph.compile().unwrap()
}

/// `waiting-approval.json`'s graph: `Approve` (a boundary that rejects to
/// `Abort` and times out after 30 minutes to `Escalate`) -> `Execute`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Approval {
    plan: String,
    presented: bool,
    executed: bool,
    escalated: bool,
    aborted: bool,
}

fn approval() -> Approval {
    Approval {
        plan: "restart api".to_string(),
        presented: false,
        executed: false,
        escalated: false,
        aborted: false,
    }
}

fn approval_graph(route: bool) -> CompiledGraph<Approval> {
    let mut options = BoundaryOptions::new()
        .with_reject("Abort")
        .with_timeout(Duration::from_secs(30 * 60));
    if route {
        options = options.with_on_timeout("Escalate");
    }
    let mut graph = StateGraph::<Approval>::new()
        .add_node("Approve", |mut s: Approval| {
            s.presented = true;
            s
        })
        .add_interrupt_boundary_with("Approve", options)
        .add_node("Execute", |mut s: Approval| {
            s.executed = true;
            s
        })
        .add_node("Abort", |mut s: Approval| {
            s.aborted = true;
            s
        })
        .add_edge("Approve", "Execute");
    if route {
        graph = graph.add_node("Escalate", |mut s: Approval| {
            s.escalated = true;
            s
        });
    }
    graph.compile().unwrap()
}

/// The verification window: `Check` -> `Window` (a five-minute timer) ->
/// `Check`, until two checks find it healthy, then `Done`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Health {
    checks: u32,
    healthy: bool,
}

fn window_graph() -> CompiledGraph<Health> {
    StateGraph::<Health>::new()
        .add_node("Check", |mut s: Health| {
            s.checks += 1;
            s.healthy = s.checks >= 2;
            s
        })
        .add_timer("Window", Duration::from_secs(5 * 60))
        .add_node("Done", |s: Health| s)
        .add_conditional_edge("Check", "Window", |s: &Health| !s.healthy)
        .add_edge("Check", "Done")
        .add_edge("Window", "Check")
        .compile()
        .unwrap()
}

/// `nested-timer.json`'s graph: `Remediate` -> `Verify` (the window, as a
/// subgraph) -> `Close`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Remediation {
    step: String,
    verified: bool,
    checks: u32,
}

fn remediation() -> Remediation {
    Remediation {
        step: "start".to_string(),
        verified: false,
        checks: 0,
    }
}

fn remediation_graph<T>(
    child: CompiledGraph<T>,
    input: fn(&Remediation) -> T,
    output: fn(&Remediation, T) -> Remediation,
) -> CompiledGraph<Remediation>
where
    T: Clone + Send + Sync + Serialize + serde::de::DeserializeOwned + 'static,
{
    StateGraph::<Remediation>::new()
        .add_node("Remediate", |mut s: Remediation| {
            s.step = "remediated".to_string();
            s
        })
        .add_subgraph("Verify", child, input, output)
        .add_node("Close", |mut s: Remediation| {
            s.step = "closed".to_string();
            s
        })
        .add_edge("Remediate", "Verify")
        .add_edge("Verify", "Close")
        .compile()
        .unwrap()
}

fn verify_graph() -> CompiledGraph<Remediation> {
    remediation_graph(
        window_graph(),
        |_| Health {
            checks: 0,
            healthy: false,
        },
        |entry, health| Remediation {
            verified: health.healthy,
            checks: health.checks,
            ..entry.clone()
        },
    )
}

// --- Helpers -----------------------------------------------------------------

fn fixture(relative: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures")
        .join(relative);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn lifecycle_fixture(name: &str) -> Checkpoint {
    serde_json::from_value(fixture(&format!("checkpoint-lifecycle/{name}"))).unwrap()
}

/// Blanks every level's `created_at` and `wake_at`: the fixture's are fixed, a
/// live freeze's come from the clock.
fn without_times(mut checkpoint: Checkpoint) -> Checkpoint {
    checkpoint.created_at = String::new();
    if let Some(wait) = &mut checkpoint.wait {
        wait.wake_at = wait.wake_at.as_ref().map(|_| String::new());
    }
    checkpoint.nested = checkpoint
        .nested
        .map(|child| Box::new(without_times(*child)));
    checkpoint
}

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .unwrap()
        .with_timezone(&Utc)
}

/// `checkpoint`'s innermost wake time, as a time.
fn wake_at(checkpoint: &Checkpoint) -> DateTime<Utc> {
    at(checkpoint
        .innermost_wait()
        .unwrap()
        .wake_at
        .as_deref()
        .unwrap())
}

fn in_minutes(minutes: i64) -> DateTime<Utc> {
    Utc::now() + chrono::Duration::minutes(minutes)
}

fn failed_message<S>(result: GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Failed(error) => format!("{error:?}"),
        _ => panic!("expected Failed"),
    }
}

fn completed<S: std::fmt::Debug>(result: &GraphExecutionResult<S>) -> &S {
    match result {
        GraphExecutionResult::Completed(state) => state,
        _ => panic!("expected Completed"),
    }
}

fn woken_ids<S>(report: &WakeReport<S>) -> Vec<(&str, WakeReason)> {
    report
        .woken
        .iter()
        .map(|woken| (woken.thread_id.as_str(), woken.reason))
        .collect()
}

async fn freeze<S>(
    graph: &CompiledGraph<S>,
    thread_id: &str,
    state: S,
    store: &Arc<LogStore>,
) -> Option<CheckpointWait>
where
    S: Clone + Send + Sync + Serialize + serde::de::DeserializeOwned + 'static,
{
    match graph.run(thread_id, state, store.clone()).await {
        GraphExecutionResult::Interrupted { wait, .. } => wait,
        _ => panic!("expected the run to freeze"),
    }
}

// --- Timers --------------------------------------------------------------------

#[tokio::test]
async fn a_timer_freezes_with_its_wake_time_and_wakes_only_when_due() {
    let store = Arc::new(LogStore::default());
    let graph = window_graph();
    let started = Utc::now();
    let wait = freeze(
        &graph,
        "t",
        Health {
            checks: 0,
            healthy: false,
        },
        &store,
    )
    .await;

    let wait = wait.expect("a timer's wait");
    assert_eq!(wait.kind, WaitKind::Timer);
    assert_eq!(wait.event_type, None);
    let latest = store.latest("t");
    assert_eq!(latest.wait.as_ref(), Some(&wait));
    assert_eq!(latest.schema_version, WAIT_CHECKPOINT_SCHEMA_VERSION);
    assert_eq!(latest.suspended_node.as_deref(), Some("Window"));
    assert_eq!(latest.next_node.as_deref(), Some("Check"));
    // Five minutes on, rounded up to a whole second: never early.
    let due = wake_at(&latest);
    assert!(due >= started + chrono::Duration::minutes(5));
    assert!(due <= Utc::now() + chrono::Duration::minutes(5) + chrono::Duration::seconds(1));

    // Not due yet: nothing is woken or written.
    let report = graph.wake_due(store.clone(), Utc::now()).await.unwrap();
    assert!(report.woken.is_empty() && report.skipped.is_empty());
    assert_eq!(store.log("t").len(), 1);

    // Due: the timer continues at the edge out of it.
    let report = graph.wake_due(store.clone(), due).await.unwrap();
    assert_eq!(woken_ids(&report), [("t", WakeReason::Timer)]);
    assert_eq!(
        completed(&report.woken[0].result),
        &Health {
            checks: 2,
            healthy: true
        }
    );
    // The resume stamp is the only new checkpoint, and it records no decision.
    let log = store.log("t");
    assert_eq!(log.len(), 2);
    assert!(log[1].resumed_at.is_some());
    assert_eq!(log[1].decision, None);

    // Woken, it isn't due any more.
    let report = graph.wake_due(store.clone(), in_minutes(60)).await.unwrap();
    assert!(report.woken.is_empty());
}

#[tokio::test]
async fn a_stop_token_wakes_no_more_threads_and_lets_the_one_in_flight_finish() {
    let store = Arc::new(LogStore::default());
    let stop = CancellationToken::new();

    // The host is asked to stop while a woken thread's run is in flight
    // (design/35-Hosting-Integration.md).
    let graph = StateGraph::<Health>::new()
        .add_timer("Window", Duration::from_secs(5 * 60))
        .add_async_node("Check", {
            let stop = stop.clone();
            move |mut health: Health, ctx| {
                let stop = stop.clone();
                async move {
                    stop.cancel();
                    tokio::task::yield_now().await;
                    assert!(!ctx.cancellation().is_cancelled());
                    health.checks += 1;
                    health.healthy = true;
                    health
                }
            }
        })
        .add_edge("Window", "Check")
        .compile()
        .unwrap();
    let waiting = || Health {
        checks: 0,
        healthy: false,
    };
    freeze(&graph, "a", waiting(), &store).await;
    freeze(&graph, "b", waiting(), &store).await;
    let due = in_minutes(6);
    let stopping = || WakeOptions::new().with_stop(stop.clone());

    let report = graph
        .wake_due_with(store.clone(), due, stopping())
        .await
        .unwrap();

    // The thread in flight finished, not cancelled, and the other was never
    // started.
    assert_eq!(report.woken.len(), 1);
    assert!(completed(&report.woken[0].result).healthy);
    assert!(report.skipped.is_empty());
    let other = if report.woken[0].thread_id == "a" {
        "b"
    } else {
        "a"
    };
    assert_eq!(store.log(other).len(), 1);

    // A token that has fired starts nothing.
    let none = graph
        .wake_due_with(store.clone(), due, stopping())
        .await
        .unwrap();
    assert!(none.woken.is_empty() && none.skipped.is_empty());
    assert_eq!(store.log(other).len(), 1);

    // The thread is still due for the next driver.
    let next = graph.wake_due(store.clone(), due).await.unwrap();
    assert_eq!(woken_ids(&next), [(other, WakeReason::Timer)]);
}

#[tokio::test]
async fn a_timer_in_a_loop_freezes_again_with_a_new_wake_time() {
    // Healthy only on the third check: the first wake freezes at the timer again.
    let graph = StateGraph::<Health>::new()
        .add_node("Check", |mut s: Health| {
            s.checks += 1;
            s.healthy = s.checks >= 3;
            s
        })
        .add_timer("Window", Duration::from_secs(60))
        .add_node("Done", |s: Health| s)
        .add_conditional_edge("Check", "Window", |s: &Health| !s.healthy)
        .add_edge("Check", "Done")
        .add_edge("Window", "Check")
        .compile()
        .unwrap();
    let store = Arc::new(LogStore::default());
    freeze(
        &graph,
        "loop",
        Health {
            checks: 0,
            healthy: false,
        },
        &store,
    )
    .await;
    let first = wake_at(&store.latest("loop"));

    let report = graph.wake_due(store.clone(), in_minutes(2)).await.unwrap();
    assert_eq!(woken_ids(&report), [("loop", WakeReason::Timer)]);
    match &report.woken[0].result {
        GraphExecutionResult::Interrupted {
            wait, frozen_state, ..
        } => {
            assert_eq!(frozen_state.checks, 2);
            assert!(at(wait.as_ref().unwrap().wake_at.as_deref().unwrap()) >= first);
        }
        _ => panic!("expected the loop to freeze at the timer again"),
    }
    // It froze again after the call fixed `now`, so it is woken by the next call.
    let report = graph.wake_due(store.clone(), in_minutes(4)).await.unwrap();
    assert_eq!(completed(&report.woken[0].result).checks, 3);
}

#[tokio::test]
async fn a_timer_from_the_state_wakes_at_that_time_rounded_up() {
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Scheduled {
        due_at_ms: i64,
        woke: bool,
    }
    let graph = StateGraph::<Scheduled>::new()
        .add_timer_until("Until", |s: &Scheduled| {
            DateTime::from_timestamp_millis(s.due_at_ms).unwrap()
        })
        .add_node("Woke", |mut s: Scheduled| {
            s.woke = true;
            s
        })
        .add_edge("Until", "Woke")
        .compile()
        .unwrap();
    let store = Arc::new(LogStore::default());
    let due = at("2030-01-02T03:04:05Z") + chrono::Duration::milliseconds(250);
    let state = Scheduled {
        due_at_ms: due.timestamp_millis(),
        woke: false,
    };
    let Some(wait) = freeze(&graph, "s", state, &store).await else {
        panic!("a timer's wait");
    };
    assert_eq!(wait.wake_at.as_deref(), Some("2030-01-02T03:04:06Z"));

    // Not at 03:04:05.9, which would be early; at 03:04:06 it is.
    let early = graph
        .wake_due(
            store.clone(),
            at("2030-01-02T03:04:05Z") + chrono::Duration::milliseconds(900),
        )
        .await
        .unwrap();
    assert!(early.woken.is_empty());
    let report = graph
        .wake_due(store.clone(), at("2030-01-02T03:04:06Z"))
        .await
        .unwrap();
    assert!(completed(&report.woken[0].result).woke);
}

// --- Event waits ---------------------------------------------------------------

#[tokio::test]
async fn an_event_wait_freezes_to_the_shared_fixture() {
    let store = Arc::new(LogStore::default());
    let started = Utc::now();
    let wait = freeze(&deploy_graph(true), "thread_deploy_demo", deploy(), &store).await;

    let latest = store.latest("thread_deploy_demo");
    assert_eq!(
        without_times(latest.clone()),
        without_times(lifecycle_fixture("waiting-event.json"))
    );
    assert_eq!(wait.as_ref(), latest.wait.as_ref());
    // An hour on: the timeout.
    let times_out = wake_at(&latest);
    assert!(times_out >= started + chrono::Duration::hours(1));
    assert!(times_out <= Utc::now() + chrono::Duration::hours(1) + chrono::Duration::seconds(1));
}

#[tokio::test]
async fn a_signal_resumes_an_event_wait_with_its_payload() {
    let store = Arc::new(LogStore::default());
    let graph = deploy_graph(true);
    freeze(&graph, "d", deploy(), &store).await;

    let result = graph
        .signal(
            "d",
            EventSignal::new("deployment.finished")
                .with_override("finished", true)
                .with_override("outcome", "succeeded"),
            store.clone(),
        )
        .await;

    let state = completed(&result);
    assert!(state.finished && state.verified && !state.rolled_back);
    assert_eq!(state.outcome.as_deref(), Some("succeeded"));
    // An event is not a human's answer: no decision.
    let log = store.log("d");
    assert_eq!(log.len(), 2);
    assert_eq!(log[1].decision, None);
    // Signalled, it isn't due when its timeout passes.
    let report = graph
        .wake_due(store.clone(), in_minutes(120))
        .await
        .unwrap();
    assert!(report.woken.is_empty());
}

#[tokio::test]
async fn the_wrong_answer_to_a_wait_is_refused_and_writes_nothing() {
    let store = Arc::new(LogStore::default());
    let deploy_graph = deploy_graph(true);
    let approval_graph = approval_graph(true);
    let window_graph = window_graph();
    freeze(&deploy_graph, "event", deploy(), &store).await;
    freeze(&approval_graph, "human", approval(), &store).await;
    freeze(
        &window_graph,
        "timer",
        Health {
            checks: 0,
            healthy: false,
        },
        &store,
    )
    .await;

    let wrong_type = failed_message(
        deploy_graph
            .signal(
                "event",
                EventSignal::new("deployment.started"),
                store.clone(),
            )
            .await,
    );
    assert!(
        wrong_type.contains(
            "thread 'event' is not waiting for event 'deployment.started': it is waiting for event 'deployment.finished' at [Deploying]"
        ),
        "{wrong_type}"
    );
    let to_a_human = failed_message(
        approval_graph
            .signal(
                "human",
                EventSignal::new("deployment.finished"),
                store.clone(),
            )
            .await,
    );
    assert!(
        to_a_human.contains("it is waiting for a human at [Approve]"),
        "{to_a_human}"
    );
    let to_a_timer = failed_message(
        window_graph
            .signal(
                "timer",
                EventSignal::new("deployment.finished"),
                store.clone(),
            )
            .await,
    );
    assert!(
        to_a_timer.contains("it is waiting for a timer at [Window]"),
        "{to_a_timer}"
    );

    let resume_event = failed_message(
        deploy_graph
            .resume("event", HumanSignal::approve("alice"), store.clone())
            .await,
    );
    assert!(
        resume_event.contains(
            "thread 'event' is waiting for event 'deployment.finished' at [Deploying]: only a signal of that event answers it"
        ),
        "{resume_event}"
    );
    let resume_timer = failed_message(
        window_graph
            .resume("timer", HumanSignal::approve("alice"), store.clone())
            .await,
    );
    assert!(
        resume_timer.contains("thread 'timer' is waiting for a timer at [Window] until"),
        "{resume_timer}"
    );

    // Nothing was written: each thread still waits as it froze.
    for thread in ["event", "human", "timer"] {
        assert_eq!(store.log(thread).len(), 1, "{thread}");
    }

    // A failed thread isn't waiting for anything.
    let failing = StateGraph::<Deploy>::new()
        .add_fallible_node("Fail", |_: &Deploy| {
            Err(datalium_strata::graph::NodeError::Fatal("boom".to_string()))
        })
        .compile()
        .unwrap();
    assert!(matches!(
        failing.run("failed", deploy(), store.clone()).await,
        GraphExecutionResult::Failed(_)
    ));
    let not_waiting = failed_message(
        failing
            .signal(
                "failed",
                EventSignal::new("deployment.finished"),
                store.clone(),
            )
            .await,
    );
    assert!(
        not_waiting.contains(
            "thread 'failed' is not waiting for event 'deployment.finished': its latest checkpoint is failed"
        ),
        "{not_waiting}"
    );
}

// --- Timeouts ------------------------------------------------------------------

#[tokio::test]
async fn an_event_wait_that_times_out_takes_its_timeout_route() {
    let store = Arc::new(LogStore::default());
    let graph = deploy_graph(true);
    freeze(&graph, "d", deploy(), &store).await;
    let collector = Arc::new(Collector::default());

    let report = graph
        .wake_due_with(
            store.clone(),
            in_minutes(61),
            WakeOptions::new().with_config(RunConfig::new().with_observer(collector.clone())),
        )
        .await
        .unwrap();

    assert_eq!(woken_ids(&report), [("d", WakeReason::Timeout)]);
    let state = completed(&report.woken[0].result);
    assert!(state.rolled_back && !state.verified);
    let events = collector.after(1).await;
    let resumed = events
        .iter()
        .find_map(|event| match &event.kind {
            RunEventKind::Resumed {
                wake,
                timed_out_to,
                rejected_to,
                event_type,
                ..
            } => Some((
                *wake,
                timed_out_to.clone(),
                rejected_to.clone(),
                event_type.clone(),
            )),
            _ => None,
        })
        .expect("a Resumed event");
    assert_eq!(
        resumed,
        (
            WakeReason::Timeout,
            Some("Rollback".to_string()),
            None,
            None
        )
    );
}

#[tokio::test]
async fn a_wait_that_times_out_with_no_route_ends_the_thread() {
    let store = Arc::new(LogStore::default());
    let graph = deploy_graph(false);
    freeze(&graph, "d", deploy(), &store).await;

    let report = graph.wake_due(store.clone(), in_minutes(61)).await.unwrap();

    assert_eq!(woken_ids(&report), [("d", WakeReason::Timeout)]);
    match &report.woken[0].result {
        GraphExecutionResult::Cancelled {
            reason,
            node,
            state,
            ..
        } => {
            assert_eq!(*reason, CancelReason::Ended);
            assert_eq!(node, "Deploying");
            assert!(state.started && !state.verified);
        }
        _ => panic!("expected the thread ended"),
    }
    let ended = store.latest("d");
    assert_eq!(ended.status, CheckpointStatus::Ended);
    assert_eq!(ended.wait, None);
    assert_eq!(
        ended
            .metadata
            .as_ref()
            .unwrap()
            .get(END_REASON_KEY)
            .map(String::as_str),
        Some(END_TIMED_OUT)
    );
    // The query port finds threads that timed out.
    let timed_out = store
        .list_threads(&ThreadQuery::default().with_metadata(END_REASON_KEY, END_TIMED_OUT))
        .await
        .unwrap();
    assert_eq!(timed_out.threads.len(), 1);
    // Ended, it is neither due nor resumable.
    assert!(
        graph
            .wake_due(store.clone(), in_minutes(120))
            .await
            .unwrap()
            .woken
            .is_empty()
    );
    assert!(
        failed_message(
            graph
                .signal("d", EventSignal::new("deployment.finished"), store.clone())
                .await
        )
        .contains("was ended")
    );
}

#[tokio::test]
async fn an_approval_with_a_timeout_freezes_to_the_shared_fixture_and_escalates() {
    let store = Arc::new(LogStore::default());
    let graph = approval_graph(true);
    freeze(&graph, "thread_approval_demo", approval(), &store).await;
    assert_eq!(
        without_times(store.latest("thread_approval_demo")),
        without_times(lifecycle_fixture("waiting-approval.json"))
    );

    // Not due after 29 minutes; timed out after 31.
    assert!(
        graph
            .wake_due(store.clone(), in_minutes(29))
            .await
            .unwrap()
            .woken
            .is_empty()
    );
    let report = graph.wake_due(store.clone(), in_minutes(31)).await.unwrap();

    assert_eq!(
        woken_ids(&report),
        [("thread_approval_demo", WakeReason::Timeout)]
    );
    let state = completed(&report.woken[0].result);
    assert!(state.escalated && !state.executed && !state.aborted);
    // A timeout isn't a human's answer: no decision is recorded.
    assert!(
        store
            .log("thread_approval_demo")
            .iter()
            .all(|c| c.decision.is_none())
    );
}

#[tokio::test]
async fn an_approval_with_no_timeout_route_is_ended_when_it_times_out() {
    let store = Arc::new(LogStore::default());
    let graph = approval_graph(false);
    freeze(&graph, "a", approval(), &store).await;

    let report = graph.wake_due(store.clone(), in_minutes(31)).await.unwrap();

    assert!(matches!(
        report.woken[0].result,
        GraphExecutionResult::Cancelled {
            reason: CancelReason::Ended,
            ..
        }
    ));
    assert_eq!(store.latest("a").status, CheckpointStatus::Ended);
}

#[tokio::test]
async fn an_approval_answered_before_its_timeout_is_not_due() {
    let store = Arc::new(LogStore::default());
    let graph = approval_graph(true);
    freeze(&graph, "a", approval(), &store).await;

    let result = graph
        .resume("a", HumanSignal::approve("alice"), store.clone())
        .await;
    assert!(completed(&result).executed);

    assert!(
        graph
            .wake_due(store.clone(), in_minutes(60))
            .await
            .unwrap()
            .woken
            .is_empty()
    );
}

// --- Nesting -------------------------------------------------------------------

#[tokio::test]
async fn a_timer_inside_a_subgraph_freezes_to_the_shared_fixture_and_wakes_into_the_child() {
    let store = Arc::new(LogStore::default());
    let graph = verify_graph();
    let wait = freeze(&graph, "thread_verify_demo", remediation(), &store).await;

    let latest = store.latest("thread_verify_demo");
    assert_eq!(
        without_times(latest.clone()),
        without_times(lifecycle_fixture("nested-timer.json"))
    );
    assert_eq!(wait.as_ref(), latest.innermost_wait());
    assert_eq!(latest.wait, None, "only the innermost level waits");

    // The innermost wake time decides.
    let listed = store
        .list_threads(&ThreadQuery::default().due_by(wake_at(&latest)))
        .await
        .unwrap();
    assert_eq!(listed.threads[0].wait.as_ref(), latest.innermost_wait());

    let report = graph
        .wake_due(store.clone(), wake_at(&latest))
        .await
        .unwrap();
    assert_eq!(
        woken_ids(&report),
        [("thread_verify_demo", WakeReason::Timer)]
    );
    assert_eq!(
        completed(&report.woken[0].result),
        &Remediation {
            step: "closed".to_string(),
            verified: true,
            checks: 2,
        }
    );
}

#[tokio::test]
async fn an_event_wait_inside_a_subgraph_is_signalled() {
    let graph = remediation_graph(
        deploy_graph(true),
        |_| deploy(),
        |entry, deployed| Remediation {
            verified: deployed.verified,
            ..entry.clone()
        },
    );
    let store = Arc::new(LogStore::default());
    let wait = freeze(&graph, "n", remediation(), &store).await;
    assert_eq!(
        wait.unwrap().event_type.as_deref(),
        Some("deployment.finished")
    );

    let result = graph
        .signal(
            "n",
            EventSignal::new("deployment.finished").with_override("finished", true),
            store.clone(),
        )
        .await;

    assert!(completed(&result).verified);
}

#[tokio::test]
async fn a_nested_wait_that_times_out_with_no_route_ends_every_level() {
    let graph = remediation_graph(
        deploy_graph(false),
        |_| deploy(),
        |entry, deployed| Remediation {
            verified: deployed.verified,
            ..entry.clone()
        },
    );
    let store = Arc::new(LogStore::default());
    freeze(&graph, "n", remediation(), &store).await;

    let report = graph.wake_due(store.clone(), in_minutes(61)).await.unwrap();

    match &report.woken[0].result {
        GraphExecutionResult::Cancelled {
            reason, stopped_at, ..
        } => {
            assert_eq!(*reason, CancelReason::Ended);
            assert_eq!(stopped_at, &["Verify".to_string(), "Deploying".to_string()]);
        }
        _ => panic!("expected the thread ended"),
    }
    let ended = store.latest("n");
    assert_eq!(ended.status, CheckpointStatus::Ended);
    let child = ended.nested.as_deref().unwrap();
    assert_eq!(child.status, CheckpointStatus::Ended);
    assert_eq!(child.wait, None);
}

// --- Drivers -------------------------------------------------------------------

#[tokio::test]
async fn two_drivers_wake_a_thread_once() {
    let store = Arc::new(RacingStore {
        inner: LogStore::default(),
        armed: AtomicBool::new(false),
        barrier: tokio::sync::Barrier::new(2),
    });
    let graph = window_graph();
    match graph
        .run(
            "t",
            Health {
                checks: 0,
                healthy: false,
            },
            store.clone(),
        )
        .await
    {
        GraphExecutionResult::Interrupted { .. } => {}
        _ => panic!("expected the run to freeze"),
    }
    let due = wake_at(&store.inner.latest("t"));
    store.armed.store(true, Ordering::SeqCst);

    // Both list the thread and load it before either wakes it.
    let (first, second) = tokio::time::timeout(PATIENCE, async {
        tokio::join!(
            graph.wake_due(store.clone(), due),
            graph.wake_due(store.clone(), due)
        )
    })
    .await
    .expect("both drivers finish");
    let (first, second) = (first.unwrap(), second.unwrap());

    assert_eq!(first.woken.len() + second.woken.len(), 1);
    let skipped: Vec<_> = first.skipped.iter().chain(&second.skipped).collect();
    assert_eq!(skipped.len(), 1);
    assert_eq!(skipped[0].reason, "another driver woke or ended it first");
    let stamps = store
        .inner
        .log("t")
        .iter()
        .filter(|checkpoint| checkpoint.resumed_at.is_some())
        .count();
    assert_eq!(stamps, 1, "one resume stamp");
}

/// A [`LogStore`] whose listing goes stale at once: another driver resumes
/// every thread it lists, between the listing and the load.
struct StaleListing(LogStore);

impl CheckpointStore for StaleListing {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.0.save(checkpoint)
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        self.0.load(thread_id)
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        self.0.load_latest(thread_id)
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        self.0.save_if_latest(checkpoint, expected_sequence)
    }
}

impl CheckpointQuery for StaleListing {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        Box::pin(async move {
            let page = self.0.list_threads(query).await?;
            for thread in &page.threads {
                let mut stamp = self.0.latest(&thread.thread_id);
                stamp.resumed_at = Some("2026-10-06T12:00:00Z".to_string());
                self.0.save(stamp).await?;
            }
            Ok(page)
        })
    }

    fn get_log<'a>(
        &'a self,
        thread_id: &'a str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        self.0.get_log(thread_id, after_sequence, limit)
    }
}

#[tokio::test]
async fn a_thread_woken_between_the_listing_and_the_load_is_skipped() {
    let store = Arc::new(StaleListing(LogStore::default()));
    let graph = window_graph();
    match graph
        .run(
            "t",
            Health {
                checks: 0,
                healthy: false,
            },
            store.clone(),
        )
        .await
    {
        GraphExecutionResult::Interrupted { .. } => {}
        _ => panic!("expected the run to freeze"),
    }

    let report = graph.wake_due(store.clone(), in_minutes(6)).await.unwrap();

    assert!(report.woken.is_empty());
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(report.skipped[0].reason, "it is no longer due");
    // Only the other driver's stamp: this one wrote nothing.
    assert_eq!(store.0.log("t").len(), 2);
}

#[tokio::test]
async fn a_driver_wakes_only_its_own_graphs_threads() {
    let store = Arc::new(LogStore::default());
    let deploys = deploy_graph(true);
    let windows = window_graph();
    match deploys
        .run_with(
            "deploy-1",
            deploy(),
            store.clone(),
            RunConfig::new().with_metadata("graph", "deploy"),
        )
        .await
    {
        GraphExecutionResult::Interrupted { .. } => {}
        _ => panic!("expected the run to freeze"),
    }

    // The metadata filter leaves the other graph's thread alone entirely.
    let filtered = windows
        .wake_due_with(
            store.clone(),
            in_minutes(120),
            WakeOptions::new().with_metadata("graph", "window"),
        )
        .await
        .unwrap();
    assert!(filtered.woken.is_empty() && filtered.skipped.is_empty());

    // Without it, the wait isn't one of this graph's: skipped, nothing written.
    let unfiltered = windows
        .wake_due(store.clone(), in_minutes(120))
        .await
        .unwrap();
    assert!(unfiltered.woken.is_empty());
    assert_eq!(unfiltered.skipped.len(), 1);
    assert_eq!(
        unfiltered.skipped[0].reason,
        "thread 'deploy-1' waits at [Deploying], which has no timeout in this graph"
    );
    assert_eq!(store.log("deploy-1").len(), 1);

    // Its own driver, with its own filter, times it out.
    let own = deploys
        .wake_due_with(
            store.clone(),
            in_minutes(120),
            WakeOptions::new()
                .with_metadata("graph", "deploy")
                .with_limit(10),
        )
        .await
        .unwrap();
    assert_eq!(woken_ids(&own), [("deploy-1", WakeReason::Timeout)]);
}

// --- Run events ----------------------------------------------------------------

#[tokio::test]
async fn run_events_say_what_a_run_waits_for_and_what_woke_it() {
    let store = Arc::new(LogStore::default());
    let graph = deploy_graph(true);
    let collector = Arc::new(Collector::default());
    let config = RunConfig::new().with_observer(collector.clone());
    let frozen = graph
        .run_with("d", deploy(), store.clone(), config.clone())
        .await;
    assert!(matches!(frozen, GraphExecutionResult::Interrupted { .. }));
    graph
        .signal_with(
            "d",
            EventSignal::new("deployment.finished"),
            store.clone(),
            config,
        )
        .await;

    let events = collector.after(2).await;
    let interrupted = events
        .iter()
        .find_map(|event| match &event.kind {
            RunEventKind::Interrupted { wait, .. } => Some(wait.clone()),
            _ => None,
        })
        .expect("an Interrupted event");
    let wait = interrupted.expect("its wait");
    assert_eq!(wait.kind, WaitKind::Event);
    assert_eq!(wait.event_type.as_deref(), Some("deployment.finished"));
    let resumed = events
        .iter()
        .find_map(|event| match &event.kind {
            RunEventKind::Resumed {
                wake,
                event_type,
                is_approved,
                reviewer,
                ..
            } => Some((*wake, event_type.clone(), *is_approved, reviewer.clone())),
            _ => None,
        })
        .expect("a Resumed event");
    assert_eq!(
        resumed,
        (
            WakeReason::Event,
            Some("deployment.finished".to_string()),
            true,
            String::new()
        )
    );

    // A human resume says so.
    let approvals = approval_graph(true);
    let collector = Arc::new(Collector::default());
    let config = RunConfig::new().with_observer(collector.clone());
    approvals
        .run_with("a", approval(), store.clone(), config.clone())
        .await;
    approvals
        .resume_with("a", HumanSignal::approve("alice"), store.clone(), config)
        .await;
    let events = collector.after(2).await;
    assert!(events.iter().any(|event| matches!(
        &event.kind,
        RunEventKind::Resumed {
            wake: WakeReason::Human,
            ..
        }
    )));
    assert!(events.iter().any(|event| matches!(
        &event.kind,
        RunEventKind::Interrupted {
            wait: Some(CheckpointWait {
                kind: WaitKind::Human,
                ..
            }),
            ..
        }
    )));
}

// --- Definitions and compile-time rules ----------------------------------------

/// `waits.json`'s graph.
fn release_graph() -> StateGraph<i32> {
    StateGraph::<i32>::new()
        .add_node("Plan", |s: i32| s)
        .add_node("Approve", |s: i32| s)
        .add_interrupt_boundary_with(
            "Approve",
            BoundaryOptions::new()
                .with_reject("Abort")
                .with_timeout(Duration::from_secs(30 * 60))
                .with_on_timeout("Escalate"),
        )
        .add_wait_for_event_with(
            "Deploying",
            "deployment.finished",
            WaitOptions::new()
                .with_timeout(Duration::from_secs(3600))
                .with_on_timeout("Rollback"),
        )
        .add_node("Check", |s: i32| s)
        .add_timer("Window", Duration::from_secs(300))
        .set_node_metadata("Window", [("purpose", "verification window")])
        .add_node("Close", |s: i32| s)
        .add_node("Abort", |s: i32| s)
        .add_node("Escalate", |s: i32| s)
        .add_node("Rollback", |s: i32| s)
        .add_edge("Plan", "Approve")
        .add_edge("Approve", "Deploying")
        .add_edge("Deploying", "Check")
        .add_conditional_edge("Check", "Window", |s: &i32| *s < 3)
        .add_edge("Check", "Close")
        .add_edge("Window", "Check")
}

#[test]
fn the_export_matches_the_shared_waits_fixture() {
    let graph = release_graph();
    let exported = serde_json::to_value(graph.to_definition()).unwrap();
    assert_eq!(exported, fixture("graph-export/waits.json"));

    // The compiled graph exports the same, and the file round-trips and is sound.
    let compiled = graph.compile().unwrap();
    assert_eq!(
        serde_json::to_value(compiled.to_definition()).unwrap(),
        fixture("graph-export/waits.json")
    );
    let definition: GraphDefinition =
        serde_json::from_value(fixture("graph-export/waits.json")).unwrap();
    assert_eq!(
        serde_json::to_value(&definition).unwrap(),
        fixture("graph-export/waits.json")
    );
    assert_eq!(definition.check_shape(), Ok(()));
    assert!(validate(&definition).is_valid());

    // The timeout routes are what keep Escalate and Rollback reachable.
    let mut without = definition;
    without
        .edges
        .retain(|edge| edge.condition.as_deref() != Some("on_timeout"));
    let orphans: Vec<String> = validate(&without)
        .errors
        .into_iter()
        .map(|error| error.node)
        .collect();
    assert_eq!(orphans, ["Escalate", "Rollback"]);
}

#[test]
fn a_wait_in_a_maps_template_is_reported_at_any_depth() {
    let definition: GraphDefinition =
        serde_json::from_value(fixture("graph-validation/wait-in-map.json")).unwrap();
    assert_eq!(definition.check_shape(), Ok(()));

    let errors: Vec<(&str, String)> = validate(&definition)
        .errors
        .into_iter()
        .map(|error| (error.code, error.node))
        .collect();
    assert_eq!(
        errors,
        [
            ("STRATA_ERR_WAIT_IN_MAP", "Fan/*/Window".to_string()),
            (
                "STRATA_ERR_WAIT_IN_MAP",
                "Review/Batch/*/Inner/Hold".to_string()
            ),
        ]
    );
}

#[test]
fn a_definition_with_a_wait_must_say_so() {
    let mut old = fixture("graph-export/waits.json");
    old["schema_version"] = "1.3.0".into();
    // 1.3.0 had no boundaries either (design/48-Boundaries-In-Definitions.md).
    old["nodes"][1].as_object_mut().unwrap().remove("interrupt");
    let old: GraphDefinition = serde_json::from_value(old).unwrap();
    assert_eq!(
        old.check_shape(),
        Err("node 'Deploying' is a wait, which schema_version 1.3.0 doesn't have".into())
    );

    let mut no_wait = fixture("graph-export/waits.json");
    no_wait["nodes"][2].as_object_mut().unwrap().remove("wait");
    let no_wait: GraphDefinition = serde_json::from_value(no_wait).unwrap();
    assert_eq!(
        no_wait.check_shape(),
        Err("node 'Deploying' is a wait but has no wait".into())
    );

    let mut stray = fixture("graph-export/waits.json");
    stray["nodes"][0]["wait"] = fixture("graph-export/waits.json")["nodes"][2]["wait"].clone();
    let stray: GraphDefinition = serde_json::from_value(stray).unwrap();
    assert_eq!(
        stray.check_shape(),
        Err("node 'Plan' has a wait but is not a wait node".into())
    );
}

#[test]
fn compile_refuses_a_wait_a_map_item_would_hit_and_other_misconfigurations() {
    let child = window_graph();
    let refused = StateGraph::<Vec<u32>>::new()
        .add_map(
            "Fan",
            |items: &Vec<u32>| items.clone(),
            |item: &u32| item.to_string(),
            child,
            |_: &Vec<u32>, _| Health {
                checks: 0,
                healthy: false,
            },
            |entry: &Vec<u32>, _| entry.clone(),
        )
        .compile();
    assert_eq!(
        refused.err().map(|error| format!("{error:?}")),
        Some(
            "CompilationFailed(\"map 'Fan' runs a graph with a wait at 'Window'; a map item can't freeze\")"
                .to_string()
        )
    );

    let compile = |graph: StateGraph<i32>| {
        graph
            .compile()
            .err()
            .map(|error| format!("{error:?}"))
            .unwrap_or_default()
    };
    assert!(
        compile(
            StateGraph::<i32>::new()
                .add_timer("Wait", Duration::from_secs(1))
                .add_interrupt_boundary("Wait")
        )
        .contains("'Wait' is a wait node and can't also be an interrupt boundary")
    );
    assert!(
        compile(StateGraph::<i32>::new().add_timer("Wait", Duration::ZERO))
            .contains("timer 'Wait' has a delay of zero")
    );
    assert!(
        compile(StateGraph::<i32>::new().add_wait_for_event_with(
            "Wait",
            "e",
            WaitOptions::new().with_on_timeout("Wait")
        ))
        .contains("wait 'Wait' has a timeout route but no timeout")
    );
    assert!(
        compile(
            StateGraph::<i32>::new()
                .add_node("Approve", |s: i32| s)
                .add_interrupt_boundary_with(
                    "Approve",
                    BoundaryOptions::new().with_timeout(Duration::ZERO)
                )
        )
        .contains("interrupt boundary 'Approve' has a timeout of zero")
    );
    // A timeout route to a node that doesn't exist dangles.
    assert!(
        compile(
            StateGraph::<i32>::new().add_wait_for_event_with(
                "Wait",
                "e",
                WaitOptions::new()
                    .with_timeout(Duration::from_secs(1))
                    .with_on_timeout("Nowhere")
            )
        )
        .contains("DanglingEdge")
    );

    // A map's child may not wait inside a subgraph either.
    let nested = remediation_graph(
        window_graph(),
        |_| Health {
            checks: 0,
            healthy: false,
        },
        |entry, _| entry.clone(),
    );
    let refused = StateGraph::<Vec<u32>>::new()
        .add_map_with(
            "Fan",
            |items: &Vec<u32>| items.clone(),
            |item: &u32| item.to_string(),
            nested,
            |_: &Vec<u32>, _| remediation(),
            |entry: &Vec<u32>, _| entry.clone(),
            MapOptions::default(),
        )
        .compile();
    assert!(
        format!("{:?}", refused.err()).contains("a wait at 'Verify/Window'"),
        "the wait is named under its subgraph"
    );
}
