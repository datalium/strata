//! Tests against a live Strata Cloud (`design/30-Strata-Cloud-Store.md`,
//! "Tests"): a local Strata Cloud stack at
//! `STRATA_CLOUD_URL`, with an SDK key minted from the dev token in
//! `STRATA_CLOUD_KEY`, and for the decision `STRATA_CLOUD_CONSOLE_TOKEN`.
//! Skipped without them, and not part of CI. Mirrors the .NET `CloudLiveTests`.
//!
//! Leases and the query port (`design/43-Cloud-Leases-And-Listing.md`): a
//! leased run, a held thread, a lost lease, the wake driver, a wake through a
//! resume and the recovery driver, each on graphs with an identity of their
//! own per run, so the drivers list only this run's threads in a dev account
//! that keeps everything earlier runs left.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::Utc;
use common::{initial, rollout_graph};
use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::{CancelReason, GraphExecutionResult, LevelSignals};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::lease::LeaseOptions;
use datalium_strata::recovery::RecoverOptions;
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::store::{
    CheckpointQuery, CheckpointStore, CheckpointStoreError, LatestCheckpoint, LeaseAttempt,
    LeasedSave, ThreadLease,
};
use datalium_strata_cloud::{
    DecisionOutcome, DecisionWorker, Decisions, StrataCloudCheckpointStore, StrataCloudOptions,
    equivalent,
};
use serde_json::{Value, json};

const URL_VAR: &str = "STRATA_CLOUD_URL";
const KEY_VAR: &str = "STRATA_CLOUD_KEY";
const CONSOLE_TOKEN_VAR: &str = "STRATA_CLOUD_CONSOLE_TOKEN";

/// The live options, or `None` (the test is skipped) when a variable is unset.
fn live(console: bool) -> Option<StrataCloudOptions> {
    let mut vars = vec![URL_VAR, KEY_VAR];
    if console {
        vars.push(CONSOLE_TOKEN_VAR);
    }
    let missing: Vec<_> = vars
        .into_iter()
        .filter(|var| std::env::var(var).map_or(true, |v| v.is_empty()))
        .collect();
    if !missing.is_empty() {
        eprintln!(
            "SKIPPED: {} not set (a live Strata Cloud; see design/30)",
            missing.join(", ")
        );
        return None;
    }
    Some(StrataCloudOptions::new(
        std::env::var(URL_VAR).unwrap(),
        std::env::var(KEY_VAR).unwrap(),
    ))
}

fn thread_id(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("sg18-live-{name}-{nanos}")
}

/// A thread id, or a graph id, unique to this run, for SG-36's tests.
fn unique(name: &str) -> String {
    thread_id(name).replacen("sg18-live-", "sg36-live-", 1)
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Transfer {
    drafted: bool,
    sent: bool,
}

fn transfer() -> Transfer {
    Transfer {
        drafted: false,
        sent: false,
    }
}

/// `Draft` -> `Send`, a checkpoint per node, under a graph id of its own.
fn durable_graph(graph_id: &str) -> Arc<CompiledGraph<Transfer>> {
    Arc::new(
        StateGraph::new()
            .add_node("Draft", |mut s: Transfer| {
                s.drafted = true;
                s
            })
            .add_node("Send", |mut s: Transfer| {
                s.sent = true;
                s
            })
            .add_edge("Draft", "Send")
            .compile_with(
                CompileOptions::new()
                    .with_checkpointing(CheckpointPolicy::EveryNode)
                    .with_graph_id(graph_id),
            )
            .unwrap(),
    )
}

/// `Draft` -> a one-hour timer `Window` -> `Send`, under a graph id of its
/// own.
fn timer_graph(graph_id: &str) -> Arc<CompiledGraph<Transfer>> {
    Arc::new(
        StateGraph::new()
            .add_node("Draft", |mut s: Transfer| {
                s.drafted = true;
                s
            })
            .add_timer("Window", Duration::from_secs(3600))
            .add_node("Send", |mut s: Transfer| {
                s.sent = true;
                s
            })
            .add_edge("Draft", "Window")
            .add_edge("Window", "Send")
            .compile_with(CompileOptions::new().with_graph_id(graph_id))
            .unwrap(),
    )
}

fn leased(owner: &str, ttl: Duration) -> RunConfig {
    RunConfig::new().with_lease(LeaseOptions::new(owner, ttl))
}

async fn statuses(store: &StrataCloudCheckpointStore, thread_id: &str) -> Vec<CheckpointStatus> {
    store
        .get_log(thread_id, None, 100)
        .await
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| entry.checkpoint.status)
        .collect()
}

/// The Cloud store with renewals answered here and never sent: the run
/// believes its lease is renewed while it runs out on Cloud.
struct NoRenewals(Arc<StrataCloudCheckpointStore>);

impl CheckpointStore for NoRenewals {
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

    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        self.0.try_acquire_lease(thread_id, owner, ttl)
    }

    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(Some(lease.clone()))))
    }

    fn release_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
    ) -> BoxFuture<'a, Result<bool, CheckpointStoreError>> {
        self.0.release_lease(lease)
    }

    fn get_lease<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        self.0.get_lease(thread_id)
    }

    fn save_under_lease<'a>(
        &'a self,
        checkpoint: Checkpoint,
        lease: &'a ThreadLease,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>> {
        self.0
            .save_under_lease(checkpoint, lease, expected_sequence)
    }
}

/// A reviewer approves the thread's pending approval through the console
/// API, with the dev token.
async fn approve(thread_id: &str, reviewer: &str, notes: Option<&str>) {
    let base = std::env::var(URL_VAR).unwrap();
    let base = base.trim_end_matches('/');
    let token = std::env::var(CONSOLE_TOKEN_VAR).unwrap();
    let client = reqwest::Client::new();
    let mut approval_id = None;
    for _ in 0..20 {
        let queue = client
            .get(format!(
                "{base}/v1/approvals?status=pending&order=desc&limit=100"
            ))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let queue: Value = serde_json::from_str(&queue).unwrap();
        approval_id = queue["data"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["thread"]["threadId"] == thread_id)
            .map(|item| item["approvalId"].as_str().unwrap().to_string());
        if approval_id.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let approval_id = approval_id.expect("an approval for the thread");
    let mut body = json!({ "isApproved": true, "reviewerToken": reviewer });
    if let Some(notes) = notes {
        body["notes"] = json!(notes);
    }
    let decided = client
        .post(format!("{base}/v1/approvals/{approval_id}/decision"))
        .bearer_auth(&token)
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert!(
        decided.status().is_success(),
        "decision: {}",
        decided.status()
    );
}

#[tokio::test]
async fn saves_loads_and_interrupts_on_a_live_cloud() {
    let Some(options) = live(false) else {
        return;
    };
    let store = Arc::new(StrataCloudCheckpointStore::new(options).unwrap());
    let (graph, _) = rollout_graph();
    let thread_id = thread_id("interrupt");

    assert!(store.load_checkpoint(&thread_id).await.unwrap().is_none());
    let result = graph.run(&thread_id, initial(), store.clone()).await;
    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));

    let latest = store.load_checkpoint(&thread_id).await.unwrap().unwrap();
    assert_eq!(CheckpointStatus::Interrupted, latest.checkpoint.status);
    assert_eq!(Some("Prepare"), latest.checkpoint.suspended_node.as_deref());

    // The idempotent retry: the same checkpoint again appends nothing.
    store.save(latest.checkpoint.clone()).await.unwrap();
    let again = store.load_checkpoint(&thread_id).await.unwrap().unwrap();
    assert!(equivalent(&latest.checkpoint, &again.checkpoint));
    assert_eq!(latest.sequence, again.sequence);
}

#[tokio::test]
async fn a_decision_in_the_console_is_resumed_by_the_worker() {
    let Some(options) = live(true) else {
        return;
    };
    let store = Arc::new(StrataCloudCheckpointStore::new(options.clone()).unwrap());
    let (graph, executed) = rollout_graph();
    let thread_id = thread_id("decision");
    let result = graph.run(&thread_id, initial(), store.clone()).await;
    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));

    approve(&thread_id, "sg18-live-reviewer", Some("sg38 live notes")).await;

    let mut worker = DecisionWorker::new(graph, store.clone(), Decisions::new(options).unwrap())
        .with_thread_id(&thread_id);
    let mut handled = None;
    for _ in 0..5 {
        if let Some(first) = worker.poll(Duration::from_secs(5)).await.unwrap().pop() {
            handled = Some(first);
            break;
        }
    }
    let handled = handled.expect("the worker handles the decision");
    assert_eq!(DecisionOutcome::Resumed, handled.outcome);
    assert_eq!("sg18-live-reviewer", handled.decision.signal.reviewer_token);
    // Who decided is the console's verified identity, not the reviewer
    // token (contract §5, SG-38).
    assert_eq!(Some("dev-console"), handled.decision.decided_by.as_deref());
    assert_eq!(None, handled.decision.decided_by_email);
    assert_eq!(Some("sg38 live notes"), handled.decision.notes.as_deref());
    assert!(matches!(
        handled.result,
        Some(GraphExecutionResult::Completed(_))
    ));
    assert_eq!(1, executed.load(Ordering::SeqCst));
    let latest = store.load(&thread_id).await.unwrap().unwrap();
    assert!(latest.resumed_at.is_some());
    assert!(worker.poll(Duration::ZERO).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_leased_run_on_a_live_cloud_completes_and_releases_its_lease() {
    let Some(options) = live(false) else {
        return;
    };
    let store = Arc::new(StrataCloudCheckpointStore::new(options).unwrap());
    let thread_id = unique("leased");

    let result = durable_graph(&unique("graph"))
        .run_with(
            &thread_id,
            transfer(),
            store.clone(),
            leased("sg36-worker-1", Duration::from_secs(30)),
        )
        .await;

    assert!(matches!(result, GraphExecutionResult::Completed(_)));
    use CheckpointStatus::{Completed, Running};
    assert_eq!(
        statuses(&store, &thread_id).await,
        [Running, Running, Completed]
    );
    assert_eq!(None, store.get_lease(&thread_id).await.unwrap());
}

#[tokio::test]
async fn a_thread_held_on_a_live_cloud_is_refused_with_nothing_written() {
    let Some(options) = live(false) else {
        return;
    };
    let store = Arc::new(StrataCloudCheckpointStore::new(options).unwrap());
    let thread_id = unique("held");
    let holder = store
        .try_acquire_lease(&thread_id, "sg36-worker-2", Duration::from_secs(60))
        .await
        .unwrap()
        .acquired()
        .unwrap();

    let result = durable_graph(&unique("graph"))
        .run_with(
            &thread_id,
            transfer(),
            store.clone(),
            leased("sg36-worker-1", Duration::from_secs(30)),
        )
        .await;

    match result {
        GraphExecutionResult::Failed(error) => {
            let message = format!("{error:?}");
            assert!(
                message.contains("is held by 'sg36-worker-2' until "),
                "{message}"
            );
        }
        _ => panic!("expected the held thread to be refused"),
    }
    assert!(store.load_checkpoint(&thread_id).await.unwrap().is_none());
    assert!(store.release_lease(&holder).await.unwrap());
}

#[tokio::test]
async fn a_save_after_a_takeover_on_a_live_cloud_is_cancelled_as_lease_lost() {
    let Some(options) = live(false) else {
        return;
    };
    let cloud = Arc::new(StrataCloudCheckpointStore::new(options).unwrap());
    let thread_id = unique("takeover");
    // `Draft` outlives the run's one-second lease, which isn't renewed, and
    // another owner takes the thread: the run's next save is fenced off.
    let (thief, stolen) = (cloud.clone(), thread_id.clone());
    let graph = StateGraph::new()
        .add_async_node("Draft", move |mut s: Transfer, _| {
            let (thief, stolen) = (thief.clone(), stolen.clone());
            async move {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                let taken = thief
                    .try_acquire_lease(&stolen, "sg36-worker-2", Duration::from_secs(60))
                    .await
                    .unwrap();
                assert!(matches!(taken, LeaseAttempt::Acquired(_)));
                s.drafted = true;
                s
            }
        })
        .add_node("Send", |mut s: Transfer| {
            s.sent = true;
            s
        })
        .add_edge("Draft", "Send")
        .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
        .unwrap();

    let result = graph
        .run_with(
            &thread_id,
            transfer(),
            Arc::new(NoRenewals(cloud.clone())),
            leased("sg36-worker-1", Duration::from_secs(1)),
        )
        .await;

    assert!(
        matches!(
            result,
            GraphExecutionResult::Cancelled {
                reason: CancelReason::LeaseLost,
                ..
            }
        ),
        "expected the run to lose its lease"
    );
    // Only the claim before `Draft`.
    assert_eq!(
        statuses(&cloud, &thread_id).await,
        [CheckpointStatus::Running]
    );
    let held = cloud.get_lease(&thread_id).await.unwrap().unwrap();
    assert_eq!("sg36-worker-2", held.owner);
    assert!(cloud.release_lease(&held).await.unwrap());
}

#[tokio::test]
async fn due_timers_wake_on_a_live_cloud_by_the_driver_and_through_a_resume() {
    let Some(options) = live(false) else {
        return;
    };
    let store = Arc::new(StrataCloudCheckpointStore::new(options).unwrap());
    let graph = timer_graph(&unique("timers"));
    let (driven, resumed) = (unique("timer-driven"), unique("timer-resumed"));
    for thread_id in [&driven, &resumed] {
        let result = graph.run(thread_id, transfer(), store.clone()).await;
        assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    }
    let later = Utc::now() + chrono::Duration::hours(2);

    let woken = graph
        .resume_with(
            &resumed,
            LevelSignals::due_at(later),
            store.clone(),
            RunConfig::new(),
        )
        .await;
    assert!(matches!(woken, GraphExecutionResult::Completed(_)));

    // The driver lists this graph's due threads: only `driven` is left.
    let report = graph.wake_due(store.clone(), later).await.unwrap();
    assert_eq!(1, report.woken.len(), "skipped: {:?}", report.skipped);
    assert_eq!(driven, report.woken[0].thread_id);
    assert!(matches!(
        report.woken[0].result,
        GraphExecutionResult::Completed(_)
    ));
}

#[tokio::test]
async fn recover_stale_lists_and_recovers_a_running_thread_on_a_live_cloud() {
    let Some(options) = live(false) else {
        return;
    };
    let store = Arc::new(StrataCloudCheckpointStore::new(options).unwrap());
    let graph = durable_graph(&unique("durable"));
    let (finished, dead) = (unique("finished"), unique("dead"));
    graph.run(&finished, transfer(), store.clone()).await;

    // A thread whose process died after `Draft`.
    let mut running = store
        .get_log(&finished, None, 100)
        .await
        .unwrap()
        .entries
        .remove(1)
        .checkpoint;
    running.thread_id = dead.clone();
    store.save(running).await.unwrap();

    let report = graph
        .recover_stale(
            store.clone(),
            Utc::now() + chrono::Duration::hours(1),
            RecoverOptions::new().with_stale_after(Duration::from_secs(60)),
        )
        .await
        .unwrap();

    assert_eq!(1, report.recovered.len(), "skipped: {:?}", report.skipped);
    assert_eq!(dead, report.recovered[0].thread_id);
    assert!(matches!(
        report.recovered[0].result,
        GraphExecutionResult::Completed(_)
    ));
    use CheckpointStatus::{Completed, Running};
    assert_eq!(statuses(&store, &dead).await, [Running, Running, Completed]);
}
