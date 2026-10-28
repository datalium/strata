//! Run metadata (`design/18-Run-Observability.md`, SG-14): caller metadata is
//! stamped on every checkpoint a run writes, at every nesting level, beside the
//! engine's own keys; reserved keys are refused before anything is written; a
//! resume carries the stored metadata forward and lays its own over it. The
//! shared fixtures `metadata-nested.json` and `metadata-failed.json` are
//! produced and resumed by both engines. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/RunMetadataTests.cs` — keep the two in
//! step.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;

use datalium_strata::checkpoint::{
    CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, NodeError, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::Checkpoint;
use datalium_strata::{CancellationToken, StrataError};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    amount: i64,
    log: Vec<String>,
    approved_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: i64,
    trail: Vec<String>,
    approved_by: Option<String>,
}

fn order(amount: i64) -> Order {
    Order {
        amount,
        log: Vec::new(),
        approved_by: None,
    }
}

fn job() -> Job {
    Job {
        n: 1,
        trail: Vec::new(),
        approved_by: None,
    }
}

#[derive(Default)]
struct TestStore {
    checkpoints: Mutex<HashMap<String, Checkpoint>>,
}

impl TestStore {
    fn save(&self, checkpoint: Checkpoint) -> Result<(), CheckpointStoreError> {
        self.checkpoints
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
        Ok(())
    }

    fn load(&self, thread_id: &str) -> Result<Option<Checkpoint>, CheckpointStoreError> {
        Ok(self.checkpoints.lock().unwrap().get(thread_id).cloned())
    }
}

impl CheckpointStore for TestStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        Box::pin(std::future::ready(TestStore::save(self, checkpoint)))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(TestStore::load(self, thread_id)))
    }
}

fn store() -> Arc<TestStore> {
    Arc::new(TestStore::default())
}

fn with(metadata: &[(&str, &str)]) -> RunConfig {
    metadata
        .iter()
        .fold(RunConfig::new(), |config, (key, value)| {
            config.with_metadata(*key, *value)
        })
}

fn map(entries: &[(&str, &str)]) -> HashMap<String, String> {
    entries
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn no_signal() -> HumanSignal {
    HumanSignal {
        is_approved: true,
        reviewer_token: "x".to_string(),
        field_overrides: HashMap::new(),
        ..Default::default()
    }
}

fn approve_as(who: &str) -> HumanSignal {
    HumanSignal {
        is_approved: true,
        reviewer_token: "token".to_string(),
        field_overrides: map(&[("approved_by", who)]),
        ..Default::default()
    }
}

type Metadata = HashMap<String, String>;
type Seen = Arc<Mutex<Vec<Metadata>>>;

/// `Draft` (n * 2) -> `Approve` (a boundary) -> `Send` (n + 1), which records
/// the metadata it was handed in `seen`, or fails fatally when `send_fails`.
fn review_child(send_fails: bool, seen: Seen) -> CompiledGraph<Job> {
    StateGraph::<Job>::new()
        .add_node("Draft", |mut j: Job| {
            j.n *= 2;
            j.trail.push("draft".to_string());
            j
        })
        .add_node("Approve", |mut j: Job| {
            j.trail.push("approve".to_string());
            j
        })
        .add_interrupt_boundary("Approve")
        .add_async_fallible_node("Send", move |mut j: Job, ctx: NodeContext| {
            seen.lock().unwrap().push(ctx.metadata().clone());
            async move {
                if send_fails {
                    return Err(NodeError::Fatal("mailbox closed".to_string()));
                }
                j.n += 1;
                j.trail.push("send".to_string());
                Ok(j)
            }
        })
        .add_edge("Draft", "Approve")
        .add_edge("Approve", "Send")
        .compile()
        .unwrap()
}

/// `Prepare` -> `Work` (the subgraph) -> `Finish`, the shape of `nested.json`.
fn parent(child: CompiledGraph<Job>) -> CompiledGraph<Order> {
    StateGraph::<Order>::new()
        .add_node("Prepare", |mut o: Order| {
            o.log.push("prepared".to_string());
            o
        })
        .add_subgraph(
            "Work",
            child,
            |o: &Order| Job {
                n: o.amount,
                trail: Vec::new(),
                approved_by: None,
            },
            |entry: &Order, job: Job| {
                let mut next = entry.clone();
                next.amount = job.n;
                next.log.extend(job.trail);
                next.approved_by = job.approved_by;
                next
            },
        )
        .add_node("Finish", |mut o: Order| {
            o.log.push("finished".to_string());
            o
        })
        .add_edge("Prepare", "Work")
        .add_edge("Work", "Finish")
        .compile()
        .unwrap()
}

fn lifecycle_fixture(name: &str) -> Checkpoint {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/checkpoint-lifecycle")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn without_timestamps(mut checkpoint: Checkpoint) -> Checkpoint {
    checkpoint.created_at = String::new();
    if let Some(decision) = checkpoint.decision.as_mut() {
        decision.decided_at = String::new();
    }
    checkpoint.nested = checkpoint
        .nested
        .map(|child| Box::new(without_timestamps(*child)));
    checkpoint
}

fn assert_matches_fixture(name: &str, persisted: Checkpoint) {
    assert_eq!(
        serde_json::to_value(without_timestamps(persisted)).unwrap(),
        serde_json::to_value(without_timestamps(lifecycle_fixture(name))).unwrap()
    );
}

fn failed_message(result: GraphExecutionResult<Order>) -> String {
    match result {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => message,
        GraphExecutionResult::Failed(other) => panic!("expected ExecutionAborted, got {other:?}"),
        _ => panic!("expected Failed"),
    }
}

fn completed<S>(result: GraphExecutionResult<S>) -> S {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => panic!("expected Completed, got Failed({err:?})"),
        _ => panic!("expected Completed"),
    }
}

// --- Every checkpoint kind ---------------------------------------------------

#[tokio::test]
async fn an_interrupted_checkpoint_carries_the_run_metadata() {
    let graph = StateGraph::<Job>::new()
        .add_node("Approve", |j: Job| j)
        .add_interrupt_boundary("Approve")
        .add_node("Send", |j: Job| j)
        .add_edge("Approve", "Send")
        .compile()
        .unwrap();
    let store = store();

    graph
        .run_with("t", job(), store.clone(), with(&[("tenant", "acme")]))
        .await;

    assert_eq!(
        store.load("t").unwrap().unwrap().metadata,
        Some(map(&[("tenant", "acme")]))
    );
}

#[tokio::test]
async fn a_failed_checkpoint_merges_the_run_metadata_with_the_engine_keys() {
    let graph = StateGraph::<Job>::new()
        .add_fallible_node("Send", |_: &Job| {
            Err(NodeError::Transient("gateway timeout".to_string()))
        })
        .compile()
        .unwrap();
    let store = store();

    graph
        .run_with("t", job(), store.clone(), with(&[("tenant", "acme")]))
        .await;

    assert_eq!(
        store.load("t").unwrap().unwrap().metadata,
        Some(map(&[
            ("tenant", "acme"),
            ("error", "gateway timeout"),
            ("error_kind", "transient"),
            ("attempts", "1"),
        ]))
    );
}

#[tokio::test]
async fn a_cancelled_checkpoint_merges_the_run_metadata_with_the_cancel_reason() {
    let started = Arc::new(Notify::new());
    let graph = {
        let started = started.clone();
        StateGraph::<Job>::new()
            .add_async_node("Send", move |j: Job, _ctx: NodeContext| {
                let started = started.clone();
                async move {
                    started.notify_one();
                    std::future::pending::<()>().await;
                    j
                }
            })
            .compile()
            .unwrap()
    };
    let store = store();
    let token = CancellationToken::new();

    let run = graph.run_with(
        "t",
        job(),
        store.clone(),
        with(&[("tenant", "acme")]).with_cancellation(token.clone()),
    );
    let cancel = async {
        started.notified().await;
        token.cancel();
    };
    let (result, ()) = tokio::time::timeout(PATIENCE, async { tokio::join!(run, cancel) })
        .await
        .unwrap();
    assert!(matches!(result, GraphExecutionResult::Cancelled { .. }));

    assert_eq!(
        store.load("t").unwrap().unwrap().metadata,
        Some(map(&[("tenant", "acme"), ("cancel_reason", "requested")]))
    );
}

#[tokio::test]
async fn without_metadata_an_interrupted_checkpoint_still_has_none() {
    let store = store();

    parent(review_child(false, Seen::default()))
        .run("t", order(5), store.clone())
        .await;

    let checkpoint = store.load("t").unwrap().unwrap();
    assert_eq!(checkpoint.metadata, None);
    assert_eq!(checkpoint.nested.unwrap().metadata, None);
}

// --- Reserved keys -----------------------------------------------------------

#[tokio::test]
async fn a_reserved_key_is_refused_before_anything_is_written() {
    for key in [
        "error",
        "error_kind",
        "attempts",
        "cancel_reason",
        "strata.run_id",
        "strata.",
    ] {
        let store = store();
        let graph = parent(review_child(false, Seen::default()));

        let message = failed_message(
            graph
                .run_with("t", order(5), store.clone(), with(&[(key, "x")]))
                .await,
        );

        assert_eq!(message, format!("run metadata key '{key}' is reserved"));
        assert!(store.load("t").unwrap().is_none());
    }
}

#[tokio::test]
async fn an_empty_key_is_refused() {
    let message = failed_message(
        parent(review_child(false, Seen::default()))
            .run_with("t", order(5), store(), with(&[("", "x")]))
            .await,
    );

    assert_eq!(message, "run metadata keys can't be empty");
}

#[tokio::test]
async fn a_resume_with_a_reserved_key_is_refused_before_the_checkpoint_is_stamped() {
    let store = store();
    let graph = parent(review_child(false, Seen::default()));
    graph.run("t", order(5), store.clone()).await;

    failed_message(
        graph
            .resume_with("t", no_signal(), store.clone(), with(&[("error", "x")]))
            .await,
    );

    assert!(store.load("t").unwrap().unwrap().resumed_at.is_none());
}

#[tokio::test]
async fn a_streaming_run_with_a_reserved_key_is_refused() {
    let graph = Arc::new(parent(review_child(false, Seen::default())));
    let store = store();

    let (handle, _chunks) =
        graph.run_streaming_with("t", order(5), store.clone(), with(&[("attempts", "x")]));

    failed_message(handle.await.unwrap());
    assert!(store.load("t").unwrap().is_none());
}

// --- Through a resume --------------------------------------------------------

#[tokio::test]
async fn a_resume_carries_the_stored_metadata_and_overrides_it_key_by_key() {
    let seen = Seen::default();
    let graph = parent(review_child(false, seen.clone()));
    let store = store();
    graph
        .run_with(
            "t",
            order(5),
            store.clone(),
            with(&[("tenant", "acme"), ("incident", "INC-1")]),
        )
        .await;

    graph
        .resume_with(
            "t",
            no_signal(),
            store.clone(),
            with(&[("incident", "INC-2")]),
        )
        .await;

    assert_eq!(
        *seen.lock().unwrap(),
        vec![map(&[("tenant", "acme"), ("incident", "INC-2")])]
    );
}

#[tokio::test]
async fn the_resume_stamp_leaves_the_old_checkpoint_metadata_alone() {
    let graph = parent(review_child(false, Seen::default()));
    let store = store();
    graph
        .run_with("t", order(5), store.clone(), with(&[("incident", "INC-1")]))
        .await;

    graph
        .resume_with(
            "t",
            no_signal(),
            store.clone(),
            with(&[("incident", "INC-2")]),
        )
        .await;

    // The run completed, so the latest checkpoint is the stamped one.
    let stamped = store.load("t").unwrap().unwrap();
    assert!(stamped.resumed_at.is_some());
    assert_eq!(stamped.metadata.unwrap()["incident"], "INC-1");
    assert_eq!(
        stamped.nested.unwrap().metadata.unwrap()["incident"],
        "INC-1"
    );
}

// --- NodeContext -------------------------------------------------------------

#[tokio::test]
async fn every_depth_sees_the_run_metadata() {
    let seen: Arc<Mutex<Vec<(String, Metadata)>>> = Arc::default();
    let record = {
        let seen = seen.clone();
        move |j: Job, ctx: NodeContext| {
            seen.lock()
                .unwrap()
                .push((ctx.node_path().to_string(), ctx.metadata().clone()));
            async move { j }
        }
    };
    let branch_record = {
        let seen = seen.clone();
        move |j: Job, ctx: NodeContext| {
            seen.lock()
                .unwrap()
                .push((ctx.node_path().to_string(), ctx.metadata().clone()));
            async move { Ok(j) }
        }
    };

    let child = StateGraph::<Job>::new()
        .add_async_node("Inner", record.clone())
        .compile()
        .unwrap();
    let graph = StateGraph::<Job>::new()
        .add_async_node("Top", record)
        .add_subgraph("Work", child, |j: &Job| j.clone(), |_: &Job, j: Job| j)
        .add_parallel_group(
            "Gather",
            ParallelBranches::new().async_branch("Acme", branch_record),
            |s: &Job, _| s.clone(),
        )
        .add_edge("Top", "Work")
        .add_edge("Work", "Gather")
        .compile()
        .unwrap();

    graph
        .run_with("t", job(), store(), with(&[("tenant", "acme")]))
        .await;

    let seen = seen.lock().unwrap();
    assert_eq!(
        seen.iter()
            .map(|(path, _)| path.as_str())
            .collect::<Vec<_>>(),
        ["Top", "Work/Inner", "Gather/Acme"]
    );
    assert!(
        seen.iter()
            .all(|(_, metadata)| metadata["tenant"] == "acme")
    );
}

#[test]
fn a_hand_built_context_has_empty_metadata() {
    let context = NodeContext::new("t", "n");

    assert!(context.metadata().is_empty());
    assert_eq!(context.run_id(), "");
    assert!(context.span().is_none());
}

// --- The shared cross-language fixtures --------------------------------------

#[tokio::test]
async fn freezing_with_metadata_matches_the_shared_fixture_at_every_level() {
    let store = store();

    parent(review_child(false, Seen::default()))
        .run_with(
            "thread_metadata_demo",
            order(5),
            store.clone(),
            with(&[("tenant", "acme"), ("incident", "INC-284891")]),
        )
        .await;

    assert_matches_fixture(
        "metadata-nested.json",
        store.load("thread_metadata_demo").unwrap().unwrap(),
    );
}

#[tokio::test]
async fn resuming_the_shared_fixture_hands_its_metadata_to_the_resumed_run() {
    let seen = Seen::default();
    let store = store();
    let fixture = lifecycle_fixture("metadata-nested.json");
    let thread_id = fixture.thread_id.clone();
    store.save(fixture).unwrap();

    let state = completed(
        parent(review_child(false, seen.clone()))
            .resume(thread_id, approve_as("admin_01"), store.clone())
            .await,
    );

    assert_eq!(state.amount, 11);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![map(&[("tenant", "acme"), ("incident", "INC-284891")])]
    );
}

#[tokio::test]
async fn a_failed_resume_with_metadata_matches_the_shared_fixture_at_every_level() {
    let graph = parent(review_child(true, Seen::default()));
    let store = store();
    graph
        .run_with(
            "thread_metadata_failed_demo",
            order(5),
            store.clone(),
            with(&[("tenant", "acme"), ("incident", "INC-284891")]),
        )
        .await;

    graph
        .resume_with(
            "thread_metadata_failed_demo",
            approve_as("admin_01"),
            store.clone(),
            with(&[("incident", "INC-284892")]),
        )
        .await;

    assert_matches_fixture(
        "metadata-failed.json",
        store.load("thread_metadata_failed_demo").unwrap().unwrap(),
    );
}

#[tokio::test]
async fn resuming_the_shared_failed_fixture_completes_with_its_metadata() {
    let seen = Seen::default();
    let store = store();
    let fixture = lifecycle_fixture("metadata-failed.json");
    let thread_id = fixture.thread_id.clone();
    store.save(fixture).unwrap();

    let state = completed(
        parent(review_child(false, seen.clone()))
            .resume(thread_id, no_signal(), store.clone())
            .await,
    );

    assert_eq!(
        state.log,
        ["prepared", "draft", "approve", "send", "finished"]
    );
    // The engine's keys stay behind: only the caller's are carried forward.
    assert_eq!(
        *seen.lock().unwrap(),
        vec![map(&[("tenant", "acme"), ("incident", "INC-284892")])]
    );
}
