//! Cancellation, node timeouts and run deadlines (`design/17-Async-Execution.md`,
//! SG-03): a cancelled run (token or deadline) ends `Cancelled` with a
//! `cancelled` checkpoint that a resume re-enters; a node timeout is a
//! transient failure; the engine never waits for a handler it gave up on;
//! cancellation reaches groups and subgraphs at any depth. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/CancellationTests.cs` — keep the two in
//! step.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use datalium_strata::checkpoint::{
    CancelReason, CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::streaming::{DEFAULT_CHANNEL_CAPACITY, StreamChunkType};
use datalium_strata::{CancellationToken, StrataError};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Doc {
    steps: Vec<String>,
    note: Option<String>,
}

fn doc() -> Doc {
    Doc {
        steps: Vec::new(),
        note: None,
    }
}

fn did(step: &'static str) -> impl Fn(Doc) -> Doc + Send + Sync + 'static {
    move |mut doc| {
        doc.steps.push(step.to_string());
        doc
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
    fn save(
        &self,
        checkpoint: Checkpoint,
    ) -> datalium_strata::BoxFuture<'_, Result<(), CheckpointStoreError>> {
        Box::pin(std::future::ready(TestStore::save(self, checkpoint)))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> datalium_strata::BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(TestStore::load(self, thread_id)))
    }
}

fn completed(result: GraphExecutionResult<Doc>) -> Doc {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => panic!("expected Completed, got Failed({err:?})"),
        GraphExecutionResult::Interrupted { .. } => panic!("expected Completed, got Interrupted"),
        GraphExecutionResult::Cancelled { node, .. } => {
            panic!("expected Completed, got Cancelled at {node}")
        }
    }
}

struct Cancelled {
    thread_id: String,
    node: String,
    reason: CancelReason,
    state: Doc,
    stopped_at: Vec<String>,
}

fn cancelled(result: GraphExecutionResult<Doc>) -> Cancelled {
    match result {
        GraphExecutionResult::Cancelled {
            thread_id,
            node,
            reason,
            state,
            stopped_at,
        } => Cancelled {
            thread_id,
            node,
            reason,
            state,
            stopped_at,
        },
        GraphExecutionResult::Completed(_) => panic!("expected Cancelled, got Completed"),
        GraphExecutionResult::Interrupted { .. } => panic!("expected Cancelled, got Interrupted"),
        GraphExecutionResult::Failed(err) => panic!("expected Cancelled, got Failed({err:?})"),
    }
}

/// `(node, attempts, message)` of a `Failed(NodeFailed)`.
fn node_failed(result: GraphExecutionResult<Doc>) -> (String, u32, String) {
    match result {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => (node, attempts, message),
        _ => panic!("expected Failed(NodeFailed)"),
    }
}

fn signal(note: Option<&str>) -> HumanSignal {
    HumanSignal {
        is_approved: true,
        reviewer_token: "x".to_string(),
        field_overrides: note
            .map(|note| HashMap::from([("note".to_string(), note.to_string())]))
            .unwrap_or_default(),
        ..Default::default()
    }
}

fn cancel_with(token: &CancellationToken) -> RunConfig {
    RunConfig::new().with_cancellation(token.clone())
}

fn already_cancelled() -> RunConfig {
    let token = CancellationToken::new();
    token.cancel();
    cancel_with(&token)
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// An async handler that waits until it is cancelled (its future dropped) — or,
/// once released, records its step and returns. Says when it started, and keeps
/// every token it was given so a test can check they were cancelled.
#[derive(Clone)]
struct Hang {
    step: &'static str,
    calls: Arc<AtomicU32>,
    started: Arc<Notify>,
    tokens: Arc<Mutex<Vec<CancellationToken>>>,
    released: Arc<AtomicBool>,
}

impl Hang {
    fn new(step: &'static str) -> Self {
        Self {
            step,
            calls: Arc::default(),
            started: Arc::default(),
            tokens: Arc::default(),
            released: Arc::default(),
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    async fn started(&self) {
        tokio::time::timeout(PATIENCE, self.started.notified())
            .await
            .expect("the handler never started");
    }

    /// Every token this handler was given has been cancelled.
    fn saw_cancel(&self) -> bool {
        let tokens = self.tokens.lock().unwrap();
        !tokens.is_empty() && tokens.iter().all(CancellationToken::is_cancelled)
    }

    async fn wait(self, mut doc: Doc, ctx: NodeContext) -> Doc {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.released.load(Ordering::SeqCst) {
            doc.steps.push(self.step.to_string());
            return doc;
        }
        self.tokens.lock().unwrap().push(ctx.cancellation().clone());
        self.started.notify_one();
        std::future::pending().await
    }

    fn node(&self) -> impl Fn(Doc, NodeContext) -> BoxFuture<Doc> + Send + Sync + 'static {
        let hang = self.clone();
        move |doc, ctx| Box::pin(hang.clone().wait(doc, ctx))
    }

    fn fallible(
        &self,
    ) -> impl Fn(Doc, NodeContext) -> BoxFuture<Result<Doc, NodeError>> + Send + Sync + 'static
    {
        let hang = self.clone();
        move |doc, ctx| {
            let hang = hang.clone();
            Box::pin(async move { Ok(hang.wait(doc, ctx).await) })
        }
    }
}

/// A gate a sync handler blocks on until the test opens it.
#[derive(Clone, Default)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);

impl Gate {
    fn wait(&self) {
        let (lock, opened) = &*self.0;
        let open = lock.lock().unwrap();
        let _open = opened
            .wait_timeout_while(open, PATIENCE, |open| !*open)
            .unwrap();
    }

    fn open(&self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all();
    }
}

/// `Prepare` -> `Slow` -> `Finish`, with `Slow` added by `add_slow`.
fn around(
    add_slow: impl FnOnce(StateGraph<Doc>) -> StateGraph<Doc>,
    prepares: Option<Arc<AtomicU32>>,
) -> StateGraph<Doc> {
    let prepare = did("prepare");
    add_slow(
        StateGraph::<Doc>::new().add_node("Prepare", move |doc: Doc| {
            if let Some(prepares) = &prepares {
                prepares.fetch_add(1, Ordering::SeqCst);
            }
            prepare(doc)
        }),
    )
    .add_node("Finish", did("finish"))
    .add_edge("Prepare", "Slow")
    .add_edge("Slow", "Finish")
}

/// Awaits `future`, failing the test instead of hanging if it never ends.
async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(PATIENCE, future)
        .await
        .expect("the run should have ended")
}

fn store() -> Arc<TestStore> {
    Arc::new(TestStore::default())
}

/// Runs `graph` on a task, so the test can cancel it while it runs.
fn spawn_run(
    graph: &Arc<CompiledGraph<Doc>>,
    store: &Arc<TestStore>,
    config: RunConfig,
) -> tokio::task::JoinHandle<GraphExecutionResult<Doc>> {
    let (graph, store) = (graph.clone(), store.clone());
    tokio::spawn(async move { within(graph.run_with("t", doc(), store, config)).await })
}

#[tokio::test]
async fn cancelling_during_a_node_persists_a_cancelled_checkpoint_at_that_node() {
    let slow = Hang::new("slow");
    let graph = Arc::new(
        around(|g| g.add_async_node("Slow", slow.node()), None)
            .compile()
            .unwrap(),
    );
    let store = store();
    let token = CancellationToken::new();

    let run = {
        let (graph, store) = (graph.clone(), store.clone());
        let config = cancel_with(&token);
        tokio::spawn(
            async move { within(graph.run_with("thread_cancel", doc(), store, config)).await },
        )
    };
    slow.started().await;
    token.cancel();
    let cancelled = cancelled(within(run).await.unwrap());

    assert_eq!(
        (
            cancelled.thread_id.as_str(),
            cancelled.node.as_str(),
            cancelled.reason
        ),
        ("thread_cancel", "Slow", CancelReason::Requested)
    );
    assert_eq!(cancelled.state.steps, vec!["prepare"]);
    assert_eq!(cancelled.stopped_at, vec!["Slow"]);
    assert!(slow.saw_cancel());

    let checkpoint = store.load("thread_cancel").unwrap().unwrap();
    assert_eq!(checkpoint.status, CheckpointStatus::Cancelled);
    assert_eq!(checkpoint.schema_version, "1.2.0");
    assert_eq!(
        (
            checkpoint.suspended_node.as_deref(),
            checkpoint.next_node.as_deref()
        ),
        (Some("Slow"), Some("Slow"))
    );
    assert_eq!(
        checkpoint.frozen_state,
        serde_json::json!({ "steps": ["prepare"], "note": null })
    );
    assert_eq!(
        checkpoint.metadata,
        Some(HashMap::from([(
            "cancel_reason".to_string(),
            "requested".to_string()
        )]))
    );
    assert!(checkpoint.nested.is_none());
}

#[tokio::test]
async fn a_run_started_already_cancelled_runs_nothing() {
    let prepares = Arc::new(AtomicU32::new(0));
    let graph = around(|g| g.add_node("Slow", did("slow")), Some(prepares.clone()))
        .compile()
        .unwrap();
    let store = store();

    let cancelled =
        cancelled(within(graph.run_with("t", doc(), store.clone(), already_cancelled())).await);

    assert_eq!(cancelled.node, "Prepare");
    assert!(cancelled.state.steps.is_empty());
    assert_eq!(prepares.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.load("t").unwrap().unwrap().next_node.as_deref(),
        Some("Prepare")
    );
}

#[tokio::test]
async fn cancelling_between_nodes_stops_before_the_next_one_with_the_committed_state() {
    let token = CancellationToken::new();
    let cancel = token.clone();
    // Edge conditions run once the node has completed: the cancellation lands between nodes.
    let graph = StateGraph::<Doc>::new()
        .add_node("Prepare", did("prepare"))
        .add_node("Finish", did("finish"))
        .add_conditional_edge("Prepare", "Finish", move |_: &Doc| {
            cancel.cancel();
            true
        })
        .compile()
        .unwrap();

    let cancelled =
        cancelled(within(graph.run_with("t", doc(), store(), cancel_with(&token))).await);

    assert_eq!(cancelled.node, "Finish");
    assert_eq!(cancelled.state.steps, vec!["prepare"]);
}

#[tokio::test]
async fn the_deadline_ends_the_run_as_cancelled_with_reason_deadline() {
    let slow = Hang::new("slow");
    let graph = around(|g| g.add_async_node("Slow", slow.node()), None)
        .compile()
        .unwrap();
    let store = store();
    let config = RunConfig::new().with_deadline(Instant::now() + Duration::from_millis(150));

    let cancelled = cancelled(within(graph.run_with("t", doc(), store.clone(), config)).await);

    assert_eq!(
        (cancelled.node.as_str(), cancelled.reason),
        ("Slow", CancelReason::Deadline)
    );
    assert_eq!(
        store.load("t").unwrap().unwrap().metadata.unwrap()["cancel_reason"],
        "deadline"
    );
    assert!(slow.saw_cancel());
}

#[tokio::test]
async fn a_deadline_already_passed_runs_nothing() {
    let graph = around(|g| g.add_node("Slow", did("slow")), None)
        .compile()
        .unwrap();
    let config = RunConfig::new().with_deadline(Instant::now() - Duration::from_secs(1));

    let cancelled = cancelled(within(graph.run_with("t", doc(), store(), config)).await);

    assert_eq!(
        (cancelled.node.as_str(), cancelled.reason),
        ("Prepare", CancelReason::Deadline)
    );
}

#[tokio::test]
async fn a_sync_node_is_abandoned_not_waited_for() {
    let gate = Gate::default();
    let started = Arc::new(Notify::new());
    let finished = Arc::new(AtomicBool::new(false));
    let (blocking_gate, blocking_started, blocking_finished) =
        (gate.clone(), started.clone(), finished.clone());
    let graph = Arc::new(
        around(
            move |g| {
                g.add_node("Slow", move |doc: Doc| {
                    blocking_started.notify_one();
                    blocking_gate.wait();
                    blocking_finished.store(true, Ordering::SeqCst);
                    doc
                })
            },
            None,
        )
        .compile()
        .unwrap(),
    );
    let token = CancellationToken::new();

    let run = spawn_run(&graph, &store(), cancel_with(&token));
    tokio::time::timeout(PATIENCE, started.notified())
        .await
        .unwrap();
    token.cancel();

    let result = tokio::time::timeout(PATIENCE, run).await.unwrap().unwrap();
    assert_eq!(cancelled(result).node, "Slow");
    // The run is over while the handler is still blocked.
    assert!(!finished.load(Ordering::SeqCst));
    gate.open();
}

#[tokio::test]
async fn cancellation_is_not_a_failure_nothing_is_retried_and_no_fallback_runs() {
    let slow = Hang::new("slow");
    let graph = Arc::new(
        around(
            |g| {
                g.add_async_fallible_node("Slow", slow.fallible())
                    .add_node("Fallback", did("fallback"))
                    .set_recovery_policy("Slow", RecoveryPolicy::retry(3).with_fallback("Fallback"))
            },
            None,
        )
        .compile()
        .unwrap(),
    );
    let token = CancellationToken::new();

    let run = spawn_run(&graph, &store(), cancel_with(&token));
    slow.started().await;
    token.cancel();
    let cancelled = cancelled(within(run).await.unwrap());

    assert_eq!(cancelled.node, "Slow");
    assert_eq!(slow.calls(), 1);
    assert!(!cancelled.state.steps.contains(&"fallback".to_string()));
}

#[tokio::test]
async fn a_timeout_is_a_transient_failure_that_is_retried() {
    let first = Hang::new("unused");
    let first_attempt = first.clone();
    let calls = Arc::new(AtomicU32::new(0));
    let counted = calls.clone();
    let graph = around(
        move |g| {
            g.add_async_fallible_node("Slow", move |mut doc: Doc, ctx: NodeContext| {
                counted.fetch_add(1, Ordering::SeqCst);
                let first_attempt = first_attempt.clone();
                async move {
                    if ctx.attempt() == 1 {
                        first_attempt.wait(doc, ctx).await;
                        unreachable!("the first attempt only ends by being dropped");
                    }
                    doc.steps.push("slow".to_string());
                    Ok(doc)
                }
            })
            .set_timeout("Slow", Duration::from_millis(100))
            .set_recovery_policy("Slow", RecoveryPolicy::retry(1))
        },
        None,
    )
    .compile()
    .unwrap();

    let doc = completed(within(graph.run("t", doc(), store())).await);

    assert_eq!(doc.steps, vec!["prepare", "slow", "finish"]);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(first.saw_cancel());
}

#[tokio::test]
async fn an_exhausted_timeout_fails_even_a_plain_node_with_a_failed_checkpoint() {
    let slow = Hang::new("slow");
    let graph = around(
        |g| {
            g.add_async_node("Slow", slow.node())
                .set_timeout("Slow", Duration::from_millis(50))
        },
        None,
    )
    .compile()
    .unwrap();
    let store = store();

    let (node, attempts, message) = node_failed(within(graph.run("t", doc(), store.clone())).await);

    assert_eq!(
        (node.as_str(), attempts, message.as_str()),
        ("Slow", 1, "node 'Slow' timed out after 50 ms")
    );
    let checkpoint = store.load("t").unwrap().unwrap();
    assert_eq!(
        (checkpoint.status, checkpoint.schema_version.as_str()),
        (CheckpointStatus::Failed, "1.0.0")
    );
    assert_eq!(checkpoint.metadata.unwrap()["error_kind"], "transient");
}

#[tokio::test]
async fn a_timeout_routes_to_the_fallback_with_the_pre_failure_state() {
    let slow = Hang::new("slow");
    let graph = around(
        |g| {
            g.add_async_fallible_node("Slow", slow.fallible())
                .add_node("Fallback", did("fallback"))
                .set_timeout("Slow", Duration::from_millis(50))
                .set_recovery_policy("Slow", RecoveryPolicy::default().with_fallback("Fallback"))
        },
        None,
    )
    .compile()
    .unwrap();

    let doc = completed(within(graph.run("t", doc(), store())).await);

    assert_eq!(doc.steps, vec!["prepare", "fallback"]);
}

#[tokio::test]
async fn a_timeout_abandons_a_sync_node() {
    let gate = Gate::default();
    let blocking_gate = gate.clone();
    let graph = around(
        move |g| {
            g.add_node("Slow", move |doc: Doc| {
                blocking_gate.wait();
                doc
            })
            .set_timeout("Slow", Duration::from_millis(50))
        },
        None,
    )
    .compile()
    .unwrap();

    let result = tokio::time::timeout(PATIENCE, graph.run("t", doc(), store()))
        .await
        .unwrap();

    assert_eq!(node_failed(result).2, "node 'Slow' timed out after 50 ms");
    gate.open();
}

#[test]
fn a_timeout_must_be_positive_and_name_a_node() {
    let zero = StateGraph::<Doc>::new()
        .add_node("A", did("a"))
        .set_timeout("A", Duration::ZERO)
        .compile();
    assert!(
        matches!(&zero, Err(StrataError::CompilationFailed(m)) if m == "timeout for 'A' must be greater than zero")
    );

    let missing = StateGraph::<Doc>::new()
        .add_node("A", did("a"))
        .set_timeout("Missing", Duration::from_secs(1))
        .compile();
    assert!(
        matches!(&missing, Err(StrataError::CompilationFailed(m)) if m == "timeout set for 'Missing', which does not exist")
    );
}

#[tokio::test]
async fn cancelling_a_group_cancels_every_branch() {
    let acme = Hang::new("acme");
    let globex = Hang::new("globex");
    let branches = ParallelBranches::<Doc>::new()
        .async_branch("Acme", acme.fallible())
        .async_branch("Globex", globex.fallible());
    let graph = Arc::new(
        around(
            |g| g.add_parallel_group("Slow", branches, |entry: &Doc, _| entry.clone()),
            None,
        )
        .compile()
        .unwrap(),
    );
    let token = CancellationToken::new();

    let run = spawn_run(&graph, &store(), cancel_with(&token));
    acme.started().await;
    globex.started().await;
    token.cancel();

    assert_eq!(cancelled(within(run).await.unwrap()).node, "Slow");
    assert!(acme.saw_cancel() && globex.saw_cancel());
}

#[tokio::test]
async fn a_timeout_on_a_group_covers_the_whole_group() {
    let acme = Hang::new("acme");
    let branches = ParallelBranches::<Doc>::new()
        .async_branch("Acme", acme.fallible())
        .branch("Quick", |doc: &Doc| Ok(doc.clone()));
    let graph = around(
        |g| {
            g.add_parallel_group("Slow", branches, |entry: &Doc, _| entry.clone())
                .set_timeout("Slow", Duration::from_millis(50))
        },
        None,
    )
    .compile()
    .unwrap();

    let (_, _, message) = node_failed(within(graph.run("t", doc(), store())).await);

    assert_eq!(message, "node 'Slow' timed out after 50 ms");
    assert!(acme.saw_cancel());
}

#[tokio::test]
async fn a_cancelled_thread_resumes_once_at_the_node_it_stopped() {
    let prepares = Arc::new(AtomicU32::new(0));
    let slow = Hang::new("slow");
    let graph = Arc::new(
        around(
            |g| g.add_async_node("Slow", slow.node()),
            Some(prepares.clone()),
        )
        .compile()
        .unwrap(),
    );
    let store = store();
    let token = CancellationToken::new();

    let run = spawn_run(&graph, &store, cancel_with(&token));
    slow.started().await;
    token.cancel();
    cancelled(within(run).await.unwrap());

    slow.release();
    let doc = completed(within(graph.resume("t", signal(Some("fixed")), store.clone())).await);

    assert_eq!(doc.steps, vec!["prepare", "slow", "finish"]);
    assert_eq!(doc.note.as_deref(), Some("fixed"));
    assert_eq!(prepares.load(Ordering::SeqCst), 1);

    match within(graph.resume("t", signal(None), store.clone())).await {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(message.contains("already resumed"), "{message}");
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }
}

/// `Draft` -> `Send`, a child whose `Send` is `send`.
fn child(send: &Hang, drafts: Arc<AtomicU32>) -> CompiledGraph<Doc> {
    let draft = did("draft");
    StateGraph::<Doc>::new()
        .add_node("Draft", move |doc: Doc| {
            drafts.fetch_add(1, Ordering::SeqCst);
            draft(doc)
        })
        .add_async_node("Send", send.node())
        .add_edge("Draft", "Send")
        .compile()
        .unwrap()
}

/// `Prepare` -> `Work` (the subgraph) -> `Finish`.
fn parent(child: CompiledGraph<Doc>, inputs: Arc<AtomicU32>) -> StateGraph<Doc> {
    StateGraph::<Doc>::new()
        .add_node("Prepare", did("prepare"))
        .add_subgraph(
            "Work",
            child,
            move |doc: &Doc| {
                inputs.fetch_add(1, Ordering::SeqCst);
                Doc {
                    steps: Vec::new(),
                    note: doc.note.clone(),
                }
            },
            |entry: &Doc, result: Doc| {
                let mut merged = entry.clone();
                merged.steps.extend(result.steps);
                merged.note = result.note;
                merged
            },
        )
        .add_node("Finish", did("finish"))
        .add_edge("Prepare", "Work")
        .add_edge("Work", "Finish")
}

#[tokio::test]
async fn cancelled_inside_a_subgraph_nests_the_childs_checkpoint_and_resumes_there() {
    let drafts = Arc::new(AtomicU32::new(0));
    let inputs = Arc::new(AtomicU32::new(0));
    let send = Hang::new("send");
    let graph = Arc::new(
        parent(child(&send, drafts.clone()), inputs.clone())
            .compile()
            .unwrap(),
    );
    let store = store();
    let token = CancellationToken::new();

    let run = spawn_run(&graph, &store, cancel_with(&token));
    send.started().await;
    token.cancel();
    let cancelled = cancelled(within(run).await.unwrap());

    assert_eq!(cancelled.node, "Work");
    assert_eq!(cancelled.stopped_at, vec!["Work", "Send"]);
    let checkpoint = store.load("t").unwrap().unwrap();
    assert_eq!(
        (
            checkpoint.status,
            checkpoint.schema_version.as_str(),
            checkpoint.suspended_node.as_deref(),
            checkpoint.next_node.as_deref()
        ),
        (
            CheckpointStatus::Cancelled,
            "1.2.0",
            Some("Work"),
            Some("Work")
        )
    );
    assert_eq!(
        checkpoint.frozen_state,
        serde_json::json!({ "steps": ["prepare"], "note": null })
    );
    let nested = checkpoint.nested.unwrap();
    assert_eq!(
        (
            nested.status,
            nested.schema_version.as_str(),
            nested.thread_id.as_str(),
            nested.suspended_node.as_deref(),
            nested.next_node.as_deref()
        ),
        (
            CheckpointStatus::Cancelled,
            "1.2.0",
            "Work",
            Some("Send"),
            Some("Send")
        )
    );
    assert_eq!(
        nested.frozen_state,
        serde_json::json!({ "steps": ["draft"], "note": null })
    );
    assert_eq!(nested.metadata.unwrap()["cancel_reason"], "requested");

    send.release();
    let doc = completed(within(graph.resume("t", signal(None), store.clone())).await);

    assert_eq!(doc.steps, vec!["prepare", "draft", "send", "finish"]);
    assert_eq!(
        (drafts.load(Ordering::SeqCst), inputs.load(Ordering::SeqCst)),
        (1, 1)
    );
}

#[tokio::test]
async fn cancellation_reaches_a_grandchild() {
    let send = Hang::new("send");
    let middle = StateGraph::<Doc>::new()
        .add_subgraph(
            "Inner",
            child(&send, Arc::default()),
            |doc: &Doc| doc.clone(),
            |_: &Doc, result| result,
        )
        .compile()
        .unwrap();
    let graph = Arc::new(parent(middle, Arc::default()).compile().unwrap());
    let store = store();
    let token = CancellationToken::new();

    let run = spawn_run(&graph, &store, cancel_with(&token));
    send.started().await;
    token.cancel();

    assert_eq!(
        cancelled(within(run).await.unwrap()).stopped_at,
        vec!["Work", "Inner", "Send"]
    );
    assert_eq!(
        store.load("t").unwrap().unwrap().waiting_at(),
        vec!["Work", "Inner", "Send"]
    );
    assert!(send.saw_cancel());
}

#[tokio::test]
async fn a_subgraph_nodes_timeout_fails_it_and_nests_where_the_child_stopped() {
    let drafts = Arc::new(AtomicU32::new(0));
    let send = Hang::new("send");
    let graph = parent(child(&send, drafts.clone()), Arc::default())
        .set_timeout("Work", Duration::from_millis(100))
        .compile()
        .unwrap();
    let store = store();

    let (node, _, message) = node_failed(within(graph.run("t", doc(), store.clone())).await);

    assert_eq!(
        (node.as_str(), message.as_str()),
        ("Work", "node 'Work' timed out after 100 ms")
    );
    let checkpoint = store.load("t").unwrap().unwrap();
    assert_eq!(
        (checkpoint.status, checkpoint.schema_version.as_str()),
        (CheckpointStatus::Failed, "1.2.0")
    );
    assert_eq!(checkpoint.metadata.unwrap()["error_kind"], "transient");
    let nested = checkpoint.nested.unwrap();
    assert_eq!(
        (nested.status, nested.suspended_node.as_deref()),
        (CheckpointStatus::Cancelled, Some("Send"))
    );
    assert_eq!(nested.metadata.unwrap()["cancel_reason"], "timeout");

    // A resume re-enters the child where it was stopped: Draft doesn't run again.
    send.release();
    let doc = completed(within(graph.resume("t", signal(None), store.clone())).await);
    assert_eq!(doc.steps, vec!["prepare", "draft", "send", "finish"]);
    assert_eq!(drafts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_resume_started_cancelled_keeps_the_humans_answer_in_a_nested_thread() {
    let drafts = Arc::new(AtomicU32::new(0));
    let counted = drafts.clone();
    let draft = did("draft");
    let child = StateGraph::<Doc>::new()
        .add_node("Draft", move |doc: Doc| {
            counted.fetch_add(1, Ordering::SeqCst);
            draft(doc)
        })
        .add_node("Approve", did("approve"))
        .add_interrupt_boundary("Approve")
        .add_node("Send", did("send"))
        .add_edge("Draft", "Approve")
        .add_edge("Approve", "Send")
        .compile()
        .unwrap();
    let graph = parent(child, Arc::default()).compile().unwrap();
    let store = store();
    assert!(matches!(
        within(graph.run("t", doc(), store.clone())).await,
        GraphExecutionResult::Interrupted { .. }
    ));

    let cancelled = cancelled(
        within(graph.resume_with(
            "t",
            signal(Some("admin")),
            store.clone(),
            already_cancelled(),
        ))
        .await,
    );

    assert_eq!(cancelled.stopped_at, vec!["Work", "Send"]);
    assert_eq!(
        store
            .load("t")
            .unwrap()
            .unwrap()
            .nested
            .unwrap()
            .frozen_state["note"],
        "admin"
    );

    let doc = completed(within(graph.resume("t", signal(None), store.clone())).await);
    assert_eq!(
        doc.steps,
        vec!["prepare", "draft", "approve", "send", "finish"]
    );
    assert_eq!(doc.note.as_deref(), Some("admin"));
    assert_eq!(drafts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_cancelled_stream_ends_with_no_node_end_for_the_stopped_node() {
    let slow = Hang::new("slow");
    let graph = Arc::new(
        around(|g| g.add_async_node("Slow", slow.node()), None)
            .compile()
            .unwrap(),
    );
    let token = CancellationToken::new();

    let (handle, mut receiver) = graph.run_streaming_with("t", doc(), store(), cancel_with(&token));
    let drained = tokio::spawn(async move {
        let mut chunks = Vec::new();
        while let Some(chunk) = receiver.recv().await {
            chunks.push((chunk.chunk_type, chunk.node_name.to_string()));
        }
        chunks
    });
    slow.started().await;
    token.cancel();

    assert_eq!(cancelled(within(handle).await.unwrap()).node, "Slow");
    let chunks = tokio::time::timeout(PATIENCE, drained)
        .await
        .expect("the stream should end with the run")
        .unwrap();
    assert_eq!(
        chunks,
        vec![
            (StreamChunkType::NodeStart, "Prepare".to_string()),
            (StreamChunkType::NodeEnd, "Prepare".to_string()),
            (StreamChunkType::NodeStart, "Slow".to_string()),
        ]
    );
}

#[tokio::test]
async fn a_cancelled_run_doesnt_wait_on_a_full_stream_to_send_its_own_chunks() {
    let returned = Arc::new(Notify::new());
    let signal = returned.clone();
    let graph = Arc::new(
        around(
            move |g| {
                g.add_async_node("Slow", move |doc: Doc, ctx: NodeContext| {
                    let signal = signal.clone();
                    Box::pin(async move {
                        // Prepare's NodeStart and NodeEnd and this node's
                        // NodeStart are already in: fill the rest.
                        let stream = ctx.stream().unwrap();
                        for _ in 0..DEFAULT_CHANNEL_CAPACITY - 3 {
                            stream.emit_token("token").await.unwrap();
                        }
                        signal.notify_one();
                        did("slow")(doc)
                    }) as BoxFuture<Doc>
                })
            },
            None,
        )
        .compile()
        .unwrap(),
    );
    let token = CancellationToken::new();

    // The consumer keeps the receiver but stopped reading, then cancelled:
    // the node's NodeEnd has no room.
    let (handle, _receiver) = graph.run_streaming_with("t", doc(), store(), cancel_with(&token));
    within(returned.notified()).await;
    token.cancel();

    let cancelled = cancelled(within(handle).await.unwrap());
    assert_eq!(cancelled.node, "Finish");
    assert_eq!(cancelled.state.steps, vec!["prepare", "slow"]);
}

// --- The shared cross-language fixtures ---------------------------------------
//
// `schema/fixtures/checkpoint-lifecycle/cancelled.json` and
// `nested-cancelled.json` are the interop artifacts for a cancelled run, flat
// and inside a subgraph. These prove the engine produces a checkpoint matching
// each (timestamps aside, at every level) from a live cancelled run, and resumes
// each to the expected final state;
// `dotnet/tests/Datalium.Strata.Tests/CancelledCheckpointFixtureParityTests.cs`
// proves the same in C# against the identical files.

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Payment {
    prepared: bool,
    charged: bool,
    notes: Vec<String>,
}

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

fn lifecycle_fixture_raw(name: &str) -> String {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/checkpoint-lifecycle")
        .join(name);
    std::fs::read_to_string(path).unwrap()
}

fn lifecycle_fixture(name: &str) -> Checkpoint {
    serde_json::from_str(&lifecycle_fixture_raw(name)).unwrap()
}

/// Blanks every level's `created_at`: the fixture's is a fixed value, a live
/// run's the real time.
fn without_timestamps(mut checkpoint: Checkpoint) -> Checkpoint {
    checkpoint.created_at = String::new();
    checkpoint.nested = checkpoint
        .nested
        .map(|child| Box::new(without_timestamps(*child)));
    checkpoint
}

/// An async handler that signals it started and then waits until it is
/// cancelled, or (when `working`) returns `done`'s result.
fn hang_unless<T: Send + 'static>(
    working: bool,
    done: fn(T) -> T,
    started: Arc<Notify>,
) -> impl Fn(T, NodeContext) -> BoxFuture<T> + Send + Sync + 'static {
    move |state, _| {
        let started = started.clone();
        Box::pin(async move {
            if working {
                return done(state);
            }
            started.notify_one();
            std::future::pending().await
        })
    }
}

/// `Prepare` -> `Charge`: what `cancelled.json` records, cancelled at `Charge`.
fn payment_graph(charge_works: bool, started: Arc<Notify>) -> Arc<CompiledGraph<Payment>> {
    Arc::new(
        StateGraph::<Payment>::new()
            .add_node("Prepare", |mut p: Payment| {
                p.prepared = true;
                p.notes.push("prepared".to_string());
                p
            })
            .add_async_node(
                "Charge",
                hang_unless(
                    charge_works,
                    |mut p: Payment| {
                        p.charged = true;
                        p.notes.push("charged".to_string());
                        p
                    },
                    started,
                ),
            )
            .add_edge("Prepare", "Charge")
            .compile()
            .unwrap(),
    )
}

/// `Prepare` -> `Work` (child: `Draft` -> `Send`) -> `Finish`: what
/// `nested-cancelled.json` records, cancelled at `Send`. The shapes are those
/// of `nested.json`'s graph (`nested_interrupts.rs`), without the boundary.
fn order_graph(send_works: bool, started: Arc<Notify>) -> Arc<CompiledGraph<Order>> {
    let child = StateGraph::<Job>::new()
        .add_node("Draft", |mut j: Job| {
            j.n *= 2;
            j.trail.push("draft".to_string());
            j
        })
        .add_async_node(
            "Send",
            hang_unless(
                send_works,
                |mut j: Job| {
                    j.n += 1;
                    j.trail.push("send".to_string());
                    j
                },
                started,
            ),
        )
        .add_edge("Draft", "Send")
        .compile()
        .unwrap();
    Arc::new(
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
            .unwrap(),
    )
}

#[tokio::test]
async fn cancelling_produces_a_checkpoint_matching_the_shared_fixture() {
    let started = Arc::new(Notify::new());
    let graph = payment_graph(false, started.clone());
    let store = store();
    let token = CancellationToken::new();

    let run = {
        let (graph, store, config) = (graph.clone(), store.clone(), cancel_with(&token));
        let initial = Payment {
            prepared: false,
            charged: false,
            notes: Vec::new(),
        };
        tokio::spawn(async move {
            graph
                .run_with("thread_cancelled_demo", initial, store, config)
                .await
        })
    };
    within(started.notified()).await;
    token.cancel();

    assert!(matches!(
        within(run).await.unwrap(),
        GraphExecutionResult::Cancelled { .. }
    ));
    assert_eq!(
        without_timestamps(store.load("thread_cancelled_demo").unwrap().unwrap()),
        without_timestamps(lifecycle_fixture("cancelled.json"))
    );
}

#[tokio::test]
async fn resuming_the_shared_fixture_re_enters_the_node_that_was_stopped() {
    let graph = payment_graph(true, Arc::default());
    let store = store();
    let fixture = lifecycle_fixture("cancelled.json");
    let thread_id = fixture.thread_id.clone();
    store.save(fixture).unwrap();

    match within(graph.resume(&thread_id, signal(None), store.clone())).await {
        GraphExecutionResult::Completed(state) => {
            // `Prepare` did not run again: its note is the fixture's.
            assert!(state.prepared && state.charged);
            assert_eq!(state.notes, ["prepared", "charged"]);
        }
        _ => panic!("expected Completed"),
    }
}

#[tokio::test]
async fn cancelling_inside_a_subgraph_matches_the_shared_nested_fixture_at_every_level() {
    let started = Arc::new(Notify::new());
    let graph = order_graph(false, started.clone());
    let store = store();
    let token = CancellationToken::new();

    let run = {
        let (graph, store, config) = (graph.clone(), store.clone(), cancel_with(&token));
        let initial = Order {
            amount: 5,
            log: Vec::new(),
            approved_by: None,
        };
        tokio::spawn(async move {
            graph
                .run_with("thread_nested_cancelled_demo", initial, store, config)
                .await
        })
    };
    within(started.notified()).await;
    token.cancel();

    match within(run).await.unwrap() {
        GraphExecutionResult::Cancelled { stopped_at, .. } => {
            assert_eq!(stopped_at, vec!["Work", "Send"]);
        }
        _ => panic!("expected Cancelled"),
    }
    assert_eq!(
        without_timestamps(store.load("thread_nested_cancelled_demo").unwrap().unwrap()),
        without_timestamps(lifecycle_fixture("nested-cancelled.json"))
    );
}

#[tokio::test]
async fn resuming_the_shared_nested_fixture_re_enters_the_child_where_it_was_stopped() {
    let graph = order_graph(true, Arc::default());
    let store = store();
    let fixture = lifecycle_fixture("nested-cancelled.json");
    let thread_id = fixture.thread_id.clone();
    store.save(fixture).unwrap();

    let approve = HumanSignal {
        is_approved: true,
        reviewer_token: "x".to_string(),
        field_overrides: HashMap::from([("approved_by".to_string(), "admin_01".to_string())]),
        ..Default::default()
    };
    match within(graph.resume(&thread_id, approve, store.clone())).await {
        GraphExecutionResult::Completed(state) => {
            // Draft did not run again (n is 10 from the fixture, +1 by Send),
            // and the override reached the child.
            assert_eq!(state.amount, 11);
            assert_eq!(state.log, ["prepared", "draft", "send", "finished"]);
            assert_eq!(state.approved_by.as_deref(), Some("admin_01"));
        }
        _ => panic!("expected Completed"),
    }
}

/// A node with no timeout gets its token only if its handler asks for one
/// (`design/38-Rust-Executor-Overhead.md`). It is still a token of the node's
/// own, not the run's: a handler that cancels it stops what it handed the
/// token to, and nothing else.
#[tokio::test]
async fn a_handlers_token_is_its_own_and_cancelling_it_does_not_cancel_the_run() {
    let graph = StateGraph::<Doc>::new()
        .add_async_node("Ask", |mut doc: Doc, ctx: NodeContext| async move {
            ctx.cancellation().cancel();
            doc.steps.push("asked".to_string());
            doc
        })
        .add_async_node("After", |mut doc: Doc, ctx: NodeContext| async move {
            assert!(!ctx.cancellation().is_cancelled());
            doc.steps.push("after".to_string());
            doc
        })
        .add_edge("Ask", "After")
        .compile()
        .unwrap();

    match within(graph.run("t", doc(), store())).await {
        GraphExecutionResult::Completed(state) => assert_eq!(state.steps, ["asked", "after"]),
        _ => panic!("expected Completed"),
    }
}

#[test]
fn the_shared_fixtures_round_trip_through_the_schema_types() {
    for name in ["cancelled.json", "nested-cancelled.json"] {
        let raw: serde_json::Value = serde_json::from_str(&lifecycle_fixture_raw(name)).unwrap();
        let checkpoint: Checkpoint = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(checkpoint.status, CheckpointStatus::Cancelled);
        assert_eq!(serde_json::to_value(&checkpoint).unwrap(), raw, "{name}");
    }
}
