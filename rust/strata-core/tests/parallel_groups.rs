//! Parallel fan-out/fan-in (`design/14-Parallel-Execution.md`), exercised
//! through the public API only. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/ParallelGroupTests.cs` — keep the two in step.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use datalium_strata::BoxFuture;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::streaming::StreamChunkType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Report {
    seed: i32,
    results: Vec<String>,
    notes: Vec<String>,
}

fn report() -> Report {
    Report {
        seed: 7,
        results: Vec::new(),
        notes: Vec::new(),
    }
}

/// A branch that records `label` into `results` after `delay_ms`.
fn labelled(
    label: &'static str,
    delay_ms: u64,
) -> impl Fn(&Report) -> Result<Report, NodeError> + Send + Sync + 'static {
    move |s: &Report| {
        sleep(Duration::from_millis(delay_ms));
        let mut next = s.clone();
        next.results.push(label.to_string());
        Ok(next)
    }
}

/// Concatenates every branch's `results`, in the order the reducer is given
/// them, onto the entry state.
fn concat(base: &Report, outputs: Vec<Report>) -> Report {
    let mut merged = base.clone();
    for output in outputs {
        merged.results.extend(output.results);
    }
    merged
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

/// `Prepare` -> `Gather` (the parallel group) -> `Finish`.
fn graph_around(
    branches: ParallelBranches<Report>,
    policy: Option<RecoveryPolicy>,
) -> Result<CompiledGraph<Report>, StrataError> {
    let mut graph = StateGraph::<Report>::new()
        .add_node("Prepare", |mut s: Report| {
            s.notes.push("prepared".to_string());
            s
        })
        .add_parallel_group("Gather", branches, concat)
        .add_node("Finish", |mut s: Report| {
            s.notes.push("finished".to_string());
            s
        })
        .add_edge("Prepare", "Gather")
        .add_edge("Gather", "Finish");
    if let Some(policy) = policy {
        // A fallback node has no ordinary edge into it, so it's only added
        // when a policy actually routes there (otherwise it'd be an orphan).
        if policy.fallback.is_some() {
            graph = graph.add_node("Fallback", |mut s: Report| {
                s.notes.push("fell back".to_string());
                s
            });
        }
        graph = graph.set_recovery_policy("Gather", policy);
    }
    graph.compile()
}

fn completed(result: GraphExecutionResult<Report>) -> Report {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => panic!("expected Completed, got Failed({err:?})"),
        GraphExecutionResult::Interrupted { .. } => panic!("expected Completed, got Interrupted"),
        GraphExecutionResult::Cancelled { node, .. } => {
            panic!("expected Completed, got Cancelled at {node}")
        }
    }
}

#[tokio::test]
async fn branches_run_concurrently_not_one_after_another() {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let mut branches = ParallelBranches::<Report>::new();
    for label in ["a", "b", "c"] {
        let (in_flight, peak) = (in_flight.clone(), peak.clone());
        branches = branches.branch(label, move |s: &Report| {
            let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            sleep(Duration::from_millis(200));
            in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(s.clone())
        });
    }
    let graph = graph_around(branches, None).unwrap();

    completed(
        graph
            .run("t", report(), Arc::new(TestStore::default()))
            .await,
    );

    // Run one after another the peak would be 1.
    assert_eq!(peak.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn results_reach_the_reducer_in_declaration_order_not_completion_order() {
    let finished = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let mut branches = ParallelBranches::<Report>::new();
    // Declared slowest-first, so they complete in the reverse order.
    for (label, delay_ms) in [("slow", 240), ("medium", 120), ("fast", 10)] {
        let finished = finished.clone();
        let inner = labelled(label, delay_ms);
        branches = branches.branch(label, move |s: &Report| {
            let out = inner(s);
            finished.lock().unwrap().push(label);
            out
        });
    }
    let graph = graph_around(branches, None).unwrap();

    let state = completed(
        graph
            .run("t", report(), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(*finished.lock().unwrap(), vec!["fast", "medium", "slow"]);
    assert_eq!(state.results, vec!["slow", "medium", "fast"]);
}

#[tokio::test]
async fn every_branch_starts_from_the_entry_state_and_the_reducer_sees_it_as_the_base() {
    let branches = ParallelBranches::<Report>::new()
        .branch("a", |s: &Report| {
            // Branches can't see each other's work: both start from `Prepare`'s output.
            assert_eq!(s.notes, vec!["prepared"]);
            let mut next = s.clone();
            next.results.push("a".to_string());
            Ok(next)
        })
        .branch("b", |s: &Report| {
            assert_eq!(s.notes, vec!["prepared"]);
            let mut next = s.clone();
            next.results.push("b".to_string());
            Ok(next)
        });
    let graph = StateGraph::<Report>::new()
        .add_node("Prepare", |mut s: Report| {
            s.notes.push("prepared".to_string());
            s
        })
        .add_parallel_group("Gather", branches, |base: &Report, outputs: Vec<Report>| {
            let mut merged = base.clone();
            merged.notes.push(format!("base seed {}", base.seed));
            for output in outputs {
                merged.results.extend(output.results);
            }
            merged
        })
        .add_edge("Prepare", "Gather")
        .compile()
        .unwrap();

    let state = completed(
        graph
            .run("t", report(), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(state.results, vec!["a", "b"]);
    assert_eq!(state.notes, vec!["prepared", "base seed 7"]);
}

#[tokio::test]
async fn the_merged_state_flows_on_through_ordinary_edges() {
    let branches = ParallelBranches::new()
        .branch("a", labelled("a", 0))
        .branch("b", labelled("b", 0));
    let graph = graph_around(branches, None).unwrap();

    let state = completed(
        graph
            .run("t", report(), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(state.results, vec!["a", "b"]);
    assert_eq!(state.notes, vec!["prepared", "finished"]);
}

#[tokio::test]
async fn a_failing_branch_cancels_its_siblings_and_fails_the_group_at_once() {
    let tokens = Arc::new(Mutex::new(Vec::new()));
    let given = tokens.clone();
    let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let sync_done = Arc::new(AtomicU32::new(0));
    let (blocking_gate, blocking_done) = (gate.clone(), sync_done.clone());
    let branches = ParallelBranches::<Report>::new()
        .branch("broken", |_: &Report| {
            Err(NodeError::Fatal("card declined".to_string()))
        })
        .async_branch("waiting", move |s: Report, ctx| {
            given.lock().unwrap().push(ctx.cancellation().clone());
            async move {
                std::future::pending::<()>().await;
                Ok(s)
            }
        })
        .branch("blocking", move |s: &Report| {
            let (lock, released) = &*blocking_gate;
            let open = lock.lock().unwrap();
            let _open = released
                .wait_timeout_while(open, Duration::from_secs(10), |open| !*open)
                .unwrap();
            blocking_done.fetch_add(1, Ordering::SeqCst);
            Ok(s.clone())
        });
    let graph = graph_around(branches, None).unwrap();
    let store = Arc::new(TestStore::default());

    match graph.run("thread_failed", report(), store.clone()).await {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => {
            assert_eq!(node, "Gather");
            assert_eq!(attempts, 1);
            // The cancelled siblings are not failures.
            assert_eq!(message, "branch 'broken': card declined");
        }
        _ => panic!("expected Failed(NodeFailed)"),
    }

    // The async sibling was told; the sync one can't be, and the group didn't wait for it.
    let tokens = tokens.lock().unwrap();
    assert!(!tokens.is_empty() && tokens.iter().all(|t| t.is_cancelled()));
    assert_eq!(sync_done.load(Ordering::SeqCst), 0);
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();

    let persisted = store.load("thread_failed").unwrap().unwrap();
    assert_eq!(persisted.status, CheckpointStatus::Failed);
    assert_eq!(persisted.suspended_node.as_deref(), Some("Gather"));
    // The last committed state — after `Prepare`, before any branch's work.
    assert_eq!(
        persisted.frozen_state,
        serde_json::json!({ "seed": 7, "results": [], "notes": ["prepared"] })
    );
    assert_eq!(persisted.metadata.unwrap()["error_kind"], "fatal");
}

#[tokio::test]
async fn a_fatal_branch_makes_the_group_fatal_and_a_sibling_it_cancelled_is_not_named() {
    let branches = ParallelBranches::<Report>::new()
        .async_branch("flaky", |_: Report, _| async {
            // Cancelled by `broken`'s failure long before it gets here.
            tokio::time::sleep(Duration::from_secs(10)).await;
            Err(NodeError::Transient("timeout".to_string()))
        })
        .branch("broken", |_: &Report| {
            Err(NodeError::Fatal("bad input".to_string()))
        })
        .branch("fine", labelled("fine", 0));
    // Would retry five times if the group were treated as transient.
    let graph = graph_around(branches, Some(RecoveryPolicy::retry(5))).unwrap();
    let store = Arc::new(TestStore::default());

    match graph.run("t", report(), store.clone()).await {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            attempts, message, ..
        }) => {
            assert_eq!(attempts, 1);
            assert_eq!(message, "branch 'broken': bad input");
        }
        _ => panic!("expected Failed(NodeFailed)"),
    }
}

#[tokio::test]
async fn a_transient_failure_retries_the_whole_group() {
    let calls = Arc::new(AtomicU32::new(0));
    let flaky_calls = calls.clone();
    let steady_calls = Arc::new(AtomicU32::new(0));
    let steady = steady_calls.clone();
    let branches = ParallelBranches::<Report>::new()
        .branch("flaky", move |s: &Report| {
            if flaky_calls.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(NodeError::Transient("timeout".to_string()))
            } else {
                let mut next = s.clone();
                next.results.push("flaky".to_string());
                Ok(next)
            }
        })
        .branch("steady", move |s: &Report| {
            steady.fetch_add(1, Ordering::SeqCst);
            let mut next = s.clone();
            next.results.push("steady".to_string());
            Ok(next)
        });
    let graph = graph_around(branches, Some(RecoveryPolicy::retry(3))).unwrap();

    let state = completed(
        graph
            .run("t", report(), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(state.results, vec!["flaky", "steady"]);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    // The healthy branch is re-run on every group attempt that gets to start
    // it: when `flaky` fails first, the group cancels its siblings, and a sync
    // branch still queued for the blocking pool is then never run at all
    // (`design/17-Async-Execution.md`). The last attempt always runs it.
    let steady = steady_calls.load(Ordering::SeqCst);
    assert!((1..=3).contains(&steady), "steady ran {steady} times");
}

#[tokio::test]
async fn an_unrecoverable_group_routes_to_the_fallback_on_the_pre_group_state() {
    let branches = ParallelBranches::<Report>::new()
        .branch("ok", labelled("ok", 0))
        .branch("down", |_: &Report| {
            Err(NodeError::Transient("outage".to_string()))
        });
    let graph = graph_around(
        branches,
        Some(RecoveryPolicy::retry(1).with_fallback("Fallback")),
    )
    .unwrap();

    let state = completed(
        graph
            .run("t", report(), Arc::new(TestStore::default()))
            .await,
    );

    // `ok`'s result was discarded with the failed group; `Fallback` saw `Prepare`'s state.
    assert!(state.results.is_empty());
    assert_eq!(state.notes, vec!["prepared", "fell back"]);
}

#[tokio::test]
async fn a_group_can_be_an_interrupt_boundary_and_freezes_the_merged_state() {
    let branches = ParallelBranches::new()
        .branch("a", labelled("a", 0))
        .branch("b", labelled("b", 0));
    let graph = StateGraph::<Report>::new()
        .add_parallel_group("Gather", branches, concat)
        .add_interrupt_boundary("Gather")
        .add_node("Finish", |mut s: Report| {
            s.notes.push("finished".to_string());
            s
        })
        .add_edge("Gather", "Finish")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());

    match graph.run("t", report(), store.clone()).await {
        GraphExecutionResult::Interrupted {
            suspended_node,
            next_node,
            frozen_state,
            ..
        } => {
            assert_eq!(suspended_node, "Gather");
            assert_eq!(next_node, "Finish");
            assert_eq!(frozen_state.results, vec!["a", "b"]);
        }
        _ => panic!("expected Interrupted"),
    }

    let state = completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );
    assert_eq!(state.results, vec!["a", "b"]);
    assert_eq!(state.notes, vec!["finished"]);
}

#[tokio::test]
async fn run_streaming_reports_the_group_as_one_node() {
    let branches = ParallelBranches::new()
        .branch("a", labelled("a", 0))
        .branch("b", labelled("b", 0));
    let graph = Arc::new(graph_around(branches, None).unwrap());
    let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());

    let (handle, mut receiver) = graph.run_streaming("t", report(), store);
    let mut chunks = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push((chunk.chunk_type, chunk.node_name.to_string()));
    }

    assert_eq!(completed(handle.await.unwrap()).results, vec!["a", "b"]);
    let gather: Vec<_> = chunks.iter().filter(|(_, node)| node == "Gather").collect();
    assert_eq!(
        gather,
        vec![
            &(StreamChunkType::NodeStart, "Gather".to_string()),
            &(StreamChunkType::NodeEnd, "Gather".to_string()),
        ]
    );
}

#[tokio::test]
async fn a_panicking_branch_unwinds_through_the_engine_like_a_plain_node() {
    let branches = ParallelBranches::<Report>::new()
        .branch("ok", labelled("ok", 0))
        .branch("bug", |_: &Report| panic!("branch bug"));
    let graph = graph_around(branches, Some(RecoveryPolicy::retry(3))).unwrap();

    let outcome = tokio::spawn(async move {
        graph
            .run("t", report(), Arc::new(TestStore::default()))
            .await
    })
    .await;

    assert!(
        outcome.is_err_and(|error| error.is_panic()),
        "the panic must not be swallowed or retried"
    );
}

#[test]
fn compile_rejects_a_group_with_no_branches() {
    let result = StateGraph::<Report>::new()
        .add_parallel_group("Gather", ParallelBranches::new(), concat)
        .compile();

    match result {
        Err(StrataError::CompilationFailed(message)) => {
            assert!(message.contains("'Gather'"), "message was: {message}");
            assert!(message.contains("no branches"), "message was: {message}");
        }
        _ => panic!("expected CompilationFailed"),
    }
}

#[test]
fn compile_rejects_duplicate_branch_names() {
    let branches = ParallelBranches::new()
        .branch("a", labelled("a", 0))
        .branch("a", labelled("a", 0));
    let result = StateGraph::<Report>::new()
        .add_parallel_group("Gather", branches, concat)
        .compile();

    match result {
        Err(StrataError::CompilationFailed(message)) => {
            assert!(message.contains("'a'"), "message was: {message}");
        }
        _ => panic!("expected CompilationFailed"),
    }
}

#[test]
fn re_adding_a_name_replaces_a_group_and_its_compile_checks() {
    // An empty group would be rejected — but it was replaced by a plain node.
    let result = StateGraph::<Report>::new()
        .add_parallel_group("Gather", ParallelBranches::new(), concat)
        .add_node("Gather", |s: Report| s)
        .compile();

    assert!(result.is_ok());
}
