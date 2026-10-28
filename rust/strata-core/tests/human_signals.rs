//! Typed human signals, recorded decisions and the reject route
//! (`design/21-Typed-Human-Signals.md`, SG-17). Mirrors
//! `dotnet/tests/Datalium.Strata.Tests/HumanSignalTests.cs`; keep the two in
//! step. Both produce and resume the shared fixtures
//! `checkpoint-lifecycle/decision-failed.json` and `nested-decision.json`, and
//! both export `graph-export/reject-route.json`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::checkpoint::{
    CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal, LevelSignals,
};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::{
    Checkpoint, CheckpointStatus, DECISION_CHECKPOINT_SCHEMA_VERSION, HumanDecision,
};
use datalium_strata::validate::validate;
use datalium_strata::{BoxFuture, CancellationToken, StrataError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Plan {
    steps: Vec<String>,
}

/// The flat graph's state; the fixture's `frozen_state` has exactly these fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Remediation {
    plan: Option<Plan>,
    replicas: i32,
    outcome: Option<String>,
}

/// The nested fixture's parent state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Incident {
    replicas: i32,
    log: Vec<String>,
    outcome: Option<String>,
}

/// The nested fixture's child state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Change {
    replicas: i32,
    trail: Vec<String>,
    reason: Option<String>,
}

/// For the scalar overrides.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Settings {
    count: i32,
    dry_run: bool,
    note: Option<String>,
}

#[derive(Default)]
struct TestStore {
    checkpoints: Mutex<HashMap<String, Checkpoint>>,
}

impl TestStore {
    fn put(&self, checkpoint: Checkpoint) {
        self.checkpoints
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
    }

    fn get(&self, thread_id: &str) -> Checkpoint {
        self.checkpoints
            .lock()
            .unwrap()
            .get(thread_id)
            .cloned()
            .expect("a checkpoint")
    }
}

impl CheckpointStore for TestStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.put(checkpoint);
        Box::pin(std::future::ready(Ok(())))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        let found = self.checkpoints.lock().unwrap().get(thread_id).cloned();
        Box::pin(std::future::ready(Ok(found)))
    }
}

/// One node path and the decision the node saw there.
type Sighting = (String, Option<HumanDecision>);

/// What each node saw in `NodeContext::decision`, in the order they ran.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<Sighting>>>);

impl Seen {
    fn add(&self, ctx: &NodeContext) {
        self.0
            .lock()
            .unwrap()
            .push((ctx.node_path().to_string(), ctx.decision().cloned()));
    }

    fn at(&self, path: &str) -> Option<HumanDecision> {
        self.all(path).pop().expect("the node ran")
    }

    fn all(&self, path: &str) -> Vec<Option<HumanDecision>> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p == path)
            .map(|(_, d)| d.clone())
            .collect()
    }
}

/// An async node that records what it saw, then applies `then`.
fn record<S, F>(
    seen: &Seen,
    then: F,
) -> impl Fn(S, NodeContext) -> std::future::Ready<S> + Send + Sync + 'static
where
    S: Send + 'static,
    F: Fn(S, &NodeContext) -> S + Send + Sync + 'static,
{
    let seen = seen.clone();
    move |state, ctx| {
        seen.add(&ctx);
        std::future::ready(then(state, &ctx))
    }
}

fn same<S>(state: S, _: &NodeContext) -> S {
    state
}

fn reviewer(ctx: &NodeContext) -> String {
    ctx.decision()
        .map(|d| d.reviewer.clone())
        .unwrap_or_default()
}

fn approve(reviewer: &str) -> HumanSignal {
    HumanSignal::approve(reviewer)
}

fn reject(reviewer: &str) -> HumanSignal {
    HumanSignal::reject(reviewer)
}

fn completed<S>(result: GraphExecutionResult<S>) -> S {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => panic!("expected Completed, got Failed({err:?})"),
        GraphExecutionResult::Interrupted { .. } => panic!("expected Completed, got Interrupted"),
        GraphExecutionResult::Cancelled { node, .. } => {
            panic!("expected Completed, got Cancelled at {node}")
        }
    }
}

fn interrupted<S>(result: GraphExecutionResult<S>) {
    assert!(
        matches!(result, GraphExecutionResult::Interrupted { .. }),
        "expected Interrupted"
    );
}

/// The message of a resume refused before it ran anything.
fn refused<S>(result: GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => message,
        GraphExecutionResult::Failed(err) => panic!("expected ExecutionAborted, got {err:?}"),
        _ => panic!("expected Failed"),
    }
}

fn node_failed<S>(result: GraphExecutionResult<S>) {
    assert!(
        matches!(
            result,
            GraphExecutionResult::Failed(StrataError::NodeFailed { .. })
        ),
        "expected a node failure"
    );
}

// --- The flat reference graph -------------------------------------------------
//
// Plan -> Approve (boundary, rejects to Abort) -> Apply. Apply is fallible and
// fails fatally while `failures` lasts; Abort and Apply both say who decided.

fn remediate(seen: &Seen, failures: i32, reject_route: bool) -> StateGraph<Remediation> {
    let left = Arc::new(AtomicI32::new(failures));
    let apply_seen = seen.clone();
    let graph = StateGraph::<Remediation>::new()
        .add_node("Plan", |mut s: Remediation| {
            s.plan = Some(Plan {
                steps: vec!["drain".to_string(), "restart".to_string()],
            });
            s.replicas = 2;
            s
        })
        .add_node("Approve", |s: Remediation| s)
        .add_async_fallible_node("Apply", move |mut s: Remediation, ctx: NodeContext| {
            apply_seen.add(&ctx);
            let fail = left.fetch_sub(1, Ordering::SeqCst) > 0;
            async move {
                if fail {
                    return Err(NodeError::Fatal("cluster unreachable".to_string()));
                }
                s.outcome = Some(format!("applied, approved by {}", reviewer(&ctx)));
                Ok(s)
            }
        })
        .add_async_node(
            "Abort",
            record(seen, |mut s: Remediation, ctx| {
                s.outcome = Some(format!("aborted by {}", reviewer(ctx)));
                s
            }),
        )
        .add_edge("Plan", "Approve")
        .add_edge("Approve", "Apply");
    if reject_route {
        graph.add_interrupt_boundary_with_reject("Approve", "Abort")
    } else {
        graph
            .add_interrupt_boundary("Approve")
            .add_edge("Apply", "Abort")
    }
}

fn fresh() -> Remediation {
    Remediation {
        plan: None,
        replicas: 0,
        outcome: None,
    }
}

fn approve_with_edits() -> HumanSignal {
    approve("alice@acme.io")
        .with_override("replicas", 3)
        .with_override("plan", json!({ "steps": ["drain", "rollback"] }))
}

fn settings_graph() -> CompiledGraph<Settings> {
    StateGraph::<Settings>::new()
        .add_node("Ask", |s: Settings| s)
        .add_node("Done", |s: Settings| s)
        .add_edge("Ask", "Done")
        .add_interrupt_boundary("Ask")
        .compile()
        .unwrap()
}

// --- Typed overrides ------------------------------------------------------------

#[tokio::test]
async fn typed_overrides_rehydrate_with_their_json_type() {
    let graph = remediate(&Seen::default(), 0, true).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", fresh(), store.clone()).await);

    let state = completed(graph.resume("t", approve_with_edits(), store.clone()).await);

    assert_eq!(state.replicas, 3);
    assert_eq!(state.plan.unwrap().steps, ["drain", "rollback"]);
    assert_eq!(
        state.outcome.as_deref(),
        Some("applied, approved by alice@acme.io")
    );
}

#[tokio::test]
async fn integer_boolean_string_and_null_overrides_keep_their_type() {
    let graph = settings_graph();
    let store = Arc::new(TestStore::default());
    let start = Settings {
        count: 1,
        dry_run: false,
        note: Some("keep".to_string()),
    };
    interrupted(graph.run("t", start, store.clone()).await);

    let signal = approve("alice")
        .with_override("count", 7)
        .with_override("dry_run", true)
        .with_override("note", Value::Null);
    let state = completed(graph.resume("t", signal, store.clone()).await);

    assert_eq!(
        state,
        Settings {
            count: 7,
            dry_run: true,
            note: None
        }
    );
}

#[tokio::test]
async fn a_string_override_still_writes_a_json_string() {
    let graph = settings_graph();
    let store = Arc::new(TestStore::default());
    let start = Settings {
        count: 1,
        dry_run: false,
        note: None,
    };
    interrupted(graph.run("t", start, store.clone()).await);

    let signal = approve("alice").with_field_override("note", "42");
    let state = completed(graph.resume("t", signal, store.clone()).await);

    assert_eq!(state.note.as_deref(), Some("42"));
}

#[tokio::test]
async fn a_value_can_be_reused_across_signals() {
    let graph = remediate(&Seen::default(), 0, true).compile().unwrap();
    let store = Arc::new(TestStore::default());
    let plan = json!({ "steps": ["drain"] });

    interrupted(graph.run("a", fresh(), store.clone()).await);
    interrupted(graph.run("b", fresh(), store.clone()).await);
    completed(
        graph
            .resume(
                "a",
                approve("alice").with_override("plan", plan.clone()),
                store.clone(),
            )
            .await,
    );
    let state = completed(
        graph
            .resume(
                "b",
                approve("bob").with_override("plan", plan),
                store.clone(),
            )
            .await,
    );

    assert_eq!(state.plan.unwrap().steps, ["drain"]);
}

#[tokio::test]
async fn a_key_in_both_maps_is_refused_before_the_stamp() {
    let graph = remediate(&Seen::default(), 0, true).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", fresh(), store.clone()).await);

    let twice = approve("alice")
        .with_field_override("outcome", "x")
        .with_override("outcome", "y");
    let message = refused(graph.resume("t", twice, store.clone()).await);

    assert!(
        message.contains("override 'outcome' is set as both a string and a typed value"),
        "{message}"
    );
    assert!(store.get("t").resumed_at.is_none());
    completed(graph.resume("t", approve("alice"), store.clone()).await);
}

// --- Preflight -----------------------------------------------------------------

#[tokio::test]
async fn an_override_that_does_not_fit_is_refused_and_the_thread_stays_resumable() {
    let graph = remediate(&Seen::default(), 0, true).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", fresh(), store.clone()).await);

    let message = refused(
        graph
            .resume(
                "t",
                approve("alice").with_override("replicas", "three"),
                store.clone(),
            )
            .await,
    );

    assert!(
        message.contains("failed to rehydrate state for thread 't'"),
        "{message}"
    );
    assert!(store.get("t").resumed_at.is_none());
    let state = completed(
        graph
            .resume(
                "t",
                approve("alice").with_override("replicas", 3),
                store.clone(),
            )
            .await,
    );
    assert_eq!(state.replicas, 3);
}

/// Prepare -> Work (a subgraph: Draft -> Approve (boundary, rejects to Revoke)
/// -> Apply) -> Finish.
fn incident_around(child: CompiledGraph<Change>, seen: &Seen) -> StateGraph<Incident> {
    StateGraph::<Incident>::new()
        .add_node("Prepare", |mut s: Incident| {
            s.log.push("prepared".to_string());
            s
        })
        .add_subgraph(
            "Work",
            child,
            |s: &Incident| Change {
                replicas: s.replicas,
                trail: Vec::new(),
                reason: None,
            },
            |s: &Incident, c: Change| {
                let mut next = s.clone();
                next.replicas = c.replicas;
                next.log.extend(c.trail);
                next
            },
        )
        .add_async_node(
            "Finish",
            record(seen, |mut s: Incident, _| {
                s.log.push("finished".to_string());
                s
            }),
        )
        .add_edge("Prepare", "Work")
        .add_edge("Work", "Finish")
}

fn change_child(seen: &Seen, revoke_failures: i32) -> CompiledGraph<Change> {
    let left = Arc::new(AtomicI32::new(revoke_failures));
    let revoke_seen = seen.clone();
    StateGraph::<Change>::new()
        .add_node("Draft", |mut c: Change| {
            c.trail.push("draft".to_string());
            c
        })
        .add_node("Approve", |mut c: Change| {
            c.trail.push("approve".to_string());
            c
        })
        .add_interrupt_boundary_with_reject("Approve", "Revoke")
        .add_async_node(
            "Apply",
            record(seen, |mut c: Change, _| {
                c.trail.push("apply".to_string());
                c
            }),
        )
        .add_async_fallible_node("Revoke", move |mut c: Change, ctx: NodeContext| {
            revoke_seen.add(&ctx);
            let fail = left.fetch_sub(1, Ordering::SeqCst) > 0;
            async move {
                if fail {
                    return Err(NodeError::Fatal("revocation queue full".to_string()));
                }
                c.trail.push(format!("revoke:{}", reviewer(&ctx)));
                Ok(c)
            }
        })
        .add_edge("Draft", "Approve")
        .add_edge("Approve", "Apply")
        .compile()
        .unwrap()
}

fn new_incident() -> Incident {
    Incident {
        replicas: 2,
        log: Vec::new(),
        outcome: None,
    }
}

#[tokio::test]
async fn a_bad_override_for_an_inner_level_is_refused_naming_the_level() {
    let graph = incident_around(change_child(&Seen::default(), 0), &Seen::default())
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", new_incident(), store.clone()).await);

    let message = refused(
        graph
            .resume(
                "t",
                approve("alice").with_override("trail", 5),
                store.clone(),
            )
            .await,
    );

    assert!(
        message.contains("failed to rehydrate state for thread 't' at level [Work]"),
        "{message}"
    );
    assert!(store.get("t").resumed_at.is_none());
    let state = completed(graph.resume("t", approve("alice"), store.clone()).await);
    assert_eq!(
        state.log,
        ["prepared", "draft", "approve", "apply", "finished"]
    );
}

#[tokio::test]
async fn a_bad_override_for_an_outer_level_is_refused_too() {
    let graph = incident_around(change_child(&Seen::default(), 0), &Seen::default())
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", new_incident(), store.clone()).await);

    let signals = LevelSignals::new(approve("alice"))
        .at(&[], approve("ops").with_override("log", "not a list"));
    let message = refused(graph.resume("t", signals, store.clone()).await);

    assert!(
        message.contains("failed to rehydrate state for thread 't':"),
        "{message}"
    );
    assert!(store.get("t").resumed_at.is_none());
    completed(graph.resume("t", approve("alice"), store.clone()).await);
}

// --- Decisions -----------------------------------------------------------------

#[tokio::test]
async fn a_fresh_run_has_no_decision() {
    let seen = Seen::default();
    let graph = StateGraph::<i32>::new()
        .add_async_node("Only", record(&seen, same))
        .compile()
        .unwrap();

    graph.run("t", 0, Arc::new(TestStore::default())).await;

    assert_eq!(seen.at("Only"), None);
}

#[tokio::test]
async fn every_node_and_branch_after_the_boundary_sees_the_decision() {
    let seen = Seen::default();
    let left = seen.clone();
    let right = seen.clone();
    let graph = StateGraph::<i32>::new()
        .add_async_node("Before", record(&seen, same))
        .add_node("Approve", |s: i32| s)
        .add_interrupt_boundary("Approve")
        .add_async_node("Next", record(&seen, same))
        .add_parallel_group(
            "Fan",
            ParallelBranches::new()
                .async_branch("Left", move |s: i32, ctx: NodeContext| {
                    left.add(&ctx);
                    std::future::ready(Ok(s))
                })
                .async_branch("Right", move |s: i32, ctx: NodeContext| {
                    right.add(&ctx);
                    std::future::ready(Ok(s))
                }),
            |s: &i32, _| *s,
        )
        .add_async_node("Last", record(&seen, same))
        .add_edge("Before", "Approve")
        .add_edge("Approve", "Next")
        .add_edge("Next", "Fan")
        .add_edge("Fan", "Last")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", 0, store.clone()).await);

    completed(
        graph
            .resume(
                "t",
                approve("alice").with_override("note", "ok"),
                store.clone(),
            )
            .await,
    );

    assert_eq!(seen.at("Before"), None);
    let decision = seen.at("Next").unwrap();
    assert_eq!(decision.boundary, "Approve");
    assert!(decision.is_approved);
    assert_eq!(decision.reviewer, "alice");
    assert_eq!(decision.overrides["note"], "ok");
    assert!(!decision.decided_at.is_empty());
    assert_eq!(seen.at("Fan/Left"), Some(decision.clone()));
    assert_eq!(seen.at("Fan/Right"), Some(decision.clone()));
    assert_eq!(seen.at("Last"), Some(decision));
}

#[tokio::test]
async fn a_fresh_child_does_not_inherit_its_parents_decision() {
    let seen = Seen::default();
    let child = StateGraph::<i32>::new()
        .add_async_node("Inside", record(&seen, same))
        .compile()
        .unwrap();
    let graph = StateGraph::<i32>::new()
        .add_node("Approve", |s: i32| s)
        .add_interrupt_boundary("Approve")
        .add_subgraph("Work", child, |s: &i32| *s, |_: &i32, c: i32| c)
        .add_async_node("After", record(&seen, same))
        .add_edge("Approve", "Work")
        .add_edge("Work", "After")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", 0, store.clone()).await);

    completed(graph.resume("t", approve("alice"), store.clone()).await);

    assert_eq!(seen.at("Work/Inside"), None);
    assert_eq!(seen.at("After").unwrap().reviewer, "alice");
}

#[tokio::test]
async fn the_innermost_level_gets_the_decision_and_the_outer_level_does_not() {
    let seen = Seen::default();
    let graph = incident_around(change_child(&seen, 0), &seen)
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", new_incident(), store.clone()).await);

    completed(graph.resume("t", approve("alice"), store.clone()).await);

    assert_eq!(seen.at("Work/Apply").unwrap().boundary, "Approve");
    assert_eq!(seen.at("Finish"), None);
}

#[tokio::test]
async fn an_outer_level_keeps_its_decision_across_a_childs_freeze() {
    let seen = Seen::default();
    let child = StateGraph::<i32>::new()
        .add_node("Check", |s: i32| s)
        .add_interrupt_boundary("Check")
        .add_async_node("Fix", record(&seen, same))
        .add_edge("Check", "Fix")
        .compile()
        .unwrap();
    let graph = StateGraph::<i32>::new()
        .add_node("Approve", |s: i32| s)
        .add_interrupt_boundary("Approve")
        .add_subgraph("Work", child, |s: &i32| *s, |_: &i32, c: i32| c)
        .add_async_node("Execute", record(&seen, same))
        .add_edge("Approve", "Work")
        .add_edge("Work", "Execute")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", 0, store.clone()).await);
    interrupted(graph.resume("t", approve("alice"), store.clone()).await);

    // The parent's checkpoint around the child carries the parent's decision.
    let frozen = store.get("t");
    assert_eq!(frozen.decision.as_ref().unwrap().reviewer, "alice");
    assert!(frozen.nested.as_ref().unwrap().decision.is_none());

    completed(graph.resume("t", approve("bob"), store.clone()).await);

    assert_eq!(seen.at("Work/Fix").unwrap().reviewer, "bob");
    assert_eq!(seen.at("Work/Fix").unwrap().boundary, "Check");
    assert_eq!(seen.at("Execute").unwrap().reviewer, "alice");
    assert_eq!(seen.at("Execute").unwrap().boundary, "Approve");
}

#[tokio::test]
async fn answering_a_second_boundary_replaces_the_decision() {
    let seen = Seen::default();
    let graph = StateGraph::<i32>::new()
        .add_node("First", |s: i32| s)
        .add_async_node("Middle", record(&seen, same))
        .add_node("Second", |s: i32| s)
        .add_async_node("End", record(&seen, same))
        .add_edge("First", "Middle")
        .add_edge("Middle", "Second")
        .add_edge("Second", "End")
        .add_interrupt_boundary("First")
        .add_interrupt_boundary("Second")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", 0, store.clone()).await);
    interrupted(graph.resume("t", approve("alice"), store.clone()).await);

    // The second freeze carries the decision in effect when it was written.
    let frozen = store.get("t");
    assert_eq!(frozen.decision.as_ref().unwrap().boundary, "First");
    assert_eq!(frozen.schema_version, DECISION_CHECKPOINT_SCHEMA_VERSION);

    completed(graph.resume("t", approve("bob"), store.clone()).await);

    assert_eq!(seen.at("Middle").unwrap().reviewer, "alice");
    assert_eq!(seen.at("End").unwrap().reviewer, "bob");
    assert_eq!(seen.at("End").unwrap().boundary, "Second");
}

#[tokio::test]
async fn every_retry_of_a_resumed_subgraph_sees_the_same_decision() {
    let seen = Seen::default();
    let flaky_seen = seen.clone();
    let calls = Arc::new(AtomicI32::new(0));
    let child = StateGraph::<i32>::new()
        .add_node("Check", |s: i32| s)
        .add_interrupt_boundary("Check")
        .add_async_fallible_node("Flaky", move |s: i32, ctx: NodeContext| {
            flaky_seen.add(&ctx);
            let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
            async move {
                if first {
                    Err(NodeError::Transient("blip".to_string()))
                } else {
                    Ok(s)
                }
            }
        })
        .add_edge("Check", "Flaky")
        .compile()
        .unwrap();
    let graph = StateGraph::<i32>::new()
        .add_subgraph("Work", child, |s: &i32| *s, |_: &i32, c: i32| c)
        .set_recovery_policy("Work", RecoveryPolicy::retry(1))
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", 0, store.clone()).await);

    completed(graph.resume("t", approve("alice"), store.clone()).await);

    let decisions = seen.all("Work/Flaky");
    assert_eq!(decisions.len(), 2);
    assert_eq!(decisions[0], decisions[1]);
}

// --- Persistence ---------------------------------------------------------------

/// A config with a fresh channel observer, and the receiver its events arrive on.
fn observed() -> (RunConfig, mpsc::UnboundedReceiver<RunEvent>) {
    let (observer, events) = RunEventChannel::new();
    (RunConfig::new().with_observer(observer), events)
}

/// Every event the channel delivers, until it ends after `RunCompleted`.
async fn drain(mut events: mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEventKind> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .unwrap()
    {
        all.push(event.kind);
    }
    all
}

/// The single `Resumed` event's `(is_approved, reviewer, rejected_to)`, and the
/// node `RunStarted` names.
fn resumed(events: &[RunEventKind]) -> (bool, String, Option<String>, String) {
    let started = events
        .iter()
        .find_map(|e| match e {
            RunEventKind::RunStarted { node, .. } => Some(node.clone()),
            _ => None,
        })
        .unwrap();
    let mut resumed = events.iter().filter_map(|e| match e {
        RunEventKind::Resumed {
            is_approved,
            reviewer,
            rejected_to,
            ..
        } => Some((*is_approved, reviewer.clone(), rejected_to.clone())),
        _ => None,
    });
    let (is_approved, reviewer, rejected_to) = resumed.next().unwrap();
    assert!(resumed.next().is_none());
    (is_approved, reviewer, rejected_to, started)
}

#[tokio::test]
async fn a_failure_after_an_approval_carries_the_decision_and_a_retry_keeps_it() {
    let seen = Seen::default();
    let graph = remediate(&seen, 1, true).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", fresh(), store.clone()).await);
    node_failed(graph.resume("t", approve_with_edits(), store.clone()).await);

    let failed = store.get("t");
    assert_eq!(failed.status, CheckpointStatus::Failed);
    assert_eq!(failed.schema_version, DECISION_CHECKPOINT_SCHEMA_VERSION);
    assert_eq!(failed.decision.as_ref().unwrap().reviewer, "alice@acme.io");

    // The operator retries: Apply runs again and still sees Alice's approval.
    let (config, events) = observed();
    let state = completed(
        graph
            .resume_with("t", approve("bob@ops"), store.clone(), config)
            .await,
    );

    assert_eq!(
        state.outcome.as_deref(),
        Some("applied, approved by alice@acme.io")
    );
    assert_eq!(seen.at("Apply").unwrap().reviewer, "alice@acme.io");
    let (_, reviewer, rejected_to, _) = resumed(&drain(events).await);
    assert_eq!(reviewer, "bob@ops");
    assert_eq!(rejected_to, None);
}

#[tokio::test]
async fn a_cancelled_run_after_an_approval_carries_the_decision() {
    let seen = Seen::default();
    let slow_seen = seen.clone();
    let started = Arc::new(Notify::new());
    let block = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let (on_start, blocking) = (started.clone(), block.clone());
    let graph = StateGraph::<i32>::new()
        .add_node("Approve", |s: i32| s)
        .add_interrupt_boundary("Approve")
        .add_async_node("Slow", move |s: i32, ctx: NodeContext| {
            slow_seen.add(&ctx);
            let (on_start, blocking) = (on_start.clone(), blocking.clone());
            async move {
                if blocking.load(Ordering::SeqCst) {
                    on_start.notify_one();
                    std::future::pending::<()>().await;
                }
                s
            }
        })
        .add_edge("Approve", "Slow")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", 0, store.clone()).await);

    let token = CancellationToken::new();
    let (result, _) = tokio::join!(
        graph.resume_with(
            "t",
            approve("alice"),
            store.clone(),
            RunConfig::new().with_cancellation(token.clone())
        ),
        async {
            started.notified().await;
            token.cancel();
        }
    );
    assert!(matches!(result, GraphExecutionResult::Cancelled { .. }));

    let cancelled = store.get("t");
    assert_eq!(cancelled.status, CheckpointStatus::Cancelled);
    assert_eq!(cancelled.schema_version, DECISION_CHECKPOINT_SCHEMA_VERSION);
    assert_eq!(cancelled.decision.as_ref().unwrap().reviewer, "alice");

    block.store(false, Ordering::SeqCst);
    completed(graph.resume("t", approve("bob"), store.clone()).await);
    assert_eq!(seen.at("Slow").unwrap().reviewer, "alice");
}

// --- The reject route ------------------------------------------------------------

#[tokio::test]
async fn a_rejection_continues_at_the_reject_route() {
    let seen = Seen::default();
    let graph = remediate(&seen, 0, true).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", fresh(), store.clone()).await);

    let state = completed(graph.resume("t", reject("bob"), store.clone()).await);

    assert_eq!(state.outcome.as_deref(), Some("aborted by bob"));
    assert!(!seen.at("Abort").unwrap().is_approved);
    assert!(seen.all("Apply").is_empty());
}

#[tokio::test]
async fn an_approval_continues_at_the_next_node() {
    let seen = Seen::default();
    let graph = remediate(&seen, 0, true).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", fresh(), store.clone()).await);

    let state = completed(graph.resume("t", approve("alice"), store.clone()).await);

    assert_eq!(state.outcome.as_deref(), Some("applied, approved by alice"));
    assert!(seen.all("Abort").is_empty());
}

#[tokio::test]
async fn a_rejection_without_a_route_continues_at_the_next_node() {
    let seen = Seen::default();
    let graph = remediate(&seen, 0, false).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", fresh(), store.clone()).await);

    completed(graph.resume("t", reject("bob"), store.clone()).await);

    assert!(!seen.at("Apply").unwrap().is_approved);
}

#[tokio::test]
async fn overrides_apply_on_a_rejection() {
    let graph = remediate(&Seen::default(), 0, true).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", fresh(), store.clone()).await);

    let state = completed(
        graph
            .resume(
                "t",
                reject("bob").with_override("replicas", 9),
                store.clone(),
            )
            .await,
    );

    assert_eq!(state.replicas, 9);
    assert_eq!(state.outcome.as_deref(), Some("aborted by bob"));
}

#[tokio::test]
async fn a_rejection_inside_a_child_routes_inside_the_child() {
    let seen = Seen::default();
    let graph = incident_around(change_child(&seen, 0), &seen)
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", new_incident(), store.clone()).await);

    let state = completed(graph.resume("t", reject("bob"), store.clone()).await);

    assert_eq!(
        state.log,
        ["prepared", "draft", "approve", "revoke:bob", "finished"]
    );
}

#[tokio::test]
async fn a_subgraph_node_that_is_a_boundary_takes_its_route_once_its_child_completes() {
    let seen = Seen::default();
    let child = StateGraph::<i32>::new()
        .add_node("Inside", |s: i32| s + 1)
        .compile()
        .unwrap();
    let graph = StateGraph::<i32>::new()
        .add_subgraph("Work", child, |s: &i32| *s, |_: &i32, c: i32| c)
        .add_interrupt_boundary_with_reject("Work", "Undo")
        .add_async_node("Ship", record(&seen, same))
        .add_async_node("Undo", record(&seen, same))
        .add_edge("Work", "Ship")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", 0, store.clone()).await);
    assert!(store.get("t").nested.is_none());

    completed(graph.resume("t", reject("bob"), store.clone()).await);

    assert_eq!(seen.at("Undo").unwrap().boundary, "Work");
    assert!(seen.all("Ship").is_empty());
}

#[tokio::test]
async fn a_failed_checkpoint_ignores_is_approved() {
    let seen = Seen::default();
    let graph = remediate(&seen, 1, true).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", fresh(), store.clone()).await);
    graph.resume("t", approve("alice"), store.clone()).await;

    let state = completed(graph.resume("t", reject("bob"), store.clone()).await);

    assert_eq!(state.outcome.as_deref(), Some("applied, approved by alice"));
    assert!(seen.all("Abort").is_empty());
}

#[tokio::test]
async fn a_boundary_node_that_failed_is_re_entered_even_on_a_rejection() {
    // The boundary itself failed, so its failed checkpoint is suspended at the
    // boundary: a resume re-runs it rather than taking its reject route, and it
    // freezes for the human again.
    let seen = Seen::default();
    let calls = Arc::new(AtomicI32::new(0));
    let counted = calls.clone();
    let graph = StateGraph::<i32>::new()
        .add_fallible_node("Approve", move |s: &i32| {
            if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(NodeError::Fatal("flaky".to_string()))
            } else {
                Ok(*s)
            }
        })
        .add_interrupt_boundary_with_reject("Approve", "Abort")
        .add_node("Apply", |s: i32| s)
        .add_async_node("Abort", record(&seen, same))
        .add_edge("Approve", "Apply")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    node_failed(graph.run("t", 0, store.clone()).await);
    assert_eq!(store.get("t").suspended_node.as_deref(), Some("Approve"));

    interrupted(graph.resume("t", reject("bob"), store.clone()).await);

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(seen.all("Abort").is_empty());
    assert!(store.get("t").decision.is_none());
}

#[tokio::test]
async fn reject_and_revise_loops_back_to_the_boundary() {
    let seen = Seen::default();
    let graph = StateGraph::<i32>::new()
        .add_node("Draft", |s: i32| s + 1)
        .add_node("Approve", |s: i32| s)
        .add_interrupt_boundary_with_reject("Approve", "Revise")
        .add_node("Revise", |s: i32| s + 10)
        .add_async_node("Publish", record(&seen, same))
        .add_edge("Draft", "Approve")
        .add_edge("Approve", "Publish")
        .add_edge("Revise", "Approve")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", 0, store.clone()).await);

    interrupted(graph.resume("t", reject("bob"), store.clone()).await);
    assert_eq!(store.get("t").decision.unwrap().reviewer, "bob");
    let state = completed(graph.resume("t", approve("alice"), store.clone()).await);

    assert_eq!(state, 11);
    assert_eq!(seen.at("Publish").unwrap().reviewer, "alice");
}

#[tokio::test]
async fn a_boundary_with_no_outgoing_edge_refuses_an_approval_and_routes_a_rejection() {
    let seen = Seen::default();
    let graph = StateGraph::<i32>::new()
        .add_node("Last", |s: i32| s)
        .add_interrupt_boundary_with_reject("Last", "Undo")
        .add_async_node("Undo", record(&seen, same))
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", 0, store.clone()).await);

    let message = refused(graph.resume("t", approve("alice"), store.clone()).await);
    assert!(message.contains("to resume from"), "{message}");
    assert!(store.get("t").resumed_at.is_none());

    completed(graph.resume("t", reject("bob"), store.clone()).await);
    assert_eq!(seen.at("Undo").unwrap().reviewer, "bob");
}

// --- Validation and export --------------------------------------------------------

fn fixture(relative: &str) -> Value {
    let path = format!(
        "{}/../../schema/fixtures/{relative}",
        env!("CARGO_MANIFEST_DIR")
    );
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&raw).unwrap()
}

#[test]
fn the_export_matches_the_shared_reject_route_fixture() {
    let exported =
        serde_json::to_value(remediate(&Seen::default(), 0, true).to_definition()).unwrap();

    assert_eq!(exported, fixture("graph-export/reject-route.json"));
}

#[test]
fn reject_edges_follow_fallback_edges_in_node_declaration_order() {
    let graph = StateGraph::<i32>::new()
        .add_fallible_node("A", |s: &i32| Ok::<i32, NodeError>(*s))
        .add_node("B", |s: i32| s)
        .add_node("C", |s: i32| s)
        .add_node("Rescue", |s: i32| s)
        .add_node("NoB", |s: i32| s)
        .add_node("NoC", |s: i32| s)
        .add_edge("A", "B")
        .add_edge("B", "C")
        .add_interrupt_boundary_with_reject("C", "NoC")
        .add_interrupt_boundary_with_reject("B", "NoB")
        .set_recovery_policy("A", RecoveryPolicy::default().with_fallback("Rescue"));

    let edges = graph.to_definition().edges;

    let described: Vec<String> = edges
        .iter()
        .map(|e| {
            format!(
                "{}->{} {}",
                e.from,
                e.to,
                e.condition.as_deref().unwrap_or_default()
            )
        })
        .collect();
    assert_eq!(
        described,
        [
            "A->B ",
            "B->C ",
            "A->Rescue on_failure",
            "B->NoB on_reject",
            "C->NoC on_reject"
        ]
    );
    assert!(
        edges
            .iter()
            .filter(|e| e.condition.is_some())
            .all(|e| e.guarded)
    );
}

#[test]
fn a_node_reachable_only_by_rejection_is_not_an_orphan() {
    let compiled = remediate(&Seen::default(), 0, true).compile().unwrap();

    assert!(validate(&compiled.to_definition()).is_valid());
    // The executor's own edge list doesn't route over it.
    assert!(
        compiled
            .definition
            .edges
            .iter()
            .all(|e| e.condition.as_deref() != Some("on_reject"))
    );
    let errors = validate(&compiled.definition).errors;
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].code, "STRATA_ERR_ORPHAN");
}

#[test]
fn a_reject_route_to_a_missing_node_is_a_dangling_edge() {
    let result = StateGraph::<i32>::new()
        .add_node("Approve", |s: i32| s)
        .add_interrupt_boundary_with_reject("Approve", "Nope")
        .compile();

    let Err(StrataError::GraphInvalid(validation)) = result else {
        panic!("expected GraphInvalid");
    };
    assert_eq!(validation.errors.len(), 1);
    assert_eq!(validation.errors[0].code, "STRATA_ERR_DANGLING_EDGE");
    assert_eq!(validation.errors[0].node, "Approve");
    assert_eq!(validation.errors[0].related_node.as_deref(), Some("Nope"));
}

#[test]
fn a_reject_route_on_a_name_that_is_not_a_node_is_a_dangling_edge() {
    let result = StateGraph::<i32>::new()
        .add_node("A", |s: i32| s)
        .add_node("Undo", |s: i32| s)
        .add_edge("A", "Undo")
        .add_interrupt_boundary_with_reject("Ghost", "Undo")
        .compile();

    let Err(StrataError::GraphInvalid(validation)) = result else {
        panic!("expected GraphInvalid");
    };
    assert_eq!(validation.errors.len(), 1);
    assert_eq!(validation.errors[0].code, "STRATA_ERR_DANGLING_EDGE");
}

#[test]
fn marking_a_boundary_again_without_a_route_removes_it() {
    let graph = StateGraph::<i32>::new()
        .add_node("A", |s: i32| s)
        .add_node("B", |s: i32| s)
        .add_edge("A", "B")
        .add_interrupt_boundary_with_reject("A", "B")
        .add_interrupt_boundary("A");

    assert!(
        graph
            .to_definition()
            .edges
            .iter()
            .all(|e| e.condition.as_deref() != Some("on_reject"))
    );
}

// --- Run events ----------------------------------------------------------------

#[tokio::test]
async fn resumed_carries_the_answer_and_the_route_taken() {
    let graph = remediate(&Seen::default(), 0, true).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", fresh(), store.clone()).await);

    let (config, events) = observed();
    completed(
        graph
            .resume_with("t", reject("bob"), store.clone(), config)
            .await,
    );

    let (is_approved, reviewer, rejected_to, started) = resumed(&drain(events).await);
    assert_eq!(started, "Abort");
    assert!(!is_approved);
    assert_eq!(reviewer, "bob");
    assert_eq!(rejected_to.as_deref(), Some("Abort"));
}

#[tokio::test]
async fn resumed_names_a_reject_route_taken_inside_a_child() {
    let graph = incident_around(change_child(&Seen::default(), 0), &Seen::default())
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", new_incident(), store.clone()).await);

    let (config, events) = observed();
    completed(
        graph
            .resume_with("t", reject("bob"), store.clone(), config)
            .await,
    );

    let (_, _, rejected_to, started) = resumed(&drain(events).await);
    assert_eq!(started, "Work");
    assert_eq!(rejected_to.as_deref(), Some("Revoke"));
}

// --- The shared fixtures --------------------------------------------------------

fn lifecycle_fixture(name: &str) -> Checkpoint {
    serde_json::from_value(fixture(&format!("checkpoint-lifecycle/{name}"))).unwrap()
}

/// Blanks every level's `created_at` and `decided_at`, which a live run sets to now.
fn without_timestamps(mut checkpoint: Checkpoint) -> Value {
    fn strip(checkpoint: &mut Checkpoint) {
        checkpoint.created_at = String::new();
        if let Some(decision) = checkpoint.decision.as_mut() {
            decision.decided_at = String::new();
        }
        if let Some(child) = checkpoint.nested.as_deref_mut() {
            strip(child);
        }
    }
    strip(&mut checkpoint);
    serde_json::to_value(checkpoint).unwrap()
}

#[tokio::test]
async fn failing_after_an_approval_matches_the_shared_decision_failed_fixture() {
    let graph = remediate(&Seen::default(), 1, true).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(
        graph
            .run("thread_decision_demo", fresh(), store.clone())
            .await,
    );
    node_failed(
        graph
            .resume("thread_decision_demo", approve_with_edits(), store.clone())
            .await,
    );

    assert_eq!(
        without_timestamps(store.get("thread_decision_demo")),
        without_timestamps(lifecycle_fixture("decision-failed.json"))
    );
}

#[tokio::test]
async fn resuming_the_shared_decision_failed_fixture_keeps_the_approver() {
    let graph = remediate(&Seen::default(), 0, true).compile().unwrap();
    let store = Arc::new(TestStore::default());
    let fixture = lifecycle_fixture("decision-failed.json");
    store.put(fixture.clone());

    let state = completed(
        graph
            .resume(
                fixture.thread_id.as_str(),
                approve("bob@ops"),
                store.clone(),
            )
            .await,
    );

    assert_eq!(
        state.outcome.as_deref(),
        Some("applied, approved by alice@acme.io")
    );
    assert_eq!(state.replicas, 3);
    assert_eq!(state.plan.unwrap().steps, ["drain", "rollback"]);
}

#[tokio::test]
async fn a_rejection_failing_inside_a_child_matches_the_shared_nested_decision_fixture() {
    let graph = incident_around(change_child(&Seen::default(), 1), &Seen::default())
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(
        graph
            .run("thread_nested_decision_demo", new_incident(), store.clone())
            .await,
    );
    node_failed(
        graph
            .resume(
                "thread_nested_decision_demo",
                reject("bob@acme.io").with_override("reason", "wrong replica count"),
                store.clone(),
            )
            .await,
    );

    assert_eq!(
        without_timestamps(store.get("thread_nested_decision_demo")),
        without_timestamps(lifecycle_fixture("nested-decision.json"))
    );
}

#[tokio::test]
async fn resuming_the_shared_nested_decision_fixture_re_enters_the_reject_route() {
    let seen = Seen::default();
    let graph = incident_around(change_child(&seen, 0), &seen)
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    let fixture = lifecycle_fixture("nested-decision.json");
    store.put(fixture.clone());

    let state = completed(
        graph
            .resume(fixture.thread_id.as_str(), approve("ops"), store.clone())
            .await,
    );

    assert_eq!(
        state.log,
        [
            "prepared",
            "draft",
            "approve",
            "revoke:bob@acme.io",
            "finished"
        ]
    );
    assert_eq!(
        seen.at("Work/Revoke").unwrap().overrides["reason"],
        "wrong replica count"
    );
}

#[test]
fn the_decision_fixtures_round_trip_through_the_schema_types() {
    for name in ["decision-failed.json", "nested-decision.json"] {
        let raw = fixture(&format!("checkpoint-lifecycle/{name}"));
        let typed: Checkpoint = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(serde_json::to_value(typed).unwrap(), raw, "{name}");
    }
}
