//! Run events (`design/18-Run-Observability.md`, SG-20): the exact event
//! sequence for reference graphs, in every run mode; gapless ordered sequence
//! numbers under concurrent branches; every started node and branch completed
//! however it ends; and an observer that panics or blocks affects nothing. Both
//! engines' suites describe events as the same strings ([`describe`]), so the
//! sequences can be compared line for line. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/RunEventsTests.cs` — keep the two in
//! step.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;

use datalium_strata::checkpoint::{
    CancelReason, CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::observability::{
    BranchResult, RunEvent, RunEventChannel, RunEventKind, RunObserver,
};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::Checkpoint;
use datalium_strata::tools::{ToolCallContext, ToolRegistry};
use datalium_strata::{CancellationToken, StrataError};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};

const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: i64,
    trail: Vec<String>,
}

fn job() -> Job {
    Job {
        n: 1,
        trail: Vec::new(),
    }
}

fn step(label: &'static str) -> impl Fn(Job) -> Job + Send + Sync + 'static {
    move |mut j| {
        j.trail.push(label.to_string());
        j
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

fn no_signal() -> HumanSignal {
    HumanSignal {
        is_approved: true,
        reviewer_token: "x".to_string(),
        field_overrides: HashMap::new(),
        ..Default::default()
    }
}

/// One line per event, the same in both engines' suites.
fn describe(event: &RunEvent) -> String {
    let path = event.node_path.as_deref().unwrap_or_default();
    let attempt = event.attempt.unwrap_or_default();
    match &event.kind {
        RunEventKind::RunStarted { mode, node, .. } => format!("RunStarted {mode:?} {node}"),
        RunEventKind::Resumed {
            from, waiting_at, ..
        } => {
            format!("Resumed {from:?} {}", waiting_at.join("/"))
        }
        RunEventKind::NodeStarted => format!("NodeStarted {path}#{attempt}"),
        RunEventKind::RetryScheduled { .. } => format!("RetryScheduled {path}#{attempt}"),
        RunEventKind::BranchStarted => format!("BranchStarted {path}#{attempt}"),
        RunEventKind::BranchCompleted { result, .. } => {
            format!("BranchCompleted {path} {result:?}")
        }
        RunEventKind::NodeCompleted {
            result, fallback, ..
        } => match fallback {
            Some(fallback) => format!("NodeCompleted {path}#{attempt} {result:?} -> {fallback}"),
            None => format!("NodeCompleted {path}#{attempt} {result:?}"),
        },
        RunEventKind::CheckpointSaved {
            status,
            waiting_at,
            resume_stamp,
            ..
        } => format!(
            "CheckpointSaved {status:?} {}{}",
            waiting_at.join("/"),
            if *resume_stamp { " (stamp)" } else { "" }
        ),
        RunEventKind::Interrupted {
            waiting_at,
            next_node,
            ..
        } => format!("Interrupted {} -> {next_node}", waiting_at.join("/")),
        RunEventKind::RunCompleted { outcome, .. } => format!("RunCompleted {outcome:?}"),
        RunEventKind::ToolCallStarted { tool, .. } => {
            format!("ToolCallStarted {path}#{attempt} {tool}")
        }
        RunEventKind::ToolCallCompleted { tool, result, .. } => {
            format!("ToolCallCompleted {path}#{attempt} {tool} {result:?}")
        }
        _ => "other".to_string(),
    }
}

/// Every event the channel delivers, until it ends after `RunCompleted`.
async fn drain(mut events: mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        all.push(event);
    }
    all
}

fn described(events: &[RunEvent]) -> Vec<String> {
    events.iter().map(describe).collect()
}

/// A config with a fresh channel observer, and the receiver its events arrive on.
fn observed() -> (RunConfig, mpsc::UnboundedReceiver<RunEvent>) {
    let (observer, events) = RunEventChannel::new();
    (RunConfig::new().with_observer(observer), events)
}

/// `Classify` (fallible, fails transiently once, one retry) -> `Approve` (a
/// boundary) -> `Execute`: the design doc's example.
fn classify_approve_execute() -> Arc<CompiledGraph<Job>> {
    let calls = Arc::new(AtomicU32::new(0));
    Arc::new(
        StateGraph::<Job>::new()
            .add_fallible_node("Classify", move |j: &Job| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(NodeError::Transient("gateway timeout".to_string()))
                } else {
                    Ok(j.clone())
                }
            })
            .add_node("Approve", step("approve"))
            .add_interrupt_boundary("Approve")
            .add_node("Execute", step("execute"))
            .add_edge("Classify", "Approve")
            .add_edge("Approve", "Execute")
            .set_recovery_policy("Classify", RecoveryPolicy::retry(1))
            .compile()
            .unwrap(),
    )
}

const FLAT_RUN: [&str; 9] = [
    "RunStarted Run Classify",
    "NodeStarted Classify#1",
    "RetryScheduled Classify#1",
    "NodeCompleted Classify#2 Committed",
    "NodeStarted Approve#1",
    "NodeCompleted Approve#1 Committed",
    "CheckpointSaved Interrupted Approve",
    "Interrupted Approve -> Execute",
    "RunCompleted Interrupted",
];

const FLAT_RESUME: [&str; 6] = [
    "RunStarted Resume Execute",
    "CheckpointSaved Interrupted Approve (stamp)",
    "Resumed Interrupted Approve",
    "NodeStarted Execute#1",
    "NodeCompleted Execute#1 Committed",
    "RunCompleted Completed",
];

#[tokio::test]
async fn a_flat_run_with_a_retry_and_a_boundary_emits_the_documented_sequence() {
    let graph = classify_approve_execute();
    let store = store();

    let (config, events) = observed();
    graph.run_with("t", job(), store.clone(), config).await;
    let run = described(&drain(events).await);
    let (config, events) = observed();
    graph
        .resume_with("t", no_signal(), store.clone(), config)
        .await;
    let resume = described(&drain(events).await);

    assert_eq!(run, FLAT_RUN);
    assert_eq!(resume, FLAT_RESUME);
}

#[tokio::test]
async fn streaming_runs_emit_the_same_sequence() {
    let graph = classify_approve_execute();
    let store = store();

    let (config, events) = observed();
    let (handle, mut chunks) = graph
        .clone()
        .run_streaming_with("t", job(), store.clone(), config);
    while chunks.recv().await.is_some() {}
    handle.await.unwrap();
    let run = described(&drain(events).await);

    let (config, events) = observed();
    let (handle, mut chunks) = graph.resume_streaming_with("t", no_signal(), store.clone(), config);
    while chunks.recv().await.is_some() {}
    handle.await.unwrap();
    let resume = described(&drain(events).await);

    assert_eq!(run, FLAT_RUN);
    assert_eq!(resume, FLAT_RESUME);
}

#[tokio::test]
async fn a_failing_branch_cancels_its_sibling_and_the_group_falls_back() {
    let graph = StateGraph::<Job>::new()
        .add_parallel_group(
            "Gather",
            ParallelBranches::new()
                .async_branch("Slow", |j: Job, _ctx: NodeContext| async move {
                    std::future::pending::<()>().await;
                    Ok(j)
                })
                .async_branch("Broken", |_: Job, _ctx: NodeContext| async move {
                    Err(NodeError::Fatal("source offline".to_string()))
                }),
            |s: &Job, _| s.clone(),
        )
        .add_node("Recover", step("recover"))
        .set_recovery_policy("Gather", RecoveryPolicy::default().with_fallback("Recover"))
        .compile()
        .unwrap();

    let (config, events) = observed();
    graph.run_with("t", job(), store(), config).await;

    assert_eq!(
        described(&drain(events).await),
        [
            "RunStarted Run Gather",
            "NodeStarted Gather#1",
            "BranchStarted Gather/Slow#1",
            "BranchStarted Gather/Broken#1",
            "BranchCompleted Gather/Broken Failed",
            "BranchCompleted Gather/Slow Cancelled",
            "NodeCompleted Gather#1 Failed -> Recover",
            "NodeStarted Recover#1",
            "NodeCompleted Recover#1 Committed",
            "RunCompleted Completed",
        ]
    );
}

#[tokio::test]
async fn a_subgraph_boundary_and_its_resume_emit_the_documented_sequence() {
    let child = StateGraph::<Job>::new()
        .add_node("Draft", step("draft"))
        .add_node("Approve", step("approve"))
        .add_interrupt_boundary("Approve")
        .add_node("Send", step("send"))
        .add_edge("Draft", "Approve")
        .add_edge("Approve", "Send")
        .compile()
        .unwrap();
    let graph = StateGraph::<Job>::new()
        .add_node("Prepare", step("prepare"))
        .add_subgraph("Work", child, |j: &Job| j.clone(), |_: &Job, j: Job| j)
        .add_node("Finish", step("finish"))
        .add_edge("Prepare", "Work")
        .add_edge("Work", "Finish")
        .compile()
        .unwrap();
    let store = store();

    let (config, events) = observed();
    graph.run_with("t", job(), store.clone(), config).await;
    let run = described(&drain(events).await);
    let (config, events) = observed();
    graph
        .resume_with("t", no_signal(), store.clone(), config)
        .await;
    let resume = described(&drain(events).await);

    assert_eq!(
        run,
        [
            "RunStarted Run Prepare",
            "NodeStarted Prepare#1",
            "NodeCompleted Prepare#1 Committed",
            "NodeStarted Work#1",
            "NodeStarted Work/Draft#1",
            "NodeCompleted Work/Draft#1 Committed",
            "NodeStarted Work/Approve#1",
            "NodeCompleted Work/Approve#1 Committed",
            "NodeCompleted Work#1 Interrupted",
            "CheckpointSaved Interrupted Work/Approve",
            "Interrupted Work/Approve -> Work",
            "RunCompleted Interrupted",
        ]
    );
    assert_eq!(
        resume,
        [
            "RunStarted Resume Work",
            "CheckpointSaved Interrupted Work/Approve (stamp)",
            "Resumed Interrupted Work/Approve",
            "NodeStarted Work#1",
            "NodeStarted Work/Send#1",
            "NodeCompleted Work/Send#1 Committed",
            "NodeCompleted Work#1 Committed",
            "NodeStarted Finish#1",
            "NodeCompleted Finish#1 Committed",
            "RunCompleted Completed",
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequence_numbers_are_gapless_and_in_delivery_order_under_concurrent_branches() {
    let branches = (0..16).fold(ParallelBranches::new(), |branches, i| {
        branches.async_branch(format!("B{i}"), |j: Job, _ctx: NodeContext| async move {
            tokio::task::yield_now().await;
            Ok(j)
        })
    });
    let graph = StateGraph::<Job>::new()
        .add_parallel_group("Fan", branches, |s: &Job, _| s.clone())
        .compile()
        .unwrap();

    let (config, events) = observed();
    graph.run_with("t", job(), store(), config).await;
    let events = drain(events).await;

    assert_eq!(
        events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        (1..=events.len() as u64).collect::<Vec<_>>()
    );
    let count =
        |wanted: fn(&RunEventKind) -> bool| events.iter().filter(|e| wanted(&e.kind)).count();
    assert_eq!(count(|k| matches!(k, RunEventKind::BranchStarted)), 16);
    assert_eq!(
        count(|k| matches!(
            k,
            RunEventKind::BranchCompleted {
                result: BranchResult::Succeeded,
                ..
            }
        )),
        16
    );
    assert!(matches!(
        events.last().unwrap().kind,
        RunEventKind::RunCompleted { .. }
    ));
    assert!(events.windows(2).all(|p| p[0].timestamp <= p[1].timestamp));
}

// --- Pairs always close ------------------------------------------------------

#[tokio::test]
async fn a_cancelled_node_is_completed_as_cancelled() {
    let started = Arc::new(Notify::new());
    let graph = {
        let started = started.clone();
        StateGraph::<Job>::new()
            .add_async_node("Charge", move |j: Job, _ctx: NodeContext| {
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
    let token = CancellationToken::new();
    let (config, events) = observed();

    let run = graph.run_with("t", job(), store(), config.with_cancellation(token.clone()));
    let cancel = async {
        started.notified().await;
        token.cancel();
    };
    tokio::time::timeout(PATIENCE, async { tokio::join!(run, cancel) })
        .await
        .unwrap();
    let events = drain(events).await;

    assert_eq!(
        described(&events),
        [
            "RunStarted Run Charge",
            "NodeStarted Charge#1",
            "NodeCompleted Charge#1 Cancelled",
            "CheckpointSaved Cancelled Charge",
            "RunCompleted Cancelled",
        ]
    );
    assert!(matches!(
        events.last().unwrap().kind,
        RunEventKind::RunCompleted {
            cancel_reason: Some(CancelReason::Requested),
            ..
        }
    ));
}

#[tokio::test]
async fn a_timed_out_node_is_retried_then_completed_as_failed() {
    let graph = StateGraph::<Job>::new()
        .add_async_fallible_node("Charge", |j: Job, _ctx: NodeContext| async move {
            std::future::pending::<()>().await;
            Ok(j)
        })
        .set_timeout("Charge", Duration::from_millis(20))
        .set_recovery_policy("Charge", RecoveryPolicy::retry(1))
        .compile()
        .unwrap();

    let (config, events) = observed();
    graph.run_with("t", job(), store(), config).await;
    let events = drain(events).await;

    assert_eq!(
        described(&events),
        [
            "RunStarted Run Charge",
            "NodeStarted Charge#1",
            "RetryScheduled Charge#1",
            "NodeCompleted Charge#2 Failed",
            "CheckpointSaved Failed Charge",
            "RunCompleted Failed",
        ]
    );
    assert!(events.iter().any(|e| matches!(
        e.kind,
        RunEventKind::RetryScheduled {
            timed_out: true,
            ..
        }
    )));
    assert!(events.iter().any(|e| matches!(
        &e.kind,
        RunEventKind::NodeCompleted { error: Some(error), .. }
            if error == "node 'Charge' timed out after 20 ms"
    )));
}

#[tokio::test]
async fn a_refused_streaming_node_is_started_and_completed_as_failed() {
    let graph = StateGraph::<Job>::new()
        .add_async_streaming_node("Draft", |j: Job, ctx: NodeContext| async move {
            ctx.stream().unwrap().emit_token("x").await.ok();
            j
        })
        .compile()
        .unwrap();

    let (config, events) = observed();
    graph.run_with("t", job(), store(), config).await;

    assert_eq!(
        described(&drain(events).await),
        [
            "RunStarted Run Draft",
            "NodeStarted Draft#1",
            "NodeCompleted Draft#1 Failed",
            "RunCompleted Failed",
        ]
    );
}

#[tokio::test]
async fn an_abandoned_sync_branch_is_completed_as_cancelled_while_it_still_runs() {
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let graph = {
        let release = release.clone();
        StateGraph::<Job>::new()
            .add_parallel_group(
                "Gather",
                ParallelBranches::new()
                    .branch("Blocking", move |j: &Job| {
                        let (released, wake) = &*release;
                        let guard = released.lock().unwrap();
                        let _guard = wake.wait_timeout_while(guard, PATIENCE, |r| !*r).unwrap();
                        Ok(j.clone())
                    })
                    .async_branch("Broken", |_: Job, _ctx: NodeContext| async move {
                        Err(NodeError::Fatal("source offline".to_string()))
                    }),
                |s: &Job, _| s.clone(),
            )
            .compile()
            .unwrap()
    };

    let (config, events) = observed();
    tokio::time::timeout(PATIENCE, graph.run_with("t", job(), store(), config))
        .await
        .unwrap();
    let events = described(&drain(events).await);
    *release.0.lock().unwrap() = true;
    release.1.notify_all();

    assert!(events.contains(&"BranchCompleted Gather/Blocking Cancelled".to_string()));
    assert_eq!(events.last().unwrap(), "RunCompleted Failed");
}

#[tokio::test]
async fn a_panic_in_a_node_still_completes_the_node_and_the_run_before_it_propagates() {
    let graph = Arc::new(
        StateGraph::<Job>::new()
            .add_async_node("Broken", |j: Job, _ctx: NodeContext| async move {
                if j.n > 0 {
                    panic!("bug");
                }
                j
            })
            .compile()
            .unwrap(),
    );
    let (config, events) = observed();

    let joined =
        tokio::spawn(async move { graph.run_with("t", job(), store(), config).await }).await;
    let events = drain(events).await;

    assert!(joined.is_err_and(|error| error.is_panic()));
    assert_eq!(
        described(&events),
        [
            "RunStarted Run Broken",
            "NodeStarted Broken#1",
            "NodeCompleted Broken#1 Failed",
            "RunCompleted Failed",
        ]
    );
    assert!(matches!(
        &events.last().unwrap().kind,
        RunEventKind::RunCompleted { error: Some(error), .. } if error == "bug"
    ));
}

// --- The observer can't stall or fail the run --------------------------------

#[derive(Default)]
struct PanicsOnFirst {
    calls: AtomicU32,
    seen: Mutex<Vec<RunEvent>>,
    done: Notify,
}

impl RunObserver for PanicsOnFirst {
    fn on_event(&self, event: &RunEvent) {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("observer bug");
        }
        self.seen.lock().unwrap().push(event.clone());
        if matches!(event.kind, RunEventKind::RunCompleted { .. }) {
            self.done.notify_one();
        }
    }
}

#[tokio::test]
async fn a_panicking_observer_neither_fails_the_run_nor_stops_the_events_after() {
    let observer = Arc::new(PanicsOnFirst::default());

    let result = classify_approve_execute()
        .run_with(
            "t",
            job(),
            store(),
            RunConfig::new().with_observer(observer.clone()),
        )
        .await;
    tokio::time::timeout(PATIENCE, observer.done.notified())
        .await
        .unwrap();

    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    assert_eq!(described(&observer.seen.lock().unwrap()), FLAT_RUN[1..]);
}

#[derive(Default)]
struct Blocked {
    gate: (Mutex<bool>, Condvar),
    seen: Mutex<Vec<RunEvent>>,
    done: Notify,
}

impl RunObserver for Blocked {
    fn on_event(&self, event: &RunEvent) {
        let (open, wake) = &self.gate;
        let guard = open.lock().unwrap();
        let _guard = wake
            .wait_timeout_while(guard, PATIENCE, |open| !*open)
            .unwrap();
        self.seen.lock().unwrap().push(event.clone());
        if matches!(event.kind, RunEventKind::RunCompleted { .. }) {
            self.done.notify_one();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocked_observer_does_not_delay_the_run() {
    let observer = Arc::new(Blocked::default());

    let result = tokio::time::timeout(
        PATIENCE,
        classify_approve_execute().run_with(
            "t",
            job(),
            store(),
            RunConfig::new().with_observer(observer.clone()),
        ),
    )
    .await
    .unwrap();

    // The run is over while the observer is still parked on its first event.
    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    assert!(observer.seen.lock().unwrap().is_empty());

    *observer.gate.0.lock().unwrap() = true;
    observer.gate.1.notify_all();
    tokio::time::timeout(PATIENCE, observer.done.notified())
        .await
        .unwrap();
    assert_eq!(described(&observer.seen.lock().unwrap()), FLAT_RUN);
}

#[tokio::test]
async fn a_channel_refuses_a_second_run() {
    let graph = StateGraph::<Job>::new()
        .add_node("Only", step("only"))
        .compile()
        .unwrap();
    let (observer, events) = RunEventChannel::new();

    graph
        .run_with(
            "t1",
            job(),
            store(),
            RunConfig::new().with_observer(observer.clone()),
        )
        .await;
    let events = drain(events).await;
    let second = graph
        .run_with(
            "t2",
            job(),
            store(),
            RunConfig::new().with_observer(observer),
        )
        .await;

    // The second run is unaffected by its observer refusing it.
    assert!(matches!(second, GraphExecutionResult::Completed(_)));
    assert!(events.iter().all(|e| &*e.thread_id == "t1"));
}

// --- Run id ------------------------------------------------------------------

#[tokio::test]
async fn a_caller_supplied_run_id_reaches_every_event_and_every_context() {
    let seen: Arc<Mutex<Vec<String>>> = Arc::default();
    let child = {
        let seen = seen.clone();
        StateGraph::<Job>::new()
            .add_async_node("Inner", move |j: Job, ctx: NodeContext| {
                seen.lock().unwrap().push(ctx.run_id().to_string());
                async move { j }
            })
            .compile()
            .unwrap()
    };
    let graph = StateGraph::<Job>::new()
        .add_subgraph("Work", child, |j: &Job| j.clone(), |_: &Job, j: Job| j)
        .compile()
        .unwrap();

    let (config, events) = observed();
    graph
        .run_with("t", job(), store(), config.with_run_id("exec-42"))
        .await;
    let events = drain(events).await;

    assert!(events.iter().all(|e| &*e.run_id == "exec-42"));
    assert_eq!(*seen.lock().unwrap(), ["exec-42"]);
}

#[tokio::test]
async fn the_default_run_id_is_a_fresh_uuid_v7_per_call() {
    let graph = StateGraph::<Job>::new()
        .add_node("Only", step("only"))
        .compile()
        .unwrap();

    let (config, first) = observed();
    graph.run_with("t", job(), store(), config).await;
    let (config, second) = observed();
    graph.run_with("t", job(), store(), config).await;
    let a = drain(first).await[0].run_id.clone();
    let b = drain(second).await[0].run_id.clone();

    assert_ne!(a, b);
    assert_eq!(uuid::Uuid::parse_str(&a).unwrap().get_version_num(), 7);
}

#[tokio::test]
async fn an_empty_run_id_is_refused() {
    let graph = StateGraph::<Job>::new()
        .add_node("Only", step("only"))
        .compile()
        .unwrap();

    let result = graph
        .run_with("t", job(), store(), RunConfig::new().with_run_id(""))
        .await;

    assert!(matches!(
        result,
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message))
            if message == "a run id can't be empty"
    ));
}

// --- Tool calls (design/20-Tool-Dispatch-And-Registry.md) ------------------

#[derive(Deserialize, datalium_strata::prelude::StrataSchema)]
struct NoArgs {}

#[datalium_strata::prelude::strata_tool(name = "events.lookup", description = "Looks.")]
async fn events_lookup(_args: NoArgs) -> Result<String, StrataError> {
    Ok("found".to_string())
}

#[datalium_strata::prelude::strata_tool(name = "events.hang", description = "Never returns.")]
async fn events_hang(_args: NoArgs) -> Result<String, StrataError> {
    std::future::pending().await
}

#[datalium_strata::prelude::strata_tool(name = "events.hidden", description = "Left out.")]
async fn events_hidden(_args: NoArgs) -> Result<String, StrataError> {
    Ok(String::new())
}

fn event_tools() -> Arc<ToolRegistry> {
    let mut tools = ToolRegistry::new();
    tools
        .register(Box::new(EventsLookupTool))
        .register(Box::new(EventsHangTool))
        .register(Box::new(EventsHiddenTool));
    Arc::new(tools.subset(["events.lookup", "events.hang"]).unwrap())
}

#[tokio::test]
async fn tool_calls_are_reported_between_their_nodes_events() {
    let tools = event_tools();
    let graph = StateGraph::<Job>::new()
        .add_async_node("Investigate", move |j: Job, ctx: NodeContext| {
            let tools = tools.clone();
            async move {
                let call = ToolCallContext::from(&ctx).with_caller_id("investigator");
                tools
                    .dispatch_with("events.lookup", "{}", call.clone().with_call_id("c1"))
                    .await
                    .unwrap();
                let refused = tools.dispatch_with("events.hidden", "{}", call).await;
                assert!(matches!(refused, Err(StrataError::ToolNotPermitted(_))));
                j
            }
        })
        .compile()
        .unwrap();
    let (config, events) = observed();

    graph.run_with("t", job(), store(), config).await;
    let events = drain(events).await;

    assert_eq!(
        described(&events),
        [
            "RunStarted Run Investigate",
            "NodeStarted Investigate#1",
            "ToolCallStarted Investigate#1 events.lookup",
            "ToolCallCompleted Investigate#1 events.lookup Succeeded",
            "ToolCallStarted Investigate#1 events.hidden",
            "ToolCallCompleted Investigate#1 events.hidden Refused",
            "NodeCompleted Investigate#1 Committed",
            "RunCompleted Completed",
        ]
    );
    let RunEventKind::ToolCallStarted {
        call_id, caller_id, ..
    } = &events[2].kind
    else {
        panic!("{:?}", events[2].kind)
    };
    assert_eq!(
        (call_id.as_str(), caller_id.as_deref()),
        ("c1", Some("investigator"))
    );
    let RunEventKind::ToolCallCompleted { error, .. } = &events[5].kind else {
        panic!("{:?}", events[5].kind)
    };
    assert_eq!(
        error.as_deref(),
        Some("tool 'events.hidden' is not permitted here")
    );
}

#[tokio::test]
async fn a_tool_call_the_engine_gave_up_on_is_completed_as_cancelled() {
    let tools = event_tools();
    let graph = StateGraph::<Job>::new()
        .add_async_node("Investigate", move |j: Job, ctx: NodeContext| {
            let tools = tools.clone();
            async move {
                let _ = tools
                    .dispatch_with("events.hang", "{}", ToolCallContext::from(&ctx))
                    .await;
                j
            }
        })
        .set_timeout("Investigate", Duration::from_millis(20))
        .compile()
        .unwrap();
    let (config, events) = observed();

    graph.run_with("t", job(), store(), config).await;
    let events = drain(events).await;

    assert_eq!(
        described(&events)[..5],
        [
            "RunStarted Run Investigate",
            "NodeStarted Investigate#1",
            "ToolCallStarted Investigate#1 events.hang",
            "ToolCallCompleted Investigate#1 events.hang Cancelled",
            "NodeCompleted Investigate#1 Failed",
        ]
    );
}

#[tokio::test]
async fn a_tool_call_with_a_hand_built_context_records_no_events() {
    let tools = event_tools();
    let graph = StateGraph::<Job>::new()
        .add_async_node("Investigate", move |j: Job, _ctx: NodeContext| {
            let tools = tools.clone();
            async move {
                let call = ToolCallContext::new().with_node_path("Investigate");
                tools
                    .dispatch_with("events.lookup", "{}", call)
                    .await
                    .unwrap();
                j
            }
        })
        .compile()
        .unwrap();
    let (config, events) = observed();

    graph.run_with("t", job(), store(), config).await;
    let events = drain(events).await;

    assert!(
        !described(&events).iter().any(|e| e.starts_with("ToolCall")),
        "{:?}",
        described(&events)
    );
}
