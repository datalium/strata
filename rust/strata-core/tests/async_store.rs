//! The engine against a genuinely asynchronous checkpoint store
//! (`design/19-Async-Queryable-Store.md`): every save point waits for a save
//! that completes on a later poll, a cancellation never cuts a save short, and
//! a store that panics still ends the run's telemetry before the panic
//! propagates.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::CancellationToken;
use datalium_strata::checkpoint::{
    CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CompiledGraph, NodeError, StateGraph};
use datalium_strata::observability::{RunEventChannel, RunEventKind};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Doc {
    steps: Vec<String>,
}

fn doc() -> Doc {
    Doc { steps: Vec::new() }
}

fn did(step: &'static str) -> impl Fn(Doc) -> Doc + Send + Sync + 'static {
    move |mut doc| {
        doc.steps.push(step.to_string());
        doc
    }
}

/// An append-only store whose calls suspend: each waits `delay`, and, when
/// gated, says it has started and waits to be released before it saves.
#[derive(Default)]
struct SlowStore {
    log: Mutex<HashMap<String, Vec<Checkpoint>>>,
    delay: Duration,
    gate: Option<(Notify, Notify)>,
    panic_on_save: bool,
}

impl SlowStore {
    fn slow() -> Self {
        Self {
            delay: Duration::from_millis(10),
            ..Self::default()
        }
    }

    fn gated() -> Self {
        Self {
            gate: Some((Notify::new(), Notify::new())),
            ..Self::default()
        }
    }

    fn saved(&self, thread_id: &str) -> Vec<Checkpoint> {
        self.log
            .lock()
            .unwrap()
            .get(thread_id)
            .cloned()
            .unwrap_or_default()
    }
}

impl CheckpointStore for SlowStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        Box::pin(async move {
            if self.panic_on_save {
                panic!("store bug");
            }
            tokio::time::sleep(self.delay).await;
            if let Some((started, release)) = &self.gate {
                started.notify_one();
                release.notified().await;
            }
            self.log
                .lock()
                .unwrap()
                .entry(checkpoint.thread_id.clone())
                .or_default()
                .push(checkpoint);
            Ok(())
        })
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            Ok(self.saved(thread_id).last().cloned())
        })
    }
}

/// `Prepare` -> (boundary) -> `Execute`.
fn freezing_graph() -> CompiledGraph<Doc> {
    StateGraph::<Doc>::new()
        .add_node("Prepare", did("prepare"))
        .add_interrupt_boundary("Prepare")
        .add_node("Execute", did("execute"))
        .add_edge("Prepare", "Execute")
        .compile()
        .unwrap()
}

#[tokio::test]
async fn every_save_point_waits_for_a_store_that_really_suspends() {
    let store = Arc::new(SlowStore::slow());

    // Freeze, then resume: the frozen checkpoint, then the resume stamp.
    let graph = freezing_graph();
    assert!(matches!(
        graph.run("t", doc(), store.clone()).await,
        GraphExecutionResult::Interrupted { .. }
    ));
    assert_eq!(store.saved("t").len(), 1);
    match graph
        .resume("t", HumanSignal::default(), store.clone())
        .await
    {
        GraphExecutionResult::Completed(state) => {
            assert_eq!(state.steps, ["prepare", "execute"])
        }
        _ => panic!("expected Completed"),
    }
    let log = store.saved("t");
    assert_eq!(log.len(), 2);
    assert!(log[1].resumed_at.is_some());

    // A failure.
    let failing = StateGraph::<Doc>::new()
        .add_fallible_node("Charge", |_: &Doc| {
            Err(NodeError::Fatal("declined".to_string()))
        })
        .compile()
        .unwrap();
    assert!(matches!(
        failing.run("f", doc(), store.clone()).await,
        GraphExecutionResult::Failed(_)
    ));
    assert_eq!(store.saved("f")[0].status, CheckpointStatus::Failed);

    // A freeze inside a subgraph: the parent's checkpoint nests the child's.
    let parent = StateGraph::<Doc>::new()
        .add_subgraph(
            "Work",
            freezing_graph(),
            |d: &Doc| d.clone(),
            |_, child| child,
        )
        .compile()
        .unwrap();
    match parent.run("n", doc(), store.clone()).await {
        GraphExecutionResult::Interrupted { waiting_at, .. } => {
            assert_eq!(waiting_at, ["Work", "Prepare"])
        }
        _ => panic!("expected Interrupted"),
    }
    let nested = &store.saved("n")[0];
    assert_eq!(nested.waiting_at(), ["Work", "Prepare"]);
    assert!(matches!(
        parent
            .resume("n", HumanSignal::default(), store.clone())
            .await,
        GraphExecutionResult::Completed(_)
    ));
}

#[tokio::test]
async fn a_cancellation_during_a_boundary_save_lets_the_save_finish() {
    // design/17: "A cancellation that arrives during an interrupt boundary's
    // save lets the save finish, and the run returns `Interrupted`." With an
    // async store, that means the engine never races a save against the token.
    let store = Arc::new(SlowStore::gated());
    let token = CancellationToken::new();
    let graph = freezing_graph();
    let run = {
        let (store, token) = (store.clone(), token.clone());
        tokio::spawn(async move {
            graph
                .run_with(
                    "t",
                    doc(),
                    store,
                    RunConfig::default().with_cancellation(token),
                )
                .await
        })
    };

    let (started, release) = store.gate.as_ref().unwrap();
    started.notified().await;
    token.cancel();
    // Room for an engine that raced the save to drop it.
    tokio::time::sleep(Duration::from_millis(20)).await;
    release.notify_one();

    assert!(matches!(
        run.await.unwrap(),
        GraphExecutionResult::Interrupted { .. }
    ));
    let saved = store.saved("t");
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].status, CheckpointStatus::Interrupted);
}

#[tokio::test]
async fn a_run_started_cancelled_still_persists_its_cancelled_checkpoint() {
    // The token has fired before the save even starts: an engine that raced
    // store calls against it would never write the record cancellation exists
    // to leave.
    let store = Arc::new(SlowStore::slow());
    let token = CancellationToken::new();
    token.cancel();

    let result = freezing_graph()
        .run_with(
            "t",
            doc(),
            store.clone(),
            RunConfig::default().with_cancellation(token),
        )
        .await;

    assert!(matches!(result, GraphExecutionResult::Cancelled { .. }));
    let saved = store.saved("t");
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].status, CheckpointStatus::Cancelled);
}

#[tokio::test]
async fn a_store_that_panics_still_ends_the_run_before_the_panic_propagates() {
    let store = Arc::new(SlowStore {
        panic_on_save: true,
        ..SlowStore::default()
    });
    let (observer, mut events) = RunEventChannel::new();
    let graph = freezing_graph();

    let joined = tokio::spawn(async move {
        graph
            .run_with(
                "t",
                doc(),
                store,
                RunConfig::default().with_observer(observer),
            )
            .await
    })
    .await;

    assert!(joined.is_err_and(|error| error.is_panic()));
    let mut last = None;
    while let Some(event) = events.recv().await {
        last = Some(event);
    }
    assert!(matches!(
        last.expect("events were recorded").kind,
        RunEventKind::RunCompleted { .. }
    ));
}
