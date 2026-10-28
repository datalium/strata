//! A parallel group's controls (`design/26-Parallel-Group-Controls-And-Joins.md`,
//! SG-07), exercised through the public API only: the concurrency limit,
//! per-branch timeouts, partial results and sibling cancellation. Every test
//! runs on tokio's paused clock. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/GroupControlsTests.cs` — keep the two in
//! step.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::checkpoint::{CheckpointStore, CheckpointStoreError, GraphExecutionResult};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, NodeError, StateGraph};
use datalium_strata::observability::{
    BranchEndReason, BranchResult, RunEvent, RunEventChannel, RunEventKind,
};
use datalium_strata::parallel::{
    BranchOutcomes, BranchStatus, ParallelBranches, ParallelGroupOptions,
};
use datalium_strata::schema::Checkpoint;
use datalium_strata::{BoxFuture, CancellationToken, StrataError};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: i32,
    trail: Vec<String>,
}

fn job() -> Job {
    Job {
        n: 0,
        trail: Vec::new(),
    }
}

fn done(mut job: Job, branch: &str) -> Job {
    job.trail.push(branch.to_string());
    job
}

#[derive(Default)]
struct TestStore {
    checkpoints: Mutex<HashMap<String, Checkpoint>>,
}

impl TestStore {
    fn latest(&self, thread_id: &str) -> Checkpoint {
        self.checkpoints.lock().unwrap()[thread_id].clone()
    }
}

impl CheckpointStore for TestStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.checkpoints
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
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

type Handler =
    Box<dyn Fn(Job, NodeContext) -> BoxFuture<'static, Result<Job, NodeError>> + Send + Sync>;

fn succeeds(name: &'static str) -> Handler {
    Box::new(move |job, _ctx| Box::pin(async move { Ok(done(job, name)) }))
}

fn fails(error: NodeError) -> Handler {
    Box::new(move |_job, _ctx| {
        let error = error.clone();
        Box::pin(async move { Err(error) })
    })
}

/// Waits `delay` on tokio's clock, then succeeds.
fn slow(name: &'static str, delay: Duration) -> Handler {
    Box::new(move |job, _ctx| {
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(done(job, name))
        })
    })
}

fn branches(handlers: Vec<(&'static str, Handler)>) -> ParallelBranches<Job> {
    handlers
        .into_iter()
        .fold(ParallelBranches::new(), |branches, (name, handler)| {
            branches.async_branch(name, handler)
        })
}

async fn drain(mut events: mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        all.push(event);
    }
    all
}

async fn run(
    graph: &CompiledGraph<Job>,
    store: Arc<TestStore>,
) -> (GraphExecutionResult<Job>, Vec<RunEvent>) {
    let (observer, events) = RunEventChannel::new();
    let result = graph
        .run_with("t", job(), store, RunConfig::new().with_observer(observer))
        .await;
    (result, drain(events).await)
}

/// Every branch's `BranchCompleted`, sorted by path.
fn branch_ends(events: &[RunEvent]) -> Vec<(String, BranchResult, Option<BranchEndReason>)> {
    let mut ends: Vec<_> = events
        .iter()
        .filter_map(|event| match &event.kind {
            RunEventKind::BranchCompleted { result, reason, .. } => Some((
                event.node_path.as_deref().unwrap_or_default().to_string(),
                *result,
                *reason,
            )),
            _ => None,
        })
        .collect();
    ends.sort_by(|a, b| a.0.cmp(&b.0));
    ends
}

fn completed(result: GraphExecutionResult<Job>) -> Job {
    match result {
        GraphExecutionResult::Completed(job) => job,
        GraphExecutionResult::Failed(error) => panic!("expected Completed, got Failed({error:?})"),
        _ => panic!("expected Completed"),
    }
}

fn failed_message(result: GraphExecutionResult<Job>) -> String {
    match result {
        GraphExecutionResult::Failed(StrataError::NodeFailed { message, .. }) => message,
        _ => panic!("expected NodeFailed"),
    }
}

/// A group of `branches` whose reducer keeps the outcomes it was handed.
fn group(
    branches: ParallelBranches<Job>,
    options: ParallelGroupOptions<Job>,
    seen: Arc<Mutex<Vec<BranchOutcomes<Job>>>>,
) -> CompiledGraph<Job> {
    StateGraph::<Job>::new()
        .add_parallel_group_with_outcomes(
            "Investigate",
            branches,
            move |entry: &Job, results: BranchOutcomes<Job>| {
                let trail = results.succeeded().flat_map(|s| s.trail.clone()).collect();
                seen.lock().unwrap().push(results);
                Job {
                    trail,
                    ..entry.clone()
                }
            },
            options,
        )
        .compile()
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn the_concurrency_limit_holds_and_branches_start_in_declaration_order() {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Mutex::new(Vec::new()));
    let tracked = |name: &'static str| -> Handler {
        let (in_flight, peak, started) = (in_flight.clone(), peak.clone(), started.clone());
        Box::new(move |job, _ctx| {
            let (in_flight, peak, started) = (in_flight.clone(), peak.clone(), started.clone());
            Box::pin(async move {
                started.lock().unwrap().push(name);
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(done(job, name))
            })
        })
    };
    let graph = StateGraph::<Job>::new()
        .add_parallel_group_with(
            "Investigate",
            branches(["a", "b", "c", "d", "e"].map(|n| (n, tracked(n))).into()),
            |entry: &Job, results: Vec<Job>| Job {
                trail: results.into_iter().flat_map(|r| r.trail).collect(),
                ..entry.clone()
            },
            ParallelGroupOptions::new().with_max_concurrency(2),
        )
        .compile()
        .unwrap();

    let (result, _) = run(&graph, Arc::default()).await;

    assert_eq!(completed(result).trail, ["a", "b", "c", "d", "e"]);
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    assert_eq!(*started.lock().unwrap(), ["a", "b", "c", "d", "e"]);
}

#[tokio::test(start_paused = true)]
async fn a_branch_timeout_times_out_only_that_branch() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let graph = group(
        branches(vec![
            ("telemetry", succeeds("telemetry")),
            ("kubernetes", slow("kubernetes", Duration::from_secs(3600))),
            ("database", slow("database", Duration::from_secs(10))),
        ]),
        ParallelGroupOptions::new()
            .with_branch_timeout(Duration::from_secs(45))
            .keep_succeeded(),
        seen.clone(),
    );

    let (result, events) = run(&graph, Arc::default()).await;

    assert_eq!(completed(result).trail, ["telemetry", "database"]);
    let seen = seen.lock().unwrap();
    let kubernetes = seen[0].get("kubernetes").unwrap();
    assert_eq!(kubernetes.status, BranchStatus::TimedOut);
    assert_eq!(
        kubernetes.error,
        Some(NodeError::Transient(
            "branch 'kubernetes' timed out after 45000 ms".to_string()
        ))
    );
    assert_eq!(
        branch_ends(&events),
        [
            (
                "Investigate/database".to_string(),
                BranchResult::Succeeded,
                None
            ),
            (
                "Investigate/kubernetes".to_string(),
                BranchResult::Failed,
                Some(BranchEndReason::TimedOut)
            ),
            (
                "Investigate/telemetry".to_string(),
                BranchResult::Succeeded,
                None
            ),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_queued_branch_is_not_charged_for_its_wait() {
    // One at a time, 30 s each, 45 s each: the second would time out if its
    // clock started with the first's.
    let graph = group(
        branches(vec![
            ("first", slow("first", Duration::from_secs(30))),
            ("second", slow("second", Duration::from_secs(30))),
        ]),
        ParallelGroupOptions::new()
            .with_branch_timeout(Duration::from_secs(45))
            .with_max_concurrency(1),
        Arc::default(),
    );

    let (result, _) = run(&graph, Arc::default()).await;

    assert_eq!(completed(result).trail, ["first", "second"]);
}

#[tokio::test(start_paused = true)]
async fn keep_succeeded_hands_every_outcome_to_the_reducer_in_declaration_order() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let graph = group(
        branches(vec![
            ("a", succeeds("a")),
            ("b", fails(NodeError::Fatal("bad query".to_string()))),
            ("c", fails(NodeError::Transient("api down".to_string()))),
            ("d", succeeds("d")),
        ]),
        ParallelGroupOptions::new().keep_succeeded(),
        seen.clone(),
    );

    let (result, events) = run(&graph, Arc::default()).await;

    assert_eq!(completed(result).trail, ["a", "d"]);
    let seen = seen.lock().unwrap();
    let outcomes = &seen[0];
    assert!(!outcomes.join_met());
    assert!(outcomes.deciding().is_empty());
    let names: Vec<&str> = outcomes
        .outcomes()
        .iter()
        .map(|o| o.name.as_str())
        .collect();
    assert_eq!(names, ["a", "b", "c", "d"]);
    let statuses: Vec<BranchStatus> = outcomes.outcomes().iter().map(|o| o.status).collect();
    assert_eq!(
        statuses,
        [
            BranchStatus::Succeeded,
            BranchStatus::Failed,
            BranchStatus::Failed,
            BranchStatus::Succeeded
        ]
    );
    assert_eq!(
        outcomes.get("a").unwrap().state.as_ref().unwrap().trail,
        ["a"]
    );
    assert!(outcomes.get("b").unwrap().state.is_none());
    assert_eq!(
        outcomes.get("b").unwrap().error,
        Some(NodeError::Fatal("bad query".to_string()))
    );
    assert!(
        outcomes
            .get("c")
            .unwrap()
            .error
            .as_ref()
            .unwrap()
            .is_transient()
    );
    // No branch was cancelled for its siblings' failures.
    assert!(
        branch_ends(&events)
            .iter()
            .all(|(_, result, _)| *result != BranchResult::Cancelled)
    );
}

#[tokio::test(start_paused = true)]
async fn the_plain_reducer_gets_the_succeeded_states_under_keep_succeeded() {
    let handed = Arc::new(Mutex::new(Vec::new()));
    let record = handed.clone();
    let graph = StateGraph::<Job>::new()
        .add_parallel_group_with(
            "Investigate",
            branches(vec![
                ("a", succeeds("a")),
                ("b", fails(NodeError::Fatal("x".to_string()))),
                ("c", succeeds("c")),
            ]),
            move |entry: &Job, results: Vec<Job>| {
                *record.lock().unwrap() = results;
                entry.clone()
            },
            ParallelGroupOptions::new().keep_succeeded(),
        )
        .compile()
        .unwrap();

    run(&graph, Arc::default()).await;

    let trails: Vec<Vec<String>> = handed
        .lock()
        .unwrap()
        .iter()
        .map(|j| j.trail.clone())
        .collect();
    assert_eq!(trails, [vec!["a"], vec!["c"]]);
}

#[tokio::test(start_paused = true)]
async fn when_every_branch_fails_the_group_fails_with_the_longest_hint() {
    let store = Arc::new(TestStore::default());
    let graph = group(
        branches(vec![
            (
                "a",
                fails(NodeError::transient_after("429", Duration::from_secs(2))),
            ),
            (
                "b",
                fails(NodeError::transient_after("429", Duration::from_secs(5))),
            ),
        ]),
        ParallelGroupOptions::new().keep_succeeded(),
        Arc::default(),
    );

    let (result, _) = run(&graph, store.clone()).await;

    assert_eq!(failed_message(result), "branch 'a': 429; branch 'b': 429");
    let metadata = store.latest("t").metadata.unwrap();
    assert_eq!(metadata["error_kind"], "transient");
    assert_eq!(metadata["strata.retry_after_ms"], "5000");
}

#[tokio::test(start_paused = true)]
async fn under_all_a_fatal_failure_makes_the_group_fatal() {
    let store = Arc::new(TestStore::default());
    let graph = group(
        branches(vec![
            ("a", fails(NodeError::Transient("api down".to_string()))),
            ("b", fails(NodeError::Fatal("bad query".to_string()))),
        ]),
        ParallelGroupOptions::new().keep_succeeded(),
        Arc::default(),
    );

    let (result, _) = run(&graph, store.clone()).await;

    failed_message(result);
    assert_eq!(store.latest("t").metadata.unwrap()["error_kind"], "fatal");
}

/// `a` fails at once; `b` takes 50 ms and keeps its token, to show whether it
/// was told to stop.
fn fail_fast(cancel_siblings: bool) -> (CompiledGraph<Job>, Arc<Mutex<Option<CancellationToken>>>) {
    let token = Arc::new(Mutex::new(None));
    let kept = token.clone();
    let b: Handler = Box::new(move |job, ctx| {
        *kept.lock().unwrap() = Some(ctx.cancellation().clone());
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(done(job, "b"))
        })
    });
    let graph = StateGraph::<Job>::new()
        .add_parallel_group_with(
            "Investigate",
            branches(vec![
                ("a", fails(NodeError::Fatal("bad query".to_string()))),
                ("b", b),
            ]),
            |entry: &Job, _: Vec<Job>| entry.clone(),
            ParallelGroupOptions::new().with_cancel_siblings_on_failure(cancel_siblings),
        )
        .compile()
        .unwrap();
    (graph, token)
}

#[tokio::test(start_paused = true)]
async fn by_default_a_failure_cancels_the_siblings() {
    let (graph, token) = fail_fast(true);

    let (result, events) = run(&graph, Arc::default()).await;

    assert_eq!(failed_message(result), "branch 'a': bad query");
    assert_eq!(
        branch_ends(&events),
        [
            ("Investigate/a".to_string(), BranchResult::Failed, None),
            (
                "Investigate/b".to_string(),
                BranchResult::Cancelled,
                Some(BranchEndReason::SiblingFailed)
            ),
        ]
    );
    assert!(token.lock().unwrap().as_ref().unwrap().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn without_sibling_cancellation_every_branch_finishes_before_the_group_fails() {
    let (graph, token) = fail_fast(false);

    let (result, events) = run(&graph, Arc::default()).await;

    assert_eq!(failed_message(result), "branch 'a': bad query");
    assert_eq!(
        branch_ends(&events),
        [
            ("Investigate/a".to_string(), BranchResult::Failed, None),
            ("Investigate/b".to_string(), BranchResult::Succeeded, None),
        ]
    );
    assert!(!token.lock().unwrap().as_ref().unwrap().is_cancelled());
}

#[test]
fn bad_options_are_refused_at_compile() {
    let compile = |options: ParallelGroupOptions<Job>| {
        StateGraph::<Job>::new()
            .add_parallel_group_with(
                "G",
                ParallelBranches::new().branch("a", |job: &Job| Ok(job.clone())),
                |entry: &Job, _: Vec<Job>| entry.clone(),
                options,
            )
            .compile()
    };

    assert!(matches!(
        compile(ParallelGroupOptions::new().with_max_concurrency(0)),
        Err(StrataError::CompilationFailed(_))
    ));
    assert!(matches!(
        compile(ParallelGroupOptions::new().with_branch_timeout(Duration::ZERO)),
        Err(StrataError::CompilationFailed(_))
    ));
}
