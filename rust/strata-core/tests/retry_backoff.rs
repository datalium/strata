//! Retry backoff and failure info for fallbacks (`design/25-Retry-Backoff.md`,
//! SG-04), exercised through the public API only: backoff between retries
//! (growth, cap, jitter, the retry hint), the wait under cancellation and a
//! deadline, `NodeContext::last_failure` in a fallback, and groups and
//! subgraphs backing off. Every test runs on tokio's paused clock, so waits
//! take no real time. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/RetryBackoffTests.cs` — keep the two in
//! step.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::checkpoint::{
    CancelReason, CheckpointStore, CheckpointStoreError, GraphExecutionResult,
};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{
    CompiledGraph, Jitter, NodeError, NodeFailure, RecoveryPolicy, StateGraph,
};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind, RunObserver};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::{BoxFuture, CancellationToken, StrataError};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::Instant;

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

fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
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

fn store() -> Arc<TestStore> {
    Arc::new(TestStore::default())
}

fn transient(hint: Option<Duration>) -> NodeError {
    match hint {
        Some(after) => NodeError::transient_after("rate limited", after),
        None => NodeError::Transient("rate limited".to_string()),
    }
}

/// A fallible node that fails with the next of its failures per call, then
/// succeeds, recording when each attempt started.
#[derive(Clone)]
struct Flaky {
    failures: Arc<Vec<NodeError>>,
    calls: Arc<AtomicU32>,
    started: Arc<Mutex<Vec<Instant>>>,
}

impl Flaky {
    fn new(failures: Vec<NodeError>) -> Self {
        Self {
            failures: Arc::new(failures),
            calls: Arc::new(AtomicU32::new(0)),
            started: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn times(n: usize, failure: impl Fn() -> NodeError) -> Self {
        Self::new((0..n).map(|_| failure()).collect())
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    fn started(&self) -> Vec<Instant> {
        self.started.lock().unwrap().clone()
    }

    fn graph(&self, policy: RecoveryPolicy) -> CompiledGraph<Job> {
        let flaky = self.clone();
        StateGraph::<Job>::new()
            .add_async_fallible_node("Charge", move |mut job: Job, _ctx: NodeContext| {
                let flaky = flaky.clone();
                async move {
                    flaky.started.lock().unwrap().push(Instant::now());
                    let call = flaky.calls.fetch_add(1, Ordering::SeqCst) as usize;
                    match flaky.failures.get(call) {
                        Some(failure) => Err(failure.clone()),
                        None => {
                            job.trail.push("charged".to_string());
                            Ok(job)
                        }
                    }
                }
            })
            .set_recovery_policy("Charge", policy)
            .compile()
            .unwrap()
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

/// Runs `graph` with an observer, returning its result and the delays of its
/// `RetryScheduled` events.
async fn run_observed(
    graph: &CompiledGraph<Job>,
    store: Arc<TestStore>,
    config: RunConfig,
) -> (GraphExecutionResult<Job>, Vec<Duration>) {
    let (observer, events) = RunEventChannel::new();
    let result = graph
        .run_with("t", job(), store, config.with_observer(observer))
        .await;
    let delays = drain(events)
        .await
        .into_iter()
        .filter_map(|event| match event.kind {
            RunEventKind::RetryScheduled { delay, .. } => Some(delay),
            _ => None,
        })
        .collect();
    (result, delays)
}

fn completed(result: GraphExecutionResult<Job>) -> Job {
    match result {
        GraphExecutionResult::Completed(job) => job,
        other => panic!("expected Completed, got {}", describe(&other)),
    }
}

fn describe(result: &GraphExecutionResult<Job>) -> String {
    match result {
        GraphExecutionResult::Completed(_) => "Completed".to_string(),
        GraphExecutionResult::Interrupted { .. } => "Interrupted".to_string(),
        GraphExecutionResult::Failed(error) => format!("Failed({error:?})"),
        GraphExecutionResult::Cancelled { node, reason, .. } => {
            format!("Cancelled({node}, {reason:?})")
        }
    }
}

#[tokio::test(start_paused = true)]
async fn without_backoff_retries_start_at_once_hint_or_not() {
    let flaky = Flaky::new(vec![transient(Some(secs(5))), transient(None)]);
    let graph = flaky.graph(RecoveryPolicy::retry(2));
    let begun = Instant::now();

    let (result, delays) = run_observed(&graph, store(), RunConfig::new()).await;

    completed(result);
    assert_eq!(flaky.calls(), 3);
    assert_eq!(delays, vec![Duration::ZERO, Duration::ZERO]);
    assert_eq!(Instant::now() - begun, Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn delays_double_from_initial_and_stop_at_max() {
    let flaky = Flaky::times(5, || transient(None));
    let graph = flaky.graph(RecoveryPolicy::retry(5).with_backoff(secs(1), secs(5), Jitter::None));

    let (result, delays) = run_observed(&graph, store(), RunConfig::new()).await;

    completed(result);
    assert_eq!(delays, vec![secs(1), secs(2), secs(4), secs(5), secs(5)]);
    // Each attempt started no sooner than the delay before it.
    let started = flaky.started();
    for i in 1..started.len() {
        assert!(
            started[i] - started[i - 1] >= delays[i - 1],
            "attempt {} started early",
            i + 1
        );
    }
}

async fn jitter_stays_within_its_bounds(jitter: Jitter, floor: f64) {
    let ceilings = [secs(1), secs(2), secs(4), secs(8), secs(8)];
    let mut ratios = Vec::new();
    for _ in 0..5 {
        let flaky = Flaky::times(5, || transient(None));
        let graph = flaky.graph(RecoveryPolicy::retry(5).with_backoff(secs(1), secs(8), jitter));

        let (result, delays) = run_observed(&graph, store(), RunConfig::new()).await;

        completed(result);
        assert_eq!(delays.len(), 5);
        for (delay, ceiling) in delays.iter().zip(ceilings) {
            assert!(
                *delay >= ceiling.mul_f64(floor) && *delay <= ceiling,
                "{delay:?} is outside [{:?}, {ceiling:?}]",
                ceiling.mul_f64(floor)
            );
            ratios.push(delay.as_secs_f64() / ceiling.as_secs_f64());
        }
    }
    // Jittered: not every delay sits at its ceiling.
    assert!(ratios.iter().any(|ratio| *ratio < 0.99));
}

#[tokio::test(start_paused = true)]
async fn full_jitter_stays_within_zero_and_the_ceiling() {
    jitter_stays_within_its_bounds(Jitter::Full, 0.0).await;
}

#[tokio::test(start_paused = true)]
async fn equal_jitter_stays_within_half_the_ceiling_and_the_ceiling() {
    jitter_stays_within_its_bounds(Jitter::Equal, 0.5).await;
}

#[tokio::test(start_paused = true)]
async fn the_hint_replaces_the_delay_up_to_max_unjittered() {
    // A 3 s hint, a 60 s hint, then none: the third retry's computed ceiling
    // is 4 s.
    let flaky = Flaky::new(vec![
        transient(Some(secs(3))),
        transient(Some(secs(60))),
        transient(None),
    ]);
    let graph = flaky.graph(RecoveryPolicy::retry(3).with_backoff(secs(1), secs(10), Jitter::Full));

    let (result, delays) = run_observed(&graph, store(), RunConfig::new()).await;

    completed(result);
    assert_eq!(delays.len(), 3);
    assert_eq!(delays[0], secs(3));
    assert_eq!(delays[1], secs(10));
    // The earlier hints don't carry over: back to full jitter under 4 s.
    assert!(delays[2] <= secs(4));
}

/// Cancels a run's token once its first retry has been scheduled.
struct CancelOnRetry(CancellationToken);

impl RunObserver for CancelOnRetry {
    fn on_event(&self, event: &RunEvent) {
        if matches!(event.kind, RunEventKind::RetryScheduled { .. }) {
            self.0.cancel();
        }
    }
}

#[tokio::test(start_paused = true)]
async fn cancelling_the_run_stops_the_wait() {
    let flaky = Flaky::new(vec![transient(None)]);
    let hour = secs(3600);
    let graph = flaky.graph(RecoveryPolicy::retry(1).with_backoff(hour, hour, Jitter::None));
    let store = store();
    let token = CancellationToken::new();
    let begun = Instant::now();

    let result = graph
        .run_with(
            "t",
            job(),
            store.clone(),
            RunConfig::new()
                .with_cancellation(token.clone())
                .with_observer(Arc::new(CancelOnRetry(token))),
        )
        .await;

    match result {
        GraphExecutionResult::Cancelled { node, reason, .. } => {
            assert_eq!(node, "Charge");
            assert_eq!(reason, CancelReason::Requested);
        }
        other => panic!("expected Cancelled, got {}", describe(&other)),
    }
    assert_eq!(flaky.calls(), 1);
    assert!(Instant::now() - begun < hour);
    let checkpoint = store.latest("t");
    assert_eq!(checkpoint.status, CheckpointStatus::Cancelled);
    assert_eq!(checkpoint.next_node.as_deref(), Some("Charge"));
}

#[tokio::test(start_paused = true)]
async fn a_deadline_inside_the_wait_cancels_when_it_passes_and_not_before() {
    let flaky = Flaky::new(vec![transient(None)]);
    let graph =
        flaky.graph(RecoveryPolicy::retry(1).with_backoff(secs(60), secs(60), Jitter::None));
    let begun = Instant::now();

    let result = graph
        .run_with(
            "t",
            job(),
            store(),
            RunConfig::new().with_deadline((begun + secs(5)).into_std()),
        )
        .await;

    match result {
        GraphExecutionResult::Cancelled { reason, .. } => {
            assert_eq!(reason, CancelReason::Deadline);
        }
        other => panic!("expected Cancelled, got {}", describe(&other)),
    }
    assert_eq!(flaky.calls(), 1);
    let waited = Instant::now() - begun;
    assert!(
        waited >= secs(5) && waited < secs(60),
        "ended after {waited:?}"
    );
}

/// Each recording node's name, with the `last_failure` it saw.
type Sightings = Arc<Mutex<Vec<(String, Option<NodeFailure>)>>>;

/// What each recording node saw as its `last_failure`.
#[derive(Clone, Default)]
struct Seen(Sightings);

impl Seen {
    fn of(&self, node: &str) -> Vec<Option<NodeFailure>> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(seen, _)| seen == node)
            .map(|(_, failure)| failure.clone())
            .collect()
    }

    /// A handler that records its context's `last_failure`, then fails with
    /// what `fail` returns for its call number (1-based), if anything.
    #[allow(clippy::type_complexity)]
    fn record(
        &self,
        node: &str,
        fail: impl Fn(u32) -> Option<NodeError> + Send + Sync + 'static,
    ) -> impl Fn(Job, NodeContext) -> BoxFuture<'static, Result<Job, NodeError>> + Send + Sync + 'static
    {
        let (seen, node, calls, fail) = (
            self.clone(),
            node.to_string(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(fail),
        );
        move |mut job: Job, ctx: NodeContext| {
            seen.0
                .lock()
                .unwrap()
                .push((node.clone(), ctx.last_failure().cloned()));
            let outcome = fail(calls.fetch_add(1, Ordering::SeqCst) + 1);
            let node = node.clone();
            Box::pin(async move {
                match outcome {
                    Some(error) => Err(error),
                    None => {
                        job.trail.push(node);
                        Ok(job)
                    }
                }
            })
        }
    }
}

fn never(_: u32) -> Option<NodeError> {
    None
}

fn fallback_graph(
    seen: &Seen,
    charge: impl Fn(Job, NodeContext) -> BoxFuture<'static, Result<Job, NodeError>>
    + Send
    + Sync
    + 'static,
    policy: RecoveryPolicy,
    fallback_fails: impl Fn(u32) -> Option<NodeError> + Send + Sync + 'static,
) -> StateGraph<Job> {
    StateGraph::<Job>::new()
        .add_node("Start", |job: Job| job)
        .add_async_fallible_node("Charge", charge)
        .add_async_fallible_node("Fallback", seen.record("Fallback", fallback_fails))
        .add_async_fallible_node("After", seen.record("After", never))
        .add_edge("Start", "Charge")
        .add_edge("Charge", "After")
        .add_edge("Fallback", "After")
        .set_recovery_policy("Charge", policy)
}

#[tokio::test(start_paused = true)]
async fn the_fallback_sees_why_a_transient_failure_ran_out_of_retries() {
    let seen = Seen::default();
    let graph = fallback_graph(
        &seen,
        seen.record("Charge", |_| Some(transient(Some(secs(2))))),
        RecoveryPolicy::retry(1)
            .with_backoff(secs(1), secs(5), Jitter::Full)
            .with_fallback("Fallback"),
        never,
    )
    .compile()
    .unwrap();

    let job = completed(graph.run("t", job(), store()).await);

    assert_eq!(job.trail, vec!["Fallback", "After"]);
    let failures = seen.of("Fallback");
    assert_eq!(failures.len(), 1);
    let failure = failures[0].clone().expect("the fallback sees the failure");
    assert_eq!(failure.node, "Charge");
    assert_eq!(failure.node_path, "Charge");
    assert!(failure.error.is_transient());
    assert_eq!(failure.error.message(), "rate limited");
    assert_eq!(failure.error.retry_after(), Some(secs(2)));
    assert_eq!(failure.attempts, 2);
    assert!(!failure.timed_out);

    // Nobody else sees it: not the failing node, nor the node after the fallback.
    assert!(seen.of("Charge").iter().all(Option::is_none));
    assert_eq!(seen.of("After"), vec![None]);
}

#[tokio::test(start_paused = true)]
async fn the_fallback_sees_a_fatal_failure_after_one_attempt() {
    let seen = Seen::default();
    let graph = fallback_graph(
        &seen,
        seen.record("Charge", |_| {
            Some(NodeError::Fatal("card declined".to_string()))
        }),
        RecoveryPolicy::retry(3).with_fallback("Fallback"),
        never,
    )
    .compile()
    .unwrap();

    completed(graph.run("t", job(), store()).await);

    let failure = seen.of("Fallback")[0].clone().unwrap();
    assert_eq!(failure.error, NodeError::Fatal("card declined".to_string()));
    assert_eq!(failure.error.retry_after(), None);
    assert_eq!(failure.attempts, 1);
}

#[tokio::test(start_paused = true)]
async fn the_fallback_sees_that_the_node_timed_out() {
    let seen = Seen::default();
    let graph = fallback_graph(
        &seen,
        |job: Job, _ctx: NodeContext| -> BoxFuture<'static, Result<Job, NodeError>> {
            Box::pin(async move {
                tokio::time::sleep(secs(3600)).await;
                Ok(job)
            })
        },
        RecoveryPolicy::retry(0).with_fallback("Fallback"),
        never,
    )
    .set_timeout("Charge", secs(1))
    .compile()
    .unwrap();

    completed(graph.run("t", job(), store()).await);

    let failure = seen.of("Fallback")[0].clone().unwrap();
    assert!(failure.timed_out);
    assert_eq!(
        failure.error,
        NodeError::Transient("node 'Charge' timed out after 1000 ms".to_string())
    );
}

#[tokio::test(start_paused = true)]
async fn every_attempt_of_a_retried_fallback_sees_it() {
    let seen = Seen::default();
    let graph = fallback_graph(
        &seen,
        seen.record("Charge", |_| {
            Some(NodeError::Fatal("card declined".to_string()))
        }),
        RecoveryPolicy::retry(0).with_fallback("Fallback"),
        |call| (call == 1).then(|| NodeError::Transient("busy".to_string())),
    )
    .set_recovery_policy("Fallback", RecoveryPolicy::retry(1))
    .compile()
    .unwrap();

    completed(graph.run("t", job(), store()).await);

    let failures = seen.of("Fallback");
    assert_eq!(failures.len(), 2);
    assert!(
        failures
            .iter()
            .all(|f| f.as_ref().map(|f| f.error.message()) == Some("card declined"))
    );
}

#[tokio::test(start_paused = true)]
async fn inside_a_subgraph_the_path_is_qualified_and_the_parent_sees_nothing() {
    let seen = Seen::default();
    let child = fallback_graph(
        &seen,
        seen.record("Charge", |_| {
            Some(NodeError::Fatal("card declined".to_string()))
        }),
        RecoveryPolicy::retry(0).with_fallback("Fallback"),
        never,
    )
    .compile()
    .unwrap();
    let parent = StateGraph::<Job>::new()
        .add_subgraph("Work", child, Job::clone, |_: &Job, done: Job| done)
        .add_async_fallible_node("Next", seen.record("Next", never))
        .add_edge("Work", "Next")
        .compile()
        .unwrap();

    completed(parent.run("t", job(), store()).await);

    let failure = seen.of("Fallback")[0].clone().unwrap();
    assert_eq!(failure.node, "Charge");
    assert_eq!(failure.node_path, "Work/Charge");
    assert_eq!(seen.of("Next"), vec![None]);
}

#[tokio::test(start_paused = true)]
async fn a_group_used_as_a_fallback_hands_it_to_its_branches() {
    let seen = Seen::default();
    let graph = StateGraph::<Job>::new()
        .add_async_fallible_node(
            "Charge",
            seen.record("Charge", |_| {
                Some(NodeError::Fatal("card declined".to_string()))
            }),
        )
        .add_parallel_group(
            "Fallback",
            ParallelBranches::new()
                .async_branch("a", seen.record("a", never))
                .async_branch("b", seen.record("b", never)),
            |job: &Job, _: Vec<Job>| job.clone(),
        )
        .set_recovery_policy("Charge", RecoveryPolicy::retry(0).with_fallback("Fallback"))
        .compile()
        .unwrap();

    completed(graph.run("t", job(), store()).await);

    for branch in ["a", "b"] {
        let failures = seen.of(branch);
        assert_eq!(failures.len(), 1);
        assert_eq!(
            failures[0].as_ref().map(|f| f.error.message()),
            Some("card declined")
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_group_backs_off_for_the_longest_hint_among_its_failed_branches() {
    let calls = Arc::new(AtomicU32::new(0));
    let branch = |hint: Duration| {
        let calls = calls.clone();
        move |job: Job, _ctx: NodeContext| {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if call < 2 {
                    Err(transient(Some(hint)))
                } else {
                    Ok(job)
                }
            }
        }
    };
    let graph = StateGraph::<Job>::new()
        .add_parallel_group(
            "Ask",
            ParallelBranches::new()
                .async_branch("a", branch(secs(2)))
                .async_branch("b", branch(secs(5))),
            |job: &Job, _: Vec<Job>| job.clone(),
        )
        .set_recovery_policy(
            "Ask",
            RecoveryPolicy::retry(1).with_backoff(secs(1), secs(10), Jitter::None),
        )
        .compile()
        .unwrap();

    let (result, delays) = run_observed(&graph, store(), RunConfig::new()).await;

    completed(result);
    assert_eq!(delays, vec![secs(5)]);
}

/// A child whose only node fails transiently with a 3 s hint while `fails`
/// says so for its call number (1-based), with no retries of its own.
fn rate_limited_child(fails: impl Fn(u32) -> bool + Send + Sync + 'static) -> CompiledGraph<Job> {
    let (calls, fails) = (Arc::new(AtomicU32::new(0)), Arc::new(fails));
    StateGraph::<Job>::new()
        .add_async_fallible_node("Ask", move |job: Job, _ctx: NodeContext| {
            let fail = fails(calls.fetch_add(1, Ordering::SeqCst) + 1);
            async move {
                if fail {
                    Err(transient(Some(secs(3))))
                } else {
                    Ok(job)
                }
            }
        })
        .compile()
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn a_subgraph_backs_off_for_its_childs_hint() {
    let parent = StateGraph::<Job>::new()
        .add_subgraph(
            "Work",
            rate_limited_child(|call| call == 1),
            Job::clone,
            |_: &Job, done: Job| done,
        )
        .set_recovery_policy(
            "Work",
            RecoveryPolicy::retry(1).with_backoff(secs(1), secs(10), Jitter::None),
        )
        .compile()
        .unwrap();

    let (result, delays) = run_observed(&parent, store(), RunConfig::new()).await;

    completed(result);
    assert_eq!(delays, vec![secs(3)]);
}

#[tokio::test(start_paused = true)]
async fn a_failed_checkpoint_records_the_hint_at_every_level() {
    let parent = StateGraph::<Job>::new()
        .add_subgraph(
            "Work",
            rate_limited_child(|_| true),
            Job::clone,
            |_: &Job, done: Job| done,
        )
        .compile()
        .unwrap();
    let store = store();

    let result = parent.run("t", job(), store.clone()).await;

    assert!(matches!(
        result,
        GraphExecutionResult::Failed(StrataError::NodeFailed { .. })
    ));
    let checkpoint = store.latest("t");
    assert_eq!(
        checkpoint.metadata.as_ref().unwrap()["strata.retry_after_ms"],
        "3000"
    );
    let nested = checkpoint.nested.as_ref().unwrap();
    assert_eq!(
        nested.metadata.as_ref().unwrap()["strata.retry_after_ms"],
        "3000"
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_checkpoint_without_a_hint_has_no_key() {
    let store = store();
    let graph = Flaky::new(vec![transient(None)]).graph(RecoveryPolicy::retry(0));

    let result = graph.run("t", job(), store.clone()).await;

    assert!(matches!(
        result,
        GraphExecutionResult::Failed(StrataError::NodeFailed { .. })
    ));
    assert!(
        !store
            .latest("t")
            .metadata
            .unwrap()
            .contains_key("strata.retry_after_ms")
    );
}

#[test]
fn a_backoff_whose_max_is_below_its_initial_delay_is_refused() {
    let compiled = StateGraph::<Job>::new()
        .add_fallible_node("Charge", |job: &Job| Ok(job.clone()))
        .set_recovery_policy(
            "Charge",
            RecoveryPolicy::retry(1).with_backoff(secs(2), secs(1), Jitter::None),
        )
        .compile();

    assert!(matches!(compiled, Err(StrataError::CompilationFailed(_))));
}
