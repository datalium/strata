//! A parallel group's join (`design/26-Parallel-Group-Controls-And-Joins.md`,
//! SG-08), exercised through the public API only: join policies, losers
//! cancelled and reported, the unmet join, a race inside a subgraph, and the
//! join in the exported definition (`schema/fixtures/graph-export/joins.json`,
//! `graph-validation/invalid-quorum.json`). Runs on tokio's paused clock. The
//! .NET twin is `dotnet/tests/Datalium.Strata.Tests/GroupJoinTests.cs` — keep
//! the two in step.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::checkpoint::{CheckpointStore, CheckpointStoreError, GraphExecutionResult};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, NodeError, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind};
use datalium_strata::parallel::{
    BranchOutcomes, BranchStatus, Join, ParallelBranches, ParallelGroupOptions,
};
use datalium_strata::schema::{Checkpoint, GraphDefinition};
use datalium_strata::validate::validate;
use datalium_strata::{BoxFuture, CancellationToken, StrataError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

const PATIENCE: Duration = Duration::from_secs(10);

/// A model's answer: `n` is its confidence in percent.
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

/// Each branch's token, kept to show which were told to stop.
type Tokens = Arc<Mutex<HashMap<&'static str, CancellationToken>>>;

/// Answers with `confidence` after `after` (`None`: never), keeping its token.
fn answers(
    name: &'static str,
    confidence: i32,
    after: Option<Duration>,
    tokens: &Tokens,
) -> Handler {
    let tokens = tokens.clone();
    Box::new(move |mut job, ctx| {
        tokens
            .lock()
            .unwrap()
            .insert(name, ctx.cancellation().clone());
        Box::pin(async move {
            match after {
                Some(delay) => tokio::time::sleep(delay).await,
                None => std::future::pending().await,
            }
            job.n = confidence;
            job.trail.push(name.to_string());
            Ok(job)
        })
    })
}

fn at_once(name: &'static str, confidence: i32, tokens: &Tokens) -> Handler {
    answers(name, confidence, Some(Duration::ZERO), tokens)
}

fn after_ms(name: &'static str, confidence: i32, ms: u64, tokens: &Tokens) -> Handler {
    answers(name, confidence, Some(Duration::from_millis(ms)), tokens)
}

fn hangs(name: &'static str, tokens: &Tokens) -> Handler {
    answers(name, 0, None, tokens)
}

fn fails(error: NodeError) -> Handler {
    Box::new(move |_job, _ctx| {
        let error = error.clone();
        Box::pin(async move { Err(error) })
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

/// The group's branch and join events, one line each, in order: the same
/// lines as the .NET suite's `Story`.
fn story(events: &[RunEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| {
            let path = event.node_path.as_deref().unwrap_or_default();
            match &event.kind {
                RunEventKind::BranchStarted => Some(format!("started {path}")),
                RunEventKind::BranchCompleted { result, reason, .. } => Some(format!(
                    "completed {path} {result:?}{}",
                    reason.map(|r| format!(" {r:?}")).unwrap_or_default()
                )),
                RunEventKind::JoinDecided {
                    policy,
                    met,
                    deciding,
                } => Some(format!(
                    "decided {path} {policy} {} [{}]",
                    if *met { "met" } else { "unmet" },
                    deciding.join(",")
                )),
                _ => None,
            }
        })
        .collect()
}

fn ends(events: &[RunEvent]) -> Vec<String> {
    story(events)
        .into_iter()
        .filter(|line| !line.starts_with("started"))
        .collect()
}

/// A group named `Race` whose reducer keeps the outcomes it was handed and
/// returns the deciding branches' trails.
fn race(
    branches: ParallelBranches<Job>,
    options: ParallelGroupOptions<Job>,
    seen: Arc<Mutex<Vec<BranchOutcomes<Job>>>>,
) -> CompiledGraph<Job> {
    StateGraph::<Job>::new()
        .add_parallel_group_with_outcomes(
            "Race",
            branches,
            move |entry: &Job, results: BranchOutcomes<Job>| {
                let trail = results
                    .outcomes()
                    .iter()
                    .filter(|o| results.deciding().contains(&o.name))
                    .flat_map(|o| o.state.as_ref().unwrap().trail.clone())
                    .collect();
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

fn statuses(outcomes: &BranchOutcomes<Job>) -> Vec<BranchStatus> {
    outcomes.outcomes().iter().map(|o| o.status).collect()
}

#[tokio::test(start_paused = true)]
async fn first_success_reduces_as_soon_as_one_branch_succeeds_and_cancels_the_rest() {
    let tokens = Tokens::default();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let graph = race(
        branches(vec![
            ("openai", hangs("openai", &tokens)),
            ("anthropic", at_once("anthropic", 80, &tokens)),
            ("local", hangs("local", &tokens)),
        ]),
        ParallelGroupOptions::new().with_join(Join::first_success()),
        seen.clone(),
    );

    let (result, events) = run(&graph, Arc::default()).await;

    assert_eq!(completed(result).trail, ["anthropic"]);
    let seen = seen.lock().unwrap();
    assert!(seen[0].join_met());
    assert_eq!(seen[0].deciding(), ["anthropic"]);
    assert_eq!(
        statuses(&seen[0]),
        [
            BranchStatus::Cancelled,
            BranchStatus::Succeeded,
            BranchStatus::Cancelled
        ]
    );
    let lines = ends(&events);
    assert_eq!(
        lines[..2],
        [
            "completed Race/anthropic Succeeded",
            "decided Race first_success met [anthropic]"
        ]
    );
    assert!(lines.contains(&"completed Race/openai Cancelled LostRace".to_string()));
    assert!(lines.contains(&"completed Race/local Cancelled LostRace".to_string()));
    let tokens = tokens.lock().unwrap();
    assert!(tokens["openai"].is_cancelled() && tokens["local"].is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn first_matching_waits_for_a_result_that_passes_the_predicate() {
    let tokens = Tokens::default();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let graph = race(
        branches(vec![
            ("openai", at_once("openai", 50, &tokens)),
            ("anthropic", after_ms("anthropic", 95, 30, &tokens)),
            ("local", hangs("local", &tokens)),
        ]),
        ParallelGroupOptions::new().with_join(Join::first_matching(|j: &Job| j.n >= 90)),
        seen.clone(),
    );

    let (result, events) = run(&graph, Arc::default()).await;

    assert_eq!(completed(result).trail, ["anthropic"]);
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].deciding(), ["anthropic"]);
    assert_eq!(
        seen[0].get("openai").unwrap().status,
        BranchStatus::Succeeded
    );
    assert_eq!(
        ends(&events),
        [
            "completed Race/openai Succeeded",
            "completed Race/anthropic Succeeded",
            "decided Race first_matching met [anthropic]",
            "completed Race/local Cancelled LostRace",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_quorum_is_met_by_the_first_branches_to_succeed() {
    let tokens = Tokens::default();
    let handed = Arc::new(Mutex::new(Vec::new()));
    let record = handed.clone();
    let graph = StateGraph::<Job>::new()
        .add_parallel_group_with(
            "Race",
            branches(vec![
                ("openai", after_ms("openai", 70, 30, &tokens)),
                ("anthropic", at_once("anthropic", 80, &tokens)),
                ("local", hangs("local", &tokens)),
            ]),
            move |entry: &Job, results: Vec<Job>| {
                *record.lock().unwrap() = results;
                entry.clone()
            },
            ParallelGroupOptions::new().with_join(Join::quorum(2)),
        )
        .compile()
        .unwrap();

    let (result, events) = run(&graph, Arc::default()).await;

    completed(result);
    // The plain reducer gets the deciding states, in declaration order.
    let confidences: Vec<i32> = handed.lock().unwrap().iter().map(|j| j.n).collect();
    assert_eq!(confidences, [70, 80]);
    let lines = ends(&events);
    assert!(lines.contains(&"decided Race quorum met [anthropic,openai]".to_string()));
    assert!(lines.contains(&"completed Race/local Cancelled LostRace".to_string()));
}

#[tokio::test(start_paused = true)]
async fn branches_held_back_by_the_limit_never_start_once_the_race_is_won() {
    let tokens = Tokens::default();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let graph = race(
        branches(vec![
            ("openai", at_once("openai", 80, &tokens)),
            ("anthropic", at_once("anthropic", 90, &tokens)),
            ("local", at_once("local", 99, &tokens)),
        ]),
        ParallelGroupOptions::new()
            .with_join(Join::first_success())
            .with_max_concurrency(1),
        seen.clone(),
    );

    let (result, events) = run(&graph, Arc::default()).await;

    assert_eq!(completed(result).trail, ["openai"]);
    assert_eq!(
        story(&events),
        [
            "started Race/openai",
            "completed Race/openai Succeeded",
            "decided Race first_success met [openai]"
        ]
    );
    assert_eq!(
        statuses(&seen.lock().unwrap()[0]),
        [
            BranchStatus::Succeeded,
            BranchStatus::Cancelled,
            BranchStatus::Cancelled
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_race_goes_on_after_a_failure_while_it_can_still_be_won() {
    let tokens = Tokens::default();
    let graph = race(
        branches(vec![
            ("openai", fails(NodeError::Transient("500".to_string()))),
            ("anthropic", after_ms("anthropic", 80, 30, &tokens)),
        ]),
        ParallelGroupOptions::new().with_join(Join::first_success()),
        Arc::default(),
    );

    let (result, events) = run(&graph, Arc::default()).await;

    assert_eq!(completed(result).trail, ["anthropic"]);
    assert_eq!(
        ends(&events),
        [
            "completed Race/openai Failed",
            "completed Race/anthropic Succeeded",
            "decided Race first_success met [anthropic]"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_race_that_can_no_longer_be_won_fails_transiently_if_any_failure_was() {
    let store = Arc::new(TestStore::default());
    let graph = race(
        branches(vec![
            ("openai", fails(NodeError::Transient("500".to_string()))),
            ("anthropic", fails(NodeError::Fatal("refused".to_string()))),
        ]),
        ParallelGroupOptions::new().with_join(Join::first_success()),
        Arc::default(),
    );

    let (result, events) = run(&graph, store.clone()).await;

    assert_eq!(
        failed_message(result),
        "branch 'openai': 500; branch 'anthropic': refused"
    );
    assert_eq!(
        store.latest("t").metadata.unwrap()["error_kind"],
        "transient"
    );
    assert!(ends(&events).contains(&"decided Race first_success unmet []".to_string()));
}

#[tokio::test(start_paused = true)]
async fn an_unmet_quorum_fails_and_says_so() {
    let tokens = Tokens::default();
    let store = Arc::new(TestStore::default());
    let graph = race(
        branches(vec![
            ("openai", at_once("openai", 80, &tokens)),
            ("anthropic", fails(NodeError::Fatal("refused".to_string()))),
            ("local", fails(NodeError::Fatal("crashed".to_string()))),
        ]),
        ParallelGroupOptions::new().with_join(Join::quorum(2)),
        Arc::default(),
    );

    let (result, _) = run(&graph, store.clone()).await;

    assert_eq!(
        failed_message(result),
        "quorum of 2 not reached: 1 of 3 branches succeeded; branch 'anthropic': refused; branch 'local': crashed"
    );
    assert_eq!(store.latest("t").metadata.unwrap()["error_kind"], "fatal");
}

#[tokio::test(start_paused = true)]
async fn with_no_match_keep_succeeded_reduces_over_every_answer() {
    let tokens = Tokens::default();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let record = seen.clone();
    let graph = StateGraph::<Job>::new()
        .add_parallel_group_with_outcomes(
            "Race",
            branches(vec![
                ("openai", at_once("openai", 50, &tokens)),
                ("anthropic", at_once("anthropic", 70, &tokens)),
                ("local", at_once("local", 60, &tokens)),
            ]),
            move |entry: &Job, results: BranchOutcomes<Job>| {
                let best = results.succeeded().max_by_key(|j| j.n).cloned();
                record.lock().unwrap().push(results);
                best.unwrap_or_else(|| entry.clone())
            },
            ParallelGroupOptions::new()
                .with_join(Join::first_matching(|j: &Job| j.n >= 90))
                .keep_succeeded(),
        )
        .compile()
        .unwrap();

    let (result, events) = run(&graph, Arc::default()).await;

    assert_eq!(completed(result).trail, ["anthropic"]);
    let seen = seen.lock().unwrap();
    assert!(!seen[0].join_met());
    assert!(seen[0].deciding().is_empty());
    assert!(
        statuses(&seen[0])
            .iter()
            .all(|s| *s == BranchStatus::Succeeded)
    );
    assert!(ends(&events).contains(&"decided Race first_matching unmet []".to_string()));
}

#[tokio::test(start_paused = true)]
async fn with_no_match_fail_group_fails_the_group() {
    let tokens = Tokens::default();
    let store = Arc::new(TestStore::default());
    let graph = race(
        branches(vec![
            ("openai", at_once("openai", 50, &tokens)),
            ("anthropic", at_once("anthropic", 70, &tokens)),
        ]),
        ParallelGroupOptions::new().with_join(Join::first_matching(|j: &Job| j.n >= 90)),
        Arc::default(),
    );

    let (result, _) = run(&graph, store.clone()).await;

    assert_eq!(
        failed_message(result),
        "no branch matched the join (2 succeeded)"
    );
    assert_eq!(
        store.latest("t").metadata.unwrap()["error_kind"],
        "transient"
    );
}

#[tokio::test(start_paused = true)]
async fn a_race_inside_a_subgraph_reports_under_the_subgraph() {
    let tokens = Tokens::default();
    let child = race(
        branches(vec![
            ("openai", at_once("openai", 95, &tokens)),
            ("local", hangs("local", &tokens)),
        ]),
        ParallelGroupOptions::new().with_join(Join::first_success()),
        Arc::default(),
    );
    let parent = StateGraph::<Job>::new()
        .add_subgraph("Rca", child, Job::clone, |_: &Job, done: Job| done)
        .compile()
        .unwrap();

    let (result, events) = run(&parent, Arc::default()).await;

    assert_eq!(completed(result).trail, ["openai"]);
    let lines = ends(&events);
    assert!(lines.contains(&"decided Rca/Race first_success met [openai]".to_string()));
    assert!(lines.contains(&"completed Rca/Race/local Cancelled LostRace".to_string()));
}

// The join in the definition.

fn fixture(folder: &str, name: &str) -> String {
    let path = format!(
        "{}/../../schema/fixtures/{folder}/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn named(names: &[&str]) -> ParallelBranches<Job> {
    names
        .iter()
        .fold(ParallelBranches::new(), |branches, name| {
            branches.branch(*name, |job: &Job| Ok(job.clone()))
        })
}

fn keep(entry: &Job, _: BranchOutcomes<Job>) -> Job {
    entry.clone()
}

fn first(entry: &Job, _: Vec<Job>) -> Job {
    entry.clone()
}

/// The graph `joins.json` describes.
fn joins() -> StateGraph<Job> {
    StateGraph::<Job>::new()
        .add_parallel_group_with_outcomes(
            "Specialists",
            named(&["telemetry", "kubernetes", "database"]),
            keep,
            ParallelGroupOptions::new()
                .keep_succeeded()
                .with_branch_timeout(Duration::from_secs(45))
                .with_max_concurrency(2),
        )
        .add_parallel_group_with_outcomes(
            "Race",
            named(&["openai", "anthropic", "local"]),
            keep,
            ParallelGroupOptions::new()
                .with_join(
                    Join::first_matching(|j: &Job| j.n >= 90)
                        .described("validated, confidence >= 0.9"),
                )
                .keep_succeeded(),
        )
        .add_parallel_group_with(
            "Fastest",
            named(&["primary", "secondary"]),
            first,
            ParallelGroupOptions::new()
                .with_join(Join::first_success())
                .with_cancel_siblings_on_failure(false),
        )
        .add_parallel_group_with(
            "Majority",
            named(&["openai", "anthropic", "local"]),
            first,
            ParallelGroupOptions::new().with_join(Join::quorum(2)),
        )
        .add_parallel_group_with(
            "Plain",
            named(&["a", "b"]),
            first,
            ParallelGroupOptions::new(),
        )
        .add_edge("Specialists", "Race")
        .add_edge("Race", "Fastest")
        .add_edge("Fastest", "Majority")
        .add_edge("Majority", "Plain")
        .set_entry_point("Specialists")
}

#[test]
fn the_join_is_exported_as_the_shared_fixture_has_it() {
    let expected: Value = serde_json::from_str(&fixture("graph-export", "joins.json")).unwrap();
    let exported = serde_json::to_value(joins().compile().unwrap().to_definition()).unwrap();

    assert_eq!(exported, expected);
}

#[test]
fn the_joins_fixture_round_trips_and_validates() {
    let raw = fixture("graph-export", "joins.json");
    let original: Value = serde_json::from_str(&raw).unwrap();
    let definition: GraphDefinition = serde_json::from_str(&raw).unwrap();

    assert_eq!(serde_json::to_value(&definition).unwrap(), original);
    assert_eq!(definition.check_shape(), Ok(()));
    assert!(validate(&definition).is_valid());
}

#[test]
fn a_group_with_default_options_exports_no_join() {
    let definition = StateGraph::<Job>::new()
        .add_parallel_group_with(
            "Plain",
            named(&["a"]),
            first,
            ParallelGroupOptions::new().with_max_concurrency(1),
        )
        .to_definition();

    assert_eq!(definition.schema_version, "1.1.0");
    assert!(
        !serde_json::to_string(&definition)
            .unwrap()
            .contains("\"join\"")
    );
}

#[test]
fn an_invalid_quorum_is_reported_at_every_level() {
    let definition: GraphDefinition =
        serde_json::from_str(&fixture("graph-validation", "invalid-quorum.json")).unwrap();

    let result = validate(&definition);

    let errors: Vec<(&str, &str)> = result
        .errors
        .iter()
        .map(|e| (e.code, e.node.as_str()))
        .collect();
    assert_eq!(
        errors,
        [
            ("STRATA_ERR_INVALID_QUORUM", "Vote"),
            ("STRATA_ERR_INVALID_QUORUM", "Review/Ask")
        ]
    );
}

#[test]
fn compile_refuses_a_quorum_above_the_branch_count() {
    let compiled = StateGraph::<Job>::new()
        .add_parallel_group_with(
            "Vote",
            named(&["a", "b"]),
            first,
            ParallelGroupOptions::new().with_join(Join::quorum(3)),
        )
        .compile();

    match compiled {
        Err(StrataError::GraphInvalid(result)) => {
            assert!(
                result
                    .errors
                    .iter()
                    .any(|e| e.code == "STRATA_ERR_INVALID_QUORUM" && e.node == "Vote")
            );
        }
        _ => panic!("expected GraphInvalid"),
    }
}

fn misplaced(version: &str, node: &str) -> Result<(), String> {
    let definition: GraphDefinition = serde_json::from_str(&format!(
        r#"{{ "schema_version": "{version}", "entry_point": "G", "nodes": [ {node} ], "edges": [] }}"#
    ))
    .unwrap();
    definition.check_shape()
}

fn group_with(join: &str) -> String {
    format!(
        r#"{{ "name": "G", "metadata": null, "kind": "parallel_group", "branches": [ {{ "name": "a", "metadata": null }} ], "join": {join} }}"#
    )
}

#[test]
fn a_misplaced_join_is_not_a_graph_definition() {
    assert_eq!(
        misplaced(
            "1.1.0",
            &group_with(
                r#"{ "policy": "first_success", "count": null, "condition": null, "partial": "fail_group" }"#
            )
        ),
        Err("node 'G' has a join, which schema_version 1.1.0 doesn't have".to_string())
    );
    assert_eq!(
        misplaced(
            "1.2.0",
            &group_with(
                r#"{ "policy": "first_success", "count": 2, "condition": null, "partial": "fail_group" }"#
            )
        ),
        Err("node 'G' has a join with a count, which only a quorum has".to_string())
    );
    assert_eq!(
        misplaced(
            "1.2.0",
            &group_with(
                r#"{ "policy": "quorum", "count": 1, "condition": "x", "partial": "fail_group" }"#
            )
        ),
        Err("node 'G' has a join with a condition, which only first_matching has".to_string())
    );
    assert_eq!(
        misplaced(
            "1.2.0",
            r#"{ "name": "G", "metadata": null, "join": { "policy": "all", "count": null, "condition": null, "partial": "keep_succeeded" } }"#
        ),
        Err("node 'G' has a join but is neither a parallel_group nor a map".to_string())
    );
}
