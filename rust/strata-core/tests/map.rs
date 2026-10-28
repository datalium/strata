//! A map (`design/27-Map-Fan-Out.md`, SG-06), exercised through the public API
//! only: a compiled child graph run per item of a list read from the state,
//! with keys checked before anything starts, items named `<map>/<key>`,
//! `MapExpanded`, the group runner's limits, timeouts, joins and partial
//! results, items that wind down, and `kind: map` in the exported definition
//! (`schema/fixtures/graph-export/map.json`,
//! `graph-validation/map-template-errors.json`). Every test runs on tokio's
//! paused clock. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/MapTests.cs` — keep the two in step.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::checkpoint::{CheckpointStore, CheckpointStoreError, GraphExecutionResult};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{
    CompileOptions, CompiledGraph, NodeError, RecoveryPolicy, StateGraph,
};
use datalium_strata::map::MapOptions;
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind};
use datalium_strata::parallel::{
    BranchOutcomes, BranchStatus, Join, ParallelBranches, ParallelGroupOptions,
};
use datalium_strata::schema::{Checkpoint, CheckpointStatus, GraphDefinition};
use datalium_strata::streaming::StreamChunkType;
use datalium_strata::validate::validate;
use datalium_strata::{BoxFuture, CancellationToken, StrataError};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const PATIENCE: Duration = Duration::from_secs(10);

/// The parent's state: the items to fan out over, and what the reducer found.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Case {
    items: Vec<String>,
    found: Vec<String>,
}

/// An item's child state: the item, and the child nodes it passed through.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Probe {
    item: String,
    trail: Vec<String>,
}

fn case(items: &[&str]) -> Case {
    Case {
        items: items.iter().map(|item| item.to_string()).collect(),
        found: Vec::new(),
    }
}

fn passed(mut probe: Probe, node: &str) -> Probe {
    probe.trail.push(node.to_string());
    probe
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
    Box<dyn Fn(Probe, NodeContext) -> BoxFuture<'static, Result<Probe, NodeError>> + Send + Sync>;

fn assessed() -> Handler {
    Box::new(|probe, _| Box::pin(async move { Ok(passed(probe, "assess")) }))
}

/// A child graph: `assess` (fallible, `assess`), then `summarize`.
fn child_with(assess: Handler, options: CompileOptions) -> Arc<CompiledGraph<Probe>> {
    Arc::new(
        StateGraph::<Probe>::new()
            .add_async_fallible_node("assess", assess)
            .add_node("summarize", |probe: Probe| passed(probe, "summarize"))
            .add_edge("assess", "summarize")
            .compile_with(options)
            .unwrap(),
    )
}

fn child(assess: Handler) -> Arc<CompiledGraph<Probe>> {
    child_with(assess, CompileOptions::new())
}

type Seen = Arc<Mutex<Vec<BranchOutcomes<Probe>>>>;

/// A graph whose one node is the map `Investigate` over the case's items,
/// keyed by themselves; the reducer records each succeeded item's trail as
/// `item:node+node` and keeps the outcomes.
fn mapping(
    child: Arc<CompiledGraph<Probe>>,
    options: MapOptions<Probe>,
    seen: Option<Seen>,
) -> StateGraph<Case> {
    StateGraph::<Case>::new().add_map_with_outcomes(
        "Investigate",
        |c: &Case| c.items.clone(),
        |item: &String| item.clone(),
        child,
        |_: &Case, item: String| Probe {
            item,
            trail: Vec::new(),
        },
        move |c: &Case, results: BranchOutcomes<Probe>| {
            let found = results
                .succeeded()
                .map(|p| format!("{}:{}", p.item, p.trail.join("+")))
                .collect();
            if let Some(seen) = &seen {
                seen.lock().unwrap().push(results);
            }
            Case { found, ..c.clone() }
        },
        options,
    )
}

async fn drain(mut events: mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        all.push(event);
    }
    all
}

async fn run_with(
    graph: &CompiledGraph<Case>,
    start: Case,
    store: Arc<TestStore>,
    config: RunConfig,
) -> (GraphExecutionResult<Case>, Vec<RunEvent>) {
    let (observer, events) = RunEventChannel::new();
    let result = graph
        .run_with("t", start, store, config.with_observer(observer))
        .await;
    (result, drain(events).await)
}

async fn run(
    graph: &CompiledGraph<Case>,
    start: Case,
) -> (GraphExecutionResult<Case>, Vec<RunEvent>) {
    run_with(
        graph,
        start,
        Arc::new(TestStore::default()),
        RunConfig::new(),
    )
    .await
}

fn completed(result: GraphExecutionResult<Case>) -> Case {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(error) => panic!("expected Completed, got Failed({error:?})"),
        _ => panic!("expected Completed"),
    }
}

/// `(node, attempts, message)` of a node failure.
fn node_failed(result: GraphExecutionResult<Case>) -> (String, u32, String) {
    match result {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => (node, attempts, message),
        GraphExecutionResult::Failed(error) => panic!("expected NodeFailed, got {error:?}"),
        _ => panic!("expected NodeFailed"),
    }
}

/// The events that tell a map's story, one line each, in order: the same
/// lines as the .NET suite's `Story`.
fn story(events: &[RunEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| {
            let path = event.node_path.as_deref().unwrap_or_default();
            match &event.kind {
                RunEventKind::NodeStarted => Some(format!("node {path}")),
                RunEventKind::NodeCompleted { result, .. } => {
                    Some(format!("node done {path} {result:?}"))
                }
                RunEventKind::MapExpanded { keys } => {
                    Some(format!("expanded {path} [{}]", keys.join(", ")))
                }
                RunEventKind::BranchStarted => Some(format!("item {path}")),
                RunEventKind::BranchCompleted { result, reason, .. } => Some(format!(
                    "item done {path} {result:?}{}",
                    reason.map(|r| format!(" {r:?}")).unwrap_or_default()
                )),
                RunEventKind::JoinDecided {
                    policy,
                    met,
                    deciding,
                } => Some(format!(
                    "decided {path} {policy} {} [{}]",
                    if *met { "met" } else { "unmet" },
                    deciding.join(", ")
                )),
                _ => None,
            }
        })
        .collect()
}

fn index_of(story: &[String], line: &str) -> usize {
    story
        .iter()
        .position(|l| l == line)
        .unwrap_or_else(|| panic!("'{line}' is not in the story:\n{}", story.join("\n")))
}

fn has_map_expanded_or_items(events: &[RunEvent]) -> bool {
    events.iter().any(|event| {
        matches!(
            event.kind,
            RunEventKind::MapExpanded { .. } | RunEventKind::BranchStarted
        )
    })
}

// --- Items ---------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn an_empty_list_reduces_with_no_results() {
    let seen: Seen = Arc::default();
    let graph = mapping(child(assessed()), MapOptions::new(), Some(seen.clone()))
        .compile()
        .unwrap();

    let (result, events) = run(&graph, case(&[])).await;

    assert!(completed(result).found.is_empty());
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert!(seen[0].outcomes().is_empty());
    assert!(seen[0].join_met());
    assert_eq!(
        story(&events),
        [
            "node Investigate",
            "expanded Investigate []",
            "node done Investigate Committed"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn one_item_runs_its_child() {
    let graph = mapping(child(assessed()), MapOptions::new(), None)
        .compile()
        .unwrap();

    let (result, events) = run(&graph, case(&["h1"])).await;

    assert_eq!(completed(result).found, ["h1:assess+summarize"]);
    assert_eq!(
        story(&events),
        [
            "node Investigate",
            "expanded Investigate [h1]",
            "item Investigate/h1",
            "node Investigate/h1/assess",
            "node done Investigate/h1/assess Committed",
            "node Investigate/h1/summarize",
            "node done Investigate/h1/summarize Committed",
            "item done Investigate/h1 Succeeded",
            "node done Investigate Committed",
        ]
    );
}

/// An assess step that tracks the peak number in flight and the order items
/// started in, taking `delay(item)` on tokio's clock.
fn tracked(
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    started: Arc<Mutex<Vec<String>>>,
    delay: fn(&str) -> Duration,
) -> Handler {
    Box::new(move |probe, _| {
        let (in_flight, peak, started) = (in_flight.clone(), peak.clone(), started.clone());
        Box::pin(async move {
            started.lock().unwrap().push(probe.item.clone());
            let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(delay(&probe.item)).await;
            in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(passed(probe, "assess"))
        })
    })
}

#[tokio::test(start_paused = true)]
async fn many_items_respect_the_limit_start_in_list_order_and_reduce_in_list_order() {
    let (in_flight, peak, started) = (
        Arc::default(),
        Arc::new(AtomicUsize::new(0)),
        Arc::default(),
    );
    // Later items finish first, so completion order isn't list order.
    let assess = tracked(in_flight, peak.clone(), Arc::clone(&started), |item| {
        Duration::from_millis(if item == "a" { 60 } else { 20 })
    });
    let graph = mapping(
        child(assess),
        MapOptions::new().with_max_concurrency(2),
        None,
    )
    .compile()
    .unwrap();

    let (result, _) = run(&graph, case(&["a", "b", "c", "d", "e"])).await;

    assert_eq!(
        completed(result).found,
        [
            "a:assess+summarize",
            "b:assess+summarize",
            "c:assess+summarize",
            "d:assess+summarize",
            "e:assess+summarize"
        ]
    );
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    assert_eq!(*started.lock().unwrap(), ["a", "b", "c", "d", "e"]);
}

#[tokio::test(start_paused = true)]
async fn the_default_concurrency_limit_is_eight() {
    let peak = Arc::new(AtomicUsize::new(0));
    let assess = tracked(Arc::default(), peak.clone(), Arc::default(), |_| {
        Duration::from_millis(30)
    });
    let graph = mapping(child(assess), MapOptions::new(), None)
        .compile()
        .unwrap();
    let items: Vec<String> = (1..=12).map(|i| format!("h{i}")).collect();
    let items: Vec<&str> = items.iter().map(String::as_str).collect();

    let (result, _) = run(&graph, case(&items)).await;

    assert_eq!(completed(result).found.len(), 12);
    assert_eq!(MapOptions::<Probe>::DEFAULT_MAX_CONCURRENCY, 8);
    assert_eq!(peak.load(Ordering::SeqCst), 8);
}

#[tokio::test(start_paused = true)]
async fn the_plain_reducer_gets_the_childrens_states_in_list_order() {
    let graph = StateGraph::<Case>::new()
        .add_map(
            "Investigate",
            |c: &Case| c.items.clone(),
            |item: &String| item.clone(),
            child(assessed()),
            |_: &Case, item: String| Probe {
                item,
                trail: Vec::new(),
            },
            |c: &Case, probes: Vec<Probe>| Case {
                found: probes.into_iter().map(|p| p.item).collect(),
                ..c.clone()
            },
        )
        .compile()
        .unwrap();

    let (result, _) = run(&graph, case(&["x", "y"])).await;

    assert_eq!(completed(result).found, ["x", "y"]);
}

// --- Keys ----------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn bad_keys_fail_the_map_fatally_before_anything_starts() {
    let cases: [(&[&str], &str); 3] = [
        (
            &["h1", "h2", "h1"],
            "map 'Investigate': items 0 and 2 have the same key 'h1'",
        ),
        (&["h1", ""], "map 'Investigate': item 1 has an empty key"),
        (
            &["a/b"],
            "map 'Investigate': item 0 has the key 'a/b', which contains '/'",
        ),
    ];
    for (items, message) in cases {
        let ran = Arc::new(AtomicUsize::new(0));
        let counter = ran.clone();
        let assess: Handler = Box::new(move |probe, _| {
            counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(probe) })
        });
        let graph = mapping(child(assess), MapOptions::new(), None)
            .set_recovery_policy("Investigate", RecoveryPolicy::retry(3))
            .compile()
            .unwrap();

        let (result, events) = run(&graph, case(items)).await;

        assert_eq!(
            node_failed(result),
            ("Investigate".to_string(), 1, message.to_string())
        );
        assert_eq!(ran.load(Ordering::SeqCst), 0);
        assert!(!has_map_expanded_or_items(&events));
    }
}

#[tokio::test(start_paused = true)]
async fn more_items_than_max_items_fail_the_map_fatally() {
    let graph = mapping(child(assessed()), MapOptions::new().with_max_items(2), None)
        .set_recovery_policy("Investigate", RecoveryPolicy::retry(3))
        .compile()
        .unwrap();

    let (result, events) = run(&graph, case(&["a", "b", "c"])).await;

    let (_, attempts, message) = node_failed(result);
    assert_eq!(
        (attempts, message.as_str()),
        (1, "map 'Investigate' has 3 items, more than its limit of 2")
    );
    assert!(!has_map_expanded_or_items(&events));
}

#[test]
fn options_refuse_values_out_of_range() {
    let refused =
        |options: MapOptions<Probe>| match mapping(child(assessed()), options, None).compile() {
            Err(StrataError::CompilationFailed(message)) => message,
            Err(other) => panic!("expected CompilationFailed, got {other:?}"),
            Ok(_) => panic!("expected CompilationFailed, got a compiled graph"),
        };

    assert_eq!(
        refused(MapOptions::new().with_max_concurrency(0)),
        "map 'Investigate' has a max_concurrency of 0; it must be at least 1"
    );
    assert_eq!(
        refused(MapOptions::new().with_item_timeout(Duration::ZERO)),
        "map 'Investigate' has an item timeout of zero; it must be greater than zero"
    );
    assert_eq!(
        refused(MapOptions::new().with_max_items(0)),
        "map 'Investigate' has a max_items of 0; it must be at least 1"
    );
}

// --- Failures, partial results, timeouts, joins ----------------------------------

#[tokio::test(start_paused = true)]
async fn keep_succeeded_hands_every_outcome_to_the_reducer() {
    let seen: Seen = Arc::default();
    let assess: Handler = Box::new(|probe, _| {
        Box::pin(async move {
            if probe.item == "h2" {
                Err(NodeError::Fatal("no evidence".to_string()))
            } else {
                Ok(passed(probe, "assess"))
            }
        })
    });
    let graph = mapping(
        child(assess),
        MapOptions::new().keep_succeeded(),
        Some(seen.clone()),
    )
    .compile()
    .unwrap();

    let (result, events) = run(&graph, case(&["h1", "h2", "h3"])).await;

    assert_eq!(
        completed(result).found,
        ["h1:assess+summarize", "h3:assess+summarize"]
    );
    let seen = seen.lock().unwrap();
    let outcomes = &seen[0];
    let statuses: Vec<(&str, BranchStatus)> = outcomes
        .outcomes()
        .iter()
        .map(|o| (o.name.as_str(), o.status))
        .collect();
    assert_eq!(
        statuses,
        [
            ("h1", BranchStatus::Succeeded),
            ("h2", BranchStatus::Failed),
            ("h3", BranchStatus::Succeeded)
        ]
    );
    let error = outcomes.get("h2").unwrap().error.as_ref().unwrap();
    assert_eq!(
        error.message(),
        "failed at 'assess' after 1 attempt(s): no evidence"
    );
    assert!(!error.is_transient());
    assert!(!outcomes.join_met());
    assert!(story(&events).contains(&"item done Investigate/h2 Failed".to_string()));
}

#[tokio::test(start_paused = true)]
async fn a_transient_child_failure_fails_the_map_transiently_with_its_hint() {
    let store = Arc::new(TestStore::default());
    let assess: Handler = Box::new(|probe, _| {
        Box::pin(async move {
            if probe.item == "b" {
                Err(NodeError::transient_after(
                    "rate limited",
                    Duration::from_secs(5),
                ))
            } else {
                Ok(passed(probe, "assess"))
            }
        })
    });
    let graph = mapping(child(assess), MapOptions::new(), None)
        .compile()
        .unwrap();

    let (result, _) = run_with(&graph, case(&["a", "b"]), store.clone(), RunConfig::new()).await;

    assert_eq!(
        node_failed(result),
        (
            "Investigate".to_string(),
            1,
            "item 'b': failed at 'assess' after 1 attempt(s): rate limited".to_string()
        )
    );
    let checkpoint = store.latest("t");
    assert_eq!(checkpoint.status, CheckpointStatus::Failed);
    assert!(checkpoint.nested.is_none());
    let metadata = checkpoint.metadata.unwrap();
    assert_eq!(metadata["error_kind"], "transient");
    assert_eq!(metadata["strata.retry_after_ms"], "5000");
}

#[tokio::test(start_paused = true)]
async fn a_failing_item_winds_its_siblings_down() {
    let assess: Handler = Box::new(|probe, _| {
        Box::pin(async move {
            if probe.item == "bad" {
                tokio::time::sleep(Duration::from_millis(10)).await;
                return Err(NodeError::Fatal("boom".to_string()));
            }
            std::future::pending::<()>().await;
            Ok(probe)
        })
    });
    let graph = mapping(child(assess), MapOptions::new(), None)
        .compile()
        .unwrap();

    let (result, events) = run(&graph, case(&["slow", "bad"])).await;

    assert_eq!(
        node_failed(result).2,
        "item 'bad': failed at 'assess' after 1 attempt(s): boom"
    );
    let story = story(&events);
    let child_stopped = index_of(&story, "node done Investigate/slow/assess Cancelled");
    let item_stopped = index_of(&story, "item done Investigate/slow Cancelled SiblingFailed");
    assert!(child_stopped < item_stopped, "{}", story.join("\n"));
    assert!(item_stopped < index_of(&story, "node done Investigate Failed"));
}

#[tokio::test(start_paused = true)]
async fn an_item_timeout_times_out_only_that_item() {
    let seen: Seen = Arc::default();
    let assess: Handler = Box::new(|probe, _| {
        Box::pin(async move {
            let delay = if probe.item == "hung" {
                Duration::from_secs(3600)
            } else {
                Duration::from_secs(10)
            };
            tokio::time::sleep(delay).await;
            Ok(passed(probe, "assess"))
        })
    });
    let graph = mapping(
        child(assess),
        MapOptions::new()
            .with_item_timeout(Duration::from_secs(45))
            .keep_succeeded(),
        Some(seen.clone()),
    )
    .compile()
    .unwrap();

    let (result, events) = run(&graph, case(&["ok", "hung"])).await;

    assert_eq!(completed(result).found, ["ok:assess+summarize"]);
    let seen = seen.lock().unwrap();
    let hung = seen[0].get("hung").unwrap();
    assert_eq!(hung.status, BranchStatus::TimedOut);
    let error = hung.error.as_ref().unwrap();
    assert_eq!(error.message(), "item 'hung' timed out after 45000 ms");
    assert!(error.is_transient());
    let story = story(&events);
    assert!(
        index_of(&story, "node done Investigate/hung/assess Cancelled")
            < index_of(&story, "item done Investigate/hung Failed TimedOut"),
        "{}",
        story.join("\n")
    );
}

#[tokio::test(start_paused = true)]
async fn first_success_winds_the_losers_down() {
    let assess: Handler = Box::new(|probe, _| {
        Box::pin(async move {
            if probe.item == "slow" {
                std::future::pending::<()>().await;
            }
            Ok(passed(probe, "assess"))
        })
    });
    let graph = mapping(
        child(assess),
        MapOptions::new().with_join(Join::first_success()),
        None,
    )
    .compile()
    .unwrap();

    let (result, events) = run(&graph, case(&["slow", "fast"])).await;

    assert_eq!(completed(result).found, ["fast:assess+summarize"]);
    let story = story(&events);
    let decided = index_of(&story, "decided Investigate first_success met [fast]");
    assert!(index_of(&story, "item done Investigate/fast Succeeded") < decided);
    let child_stopped = index_of(&story, "node done Investigate/slow/assess Cancelled");
    let loser = index_of(&story, "item done Investigate/slow Cancelled LostRace");
    assert!(
        decided < child_stopped && child_stopped < loser,
        "{}",
        story.join("\n")
    );
}

#[tokio::test(start_paused = true)]
async fn an_empty_race_fails_fatally() {
    let graph = mapping(
        child(assessed()),
        MapOptions::new().with_join(Join::first_success()),
        None,
    )
    .set_recovery_policy("Investigate", RecoveryPolicy::retry(3))
    .compile()
    .unwrap();

    let (result, events) = run(&graph, case(&[])).await;

    let (_, attempts, message) = node_failed(result);
    assert_eq!((attempts, message.as_str()), (1, "no items to run"));
    assert!(story(&events).contains(&"decided Investigate first_success unmet []".to_string()));
}

// --- Composition, streaming, retries, cancellation, hops ---------------------------

#[tokio::test(start_paused = true)]
async fn a_map_inside_a_subgraph_names_paths_through_both() {
    let inner = Arc::new(
        mapping(child(assessed()), MapOptions::new(), None)
            .compile()
            .unwrap(),
    );
    let graph = StateGraph::<Case>::new()
        .add_subgraph("Outer", inner, |c: &Case| c.clone(), |_: &Case, c: Case| c)
        .compile()
        .unwrap();

    let (result, events) = run(&graph, case(&["h1"])).await;

    assert_eq!(completed(result).found, ["h1:assess+summarize"]);
    let story = story(&events);
    index_of(&story, "expanded Outer/Investigate [h1]");
    index_of(&story, "item Outer/Investigate/h1");
    index_of(&story, "node done Outer/Investigate/h1/summarize Committed");
}

#[tokio::test(start_paused = true)]
async fn item_chunks_stream_under_the_item_path() {
    let speaker = Arc::new(
        StateGraph::<Probe>::new()
            .add_async_streaming_node("speak", |probe: Probe, ctx: NodeContext| async move {
                ctx.stream()
                    .expect("a streaming node always has a stream")
                    .emit_token(&probe.item)
                    .await
                    .unwrap();
                probe
            })
            .compile()
            .unwrap(),
    );
    let graph = Arc::new(
        mapping(speaker, MapOptions::new().with_max_concurrency(1), None)
            .compile()
            .unwrap(),
    );

    let (handle, mut receiver) =
        graph
            .clone()
            .run_streaming("t", case(&["a", "b"]), Arc::new(TestStore::default()));
    let mut tokens = Vec::new();
    while let Some(chunk) = tokio::time::timeout(PATIENCE, receiver.recv())
        .await
        .unwrap()
    {
        if chunk.chunk_type == StreamChunkType::Token {
            tokens.push((chunk.node_name.to_string(), chunk.content.to_string()));
        }
    }

    assert!(matches!(
        handle.await.unwrap(),
        GraphExecutionResult::Completed(_)
    ));
    assert_eq!(
        tokens,
        [
            ("Investigate/a/speak".to_string(), "a".to_string()),
            ("Investigate/b/speak".to_string(), "b".to_string())
        ]
    );

    // A plain run refuses the map before reading any item.
    let (plain, events) = run(&graph, case(&["a"])).await;
    assert!(matches!(plain, GraphExecutionResult::Failed(_)));
    assert!(
        !events
            .iter()
            .any(|e| matches!(e.kind, RunEventKind::MapExpanded { .. }))
    );
}

#[tokio::test(start_paused = true)]
async fn a_retried_map_reruns_every_item() {
    let runs: Arc<Mutex<Vec<String>>> = Arc::default();
    let failed_once = Arc::new(AtomicUsize::new(0));
    let (log, once) = (runs.clone(), failed_once.clone());
    let assess: Handler = Box::new(move |probe, _| {
        log.lock().unwrap().push(probe.item.clone());
        let fail = probe.item == "b" && once.fetch_add(1, Ordering::SeqCst) == 0;
        Box::pin(async move {
            if fail {
                Err(NodeError::Transient("flaky".to_string()))
            } else {
                Ok(passed(probe, "assess"))
            }
        })
    });
    let graph = mapping(
        child(assess),
        MapOptions::new().with_max_concurrency(1),
        None,
    )
    .set_recovery_policy("Investigate", RecoveryPolicy::retry(1))
    .compile()
    .unwrap();

    let (result, events) = run(&graph, case(&["a", "b"])).await;

    assert_eq!(
        completed(result).found,
        ["a:assess+summarize", "b:assess+summarize"]
    );
    assert_eq!(*runs.lock().unwrap(), ["a", "b", "a", "b"]);
    let expansions: Vec<(String, u32)> = events
        .iter()
        .filter(|e| matches!(e.kind, RunEventKind::MapExpanded { .. }))
        .map(|e| {
            (
                e.node_path.as_deref().unwrap().to_string(),
                e.attempt.unwrap(),
            )
        })
        .collect();
    assert_eq!(
        expansions,
        [
            ("Investigate".to_string(), 1),
            ("Investigate".to_string(), 2)
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_run_winds_items_down_and_checkpoints_the_map() {
    let token = CancellationToken::new();
    let started = Arc::new(AtomicUsize::new(0));
    let (canceller, count) = (token.clone(), started.clone());
    let assess: Handler = Box::new(move |probe, _| {
        if count.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
            canceller.cancel();
        }
        Box::pin(async move {
            std::future::pending::<()>().await;
            Ok(probe)
        })
    });
    let graph = mapping(child(assess), MapOptions::new(), None)
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());

    let (result, events) = run_with(
        &graph,
        case(&["a", "b"]),
        store.clone(),
        RunConfig::new().with_cancellation(token),
    )
    .await;

    match result {
        GraphExecutionResult::Cancelled { node, .. } => assert_eq!(node, "Investigate"),
        _ => panic!("expected Cancelled"),
    }
    let checkpoint = store.latest("t");
    assert_eq!(
        (checkpoint.status, checkpoint.next_node.as_deref()),
        (CheckpointStatus::Cancelled, Some("Investigate"))
    );
    assert!(checkpoint.nested.is_none());
    let story = story(&events);
    for item in ["a", "b"] {
        assert!(
            index_of(
                &story,
                &format!("node done Investigate/{item}/assess Cancelled")
            ) < index_of(&story, &format!("item done Investigate/{item} Cancelled")),
            "{}",
            story.join("\n")
        );
    }
    assert!(
        index_of(&story, "item done Investigate/b Cancelled")
            < index_of(&story, "node done Investigate Cancelled")
    );
}

#[tokio::test(start_paused = true)]
async fn a_map_is_one_hop_and_each_item_counts_its_own() {
    let one_hop = || CompileOptions::new().with_max_node_hops(1);
    let graph = mapping(child(assessed()), MapOptions::new(), None)
        .compile_with(one_hop())
        .unwrap();
    let (result, _) = run(&graph, case(&["a", "b", "c"])).await;
    assert_eq!(completed(result).found.len(), 3);

    let graph = mapping(child_with(assessed(), one_hop()), MapOptions::new(), None)
        .compile_with(one_hop())
        .unwrap();
    let (tripped, events) = run(&graph, case(&["a"])).await;
    assert_eq!(
        node_failed(tripped).2,
        "item 'a': hit its hop limit: hop limit of 1 reached: node 'summarize' would be hop 2"
    );
    assert!(events.iter().any(|e| {
        matches!(e.kind, RunEventKind::HopLimitExceeded { .. })
            && e.node_path.as_deref() == Some("Investigate/a/summarize")
    }));
}

// --- Compile-time checks -----------------------------------------------------------

fn compilation_failure(graph: StateGraph<Case>) -> String {
    match graph.compile() {
        Err(StrataError::CompilationFailed(message)) => message,
        Err(other) => panic!("expected CompilationFailed, got {other:?}"),
        Ok(_) => panic!("expected CompilationFailed, got a compiled graph"),
    }
}

#[test]
fn a_child_with_an_interrupt_boundary_is_refused_at_compile_time() {
    let direct = Arc::new(
        StateGraph::<Probe>::new()
            .add_node("assess", |p: Probe| p)
            .add_interrupt_boundary("assess")
            .compile()
            .unwrap(),
    );
    assert_eq!(
        compilation_failure(mapping(direct.clone(), MapOptions::new(), None)),
        "map 'Investigate' runs a graph with an interrupt boundary at 'assess'; a map item can't freeze"
    );

    let nested = Arc::new(
        StateGraph::<Probe>::new()
            .add_node("assess", |p: Probe| p)
            .add_subgraph(
                "Review",
                direct,
                |p: &Probe| p.clone(),
                |_: &Probe, p: Probe| p,
            )
            .add_edge("assess", "Review")
            .compile()
            .unwrap(),
    );
    assert_eq!(
        compilation_failure(mapping(nested, MapOptions::new(), None)),
        "map 'Investigate' runs a graph with an interrupt boundary at 'Review/assess'; a map item can't freeze"
    );

    // A boundary name that isn't a node never triggers.
    let stray = Arc::new(
        StateGraph::<Probe>::new()
            .add_node("assess", |p: Probe| p)
            .add_interrupt_boundary("elsewhere")
            .compile()
            .unwrap(),
    );
    assert!(mapping(stray, MapOptions::new(), None).compile().is_ok());
}

#[test]
fn an_invalid_map_quorum_is_refused_at_compile_time() {
    let invalid = mapping(
        child(assessed()),
        MapOptions::new().with_join(Join::quorum(0)),
        None,
    );
    match invalid.compile() {
        Err(StrataError::GraphInvalid(result)) => {
            assert_eq!(result.errors.len(), 1);
            let error = &result.errors[0];
            assert_eq!(
                (error.code, error.node.as_str()),
                ("STRATA_ERR_INVALID_QUORUM", "Investigate")
            );
            assert_eq!(
                error.message,
                "Map 'Investigate' has a quorum of 0, which isn't at least 1."
            );
        }
        Err(other) => panic!("expected GraphInvalid, got {other:?}"),
        Ok(_) => panic!("expected GraphInvalid, got a compiled graph"),
    }

    // Above the item count is decided at run time, not refused.
    assert!(
        mapping(
            child(assessed()),
            MapOptions::new().with_join(Join::quorum(50)),
            None
        )
        .compile()
        .is_ok()
    );
}

// --- The definition ------------------------------------------------------------------

fn fixture(folder: &str, name: &str) -> String {
    let path = format!(
        "{}/../../schema/fixtures/{folder}/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn same(probe: Probe) -> Probe {
    probe
}

/// The graph `map.json` describes.
fn map_fixture() -> StateGraph<Case> {
    let dig = Arc::new(
        StateGraph::<Probe>::new()
            .add_node("Dig", same)
            .compile()
            .unwrap(),
    );
    let template = Arc::new(
        StateGraph::<Probe>::new()
            .add_node("Assess", same)
            .add_parallel_group(
                "Probe",
                ParallelBranches::new()
                    .branch("logs", |p: &Probe| Ok(p.clone()))
                    .branch("metrics", |p: &Probe| Ok(p.clone())),
                |p: &Probe, _: Vec<Probe>| p.clone(),
            )
            .add_subgraph(
                "Deep",
                dig,
                |p: &Probe| p.clone(),
                |p: &Probe, _: Probe| p.clone(),
            )
            .add_edge("Assess", "Probe")
            .add_edge("Probe", "Deep")
            .compile()
            .unwrap(),
    );
    let check = Arc::new(
        StateGraph::<Probe>::new()
            .add_node("Check", same)
            .compile()
            .unwrap(),
    );
    let items = |c: &Case| c.items.clone();
    let key = |item: &String| item.clone();
    let input = |_: &Case, item: String| Probe {
        item,
        trail: Vec::new(),
    };

    StateGraph::<Case>::new()
        .add_node("Generate", |c: Case| c)
        .add_map(
            "Investigate",
            items,
            key,
            template,
            input,
            |c: &Case, _: Vec<Probe>| c.clone(),
        )
        .set_node_metadata(
            "Investigate",
            [("key", "hypothesis id"), ("items", "hypotheses")],
        )
        .add_map_with_outcomes(
            "Confirm",
            items,
            key,
            check,
            input,
            |c: &Case, _: BranchOutcomes<Probe>| c.clone(),
            MapOptions::new()
                .with_join(Join::first_matching(|_: &Probe| true).described("confidence >= 0.9"))
                .keep_succeeded()
                .with_max_concurrency(3)
                .with_item_timeout(Duration::from_secs(30))
                .with_max_items(5),
        )
        .add_parallel_group_with(
            "Notify",
            ParallelBranches::new()
                .branch("email", |c: &Case| Ok(c.clone()))
                .branch("chat", |c: &Case| Ok(c.clone())),
            |c: &Case, _: Vec<Case>| c.clone(),
            ParallelGroupOptions::new().with_join(Join::first_success()),
        )
        .add_edge("Generate", "Investigate")
        .add_edge("Investigate", "Confirm")
        .add_edge("Confirm", "Notify")
}

#[test]
fn the_map_is_exported_as_the_shared_fixture_has_it() {
    let expected: serde_json::Value =
        serde_json::from_str(&fixture("graph-export", "map.json")).unwrap();
    let exported = serde_json::to_value(map_fixture().compile().unwrap().to_definition()).unwrap();

    assert_eq!(exported, expected);
}

#[test]
fn the_map_fixture_round_trips_and_validates() {
    let raw: serde_json::Value =
        serde_json::from_str(&fixture("graph-export", "map.json")).unwrap();
    let definition: GraphDefinition = serde_json::from_value(raw.clone()).unwrap();

    assert_eq!(serde_json::to_value(&definition).unwrap(), raw);
    assert_eq!(definition.check_shape(), Ok(()));
    assert!(validate(&definition).is_valid());
}

#[test]
fn check_shape_refuses_a_misplaced_map() {
    let shape_of = |value: serde_json::Value| {
        serde_json::from_value::<GraphDefinition>(value)
            .unwrap()
            .check_shape()
    };
    let map: serde_json::Value =
        serde_json::from_str(&fixture("graph-export", "map.json")).unwrap();

    let mut old = map.clone();
    old["schema_version"] = "1.2.0".into();
    assert_eq!(
        shape_of(old),
        Err("node 'Investigate' is a map, which schema_version 1.2.0 doesn't have".to_string())
    );

    let mut branches = map;
    branches["nodes"][1]["branches"] = serde_json::json!([]);
    assert_eq!(
        shape_of(branches),
        Err("node 'Investigate' is a map but has both branches and graph".to_string())
    );
}

#[test]
fn validation_names_template_nodes_with_a_star() {
    let definition: GraphDefinition =
        serde_json::from_str(&fixture("graph-validation", "map-template-errors.json")).unwrap();
    let result = validate(&definition);

    let summary: Vec<(&str, &str, Option<&str>)> = result
        .errors
        .iter()
        .map(|e| (e.code, e.node.as_str(), e.related_node.as_deref()))
        .collect();
    assert_eq!(
        summary,
        [
            ("STRATA_ERR_INVALID_QUORUM", "Vote", None),
            ("STRATA_ERR_ORPHAN", "Investigate/*/Stray", None),
            (
                "STRATA_ERR_UNGUARDED_CYCLE",
                "Review/Fan/*/Loop",
                Some("Review/Fan/*/Read")
            ),
        ]
    );
}
