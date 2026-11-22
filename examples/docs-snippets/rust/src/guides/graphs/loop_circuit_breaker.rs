//! Samples for <https://stratamesh.dev/guides/graphs/loop-circuit-breaker/>.
//!
//! The page shows the whole program, `main` included, so the first sample is a `main` that the test calls.

// docs:begin guides/graphs/loop-circuit-breaker/compile
use std::sync::Arc;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult, HumanSignal};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CompileOptions, StateGraph};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Essay {
    min_score: u32,
    revision: u32,
    score: u32,
    published: bool,
}

/// A stand-in for a model call. It never scores above 7.
fn review_score(essay: &Essay) -> u32 {
    (4 + essay.revision).min(7)
}

#[tokio::main]
async fn main() -> Result<(), StrataError> {
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());

    let graph = StateGraph::<Essay>::new()
        .add_node("Draft", |mut essay: Essay| {
            essay.revision += 1;
            essay
        })
        .add_node("Review", |mut essay: Essay| {
            essay.score = review_score(&essay);
            essay
        })
        .add_node("Publish", |mut essay: Essay| {
            essay.published = true;
            essay
        })
        .add_edge("Draft", "Review")
        .add_conditional_edge("Review", "Draft", |essay: &Essay| {
            essay.score < essay.min_score
        })
        .add_edge("Review", "Publish")
        .compile_with(CompileOptions::new().with_max_node_hops(20))?;

    // docs:hide ...
    go_on(&graph, store).await;
    // docs:show
    Ok(())
}
// docs:end guides/graphs/loop-circuit-breaker/compile

#[test]
fn loop_circuit_breaker() {
    main().expect("the samples run");
}

async fn go_on(
    graph: &datalium_strata::graph::CompiledGraph<Essay>,
    store: Arc<dyn CheckpointStore>,
) {
    let mut tripped = None;
    // docs:begin guides/graphs/loop-circuit-breaker/detect
    let essay = Essay {
        min_score: 8,
        ..Default::default()
    };
    match graph.run("essay-1", essay, store.clone()).await {
        GraphExecutionResult::Completed(essay) => {
            println!("Published after {} revisions", essay.revision);
        }
        GraphExecutionResult::Failed(StrataError::HopLimitExceeded { node, hops, limit }) => {
            println!("Stopped: node = {node}, hops = {hops}, limit = {limit}");
            // docs:hide
            tripped = Some((node, hops, limit));
            // docs:show
        }
        GraphExecutionResult::Failed(other) => println!("Failed: {other:?}"),
        _ => {}
    }
    // docs:end guides/graphs/loop-circuit-breaker/detect
    assert_eq!(tripped, Some(("Draft".to_string(), 20, 20)));

    // docs:begin guides/graphs/loop-circuit-breaker/checkpoint
    let checkpoint = store
        .load("essay-1")
        .await
        .expect("the store works")
        .expect("a checkpoint");
    let metadata = checkpoint.metadata.clone().unwrap_or_default();
    println!(
        "{:?} at {}",
        checkpoint.status,
        checkpoint.next_node.as_deref().unwrap_or("-")
    );
    println!("error: {}", metadata["error"]);
    println!(
        "error_kind = {}, strata.hops = {}",
        metadata["error_kind"], metadata["strata.hops"]
    );
    println!("revision: {}", checkpoint.frozen_state["revision"]);
    // docs:end guides/graphs/loop-circuit-breaker/checkpoint
    assert_eq!(format!("{:?}", checkpoint.status), "Failed");
    assert_eq!(checkpoint.next_node.as_deref(), Some("Draft"));
    assert_eq!(
        metadata["error"],
        "hop limit of 20 reached: node 'Draft' would be hop 21"
    );
    assert_eq!(metadata["error_kind"], "hop_limit");
    assert_eq!(metadata["strata.hops"], "20");
    assert!(!metadata.contains_key("attempts"));
    assert_eq!(checkpoint.frozen_state["revision"], json!(10));

    // docs:begin guides/graphs/loop-circuit-breaker/resume
    // As it is: it trips again before any node runs.
    let again = graph
        .resume("essay-1", HumanSignal::approve("editor"), store.clone())
        .await;
    if let GraphExecutionResult::Failed(StrataError::HopLimitExceeded { node, hops, limit }) = again
    {
        println!("Tripped again: node = {node}, hops = {hops}, limit = {limit}");
        // docs:hide
        assert_eq!((node.as_str(), hops, limit), ("Draft", 20, 20));
        // docs:show
    }

    // Lower the bar, and allow ten more hops for this resume.
    let signal = HumanSignal::approve("editor").with_override("min_score", json!(7));
    let config = RunConfig::new().with_max_node_hops(30);
    if let GraphExecutionResult::Completed(essay) = graph
        .resume_with("essay-1", signal, store.clone(), config)
        .await
    {
        println!("Published after {} revisions", essay.revision);
        // docs:hide
        assert_eq!(essay.revision, 11);
        assert!(essay.published);
        return;
        // docs:show
    }
    // docs:end guides/graphs/loop-circuit-breaker/resume
    panic!("the resume with a higher limit should complete");
}
