//! Samples for <https://stratamesh.dev/concepts/threads-and-checkpoints/>.

use std::sync::Arc;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::graph::StateGraph;
use datalium_strata::schema::CheckpointStatus;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    approved: bool,
}

// docs:begin concepts/threads-and-checkpoints/query
use datalium_strata::store::{CheckpointQuery, ThreadQuery, ThreadState};

// docs:hide
#[tokio::test]
async fn query() -> Result<(), Box<dyn std::error::Error>> {
    let graph = StateGraph::<Order>::new()
        .add_node("Review", |s: Order| s)
        .add_interrupt_boundary("Review")
        .add_node("Ship", |s: Order| s)
        .add_edge("Review", "Ship")
        .compile()
        .expect("graph is structurally valid");
    let store = Arc::new(InMemoryCheckpointStore::new());
    let result = graph
        .run("order-1001", Order { approved: false }, store.clone())
        .await;
    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));

    // docs:show
    let query = ThreadQuery::default().with_states([ThreadState::Interrupted]);
    let page = store.list_threads(&query).await?;
    for thread in &page.threads {
        println!("{} waits at {:?}", thread.thread_id, thread.suspended_node);
    }

    let log = store.get_log("order-1001", None, 100).await?;
    for entry in &log.entries {
        println!("#{} {:?}", entry.sequence, entry.checkpoint.status);
    }
    // docs:hide

    assert_eq!(page.threads.len(), 1);
    assert_eq!(page.threads[0].thread_id, "order-1001");
    assert_eq!(page.threads[0].suspended_node.as_deref(), Some("Review"));
    assert_eq!(log.entries.len(), 1);
    assert_eq!(
        log.entries[0].checkpoint.status,
        CheckpointStatus::Interrupted
    );
    Ok(())
}
// docs:show
// docs:end concepts/threads-and-checkpoints/query
