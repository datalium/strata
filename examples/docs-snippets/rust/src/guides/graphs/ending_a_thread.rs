//! Samples for <https://stratamesh.dev/guides/graphs/ending-a-thread/>.
//!
//! Each sample starts with its own `use` lines, so each is in a module of its own.

use std::sync::Arc;
use std::time::Duration;

use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult, HumanSignal};
use datalium_strata::context::NodeContext;
use datalium_strata::graph::{CompiledGraph, StateGraph};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Transfer {
    payee: String,
    amount: i64,
    prepared: bool,
    executed: bool,
}

fn transfer(payee: &str, amount: i64) -> Transfer {
    Transfer {
        payee: payee.to_string(),
        amount,
        prepared: false,
        executed: false,
    }
}

/// `Prepare`, an interrupt boundary at `Prepare`, then `Execute`, which takes `execute`.
fn transfer_graph(execute: Duration) -> CompiledGraph<Transfer> {
    StateGraph::<Transfer>::new()
        .add_node("Prepare", |t: Transfer| Transfer {
            prepared: true,
            ..t
        })
        .add_async_node("Execute", move |t: Transfer, _: NodeContext| async move {
            tokio::time::sleep(execute).await;
            Transfer {
                executed: true,
                ..t
            }
        })
        .add_edge("Prepare", "Execute")
        .add_interrupt_boundary("Prepare")
        .compile()
        .expect("the transfer graph is valid")
}

/// A thread waiting for approval at `Prepare`.
async fn waiting(
    execute: Duration,
    thread_id: &str,
    payee: &str,
) -> (CompiledGraph<Transfer>, Arc<dyn CheckpointStore>) {
    let transfers = transfer_graph(execute);
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let result = transfers
        .run(thread_id, transfer(payee, 1200), store.clone())
        .await;
    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    (transfers, store)
}

/// A result as one line.
fn show(result: &GraphExecutionResult<Transfer>) -> String {
    match result {
        GraphExecutionResult::Cancelled {
            node,
            reason,
            stopped_at,
            ..
        } => format!(
            "Cancelled at {node}, reason {reason:?}, stopped at [{}]",
            stopped_at.join(", ")
        ),
        GraphExecutionResult::Failed(error) => format!("Failed: {error:?}"),
        GraphExecutionResult::Interrupted { suspended_node, .. } => {
            format!("Interrupted at {suspended_node}")
        }
        GraphExecutionResult::Completed(_) => "Completed".to_string(),
    }
}

mod end {
    // docs:begin guides/graphs/ending-a-thread/end
    use datalium_strata::checkpoint::{
        CancelReason, END_REASON_KEY, ENDED_BY_KEY, GraphExecutionResult, ThreadEnd,
    };
    use datalium_strata::context::RunConfig;

    // docs:hide
    use super::*;
    use datalium_strata::StrataError;
    use datalium_strata::checkpoint::END_REQUESTED;
    use datalium_strata::schema::CheckpointStatus;

    #[tokio::test]
    async fn end_a_waiting_thread() {
        let (transfers, store) = waiting(Duration::ZERO, "transfer-acme", "acme").await;
        // docs:show
        let result = transfers
            .end_with(
                "transfer-acme",
                ThreadEnd::by("oncall"),
                store.clone(),
                RunConfig::new().with_metadata("ticket", "OPS-7"),
            )
            .await;
        match &result {
            GraphExecutionResult::Cancelled {
                reason: CancelReason::Ended,
                node,
                stopped_at,
                ..
            } => {
                println!("Ended at {node} ({stopped_at:?})");
            }
            GraphExecutionResult::Failed(refused) => println!("Refused: {refused:?}"),
            _ => unreachable!("ending returns Cancelled or Failed"),
        }

        let checkpoint = store
            .load("transfer-acme")
            .await
            .unwrap()
            .expect("a checkpoint");
        let metadata = checkpoint.metadata.clone().unwrap_or_default();
        println!(
            "checkpoint: status {:?}, next {:?}, {END_REASON_KEY}={}, {ENDED_BY_KEY}={}, ticket={}",
            checkpoint.status,
            checkpoint.next_node,
            metadata[END_REASON_KEY],
            metadata[ENDED_BY_KEY],
            metadata["ticket"]
        );
        // docs:end guides/graphs/ending-a-thread/end

        assert_eq!(
            show(&result),
            "Cancelled at Prepare, reason Ended, stopped at [Prepare]"
        );
        assert_eq!(checkpoint.status, CheckpointStatus::Ended);
        assert_eq!(checkpoint.next_node, None);
        assert_eq!(metadata[END_REASON_KEY], END_REQUESTED);
        assert_eq!(metadata[ENDED_BY_KEY], "oncall");
        assert_eq!(metadata["ticket"], "OPS-7");

        // An answer that arrives after the end is refused, and writes nothing.
        let late = transfers
            .resume(
                "transfer-acme",
                HumanSignal::approve("alice"),
                store.clone(),
            )
            .await;
        assert!(matches!(
            late,
            GraphExecutionResult::Failed(StrataError::ExecutionAborted(message))
                if message == "thread 'transfer-acme' was ended: an ended thread can't be resumed"
        ));

        // Ending it again returns the same result, and the first caller's ended_by stays.
        let again = transfers
            .end_with(
                "transfer-acme",
                ThreadEnd::by("someone-else"),
                store.clone(),
                RunConfig::new(),
            )
            .await;
        assert_eq!(show(&again), show(&result));
        let checkpoint = store.load("transfer-acme").await.unwrap().unwrap();
        assert_eq!(checkpoint.metadata.unwrap()[ENDED_BY_KEY], "oncall");
    }
}

mod cancel {
    // docs:begin guides/graphs/ending-a-thread/cancel
    use datalium_strata::CancellationToken;

    // docs:hide
    use super::*;
    use datalium_strata::context::RunConfig;

    #[tokio::test]
    async fn cancel_a_run_instead() {
        let (transfers, store) =
            waiting(Duration::from_secs(30), "transfer-initech", "initech").await;
        // docs:show
        let token = CancellationToken::new();
        let cancel = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cancel.cancel(); // Execute is still running.
        });
        let cancelled = transfers
            .resume_with(
                "transfer-initech",
                HumanSignal::approve("alice"),
                store.clone(),
                RunConfig::new().with_cancellation(token),
            )
            .await;
        println!("cancelled: {}", show(&cancelled));

        // Later: run Execute again with resume, or give up on the thread.
        println!(
            "end cancelled: {}",
            show(&transfers.end("transfer-initech", store.clone()).await)
        );
        // docs:end guides/graphs/ending-a-thread/cancel

        assert_eq!(
            show(&cancelled),
            "Cancelled at Execute, reason Requested, stopped at [Execute]"
        );
        let checkpoint = store.load("transfer-initech").await.unwrap().unwrap();
        assert_eq!(
            format!("{:?}", checkpoint.status),
            "Ended",
            "the end after the cancel ends the thread"
        );
        assert_eq!(checkpoint.suspended_node.as_deref(), Some("Execute"));
    }
}
