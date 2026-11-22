//! Samples for <https://stratamesh.dev/guides/human-in-the-loop/pause-for-approval/>.

use std::sync::{Arc, Mutex};

// docs:begin guides/human-in-the-loop/pause-for-approval/boundary
use datalium_strata::StrataError;
use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::context::NodeContext;
use datalium_strata::graph::StateGraph;
use serde::{Deserialize, Serialize};

const TRANSFER_LIMIT: f64 = 10_000.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireTransferState {
    beneficiary: String,
    amount: f64,
    currency: String,
    invoice_generated: bool,
    status: String,
    approved_by: Option<String>,
    note: Option<String>,
}

// docs:hide
impl WireTransferState {
    fn new(beneficiary: &str, amount: f64) -> Self {
        Self {
            beneficiary: beneficiary.to_string(),
            amount,
            currency: "USD".to_string(),
            invoice_generated: false,
            status: "new".to_string(),
            approved_by: None,
            note: None,
        }
    }
}

/// Stands in for the payment client: it counts what was sent.
#[derive(Clone, Default)]
struct Bank(Arc<Mutex<Vec<String>>>);

impl Bank {
    fn send(&self, beneficiary: &str, amount: f64, currency: &str) -> String {
        let mut sent = self.0.lock().unwrap();
        sent.push(format!("{beneficiary} {amount:.2} {currency}"));
        format!("ref {:04}", sent.len())
    }

    fn sent(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

/// The page's file name, in the crate's directory, where `cargo test` runs. No other page uses it.
const DB_PATH: &str = "checkpoints.db";

/// The SQLite file and the files SQLite may leave next to it.
fn remove_database() {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{DB_PATH}{suffix}"));
    }
}

#[tokio::test]
async fn approve_and_reject() -> Result<(), Box<dyn std::error::Error>> {
    remove_database();
    let bank = Bank::default();
    let sent = bank.clone();
    // docs:show
    let graph = StateGraph::<WireTransferState>::new()
        .add_node("PrepareWireTransfer", |s: WireTransferState| {
            WireTransferState {
                invoice_generated: true,
                status: "awaiting approval".to_string(),
                ..s
            }
        })
        .add_interrupt_boundary_with_reject("PrepareWireTransfer", "CancelWireTransfer")
        .add_async_node(
            "ExecuteWireTransfer",
            move |s: WireTransferState, ctx: NodeContext| {
                let decision = ctx
                    .decision()
                    .expect("a transfer must not run without a decision");
                let approved_by = Some(decision.reviewer.clone());

                // The world may have changed while the thread waited. Check again before the irreversible step.
                if s.amount <= 0.0 || s.amount > TRANSFER_LIMIT {
                    return std::future::ready(WireTransferState {
                        status: "held: amount outside the limit".to_string(),
                        approved_by,
                        ..s
                    });
                }

                let reference = bank.send(&s.beneficiary, s.amount, &s.currency);
                std::future::ready(WireTransferState {
                    status: format!("sent ({reference})"),
                    approved_by,
                    ..s
                })
            },
        )
        .add_async_node(
            "CancelWireTransfer",
            |s: WireTransferState, ctx: NodeContext| {
                let decision = ctx.decision().expect("a rejection is a decision");
                std::future::ready(WireTransferState {
                    status: format!("cancelled by {}", decision.reviewer),
                    ..s
                })
            },
        )
        .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
        .compile()
        .expect("graph is structurally valid");
    // docs:end guides/human-in-the-loop/pause-for-approval/boundary

    // docs:begin guides/human-in-the-loop/pause-for-approval/open-store
    use datalium_strata_memory::checkpoint::SqliteCheckpointStore;

    let store = Arc::new(SqliteCheckpointStore::open("checkpoints.db")?);
    // docs:end guides/human-in-the-loop/pause-for-approval/open-store

    let mut paused = false;
    // docs:begin guides/human-in-the-loop/pause-for-approval/run
    let result = graph
        .run(
            "wire-acme-corp",
            WireTransferState::new("Acme Corp", 4200.50),
            store.clone(),
        )
        .await;
    if let GraphExecutionResult::Interrupted {
        thread_id,
        suspended_node,
        next_node,
        ..
    } = result
    {
        println!("{thread_id}: paused after {suspended_node}, continues at {next_node}");
        // docs:hide
        assert_eq!(suspended_node, "PrepareWireTransfer");
        assert_eq!(next_node, "ExecuteWireTransfer");
        paused = true;
        // docs:show
    }
    // docs:end guides/human-in-the-loop/pause-for-approval/run
    assert!(paused, "the run pauses at the boundary");

    let globex = graph
        .run(
            "wire-globex",
            WireTransferState::new("Globex Ltd", 9800.00),
            store.clone(),
        )
        .await;
    assert!(matches!(globex, GraphExecutionResult::Interrupted { .. }));
    assert!(
        sent.sent().is_empty(),
        "nothing is sent before a human answers"
    );
    // The process stops: nothing of either thread stays in memory.
    drop(store);

    // docs:begin guides/human-in-the-loop/pause-for-approval/find-paused
    use datalium_strata::store::{CheckpointQuery, ThreadQuery, ThreadState};

    let store = Arc::new(SqliteCheckpointStore::open("checkpoints.db")?);
    let query = ThreadQuery::default().with_states([ThreadState::Interrupted]);
    let page = store.list_threads(&query).await?;
    for thread in &page.threads {
        println!(
            "{}: waiting at {}, continues at {}",
            thread.thread_id,
            thread.waiting_at.join("/"),
            thread.next_node.as_deref().unwrap_or("-")
        );
    }
    // docs:end guides/human-in-the-loop/pause-for-approval/find-paused

    let mut ids: Vec<&str> = page.threads.iter().map(|t| t.thread_id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, ["wire-acme-corp", "wire-globex"]);
    for thread in &page.threads {
        assert_eq!(thread.waiting_at, ["PrepareWireTransfer"]);
        assert_eq!(thread.next_node.as_deref(), Some("ExecuteWireTransfer"));
    }

    // docs:begin guides/human-in-the-loop/pause-for-approval/show-reviewer
    use datalium_strata::store::CheckpointStore;

    let checkpoint = store
        .load("wire-acme-corp")
        .await?
        .expect("the thread has a checkpoint");
    let pending: WireTransferState = serde_json::from_value(checkpoint.frozen_state)?;
    println!(
        "Review: {} {} to {}",
        pending.amount, pending.currency, pending.beneficiary
    );
    // docs:end guides/human-in-the-loop/pause-for-approval/show-reviewer

    assert_eq!(pending.amount, 4200.50);
    assert!(pending.invoice_generated);
    assert_eq!(pending.status, "awaiting approval");

    let mut approved = false;
    // docs:begin guides/human-in-the-loop/pause-for-approval/approve
    let approval = HumanSignal::approve("jdoe@acme.io").with_override("amount", 4150.25);
    if let GraphExecutionResult::Completed(done) = graph
        .resume("wire-acme-corp", approval, store.clone())
        .await
    {
        println!(
            "{}: {} {}, approved by {}",
            done.status,
            done.amount,
            done.currency,
            done.approved_by.unwrap_or_default()
        );
        // docs:hide
        assert_eq!(done.status, "sent (ref 0001)");
        assert_eq!(done.amount, 4150.25);
        approved = true;
        // docs:show
    }
    // docs:end guides/human-in-the-loop/pause-for-approval/approve
    assert!(approved, "the approval completes the run");
    assert_eq!(sent.sent(), ["Acme Corp 4150.25 USD"]);

    let mut cancelled_seen = false;
    // docs:begin guides/human-in-the-loop/pause-for-approval/reject
    let rejection = HumanSignal::reject("mlee@acme.io")
        .with_override("note", "beneficiary not on the vendor list");
    if let GraphExecutionResult::Completed(cancelled) =
        graph.resume("wire-globex", rejection, store.clone()).await
    {
        println!(
            "{}: {}",
            cancelled.status,
            cancelled.note.unwrap_or_default()
        );
        // docs:hide
        assert_eq!(cancelled.status, "cancelled by mlee@acme.io");
        cancelled_seen = true;
        // docs:show
    }
    // docs:end guides/human-in-the-loop/pause-for-approval/reject
    assert!(cancelled_seen, "the rejection takes the reject route");
    assert_eq!(
        sent.sent().len(),
        1,
        "a rejected transfer never reaches the bank"
    );

    let mut refused = false;
    // docs:begin guides/human-in-the-loop/pause-for-approval/refused
    let again = HumanSignal::approve("jdoe@acme.io");
    if let GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) =
        graph.resume("wire-acme-corp", again, store.clone()).await
    {
        println!("Refused: {message}");
        // docs:hide
        assert_eq!(
            message,
            "checkpoint for thread 'wire-acme-corp' was already resumed"
        );
        refused = true;
        // docs:show
    }
    // docs:end guides/human-in-the-loop/pause-for-approval/refused
    assert!(refused, "a second answer is refused");
    assert_eq!(sent.sent().len(), 1);

    // The check after the boundary holds a corrected amount above the limit.
    graph
        .run(
            "wire-initech",
            WireTransferState::new("Initech", 900.0),
            store.clone(),
        )
        .await;
    let too_much = HumanSignal::approve("jdoe@acme.io").with_override("amount", 25_000.0);
    match graph.resume("wire-initech", too_much, store.clone()).await {
        GraphExecutionResult::Completed(held) => {
            assert_eq!(held.status, "held: amount outside the limit");
        }
        _ => panic!("expected the held transfer to complete"),
    }
    assert_eq!(sent.sent().len(), 1);

    // A string for the amount is refused, and the thread keeps waiting.
    graph
        .run(
            "wire-umbrella",
            WireTransferState::new("Umbrella", 100.0),
            store.clone(),
        )
        .await;
    let wrong_type = HumanSignal::approve("jdoe@acme.io").with_override("amount", "a lot");
    match graph
        .resume("wire-umbrella", wrong_type, store.clone())
        .await
    {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(
                message.starts_with("failed to rehydrate state for thread"),
                "{message}"
            );
        }
        _ => panic!("expected the string amount to be refused"),
    }
    let waiting = store.load("wire-umbrella").await?.expect("a checkpoint");
    assert!(waiting.resumed_at.is_none());

    drop(store);
    remove_database();
    Ok(())
}

#[tokio::test]
async fn approval_timeout() {
    // docs:begin guides/human-in-the-loop/pause-for-approval/timeout
    use chrono::Utc;
    use datalium_strata::wait::BoundaryOptions;
    use std::time::Duration;

    // docs:hide
    use datalium_strata::wait::WakeReason;
    use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;

    let store = Arc::new(InMemoryCheckpointStore::new());
    let graph = StateGraph::<WireTransferState>::new()
        .add_node("PrepareWireTransfer", |s: WireTransferState| {
            WireTransferState {
                invoice_generated: true,
                status: "awaiting approval".to_string(),
                ..s
            }
        })
        // docs:show
        .add_interrupt_boundary_with(
            "PrepareWireTransfer",
            BoundaryOptions::new()
                .with_reject("CancelWireTransfer")
                .with_timeout(Duration::from_secs(24 * 3600))
                .with_on_timeout("EscalateWireTransfer"),
        )
        // docs:hide ...
        .add_node("ExecuteWireTransfer", |s: WireTransferState| {
            WireTransferState {
                status: "sent".to_string(),
                ..s
            }
        })
        .add_node("CancelWireTransfer", |s: WireTransferState| {
            WireTransferState {
                status: "cancelled".to_string(),
                ..s
            }
        })
        // docs:show
        .add_node("EscalateWireTransfer", |s: WireTransferState| {
            WireTransferState {
                status: "escalated: no answer within 24 h".to_string(),
                ..s
            }
        })
        // docs:hide
        .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
        .compile()
        .expect("graph is structurally valid");

    let paused = graph
        .run(
            "wire-timeout",
            WireTransferState::new("Hooli", 500.0),
            store.clone(),
        )
        .await;
    assert!(matches!(paused, GraphExecutionResult::Interrupted { .. }));
    // docs:show

    // On a schedule:
    let report = graph
        .wake_due(store.clone(), Utc::now())
        .await
        .expect("the store can be read");
    for woken in &report.woken {
        println!("{}: {:?}", woken.thread_id, woken.reason);
    }
    // docs:end guides/human-in-the-loop/pause-for-approval/timeout

    // Within the 24 hours, the wake call leaves the thread waiting.
    assert!(report.woken.is_empty());

    // Once they have passed, it continues at the timeout route.
    let later = Utc::now() + chrono::Duration::hours(25);
    let report = graph
        .wake_due(store.clone(), later)
        .await
        .expect("the store can be read");
    assert_eq!(report.woken.len(), 1);
    let woken = &report.woken[0];
    assert_eq!(woken.thread_id, "wire-timeout");
    assert_eq!(woken.reason, WakeReason::Timeout);
    match &woken.result {
        GraphExecutionResult::Completed(state) => {
            assert_eq!(state.status, "escalated: no answer within 24 h");
        }
        _ => panic!("expected the timeout route to complete the run"),
    }
}
