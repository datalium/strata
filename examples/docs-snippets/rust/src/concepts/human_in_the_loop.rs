//! Samples for <https://stratamesh.dev/concepts/human-in-the-loop/>.

use std::sync::{Arc, Mutex};

use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::context::NodeContext;
use datalium_strata::graph::StateGraph;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireTransferState {
    beneficiary: String,
    amount: f64,
    currency: String,
    invoice_generated: bool,
    status: String,
    approved_by: Option<String>,
}

impl WireTransferState {
    fn new(beneficiary: &str, amount: f64) -> Self {
        Self {
            beneficiary: beneficiary.to_string(),
            amount,
            currency: "USD".to_string(),
            invoice_generated: false,
            status: "new".to_string(),
            approved_by: None,
        }
    }
}

/// The irreversible side effect, kept in memory so the test can see what was sent.
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

#[tokio::test]
async fn wire_transfer() {
    let bank = Bank::default();
    let sent = bank.clone();
    let store = Arc::new(InMemoryCheckpointStore::new());

    // docs:begin concepts/human-in-the-loop/wire-transfer
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
                let reference = bank.send(&s.beneficiary, s.amount, &s.currency);
                std::future::ready(WireTransferState {
                    status: format!("sent ({reference})"),
                    approved_by: Some(decision.reviewer.clone()),
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

    // Runs PrepareWireTransfer, saves the thread, and returns Interrupted.
    let result = graph
        .run(
            "wire-acme-corp",
            WireTransferState::new("Acme Corp", 4200.50),
            store.clone(),
        )
        .await;

    // Later, in this process or another one: approve, and correct the amount.
    let approval = HumanSignal::approve("jdoe@acme.io").with_override("amount", 4150.25);
    let resumed = graph
        .resume("wire-acme-corp", approval, store.clone())
        .await;
    // docs:end concepts/human-in-the-loop/wire-transfer

    match result {
        GraphExecutionResult::Interrupted {
            suspended_node,
            next_node,
            frozen_state,
            ..
        } => {
            assert_eq!(suspended_node, "PrepareWireTransfer");
            assert_eq!(next_node, "ExecuteWireTransfer");
            assert!(frozen_state.invoice_generated);
        }
        _ => panic!("expected the run to pause at the boundary"),
    }
    match resumed {
        GraphExecutionResult::Completed(state) => {
            assert_eq!(state.amount, 4150.25);
            assert_eq!(state.approved_by.as_deref(), Some("jdoe@acme.io"));
            assert_eq!(state.status, "sent (ref 0001)");
        }
        _ => panic!("expected the approval to complete the run"),
    }
    assert_eq!(sent.sent(), ["Acme Corp 4150.25 USD"]);

    // A rejection takes the reject route, and the transfer node never runs.
    let paused = graph
        .run(
            "wire-globex",
            WireTransferState::new("Globex Ltd", 9800.00),
            store.clone(),
        )
        .await;
    assert!(matches!(paused, GraphExecutionResult::Interrupted { .. }));
    let rejected = graph
        .resume("wire-globex", HumanSignal::reject("mlee@acme.io"), store)
        .await;
    match rejected {
        GraphExecutionResult::Completed(state) => {
            assert_eq!(state.status, "cancelled by mlee@acme.io")
        }
        _ => panic!("expected the rejection to complete the run"),
    }
    assert_eq!(sent.sent().len(), 1);
}
