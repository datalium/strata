//! Samples for <https://stratamesh.dev/concepts/graphs/>.

use std::sync::Arc;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::graph::StateGraph;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderState {
    item: String,
    quantity: u32,
    unit_price: f64,
    subtotal: Option<f64>,
    discount: Option<f64>,
    total: Option<f64>,
    shipping_note: Option<String>,
    receipt: Option<String>,
}

fn order(quantity: u32) -> OrderState {
    OrderState {
        item: "widget".to_string(),
        quantity,
        unit_price: 9.50,
        subtotal: None,
        discount: None,
        total: None,
        shipping_note: None,
        receipt: None,
    }
}

#[tokio::test]
async fn branching() {
    // docs:begin concepts/graphs/branching
    let graph = StateGraph::<OrderState>::new()
        .add_node("CalculateSubtotal", |state: OrderState| OrderState {
            subtotal: Some(state.quantity as f64 * state.unit_price),
            ..state
        })
        // docs:hide ... ApplyDiscount, TagBulkShipping, TagStandardShipping and FormatReceipt
        .add_node("ApplyDiscount", |state: OrderState| {
            let subtotal = state.subtotal.expect("CalculateSubtotal ran");
            let discount = if subtotal >= 100.0 {
                subtotal * 0.10
            } else {
                0.0
            };
            OrderState {
                discount: Some(discount),
                total: Some(subtotal - discount),
                ..state
            }
        })
        .add_node("TagBulkShipping", |state: OrderState| OrderState {
            shipping_note: Some("bulk order, ships via freight".to_string()),
            ..state
        })
        .add_node("TagStandardShipping", |state: OrderState| OrderState {
            shipping_note: Some("standard shipping".to_string()),
            ..state
        })
        .add_node("FormatReceipt", |state: OrderState| OrderState {
            receipt: Some(format!("{}x {}", state.quantity, state.item)),
            ..state
        })
        // docs:show
        .set_entry_point("CalculateSubtotal")
        .add_edge("CalculateSubtotal", "ApplyDiscount")
        .add_conditional_edge("ApplyDiscount", "TagBulkShipping", |s: &OrderState| {
            s.quantity >= 10
        })
        .add_edge("ApplyDiscount", "TagStandardShipping")
        .add_edge("TagBulkShipping", "FormatReceipt")
        .add_edge("TagStandardShipping", "FormatReceipt")
        .compile()
        .expect("graph is structurally valid");
    // docs:end concepts/graphs/branching

    let store = Arc::new(InMemoryCheckpointStore::new());
    for (quantity, expected) in [
        (12, "bulk order, ships via freight"),
        (3, "standard shipping"),
    ] {
        let thread_id = format!("order-{quantity}");
        match graph.run(&thread_id, order(quantity), store.clone()).await {
            GraphExecutionResult::Completed(state) => {
                assert_eq!(state.shipping_note.as_deref(), Some(expected));
                assert!(state.receipt.is_some());
            }
            GraphExecutionResult::Failed(err) => panic!("the run failed: {err:?}"),
            _ => panic!("the run paused, but this graph has nothing that pauses"),
        }
    }
}
