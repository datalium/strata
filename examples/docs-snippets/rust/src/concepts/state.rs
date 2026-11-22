//! Samples for <https://stratamesh.dev/concepts/state/>.

// docs:begin concepts/state/cross-language
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireTransferState {
    invoice_generated: bool,
    amount: f64,
    currency: String,
    approved_by: Option<String>,
}

// docs:hide
#[tokio::test]
async fn cross_language() {
    use std::sync::Arc;

    use datalium_strata::checkpoint::GraphExecutionResult;
    use datalium_strata::graph::StateGraph;
    use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
    use serde_json::json;

    let graph = StateGraph::<WireTransferState>::new()
        // docs:show
        // A node returns a new struct with the fields it changes.
        .add_node("PrepareWireTransfer", |s: WireTransferState| {
            WireTransferState {
                invoice_generated: true,
                ..s
            }
        })
        // docs:hide
        .compile()
        .expect("graph is structurally valid");

    let initial = WireTransferState {
        invoice_generated: false,
        amount: 4200.50,
        currency: "USD".to_string(),
        approved_by: None,
    };
    let state = match graph
        .run(
            "state-wire-1",
            initial,
            Arc::new(InMemoryCheckpointStore::new()),
        )
        .await
    {
        GraphExecutionResult::Completed(state) => state,
        _ => panic!("expected the run to complete"),
    };
    assert!(state.invoice_generated);

    // The field names are the snake_case ones the C# record pins with [JsonPropertyName].
    assert_eq!(
        serde_json::to_value(&state).unwrap(),
        json!({ "invoice_generated": true, "amount": 4200.50, "currency": "USD", "approved_by": null })
    );
}
// docs:show
// docs:end concepts/state/cross-language
