//! Proves the example agent actually runs end to end (Phase 8 exit
//! criterion: "both example agents run end-to-end locally and in CI"),
//! not just that it compiles.

use datalium_strata::history::MessageRole;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_demo_graph_pauses_classifies_and_resolves_the_ticket() {
    let (final_state, history) = strata_rust_agent::run_demo().await;

    assert_eq!(final_state.category.as_deref(), Some("billing"));
    assert_eq!(final_state.resolved_by.as_deref(), Some("demo-reviewer"));
    assert!(
        final_state
            .resolution
            .as_deref()
            .unwrap()
            .contains("billing")
    );

    assert_eq!(history.len(), 1);
    assert_eq!(history[0].role, MessageRole::Assistant);
    assert!(history[0].content.contains("billing"));
}
