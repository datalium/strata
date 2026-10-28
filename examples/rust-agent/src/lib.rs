//! Minimal example agent demonstrating `datalium-strata`'s Phase 2-6
//! primitives working together: a [`StateGraph`] (Phase 2) whose entry node
//! calls a `#[strata_tool]`-generated tool (Phase 5), streams progress
//! tokens over a [`StreamContext`] (Phase 3), and records to history via an
//! [`ExecutionContext`] handle (Phase 6) — then the graph pauses at a
//! human-in-the-loop interrupt boundary (Phase 4) before a human approves
//! and it resumes to completion.
//!
//! Scenario: a support-ticket triage agent classifies an incoming ticket,
//! pauses for human approval before escalating, then resolves it.
//!
//! `ClassifyTicket` is an async node (`design/17-Async-Execution.md`): it
//! awaits the tool dispatch and the stream directly, and reaches history
//! through its [`NodeContext`], which carries the store passed in
//! [`RunConfig::with_history`]. `ResolveTicket` is an ordinary sync node; the
//! two mix freely.

use std::collections::HashMap;
use std::sync::Arc;

use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::StateGraph;
use datalium_strata::history::{ChatMessage, HistoryStore, MessageRole};
use datalium_strata::prelude::*;
use datalium_strata::streaming::StreamContext;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use datalium_strata_memory::history::InMemoryHistoryStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TicketState {
    pub ticket_id: String,
    pub subject: String,
    pub category: Option<String>,
    pub resolved_by: Option<String>,
    pub resolution: Option<String>,
}

#[derive(Deserialize, StrataSchema)]
struct ClassifyArgs {
    subject: String,
}

#[strata_tool(
    name = "classify_ticket",
    description = "Classifies a support ticket subject line into a category."
)]
async fn classify_ticket(args: ClassifyArgs) -> Result<String, StrataError> {
    let subject = args.subject.to_lowercase();
    let category = if subject.contains("invoice") || subject.contains("bill") {
        "billing"
    } else if subject.contains("crash") || subject.contains("error") {
        "technical"
    } else {
        "general"
    };
    Ok(category.to_string())
}

/// Runs the full demo end to end and returns the resolved state plus
/// whatever ended up in history — used by both `main` (for the printed
/// narration) and the integration test (for assertions).
pub async fn run_demo() -> (TicketState, Vec<ChatMessage>) {
    let ticket_id = "T-1001";

    // ---- Shared runtime services (Phase 3/5/6 primitives) ----
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(ClassifyTicketTool));
    let tools = Arc::new(tools);

    let history_store: Arc<dyn HistoryStore> = Arc::new(InMemoryHistoryStore::new());

    let (stream_ctx, mut stream_rx) = StreamContext::channel("ClassifyTicket");
    let printer = tokio::spawn(async move {
        print!("  ");
        while let Some(chunk) = stream_rx.recv().await {
            print!("{}", chunk.content);
        }
        println!();
    });

    // ---- Build the graph (Phase 2) ----
    let classify_tools = Arc::clone(&tools);
    let classify_stream = stream_ctx.clone();

    let graph = StateGraph::<TicketState>::new()
        .add_async_node(
            "ClassifyTicket",
            move |state: TicketState, ctx: NodeContext| {
                let (tools, stream) = (Arc::clone(&classify_tools), classify_stream.clone());
                async move {
                    stream.emit_token("Classifying ticket").await.unwrap();
                    stream.emit_token("...").await.unwrap();

                    let raw_args = serde_json::json!({ "subject": state.subject }).to_string();
                    let category = tools
                        .dispatch("classify_ticket", &raw_args)
                        .await
                        .expect("classify_ticket tool call failed");

                    // The history store is async, whichever backend it is
                    // (design/19-Async-Queryable-Store.md): just await it.
                    ctx.history()
                        .expect("run_demo passes a history store")
                        .append(
                            ctx.thread_id(),
                            ChatMessage {
                                role: MessageRole::Assistant,
                                content: Arc::from(format!("Classified as '{category}'.").as_str()),
                                timestamp: chrono::Utc::now(),
                            },
                        )
                        .await
                        .expect("failed to record history");

                    TicketState {
                        category: Some(category),
                        ..state
                    }
                }
            },
        )
        .add_interrupt_boundary("ClassifyTicket")
        .add_node("ResolveTicket", |state: TicketState| TicketState {
            resolution: Some(format!(
                "Escalated to the {} team (approved by {}).",
                state
                    .category
                    .clone()
                    .unwrap_or_else(|| "general".to_string()),
                state
                    .resolved_by
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string())
            )),
            ..state
        })
        .add_edge("ClassifyTicket", "ResolveTicket")
        .compile()
        .expect("graph definition is structurally valid");

    // ---- Run to the interrupt boundary (Phase 4: Freeze/Flush) ----
    let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
    let config = RunConfig::new().with_history(Arc::clone(&history_store));
    let initial = TicketState {
        ticket_id: ticket_id.to_string(),
        subject: "My invoice total looks wrong this month".to_string(),
        category: None,
        resolved_by: None,
        resolution: None,
    };

    println!("Running ClassifyTicket...");
    let result = graph
        .run_with(ticket_id, initial, checkpoint_store.clone(), config.clone())
        .await;
    let GraphExecutionResult::Interrupted {
        suspended_node,
        next_node,
        ..
    } = result
    else {
        panic!("expected the graph to pause at the interrupt boundary");
    };
    println!("Paused after '{suspended_node}', awaiting human approval before '{next_node}'.");

    // ---- A human approves the escalation; resume (Evict/Resume) ----
    let mut field_overrides = HashMap::new();
    field_overrides.insert("resolved_by".to_string(), "demo-reviewer".to_string());
    let signal = HumanSignal {
        is_approved: true,
        reviewer_token: "demo-reviewer".to_string(),
        field_overrides,
        ..Default::default()
    };
    let result = graph
        .resume_with(ticket_id, signal, checkpoint_store.clone(), config)
        .await;
    let GraphExecutionResult::Completed(final_state) = result else {
        panic!("expected the graph to complete after resume");
    };
    println!("Resolved: {}", final_state.resolution.clone().unwrap());

    // The node closures (and their captured `classify_stream` clone) must be
    // dropped before awaiting `printer`, or its sender never closes and
    // `recv()` never returns `None`.
    drop(graph);
    drop(stream_ctx);
    printer.await.unwrap();

    // ---- Show what ended up in history (Phase 6) ----
    let history = history_store.get_history(ticket_id, 10).await.unwrap();
    println!("History for {ticket_id}:");
    for message in &history {
        println!("  [{:?}] {}", message.role, message.content);
    }

    (final_state, history)
}
