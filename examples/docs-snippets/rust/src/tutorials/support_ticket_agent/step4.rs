//! The tutorial's `src/main.rs` after step 4: the tool, and a graph whose one node calls it. Nothing runs
//! it yet, so most imports and the graph go unused, as the page warns.
#![allow(dead_code, unused_imports, unused_variables)]

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::StateGraph;
use datalium_strata::history::{ChatMessage, HistoryStore, MessageRole};
use datalium_strata::prelude::*;
use datalium_strata::store::CheckpointStore;
use datalium_strata::streaming::StreamChunkType;
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use datalium_strata_memory::history::SqliteHistoryStore;
use serde::{Deserialize, Serialize};

const TICKET_ID: &str = "T-1001";
const DB_PATH: &str = "tickets.db";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ticket {
    ticket_id: String,
    subject: String,
    category: Option<String>,
    draft: Option<String>,
    status: String,
}

impl Ticket {
    fn new(ticket_id: &str, subject: &str) -> Self {
        Ticket {
            ticket_id: ticket_id.to_string(),
            subject: subject.to_string(),
            category: None,
            draft: None,
            status: "open".to_string(),
        }
    }
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

// docs:begin tutorials/support-ticket-agent/classify-node
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(ClassifyTicketTool));
    let tools = Arc::new(tools);

    let graph = Arc::new(
        StateGraph::<Ticket>::new()
            .add_async_node("Classify", move |state: Ticket, ctx: NodeContext| {
                let tools = tools.clone();
                async move {
                    let arguments = serde_json::json!({ "subject": state.subject }).to_string();
                    let category = tools
                        .dispatch_with("classify_ticket", &arguments, ToolCallContext::from(&ctx))
                        .await
                        .expect("classify_ticket failed");
                    Ticket {
                        category: Some(category),
                        ..state
                    }
                }
            })
            .compile()
            .expect("the graph is valid"),
    );

    Ok(())
}
// docs:end tutorials/support-ticket-agent/classify-node

#[test]
fn main_runs() {
    main().expect("the program runs");
}
