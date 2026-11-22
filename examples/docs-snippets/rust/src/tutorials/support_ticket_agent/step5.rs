//! The tutorial's `src/main.rs` after step 5: the stores, and a streaming run of the `Classify` node.
#![allow(dead_code, unused_imports)]

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // docs:begin tutorials/support-ticket-agent/run
    let approve = std::env::args().nth(1).as_deref() == Some("approve");

    // docs:hide ... the tools and the graph ...
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
    // docs:show

    if !approve {
        let _ = std::fs::remove_file(DB_PATH); // a new ticket starts from an empty file
    }

    let checkpoints = Arc::new(SqliteCheckpointStore::open(DB_PATH)?);
    let history = Arc::new(SqliteHistoryStore::open(DB_PATH)?);
    let config = RunConfig::new().with_history(history.clone());

    let ticket = Ticket::new(TICKET_ID, "My invoice total looks wrong this month");
    let (handle, mut chunks) =
        graph.run_streaming_with(TICKET_ID, ticket, checkpoints.clone(), config);
    while let Some(chunk) = chunks.recv().await {
        match chunk.chunk_type {
            StreamChunkType::NodeStart => println!("-> {}", chunk.node_name),
            StreamChunkType::Token => {
                print!("{}", chunk.content);
                std::io::stdout().flush()?;
            }
            _ => {}
        }
    }
    println!();
    println!("{}", describe(&handle.await?));
    // docs:end tutorials/support-ticket-agent/run
    Ok(())
}

fn describe(result: &GraphExecutionResult<Ticket>) -> String {
    match result {
        GraphExecutionResult::Completed(state) => format!(
            "Completed: category {}, status {}",
            state.category.as_deref().unwrap_or("-"),
            state.status
        ),
        GraphExecutionResult::Interrupted {
            suspended_node,
            next_node,
            ..
        } => {
            format!("Paused after {suspended_node}, before {next_node}.")
        }
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            format!("Failed: {message}")
        }
        GraphExecutionResult::Failed(error) => format!("Failed: {error:?}"),
        _ => "The run ended early.".to_string(),
    }
}

#[test]
fn main_runs() {
    crate::child::main_in_child(|| main().expect("the program runs"));
}
