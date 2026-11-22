//! The tutorial's `src/main.rs` after step 7: `DraftReply` streams and records the reply, and the run
//! pauses after it.
#![allow(unused_imports)]

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
    let approve = std::env::args().nth(1).as_deref() == Some("approve");

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
            // docs:begin tutorials/support-ticket-agent/draft-reply-node
            .add_async_node("DraftReply", |state: Ticket, ctx: NodeContext| async move {
                let mut draft = String::new();
                for token in draft_tokens(state.category.as_deref().unwrap_or("general")) {
                    tokio::time::sleep(Duration::from_millis(50)).await; // stands in for a model's latency
                    if let Some(stream) = ctx.stream() {
                        stream.emit_token(&token).await.expect("the stream is open");
                    }
                    draft.push_str(&token);
                }

                let history = ctx.history().expect("the run has a history store");
                let now = chrono::Utc::now();
                for (role, content) in [
                    (MessageRole::User, state.subject.as_str()),
                    (MessageRole::Assistant, draft.as_str()),
                ] {
                    let message = ChatMessage {
                        role,
                        content: Arc::from(content),
                        timestamp: now,
                    };
                    history
                        .append(ctx.thread_id(), message)
                        .await
                        .expect("history write failed");
                }
                Ticket {
                    draft: Some(draft),
                    ..state
                }
            })
            .add_edge("Classify", "DraftReply")
            // docs:end tutorials/support-ticket-agent/draft-reply-node
            // docs:begin tutorials/support-ticket-agent/pause
            .add_interrupt_boundary("DraftReply")
            .add_async_node("SendReply", |state: Ticket, ctx: NodeContext| {
                let reviewer = &ctx
                    .decision()
                    .expect("SendReply runs after an approval")
                    .reviewer;
                std::future::ready(Ticket {
                    status: format!("sent (approved by {reviewer})"),
                    ..state
                })
            })
            .add_edge("DraftReply", "SendReply")
            // docs:end tutorials/support-ticket-agent/pause
            .compile()
            .expect("the graph is valid"),
    );

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
    Ok(())
}

fn draft_tokens(category: &str) -> Vec<String> {
    let reply = format!(
        "Thanks for writing in. Your ticket is with our {category} team, and they will reply within one business day."
    );
    reply
        .split(' ')
        .enumerate()
        .map(|(i, word)| {
            if i == 0 {
                word.to_string()
            } else {
                format!(" {word}")
            }
        })
        .collect()
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
