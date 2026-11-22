//! Samples for <https://stratamesh.dev/start-here/quickstart/>. The sample is the whole of `src/main.rs`, so
//! [`main_runs`] runs it in a child process and checks what it prints.

// docs:begin start-here/quickstart/program
use std::sync::Arc;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::graph::StateGraph;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Greeting {
    name: String,
    message: String,
}

#[tokio::main]
async fn main() {
    let graph = StateGraph::<Greeting>::new()
        .add_node("Compose", |state: Greeting| Greeting {
            message: format!("Hello, {}", state.name),
            ..state
        })
        .add_node("Punctuate", |state: Greeting| Greeting {
            message: format!("{}!", state.message),
            ..state
        })
        .add_edge("Compose", "Punctuate")
        .compile()
        .expect("the graph is valid");

    let store = Arc::new(InMemoryCheckpointStore::new());
    let initial = Greeting {
        name: "Ada".to_string(),
        message: String::new(),
    };

    match graph.run("greeting-1", initial, store).await {
        GraphExecutionResult::Completed(state) => println!("{}", state.message),
        _ => println!("The run did not complete."),
    }
}
// docs:end start-here/quickstart/program

use crate::child;

#[test]
fn main_runs() {
    child::main_in_child(main);
}

#[test]
fn prints_the_greeting() {
    let dir = child::TempDir::new("quickstart");
    let output = child::run("start_here::quickstart::main_runs", "run", dir.path());
    assert_eq!(output, "Hello, Ada!");
}

/// Without the edge, compiling fails with the error the page shows.
#[test]
fn compiling_without_the_edge_fails() {
    let error = StateGraph::<Greeting>::new()
        .add_node("Compose", |state: Greeting| state)
        .add_node("Punctuate", |state: Greeting| state)
        .compile()
        .err()
        .expect("a graph with an isolated node doesn't compile");
    assert_eq!(
        format!("{error:?}"),
        "GraphInvalid(ValidationResult { errors: [ValidationError { kind: OrphanNode, code: \"STRATA_ERR_ORPHAN\", \
         node: \"Punctuate\", related_node: None, message: \"Node 'Punctuate' is isolated and unreachable from the \
         entry point 'Compose'.\" }] })"
    );
}
