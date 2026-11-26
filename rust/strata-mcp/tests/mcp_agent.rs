//! An MCP tool used by an agent (`design/50-MCP-Client.md`, "Results" and
//! "Errors"): a tool's own error reaches the model as `{"error": …}` and
//! the agent goes on; a transport failure fails the node, transient through
//! `mcp_fault_classifier`. Mirrors .NET's `McpAgentTests`.

mod support;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use datalium_strata::BoxFuture;
use datalium_strata::context::NodeContext;
use datalium_strata::observability::ToolCallResult;
use datalium_strata::tools::ToolRegistry;
use datalium_strata_ai::{
    Agent, AgentOptions, ChatClient, ChatError, ChatRequest, ChatResponse, ChatStream, ChatUpdate,
    ChatUpdates, Message, Role,
};
use datalium_strata_mcp::{
    McpClient, McpRegistryExt, McpServer, McpToolOptions, mcp_fault_classifier,
};
use support::server::{Era, Script, serve};

/// A model that asks for one tool call per scripted turn, then answers.
struct Model {
    turns: Mutex<VecDeque<Option<(&'static str, &'static str)>>>,
    requests: Mutex<Vec<ChatRequest>>,
}

impl ChatClient for Model {
    fn provider(&self) -> &str {
        "scripted"
    }

    fn default_model(&self) -> Option<&str> {
        Some("scripted-model")
    }

    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>> {
        self.requests.lock().unwrap().push(request);
        let turn = self
            .turns
            .lock()
            .unwrap()
            .pop_front()
            .expect("a scripted turn");
        Box::pin(async move {
            let mut updates = ChatUpdates::new();
            match turn {
                Some((name, arguments)) => {
                    updates.push(ChatUpdate::ToolCall {
                        index: 0,
                        id: Some("call-1".into()),
                        name: Some(name.into()),
                        arguments: arguments.into(),
                    });
                    updates.push(ChatUpdate::Finish("tool_calls".into()));
                }
                None => {
                    updates.push(ChatUpdate::Text("done".into()));
                    updates.push(ChatUpdate::Finish("stop".into()));
                }
            }
            updates.finish()
        })
    }

    fn stream(&self, _: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>> {
        unreachable!("the test's node doesn't stream")
    }
}

fn model(turns: Vec<Option<(&'static str, &'static str)>>) -> Arc<Model> {
    Arc::new(Model {
        turns: Mutex::new(turns.into()),
        requests: Mutex::default(),
    })
}

async fn tools(script: &Script) -> ToolRegistry {
    let (client_side, server_side) = tokio::io::duplex(1 << 20);
    let (server_read, server_write) = tokio::io::split(server_side);
    tokio::spawn(serve(script.clone(), server_read, server_write));
    let (read, write) = tokio::io::split(client_side);
    let client = McpClient::connect(McpServer::streams(read, write))
        .await
        .unwrap();
    let mut registry = ToolRegistry::new();
    registry
        .register_mcp(
            client
                .tool_set(McpToolOptions::new().prefix("fx."))
                .unwrap(),
        )
        .unwrap();
    registry
}

#[tokio::test]
async fn a_tool_s_own_error_reaches_the_model_and_the_agent_goes_on() {
    let script = Script::new(Era::Modern);
    let chat = model(vec![Some(("fx.fail", "{}")), None]);
    let agent = Agent::<String>::new(
        chat.clone(),
        Arc::new(tools(&script).await),
        AgentOptions::new("a"),
    );
    let ctx = NodeContext::new("thread", "Node");

    let result = agent.run(vec![Message::user("?")], &ctx).await.unwrap();

    assert_eq!(result.output, "done");
    assert_eq!(result.tool_calls[0].result, ToolCallResult::Succeeded);
    let tool_message = result
        .messages
        .iter()
        .find(|m| m.role == Role::Tool)
        .unwrap();
    assert!(
        format!("{:?}", tool_message.content).contains(r#"{\"error\":\"it failed\"}"#),
        "{:?}",
        tool_message.content
    );
    // The model saw the tool under a provider-safe name.
    let declared = format!("{:?}", chat.requests.lock().unwrap()[0].tools);
    assert!(declared.contains("fx_fail"), "{declared}");
}

#[tokio::test]
async fn a_transport_failure_fails_the_node_transient_with_the_classifier() {
    let script = Script::new(Era::Modern);
    let registry = Arc::new(tools(&script).await);

    let plain = Agent::<String>::new(
        model(vec![Some(("fx.garbage", "{}"))]),
        registry.clone(),
        AgentOptions::new("a"),
    );
    let error = plain
        .run(vec![Message::user("?")], &NodeContext::new("t", "N"))
        .await
        .err()
        .unwrap();
    assert!(!error.is_transient(), "{error:?}");

    let script = Script::new(Era::Modern);
    let registry = Arc::new(tools(&script).await);
    let classified = Agent::<String>::new(
        model(vec![Some(("fx.garbage", "{}"))]),
        registry,
        AgentOptions::new("a").with_fault_classifier(mcp_fault_classifier),
    );
    let error = classified
        .run(vec![Message::user("?")], &NodeContext::new("t", "N"))
        .await
        .err()
        .unwrap();
    assert!(error.is_transient(), "{error:?}");
    assert!(error.message().contains("isn't JSON"), "{error:?}");
}
