//! The tutorial's `src/scripted_model.rs`, for <https://stratamesh.dev/tutorials/llm-agent/>.

// docs:begin tutorials/llm-agent/scripted-model
use std::collections::VecDeque;
use std::sync::Mutex;

use datalium_strata::BoxFuture;
use datalium_strata_ai::{
    ChatClient, ChatError, ChatRequest, ChatResponse, ChatStream, ChatUpdate, ChatUpdates,
};

/// A stand-in for a model: plays one scripted answer per request, in order.
pub struct ScriptedModel {
    turns: Mutex<VecDeque<Vec<ChatUpdate>>>,
}

impl ScriptedModel {
    pub fn new(turns: Vec<Vec<ChatUpdate>>) -> Self {
        Self {
            turns: Mutex::new(turns.into()),
        }
    }

    /// A turn that calls one tool, with its arguments as JSON text.
    pub fn call(id: &str, tool: &str, arguments: &str) -> Vec<ChatUpdate> {
        vec![
            ChatUpdate::ToolCall {
                index: 0,
                id: Some(id.to_string()),
                name: Some(tool.to_string()),
                arguments: arguments.to_string(),
            },
            ChatUpdate::Finish("tool_calls".to_string()),
        ]
    }

    /// A turn that answers with text.
    pub fn say(text: &str) -> Vec<ChatUpdate> {
        vec![
            ChatUpdate::Text(text.to_string()),
            ChatUpdate::Finish("stop".to_string()),
        ]
    }

    fn next(&self) -> Result<Vec<ChatUpdate>, ChatError> {
        self.turns
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| ChatError::fatal("script", "the script has no more turns"))
    }
}

impl ChatClient for ScriptedModel {
    fn provider(&self) -> &str {
        "scripted"
    }

    // Without streaming: the whole answer at once.
    fn complete(&self, _request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>> {
        let turn = self.next();
        Box::pin(async move {
            let mut assembled = ChatUpdates::new();
            for update in turn? {
                assembled.push(update);
            }
            assembled.finish()
        })
    }

    // Under run_streaming: the same answer, as a stream.
    fn stream(&self, _request: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>> {
        let turn = self.next();
        Box::pin(async move {
            let stream: ChatStream =
                Box::pin(futures_util::stream::iter(turn?.into_iter().map(Ok)));
            Ok(stream)
        })
    }
}
// docs:end tutorials/llm-agent/scripted-model
