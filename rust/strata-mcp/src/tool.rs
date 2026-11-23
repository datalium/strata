//! An MCP server's tools as Strata tools (`design/50-MCP-Client.md`,
//! "Tools"): what a listed tool is, how a set of them is chosen and named,
//! and how one is called through a `ToolRegistry`.

use std::sync::Arc;

use datalium_strata::BoxFuture;
use datalium_strata::StrataError;
use datalium_strata::tools::{StrataTool, ToolCallContext, ToolRegistry};
use serde_json::{Value, json};

use crate::McpError;
use crate::client::{Inner, McpClient};

/// A tool as the server listed it. `annotations` (and the rest) are
/// untrusted: a server says what it likes about its tools.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct McpTool {
    pub name: String,
    pub title: Option<String>,
    pub description: Option<String>,
    /// The JSON Schema of its arguments; the Strata tool's `parameters`.
    pub input_schema: Value,
    pub output_schema: Option<Value>,
    pub annotations: Option<Value>,
}

impl McpTool {
    pub(crate) fn from_value(tool: &Value) -> Option<Self> {
        let text = |name: &str| tool.get(name).and_then(Value::as_str).map(str::to_string);
        let object = |name: &str| tool.get(name).filter(|value| !value.is_null()).cloned();
        Some(Self {
            name: text("name")?,
            title: text("title").or_else(|| {
                tool.get("annotations")
                    .and_then(|annotations| annotations.get("title"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            }),
            description: text("description"),
            input_schema: object("inputSchema").unwrap_or_else(|| json!({ "type": "object" })),
            output_schema: object("outputSchema"),
            annotations: object("annotations"),
        })
    }

    /// The Strata manifest entry: the description, else the title.
    fn schema(&self, name: &str) -> Value {
        json!({
            "name": name,
            "description": self
                .description
                .as_deref()
                .or(self.title.as_deref())
                .unwrap_or_default(),
            "parameters": self.input_schema,
        })
    }
}

/// Which of a server's tools become Strata tools, and how they are named.
#[derive(Debug, Clone, Default)]
pub struct McpToolOptions {
    prefix: Option<String>,
    include: Option<Vec<String>>,
}

impl McpToolOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Put before each tool's name (`"github."` makes `github.create_issue`),
    /// since a registry refuses a name it already has. The call sends the
    /// server's name.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Only these tools, named as the server names them (before the prefix).
    pub fn include<I, N>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = N>,
        N: Into<String>,
    {
        self.include = Some(names.into_iter().map(Into::into).collect());
        self
    }
}

/// Some of a server's tools as Strata tools, for
/// [`McpRegistryExt::register_mcp`]. They share the client's connection and
/// keep it open.
pub struct McpToolSet {
    tools: Vec<McpToolHandle>,
}

impl McpToolSet {
    /// The Strata names, prefix included, in the server's order.
    pub fn names(&self) -> impl Iterator<Item = &str> + '_ {
        self.tools.iter().map(|tool| tool.name.as_str())
    }

    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// The tools, for code that registers them itself.
    pub fn into_tools(self) -> Vec<Box<dyn StrataTool>> {
        self.tools
            .into_iter()
            .map(|tool| Box::new(tool) as Box<dyn StrataTool>)
            .collect()
    }
}

impl McpClient {
    /// The server's tools as Strata tools: all of them, or those
    /// `options` includes, named with its prefix.
    ///
    /// # Errors
    ///
    /// [`StrataError::ToolNotFound`] for an included name the server didn't
    /// list, as `ToolRegistry::subset` does.
    pub fn tool_set(&self, options: McpToolOptions) -> Result<McpToolSet, StrataError> {
        let listed = self.tools();
        if let Some(missing) = options
            .include
            .iter()
            .flatten()
            .find(|name| !listed.iter().any(|tool| &tool.name == *name))
        {
            return Err(StrataError::ToolNotFound(missing.clone()));
        }
        let prefix = options.prefix.as_deref().unwrap_or_default();
        let tools = listed
            .iter()
            .filter(|tool| {
                options
                    .include
                    .as_ref()
                    .is_none_or(|include| include.contains(&tool.name))
            })
            .map(|tool| {
                let name = format!("{prefix}{}", tool.name);
                McpToolHandle {
                    schema: tool.schema(&name),
                    name,
                    server_name: tool.name.clone(),
                    inner: self.inner().clone(),
                }
            })
            .collect();
        Ok(McpToolSet { tools })
    }
}

/// One MCP tool as a Strata tool.
struct McpToolHandle {
    name: String,
    server_name: String,
    schema: Value,
    inner: Arc<Inner>,
}

impl StrataTool for McpToolHandle {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> Value {
        self.schema.clone()
    }

    fn execute<'a>(&'a self, raw_args: &'a str) -> BoxFuture<'a, Result<String, StrataError>> {
        self.execute_with(raw_args, ToolCallContext::default())
    }

    fn execute_with<'a>(
        &'a self,
        raw_args: &'a str,
        ctx: ToolCallContext,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            let arguments = arguments(raw_args).map_err(StrataError::ToolArgumentParseFailure)?;
            match self
                .inner
                .call_tool(&self.server_name, arguments, Some(ctx.cancellation()))
                .await
            {
                Ok(result) => Ok(result.tool_text()),
                Err(McpError::Cancelled) => Err(StrataError::ToolCancelled(self.name.clone())),
                Err(error) => Err(StrataError::ToolCallFailed {
                    tool: self.name.clone(),
                    message: error.to_string(),
                }),
            }
        })
    }
}

/// The call's arguments: a JSON object, sent as it is (the server
/// validates). Empty text is no arguments. In the words of
/// `argument_problems` otherwise.
fn arguments(raw: &str) -> Result<Value, String> {
    if raw.trim().is_empty() {
        return Ok(json!({}));
    }
    match serde_json::from_str::<Value>(raw) {
        Ok(object @ Value::Object(_)) => Ok(object),
        Ok(_) => Err("the arguments aren't a JSON object".to_string()),
        Err(_) => Err("the arguments aren't valid JSON".to_string()),
    }
}

/// Registers an [`McpToolSet`] in a `ToolRegistry`. An extension trait, so
/// the engine doesn't know MCP exists.
pub trait McpRegistryExt {
    /// Registers every tool of the set, or none: [`StrataError::DuplicateTool`]
    /// when any name is already taken.
    fn register_mcp(&mut self, tools: McpToolSet) -> Result<&mut Self, StrataError>;
}

impl McpRegistryExt for ToolRegistry {
    fn register_mcp(&mut self, tools: McpToolSet) -> Result<&mut Self, StrataError> {
        if let Some(taken) = tools
            .names()
            .find(|name| self.names().any(|have| have == *name))
        {
            return Err(StrataError::DuplicateTool(taken.to_string()));
        }
        for tool in tools.into_tools() {
            self.try_register(tool)?;
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_are_an_object_or_nothing() {
        assert_eq!(arguments(""), Ok(json!({})));
        assert_eq!(arguments(" {\"a\":1} "), Ok(json!({"a":1})));
        assert_eq!(
            arguments("[1]"),
            Err("the arguments aren't a JSON object".into())
        );
        assert_eq!(
            arguments("{"),
            Err("the arguments aren't valid JSON".into())
        );
    }

    #[test]
    fn a_listed_tool_falls_back_to_its_title() {
        let tool = McpTool::from_value(&json!({"name":"t","title":"The tool"})).unwrap();
        assert_eq!(
            tool.schema("x.t"),
            json!({"name":"x.t","description":"The tool","parameters":{"type":"object"}})
        );
        assert!(McpTool::from_value(&json!({"title":"nameless"})).is_none());
    }
}
