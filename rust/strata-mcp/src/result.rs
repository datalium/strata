//! An MCP `CallToolResult`, and the text a Strata tool returns for it
//! (`design/50-MCP-Client.md`, "Results"; `schema/fixtures/mcp/tool-results.json`).

use serde_json::{Value, json};

/// What `tools/call` returned, whole: for code that needs more than the
/// text a tool gives the model (an image's bytes, `structuredContent` as a
/// value).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct McpToolResult {
    /// The content blocks, as the server sent them.
    pub content: Vec<Value>,
    /// `structuredContent`, when the server sent one (and not `null`).
    pub structured_content: Option<Value>,
    /// The tool's own error: the call failed, and `content` says why.
    pub is_error: bool,
}

impl McpToolResult {
    pub(crate) fn from_value(result: &Value) -> Self {
        Self {
            content: result
                .get("content")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            structured_content: result
                .get("structuredContent")
                .filter(|value| !value.is_null())
                .cloned(),
            is_error: result.get("isError").and_then(Value::as_bool) == Some(true),
        }
    }

    /// The text without the error wrapper: `structuredContent` as compact
    /// JSON when present, else the content blocks joined by a blank line,
    /// with a placeholder for binary content.
    pub fn text(&self) -> String {
        if let Some(structured) = &self.structured_content {
            return structured.to_string();
        }
        self.content
            .iter()
            .map(block_text)
            .collect::<Vec<_>>()
            .join("\n\n")
    }

    /// What the Strata tool returns: [`Self::text`], or for the tool's own
    /// error `{"error":"<text>"}`, the shape the agent gives the model for a
    /// refusal.
    pub fn tool_text(&self) -> String {
        if !self.is_error {
            return self.text();
        }
        let text = self.text();
        let message = if text.is_empty() {
            "the tool reported an error without a message".to_string()
        } else {
            text
        };
        json!({ "error": message }).to_string()
    }
}

fn block_text(block: &Value) -> String {
    let kind = block
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let field = |name: &str| block.get(name).and_then(Value::as_str);
    match kind {
        "text" => field("text").unwrap_or_default().to_string(),
        "image" | "audio" => placeholder(field("mimeType"), field("data")),
        "resource_link" => {
            let uri = field("uri").unwrap_or_default();
            match field("name") {
                Some(name) => format!("[resource: {name} {uri}]"),
                None => format!("[resource: {uri}]"),
            }
        }
        "resource" => {
            let resource = block.get("resource").cloned().unwrap_or(Value::Null);
            let field = |name: &str| resource.get(name).and_then(Value::as_str);
            match field("text") {
                Some(text) => text.to_string(),
                None => placeholder(field("mimeType"), field("blob")),
            }
        }
        other => format!("[{other} content, not shown]"),
    }
}

/// `[<mime type>, <size> KB, not shown]`, the size being what the base64
/// decodes to, rounded up to the next KB.
fn placeholder(mime_type: Option<&str>, base64: Option<&str>) -> String {
    let characters = base64
        .unwrap_or_default()
        .chars()
        .filter(|c| *c != '=' && !c.is_whitespace())
        .count();
    let bytes = characters * 3 / 4;
    format!(
        "[{}, {} KB, not shown]",
        mime_type.unwrap_or("application/octet-stream"),
        bytes.div_ceil(1024)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures;

    #[test]
    fn the_shared_result_fixture() {
        let fixture = fixtures::load("tool-results.json");
        for case in fixture["cases"].as_array().unwrap() {
            let name = &case["name"];
            let text = McpToolResult::from_value(&case["result"]).tool_text();
            let expected = case["text"].as_str().unwrap();
            if case["json"] == true {
                let text: Value = serde_json::from_str(&text).unwrap();
                let expected: Value = serde_json::from_str(expected).unwrap();
                assert_eq!(text, expected, "{name}");
            } else {
                assert_eq!(text, expected, "{name}");
            }
        }
    }
}
