//! Modern Streamable HTTP's mirrored headers (`design/50-MCP-Client.md`,
//! "Streamable HTTP"): the value encoding of `Mcp-Name` and `Mcp-Param-*`
//! (`schema/fixtures/mcp/header-values.json`), and the `x-mcp-header`
//! annotations a tool's `inputSchema` may carry
//! (`schema/fixtures/mcp/x-mcp-header.json`).

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Map, Value};

const SENTINEL_START: &str = "=?base64?";
const SENTINEL_END: &str = "?=";
/// JavaScript's safe integer range, which an integer header must be in.
const SAFE_INTEGER: i64 = (1 << 53) - 1;

/// `text` as a header value: plain when it is visible ASCII or spaces, with
/// no space at either end, and not shaped like the Base64 sentinel;
/// otherwise `=?base64?…?=`.
pub(crate) fn encode_text(text: &str) -> String {
    let visible = text.bytes().all(|b| (0x20..=0x7e).contains(&b));
    let padded = text.starts_with(' ') || text.ends_with(' ');
    let sentinel = text.starts_with(SENTINEL_START) && text.ends_with(SENTINEL_END);
    let plain = visible && !padded && !sentinel;
    if plain {
        text.to_string()
    } else {
        format!("{SENTINEL_START}{}{SENTINEL_END}", STANDARD.encode(text))
    }
}

/// A JSON value as a header value: a string, an integer in JavaScript's
/// safe range, or a boolean. Anything else gives no header.
pub(crate) fn encode_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(encode_text(text)),
        Value::Bool(flag) => Some(flag.to_string()),
        Value::Number(number) => {
            let integer = number
                .as_i64()
                .or_else(|| number.as_u64().and_then(|n| i64::try_from(n).ok()))?;
            (-SAFE_INTEGER..=SAFE_INTEGER)
                .contains(&integer)
                .then(|| integer.to_string())
        }
        _ => None,
    }
}

/// One `x-mcp-header` annotation: the header's name part and the property
/// path its value is read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeaderParam {
    pub name: String,
    pub path: Vec<String>,
}

/// The `Mcp-Param-*` headers a call with `arguments` sends.
pub(crate) fn param_headers(params: &[HeaderParam], arguments: &Value) -> Vec<(String, String)> {
    params
        .iter()
        .filter_map(|param| {
            let value = param
                .path
                .iter()
                .try_fold(arguments, |value, key| value.get(key))?;
            Some((format!("Mcp-Param-{}", param.name), encode_value(value)?))
        })
        .collect()
}

const UNREACHABLE: &str =
    "an x-mcp-header isn't reachable from the schema root through properties only";

/// The annotations of a tool's `inputSchema`, or why the tool must be left
/// out of the list.
pub(crate) fn header_params(input_schema: &Value) -> Result<Vec<HeaderParam>, String> {
    let mut found = Vec::new();
    walk(input_schema, Some(&mut Vec::new()), &mut found)?;
    let mut seen: Vec<String> = Vec::new();
    for param in &found {
        let lower = param.name.to_ascii_lowercase();
        if seen.contains(&lower) {
            return Err(format!("x-mcp-header '{}' is used twice", param.name));
        }
        seen.push(lower);
    }
    Ok(found)
}

/// Walks `schema`; `path` is `Some` while the schema is reached from the
/// root through `properties` only.
fn walk(
    schema: &Value,
    path: Option<&mut Vec<String>>,
    found: &mut Vec<HeaderParam>,
) -> Result<(), String> {
    let Value::Object(schema) = schema else {
        if let Value::Array(items) = schema {
            for item in items {
                walk(item, None, found)?;
            }
        }
        return Ok(());
    };
    let mut path = path;
    if let Some(name) = schema.get("x-mcp-header") {
        match path.as_deref() {
            Some(at) if !at.is_empty() => found.push(annotation(schema, name, at)?),
            _ => return Err(UNREACHABLE.to_string()),
        }
    }
    for (keyword, value) in schema {
        match keyword.as_str() {
            "properties" => {
                if let Value::Object(properties) = value {
                    for (property, subschema) in properties {
                        match path.as_deref_mut() {
                            Some(at) => {
                                at.push(property.clone());
                                let walked = walk(subschema, Some(at), found);
                                at.pop();
                                walked?;
                            }
                            None => walk(subschema, None, found)?,
                        }
                    }
                }
            }
            "$defs" | "definitions" | "patternProperties" | "dependentSchemas" => {
                if let Value::Object(named) = value {
                    for subschema in named.values() {
                        walk(subschema, None, found)?;
                    }
                }
            }
            "const" | "enum" | "default" | "examples" | "x-mcp-header" => {}
            _ => walk(value, None, found)?,
        }
    }
    Ok(())
}

fn annotation(
    schema: &Map<String, Value>,
    name: &Value,
    path: &[String],
) -> Result<HeaderParam, String> {
    let at = path.join(".");
    let name = name
        .as_str()
        .filter(|name| !name.is_empty() && name.bytes().all(is_tchar))
        .ok_or_else(|| format!("x-mcp-header on '{at}' isn't a valid header name"))?;
    match schema.get("type") {
        Some(Value::String(kind)) if matches!(kind.as_str(), "string" | "integer" | "boolean") => {
            Ok(HeaderParam {
                name: name.to_string(),
                path: path.to_vec(),
            })
        }
        kind => Err(format!(
            "x-mcp-header on '{at}' is on a {} property: only string, integer and boolean can be headers",
            kind.map_or_else(|| "untyped".to_string(), Value::to_string)
        )),
    }
}

/// RFC 9110 `tchar`.
fn is_tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures;

    #[test]
    fn the_shared_header_value_fixture() {
        let fixture = fixtures::load("header-values.json");
        for case in fixture["cases"].as_array().unwrap() {
            assert_eq!(
                encode_value(&case["value"]).as_deref(),
                case["header"].as_str(),
                "{}",
                case["name"]
            );
        }
    }

    #[test]
    fn the_shared_x_mcp_header_fixture() {
        let fixture = fixtures::load("x-mcp-header.json");
        for case in fixture["cases"].as_array().unwrap() {
            let name = &case["name"];
            let params = header_params(&case["inputSchema"]);
            match case["rejected"].as_str() {
                Some(reason) => assert_eq!(params, Err(reason.to_string()), "{name}"),
                None => {
                    let params = params.unwrap_or_else(|e| panic!("{name}: {e}"));
                    for call in case["calls"].as_array().unwrap() {
                        let mut sent = param_headers(&params, &call["arguments"]);
                        sent.sort();
                        let mut expected: Vec<(String, String)> = call["headers"]
                            .as_object()
                            .unwrap()
                            .iter()
                            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
                            .collect();
                        expected.sort();
                        assert_eq!(sent, expected, "{name}");
                    }
                }
            }
        }
    }
}
