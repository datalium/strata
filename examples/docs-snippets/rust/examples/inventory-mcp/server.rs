//! The inventory MCP server of the samples for
//! <https://stratamesh.dev/guides/tools/mcp-servers/>. It speaks the stateless
//! 2026-07-28 revision only, and has three tools: `lookup_stock` (text, or the
//! tool's own error for an unknown SKU, or a JSON-RPC error without a SKU),
//! `list_warehouses` (structured content) and `reserve_stock`. The C# twin,
//! `examples/docs-snippets/inventory-mcp/InventoryServer.cs`, answers the same.
//!
//! The example's `main.rs` serves it over stdio; the samples' tests serve it
//! over HTTP on loopback.

use serde_json::{Value, json};

const STOCK: &[(&str, &[(&str, u32)])] = &[
    ("east", &[("AB-12", 7), ("CD-34", 0)]),
    ("west", &[("AB-12", 3)]),
];

pub struct InventoryServer {
    /// The warehouse a lookup uses when the call names none.
    pub warehouse: String,
}

impl InventoryServer {
    pub fn new(warehouse: impl Into<String>) -> Self {
        Self {
            warehouse: warehouse.into(),
        }
    }

    /// The answer to one JSON-RPC message, or `None` for a notification,
    /// which gets none.
    pub fn answer(&self, message: &Value) -> Option<Value> {
        let id = message.get("id")?.clone();
        let method = message["method"].as_str().unwrap_or_default();
        let outcome = match method {
            "server/discover" => Ok(json!({
                "resultType": "complete",
                "supportedVersions": ["2026-07-28"],
                "capabilities": {"tools": {}},
                "_meta": {
                    "io.modelcontextprotocol/serverInfo": {"name": "inventory-mcp", "version": "1.0.0"},
                },
            })),
            "tools/list" => Ok(json!({"tools": tools()})),
            "tools/call" => self.call(
                message["params"]["name"].as_str().unwrap_or_default(),
                &message["params"]["arguments"],
            ),
            other => Err((-32601, format!("Method not found: {other}"))),
        };
        Some(match outcome {
            Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
            Err((code, message)) => {
                json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
            }
        })
    }

    fn call(&self, tool: &str, arguments: &Value) -> Result<Value, (i64, String)> {
        match tool {
            "lookup_stock" => {
                let sku = sku(arguments)?;
                let warehouse = arguments["warehouse"].as_str().unwrap_or(&self.warehouse);
                let units = STOCK
                    .iter()
                    .find(|(name, _)| *name == warehouse)
                    .and_then(|(_, skus)| skus.iter().find(|(have, _)| *have == sku));
                Ok(match units {
                    Some((_, units)) => {
                        text(&format!("{sku}: {units} units in {warehouse}"), false)
                    }
                    None => text(&format!("no SKU '{sku}' in {warehouse}"), true),
                })
            }
            "list_warehouses" => {
                let listed =
                    json!({"warehouses": STOCK.iter().map(|(name, _)| *name).collect::<Vec<_>>()});
                let mut result = text(&listed.to_string(), false);
                result["structuredContent"] = listed;
                Ok(result)
            }
            "reserve_stock" => {
                let units = arguments["units"].as_u64().unwrap_or(0);
                Ok(text(
                    &format!(
                        "reserved {units} units of {} in {}",
                        sku(arguments)?,
                        self.warehouse
                    ),
                    false,
                ))
            }
            other => Err((-32602, format!("Unknown tool: {other}"))),
        }
    }
}

fn tools() -> Value {
    json!([
        {"name": "lookup_stock", "description": "Units in stock for a SKU.",
         "inputSchema": {"type": "object", "properties": {"sku": {"type": "string"}, "warehouse": {"type": "string"}}, "required": ["sku"]}},
        {"name": "list_warehouses", "description": "The warehouses this server knows.",
         "inputSchema": {"type": "object"}},
        {"name": "reserve_stock", "description": "Reserves units of a SKU for an order.",
         "inputSchema": {"type": "object", "properties": {"sku": {"type": "string"}, "units": {"type": "integer"}}, "required": ["sku", "units"]}},
    ])
}

fn sku(arguments: &Value) -> Result<&str, (i64, String)> {
    arguments["sku"]
        .as_str()
        .ok_or_else(|| (-32602, "Invalid params: sku is required".to_string()))
}

fn text(text: &str, is_error: bool) -> Value {
    let mut result = json!({"content": [{"type": "text", "text": text}]});
    if is_error {
        result["isError"] = json!(true);
    }
    result
}
