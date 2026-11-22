//! Samples for <https://stratamesh.dev/guides/tools/define-tools/>.

use std::sync::Arc;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::StateGraph;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::Serialize;
use serde_json::json;

// docs:begin guides/tools/define-tools/write-tool
use std::time::Duration;

use datalium_strata::prelude::*;
use serde::Deserialize;

fn stock(sku: &str) -> u32 {
    match sku {
        "AB-12" => 7,
        _ => 0,
    }
}

#[derive(Deserialize, StrataSchema)]
struct LookupArgs {
    sku: String,
    warehouse: Option<String>,
}

#[strata_tool(name = "inventory.lookup", description = "Units in stock for a SKU.")]
async fn inventory_lookup(args: LookupArgs) -> Result<String, StrataError> {
    Ok(stock(&args.sku).to_string())
}

#[derive(Deserialize, StrataSchema)]
struct ReserveArgs {
    sku: String,
    count: u32,
}

#[strata_tool(
    name = "inventory.reserve",
    description = "Reserves units of a SKU for the calling tenant."
)]
async fn inventory_reserve(args: ReserveArgs, ctx: ToolCallContext) -> Result<String, String> {
    // Stands in for a call to the warehouse system.
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        _ = ctx.cancellation().cancelled() => return Err("cancelled".to_string()),
    }
    let in_stock = stock(&args.sku);
    if args.count > in_stock {
        eprintln!(
            "inventory.reserve failed: only {in_stock} x {} in stock",
            args.sku
        );
        return Err(format!("only {in_stock} x {} in stock", args.sku));
    }
    let tenant = ctx
        .metadata()
        .get("tenant")
        .map_or("(no tenant)", String::as_str);
    Ok(format!(
        "reserved {} x {} for {tenant}",
        args.count, args.sku
    ))
}
// docs:end guides/tools/define-tools/write-tool

// The page mentions billing.refund, "a tool like the ones above", without showing it.
#[derive(Deserialize, StrataSchema)]
#[serde(rename_all = "camelCase")]
struct RefundArgs {
    order_id: String,
    amount: f64,
}

#[strata_tool(
    name = "billing.refund",
    description = "Refunds part or all of an order."
)]
async fn billing_refund(args: RefundArgs) -> Result<String, StrataError> {
    Ok(format!("refunded {:.2} on {}", args.amount, args.order_id))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    sku: String,
    count: u32,
    confirmation: Option<String>,
}

// docs:begin guides/tools/define-tools/errors
async fn ask(tools: &ToolRegistry, name: &str, arguments: &str, ctx: ToolCallContext) -> String {
    match tools.dispatch_with(name, arguments, ctx).await {
        Ok(result) => result,
        Err(StrataError::ToolArgumentParseFailure(problems)) => {
            format!("invalid arguments: {problems}")
        }
        Err(StrataError::ToolNotPermitted(_)) => "not permitted for this agent".to_string(),
        Err(StrataError::ToolNotFound(_)) => "no such tool".to_string(),
        Err(StrataError::ToolExecutionFailure(tool)) => format!("{tool} failed"),
        Err(other) => format!("{other:?}"),
    }
}
// docs:end guides/tools/define-tools/errors

/// The manifest entry the page shows for `inventory.lookup`, the same in both engines.
fn lookup_entry() -> serde_json::Value {
    json!({
        "name": "inventory.lookup",
        "description": "Units in stock for a SKU.",
        "parameters": {
            "type": "object",
            "properties": {
                "sku": { "type": "string" },
                "warehouse": { "type": "string" }
            },
            "required": ["sku"]
        }
    })
}

#[tokio::test]
async fn write_a_tool_and_read_its_manifest() {
    // docs:begin guides/tools/define-tools/manifest
    let entry: serde_json::Value = InventoryLookupTool.schema();
    // docs:end guides/tools/define-tools/manifest
    assert_eq!(entry, lookup_entry());

    // `warehouse` is optional: an Option field.
    let args: LookupArgs = serde_json::from_str(r#"{"sku":"AB-12"}"#).expect("sku alone is enough");
    assert_eq!((args.sku.as_str(), args.warehouse), ("AB-12", None));

    let lookup = InventoryLookupTool
        .execute(r#"{"sku":"AB-12"}"#)
        .await
        .expect("the lookup succeeds");
    assert_eq!(lookup, "7");
    let reserved = InventoryReserveTool
        .execute_with(
            r#"{"sku":"AB-12","count":2}"#,
            ToolCallContext::new().with_metadata("tenant", "acme"),
        )
        .await
        .expect("the reservation succeeds");
    assert_eq!(reserved, "reserved 2 x AB-12 for acme");
}

#[tokio::test]
async fn register_and_call_from_a_node() {
    // docs:begin guides/tools/define-tools/register
    // BillingRefundTool is the handle of billing_refund, a tool like the ones above.
    let mut tools = ToolRegistry::new();
    tools
        .register(Box::new(InventoryLookupTool))
        .register(Box::new(InventoryReserveTool))
        .register(Box::new(BillingRefundTool));

    // One agent's view: only these two tools.
    let support = Arc::new(
        tools
            .subset(["inventory.lookup", "inventory.reserve"])
            .expect("both tools are registered"),
    );
    // docs:end guides/tools/define-tools/register

    assert_eq!(
        tools.names().collect::<Vec<_>>(),
        ["inventory.lookup", "inventory.reserve", "billing.refund"]
    );
    assert_eq!(
        support.names().collect::<Vec<_>>(),
        ["inventory.lookup", "inventory.reserve"]
    );
    assert!(matches!(
        tools.try_register(Box::new(InventoryLookupTool)),
        Err(StrataError::DuplicateTool(name)) if name == "inventory.lookup"
    ));

    // docs:begin guides/tools/define-tools/call-from-node
    let node_tools = support.clone();
    let graph = StateGraph::<Order>::new()
        .add_async_node("Reserve", move |state: Order, ctx: NodeContext| {
            let tools = node_tools.clone();
            async move {
                let arguments =
                    serde_json::json!({ "sku": state.sku, "count": state.count }).to_string();
                let result = tools
                    .dispatch_with("inventory.reserve", &arguments, ToolCallContext::from(&ctx))
                    .await;
                match result {
                    Ok(confirmation) => Order {
                        confirmation: Some(confirmation),
                        ..state
                    },
                    Err(error) => Order {
                        confirmation: Some(format!("failed: {error:?}")),
                        ..state
                    },
                }
            }
        })
        .compile()
        .expect("the graph is valid");

    let store = Arc::new(InMemoryCheckpointStore::new());
    let initial = Order {
        sku: "AB-12".to_string(),
        count: 2,
        confirmation: None,
    };
    let run = graph
        .run_with(
            "order-1001",
            initial,
            store,
            RunConfig::new().with_metadata("tenant", "acme"),
        )
        .await;
    // docs:end guides/tools/define-tools/call-from-node

    match run {
        GraphExecutionResult::Completed(state) => {
            assert_eq!(
                state.confirmation.as_deref(),
                Some("reserved 2 x AB-12 for acme")
            );
        }
        GraphExecutionResult::Failed(error) => panic!("the run failed: {error:?}"),
        _ => panic!("the run paused, but this graph has nothing that pauses"),
    }

    // The table under "Handle tool errors", with the support subset.
    let none = ToolCallContext::default;
    assert_eq!(
        ask(&support, "inventory.reserve", r#"{"sku":"AB-12"}"#, none()).await,
        "invalid arguments: argument 'count' is missing"
    );
    assert_eq!(
        ask(
            &support,
            "inventory.reserve",
            r#"{"sku":"AB-12","count":9}"#,
            none()
        )
        .await,
        "inventory.reserve failed"
    );
    assert_eq!(
        ask(
            &support,
            "billing.refund",
            r#"{"orderId":"A-1","amount":5}"#,
            none()
        )
        .await,
        "not permitted for this agent"
    );
    assert_eq!(
        ask(&support, "inventory.restock", "{}", none()).await,
        "no such tool"
    );
}

#[tokio::test]
async fn a_cancelled_call_never_runs_the_tool() {
    let token = datalium_strata::CancellationToken::new();
    token.cancel();
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(InventoryReserveTool));
    let result = tools
        .dispatch_with(
            "inventory.reserve",
            r#"{"sku":"AB-12","count":1}"#,
            ToolCallContext::new().with_cancellation(token),
        )
        .await;
    assert!(matches!(result, Err(StrataError::ToolCancelled(name)) if name == "inventory.reserve"));
}
