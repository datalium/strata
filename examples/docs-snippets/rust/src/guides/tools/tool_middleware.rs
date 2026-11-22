//! Samples for <https://stratamesh.dev/guides/tools/tool-middleware/>.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::Deserialize;

/// The page shows `ToolMiddleware` as `datalium_strata::tools` declares it. This is a copy of that
/// declaration; `the_trait_on_the_page_is_the_engines` checks it can stand in for the engine's trait,
/// and the engine's for it, so a change to the trait fails here.
mod declaration {
    use datalium_strata::BoxFuture;
    use datalium_strata::StrataError;
    use datalium_strata::tools::{Next, ToolCall};

    // docs:begin guides/tools/tool-middleware/declaration
    pub trait ToolMiddleware: Send + Sync {
        fn invoke<'a>(
            &'a self,
            call: ToolCall,
            next: Next<'a>,
        ) -> BoxFuture<'a, Result<String, StrataError>>;

        fn on_refused<'a>(
            &'a self,
            call: &'a ToolCall,
            refusal: &'a StrataError,
        ) -> BoxFuture<'a, Result<(), StrataError>> {
            let _ = (call, refusal);
            Box::pin(async { Ok(()) })
        }
    }
    // docs:end guides/tools/tool-middleware/declaration

    /// Any engine middleware is a middleware of the page's trait.
    pub struct FromEngine<M>(pub M);

    impl<M: datalium_strata::tools::ToolMiddleware> ToolMiddleware for FromEngine<M> {
        fn invoke<'a>(
            &'a self,
            call: ToolCall,
            next: Next<'a>,
        ) -> BoxFuture<'a, Result<String, StrataError>> {
            self.0.invoke(call, next)
        }

        fn on_refused<'a>(
            &'a self,
            call: &'a ToolCall,
            refusal: &'a StrataError,
        ) -> BoxFuture<'a, Result<(), StrataError>> {
            self.0.on_refused(call, refusal)
        }
    }

    /// And any middleware of the page's trait is one the engine runs.
    pub struct ToEngine<M>(pub M);

    impl<M: ToolMiddleware> datalium_strata::tools::ToolMiddleware for ToEngine<M> {
        fn invoke<'a>(
            &'a self,
            call: ToolCall,
            next: Next<'a>,
        ) -> BoxFuture<'a, Result<String, StrataError>> {
            self.0.invoke(call, next)
        }

        fn on_refused<'a>(
            &'a self,
            call: &'a ToolCall,
            refusal: &'a StrataError,
        ) -> BoxFuture<'a, Result<(), StrataError>> {
            self.0.on_refused(call, refusal)
        }
    }
}

// docs:begin guides/tools/tool-middleware/audit
use std::time::{Duration, Instant};

use datalium_strata::BoxFuture;
use datalium_strata::prelude::*;
use datalium_strata::tools::{Next, ToolCall, ToolMiddleware};

struct Audit;

impl Audit {
    // call.arguments() holds the JSON arguments. Log them only if they can't hold secrets.
    fn write(caller: &str, tool: &str, outcome: &str, elapsed: Duration) {
        println!(
            "[audit] {caller} called {tool}: {outcome} in {} ms",
            elapsed.as_millis()
        );
    }
}

impl ToolMiddleware for Audit {
    fn invoke<'a>(
        &'a self,
        call: ToolCall,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            // `next.run` takes the call, so copy what the log line needs first.
            let caller = call.context().caller_id().unwrap_or("-").to_string();
            let tool = call.name().to_string();
            let clock = Instant::now();
            let result = next.run(call).await;
            let outcome = match &result {
                Ok(_) => "succeeded".to_string(),
                Err(StrataError::ToolCallDenied { reason, .. }) => format!("denied ({reason})"),
                Err(StrataError::ToolCallFailed { message, .. }) => format!("failed ({message})"),
                Err(other) => format!("failed ({other:?})"),
            };
            Self::write(&caller, &tool, &outcome, clock.elapsed());
            result
        })
    }

    // Calls the registry refused before the chain ran: not found, or left out of a subset.
    fn on_refused<'a>(
        &'a self,
        call: &'a ToolCall,
        refusal: &'a StrataError,
    ) -> BoxFuture<'a, Result<(), StrataError>> {
        let outcome = match refusal {
            StrataError::ToolNotPermitted(_) => "not permitted",
            _ => "not found",
        };
        let caller = call.context().caller_id().unwrap_or("-");
        Self::write(caller, call.name(), outcome, Duration::ZERO);
        Box::pin(async { Ok(()) })
    }
}
// docs:end guides/tools/tool-middleware/audit

// docs:begin guides/tools/tool-middleware/caller-policy
use std::collections::HashMap;

struct CallerPolicy {
    allowed_callers: HashMap<&'static str, Vec<&'static str>>,
}

impl ToolMiddleware for CallerPolicy {
    fn invoke<'a>(
        &'a self,
        call: ToolCall,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            let caller = call.context().caller_id().unwrap_or("");
            let refused = self
                .allowed_callers
                .get(call.name())
                .is_some_and(|callers| !callers.contains(&caller));
            if refused {
                return Err(StrataError::ToolCallDenied {
                    tool: call.name().to_string(),
                    reason: format!("caller '{caller}' may not call {}", call.name()),
                });
            }
            next.run(call).await
        })
    }
}
// docs:end guides/tools/tool-middleware/caller-policy

// docs:begin guides/tools/tool-middleware/timeout
struct Timeout(Duration);

impl ToolMiddleware for Timeout {
    fn invoke<'a>(
        &'a self,
        call: ToolCall,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            let tool = call.name().to_string();
            // On expiry, the chain's future is dropped, and the tool's with it.
            match tokio::time::timeout(self.0, next.run(call)).await {
                Ok(result) => result,
                Err(_) => Err(StrataError::ToolCallFailed {
                    message: format!("{tool} took longer than {:?}", self.0),
                    tool,
                }),
            }
        })
    }
}
// docs:end guides/tools/tool-middleware/timeout

// docs:begin guides/tools/tool-middleware/concurrency-limit
use tokio::sync::Semaphore;

struct ConcurrencyLimit(Semaphore);

impl ConcurrencyLimit {
    fn new(max_calls: usize) -> Self {
        Self(Semaphore::new(max_calls))
    }
}

impl ToolMiddleware for ConcurrencyLimit {
    fn invoke<'a>(
        &'a self,
        call: ToolCall,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            // The slot is held until the chain returns or is dropped.
            let _slot = self
                .0
                .acquire()
                .await
                .map_err(|_| StrataError::ToolCallFailed {
                    tool: call.name().to_string(),
                    message: "the concurrency limit was closed".to_string(),
                })?;
            next.run(call).await
        })
    }
}
// docs:end guides/tools/tool-middleware/concurrency-limit

// The tools from Define tools that the page registers.
fn stock(sku: &str) -> u32 {
    match sku {
        "AB-12" => 7,
        _ => 0,
    }
}

#[derive(Deserialize, StrataSchema)]
struct LookupArgs {
    sku: String,
}

#[strata_tool(name = "inventory.lookup", description = "Units in stock for a SKU.")]
async fn inventory_lookup(args: LookupArgs) -> Result<String, StrataError> {
    Ok(stock(&args.sku).to_string())
}

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

// docs:begin guides/tools/tool-middleware/recount
#[derive(Deserialize, StrataSchema)]
struct SkuArgs {
    sku: String,
}

#[strata_tool(
    name = "inventory.recount",
    description = "Counts the units of a SKU on the shelf."
)]
async fn inventory_recount(args: SkuArgs, ctx: ToolCallContext) -> Result<String, String> {
    // Stands in for a slow call to the warehouse system.
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(10)) => Ok(stock(&args.sku).to_string()),
        _ = ctx.cancellation().cancelled() => Err("cancelled".to_string()),
    }
}
// docs:end guides/tools/tool-middleware/recount

// Kept as the page prints it: rustfmt would put each call of the run sample on eight lines.
#[rustfmt::skip]
#[tokio::test]
async fn register_and_run_some_calls() {
    // docs:begin guides/tools/tool-middleware/register
    let mut tools = ToolRegistry::new();
    tools
        .register(Box::new(InventoryLookupTool))
        .register(Box::new(InventoryRecountTool))
        .register(Box::new(BillingRefundTool))
        .use_middleware(Audit) // outermost: sees every call first
        .use_middleware(CallerPolicy {
            allowed_callers: HashMap::from([("billing.refund", vec!["billing-agent"])]),
        })
        .use_middleware(Timeout(Duration::from_secs(1)))
        .use_middleware(ConcurrencyLimit::new(4)); // innermost: next to the tool
    // docs:end guides/tools/tool-middleware/register

    // docs:begin guides/tools/tool-middleware/run
    let support = ToolCallContext::new().with_caller_id("support-agent");
    let billing = ToolCallContext::new().with_caller_id("billing-agent");
    let refund = r#"{"orderId":"A-1001","amount":40}"#;

    println!("{}", ask(&tools, "inventory.lookup", r#"{"sku":"AB-12"}"#, support.clone()).await);
    println!("{}", ask(&tools, "billing.refund", refund, support.clone()).await);
    println!("{}", ask(&tools, "billing.refund", refund, billing.clone()).await);
    println!("{}", ask(&tools, "inventory.recount", r#"{"sku":"AB-12"}"#, support.clone()).await);
    println!("{}", ask(&tools, "inventory.restock", r#"{"sku":"AB-12"}"#, support.clone()).await);

    async fn ask(tools: &ToolRegistry, name: &str, arguments: &str, ctx: ToolCallContext) -> String {
        match tools.dispatch_with(name, arguments, ctx).await {
            Ok(result) => result,
            Err(StrataError::ToolCallDenied { reason, .. }) => format!("denied: {reason}"),
            Err(StrataError::ToolCallFailed { message, .. }) => format!("failed: {message}"),
            Err(other) => format!("{other:?}"),
        }
    }
    // docs:end guides/tools/tool-middleware/run

    // The results the page prints, asked again.
    let started = Instant::now();
    let results = [
        ask(&tools, "inventory.lookup", r#"{"sku":"AB-12"}"#, support.clone()).await,
        ask(&tools, "billing.refund", refund, support.clone()).await,
        ask(&tools, "billing.refund", refund, billing).await,
        ask(&tools, "inventory.recount", r#"{"sku":"AB-12"}"#, support.clone()).await,
        ask(&tools, "inventory.restock", r#"{"sku":"AB-12"}"#, support).await,
    ];
    assert_eq!(
        results,
        [
            "7",
            "denied: caller 'support-agent' may not call billing.refund",
            "refunded 40.00 on A-1001",
            "failed: inventory.recount took longer than 1s",
            "ToolNotFound(\"inventory.restock\")",
        ]
    );
    // The time limit stopped the 10 s recount after a second.
    assert!(started.elapsed() < Duration::from_secs(5));
}

/// A tool that counts how many calls are inside it at once.
static INSIDE: AtomicUsize = AtomicUsize::new(0);
static MOST_INSIDE: AtomicUsize = AtomicUsize::new(0);

#[derive(Deserialize, StrataSchema)]
struct NoArgs {}

#[strata_tool(name = "probe.busy", description = "Stays busy for a moment.")]
async fn probe_busy(_args: NoArgs) -> Result<String, StrataError> {
    let now = INSIDE.fetch_add(1, Ordering::SeqCst) + 1;
    MOST_INSIDE.fetch_max(now, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(50)).await;
    INSIDE.fetch_sub(1, Ordering::SeqCst);
    Ok(String::new())
}

#[tokio::test]
async fn the_concurrency_limit_lets_two_calls_through_at_once() {
    let mut tools = ToolRegistry::new();
    tools
        .register(Box::new(ProbeBusyTool))
        .use_middleware(ConcurrencyLimit::new(2));
    let tools = Arc::new(tools);
    let calls: Vec<_> = (0..6)
        .map(|_| {
            let tools = tools.clone();
            tokio::spawn(async move { tools.dispatch("probe.busy", "{}").await })
        })
        .collect();
    for call in calls {
        call.await
            .expect("the call ran")
            .expect("the call succeeded");
    }
    assert_eq!(MOST_INSIDE.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn the_trait_on_the_page_is_the_engines() {
    // The page's trait, implemented by an engine middleware, and run by the engine.
    let mut tools = ToolRegistry::new();
    tools
        .register(Box::new(BillingRefundTool))
        .use_middleware(declaration::ToEngine(declaration::FromEngine(
            CallerPolicy {
                allowed_callers: HashMap::from([("billing.refund", vec!["billing-agent"])]),
            },
        )));
    let refused = tools
        .dispatch_with(
            "billing.refund",
            r#"{"orderId":"A-1001","amount":40}"#,
            ToolCallContext::new().with_caller_id("support-agent"),
        )
        .await;
    assert!(matches!(refused, Err(StrataError::ToolCallDenied { .. })));
}
