//! Tool middleware (`design/23-Tool-Middleware.md`, SG-22): middleware order,
//! short-circuiting, a middleware's own refusal and failure, a timeout that
//! stops waiting, context and argument changes, subsets sharing the chain,
//! refused calls reaching the on-refused hooks, and middleware running inside
//! the `strata.tool` span. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/Tools/ToolMiddlewareTests.cs` — keep
//! the two in step.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::CancellationToken;
use datalium_strata::checkpoint::{CheckpointStore, CheckpointStoreError};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::StateGraph;
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind, ToolCallResult};
use datalium_strata::prelude::*;
use datalium_strata::schema::Checkpoint;
use datalium_strata::tools::{Next, ToolCall, ToolMiddleware};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::Subscriber;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

const PATIENCE: Duration = Duration::from_secs(10);

type Outcome = Result<String, StrataError>;

// --- Tools -------------------------------------------------------------------

/// What a tool saw of its context: caller id, tenant, call id.
type Seen = (Option<String>, Option<String>, Option<String>);

/// How often the tools ran and what context they saw, reached through the
/// call's services so each test has its own.
#[derive(Default)]
struct Probe {
    runs: AtomicU32,
    seen: Mutex<Vec<Seen>>,
}

impl Probe {
    fn runs(&self) -> u32 {
        self.runs.load(Ordering::SeqCst)
    }

    fn ran(&self, ctx: &ToolCallContext) {
        self.runs.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().unwrap().push((
            ctx.caller_id().map(str::to_string),
            ctx.metadata().get("tenant").cloned(),
            ctx.call_id().map(str::to_string),
        ));
    }
}

#[derive(Deserialize, StrataSchema)]
struct EchoArgs {
    text: String,
}

#[derive(Deserialize, StrataSchema)]
struct NoArgs {}

#[strata_tool(name = "mw.echo", description = "Echoes its text.")]
async fn mw_echo(args: EchoArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
    ctx.service::<Probe>().expect("a probe").ran(&ctx);
    Ok(args.text)
}

/// Ignores its token: only dropping its future stops it.
#[strata_tool(name = "mw.stall", description = "Ignores its token.")]
async fn mw_stall(_args: NoArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
    ctx.service::<Probe>().expect("a probe").ran(&ctx);
    std::future::pending::<()>().await;
    Ok("late".to_string())
}

#[strata_tool(name = "mw.other", description = "Another tool.")]
async fn mw_other(_args: NoArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
    ctx.service::<Probe>().expect("a probe").ran(&ctx);
    Ok("other".to_string())
}

fn registry() -> ToolRegistry {
    let mut tools = ToolRegistry::new();
    tools
        .register(Box::new(MwEchoTool))
        .register(Box::new(MwStallTool))
        .register(Box::new(MwOtherTool));
    tools
}

fn echo(text: &str) -> String {
    serde_json::json!({ "text": text }).to_string()
}

/// A context outside any run, carrying `probe`.
fn with_probe(probe: &Arc<Probe>) -> ToolCallContext {
    ToolCallContext::new().with_service(probe.clone())
}

// --- Middleware --------------------------------------------------------------

type Trace = Arc<Mutex<Vec<String>>>;

fn trace() -> Trace {
    Arc::new(Mutex::new(Vec::new()))
}

fn entries(trace: &Trace) -> Vec<String> {
    trace.lock().unwrap().clone()
}

/// Records entering and leaving the chain, and refused calls.
struct Recording {
    name: &'static str,
    trace: Trace,
}

fn recording(name: &'static str, trace: &Trace) -> Recording {
    Recording {
        name,
        trace: trace.clone(),
    }
}

impl ToolMiddleware for Recording {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            self.trace.lock().unwrap().push(format!("{} in", self.name));
            let result = next.run(call).await;
            self.trace
                .lock()
                .unwrap()
                .push(format!("{} out", self.name));
            result
        })
    }

    fn on_refused<'a>(
        &'a self,
        call: &'a ToolCall,
        refusal: &'a StrataError,
    ) -> BoxFuture<'a, Result<(), StrataError>> {
        let kind = match refusal {
            StrataError::ToolNotPermitted(_) => "ToolNotPermitted",
            StrataError::ToolNotFound(_) => "ToolNotFound",
            other => panic!("{other:?}"),
        };
        self.trace
            .lock()
            .unwrap()
            .push(format!("{} refused {} {kind}", self.name, call.name()));
        Box::pin(async { Ok(()) })
    }
}

/// Refuses every call.
struct Deny(&'static str);

impl ToolMiddleware for Deny {
    fn invoke<'a>(&'a self, call: ToolCall, _next: Next<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            Err(StrataError::ToolCallDenied {
                tool: call.name().to_string(),
                reason: self.0.to_string(),
            })
        })
    }
}

/// Fails every call, as an unreachable policy backend would.
struct Fail;

impl ToolMiddleware for Fail {
    fn invoke<'a>(&'a self, call: ToolCall, _next: Next<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            Err(StrataError::ToolCallFailed {
                tool: call.name().to_string(),
                message: "the policy backend is down".to_string(),
            })
        })
    }
}

/// Answers a repeated (tool, arguments) pair from memory, as StrataOps' read cache does.
#[derive(Default)]
struct Cache(Mutex<HashMap<(String, String), String>>);

impl ToolMiddleware for Cache {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            let key = (call.name().to_string(), call.arguments().to_string());
            if let Some(cached) = self.0.lock().unwrap().get(&key) {
                return Ok(cached.clone());
            }
            let result = next.run(call).await?;
            self.0.lock().unwrap().insert(key, result.clone());
            Ok(result)
        })
    }
}

/// Stops waiting after the limit; dropping the chain's future drops the tool's.
struct Timeout(Duration);

impl ToolMiddleware for Timeout {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            let tool = call.name().to_string();
            match tokio::time::timeout(self.0, next.run(call)).await {
                Ok(result) => result,
                Err(_) => Err(StrataError::ToolCallFailed {
                    tool,
                    message: format!("timed out after {:?}", self.0),
                }),
            }
        })
    }
}

/// Replaces the arguments and enriches the context.
struct Enrich;

impl ToolMiddleware for Enrich {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        let ctx = call
            .context()
            .clone()
            .with_caller_id("gateway")
            .with_metadata("tenant", "acme");
        next.run(call.with_arguments(echo("redacted")).with_context(ctx))
    }
}

/// Runs the rest of the chain twice.
struct Twice;

impl ToolMiddleware for Twice {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            next.run(call.clone()).await?;
            next.run(call).await
        })
    }
}

/// Opens a span of its own and waits, then continues.
struct Slow;

impl ToolMiddleware for Slow {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            drop(tracing::info_span!("middleware-check").entered());
            tokio::time::sleep(Duration::from_millis(50)).await;
            next.run(call).await
        })
    }
}

/// Cancels the caller's token, then fails or denies.
struct CancelThen {
    token: CancellationToken,
    deny: bool,
}

impl ToolMiddleware for CancelThen {
    fn invoke<'a>(&'a self, call: ToolCall, _next: Next<'a>) -> BoxFuture<'a, Outcome> {
        self.token.cancel();
        let tool = call.name().to_string();
        let error = if self.deny {
            StrataError::ToolCallDenied {
                tool,
                reason: "no".to_string(),
            }
        } else {
            StrataError::ToolCallFailed {
                tool,
                message: "stopped".to_string(),
            }
        };
        Box::pin(std::future::ready(Err(error)))
    }
}

/// Passes calls on; its on-refused hook fails, as an audit that can't write would.
struct BrokenAudit;

impl ToolMiddleware for BrokenAudit {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        next.run(call)
    }

    fn on_refused<'a>(
        &'a self,
        call: &'a ToolCall,
        _refusal: &'a StrataError,
    ) -> BoxFuture<'a, Result<(), StrataError>> {
        Box::pin(async move {
            Err(StrataError::ToolCallFailed {
                tool: call.name().to_string(),
                message: "audit is down".to_string(),
            })
        })
    }
}

/// Keeps the refused call it was told about.
#[derive(Default)]
struct SeesRefused(Mutex<Option<(String, bool)>>);

impl ToolMiddleware for SeesRefused {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        next.run(call)
    }

    fn on_refused<'a>(
        &'a self,
        call: &'a ToolCall,
        _refusal: &'a StrataError,
    ) -> BoxFuture<'a, Result<(), StrataError>> {
        *self.0.lock().unwrap() = Some((call.name().to_string(), call.schema().is_some()));
        Box::pin(async { Ok(()) })
    }
}

/// A sample, not shipped (`design/23-Tool-Middleware.md`, "Not built"):
/// denies a call missing an argument its manifest entry requires.
struct RequiredArguments;

impl ToolMiddleware for RequiredArguments {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Outcome> {
        let missing = call.schema().and_then(|schema| {
            let arguments: serde_json::Value = serde_json::from_str(call.arguments()).ok()?;
            schema["parameters"]["required"]
                .as_array()?
                .iter()
                .filter_map(|name| name.as_str())
                .find(|name| arguments.get(name).is_none())
                .map(str::to_string)
        });
        match missing {
            Some(name) => Box::pin(std::future::ready(Err(StrataError::ToolCallDenied {
                tool: call.name().to_string(),
                reason: format!("missing required argument '{name}'"),
            }))),
            None => next.run(call),
        }
    }
}

// --- Order ---------------------------------------------------------------------

#[tokio::test]
async fn middleware_runs_in_registration_order_outermost_first() {
    let probe = Arc::new(Probe::default());
    let trace = trace();
    let mut tools = registry();
    tools
        .use_middleware(recording("a", &trace))
        .use_middleware(recording("b", &trace))
        .use_middleware(recording("c", &trace));

    let result = tools
        .dispatch_with("mw.echo", &echo("hi"), with_probe(&probe))
        .await;

    assert_eq!(result.unwrap(), "hi");
    assert_eq!(
        entries(&trace),
        ["a in", "b in", "c in", "c out", "b out", "a out"]
    );
}

#[tokio::test]
async fn a_registry_without_middleware_dispatches_as_before() {
    let probe = Arc::new(Probe::default());
    let tools = registry();

    let result = tools
        .dispatch_with("mw.echo", &echo("hi"), with_probe(&probe))
        .await;
    let unknown = tools.dispatch("mw.nope", "{}").await;

    assert_eq!(result.unwrap(), "hi");
    assert!(matches!(unknown, Err(StrataError::ToolNotFound(_))));
    assert_eq!(probe.runs(), 1);
}

// --- Short-circuit, denial, failure ------------------------------------------------

#[tokio::test]
async fn a_middleware_that_returns_without_calling_next_short_circuits_the_tool() {
    let probe = Arc::new(Probe::default());
    let mut tools = registry();
    tools.use_middleware(Cache::default());

    let run = in_a_node(
        "mw-cache",
        tools,
        &probe,
        vec![("mw.echo", echo("once")), ("mw.echo", echo("once"))],
    )
    .await;

    assert_eq!(probe.runs(), 1);
    assert_eq!(
        run.results,
        [Ok("once".to_string()), Ok("once".to_string())].map(ok_text)
    );
    assert_eq!(
        run.completed.iter().map(|c| c.0).collect::<Vec<_>>(),
        [ToolCallResult::Succeeded, ToolCallResult::Succeeded]
    );
    let spans = run.trace.all("execute_tool mw.echo");
    assert_eq!(spans.len(), 2);
    assert_eq!(spans[0].fields.get("strata.tool.short_circuited"), None);
    assert_eq!(
        spans[1]
            .fields
            .get("strata.tool.short_circuited")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        spans[1]
            .fields
            .get("strata.tool.result")
            .map(String::as_str),
        Some("succeeded")
    );
}

#[tokio::test]
async fn a_denial_is_a_refused_call_with_its_reason() {
    let probe = Arc::new(Probe::default());
    let mut tools = registry();
    tools.use_middleware(Deny("tenant acme may not echo"));

    let run = in_a_node("mw-denied", tools, &probe, vec![("mw.echo", echo("x"))]).await;

    assert_eq!(probe.runs(), 0);
    assert!(matches!(
        &run.results[0],
        Err(text) if text == "ToolCallDenied { tool: \"mw.echo\", reason: \"tenant acme may not echo\" }"
    ));
    let (result, error, _) = &run.completed[0];
    assert_eq!(*result, ToolCallResult::Refused);
    assert_eq!(
        error.as_deref(),
        Some("tool 'mw.echo' was denied: tenant acme may not echo")
    );
    let span = &run.trace.all("execute_tool mw.echo")[0];
    assert_eq!(
        span.fields.get("strata.tool.result").map(String::as_str),
        Some("refused")
    );
    assert_eq!(
        span.fields.get("error.type").map(String::as_str),
        Some("denied")
    );
    assert_eq!(
        span.fields.get("otel.status_code").map(String::as_str),
        Some("ERROR")
    );
}

#[tokio::test]
async fn a_middleware_failure_is_a_failed_call() {
    let probe = Arc::new(Probe::default());
    let mut tools = registry();
    tools.use_middleware(Fail);

    let run = in_a_node("mw-failed", tools, &probe, vec![("mw.echo", echo("x"))]).await;

    assert_eq!(probe.runs(), 0);
    let (result, error, _) = &run.completed[0];
    assert_eq!(*result, ToolCallResult::Failed);
    assert_eq!(
        error.as_deref(),
        Some("tool 'mw.echo' failed: the policy backend is down")
    );
    let span = &run.trace.all("execute_tool mw.echo")[0];
    assert_eq!(
        span.fields.get("error.type").map(String::as_str),
        Some("failed")
    );
}

#[tokio::test]
async fn a_timeout_middleware_stops_waiting_for_a_tool_that_ignores_its_token() {
    let probe = Arc::new(Probe::default());
    let mut tools = registry();
    tools.use_middleware(Timeout(Duration::from_millis(100)));

    let run = in_a_node(
        "mw-timeout",
        tools,
        &probe,
        vec![("mw.stall", "{}".to_string())],
    )
    .await;

    assert_eq!(probe.runs(), 1);
    assert!(matches!(&run.results[0], Err(text) if text.starts_with("ToolCallFailed")));
    // The middleware gave up, not the run: a failure, not a cancellation.
    assert_eq!(run.completed[0].0, ToolCallResult::Failed);
}

// --- What a middleware may change --------------------------------------------------

#[tokio::test]
async fn a_middleware_can_enrich_the_context_and_replace_the_arguments() {
    let probe = Arc::new(Probe::default());
    let mut tools = registry();
    tools.use_middleware(Enrich);

    let result = tools
        .dispatch_with(
            "mw.echo",
            &echo("secret"),
            with_probe(&probe).with_call_id("c1"),
        )
        .await;

    assert_eq!(result.unwrap(), "redacted");
    assert_eq!(
        probe.seen.lock().unwrap().as_slice(),
        [(
            Some("gateway".to_string()),
            Some("acme".to_string()),
            Some("c1".to_string())
        )]
    );
}

#[tokio::test]
async fn a_middleware_sees_the_tools_schema_and_can_validate_against_it() {
    let probe = Arc::new(Probe::default());
    let mut tools = registry();
    tools.use_middleware(RequiredArguments);

    let denied = tools
        .dispatch_with("mw.echo", "{}", with_probe(&probe))
        .await;
    let allowed = tools
        .dispatch_with("mw.echo", &echo("ok"), with_probe(&probe))
        .await;

    assert!(matches!(
        denied,
        Err(StrataError::ToolCallDenied { reason, .. }) if reason == "missing required argument 'text'"
    ));
    assert_eq!(allowed.unwrap(), "ok");
    assert_eq!(probe.runs(), 1);
}

#[tokio::test]
async fn a_middleware_may_call_next_more_than_once() {
    let probe = Arc::new(Probe::default());
    let mut tools = registry();
    tools.use_middleware(Twice);

    let result = tools
        .dispatch_with("mw.echo", &echo("twice"), with_probe(&probe))
        .await;

    assert_eq!(result.unwrap(), "twice");
    assert_eq!(probe.runs(), 2);
}

#[tokio::test]
async fn one_middleware_can_be_shared_between_registries() {
    let probe = Arc::new(Probe::default());
    let cache = Arc::new(Cache::default());
    let mut first = registry();
    let mut second = registry();
    first.use_middleware(cache.clone());
    second.use_middleware(cache);

    first
        .dispatch_with("mw.echo", &echo("shared"), with_probe(&probe))
        .await
        .unwrap();
    second
        .dispatch_with("mw.echo", &echo("shared"), with_probe(&probe))
        .await
        .unwrap();

    assert_eq!(probe.runs(), 1);
}

// --- Subsets --------------------------------------------------------------------------

#[tokio::test]
async fn a_subset_shares_its_parents_middleware_and_adds_its_own_inside_it() {
    let probe = Arc::new(Probe::default());
    let trace = trace();
    let mut parent = registry();
    parent.use_middleware(recording("gateway", &trace));
    let mut subset = parent.subset(["mw.echo"]).unwrap();
    subset.use_middleware(recording("agent", &trace));
    let earlier = parent.subset(["mw.echo"]).unwrap();
    parent.use_middleware(recording("late", &trace));

    subset
        .dispatch_with("mw.echo", &echo("1"), with_probe(&probe))
        .await
        .unwrap();
    assert_eq!(
        entries(&trace),
        ["gateway in", "agent in", "agent out", "gateway out"]
    );

    trace.lock().unwrap().clear();
    parent
        .dispatch_with("mw.echo", &echo("2"), with_probe(&probe))
        .await
        .unwrap();
    assert_eq!(
        entries(&trace),
        ["gateway in", "late in", "late out", "gateway out"]
    );

    trace.lock().unwrap().clear();
    earlier
        .dispatch_with("mw.echo", &echo("3"), with_probe(&probe))
        .await
        .unwrap();
    assert_eq!(entries(&trace), ["gateway in", "gateway out"]);
}

// --- Refused calls -----------------------------------------------------------------------

#[tokio::test]
async fn refused_calls_reach_every_on_refused_hook_and_no_invoke() {
    let trace = trace();
    let mut tools = registry().subset(["mw.echo"]).unwrap();
    tools
        .use_middleware(recording("a", &trace))
        .use_middleware(recording("b", &trace));

    let not_permitted = tools.dispatch("mw.other", "{}").await;
    let not_found = tools.dispatch("mw.nope", "{}").await;

    assert!(matches!(
        not_permitted,
        Err(StrataError::ToolNotPermitted(_))
    ));
    assert!(matches!(not_found, Err(StrataError::ToolNotFound(_))));
    assert_eq!(
        entries(&trace),
        [
            "a refused mw.other ToolNotPermitted",
            "b refused mw.other ToolNotPermitted",
            "a refused mw.nope ToolNotFound",
            "b refused mw.nope ToolNotFound",
        ]
    );
}

#[tokio::test]
async fn a_refused_call_hook_sees_no_schema_and_runs_even_when_cancelled() {
    let sees = Arc::new(SeesRefused::default());
    let mut tools = registry();
    tools.use_middleware(sees.clone());
    let token = CancellationToken::new();
    token.cancel();

    let result = tools
        .dispatch_with(
            "mw.nope",
            "{}",
            ToolCallContext::new().with_cancellation(token),
        )
        .await;

    assert!(matches!(result, Err(StrataError::ToolNotFound(_))));
    assert_eq!(
        sees.0.lock().unwrap().clone(),
        Some(("mw.nope".to_string(), false))
    );
}

#[tokio::test]
async fn a_failing_on_refused_hook_replaces_the_refusal_and_stops_the_rest() {
    let trace = trace();
    let mut tools = registry();
    tools
        .use_middleware(BrokenAudit)
        .use_middleware(recording("after", &trace));

    let result = tools.dispatch("mw.nope", "{}").await;

    assert!(matches!(
        result,
        Err(StrataError::ToolCallFailed { message, .. }) if message == "audit is down"
    ));
    assert!(entries(&trace).is_empty());
}

// --- Telemetry and cancellation ----------------------------------------------------------

#[tokio::test]
async fn middleware_runs_inside_the_tool_span() {
    let probe = Arc::new(Probe::default());
    let mut tools = registry();
    tools.use_middleware(Slow);

    let run = in_a_node("mw-span", tools, &probe, vec![("mw.echo", echo("x"))]).await;

    assert!(run.completed[0].2 >= Duration::from_millis(45));
    let tool = run.trace.index_of("execute_tool mw.echo");
    assert_eq!(run.trace.all("middleware-check")[0].parent, Some(tool));
}

#[tokio::test]
async fn an_already_cancelled_call_runs_no_middleware() {
    let probe = Arc::new(Probe::default());
    let trace = trace();
    let mut tools = registry();
    tools.use_middleware(recording("a", &trace));
    let token = CancellationToken::new();
    token.cancel();

    let result = tools
        .dispatch_with(
            "mw.echo",
            &echo("x"),
            with_probe(&probe).with_cancellation(token),
        )
        .await;

    assert!(matches!(result, Err(StrataError::ToolCancelled(_))));
    assert!(entries(&trace).is_empty());
    assert_eq!(probe.runs(), 0);
}

#[tokio::test]
async fn cancelling_the_callers_token_during_the_chain_is_a_cancellation_but_a_denial_stays_one() {
    for (deny, cancelled) in [(false, true), (true, false)] {
        let token = CancellationToken::new();
        let mut tools = registry();
        tools.use_middleware(CancelThen {
            token: token.clone(),
            deny,
        });

        let result = tools
            .dispatch_with(
                "mw.echo",
                &echo("x"),
                ToolCallContext::new().with_cancellation(token),
            )
            .await;

        assert_eq!(
            matches!(result, Err(StrataError::ToolCancelled(_))),
            cancelled,
            "deny: {deny}, {result:?}"
        );
        if deny {
            assert!(matches!(result, Err(StrataError::ToolCallDenied { .. })));
        }
    }
}

// --- Helpers -------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: i64,
}

#[derive(Default)]
struct TestStore {
    checkpoints: Mutex<HashMap<String, Checkpoint>>,
}

impl CheckpointStore for TestStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.checkpoints
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
        Box::pin(std::future::ready(Ok(())))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        let found = self.checkpoints.lock().unwrap().get(thread_id).cloned();
        Box::pin(std::future::ready(Ok(found)))
    }
}

/// A call's result with any error shown as its `Debug` text, so results compare.
type Shown = Result<String, String>;

fn ok_text(result: Outcome) -> Shown {
    result.map_err(|e| format!("{e:?}"))
}

/// What a one-node run of tool calls left behind.
struct NodeRun {
    results: Vec<Shown>,
    /// (result, error, duration) per `ToolCallCompleted`.
    completed: Vec<(ToolCallResult, Option<String>, Duration)>,
    trace: Spans,
}

/// Runs `calls` in order inside a one-node graph whose run carries `probe`.
async fn in_a_node(
    thread_id: &str,
    tools: ToolRegistry,
    probe: &Arc<Probe>,
    calls: Vec<(&'static str, String)>,
) -> NodeRun {
    init();
    let tools = Arc::new(tools);
    let results = Arc::new(Mutex::new(Vec::new()));
    let seen = results.clone();
    let graph = StateGraph::<Job>::new()
        .add_async_node("Agent", move |j: Job, ctx: NodeContext| {
            let (tools, calls, seen) = (tools.clone(), calls.clone(), seen.clone());
            async move {
                for (tool, args) in calls {
                    let result = tools
                        .dispatch_with(tool, &args, ToolCallContext::from(&ctx))
                        .await;
                    seen.lock().unwrap().push(ok_text(result));
                }
                j
            }
        })
        .compile()
        .unwrap();
    let (observer, events) = RunEventChannel::new();
    let config = RunConfig::new()
        .with_observer(observer)
        .with_service(probe.clone());

    tokio::time::timeout(
        PATIENCE,
        graph.run_with(
            thread_id,
            Job { n: 1 },
            Arc::new(TestStore::default()),
            config,
        ),
    )
    .await
    .unwrap();
    let completed = drain(events)
        .await
        .into_iter()
        .filter_map(|event| match event.kind {
            RunEventKind::ToolCallCompleted {
                result,
                error,
                duration,
                ..
            } => Some((result, error, duration)),
            _ => None,
        })
        .collect();
    let results = results.lock().unwrap().clone();
    NodeRun {
        results,
        completed,
        trace: Spans::of_run(thread_id),
    }
}

async fn drain(mut events: mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        all.push(event);
    }
    all
}

// --- A capturing subscriber ------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SpanRecord {
    name: &'static str,
    fields: HashMap<String, String>,
    parent: Option<usize>,
}

impl SpanRecord {
    /// What a trace viewer shows: `otel.name`, or the span's own name.
    fn display(&self) -> &str {
        self.fields
            .get("otel.name")
            .map(String::as_str)
            .unwrap_or(self.name)
    }
}

#[derive(Default)]
struct Captured {
    spans: Vec<SpanRecord>,
    // Span ids are reused once a span closes, so they map to a record only while open.
    open: HashMap<u64, usize>,
}

static CAPTURED: LazyLock<Mutex<Captured>> = LazyLock::new(Mutex::default);

struct Fields<'a>(&'a mut HashMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

struct Capture;

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        attrs.record(&mut Fields(&mut fields));
        let parent = ctx
            .span(id)
            .and_then(|span| span.parent())
            .map(|parent| parent.id().into_u64());
        let mut captured = CAPTURED.lock().unwrap();
        let parent = parent.and_then(|parent| captured.open.get(&parent).copied());
        let index = captured.spans.len();
        captured.spans.push(SpanRecord {
            name: attrs.metadata().name(),
            fields,
            parent,
        });
        captured.open.insert(id.into_u64(), index);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        let mut captured = CAPTURED.lock().unwrap();
        if let Some(&index) = captured.open.get(&id.into_u64()) {
            values.record(&mut Fields(&mut captured.spans[index].fields));
        }
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        CAPTURED.lock().unwrap().open.remove(&id.into_u64());
    }
}

static SUBSCRIBER: LazyLock<()> = LazyLock::new(|| {
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture)).unwrap();
});

/// Installs the subscriber, once for the binary.
fn init() {
    LazyLock::force(&SUBSCRIBER);
}

/// The spans under the run whose `strata.run` span has a thread id, by capture index.
struct Spans(Vec<(usize, SpanRecord)>);

impl Spans {
    fn of_run(thread_id: &str) -> Self {
        let captured = CAPTURED.lock().unwrap();
        let root = captured
            .spans
            .iter()
            .position(|s| {
                s.name == "strata.run"
                    && s.fields.get("strata.thread_id").map(String::as_str) == Some(thread_id)
            })
            .expect("no run span for that thread");
        let under_root = |mut index: usize| loop {
            if index == root {
                return true;
            }
            match captured.spans[index].parent {
                Some(parent) => index = parent,
                None => return false,
            }
        };
        Self(
            captured
                .spans
                .iter()
                .enumerate()
                .filter(|(index, _)| under_root(*index))
                .map(|(index, span)| (index, span.clone()))
                .collect(),
        )
    }

    /// Every span shown as `display`, in the order they started.
    fn all(&self, display: &str) -> Vec<&SpanRecord> {
        self.0
            .iter()
            .filter(|(_, s)| s.display() == display)
            .map(|(_, s)| s)
            .collect()
    }

    fn index_of(&self, display: &str) -> usize {
        let found: Vec<_> = self
            .0
            .iter()
            .filter(|(_, s)| s.display() == display)
            .collect();
        assert_eq!(found.len(), 1, "expected one span shown as '{display}'");
        found[0].0
    }
}
