//! Runtime pieces of STRATA-SPEC-003's tool-calling architecture: the
//! `StrataSchema`/`StrataTool` traits that `datalium-strata-macros`'
//! `#[derive(StrataSchema)]` and `#[strata_tool(...)]` generate implementations
//! of, the [`ToolCallContext`] a call carries, a [`ToolRegistry`] that
//! dispatches by name (`design/20-Tool-Dispatch-And-Registry.md`, SG-21), and
//! the [`ToolMiddleware`] it runs around every call
//! (`design/23-Tool-Middleware.md`, SG-22).
//!
//! Dispatch is a hash lookup on the tool's `&'static str` name and a direct
//! `dyn StrataTool` virtual call — no reflection-based type downcasting
//! anywhere in this path.
//!
//! ```rust,compile_fail
//! // Forgetting `#[derive(StrataSchema)]` on a #[strata_tool] argument struct
//! // is a compile-time trait-bound failure, not a silent runtime schema/argument
//! // mismatch — proving STRATA-SPEC-003's schema-drift mitigation.
//! use datalium_strata::prelude::*;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)] // missing `StrataSchema`
//! struct BadArgs {
//!     sku: String,
//! }
//!
//! #[strata_tool(name = "bad_tool", description = "won't compile")]
//! async fn bad_tool(args: BadArgs) -> Result<String, StrataError> {
//!     Ok(args.sku)
//! }
//! ```
//!
//! ```rust,compile_fail
//! // The context goes second, after the arguments struct.
//! use datalium_strata::prelude::*;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize, StrataSchema)]
//! struct Args {
//!     sku: String,
//! }
//!
//! #[strata_tool(name = "context_first", description = "won't compile")]
//! async fn context_first(ctx: ToolCallContext, args: Args) -> Result<String, StrataError> {
//!     Ok(args.sku)
//! }
//! ```
//!
//! ```rust,compile_fail
//! // A second parameter that isn't the context is refused.
//! use datalium_strata::prelude::*;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize, StrataSchema)]
//! struct Args {
//!     sku: String,
//! }
//!
//! #[strata_tool(name = "two_args", description = "won't compile")]
//! async fn two_args(args: Args, extra: String) -> Result<String, StrataError> {
//!     Ok(args.sku + &extra)
//! }
//! ```
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span};

use crate::StrataError;
use crate::context::{NodeContext, Services, insert_service, service_of};
use crate::node::BoxFuture;
use crate::observability::{RunTelemetry, ToolCallScope};

/// Implemented by `#[derive(StrataSchema)]` on a tool's argument struct, an
/// agent's answer (`design/44-LLM-Integration.md`) or a type either holds.
/// Produces the `parameters` object of a tool's JSON-Schema manifest entry,
/// or the schema an agent's answer must match.
///
/// Strings, booleans, numbers, `Option<T>` (as `T`), sequences (`array` with
/// `items`), maps with string keys (`object` with `additionalProperties`),
/// `Box<T>` and `Arc<T>` implement it too, so a derived type's fields can be
/// any of them, nested to any depth.
pub trait StrataSchema {
    fn json_schema() -> serde_json::Value;
}

macro_rules! schema_of {
    ($json:literal: $($ty:ty),+) => {
        $(impl StrataSchema for $ty {
            fn json_schema() -> serde_json::Value {
                serde_json::json!({ "type": $json })
            }
        })+
    };
}

schema_of!("string": String, str, char);
schema_of!("boolean": bool);
schema_of!("integer": i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize);
schema_of!("number": f32, f64);

fn array_of<T: StrataSchema + ?Sized>() -> serde_json::Value {
    serde_json::json!({ "type": "array", "items": T::json_schema() })
}

fn map_of<T: StrataSchema + ?Sized>() -> serde_json::Value {
    serde_json::json!({ "type": "object", "additionalProperties": T::json_schema() })
}

impl<T: StrataSchema> StrataSchema for Vec<T> {
    fn json_schema() -> serde_json::Value {
        array_of::<T>()
    }
}

impl<T: StrataSchema> StrataSchema for [T] {
    fn json_schema() -> serde_json::Value {
        array_of::<T>()
    }
}

impl<T: StrataSchema, const N: usize> StrataSchema for [T; N] {
    fn json_schema() -> serde_json::Value {
        array_of::<T>()
    }
}

impl<T: StrataSchema> StrataSchema for std::collections::VecDeque<T> {
    fn json_schema() -> serde_json::Value {
        array_of::<T>()
    }
}

impl<T: StrataSchema> StrataSchema for HashSet<T> {
    fn json_schema() -> serde_json::Value {
        array_of::<T>()
    }
}

impl<T: StrataSchema> StrataSchema for std::collections::BTreeSet<T> {
    fn json_schema() -> serde_json::Value {
        array_of::<T>()
    }
}

impl<T: StrataSchema, H> StrataSchema for HashMap<String, T, H> {
    fn json_schema() -> serde_json::Value {
        map_of::<T>()
    }
}

impl<T: StrataSchema> StrataSchema for std::collections::BTreeMap<String, T> {
    fn json_schema() -> serde_json::Value {
        map_of::<T>()
    }
}

/// An optional value is described as its value; whether it may be left out
/// is the containing object's `required` list.
impl<T: StrataSchema> StrataSchema for Option<T> {
    fn json_schema() -> serde_json::Value {
        T::json_schema()
    }
}

impl<T: StrataSchema + ?Sized> StrataSchema for Box<T> {
    fn json_schema() -> serde_json::Value {
        T::json_schema()
    }
}

impl<T: StrataSchema + ?Sized> StrataSchema for Arc<T> {
    fn json_schema() -> serde_json::Value {
        T::json_schema()
    }
}

/// What `#[derive(StrataSchema)]` generates code against; not an API.
///
/// A field whose type implements [`StrataSchema`] is described by it, and
/// any other by `{"type": "object"}`, as every field was before the derive
/// knew nested types: `(&Probe::<T>::new()).schema()` resolves to
/// [`ViaSchema`](__schema::ViaSchema) when `T: StrataSchema` and to
/// [`ViaFallback`](__schema::ViaFallback) otherwise (autoref specialization),
/// so a field of a type nobody derived for still compiles.
#[doc(hidden)]
pub mod __schema {
    use std::marker::PhantomData;

    use super::StrataSchema;

    pub struct Probe<T: ?Sized>(PhantomData<T>);

    impl<T: ?Sized> Probe<T> {
        #[allow(clippy::new_without_default)]
        pub fn new() -> Self {
            Self(PhantomData)
        }
    }

    pub trait ViaSchema {
        fn schema(&self) -> serde_json::Value;
    }

    impl<T: StrataSchema + ?Sized> ViaSchema for Probe<T> {
        fn schema(&self) -> serde_json::Value {
            T::json_schema()
        }
    }

    pub trait ViaFallback {
        fn schema(&self) -> serde_json::Value;
    }

    impl<T: ?Sized> ViaFallback for &Probe<T> {
        fn schema(&self) -> serde_json::Value {
            serde_json::json!({ "type": "object" })
        }
    }
}

/// What a tool call's `arguments` (raw JSON text) got wrong against the
/// tool's `parameters` schema (an object schema with `properties` and
/// `required`), in the words both engines use
/// (`schema/fixtures/tools/argument-problems.json`), joined with `; `, or
/// `None` when they match it. `#[strata_tool]` reports it as
/// [`StrataError::ToolArgumentParseFailure`] when its arguments struct won't
/// deserialize, so an agent can tell the model what to fix; when it explains
/// nothing, serde's own message is reported. Mirrors .NET's
/// `ToolArguments.Problems`.
///
/// Text that isn't JSON is `the arguments aren't valid JSON`; JSON that isn't
/// an object is `the arguments aren't a JSON object`. Otherwise each property
/// of the schema, in byte order of its name: a required one that is absent
/// is `argument 'x' is missing`, null is fine unless it is required, and a
/// value not of the property's `type` is `argument 'x' must be a string` (an
/// integer, a number, true or false, an array, an object). Properties the
/// schema doesn't name, and a property without a known type, aren't checked.
pub fn argument_problems(arguments: &str, parameters: &serde_json::Value) -> Option<String> {
    use serde_json::Value;

    let Ok(arguments) = serde_json::from_str::<Value>(arguments) else {
        return Some("the arguments aren't valid JSON".to_string());
    };
    let Value::Object(arguments) = arguments else {
        return Some("the arguments aren't a JSON object".to_string());
    };
    let properties = parameters["properties"].as_object()?;
    let required: HashSet<&str> = parameters["required"]
        .as_array()
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let mut names: Vec<&String> = properties.keys().collect();
    names.sort();
    let mut problems = Vec::new();
    for name in names {
        let is_required = required.contains(name.as_str());
        let Some(value) = arguments.get(name) else {
            if is_required {
                problems.push(format!("argument '{name}' is missing"));
            }
            continue;
        };
        let (expected, fits): (&str, fn(&Value) -> bool) = match properties[name]["type"].as_str() {
            Some("string") => ("a string", Value::is_string),
            Some("integer") => ("an integer", |v| v.is_i64() || v.is_u64()),
            Some("number") => ("a number", Value::is_number),
            Some("boolean") => ("true or false", Value::is_boolean),
            Some("array") => ("an array", Value::is_array),
            Some("object") => ("an object", Value::is_object),
            _ => continue,
        };
        if (value.is_null() && !is_required) || fits(value) {
            continue;
        }
        problems.push(format!("argument '{name}' must be {expected}"));
    }
    (!problems.is_empty()).then(|| problems.join("; "))
}

/// Implemented by the zero-sized struct `#[strata_tool(...)]` generates for
/// each annotated function.
pub trait StrataTool: Send + Sync {
    fn name(&self) -> &'static str;
    fn schema(&self) -> serde_json::Value;
    fn execute<'a>(
        &'a self,
        raw_args: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, StrataError>> + Send + 'a>>;

    /// Runs the tool with the call's context. `#[strata_tool]` implements it,
    /// passing the context to a function that takes one; the default ignores
    /// the context, for tools written before it existed.
    fn execute_with<'a>(
        &'a self,
        raw_args: &'a str,
        ctx: ToolCallContext,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        drop(ctx);
        self.execute(raw_args)
    }
}

/// What a tool call is told about its caller (`design/20-Tool-Dispatch-And-Registry.md`).
/// Every field is optional, so a tool can be called outside any run; build one
/// from a node's context with `ToolCallContext::from(&ctx)`, then add
/// [`Self::with_caller_id`] / [`Self::with_call_id`]. Mirrors the .NET engine's
/// `ToolCallContext` record.
///
/// Fields are private behind accessors, so new ones can be added without
/// breaking anyone.
#[derive(Clone)]
pub struct ToolCallContext {
    thread_id: Option<Arc<str>>,
    run_id: Option<Arc<str>>,
    node_path: Option<Arc<str>>,
    attempt: Option<u32>,
    metadata: Arc<HashMap<String, String>>,
    caller_id: Option<Arc<str>>,
    call_id: Option<Arc<str>>,
    span: Span,
    services: Arc<Services>,
    cancellation: CancellationToken,
    telemetry: Option<Weak<RunTelemetry>>,
}

impl Default for ToolCallContext {
    /// Nothing known: a call outside any run, with a token nobody cancels.
    fn default() -> Self {
        Self {
            thread_id: None,
            run_id: None,
            node_path: None,
            attempt: None,
            metadata: Arc::new(HashMap::new()),
            caller_id: None,
            call_id: None,
            span: Span::none(),
            services: Arc::new(HashMap::new()),
            cancellation: CancellationToken::new(),
            telemetry: None,
        }
    }
}

impl From<&NodeContext> for ToolCallContext {
    /// Everything the node knows: thread id, run id, node path, attempt,
    /// metadata, its span, services and its token. The caller and call ids
    /// aren't a node's to know.
    fn from(node: &NodeContext) -> Self {
        Self {
            thread_id: Some(Arc::from(node.thread_id())),
            run_id: (!node.run_id().is_empty()).then(|| Arc::from(node.run_id())),
            node_path: Some(Arc::from(node.node_path())),
            attempt: Some(node.attempt()),
            metadata: node.metadata_arc(),
            caller_id: None,
            call_id: None,
            span: node.span().clone(),
            services: node.services_arc(),
            cancellation: node.cancellation().clone(),
            telemetry: node.telemetry(),
        }
    }
}

impl ToolCallContext {
    pub fn new() -> Self {
        Self::default()
    }

    /// The thread id of the run the call is made from, at every depth.
    pub fn thread_id(&self) -> Option<&str> {
        self.thread_id.as_deref()
    }

    /// The id of the run the call is made from.
    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }

    /// The calling node's qualified path (`"Work/Gather/Acme"`).
    pub fn node_path(&self) -> Option<&str> {
        self.node_path.as_deref()
    }

    /// The calling node's attempt.
    pub fn attempt(&self) -> Option<u32> {
        self.attempt
    }

    /// The run's metadata (`tenant`, `incident`, …). Empty when there is none.
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Who is calling, typically the agent's identity. Opaque to Strata.
    pub fn caller_id(&self) -> Option<&str> {
        self.caller_id.as_deref()
    }

    /// The model's tool-call id. A [`ToolRegistry`] generates one (a UUID v7)
    /// when the caller didn't, so a tool it dispatches always has one.
    pub fn call_id(&self) -> Option<&str> {
        self.call_id.as_deref()
    }

    /// The span the call runs inside: the tool's `strata.tool` span when a
    /// [`ToolRegistry`] dispatched it, otherwise what the caller supplied
    /// (a node's attempt span, via `From`). [`Span::none`] when unset.
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// The service of type `T` the run (or [`Self::with_service`]) supplied, if any.
    pub fn service<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        service_of(&self.services)
    }

    /// The call's token: from a node, the node's (cancelled when the run is
    /// cancelled or its deadline passes, the node times out, or a sibling
    /// branch fails). A token nobody cancels by default.
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub fn with_thread_id(mut self, thread_id: impl Into<Arc<str>>) -> Self {
        self.thread_id = Some(thread_id.into());
        self
    }

    pub fn with_run_id(mut self, run_id: impl Into<Arc<str>>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    pub fn with_node_path(mut self, node_path: impl Into<Arc<str>>) -> Self {
        self.node_path = Some(node_path.into());
        self
    }

    pub fn with_attempt(mut self, attempt: u32) -> Self {
        self.attempt = Some(attempt);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        Arc::make_mut(&mut self.metadata).insert(key.into(), value.into());
        self
    }

    pub fn with_caller_id(mut self, caller_id: impl Into<Arc<str>>) -> Self {
        self.caller_id = Some(caller_id.into());
        self
    }

    pub fn with_call_id(mut self, call_id: impl Into<Arc<str>>) -> Self {
        self.call_id = Some(call_id.into());
        self
    }

    pub fn with_span(mut self, span: Span) -> Self {
        self.span = span;
        self
    }

    pub fn with_service<T: Send + Sync + 'static>(mut self, service: Arc<T>) -> Self {
        insert_service(Arc::make_mut(&mut self.services), service);
        self
    }

    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = token;
        self
    }

    /// The run's telemetry, while the run is still around: what puts a call's
    /// events in its run's event stream.
    pub(crate) fn telemetry(&self) -> Option<Arc<RunTelemetry>> {
        self.telemetry.as_ref().and_then(Weak::upgrade)
    }
}

/// One tool call as a [`ToolMiddleware`] sees it, after the registry resolved
/// the name (`design/23-Tool-Middleware.md`). Derive a changed call with
/// [`Self::with_arguments`] / [`Self::with_context`]; the name can't change, so
/// the end of the chain always runs the tool the registry resolved. Mirrors
/// the .NET engine's `ToolCall` record.
#[derive(Clone)]
pub struct ToolCall {
    name: Arc<str>,
    arguments: String,
    context: ToolCallContext,
    tool: Option<Arc<dyn StrataTool>>,
}

impl ToolCall {
    /// A call to `name` with `arguments` (JSON) and `ctx`, with no resolved tool.
    pub fn new(
        name: impl Into<Arc<str>>,
        arguments: impl Into<String>,
        ctx: ToolCallContext,
    ) -> Self {
        Self {
            name: name.into(),
            arguments: arguments.into(),
            context: ctx,
            tool: None,
        }
    }

    /// The tool's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The call's JSON arguments.
    pub fn arguments(&self) -> &str {
        &self.arguments
    }

    /// The call's context, carrying the `strata.tool` span unless a
    /// middleware replaced it.
    pub fn context(&self) -> &ToolCallContext {
        &self.context
    }

    /// The tool's manifest entry, built on demand; `None` for a call the
    /// registry refused.
    pub fn schema(&self) -> Option<serde_json::Value> {
        self.tool.as_ref().map(|tool| tool.schema())
    }

    /// The same call with other arguments (redaction, normalization).
    pub fn with_arguments(mut self, arguments: impl Into<String>) -> Self {
        self.arguments = arguments.into();
        self
    }

    /// The same call with another context: typically the old one, enriched
    /// through its builders.
    pub fn with_context(mut self, ctx: ToolCallContext) -> Self {
        self.context = ctx;
        self
    }
}

/// Runs around every tool call a [`ToolRegistry`] dispatches
/// (`design/23-Tool-Middleware.md`), registered with
/// [`ToolRegistry::use_middleware`]: outermost first, inside the call's
/// `strata.tool` span. Mirrors the .NET engine's `IToolMiddleware`.
///
/// A middleware may run `next` (once, more than once, or with a changed call),
/// return a result without running it (a short-circuit, reported succeeded),
/// return [`StrataError::ToolCallDenied`] to refuse the call (reported refused,
/// `denied`), or any other error to fail it ([`StrataError::ToolCallFailed`]
/// says why).
pub trait ToolMiddleware: Send + Sync {
    /// Handles `call`, continuing the chain with `next` or not.
    fn invoke<'a>(
        &'a self,
        call: ToolCall,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<String, StrataError>>;

    /// Told of a call the registry refused before the chain ran: a name it
    /// doesn't have ([`StrataError::ToolNotFound`]) or that its subset left
    /// out ([`StrataError::ToolNotPermitted`]). Runs in registration order,
    /// even when the token has fired, and can't turn the refusal into a
    /// result. An `Err` ends the call in place of the refusal, and the hooks
    /// after it don't run. The default does nothing.
    fn on_refused<'a>(
        &'a self,
        call: &'a ToolCall,
        refusal: &'a StrataError,
    ) -> BoxFuture<'a, Result<(), StrataError>> {
        let _ = (call, refusal);
        Box::pin(async { Ok(()) })
    }
}

impl<M: ToolMiddleware + ?Sized> ToolMiddleware for Arc<M> {
    fn invoke<'a>(
        &'a self,
        call: ToolCall,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        (**self).invoke(call, next)
    }

    fn on_refused<'a>(
        &'a self,
        call: &'a ToolCall,
        refusal: &'a StrataError,
    ) -> BoxFuture<'a, Result<(), StrataError>> {
        (**self).on_refused(call, refusal)
    }
}

/// The rest of a tool call's middleware chain, ending at the tool
/// (`design/23-Tool-Middleware.md`). `Copy`, so a middleware can run it more
/// than once (a retry).
#[derive(Clone, Copy)]
pub struct Next<'a> {
    middleware: &'a [Arc<dyn ToolMiddleware>],
    terminal: &'a Terminal,
}

impl<'a> Next<'a> {
    /// Runs the rest of the chain, then the tool, with `call`.
    pub fn run(&self, call: ToolCall) -> BoxFuture<'a, Result<String, StrataError>> {
        match self.middleware.split_first() {
            Some((first, rest)) => first.invoke(
                call,
                Next {
                    middleware: rest,
                    terminal: self.terminal,
                },
            ),
            None => self.terminal.run(call),
        }
    }
}

/// The end of a call's chain: the tool the registry resolved, and whether it ran.
struct Terminal {
    tool: Arc<dyn StrataTool>,
    ran: AtomicBool,
}

impl Terminal {
    fn run(&self, call: ToolCall) -> BoxFuture<'_, Result<String, StrataError>> {
        Box::pin(async move {
            self.ran.store(true, Ordering::Relaxed);
            // The resolved tool, not the call's name: a middleware can't route
            // to another tool.
            let ToolCall {
                arguments, context, ..
            } = call;
            self.tool.execute_with(&arguments, context).await
        })
    }
}

/// Aggregates tools and dispatches to them by name, refusing duplicate names
/// at registration (`design/20-Tool-Dispatch-And-Registry.md`). Built once at
/// startup from `#[strata_tool]`-generated handles; `subset` gives one agent a
/// curated view. Mirrors the .NET engine's `ToolRegistry`.
///
/// Every dispatch is traced: a `strata.tool` span, the `strata.tool.*`
/// metrics, and, for a call made from a node, `ToolCallStarted` /
/// `ToolCallCompleted` run events. Middleware added with
/// [`Self::use_middleware`] runs around every call, inside its span
/// (`design/23-Tool-Middleware.md`).
#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: Vec<Arc<dyn StrataTool>>,
    index: HashMap<&'static str, usize>,
    /// Names a subset left out of its parent: refused as not permitted, not
    /// as not found.
    excluded: Arc<HashSet<&'static str>>,
    /// Outermost first.
    middleware: Vec<Arc<dyn ToolMiddleware>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `tool`.
    ///
    /// # Panics
    ///
    /// If a tool with the same name is already registered: a programming
    /// error found at startup. [`Self::try_register`] returns it instead.
    pub fn register(&mut self, tool: Box<dyn StrataTool>) -> &mut Self {
        if let Err(StrataError::DuplicateTool(name)) = self.try_register_arc(Arc::from(tool)) {
            panic!("tool '{name}' is already registered");
        }
        self
    }

    /// Adds `tool`, or returns [`StrataError::DuplicateTool`] (registering
    /// nothing) if a tool with the same name is already registered.
    pub fn try_register(&mut self, tool: Box<dyn StrataTool>) -> Result<&mut Self, StrataError> {
        self.try_register_arc(Arc::from(tool))?;
        Ok(self)
    }

    fn try_register_arc(&mut self, tool: Arc<dyn StrataTool>) -> Result<(), StrataError> {
        let name = tool.name();
        if self.index.contains_key(name) {
            return Err(StrataError::DuplicateTool(name.to_string()));
        }
        self.index.insert(name, self.tools.len());
        self.tools.push(tool);
        Ok(())
    }

    /// Adds `middleware` to the end of the chain every call runs through, so
    /// it runs inside the middleware added before it
    /// (`design/23-Tool-Middleware.md`). A subset made afterwards shares it;
    /// one made before doesn't. Pass an `Arc` to share one middleware between
    /// registries.
    pub fn use_middleware(&mut self, middleware: impl ToolMiddleware + 'static) -> &mut Self {
        self.middleware.push(Arc::new(middleware));
        self
    }

    /// A new registry holding only the named tools, in this registry's order
    /// (not the order given), so agents sharing tools list them identically.
    /// Calling a tool of this registry that the subset left out is refused
    /// with [`StrataError::ToolNotPermitted`].
    ///
    /// A snapshot: tools registered here afterwards don't appear in it. It
    /// shares this registry's middleware as it is now, and middleware added
    /// to the subset runs inside it. A repeated name counts once, and an
    /// empty subset is allowed.
    ///
    /// # Errors
    ///
    /// [`StrataError::ToolNotFound`] for a name this registry never had, and
    /// [`StrataError::ToolNotPermitted`] for one it had excluded itself (a
    /// subset can only narrow).
    pub fn subset<I, N>(&self, names: I) -> Result<ToolRegistry, StrataError>
    where
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let mut chosen = HashSet::new();
        for name in names {
            let name = name.as_ref();
            match self.index.get_key_value(name) {
                Some((&name, _)) => {
                    chosen.insert(name);
                }
                None => return Err(self.missing(name)),
            }
        }
        let mut subset = ToolRegistry::new();
        let mut excluded = (*self.excluded).clone();
        for tool in &self.tools {
            if chosen.contains(tool.name()) {
                subset.try_register_arc(tool.clone())?;
            } else {
                excluded.insert(tool.name());
            }
        }
        subset.excluded = Arc::new(excluded);
        subset.middleware = self.middleware.clone();
        Ok(subset)
    }

    /// The registered tools' names, in registration order.
    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.tools.iter().map(|tool| tool.name())
    }

    /// The merged JSON-Schema manifest array, in registration order, suitable
    /// for handing to an LLM's tool-calling API.
    pub fn manifest(&self) -> serde_json::Value {
        serde_json::Value::Array(self.tools.iter().map(|tool| tool.schema()).collect())
    }

    /// [`Self::dispatch_with`] with no context.
    pub async fn dispatch(&self, name: &str, raw_args: &str) -> Result<String, StrataError> {
        self.dispatch_with(name, raw_args, ToolCallContext::default())
            .await
    }

    /// Runs the tool `name` with `raw_args` (its JSON arguments) and `ctx`.
    ///
    /// The tool doesn't run if `ctx`'s token has already been cancelled, and
    /// neither does any middleware. Otherwise the call runs through the
    /// middleware, outermost first, then the tool. An error returned after
    /// the token fired counts as a cancellation, except a middleware's
    /// [`StrataError::ToolCallDenied`]: both return
    /// [`StrataError::ToolCancelled`]. A tool that ignores its token is not
    /// abandoned; inside a graph, the engine drops the node's future (and this
    /// one with it) when it gives up on the node. A refused name runs each
    /// middleware's [`ToolMiddleware::on_refused`] instead.
    pub async fn dispatch_with(
        &self,
        name: &str,
        raw_args: &str,
        ctx: ToolCallContext,
    ) -> Result<String, StrataError> {
        let ctx = match ctx.call_id {
            Some(_) => ctx,
            None => ctx.with_call_id(uuid::Uuid::now_v7().hyphenated().to_string()),
        };
        let tool = self.index.get(name).map(|&index| self.tools[index].clone());
        let scope =
            ToolCallScope::start(name, tool.is_some() || self.excluded.contains(name), &ctx);
        let span = scope.span.clone();
        let token = ctx.cancellation.clone();
        let ctx = ctx.with_span(span.clone());
        let result = async {
            let Some(tool) = tool else {
                let refusal = self.missing(name);
                let refused = ToolCall::new(name, raw_args, ctx);
                for middleware in &self.middleware {
                    middleware.on_refused(&refused, &refusal).await?;
                }
                return Err(refusal);
            };
            if token.is_cancelled() {
                return Err(StrataError::ToolCancelled(name.to_string()));
            }
            let terminal = Terminal {
                tool: tool.clone(),
                ran: AtomicBool::new(false),
            };
            let call = ToolCall {
                tool: Some(tool),
                ..ToolCall::new(name, raw_args, ctx)
            };
            let chain = Next {
                middleware: &self.middleware,
                terminal: &terminal,
            };
            match chain.run(call).await {
                // A denial is a decision, not a consequence of the token.
                Err(error)
                    if token.is_cancelled()
                        && !matches!(error, StrataError::ToolCallDenied { .. }) =>
                {
                    Err(StrataError::ToolCancelled(name.to_string()))
                }
                Ok(result) => {
                    if !terminal.ran.load(Ordering::Relaxed) {
                        scope.short_circuited();
                    }
                    Ok(result)
                }
                result => result,
            }
        }
        .instrument(span)
        .await;
        scope.completed(&result);
        result
    }

    /// Why `name` isn't here: left out by a subset, or never registered.
    fn missing(&self, name: &str) -> StrataError {
        if self.excluded.contains(name) {
            StrataError::ToolNotPermitted(name.to_string())
        } else {
            StrataError::ToolNotFound(name.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoTool;

    impl StrataTool for EchoTool {
        fn name(&self) -> &'static str {
            "echo"
        }

        fn schema(&self) -> serde_json::Value {
            serde_json::json!({ "name": "echo" })
        }

        fn execute<'a>(
            &'a self,
            raw_args: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<String, StrataError>> + Send + 'a>> {
            Box::pin(async move { Ok(raw_args.to_string()) })
        }
    }

    #[tokio::test]
    async fn dispatch_finds_registered_tool_by_name() {
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(EchoTool));

        let result = registry.dispatch("echo", "hello").await.unwrap();
        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn dispatch_reports_tool_not_found_for_unknown_names() {
        let registry = ToolRegistry::new();
        let err = registry.dispatch("missing", "{}").await.unwrap_err();
        assert!(matches!(err, StrataError::ToolNotFound(name) if name == "missing"));
    }

    #[test]
    fn manifest_collects_every_registered_tool_schema() {
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(EchoTool));
        let manifest = registry.manifest();
        assert_eq!(manifest.as_array().unwrap().len(), 1);
        assert_eq!(manifest[0]["name"], "echo");
    }

    #[tokio::test]
    async fn a_tool_written_before_the_context_existed_runs_through_dispatch_with() {
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(EchoTool));

        let ctx = ToolCallContext::new().with_caller_id("agent");
        let result = registry.dispatch_with("echo", "hi", ctx).await.unwrap();
        assert_eq!(result, "hi");
    }
}
