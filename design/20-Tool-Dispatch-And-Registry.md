# Datalium Strata: Tool Dispatch with Context and Cancellation, and a Tool Registry

Tools (STRATA-SPEC-003) predate everything in `design/17` to `19`, and it shows:

- **No context.** .NET's `IStrataToolProvider.DispatchToolAsync(toolName, jsonArguments)` and Rust's `ToolRegistry::dispatch(name, raw_args)` carry only the call itself. A tool can't tell which thread, run, node, tenant or agent it is working for, so a downstream product carries that through an `AsyncLocal`, the pattern design/17 removed for nodes.
- **No cancellation.** A tool can't be told that its run was cancelled, timed out or passed its deadline.
- **No .NET registry, and a weak Rust one.** Each generated .NET provider stands alone: nothing merges manifests or routes by name. Rust has a `ToolRegistry`, but it dispatches by linear scan and doesn't notice duplicate names (the first registered wins, and the second is silently shadowed).
- **No curated subsets.** An agent should see, and be able to call, only the tools it is allowed (StrataOps Arch §5.3: `tools: [k8s.read, prometheus.query]`).
- **Invisible to telemetry.** design/18 left tool requests to SG-21, so a tool call is neither a span, a metric nor a run event.

This document specifies SG-21, which closes all five in one change. The SG numbers refer to the gap log of StrataOps (not public yet). Both engines get the capability in one change (strict parity, StrataOps ADR 0003). The APIs are idiomatic in each language, not identical, and the two tool systems already differ by design: C# tools are methods with any number of parameters, Rust tools are free `async fn`s with one arguments struct.

It was written as a blueprint before implementation, reviewed, and is now built. "Review decisions" settles the questions the review was asked, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Context shape | **`ToolCallContext`**. .NET: a `sealed record` with `init` properties and no positional parameters, so `with` works and new properties are non-breaking. Rust: a struct with private fields, accessors and `with_*` builders, like `NodeContext`. |
| What it carries | Thread id, run id, node path, attempt, run metadata, the current span, services, a caller id (the agent's identity) and a call id. In Rust, also the cancellation token. **Every field is optional**, so a tool can be called outside a graph (a test, a script, an MCP server). |
| Building one from a node | `ToolCallContext.From(nodeContext)` / `ToolCallContext::from(&node_context)` copies everything the node knows. The caller adds `CallerId` / `CallId`. |
| Cancellation | .NET: a separate `CancellationToken` parameter, as for node handlers. Rust: on the context (`ctx.cancellation()`), as for `NodeContext`. **Cooperative**: the registry refuses to start a call whose token has already fired, and otherwise doesn't abandon a running tool (the engine already abandons the node around it). |
| .NET compatibility | **Source-compatible.** `IStrataToolProvider` gains `DispatchToolAsync(name, args, ToolCallContext, CancellationToken)` as a **default interface method** that forwards to the existing two-argument one. Generated providers implement both. Hand-written providers keep compiling unchanged. |
| Rust compatibility | `StrataTool` gains `execute_with(raw_args, ctx)` with a default that calls `execute`, so hand-written tools keep compiling. `ToolRegistry::dispatch` is unchanged, and `dispatch_with(name, args, ctx)` is new (the `run` / `run_with` convention). Breaking only in two small ways: `StrataError` gains variants, and `register` panics on a duplicate (see "Migration"). |
| Tools opting in | .NET: a `[StrataTool]` method may take a `ToolCallContext` and/or a `CancellationToken` parameter, in any position. Rust: a `#[strata_tool]` fn may take `ctx: ToolCallContext` as a second parameter. Neither appears in the manifest. |
| Registry | **.NET gets `ToolRegistry`**, and the Rust one is brought up to it: aggregates providers / tools, **refuses duplicate names at registration**, exposes one merged manifest in registration order, and dispatches by hash lookup. The .NET registry is itself an `IStrataToolProvider`. |
| Curated subsets | `registry.Subset(names)` / `registry.subset(names)` returns a new registry limited to those tools. An unknown name is refused when the subset is made. Calling a registered tool outside the subset is refused with a **distinct "not permitted" error**, not "not found". |
| Telemetry | **Yes, from the registry.** A `strata.tool` span under the calling node's attempt span, two metrics (`strata.tool.requests`, `strata.tool.duration`), and two run events (`ToolCallStarted`, `ToolCallCompleted`), from one hook, like design/18. Dispatching a provider directly stays the raw, untraced path. |
| Manifest renderers (OpenAI, Anthropic, Gemini, MCP formats) | **Not in the engine.** The provider-neutral JSON-Schema core stays here. The registry exposes the parsed entries so a renderer is a few lines, and the renderers belong with the LLM integration layer (SG-23). See "Manifest renderers". |
| SG-22 (tool middleware) | **Not in this change.** The registry's dispatch path has one choke point for it, and the context is already what middleware will receive. See "Room for SG-22". |
| Wire format | **No change.** Tool manifests and calls aren't on the checkpoint wire. |
| Not in scope | SG-22, SG-23 (LLM integration), SG-24 (model routing), tool-level authorization policy (StrataOps' Tool Gateway owns it: Strata supplies the context and the hooks). |

---

## Usage

```csharp
// Startup: one registry for the process. Providers are ordinary objects, so they can take dependencies.
var tools = new ToolRegistry()
    .Register(new KubernetesTools(k8sPort))
    .Register(new PrometheusTools(metricsPort));          // a duplicate tool name throws here

// Per agent: a curated subset. An unknown name throws here, not at dispatch.
var investigator = tools.Subset("k8s.read", "prometheus.query");

graph.AddNode("Investigate", async (s, ctx, ct) =>
{
    var manifest = investigator.GetToolsManifest();      // only the two tools
    // … the model asks for a tool call …
    var result = await investigator.DispatchToolAsync(
        call.Name, call.Arguments,
        ToolCallContext.From(ctx) with { CallerId = "kubernetes-investigator", CallId = call.Id },
        ct);
    return s with { Evidence = s.Evidence.Add(result) };
});

public partial class KubernetesTools(IKubernetesPort k8s)
{
    [StrataTool(Name = "k8s.read", Description = "Reads a Kubernetes object.")]
    public Task<string> ReadAsync(string kind, string name, ToolCallContext context, CancellationToken ct)
        => k8s.ReadAsync(context.Metadata["tenant"], kind, name, ct);   // kind, name in the manifest; the rest not
}
```

```rust
#[derive(Deserialize, StrataSchema)]
struct ReadArgs { kind: String, name: String }

#[strata_tool(name = "k8s.read", description = "Reads a Kubernetes object.")]
async fn k8s_read(args: ReadArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
    let k8s = ctx.service::<KubeClient>().expect("passed in RunConfig");
    let tenant = ctx.metadata().get("tenant").cloned().unwrap_or_default();
    tokio::select! {
        out = k8s.read(&tenant, &args.kind, &args.name) => out,
        _ = ctx.cancellation().cancelled() => Err(StrataError::ToolCancelled("k8s.read".into())),
    }
}

let mut tools = ToolRegistry::new();
tools.register(Box::new(K8sReadTool)).register(Box::new(PrometheusQueryTool));   // panics on a duplicate
let investigator = Arc::new(tools.subset(["k8s.read", "prometheus.query"])?);   // Err on an unknown name

graph.add_async_node("Investigate", move |s: Incident, ctx: NodeContext| {
    let tools = investigator.clone();
    async move {
        let manifest = tools.manifest();
        // … the model asks for a tool call …
        let call_ctx = ToolCallContext::from(&ctx)
            .with_caller_id("kubernetes-investigator")
            .with_call_id(&call.id);
        let result = tools.dispatch_with(&call.name, &call.arguments, call_ctx).await;
        // …
    }
})
```

---

## The tool call context

```csharp
public sealed record ToolCallContext
{
    public static ToolCallContext None { get; }               // nothing known: a call outside any run

    public string? ThreadId { get; init; }
    public string? RunId { get; init; }
    public string? NodePath { get; init; }
    public int? Attempt { get; init; }
    public IReadOnlyDictionary<string, string> Metadata { get; init; }   // empty by default
    public string? CallerId { get; init; }
    public string? CallId { get; init; }
    public Activity? Activity { get; init; }
    public IServiceProvider? Services { get; init; }

    public static ToolCallContext From(NodeContext node);
}
```

```rust
#[derive(Clone, Default)]
pub struct ToolCallContext { /* private */ }

impl ToolCallContext {
    pub fn thread_id(&self) -> Option<&str>;
    pub fn run_id(&self) -> Option<&str>;
    pub fn node_path(&self) -> Option<&str>;
    pub fn attempt(&self) -> Option<u32>;
    pub fn metadata(&self) -> &HashMap<String, String>;
    pub fn caller_id(&self) -> Option<&str>;
    pub fn call_id(&self) -> Option<&str>;
    pub fn span(&self) -> &Span;                               // Span::none() when unset
    pub fn service<T: Send + Sync + 'static>(&self) -> Option<Arc<T>>;
    pub fn cancellation(&self) -> &CancellationToken;          // a token nobody cancels by default

    // with_thread_id / with_run_id / with_node_path / with_attempt / with_metadata / with_caller_id /
    // with_call_id / with_span / with_service / with_cancellation, for callers outside a graph and for tests
}

impl From<&NodeContext> for ToolCallContext { /* … */ }
```

| Field | From a `NodeContext` | Meaning |
|---|---|---|
| **Thread id, run id** | copied | The outermost run's, as `NodeContext` gives them at every depth. |
| **Node path, attempt** | copied | The calling node's qualified path (`"Work/Gather/Acme"`) and attempt. Together with the thread id, an idempotency key for the call, as design/17 recommends for nodes. |
| **Metadata** | copied | The run's metadata (SG-14): `tenant`, `incident` and so on. Strata gives no key a meaning. A Tool Gateway reads `tenant` from here. |
| **Caller id** | not set | Who is calling, typically the agent's identity (`"kubernetes-investigator"`, or with a version). Opaque to Strata. It goes on the span and the run events. |
| **Call id** | not set | The model's tool-call id, for correlating the result with the request and for audit. If it's missing, the registry generates a UUID v7, as it does for run ids, and passes it on to the tool. |
| **Activity / span** | the node's attempt span | The span the call runs inside. When the registry traces the call, it uses this as the tool span's parent **if nothing is current** (see "Telemetry"), and hands the tool a context whose `Activity` / `span()` is the tool span. |
| **Services** | copied | Whatever `RunOptions.Services` / `RunConfig::with_service` supplied. This is how a Rust tool, a free function with no `self`, reaches a client. A .NET provider usually takes its dependencies through its constructor instead. |
| **Cancellation (Rust)** | the node's token | Cancelled when the node's run is cancelled, its deadline passes, the node times out, or a sibling branch fails. In .NET the same token is the separate `CancellationToken` argument. |

**Why everything is optional.** A context built from a node always has the thread id, run id, node path and attempt, but a tool is also called from tests, scripts and (later) an MCP server, with no run at all. Making those fields required would force such callers to invent values. `ToolCallContext.None` / `ToolCallContext::default()` is the honest "nothing known".

**Why a record in .NET.** `NodeContext` is a sealed class because the engine builds it. A tool context is built by the caller, who takes a node's and adds the caller and call ids. `with` makes that one expression, and SG-22 middleware will want to enrich a context the same way. Adding an `init` property to a record is non-breaking. Its value equality is harmless: the reference-typed members compare by reference.

**Not on the context:** the tool's own name (the tool knows it, and SG-22's invocation object will carry it), the history store, and the stream. Tools that stream are not in scope.

**Run telemetry rides along, privately.** `NodeContext` gains an internal (.NET) / private (Rust) handle to its run's telemetry, and `From` copies it into the tool context. That handle is what lets the registry's run events join the run's event stream (see "Telemetry"). It is not public API, and a context built by hand has none.

---

## Dispatch with context

### .NET

```csharp
public interface IStrataToolProvider
{
    string GetToolsManifest();

    Task<string> DispatchToolAsync(string toolName, string jsonArguments);

    /// Default: ignores the context and the token, for providers written before SG-21.
    Task<string> DispatchToolAsync(string toolName, string jsonArguments,
        ToolCallContext context, CancellationToken cancellationToken = default)
        => DispatchToolAsync(toolName, jsonArguments);
}
```

The two-argument member stays abstract, so a hand-written provider is unaffected. A generated provider implements both: the four-argument one holds the `switch`, and the two-argument one forwards to it with `ToolCallContext.None` and no token. Making the four-argument member the abstract one would be cleaner, but it would break every hand-written provider.

### Rust

```rust
pub trait StrataTool: Send + Sync {
    fn name(&self) -> &'static str;
    fn schema(&self) -> serde_json::Value;
    fn execute<'a>(&'a self, raw_args: &'a str) -> BoxFuture<'a, Result<String, StrataError>>;

    /// Default: ignores the context, for tools written before SG-21.
    fn execute_with<'a>(&'a self, raw_args: &'a str, ctx: ToolCallContext)
        -> BoxFuture<'a, Result<String, StrataError>> {
        self.execute(raw_args)
    }
}
```

`BoxFuture` is the alias design/19 introduced, and it is the same type `execute` returns today, so the existing signature doesn't change. A `#[strata_tool]` handle implements both: `execute_with` calls the function, and `execute` calls `execute_with` with `ToolCallContext::default()`. The context is passed by value, as `NodeContext` is to async handlers: it is a few `Arc`s.

### Cancellation

- **Before the call:** the registry checks the token first. If it has already fired, the tool doesn't run. .NET throws `OperationCanceledException` for that token. Rust returns `Err(StrataError::ToolCancelled(name))`.
- **During the call:** cooperative. The token reaches the tool, and the registry does **not** race the tool against it. design/17's abandonment already happens one level up: when a node's token fires, the engine stops waiting for the node (.NET) or drops its future (Rust, which drops the tool's future with it). Abandoning the tool as well would add nothing inside a graph, and outside one the caller owns the call.
- **Classifying the outcome:** in .NET, an `OperationCanceledException` counts as a cancellation only if the token the registry passed was cancelled (design/17's rule). Otherwise, for example `HttpClient`'s own timeout, it is a failure. In Rust, any `Err` a tool returns **after its token fired** is returned as `ToolCancelled`, and a dispatch future dropped before it finishes counts as a cancellation. (A tool can't return `ToolCancelled` itself and have it survive: `#[strata_tool]` maps every error a tool returns to `ToolExecutionFailure`. See "Implementation Status".)
- **Not a failure:** a cancelled call's span is not marked as an error, as for nodes.

---

## Tools that take the context

### .NET: the source generator

- A `[StrataTool]` method may declare parameters of type `Datalium.Strata.Tools.ToolCallContext` and `System.Threading.CancellationToken`, in any position. The generator recognizes them by full metadata name (it still has no reference to `Datalium.Strata`), passes the dispatch's context and token to them, and leaves them out of the manifest and of the synthesized arguments record.
- Today such a parameter is reported as `STRATA002` (unsupported type), so no existing tool declares one, and nothing that compiles today changes meaning.
- A method whose only parameters are these two has an empty `parameters` object, like a parameterless tool today.
- **New diagnostic `STRATA003`:** two `[StrataTool]` methods in one class with the same tool name. Today that surfaces as the compiler's duplicate `case` label error (`CS0152`) in generated code. Duplicates across classes are the registry's job.
- Tool methods may be instance methods (they already may: the generated `switch` calls them unqualified), so a provider can take its dependencies through its constructor.

### Rust: `#[strata_tool]`

- A tool fn takes either one parameter (the arguments struct, as today) or two: the arguments struct, then `ToolCallContext`. The macro recognizes the second by the last segment of its type path, then passes the context to it. Any other type there fails to compile at the call the macro generates, with an ordinary type-mismatch error. Any other shape (no parameters, three, the context first) keeps the macro's existing "wrong parameters" error, reworded to mention the context.
- A tool fn with no arguments struct is still not supported. That is unchanged and orthogonal.
- The context isn't in the manifest, because the manifest comes from the arguments struct's `StrataSchema`.

---

## The registry

### .NET

```csharp
public sealed class ToolRegistry : IStrataToolProvider
{
    public ToolRegistry Register(IStrataToolProvider provider);          // throws DuplicateToolException
    public ToolRegistry Subset(params IEnumerable<string> toolNames);     // throws ToolNotFoundException
    public IReadOnlyList<ToolDescriptor> Tools { get; }                   // registration order

    public string GetToolsManifest();                                     // merged, registration order
    public Task<string> DispatchToolAsync(string toolName, string jsonArguments);
    public Task<string> DispatchToolAsync(string toolName, string jsonArguments,
        ToolCallContext context, CancellationToken cancellationToken = default);
}

public sealed record ToolDescriptor(string Name, string Description, JsonElement Parameters);

public sealed class DuplicateToolException(string toolName) : InvalidOperationException(…);
public class ToolNotFoundException(string toolName) : Exception(…);         // unsealed
public sealed class ToolNotPermittedException(string toolName) : ToolNotFoundException(…);
```

- **Registration** parses the provider's manifest once, to learn its tool names and entries. That works for hand-written providers too, so the provider interface needs no new member. A name already registered throws `DuplicateToolException` and registers nothing from that provider. Registering a `ToolRegistry` adds its tools, not itself, so a call is traced once, not once per level.
- **Thread safety.** `Register` is synchronized and copy-on-write: dispatch reads an immutable snapshot, so registering while calls are in flight is safe. Registering at startup is the expected use.
- **Dispatch** is a dictionary lookup, then the owning provider's four-argument `DispatchToolAsync`.
- **Implements `IStrataToolProvider`**, so a registry or a subset can be handed to anything that takes a provider.

### Rust

```rust
#[derive(Clone, Default)]
pub struct ToolRegistry { /* Arc'd tools, name → index map, registration order */ }

impl ToolRegistry {
    pub fn new() -> Self;
    pub fn register(&mut self, tool: Box<dyn StrataTool>) -> &mut Self;                   // panics on a duplicate
    pub fn try_register(&mut self, tool: Box<dyn StrataTool>) -> Result<&mut Self, StrataError>;   // Err(DuplicateTool)
    pub fn subset<I, N>(&self, names: I) -> Result<ToolRegistry, StrataError>             // Err(ToolNotFound)
        where I: IntoIterator<Item = N>, N: AsRef<str>;
    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_;                       // registration order
    pub fn manifest(&self) -> serde_json::Value;                                          // merged, registration order
    pub async fn dispatch(&self, name: &str, raw_args: &str) -> Result<String, StrataError>;
    pub async fn dispatch_with(&self, name: &str, raw_args: &str, ctx: ToolCallContext)
        -> Result<String, StrataError>;
}
```

- **`register` panics on a duplicate name.** A duplicate is a programming error found at startup, and keeping `register`'s chaining signature keeps existing code compiling. axum panics on a duplicate route for the same reason. `try_register` returns the error instead, for registries built from configuration.
- Tools are held as `Arc<dyn StrataTool>`, so a subset or a clone shares them. `register` still takes the `Box` that `#[strata_tool]` users already write.
- `ToolRegistry` is `Send + Sync`, so an `Arc<ToolRegistry>` can be put in `RunConfig::with_service` and read back in a node with `ctx.service::<ToolRegistry>()`.
- `dispatch` is `dispatch_with(name, raw_args, ToolCallContext::default())`, so it is traced too.

### Curated subsets

- **What a subset is.** A new registry holding the named tools, in the parent's registration order (not the order the names were given), so two agents that share tools list them identically. The order matters for providers' prompt caching.
- **Unknown names are refused when the subset is made**, not when it is first called. A repeated name counts once. An empty subset is allowed (an agent with no tools). A name the parent never had is "not found"; a name the parent had itself left out (a subset of a subset trying to widen) is "not permitted".
- **A snapshot.** Tools registered on the parent afterwards don't appear in the subset. A curated subset names its tools explicitly, so it shouldn't grow by itself.
- **Outside the subset.** A subset remembers every name its parent had. Calling one of those that isn't in the subset is refused with `ToolNotPermittedException` / `StrataError::ToolNotPermitted`. A name the parent never had is `ToolNotFoundException` / `ToolNotFound`, as today. The difference is a security signal: a model asking for a real tool it wasn't given is either confused or being steered by injected text. In .NET, `ToolNotPermittedException` derives from `ToolNotFoundException`, so an agent loop that already catches "not found" and tells the model "no such tool" handles both correctly.
- **Subsets of subsets** work the same way and can only narrow.
- **Registering into a subset** is allowed, and adds only to that subset. It isn't a reason to make subsets read-only types of their own: one type keeps the API small.

### Errors

| Situation | .NET | Rust |
|---|---|---|
| Duplicate name at registration | `DuplicateToolException` | panic in `register`, `Err(StrataError::DuplicateTool(name))` from `try_register` |
| Unknown name in `Subset` | `ToolNotFoundException`, or `ToolNotPermittedException` for a name a subset left out | `Err(StrataError::ToolNotFound(name))`, or `ToolNotPermitted` likewise |
| Dispatch: no such tool | `ToolNotFoundException` | `StrataError::ToolNotFound(name)` |
| Dispatch: outside the subset | `ToolNotPermittedException` | `StrataError::ToolNotPermitted(name)` |
| Bad arguments | `ToolArgumentsException` (since 2026-10-24; `JsonException` / `ArgumentException` before), see below | `StrataError::ToolArgumentParseFailure`, see below |
| Token already cancelled, or the tool failed because of it | `OperationCanceledException` | `StrataError::ToolCancelled(name)` |
| The tool fails | the tool's own exception, unwrapped, as today | `StrataError::ToolExecutionFailure(name)`, as today |

### Arguments a tool can't read

Added 2026-10-24, from the .NET demos' move to 0.9 (`strata-demos-dotnet`, finding A1). Before, .NET's generated dispatch let the deserializer's `JsonException` escape and threw a plain `ArgumentException` for `null` arguments. So a caller couldn't tell bad arguments from a tool whose own code threw `ArgumentException` (a domain check, say), and the agent refused both back to the model as bad arguments, hiding a real tool failure. The message also named the generated arguments record and a position in text the agent had re-serialized.

- **.NET:** the generated dispatch throws **`ToolArgumentsException`** (`Datalium.Strata.Tools`) with `ToolName` and **`Problems`**, before the tool runs. It is an `ArgumentException`, so code that caught that still does. Nothing else throws one: a tool's own `ArgumentException` or `JsonException` is the tool's failure. A hand-written provider may throw it for the same reasons.
- **The generated dispatch now refuses a missing required argument, and null for a non-nullable one** (`RespectRequiredConstructorParameters`, `RespectNullableAnnotations`), as the manifest says and as serde refuses them for a `#[strata_tool]`. Before, the tool ran with `null` or `0`.
- **Rust** already had its own variant (`ToolArgumentParseFailure`, produced by the macro only). It now holds the same words.
- **The words are the same in both engines** (`ToolArguments.Problems` / `tools::argument_problems(arguments, parameters)`, pinned by `schema/fixtures/tools/argument-problems.json`):
  - text that isn't JSON: `the arguments aren't valid JSON`;
  - JSON that isn't an object: `the arguments aren't a JSON object`;
  - otherwise each property of the tool's `parameters` schema, in byte order of its name. A required one that is absent is `argument 'orderId' is missing`. Null is fine unless the property is required. A value not of its `type` is `argument 'count' must be an integer` (or a string, a number, true or false, an array, an object).
  - Every problem is listed, joined with `; `.
  - When the rule explains nothing (an integer too large for an `int`, or null for an optional parameter that isn't nullable), .NET names the property the parser stopped at (`argument 'x' isn't valid`, `argument 'x' can't be null`), and Rust keeps serde's message.
- **Telemetry:** `error.type` `invalid_arguments` now means exactly this in both engines. In .NET it used to be any `JsonException`, a tool's own included; that is now reported by its type name. The result is still `failed` in the registry and `Refused` in the agent's record (design/44).

---

## Telemetry

design/18 deferred tool requests to SG-21, and StrataOps' architecture (line 960) emits tool spans and metrics itself only because Strata couldn't see tool calls. The registry can, so it reports them through the same kind of hook as nodes: one internal call at the start and one at the end of each dispatch, each recording the event (when there is an observer), opening or closing the span, and updating the metrics.

**Only the registry is traced.** A provider's or a tool's own dispatch method is the raw path, with no telemetry, so tests and composite providers don't double-count.

### Span

| | Value |
|---|---|
| .NET `OperationName` / Rust span name | `strata.tool` |
| Display name (`DisplayName` / `otel.name`) | `execute_tool <tool name>`, the OpenTelemetry GenAI convention |
| Kind | Internal |
| Parent | **Whatever is current**: inside a node, its attempt span, or a span the handler opened around the call (an agent's iteration, say). **When nothing is current**, the context's `Activity` / `span()` (a node's attempt span, via `From`): a spawned Rust task doesn't carry the current span, and a .NET flow with suppressed `ExecutionContext` doesn't carry `Activity.Current`, but the context does. |
| Attributes | `gen_ai.operation.name` = `execute_tool`, `gen_ai.tool.name`, `gen_ai.tool.call.id` (the GenAI semantic conventions, so GenAI-aware backends recognize the span); `strata.thread_id`, `strata.run_id`, `strata.node.path`, `strata.attempt` when the context has them; `strata.tool.caller` when set; `strata.tool.result`; `error.type` when it fails or is refused. |
| `strata.tool.result` | `succeeded` / `failed` / `cancelled` / `refused` |
| `error.type` | `not_found`, `not_permitted`, `invalid_arguments`, then in .NET the exception's type name and in Rust `failed`. |
| Status | `Error` with the message for `failed` and `refused`. A cancellation is not an error. |

The tool runs inside the span: `Activity.Current` in .NET, an instrumented future in Rust, and the context the tool receives carries it. So an `HttpClient` or Kubernetes client span the tool starts nests under the tool span. The span starts before any SG-22 middleware will run, so middleware time (an authorization check, a rate-limit wait) will be inside it.

### Metrics

| Instrument | Type, unit | Attributes | Prometheus | Why |
|---|---|---|---|---|
| `strata.tool.requests` | counter, `{request}` | `gen_ai.tool.name`, `strata.tool.result` | `strata_tool_requests_total` | Arch §16 names this one. Throughput, error and refusal rates per tool. |
| `strata.tool.duration` | histogram, `s` | `gen_ai.tool.name`, `strata.tool.result` | `strata_tool_duration_seconds` | Where a node's time goes. |

- **Bounded attributes only.** A tool name is bounded by what is registered, except for a `not_found` call, whose name comes from the model. Those are recorded without `gen_ai.tool.name`. No thread, run, node path, caller or call id on metrics, as in design/18.
- Rust adds both to `observability::metric_names`.

### Run events

Two new events, recorded only when the context came from a node (it carries the run's telemetry handle). A call made outside a run has spans and metrics, and no events.

| Event | When | Payload |
|---|---|---|
| `ToolCallStarted` | The registry received the call, before resolving the name. | Tool name, call id, caller id. |
| `ToolCallCompleted` | The call ended, however it ended. | Tool name, call id, result (`Succeeded` / `Failed` / `Cancelled` / `Refused`), duration, and the error message for `Failed` / `Refused`. |

- Node path and attempt are the calling node's, from the context, so tool events sit between that node's `NodeStarted` and `NodeCompleted`. Sequence numbers, delivery and the observer's isolation are design/18's.
- **No arguments or results.** design/18's rule that events carry no state applies: arguments can hold secrets, and results can be large. An audit log that needs them belongs to SG-22 middleware.
- Every `ToolCallStarted` has exactly one `ToolCallCompleted`, including refused calls. That is why a refused call gets a `Started` too.
- An abandoned node can call a tool after its run has ended. Those events are dropped, as every event recorded after `RunCompleted` already is.
- .NET: two new sealed records deriving from `RunEvent`, and a `ToolCallResult` enum. Rust: two new `RunEventKind` variants (the enum is `#[non_exhaustive]`, so that isn't breaking) and a `ToolCallResult` enum.

---

## Room for SG-22

> **Built:** SG-22 is specified and built in [`design/23-Tool-Middleware.md`](23-Tool-Middleware.md). It settles the questions left open below: around-style middleware, registration order outermost first, refused calls reaching an on-refused hook rather than the chain, and short-circuiting allowed.

SG-22 will add a middleware pipeline (before, after, on-error) that the registry runs around every dispatch. This change keeps the path open for it without committing to the middleware shape:

- **One choke point.** Every registry dispatch, both overloads in both engines, goes through one private method: resolve the name, start telemetry, check the token, call the tool, end telemetry. SG-22 inserts its chain between "check the token" and "call the tool", so the span and events include middleware time and middleware failures.
- **The context is what middleware will receive.** It already carries the caller, tenant metadata, call id and span. In .NET, middleware can enrich it with `with`. In Rust, with the builders.
- **Subsets will share the parent's middleware.** A subset copies the parent's tools now, and will copy its middleware list the same way, so a gateway registered once applies to every agent's subset.
- **Left to SG-22:** the middleware interface itself, ordering, whether a refused call (not found or not permitted) passes through on-error hooks, and whether middleware can short-circuit with a result. Each of those is additive to what this change builds.

Including SG-22 now was considered and rejected: StrataOps' Tool Gateway works as a decorator around the registry until SG-22 lands (it is P1, M3), and getting the middleware contract right deserves its own review.

---

## Manifest renderers

The gap lists renderers for each model provider's tool format. They are **not in the engine**, for three reasons:

1. **They belong with the code that calls the model.** OpenAI wants `{"type":"function","function":{name, description, parameters}}`, Anthropic `{name, description, input_schema}`, Gemini `functionDeclarations`, and MCP `inputSchema`. Each also has its own rules for "strict" schemas (OpenAI's strict mode requires `additionalProperties: false` and every property listed as required). Those rules change with the provider's API, and SG-23's LLM integration layer is where provider-specific code lives. In .NET, SG-23 will likely bridge to `Microsoft.Extensions.AI`, whose `AIFunction` already renders per provider.
2. **They would be the engine's first provider-specific code.** Nothing else in `strata-core` or `Datalium.Strata` knows a model vendor exists.
3. **The engine's part is making them trivial.** The merged manifest keeps the shared `name` / `description` / `parameters` shape. The .NET registry exposes `Tools` as parsed `ToolDescriptor`s (until now .NET had only a JSON string), and Rust's `manifest()` is already a `serde_json::Value`. Either way, a renderer is a few lines of mapping:

```csharp
var openAiTools = registry.Tools.Select(t => new { type = "function", function = new { t.Name, t.Description, t.Parameters } });
```

---

## Wire format

**None.** Tool manifests and tool calls aren't in checkpoints, graph definitions or history records, and this change adds nothing that is. The manifest's shared shape is unchanged, and property naming stays each language's own (`warehouse_id` in Rust, `warehouseId` in C#). No `schema_version` bump, no DDL change and no new fixtures.

---

## Migration

**.NET** (source-compatible, no behaviour change for existing calls):

- Hand-written `IStrataToolProvider` implementations compile unchanged. The new overload's default ignores the context and the token.
- Generated providers gain the four-argument overload. Existing two-argument calls behave as before.
- `ToolNotFoundException` is no longer `sealed`.
- An observer with an exhaustive `switch` over `RunEvent` sees two new event types (a compiler warning, not an error, where a `switch` expression has no discard arm).

**Rust:**

- `StrataTool` implementations compile unchanged: `execute_with` has a default.
- `ToolRegistry::register` **panics on a duplicate name**. Before, the first registration silently won and the second was unreachable. Use `try_register` to handle it.
- `StrataError` gains `DuplicateTool`, `ToolNotPermitted` and `ToolCancelled`. It isn't `#[non_exhaustive]`, so an exhaustive `match` on it needs arms for them.
- `#[strata_tool]` fns with one parameter compile and behave as before.

**Downstream:**

- `strata-demos-rust` (demos 02 and 03) and `strata-demos-dotnet` (demos 02 and 03) need no change for this. Each registers distinct names, and none matches exhaustively on `StrataError`. (The Rust demos still need design/17's port: see "Implementation Status".)
- StrataOps bumps `strata.lock` in its own PR. M1's Tool Fabric uses `ToolRegistry`, subsets and `ToolCallContext` directly, with no `AsyncLocal` `ToolCatalog`. Its architecture note (line 960) should then say that tool spans and the two tool metrics are Strata's, leaving model, token and cost metrics to StrataOps. The Tool Gateway stays a decorator until SG-22.

---

## Tests

Mirrored per engine:

- **`tool_registry.rs` / `ToolRegistryTests.cs`:** registration across providers, the merged manifest in registration order, duplicate refusal (and `try_register`), registering a registry flattens it, dispatch by name, not found; subsets: order, unknown names refused at creation, not permitted vs not found, subsets of subsets, snapshot semantics, an empty subset; concurrent dispatch during registration (.NET).
- **`tool_context.rs` / `ToolCallContextTests.cs`:** `From` a node copies every field (at depth, in a branch, on a retry); a tool receives the context and the token; a call id is generated when missing and kept when given; an already-cancelled token stops the call before the tool runs; a cancellation inside the tool is classified as cancelled, and a foreign `OperationCanceledException` as a failure (.NET); old-style providers and tools still work through the new overloads.
- **Generator and macro:** `[StrataTool]` methods taking the context, the token, both, in different positions, and only those; the manifest excludes them; `STRATA003` for a duplicate name in one class (driver-based tests, like the existing diagnostics); instance tool methods on a provider with a constructor. Rust: a two-parameter tool, and `compile_fail` doctests for a context in the first position and a wrong second type.
- **Telemetry:** the tool span's parent is the node's attempt span; attributes and result per outcome; the tool's own spans nest under it; metrics recorded with the bounded attributes (no tool name for `not_found`); `ToolCallStarted` / `ToolCallCompleted` in sequence between the node's events; no events outside a run; a dropped Rust dispatch future records `cancelled`.
- **Every existing tool test unchanged**, in `tool_calling.rs`, `tools.rs`'s unit tests and `StrataToolGeneratorTests.cs`.
- **Mutation checks**, as for design/17 to 19: letting a duplicate through, letting a subset call an excluded tool, and parenting the tool span to the run span instead of the attempt span should each fail a test in both engines.

---

## Review decisions

Reviewed 2026-10-02. Every recommendation below was accepted as asked.

### The questions as asked

Recommendations are in bold.

1. **Not permitted vs not found.** A distinct error for a registered tool outside the subset, or plain "not found" so the model can't tell the difference? **Distinct**, derived from `ToolNotFoundException` in .NET so existing handling still works. The model only sees what the agent loop chooses to tell it.
2. **Tool run events.** Add `ToolCallStarted` / `ToolCallCompleted` to the run event stream, or keep tool calls to spans and metrics? **Add them**: StrataOps' `ops.execution.events` carries tool events, and design/18's principle is that events, spans and metrics come from one hook.
3. **Rust duplicate registration.** Panic in `register` plus a `try_register`, or change `register` to return a `Result`? **Panic plus `try_register`**: it keeps the chaining signature and every existing call compiling.
4. **`ToolCallContext` in .NET.** A `sealed record` (for `with`) or a sealed class like `NodeContext`? **A record.**
5. **SG-22.** Out of this change, with the seam above? **Out.**
6. **Manifest renderers.** Out of the engine, with `ToolDescriptor` making them trivial? **Out**, to SG-23.

---

## Implementation Status

Built 2026-10-02 in both engines, in one change, as specified above. These are the differences and details.

**Where it diverged, and why:**

- **The tool span's parent is the current span, not the context's.** The blueprint had the context's span win. A mutation check showed that rule adds nothing inside a node, where both are the attempt span. It is also wrong when they differ: an agent that opens its own span around a model ↔ tool iteration wants the tool spans under it, which is OpenTelemetry's implicit-parent rule. So the current span wins, and the context's span is the fallback when nothing is current: a spawned Rust task, or a .NET flow with suppressed `ExecutionContext`. Both cases are tested in both engines.
- **Widening a subset is "not permitted".** `first.Subset("gamma")`, where `first` had left `gamma` out, throws `ToolNotPermittedException` / returns `ToolNotPermitted`, not "not found" as the blueprint first said. It matches what dispatching `gamma` through `first` does.
- **Rust: an error after the token fired is a cancellation.** `#[strata_tool]` maps every error a tool returns to `ToolExecutionFailure`, as it always has (see "Not built"), so a tool can't return `ToolCancelled` itself and have it survive. Instead, the registry returns `ToolCancelled` for any `Err` that comes back after the call's token fired. That is the Rust counterpart of .NET's rule for `OperationCanceledException`.
- **Error messages are each engine's own.** .NET keeps `ToolNotFoundException`'s existing sentence style ("Tool 'x' is not permitted here."), and Rust's `error_message` gains lower-case arms for the tool variants ("tool 'x' is not permitted here"). Events carry these messages, so the two engines' strings differ in case and punctuation only.
- **Two tests are stand-ins rather than twins.**
  - Rust's `a_tool_call_the_engine_gave_up_on_is_completed_as_cancelled` relies on Rust dropping the call's future when the engine gives up on the node, which records the outcome at once. A .NET node is abandoned instead, so the tool notices its token on another thread, possibly after `RunCompleted`. .NET's `A_Cancelled_Tool_Call_Is_Completed_As_Cancelled` cancels the call from inside the node, so the order is fixed.
  - .NET's `A_Foreign_OperationCanceledException_From_A_Tool_Is_A_Failure` has no Rust twin, because Rust has no such exception.
  - .NET's `Registering_A_Registry_Adds_Its_Tools_Not_Itself` and `Registering_While_Calls_Are_In_Flight_Is_Safe` have no Rust twins either: a Rust registry isn't a tool, and `&mut self` rules out registering during dispatch.

**Found along the way, not changed:** a Rust async handler can't report "I was cancelled". If a node turns its tool's `ToolCancelled` into a `NodeError` in the same poll in which its token fired, the engine's `biased` select takes the handler's result, and the node **fails** instead of ending `Cancelled`. .NET behaves the same for a `NodeException`, but a .NET handler can let the `OperationCanceledException` propagate, which the engine treats as cancellation. In Rust today, a handler that wants a cancellation outcome stops returning (`std::future::pending().await`) and lets the engine see the token. `tool_context.rs`, `cancelling_the_run_reaches_a_tool_through_the_nodes_token`, shows it. A `NodeError::Cancelled`, or the engine preferring its token over a handler error, belongs to design/17, not here.

**How it was built:**

- **.NET:**
  - `Tools/ToolCallContext.cs`, `Tools/ToolRegistry.cs`, `Tools/ToolDescriptor.cs`, and the exceptions in `Tools/ToolNotFoundException.cs`. `IStrataToolProvider` gains the four-argument default method.
  - The registry keeps an immutable `Snapshot` (entries, a name index, the excluded names, the rendered manifest) and swaps it under a lock. Registration parses each provider's manifest with `System.Text.Json`.
  - `Observability/ToolCallScope.cs` is the hook: one per dispatch, starting the activity, recording both events and both instruments. The instruments are in `StrataTelemetry`. `NodeContext` gains an internal `Telemetry` property, which `RunScope.ContextFor` sets.
  - The generator recognizes `ToolCallContext` / `CancellationToken` parameters by display name (`ParameterKind`), keeps them out of the manifest and the arguments record, and emits the two-argument dispatch as a forwarder to the four-argument one. `STRATA003` keeps the first method that claims a name and leaves the rest out of the `switch`, so no `CS0152` is reported on top of it.
- **Rust:**
  - `tools.rs` holds `ToolCallContext`, the `execute_with` default and the registry: `Arc<dyn StrataTool>`s, a name → index map, and the excluded names behind an `Arc`. A guard test forbids `std::any` in `tools.rs`, so the type-map lookups for services live in `context.rs` (`service_of`, `insert_service`), which `NodeContext` now uses too.
  - `NodeContext` holds a `Weak<RunTelemetry>`, so a context kept past its run doesn't keep the run's telemetry, and its event pump, alive.
  - `observability.rs` gains `ToolCallScope`, whose `Drop` records a dropped call as cancelled, plus `ToolCallResult`, the two `RunEventKind` variants and the two `metric_names`.
  - `#[strata_tool]` accepts an optional second `ToolCallContext` parameter, recognized by the last segment of its type path. It implements `execute_with`, with `execute` forwarding to it.

**Tests:**

- **New suites:**
  - `tool_registry.rs` (11) / `ToolRegistryTests.cs` (12).
  - `tool_context.rs` (9) / `ToolCallContextTests.cs` (10).
- **Added to existing suites:**
  - `telemetry.rs` / `TelemetryTests.cs`: 6 each.
  - `run_events.rs`: 3. `RunEventsTests.cs`: 4.
  - `StrataToolGeneratorTests.cs`: 2.
  - `tools.rs`: 1 unit test and 2 `compile_fail` doctests (context first; a second parameter that isn't the context).
- **Totals when this landed:** .NET 390 (all backends live) plus the example's 1, `rust/` 359, `cli/` 40, `examples/rust-agent` 1.
- **Mutation checks, in both engines:**
  - Letting a duplicate through fails 2 tests in each.
  - A subset that keeps every tool fails 8 in .NET, and at least 1 in Rust (cargo stops at the first failing binary).
  - Ignoring the context's span when nothing is current fails 1 in each.

**Since then:** tool middleware (SG-22) is built on the choke point described in "Room for SG-22". See `design/23-Tool-Middleware.md`.

On 2026-10-24, arguments a tool can't read became their own exception in .NET and got the same words in both engines, and .NET's generated dispatch began refusing a missing required argument: see "Arguments a tool can't read". Tests: `argument_problems.rs` (7) and `ToolArgumentProblemTests.cs` (the shared cases plus 9), and a span test in `TelemetryTests.cs`.

**Downstream, checked against this branch:** `strata-demos-dotnet` builds with no warnings. `strata-demos-rust` doesn't compile, but only because it hasn't been ported to design/17's async `run` yet. That predates this change, and none of its errors involve tool APIs.

## Not built (deliberately out of scope)

- **Tool middleware** (SG-22). Since built: `design/23-Tool-Middleware.md`.
- **Provider manifest renderers** and the model ↔ tool loop (SG-23).
- **Tool-level authorization policy.** Strata supplies the caller, tenant metadata and call id. StrataOps' Tool Gateway decides.
- **Validating arguments against the JSON Schema** before dispatch, beyond what deserialization already enforces. It is one of the Tool Gateway's responsibilities. design/23's middleware can do it (`ToolCall.Descriptor` / `ToolCall::schema`, with a sample in its tests), and nothing ships.
- **Per-tool timeouts.** The node's timeout and the run's deadline already reach the tool through its token. A per-tool timeout is a middleware concern, which design/23's middleware can express.
- **Streaming tools**, and history on the tool context.
- **Carrying the tool's own error in Rust's `ToolExecutionFailure`.** The macro discards the tool's error and keeps only the tool name, because the error type isn't required to implement `Display`. Changing that would break tools whose error type doesn't.
- **Zero-argument Rust tools.** `#[strata_tool]` still needs an arguments struct.
