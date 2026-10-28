# Datalium Strata: Tool Middleware, and Optional `string?` Tool Parameters

design/20 gave tools a context, a registry and telemetry, and left one seam open: **"Room for SG-22"**. A product that needs a policy around every tool call (audit, authorization, rate limits, timeouts, a read cache, idempotency) has nowhere to put it. StrataOps' Tool Gateway (plan §6.6) works around that today with a seam of its own (StrataOps ADR 0012):

- `GatedToolRegistry` registers each provider wrapped in the chain, so the chain runs inside the registry's dispatch.
- `GatedToolSet` wraps an agent's subset, to report the calls the subset refused before the chain ran.
- A refusal from the gateway is a `ToolNotFoundException` subclass, so the `strata.tool` span tags it `not_found`.

This document specifies **SG-22**, which replaces that seam with middleware the registry runs itself, and **SG-33**, a .NET source-generator bug found along the way: a `[StrataTool]` method with an optional `string?` parameter generates code that doesn't compile cleanly. Both touch tool dispatch, so they are one change. The SG numbers refer to the gap log of StrataOps (not public yet).

SG-22 lands in both engines at one commit (strict parity, StrataOps ADR 0003): the same capability, in each language's idiom. **SG-33 is .NET-only**, because Rust's `#[strata_tool]` already maps an `Option<T>` field to an optional parameter (see "SG-33").

It was written as a blueprint before implementation. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Middleware shape | **Around-style**: a middleware receives the call and a `next` to continue the chain, and returns the call's result. .NET `IToolMiddleware.InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken ct)`. Rust `ToolMiddleware::invoke(&self, call: ToolCall, next: Next<'_>)`. Before/after/on-error hooks alone can't express a timeout that stops waiting, or a cache that answers without calling the tool. |
| What a middleware sees | A **`ToolCall`**: the tool's name, its JSON arguments, the `ToolCallContext` (design/20), and the tool's manifest entry. The name is fixed: `next` always runs the tool the registry resolved, so a middleware can't route a call to a different tool. The arguments and the context can be replaced (.NET `with`, Rust `with_arguments` / `with_context`). |
| Registration | `registry.Use(middleware)` (.NET, chainable) / `registry.use_middleware(middleware)` (Rust, `&mut self -> &mut Self`, like `register`). |
| Order | **Registration order, outermost first.** The first middleware registered sees the call first and the result last. One chain per registry. |
| Subsets | **Share the parent's chain.** `Subset` / `subset` copies the middleware list as it copies the tools: a snapshot. A gateway registered once applies to every agent's subset. Middleware added to a subset applies to that subset only, inside the parent's. |
| Where the chain runs | At design/20's choke point, **between the token check and the tool call**, inside the `strata.tool` span. Middleware time and middleware outcomes are part of the span, the two metrics and `ToolCallCompleted`. A call whose token has already fired runs no middleware, as it runs no tool. |
| Short-circuiting | **Yes**: a middleware that returns without calling `next` is the call's result, reported `succeeded`. The span is tagged `strata.tool.short_circuited = true` when the tool never ran. |
| Calling `next` more than once | Allowed (a retry middleware). Each call runs the rest of the chain and the tool again. |
| A middleware's own refusal | A new error: .NET **`ToolCallDeniedException(toolName, reason)`**, Rust **`StrataError::ToolCallDenied { tool, reason }`**. Reported `Refused`, `error.type` = **`denied`**. Not a subtype of "not found": the tool exists and the caller may use it, but this call was refused. |
| A middleware's own failure | .NET: any exception (a `TimeoutException`, say), reported `Failed` with its type name, as a tool's exception is. Rust: a new **`StrataError::ToolCallFailed { tool, message }`**, reported `Failed`, `error.type` = `failed`, so a timeout or an unreachable policy backend can say why. |
| Refused calls (not found, not permitted) | **Don't enter the chain.** Each middleware gets an **on-refused hook** instead, called in registration order before the refusal is returned: `OnRefusedAsync(call, refusal, ct)` / `on_refused(&call, &refusal)`, default no-op. An audit sees every refused call; a cache can't answer for a tool the caller isn't allowed. |
| Enriching the context | `next(call with { Context = call.Context with { … } }, ct)` / `next.run(call.with_context(call.context().clone().with_metadata(…)))`. The span is current while a middleware runs, so it can tag it. |
| Registering a registry into another (.NET) | The inner registry's middleware **stays with its tools**, running inside the outer registry's chain. A gateway can't be bypassed by flattening its registry into another. (A Rust registry isn't a tool, so there is no Rust case.) |
| Telemetry | No new instruments or events, and no new metric labels. One new span attribute (`strata.tool.short_circuited`) and one new `error.type` value (`denied`). |
| Wire format | **No change.** Tool calls aren't on the checkpoint wire (design/20, "Wire format"). |
| SG-33 | The .NET generator emits the arguments record with nullable reference types (`string? app = null`), so an optional `string?` parameter no longer produces `CS8625`. Rust needs no change. |
| Not in scope | Concrete policies (authorization rules, rate limiting, credentials, audit storage: StrataOps owns them), argument JSON-Schema validation as a shipped middleware (a sample is in the tests), manifest renderers and the model ↔ tool loop (SG-23). |

---

## Usage

```csharp
// Startup: one registry, one gateway, every agent's subset shares it.
var tools = new ToolRegistry()
    .Register(new KubernetesTools(k8sPort))
    .Register(new PrometheusTools(metricsPort))
    .Use(new AuditMiddleware(auditLog))            // outermost: sees every call, and every refusal
    .Use(new AuthorizationMiddleware(policy))      // throws ToolCallDeniedException("…", reason)
    .Use(new TimeoutMiddleware(TimeSpan.FromSeconds(20)));

var investigator = tools.Subset("k8s.read", "prometheus.query");   // same chain

public sealed class AuthorizationMiddleware(IPolicy policy) : IToolMiddleware
{
    public async Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken ct)
    {
        var tenant = call.Context.Metadata.GetValueOrDefault("tenant");
        var verdict = await policy.CheckAsync(tenant, call.Context.CallerId, call.Name, ct);
        if (!verdict.Allowed)
        {
            throw new ToolCallDeniedException(call.Name, verdict.Reason);    // Refused, error.type "denied"
        }

        Activity.Current?.SetTag("strataops.policy", verdict.PolicyId);     // the strata.tool span
        return await next(call with { Context = call.Context with { CallerId = verdict.Principal } }, ct);
    }
}

public sealed class TimeoutMiddleware(TimeSpan limit) : IToolMiddleware
{
    public async Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(limit);
        // WaitAsync stops waiting even for a tool that ignores its token.
        return await next(call, timeout.Token).WaitAsync(limit, ct);         // TimeoutException: Failed
    }
}

public sealed class AuditMiddleware(IAuditLog log) : IToolMiddleware
{
    public async Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken ct)
    {
        try
        {
            var result = await next(call, ct);
            await log.RecordAsync(call, "succeeded", ct);
            return result;
        }
        catch (Exception ex)
        {
            await log.RecordAsync(call, ex.GetType().Name, CancellationToken.None);
            throw;
        }
    }

    public Task OnRefusedAsync(ToolCall call, ToolNotFoundException refusal, CancellationToken ct) =>
        log.RecordAsync(call, refusal is ToolNotPermittedException ? "not_permitted" : "not_found", ct);
}
```

```rust
struct Authorization { policy: Arc<dyn Policy> }

impl ToolMiddleware for Authorization {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            let tenant = call.context().metadata().get("tenant").cloned().unwrap_or_default();
            let verdict = self.policy.check(&tenant, call.context().caller_id(), call.name()).await;
            if !verdict.allowed {
                return Err(StrataError::ToolCallDenied { tool: call.name().into(), reason: verdict.reason });
            }
            let ctx = call.context().clone().with_caller_id(verdict.principal);
            next.run(call.with_context(ctx)).await
        })
    }
}

struct Timeout(Duration);

impl ToolMiddleware for Timeout {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            let name = call.name().to_string();
            // Dropping the chain's future on expiry drops the tool's with it.
            match tokio::time::timeout(self.0, next.run(call)).await {
                Ok(result) => result,
                Err(_) => Err(StrataError::ToolCallFailed { tool: name, message: format!("timed out after {:?}", self.0) }),
            }
        })
    }
}

let mut tools = ToolRegistry::new();
tools
    .register(Box::new(K8sReadTool))
    .register(Box::new(PrometheusQueryTool))
    .use_middleware(Audit::new(log))
    .use_middleware(Authorization { policy })
    .use_middleware(Timeout(Duration::from_secs(20)));
let investigator = tools.subset(["k8s.read", "prometheus.query"])?;   // same chain
```

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Tools;

/// What a middleware is handed: one tool call, after the registry resolved it.
public sealed record ToolCall
{
    public ToolCall(string name, string arguments, ToolCallContext context, ToolDescriptor? descriptor = null);

    public string Name { get; }                       // no init: `with` can't change it
    public string Arguments { get; init; }            // the JSON arguments
    public ToolCallContext Context { get; init; }
    public ToolDescriptor? Descriptor { get; }        // the manifest entry; null for a refused call
}

/// The rest of the chain, ending at the tool.
public delegate Task<string> ToolCallDelegate(ToolCall call, CancellationToken cancellationToken);

public interface IToolMiddleware
{
    Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken);

    /// A call the registry refused before the chain ran (not found, not permitted). Default: nothing.
    Task OnRefusedAsync(ToolCall call, ToolNotFoundException refusal, CancellationToken cancellationToken)
        => Task.CompletedTask;
}

public sealed class ToolRegistry : IStrataToolProvider
{
    // … design/20 …
    public ToolRegistry Use(IToolMiddleware middleware);
    public IReadOnlyList<IToolMiddleware> Middleware { get; }       // registration order
}

/// A middleware refused this call. Reported Refused, error.type "denied".
public sealed class ToolCallDeniedException(string toolName, string reason)
    : Exception($"Tool '{toolName}' was denied: {reason}")
{
    public string ToolName { get; }
    public string Reason { get; }
}
```

- `ToolCall` is a record for the same reason `ToolCallContext` is: `with` is the one-expression way to derive a changed call. `Name` and `Descriptor` have no `init`, so `with` can't change them. Its public constructor lets a middleware be unit-tested with a lambda for `next`.
- `ToolCallDelegate` is a delegate, not an interface, so a test passes `(call, ct) => Task.FromResult("…")`.
- `Use` is synchronized and copy-on-write, like `Register`: a dispatch in flight keeps the chain it started with.

### Rust

```rust
/// What a middleware is handed: one tool call, after the registry resolved it.
#[derive(Clone)]
pub struct ToolCall { /* private */ }

impl ToolCall {
    pub fn new(name: impl Into<Arc<str>>, arguments: impl Into<String>, ctx: ToolCallContext) -> Self;
    pub fn name(&self) -> &str;
    pub fn arguments(&self) -> &str;
    pub fn context(&self) -> &ToolCallContext;
    pub fn schema(&self) -> Option<serde_json::Value>;     // the manifest entry; None for a refused call
    pub fn with_arguments(self, arguments: impl Into<String>) -> Self;
    pub fn with_context(self, ctx: ToolCallContext) -> Self;
}

/// The rest of the chain, ending at the tool. Copy, so a retry can run it again.
#[derive(Clone, Copy)]
pub struct Next<'a> { /* private */ }

impl<'a> Next<'a> {
    pub fn run(&self, call: ToolCall) -> BoxFuture<'a, Result<String, StrataError>>;
}

pub trait ToolMiddleware: Send + Sync {
    fn invoke<'a>(&'a self, call: ToolCall, next: Next<'a>) -> BoxFuture<'a, Result<String, StrataError>>;

    /// A call the registry refused before the chain ran. Default: nothing.
    fn on_refused<'a>(&'a self, call: &'a ToolCall, refusal: &'a StrataError)
        -> BoxFuture<'a, Result<(), StrataError>> {
        Box::pin(async { Ok(()) })
    }
}

impl<M: ToolMiddleware + ?Sized> ToolMiddleware for Arc<M> { /* forwards */ }

impl ToolRegistry {
    pub fn use_middleware(&mut self, middleware: impl ToolMiddleware + 'static) -> &mut Self;
}

pub enum StrataError {
    // … design/20 …
    /// A tool middleware refused the call. Reported Refused, error.type "denied".
    ToolCallDenied { tool: String, reason: String },
    /// A tool middleware failed the call for a reason of its own (a timeout, an unreachable backend).
    ToolCallFailed { tool: String, message: String },
}
```

- `invoke` returns a `BoxFuture`, as `StrataTool::execute_with` and the stores do, so the trait stays object-safe and middleware is held as `Arc<dyn ToolMiddleware>`.
- The blanket `impl` for `Arc<M>` lets one middleware instance be registered on two registries (`use_middleware(shared.clone())`).
- `Next` holds only borrows, so it is `Copy`; `run` takes `&self`. A middleware can't construct one: in Rust a middleware is tested through a registry with a stub tool, which is a few lines.
- `ToolCall::schema` calls the tool's `schema()` on demand, so a middleware that never asks pays nothing.

---

## Semantics

### The dispatch path

design/20's choke point becomes, in both engines:

1. Fill the call id if it's missing.
2. Resolve the name.
3. Start telemetry (`ToolCallStarted`, the `strata.tool` span).
4. **Not found / not permitted:** run every middleware's on-refused hook in registration order, then return the refusal.
5. Check the token. Already fired: the call is cancelled, and no middleware runs.
6. **Run the chain**: the registry's middleware, outermost first, each handed a `ToolCall` and a `next`. The innermost `next` calls the resolved tool with the call's (possibly replaced) arguments, context and, in .NET, token.
7. End telemetry with the chain's outcome.

Every step from 3 on is inside the span, so the span's duration and `ToolCallCompleted.duration` include middleware time (an authorization round-trip, a rate-limit wait). The tool's context carries the span, as before, and middleware runs with it current (`Activity.Current` / an instrumented future), so `Activity.Current?.SetTag(…)` / `Span::current().record(…)` tags the tool span. (Rust: only fields the span declared can be recorded; a middleware wanting its own fields opens a child span.)

### Outcomes

| What happened | .NET | Rust | `strata.tool.result` | `error.type` |
|---|---|---|---|---|
| The chain returned a result | returned | `Ok` | `succeeded` | — |
| A middleware refused | `ToolCallDeniedException` | `ToolCallDenied` | `refused` | `denied` |
| A middleware failed (timeout, backend down) | its exception | `ToolCallFailed` (or any other error) | `failed` | .NET the type name, Rust `failed` |
| The registry's token fired during the chain | `OperationCanceledException` | any error but `ToolCallDenied` | `cancelled` | — |
| A middleware cancelled its own token (a timeout) | `OperationCanceledException` for a token the registry didn't pass | — | `failed` | `System.OperationCanceledException` |

- **Cancellation is the registry's token's.** design/17's rule applies to the chain as a whole: in .NET, an `OperationCanceledException` counts as a cancellation only if the token the registry received was cancelled; in Rust, an error returned after the registry's token fired is `ToolCancelled`. A timeout middleware that links its own token is a failure, not a cancellation, because the run wasn't cancelled.
- **Except a denial.** In Rust, a `ToolCallDenied` returned after the token fired stays a denial: it is a decision, not a consequence of cancellation. (In .NET it already is: only `OperationCanceledException` is reclassified.)
- **A short-circuit is a success.** The registry tracks whether the innermost `next` ran. If the chain succeeded without it, the span gets `strata.tool.short_circuited = true`. Metrics and events don't distinguish it: a cache hit is a successful call, and a new metric label would split every existing series.

### Refused calls and the on-refused hook

A call to a name the registry doesn't have (not found), or that its subset left out (not permitted), is refused **before** the chain:

- Each middleware's on-refused hook runs, in registration order, with a `ToolCall` whose descriptor / schema is absent, the refusal, and (in .NET) the token. The hooks run even if the token has fired, so an audit records a refused call however the run ends.
- The hooks **observe**: they can't turn a refusal into a result. A refusal stays a refusal.
- A hook that throws (.NET) or returns `Err` (Rust) ends the call with that error in place of the refusal, reported as a failure, and the remaining hooks don't run. An audit that can't record is a real failure, and failing closed is the right default for a gateway.
- A **denial by a middleware** is not passed to on-refused hooks. The middleware outside it already sees it, as the exception or error `next` returned; the ones inside it never saw the call. An audit registered outermost therefore sees every call: refused by the registry through its hook, everything else through `next`.

Why not run refused calls through the chain? Every middleware would then have to check "is there a tool?" before doing anything, and one that forgot could answer for a tool the caller isn't permitted: a cache shared by a registry and its subset would hand the subset a cached result for a tool it was refused.

### Subsets and composition

- `Subset` / `subset` copies the middleware list with the tools. Middleware added to the parent afterwards doesn't reach the subset, as tools added afterwards don't (design/20, "a snapshot").
- Middleware added to a subset runs **inside** the parent's: the parent's chain is the start of the subset's list.
- **.NET, registering a registry into another:** `Register(registry)` adds the inner registry's tools, and each keeps the inner registry's middleware (as of that moment). A call to one of them runs the outer chain, then the inner chain, then the tool. Without this, flattening a gated registry into an ungated one would silently drop its gateway. Refusals happen at the outer registry, so only its middleware's on-refused hooks run.

### What a middleware may change

| | Change it? | How |
|---|---|---|
| Arguments | yes | `call with { Arguments = … }` / `call.with_arguments(…)`: redaction, normalization, defaults. |
| Context | yes | `call with { Context = call.Context with { … } }` / `call.with_context(…)`. The run's telemetry handle rides along (it is copied by `with` and the builders), so tool events still reach the run. |
| Token (.NET) / cancellation (Rust) | yes | Pass another token to `next` / `with_cancellation` on the context: a timeout links one. |
| Tool name | **no** | `next` always runs the resolved tool. Routing to another tool is the caller's job, through the registry, so it is resolved and permitted. |
| Call id | technically | It's on the context. The span and both events keep the id the registry received. |

---

## Telemetry

No new instruments and no new events (design/20 already has the span, the two metrics and `ToolCallStarted` / `ToolCallCompleted`). What changes:

| | Change |
|---|---|
| Span duration, `strata.tool.duration`, `ToolCallCompleted.duration` | Include middleware time. |
| `strata.tool.result` | A denial is `refused`; a middleware failure is `failed`. |
| `error.type` | New value `denied`. |
| New span attribute | `strata.tool.short_circuited` = `true` when the chain succeeded without running the tool. Absent otherwise. |
| `ToolCallCompleted.error` | The denial's message (`Tool 'x' was denied: <reason>` / `tool 'x' was denied: <reason>`), as for other refusals. |
| Metric labels | Unchanged. A denied call's tool name is a registered one, so it carries `gen_ai.tool.name`. |

Arguments and results still appear in no event (design/18's rule). An audit middleware that needs them has them in hand.

---

## SG-33: optional `string?` parameters (.NET)

**The bug.** `Extract` marks a parameter optional when its type is annotated nullable, and gives it the default literal `null`. Its type name comes from `ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)`, which drops the `?` of a nullable *reference* type. So

```csharp
[StrataTool(Name = "find", Description = "…")]
public static string Find(string? app) => …;
```

emits `private sealed record FindStrataArgs(string app = null);` under `#nullable enable`: warning `CS8625` ("cannot convert null literal to non-nullable reference type"), an error under `TreatWarningsAsErrors`. StrataOps had to make its optional filters non-nullable strings with a `""` default. `int?` was never affected: `Nullable<T>` keeps its `?` in every format.

**The fix.** Use `FullyQualifiedFormat` with `SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier` added (added, not replacing: `UseSpecialTypes` keeps `string` rather than `global::System.String`). The record becomes `FindStrataArgs(string? app = null)`, and the method receives `string?` as declared. The manifest is unchanged: `app` was already optional there.

- `string? app = "web"` (an explicit default) emits `string? app = "web"`.
- `int? limit` emits `int? limit = null`, as before.
- Recognizing `ToolCallContext` / `CancellationToken` parameters still uses the display name without the modifier, so nothing about them changes.
- `STRATA002`'s text ("optionally nullable / with a default value") was a promise the generator didn't keep for reference types. It now holds, and the text stays.

**Rust needs nothing.** `#[derive(StrataSchema)]` leaves an `Option<T>` field out of `required`, and serde deserializes a missing field as `None` (`rust/strata-macros/src/lib.rs`, the `is_option` check). There is no Rust counterpart of a nullable reference type to get wrong.

---

## Wire format

**None.** Tool calls aren't in checkpoints, graph definitions or history records (design/20). No `schema_version` bump, no DDL change and no new fixtures. Tool calls are not cross-language, so the middleware contract is not on a wire either.

---

## Migration

**.NET** (source-compatible):

- No middleware, no change: a registry without `Use` dispatches exactly as before.
- `ToolCallDeniedException` and `ToolCall` are new types. The new `strata.tool.short_circuited` attribute and `denied` error type only appear with middleware.
- A generated provider whose tool has a `string?` parameter now gets `string?` in its arguments record. The only observable difference is that the `CS8625` warning is gone.

**Rust:**

- No middleware, no change.
- `StrataError` gains `ToolCallDenied` and `ToolCallFailed`. It isn't `#[non_exhaustive]`, so an exhaustive `match` needs arms for them (as design/20's variants did).
- `ToolRegistry` is still `Clone + Send + Sync`: the middleware is held as `Arc<dyn ToolMiddleware>`.

**Downstream:** StrataOps bumps `strata.lock` in its own PR and retires `GatedToolRegistry`, `GatedToolSet` and its own `IToolMiddleware` in favour of these, and makes its optional tool filters `string?` again.

---

## Tests

Mirrored per engine, in new suites `tool_middleware.rs` / `ToolMiddlewareTests.cs`:

- **Order:** three middleware record entry and exit; the trace is registration order in, reverse order out.
- **No middleware:** a registry without any dispatches, refuses and reports as before (the existing suites, unchanged, are the main evidence).
- **Short-circuit:** a cache middleware answers the second call; the tool runs once; both calls succeed; the second span is tagged `short_circuited`.
- **Refusal:** a denying middleware gives `ToolCallDeniedException` / `ToolCallDenied`; the tool doesn't run; `ToolCallCompleted` is `Refused` with the reason in its error; the span's `error.type` is `denied`. In Rust, a denial after the token fired stays a denial.
- **A middleware's failure:** an exception / `ToolCallFailed` from a middleware is `Failed`.
- **Timeout:** a timeout middleware stops waiting for a tool that ignores its token, within the limit; the outcome is `Failed`, not `Cancelled`.
- **Context enrichment:** a middleware's `with` / builder changes reach the tool (caller id, metadata), and the tool still has the call id and span.
- **Arguments:** a middleware that replaces the arguments changes what the tool receives.
- **Subsets share the chain:** a subset's call runs the parent's middleware; middleware added to the subset runs inside it and not on the parent; middleware added to the parent afterwards doesn't reach an existing subset.
- **Refused calls reach middleware:** a not-found and a not-permitted call each run every on-refused hook in order, run no `InvokeAsync` / `invoke`, and still return the refusal; a hook that fails replaces the refusal.
- **Inside the span:** a middleware's delay is inside `ToolCallCompleted.duration`, and a tag it sets on `Activity.Current` lands on the tool span (.NET); in Rust, a span a middleware opens is a child of the tool span.
- **Already cancelled:** no middleware runs.
- **Retry:** a middleware that calls `next` twice runs the tool twice.
- **.NET only:** registering a registry with middleware into another keeps its middleware on its tools; a `ToolCall`'s name can't be changed by `with` (compile-time, so asserted by the shape of the type: no `init` accessor).
- **SG-33 (`StrataToolGeneratorTests.cs`):** driver-based tests compiling the generated source with nullable enabled and asserting no diagnostics, for an optional `string?`, a `string?` with an explicit default, and an `int?`; plus an end-to-end tool in `SampleTools` with a `string?` parameter, called with and without the argument.
- **Mutation checks**, as for design/20: running the chain outside the span, letting a refused call into the chain, and dropping the parent's chain from a subset should each fail a test in both engines.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

### The questions as asked

Recommendations are in bold.

1. **Around-style or hooks?** One `InvokeAsync(call, next, ct)` / `invoke(call, next)`, or separate before/after/on-error hooks? **Around-style.** A timeout that stops waiting and a cache that answers can't be written as hooks, and hooks can be written as around-style.
2. **Refused calls.** Through the chain, or an on-refused hook? **A hook.** Running refused calls through the chain makes every middleware responsible for not answering for a tool that isn't there or isn't permitted.
3. **Can an on-refused hook fail the call?** **Yes**, in place of the refusal (fail closed), and the remaining hooks don't run.
4. **A denial's error type.** One fixed `denied`, or a code the middleware chooses (`rate_limited`, `unauthorized`)? **Fixed `denied`**, with the reason in the message. A code of the middleware's choosing is additive later, and a fixed set keeps `error.type` bounded.
5. **`ToolCallDeniedException` under `ToolNotFoundException`?** **No.** "Not found" tells the model the tool doesn't exist; a denial means it exists and this call was refused, with a reason the agent loop may pass on.
6. **A Rust `ToolCallFailed` variant?** Or make middleware return `ToolExecutionFailure(name)`, which can't say why? **Add it**: middleware failures are new and can carry a message from the start.
7. **Flattening a registry with middleware (.NET).** Refuse it, or keep the inner middleware on its tools? **Keep it**: refusing would surprise, and dropping it would bypass a gateway.
8. **Can a middleware change the tool name?** **No**: routing goes through the registry.

---

## Implementation Status

Built 2026-10-04 in both engines, in one change, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **Error messages are each engine's own.** Rust's `error_message` renders a denial as `tool 'x' was denied: <reason>` and a `ToolCallFailed` as `tool 'x' failed: <message>`. .NET's denial message is `Tool 'x' was denied: <reason>`, and a .NET middleware failure carries its own exception's message. Events carry these, so the denial strings differ between engines in case only, as design/20's do.
- **The tool span is current for the on-refused hooks too.** Both engines build the context the hooks see after the span starts, so a hook's own spans nest under the tool span, as `InvokeAsync` / `invoke`'s do.
- **Rust copies the arguments once per call.** `ToolCall` owns its arguments (a `String`), so a middleware can replace them, and dispatch copies the borrowed `&str` once. Skipping the copy when no middleware is registered would mean a second code path, and the copy is small next to the JSON parse that follows.
- **Twins that differ.**
  - .NET's `A_Calls_Name_And_Descriptor_Cannot_Be_Changed_With_With` checks the record's shape. Rust's `ToolCall` has no way to set the name either, so there is nothing to test at run time.
  - .NET's `Registering_A_Registry_Keeps_Its_Middleware_On_Its_Tools` has no Rust twin: a Rust registry isn't a tool.
  - Rust's `one_middleware_can_be_shared_between_registries` covers the blanket `impl` for `Arc<M>`. In .NET, sharing is passing the same instance twice.
  - Cancellation: Rust's test also covers a denial returned after the token fired staying a denial, a rule .NET doesn't need because it only reclassifies `OperationCanceledException`. .NET's test checks the `Cancelled` event instead.
- **The generator test leaves out `CS1998`.** The driver-based test compiles the generated code with the Roslyn 4.11 the tests reference, which warns that the generated `async` dispatch has no `await` when every tool is synchronous. The SDK's compiler doesn't report it, and the warning predates SG-33, so the test excludes it by id.

**How it was built:**

- **.NET:**
  - `Tools/IToolMiddleware.cs` holds `ToolCall`, `ToolCallDelegate` and `IToolMiddleware`. `ToolCallDeniedException` is in `Tools/ToolNotFoundException.cs` with the other tool exceptions.
  - `ToolRegistry`'s immutable `Snapshot` gains the middleware list, and `Use` swaps it under the lock as `Register` does. Each `Entry` gains the middleware of the registry it was registered through (empty for a provider), which `Register(registry)` fills in.
  - A private `Chain` per call runs the middleware by index and records whether the provider ran. Its terminal dispatches the resolved name, not `call.Name`.
  - `ToolCallScope` maps `ToolCallDeniedException` to `Refused` / `denied`, and `ShortCircuited()` sets the span attribute.
  - SG-33: the generator's `ArgumentTypeFormat` is `FullyQualifiedFormat` plus `IncludeNullableReferenceTypeModifier`, used for JSON parameters only.
- **Rust:**
  - `tools.rs` holds `ToolCall`, `ToolMiddleware` (with the `Arc<M>` blanket `impl`), `Next` and a private `Terminal`. `Next::run` takes the first middleware off a slice and hands it a `Next` over the rest; an empty slice runs the terminal.
  - `ToolRegistry` gains `middleware: Vec<Arc<dyn ToolMiddleware>>`, which `subset` copies.
  - `StrataError` gains `ToolCallDenied` and `ToolCallFailed`. `observability.rs`'s `ToolCallScope` gains `short_circuited()` and its span field, and maps a denial to `Refused` / `denied`.

**Tests:**

- **New suites:** `tool_middleware.rs` (17) / `ToolMiddlewareTests.cs` (18).
- **Added to `StrataToolGeneratorTests.cs`:** a three-case theory that compiles the generated code with nullable enabled, for an optional `string?`, a `string?` with a default and an `int?`; and an end-to-end `find_service` tool in `SampleTools.cs`, called with and without its arguments.
- **Totals when this landed:** .NET 448 passed, with 38 Postgres/Redis tests skipped because those services weren't running locally (this change touches no store), plus the example's 1. `rust/` 447, `cli/` 57, `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` (all three projects) and `dotnet format --verify-no-changes` are clean.
- **Mutation checks, in both engines:**
  - Running the chain outside the tool span fails 2 tests in each (the new one, and design/20's tool-span test).
  - Dropping the parent's chain from a subset fails 1 in each.
  - Refused calls: in .NET, sending them through `InvokeAsync` instead of the hook fails 3. In Rust, skipping the hooks fails 3.
  - SG-33: reverting the display format fails both `string?` cases (`int?` passes, as it always did), and the test project's own build then warns `CS8625` for `find_service`.

## Not built (deliberately out of scope)

- **Concrete policies**: authorization rules, rate limiting, credentials, audit storage. StrataOps' Tool Gateway owns them; Strata provides the hook points.
- **Argument validation against the JSON Schema as a shipped middleware.** `ToolCall.Descriptor` / `ToolCall::schema` make it a short middleware; the tests include a sample, and nothing ships.
- **A denial code chosen by the middleware** (review question 4).
- **Middleware on a provider dispatched directly**, outside a registry. A provider's own `DispatchToolAsync` / a tool's `execute_with` stays the raw path, untraced and ungated, as design/20 made it.
- **A closure-based Rust middleware helper** (`from_fn`). The lifetime of `Next<'a>` makes a closure signature awkward to state; a trait impl is a few lines.
- **Manifest renderers and the model ↔ tool loop** (SG-23).
