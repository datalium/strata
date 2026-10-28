# Datalium Strata: Model Routes (Failover and Hedging)

design/44 gave Strata model calls and agents over one chat client. A production agent rarely trusts one model: a provider has outages, rate limits and refusals, and a slow first token holds up the whole run. StrataOps' Model Fabric answers this with its `ModelRouter` (`src/Fabrics/StrataOps.Models/Routing/ModelRouter.cs`, about 660 lines on Polly):

- **Policies of candidates.** A request names a policy, and the policy's candidates are filtered by tenant, budget tier, tool use and context length.
- **Failover.** The router tries the remaining candidates in order. It moves on after a transient fault or a content-filter refusal, and a stream fails over only until its first update.
- **Resilience per provider:** two retries on the same model, honouring `Retry-After` up to 20 s, and a circuit breaker per provider.
- **A ledger row per attempt.**
- **A transient error when nothing answered,** carrying the provider's retry hint.

This document closes **SG-24** (StrataOps' gap log: "model routing and race primitives … upstream once the shape has proven itself"). It lands in both engines (strict parity, StrataOps ADR 0003), in **one PR**. **A model route** is a chat client over ordered candidates. It fails over and, optionally, hedges: if the current candidate is slow to start, it starts the next one, and the first accepted answer wins. Each attempt is observed as a model call of its own.

**What it doesn't do: race agents.** StrataOps' root-cause race (ADR 0038) is a parallel group of two agents, each with its validators, joined by `Quorum(2)` with `KeepSucceeded` and then checked for agreement. That is graph structure, and Strata has had it since design/26. The one engine gap ADR 0038 found there, a join that counts only some branches, is not recorded as a gap and isn't built here (see "Not built").

It was written as a blueprint on 2026-10-21, with the scope settled with Nick the same day, and built that day in both engines. "Review decisions" lists the questions with the answer the build follows. "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| The primitives | **A model route: failover over ordered candidates, with optional hedging** (settled 2026-10-21). One primitive serves both failover and a latency race. |
| Where it lives | **The AI packages** (`Datalium.Strata.AI`, `datalium-strata-ai`). The engine gains one run event (`ModelRouted`) and a `Route` on the model-call events and info. No new dependency (no Polly). |
| Routing policy | **The application's** (settled 2026-10-21). An application builds a route per policy and gives each agent or node its route. Strata takes an optional per-candidate eligibility check per request, which skips a candidate with a reason. No catalog, policy files or request keys in Strata. |
| What it is | **A chat client**: .NET `ModelRoute : IChatClient`, Rust `ModelRoute: ChatClient`. Anything that takes a client takes a route: `StrataChat`, `complete`, the model node, `StrataAgent` / `Agent`. |
| When it moves on | **A transient fault, a refusal** (finish reason `content_filter`), **or an answer `Accept` rejects.** A fatal fault (a 4xx such as a bad request or authentication) stops the route at once: the request is wrong, not the model. A stream moves on only before its first update. |
| Hedging | **`HedgeAfter`, off by default.** When the current attempt hasn't produced an answer (or a stream's first update) within the delay, the next candidate starts as well. The first accepted answer, or the first stream to produce an update, wins, and every other attempt is cancelled. |
| Retries | **None on the same candidate** (settled 2026-10-21). The node's `RecoveryPolicy` retries the whole route, as design/44 decision 4 has it: one retry loop, Strata's. |
| Health | **Settled 2026-10-21: a cooldown plus a simple breaker,** kept per health key (the provider by default) and shared by every route given the same `ModelHealth`. A candidate whose fault carried `Retry-After` is skipped until it passes. With `Breaker(failures, openFor)`, that many transient failures in a row skip the key for `openFor`, after which one call is let through to test it. |
| When nothing answers | **A transient fault** whose retry hint is the shortest wait until some candidate is available again. When every candidate was ineligible, the fault is fatal. A fatal fault from a candidate stands as it is. |
| Observation | **Each attempt is a model call** (settled 2026-10-21): `ModelCallStarted` / `ModelCallCompleted` with its own model, usage and span, named with the route. A `route <name>` span holds the attempts, and a new **`ModelRouted`** run event says which candidate answered and what happened to every other. |
| Wire format | **None.** Run events and telemetry are additive. |

---

## Usage

### .NET

```csharp
var health = new ModelHealth(new ModelHealthOptions { Breaker = new(Failures: 5, OpenFor: TimeSpan.FromSeconds(30)) });

var rca = new ModelRoute(new ModelRouteOptions
{
    Name = "rca",
    Candidates =
    [
        new ModelCandidate("frontier", openAi, model: "gpt-6.1-sol"),
        new ModelCandidate("independent", anthropic, model: "claude-opus-5-5")
        {
            // The application's rules: tenant, budget, data classification, context size, …
            Eligible = (messages, options, ctx) => Budget.AllowsTier(ctx, Tier.Frontier) ? null : "above the incident's budget tier",
        },
        new ModelCandidate("local", ollama, model: "qwen3:32b"),
    ],
    Accept = response => response.Text.Length > 0 ? null : "an empty answer",
    HedgeAfter = TimeSpan.FromSeconds(20),
    Health = health,
});

// Anywhere a client goes: an agent, a model node, StrataChat.
var agent = new StrataAgent<RootCause>(rca, tools, new StrataAgentOptions<RootCause> { Id = "rca-analyst" });
```

### Rust

```rust
let health = ModelHealth::new(ModelHealthOptions::new().with_breaker(5, Duration::from_secs(30)));
let rca = Arc::new(
    ModelRoute::new("rca")
        .candidate(ModelCandidate::new("frontier", openai.clone()).with_model("gpt-6.1-sol"))
        .candidate(
            ModelCandidate::new("independent", anthropic.clone())
                .with_model("claude-opus-5-5")
                .with_eligible(|_request, ctx| budget_allows(ctx).then_some(()).ok_or("above the budget tier".into())),
        )
        .candidate(ModelCandidate::new("local", ollama.clone()).with_model("qwen3:32b"))
        .with_accept(|response| (!response.text().is_empty()).then_some(()).ok_or("an empty answer".into()))
        .with_hedge_after(Duration::from_secs(20))
        .with_health(health.clone()),
);
let agent = Agent::<RootCause>::new(rca, registry, AgentOptions::new("rca-analyst"));
```

---

## The API

| .NET (`Datalium.Strata.AI`) | Rust (`datalium_strata_ai::route`) | What |
|---|---|---|
| `ModelRoute(ModelRouteOptions)` : `IChatClient` | `ModelRoute::new(name)` with builders; `impl ChatClient` | The route. The `Name` is required and appears in spans and events. At least one candidate, with names unique within the route. |
| `ModelRouteOptions { Name, Candidates, Accept, HedgeAfter, Health, FaultClassifier }` | `.candidate()`, `.with_accept()`, `.with_hedge_after()`, `.with_health()`, `.with_fault_classifier()` | `Accept(ChatResponse)` returns null (accepted) or a reason. `HedgeAfter` is null (off) or above zero. `Health` defaults to a route-private `ModelHealth` with no breaker. |
| `ModelCandidate(Name, IChatClient Client, string? model = null) { HealthKey, Eligible }` | `ModelCandidate::new(name, Arc<dyn ChatClient>)`, `.with_model()`, `.with_health_key()`, `.with_eligible()` | One candidate. `model` replaces the request's model for this candidate. `HealthKey` defaults to the client's provider name (`ChatClientMetadata.ProviderName` / `provider()`), else the candidate's name. `Eligible(ChatOptions / ChatRequest, NodeContext?)` returns null or why not. |
| `ModelHealth(ModelHealthOptions { Breaker, TimeProvider })`: `Snapshot()` | `ModelHealth::new(ModelHealthOptions)` (`.with_breaker(failures, open_for)`), `snapshot()` | Health per key, thread-safe, shared by the routes given it. `Snapshot()` lists each key's state (`Available`, `CoolingDown(until)`, `Open(until)`, `HalfOpen`) and its failures in a row, for a health check or a panel. Rust's clock is tokio's `Instant`, so tests run with `start_paused`. |
| `ModelRouteException(Route, IsTransient, RetryAfter, ErrorType, Attempts)`, which `ModelFaults.Classify` reads first (`ModelCallException` is sealed) | `ChatError` with `error_type` `route_exhausted`, `route_ineligible` or `rejected` | The route failed. `Attempts` lists every candidate as a `ModelRouteAttempt`. Rust's `ChatError` has no room for them, so a caller reads them from `OnRouted`. |
| `ModelRouteOptions.OnRouted: Action<ModelRouteResult>` | `.on_routed(Fn(&ModelRouteResult))` | Called once per call through the route, inside a run or not, with what `ModelRouted` carries (`Route`, `Answered`, `Model`, `Attempts`, `Hedges`, `Duration`), before the call returns. It is for a caller outside a run (a ledger, the shared cases). It must not throw or block: an exception is caught and ignored, and a panic is caught. |
| — | `ChatClient::as_route(&self) -> Option<&ModelRoute>` (a default method returning `None`) | How `complete_with` finds a route behind `&dyn ChatClient`. .NET uses `client is ModelRoute`. A route wrapped in another client is called as that client (one model call), since a wrapper's `GetService` would hand back the route and silently skip the wrapper. |

**Engine (both):** `ModelCallInfo.Route` (`with_route`), and `ModelCallStarted` / `ModelCallCompleted` gain `Route` (null outside a route). A new run event:

`ModelRouted { Route, CallId, Answered (candidate name or null), Model (the answering model or null), Attempts: IReadOnlyList<ModelRouteAttempt(Candidate, Outcome, Reason, CallId?)>, Duration }`

`ModelRouteOutcome` is one of `Answered`, `Failed`, `Refused`, `Rejected`, `Ineligible`, `CoolingDown`, `BreakerOpen`, `LostRace`, `NotTried` or `Cancelled`. `CallId` pairs an attempt with its model-call events, and a candidate that was never asked has none.

---

## Semantics

### One call through a route

1. **Choose the candidates.** In order, each candidate is either **ineligible** (its `Eligible` gave a reason), **cooling down** (its key has a `Retry-After` in force), **breaker open** (its key's breaker is open and not yet due a test), or **available**. A half-open key lets one call through at a time; other calls see it as `BreakerOpen`.
2. **Ask the first available candidate.** The request is copied with the candidate's model when it names one. The attempt is one model call (below).
3. **When the attempt ends:**
   - *Succeeded:* `Accept` judges the response. Accepted, it is the answer. Rejected, the route moves on (`Rejected`), and the key's health is untouched.
   - *Refused* (finish reason `content_filter`): it moves on (`Refused`), and health is untouched.
   - *Transient fault:* it moves on (`Failed`). The key records a failure in a row and, when the fault carried one, a cooldown until `now + Retry-After`.
   - *Fatal fault:* the route stops. The fault is the call's (`Failed`), and the remaining candidates are `NotTried`.
   - Any success resets the key's failures in a row and closes a half-open breaker.
4. **Out of candidates:** the route fails.
   - It fails **fatal** (`route_ineligible`) when no candidate was ever available because each was ineligible.
   - Otherwise it fails **transient** (`route_exhausted`). Its retry hint is the shortest wait until some key's cooldown or open breaker ends, or none when a candidate could be asked again at once.
   - The message names each candidate and what happened: `no candidate of route 'rca' answered: frontier failed (429), independent refused, local is cooling down for 12 s`.

The node's `RecoveryPolicy` then retries as design/25 says, waiting for the hint when it has a backoff. Health persists across those retries, so a retry skips a candidate that is still cooling down.

### The exact rules (what the shared cases pin)

- **A candidate's state is decided when its turn comes,** not when the call starts. A candidate that fails with `Retry-After` therefore makes a later candidate under the same health key `CoolingDown` within the same call.
- **The key's state, in precedence:**
  - `BreakerOpen` while its breaker is open and `openFor` hasn't passed;
  - half-open once it has, letting one call through while the others see `BreakerOpen`;
  - `CoolingDown` while a `Retry-After` is in force;
  - otherwise available.
  
  A `Snapshot()` reports `open`, `half_open`, `cooling_down` or `available`, with `failures` (in a row) and `until` for the first and third.
- **Failures in a row count transient faults only,** a failure before a stream's first update and a `stream_broken` among them. A refusal, a rejection, a lost race and a cancellation leave the count alone. Any success on the key sets it to 0. When the count reaches `Breaker.Failures`, the breaker opens for `OpenFor`. A half-open test that fails opens it again for `OpenFor`.
- **The order of `Attempts`** is the candidates' declaration order, every candidate listed once. A candidate never reached is `NotTried`.
- **The messages** (the shared cases compare them exactly):
  - exhausted: `no candidate of route 'rca' answered: <each, joined by ", ">`. Each candidate reads `<c> failed (<error type>)`, `<c> refused`, `<c>'s answer was rejected (<reason>)`, `<c> is ineligible (<reason>)`, `<c> is cooling down for <n> s` or `<c>'s breaker is open for <n> s`, where `<n>` is the seconds left, rounded up.
  - all ineligible: `no candidate of route 'rca' can take this request: <each>`.
  - a streamed answer rejected: `route 'rca': <c>'s streamed answer was rejected (<reason>)`.
  - a streamed answer refused: `route 'rca': <c>'s streamed answer was refused`.
  - A fatal fault is the candidate's own error, unchanged.
- **Hedging's timer** runs from the latest attempt's start. When an attempt ends without winning and no other attempt is still running, the next available candidate starts at once (failover). When others are still running, the timer just keeps going.

### Hedging

With `HedgeAfter`, an attempt that hasn't finished (whole) or produced its first update (streamed) within the delay doesn't stop. The next available candidate starts beside it, and then the next after another delay, and so on. Each starts at most once and never more than the candidates there are.

- **Whole answers:** the first answer that is accepted wins. A rejected, refused or failed answer lets the others go on; a fatal fault ends the route after cancelling the rest.
- **Streams:** the first stream to produce an update wins, and only its updates reach the node's stream. A stream that fails before its first update counts as `Failed` and the others go on.
- **The losers** are cancelled through their tokens and recorded `LostRace` (their model calls `Cancelled`). The cancellation doesn't count against a key's health.

A hedge costs the tokens of every attempt that started. That is the price of a latency race, and why hedging is off unless set.

### Streams

As in StrataOps, a stream fails over only until its first update; after that the route has committed. A failure after the first update is the call's `stream_broken` (transient), as design/44's is, and it counts against the key's health. `Accept` and the refusal check are made when a stream ends. A streamed answer the route would have moved on from can't be withdrawn (its tokens have gone out), so the route fails the call transiently rather than moving on. The error type is `rejected` or `refused`, the reason is in the message, and the key's health is untouched.

### Cancellation

The node's cancellation stops every attempt in flight. Each one is recorded `Cancelled`, the route's `ModelRouted` is recorded with the remaining candidates `NotTried`, and the cancellation surfaces as it does today (.NET rethrows the node's own; Rust's future is dropped and the scopes record themselves cancelled). A cancellation never counts against health.

### Outside a node

A route works with no `NodeContext`: as an `IChatClient` passed to code that knows nothing of Strata, or through `StrataChat` without a context. The attempts still open spans and record metrics. There are no run events, as with any model call outside a run.

---

## Observation

When `StrataChat` / `complete_with` get a route, they don't open one model call for the whole route. The route instead opens:

- **A `strata.route` span**, display name `route <name>`, a child of the node's attempt. Its attributes are `strata.route`, `strata.route.answered`, `strata.route.attempts` (how many were asked) and `error.type` when the route fails.
- **One `ModelCallScope` per attempt**, a child of the route span, with the candidate's model and provider and `strata.route` / `strata.route.candidate` on it. Each has its own `ModelCallStarted` / `ModelCallCompleted` (with `Route`), usage, `gen_ai.client.token.usage` and duration. A ledger built on run events therefore sees every attempt with its tokens, as StrataOps' does.
- **`ModelRouted` once**, after the last attempt ends, with every candidate's outcome.

Metrics, with the meter `Datalium.Strata` (.NET) and the `metrics` facade (Rust):

- `strata.route.calls`, by `strata.route` and `strata.outcome` (`answered`, `exhausted`, `ineligible`, `fatal`, `cancelled`);
- `strata.route.failovers`, by `strata.route` and `strata.route.reason` (`failed`, `refused`, `rejected`, `cooling_down`, `breaker_open`, `ineligible`);
- `strata.route.hedges`, by `strata.route`;
- `strata.model.breaker.transitions`, by `strata.model.health_key` and `strata.model.breaker.state` (`open`, `half_open`, `closed`).

Without a route, `StrataChat` and `complete_with` behave exactly as today.

---

## Packaging and compatibility

- **The AI packages:** the route, candidates, health and errors, with no new dependency. Hedging uses `Task.WhenAny` and `CancellationTokenSource` in .NET, and `futures_util::stream::FuturesUnordered` with `tokio::time` in Rust (`tokio` is already in the dependency tree through the engine).
- **The engine:** `ModelCallInfo.Route`, the `Route` field on two events, and the `ModelRouted` event. Additive: observers already need a default arm. Rust's `RunEvent` variants carry `#[non_exhaustive]` fields where design/44 put them.
- **`ChatClient::as_route`** is a default method, so existing implementations compile unchanged.
- **Hosting:** none. A route is built in code because its candidates are clients and its rules are code. An application registers it like any `IChatClient`. `ModelHealth.Snapshot()` is there for an application's own health check; Strata adds none (review decision 6).

---

## Tests

- **Shared cases** in `schema/fixtures/ai/route-cases.json`, run by `strata-ai/tests/route_cases.rs` and `AI/ModelRouteCaseTests.cs`. Scripted candidates each answer, fail (transient with or without a hint, or fatal), refuse, or answer late, on a fake clock. Each case states the expected answer or error, the error's kind and hint, every attempt's outcome, and the health afterwards. They cover:
  - failover on each reason, and a fatal fault stopping the route;
  - all ineligible (fatal), and all exhausted (transient, with the shortest hint);
  - a cooldown skipping a candidate on the next call, and a breaker opening, half-opening and closing;
  - hedging: the second wins, the first wins after the second has started, and a hedged attempt is rejected while the first goes on;
  - a stream failing over before its first update and breaking after it;
  - a streamed answer `Accept` rejects;
  - the candidate's model replacing the request's.
- **Events and spans:** a route inside a run gives a `ModelCallStarted` / `ModelCallCompleted` pair per attempt with `Route`, one `ModelRouted`, a `route <name>` span holding the attempts' spans, and the metrics.
- **Cancellation:** attempts in flight are cancelled and recorded, the remaining candidates are `NotTried`, and health is untouched.
- **Composition:** `StrataAgent` / `Agent` over a route (a tool loop whose second turn fails over), a model node over a route, and a route outside a node.
- **Live (opt-in):** a route of the OpenAI-compatible and Anthropic clients where the first candidate names a model that doesn't exist (a fatal 404, so the route stops) and one with a bad key on a compatible server (401, fatal). Then a route whose first candidate is a closed local port (connection refused, transient) fails over to the real one.

---

## Wire format

None. No checkpoint, definition, database or history change. The run event, the event fields and the telemetry names are additive.

---

## Review decisions

Recommendations are in bold; the build will follow each unless review changes it.

1. **Does a fatal fault fail over?** **No.** A 400 or 401 says the request or the configuration is wrong, and another provider would hide it at the price of a second call. StrataOps' router does the same. An application that wants otherwise can classify that provider's fault as transient (`FaultClassifier`).
2. **The retry hint when nothing answered: the shortest or the longest wait?** **The shortest** until some candidate is available again: the route can then try that one. StrataOps' router takes the longest ("every candidate can be asked again"), which waits for the slowest provider when one would do.
3. **A refusal: fail over, and does it count against health?** **Fail over, health untouched.** Another model may answer, and a refusal says nothing about the provider's availability.
4. **A streamed answer `Accept` rejects, or one refused after its first update.** **Fail the call transiently** (`rejected` / `refused`), since its tokens are out. The alternative, buffering a stream until `Accept` has judged it, defeats streaming.
5. **Health keys: the provider by default?** **Yes**, as StrataOps' breaker is per provider (an outage or rate limit takes every model of a provider). A candidate can name its own key, say per model or per region.
6. **A health check over `ModelHealth`?** **Not built.** A breaker open on one provider is the route working as intended, not an unhealthy host. `Snapshot()` is there for an application that wants one.
7. **Configuration-bound routes in hosting** (`Strata:Routes:rca:Candidates`)? **No** (settled 2026-10-21 with the application composing routes): candidates are clients and rules are code.
8. **Cost and budgets?** **Not here**, as design/44 decision 11 has it. `Eligible` is where an application's budget rule goes, and per-attempt usage in the run events is what its ledger reads.

---

## Implementation Status

**Built 2026-10-21 in both engines** on review decisions 1 to 8 as written (agreed 2026-10-21). All 27 shared cases pass in each engine. StrataOps' router, whose failover this generalizes, has run since M1; hedging is new to both. Where the build diverges from or completes the text above:

- **The recording API is public in the engine:** `ModelRouteScope` (.NET `Datalium.Strata.Observability`, Rust `observability`).
  - `Start(ctx, route)` opens the `strata.route` span.
  - `Attempt` (Rust) or a `ModelCallInfo` with `Route` (.NET) opens an attempt's model call.
  - `Hedged` and `FailedOver` / `failover` count.
  - `Completed` / `finish` records `ModelRouted` once.
  
  An application's own router can record through it too. `ModelCallInfo` also gains `Candidate` (Rust `route_candidate`) for the `strata.route.candidate` tag.
- **`strata.route.calls` has a sixth outcome, `failed`:** a stream the route had committed to that broke, or was refused or rejected, after its first update.
- **`strata.route.failovers`** counts every time the route moves past a candidate: a transient failure (on the last candidate too), a refusal, a rejection and each skip. A fatal fault, an outcome after a stream committed, a lost race and a cancellation aren't counted.
- **A fatal fault while hedging** records the other attempts still in flight `Cancelled`; only a race's losers are `LostRace`.
- **`ModelRouteAttempt.Reason`** holds a failure's error type (`429`, `connection`, `stream_broken`), the `Accept` or eligibility reason, or `<n> s` for a cooldown or an open breaker (whole seconds rounded up; `0 s` for a half-open key whose test call is out). It is null otherwise.
- **Unspecified cases settled the same way in both engines:**
  - a success clears a key's failures and closes its breaker but leaves a cooldown in force;
  - a stream that ends without any update is judged as a whole answer, and may still fail over;
  - a route with no candidates fails fatally (`route_ineligible`); .NET refuses one at construction.
  - .NET only: an `Accept` that throws fails the call with that exception (fatal, `accept`). Rust's `Accept` returns a `Result`.
- **Settled after a review of the build**, and pinned by two more shared cases where they can be:
  - **Cooldowns:** concurrent failures on one key keep the later cooldown (a shorter `Retry-After` never shortens it), and a failure doesn't re-open or push back a breaker that is already open. A cooldown is capped at 24 hours.
  - **A half-open test call is always released,** whatever throws or panics around it (an `Accept`, a classifier, a blank model), so a key can't stay `BreakerOpen` for good.
  - **.NET's winner doesn't wait for its losers:** they are cancelled and left to finish in the background. So a loser's `ModelCallCompleted` may come after the run's `RunCompleted`, and an event recorded then is dropped (design/18, "Delivery"): read the losers from `ModelRouted`'s attempts. Rust drops a loser's future before the route returns, so its events always arrive. (Documented 2026-10-24, from the .NET demo 24.)
  - **A loser whose answer had already arrived** is `LostRace`, but its model call records `Succeeded` with its usage, because those tokens were billed. Only one still pending is recorded `Cancelled`.
  - **A token the node's stream won't take** (not a cancellation) fails the call transiently with `stream_closed`, and the route's outcome is `failed`.
  - **`ModelRouted.Model`** is `unknown` when nothing names a model, as the model call says. A blank candidate model is refused when the candidate is made.
  - **A call that stops** (cancelled, a fatal fault, the caller abandoning it) still waits for its attempts in flight in .NET, so their `ModelCallCompleted` comes before `ModelRouted` and before the run ends. A client that ignores its token therefore delays a cancellation. Rust drops them at once.
- **A route's failure in a node** reads `model call failed (route_exhausted): no candidate of route 'rca' answered: …` in both engines, as any model call's does (Rust through `NodeError::from(ChatError)`, .NET through `ModelFaults.ToNodeException`).
- **.NET's `Eligible`** takes `(messages, options, ctx)`, so it can apply a context-size rule as Rust's `(request, ctx)` can.
- **Called as a plain client,** a stream that breaks after its first update surfaces the candidate's own error. Through `StrataChat` / `complete_with` it becomes `stream_broken`, as design/44 has it.
- **Not built:** the live opt-in route tests listed under "Tests". Neither engine has a dedicated test of the `strata.model.breaker.transitions` and `strata.route.hedges` metrics. The breaker's states are covered by the shared cases through `Snapshot()`.
- **The package smoke app's model node** goes through a two-candidate route whose first candidate refuses the connection.

---

## Not built (deliberately out of scope)

- **Racing agents, and a join over some branches only.** Agents race as graph branches (design/26). A quorum that counts only named branches (ADR 0038's "the local model races but doesn't decide") would be an engine change of its own, and nothing needs it now.
- **Retries on the same candidate** (decision: the node's `RecoveryPolicy` retries).
- **A policy catalog, policy files, request keys, and configuration-bound routes** (the application's).
- **Budget, cost and pricing** (design/44 decision 11).
- **Load balancing** (weighted or round-robin choice among candidates): a route is ordered. An application can shuffle its candidates per call through `Eligible` or build routes in another order.
- **Embedding routes**: an `IEmbeddingGenerator` / `Embedder` route waits for SG-32's embedders (design/45).
- **Persisting health across processes**: health is in memory, per process, as StrataOps' breaker is.
