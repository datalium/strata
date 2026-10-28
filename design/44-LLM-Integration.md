# Datalium Strata: LLM Integration

Strata runs agents but has never called a model. Its tool registry, middleware, run events, telemetry, retries with `Retry-After`, streaming and node context were all built so that an application could wire a model into a node. Every application then writes the same plumbing: StrataOps' Model Fabric (`LlmAgent`, `StreamedOutput`, `AgentOutput`, `RetryAfter`, `DescriptorChatClient`) is about a thousand lines of it. `design/20-Tool-Dispatch-And-Registry.md` left provider manifest renderers and the model ↔ tool loop out of the engine on purpose, for this document.

This document closes that gap: **SG-23**. The SG numbers refer to the gap log of StrataOps (not public yet). It lands in both engines (strict parity, StrataOps ADR 0003), in **two PRs** under this one blueprint:

- **PR 1, models:** the chat abstraction (.NET: `Microsoft.Extensions.AI`'s `IChatClient`; Rust: a `ChatClient` trait of Strata's own, with OpenAI-compatible and Anthropic clients), a model call from a node that streams and is observed (`ModelCallStarted` / `ModelCallCompleted` run events, a GenAI `chat` span and metrics), provider faults mapped to `NodeException` / `NodeError` with the retry hint, and a model node.
- **PR 2, agents:** the agent (the model ↔ tool loop through a `ToolRegistry`), provider-safe tool names, and structured output parsed into a type, validated and repaired.

It was written as a blueprint before implementation; both PRs are built. "Review decisions" lists the questions for review with the answer the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Where it lives | **New packages**: .NET `Datalium.Strata.AI` (the engine plus `Microsoft.Extensions.AI.Abstractions`), Rust `rust/strata-ai` (`datalium-strata-ai`: `strata-core`, `reqwest` with `rustls`, `serde`). The engine gains only the model-call scope and its two run events, with no new dependency. |
| The chat abstraction | **.NET: `IChatClient`**, as is: every provider that implements it (OpenAI, Azure OpenAI, Anthropic's SDK, Ollama, a test fake) works unchanged. **Rust: Strata's own `ChatClient` trait** (`complete`, `stream`), with `Message` / `Content` / `ChatRequest` / `ChatResponse` / `ChatUpdate` types shaped after the same model, not a third-party crate. |
| Rust providers | **Two, both streaming (SSE):** `OpenAiChatClient` (Chat Completions, any OpenAI-compatible endpoint: OpenAI, Azure, Ollama, vLLM, …) and `AnthropicChatClient` (Messages). Each behind a cargo feature, both on by default. No retries inside them: Strata's `RecoveryPolicy` retries, with the provider's `Retry-After` as the hint. |
| A model call from a node | **`StrataChat.GetResponseAsync(client, messages, options, ctx, ct)`** (Rust `strata_ai::complete(&client, request, &ctx)`): streams when the run streams (text deltas to `ctx.Stream` as `Token` chunks) and otherwise doesn't, assembles the response, records the call, and maps a provider fault to a node failure. Everything else in this document is built on it. |
| Observing it | **Engine run events `ModelCallStarted { Model, CallId }` and `ModelCallCompleted { Model, CallId, Result, Usage, FinishReason, Duration, Error }`**, recorded through a new public `ModelCallScope` in the engine (as `ToolCallScope` records tool calls). The scope also opens a GenAI-convention span (`chat <model>`) and records `gen_ai.client.token.usage` and `gen_ai.client.operation.duration`. Prompts and completions are never recorded. |
| Faults | **Transient** (with the retry hint): 408, 409, 429, 5xx, timeouts, connection failures, a stream broken after it started. **Fatal**: every other 4xx (bad request, auth, not found, content refused). The hint is `retry-after-ms`, else `Retry-After` (seconds or an HTTP date). .NET classifies `HttpRequestException` and `System.ClientModel`'s `ClientResultException`, and takes a classifier for anything else. |
| The model node | **`AddModelNode(name, client, prompt, apply, options)`** (Rust `add_model_node` on an extension trait): a fallible async node that builds messages from the state, makes one call, and folds the response into the state. Sugar over the call; a node can make the call itself. |
| The agent | **`StrataAgent<TOutput>`** (Rust `Agent<O>`), a value a node runs: turns of model call → tool calls through a `ToolRegistry` (a subset, its middleware included) → results back to the model, until the model answers without tools or `MaxTurns` (8) is reached. `AddAgentNode` is sugar. |
| Tool calls in a turn | **Sequential by default; `ToolConcurrency` runs them concurrently**, results returned in the model's order either way. Each call's context is `ToolCallContext.From(ctx) with { CallerId = agent id, CallId = the model's call id }`. |
| A refused or failed tool call | **Refusals go back to the model** as `{"error": "…"}`: not found, not permitted, denied by middleware, arguments that don't parse. **Other failures fail the node**, transient or fatal as the tool's failure says (`ToolFailures.ReturnToModel` sends those back too). |
| Tool names | **Renamed for the provider and back**: a Strata name like `k8s.deployment` becomes `k8s_deployment` (`[a-zA-Z0-9_-]`, at most 64 characters, made unique), per agent, mapped back before dispatch. Events, spans and middleware see the Strata name. |
| Structured output | **A typed `TOutput`**: its JSON schema as the response format (.NET from `AIJsonUtilities` with the agent's `JsonSerializerOptions`; Rust from `#[derive(StrataSchema)]`, extended for nested types), the answer parsed (a Markdown fence stripped), then a validator. A bad answer gets `MaxRepairs` (1) repair turns with the problems listed, then the node fails transiently. A text agent's output is the answer's text. |
| History | **Per attempt, in memory**: the agent's messages live for one attempt; a retry starts over. `IHistoryStore` is not used: its records are text only. |
| Wire format | **None.** No checkpoint, definition, database or history change. Two new run event types and new span and metric names. |

---

## Usage

```csharp
// One model call from a node: streams under RunStreaming, observed either way.
graph.AddNode("Summarize", async (state, ctx, ct) =>
{
    var response = await StrataChat.GetResponseAsync(chat,
        [new(ChatRole.System, "Summarize the incident in two sentences."), new(ChatRole.User, state.Timeline)],
        new ChatOptions { ModelId = "gpt-5-mini" }, ctx, cancellationToken: ct);
    return state with { Summary = response.Text };
});

// The same as a model node.
graph.AddModelNode<IncidentState>("Summarize", chat,
    prompt: (s, ctx) => [new(ChatRole.System, "Summarize …"), new(ChatRole.User, s.Timeline)],
    apply: (s, response) => s with { Summary = response.Text },
    options: new ModelNodeOptions { Chat = new() { ModelId = "gpt-5-mini" } });
graph.SetRecoveryPolicy("Summarize", RecoveryPolicy.Retry(3).WithBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)));

// An agent with tools and a typed answer.
public sealed record Diagnosis(string Cause, double Confidence, IReadOnlyList<string> Evidence);

var investigator = new StrataAgent<Diagnosis>(chat, registry.Subset("k8s.pods", "k8s.deployment", "prometheus.query"),
    new StrataAgentOptions<Diagnosis>
    {
        Id = "kubernetes-investigator",
        Chat = new() { ModelId = "claude-sonnet-5-5" },
        MaxTurns = 6,
        Validate = d => d.Confidence is >= 0 and <= 1 ? [] : ["confidence must be between 0 and 1"],
    });

graph.AddFallibleNode("Investigate", async (state, ctx, ct) =>
{
    var result = await investigator.RunAsync(
        [new(ChatRole.System, Prompts.Investigator), new(ChatRole.User, state.Alert)], ctx, ct);
    return state with { Diagnosis = result.Output, ModelTokens = state.ModelTokens + (result.Usage.TotalTokens ?? 0) };
});
```

```rust
use datalium_strata_ai::{complete, Agent, AgentOptions, ChatRequest, Message, OpenAiChatClient, AnthropicChatClient};

let chat: Arc<dyn ChatClient> = Arc::new(OpenAiChatClient::new(OpenAiOptions::new(api_key).with_model("gpt-5-mini")));

graph.add_async_fallible_node("summarize", move |state: Incident, ctx| {
    let chat = chat.clone();
    async move {
        let response = complete(&*chat, ChatRequest::new(vec![
            Message::system("Summarize the incident in two sentences."),
            Message::user(&state.timeline),
        ]), &ctx).await?;
        Ok(Incident { summary: response.text(), ..state })
    }
});

#[derive(Deserialize, StrataSchema)]
struct Diagnosis { cause: String, confidence: f64, evidence: Vec<String> }

let investigator = Agent::<Diagnosis>::new(claude, Arc::new(registry.subset(["k8s.pods", "prometheus.query"])?),
    AgentOptions::new("kubernetes-investigator").with_max_turns(6)
        .with_validate(|d: &Diagnosis| if (0.0..=1.0).contains(&d.confidence) { vec![] } else { vec!["confidence must be between 0 and 1".into()] }));
let result = investigator.run(vec![Message::system(PROMPT), Message::user(&state.alert)], &ctx).await?;
```

---

## The API

### Engine (both): the model-call scope

The engine records run events only from its own code (`RunEvent`'s constructor is private, the run's telemetry internal). A model call made by a package needs a public way in, so the engine gains one, beside `ToolCallScope`:

```csharp
namespace Datalium.Strata.Observability;

public sealed record ModelCallStarted(string Model, string CallId, string? Provider) : RunEvent;
public sealed record ModelCallCompleted(
    string Model, string CallId, ModelCallResult Result, ModelUsage Usage, string? FinishReason, TimeSpan Duration, string? Error)
    : RunEvent;
public enum ModelCallResult { Succeeded, Failed, Cancelled }
public sealed record ModelUsage(long? InputTokens, long? OutputTokens);

/// One model call, from ModelCallStarted to ModelCallCompleted. Disposing it without an outcome records Cancelled.
public sealed class ModelCallScope : IDisposable
{
    public static ModelCallScope Start(NodeContext? node, ModelCallInfo info);   // info: Provider, Model, Operation ("chat"), request attributes
    public Activity? Activity { get; }
    public string CallId { get; }                                                // a UUID v7
    public void FirstChunk();                                                    // time to first chunk, for a streamed call
    public void Succeeded(ModelUsage usage, string? finishReason, string? responseModel, string? responseId);
    public void Failed(string errorType, string message);
    public void Cancelled();
}
```

Rust: `observability::{ModelCallScope, ModelCallInfo, ModelUsage, ModelCallResult}` and `RunEventKind::ModelCallStarted` / `ModelCallCompleted` (the enum is `#[non_exhaustive]`, so adding variants is not breaking). `ModelCallScope::start(Some(&ctx), info)`.

### .NET: `Datalium.Strata.AI`

| Type | What |
|---|---|
| `StrataChat` | `GetResponseAsync(IChatClient, IEnumerable<ChatMessage>, ChatOptions?, NodeContext?, StrataChatOptions?, CancellationToken)` → `ChatResponse`; `StrataChatOptions` carries the `FaultClassifier` and the `Agent`. |
| `ModelFaults`, `ModelFault`, `RetryAfter` | `Classify(Exception, IModelFaultClassifier?)` → `ModelFault(IsTransient, RetryAfter, ErrorType)`; `ToNodeException(fault, exception)`; `IsTransientStatus(int)`; `RetryAfter.Parse(retryAfterMs, retryAfter, now)` and `From(response)`; `IModelFaultClassifier` for providers the default doesn't know. |
| `RetryAfterHandler` | A `DelegatingHandler` for an `HttpClient`-based provider: a 429/5xx response becomes a `ModelCallException` carrying the status and the hint. |
| `ModelCallException` | A provider fault Strata understands: `IsTransient`, `StatusCode`, `RetryAfter`. |
| `GraphModelExtensions` | `AddModelNode<TState>(name, client, prompt, apply, ModelNodeOptions?)`. |
| `StrataAgent<TOutput>` | `new StrataAgent<TOutput>(IChatClient, ToolRegistry, StrataAgentOptions<TOutput>)`; `RunAsync(messages, NodeContext, CancellationToken)` → `AgentResult<TOutput>`; `Id`, `Options`, `Declarations`, `ResponseFormat` (`null` for a text agent). The text agent is `StrataAgent<string>`. |
| `StrataAgentOptions<TOutput>` | `Id` (required), `Chat` (`ChatOptions`), `MaxTurns` (8), `MaxRepairs` (1), `ToolConcurrency` (1), `ToolFailures` (`FailNode` / `ReturnToModel`), `Validate` (`Func<TOutput, IReadOnlyList<string>>`), `JsonOptions`, `FaultClassifier`, `ConfigureTurn` (`Action<AgentTurn, ChatOptions>`, since 2026-10-24: changes one turn's copy of the options). |
| `AgentResult<TOutput>`, `AgentToolCall` | `Output`, `Messages` (the whole conversation), `Usage` (summed), `Turns`, `ToolCalls`: `AgentToolCall(Name, CallId, Result)`, `Result` the engine's `ToolCallResult`. |
| `ToolDeclarations` | `For(ToolRegistry)` → `Tools` (`AIFunctionDeclaration`s under provider-safe names), `StrataName(name)`, `ProviderNameOf(strataName)`. |
| `AgentNodes` | `AddAgentNode<TState, TOutput>(name, agent, prompt, apply, metadata?)`. |

### Rust: `datalium-strata-ai`

| Item | What |
|---|---|
| `ChatClient` (trait) | `fn complete(&self, ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>>` and `fn stream(&self, ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>>` (`ChatStream` a boxed `Stream` of `Result<ChatUpdate, ChatError>`), plus `fn provider(&self) -> &str` and `fn default_model(&self) -> Option<&str>`. Object safe. Cancellation is dropping the future. |
| `Message`, `Role`, `Content` | `Content::Text`, `ToolCall { id, name, arguments: Value }`, `ToolResult { call_id, content }`, `Reasoning { text, signature }` (Anthropic's thinking goes back with its signature; nothing else is sent back). |
| `ChatRequest` | messages, `model`, `tools: Vec<ToolDeclaration>`, `tool_choice`, `response_format` (`Text` / `JsonSchema { name, schema, strict }`), `max_tokens`, `temperature`, `extra` (a JSON map merged into the body). |
| `ChatResponse`, `ChatUpdate`, `ModelUsage` | The answer (message, finish reason, model, id, usage); a streamed piece (`Text`, `Reasoning`, `ToolCall { index, id, name, arguments }`, `Usage`, `Finish`, `Meta`); `ChatUpdates` assembles pieces into a response. The usage type is the engine's `ModelUsage`, re-exported. |
| `ChatError` | `kind` (`Transient` / `Fatal`), `status`, `retry_after`, `error_type`, `message`; `impl From<ChatError> for NodeError`. |
| `OpenAiChatClient`, `AnthropicChatClient` | Options: base URL, API key (never printed), model, timeouts, extra headers; `OpenAiOptions::compatible(url)` and `ollama(url)` for other endpoints, `MaxTokensField` for which body field carries the limit; `AnthropicOptions::default_max_tokens` (4096). |
| `complete`, `complete_with`, `CallOptions`, `StateGraphAiExt::add_model_node`, `ModelNodeOptions` | As .NET's `StrataChat`, `StrataChatOptions` and `AddModelNode`. |
| `Agent<O>`, `AgentOptions<O>`, `AgentResult<O>`, `AgentToolCall`, `ToolFailures` | As .NET's agent: `Agent::new(Arc<dyn ChatClient>, Arc<ToolRegistry>, AgentOptions::new(id)…)`, `run(messages, &ctx)`. `O: DeserializeOwned + StrataSchema`; `Agent<String>` is a text agent. Options are builders (`with_request`, `with_model`, `with_max_tokens`, `with_temperature`, `with_max_turns`, `with_max_repairs`, `with_tool_concurrency`, `with_tool_failures`, `with_validate`, `with_fault_classifier`, and since 2026-10-24 `with_configure_turn` with `AgentTurn`). |
| `ToolDeclarations` | `for_registry(&ToolRegistry)` → `declarations()`, `strata_name(name)`, `provider_name_of(name)`. |
| `StateGraphAiExt::add_agent_node` | As .NET's `AddAgentNode`, taking an `Arc<Agent<O>>`. |

---

## Semantics

### A model call (PR 1)

1. **Start**: a `ModelCallScope` with the request's model (the options' model, else the client's default, else `unknown`) and the provider (`ChatClientMetadata.ProviderName`; Rust `provider()`). `ModelCallStarted` is recorded when the call is made from a node.
2. **Stream or not**: when `ctx.Stream` is present the call streams, and each update's text goes to it as a `Token` chunk (reasoning and tool-call deltas don't); otherwise it is one request. Either way the caller gets a whole `ChatResponse`.
3. **Cancellation**: the node's token stops the call. A cancelled call records `Cancelled` and rethrows the cancellation (the engine then ends the node `Cancelled` as for any handler).
4. **A fault**: the scope records `Failed` with `error.type` (the HTTP status, `timeout`, `connection`, `stream_broken`, or the exception's type), and the call throws a `NodeException` (Rust returns `NodeError`): transient with the hint, or fatal. A stream that breaks after its first update is transient: the node's retry runs the call again, and the stream has a `Retry` chunk before the new attempt's tokens, as for any retried node.
5. **Success**: usage from the response (summed from `UsageContent` updates when streamed; Rust asks OpenAI for `stream_options.include_usage`), the finish reason, the response model and id. Usage a provider doesn't report is `null`, not estimated.

### Faults

| Response | Kind | Hint |
|---|---|---|
| 408, 409, 429, every 5xx (Anthropic's 529, overloaded, included) | transient | `retry-after-ms`, else `Retry-After` (seconds or an HTTP date), else none |
| Timeout, connection failure, a stream broken after it started | transient | none |
| 400, 401, 403, 404, 413, 422, other 4xx | fatal | — |
| A refusal (finish reason `content_filter` / `refusal`) | not a fault | the response, as it is |

.NET's default classifier recognizes `ModelCallException`, `HttpRequestException` (`StatusCode`), `System.ClientModel.ClientResultException` (`Status`, and its raw response's headers for the hint: what the OpenAI and Azure SDKs throw) and `TimeoutException`; an `IModelFaultClassifier` in the options handles anything else (a provider SDK's own exception), and anything nobody classifies is fatal. The OpenAI .NET SDK retries on its own by default; the doc says to turn that off (`MaxRetries = 0` through its pipeline options) so Strata's policy, which sees the hint, does the retrying.

### The model node (PR 1)

`AddModelNode(name, client, prompt, apply, options)` adds a **fallible async node**: `prompt(state, ctx)` builds the messages, `StrataChat` makes the call, `apply(state, response)` returns the next state. A transient fault fails the attempt transiently, so `SetRecoveryPolicy` with a backoff retries it with the provider's hint (design/25). Exported like any node; `options.Metadata` is its node metadata (design/22).

### The agent's loop (PR 2)

1. **Declarations**: the registry's tools become the provider's tool declarations under provider-safe names (see "Tool names"). The `ChatOptions` / `ChatRequest` the options give are copied for each turn, and tools and the response format are set by the agent. Then `ConfigureTurn` / `with_configure_turn`, when given, may change that turn's copy. It sees an `AgentTurn`: the turn's number from 1, the tool turns before it, the repairs so far, whether it is a repair, and in .NET the conversation. That is how a tool call is required on the first turn only; required on every turn, the model could never answer.
2. **A turn**: one model call through `StrataChat` (so it streams, is observed and is classified like any call), with the conversation so far.
3. **Tool calls**: if the answer has any, each is mapped back to its Strata name and dispatched through the registry with `ToolCallContext.From(ctx) with { CallerId = Id, CallId = <the model's call id> }`, so middleware, the `strata.tool` span and the `ToolCallStarted` / `ToolCallCompleted` events see the agent and the model's id. With `ToolConcurrency` 1 they run one after another, in the model's order; above 1, up to that many at once. Results go back in one tool message, in the model's order. Then the next turn.
4. **The answer**: a turn with no tool calls is the answer. A typed agent parses and validates it (below); a text agent returns its text.
5. **Limits**: more than `MaxTurns` turns with tool calls fails the node **transiently** (`agent 'X' made 8 tool turns without answering`). Repair turns don't count as tool turns.

**A refused or failed tool call**:

| What happened | What the model gets | The node |
|---|---|---|
| Not found, not permitted (`ToolNotFoundException`, `ToolNotPermittedException`; Rust `ToolNotFound`, `ToolNotPermitted`) | `{"error": "<message>"}` | goes on |
| Denied by middleware (`ToolCallDeniedException`; Rust `ToolCallDenied`) | `{"error": "denied: <reason>"}` | goes on |
| Arguments the tool can't take (.NET `ToolArgumentsException` from the generated dispatch; Rust `ToolArgumentParseFailure`; design/20, "Arguments a tool can't read") | `{"error": "invalid arguments for tool 'x': <problems>"}` | goes on |
| Cancellation | — | ends `Cancelled` |
| Any other failure | `{"error": "<message>"}` with `ToolFailures.ReturnToModel` | fails, with the default `FailNode`: a `NodeException` / `NodeError` the tool threw as it is, anything else through the fault classifier (transient for a timeout or an HTTP fault, else fatal) |

### Tool names (PR 2)

Providers accept `^[a-zA-Z0-9_-]{1,64}$`; Strata names are free text (`k8s.deployment`). The renderer replaces each other character with `_`, truncates to 64, and makes the result unique within the agent's tools by a `_2`, `_3` suffix, in registration order. The map is per agent and fixed when the agent is built, so a model sees the same names on every turn. A call to a name the map doesn't have is dispatched under that name, and the registry refuses it as not found (and the model is told).

### Structured output (PR 2)

- **The schema**: .NET `AIJsonUtilities.CreateJsonSchema(typeof(TOutput), serializerOptions: JsonOptions)` as `ChatResponseFormat.ForJsonSchema`; Rust `O::json_schema()` as `ResponseFormat::JsonSchema` with `strict` on for OpenAI, and the request's `output_format` for Anthropic (where the model doesn't support it, the system prompt carries the schema and the parse-and-repair below does the rest).
- **`#[derive(StrataSchema)]` grows** to describe a structured answer: a field of another `StrataSchema` type becomes its schema, `Vec<T>` / slices an `array` with `items`, `HashMap<String, T>` an `object` with `additionalProperties`, `Option<T>` stays optional, a fieldless `enum` a `string` `enum`, and `#[serde(rename_all)]` / `#[serde(rename)]` are honoured. Tool argument manifests get the same, so a `Vec<String>` tool parameter now says `array` (it said `object`).
- **Parsing**: the answer's text, with a surrounding Markdown code fence removed, deserialized with the agent's options. Then `Validate(output)` returns problems (an empty list is valid).
- **Repair**: unparseable or invalid output gets a user message listing the problems ("Your answer was not valid: …. Answer again with one JSON object that matches the schema."), up to `MaxRepairs` times. Still bad: the node fails **transiently** (`agent 'X' gave no valid answer: …`), so a retry gets a fresh conversation.

---

## Telemetry

The span and metrics follow the OpenTelemetry GenAI semantic conventions, so a GenAI-aware backend shows them as model calls. They come from the engine's `ModelCallScope`, under the engine's source (`Datalium.Strata`; Rust target `datalium_strata`), so `AddInstrumentation()` needs no change.

| | |
|---|---|
| Span | `strata.model`, display name `chat <model>` (`otel.name`), a child of the current span (inside a node, its attempt's), and of the node context's span when nothing is current, as `strata.tool` is. |
| Span attributes | `gen_ai.operation.name` `chat`, `gen_ai.provider.name`, `gen_ai.request.model`, `gen_ai.response.model`, `gen_ai.response.id`, `gen_ai.response.finish_reasons`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `gen_ai.request.max_tokens`, `gen_ai.request.temperature`; `strata.thread_id`, `strata.run_id`, `strata.node.path`, `strata.attempt`, `strata.model.call_id`, `strata.agent` (the agent's id, PR 2); `error.type` on a failure. No message content. |
| `gen_ai.client.token.usage` | histogram, `{token}`, by `gen_ai.token.type` (`input`/`output`), `gen_ai.operation.name`, `gen_ai.provider.name`, `gen_ai.request.model`, `gen_ai.response.model`. Recorded only for usage the provider reported. |
| `gen_ai.client.operation.duration` | histogram, `s`, the same attributes plus `error.type`. |
| `strata.model.time_to_first_chunk` | histogram, `s`, for a streamed call. |

An application that also wraps its client in `Microsoft.Extensions.AI`'s `UseOpenTelemetry()` gets that client's `chat` span inside Strata's, with the same names; the doc recommends one or the other.

---

## Packaging

- **.NET:** `Datalium.Strata.AI` is the **fifth package** (`dotnet/src/Package.props`, `.github/scripts/pack-smoke.sh`'s expected set, and the smoke app runs a model node against a scripted `IChatClient`). It references the engine with `PrivateAssets="contentfiles;build"` like the others, and `Microsoft.Extensions.AI.Abstractions` (10.x) and `System.ClientModel`.
- **Rust:** `datalium-strata-ai` is the **sixth crate**, `[workspace.dependencies]` pinned at the one version, published after `datalium-strata` (it doesn't depend on `-memory`, `-cloud` or `-host`). Features `openai` and `anthropic`, both default.
- `CHANGELOG.md`: an entry under the unreleased version. CLAUDE.md: a new item and the project tree.

---

## Tests

- **A scripted chat client in each engine** (`ScriptedChatClient`: a queue of responses or update sequences, each request recorded), used by every test below.
- **Shared cases** `schema/fixtures/ai/agent-cases.json` (not a wire format), run by both engines like the scheduler's: each case is a scripted model (turns of text and tool calls, faults), a registry (tools that answer, fail, are left out of the subset, or are denied by middleware), options, and the expected tool calls in order, run events, and outcome (output, failure kind and message).
- **PR 1**: a call streams under `RunStreaming` and doesn't otherwise; usage and finish reason reach `ModelCallCompleted` and the span; each fault row of the table maps as stated, the hint reaching `RetryScheduled.Delay` through a `RecoveryPolicy` with backoff; a stream broken mid-way retries with a `Retry` chunk; cancellation records `Cancelled`; a call outside a node opens the span and records no event; the model node exports and runs.
- **PR 1, Rust providers**: request bodies and response/SSE parsing against recorded fixtures under `rust/strata-ai/tests/fixtures/` (OpenAI: text, tool calls streamed in pieces, `include_usage`, a 429 with `retry-after-ms`; Anthropic: text, `tool_use`, `message_delta` usage, a 529), served by a local test server.
- **PR 2**: every row of the refusal table; concurrent tool calls keep the model's order; `MaxTurns`; the name map (collisions, 64 characters, an unknown name); repair once then succeed, and repair exhausted; middleware sees the agent's caller id and the model's call id; `#[derive(StrataSchema)]` on nested types, vectors, maps and enums.
- **Live, opt-in**: `STRATA_TEST_OPENAI_KEY` (and `STRATA_TEST_OPENAI_URL` for an OpenAI-compatible endpoint such as Ollama), `STRATA_TEST_ANTHROPIC_KEY`: one call, one streamed call, one tool loop with a typed answer, per provider and engine.
- **Mutation checks**: mapping a 429 as fatal; dropping the hint; recording usage before the call ends; returning a non-refusal tool failure to the model by default; not mapping a tool name back.

---

## Wire format

None. No checkpoint, graph definition, database or history change. New run event types (`ModelCallStarted`, `ModelCallCompleted`) are additive (both engines' observers already need a default arm), and the new span and metric names are additive. The tool manifest a Rust `#[derive(StrataSchema)]` type produces describes `Vec`, maps, nested types and enums properly from PR 2; manifests aren't stored.

---

## Review decisions

Recommendations are in bold; the build follows each.

1. **Rust: Strata's own trait, or a crate (`genai`, `rig`, `async-openai`)?** **Our own** (decided 2026-10-21). A dependency would put another project's release cadence and type model into the engine's public API; the trait is small, and two clients cover most endpoints.
2. **Which Rust providers?** **OpenAI-compatible Chat Completions and Anthropic Messages** (decided 2026-10-21). Not the OpenAI Responses API: Chat Completions reaches every compatible endpoint; Responses can come later as a third client.
3. **Usage as run events, or telemetry only?** **Run events in the engine** (decided 2026-10-21), through a public scope, so an observer sees a run's model calls and usage like its tool calls.
4. **Retries inside the providers?** **No.** One retry loop, Strata's, which honours the hint and the node's deadline and shows each attempt as an event. Two loops multiply attempts and hide waits.
5. **The agent: a node kind or a value a node runs?** **A value** (`StrataAgent.RunAsync(messages, ctx, ct)`), with `AddAgentNode` as sugar. A node kind would need export and validation support for nothing the graph can see; a value composes with whatever the node does before and after (StrataOps' prefetch, its evidence blocks), and an agent can run inside a branch or a map item.
6. **Too many tool turns: transient or fatal?** **Transient.** A model's path is not deterministic and a fresh attempt often converges; a policy without retries makes it final anyway.
7. **Other tool failures: back to the model, or fail the node?** **Fail the node by default**: a failing tool is usually infrastructure, which the node's retry and the fault classes handle. `ReturnToModel` is there for agents that should work around it.
8. **Tool calls of a turn: concurrent by default?** **No, sequential by default**, as `Microsoft.Extensions.AI`'s function invocation does: tools may have effects, and the model's order is the safe reading. `ToolConcurrency` opts in.
9. **History in `IHistoryStore`?** **No.** Its records are text only (role, content), so a conversation with tool calls doesn't fit without a history wire-format change; StrataOps keeps conversations per attempt on purpose (failover resends the whole conversation). A history port that holds tool calls is SG-32's question.
10. **Extend `#[derive(StrataSchema)]` or depend on `schemars`?** **Extend it.** It is already the tools' schema source; a second derive for answers would let the two drift, and the extension fixes tool manifests too.
11. **Model routing, failover, budgets, cost?** **Not here.** Routing and races are SG-24; cost needs prices, which are an application's. Usage is what Strata reports.

---

## Implementation Status

**PR 1 (models) built 2026-10-21 in both engines**, as specified above, with the API tables updated to what was built. **PR 2 (the agent, tool names, structured output, the `#[derive(StrataSchema)]` extension) built 2026-10-21 in both engines** on review decisions 4 to 10 as written (agreed 2026-10-21); the API tables are updated to it, and its divergences are after PR 1's.

**Where PR 1 diverged, or filled a gap, and why:**

- **Every 5xx is transient**, not the list the fault table first named: a provider's 501 or 505 is as much a server's fault as its 500, and Anthropic's 529 falls inside the range. `ModelFaults.IsTransientStatus` / `is_transient_status` is the one rule.
- **Rust's `ChatClient` takes no cancellation token**, and `complete` doesn't race the node's: when the token fires the engine drops the node's future, and dropping the call's `ModelCallScope` records it `Cancelled`, as a tool call's scope does (design/20). So `ChatError` has no `Cancelled` kind.
- **In .NET a call an abandoned node finishes after the run ended records nothing**: the engine abandons a node it gives up on rather than stopping it, so the call notices its token after `RunCompleted`, and events recorded after it are dropped (design/20 says the same of tool calls). A call cancelled from inside its node records `Cancelled` before the node completes; the tests cover both.
- **.NET `StrataChat` takes a `StrataChatOptions`** (the fault classifier, the agent's id for `strata.agent`) rather than separate arguments, and `ModelFaults` returns a `ModelFault` record that `ToNodeException` turns into the node failure, so a classifier and the default speak one type.
- **A client's own `NodeException` passes through** `StrataChat` as it is (the scope records `Failed` with `transient` or `fatal`): an application's client that already knows a budget is exhausted says so in Strata's terms.
- **Rust's usage type is the engine's `ModelUsage`**, re-exported, not a second `Usage` type, and `ChatUpdate` is an enum of pieces rather than one struct of optional deltas.
- **The OpenAI client chooses the token-limit field** (`MaxTokensField`): OpenAI's current models refuse `max_tokens` and want `max_completion_tokens`, many compatible servers know only `max_tokens`. `OpenAiOptions::new` uses the first, `compatible` and `ollama` the second.
- **The Anthropic client always puts a JSON-schema response format in the system prompt**, the blueprint's fallback; whether the Messages API takes a schema parameter wasn't verified, and PR 2's parse-and-repair is what makes an answer typed on any provider.
- **`gen_ai.response.finish_reasons` is a string in Rust** (`tracing` records no arrays) and an array in .NET.
- **The CLI's `tests/version.rs` counts six crates** (it pins the count so a new crate can't miss the version check).
- **Dependencies:** .NET `Microsoft.Extensions.AI.Abstractions` 10.10.1 and `System.ClientModel` 1.15.0, and the tests `Microsoft.Extensions.AI.OpenAI` 10.10.1 for the live tests; Rust `reqwest` 0.12 (`rustls-tls`, `json`), `futures-core`, `futures-util`, `chrono`, `serde`, `tokio`.

**How it was built:**

- **Engine:** .NET `Observability/ModelCallScope.cs` (`ModelCallInfo`, `ModelCallScope`), `RunEvent.cs` (`ModelCallResult`, `ModelUsage`, `ModelCallStarted`, `ModelCallCompleted`), `StrataTelemetry.cs` (three instruments). Rust `observability.rs`: the same types, two `RunEventKind` variants, three `metric_names`.
- **.NET `Datalium.Strata.AI`:** `ModelFaults.cs` (`ModelFault`, `IModelFaultClassifier`, `ModelCallException`, `ModelFaults`, `RetryAfter`, `RetryAfterHandler`), `StrataChat.cs`, `ModelNodes.cs`.
- **Rust `datalium-strata-ai`:** `types.rs`, `client.rs` (the trait, `ChatError`), `collect.rs` (`ChatUpdates`), `faults.rs`, `call.rs` (`complete`, `complete_with`), `node.rs` (`StateGraphAiExt`), `http.rs` (sending, error bodies, `EventParser`, the event stream), `openai.rs`, `anthropic.rs`.
- **Packaging:** `pack-smoke.sh` expects five packages and the smoke app runs a model node against a chat client of its own; `rust-ci.yml` publishes `datalium-strata-ai` last.

**Tests:**

- **.NET** `AI/ModelCallTests.cs` (31, with a scripted `IChatClient`, `AI/ScriptedChatClient.cs`) and `AI/ModelLiveTests.cs` (2, skipped without the variables). **Rust** `strata-ai/tests/model_calls.rs` (14), `providers.rs` (15: both clients against a raw local HTTP server, with server-sent-event fixtures under `tests/fixtures/`, a connection cut off mid-answer, 429, 401, 529 and a mid-stream `error` event), `telemetry.rs` (1), `live.rs` (2, skipped without the variables), and the event parser's unit test.
- **The live tests did not run**: no keys or local endpoint were available when this was built.
- **Mutation checks, in both engines** (each broken, the suites run, then restored): a 429 classed fatal fails 3 Rust and 2 .NET tests; dropping the retry hint fails the rate-limit test in each; not streaming the text fails the two streaming tests in each.
- **Totals when PR 1 landed:** .NET 915 passed and 135 skipped (Postgres and Redis weren't running, and the 2 live tests), plus the example's 1. Rust: `rust/` 977, `cli/` 82 and `examples/rust-agent` 1, also without Postgres or Redis. `cargo fmt`, `cargo +1.99.0 clippy --all-targets --all-features -D warnings`, `dotnet format --verify-no-changes` and `pack-smoke.sh` are clean.

**Where PR 2 diverged, or filled a gap, and why:**

- **A Rust tool's failure carries no kind**, so "a tool's own `NodeError` stands" has no Rust counterpart: a tool returns a `StrataError`. `AgentOptions::with_fault_classifier(Fn(&StrataError) -> Option<NodeError>)` reads it, and what it doesn't read fails the node fatally (`tool 'x' failed: …`), as anything nobody classifies does in .NET. And `#[strata_tool]` keeps only the tool's name of a function's error (`ToolExecutionFailure`, as before this PR), so such a failure says `the tool returned an error`; a hand-written tool returning `ToolCallFailed { message }` keeps its message. .NET's default reads a tool's exception with `ModelFaults.Classify` (an HTTP fault or a timeout is transient, with its hint).
- **Arguments that aren't a JSON object are refused before any dispatch**, in both engines, so they make no tool events: .NET's providers parse them into `FunctionCallContent.Arguments` and set its `Exception` when they can't, and Rust now does the same. **PR 1's Rust behaviour changed with it:** `ChatUpdates` and the OpenAI client no longer fail the whole call with a fatal `invalid_response` for such arguments, but keep the model's text as a `Value::String` in `Content::ToolCall.arguments`; the OpenAI client sends that text back as it came, and the Anthropic client sends `{}` (Messages takes an object only). Arguments the tool can't read (a `JsonException` / `ArgumentException` from the generated dispatch, `ToolArgumentParseFailure` from `#[strata_tool]`) are dispatched, so the registry records them `Failed`, and then refused back to the model.
- **The model is told in the agent's own words, the same in both engines**, naming the tool as the model called it: `tool 'k8s_nodes' was not found`, `tool 'secret.read' is not permitted here`, `denied: <reason>`, `the arguments for tool 'k8s_pods' aren't a JSON object`, `invalid arguments for tool 'k8s_pods': <the parser's words>`, and under `ReturnToModel` `tool 'k8s_logs' failed: <message>`, each as `{"error": "…"}` (.NET writes it with relaxed escaping so a quote comes out as serde writes it). A node's failure names the Strata name. The repair request for a text agent drops "with one JSON object that matches the schema".
- **`AgentToolCall.Result` is the engine's `ToolCallResult`**, not a new enum: `Refused` for every refusal of the table, `Failed` for a failure sent back under `ReturnToModel`. It is the agent's reading, so a call whose arguments the tool couldn't read is `Refused` there and `Failed` in the registry's `ToolCallCompleted`.
- **The answer's schema is made strict-compatible.** .NET asks `AIJsonUtilities.CreateJsonSchema` for `DisallowAdditionalProperties`, `RequireAllProperties` and `MoveDefaultKeywordToDescription`, what `Microsoft.Extensions.AI`'s own structured output asks for. Rust does the same to the derived schema itself: every object with properties closed and every property required, an optional one as `anyOf` itself or `null`, and `strict` off when the schema holds a map (OpenAI's strict mode can't express one) or isn't an object at the top. The format's name is the output type's name (its last path segment in Rust), made provider-safe.
- **.NET reads answers with `AIJsonUtilities.DefaultOptions` plus `RespectNullableAnnotations` and `RespectRequiredConstructorParameters`** unless `JsonOptions` says otherwise, so an answer missing a field it can't do without is sent back to be repaired, as serde refuses one in Rust.
- **`Agent<String>` is a text agent by its type** (`TypeId`), which needs `String: StrataSchema`: `strata-core` now implements `StrataSchema` for strings, booleans, numbers, `Option`, `Vec`, slices, arrays, sets, maps with string keys, `Box` and `Arc`.
- **The derive describes a field by its spelling for primitives and the standard containers** (so a `Vec` of a type nobody derived for is still an `array`) **and any other type by its own `StrataSchema`, through an autoref probe (`tools::__schema`, hidden) that falls back to `{"type": "object"}`**, so a struct with a field of a type that implements nothing still compiles and says what it said before. A generic type's parameters are bounded by `StrataSchema`, as serde's derive bounds them by `Deserialize`. Besides `rename` and `rename_all` (and their `deserialize = …` form), `skip` / `skip_deserializing` leave a field or variant out and `default` makes a field optional. Doc comments don't become descriptions.
- **Cancellation:** in .NET the node's own `OperationCanceledException` passes through every catch. In Rust the agent never returns an error once the node's token has fired: a tool call that ends `ToolCancelled` waits to be dropped, as PR 1's call does, since a failure returned then would race the engine's drop and fail the node instead of cancelling it.
- **Concurrent tool calls:** Rust runs a turn's calls as `buffered(n)` futures in the node's own task; the first failure in the model's order ends the turn and drops the rest. .NET starts each under a semaphore with a linked token; the first failure in the model's order cancels the rest and waits for them. `ToolConcurrency` below 1 is refused when the agent is made (.NET `ArgumentOutOfRangeException`, Rust a panic); `MaxTurns` 0 allows no tool turn. A call the model gave no id gets the registry's.
- **The package smoke app's pricing step is now an agent** whose scripted model calls the generated `price_order` tool, so the packed packages prove the agent and the declarations too.

**How PR 2 was built:**

- **Rust:** `strata-macros/src/schema.rs` (the derive, `RenameRule` as serde applies it); `strata-core/src/tools.rs` (`StrataSchema` impls, `__schema`); `strata-ai/src/declarations.rs` (`ToolDeclarations`), `agent.rs` (`Agent`, `AgentOptions`, `AgentResult`, `AgentToolCall`, `ToolFailures`, the strict schema, `unfenced`), `node.rs` (`add_agent_node`), `collect.rs` / `openai.rs` / `anthropic.rs` (arguments kept as text).
- **.NET `Datalium.Strata.AI`:** `ToolDeclarations.cs`, `StrataAgent.cs` (`StrataAgent`, `StrataAgentOptions`, `AgentResult`, `AgentToolCall`, `ToolFailures`), `AgentNodes.cs`.
- **Shared cases** `schema/fixtures/ai/agent-cases.json` (20): the loop, every refusal, the name map (collisions, 64 characters, an unknown name), `MaxTurns`, repairs, a model fault, concurrent calls, with the expected tool calls, tool results, repair requests, run events and outcome; run by `rust/strata-ai/tests/agent_cases.rs` and `AI/AgentCaseTests.cs`, each translating a failing tool into its own idiom (a .NET tool throws `NodeException.Transient`, a Rust one is read by the agent's classifier).

**PR 2's tests:**

- **Rust** `strata-core/tests/schema_derive.rs` (6: nested types, vectors, maps, enums, renames, generics, a field with no schema, a `Vec` tool parameter), `strata-ai/tests/agent_cases.rs` (the 20 cases), `agent.rs` (8: middleware sees the Strata name, the agent and the model's call id; a `#[strata_tool]` that can't read its arguments; sequential by default and concurrent with `ToolConcurrency`, in the model's order; every turn's declarations and the strict schema; streaming every turn; cancellation; the agent node), `telemetry.rs` (+1: the turns carry `strata.agent`, the tool span is the attempt's), `providers.rs` (+1: arguments that aren't an object), the unit tests of the name map, the fence and the strict schema, and `live.rs` (a tool loop to a typed answer per provider). **.NET** `AI/AgentCaseTests.cs` (the 20 cases), `AI/AgentTests.cs` (11: the same as Rust's, plus a tool's own `NodeException` keeping its hint while a retry starts the conversation over, and a tool's HTTP fault read as transient), and `ModelLiveTests.cs` (+1, the tool loop).
- **The live tests did not run**: no keys or local endpoint here, as for PR 1.
- **Mutation checks, in both engines** (each broken, the AI suites run, then restored): sending a non-refusal tool failure back to the model by default fails the shared cases in Rust and 4 .NET tests; dispatching under the model's name instead of the Strata name fails 3 Rust tests and 15 .NET ones; counting every turn, repairs included, against `MaxTurns` fails the shared cases in each (the case `repair turns don't count as tool turns` has a tool turn after its repair). Rust runs the 20 cases as one test, so a mutation fails it once.
- **Totals when PR 2 landed:** .NET 947 passed and 136 skipped (Postgres and Redis weren't running, and the 3 live tests), plus the example's 1. Rust: `rust/` 998, `cli/` 82 and `examples/rust-agent` 1, also without Postgres or Redis. `cargo fmt`, `cargo +1.99.0 clippy --all-targets --all-features -D warnings`, `dotnet format --verify-no-changes`, `pack-smoke.sh` and `cargo package --workspace --exclude strata-bench` (a fresh target directory) are clean.

**Since then (2026-10-24, from the .NET demos' move to 0.9):**

- **Only arguments the tool can't take are refused as such.** The .NET agent caught `JsonException` and `ArgumentException`. So a tool whose own code threw `ArgumentException` (a domain check) was refused back to the model as `invalid arguments …`, instead of failing the node, or going back as `tool 'x' failed: …` under `ReturnToModel`. The generated dispatch now throws `ToolArgumentsException`, and the agent catches only that. The model gets the problems in the words both engines use (`invalid arguments for tool 'lookup': argument 'orderId' must be a string`), not the parser's, and the shared case now pins them exactly. See design/20, "Arguments a tool can't read".
- **`ConfigureTurn` / `with_configure_turn`** changes one turn's options. It was chosen over a narrower "tool mode for the first turn" option, which it covers. It runs after the agent sets the tools and the response format, so it can change those too, at the caller's risk.
- **Tests:** .NET `AgentTests.cs` +3 (a tool's own `ArgumentException` fails the node, and goes back as a failure under `ReturnToModel`; `ConfigureTurn`), Rust `agent.rs` +1 (`with_configure_turn`). The argument-refusal tests in both engines now check the exact words.

## Not built (deliberately out of scope)

- **Model routing, failover across models, model races** (SG-24).
- **Cost and budgets.** Usage is reported; prices are an application's.
- **The OpenAI Responses API, embeddings, images, audio** in the Rust clients.
- **A tool-call chunk type in the stream.** Observers see tool calls as run events; the stream carries text.
- **Persisting conversations** (review decision 9).
- **Prompt templates.**
- **In `#[derive(StrataSchema)]`: descriptions from doc comments, `#[serde(flatten)]` (a flattened field is described as a property of its own), and enums whose variants have fields (refused, as every enum was before).
- **A history port that holds tool calls** (review decision 9): the agent's conversation lives for one attempt.
