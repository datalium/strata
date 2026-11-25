# Datalium Strata: An MCP Client, Strata's Own

An MCP server's tools can't be used as Strata tools today. LangChain has MCP adapters, and Strata has nothing. People evaluating an agent framework check for MCP early, so this is the most visible gap in the LangChain comparison (strata-management, `COMPARISON-LANGCHAIN-LANGGRAPH.md`).

This document specifies **STR-92**: an MCP client in both engines that connects to a server, lists its tools and registers them in a `ToolRegistry`, where they behave like `[StrataTool]` / `#[strata_tool]` tools. It is a **client only**, and for **tools only**. Strata as an MCP server, resources, prompts and the client features (sampling, roots, elicitation) are not built (see "Not built").

**Strata implements the MCP spec itself.** On 2026-11-22 the official SDKs, `ModelContextProtocol.Core` 2.4.0 (C#) and `rmcp` 3.11.0 (Rust), were evaluated and not chosen. Strata takes no outside MCP dependency, and its own implementation behaves the same in both engines by construction. The cost is following each spec revision ourselves. There have been five so far (2024-11-05, 2025-03-26, 2025-06-18, 2025-11-25, 2026-07-28), and the last one changed how a client and server open a connection.

Both engines get the capability in one change (strict parity). The APIs are idiomatic in each language, not identical. The tasks are STR-151 (.NET) and STR-152 (Rust), and they land in one PR.

It was written as a blueprint before implementation. "Review decisions" lists the questions for review and their answers, and "Implementation Status" at the end records where the build diverged. Read it before trusting a snippet here.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Where it lives | Two new packages in `datalium/strata`, released at Strata's one version: **`Datalium.Strata.Mcp`** (`dotnet/src/Datalium.Strata.Mcp`) and **`datalium-strata-mcp`** (`rust/strata-mcp`). Each depends only on its engine, so the engine doesn't depend on MCP. |
| Outside dependencies | **None for MCP.** .NET: the shared framework only (`System.Text.Json`, `HttpClient`, `Process`). Rust: crates the workspace already uses (`tokio`, `serde_json`, `reqwest` 0.13 with rustls, `tracing`, `metrics`), plus `libc` on Unix to stop a server's process group. |
| Protocol versions | **Modern** `2026-07-28` (stateless, version in every request's `_meta`) and **legacy** `2025-11-25`, `2025-06-18`, `2025-03-26` (the `initialize` handshake). `2024-11-05` on stdio only: its HTTP+SSE transport is deprecated and not built. |
| Which era a server speaks | Found once per connection, as the spec says. stdio: probe with `server/discover`, and fall back to `initialize` on any error that isn't a recognized modern one, or no answer in 5 s. HTTP: send `server/discover`, and fall back on a `4xx` whose body isn't a recognized modern error. |
| Transports | **stdio** (a child process), **Streamable HTTP** (both eras), and **streams** (any read/write pair, with stdio's framing: a socket, or an in-memory pipe in tests). |
| Authorization | **Headers, and a token hook** called before each HTTP request, which can refresh the token. stdio servers take credentials from their environment, as the spec says. Full OAuth 2.1 is not built. |
| What Strata sees | An MCP tool is a Strata tool: .NET, an `IStrataToolProvider` (`client.ToolProvider(…)`); Rust, one `StrataTool` per MCP tool (`client.tool_set(…)`). `Register` / `register_mcp`, `Subset`, middleware, the `strata.tool` span and the run events apply unchanged. |
| Which tools | All of a server's tools, or a named subset (`Include`). An optional `Prefix` per server (`"github."`), because the registry refuses duplicate names. |
| Schemas | `inputSchema` becomes the tool's `parameters` unchanged. No client-side validation of arguments: the server validates them, and its error reaches the model. A `$ref` to a network URI is never fetched. |
| Results | `structuredContent`, when present, as JSON. Otherwise the text blocks, joined with blank lines. Images, audio and binary resources become a short placeholder, not base64. See "Results". |
| A tool's own error (`isError: true`) | **Sent back to the model** as `{"error": "<text>"}`, the shape the agent loop already uses for refusals, and the node goes on. Review question 1. |
| Protocol and transport failures | A failure of the call: .NET an `McpException` (or the `HttpRequestException` / `IOException` underneath), Rust `StrataError::ToolCallFailed { tool, message }`. The agent loop handles them like any tool failure (design/44). |
| Cancellation | When the call's token fires (Rust: or its future is dropped): `notifications/cancelled` on stdio and legacy HTTP, and closing the response stream on modern HTTP. The result is the registry's usual cancellation. |
| Telemetry | One client span per MCP request (`tools/call <tool>`, `tools/list`, `server/discover`, `initialize`), with the OpenTelemetry MCP attributes, a child of the `strata.tool` span for calls. One histogram, `mcp.client.operation.duration`. Review question 2. |
| Trace context to the server | `traceparent` / `tracestate` in each request's `_meta`. .NET from `Activity.Current`. Rust from a hook, since the Rust engine has no OpenTelemetry context of its own. Review question 3. |
| Lifetime | Explicit. .NET `await using`; Rust `close().await`, and a forced stop when the last handle is dropped. A stdio server that exits is started again on the next call. Review question 4. |
| Engine change | **None.** Rust's `StrataTool::name()` stays `&'static str`, as 0.9.0's API promise requires. `datalium-strata-mcp` interns each tool name it learns, leaking each distinct name once. See "Tool names in Rust". |
| Wire format | **No change.** Nothing MCP is written to a checkpoint, a graph definition or a history record. |
| Not built | Strata as an MCP server; resources, prompts, completion; sampling, roots, elicitation (and so MRTR input); subscriptions and list-changed notifications; the tasks extension; progress; full OAuth 2.1; the HTTP+SSE transport; validation of `structuredContent` against `outputSchema`; settings-based registration in `AddStrata()`. |

---

## Usage

```csharp
// A local server over stdio, and a remote one over HTTP with a token that may be refreshed.
await using var files = await McpClient.ConnectAsync(
    McpServer.Stdio("npx", ["-y", "@modelcontextprotocol/server-filesystem", "/data"]));

await using var github = await McpClient.ConnectAsync(
    McpServer.Http(new Uri("https://api.example.com/mcp")) with
    {
        AccessToken = ct => tokens.GetAsync("github", ct),        // called before each request
    });

var tools = new ToolRegistry()
    .Register(files.ToolProvider(new() { Include = ["read_file", "list_directory"] }))
    .Register(github.ToolProvider(new() { Prefix = "github." }))   // github.create_issue, …
    .Use(new AuditMiddleware(auditLog));                          // runs for MCP tools too

var agent = new StrataAgent<string>(chat, tools.Subset("read_file", "github.create_issue"), new() { Id = "triage" });
```

```rust
let files = McpClient::connect(
    McpServer::stdio("npx").args(["-y", "@modelcontextprotocol/server-filesystem", "/data"]),
)
.await?;

let github = McpClient::connect(
    McpServer::http("https://api.example.com/mcp")?
        .access_token(move || { let tokens = tokens.clone(); async move { tokens.get("github").await } }),
)
.await?;

let mut tools = ToolRegistry::new();
tools
    .register_mcp(files.tool_set(McpToolOptions::new().include(["read_file", "list_directory"]))?)?
    .register_mcp(github.tool_set(McpToolOptions::new().prefix("github."))?)?
    .use_middleware(Audit::new(log));

// … at shutdown
files.close().await;
github.close().await;
```

`register_mcp` comes from an extension trait in `datalium-strata-mcp` (`McpRegistryExt`), so `strata-core` doesn't know MCP exists.

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Mcp;

/// How to reach one MCP server. Records, so `with` adjusts one.
public abstract record McpServer
{
    public static StdioMcpServer Stdio(string command, IEnumerable<string>? arguments = null);
    public static HttpMcpServer Http(Uri endpoint);
    public static StreamMcpServer Streams(Stream fromServer, Stream toServer);
}

public sealed record StdioMcpServer : McpServer
{
    public string Command { get; init; }
    public IReadOnlyList<string> Arguments { get; init; }
    public string? WorkingDirectory { get; init; }
    public IReadOnlyDictionary<string, string?> Environment { get; init; }  // added to, or removed from, the inherited one
    public bool InheritEnvironment { get; init; } = true;
    public TimeSpan ShutdownTimeout { get; init; } = TimeSpan.FromSeconds(5);
}

public sealed record HttpMcpServer : McpServer
{
    public Uri Endpoint { get; init; }
    public IReadOnlyDictionary<string, string> Headers { get; init; }
    public Func<CancellationToken, ValueTask<string?>>? AccessToken { get; init; }   // "Authorization: Bearer <token>"
    public HttpClient? HttpClient { get; init; }                                      // else one of the client's own
}

public sealed record McpClientOptions
{
    public string ClientName { get; init; } = "datalium-strata";
    public string ClientVersion { get; init; }                     // Strata's version
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(30);   // discover, initialize, tools/list
    public TimeSpan? CallTimeout { get; init; }                    // tools/call; none by default
    public TimeSpan ProbeTimeout { get; init; } = TimeSpan.FromSeconds(5);      // the stdio era probe
    public bool RestartOnExit { get; init; } = true;               // stdio
    public int MaxMessageBytes { get; init; } = 16 * 1024 * 1024;
    public Action<McpLogEntry>? Log { get; init; }                 // server stderr, rejected tools, ignored messages
}

public sealed class McpClient : IAsyncDisposable
{
    public static Task<McpClient> ConnectAsync(McpServer server, McpClientOptions? options = null,
        CancellationToken cancellationToken = default);

    public string ProtocolVersion { get; }                 // "2026-07-28", "2025-11-25", …
    public McpImplementation? ServerInfo { get; }          // self-reported, for display only
    public string? Instructions { get; }
    public IReadOnlyList<McpTool> Tools { get; }           // as listed at connect, in the server's order

    public IStrataToolProvider ToolProvider(McpToolOptions? options = null);   // throws ToolNotFoundException for an unknown Include
    public Task<McpToolResult> CallToolAsync(string name, JsonElement arguments,
        CancellationToken cancellationToken = default);   // the raw result, outside any registry

    public ValueTask DisposeAsync();                       // stdio: close stdin, wait, then stop the process tree
}

public sealed record McpToolOptions
{
    public string? Prefix { get; init; }
    public IReadOnlyList<string>? Include { get; init; }   // the server's names, before the prefix
}

public sealed record McpTool(string Name, string? Title, string? Description, JsonElement InputSchema,
    JsonElement? OutputSchema, JsonElement? Annotations);  // annotations are untrusted

public sealed record McpToolResult(IReadOnlyList<JsonElement> Content, JsonElement? StructuredContent, bool IsError);

public class McpException(string message, int? code = null) : Exception(message)
{
    public int? Code { get; }                              // the JSON-RPC error code, when there was one
}
```

### Rust

```rust
pub struct McpServer { /* private */ }

impl McpServer {
    pub fn stdio(command: impl Into<OsString>) -> StdioServer;                 // .args, .env, .env_remove, .current_dir,
                                                                               // .inherit_env(bool), .shutdown_timeout
    pub fn http(endpoint: impl IntoUrl) -> Result<HttpServer, McpError>;       // .header, .access_token, .client(reqwest::Client)
    pub fn streams(from_server: impl AsyncRead, to_server: impl AsyncWrite) -> StreamServer;
}

#[derive(Clone, Default)]
pub struct McpClientOptions { /* client_name, client_version, request_timeout, call_timeout, probe_timeout,
                                 restart_on_exit, max_message_bytes, log, trace_context: with_* builders */ }

#[derive(Clone)]
pub struct McpClient { /* Arc'd connection */ }

impl McpClient {
    pub async fn connect(server: impl Into<McpServer>) -> Result<McpClient, McpError>;
    pub async fn connect_with(server: impl Into<McpServer>, options: McpClientOptions) -> Result<McpClient, McpError>;

    pub fn protocol_version(&self) -> &str;
    pub fn server_info(&self) -> Option<&McpImplementation>;
    pub fn instructions(&self) -> Option<&str>;
    pub fn tools(&self) -> &[McpTool];

    pub fn tool_set(&self, options: McpToolOptions) -> Result<McpToolSet, StrataError>;   // Err(ToolNotFound) for an unknown include
    pub async fn call_tool(&self, name: &str, arguments: serde_json::Value) -> Result<McpToolResult, McpError>;
    pub async fn close(&self);
}

pub trait McpRegistryExt {
    /// Registers every tool of the set, or none: Err(DuplicateTool) if any name is taken.
    fn register_mcp(&mut self, tools: McpToolSet) -> Result<&mut Self, StrataError>;
}
impl McpRegistryExt for ToolRegistry { /* … */ }

#[derive(Debug)]
pub enum McpError { Connect(String), Protocol { code: i64, message: String }, Transport(String), Closed, /* non_exhaustive */ }
```

- **`McpServer` describes, `McpClient` connects.** Connecting does the era probe and lists the tools, so a server that is down or speaks no version Strata knows fails at startup, not on the first call.
- **`McpError` is Rust's error for connecting and for `call_tool`.** Through the registry, a tool's failure is a `StrataError`, as for every tool.
- **`register_mcp` registers all of the set or nothing**, as .NET's `Register` does for one provider. Rust's `try_register` adds one tool at a time, so `register_mcp` checks every name first.
- **The tools keep the connection alive.** They share the client's connection. After `DisposeAsync` / `close`, a call fails with "connection closed".

---

## The protocol

### Messages

JSON-RPC 2.0, UTF-8. Request ids are integers, increasing per connection, so one connection can carry many calls at once (an agent with `ToolConcurrency` above 1). Every request carries `_meta`:

| Key | Modern | Legacy |
|---|---|---|
| `io.modelcontextprotocol/protocolVersion` | the chosen version | not sent (the version is the session's) |
| `io.modelcontextprotocol/clientCapabilities` | `{}`: Strata declares none | not sent (`initialize` declared `{}`) |
| `io.modelcontextprotocol/clientInfo` | `{ name, version }` | not sent (in `initialize`) |
| `traceparent`, `tracestate` | when known (see "Telemetry") | when known |

- **A result's `resultType`:** `complete`, or absent (a legacy server), is the result. `input_required` asks for input Strata doesn't provide, which a server shouldn't do to a client that declared no capabilities. It fails the call with a message naming what was asked for. Any other value is invalid, as the spec says.
- **Errors from the server** keep their code and message: .NET `McpException.Code`, Rust `McpError::Protocol { code, .. }` or, through the registry, a `ToolCallFailed` message `mcp error -32602: Unknown tool: x`. `-32020` to `-32022` are recognized as modern errors, for era detection.
- **Requests from the server** (legacy only: modern servers must not send any) get an answer, so the server doesn't wait forever: `ping` gets an empty result, and anything else (`sampling/createMessage`, `roots/list`, `elicitation/create`) gets `-32601` (method not found).
- **Notifications from the server** (`notifications/progress`, `notifications/message`, list-changed) are ignored. They are reported to `Log` at debug level.
- **Limits:** a message bigger than `MaxMessageBytes` (16 MiB) fails the request and, on stdio, the connection, because the stream can't be resynchronized. A line that isn't JSON fails the connection too: on stdio, a server must write nothing else to stdout.

### Opening a connection

**Modern**, `2026-07-28`. There's no handshake. Strata sends `server/discover` first, on both transports: the spec recommends it on stdio, it gives the server's identity and instructions, and it makes the era check deterministic.

- A `DiscoverResult` whose `supportedVersions` holds `2026-07-28`: modern.
- `UnsupportedProtocolVersionError` (`-32022`): modern, but not this version. If its `supported` list has a legacy version Strata knows, Strata uses `initialize` with it. Otherwise connecting fails, naming both lists.

**Legacy**, when the probe says so. Strata sends `initialize` with `protocolVersion` `2025-11-25`, capabilities `{}` and `clientInfo`.

- The server answers with its version. If it is one Strata knows (`2025-11-25`, `2025-06-18`, `2025-03-26`, or on stdio `2024-11-05`), Strata sends `notifications/initialized` and goes on. Otherwise connecting fails, as the spec says ("the client SHOULD disconnect").

**The era is cached for the connection.** A stdio server that is restarted is probed again, since the new process may be a different version.

| The probe gets | stdio | HTTP |
|---|---|---|
| A `DiscoverResult` | modern | modern |
| A recognized modern error (`-32020`–`-32022`) | modern: retry with a version from `supported`, or fail | modern: likewise |
| Any other error | legacy: `initialize` | legacy: a `4xx` without a recognized modern body (what the spec describes), or a JSON-RPC error that isn't a modern one, even in a `200` (what some legacy servers send for a method they don't know) |
| Nothing within `ProbeTimeout` (5 s) | legacy: `initialize` | (HTTP times out under `RequestTimeout`, and fails) |
| `2xx` / `5xx` / connection refused | — | `5xx` and connection failures fail connecting: they say nothing about the era |

Then Strata lists the tools (see "Tools").

### stdio

- **Starting the server:** the command, its arguments, its working directory, and the environment: inherited by default, with `Environment` / `.env` entries added or removed (review question 5).
- **Windows:** a command without an extension is resolved through `PATH` and `PATHEXT`, as a shell would. Neither engine's process API does that: both look for `.exe` only, and `npx` is `npx.cmd`. A `.cmd` / `.bat` command is run through `cmd.exe /c`, with its arguments quoted for `cmd`. The quoting is tested, because batch-file argument quoting is a known source of injection bugs.
- **Framing:** one message per line, `\n`-terminated, no embedded newlines (serialization never emits one).
- **stderr** is read all the time, so a chatty server can't fill the pipe and stall. Each line goes to `Log` at debug level and is otherwise dropped. A server's stderr can hold secrets, so it isn't put on spans.
- **Shutdown** (`DisposeAsync` / `close`), as the spec says:
  1. Close the server's stdin.
  2. Wait up to `ShutdownTimeout` (5 s) for it to exit.
  3. Stop it, and the processes it started: `npx` runs the server as a child, and stopping only `npx` would leave it running. .NET: `Process.Kill(entireProcessTree: true)`. Rust: on Unix, the server starts in its own process group, which gets `SIGTERM`, then `SIGKILL` a second later. On Windows, `taskkill /T /F`.
  - **Rust's `Drop`** can't wait. When the last handle is dropped without `close`, the process tree is stopped at once.
- **A server that exits** fails the calls in flight with a transport error. With `RestartOnExit` (the default), the next call starts it again, probes it again and sends the call. The tool list isn't read again: a tool that disappeared gets the server's own "unknown tool" error. If a start fails, the call fails, and the next call tries again.

### Streamable HTTP

**Every request** is a POST to the endpoint, with:
- `Content-Type: application/json` and `Accept: application/json, text/event-stream`;
- `MCP-Protocol-Version` with the version in use, in both eras;
- `Authorization: Bearer <token>` when `AccessToken` returns one, then the configured `Headers`.

The response is either one JSON object, or an SSE stream ending with the response. Both are supported.

**SSE:**
- Each engine has a parser of its own, of about a hundred lines. (The blueprint had .NET use `System.Net.ServerSentEvents.SseParser`, which doesn't follow the WHATWG rules the fixture pins. See "Implementation Status".)
- Both pass the same fixture cases (`schema/fixtures/mcp/sse.json`): `data:` lines joined, comments (`:`) and unknown fields ignored, `\r\n` / `\n` / `\r` line endings.
- Events before the response are notifications (ignored) or, on a legacy server, requests, which are answered with a POST.

**Modern only:**
- `Mcp-Method` on every request, and `Mcp-Name` for `tools/call`. The value is Base64-encoded in the spec's `=?base64?…?=` form when it isn't plain visible ASCII, or when it looks like that form already.
- **`x-mcp-header`.** The spec requires HTTP clients to support it. When a tool's `inputSchema` marks a property with `x-mcp-header`, its value goes in an `Mcp-Param-{name}` header, encoded the same way: a string as is, an integer in decimal, a boolean as `true` / `false`.
  - A tool whose annotations break the spec's rules is left out of the list, with a warning to `Log`, as the spec requires. The rules: a token name; case-insensitively unique; on a string, integer or boolean property; reachable from the root through `properties` only.
  - A `HeaderMismatch` (`-32020`) answer lists the tools again once and retries the call, as the spec recommends.
- **Cancellation** closes the response stream: the request is aborted.

**Legacy only:**
- **Sessions.** The `Mcp-Session-Id` from the `initialize` response is sent on every request.
  - A `404` with a session id means the session ended: Strata initializes again once and retries the request.
  - `close` sends a `DELETE` with the session id, and a `405` answer is fine.
- **No GET stream.** Strata needs nothing from the server outside a request's own response.
- **Cancellation** POSTs `notifications/cancelled` and then aborts the request. Closing a connection doesn't cancel in legacy HTTP.

**The HTTP client:** .NET uses the given `HttpClient` (for `IHttpClientFactory`), else one of the client's own, disposed with it. Rust uses the given `reqwest::Client`, else a new one. Neither follows redirects to another origin with the token.

### Authorization

- **The token hook** runs before each HTTP request, so a token that expires is refreshed by the code that owns it: a secret store, client credentials, or StrataOps' Tool Gateway. A hook that throws fails the request, and nothing is retried.
- **A `401` / `403`** fails the call with the status. Strata doesn't read `WWW-Authenticate`, discover an authorization server or start a sign-in. That is full OAuth 2.1, which assumes a person at a browser. Strata agents usually run unattended in services. See "Not built".
- **Tokens appear nowhere else:** not in logs, spans, metrics or errors. A configured header whose name is `Authorization` or ends in `-Key` / `-Token` is redacted from error messages.

### Cancellation and timeouts

- **The call's token** (.NET the `CancellationToken`, Rust `ctx.cancellation()` or dropping the call's future) cancels the request:
  - on stdio, `notifications/cancelled` with the request id and the reason `cancelled by the caller`;
  - on modern HTTP, aborting the request;
  - on legacy HTTP, both.
- **A late response** to a cancelled request is ignored.
- **The outcome** is the registry's usual cancellation: .NET `OperationCanceledException` for the call's token, Rust `ToolCancelled`.
- **`RequestTimeout`** (30 s) bounds the requests Strata makes on its own: `server/discover`, `initialize`, `tools/list`.
- **`CallTimeout`** (none by default) bounds a `tools/call`. A tool call is already bounded by the node's timeout and the run's deadline, through its token (design/20). A per-tool timeout is a middleware's job (design/23).
- **A timeout cancels the request** as above, and fails the call: .NET a `TimeoutException`, Rust `ToolCallFailed` "timed out after …". It is a failure, not a cancellation.

---

## Tools

### Listing

`tools/list` at connect, following `nextCursor` until it is absent. A cursor seen twice, or more than 1,000 pages, fails connecting rather than looping forever. `ttlMs` / `cacheScope` are read and not used: the list is read once, at connect.

A server that lists a name twice keeps the first, with a warning to `Log`. The spec only says names *should* be unique, and the registry would refuse the whole server.

### From an MCP tool to a Strata tool

| Strata | From the MCP tool |
|---|---|
| Name | `Prefix` + `name`. The call sends the server's `name`. |
| Description | `description`, else `title`, else empty. |
| Parameters | `inputSchema`, unchanged. A `$ref` to a network URI is never fetched, as the spec requires: Strata never dereferences anything. |
| Not mapped | `title` (beyond the fallback), `icons`, `outputSchema` and `annotations` stay on `McpTool`, for code that wants them: a middleware that only allows `readOnlyHint` tools, for example. **They are untrusted**, as the spec warns. |

- **Names reach the model as they are.** The agent's renderer already maps names to what a provider accepts (design/44, "Tool names"), so `github.create_issue` becomes `github_create_issue` for OpenAI.
- **Arguments aren't validated by Strata.** The server must validate them (spec, "Security"), and its input errors come back as tool errors, which reach the model. Validating against JSON Schema 2020-12 on the client would need a validator in each engine, a dependency or a project of its own, and the server would still decide. design/23's middleware can validate when an application wants it.
- **`Include`** names the server's tools, before the prefix. A name the server didn't list throws `ToolNotFoundException` / returns `ToolNotFound` when the provider or set is made, as `Subset` does.

### Calling

The registry resolves the name, starts the `strata.tool` span, runs the middleware and calls the tool (design/20, design/23). The tool sends `tools/call` with the server's name and the arguments as JSON, and waits.

### Results

A Strata tool returns text, and the agent sends that text to the model. An MCP result becomes text like this, in order:

1. **`structuredContent` present:** its JSON, compact. It is the machine-readable result, and the spec asks servers to repeat it as text anyway.
2. **Otherwise, the content blocks in order**, joined by a blank line:
   - `text` as is;
   - an embedded `resource` with `text`: that text;
   - a `resource_link`: `[resource: <name> <uri>]`;
   - `image`, `audio`, or an embedded binary `resource`: `[<mimeType>, <size> KB, not shown]`.
3. **No content at all:** an empty string.

- **Why placeholders:** base64 in the model's context costs tokens and tells the model nothing. Strata's tool result is text, so there is no way to hand the model an image through it today. `CallToolAsync` / `call_tool` return the whole result, for code that needs the bytes.
- **The mapping is pinned by shared fixtures** (`schema/fixtures/mcp/tool-results.json`), so both engines produce the same text.

**A tool's own error (`isError: true`)** is the server saying "this call failed, and here is why", for the model to correct itself, and the spec asks clients to pass it to the model. So the text is returned as **`{"error": "<the text above>"}`**:
- this is the shape the agent loop already gives the model for refusals (design/44), so the model sees one shape for every error;
- the node goes on;
- the registry records the call as `succeeded`, since the tool answered;
- the MCP span records `error.type` = `tool_error`, as the OpenTelemetry conventions say.

Review question 1.

### Errors

| What happened | .NET | Rust (through the registry) | In the agent loop (design/44) |
|---|---|---|---|
| `isError: true` | returned as `{"error": "…"}` | `Ok("{\"error\": \"…\"}")` | the model gets it; the node goes on |
| JSON-RPC error (unknown tool `-32602`, a server error) | `McpException` with `Code` | `ToolCallFailed { tool, message: "mcp error -32602: …" }` | a tool failure: `FailNode` by default, or sent to the model with `ReturnToModel` |
| `input_required` | `McpException` | `ToolCallFailed` | as above |
| Transport: the process exited, a connection failed, HTTP `5xx` | `IOException` / `HttpRequestException` | `ToolCallFailed` | as above. In .NET the default fault classifier makes an HTTP fault transient. In Rust a tool error carries no kind, so `datalium-strata-mcp` exports `mcp_fault_classifier` for `with_fault_classifier`, which makes these transient |
| HTTP `401` / `403` | `HttpRequestException` with the status | `ToolCallFailed` "unauthorized (401)" | fatal |
| Timeout (`CallTimeout`) | `TimeoutException` | `ToolCallFailed` "timed out after …" | transient |
| The call's token fired | `OperationCanceledException` | `ToolCancelled` | the node ends `Cancelled` |
| The connection was closed (`DisposeAsync` / `close`) | `ObjectDisposedException` | `ToolCallFailed` "connection closed" | fatal |

`StrataError::ToolCallFailed` was added for a middleware's own failures (design/23). This change widens its doc comment to "a tool call failed for a reason with a message", so a tool that knows why it failed can say so. The variant itself doesn't change.

---

## Telemetry

**The registry's telemetry applies unchanged:** the `strata.tool` span, `strata.tool.requests`, `strata.tool.duration`, `ToolCallStarted` / `ToolCallCompleted`. An MCP tool is a Strata tool.

**Each MCP request also gets a client span,** following the OpenTelemetry MCP conventions (Development status):

| | Value |
|---|---|
| Name | `{mcp.method.name} {gen_ai.tool.name}` for `tools/call` (`tools/call create_issue`, the server's name), else `{mcp.method.name}` (`tools/list`, `server/discover`, `initialize`) |
| Kind | Client |
| Parent | Whatever is current: the `strata.tool` span for a call made through the registry |
| Attributes | `mcp.method.name`, `mcp.protocol.version`, `jsonrpc.request.id`, `network.transport` (`pipe` for stdio and streams, `tcp` for HTTP), `server.address` / `server.port` (HTTP), `mcp.session.id` (legacy HTTP), and for a call `gen_ai.tool.name` and `gen_ai.operation.name` = `execute_tool` |
| On failure | `error.type`: the JSON-RPC code as a string, else the HTTP status, else `timeout` / `transport`, and `tool_error` for `isError`. `rpc.status_code` when there was a JSON-RPC code. Status `Error` with the message, except for `tool_error`. A cancellation isn't an error |

**One metric:** `mcp.client.operation.duration` (histogram, `s`), with `mcp.method.name`, `gen_ai.tool.name` (bounded by what the server listed), `mcp.protocol.version`, `network.transport` and `error.type`. No session ids, request ids, arguments or results on metrics.

**Arguments and results** are on no span and no event, as for every Strata tool (design/20). The conventions' opt-in `gen_ai.tool.call.arguments` / `.result` aren't recorded.

**Where it is reported:**
- **.NET:** an `ActivitySource` and a `Meter` named `Datalium.Strata.Mcp`. `AddStrata()`'s OpenTelemetry registration adds the name as a string, so `Datalium.Strata.Hosting` doesn't reference the MCP package. A source nobody created is harmless.
- **Rust:** `tracing` spans with target `datalium_strata_mcp`, and the metric through `metrics`, as the engine does.

**Trace context to the server.** The conventions ask clients to put `traceparent` / `tracestate` in `_meta`, so a server's spans join the caller's trace:
- **.NET** writes them from `Activity.Current` through `DistributedContextPropagator.Current`. On HTTP, `HttpClient` also adds the `traceparent` header, as it always does.
- **The Rust engine** has `tracing` spans but no OpenTelemetry context: there is no W3C trace id to read without `tracing-opentelemetry`, which Strata doesn't depend on. So `McpClientOptions::with_trace_context(f)` takes a function that returns the current `traceparent` / `tracestate`, and an application that uses `tracing-opentelemetry` writes it in a few lines. Without one, nothing is sent.

Review question 3.

**Why a span of its own, when the conventions prefer attributes on the outer `execute_tool` span** (review question 2):
- Rust's `tracing` can only record fields a span declared when it was created. Putting `mcp.*` attributes on `strata.tool` would make the engine declare MCP's names.
- MCP requests made outside a tool call (`tools/list`, `server/discover`, `initialize`, a restart's probe) need a span anyway.
- The child span measures the protocol round trip apart from the middleware time inside `strata.tool`.

---

## Tool names in Rust

Rust's `StrataTool::name()` returns `&'static str`, and `ToolRegistry` keys on it. An MCP tool's name is known only once the server has been listed, so `datalium-strata-mcp` makes it `'static` itself:

- **Each distinct name is leaked once** (`names.rs`). The first tool set with the name `github.create_issue` leaks it. Every later one, from a reconnect, a restarted server or a second client, gets the same `&'static str` from a process-wide set.
- **What it holds is bounded by the names, not the connections.** It's one string per distinct prefixed name the process ever sees, usually a few hundred bytes per server. A server whose tool names change on every listing would grow it with each new name. That belongs on the Known limitations page, with STR-92's docs.
- **The engine doesn't change**, so 0.9.0's promise holds: no public API changes, and `names()` still yields `&'static str`.
- **.NET needs nothing:** its names were always `string`s.

**Why not change the trait:** the first build (2026-11-22) changed `name()` to `&str` and keyed the registry on `Arc<str>`. Implementors still compiled, but callers lost the `'static`: code that kept a name from `names()` after its registry was gone stopped compiling. From `0.9.0`, no release breaks a public API (`design/36`), so that change was replaced with interning on 2026-11-25, before it merged. A later major version can still make it.

---

## Security

- **An MCP server is code Strata runs (stdio) or calls (HTTP) on the application's behalf.** Its tool names, descriptions and results are untrusted text that reaches the model. A malicious description can try to steer the model ("tool poisoning"). Subsets (`Include`, `Subset`), middleware and StrataOps' Tool Gateway are where an application limits what a server's tools may do. Strata passes text through and doesn't sanitize it.
- **stdio servers inherit the environment by default** (review question 5), including any secrets in it. `InheritEnvironment = false` / `.inherit_env(false)` starts one with only the variables given, plus what the OS needs to start a process (`PATH`, and on Windows `SystemRoot`).
- **Nothing a server returns is fetched:** no `$ref`, no icon, no `resource_link` URI.
- **`x-mcp-header` values** are encoded as the spec says, so a value can't inject a header.
- **Size limits** (`MaxMessageBytes`) bound what a server can make Strata buffer.

---

## Wire format

**None.** Nothing MCP is in checkpoints, graph definitions or history records. No `schema_version` bump, no DDL change.

**New shared test fixtures** in `schema/fixtures/mcp/`, like `schema/fixtures/tools/argument-problems.json`. They are tests both engines must pass, not a format Strata writes:

| Fixture | Pins |
|---|---|
| `tool-results.json` | A `CallToolResult` → the text Strata returns, including `isError`, `structuredContent` and each content type |
| `x-mcp-header.json` | Tool schemas → accepted or rejected (with the reason), and arguments → the `Mcp-Param-*` headers |
| `header-values.json` | Values → their header encoding, including the Base64 sentinel |
| `sse.json` | Byte streams → events |
| `era-detection.json` | Probe answers → modern, legacy or failure, per transport |

---

## Packaging

- **.NET:** the sixth package. It goes in `Datalium.Strata.sln` and `Package.props`, in `pack-smoke.sh` (packed and checked), in the `PackageSmoke` app (which connects to the stdio fixture server through the package alone), and in design/36.
- **Rust:** the seventh published crate. It goes in the workspace members, and in `[workspace.dependencies]` pinned at `=<version>` (which `versions.sh` checks). `cargo package --workspace --exclude strata-bench` builds it from its package. Cargo features `stdio` and `http`, both on by default, so a build that only uses one doesn't compile the other (and `http` brings `reqwest`).
- **Docs:** `CHANGELOG.md`, the stratamesh.dev page and the Known limitations page (STR-92's other tasks).

---

## Migration

- **.NET:** nothing changes for existing code. The package is new, and the only engine-adjacent change is the source name `AddStrata()` registers.
- **Rust:**
  - Nothing in the engine changes: `StrataTool` and `ToolRegistry` are as they were.
  - `ToolCallFailed`'s meaning widens (doc only).
- **Downstream:** demos and StrataOps compile unchanged. StrataOps could later use an MCP server's tools, but that is a separate decision, after 0.9.0.

---

## Tests

**Fixture servers.** No network, no live LLM calls, and no official SDK:
- **In memory** (both engines): a scripted server behind the streams transport. It speaks modern, legacy (each version), dual-era, or misbehaves: no answer to the probe, a line that isn't JSON, a response too big, an exit in the middle of a call, a slow tool, requests from the server. Most tests use it.
- **stdio:** a small server process. .NET: a console project in `dotnet/tests/`, not packed. Rust: an example target of `strata-mcp`, which `cargo test` builds. Each runs the scripted server over its own stdin and stdout.
- **HTTP:** a server on loopback. .NET: Kestrel, in the test project. Rust: `hyper`, a dev-dependency already in the tree through `reqwest`.

**Mirrored suites,** `mcp_*.rs` / `Mcp*Tests.cs`:
- **Era detection:** every row of the probe table, per transport, and the era cached for a connection and probed again after a restart.
- **Legacy:** the `initialize` exchange, version acceptance and refusal, sessions (sent, a `404` that initializes again, `DELETE` on close), and requests from the server answered (`ping` → `{}`, others → `-32601`).
- **Modern:** `_meta` on every request, the HTTP headers, `Mcp-Name` encoding, `x-mcp-header` mirroring and rejection, and a `HeaderMismatch` that lists again and retries.
- **Tools:** listing across pages, a repeated cursor refused, duplicates, `Prefix`, `Include`, and an unknown `Include` refused.
- **Through the registry:** an MCP tool and a `[StrataTool]` side by side, subsets, middleware running for MCP calls (including a denial and a short-circuit), and the `strata.tool` span with the MCP span under it.
- **Results:** the shared fixtures, and `isError` reaching the model through an agent with a fake chat client.
- **Errors:** each row of the errors table, and Rust's `mcp_fault_classifier`.
- **Cancellation:** `notifications/cancelled` sent on stdio, the HTTP request aborted, a late response ignored, a Rust future dropped, and a timeout that is a failure, not a cancellation.
- **Lifetime:** shutdown that closes stdin, waits and stops the process tree (a fixture that starts a child of its own); restart on the next call; calls after `close` failing; Rust's `Drop` stopping the process.
- **Windows:** a `.cmd` command found through `PATHEXT` and run through `cmd.exe`.
- **Authorization:** the token hook called on each request, a token that changes, a hook that fails, and no token in any error or span.
- **Telemetry:** span names, kinds, parents and attributes per outcome; the metric; `traceparent` in `_meta` (.NET from `Activity`, Rust from the hook).
- **Rust tool names:** a name interned twice is the same `&'static str` and leaks nothing more. A registry's names outlive the registry and the client, and a reconnect gets the same pointers.

**Mutation checks**, as for design/20 and 23. Each of these should fail a test in both engines:
- falling back to `initialize` on a recognized modern error;
- dropping `_meta` from one request;
- not sending `notifications/cancelled`;
- stopping only the direct child process;
- returning `isError` text without the `{"error": …}` wrapper;
- leaving a tool with an invalid `x-mcp-header` in the list.

---

## Review decisions

Reviewed 2026-11-22: every recommendation (in bold) was accepted, and the build follows them.

1. **A tool's own error (`isError: true`).** (a) Return `{"error": "<text>"}` as the result: the model sees it and the node goes on, the registry says `succeeded`, and the MCP span says `tool_error`. (b) Make it a failure: the registry says `failed`, and the node fails by default unless the agent has `ToolFailures.ReturnToModel`. (c) A new engine-level "the tool reports an error to the model" outcome in both engines and both agents, which `[StrataTool]` tools could use too. **(a)**: it does what the spec asks with no engine change. (c) is the better long-term shape, and it is additive later.
2. **The MCP span.** (a) A client span of its own under `strata.tool`, or (b) MCP attributes on the `strata.tool` span, as the conventions prefer. (b) needs the Rust engine to declare MCP's field names on its tool span. **(a).**
3. **Trace context in `_meta`.** (a) .NET from `Activity`, Rust from a hook; (b) neither engine in the first version. **(a)**: the capability is the same in both engines, and only its source differs.
4. **A stdio server that exits.** (a) Started again on the next call (`RestartOnExit`, on by default), as the spec says clients should; (b) never: every later call fails. **(a).**
5. **A stdio server's environment.** (a) Inherited, plus what's given (what a shell does, and what `npx`-style servers need); (b) only what's given, plus `PATH`. **(a)**, with `InheritEnvironment = false` / `.inherit_env(false)` for the cautious.
6. **Images, audio and binary content.** (a) A placeholder (`[image/png, 34 KB, not shown]`); (b) the raw content JSON, base64 included. **(a)**: the bytes stay available through `CallToolAsync` / `call_tool`.
7. **Interop with the official SDKs in tests.** (a) Only Strata's own fixture servers. (b) Also a separate CI job that runs the SDKs' sample servers against both engines. It wouldn't be in the solution or the workspace, so no package references them. **(a) now, (b) as a follow-up**: (b) is the best evidence that the implementation reads the spec as everyone else does, and it is cheap once the fixture servers exist.
8. **Versions.** Modern `2026-07-28`; legacy `2025-11-25`, `2025-06-18`, `2025-03-26`, and `2024-11-05` on stdio. No HTTP+SSE transport. **As listed.** HTTP+SSE has been deprecated since 2025-03-26.
9. **Default timeouts.** `RequestTimeout` 30 s, `ProbeTimeout` 5 s, no `CallTimeout`. **As listed.** A call is already bounded by its node's timeout and the run's deadline.
10. **A prefix by default.** (a) None, or (b) the server's name. **(a)**: `serverInfo.name` isn't unique (the spec says not to rely on it), and a collision fails loudly at `Register`.

---

## Implementation Status

Built 2026-11-22 in both engines, in one change, as specified above: STR-151 (`Datalium.Strata.Mcp`) and STR-152 (`datalium-strata-mcp`). It merges after `v0.9.0` is tagged (2026-11-25), so the release doesn't carry it. These are the differences and details.

**Where it diverged, and why:**

- **The Rust engine doesn't change.** The first build changed `StrataTool::name()` to `&str`. After `0.9.0`'s API promise, the crate interns names instead and the engine is as it was (see "Tool names in Rust").
- **.NET reads SSE with a parser of its own, not `SseParser`.** The shared fixture caught two departures from the WHATWG rules in .NET's `SseParser`: after an event with no data it keeps that event's type for the next one, and it doesn't carry the last event id over to later events. `sse.json` is right, so .NET parses as Rust does (`McpSse.cs`), and the package has no `System.Net.ServerSentEvents` dependency.
- **Neither engine's own HTTP client follows redirects.** The blueprint said "not to another origin". Following none is simpler and the same in both (.NET `AllowAutoRedirect = false`, Rust `redirect::Policy::none()`). An `HttpClient` / `reqwest::Client` passed in keeps its own policy.
- **Windows batch files are quoted as Rust's standard library quotes them.** Rust runs a `.cmd` / `.bat` found through `PATHEXT` with `std::process::Command`, which already runs it through `cmd.exe` with audited quoting and refuses an argument it can't quote safely. .NET writes the same command line itself (`/e:ON /v:OFF /d /c`, `%` as `%%cd:~,%`), takes `cmd.exe` from `ComSpec`, and refuses an argument with a line break or a NUL. Both are tested with an argument holding `a b & c`.
- **Rust reads a Windows server's pipes on threads of its own.** tokio reads a child's pipes on its blocking pool on Windows, and a runtime waits for those reads when it shuts down. A process that escaped being stopped and still held a pipe would then stall the application's shutdown, which a mutation check showed. .NET had the same problem in `DisposeAsync`, which now stops waiting for the reader after 2 s.
- **Rust's `Drop` on Windows runs `taskkill /T` on a thread, and stops the server after it.** `taskkill` walks the tree from the server, so stopping the server first (as `kill_on_drop` does) left its children running. A test caught it.
- **Rust: `McpServer::http` takes `impl AsRef<str>`,** not `IntoUrl`, which can't be called outside reqwest. The token hook returns `Result<Option<String>, E: Display>`.
- **Rust's `McpError` has more variants than the sketch:** `Http { status, message }`, `Unauthorized(status)`, `Timeout { method, after }`, `InputRequired`, `Invalid` (an unknown `resultType`) and `Cancelled`, with `is_transient()`. `mcp_fault_classifier` reads the message's prefix (`mcp transport error: …`, `mcp request … timed out after …`), since a Rust tool error carries no kind.
- **`error.type` has one more value, `protocol`:** a failure with no JSON-RPC code (`input_required`, an invalid `resultType`). Connection failures and a closed client are `transport`.
- **Legacy HTTP:** `MCP-Protocol-Version` isn't sent on `initialize` itself (2025-06-18 asks for it on "subsequent requests"), and is sent on the session's `DELETE`. `Mcp-Method`, `Mcp-Name` and `x-mcp-header` are modern only, so a legacy server's tools are never left out for their annotations. A cancelled request is aborted first, then its `notifications/cancelled` is posted.
- **The probe isn't cancelled.** No `notifications/cancelled` follows a `server/discover` that timed out: a legacy server knows nothing of it.
- **A modern stdio `notifications/cancelled` carries the modern `_meta`.** A legacy request with no trace context has no `_meta` at all.
- **Rust's span is named `mcp.request`** (`tracing` names are static), with `otel.name` = `tools/call echo`. .NET's activity is named `tools/call echo`.
- **Rust's `protocol_version()`** is the version the connection opened with. A restarted stdio server is probed again and may speak another one.

**Details the blueprint left open:**

- Arguments: empty text is `{}`. Text that isn't JSON, or isn't an object, is refused before the server in `argument_problems`' words (`ToolArgumentsException` / `ToolArgumentParseFailure`), which the agent sends back to the model.
- A listed tool with no name is left out with a warning. One with no `inputSchema` gets `{"type": "object"}`. The description falls back to `title`, then `annotations.title`.
- A content block of an unknown type is `[<type> content, not shown]`. An `isError` result with no text is `{"error":"the tool reported an error without a message"}` (`tool-results.json`).
- `structuredContent` is compact JSON in both, but Rust's `serde_json` orders object keys and .NET keeps the server's order, so the fixture compares it as JSON.
- Windows command lookup: the current directory isn't searched, `PATHEXT` defaults to `.COM;.EXE;.BAT;.CMD`, and a path without an extension tries the extensions on itself.
- The log has three levels: `Debug` (stderr as `stderr: <line>`, ignored notifications and late answers), `Information` (`starting the MCP server again`), `Warning` (a tool left out).
- Request ids: one counter per client, continuing across restarts. An SSE answer is taken when its id matches, or is null.
- Messages are each engine's own, as for design/20: .NET `the MCP request tools/call timed out after 0.1 s`, Rust `mcp request tools/call timed out after 0.1s`. A JSON-RPC error is `mcp error <code>: <message>` in both.
- .NET extras: `McpToolResult.ToText()` is public, and `McpTelemetry.Name` holds the source and meter name, like `StrataCloudTelemetry`. Rust's `McpToolResult` has `text()` and `tool_text()`.

**Tests:**
- The five shared fixtures, in both engines.
- An in-memory scripted server, a stdio fixture server and a loopback HTTP server in both eras. .NET: `dotnet/tests/Datalium.Strata.McpFixtureServer` and Kestrel. Rust: the `mcp_fixture_server` example and a raw socket. The stdio fixture starts a child of its own for the process-tree tests.
- .NET has 197 MCP tests. Rust has 67: `mcp_streams`, `mcp_stdio`, `mcp_http`, `mcp_agent`, `mcp_telemetry`, and unit tests.
- `PackageSmoke` calls an MCP tool through the packages alone.
- Each of the six mutation checks fails tests in both engines.

**Known limitation:** a process that escapes being stopped keeps running: one that leaves its process group on Unix, or whose parent exited before `taskkill /T` ran on Windows. Strata stops what it can find.

## Not built (deliberately out of scope)

- **Strata as an MCP server:** exposing a `ToolRegistry`'s tools to MCP clients.
- **Resources, prompts and completion** (`resources/*`, `prompts/*`, `completion/complete`).
- **The client features:** sampling, roots and elicitation, and so MRTR input (`input_required` fails the call). Sampling and roots are deprecated in `2026-07-28`.
- **Subscriptions and list-changed notifications** (`subscriptions/listen`, legacy GET streams). The tool list is read once, at connect. To pick up a server's new tools, connect again and build a new registry.
- **The tasks extension** (`io.modelcontextprotocol/tasks`) and **progress** (`progressToken`, `notifications/progress`).
- **Full OAuth 2.1:** authorization server discovery, client registration, PKCE, browser sign-in, refresh. The token hook covers services that already have a token.
- **The HTTP+SSE transport** (2024-11-05).
- **Validating `structuredContent` against `outputSchema`**, and arguments against `inputSchema` (the server validates; a middleware can).
- **Images and audio passed to the model as images and audio.** Strata's tool results are text.
- **Settings-based registration** (`Strata:Mcp:Servers` in `AddStrata()`), and a CLI command to list a server's tools.
