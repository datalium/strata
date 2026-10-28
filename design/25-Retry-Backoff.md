# Datalium Strata: Retry Backoff and Failure Info for Fallbacks

`design/04-Error-Recovery.md` built retries and fallbacks, and listed two things as not built:

- **Backoff between retries.** A `RecoveryPolicy` retries a transient failure at once. A node calling a rate-limited model API retries straight into the same 429.
- **Telling a fallback node why** the node failed. The fallback runs on the pre-failure state and knows only that it is running.

`design/17-Async-Execution.md` made execution asynchronous, so a wait between retries can be an async sleep that observes the run's token. The run events were ready for it: `RetryScheduled` has carried a `Delay` since `design/18`, always zero.

StrataOps needs both for its M2 (real models). Model rate limits (a 429 with `Retry-After`) are a named failure class. The Model Fabric's HTTP pipeline retries first, and a node that still fails transiently should wait with backoff, honouring the provider's hint, before Strata retries it. Its `rca` node falls back to `postmortem`, and that fallback should know whether `rca` was rate limited, timed out or gave a refused answer, so it can say so or route to a cheaper model. This document specifies **SG-04**. The SG numbers refer to the gap log of StrataOps (not public yet).

SG-04 lands in both engines at one commit (strict parity, StrataOps ADR 0003): the same capability, in each language's idiom.

It was written as a blueprint before implementation and is now built. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Policy shape | **`RecoveryPolicy.Retry(3).WithBackoff(initial, max, jitter)`** / **`RecoveryPolicy::retry(3).with_backoff(initial, max, jitter)`**. Exponential, doubling from `initial`, capped at `max`. |
| Jitter | **`Full` (the default), `Equal` or `None`.** Full draws the delay uniformly from `[0, d]`, Equal from `[d/2, d]`, None waits exactly `d`. |
| Without backoff | **Today's behaviour, and the default.** A policy with no backoff retries at once, hint or not. |
| The hint | **`NodeException.Transient(message, retryAfter)`** / **`NodeError::transient_after(message, retry_after)`**. The hint **replaces** the computed delay, **capped by `max`**, with no jitter. |
| The wait | An async sleep **between** a failed attempt and the next, inside the node's span, that **observes the level's token**. A cancelled run (token or deadline) stops waiting at once and ends `Cancelled` as it would anywhere else. |
| A deadline inside the wait | **Wait, and let the deadline cancel.** The engine doesn't cancel early because a retry would start after the deadline: a deadline is when a run stops, never before. |
| Telemetry | `RetryScheduled.Delay` / `delay` and the span event's `strata.retry.delay_ms` carry **the real delay**. No new event. |
| Testable time | **.NET: `RunOptions.TimeProvider`** (default `TimeProvider.System`), the clock for every engine wait: backoff, node timeouts and the deadline. **Rust: tokio's clock**, which already drives all three, so `tokio::time::pause` (`start_paused`) is the seam. |
| Failure info for the fallback | **`NodeContext.LastFailure`** / **`ctx.last_failure()`**: the failed node, its path, the final error (kind, message, hint), the attempts made, and whether the last attempt timed out. Set **only in the fallback node's context** (its every attempt, and a group fallback's branches). |
| Persisted? | **No.** Context only, beside the existing `NodeCompleted.Fallback`. It doesn't survive a freeze or cancellation between the failure and the fallback (see "Not built"). |
| Groups and subgraphs | **The same backoff**, because they are nodes and their policy is a `RecoveryPolicy`. A group's transient failure carries the longest hint among its failed branches; a subgraph's carries its child's, through a new **`strata.retry_after_ms`** key on the child's `failed` checkpoint. |
| Wire format | **No schema change.** `strata.retry_after_ms` is an engine-reserved metadata key, like `strata.hops` (`design/24`). |

---

## Usage

```csharp
var investigation = new StateGraph<Investigation>()
    .AddFallibleNode("rca", async (s, ctx, ct) =>
    {
        try
        {
            return s with { Rca = await fabric.CompleteAsync(RcaPrompt(s), ct) };
        }
        catch (RateLimitedException ex)
        {
            // The Model Fabric already retried. Tell Strata how long the provider asked for.
            throw NodeException.Transient("rate limited", ex.RetryAfter, ex);
        }
    })
    .AddFallibleNode("postmortem", async (s, ctx, ct) =>
    {
        var why = ctx.LastFailure;               // set: we are rca's fallback
        var reason = why switch
        {
            { RetryAfter: not null } => "the model provider was rate limiting",
            { TimedOut: true } => "the model timed out",
            _ => $"the model failed: {why?.Message}",
        };
        return s with { Rca = await fabric.CompleteAsync(PostmortemPrompt(s, reason), ct, model: "small") };
    })
    // …
    .SetRecoveryPolicy("rca", RecoveryPolicy.Retry(3)
        .WithBackoff(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(30))   // full jitter by default
        .WithFallback("postmortem"))
    .Compile();
```

```rust
let investigation = StateGraph::new()
    .add_async_fallible_node("rca", |s: Investigation, ctx: NodeContext| async move {
        match fabric.complete(rca_prompt(&s), ctx.cancellation()).await {
            Ok(rca) => Ok(Investigation { rca: Some(rca), ..s }),
            Err(FabricError::RateLimited { retry_after }) => {
                Err(NodeError::transient_after("rate limited", retry_after))
            }
            Err(other) => Err(NodeError::Fatal(other.to_string())),
        }
    })
    .add_async_fallible_node("postmortem", |s: Investigation, ctx: NodeContext| async move {
        let reason = match ctx.last_failure() {
            Some(f) if f.error.retry_after().is_some() => "the model provider was rate limiting".to_string(),
            Some(f) if f.timed_out => "the model timed out".to_string(),
            Some(f) => format!("the model failed: {}", f.error.message()),
            None => String::new(),
        };
        // …
    })
    .set_recovery_policy(
        "rca",
        RecoveryPolicy::retry(3)
            .with_backoff(Duration::from_secs(2), Duration::from_secs(30), Jitter::Full)
            .with_fallback("postmortem"),
    )
    .compile()?;
```

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Graph;

public enum BackoffJitter
{
    /// The delay is drawn uniformly from [0, d]. The default.
    Full,
    /// The delay is drawn uniformly from [d/2, d].
    Equal,
    /// The delay is d.
    None,
}

/// Exponential backoff between retries: the n-th retry waits Initial × 2^(n−1), capped at Max, jittered.
public sealed record RetryBackoff(TimeSpan Initial, TimeSpan Max, BackoffJitter Jitter = BackoffJitter.Full);

public sealed record RecoveryPolicy(int MaxRetries = 0, string? Fallback = null)
{
    /// Null (the default): retries start at once.
    public RetryBackoff? Backoff { get; init; }

    /// ArgumentOutOfRangeException for a negative Initial, or a Max below Initial.
    public RecoveryPolicy WithBackoff(TimeSpan initial, TimeSpan max, BackoffJitter jitter = BackoffJitter.Full);
}

public sealed class NodeException
{
    /// How long the failing service asked to be left alone, if it said (a 429's Retry-After).
    public TimeSpan? RetryAfter { get; }

    /// ArgumentOutOfRangeException for a negative retryAfter; null is a transient failure with no hint.
    public static NodeException Transient(string message, TimeSpan? retryAfter, Exception? innerException = null);
}

/// Why a node failed, as its fallback node sees it (NodeContext.LastFailure).
public sealed record NodeFailure
{
    public required string Node { get; init; }           // the node that failed: "rca"
    public required string NodePath { get; init; }       // qualified: "investigate/rca"
    public required NodeFailureKind Kind { get; init; }  // of the final failure
    public required string Message { get; init; }
    public TimeSpan? RetryAfter { get; init; }           // the final failure's hint
    public required int Attempts { get; init; }
    public bool TimedOut { get; init; }                  // the final attempt ran over its timeout
}

public sealed class NodeContext
{
    /// Set only in a fallback node's context: why the node it falls back from failed.
    public NodeFailure? LastFailure { get; init; }
}

public sealed class RunOptions
{
    /// The clock for every wait the engine makes: retry backoff, node timeouts and the deadline.
    /// Null: TimeProvider.System. Timestamps on checkpoints and events stay wall-clock.
    public TimeProvider? TimeProvider { get; init; }
}
```

### Rust

```rust
/// How a backoff delay is jittered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Jitter {
    /// Uniformly from `[0, d]`. The default.
    #[default]
    Full,
    /// Uniformly from `[d/2, d]`.
    Equal,
    /// Exactly `d`.
    None,
}

/// Exponential backoff between retries: the n-th retry waits `initial × 2^(n−1)`, capped at `max`, jittered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    pub initial: Duration,
    pub max: Duration,
    pub jitter: Jitter,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecoveryPolicy {
    pub max_retries: u32,
    pub fallback: Option<String>,
    /// `None` (the default): retries start at once.
    pub backoff: Option<Backoff>,
}

impl RecoveryPolicy {
    /// `compile()` refuses a `max` below `initial` (`CompilationFailed`).
    pub fn with_backoff(self, initial: Duration, max: Duration, jitter: Jitter) -> Self;
}

pub enum NodeError {
    Transient(String),
    Fatal(String),
    /// A transient failure whose cause said when to try again (a 429's `Retry-After`).
    TransientAfter(String, Duration),
}

impl NodeError {
    pub fn transient_after(message: impl Into<String>, retry_after: Duration) -> Self;
    pub fn message(&self) -> &str;                 // existing; covers the new variant
    pub fn is_transient(&self) -> bool;            // Transient or TransientAfter
    pub fn retry_after(&self) -> Option<Duration>;
}

/// Why a node failed, as its fallback node sees it (`NodeContext::last_failure`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct NodeFailure {
    pub node: String,
    pub node_path: String,
    pub error: NodeError,        // the final failure: kind, message and hint
    pub attempts: u32,
    pub timed_out: bool,
}

impl NodeFailure {
    pub fn new(node: impl Into<String>, error: NodeError) -> Self;   // for hand-built test contexts
    pub fn with_node_path(self, path: impl Into<String>) -> Self;
    pub fn with_attempts(self, attempts: u32) -> Self;
    pub fn with_timed_out(self, timed_out: bool) -> Self;
}

impl NodeContext {
    pub fn last_failure(&self) -> Option<&NodeFailure>;
    pub fn with_last_failure(self, failure: NodeFailure) -> Self;
}
```

- Rust's `NodeError` carries the hint in a **new variant**, so constructing `Transient` / `Fatal` is unchanged; only an exhaustive `match` needs an arm (`NodeError` isn't `#[non_exhaustive]`). The `is_transient` / `retry_after` accessors are what matching code should move to.
- Rust's `NodeFailure` holds the final `NodeError` itself, which carries the kind, message and hint; .NET flattens the same three into `Kind`, `Message` and `RetryAfter`, since a `NodeException` is not a value to keep. Neither carries the failing exception's inner cause: Rust has none, and the message is what both engines persist.
- `RecoveryPolicy` is a positional record in .NET, so `Backoff` is an `init` property; Rust's gains a public field, so a struct literal needs `..Default::default()` (as `HumanSignal` did in `design/21`).

---

## Semantics

### The delay

For the `n`-th retry (`n` = 1 before the second attempt):

```text
d = min(max, initial × 2^(n−1))        // saturating: never overflows, so a long run stays at max
delay = hint is set ? min(hint, max)    // the failure that is being retried carried one
      : Full   → uniform [0, d]
      : Equal  → d/2 + uniform [0, d/2]
      : None   → d
```

- **The hint replaces the computed delay.** The failing service knows when it will take the call again; doubling past it waits too long, and waiting less than it fails again. It is capped by `max`, because `max` is the graph author's bound on how long a node may sit idle; a provider asking for an hour gets `max`. It is not jittered: it is a time the service named.
- **A hint needs a backoff.** A policy without one never waits, so a hint on its failure is ignored and `RetryScheduled.Delay` is zero. The backoff's `max` is what bounds a hint, and a policy that has none hasn't said how long a node may wait.
- **Only the failure being retried counts.** The hint on attempt 2's failure sets the wait before attempt 3; an earlier attempt's hint doesn't carry over.
- **A zero delay doesn't wait.** No timer, no yield: the next attempt starts as it did before SG-04, so `initial` = 0 with `Jitter.None` is today's behaviour exactly.
- **Constant backoff** is `initial` = `max`.

### The wait

```text
attempt n fails transiently, n ≤ MaxRetries
    RetryScheduled { delay }   +   the span's retry event   +   a Retry chunk (streaming)   (as before)
    wait `delay` on the run's clock, or until the level's token fires
    attempt n+1  (or, if the token fired: the node ends Cancelled, as at the top of any attempt)
```

- **Inside the node, between attempts.** The node's span and its `NodeCompleted.Duration` include the wait; no attempt span covers it.
- **The level's token** is the run's token at the outermost graph and the subgraph node's attempt token in a child, so a cancelled run, a passed deadline, or an enclosing subgraph's timeout all stop the wait at once. What follows is exactly what a cancellation at the top of the next attempt does today: a `Cancelled` outcome and a `cancelled` checkpoint at the node (or, inside a child stopped by its subgraph's timeout, `cancel_reason` `timeout`). A subgraph node being resumed into is entered even so, as before, so its child stops at its own innermost node and nothing a human answered is lost.
- **A deadline inside the wait** cancels the run when it passes, with `CancelReason.Deadline`, and not before. A run whose next retry would start after its deadline therefore waits until the deadline instead of failing early: a deadline is the time a run stops, and reporting `Deadline` before it had passed would be wrong. A caller that wants a shorter wait sets a smaller `max`.
- **Node timeouts don't cover the wait.** A timeout is per attempt (`design/17`), and the wait is between attempts.
- **Interrupts and resumes don't see it.** The wait happens inside one run; nothing about it is persisted. Resuming a `failed` checkpoint re-enters the node at once (it is a retry the human asked for), with attempts counted from 1 again, as before.

### Groups and subgraphs

A parallel group and a subgraph node are fallible nodes whose policy is an ordinary `RecoveryPolicy`, so `WithBackoff` on them backs off between whole-group or whole-child retries. Their failure carries a hint when it is transient:

- **A group** that failed transiently carries the **longest** hint among the branches that had failed by then (a branch with none counts as none). Waiting for the longest asks every rate-limited service for no less than it asked. A fatal group carries none.
- **A subgraph** carries its child's: when the child's own recovery ran out on a transient failure with a hint, the child's `failed` checkpoint records it as **`strata.retry_after_ms`** (milliseconds, a decimal string), and the subgraph runner, which already reads `error_kind` from that checkpoint, reads the hint too. Any level's `failed` checkpoint whose final failure had a hint records the key, so the outermost one tells an operator too.

### Failure info for the fallback node

When a node's policy gives up and routes to its fallback, the fallback node's context carries **`LastFailure`** / **`last_failure()`**:

| Field | Value |
|---|---|
| `Node` / `node` | The node that failed, by its own name. |
| `NodePath` / `node_path` | Its qualified path (`"investigate/rca"` inside the subgraph node `investigate`), the path run events use. |
| `Kind`, `Message`, `RetryAfter` / `error` | The **final** failure: what made the policy give up (a fatal error, or the last transient one). For a group or subgraph, the combined failure the group or subgraph node reported. |
| `Attempts` / `attempts` | Attempts made, retries included: what a `failed` checkpoint would have recorded as `attempts`. |
| `TimedOut` / `timed_out` | The final attempt ran over the node's timeout (its error is then the transient "timed out after N ms"). |

- **Only the fallback node sees it**, on every attempt of it (it may itself be fallible and retried). The node after the fallback doesn't, and neither does a subgraph fallback's child (a child doesn't inherit its parent's context, as with `Decision`). A parallel group used as a fallback hands it to its branches, which see the group's context as they see its attempt.
- **A fallback that itself fails** and falls back hands its own failure to the next fallback; nothing chains.
- **Not persisted** and **not in a run event.** `NodeCompleted` already records `Fallback` (where the failed node went) and its error; the fallback is told the rest in-process. So it survives nothing that ends the run between the failure and the fallback: if the fallback node is cancelled, or is a subgraph whose child freezes, a resume re-enters the fallback with `LastFailure` unset. See "Not built".

---

## Telemetry

| | |
|---|---|
| `RetryScheduled` | `Delay` / `delay` is the wait the engine is about to make: the jittered backoff, the capped hint, or zero with no backoff. The doc comments drop "always zero". |
| Span event | The node span's `retry` event (`tracing` `warn` in Rust) carries the same delay as `strata.retry.delay_ms` (a float). |
| Node span and `NodeCompleted.Duration` | Include the waits. |
| Metrics | Unchanged: `strata.node.retries` counts retries, and `strata.node.duration` includes the waits. |
| Checkpoint | `strata.retry_after_ms` on a `failed` checkpoint whose final failure carried a hint. |

No new event: when a retry is scheduled, and how long it will wait, is what `RetryScheduled` already says. A console can draw the wait from it.

---

## Wire format

**No schema change.** `strata.retry_after_ms` rides in the checkpoint's existing `metadata` map under the engine's `strata.` prefix (`design/18`), which callers can't write, like `strata.hops`. Both engines write it as a decimal string of whole milliseconds (rounded down), and read it back the same way. No `schema_version` bump, no DDL, no new fixtures. A resume doesn't read it: it re-enters the node at once, as before. The graph definition is unchanged: a recovery policy isn't exported (`design/22`).

---

## Migration

**.NET** (source-compatible): `RecoveryPolicy.Backoff` / `WithBackoff`, `RetryBackoff`, `BackoffJitter`, `NodeException.RetryAfter` and the `Transient(message, retryAfter, inner)` overload, `NodeFailure`, `NodeContext.LastFailure` and `RunOptions.TimeProvider` are new. A policy without backoff behaves as before. Record equality on `RecoveryPolicy` now includes `Backoff`.

**Rust:** `RecoveryPolicy` gains the public field `backoff`, so a struct literal needs `backoff: None` or `..Default::default()`; `retry()` / `with_fallback()` are unchanged. `NodeError` gains `TransientAfter`, so an exhaustive `match` needs an arm (or `is_transient()`). `Backoff`, `Jitter`, `NodeFailure`, `NodeContext::last_failure` / `with_last_failure` are new. Tests that use paused time need tokio's `test-util` feature.

**StrataOps:** the `rca` node's policy gets `WithBackoff`, its rate-limit failures throw `NodeException.Transient(message, retryAfter)`, and `postmortem` reads `ctx.LastFailure`. Its execution events show the real delay from `RetryScheduled`.

---

## Tests

Mirrored per engine, in new suites `retry_backoff.rs` / `RetryBackoffTests.cs`, on a controlled clock (.NET `FakeTimeProvider`, Rust `start_paused`):

- **Without backoff nothing changes:** retries start at once and `RetryScheduled.Delay` is zero, hint or not.
- **Delays grow and cap:** `Jitter.None`, initial 1 s, max 5 s, five retries: 1, 2, 4, 5, 5 s, in `RetryScheduled` and on the clock (the next attempt doesn't start before the delay has passed).
- **Jitter stays in bounds:** Full within `[0, d]`, Equal within `[d/2, d]`, over many retries.
- **The hint wins, and is capped:** a 3 s hint replaces the computed 1 s; a 60 s hint with max 10 s waits 10 s; the hint is not jittered; a hint on an earlier attempt doesn't carry over.
- **Cancellation interrupts the wait:** cancelled mid-wait, the run ends `Cancelled` at the node with a `cancelled` checkpoint and the next attempt never starts.
- **A deadline inside the wait** ends the run `Cancelled` with `Deadline` when it passes, not before.
- **The fallback sees `LastFailure`:** a transient failure out of retries (kind, message, hint, attempts); a fatal one (attempts 1); a timeout (`TimedOut`); its path inside a subgraph; on every attempt of a retried fallback; the node after the fallback sees none; a node that wasn't reached by a fallback sees none.
- **A group's and a subgraph's retries back off too**, and carry the hint (a group's longest; a subgraph's from its child, with `strata.retry_after_ms` on the child's and the outer `failed` checkpoints).
- **Validation:** a negative initial (.NET), a max below initial and a negative hint (.NET) are refused.
- **Mutation checks:** not waiting, ignoring the hint, and not capping the hint should each fail tests in both engines.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

### The questions as asked

Recommendations are in bold.

1. **Full or equal jitter, and which by default?** **Both, plus `None`; Full by default.** Full spreads synchronized retries best (many investigations hitting one rate-limited provider at once), which is the case SG-04 is for; Equal suits a caller that wants a floor; None suits tests and a single caller.
2. **Can a policy exist without backoff?** **Yes, and it is the default.** Every existing policy keeps retrying at once.
3. **Does the hint replace the delay or floor it, and is it capped?** **It replaces it, capped by `max`.** A floor would double past the time the service named.
4. **A hint on a policy without backoff?** **Ignored.** The alternative, waiting an uncapped hint, lets a provider decide how long a run sits idle. Recorded in "Semantics" so it isn't a surprise.
5. **A deadline inside the wait: cancel at once, or wait for it?** **Wait for it.** A run reports `Deadline` only once the deadline has passed.
6. **How is the wait tested?** **A `TimeProvider` on `RunOptions` in .NET, tokio's paused clock in Rust.** Both are the platform's own seam. The .NET one also drives node timeouts and the deadline, so one fake clock controls every engine wait, as tokio's does.
7. **What does the fallback learn, and where?** **`NodeContext.LastFailure`, context only.** Persisting it would need a checkpoint field (a schema bump) to survive a freeze that is rare between a failure and its fallback; `NodeCompleted` already carries the error and the route for observers.
8. **Do groups and subgraphs back off?** **Yes, they are nodes.** Their hint is the longest among a group's failed branches, and the child's for a subgraph, carried on the child's `failed` checkpoint as `strata.retry_after_ms`.

---

## Implementation Status

Built 2026-10-05 in both engines, in one change, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **.NET's `RetryBackoff` validates in its constructor.** It is a sealed record with get-only properties rather than a positional one, so neither `new RetryBackoff(...)` nor a `with` expression can make a backoff that `WithBackoff` would refuse. Rust has no constructor to check in (the fields are public), so `compile()` refuses a `max` below `initial` instead (`CompilationFailed`), as it refuses a fallback that names a missing node.
- **The hint overload takes a `TimeSpan?`.** `NodeException.Transient(message, retryAfter, inner)` accepts a null hint, so a caller can pass a 429's optional `Retry-After` straight through. `Transient("x", null)` still binds to the old `(message, innerException)` overload, which C# prefers because it needs no default argument.
- **Rust's jitter needs no new dependency.** It draws from std's `RandomState` (fresh random hasher keys per call); .NET uses `Random.Shared`.
- **The CLI doesn't print the hint.** `strata thread inspect` hides every `strata.` key from the caller-metadata line, as it did for `strata.hops`; `--json` shows `strata.retry_after_ms`.
- **Rust's `RecoveryPolicy::retry` builds on `Default`**, so later fields won't touch it again.

**How it was built:**

- **.NET:**
  - `Graph/RecoveryPolicy.cs`: `RecoveryPolicy.Backoff` / `WithBackoff`, `BackoffJitter`, `RetryBackoff` (with the internal `DelayBefore`), and `RetryAfterHint` (the `strata.retry_after_ms` key, its format and reader).
  - `Graph/NodeException.cs`: `RetryAfter` and the hint overload. `Graph/NodeFailure.cs` is new; `NodeContext.LastFailure` and `RunOptions.TimeProvider` are new properties.
  - `Graph/Execution.cs`: `RunScope.Clock`; `LevelScope.LastFailure`, which `ContextFor` hands on and `Child` clears; `RootScope` arms the deadline with a `CancellationTokenSource` on the run's clock, linked to the caller's token.
  - `Graph/CompiledGraph.Interruption.cs`: `RunAttemptsAsync` computes the delay, reports it, emits the `Retry` chunk and then waits (`BackOffAsync`); the attempt's timeout is a `CancellationTokenSource` on the run's clock linked to the level's token; a fallback outcome carries a `NodeFailure`, which `ExecuteFromAsync` sets on the next node's level only; a hinted `failed` checkpoint records `strata.retry_after_ms`.
  - `ParallelBranches.cs`: a transient group carries the longest hint, and a branch's context carries the group's `LastFailure`. `Subgraph.cs` reads the child's hint.
  - `RunTelemetry.Retry` takes the delay for `RetryScheduled` and `strata.retry.delay_ms`.
- **Rust:**
  - `graph.rs`: `NodeError::TransientAfter` with `transient_after`, `is_transient` and `retry_after`; `Jitter`, `Backoff` (with the crate-private `delay_before`), `RecoveryPolicy::backoff` / `with_backoff`, `NodeFailure`, `jitter_sample`, and the compile check.
  - `context.rs`: `NodeContext::last_failure` / `with_last_failure`, carried into branches by `for_branch`; `LevelScope::with_last_failure`, cleared by `child`.
  - `checkpoint.rs`: `RETRY_AFTER_KEY`, `stored_retry_after` and `back_off`; `run_attempts` and `execute_from` as in .NET. Waits, node timeouts and the deadline were already on tokio's clock.
  - `parallel.rs` and `subgraph.rs` carry the hint as in .NET. `observability.rs`: `NodeScope::retry` takes the delay, and `node_error_tag` maps `TransientAfter` to `transient`.
  - `Cargo.toml`: tokio's `test-util` feature for the tests' paused clock (dev-dependency only).

**Tests:**

- **New suites:** `retry_backoff.rs` (18) / `RetryBackoffTests.cs` (18), case for case. .NET's jitter check is one theory with two cases where Rust has two tests. .NET drives a `FakeTimeProvider` forward a second at a time until the run ends, so its timing check is a lower bound (no attempt starts before its delay has passed); Rust's paused clock also checks that a deadline ends the wait at the deadline, not after the 60 s backoff.
- **Totals when this landed:** .NET 485 passed, with 38 Postgres/Redis tests skipped because those services weren't running locally (this change touches no store), plus the example's 1. `rust/` 482, `cli/` 58, `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` (all three projects) and `dotnet format --verify-no-changes` are clean.
- **Mutation checks, in both engines:**
  - Not waiting (skipping the backoff wait) fails 3 tests in each.
  - Ignoring the hint fails 3 in each.
  - Not capping the hint at `max` fails 1 in each.

## Not built (deliberately out of scope)

- **`LastFailure` across a freeze or cancellation.** If the run stops between the failure and the fallback's completion (the fallback is cancelled, or is a subgraph whose child freezes), a resume re-enters the fallback without it. Keeping it needs a checkpoint field.
- **`LastFailure` in a run event.** `NodeCompleted` carries the error and the fallback; the hint and `TimedOut` are not in an event.
- **The inner exception in `LastFailure`.** Neither engine keeps the failing exception or its cause; the message is what is persisted and what both engines can carry.
- **Other backoff shapes:** a configurable multiplier, linear backoff, decorrelated jitter. Constant backoff is `initial` = `max`.
- **Giving up early when a hint exceeds `max`.** A hint beyond `max` waits `max` and retries; routing straight to the fallback instead would be a policy option nobody has asked for.
- **Per-branch backoff or retries** inside a parallel group (`design/26`).
- **A wait that survives the process.** The wait is in memory; a crash during it loses the run like any other crash (SG-10).
- **The clock for timestamps.** `RunOptions.TimeProvider` drives waits only; checkpoint and event timestamps stay wall-clock (tokio's paused clock doesn't move `chrono` either).
