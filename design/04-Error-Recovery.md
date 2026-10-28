# Datalium Strata: Error Recovery & Checkpointing

In Datalium Strata, error recovery and state checkpointing are designed for absolute system resilience.

Python frameworks often rely on implicit try-catch blocks or heavy external state dumps to handle graph execution failures. Datalium Strata treats checkpoints as a first-class transactional layer. Because it is written in Rust and C#, it can leverage atomic database operations, deterministic state machine replays, and zero-allocation diffs to save progress after every single node transition.

---

## 🏛️ The Checkpoint Architecture Principles

- **Transactional Transitions (ACID-lite):** State mutations are only committed to the persistence layer after a node successfully completes execution. If a node crashes mid-task, the system rolls back to the previous stable state layer.
- **Deterministic Replays (Time-Travel Debugging):** Every checkpoint stores the exact state configuration and the index of the node to be executed next. This allows developers to load an old checkpoint into a local console and perfectly step-debug a production failure.
- **Pluggable Memento Engine:** Checkpoint frequencies can be customized—saving state continuously on every single node transition, or selectively only on critical milestone nodes to optimize database traffic.

---

## 📦 Checkpoint and Error Flow Blueprint

```text
[ Active Node Execution ] ──( Success )──► [ Generate State Diff / Snapshot ]
           │                                                │
       ( Failure )                                          ▼
           │                                  [ Atomic Checkpoint Store ]
           ▼                                   (Write State + Next Node ID)
[ Error Recovery Policy ]                                   │
           │                                                ▼
     ┌─────┴─────────────────────┐                [ Proceed to Next Node ]
     ▼                           ▼
( Retry Node )          ( Trigger Safe Fallback )
     │                           │
     ▼                           ▼
[ Replay Last Checkpoint ]  [ Route to Error Handlers / Nodes ]
```

## 🛠️ Cross-Language Implementation Blueprint

Here is how this robust checkpointing framework maps natively into the syntax of C# and Rust.

### 🔷 C# Architecture (Datalium.Strata.Checkpoints)

In C#, checkpointing is handled by a state middleware or direct engine hook. We use a Memento Pattern that tracks execution indices and saves raw binary state snapshots via optimized serializers like MessagePack or System.Text.Json source generation.

```csharp
namespace Datalium.Strata.Checkpoints;

// 1. Immutable checkpoint packaging
public record StateCheckpoint<TState>(
    string ThreadId,
    int SequenceId,
    string NextNode,
    TState State,
    DateTime Timestamp
);

public interface ICheckpointStore<TState>
{
    Task SaveCheckpointAsync(StateCheckpoint<TState> checkpoint, CancellationToken ct = default);
    Task<StateCheckpoint<TState>?> GetLatestCheckpointAsync(string threadId, CancellationToken ct = default);
}

// 2. Resilience and Error Handling in the Graph Engine
var graph = new StateGraph<MyState>(initialState)
    .AddNode("ProcessPayment", async (state, ctx) =>
    {
        // Network or API failure occurs here
        var receipt = await ctx.Services.PaymentGateway.ChargeAsync(state.Amount);
        return state with { ReceiptId = receipt.Id };
    })
    .SetRecoveryPolicy("ProcessPayment", Policy
        .Handle<TimeoutException>()
        .RetryAsync(3) // Retry 3 times natively
        .OnFallbackAsync(async (state, ctx) =>
        {
            // If all retries fail, cleanly route to a dedicated error mitigation node
            return ctx.RouteTo("PaymentErrorFallback");
        })
```

## 🦀 Rust Architecture (datalium-strata-checkpoints)

In Rust, we lean into Compile-Time Error Propagation (`Result<T, E>`) and explicit ownership patterns. Because Rust lacks a standard dynamic exception hierarchy, every error is a strongly typed struct, making recovery paths entirely predictable.

```rust
use async_trait::async_trait;
use serde::{Serialize, Deserialize};

// 1. Thread-safe checkpoint metadata structure
#[derive(Serialize, Deserialize, Clone)]
pub struct Checkpoint<S> {
    pub thread_id: String,
    pub sequence_id: u64,
    pub next_node: String,
    pub state: S,
}

#[async_trait]
pub trait CheckpointStore<S>: Send + Sync {
    async fn save_checkpoint(&self, cp: Checkpoint<S>) -> Result<(), StrataError>;
    async fn get_latest_checkpoint(&self, thread_id: &str) -> Result<Option<Checkpoint<S>>, StrataError>;
}

// 2. Graph execution loop under strict error handling
pub async fn execute_node_step<S: Clone>(
    store: &impl CheckpointStore<S>,
    thread_id: &str,
    mut current_cp: Checkpoint<S>,
    node_fn: impl Fn(S) -> Result<S, NodeError>
) -> Result<Checkpoint<S>, StrataError> {

    // Execute the node operation safely evaluating the result
    match node_fn(current_cp.state.clone()) {
        Ok(updated_state) => {
            current_cp.sequence_id += 1;
            current_cp.state = updated_state;
            current_cp.next_node = "NextLogicalNode".to_string();

            // Atomically flush the verified checkpoint to disk
            store.save_checkpoint(current_cp.clone()).await?;
            Ok(current_cp)
        }
        Err(NodeError::Transient(err)) => {
            // Log error, reload the exact state from the previous sequence checkpoint, and retry
            println!("Transient error encountered: {}. Replaying last stable checkpoint...", err);
            Ok(current_cp) // Returns unmodified previous checkpoint to execute again
        }
        Err(NodeError::Fatal(err)) => {
            // Force route to an explicit administrative or cleanup node
            current_cp.next_node = "ErrorCleanupNode".to_string();
            Ok(current_cp)
        }
    }
}
```

## 💎 Key Architectural Differentiators Over Python

- **No Ghost States:** In LangGraph, an uncaught Python exception can leave global thread variables partially mutated in memory. In Strata, state transitions are functionally pure—the main thread state is only updated after the node completes successfully and the database transaction commits.
- **High-Speed Checkpoint Encoding:** Because Rust and C# support ahead-of-time (AOT) generation and binary memory formats, dumping a state snapshot to a local storage tier (like RocksDB or SQLite) takes microseconds, causing effectively zero latency penalties to the execution flow.
- **Native Enterprise Polling Patterns:** Strata easily hooks into enterprise messaging and resilience frameworks like Polly in .NET or Tokio-Retry crates in Rust, ensuring deep alignment with modern microservice architectures.


---

## ✅ Implementation Status (as of demo 08, 2026-09-18)

The blueprint above is the design intent. What is actually built differs in shape, so read this before relying on the snippets:

**Built (Rust and C#, with shared-fixture parity tests):**

- **Fallible nodes.** Rust: `StateGraph::add_fallible_node(name, |s: &S| -> Result<S, NodeError>)` with `NodeError::{Transient, Fatal}`. C#: `AddFallibleNode(name, Func<TState, TState>)`, failing by throwing `NodeException.Transient(...)` / `NodeException.Fatal(...)`. Plain `add_node`/`AddNode` handlers are unchanged and still cannot fail.
- **Per-node `RecoveryPolicy`** via `set_recovery_policy` / `SetRecoveryPolicy` — `RecoveryPolicy::retry(n).with_fallback(node)` (C#: `RecoveryPolicy.Retry(n).WithFallback(node)`). This replaces the Polly-style `Policy.Handle<...>().RetryAsync(3).OnFallbackAsync(...)` fluent chain and `ctx.RouteTo(...)` shown above, which do not exist. A transient error is retried up to `n` more times; a fatal error, or exhausted retries, routes to the fallback node if one is set.
- **Rollback.** Every attempt runs against the same last-committed state, so a failed attempt's partial changes never reach the retry, the fallback node, or the checkpoint.
- **Failed checkpoint.** With no fallback, the engine persists a `status: "failed"` checkpoint (last committed state in `frozen_state`; the failed node as both `suspended_node` and `next_node`; `error` / `error_kind` / `attempts` in `metadata`) and returns `StrataError::NodeFailed` / `GraphExecutionResult.NodeFailed`. See `schema/fixtures/checkpoint-lifecycle/failed.json`.
- **Validation.** A fallback route counts as a guarded edge for STRATA-SPEC-004, so a fallback-only node is not an orphan; `.compile()` also rejects a policy on a non-fallible node or a fallback that names a missing node.

**Added since (`17-Async-Execution.md`, 2026-10-01):**

- **Async fallible nodes.** Rust: `add_async_fallible_node(name, |s: S, ctx: NodeContext| async move { … Result<S, NodeError> })`. C#: an `AddFallibleNode` overload taking `Func<TState, NodeContext, CancellationToken, ValueTask<TState>>`, still failing by throwing `NodeException`. `NodeContext.Attempt` / `ctx.attempt()` counts the attempts from 1.
- **Node timeouts** (`SetTimeout` / `set_timeout`, per attempt): an attempt that runs over is a **transient** failure, retried or routed to the fallback by the policy as above. A plain node with a timeout fails with a `failed` checkpoint, like a fallible node with the default policy.
- **Cancellation is not a failure.** A cancelled run (its token, or its deadline) never reaches a `RecoveryPolicy`: no retry, no fallback, and a `cancelled` checkpoint instead of a `failed` one, which a resume re-enters the same way.

**Added since (`25-Retry-Backoff.md`, 2026-10-05):**

- **Backoff between retries** (SG-04): `RecoveryPolicy.Retry(n).WithBackoff(initial, max, jitter)` / `RecoveryPolicy::retry(n).with_backoff(initial, max, jitter)`, exponential and capped, with full (default), equal or no jitter. A transient failure can carry a retry hint (`NodeException.Transient(message, retryAfter)` / `NodeError::transient_after`), which replaces the computed delay, capped at `max`. The wait observes cancellation and the deadline, and `RetryScheduled` carries it.
- **Telling a fallback node why**: `NodeContext.LastFailure` / `ctx.last_failure()` (the failed node, its path, the final error, attempts, whether it timed out), in the fallback node's context only.

**Not built:**

- ~~**Checkpoint after every node transition.**~~ Built 2026-10-10 (SG-10, `33-Durable-Execution.md`): a graph compiled with `CheckpointPolicy.EveryNode` writes a `running` checkpoint before its first node and after every node that commits, and a thread whose process died is recovered from the last one. The default is still checkpoints only where a run stops, and the frequency isn't configurable beyond those two.
- **Time-travel debugging.** ~~`resume` refuses anything that is not `interrupted`~~ Since 2026-09-21 `resume` accepts a `failed` checkpoint too: it re-enters `next_node` (the node that failed) with the last committed state, once per failure, and a failed subgraph node keeps the child's own failed checkpoint so a nested thread resumes inside the child (`16-Nested-Interrupts.md`, "Resuming a failed thread"). That is a retry of the failed node, not replay of an earlier checkpoint: the append-only log keeps the history (`strata checkpoint replay` shows it), but nothing re-executes from an arbitrary earlier point.
- ~~**Backoff between retries**, and **telling a fallback node why** the node failed~~ Built 2026-10-05 (SG-04, `25-Retry-Backoff.md`); `LastFailure` doesn't survive a freeze or cancellation between the failure and the fallback.
- The `execute_node_step` / `Checkpoint<S>` / `ICheckpointStore<TState>` / `StateCheckpoint<TState>` shapes in the blueprint — the real types are `CheckpointStore` / `ICheckpointStore` and `schema::Checkpoint` (see `design/03-Architecture.md` and `schema/`).
