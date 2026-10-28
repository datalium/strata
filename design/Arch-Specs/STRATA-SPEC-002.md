## 📋 Architectural Spec: Human-in-the-Loop (HITL) Interruption Schema

**Component ID:** STRATA-SPEC-002  
**Status:** PROPOSED

---

### 1. System Overview & The Interruption Challenge

Autonomous AI agents running in production require safeguards when executing high-risk actions (e.g., executing financial transfers, deleting database records, or sending customer-facing emails).

Traditional multi-agent frameworks handle interruptions by blocking thread execution or relying on complex internal state flags. This introduces major vulnerabilities:

- **Resource Leaks:** Holding an active execution thread open while waiting for human input wastes server memory and connections.
- **State Corruption:** If the application server restarts or crashes during a long human wait period, the active state of that agent run is lost forever.

Datalium Strata implements an asynchronous Freeze, Flush, and Evict design pattern. When a graph hits a predefined human validation checkpoint, it treats the interruption as a transactional state shift. The graph saves its progress atomically, captures any input data, and terminates the active thread safely. The graph is reloaded and resumed only when an authorized human provides an approval signal.

---

### 2. Human-in-the-Loop State Machine Lifecycle

```text
   ┌────────────────────────────────────────────────────────┐
   │            Active Node Execution Pipeline              │
   └───────────────────────────┬────────────────────────────┘
                               │ Node completes successfully
                               ▼
   ┌────────────────────────────────────────────────────────┐
   │         Interruption Boundary Detector Engine          │
   └───────────────────────────┬────────────────────────────┘
                               │ Boundary Detected
                               ▼
   ┌────────────────────────────────────────────────────────┐
   │    1. FREEZE: Halt subsequent graph loops              │
   │    2. FLUSH: Write state + Next Node ID to DB          │
   │    3. EVICT: Clear task context from live RAM memory   │
   └───────────────────────────┬────────────────────────────┘
                               │ Thread returned to Pool
                               ▼
                    [ System Idle State ]
              (Zero active resource consumption)
                               │
                               ▼ Human Action Received via API
   ┌────────────────────────────────────────────────────────┐
   │    4. REHYDRATE: Pull snapshot variables from DB       │
   │    5. RESUME: Inject approval data & run Next Node     │
   └────────────────────────────────────────────────────────┘
```

### 3. C# Interruption Engine Design (Datalium.Strata.Interruption)

In .NET, we leverage explicit exception-less control flows via dedicated engine routing results. The graph definition uses a fluent API to specify which nodes require explicit authorization boundaries.

#### Core Types & Engine Models

```csharp
namespace Datalium.Strata.Interruption;

public enum InterruptStatus { PendingApproval, Approved, Rejected }

public record HumanFeedback(
    InterruptStatus Status,
    string ApproverId,
    string Notes,
    Dictionary<string, string> ContextOverrides
);

public interface IInterruptibleRunner<TState>
{
    Task<StateCheckpoint<TState>> SuspendAsync(string threadId, TState state, string nextNode);
    Task<TState> ResumeAsync(string threadId, HumanFeedback feedback);
}
```

#### Fluent Graph Node & Approval Logic Implementation

```csharp
// 1. Declare graph with an explicit interruption gate
var graph = new StateGraph<OrderState>(initialState)
    .AddNode("PrepareWireTransfer", async (state, ctx) =>
    {
        return state with { InvoiceGenerated = true };
    })
    .AddInterruptBoundary("PrepareWireTransfer") // Engine forces suspension immediately following this step
    .AddNode("ExecuteWireTransfer", async (state, ctx) =>
    {
        // 2. Safely read metadata injected by the human approval step
        var approvedBy = ctx.Metadata["ApproverId"];
        await ctx.Services.BankGateway.SendFundsAsync(state.Amount, approvedBy);
        return state with { IsCompleted = true };
    });

// 3. Simulating an external API controller handling the resumption event
public class AgentApprovalController : ControllerBase
{
    private readonly IInterruptibleRunner<OrderState> _strataRunner;

    [HttpPost("threads/{threadId}/resume")]
    public async Task<IActionResult> ApproveAgentStep(string threadId, [FromBody] HumanFeedback feedback)
    {
        // Engine rehydrates frozen state, binds human metrics to context metadata, and resumes
        var updatedState = await _strataRunner.ResumeAsync(threadId, feedback);
        return Ok(updatedState);
    }
}
```

### 4. Rust Interruption Engine Design (datalium-strata-interruption)

In Rust, we lean heavily into Strongly-Typed Enum State Machines. This ensures that an interrupted thread cannot be unsafely executed by the runtime until its validation token is verified.

#### Core Engine Architecture

```rust
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum GraphExecutionResult<S> {
    Completed(S),
    Interrupted {
        thread_id: String,
        suspended_node: String,
        next_node: String,
        frozen_state: S,
    },
    Failed(StrataError),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HumanSignal {
    pub is_approved: bool,
    pub reviewer_token: String,
    pub field_overrides: HashMap<String, String>,
}
```

#### Node Routing & Execution Engine Implementation

```rust
// Core state evaluation engine runner loop step
pub async fn process_graph_step<S: Clone>(
    manager: &ExecutionManager,
    mut state: S,
    node_name: &str,
    has_interrupt_gate: bool,
) -> GraphExecutionResult<S> {

    // Execute target business logic safely
    state = match manager.execute_node(node_name, state).await {
        Ok(s) => s,
        Err(e) => return GraphExecutionResult::Failed(e),
    };

    // Evaluate gate status
    if has_interrupt_gate {
        // Transactionally commit state to DB with a Suspended flag & evict thread from RAM
        if let Err(e) = manager.persist_suspension(node_name, &state).await {
            return GraphExecutionResult::Failed(e);
        }

        return GraphExecutionResult::Interrupted {
            thread_id: manager.thread_id.clone(),
            suspended_node: node_name.to_string(),
            next_node: manager.get_next_node_id(node_name),
            frozen_state: state,
        };
    }

    GraphExecutionResult::Completed(state)
}
```

### 5. Cross-Language Edge Cases & Mitigations

- **Stale State Anomalies (The Time Gap Problem):** A human might take 3 days to hit "Approve." During that time, external production variables (like inventory levels or exchange rates) could change.
    - **Mitigation:** Strata includes a `ValidationGuard` hook on resumption. When `ResumeAsync()` or `resume()` is executed, the engine runs an optional validator node to confirm that the frozen state metrics remain valid before transitioning to the next node.
- **CLI Inspection Capabilities:** Operational support teams need to be able to evaluate stuck threads quickly without accessing a web panel.

- Mitigation: The strata CLI supports specialized interruption querying:

```bash
# Identify threads awaiting user authorization loops
strata thread list --status suspended

# Manually push a mock authorization token to a thread to unblock a system bypass
strata thread resume tx_9b1a4c8e --approve --user admin_01
```

---

### Implementation Status (2026-10-01)

The spec above is the original proposal; read this before trusting its snippets.

- **Built, in both engines:** `add_interrupt_boundary` / `AddInterruptBoundary`, Freeze/Flush/Evict, `GraphExecutionResult::Interrupted` / `.Interrupted`, and `resume` / `ResumeAsync` with a `HumanSignal` whose `field_overrides` are merged into the frozen state. Checkpoints use the shared `schema/checkpoint.schema.json`.
- **Execution is asynchronous** since `design/17-Async-Execution.md`: Rust's `run` / `resume` are `async fn`s taking an `Arc<dyn CheckpointStore>`, and .NET's `RunAsync` / `ResumeAsync` are genuinely async. "Evict" means nothing of the run is still awaited once `Interrupted` is returned. The checkpoint store itself is still synchronous (SG-11).
- **Beyond the spec:** interrupt boundaries inside a subgraph (`design/16-Nested-Interrupts.md`), resuming a `failed` thread, and a `cancelled` outcome with its own checkpoint status, resumed the same way (`design/17-Async-Execution.md`).
- **Typed signals and decisions** (`design/21-Typed-Human-Signals.md`): overrides with their JSON type beside the string `field_overrides`; the answer to a boundary recorded as a `HumanDecision` that later nodes see and checkpoints carry (`schema_version` `1.3.0`); and an optional reject route per boundary, taken when `is_approved` is false. The spec's `InterruptStatus` and `Notes` were not built: a rejection reason is an override.
- **Not built:** the `ValidationGuard` resume hook, and resuming from the CLI (`strata thread resume`); the CLI is read-only by design (`design/16-Nested-Interrupts.md`, "Not built"). `strata thread list --status` filters on the stored status (`interrupted`, `failed`, `cancelled`, `completed`), not `suspended`.
