## 📋 Architectural Spec: The Graph Compilation & Validation Engine

**Component ID:** STRATA-SPEC-004  
**Status:** PROPOSED

---

### 1. System Overview & The Topological Challenge

Unlike simple linear chains, agentic frameworks like LangGraph allow cyclic graphs where nodes can route back to previous steps based on complex logic conditions. While loops are necessary for agent self-correction, they introduce catastrophic production vulnerabilities if left unvalidated:

- **Infinite Terminal Loops:** An LLM consistently failing a validation criteria can trap the system in an unchecked, infinite loop, draining API tokens and blocking server resources.
- **Orphan Nodes:** Unreachable dead-end blocks left in the code base that complicate deployment paths and increase memory footprints.
- **Missing Terminal Paths:** A workflow topology that accidentally eliminates all possible exits, ensuring that any state entering it remains trapped indefinitely.

Datalium Strata eliminates these runtime risks by turning graph validation into a mandatory compile-time or start-up transaction. The second a developer calls `.Compile()` or `.compile()`, Strata feeds the entire agent map through a static analysis engine powered by graph-theory algorithms. If the graph contains cycle traps or unreachable regions, the system aborts start-up with an explicit diagnostic error log.

---

### 2. Topological Analysis & Validation Pipeline

```text
[ Developer Graph Code ] ──► Call .Compile()
                                │
                 ┌──────────────┴──────────────┐
                 ▼                             ▼
       [ Structural Check ]          [ Mathematical Check ]
       • Missing Entry Point         • Tarjan's Strongly Connected Comps
       • Orphaned Dead Ends          • Cycle Multiplicity Analysis
       • Missing Key Reductions      • Deadlock Trap Verifications
                 │                             │
                 └──────────────┬──────────────┘
                                ▼
         [ Verdict: Valid? ] ───┼───( No )───► [ Abort System Startup ]
                                │              • Output Detailed Line Error Map
                                │              • Block Executable Assembly Run
                                ▼ ( Yes )
                 [ Emit Native Optimization Memory Map ]
```

### 3. C# Validation Engine Design (Datalium.Strata.Validation)

In .NET, graph parsing is handled during service initialization or via an optional MSBuild task. We map nodes out into adjacency lists and run non-allocating, recursive depth-first searches (DFS) to identify structural errors.

#### Core Validation Framework Models

```csharp
namespace Datalium.Strata.Validation;

public record ValidationError(string NodeName, string ErrorCode, string Message);

public class ValidationResult
{
    public bool IsValid => Errors.Count == 0;
    public List<ValidationError> Errors { get; } = new();
}

public interface IGraphValidator<TState>
{
    ValidationResult Validate(GraphTopology<TState> topology);
}
```

#### Deep-Dive Adjacency Compilation & Cycle Detection

```csharp
public class StructuralGraphValidator<TState> : IGraphValidator<TState>
{
    public ValidationResult Validate(GraphTopology<TState> topology)
    {
        var result = new ValidationResult();

        // 1. Structural Sanity Check: Ensure entry point exists
        if (!topology.Nodes.ContainsKey(topology.EntryPoint))
        {
            result.Errors.Add(new ValidationError(topology.EntryPoint, "STRATA_ERR_MISSING_ENTRY", "Defined entry point node does not exist in graph definition."));
            return result;
        }

        // 2. Traversal Trace: Discover Orphans and Cycles via DFS
        var visited = new Dictionary<string, NodeVisitState>();
        foreach (var node in topology.Nodes.Keys) visited[node] = NodeVisitState.Unvisited;

        // Run cycle detection across the topology
        DetectCycles(topology.EntryPoint, topology, visited, result);

        // 3. Discover Abandoned Branches (Orphan Check)
        foreach (var node in visited)
        {
            if (node.Value == NodeVisitState.Unvisited)
            {
                result.Errors.Add(new ValidationError(node.Key, "STRATA_ERR_ORPHAN", $"Node '{node.Key}' is isolated and completely unreachable from the entry point loop."));
            }
        }

        return result;
    }

    private void DetectCycles(string node, GraphTopology<TState> topology, Dictionary<string, NodeVisitState> visited, ValidationResult result)
    {
        visited[node] = NodeVisitState.Visiting;

        foreach (var edge in topology.GetEdgesFrom(node))
        {
            if (visited[edge] == NodeVisitState.Visiting)
            {
                // Unchecked loop verified. Ensure a conditional router or retry threshold protects this path!
                if (!topology.HasGuardCondition(node, edge))
                {
                    result.Errors.Add(new ValidationError(node, "STRATA_ERR_UNGUARDED_CYCLE", $"Unprotected cycle feedback track detected between '{node}' and '{edge}'. This path risks an unmitigated infinite loop."));
                }
            }
            else if (visited[edge] == NodeVisitState.Unvisited)
            {
                DetectCycles(edge, topology, visited, result);
            }
        }

        visited[node] = NodeVisitState.Visited;
    }

    private enum NodeVisitState { Unvisited, Visiting, Visited }
}
```

### 4. Rust Validation Engine Design (datalium-strata-validation)

In Rust, the topology structure leverages zero-cost compiler references, checking the graph layout at initialization using a specialized graph validation module.

#### Compilation Trait Realization

```rust
use std::collections::{HashMap, HashSet};

pub struct GraphTopology {
    pub entry_point: String,
    pub adjacency_list: HashMap<String, Vec<String>>,
    pub guarded_edges: HashSet<(String, String)>,
}

impl GraphTopology {
    pub fn compile_and_validate(&self) -> Result<(), CompilationError> {
        // 1. Verify Entry Point
        if !self.adjacency_list.contains_key(&self.entry_point) {
            return Err(CompilationError::InvalidEntryPoint(self.entry_point.clone()));
        }

        // 2. Execute Cycle Detection via Tarjan's Strongly Connected Components Algorithm
        let mut index = 0;
        let mut stack = Vec::new();
        let mut node_meta: HashMap<String, TarjanMeta> = HashMap::new();

        for node in self.adjacency_list.keys() {
            if !node_meta.contains_key(node) {
                self.strongly_connected_components(node, &mut index, &mut stack, &mut node_meta)?;
            }
        }

        Ok(())
    }

    fn strongly_connected_components(
        &self,
        node: &str,
        index: &mut usize,
        stack: &mut Vec<String>,
        meta: &mut HashMap<String, TarjanMeta>,
    ) -> Result<(), CompilationError> {
        meta.insert(node.to_string(), TarjanMeta::new(*index));
        *index += 1;
        stack.push(node.to_string());

        if let Some(neighbors) = self.adjacency_list.get(node) {
            for neighbor in neighbors {
                if !meta.contains_key(neighbor) {
                    self.strongly_connected_components(neighbor, index, stack, meta)?;

                    let neighbor_low = meta.get(neighbor).unwrap().low_link;
                    let current_meta = meta.get_mut(node).unwrap();
                    current_meta.low_link = current_meta.low_link.min(neighbor_low);
                } else if stack.contains(&neighbor.to_string()) {
                    let neighbor_index = meta.get(neighbor).unwrap().index;
                    let current_meta = meta.get_mut(node).unwrap();
                    current_meta.low_link = current_meta.low_link.min(neighbor_index);

                    // Evaluate if cycle link is backed by a structural loop limit/guard rule
                    if !self.guarded_edges.contains(&(node.to_string(), neighbor.to_string())) {
                        return Err(CompilationError::UnguardedLoop(node.to_string(), neighbor.to_string()));
                    }
                }
            }
        }

        Ok(())
    }
}

struct TarjanMeta { pub index: usize, pub low_link: usize }

impl TarjanMeta { fn new(idx: usize) -> Self { Self { index: idx, low_link: idx } } }
```

### 5. Cross-Language Edge Cases & Mitigations

- **Loop Escape Guards (The Runaway Agent Prevention):** Even if a loop is syntactically guarded by a conditional router, the LLM may experience cognitive drift and repeat the loop indefinitely.
    - **Mitigation:** Strata forces global circuit-breakers onto the graph compilation configuration. Developers specify a global upper limit parameter for single execution transactions:

```csharp
// Abort the transaction automatically if any runtime thread processes more than 25 total node hops
var runner = graph.Compile(new CompilationOptions { MaxNodeHops = 25 });
```

- **CI/CD Pipeline Validation Rules via CLI:** Finding topological path errors during product deployment is highly disruptive to system availability.
    - **Mitigation:** The `strata` CLI exposes a static validation rule command that parses JSON workflow layout outputs. It evaluates architectural logic without initializing live model configurations:

```bash
# Returns exit code 0 on healthy layouts, or exit code 1 with explicit structural lines on breaks
strata graph validate --path ./src/workflow.json
```

---

## ✅ Implementation Status (as of demo 14, 2026-09-21)

**Built:** `.compile()` / `.Compile()` and `strata graph validate` run the same validator over the same graph-definition shape, so they return the same diagnostics (see `design/05.Strata-CLI.md`, "Implementation Status", for the CLI and for exporting a graph to the file it reads). The validator implements four structural error classes: a missing entry point (`STRATA_ERR_MISSING_ENTRY`), orphan nodes (`STRATA_ERR_ORPHAN`), unguarded cycles (`STRATA_ERR_UNGUARDED_CYCLE`) and dangling edges (`STRATA_ERR_DANGLING_EDGE`, added 2026-09-21). A recovery-policy fallback counts as a guarded edge (`design/04-Error-Recovery.md`).

**Dangling edges:** an edge whose `from` or `to` is not a node is reported once per edge, in edge order, with `node` = the edge's `from` and `related_node` = its `to` (the CLI prints `[STRATA_ERR_DANGLING_EDGE] from -> to: message`). Until then the validator skipped such an edge, so an `add_edge("A", "Nope")` or a recovery-policy fallback to a missing node passed `strata graph validate` (a fallback to a missing node was still rejected by `.compile()`, as `CompilationFailed`, before it validated anything). A dangling edge is then left out of the graph the orphan and cycle passes walk: a name that is not a node cannot make anything reachable or close a cycle, so `A -> Ghost -> B` reports both edges and also reports `B` as an orphan. The entry-point check still runs first and returns alone. Fixtures `dangling-edge-target`, `dangling-edge-source` and `dangling-edge-bridge` in `schema/fixtures/graph-validation/` are read by both engines and the CLI. There is no end-of-graph sentinel node, so no edge to a non-node is legitimate; a node with no outgoing edge is how a graph ends.

**The rule (both engines):** a graph is rejected for a cycle only if the subgraph of its *unguarded* edges has one. Tarjan's algorithm runs over the unguarded edges alone; a guarded edge is not followed, so it can never make a cycle unguarded. Orphan detection still follows every edge. Fixed 2026-09-21; until then the validator followed guarded edges too and flagged any ordinary edge into a node still on Tarjan's stack, which rejected two shapes that are fine:

```text
A -> B -> D        A -> C -> D         D -> A (guarded)     two paths rejoining inside a guarded loop
A -> B (guarded)   B -> A                                    a guard on the loop's first edge
```

(The first was rejected as `STRATA_ERR_UNGUARDED_CYCLE C -> D`, the second as `B -> A`, depending on which edge the depth-first search happened to close the loop with.) The three fixtures under `schema/fixtures/graph-validation/` (`guarded-loop-rejoin`, `guarded-first-edge-loop`, and `unguarded-cycle-in-guarded-loop`, which must still be rejected as `C -> B`) are read by both engines. A guard on a loop of plain edges nested inside another guarded loop does not excuse the inner one.

**Loop escape guard (§5), built 2026-10-04:** `graph.Compile(new CompilationOptions { MaxNodeHops = 25 })` exists as written, and Rust has `compile_with(CompileOptions::new().with_max_node_hops(25))`; `Compile()` / `compile()` are unchanged and set no limit. Each compiled graph counts the nodes it starts (a retry is not a hop; a fallback node, a parallel group and a subgraph are one each) and stops before the one that would exceed the limit, with `GraphExecutionResult.HopLimitExceeded` / `StrataError::HopLimitExceeded` (both refine `Failed`) and a `failed` checkpoint whose `error_kind` is `hop_limit`. The count survives interrupts, cancellation and resumes in the checkpoint's metadata (`strata.hops`), with no schema change, and a run can replace the limit (`RunOptions.MaxNodeHops` / `RunConfig::with_max_node_hops`). The spec's "any runtime thread" is per graph level: a subgraph's child enforces its own limit. See `design/24-Loop-Circuit-Breaker.md`. The limit is not in the exported graph definition, so `strata graph validate` doesn't see it.

**Parallel group checks:** since SG-09 (`design/22-Graph-Structure-In-Definitions.md`) the validator recurses into groups and subgraphs and reports `STRATA_ERR_EMPTY_GROUP` and `STRATA_ERR_DUPLICATE_BRANCH`; since SG-07/08 (`design/26-Parallel-Group-Controls-And-Joins.md`, 2026-10-05) it also reports `STRATA_ERR_INVALID_QUORUM`, a quorum join whose count is missing, below 1 or above the group's branch count, so `.compile()` and `strata graph validate` refuse a group that could never be done. Since SG-06 (`design/27-Map-Fan-Out.md`, 2026-10-06) it recurses into a map's template too, naming its nodes with a `*` segment for the item (`Investigate/*/Assess`), and reports a map's quorum below 1 as `STRATA_ERR_INVALID_QUORUM` (a map's items are counted at run time, so there is no upper bound). `.compile()` also refuses a map whose child graph has an interrupt boundary at any depth (a `CompilationFailed` / `InvalidOperationException`, not a validation error: the exported definition doesn't record boundaries). A map is one hop.

**Not built:** the spec's "missing terminal paths" check (a graph with no exit passes if it has no orphan and no unguarded cycle).
