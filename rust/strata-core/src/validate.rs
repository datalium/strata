//! Compile-time graph validation (STRATA-SPEC-004).
//!
//! Runs Tarjan's Strongly Connected Components algorithm over a
//! [`GraphDefinition`] to catch the structural error classes the spec
//! calls out: a missing entry point, orphan (unreachable) nodes, and
//! unguarded cycles, plus dangling edges (an edge naming something that is
//! not a node). This is the same function both `StateGraph::compile`
//! (`graph.rs`) and the `strata graph validate` CLI command call, so a
//! graph accepted/rejected by one is accepted/rejected by the other.
//!
//! It recurses (`design/22-Graph-Structure-In-Definitions.md`): each parallel
//! group must have at least one branch, with unique names, and each subgraph's
//! child definition is validated as a graph in its own right, its errors
//! naming nodes by their qualified path (`Remediate/Abort`), the path run
//! events use.

use std::collections::{HashMap, HashSet};

use crate::parallel::JoinKind;
use crate::schema::{GraphDefinition, GraphNodeDef, GraphNodeKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValidationErrorKind {
    MissingEntryPoint,
    OrphanNode,
    UnguardedCycle,
    DanglingEdge,
    /// A parallel group with no branches.
    EmptyGroup,
    /// Two branches of one parallel group share a name.
    DuplicateBranch,
    /// A parallel group's quorum join is missing its count, or it is below 1
    /// or above the branch count (`design/26-Parallel-Group-Controls-And-Joins.md`);
    /// or a map's is missing or below 1 (`design/27-Map-Fan-Out.md`).
    InvalidQuorum,
    /// A wait node inside a map's template, at any depth
    /// (`design/29-Event-Waits-And-Timers.md`): a map item can't freeze.
    WaitInMap,
    /// An interrupt boundary inside a map's template, at any depth
    /// (`design/48-Boundaries-In-Definitions.md`): a map item can't freeze.
    BoundaryInMap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub kind: ValidationErrorKind,
    pub code: &'static str,
    pub node: String,
    pub related_node: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValidationResult {
    pub errors: Vec<ValidationError>,
}

impl ValidationResult {
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Clone)]
struct AdjacentEdge {
    to: String,
    guarded: bool,
}

/// Every error in `graph`, at every level, in this order: the level's own
/// (entry point, dangling edges, orphans, unguarded cycles), then its groups'
/// in node order, then each subgraph child's, in node order. A level whose
/// entry point is missing reports only that, as before.
pub fn validate(graph: &GraphDefinition) -> ValidationResult {
    let mut result = ValidationResult::default();
    validate_level(graph, &Qualify(""), false, &mut result);
    result
}

/// Qualifies a level's node names with the path of the subgraph node it is
/// the child of: `Abort` inside `Remediate` is `Remediate/Abort`. The top
/// level's prefix is empty, so its errors read exactly as they always have.
struct Qualify<'a>(&'a str);

impl Qualify<'_> {
    fn name(&self, name: &str) -> String {
        format!("{}{name}", self.0)
    }
}

/// Validates one level. `in_map`: the level is inside a map's template, at any
/// depth, where a wait node is an error (`design/29-Event-Waits-And-Timers.md`).
fn validate_level(
    graph: &GraphDefinition,
    q: &Qualify<'_>,
    in_map: bool,
    result: &mut ValidationResult,
) {
    let node_names: HashSet<&str> = graph.nodes.iter().map(|n| n.name.as_str()).collect();
    if !node_names.contains(graph.entry_point.as_str()) {
        let entry = q.name(&graph.entry_point);
        result.errors.push(ValidationError {
            kind: ValidationErrorKind::MissingEntryPoint,
            code: "STRATA_ERR_MISSING_ENTRY",
            message: format!("Entry point node '{entry}' does not exist in the graph definition."),
            node: entry,
            related_node: None,
        });
        return;
    }

    add_dangling_edge_errors(graph, &node_names, q, result);

    // Only edges between two real nodes are followed by the passes below, so
    // an edge that dangles is reported once, above, and can't also make a
    // name that is not a node look reachable or part of a cycle.
    let mut adjacency: HashMap<String, Vec<AdjacentEdge>> = graph
        .nodes
        .iter()
        .map(|n| (n.name.clone(), Vec::new()))
        .collect();
    for edge in &graph.edges {
        if !node_names.contains(edge.from.as_str()) || !node_names.contains(edge.to.as_str()) {
            continue;
        }
        adjacency
            .entry(edge.from.clone())
            .or_default()
            .push(AdjacentEdge {
                to: edge.to.clone(),
                guarded: edge.guarded,
            });
    }

    add_orphan_errors(graph, &adjacency, q, result);
    add_unguarded_cycle_errors(graph, &adjacency, q, result);
    add_group_errors(graph, q, result);
    if in_map {
        add_wait_in_map_errors(graph, q, result);
        add_boundary_in_map_errors(graph, q, result);
    }

    // A map's template runs once per item, so its nodes are named with a `*`
    // for the item (`design/27-Map-Fan-Out.md`).
    for node in &graph.nodes {
        if let Some(child) = &node.graph {
            let map = node.kind == Some(GraphNodeKind::Map);
            let item = if map { "/*" } else { "" };
            let prefix = format!("{}{item}/", q.name(&node.name));
            validate_level(child, &Qualify(&prefix), in_map || map, result);
        }
    }
}

/// Every wait node of a level inside a map's template, in node order: a map
/// item can't freeze (`design/29-Event-Waits-And-Timers.md`).
fn add_wait_in_map_errors(graph: &GraphDefinition, q: &Qualify<'_>, result: &mut ValidationResult) {
    for node in &graph.nodes {
        if node.kind == Some(GraphNodeKind::Wait) {
            let wait = q.name(&node.name);
            result.errors.push(ValidationError {
                kind: ValidationErrorKind::WaitInMap,
                code: "STRATA_ERR_WAIT_IN_MAP",
                message: format!(
                    "Wait node '{wait}' is inside a map's template; a map item can't wait."
                ),
                node: wait,
                related_node: None,
            });
        }
    }
}

/// Every interrupt boundary of a level inside a map's template, in node order:
/// a map item can't freeze (`design/48-Boundaries-In-Definitions.md`).
fn add_boundary_in_map_errors(
    graph: &GraphDefinition,
    q: &Qualify<'_>,
    result: &mut ValidationResult,
) {
    for node in &graph.nodes {
        if node.interrupt.is_some() {
            let boundary = q.name(&node.name);
            result.errors.push(ValidationError {
                kind: ValidationErrorKind::BoundaryInMap,
                code: "STRATA_ERR_BOUNDARY_IN_MAP",
                message: format!(
                    "Interrupt boundary '{boundary}' is inside a map's template; a map item can't freeze."
                ),
                node: boundary,
                related_node: None,
            });
        }
    }
}

/// For each parallel group, in node order: no branches at all, or one error
/// per branch name used more than once (in the order each first repeats). For
/// each map, a quorum below 1.
fn add_group_errors(graph: &GraphDefinition, q: &Qualify<'_>, result: &mut ValidationResult) {
    for node in &graph.nodes {
        if node.kind == Some(GraphNodeKind::Map) {
            add_map_errors(node, q, result);
            continue;
        }
        if node.kind != Some(GraphNodeKind::ParallelGroup) {
            continue;
        }
        let group = q.name(&node.name);
        let branches = node.branches.as_deref().unwrap_or_default();
        if branches.is_empty() {
            result.errors.push(ValidationError {
                kind: ValidationErrorKind::EmptyGroup,
                code: "STRATA_ERR_EMPTY_GROUP",
                message: format!("Parallel group '{group}' has no branches."),
                node: group,
                related_node: None,
            });
            continue;
        }
        let mut seen: HashSet<&str> = HashSet::new();
        let mut reported: HashSet<&str> = HashSet::new();
        for branch in branches {
            let name = branch.name.as_str();
            if !seen.insert(name) && reported.insert(name) {
                result.errors.push(ValidationError {
                    kind: ValidationErrorKind::DuplicateBranch,
                    code: "STRATA_ERR_DUPLICATE_BRANCH",
                    node: group.clone(),
                    related_node: Some(name.to_string()),
                    message: format!(
                        "Parallel group '{group}' has more than one branch named '{name}'."
                    ),
                });
            }
        }
        // design/26-Parallel-Group-Controls-And-Joins.md: a quorum must be reachable.
        if let Some(join) = &node.join
            && join.policy == JoinKind::Quorum
            && !join
                .count
                .is_some_and(|count| count >= 1 && count <= branches.len())
        {
            let stated = join
                .count
                .map_or_else(|| "none".to_string(), |count| count.to_string());
            result.errors.push(ValidationError {
                kind: ValidationErrorKind::InvalidQuorum,
                code: "STRATA_ERR_INVALID_QUORUM",
                message: format!(
                    "Parallel group '{group}' has a quorum of {stated}, which isn't between 1 and its {} branches.",
                    branches.len()
                ),
                node: group,
                related_node: None,
            });
        }
    }
}

/// A map's quorum must be at least 1 (`design/27-Map-Fan-Out.md`). Its items
/// are counted at run time, so there is no upper bound here: a quorum above
/// them is a join that can't be met.
fn add_map_errors(node: &GraphNodeDef, q: &Qualify<'_>, result: &mut ValidationResult) {
    if let Some(join) = &node.join
        && join.policy == JoinKind::Quorum
        && join.count.is_none_or(|count| count < 1)
    {
        let map = q.name(&node.name);
        let stated = join
            .count
            .map_or_else(|| "none".to_string(), |count| count.to_string());
        result.errors.push(ValidationError {
            kind: ValidationErrorKind::InvalidQuorum,
            code: "STRATA_ERR_INVALID_QUORUM",
            message: format!("Map '{map}' has a quorum of {stated}, which isn't at least 1."),
            node: map,
            related_node: None,
        });
    }
}

/// One error per edge with an endpoint that is not a node, in edge order.
/// `node` is the edge's `from` and `related_node` its `to`, as for a cycle.
fn add_dangling_edge_errors(
    graph: &GraphDefinition,
    node_names: &HashSet<&str>,
    q: &Qualify<'_>,
    result: &mut ValidationResult,
) {
    for edge in &graph.edges {
        let exists = (
            node_names.contains(edge.from.as_str()),
            node_names.contains(edge.to.as_str()),
        );
        let (from, to) = (q.name(&edge.from), q.name(&edge.to));
        let message = match exists {
            (true, true) => continue,
            (true, false) => format!(
                "Edge '{from}' -> '{to}' points at '{to}', which is not a node in the graph."
            ),
            (false, true) => format!(
                "Edge '{from}' -> '{to}' starts at '{from}', which is not a node in the graph."
            ),
            (false, false) => format!(
                "Edge '{from}' -> '{to}' connects '{from}' and '{to}', neither of which is a node in the graph."
            ),
        };
        result.errors.push(ValidationError {
            kind: ValidationErrorKind::DanglingEdge,
            code: "STRATA_ERR_DANGLING_EDGE",
            node: from,
            related_node: Some(to),
            message,
        });
    }
}

fn add_orphan_errors(
    graph: &GraphDefinition,
    adjacency: &HashMap<String, Vec<AdjacentEdge>>,
    q: &Qualify<'_>,
    result: &mut ValidationResult,
) {
    let mut visited: HashSet<&str> = HashSet::new();
    let mut stack = vec![graph.entry_point.as_str()];

    while let Some(node) = stack.pop() {
        if !visited.insert(node) {
            continue;
        }
        if let Some(edges) = adjacency.get(node) {
            for edge in edges {
                if !visited.contains(edge.to.as_str()) {
                    stack.push(edge.to.as_str());
                }
            }
        }
    }

    for node in &graph.nodes {
        if !visited.contains(node.name.as_str()) {
            let (name, entry) = (q.name(&node.name), q.name(&graph.entry_point));
            result.errors.push(ValidationError {
                kind: ValidationErrorKind::OrphanNode,
                code: "STRATA_ERR_ORPHAN",
                message: format!(
                    "Node '{name}' is isolated and unreachable from the entry point '{entry}'."
                ),
                node: name,
                related_node: None,
            });
        }
    }
}

/// Tarjan's SCC low-link bookkeeping, run over the **unguarded** edges only:
/// a cycle is unguarded exactly when the subgraph of unguarded edges has one,
/// so a guarded edge is not followed at all. We don't need the final component
/// groupings themselves. In that subgraph an edge to a node still `on_stack`
/// is inside a strongly connected component, so it lies on a cycle of
/// unguarded edges, and every such cycle has at least one such edge; errors
/// are raised the moment one is found rather than by re-scanning components
/// afterward. (Following guarded edges too would flag a plain edge that only
/// happens to rejoin a node of a guarded loop.)
#[derive(Default)]
struct Tarjan {
    index: usize,
    indices: HashMap<String, usize>,
    low_links: HashMap<String, usize>,
    on_stack: HashSet<String>,
    stack: Vec<String>,
}

impl Tarjan {
    fn strong_connect(
        &mut self,
        node: &str,
        adjacency: &HashMap<String, Vec<AdjacentEdge>>,
        q: &Qualify<'_>,
        errors: &mut Vec<ValidationError>,
    ) {
        self.indices.insert(node.to_string(), self.index);
        self.low_links.insert(node.to_string(), self.index);
        self.index += 1;
        self.stack.push(node.to_string());
        self.on_stack.insert(node.to_string());

        if let Some(edges) = adjacency.get(node) {
            for edge in edges.iter().filter(|edge| !edge.guarded) {
                let neighbor = edge.to.as_str();
                if !self.indices.contains_key(neighbor) {
                    self.strong_connect(neighbor, adjacency, q, errors);
                    let neighbor_low = self.low_links[neighbor];
                    let current = self.low_links.get_mut(node).unwrap();
                    *current = (*current).min(neighbor_low);
                } else if self.on_stack.contains(neighbor) {
                    let neighbor_index = self.indices[neighbor];
                    let current = self.low_links.get_mut(node).unwrap();
                    *current = (*current).min(neighbor_index);

                    let (from, to) = (q.name(node), q.name(neighbor));
                    errors.push(ValidationError {
                        kind: ValidationErrorKind::UnguardedCycle,
                        code: "STRATA_ERR_UNGUARDED_CYCLE",
                        message: format!(
                            "Unguarded cycle detected between '{from}' and '{to}'. Add a guarded/conditional edge to protect this loop."
                        ),
                        node: from,
                        related_node: Some(to),
                    });
                }
            }
        }

        if self.low_links[node] == self.indices[node] {
            loop {
                let member = self.stack.pop().unwrap();
                self.on_stack.remove(&member);
                if member == node {
                    break;
                }
            }
        }
    }
}

fn add_unguarded_cycle_errors(
    graph: &GraphDefinition,
    adjacency: &HashMap<String, Vec<AdjacentEdge>>,
    q: &Qualify<'_>,
    result: &mut ValidationResult,
) {
    let mut tarjan = Tarjan::default();
    for node in &graph.nodes {
        if !tarjan.indices.contains_key(node.name.as_str()) {
            tarjan.strong_connect(node.name.as_str(), adjacency, q, &mut result.errors);
        }
    }
}

#[cfg(test)]
mod fixture_tests {
    use super::*;
    use std::path::PathBuf;

    fn load(dir: &str, name: &str) -> GraphDefinition {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../schema/fixtures")
            .join(dir)
            .join(name);
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read fixture {}: {}", path.display(), e));
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn interop_sample_graph_is_valid() {
        // The Phase 1 checkpoint/HITL sample graph — its guarded retry edge
        // must not be misflagged as an unguarded cycle.
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../schema/fixtures/graph-definition.sample.json");
        let raw = std::fs::read_to_string(&path).unwrap();
        let graph: GraphDefinition = serde_json::from_str(&raw).unwrap();

        let result = validate(&graph);
        assert!(result.is_valid(), "expected valid, got {:?}", result.errors);
    }

    #[test]
    fn valid_graph_passes_validation() {
        let result = validate(&load("graph-validation", "valid.json"));
        assert!(result.is_valid());
    }

    #[test]
    fn missing_entry_point_is_rejected() {
        let result = validate(&load("graph-validation", "missing-entry-point.json"));
        assert!(!result.is_valid());
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.kind == ValidationErrorKind::MissingEntryPoint)
        );
    }

    #[test]
    fn orphan_node_is_rejected() {
        let result = validate(&load("graph-validation", "orphan-node.json"));
        assert!(!result.is_valid());
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.kind == ValidationErrorKind::OrphanNode && e.node == "B")
        );
    }

    #[test]
    fn unguarded_cycle_is_rejected() {
        let result = validate(&load("graph-validation", "unguarded-cycle.json"));
        assert!(!result.is_valid());
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.kind == ValidationErrorKind::UnguardedCycle)
        );
    }

    #[test]
    fn two_paths_rejoining_inside_a_guarded_loop_are_valid() {
        // A -> B -> D and A -> C -> D, with D -> A guarded. Every cycle passes
        // through the guarded edge; C -> D is only a cross edge into D.
        let result = validate(&load("graph-validation", "guarded-loop-rejoin.json"));
        assert!(result.is_valid(), "expected valid, got {:?}", result.errors);
    }

    #[test]
    fn a_guard_on_the_first_edge_of_a_loop_is_valid() {
        // A -> B guarded, B -> A plain. Which edge the DFS closes the cycle
        // with must not decide the verdict.
        let result = validate(&load("graph-validation", "guarded-first-edge-loop.json"));
        assert!(result.is_valid(), "expected valid, got {:?}", result.errors);
    }

    #[test]
    fn an_unguarded_cycle_inside_a_guarded_loop_is_still_rejected() {
        // The outer loop is guarded (C -> A) but B <-> C is a cycle of plain edges.
        let result = validate(&load(
            "graph-validation",
            "unguarded-cycle-in-guarded-loop.json",
        ));
        assert!(!result.is_valid());
        assert!(result.errors.iter().any(|e| {
            e.kind == ValidationErrorKind::UnguardedCycle
                && e.node == "C"
                && e.related_node.as_deref() == Some("B")
        }));
    }

    fn dangling(result: &ValidationResult) -> Vec<(&str, &str)> {
        result
            .errors
            .iter()
            .filter(|e| e.kind == ValidationErrorKind::DanglingEdge)
            .map(|e| (e.node.as_str(), e.related_node.as_deref().unwrap()))
            .collect()
    }

    #[test]
    fn an_edge_to_a_name_that_is_not_a_node_is_rejected() {
        let result = validate(&load("graph-validation", "dangling-edge-target.json"));
        assert_eq!(dangling(&result), [("B", "Missing")]);
        assert_eq!(result.errors.len(), 1, "got {:?}", result.errors);
        assert_eq!(result.errors[0].code, "STRATA_ERR_DANGLING_EDGE");
    }

    #[test]
    fn an_edge_from_a_name_that_is_not_a_node_is_rejected() {
        // B is reachable through A, so the dangling edge is the only error.
        let result = validate(&load("graph-validation", "dangling-edge-source.json"));
        assert_eq!(dangling(&result), [("Ghost", "B")]);
        assert_eq!(result.errors.len(), 1, "got {:?}", result.errors);
    }

    #[test]
    fn a_name_that_is_not_a_node_does_not_bridge_two_nodes() {
        // A -> Ghost -> B: both edges dangle, and B is not made reachable by
        // a route through a name that is not in the graph.
        let result = validate(&load("graph-validation", "dangling-edge-bridge.json"));
        assert_eq!(dangling(&result), [("A", "Ghost"), ("Ghost", "B")]);
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.kind == ValidationErrorKind::OrphanNode && e.node == "B")
        );
    }

    /// `(code, node, related_node)` of every error, in order.
    fn summary(result: &ValidationResult) -> Vec<(&str, &str, Option<&str>)> {
        result
            .errors
            .iter()
            .map(|e| (e.code, e.node.as_str(), e.related_node.as_deref()))
            .collect()
    }

    #[test]
    fn a_valid_hierarchy_passes_at_every_level() {
        let result = validate(&load("graph-validation", "valid-hierarchy.json"));
        assert!(result.is_valid(), "expected valid, got {:?}", result.errors);
    }

    #[test]
    fn an_orphan_inside_a_subgraph_is_named_by_its_qualified_path() {
        let result = validate(&load("graph-validation", "child-orphan.json"));
        assert_eq!(
            summary(&result),
            [("STRATA_ERR_ORPHAN", "Remediate/Abort", None)]
        );
        assert_eq!(
            result.errors[0].message,
            "Node 'Remediate/Abort' is isolated and unreachable from the entry point 'Remediate/Plan'."
        );
    }

    #[test]
    fn an_unguarded_cycle_two_levels_down_is_qualified_at_both_levels() {
        let result = validate(&load("graph-validation", "grandchild-unguarded-cycle.json"));
        assert_eq!(
            summary(&result),
            [(
                "STRATA_ERR_UNGUARDED_CYCLE",
                "Remediate/Fix/Verify",
                Some("Remediate/Fix/Apply")
            )]
        );
    }

    #[test]
    fn a_group_with_no_branches_is_rejected() {
        let result = validate(&load("graph-validation", "empty-group.json"));
        assert_eq!(
            summary(&result),
            [("STRATA_ERR_EMPTY_GROUP", "Investigate", None)]
        );
        assert_eq!(result.errors[0].kind, ValidationErrorKind::EmptyGroup);
    }

    #[test]
    fn a_group_with_two_branches_of_one_name_is_rejected() {
        let result = validate(&load("graph-validation", "duplicate-branch.json"));
        assert_eq!(
            summary(&result),
            [(
                "STRATA_ERR_DUPLICATE_BRANCH",
                "Investigate",
                Some("Kubernetes")
            )]
        );
        assert_eq!(result.errors[0].kind, ValidationErrorKind::DuplicateBranch);
    }

    #[test]
    fn errors_come_level_first_then_groups_then_children() {
        let result = validate(&load("graph-validation", "errors-at-every-level.json"));
        assert_eq!(
            summary(&result),
            [
                ("STRATA_ERR_ORPHAN", "Island", None),
                ("STRATA_ERR_DUPLICATE_BRANCH", "G", Some("X")),
                ("STRATA_ERR_ORPHAN", "S/Q", None),
                ("STRATA_ERR_EMPTY_GROUP", "S/H", None),
            ]
        );
    }
}
