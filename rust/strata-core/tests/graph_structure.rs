//! Graph structure in definitions (`design/22-Graph-Structure-In-Definitions.md`,
//! SG-09): node and branch metadata, groups and subgraphs described in the
//! export, recursive validation with qualified names, and run-event paths that
//! resolve in the export. Mirrors
//! `dotnet/tests/Datalium.Strata.Tests/GraphStructureTests.cs`; keep the two in
//! step. Both build the reference graph below and compare its export to
//! `schema/fixtures/graph-export/hierarchy.json`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use datalium_strata::checkpoint::{
    CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::{Checkpoint, GraphDefinition, GraphNodeKind};
use datalium_strata::validate::validate;
use datalium_strata::{BoxFuture, StrataError};
use serde_json::Value;
use tokio::sync::mpsc;

fn noop(s: i32) -> i32 {
    s
}

fn ok(s: &i32) -> Result<i32, NodeError> {
    Ok(*s)
}

fn fixture(name: &str) -> Value {
    let path = format!(
        "{}/../../schema/fixtures/graph-export/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&raw).unwrap()
}

fn meta(pairs: &[(&str, &str)]) -> Option<BTreeMap<String, String>> {
    Some(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    )
}

/// `Apply` -> `Verify`, two levels down.
fn fix() -> CompiledGraph<i32> {
    StateGraph::<i32>::new()
        .add_node("Apply", noop)
        .add_node("Verify", noop)
        .add_edge("Apply", "Verify")
        .set_node_metadata("Apply", [("tool", "kubectl")])
        .compile()
        .unwrap()
}

/// `Plan` -> `Approve` (a boundary, rejecting to `Abort`) -> `Fix` (a subgraph).
fn remediation() -> CompiledGraph<i32> {
    StateGraph::<i32>::new()
        .add_node("Plan", noop)
        .add_node("Approve", noop)
        .add_subgraph("Fix", fix(), |s: &i32| *s, |_: &i32, child| child)
        .add_node("Abort", noop)
        .add_edge("Plan", "Approve")
        .add_edge("Approve", "Fix")
        .add_interrupt_boundary_with_reject("Approve", "Abort")
        .set_node_metadata("Fix", [("agent", "fixer")])
        .compile()
        .unwrap()
}

/// The design doc's incident graph: `Triage` -> `Investigate` (a group of
/// three branches) -> `Remediate` (the remediation subgraph) -> `Report`.
fn incident() -> StateGraph<i32> {
    let investigate = ParallelBranches::new()
        .branch("Kubernetes", ok)
        .async_branch("Prometheus", |s, _| async move { Ok(s) })
        .branch("GitHub", ok)
        .set_metadata("Kubernetes", [("agent", "kubernetes-investigator")])
        .set_metadata("Prometheus", [("agent", "metrics-investigator")]);

    StateGraph::<i32>::new()
        .add_node("Triage", noop)
        .add_parallel_group("Investigate", investigate, |s: &i32, _| *s)
        .add_subgraph(
            "Remediate",
            remediation(),
            |s: &i32| *s,
            |_: &i32, child| child,
        )
        .add_node("Report", noop)
        .add_edge("Triage", "Investigate")
        .add_edge("Investigate", "Remediate")
        .add_edge("Remediate", "Report")
        // Set out of order: the export sorts keys.
        .set_node_metadata(
            "Triage",
            [
                ("model_policy", "ops-reasoning-standard"),
                ("agent", "triage"),
            ],
        )
        .set_node_metadata("Remediate", [("agent", "remediator")])
}

fn compile_error(graph: StateGraph<i32>) -> String {
    match graph.compile() {
        Err(StrataError::CompilationFailed(message)) => message,
        Err(other) => panic!("expected CompilationFailed, got {other:?}"),
        Ok(_) => panic!("expected compile to fail"),
    }
}

// --- Metadata ---------------------------------------------------------------

#[test]
fn metadata_reaches_the_export_on_every_kind_of_node() {
    let child = StateGraph::<i32>::new()
        .add_node("Only", noop)
        .compile()
        .unwrap();
    let names = [
        "Plain",
        "AsyncPlain",
        "Streaming",
        "AsyncStreaming",
        "Fallible",
        "AsyncFallible",
        "Group",
        "Sub",
    ];
    let mut graph = StateGraph::<i32>::new()
        .add_node("Plain", noop)
        .add_async_node("AsyncPlain", |s, _| async move { s })
        .add_streaming_node("Streaming", |s, _| s)
        .add_async_streaming_node("AsyncStreaming", |s, _| async move { s })
        .add_fallible_node("Fallible", ok)
        .add_async_fallible_node("AsyncFallible", |s, _| async move { Ok(s) })
        .add_parallel_group(
            "Group",
            ParallelBranches::new().branch("Only", ok),
            |s: &i32, _| *s,
        )
        .add_subgraph("Sub", child, |s: &i32| *s, |_: &i32, c| c);
    for pair in names.windows(2) {
        graph = graph.add_edge(pair[0], pair[1]);
    }
    for name in names {
        graph = graph.set_node_metadata(name, [("role", name)]);
    }

    let definition = graph.to_definition();
    for (node, name) in definition.nodes.iter().zip(names) {
        assert_eq!(node.name, name);
        assert_eq!(node.metadata, meta(&[("role", name)]), "{name}");
    }
    graph.compile().unwrap();
}

#[test]
fn metadata_set_again_is_replaced_not_merged() {
    let graph = StateGraph::<i32>::new()
        .add_node("A", noop)
        .set_node_metadata("A", [("agent", "first"), ("team", "ops")])
        .set_node_metadata("A", [("agent", "second")]);

    assert_eq!(
        graph.to_definition().nodes[0].metadata,
        meta(&[("agent", "second")])
    );

    let branches = ParallelBranches::<i32>::new()
        .branch("B", ok)
        .set_metadata("B", [("agent", "first"), ("team", "ops")])
        .set_metadata("B", [("agent", "second")]);
    let group = StateGraph::<i32>::new().add_parallel_group("G", branches, |s: &i32, _| *s);
    let exported = group.to_definition();
    let branch = &exported.nodes[0].branches.as_ref().unwrap()[0];
    assert_eq!(branch.metadata, meta(&[("agent", "second")]));
}

#[test]
fn re_adding_a_node_keeps_its_metadata() {
    let graph = StateGraph::<i32>::new()
        .add_node("A", noop)
        .set_node_metadata("A", [("agent", "triage")])
        .add_fallible_node("A", ok);

    assert_eq!(
        graph.to_definition().nodes[0].metadata,
        meta(&[("agent", "triage")])
    );
}

#[test]
fn metadata_keys_are_exported_sorted() {
    let graph = StateGraph::<i32>::new()
        .add_node("A", noop)
        .set_node_metadata("A", [("zeta", "1"), ("alpha", "2"), ("mid", "3")]);

    let text = serde_json::to_string(&graph.to_definition()).unwrap();
    assert!(
        text.contains(r#""metadata":{"alpha":"2","mid":"3","zeta":"1"}"#),
        "{text}"
    );
}

#[test]
fn compile_refuses_metadata_for_a_name_that_is_not_a_node() {
    let graph = StateGraph::<i32>::new()
        .add_node("A", noop)
        .set_node_metadata("Ghost", [("agent", "x")]);

    assert_eq!(
        compile_error(graph),
        "metadata set for 'Ghost', which does not exist"
    );
}

#[test]
fn compile_refuses_metadata_for_a_branch_the_group_does_not_have() {
    let branches = ParallelBranches::<i32>::new()
        .branch("Real", ok)
        .set_metadata("Ghost", [("agent", "x")]);
    let graph = StateGraph::<i32>::new().add_parallel_group("G", branches, |s: &i32, _| *s);

    assert_eq!(
        compile_error(graph),
        "metadata set for branch 'Ghost' of parallel group 'G', which does not exist"
    );
}

#[test]
fn compile_refuses_an_empty_metadata_key() {
    let graph = StateGraph::<i32>::new()
        .add_node("A", noop)
        .set_node_metadata("A", [("", "x")]);

    assert_eq!(
        compile_error(graph),
        "metadata on node 'A' has an empty key"
    );
}

#[test]
fn compile_refuses_a_reserved_metadata_key_on_a_node_or_a_branch() {
    let graph = StateGraph::<i32>::new()
        .add_node("Triage", noop)
        .set_node_metadata("Triage", [("strata.x", "y")]);
    assert_eq!(
        compile_error(graph),
        "metadata key 'strata.x' on node 'Triage' is reserved"
    );

    let branches = ParallelBranches::<i32>::new()
        .branch("B", ok)
        .set_metadata("B", [("strata.agent", "y")]);
    let graph = StateGraph::<i32>::new().add_parallel_group("G", branches, |s: &i32, _| *s);
    assert_eq!(
        compile_error(graph),
        "metadata key 'strata.agent' on branch 'B' of parallel group 'G' is reserved"
    );
}

#[test]
fn the_export_leaves_out_metadata_for_names_that_are_not_nodes_or_branches() {
    let branches = ParallelBranches::<i32>::new()
        .branch("B", ok)
        .set_metadata("Ghost", [("agent", "x")]);
    let graph = StateGraph::<i32>::new()
        .add_parallel_group("G", branches, |s: &i32, _| *s)
        .set_node_metadata("Ghost", [("agent", "x")]);

    let definition = graph.to_definition();
    assert_eq!(definition.nodes.len(), 1);
    let branches = definition.nodes[0].branches.as_ref().unwrap();
    assert_eq!(branches.len(), 1);
    assert_eq!(branches[0].metadata, None);
}

// --- Export -----------------------------------------------------------------

#[test]
fn the_hierarchy_export_matches_the_shared_fixture() {
    let exported = serde_json::to_value(incident().to_definition()).unwrap();
    assert_eq!(exported, fixture("hierarchy.json"));
}

#[test]
fn a_plain_graph_is_still_1_0_0_with_no_hierarchy_fields() {
    let graph = StateGraph::<i32>::new()
        .add_fallible_node("Charge", ok)
        .add_node("Ship", noop)
        .add_node("Review", noop)
        .add_edge("Charge", "Ship")
        .set_recovery_policy("Charge", RecoveryPolicy::default().with_fallback("Review"));

    let definition = graph.to_definition();
    assert_eq!(definition.schema_version, "1.0.0");
    let text = serde_json::to_string(&definition).unwrap();
    for field in ["\"kind\"", "\"branches\"", "\"graph\""] {
        assert!(!text.contains(field), "{field} in {text}");
    }
}

#[test]
fn a_group_exports_its_branches_in_declaration_order_with_their_metadata() {
    let definition = incident().to_definition();
    let group = &definition.nodes[1];

    assert_eq!(group.kind, Some(GraphNodeKind::ParallelGroup));
    assert!(group.graph.is_none());
    let branches: Vec<(&str, Option<BTreeMap<String, String>>)> = group
        .branches
        .as_ref()
        .unwrap()
        .iter()
        .map(|b| (b.name.as_str(), b.metadata.clone()))
        .collect();
    assert_eq!(
        branches,
        [
            ("Kubernetes", meta(&[("agent", "kubernetes-investigator")])),
            ("Prometheus", meta(&[("agent", "metrics-investigator")])),
            ("GitHub", None),
        ]
    );
}

#[test]
fn a_subgraph_exports_its_child_whole_recursively_with_each_levels_own_version() {
    let definition = incident().to_definition();
    assert_eq!(definition.schema_version, "1.1.0");

    let remediate = &definition.nodes[2];
    assert_eq!(remediate.kind, Some(GraphNodeKind::Subgraph));
    assert!(remediate.branches.is_none());
    let child = remediate.graph.as_deref().unwrap();
    assert_eq!(child, &remediation().to_definition());
    // The child's boundary puts its own level at 1.6.0, not its parent's
    // (design/48-Boundaries-In-Definitions.md).
    assert_eq!(child.schema_version, "1.6.0");
    assert!(
        child
            .edges
            .iter()
            .any(|e| e.from == "Approve" && e.to == "Abort" && e.guarded)
    );

    let grandchild = child.nodes[2].graph.as_deref().unwrap();
    assert_eq!(grandchild.schema_version, "1.0.0");
    assert_eq!(grandchild, &fix().to_definition());
}

#[test]
fn a_compiled_graph_exports_what_its_builder_did() {
    let from_builder = incident().to_definition();
    let compiled = incident().compile().unwrap();

    assert_eq!(compiled.to_definition(), from_builder);
}

#[test]
fn re_adding_a_composite_node_as_a_plain_one_drops_its_structure() {
    let graph = incident()
        .add_node("Remediate", noop)
        .add_node("Investigate", noop);
    let definition = graph.to_definition();

    assert_eq!(definition.schema_version, "1.0.0");
    assert!(definition.nodes.iter().all(|n| n.kind.is_none()));
    // ...but keeps its metadata.
    assert_eq!(
        definition.nodes[2].metadata,
        meta(&[("agent", "remediator")])
    );
}

#[test]
fn the_hierarchy_fixture_round_trips_through_the_definition_types() {
    let raw = fixture("hierarchy.json");
    let definition: GraphDefinition = serde_json::from_value(raw.clone()).unwrap();
    assert_eq!(serde_json::to_value(&definition).unwrap(), raw);
    definition.check_shape().unwrap();
    assert!(validate(&definition).is_valid());
}

// --- Paths ------------------------------------------------------------------

/// Whether a run event's node path names something in `level`: one segment per
/// level, the last a node, a branch of a group, or a node of a subgraph's child.
fn resolves(level: &GraphDefinition, path: &[&str]) -> bool {
    let Some((first, rest)) = path.split_first() else {
        return false;
    };
    let Some(node) = level.nodes.iter().find(|n| n.name == *first) else {
        return false;
    };
    match rest {
        [] => true,
        [branch]
            if node
                .branches
                .as_ref()
                .is_some_and(|bs| bs.iter().any(|b| b.name == *branch)) =>
        {
            true
        }
        _ => node
            .graph
            .as_deref()
            .is_some_and(|child| resolves(child, rest)),
    }
}

#[derive(Default)]
struct TestStore {
    checkpoints: Mutex<HashMap<String, Checkpoint>>,
}

impl CheckpointStore for TestStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.checkpoints
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
        Box::pin(std::future::ready(Ok(())))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        let found = self.checkpoints.lock().unwrap().get(thread_id).cloned();
        Box::pin(std::future::ready(Ok(found)))
    }
}

async fn drain(mut events: mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = events.recv().await {
        all.push(event);
    }
    all
}

/// Every `NodeStarted` and `BranchStarted` path of `events`.
fn started_paths(events: &[RunEvent]) -> Vec<String> {
    events
        .iter()
        .filter(|e| {
            matches!(
                e.kind,
                RunEventKind::NodeStarted | RunEventKind::BranchStarted
            )
        })
        .map(|e| e.node_path.as_deref().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn every_node_and_branch_path_a_run_reports_resolves_in_the_export() {
    let definition = incident().to_definition();
    let graph = incident().compile().unwrap();
    let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());

    let (observer, events) = RunEventChannel::new();
    let first = graph
        .run_with(
            "t",
            1,
            store.clone(),
            RunConfig::new().with_observer(observer),
        )
        .await;
    assert!(matches!(first, GraphExecutionResult::Interrupted { .. }));
    let mut paths = started_paths(&drain(events).await);

    let (observer, events) = RunEventChannel::new();
    let second = graph
        .resume_with(
            "t",
            HumanSignal::approve("ops"),
            store,
            RunConfig::new().with_observer(observer),
        )
        .await;
    assert!(matches!(second, GraphExecutionResult::Completed(1)));
    paths.extend(started_paths(&drain(events).await));

    for expected in [
        "Investigate/Kubernetes",
        "Investigate/GitHub",
        "Remediate/Plan",
        "Remediate/Fix/Apply",
        "Report",
    ] {
        assert!(
            paths.iter().any(|p| p == expected),
            "{expected} in {paths:?}"
        );
    }
    for path in &paths {
        let segments: Vec<&str> = path.split('/').collect();
        assert!(resolves(&definition, &segments), "{path} doesn't resolve");
    }
}

// --- Validation -------------------------------------------------------------

#[test]
fn compile_validates_the_whole_hierarchy_and_its_verdicts_match_the_export() {
    let compiled = incident().compile().unwrap();
    assert!(validate(&compiled.to_definition()).is_valid());

    // A group `.compile()` refuses with its own error is flagged by the export
    // with the new codes, which is what the CLI reads.
    let empty = || {
        StateGraph::<i32>::new().add_parallel_group("G", ParallelBranches::new(), |s: &i32, _| *s)
    };
    assert_eq!(compile_error(empty()), "parallel group 'G' has no branches");
    let errors = validate(&empty().to_definition()).errors;
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].code, "STRATA_ERR_EMPTY_GROUP");
    assert_eq!(errors[0].node, "G");

    let duplicate = || {
        StateGraph::<i32>::new().add_parallel_group(
            "G",
            ParallelBranches::new().branch("B", ok).branch("B", ok),
            |s: &i32, _| *s,
        )
    };
    assert_eq!(
        compile_error(duplicate()),
        "parallel group 'G' has more than one branch named 'B'"
    );
    let errors = validate(&duplicate().to_definition()).errors;
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].code, "STRATA_ERR_DUPLICATE_BRANCH");
    assert_eq!(errors[0].related_node.as_deref(), Some("B"));
}

#[test]
fn a_child_error_in_an_exported_file_is_reported_with_its_qualified_name() {
    // An edited file: the child's `Abort` loses the reject route into it.
    let mut definition = incident().to_definition();
    let child = definition.nodes[2].graph.as_deref_mut().unwrap();
    child.edges.retain(|e| e.to != "Abort");

    let errors = validate(&definition).errors;
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].code, "STRATA_ERR_ORPHAN");
    assert_eq!(errors[0].node, "Remediate/Abort");
    assert_eq!(
        errors[0].message,
        "Node 'Remediate/Abort' is isolated and unreachable from the entry point 'Remediate/Plan'."
    );
}
