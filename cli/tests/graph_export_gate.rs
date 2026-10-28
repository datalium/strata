//! The path from a graph built in code to `strata graph validate`: the engine's
//! `to_definition()` export, written to a file and checked by the real binary.
//! (`rust/strata-core/tests/graph_export.rs` pins the export itself; this pins
//! that the CLI and `.compile()` agree on what it says.)

use std::process::Command;

use datalium_strata::StrataError;
use datalium_strata::graph::{NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::GraphDefinition;

fn noop(s: i32) -> i32 {
    s
}

fn validate_file(definition: &GraphDefinition) -> (Option<i32>, serde_json::Value) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph.json");
    std::fs::write(&path, serde_json::to_string_pretty(definition).unwrap()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(["graph", "validate", "--json", "--path"])
        .arg(&path)
        .output()
        .unwrap();
    (
        output.status.code(),
        serde_json::from_slice(&output.stdout).unwrap(),
    )
}

/// `Charge` fails over to `ManualReview`, which no ordinary edge enters.
fn fallback_pipeline() -> StateGraph<i32> {
    StateGraph::<i32>::new()
        .add_node("Intake", noop)
        .add_fallible_node("Charge", |s: &i32| Ok::<i32, NodeError>(*s))
        .add_node("Ship", noop)
        .add_node("ManualReview", noop)
        .add_edge("Intake", "Charge")
        .add_edge("Charge", "Ship")
        .set_recovery_policy(
            "Charge",
            RecoveryPolicy::retry(2).with_fallback("ManualReview"),
        )
}

#[test]
fn a_graph_that_compiles_is_accepted_by_the_cli_including_a_fallback_only_node() {
    let (code, json) = validate_file(&fallback_pipeline().to_definition());

    assert_eq!(code, Some(0), "{json}");
    assert_eq!(json["valid"], true);
}

#[test]
fn the_compiled_graphs_routing_edges_alone_would_be_a_false_positive() {
    // The reason `to_definition()` exists: the `definition` field has no
    // fallback edge, so the CLI would call `ManualReview` an orphan of a graph
    // `.compile()` accepted.
    let compiled = fallback_pipeline().compile().unwrap();

    let (code, json) = validate_file(&compiled.definition);
    assert_eq!(code, Some(1));
    assert_eq!(json["errors"][0]["code"], "STRATA_ERR_ORPHAN");

    let (code, _) = validate_file(&compiled.to_definition());
    assert_eq!(code, Some(0));
}

#[test]
fn a_graph_compile_rejects_is_rejected_by_the_cli_with_the_same_diagnostics() {
    let broken = || {
        StateGraph::<i32>::new()
            .add_node("A", noop)
            .add_node("B", noop)
            .add_node("Island", noop)
            .add_edge("A", "B")
            .add_edge("B", "A")
    };

    let Err(StrataError::GraphInvalid(compile_view)) = broken().compile() else {
        panic!("expected compile to reject the graph");
    };
    let (code, json) = validate_file(&broken().to_definition());

    assert_eq!(code, Some(1));
    let cli_codes: Vec<&str> = json["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["code"].as_str().unwrap())
        .collect();
    let compile_codes: Vec<&str> = compile_view.errors.iter().map(|e| e.code).collect();
    assert_eq!(cli_codes, compile_codes);
    assert_eq!(
        cli_codes,
        ["STRATA_ERR_ORPHAN", "STRATA_ERR_UNGUARDED_CYCLE"]
    );
}

#[test]
fn the_shared_export_fixture_is_accepted_by_the_cli() {
    let path = format!(
        "{}/../schema/fixtures/graph-export/fallback-pipeline.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let output = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(["graph", "validate", "--path", &path])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn the_shared_reject_route_fixture_is_accepted_and_its_route_counts_as_an_edge() {
    // `Abort` is reached only through `Approve`'s reject route
    // (design/21-Typed-Human-Signals.md): the guarded `on_reject` edge is what
    // keeps it from being an orphan, as `.compile()` agrees.
    let fixture: GraphDefinition = serde_json::from_str(
        &std::fs::read_to_string(format!(
            "{}/../schema/fixtures/graph-export/reject-route.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap(),
    )
    .unwrap();

    let (code, json) = validate_file(&fixture);
    assert_eq!(code, Some(0), "{json}");

    let mut without_route = fixture;
    without_route
        .edges
        .retain(|edge| edge.condition.as_deref() != Some("on_reject"));
    let (code, json) = validate_file(&without_route);
    assert_eq!(code, Some(1));
    assert_eq!(json["errors"][0]["code"], "STRATA_ERR_ORPHAN");
    assert_eq!(json["errors"][0]["node"], "Abort");
}

/// `Triage` -> `Investigate` (two branches) -> `Remediate`, a subgraph whose
/// child reaches `Abort` only through `Approve`'s reject route
/// (`design/22-Graph-Structure-In-Definitions.md`).
fn hierarchy() -> StateGraph<i32> {
    let remediation = StateGraph::<i32>::new()
        .add_node("Plan", noop)
        .add_node("Approve", noop)
        .add_node("Apply", noop)
        .add_node("Abort", noop)
        .add_edge("Plan", "Approve")
        .add_edge("Approve", "Apply")
        .add_interrupt_boundary_with_reject("Approve", "Abort")
        .compile()
        .unwrap();
    StateGraph::<i32>::new()
        .add_node("Triage", noop)
        .add_parallel_group(
            "Investigate",
            ParallelBranches::new()
                .branch("Kubernetes", |s: &i32| Ok(*s))
                .branch("GitHub", |s: &i32| Ok(*s))
                .set_metadata("Kubernetes", [("agent", "kubernetes-investigator")]),
            |s: &i32, _| *s,
        )
        .add_subgraph("Remediate", remediation, |s: &i32| *s, |_: &i32, c| c)
        .add_edge("Triage", "Investigate")
        .add_edge("Investigate", "Remediate")
        .set_node_metadata("Triage", [("agent", "triage")])
}

#[test]
fn a_hierarchy_that_compiles_is_accepted_by_the_cli_at_every_level() {
    hierarchy().compile().unwrap();
    let definition = hierarchy().to_definition();
    assert_eq!(definition.schema_version, "1.1.0");

    let (code, json) = validate_file(&definition);
    assert_eq!(code, Some(0), "{json}");
}

#[test]
fn an_exported_hierarchy_edited_inside_the_child_is_rejected_with_a_qualified_error() {
    let mut definition = hierarchy().to_definition();
    let child = definition.nodes[2].graph.as_deref_mut().unwrap();
    child
        .edges
        .retain(|edge| edge.condition.as_deref() != Some("on_reject"));

    let (code, json) = validate_file(&definition);
    assert_eq!(code, Some(1));
    assert_eq!(json["errors"].as_array().unwrap().len(), 1, "{json}");
    assert_eq!(json["errors"][0]["node"], "Remediate/Abort");
}
