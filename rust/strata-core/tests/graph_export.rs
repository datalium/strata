//! `StateGraph::to_definition` / `CompiledGraph::to_definition`: the export
//! `strata graph validate` reads. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/GraphExportTests.cs` — keep the two in
//! step; both pin the same fixture, `schema/fixtures/graph-export/`.

use datalium_strata::StrataError;
use datalium_strata::graph::{NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::validate::validate;
use serde_json::Value;

fn noop(s: i32) -> i32 {
    s
}

fn fixture(name: &str) -> Value {
    let path = format!(
        "{}/../../schema/fixtures/graph-export/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&raw).unwrap()
}

/// Node `Charge` fails over to `ManualReview`, which no ordinary edge enters.
fn fallback_pipeline() -> StateGraph<i32> {
    StateGraph::<i32>::new()
        .add_node("Intake", noop)
        .add_fallible_node("Charge", |s: &i32| Ok::<i32, NodeError>(*s))
        .add_node("Ship", noop)
        .add_node("ManualReview", noop)
        .add_edge("Intake", "Charge")
        .add_edge("Charge", "Ship")
        .add_conditional_edge("Ship", "Intake", |s: &i32| *s < 3)
        .set_recovery_policy(
            "Charge",
            RecoveryPolicy::retry(2).with_fallback("ManualReview"),
        )
}

#[test]
fn export_matches_the_shared_fixture() {
    let exported = serde_json::to_value(fallback_pipeline().to_definition()).unwrap();
    assert_eq!(exported, fixture("fallback-pipeline.json"));
}

#[test]
fn a_node_reachable_only_through_a_fallback_is_not_an_orphan_in_the_export() {
    let definition = fallback_pipeline().to_definition();

    assert!(validate(&definition).is_valid());
    // ...which is what the executor's own edge list would get wrong.
    let routing_only = datalium_strata::schema::GraphDefinition {
        edges: fallback_pipeline().compile().unwrap().definition.edges,
        ..definition
    };
    let errors = validate(&routing_only).errors;
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].code, "STRATA_ERR_ORPHAN");
    assert_eq!(errors[0].node, "ManualReview");
}

#[test]
fn a_compiled_graph_exports_what_its_builder_did() {
    let from_builder = fallback_pipeline().to_definition();
    let compiled = fallback_pipeline().compile().unwrap();

    assert_eq!(compiled.to_definition(), from_builder);
    // The field is what the executor routes over: no fallback edge, so on its
    // own it would make `ManualReview` look like an orphan.
    assert_eq!(
        compiled.definition.edges.len(),
        from_builder.edges.len() - 1
    );
}

#[test]
fn fallback_edges_follow_node_declaration_order_whatever_order_the_policies_were_set_in() {
    let graph = StateGraph::<i32>::new()
        .add_fallible_node("First", |s: &i32| Ok::<i32, NodeError>(*s))
        .add_fallible_node("Second", |s: &i32| Ok::<i32, NodeError>(*s))
        .add_fallible_node("Third", |s: &i32| Ok::<i32, NodeError>(*s))
        .add_node("Rescue", noop)
        .add_edge("First", "Second")
        .add_edge("Second", "Third")
        .add_edge("Third", "Rescue")
        .set_recovery_policy("Third", RecoveryPolicy::default().with_fallback("Rescue"))
        .set_recovery_policy("First", RecoveryPolicy::default().with_fallback("Rescue"))
        .set_recovery_policy("Second", RecoveryPolicy::default().with_fallback("Rescue"));

    let edges = graph.to_definition().edges;
    let fallbacks: Vec<&str> = edges
        .iter()
        .filter(|e| e.condition.as_deref() == Some("on_failure"))
        .map(|e| e.from.as_str())
        .collect();

    assert_eq!(fallbacks, ["First", "Second", "Third"]);
    assert!(edges.iter().all(|e| e.condition.is_none() || e.guarded));
}

/// One case per way validation can answer. The graph is built twice because
/// `compile` consumes it.
type Case = (&'static str, fn() -> StateGraph<i32>);

fn cases() -> Vec<Case> {
    vec![
        ("valid chain", || {
            StateGraph::<i32>::new()
                .add_node("A", noop)
                .add_node("B", noop)
                .add_edge("A", "B")
        }),
        ("guarded retry loop", || {
            StateGraph::<i32>::new()
                .add_node("A", noop)
                .add_node("B", noop)
                .add_edge("A", "B")
                .add_conditional_edge("B", "A", |s: &i32| *s < 3)
        }),
        ("fallback-only node", fallback_pipeline),
        ("empty graph", StateGraph::<i32>::new),
        ("missing entry point", || {
            StateGraph::<i32>::new()
                .add_node("A", noop)
                .set_entry_point("Nope")
        }),
        ("orphan node", || {
            StateGraph::<i32>::new()
                .add_node("A", noop)
                .add_node("B", noop)
        }),
        ("unguarded cycle", || {
            StateGraph::<i32>::new()
                .add_node("A", noop)
                .add_node("B", noop)
                .add_edge("A", "B")
                .add_edge("B", "A")
        }),
        ("orphan and unguarded cycle", || {
            StateGraph::<i32>::new()
                .add_node("A", noop)
                .add_node("B", noop)
                .add_node("Island", noop)
                .add_edge("A", "B")
                .add_edge("B", "A")
        }),
        ("edge to a name that is not a node", || {
            StateGraph::<i32>::new()
                .add_node("A", noop)
                .add_node("B", noop)
                .add_edge("A", "B")
                .add_edge("B", "Nope")
        }),
        ("edge from a name that is not a node", || {
            StateGraph::<i32>::new()
                .add_node("A", noop)
                .add_node("B", noop)
                .add_edge("A", "B")
                .add_edge("Nope", "B")
        }),
        ("group and subgraph", || {
            let child = StateGraph::<i32>::new()
                .add_node("C", noop)
                .compile()
                .unwrap();
            StateGraph::<i32>::new()
                .add_parallel_group(
                    "G",
                    ParallelBranches::new().branch("X", |s: &i32| Ok(*s)),
                    |s: &i32, _| *s,
                )
                .add_subgraph("S", child, |s: &i32| *s, |_: &i32, c| c)
                .add_edge("G", "S")
        }),
    ]
}

#[test]
fn a_fallback_to_a_missing_node_is_flagged_by_the_export() {
    let build = || {
        StateGraph::<i32>::new()
            .add_fallible_node("Charge", |s: &i32| Ok::<i32, NodeError>(*s))
            .add_node("Ship", noop)
            .add_edge("Charge", "Ship")
            .set_recovery_policy("Charge", RecoveryPolicy::default().with_fallback("Nope"))
    };

    // `.compile()` rejects the policy before it validates anything...
    assert!(matches!(
        build().compile(),
        Err(StrataError::CompilationFailed(_))
    ));
    // ...and the export, which is what the CLI reads, shows the same fault.
    let errors = validate(&build().to_definition()).errors;
    assert_eq!(errors.len(), 1, "got {errors:?}");
    assert_eq!(errors[0].code, "STRATA_ERR_DANGLING_EDGE");
    assert_eq!(errors[0].node, "Charge");
    assert_eq!(errors[0].related_node.as_deref(), Some("Nope"));
}

#[test]
fn validating_the_export_gives_the_verdict_compile_does_for_every_case() {
    for (name, build) in cases() {
        let cli_view = validate(&build().to_definition());

        match build().compile() {
            Ok(_) => assert!(
                cli_view.is_valid(),
                "{name}: compile accepted, export rejected"
            ),
            Err(StrataError::GraphInvalid(compile_view)) => {
                assert!(
                    !cli_view.is_valid(),
                    "{name}: compile rejected, export accepted"
                );
                assert_eq!(cli_view, compile_view, "{name}: different diagnostics");
            }
            Err(other) => panic!("{name}: unexpected compile error {other:?}"),
        }
    }
}

#[test]
fn a_graph_compile_rejects_can_still_be_exported() {
    let broken = StateGraph::<i32>::new()
        .add_node("A", noop)
        .add_node("B", noop)
        .add_edge("A", "B")
        .add_edge("B", "A");

    let definition = broken.to_definition();

    assert_eq!(definition.entry_point, "A");
    assert_eq!(definition.nodes.len(), 2);
    assert_eq!(definition.edges.len(), 2);
    assert!(!validate(&definition).is_valid());
}
