//! Interrupt boundaries in exported definitions
//! (`design/48-Boundaries-In-Definitions.md`, SG-37): `interrupt` on every
//! boundary node, with its timeout, at `schema_version` `1.6.0`; the
//! `STRATA_ERR_BOUNDARY_IN_MAP` validation code; and the shape checks. Mirrors
//! `dotnet/tests/Datalium.Strata.Tests/BoundaryDefinitionTests.cs`; keep the
//! two in step. Both build the graph below and compare its export to
//! `schema/fixtures/graph-export/boundaries.json`.

use std::time::Duration;

use datalium_strata::graph::{CompileOptions, CompiledGraph, NodeError, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::{GraphDefinition, GraphInterruptDef};
use datalium_strata::validate::{ValidationErrorKind, validate};
use datalium_strata::wait::BoundaryOptions;
use serde_json::Value;

fn noop(s: i32) -> i32 {
    s
}

fn ok(s: &i32) -> Result<i32, NodeError> {
    Ok(*s)
}

fn fixture(name: &str) -> Value {
    let path = format!(
        "{}/../../schema/fixtures/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&raw).unwrap()
}

fn shape_of(value: Value) -> Result<(), String> {
    serde_json::from_value::<GraphDefinition>(value)
        .unwrap()
        .check_shape()
}

/// `Draft` -> `Sign` (a boundary with no timeout).
fn sign_off() -> CompiledGraph<i32> {
    StateGraph::<i32>::new()
        .add_node("Draft", noop)
        .add_node("Sign", noop)
        .add_edge("Draft", "Sign")
        .add_interrupt_boundary("Sign")
        .compile()
        .unwrap()
}

/// What each map item runs: one plain node.
fn check() -> CompiledGraph<i32> {
    StateGraph::<i32>::new()
        .add_node("Check", noop)
        .compile()
        .unwrap()
}

/// The fixture's graph: `Plan` -> `Approve` (a boundary with a timeout and
/// both routes) -> `Gather` (a group that is a boundary) -> `Review` (a
/// subgraph that is a boundary, whose child has its own) -> `Fan` (a map that
/// is a boundary) -> `Ship`.
fn release() -> StateGraph<i32> {
    StateGraph::<i32>::new()
        .add_node("Plan", noop)
        .add_node("Approve", noop)
        .add_parallel_group(
            "Gather",
            ParallelBranches::new()
                .branch("Logs", ok)
                .branch("Metrics", ok),
            |s: &i32, _| *s,
        )
        .add_subgraph("Review", sign_off(), |s: &i32| *s, |_: &i32, child| child)
        .add_map(
            "Fan",
            |_: &i32| Vec::<u32>::new(),
            |item: &u32| item.to_string(),
            check(),
            |s: &i32, _: u32| *s,
            |s: &i32, _| *s,
        )
        .add_node("Ship", noop)
        .add_node("Abort", noop)
        .add_node("Escalate", noop)
        .add_edge("Plan", "Approve")
        .add_edge("Approve", "Gather")
        .add_edge("Gather", "Review")
        .add_edge("Review", "Fan")
        .add_edge("Fan", "Ship")
        .add_interrupt_boundary_with(
            "Approve",
            BoundaryOptions::new()
                .with_reject("Abort")
                .with_timeout(Duration::from_secs(30 * 60))
                .with_on_timeout("Escalate"),
        )
        .add_interrupt_boundary("Gather")
        .add_interrupt_boundary_with(
            "Review",
            BoundaryOptions::new().with_timeout(Duration::from_secs(3600)),
        )
        .add_interrupt_boundary_with(
            "Fan",
            BoundaryOptions::new().with_timeout(Duration::from_secs(300)),
        )
        .set_entry_point("Plan")
        .set_node_metadata("Approve", [("role", "release-manager")])
}

fn compiled_release() -> CompiledGraph<i32> {
    release()
        .compile_with(
            CompileOptions::new()
                .with_graph_id("release")
                .with_graph_version("2"),
        )
        .unwrap()
}

// --- Export -----------------------------------------------------------------

#[test]
fn a_compiled_graph_exports_its_boundaries_as_the_shared_fixture() {
    let exported = serde_json::to_value(compiled_release().to_definition()).unwrap();

    assert_eq!(exported, fixture("graph-export/boundaries.json"));
}

#[test]
fn the_builder_exports_the_same_boundaries_without_the_identity() {
    let mut expected = fixture("graph-export/boundaries.json");
    expected.as_object_mut().unwrap().remove("identity");

    // Still 1.6.0: the boundaries alone need it.
    assert_eq!(
        serde_json::to_value(release().to_definition()).unwrap(),
        expected
    );
}

#[test]
fn the_boundaries_fixture_round_trips_and_has_a_sound_shape() {
    let raw = fixture("graph-export/boundaries.json");
    let definition: GraphDefinition = serde_json::from_value(raw.clone()).unwrap();

    assert_eq!(definition.check_shape(), Ok(()));
    assert!(validate(&definition).is_valid());
    assert_eq!(serde_json::to_value(&definition).unwrap(), raw);
    assert_eq!(
        definition.nodes[1].interrupt,
        Some(GraphInterruptDef {
            timeout_ms: Some(1_800_000)
        })
    );
}

#[test]
fn only_a_level_with_a_boundary_moves_to_1_6_0() {
    let plain = StateGraph::<i32>::new()
        .add_node("A", noop)
        .add_node("B", noop)
        .add_edge("A", "B")
        .set_entry_point("A");
    assert_eq!(plain.to_definition().schema_version, "1.0.0");
    assert!(
        plain
            .to_definition()
            .nodes
            .iter()
            .all(|n| n.interrupt.is_none())
    );

    let bounded = plain.add_interrupt_boundary("A");
    assert_eq!(bounded.to_definition().schema_version, "1.6.0");

    // A boundary inside a child moves the child's level only.
    let parent = StateGraph::<i32>::new()
        .add_subgraph("Review", sign_off(), |s: &i32| *s, |_: &i32, child| child)
        .set_entry_point("Review")
        .to_definition();
    assert_eq!(parent.schema_version, "1.1.0");
    assert_eq!(
        parent.nodes[0].graph.as_ref().unwrap().schema_version,
        "1.6.0"
    );
}

#[test]
fn a_boundary_name_that_is_not_a_node_is_not_written_and_its_route_still_dangles() {
    let definition = StateGraph::<i32>::new()
        .add_node("A", noop)
        .add_node("Abort", noop)
        .add_interrupt_boundary_with_reject("Elsewhere", "Abort")
        .set_entry_point("A")
        .to_definition();

    assert!(definition.nodes.iter().all(|n| n.interrupt.is_none()));
    assert_eq!(definition.schema_version, "1.0.0");
    let result = validate(&definition);
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.kind == ValidationErrorKind::DanglingEdge),
        "{result:?}"
    );
}

#[test]
fn a_timeout_is_written_in_whole_milliseconds_and_re_marking_drops_it() {
    let timed = StateGraph::<i32>::new()
        .add_node("A", noop)
        .add_interrupt_boundary_with(
            "A",
            BoundaryOptions::new().with_timeout(Duration::from_micros(2_500_700)),
        )
        .set_entry_point("A");
    assert_eq!(
        timed.to_definition().nodes[0].interrupt,
        Some(GraphInterruptDef {
            timeout_ms: Some(2_500)
        })
    );

    let untimed = timed.add_interrupt_boundary("A");
    assert_eq!(
        untimed.to_definition().nodes[0].interrupt,
        Some(GraphInterruptDef { timeout_ms: None })
    );
}

#[test]
fn a_wait_node_marked_a_boundary_is_not_exported_as_one() {
    // `.compile()` refuses the pair; the builder's export doesn't write a
    // shape the CLI would refuse.
    let definition = StateGraph::<i32>::new()
        .add_timer("Window", Duration::from_secs(60))
        .add_interrupt_boundary("Window")
        .set_entry_point("Window")
        .to_definition();

    assert!(definition.nodes[0].interrupt.is_none());
    assert_eq!(definition.check_shape(), Ok(()));
}

// --- Validation -------------------------------------------------------------

#[test]
fn a_boundary_in_a_maps_template_is_reported_at_any_depth_after_its_levels_waits() {
    let definition: GraphDefinition =
        serde_json::from_value(fixture("graph-validation/boundary-in-map.json")).unwrap();
    assert_eq!(definition.check_shape(), Ok(()));

    let result = validate(&definition);
    let found: Vec<(&str, &str)> = result
        .errors
        .iter()
        .map(|e| (e.code, e.node.as_str()))
        .collect();
    assert_eq!(
        found,
        [
            ("STRATA_ERR_WAIT_IN_MAP", "Fan/*/Window"),
            ("STRATA_ERR_BOUNDARY_IN_MAP", "Fan/*/Read"),
            ("STRATA_ERR_BOUNDARY_IN_MAP", "Fan/*/Inner/Sign"),
        ]
    );
    let first = &result.errors[1];
    assert_eq!(first.kind, ValidationErrorKind::BoundaryInMap);
    assert_eq!(
        first.message,
        "Interrupt boundary 'Fan/*/Read' is inside a map's template; a map item can't freeze."
    );
}

#[test]
fn a_timeout_without_a_route_is_valid() {
    let definition = StateGraph::<i32>::new()
        .add_node("A", noop)
        .add_interrupt_boundary_with(
            "A",
            BoundaryOptions::new().with_timeout(Duration::from_secs(60)),
        )
        .set_entry_point("A")
        .compile()
        .unwrap()
        .to_definition();

    assert!(validate(&definition).is_valid());
    assert_eq!(definition.check_shape(), Ok(()));
}

// --- Shape ------------------------------------------------------------------

#[test]
fn an_interrupt_needs_1_6_0() {
    let mut old = fixture("graph-export/reject-route.json");
    old["schema_version"] = "1.5.0".into();
    assert_eq!(
        shape_of(old),
        Err("node 'Approve' has an interrupt, which schema_version 1.5.0 doesn't have".into())
    );

    // In a child, named by its path.
    let mut nested = fixture("graph-export/hierarchy.json");
    nested["nodes"][2]["graph"]["schema_version"] = "1.1.0".into();
    assert_eq!(
        shape_of(nested),
        Err(
            "node 'Remediate/Approve' has an interrupt, which schema_version 1.1.0 doesn't have"
                .into()
        )
    );
}

#[test]
fn a_wait_node_cant_have_an_interrupt() {
    let mut both = fixture("graph-export/waits.json");
    both["nodes"][2]["interrupt"] = serde_json::json!({ "timeout_ms": null });
    assert_eq!(
        shape_of(both),
        Err("node 'Deploying' is a wait and can't also have an interrupt".into())
    );
}

#[test]
fn a_negative_timeout_is_refused_when_read() {
    let mut negative = fixture("graph-export/waits.json");
    negative["nodes"][1]["interrupt"]["timeout_ms"] = (-1).into();
    assert!(serde_json::from_value::<GraphDefinition>(negative).is_err());
}

#[test]
fn at_1_6_0_a_route_needs_what_takes_it() {
    let mut unmarked = fixture("graph-export/reject-route.json");
    unmarked["nodes"][1]
        .as_object_mut()
        .unwrap()
        .remove("interrupt");
    unmarked["nodes"][0]["interrupt"] = serde_json::json!({ "timeout_ms": null });
    assert_eq!(
        shape_of(unmarked),
        Err("node 'Approve' has an on_reject edge but is not an interrupt boundary".into())
    );

    let mut untimed = fixture("graph-export/waits.json");
    untimed["nodes"][1]["interrupt"]["timeout_ms"] = Value::Null;
    assert_eq!(
        shape_of(untimed),
        Err("node 'Approve' has an on_timeout edge but no timeout".into())
    );

    let mut untimed_wait = fixture("graph-export/waits.json");
    untimed_wait["nodes"][2]["wait"]["timeout_ms"] = Value::Null;
    assert_eq!(
        shape_of(untimed_wait),
        Err("node 'Deploying' has an on_timeout edge but no timeout".into())
    );

    // An edge from a name that isn't a node is the validator's.
    let mut stray = fixture("graph-export/reject-route.json");
    stray["edges"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({ "from": "Nowhere", "to": "Abort", "guarded": true, "condition": "on_reject" }));
    assert_eq!(shape_of(stray), Ok(()));
}

#[test]
fn before_1_6_0_a_route_from_an_unmarked_node_is_still_read() {
    let mut old = fixture("graph-export/waits.json");
    old["schema_version"] = "1.4.0".into();
    old["nodes"][1].as_object_mut().unwrap().remove("interrupt");
    assert_eq!(shape_of(old.clone()), Ok(()));
    assert!(validate(&serde_json::from_value(old).unwrap()).is_valid());
}
