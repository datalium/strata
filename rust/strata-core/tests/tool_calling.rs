//! Integration tests for STRATA-SPEC-003 tool-calling metaprogramming, exercised
//! through the public `datalium_strata::prelude` the same way a downstream
//! crate would use it (see the spec's `check_inventory` example, reproduced
//! here verbatim as the "same conceptual tool" the C# parity test uses).

use datalium_strata::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, StrataSchema)]
struct InventoryArgs {
    sku: String,
    warehouse_id: u32,
}

#[strata_tool(
    name = "check_inventory",
    description = "Query warehouse item quantities."
)]
async fn check_inventory(args: InventoryArgs) -> Result<String, StrataError> {
    Ok(format!(
        "SKU {} count is 42 in Zone {}",
        args.sku, args.warehouse_id
    ))
}

#[derive(Deserialize, StrataSchema)]
#[allow(dead_code)] // fields exist only to exercise StrataSchema's Option<T> handling
struct OptionalFieldArgs {
    required_field: String,
    optional_field: Option<String>,
}

#[test]
fn generated_schema_matches_the_function_signature() {
    let schema = CheckInventoryTool.schema();

    assert_eq!(schema["name"], "check_inventory");
    assert_eq!(schema["description"], "Query warehouse item quantities.");
    assert_eq!(schema["parameters"]["type"], "object");
    assert_eq!(schema["parameters"]["properties"]["sku"]["type"], "string");
    assert_eq!(
        schema["parameters"]["properties"]["warehouse_id"]["type"],
        "integer"
    );

    let mut required: Vec<&str> = schema["parameters"]["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    required.sort();
    assert_eq!(required, vec!["sku", "warehouse_id"]);
}

#[test]
fn optional_fields_are_excluded_from_the_required_list() {
    let schema = OptionalFieldArgs::json_schema();
    let required: Vec<&str> = schema["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();

    assert_eq!(required, vec!["required_field"]);
    // Still described, just not required.
    assert_eq!(schema["properties"]["optional_field"]["type"], "string");
}

#[tokio::test]
async fn registry_dispatches_by_name_and_executes_the_original_function() {
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(CheckInventoryTool));

    let result = registry
        .dispatch("check_inventory", r#"{"sku":"A100-80G","warehouse_id":7}"#)
        .await
        .unwrap();

    assert_eq!(result, "SKU A100-80G count is 42 in Zone 7");
}

#[tokio::test]
async fn malformed_json_arguments_produce_a_typed_error_not_a_panic() {
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(CheckInventoryTool));

    let err = registry
        .dispatch("check_inventory", "not json")
        .await
        .unwrap_err();

    assert!(matches!(err, StrataError::ToolArgumentParseFailure(_)));
}

#[tokio::test]
async fn dispatching_an_unregistered_tool_name_errors() {
    let registry = ToolRegistry::new();
    let err = registry.dispatch("does_not_exist", "{}").await.unwrap_err();
    assert!(matches!(err, StrataError::ToolNotFound(name) if name == "does_not_exist"));
}

#[test]
fn manifest_collects_schemas_from_every_registered_tool() {
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(CheckInventoryTool));

    let manifest = registry.manifest();
    let tools = manifest.as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "check_inventory");
}

/// STRATA-SPEC-003 §2 requires zero runtime reflection in the dispatch path.
/// `std::any` (the Rust reflection/downcasting surface) must never appear in
/// either the hand-written runtime (`tools.rs`) or the macro's generated-code
/// templates (`strata-macros/src/lib.rs`, which is what actually ends up in a
/// caller's dispatch path after expansion).
#[test]
fn dispatch_path_source_never_references_std_any() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let tools_rs =
        std::fs::read_to_string(format!("{manifest_dir}/src/tools.rs")).expect("read tools.rs");
    let macros_lib_rs =
        std::fs::read_to_string(format!("{manifest_dir}/../strata-macros/src/lib.rs"))
            .expect("read strata-macros/src/lib.rs");

    for (label, source) in [
        ("tools.rs", &tools_rs),
        ("strata-macros/src/lib.rs", &macros_lib_rs),
    ] {
        assert!(
            !source.contains("std::any"),
            "{label} must not reference std::any (reflection) in the tool dispatch path"
        );
    }
}
