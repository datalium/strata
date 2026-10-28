//! `#[derive(StrataSchema)]` on the types an agent's answer and a tool's
//! arguments are made of (`design/44-LLM-Integration.md`, "Structured
//! output"): nested types, sequences, maps, fieldless enums, and serde's
//! names for them.

use std::collections::{BTreeMap, HashMap};

use datalium_strata::prelude::*;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize, StrataSchema)]
#[allow(dead_code)]
struct Evidence {
    source: String,
    weight: f64,
}

#[derive(Deserialize, StrataSchema)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
enum Severity {
    Low,
    High,
    NeedsHuman,
}

#[derive(Deserialize, StrataSchema)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct Diagnosis {
    root_cause: String,
    confidence: f64,
    evidence: Vec<Evidence>,
    tags: Vec<String>,
    severity: Severity,
    labels: HashMap<String, String>,
    scores: BTreeMap<String, Vec<u32>>,
    #[serde(rename = "next")]
    next_step: Option<Box<Evidence>>,
    #[serde(default)]
    notes: Vec<String>,
    #[serde(skip)]
    cached: u8,
    shared: Box<str>,
    matrix: [[i32; 2]; 2],
}

/// A type nobody derived for: described as an object, as every unknown field
/// type was before.
#[derive(Deserialize)]
#[allow(dead_code)]
struct Opaque {
    anything: String,
}

#[derive(Deserialize, StrataSchema)]
#[allow(dead_code)]
struct WithOpaque {
    opaque: Opaque,
    many: Vec<Opaque>,
    r#type: String,
}

#[test]
fn nested_types_sequences_maps_and_enums_are_described() {
    assert_eq!(
        Diagnosis::json_schema(),
        json!({
            "type": "object",
            "properties": {
                "rootCause": { "type": "string" },
                "confidence": { "type": "number" },
                "evidence": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "source": { "type": "string" },
                            "weight": { "type": "number" },
                        },
                        "required": ["source", "weight"],
                    },
                },
                "tags": { "type": "array", "items": { "type": "string" } },
                "severity": { "type": "string", "enum": ["low", "high", "needs_human"] },
                "labels": { "type": "object", "additionalProperties": { "type": "string" } },
                "scores": {
                    "type": "object",
                    "additionalProperties": { "type": "array", "items": { "type": "integer" } },
                },
                "next": {
                    "type": "object",
                    "properties": {
                        "source": { "type": "string" },
                        "weight": { "type": "number" },
                    },
                    "required": ["source", "weight"],
                },
                "notes": { "type": "array", "items": { "type": "string" } },
                "shared": { "type": "string" },
                "matrix": {
                    "type": "array",
                    "items": { "type": "array", "items": { "type": "integer" } },
                },
            },
            "required": [
                "rootCause", "confidence", "evidence", "tags", "severity", "labels", "scores",
                "shared", "matrix",
            ],
        })
    );
}

#[test]
fn a_type_without_a_schema_is_an_object_and_raw_names_lose_their_prefix() {
    let schema = WithOpaque::json_schema();
    assert_eq!(schema["properties"]["opaque"], json!({ "type": "object" }));
    assert_eq!(
        schema["properties"]["many"],
        json!({ "type": "array", "items": { "type": "object" } })
    );
    assert_eq!(schema["properties"]["type"], json!({ "type": "string" }));
}

#[derive(Deserialize, StrataSchema)]
#[allow(dead_code)]
enum Plain {
    #[serde(rename = "first-one")]
    First,
    Second,
    #[serde(skip)]
    Hidden,
}

#[derive(Deserialize, StrataSchema)]
#[serde(rename_all(serialize = "UPPERCASE", deserialize = "kebab-case"))]
#[allow(dead_code)]
struct DeserializeNames {
    max_items: u32,
    #[serde(rename(serialize = "ignored"))]
    kept_name: bool,
}

#[test]
fn variant_renames_and_the_deserialize_form_are_honoured() {
    assert_eq!(
        Plain::json_schema(),
        json!({ "type": "string", "enum": ["first-one", "Second"] })
    );
    let schema = DeserializeNames::json_schema();
    assert_eq!(
        schema["required"],
        json!(["max-items", "kept-name"]),
        "deserialize names the properties, serialize alone doesn't"
    );
}

#[derive(Deserialize, StrataSchema)]
#[allow(dead_code)]
struct Page<T> {
    items: Vec<T>,
    cursor: Option<String>,
}

#[test]
fn a_generic_type_describes_its_parameter_as_it_is_given() {
    assert_eq!(
        Page::<Evidence>::json_schema()["properties"]["items"]["items"]["required"],
        json!(["source", "weight"])
    );
    assert_eq!(
        <Page<Evidence>>::json_schema()["required"],
        json!(["items"])
    );
}

#[test]
fn primitives_and_containers_describe_themselves() {
    assert_eq!(String::json_schema(), json!({ "type": "string" }));
    assert_eq!(
        <Vec<Option<bool>>>::json_schema(),
        json!({ "type": "array", "items": { "type": "boolean" } })
    );
    assert_eq!(
        <HashMap<String, Evidence>>::json_schema()["additionalProperties"]["type"],
        "object"
    );
}

#[derive(Deserialize, StrataSchema)]
struct TagArgs {
    tags: Vec<String>,
}

#[strata_tool(name = "tag", description = "Tags something.")]
async fn tag(args: TagArgs) -> Result<String, StrataError> {
    Ok(args.tags.join(","))
}

#[tokio::test]
async fn a_vec_tool_parameter_is_an_array_in_the_manifest_and_still_dispatches() {
    assert_eq!(
        TagTool.schema()["parameters"]["properties"]["tags"],
        json!({ "type": "array", "items": { "type": "string" } })
    );
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(TagTool));
    assert_eq!(
        registry
            .dispatch("tag", r#"{"tags":["a","b"]}"#)
            .await
            .unwrap(),
        "a,b"
    );
}
