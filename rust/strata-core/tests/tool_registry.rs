//! The tool registry (`design/20-Tool-Dispatch-And-Registry.md`, SG-21):
//! duplicate names refused at registration, one merged manifest in
//! registration order, dispatch by name, and curated subsets that refuse what
//! they left out as "not permitted". The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/Tools/ToolRegistryTests.cs` — keep the
//! two in step.

use datalium_strata::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, StrataSchema)]
struct Args {
    x: i64,
}

#[strata_tool(name = "alpha", description = "First.")]
async fn alpha(args: Args) -> Result<String, StrataError> {
    Ok(format!("alpha {}", args.x))
}

#[strata_tool(name = "beta", description = "Second.")]
async fn beta(args: Args) -> Result<String, StrataError> {
    Ok(format!("beta {}", args.x))
}

#[strata_tool(name = "gamma", description = "Third.")]
async fn gamma(args: Args) -> Result<String, StrataError> {
    Ok(format!("gamma {}", args.x))
}

/// A second tool named `alpha`, as another module might declare one.
mod other {
    use super::Args;
    use datalium_strata::prelude::*;

    #[strata_tool(name = "alpha", description = "Another alpha.")]
    pub async fn other_alpha(args: Args) -> Result<String, StrataError> {
        Ok(format!("other alpha {}", args.x))
    }
}

fn abc() -> ToolRegistry {
    let mut tools = ToolRegistry::new();
    tools
        .register(Box::new(AlphaTool))
        .register(Box::new(BetaTool))
        .register(Box::new(GammaTool));
    tools
}

fn manifest_names(tools: &ToolRegistry) -> Vec<String> {
    tools
        .manifest()
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["name"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn the_manifest_merges_every_tool_in_registration_order() {
    let mut tools = ToolRegistry::new();
    tools
        .register(Box::new(GammaTool))
        .register(Box::new(AlphaTool))
        .register(Box::new(BetaTool));

    assert_eq!(manifest_names(&tools), ["gamma", "alpha", "beta"]);
    assert_eq!(
        tools.names().collect::<Vec<_>>(),
        ["gamma", "alpha", "beta"]
    );
    let manifest = tools.manifest();
    assert_eq!(manifest[1]["description"], "First.");
    assert_eq!(
        manifest[1]["parameters"]["properties"]["x"]["type"],
        "integer"
    );
}

#[tokio::test]
async fn a_duplicate_name_is_refused_at_registration_and_registers_nothing() {
    let mut tools = abc();

    let err = tools
        .try_register(Box::new(other::OtherAlphaTool))
        .err()
        .expect("a duplicate is refused");

    assert!(matches!(err, StrataError::DuplicateTool(name) if name == "alpha"));
    assert_eq!(manifest_names(&tools), ["alpha", "beta", "gamma"]);
    assert_eq!(
        tools.dispatch("alpha", r#"{"x":1}"#).await.unwrap(),
        "alpha 1"
    );
}

#[test]
#[should_panic(expected = "tool 'alpha' is already registered")]
fn register_panics_on_a_duplicate_name() {
    abc().register(Box::new(other::OtherAlphaTool));
}

#[tokio::test]
async fn dispatch_routes_by_name_and_refuses_unknown_names() {
    let tools = abc();

    assert_eq!(
        tools.dispatch("beta", r#"{"x":2}"#).await.unwrap(),
        "beta 2"
    );
    assert_eq!(
        tools.dispatch("gamma", r#"{"x":3}"#).await.unwrap(),
        "gamma 3"
    );
    let err = tools.dispatch("delta", "{}").await.unwrap_err();
    assert!(matches!(err, StrataError::ToolNotFound(name) if name == "delta"));
}

#[tokio::test]
async fn a_subset_holds_only_the_named_tools_in_the_parents_order() {
    let subset = abc().subset(["gamma", "alpha"]).unwrap();

    assert_eq!(manifest_names(&subset), ["alpha", "gamma"]);
    assert_eq!(
        subset.dispatch("gamma", r#"{"x":4}"#).await.unwrap(),
        "gamma 4"
    );
}

#[test]
fn a_subset_refuses_unknown_names_when_it_is_made() {
    let err = abc().subset(["alpha", "delta"]).err().expect("refused");

    assert!(matches!(err, StrataError::ToolNotFound(name) if name == "delta"));
}

#[tokio::test]
async fn a_subset_refuses_a_tool_it_left_out_as_not_permitted_and_an_unknown_one_as_not_found() {
    let subset = abc().subset(["alpha"]).unwrap();

    let left_out = subset.dispatch("beta", r#"{"x":1}"#).await.unwrap_err();
    let unknown = subset.dispatch("delta", r#"{"x":1}"#).await.unwrap_err();

    assert!(matches!(left_out, StrataError::ToolNotPermitted(name) if name == "beta"));
    assert!(matches!(unknown, StrataError::ToolNotFound(name) if name == "delta"));
}

#[tokio::test]
async fn a_subset_of_a_subset_can_only_narrow() {
    let first = abc().subset(["alpha", "beta"]).unwrap();
    let second = first.subset(["beta"]).unwrap();

    assert_eq!(manifest_names(&second), ["beta"]);
    for name in ["alpha", "gamma"] {
        let err = second.dispatch(name, r#"{"x":1}"#).await.unwrap_err();
        assert!(
            matches!(&err, StrataError::ToolNotPermitted(n) if n == name),
            "{name}: {err:?}"
        );
    }
    // Widening back to a tool the first subset left out is refused the same way.
    let err = first.subset(["gamma"]).err().expect("refused");
    assert!(matches!(err, StrataError::ToolNotPermitted(name) if name == "gamma"));
}

#[test]
fn a_subset_is_a_snapshot_of_its_parent() {
    let mut parent = ToolRegistry::new();
    parent.register(Box::new(AlphaTool));
    let subset = parent.subset(["alpha"]).unwrap();

    parent.register(Box::new(BetaTool));

    assert_eq!(manifest_names(&subset), ["alpha"]);
}

#[tokio::test]
async fn a_repeated_name_counts_once_and_an_empty_subset_is_allowed() {
    let repeated = abc().subset(["beta", "beta"]).unwrap();
    let empty = abc().subset(Vec::<String>::new()).unwrap();

    assert_eq!(manifest_names(&repeated), ["beta"]);
    assert_eq!(empty.manifest(), serde_json::json!([]));
    let err = empty.dispatch("alpha", r#"{"x":1}"#).await.unwrap_err();
    assert!(matches!(err, StrataError::ToolNotPermitted(_)));
}

#[test]
fn a_registry_is_shareable_across_tasks() {
    fn assert_send_sync_clone<T: Send + Sync + Clone + 'static>() {}
    assert_send_sync_clone::<ToolRegistry>();
}
