//! Arguments a tool can't read (`design/20-Tool-Dispatch-And-Registry.md`):
//! the shared words in `schema/fixtures/tools/argument-problems.json`, which
//! `dotnet/tests/Datalium.Strata.Tests/Tools/ToolArgumentProblemTests.cs`
//! replays too, and `#[strata_tool]` reporting them. Keep the two in step.

use datalium_strata::prelude::*;
use datalium_strata::tools::argument_problems;
use serde::Deserialize;
use serde_json::Value;

#[test]
fn shared_cases() {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/tools/argument-problems.json");
    let json: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    for case in json["cases"].as_array().unwrap() {
        let problems = argument_problems(case["arguments"].as_str().unwrap(), &case["parameters"]);
        assert_eq!(
            problems.as_deref(),
            case["problems"].as_str(),
            "case '{}'",
            case["name"].as_str().unwrap()
        );
    }
}

#[derive(Deserialize, StrataSchema)]
struct RefundArgs {
    order_id: String,
    amount: u32,
    note: Option<String>,
}

#[strata_tool(name = "refund", description = "Refunds part of an order.")]
async fn refund(args: RefundArgs) -> Result<String, StrataError> {
    if args.amount == 0 {
        return Err(StrataError::ExecutionAborted("nothing to refund".into()));
    }
    Ok(format!(
        "refunded {} on {}{}",
        args.amount,
        args.order_id,
        args.note.map(|n| format!(" ({n})")).unwrap_or_default()
    ))
}

async fn refused(arguments: &str) -> String {
    match RefundTool.execute(arguments).await {
        Err(StrataError::ToolArgumentParseFailure(problems)) => problems,
        other => panic!("expected an argument parse failure, got {other:?}"),
    }
}

#[tokio::test]
async fn a_missing_argument_is_named() {
    assert_eq!(
        refused(r#"{"amount":5}"#).await,
        "argument 'order_id' is missing"
    );
}

#[tokio::test]
async fn every_wrong_type_is_named() {
    assert_eq!(
        refused(r#"{"order_id":42,"amount":"five"}"#).await,
        "argument 'amount' must be an integer; argument 'order_id' must be a string"
    );
}

#[tokio::test]
async fn arguments_that_arent_an_object_are_refused() {
    assert_eq!(refused("[]").await, "the arguments aren't a JSON object");
    assert_eq!(refused("{").await, "the arguments aren't valid JSON");
}

#[tokio::test]
async fn what_the_rule_doesnt_explain_is_in_serdes_words() {
    // An integer, but not one a u32 holds.
    let problems = refused(r#"{"order_id":"o-1","amount":-5}"#).await;
    assert!(
        problems.starts_with("invalid value: integer `-5`"),
        "{problems}"
    );
}

#[tokio::test]
async fn an_optional_argument_may_be_null_or_absent() {
    let result = RefundTool
        .execute(r#"{"order_id":"o-1","amount":5,"note":null}"#)
        .await
        .unwrap();
    assert_eq!(result, "refunded 5 on o-1");
}

#[tokio::test]
async fn a_tools_own_error_is_not_an_argument_failure() {
    let error = RefundTool
        .execute(r#"{"order_id":"o-1","amount":0}"#)
        .await
        .unwrap_err();
    assert!(matches!(error, StrataError::ToolExecutionFailure(name) if name == "refund"));
}
