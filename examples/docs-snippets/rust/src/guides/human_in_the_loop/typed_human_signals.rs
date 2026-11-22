//! Samples for <https://stratamesh.dev/guides/human-in-the-loop/typed-human-signals/>.

use std::error::Error;
use std::sync::{Arc, Mutex};

use datalium_strata::graph::CompiledGraph;
use datalium_strata::schema::CheckpointStatus;
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;

// docs:begin guides/human-in-the-loop/typed-human-signals/graph
use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::context::NodeContext;
use datalium_strata::graph::{NodeError, StateGraph};
use serde::{Deserialize, Serialize};

const MAX_REPLICAS: u32 = 20;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScaleChange {
    service: String,
    replicas: u32,
    dry_run: bool,
    plan: RolloutPlan,
    regions: Vec<String>,
    note: Option<String>,
    status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RolloutPlan {
    batch_size: u32,
    steps: Vec<String>,
}

fn apply(s: ScaleChange, ctx: &NodeContext) -> Result<ScaleChange, NodeError> {
    let decision = ctx
        .decision()
        .ok_or_else(|| NodeError::Fatal("no decision in effect".to_string()))?;

    // The overrides are already in the state. Check the rules the type can't express.
    if s.replicas < 1 || s.replicas > MAX_REPLICAS {
        return Err(NodeError::Fatal(format!(
            "replicas must be between 1 and {MAX_REPLICAS}, got {}",
            s.replicas
        )));
    }
    if s.regions.is_empty() {
        return Err(NodeError::Fatal(
            "at least one region is required".to_string(),
        ));
    }

    Ok(ScaleChange {
        status: format!("applied, approved by {}", decision.reviewer),
        ..s
    })
}

// docs:hide
#[allow(clippy::let_and_return)] // the sample ends with the `let`
fn scale_graph() -> CompiledGraph<ScaleChange> {
    // docs:show
    let graph = StateGraph::<ScaleChange>::new()
        .add_node("Propose", |s: ScaleChange| ScaleChange {
            status: "awaiting approval".to_string(),
            ..s
        })
        .add_interrupt_boundary_with_reject("Propose", "Cancel")
        .add_async_fallible_node("Apply", |s: ScaleChange, ctx: NodeContext| {
            std::future::ready(apply(s, &ctx))
        })
        .add_async_node("Cancel", |s: ScaleChange, ctx: NodeContext| {
            let reviewer = ctx
                .decision()
                .map(|d| d.reviewer.clone())
                .unwrap_or_default();
            std::future::ready(ScaleChange {
                status: format!("cancelled by {reviewer}"),
                ..s
            })
        })
        .add_edge("Propose", "Apply")
        .compile()
        .expect("graph is structurally valid");
    // docs:end guides/human-in-the-loop/typed-human-signals/graph
    graph
}

// docs:begin guides/human-in-the-loop/typed-human-signals/request
/// What the approval form posts. The reviewer is not in it.
#[derive(Debug, Deserialize)]
struct ApprovalRequest {
    approve: bool,
    replicas: Option<u32>,
    dry_run: Option<bool>,
    regions: Option<Vec<String>>,
    note: Option<String>,
}

fn to_signal(request: ApprovalRequest, reviewer: &str) -> HumanSignal {
    let mut signal = if request.approve {
        HumanSignal::approve(reviewer)
    } else {
        HumanSignal::reject(reviewer)
    };
    if let Some(replicas) = request.replicas {
        signal = signal.with_override("replicas", replicas);
    }
    if let Some(dry_run) = request.dry_run {
        signal = signal.with_override("dry_run", dry_run);
    }
    if let Some(regions) = request.regions {
        signal = signal.with_override("regions", regions);
    }
    if let Some(note) = request.note {
        signal = signal.with_override("note", note);
    }
    signal
}

// docs:hide
async fn resume_from_request(
    graph: &CompiledGraph<ScaleChange>,
    store: Arc<SqliteCheckpointStore>,
    body: &str,
    reviewer: &str,
) -> Result<GraphExecutionResult<ScaleChange>, Box<dyn Error>> {
    // docs:show
    // reviewer comes from your authentication, never from the body.
    let request: ApprovalRequest = serde_json::from_str(body)?;
    let result = graph
        .resume("scale-search", to_signal(request, reviewer), store.clone())
        .await;
    // docs:hide
    Ok(result)
}
// docs:show
// docs:end guides/human-in-the-loop/typed-human-signals/request

fn new_change(service: &str) -> ScaleChange {
    ScaleChange {
        service: service.to_string(),
        replicas: 4,
        dry_run: true,
        plan: RolloutPlan {
            batch_size: 1,
            steps: vec!["scale".to_string()],
        },
        regions: vec!["eu-west".to_string()],
        note: Some("first draft".to_string()),
        status: "new".to_string(),
    }
}

#[tokio::test]
async fn typed_overrides() -> Result<(), Box<dyn Error>> {
    let graph = scale_graph();
    let store = Arc::new(SqliteCheckpointStore::open_in_memory()?);
    let paused = graph
        .run("scale-checkout", new_change("checkout"), store.clone())
        .await;
    assert!(matches!(paused, GraphExecutionResult::Interrupted { .. }));

    // docs:begin guides/human-in-the-loop/typed-human-signals/typed-overrides
    let edited_plan = RolloutPlan {
        batch_size: 2,
        steps: vec!["drain".into(), "scale".into(), "verify".into()],
    };
    let approval = HumanSignal::approve("alice@example.com")
        .with_override("replicas", 6)
        .with_override("dry_run", false)
        .with_override("plan", serde_json::to_value(&edited_plan)?)
        .with_override("regions", vec!["eu-west", "us-east"])
        .with_override("note", serde_json::Value::Null);
    let result = graph
        .resume("scale-checkout", approval, store.clone())
        .await;
    // docs:end guides/human-in-the-loop/typed-human-signals/typed-overrides

    let GraphExecutionResult::Completed(applied) = result else {
        panic!("expected the approval to complete the run");
    };
    assert_eq!(applied.status, "applied, approved by alice@example.com");
    assert_eq!(applied.replicas, 6);
    assert!(!applied.dry_run);
    assert_eq!(applied.plan.batch_size, 2);
    assert_eq!(applied.plan.steps, ["drain", "scale", "verify"]);
    assert_eq!(applied.regions, ["eu-west", "us-east"]);
    assert_eq!(applied.note, None);
    Ok(())
}

#[tokio::test]
async fn refuse_then_fail_then_correct() -> Result<(), Box<dyn Error>> {
    let graph = scale_graph();
    let store = Arc::new(SqliteCheckpointStore::open_in_memory()?);
    let paused = graph
        .run("scale-search", new_change("search"), store.clone())
        .await;
    assert!(matches!(paused, GraphExecutionResult::Interrupted { .. }));

    let mut messages = Vec::new();
    // docs:begin guides/human-in-the-loop/typed-human-signals/refused
    use datalium_strata::StrataError;
    use datalium_strata::store::CheckpointStore;

    let wrong_type = HumanSignal::approve("alice@example.com").with_override("replicas", "six");
    let as_string = HumanSignal::approve("alice@example.com").with_field_override("replicas", "6");
    let in_both_maps = HumanSignal::approve("alice@example.com")
        .with_field_override("note", "a")
        .with_override("note", "b");

    for signal in [wrong_type, as_string, in_both_maps] {
        if let GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) =
            graph.resume("scale-search", signal, store.clone()).await
        {
            println!("Refused: {message}");
            // docs:hide
            messages.push(message);
            // docs:show
        }
    }

    let waiting = store
        .load("scale-search")
        .await?
        .expect("the thread has a checkpoint");
    println!(
        "Still {:?}, resumed at: {:?}",
        waiting.status, waiting.resumed_at
    );
    // docs:end guides/human-in-the-loop/typed-human-signals/refused

    assert_eq!(
        messages,
        [
            "failed to rehydrate state for thread 'scale-search': invalid type: string \"six\", expected u32",
            "failed to rehydrate state for thread 'scale-search': invalid type: string \"6\", expected u32",
            "cannot resume thread 'scale-search': override 'note' is set as both a string and a typed value",
        ]
    );
    assert_eq!(waiting.status, CheckpointStatus::Interrupted);
    assert_eq!(waiting.resumed_at, None);

    // The approval form posts 50 replicas, and apply fails the thread.
    let body = r#"{ "approve": true, "replicas": 50, "note": "peak traffic" }"#;
    match resume_from_request(&graph, store.clone(), body, "alice@example.com").await? {
        GraphExecutionResult::Failed(StrataError::NodeFailed { node, message, .. }) => {
            assert_eq!(node, "Apply");
            assert!(
                message.contains("replicas must be between 1 and 20, got 50"),
                "{message}"
            );
        }
        _ => panic!("expected Apply to fail the thread"),
    }

    // docs:begin guides/human-in-the-loop/typed-human-signals/checkpoint-decision
    let checkpoint = store
        .load("scale-search")
        .await?
        .expect("a failed checkpoint");
    if let Some(recorded) = &checkpoint.decision {
        println!(
            "{:?}: approved={} by {} at {}, {}",
            checkpoint.status,
            recorded.is_approved,
            recorded.reviewer,
            recorded.boundary,
            recorded.decided_at
        );
    }
    // docs:end guides/human-in-the-loop/typed-human-signals/checkpoint-decision

    assert_eq!(checkpoint.status, CheckpointStatus::Failed);
    let decision = checkpoint
        .decision
        .as_ref()
        .expect("the failed checkpoint carries the decision");
    assert!(decision.is_approved);
    assert_eq!(decision.reviewer, "alice@example.com");
    assert_eq!(decision.boundary, "Propose");
    assert_eq!(
        decision.overrides.get("replicas"),
        Some(&serde_json::json!(50))
    );
    assert_eq!(
        decision.overrides.get("note"),
        Some(&serde_json::json!("peak traffic"))
    );
    assert!(
        !decision.overrides.contains_key("dry_run"),
        "a field the form didn't send adds no override"
    );

    let mut corrected = None;
    // docs:begin guides/human-in-the-loop/typed-human-signals/correction
    let correction = HumanSignal::approve("bob@example.com").with_override("replicas", 12);
    if let GraphExecutionResult::Completed(done) = graph
        .resume("scale-search", correction, store.clone())
        .await
    {
        println!("{}, replicas {}", done.status, done.replicas);
        // docs:hide
        corrected = Some(done);
        // docs:show
    }
    // docs:end guides/human-in-the-loop/typed-human-signals/correction

    // A resume of a failed thread records no decision, so apply still sees Alice's approval.
    let corrected = corrected.expect("the correction completes the run");
    assert_eq!(corrected.status, "applied, approved by alice@example.com");
    assert_eq!(corrected.replicas, 12);
    Ok(())
}

fn apply_reading_plan(
    s: ScaleChange,
    ctx: &NodeContext,
    seen: &Mutex<Option<u32>>,
) -> Result<ScaleChange, NodeError> {
    let decision = ctx
        .decision()
        .ok_or_else(|| NodeError::Fatal("no decision in effect".to_string()))?;
    // docs:begin guides/human-in-the-loop/typed-human-signals/read-override
    // In apply. decision.overrides is a serde_json::Map<String, Value>.
    if let Some(plan) = decision.overrides.get("plan") {
        let plan: RolloutPlan =
            serde_json::from_value(plan.clone()).map_err(|e| NodeError::Fatal(e.to_string()))?;
        println!(
            "the reviewer changed the plan: batches of {}",
            plan.batch_size
        );
        // docs:hide
        *seen.lock().unwrap() = Some(plan.batch_size);
        // docs:show
    }
    // docs:end guides/human-in-the-loop/typed-human-signals/read-override
    Ok(s)
}

#[tokio::test]
async fn read_override_in_node() -> Result<(), Box<dyn Error>> {
    let seen = Arc::new(Mutex::new(None));
    let in_node = seen.clone();
    let graph = StateGraph::<ScaleChange>::new()
        .add_node("Propose", |s: ScaleChange| s)
        .add_interrupt_boundary("Propose")
        .add_async_fallible_node("Apply", move |s: ScaleChange, ctx: NodeContext| {
            std::future::ready(apply_reading_plan(s, &ctx, &in_node))
        })
        .add_edge("Propose", "Apply")
        .compile()
        .expect("graph is structurally valid");
    let store = Arc::new(SqliteCheckpointStore::open_in_memory()?);
    graph
        .run("scale-read-plan", new_change("checkout"), store.clone())
        .await;

    let edited_plan = RolloutPlan {
        batch_size: 2,
        steps: vec!["drain".into(), "scale".into(), "verify".into()],
    };
    let approval = HumanSignal::approve("alice@example.com")
        .with_override("plan", serde_json::to_value(&edited_plan)?);
    let result = graph
        .resume("scale-read-plan", approval, store.clone())
        .await;
    assert!(matches!(result, GraphExecutionResult::Completed(_)));
    assert_eq!(*seen.lock().unwrap(), Some(2));
    Ok(())
}
