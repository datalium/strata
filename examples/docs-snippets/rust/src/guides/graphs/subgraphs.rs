//! Samples for <https://stratamesh.dev/guides/graphs/subgraphs/>.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CheckpointStore, GraphExecutionResult, HumanSignal, LevelSignals,
};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::schema::{CheckpointStatus, GraphDefinition, GraphNodeKind};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

/// The desk's state.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ticket {
    message: String,
    category: Option<String>,
    reply: Option<String>,
    log: Vec<String>,
}

impl Ticket {
    fn new(message: &str) -> Self {
        Self {
            message: message.to_string(),
            category: None,
            reply: None,
            log: Vec::new(),
        }
    }
}

/// `BillingAgent`'s state. A reviewer's override sets `credit_cents`, a
/// goodwill credit.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BillingCase {
    charge_cents: i64,
    duplicate: bool,
    reply: String,
    steps: Vec<String>,
    credit_cents: i64,
}

/// `TechAgent`'s state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct TechCase {
    attempts: u32,
    fixed: bool,
    reply: String,
    steps: Vec<String>,
}

#[derive(Clone, Copy)]
enum Device {
    Healthy,
    /// Its firmware is too old to diagnose remotely: retrying can't help.
    OldFirmware,
}

/// How the desk reacts when an agent fails.
struct DeskPolicy {
    retries: u32,
    fallback: bool,
}

const NO_RETRIES: DeskPolicy = DeskPolicy {
    retries: 0,
    fallback: false,
};

/// The billing system. Times out for its first `fail_first` calls.
struct BillingApi {
    fail_first: u32,
    calls: AtomicU32,
}

impl BillingApi {
    fn new(fail_first: u32) -> Arc<Self> {
        Arc::new(Self {
            fail_first,
            calls: AtomicU32::new(0),
        })
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    /// The customer's latest invoice: its amount in cents, and whether it was
    /// charged twice.
    fn latest_invoice(&self) -> Result<(i64, bool), NodeError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call <= self.fail_first {
            return Err(NodeError::Transient("billing API timed out".to_string()));
        }
        Ok((4999, true))
    }
}

fn dollars(cents: i64) -> String {
    format!("${}.{:02}", cents / 100, cents % 100)
}

/// `LookupInvoice` -> (`IssueRefund` if it was a duplicate) -> `DraftReply`.
/// With `needs_approval`, a human signs the refund off first, at the interrupt
/// boundary `AwaitApproval`.
fn billing_agent(
    api: &Arc<BillingApi>,
    own_retries: u32,
    needs_approval: bool,
) -> CompiledGraph<BillingCase> {
    let api = api.clone();
    let mut graph = StateGraph::<BillingCase>::new()
        .add_fallible_node("LookupInvoice", move |case: &BillingCase| {
            let (charge_cents, duplicate) = api.latest_invoice()?;
            let mut next = case.clone();
            next.charge_cents = charge_cents;
            next.duplicate = duplicate;
            next.steps.push(format!(
                "found invoice for {}{}",
                dollars(charge_cents),
                if duplicate { " (charged twice)" } else { "" }
            ));
            Ok(next)
        })
        .set_recovery_policy("LookupInvoice", RecoveryPolicy::retry(own_retries))
        .add_node("IssueRefund", |mut case: BillingCase| {
            let credit = if case.credit_cents > 0 {
                format!(" and credited {}", dollars(case.credit_cents))
            } else {
                String::new()
            };
            case.steps
                .push(format!("refunded {}{credit}", dollars(case.charge_cents)));
            case
        })
        .add_node("DraftReply", |mut case: BillingCase| {
            case.reply = if case.duplicate {
                let mut reply = format!(
                    "We charged you twice and have refunded {}.",
                    dollars(case.charge_cents)
                );
                if case.credit_cents > 0 {
                    reply += &format!(
                        " We have also added a {} credit.",
                        dollars(case.credit_cents)
                    );
                }
                reply
            } else {
                "Your invoice is correct as issued.".to_string()
            };
            case.steps.push("drafted reply".to_string());
            case
        });

    let refund_step = if needs_approval {
        graph = graph
            .add_node("AwaitApproval", |mut case: BillingCase| {
                case.steps.push(format!(
                    "refund of {} awaits approval",
                    dollars(case.charge_cents)
                ));
                case
            })
            .add_interrupt_boundary("AwaitApproval")
            .add_edge("AwaitApproval", "IssueRefund");
        "AwaitApproval"
    } else {
        "IssueRefund"
    };

    graph
        .add_conditional_edge("LookupInvoice", refund_step, |case: &BillingCase| {
            case.duplicate
        })
        .add_edge("LookupInvoice", "DraftReply")
        .add_edge("IssueRefund", "DraftReply")
        .compile()
        .expect("billing agent is structurally valid")
}

// docs:begin guides/graphs/subgraphs/child
fn diagnostics(device: Device, network_checks: Option<Arc<AtomicU32>>) -> CompiledGraph<Probe> {
    StateGraph::<Probe>::new()
        .add_node("CheckNetwork", move |mut probe: Probe| {
            if let Some(count) = &network_checks {
                count.fetch_add(1, Ordering::SeqCst);
            }
            probe.findings.push("network ok".to_string());
            probe
        })
        .add_fallible_node("CheckFirmware", move |probe: &Probe| match device {
            Device::Healthy => {
                let mut next = probe.clone();
                next.findings.push("firmware 2.1 ok".to_string());
                Ok(next)
            }
            Device::OldFirmware => Err(NodeError::Fatal(
                "firmware 1.0 is too old to diagnose remotely".to_string(),
            )),
        })
        .add_node("Summarize", |mut probe: Probe| {
            let checks = probe.findings.len();
            probe.findings.push(format!("{checks} checks passed"));
            probe
        })
        .add_edge("CheckNetwork", "CheckFirmware")
        .add_edge("CheckFirmware", "Summarize")
        .compile()
        .expect("diagnostics graph is structurally valid")
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Probe {
    findings: Vec<String>,
}
// docs:end guides/graphs/subgraphs/child

// docs:begin guides/graphs/subgraphs/add-subgraph
fn tech_agent(device: Device, network_checks: Option<Arc<AtomicU32>>) -> CompiledGraph<TechCase> {
    StateGraph::<TechCase>::new()
        .add_subgraph(
            "Diagnose",
            diagnostics(device, network_checks),
            |_: &TechCase| Probe::default(),
            |case: &TechCase, probe: Probe| {
                let mut next = case.clone();
                next.steps.extend(
                    probe
                        .findings
                        .into_iter()
                        .map(|finding| format!("diagnostics: {finding}")),
                );
                next
            },
        )
        // docs:hide ... ApplyFix and DraftReply
        .add_node("ApplyFix", |mut case: TechCase| {
            case.attempts += 1;
            // The stand-in device only responds to the second attempt.
            case.fixed = case.attempts >= 2;
            case.steps.push(format!(
                "fix attempt {}: {}",
                case.attempts,
                if case.fixed { "resolved" } else { "no change" }
            ));
            case
        })
        .add_node("DraftReply", |mut case: TechCase| {
            case.reply = format!("Fixed after {} attempt(s).", case.attempts);
            case.steps.push("drafted reply".to_string());
            case
        })
        // docs:show
        .add_conditional_edge("ApplyFix", "ApplyFix", |case: &TechCase| {
            !case.fixed && case.attempts < 3
        })
        .add_edge("ApplyFix", "DraftReply")
        .add_edge("Diagnose", "ApplyFix")
        .compile()
        .expect("tech agent is structurally valid")
}
// docs:end guides/graphs/subgraphs/add-subgraph

/// `Triage` -> `BillingAgent` | `TechAgent` | `GeneralReply` -> `SendReply`.
/// When the policy falls back, `Escalate` is its fallback node.
fn support_desk(
    billing: CompiledGraph<BillingCase>,
    tech: CompiledGraph<TechCase>,
    policy: &DeskPolicy,
) -> CompiledGraph<Ticket> {
    // docs:begin guides/graphs/subgraphs/route
    let mut graph = StateGraph::<Ticket>::new()
        // docs:hide ... Triage sets `category` to "billing", "tech" or "general"
        .add_node("Triage", |mut ticket: Ticket| {
            // Keyword matching stands in for an LLM call.
            let text = ticket.message.to_lowercase();
            let has = |words: &[&str]| words.iter().any(|word| text.contains(word));
            let category = if has(&["charge", "invoice", "refund"]) {
                "billing"
            } else if has(&["router", "wifi", "crash"]) {
                "tech"
            } else {
                "general"
            };
            ticket.log.push(format!("triage: {category}"));
            ticket.category = Some(category.to_string());
            ticket
        })
        // docs:show
        .add_subgraph(
            "BillingAgent",
            billing,
            |_: &Ticket| BillingCase::default(),
            |ticket: &Ticket, case: BillingCase| {
                let mut next = ticket.clone();
                next.reply = Some(case.reply);
                next.log.extend(
                    case.steps
                        .into_iter()
                        .map(|step| format!("billing: {step}")),
                );
                next
            },
        )
        .add_subgraph(
            "TechAgent",
            tech,
            |_: &Ticket| TechCase::default(),
            |ticket: &Ticket, case: TechCase| {
                let mut next = ticket.clone();
                next.reply = Some(case.reply);
                next.log
                    .extend(case.steps.into_iter().map(|step| format!("tech: {step}")));
                next
            },
        )
        // docs:hide ... GeneralReply and SendReply
        .add_node("GeneralReply", |mut ticket: Ticket| {
            ticket.reply = Some("Thanks for getting in touch.".to_string());
            ticket
        })
        .add_node("SendReply", |mut ticket: Ticket| {
            ticket.log.push("reply sent".to_string());
            ticket
        })
        // docs:show
        .add_conditional_edge("Triage", "BillingAgent", |ticket: &Ticket| {
            ticket.category.as_deref() == Some("billing")
        })
        .add_conditional_edge("Triage", "TechAgent", |ticket: &Ticket| {
            ticket.category.as_deref() == Some("tech")
        })
        .add_edge("Triage", "GeneralReply")
        .add_edge("BillingAgent", "SendReply")
        .add_edge("TechAgent", "SendReply")
        .add_edge("GeneralReply", "SendReply");
    // docs:end guides/graphs/subgraphs/route

    // docs:begin guides/graphs/subgraphs/recovery
    let mut recovery = RecoveryPolicy::retry(policy.retries);
    if policy.fallback {
        graph = graph
            .add_node("Escalate", |mut ticket: Ticket| {
                // docs:hide ...
                ticket.reply = Some("A human agent will contact you shortly.".to_string());
                ticket.log.push("escalated to a human".to_string());
                // docs:show
                ticket
            })
            .add_edge("Escalate", "SendReply");
        recovery = recovery.with_fallback("Escalate");
    }
    graph
        .set_recovery_policy("BillingAgent", recovery.clone())
        .set_recovery_policy("TechAgent", recovery)
        .compile()
        .expect("support desk is structurally valid")
    // docs:end guides/graphs/subgraphs/recovery
}

/// The desk, with a healthy billing API and device unless a test says otherwise.
struct Desk {
    api: Arc<BillingApi>,
    own_retries: u32,
    needs_approval: bool,
    device: Device,
    network_checks: Option<Arc<AtomicU32>>,
    policy: DeskPolicy,
}

impl Default for Desk {
    fn default() -> Self {
        Self {
            api: BillingApi::new(0),
            own_retries: 0,
            needs_approval: false,
            device: Device::Healthy,
            network_checks: None,
            policy: NO_RETRIES,
        }
    }
}

impl Desk {
    fn build(self) -> CompiledGraph<Ticket> {
        support_desk(
            billing_agent(&self.api, self.own_retries, self.needs_approval),
            tech_agent(self.device, self.network_checks),
            &self.policy,
        )
    }
}

fn store() -> Arc<dyn CheckpointStore> {
    Arc::new(InMemoryCheckpointStore::new())
}

fn expect_completed<S>(result: GraphExecutionResult<S>) -> S {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => panic!("the run failed: {err:?}"),
        _ => panic!("the run didn't complete"),
    }
}

const FIXED_TECH_LOG: [&str; 8] = [
    "triage: tech",
    "tech: diagnostics: network ok",
    "tech: diagnostics: firmware 2.1 ok",
    "tech: diagnostics: 2 checks passed",
    "tech: fix attempt 1: no change",
    "tech: fix attempt 2: resolved",
    "tech: drafted reply",
    "reply sent",
];

#[tokio::test]
async fn a_tech_ticket_goes_three_levels_deep() {
    let desk = Desk::default().build();
    let ticket = expect_completed(
        desk.run(
            "subgraphs-2",
            Ticket::new("My router keeps crashing"),
            store(),
        )
        .await,
    );

    assert_eq!(ticket.reply.as_deref(), Some("Fixed after 2 attempt(s)."));
    assert_eq!(ticket.log, FIXED_TECH_LOG);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Job {
    n: i64,
    log: Vec<String>,
}

#[tokio::test]
async fn a_child_uses_the_parents_state_type() {
    // docs:begin guides/graphs/subgraphs/same-type
    let work = StateGraph::<Job>::new()
        .add_node("Double", |mut job: Job| {
            job.n *= 2;
            job.log.push("double".to_string());
            job
        })
        .compile()
        .expect("valid");

    let graph = StateGraph::<Job>::new()
        .add_node("Prepare", |mut job: Job| {
            job.log.push("prepare".to_string());
            job
        })
        .add_subgraph("Work", work, Job::clone, |_: &Job, result: Job| result)
        .add_edge("Prepare", "Work")
        .compile()
        .expect("valid");
    // docs:end guides/graphs/subgraphs/same-type

    let job = Job {
        n: 5,
        log: Vec::new(),
    };
    let job = expect_completed(graph.run("subgraphs-job", job, store()).await);
    assert_eq!(job.n, 10);
    assert_eq!(job.log, ["prepare", "double"]);
}

#[tokio::test]
async fn a_child_recovers_itself() {
    // The billing API times out twice, and the child's own retry(2) handles it.
    let api = BillingApi::new(2);
    let desk = Desk {
        api: api.clone(),
        own_retries: 2,
        ..Desk::default()
    }
    .build();
    let store = store();
    let result = desk
        .run(
            "subgraphs-3",
            Ticket::new("Please refund my invoice"),
            store.clone(),
        )
        .await;

    expect_completed(result);
    assert_eq!(api.calls(), 3);
    assert!(store.load("subgraphs-3").await.unwrap().is_none());
}

#[tokio::test]
async fn the_parents_policy_treats_the_child_as_one_node() {
    // The billing API is down: the child retries once, the desk retries the
    // whole child once, then falls back.
    let api = BillingApi::new(u32::MAX);
    let desk = Desk {
        api: api.clone(),
        own_retries: 1,
        policy: DeskPolicy {
            retries: 1,
            fallback: true,
        },
        ..Desk::default()
    }
    .build();
    let ticket = expect_completed(
        desk.run("subgraphs-4", Ticket::new("Where is my refund?"), store())
            .await,
    );

    assert_eq!(api.calls(), 4);
    assert_eq!(
        ticket.log,
        ["triage: billing", "escalated to a human", "reply sent"]
    );
}

#[tokio::test]
async fn a_fatal_error_two_levels_down() {
    let store = store();
    let desk = Desk {
        device: Device::OldFirmware,
        policy: DeskPolicy {
            retries: 3,
            fallback: false,
        },
        ..Desk::default()
    }
    .build();
    let result = desk
        .run("subgraphs-5", Ticket::new("My wifi is down"), store.clone())
        .await;

    // The fatal kind survives both levels, so none of the desk's retries is used.
    match result {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => {
            assert_eq!((node.as_str(), attempts), ("TechAgent", 1));
            assert_eq!(
                message,
                "subgraph 'TechAgent' failed at 'Diagnose' after 1 attempt(s): \
                 subgraph 'Diagnose' failed at 'CheckFirmware' after 1 attempt(s): \
                 firmware 1.0 is too old to diagnose remotely"
            );
        }
        _ => panic!("expected NodeFailed"),
    }
    let checkpoint = store
        .load("subgraphs-5")
        .await
        .unwrap()
        .expect("a failed checkpoint");
    assert_eq!(checkpoint.status, CheckpointStatus::Failed);
    assert_eq!(checkpoint.metadata.as_ref().unwrap()["error_kind"], "fatal");
    assert_eq!(
        checkpoint.frozen_state["log"],
        serde_json::json!(["triage: tech"])
    );
    assert_eq!(
        checkpoint.waiting_at(),
        ["TechAgent", "Diagnose", "CheckFirmware"]
    );

    // Once the firmware is fixed, a resume re-enters Diagnose at CheckFirmware:
    // CheckNetwork doesn't run again.
    let network_checks = Arc::new(AtomicU32::new(0));
    let fixed = Desk {
        network_checks: Some(network_checks.clone()),
        policy: DeskPolicy {
            retries: 3,
            fallback: false,
        },
        ..Desk::default()
    }
    .build();
    let ticket = expect_completed(
        fixed
            .resume(
                "subgraphs-5",
                HumanSignal::approve("ops@support.example"),
                store,
            )
            .await,
    );
    assert_eq!(ticket.log, FIXED_TECH_LOG);
    assert_eq!(network_checks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn pause_inside_a_subgraph() {
    let desk = Desk {
        needs_approval: true,
        ..Desk::default()
    }
    .build();
    let store = store();

    // docs:begin guides/graphs/subgraphs/pause
    let ticket = Ticket::new("I was charged twice for invoice 1042");
    if let GraphExecutionResult::Interrupted { waiting_at, .. } =
        desk.run("ticket-7", ticket, store.clone()).await
    {
        // docs:hide
        assert_eq!(waiting_at, ["BillingAgent", "AwaitApproval"]);
        // docs:show
        println!("Paused at {}", waiting_at.join("/"));
    }

    // The first signal answers the innermost level: BillingAgent's boundary.
    // `.at(&[], ...)` adds overrides for the desk's own state.
    let signals = LevelSignals::new(
        HumanSignal::approve("dana@support.example").with_override("credit_cents", 500),
    )
    .at(
        &[],
        HumanSignal::approve("dana@support.example").with_override("category", "billing-priority"),
    );
    if let GraphExecutionResult::Completed(ticket) = desk.resume("ticket-7", signals, store).await {
        // docs:hide
        assert_eq!(ticket.category.as_deref(), Some("billing-priority"));
        assert_eq!(
            ticket.reply.as_deref(),
            Some(
                "We charged you twice and have refunded $49.99. We have also added a $5.00 credit."
            )
        );
        // docs:show
        println!("Category: {}", ticket.category.unwrap_or_default());
        println!("Reply: {}", ticket.reply.unwrap_or_default());
        // docs:hide
        return;
        // docs:show
    }
    // docs:end guides/graphs/subgraphs/pause
    panic!("the resumed run didn't complete");
}

fn subgraph<'a>(definition: &'a GraphDefinition, name: &str) -> &'a GraphDefinition {
    let node = definition
        .nodes
        .iter()
        .find(|node| node.name == name)
        .expect("the node exists");
    assert_eq!(node.kind, Some(GraphNodeKind::Subgraph));
    node.graph.as_deref().expect("a subgraph holds its child")
}

#[test]
fn the_definition_nests_each_child() {
    let desk = Desk {
        needs_approval: true,
        policy: DeskPolicy {
            retries: 1,
            fallback: true,
        },
        ..Desk::default()
    }
    .build();
    let definition = desk.to_definition();

    let diagnose = subgraph(subgraph(&definition, "TechAgent"), "Diagnose");
    let names: Vec<&str> = diagnose
        .nodes
        .iter()
        .map(|node| node.name.as_str())
        .collect();
    assert_eq!(names, ["CheckNetwork", "CheckFirmware", "Summarize"]);
}
