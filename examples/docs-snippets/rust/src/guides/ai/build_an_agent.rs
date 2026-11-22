//! Samples for <https://stratamesh.dev/guides/ai/build-an-agent/>.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::prelude::*;
use datalium_strata::store::CheckpointStore;
use datalium_strata_ai::{
    Agent, AgentOptions, AgentResult, ChatClient, ChatError, ChatRequest, ChatResponse, ChatStream,
    ChatUpdate, ChatUpdates, Message, ModelUsage, StateGraphAiExt,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use support_desk::SupportDesk;

thread_local! {
    static PRINTED: RefCell<String> = const { RefCell::new(String::new()) };
}

// The samples print with `println!`. In this file it writes to `PRINTED`, so a
// test can check what a sample prints. A `#[tokio::test]` runs on one thread.
macro_rules! println {
    ($($arg:tt)*) => {
        PRINTED.with(|printed| {
            writeln!(printed.borrow_mut(), $($arg)*).expect("a String takes any text")
        })
    };
}

/// What the samples on this thread printed so far, and empties it.
fn printed() -> String {
    PRINTED.with(|printed| printed.take())
}

// docs:begin guides/ai/build-an-agent/answer-type
#[derive(Debug, Clone, Serialize, Deserialize, StrataSchema)]
#[serde(rename_all = "camelCase")]
struct RefundDecision {
    order_id: String,
    outcome: String,
    refunded: f64,
    refund_id: Option<String>,
    reply: String,
}

fn validate(decision: &RefundDecision) -> Vec<String> {
    let mut problems = Vec::new();
    if !["refunded", "declined", "escalated"].contains(&decision.outcome.as_str()) {
        problems.push(format!(
            "outcome '{}' is not one of refunded, declined, escalated",
            decision.outcome
        ));
    }
    if decision.outcome == "refunded" && (decision.refund_id.is_none() || decision.refunded <= 0.0)
    {
        problems.push("a refund needs a refund id and an amount".to_string());
    }
    problems
}
// docs:end guides/ai/build-an-agent/answer-type

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Ticket {
    id: String,
    customer: String,
    message: String,
    decision: Option<RefundDecision>,
    turns: u32,
    sent: Option<String>,
}

impl Ticket {
    fn new(id: &str, customer: &str, message: &str) -> Self {
        Self {
            id: id.into(),
            customer: customer.into(),
            message: message.into(),
            ..Self::default()
        }
    }
}

#[tokio::test]
async fn run_the_support_agent() {
    // docs:begin guides/ai/build-an-agent/subset
    let tools = support_desk::registry()
        .subset(["orders.lookup", "shipping.track", "refunds.issue"])
        .expect("the desk has these tools");
    // docs:end guides/ai/build-an-agent/subset

    // docs:begin guides/ai/build-an-agent/script
    let chat_client: Arc<dyn ChatClient> = ScriptedModel::new(vec![
        turn(|_| Reply::call(&[("call_1", "orders_lookup", r#"{"orderId":"A-1001"}"#)])),
        turn(|_| {
            Reply::call(&[(
                "call_2",
                "refunds_issue",
                r#"{"orderId":"A-1001","amount":39.9,"reason":"damaged in transit"}"#,
            )])
        }),
        turn(|_| {
            Reply::say(
                r#"{"orderId":"A-1001","outcome":"refunded","refunded":39.9,"refundId":"RF-0001","reply":"Sorry about the box. I have refunded 39.90 (RF-0001)."}"#,
            )
        }),
    ]);
    // docs:end guides/ai/build-an-agent/script

    // docs:begin guides/ai/build-an-agent/create-agent
    let agent = Arc::new(Agent::<RefundDecision>::new(
        chat_client,
        Arc::new(tools),
        AgentOptions::new("support-agent")
            .with_model("gpt-5-mini")
            .with_temperature(0.0)
            .with_max_turns(6)
            .with_validate(validate),
    ));
    // docs:end guides/ai/build-an-agent/create-agent

    // The subset's tools, under provider-safe names, mapped back to the Strata
    // names. accounts.close is left out.
    let declared: Vec<&str> = agent
        .declarations()
        .declarations()
        .iter()
        .map(|d| d.name.as_str())
        .collect();
    assert_eq!(
        declared,
        ["orders_lookup", "shipping_track", "refunds_issue"]
    );
    assert_eq!(
        agent.declarations().strata_name("orders_lookup"),
        "orders.lookup"
    );
    assert!(
        agent
            .declarations()
            .provider_name_of("accounts.close")
            .is_none()
    );

    let graph = refund_graph(agent);

    // docs:begin guides/ai/build-an-agent/run
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let desk = SupportDesk::new();
    let result = graph
        .run_with(
            "ticket-1",
            Ticket::new(
                "T-1",
                "C-77",
                "My order A-1001 arrived with the box crushed.",
            ),
            store.clone(),
            RunConfig::new().with_service(desk.clone()),
        )
        .await;

    match result {
        GraphExecutionResult::Completed(ticket) => {
            let decision = ticket.decision.expect("the agent answered");
            println!("{} after {} turns", decision.outcome, ticket.turns);
            println!("{}", ticket.sent.unwrap_or_default());
        }
        GraphExecutionResult::Failed(error) => println!("The run failed: {error:?}"),
        _ => println!("The run did not complete."),
    }
    // docs:end guides/ai/build-an-agent/run

    // What the page says the run prints with this script.
    assert_eq!(
        printed(),
        "refunded after 3 turns\nto C-77: Sorry about the box. I have refunded 39.90 (RF-0001).\n"
    );
    // Turn 1 looked the order up, turn 2 refunded it, turn 3 answered.
    assert_eq!(
        desk.calls(),
        [
            "orders.lookup(A-1001)",
            "refunds.issue(A-1001, 39.9, damaged in transit)"
        ]
    );
    assert_eq!(
        desk.refunds(),
        [("RF-0001".to_string(), "A-1001".to_string(), 39.9)]
    );
}

// docs:begin guides/ai/build-an-agent/graph
fn prompt(ticket: &Ticket, _: &NodeContext) -> Vec<Message> {
    vec![
        Message::system(
            "You are a support agent. Look an order up before deciding anything. Refund an order that \
             arrived damaged within 30 days of delivery; decline other refunds; escalate what you can't do.",
        ),
        Message::user(format!(
            "Ticket {} from customer {}: {}",
            ticket.id, ticket.customer, ticket.message
        )),
    ]
}

// docs:hide
#[allow(clippy::let_and_return)]
fn refund_graph(agent: Arc<Agent<RefundDecision>>) -> CompiledGraph<Ticket> {
    // docs:show
    let graph = StateGraph::<Ticket>::new()
        .add_agent_node(
            "Resolve",
            agent,
            prompt,
            |ticket: Ticket, result: AgentResult<RefundDecision>| Ticket {
                decision: Some(result.output),
                turns: result.turns,
                ..ticket
            },
        )
        .add_node("Notify", |ticket: Ticket| Ticket {
            sent: ticket
                .decision
                .as_ref()
                .map(|d| format!("to {}: {}", ticket.customer, d.reply)),
            ..ticket
        })
        .add_edge("Resolve", "Notify")
        .set_recovery_policy("Resolve", RecoveryPolicy::retry(1))
        .compile()
        .expect("the graph is valid");
    // docs:end guides/ai/build-an-agent/graph
    graph
}

mod approval {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, StrataSchema)]
    #[serde(rename_all = "camelCase")]
    struct RefundProposal {
        order_id: String,
        amount: f64,
        reason: String,
    }

    /// The step-4 ticket, with the proposal and the refund in place of the
    /// decision.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct Ticket {
        id: String,
        customer: String,
        message: String,
        proposal: Option<RefundProposal>,
        refund: Option<String>,
    }

    impl Ticket {
        fn new(id: &str, customer: &str, message: &str) -> Self {
            Self {
                id: id.into(),
                customer: customer.into(),
                message: message.into(),
                ..Self::default()
            }
        }
    }

    fn prompt(ticket: &Ticket, _: &NodeContext) -> Vec<Message> {
        vec![
            Message::system(
                "You are a support agent. Look an order up, then propose a refund for a person to approve.",
            ),
            Message::user(format!(
                "Ticket {} from customer {}: {}",
                ticket.id, ticket.customer, ticket.message
            )),
        ]
    }

    #[tokio::test]
    async fn put_a_person_before_the_refund() {
        let desk = SupportDesk::new();
        let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
        let config = || RunConfig::new().with_service(desk.clone());
        let chat_client: Arc<dyn ChatClient> = ScriptedModel::new(vec![
            turn(|_| Reply::call(&[("call_1", "orders_lookup", r#"{"orderId":"A-1001"}"#)])),
            turn(|_| {
                Reply::say(r#"{"orderId":"A-1001","amount":39.9,"reason":"damaged in transit"}"#)
            }),
        ]);

        // docs:begin guides/ai/build-an-agent/approval
        let desk_tools = Arc::new(support_desk::registry());

        // The agent may only read. It can't call refunds.issue.
        let proposer = Arc::new(Agent::<RefundProposal>::new(
            chat_client,
            Arc::new(
                desk_tools
                    .subset(["orders.lookup", "shipping.track"])
                    .expect("known tools"),
            ),
            AgentOptions::new("refund-proposer"),
        ));

        let approval_graph = StateGraph::<Ticket>::new()
            .add_agent_node(
                "Propose",
                proposer,
                prompt,
                |ticket: Ticket, result: AgentResult<RefundProposal>| Ticket {
                    proposal: Some(result.output),
                    ..ticket
                },
            )
            .add_interrupt_boundary("Propose")
            .add_async_fallible_node("IssueRefund", move |ticket: Ticket, ctx: NodeContext| {
                let desk_tools = desk_tools.clone();
                async move {
                    let arguments = serde_json::to_string(&ticket.proposal).expect("serializable");
                    let refund = desk_tools
                        .dispatch_with("refunds.issue", &arguments, ToolCallContext::from(&ctx))
                        .await
                        .map_err(|error| NodeError::Fatal(format!("{error:?}")))?;
                    Ok(Ticket {
                        refund: Some(refund),
                        ..ticket
                    })
                }
            })
            .add_edge("Propose", "IssueRefund")
            .compile()
            .expect("the graph is valid");

        // Runs Propose, saves the thread, and returns Interrupted. Nothing is refunded yet.
        let paused = approval_graph
            .run_with(
                "ticket-2",
                Ticket::new("T-2", "C-77", "A-1001 arrived broken."),
                store.clone(),
                config(),
            )
            .await;

        // Later: a person reads the proposal in the saved state and approves it.
        let resumed = approval_graph
            .resume_with(
                "ticket-2",
                HumanSignal::approve("lead@example.com"),
                store.clone(),
                config(),
            )
            .await;
        // docs:end guides/ai/build-an-agent/approval

        // The run paused after Propose with the proposal in the state and no
        // refund.
        let GraphExecutionResult::Interrupted { frozen_state, .. } = paused else {
            panic!("expected the run to pause after Propose");
        };
        assert_eq!(
            frozen_state.proposal,
            Some(RefundProposal {
                order_id: "A-1001".into(),
                amount: 39.9,
                reason: "damaged in transit".into(),
            })
        );
        assert_eq!(frozen_state.refund, None);

        // The approval ran IssueRefund: one refund, through the full registry.
        let GraphExecutionResult::Completed(ticket) = resumed else {
            panic!("expected the resume to complete");
        };
        assert_eq!(ticket.refund.as_deref(), Some("RF-0001"));
        assert_eq!(
            desk.refunds(),
            [("RF-0001".to_string(), "A-1001".to_string(), 39.9)]
        );
    }
}

/// What the stand-in says in one turn: text, tool calls, or both. A call's
/// arguments are JSON text, `(id, name, arguments)`, as a provider sends them.
struct Reply {
    text: String,
    calls: Vec<(String, String, String)>,
}

impl Reply {
    fn say(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            calls: Vec::new(),
        }
    }

    fn call(calls: &[(&str, &str, &str)]) -> Self {
        Self {
            text: String::new(),
            calls: calls
                .iter()
                .map(|(id, name, arguments)| {
                    (id.to_string(), name.to_string(), arguments.to_string())
                })
                .collect(),
        }
    }
}

/// One scripted turn: the conversation so far in, the reply out.
type Turn = Box<dyn Fn(&[Message]) -> Reply + Send + Sync>;

fn turn(f: impl Fn(&[Message]) -> Reply + Send + Sync + 'static) -> Turn {
    Box::new(f)
}

const MODEL: &str = "support-model-stand-in";

/// A stand-in for a real model: a `ChatClient` that plays one scripted turn per
/// request, in order.
struct ScriptedModel {
    turns: Mutex<VecDeque<Turn>>,
}

impl ScriptedModel {
    fn new(turns: Vec<Turn>) -> Arc<Self> {
        Arc::new(Self {
            turns: Mutex::new(turns.into()),
        })
    }

    /// Plays the next turn, and returns the answer as the updates a provider
    /// would stream.
    fn answer(&self, request: ChatRequest) -> Result<Vec<ChatUpdate>, ChatError> {
        let turn = self
            .turns
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| ChatError::fatal("script", "the script has no more turns"))?;
        let reply = turn(&request.messages);
        let model = request.model.clone().unwrap_or_else(|| MODEL.to_string());

        let mut updates = Vec::new();
        if !reply.text.is_empty() {
            updates.push(ChatUpdate::Text(reply.text));
        }
        let finish = if reply.calls.is_empty() {
            "stop"
        } else {
            "tool_calls"
        };
        for (index, (id, name, arguments)) in reply.calls.into_iter().enumerate() {
            updates.push(ChatUpdate::ToolCall {
                index,
                id: Some(id),
                name: Some(name),
                arguments,
            });
        }
        updates.push(ChatUpdate::Usage(ModelUsage::new(Some(40), Some(10))));
        updates.push(ChatUpdate::Finish(finish.to_string()));
        updates.push(ChatUpdate::Meta {
            model: Some(model),
            id: None,
        });
        Ok(updates)
    }
}

// docs:begin guides/ai/build-an-agent/scripted-model
impl ChatClient for ScriptedModel {
    fn provider(&self) -> &str {
        "scripted"
    }

    fn default_model(&self) -> Option<&str> {
        Some(MODEL)
    }

    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>> {
        let answer = self.answer(request);
        Box::pin(async move {
            // `ChatUpdates` assembles the answer as a real client's does: it
            // parses each call's arguments, and keeps arguments that aren't a
            // JSON object as their text, for the agent to refuse.
            let mut assembled = ChatUpdates::new();
            for update in answer? {
                assembled.push(update);
            }
            assembled.finish()
        })
    }

    // `stream` plays the same answer as a stream.
    // docs:hide ...
    fn stream(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>> {
        let answer = self.answer(request);
        Box::pin(async move {
            let stream: ChatStream =
                Box::pin(futures_util::stream::iter(answer?.into_iter().map(Ok)));
            Ok(stream)
        })
    }
    // docs:show
}
// docs:end guides/ai/build-an-agent/scripted-model

/// A small support desk: an order book, a shipping tracker, a refund gateway
/// and an account service, as four `#[strata_tool]` functions. The desk is a
/// run service (`RunConfig::with_service`): each tool finds it in the call's
/// `ToolCallContext`.
mod support_desk {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use datalium_strata::prelude::*;
    use serde::{Deserialize, Serialize};

    /// An order as `orders.lookup` returns it.
    #[derive(Debug, Clone, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct OrderInfo {
        order_id: String,
        customer_id: String,
        total: f64,
        status: String,
        days_since_delivery: u32,
    }

    #[derive(Default)]
    struct Ledger {
        /// Every tool that ran, as "name(arguments)", in order.
        calls: Vec<String>,
        /// Refunds issued: (refund id, order, amount).
        refunds: Vec<(String, String, f64)>,
    }

    pub struct SupportDesk {
        orders: BTreeMap<&'static str, OrderInfo>,
        ledger: Mutex<Ledger>,
    }

    impl SupportDesk {
        pub fn new() -> Arc<Self> {
            let order = |id: &str, customer: &str, total, status: &str, days| OrderInfo {
                order_id: id.into(),
                customer_id: customer.into(),
                total,
                status: status.into(),
                days_since_delivery: days,
            };
            Arc::new(Self {
                orders: BTreeMap::from([
                    (
                        "A-1001",
                        order(
                            "A-1001",
                            "C-77",
                            39.90,
                            "delivered, box crushed in transit",
                            3,
                        ),
                    ),
                    ("A-1002", order("A-1002", "C-77", 129.00, "in transit", 0)),
                ]),
                ledger: Mutex::default(),
            })
        }

        pub fn calls(&self) -> Vec<String> {
            self.ledger.lock().unwrap().calls.clone()
        }

        pub fn refunds(&self) -> Vec<(String, String, f64)> {
            self.ledger.lock().unwrap().refunds.clone()
        }

        fn note(&self, call: String) {
            self.ledger.lock().unwrap().calls.push(call);
        }
    }

    /// The desk's registry: all four tools, in this order.
    pub fn registry() -> ToolRegistry {
        let mut registry = ToolRegistry::new();
        registry
            .register(Box::new(OrdersLookupTool))
            .register(Box::new(ShippingTrackTool))
            .register(Box::new(RefundsIssueTool))
            .register(Box::new(AccountsCloseTool));
        registry
    }

    fn desk(ctx: &ToolCallContext) -> Result<Arc<SupportDesk>, StrataError> {
        ctx.service::<SupportDesk>()
            .ok_or_else(|| StrataError::ExecutionAborted("no SupportDesk service".into()))
    }

    #[derive(Deserialize, StrataSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct OrderArgs {
        order_id: String,
    }

    // docs:begin guides/ai/build-an-agent/define-tool
    #[strata_tool(
        name = "orders.lookup",
        description = "Looks an order up by its id: customer, total, status, days since delivery."
    )]
    async fn orders_lookup(args: OrderArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
        let desk = desk(&ctx)?;
        // docs:hide ...
        desk.note(format!("orders.lookup({})", args.order_id));
        let order = desk.orders.get(args.order_id.as_str());
        Ok(serde_json::to_string(&order).expect("an order serializes"))
        // docs:show
    }
    // docs:end guides/ai/build-an-agent/define-tool

    #[strata_tool(
        name = "shipping.track",
        description = "Where an order's parcel is now."
    )]
    async fn shipping_track(args: OrderArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
        let desk = desk(&ctx)?;
        desk.note(format!("shipping.track({})", args.order_id));
        Ok(match desk.orders.get(args.order_id.as_str()) {
            Some(order) if order.status == "in transit" => {
                "in transit: left the Rotterdam hub, no delivery date yet".to_string()
            }
            _ => "delivered".to_string(),
        })
    }

    #[derive(Deserialize, StrataSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct RefundArgs {
        order_id: String,
        amount: f64,
        reason: String,
    }

    #[strata_tool(
        name = "refunds.issue",
        description = "Refunds an amount of an order to the customer's card. Returns the refund id."
    )]
    async fn refunds_issue(args: RefundArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
        let desk = desk(&ctx)?;
        desk.note(format!(
            "refunds.issue({}, {}, {})",
            args.order_id, args.amount, args.reason
        ));
        let mut ledger = desk.ledger.lock().unwrap();
        let id = format!("RF-{:04}", ledger.refunds.len() + 1);
        ledger
            .refunds
            .push((id.clone(), args.order_id, args.amount));
        Ok(id)
    }

    #[derive(Deserialize, StrataSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct CustomerArgs {
        customer_id: String,
    }

    #[strata_tool(
        name = "accounts.close",
        description = "Closes a customer's account. Not for the support agent."
    )]
    async fn accounts_close(
        args: CustomerArgs,
        ctx: ToolCallContext,
    ) -> Result<String, StrataError> {
        desk(&ctx)?.note(format!("accounts.close({})", args.customer_id));
        Ok("closed".to_string())
    }
}
