//! Samples for <https://stratamesh.dev/guides/graphs/map-fan-out/>.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::NodeContext;
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::map::MapOptions;
use datalium_strata::parallel::{BranchOutcomes, BranchStatus};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

#[tokio::test]
async fn fan_out() {
    let invoice = Invoice::of(
        "INV-2207",
        vec![
            line("L01", "PAP-A4", 40, 420),
            line("L02", "PAP-A3", 10, 810),
            line("L03", "INK-K", 4, 6200),
            line("L04", "INK-C", 2, 7100),
            line("L05", "PEN-BLU", 200, 90),
            line("L06", "PEN-RED", 50, 90),
            line("L07", "PAD-A5", 60, 240),
            line("L08", "FLD-GRN", 100, 35),
            line("L09", "INK-M", 2, 7950),
            line("L10", "SVC-INST", 1, 4500),
            line("L11", "PAP-A4", 20, 420),
            line("L12", "SVC-DLV", 1, 1200),
        ],
    );
    let graph = invoice_graph(&PriceList::new(), MapOptions::new())
        .compile()
        .expect("the invoice graph is valid");
    let store = Arc::new(InMemoryCheckpointStore::new());

    let audited = expect_completed(graph.run("map-fan-out-1", invoice.clone(), store).await);
    let ids: Vec<_> = audited.audits.iter().map(|a| a.line.id.clone()).collect();
    assert_eq!(ids, invoice.ids());
    assert_eq!(
        audited.decision.as_deref(),
        Some("hold: L09 billed 79.50, list 71.00 (+12%)")
    );
}

#[tokio::test]
async fn options() {
    // docs:begin guides/graphs/map-fan-out/options
    let options = MapOptions::new()
        .with_max_concurrency(4) // at most 4 items at once (default 8)
        .with_max_items(100) // a longer list fails the map before any item starts
        .with_item_timeout(Duration::from_secs(30)) // each item's own time limit
        .keep_succeeded(); // reduce over the items that succeeded
    // docs:end guides/graphs/map-fan-out/options

    let invoice = Invoice::of(
        "INV-2212",
        vec![
            line("L01", "PAP-A4", 10, 420),
            line("L02", "INK-K", 1, 6200),
            line("L03", "INK-X9", 1, 5800),
            line("L04", "PEN-BLU", 20, 90),
        ],
    );
    let graph = invoice_graph(&PriceList::new(), options)
        .compile()
        .expect("the invoice graph is valid");
    let store = Arc::new(InMemoryCheckpointStore::new());

    // The unknown SKU fails L03, and the plain reducer gets the other three.
    let audited = expect_completed(graph.run("map-fan-out-options", invoice, store).await);
    let ids: Vec<_> = audited.audits.iter().map(|a| a.line.id.as_str()).collect();
    assert_eq!(ids, ["L01", "L02", "L04"]);
    assert_eq!(audited.decision.as_deref(), Some("approve"));

    // An out-of-range value is refused at compile.
    let refused =
        invoice_graph(&PriceList::new(), MapOptions::new().with_max_concurrency(0)).compile();
    assert!(matches!(
        refused,
        Err(StrataError::CompilationFailed(message))
            if message == "map 'AuditLines' has a max_concurrency of 0; it must be at least 1"
    ));
}

#[tokio::test]
async fn item_fails() {
    let invoice = Invoice::of(
        "INV-2212",
        vec![
            line("L01", "PAP-A4", 10, 420),
            line("L02", "INK-K", 1, 6200),
            line("L03", "INK-X9", 1, 5800),
            line("L04", "PEN-BLU", 20, 90),
        ],
    );
    let graph = invoice_graph(&PriceList::new(), MapOptions::new())
        .set_recovery_policy("AuditLines", RecoveryPolicy::retry(2))
        .compile()
        .expect("the invoice graph is valid");
    let store = Arc::new(InMemoryCheckpointStore::new());

    match graph.run("map-fan-out-3a", invoice, store).await {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => {
            assert_eq!((node.as_str(), attempts), ("AuditLines", 1));
            assert_eq!(
                message,
                "item 'L03': failed at 'CheckPrice' after 1 attempt(s): unknown SKU 'INK-X9'"
            );
        }
        GraphExecutionResult::Failed(other) => panic!("expected NodeFailed, got {other:?}"),
        _ => panic!("expected NodeFailed"),
    }
}

#[tokio::test]
async fn keep_partial_results() {
    let mixed = Invoice::of(
        "INV-2214",
        vec![
            line("L01", "PAP-A4", 10, 420),
            line("L02", "INK-K", 1, 6200),
            line("L03", "INK-X9", 1, 5800),
            line("L04", "PEN-BLU", 20, 90),
            line("L05", "SVC-INST", 1, 5200),
        ],
    );
    let reduced: Arc<Mutex<Option<BranchOutcomes<LineAudit>>>> = Arc::default();
    let keep = reduced.clone();

    // docs:begin guides/graphs/map-fan-out/keep-partial
    let graph = StateGraph::<Invoice>::new()
        .add_map_with_outcomes(
            "AuditLines",
            |inv: &Invoice| inv.lines.clone(),
            |line: &InvoiceLine| line.id.clone(),
            line_audit_graph(&PriceList::new()),
            |_: &Invoice, line: InvoiceLine| LineAudit::for_line(line),
            move |inv: &Invoice, results: BranchOutcomes<LineAudit>| {
                let audits = results.succeeded().cloned().collect();
                let review = results
                    .outcomes()
                    .iter()
                    .filter(|o| o.status != BranchStatus::Succeeded)
                    .map(|o| {
                        let why = o.error.as_ref().map(NodeError::message).unwrap_or("?");
                        format!("{} not audited: {why}", o.name)
                    })
                    .collect();
                // docs:hide ...
                *keep.lock().unwrap() = Some(results);
                // docs:show
                Invoice {
                    audits,
                    review,
                    ..inv.clone()
                }
            },
            MapOptions::new().keep_succeeded(),
        )
        .add_inline_node("Decide", decide)
        .add_edge("AuditLines", "Decide")
        .compile()
        .expect("the invoice graph is valid");
    // docs:end guides/graphs/map-fan-out/keep-partial

    let store = Arc::new(InMemoryCheckpointStore::new());
    let kept = expect_completed(graph.run("map-fan-out-4", mixed, store).await);
    let seen = reduced.lock().unwrap().take().expect("the reducer ran");
    let statuses: Vec<_> = seen.outcomes().iter().map(|o| o.status).collect();
    assert_eq!(
        statuses,
        [
            BranchStatus::Succeeded,
            BranchStatus::Succeeded,
            BranchStatus::Failed,
            BranchStatus::Succeeded,
            BranchStatus::Succeeded,
        ]
    );
    assert!(!seen.join_met());
    let l03 = seen
        .get("L03")
        .and_then(|o| o.error.clone())
        .expect("L03 failed");
    assert!(!l03.is_transient());
    assert_eq!(
        kept.decision.as_deref(),
        Some("hold: 2 line(s) need review")
    );
    assert_eq!(
        kept.review,
        [
            "L03 not audited: failed at 'CheckPrice' after 1 attempt(s): unknown SKU 'INK-X9'",
            "L05 billed 52.00, list 45.00 (+16%)",
        ]
    );
}

// docs:begin guides/graphs/map-fan-out/parent
fn invoice_graph(prices: &Arc<PriceList>, options: MapOptions<LineAudit>) -> StateGraph<Invoice> {
    StateGraph::<Invoice>::new()
        .add_map_with(
            "AuditLines",
            |inv: &Invoice| inv.lines.clone(),
            |line: &InvoiceLine| line.id.clone(),
            line_audit_graph(prices),
            |_: &Invoice, line: InvoiceLine| LineAudit::for_line(line),
            |inv: &Invoice, audits: Vec<LineAudit>| Invoice {
                audits,
                ..inv.clone()
            },
            options,
        )
        .add_inline_node("Decide", decide)
        .add_edge("AuditLines", "Decide")
}
// docs:end guides/graphs/map-fan-out/parent

// docs:begin guides/graphs/map-fan-out/child
fn line_audit_graph(prices: &Arc<PriceList>) -> Arc<CompiledGraph<LineAudit>> {
    let prices = prices.clone();
    Arc::new(
        StateGraph::<LineAudit>::new()
            .add_inline_node("Classify", |audit: LineAudit| LineAudit {
                category: Some(category(&audit.line.sku).to_string()),
                ..audit
            })
            .add_async_fallible_node("CheckPrice", move |audit: LineAudit, _: NodeContext| {
                let prices = prices.clone();
                async move {
                    let list = prices.look_up(&audit.line).await?;
                    Ok(LineAudit {
                        list_price: Some(list),
                        ..audit
                    })
                }
            })
            .add_inline_node("Score", score)
            .add_edge("Classify", "CheckPrice")
            .add_edge("CheckPrice", "Score")
            .set_recovery_policy("CheckPrice", RecoveryPolicy::retry(1))
            .compile()
            .expect("the line audit graph is valid"),
    )
}
// docs:end guides/graphs/map-fan-out/child

fn category(sku: &str) -> &'static str {
    match sku.split('-').next().unwrap_or_default() {
        "PAP" | "PAD" => "paper",
        "INK" => "toner",
        "PEN" | "FLD" => "stationery",
        "SVC" => "service",
        _ => "other",
    }
}

/// 100 at or below list price, 4 points off per percent over it.
fn score(audit: LineAudit) -> LineAudit {
    let list = audit.list_price.expect("CheckPrice sets the list price");
    let billed = audit.line.unit_price;
    let over = ((billed - list) as f64 / list as f64 * 100.0).round() as i64;
    if over <= 0 {
        return LineAudit {
            score: Some(100),
            ..audit
        };
    }
    LineAudit {
        score: Some((100 - 4 * over).max(0)),
        note: Some(format!(
            "billed {}, list {} (+{over}%)",
            money(billed),
            money(list)
        )),
        ..audit
    }
}

/// Any line scoring under 80 (or not audited at all) holds the invoice.
fn decide(invoice: Invoice) -> Invoice {
    let mut review = invoice.review.clone();
    review.extend(
        invoice
            .audits
            .iter()
            .filter(|a| a.score.unwrap_or(0) < 80)
            .map(|a| format!("{} {}", a.line.id, a.note.as_deref().unwrap_or_default())),
    );
    let decision = match review.len() {
        0 => "approve".to_string(),
        1 if invoice.review.is_empty() => format!("hold: {}", review[0]),
        n => format!("hold: {n} line(s) need review"),
    };
    Invoice {
        review,
        decision: Some(decision),
        ..invoice
    }
}

fn expect_completed(result: GraphExecutionResult<Invoice>) -> Invoice {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(error) => panic!("expected Completed, got {error:?}"),
        _ => panic!("expected Completed"),
    }
}

// Amounts are integer cents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct InvoiceLine {
    id: String,
    sku: String,
    quantity: u32,
    unit_price: i64,
}

fn line(id: &str, sku: &str, quantity: u32, unit_price: i64) -> InvoiceLine {
    InvoiceLine {
        id: id.to_string(),
        sku: sku.to_string(),
        quantity,
        unit_price,
    }
}

/// The parent's state: the lines to audit, the audits that came back, and the decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Invoice {
    number: String,
    lines: Vec<InvoiceLine>,
    audits: Vec<LineAudit>,
    review: Vec<String>,
    decision: Option<String>,
}

impl Invoice {
    fn of(number: &str, lines: Vec<InvoiceLine>) -> Self {
        Self {
            number: number.to_string(),
            lines,
            audits: Vec::new(),
            review: Vec::new(),
            decision: None,
        }
    }

    fn ids(&self) -> Vec<String> {
        self.lines.iter().map(|l| l.id.clone()).collect()
    }
}

/// One item's child state: the line, and what Classify, CheckPrice and Score found.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LineAudit {
    line: InvoiceLine,
    category: Option<String>,
    list_price: Option<i64>,
    score: Option<i64>,
    note: Option<String>,
}

impl LineAudit {
    fn for_line(line: InvoiceLine) -> Self {
        Self {
            line,
            category: None,
            list_price: None,
            score: None,
            note: None,
        }
    }
}

fn money(cents: i64) -> String {
    format!("{}.{:02}", cents / 100, cents % 100)
}

/// The supplier's price list, in memory, so the samples run offline.
struct PriceList;

impl PriceList {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }

    async fn look_up(&self, line: &InvoiceLine) -> Result<i64, NodeError> {
        tokio::task::yield_now().await;
        let price = match line.sku.as_str() {
            "PAP-A4" => 420,
            "PAP-A3" => 810,
            "PAD-A5" => 240,
            "INK-K" => 6200,
            "INK-C" => 7100,
            "INK-M" => 7100,
            "PEN-BLU" => 90,
            "PEN-RED" => 90,
            "FLD-GRN" => 35,
            "SVC-INST" => 4500,
            "SVC-DLV" => 1200,
            _ => return Err(NodeError::Fatal(format!("unknown SKU '{}'", line.sku))),
        };
        Ok(price)
    }
}
