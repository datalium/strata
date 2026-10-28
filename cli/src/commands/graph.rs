//! `strata graph view` / `strata graph validate` (design/05.Strata-CLI.md).

use std::collections::HashSet;
use std::process::ExitCode;

use datalium_strata::parallel::PartialResults;
use datalium_strata::schema::{
    GraphDefinition, GraphInterruptDef, GraphJoinDef, GraphNodeDef, GraphNodeKind, GraphWaitDef,
    WaitKind,
};
use datalium_strata::validate::{self, ValidationErrorKind, ValidationResult};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::widgets::{Block, Borders, Paragraph};

/// Reads a graph definition file, refusing one that isn't a graph definition
/// this build understands: unparseable, a `schema_version` it doesn't know, or
/// a node whose `kind` doesn't match its fields, at any level
/// (`design/22-Graph-Structure-In-Definitions.md`). The engines never write
/// such a file, so it is a broken file rather than a broken graph (exit `2`,
/// not `1`).
fn read_graph(path: &str) -> Result<GraphDefinition, String> {
    let raw =
        std::fs::read_to_string(path).map_err(|err| format!("failed to read '{path}': {err}"))?;

    let graph: GraphDefinition = serde_json::from_str(&raw)
        .map_err(|err| format!("failed to parse '{path}' as a graph definition: {err}"))?;
    graph
        .check_shape()
        .map_err(|err| format!("'{path}' is not a graph definition: {err}"))?;
    Ok(graph)
}

fn load_graph(path: &str) -> Result<GraphDefinition, ExitCode> {
    read_graph(path).map_err(|message| {
        eprintln!("strata: {message}");
        ExitCode::from(2)
    })
}

/// Validates one graph definition file, or — when `path` is a directory —
/// every `*.json` file under it (recursively, in path order).
///
/// Exit code: `0` if every graph is valid, `1` if any has structural errors,
/// `2` if a file can't be read or isn't a graph definition (that outranks `1`:
/// a gate that couldn't check something shouldn't look like a plain
/// rejection). A directory with no `*.json` files is also `2` rather than a
/// vacuous pass.
pub fn validate(path: &str, json: bool) -> ExitCode {
    if std::path::Path::new(path).is_dir() {
        return validate_directory(path, json);
    }

    let graph = match load_graph(path) {
        Ok(graph) => graph,
        Err(code) => return code,
    };

    let result = validate::validate(&graph);

    if json {
        print_validate_json(&graph, &result);
    } else {
        print_validate_human(path, &result);
    }

    if result.is_valid() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn print_validate_human(path: &str, result: &ValidationResult) {
    if result.is_valid() {
        println!("OK: '{path}' is a valid graph.");
        return;
    }

    println!(
        "FAILED: '{path}' has {} structural error(s):",
        result.errors.len()
    );
    print_error_lines(result);
}

fn print_error_lines(result: &ValidationResult) {
    for error in &result.errors {
        match &error.related_node {
            // The related name is a branch of the group, not an edge's target.
            Some(branch) if error.kind == ValidationErrorKind::DuplicateBranch => println!(
                "  [{}] {}/{}: {}",
                error.code, error.node, branch, error.message
            ),
            Some(related) => println!(
                "  [{}] {} -> {}: {}",
                error.code, error.node, related, error.message
            ),
            None => println!("  [{}] {}: {}", error.code, error.node, error.message),
        }
    }
}

fn errors_json(result: &ValidationResult) -> Vec<serde_json::Value> {
    result
        .errors
        .iter()
        .map(|e| {
            serde_json::json!({
                "kind": format!("{:?}", e.kind),
                "code": e.code,
                "node": e.node,
                "related_node": e.related_node,
                "message": e.message,
            })
        })
        .collect()
}

fn print_validate_json(graph: &GraphDefinition, result: &ValidationResult) {
    let output = serde_json::json!({
        "valid": result.is_valid(),
        "entry_point": graph.entry_point,
        "errors": errors_json(result),
    });

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

/// What checking one file in a directory came to.
enum FileOutcome {
    Checked(Box<GraphDefinition>, ValidationResult),
    Unreadable(String),
}

/// Every `*.json` file under `dir`, recursively, sorted by path so the report
/// (and so a CI log) is the same on every machine. Symlinked directories
/// aren't followed.
fn collect_json_files(
    dir: &std::path::Path,
    out: &mut Vec<std::path::PathBuf>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_json_files(&path, out)?;
        } else if file_type.is_file()
            && path
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        {
            out.push(path);
        }
    }
    Ok(())
}

fn validate_directory(dir: &str, json: bool) -> ExitCode {
    let mut files = Vec::new();
    if let Err(err) = collect_json_files(std::path::Path::new(dir), &mut files) {
        eprintln!("strata: failed to scan '{dir}': {err}");
        return ExitCode::from(2);
    }
    files.sort();

    if files.is_empty() {
        eprintln!("strata: no graph definition files (*.json) found under '{dir}'");
        return ExitCode::from(2);
    }

    let outcomes: Vec<(String, FileOutcome)> = files
        .iter()
        .map(|file| {
            let display = file.display().to_string();
            let outcome = match read_graph(&display) {
                Ok(graph) => {
                    let result = validate::validate(&graph);
                    FileOutcome::Checked(Box::new(graph), result)
                }
                Err(message) => FileOutcome::Unreadable(message),
            };
            (display, outcome)
        })
        .collect();

    let valid = outcomes
        .iter()
        .filter(|(_, o)| matches!(o, FileOutcome::Checked(_, r) if r.is_valid()))
        .count();
    let unreadable = outcomes
        .iter()
        .filter(|(_, o)| matches!(o, FileOutcome::Unreadable(_)))
        .count();
    let invalid = outcomes.len() - valid - unreadable;

    if json {
        print_directory_json(&outcomes, valid, invalid, unreadable);
    } else {
        print_directory_human(&outcomes, valid, invalid, unreadable);
    }

    if unreadable > 0 {
        ExitCode::from(2)
    } else if invalid > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn print_directory_human(
    outcomes: &[(String, FileOutcome)],
    valid: usize,
    invalid: usize,
    unreadable: usize,
) {
    for (path, outcome) in outcomes {
        match outcome {
            FileOutcome::Checked(_, result) if result.is_valid() => println!("OK      {path}"),
            FileOutcome::Checked(_, result) => {
                println!(
                    "FAILED  {path}  ({} structural error(s))",
                    result.errors.len()
                );
                print_error_lines(result);
            }
            FileOutcome::Unreadable(message) => println!("ERROR   {message}"),
        }
    }

    println!(
        "{} graph file(s) checked: {valid} valid, {invalid} invalid, {unreadable} unreadable.",
        outcomes.len()
    );
}

fn print_directory_json(
    outcomes: &[(String, FileOutcome)],
    valid: usize,
    invalid: usize,
    unreadable: usize,
) {
    let graphs: Vec<serde_json::Value> = outcomes
        .iter()
        .map(|(path, outcome)| match outcome {
            FileOutcome::Checked(graph, result) => serde_json::json!({
                "path": path,
                "valid": result.is_valid(),
                "entry_point": graph.entry_point,
                "errors": errors_json(result),
            }),
            FileOutcome::Unreadable(message) => serde_json::json!({
                "path": path,
                "valid": false,
                "error": message,
            }),
        })
        .collect();

    let output = serde_json::json!({
        "valid": invalid == 0 && unreadable == 0,
        "checked": outcomes.len(),
        "valid_count": valid,
        "invalid_count": invalid,
        "unreadable_count": unreadable,
        "graphs": graphs,
    });

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

pub fn view(path: &str, json: bool) -> ExitCode {
    let graph = match load_graph(path) {
        Ok(graph) => graph,
        Err(code) => return code,
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&graph).unwrap());
    } else {
        println!("{}", render_ascii(&graph));
    }

    ExitCode::SUCCESS
}

/// The graph's default "backbone" path: starting at `entry_point`,
/// repeatedly follow the first *unconditional* outgoing edge (one added via
/// `.add_edge(...)`, not `.add_conditional_edge(...)`).
///
/// This is a static approximation, not a guarantee of the path any given
/// run actually takes: `CompiledGraph::execute_from` (Phase 4) evaluates
/// conditional edges' runtime predicates in declaration order and can
/// follow one of those instead of the unconditional edge if its predicate
/// matches the live state — something this command, reading a plain JSON
/// file with no predicate closures in it, has no way to evaluate. Branches
/// and guarded retries are listed as text annotations below the diagram
/// rather than laid out visually (see `extra_edges`), since a general
/// graph-layout algorithm is out of scope. Stops on a dead end or on
/// revisiting an already-seen node (an unguarded cycle, which `.compile()`
/// rejects anyway — see STRATA-SPEC-004 — so this is just a defensive guard
/// against looping forever over an invalid graph passed straight to
/// `view`).
fn main_chain(graph: &GraphDefinition) -> Vec<String> {
    let mut chain = vec![graph.entry_point.clone()];
    let mut seen: HashSet<String> = HashSet::from([graph.entry_point.clone()]);
    let mut current = graph.entry_point.clone();

    while let Some(edge) = graph.edges.iter().find(|e| e.from == current && !e.guarded) {
        if !seen.insert(edge.to.clone()) {
            break;
        }
        chain.push(edge.to.clone());
        current = edge.to.clone();
    }

    chain
}

/// Edges not part of the rendered main chain (guarded retry edges, branches)
/// — annotated as text below the diagram rather than laid out visually,
/// since a general graph-layout algorithm is out of scope here.
fn extra_edges<'a>(
    graph: &'a GraphDefinition,
    chain: &[String],
) -> Vec<(&'a str, &'a str, Option<&'a str>)> {
    let consecutive: HashSet<(&str, &str)> = chain
        .windows(2)
        .map(|w| (w[0].as_str(), w[1].as_str()))
        .collect();

    graph
        .edges
        .iter()
        .filter(|e| !consecutive.contains(&(e.from.as_str(), e.to.as_str())))
        .map(|e| (e.from.as_str(), e.to.as_str(), e.condition.as_deref()))
        .collect()
}

fn render_ascii(graph: &GraphDefinition) -> String {
    let chain = main_chain(graph);

    let box_width = chain
        .iter()
        .map(|name| name.len() as u16)
        .max()
        .unwrap_or(4)
        + 4;
    const BOX_HEIGHT: u16 = 3;
    const ARROW_HEIGHT: u16 = 1;
    let total_height =
        (chain.len() as u16) * BOX_HEIGHT + (chain.len().saturating_sub(1) as u16) * ARROW_HEIGHT;

    let backend = TestBackend::new(box_width, total_height.max(1));
    let mut terminal = Terminal::new(backend).expect("TestBackend never fails to initialize");

    terminal
        .draw(|frame| {
            let mut constraints = Vec::new();
            for i in 0..chain.len() {
                constraints.push(Constraint::Length(BOX_HEIGHT));
                if i + 1 < chain.len() {
                    constraints.push(Constraint::Length(ARROW_HEIGHT));
                }
            }
            let areas = Layout::default()
                .direction(Direction::Vertical)
                .constraints(constraints)
                .split(frame.area());

            let mut area_index = 0;
            for (i, name) in chain.iter().enumerate() {
                let block = Paragraph::new(name.as_str())
                    .alignment(Alignment::Center)
                    .block(Block::default().borders(Borders::ALL));
                frame.render_widget(block, areas[area_index]);
                area_index += 1;

                if i + 1 < chain.len() {
                    let arrow = Paragraph::new("\u{2502}").alignment(Alignment::Center);
                    frame.render_widget(arrow, areas[area_index]);
                    area_index += 1;
                }
            }
        })
        .expect("rendering to a TestBackend never fails");

    let mut out = String::new();
    // Which graph this is, when it was compiled with an identity
    // (design/31-Graph-Identity.md).
    if let Some(identity) = &graph.identity {
        out.push_str(&format!(
            "Graph: {}\n\n",
            crate::store::identity_text(identity)
        ));
    }
    out.push_str(&buffer_to_string(terminal.backend().buffer()));

    let extra = extra_edges(graph, &chain);
    if !extra.is_empty() {
        out.push_str("\nAdditional edges:\n");
        for (from, to, condition) in extra {
            match condition {
                Some(cond) => out.push_str(&format!("  {from} -> {to}  (guarded: {cond})\n")),
                None => out.push_str(&format!("  {from} -> {to}  (guarded)\n")),
            }
        }
    }

    let composite = composite_nodes(graph);
    if !composite.is_empty() {
        out.push_str("\nComposite nodes:\n");
        for line in composite {
            out.push_str(&format!("  {line}\n"));
        }
    }

    out
}

/// A join other than the default, with its count, condition and partial mode
/// (`design/26-Parallel-Group-Controls-And-Joins.md`), as `, join …`; empty
/// for none.
fn join_annotation(join: Option<&GraphJoinDef>) -> String {
    join.map(|join| {
        let count = join.count.map(|n| format!(" {n}")).unwrap_or_default();
        let condition = join
            .condition
            .as_ref()
            .map(|c| format!(" ({c})"))
            .unwrap_or_default();
        let partial = match join.partial {
            PartialResults::FailGroup => "fail_group",
            PartialResults::KeepSucceeded => "keep_succeeded",
        };
        format!(
            ", join {}{count}{condition}, {partial}",
            join.policy.as_str()
        )
    })
    .unwrap_or_default()
}

/// A child's identity (`design/31-Graph-Identity.md`), as ` remediation
/// version 5`; empty for none.
fn identity_annotation(graph: &GraphDefinition) -> String {
    graph
        .identity
        .as_ref()
        .map(|identity| format!(" {}", crate::store::identity_text(identity)))
        .unwrap_or_default()
}

fn node_count(graph: &GraphDefinition) -> String {
    let count = graph.nodes.len();
    format!("{count} node{}", if count == 1 { "" } else { "s" })
}

/// One line per parallel group, subgraph, map, wait node or interrupt
/// boundary, in node order (`design/22-Graph-Structure-In-Definitions.md`,
/// `design/27-Map-Fan-Out.md`, `design/48-Boundaries-In-Definitions.md`). A
/// composite node that is a boundary says so on its own line. The child isn't
/// drawn: `--json` prints the whole definition.
fn composite_nodes(graph: &GraphDefinition) -> Vec<String> {
    graph
        .nodes
        .iter()
        .filter_map(|node| {
            let line = kind_annotation(node);
            match (line, node.interrupt.as_ref()) {
                (line, None) => line,
                (None, Some(interrupt)) => Some(format!(
                    "{}: {}",
                    node.name,
                    boundary_annotation(graph, &node.name, interrupt)
                )),
                (Some(line), Some(interrupt)) => Some(format!(
                    "{line}; {}",
                    boundary_annotation(graph, &node.name, interrupt)
                )),
            }
        })
        .collect()
}

/// What an interrupt boundary does (`design/48-Boundaries-In-Definitions.md`):
/// `interrupt boundary, rejects to Abort, times out after 30m to Escalate`, or
/// `…, times out after 30m, then the thread ends` with no timeout route.
fn boundary_annotation(
    graph: &GraphDefinition,
    name: &str,
    interrupt: &GraphInterruptDef,
) -> String {
    let route = |condition: &str| {
        graph
            .edges
            .iter()
            .find(|edge| edge.from == name && edge.condition.as_deref() == Some(condition))
            .map(|edge| edge.to.as_str())
    };
    let mut text = "interrupt boundary".to_string();
    if let Some(reject) = route("on_reject") {
        text.push_str(&format!(", rejects to {reject}"));
    }
    if let Some(timeout) = interrupt.timeout_ms {
        match route("on_timeout") {
            Some(to) => text.push_str(&format!(
                ", times out after {} to {to}",
                duration_text(timeout)
            )),
            None => text.push_str(&format!(
                ", times out after {}, then the thread ends",
                duration_text(timeout)
            )),
        }
    }
    text
}

/// A parallel group's, subgraph's, map's or wait node's line; `None` for a
/// plain node.
fn kind_annotation(node: &GraphNodeDef) -> Option<String> {
    match node.kind? {
        GraphNodeKind::ParallelGroup => {
            let branches: Vec<&str> = node
                .branches
                .iter()
                .flatten()
                .map(|branch| branch.name.as_str())
                .collect();
            Some(format!(
                "{}: parallel group [{}]{}",
                node.name,
                branches.join(", "),
                join_annotation(node.join.as_ref())
            ))
        }
        GraphNodeKind::Subgraph => {
            let child = node.graph.as_deref()?;
            Some(format!(
                "{}: subgraph{}, entry {}, {}",
                node.name,
                identity_annotation(child),
                child.entry_point,
                node_count(child)
            ))
        }
        GraphNodeKind::Map => {
            let template = node.graph.as_deref()?;
            Some(format!(
                "{}: map over a template{}, entry {}, {}{}",
                node.name,
                identity_annotation(template),
                template.entry_point,
                node_count(template),
                join_annotation(node.join.as_ref())
            ))
        }
        GraphNodeKind::Wait => Some(format!(
            "{}: {}",
            node.name,
            wait_annotation(node.wait.as_ref()?)
        )),
    }
}

/// What a wait node waits for (`design/29-Event-Waits-And-Timers.md`): `wait
/// for event deployment.finished, times out after 1h`, `timer, 5m`, `timer,
/// until a time from the state`.
fn wait_annotation(wait: &GraphWaitDef) -> String {
    match wait.kind {
        WaitKind::Timer => match wait.delay_ms {
            Some(delay) => format!("timer, {}", duration_text(delay)),
            None => "timer, until a time from the state".to_string(),
        },
        _ => {
            let event = wait.event_type.as_deref().unwrap_or_default();
            match wait.timeout_ms {
                Some(timeout) => format!(
                    "wait for event {event}, times out after {}",
                    duration_text(timeout)
                ),
                None => format!("wait for event {event}"),
            }
        }
    }
}

/// Milliseconds in the largest whole unit: `1h`, `5m`, `30s`, `250ms`.
fn duration_text(ms: u64) -> String {
    if ms > 0 && ms.is_multiple_of(3_600_000) {
        format!("{}h", ms / 3_600_000)
    } else if ms > 0 && ms.is_multiple_of(60_000) {
        format!("{}m", ms / 60_000)
    } else if ms > 0 && ms.is_multiple_of(1_000) {
        format!("{}s", ms / 1_000)
    } else {
        format!("{ms}ms")
    }
}

fn buffer_to_string(buffer: &ratatui::buffer::Buffer) -> String {
    let area = buffer.area;
    let mut out = String::new();
    for y in area.top()..area.bottom() {
        for x in area.left()..area.right() {
            out.push_str(buffer[(x, y)].symbol());
        }
        out.push('\n');
    }
    out
}
