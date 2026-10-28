//! `strata thread list` / `strata thread inspect` (design/05.Strata-CLI.md),
//! read through the query port (`design/19-Async-Queryable-Store.md`).

use std::process::ExitCode;

use chrono::{DateTime, Utc};
use datalium_strata::checkpoint::{ThreadQuery, ThreadState, ThreadSummary};
use tabled::Tabled;

use crate::store::{self, Storage};

#[derive(Tabled)]
struct ThreadTableRow {
    #[tabled(rename = "Thread ID")]
    thread_id: String,
    #[tabled(rename = "Current Node")]
    current_node: String,
    #[tabled(rename = "Sequence ID")]
    sequence: i64,
    #[tabled(rename = "State")]
    state: String,
    #[tabled(rename = "Waiting For")]
    wait: String,
    #[tabled(rename = "Updated At")]
    updated_at: String,
}

fn to_table_row(thread: &ThreadSummary) -> ThreadTableRow {
    ThreadTableRow {
        thread_id: thread.thread_id.clone(),
        // A thread frozen, failed or cancelled inside a subgraph shows where, e.g. `Work/Approve`.
        current_node: if thread.waiting_at.len() > 1 {
            thread.waiting_at.join("/")
        } else {
            thread
                .suspended_node
                .clone()
                .or_else(|| thread.next_node.clone())
                .unwrap_or_else(|| "-".to_string())
        },
        sequence: thread.sequence,
        state: thread.state.as_str().to_string(),
        wait: thread
            .wait
            .as_ref()
            .map_or_else(|| "-".to_string(), store::describe_wait),
        updated_at: datalium_strata::store::timestamp_text(thread.updated_at),
    }
}

fn thread_to_json(thread: &ThreadSummary) -> serde_json::Value {
    serde_json::json!({
        "thread_id": thread.thread_id,
        "sequence": thread.sequence,
        "state": thread.state.as_str(),
        "status": store::status_name(thread.status),
        "suspended_node": thread.suspended_node,
        "next_node": thread.next_node,
        "waiting_at": thread.waiting_at,
        "metadata": thread.metadata,
        "updated_at": datalium_strata::store::timestamp_text(thread.updated_at),
        "wait": thread.wait,
        "graph": thread.graph,
    })
}

async fn open(storage: &str, blobs: Option<&str>) -> Result<Storage, ExitCode> {
    store::open_with(storage, blobs).await.map_err(|err| {
        eprintln!("strata: failed to open storage '{storage}': {err}");
        ExitCode::from(2)
    })
}

pub async fn list(
    storage: &str,
    status: Option<&str>,
    meta: &[(String, String)],
    due_by: Option<DateTime<Utc>>,
    graph: Option<&str>,
    graph_version: Option<&str>,
    json: bool,
) -> ExitCode {
    let mut query = ThreadQuery::default().with_limit(datalium_strata::checkpoint::MAX_PAGE_SIZE);
    // One graph's threads (design/31-Graph-Identity.md).
    if let Some(graph) = graph {
        query = query.with_graph(graph);
    }
    if let Some(version) = graph_version {
        query = query.with_graph_version(version);
    }
    if let Some(due_by) = due_by {
        // Waiting threads whose timer or timeout is due (design/29-Event-Waits-And-Timers.md).
        query = query.due_by(due_by);
    }
    if let Some(status) = status {
        // The thread's state: a resumed thread is no longer `interrupted`.
        let Some(state) = ThreadState::parse(status) else {
            let names: Vec<&str> = ThreadState::ALL.iter().map(|s| s.as_str()).collect();
            eprintln!(
                "strata: unknown status '{status}' (expected one of: {})",
                names.join(", ")
            );
            return ExitCode::from(2);
        };
        query = query.with_states([state]);
    }
    for (key, value) in meta {
        query = query.with_metadata(key, value);
    }

    let storage_handle = match open(storage, None).await {
        Ok(storage) => storage,
        Err(code) => return code,
    };
    let threads = match storage_handle.threads(query).await {
        Ok(threads) => threads,
        Err(err) => {
            eprintln!("strata: query failed: {err}");
            return ExitCode::from(2);
        }
    };

    if json {
        let out: Vec<_> = threads.iter().map(thread_to_json).collect();
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else if threads.is_empty() {
        println!("No threads found.");
    } else {
        let table_rows: Vec<ThreadTableRow> = threads.iter().map(to_table_row).collect();
        println!("{}", tabled::Table::new(table_rows));
    }

    ExitCode::SUCCESS
}

pub async fn inspect(thread_id: &str, storage: &str, blobs: Option<&str>, json: bool) -> ExitCode {
    let storage_handle = match open(storage, blobs).await {
        Ok(storage) => storage,
        Err(code) => return code,
    };

    let log = match storage_handle.log(thread_id).await {
        Ok(log) => log,
        Err(err) => {
            eprintln!("strata: query failed: {err}");
            return ExitCode::from(2);
        }
    };

    let Some(latest_entry) = log.last() else {
        eprintln!("strata: no checkpoints found for thread '{thread_id}'");
        return ExitCode::from(1);
    };
    let latest = &latest_entry.checkpoint;
    let state = ThreadState::of(latest);
    // Who holds the thread now, if anyone (design/32-Thread-Leases.md).
    let lease = match storage_handle.lease(thread_id).await {
        Ok(lease) => lease,
        Err(err) => {
            eprintln!("strata: query failed: {err}");
            return ExitCode::from(2);
        }
    };

    if json {
        let out = serde_json::json!({
            "thread_id": thread_id,
            "sequence": latest_entry.sequence,
            "state": state.as_str(),
            "status": store::status_name(latest.status),
            "suspended_node": latest.suspended_node,
            "next_node": latest.next_node,
            "created_at": latest.created_at,
            "resumed_at": latest.resumed_at,
            "checkpoint_count": log.len(),
            "frozen_state": latest.frozen_state,
            "metadata": latest.metadata,
            "nested": latest.nested,
            "decision": latest.decision,
            "wait": latest.innermost_wait(),
            "graph": latest.graph,
            "lease": lease.as_ref().map(store::lease_json),
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else {
        println!("Thread: {thread_id}");
        println!("  Status:          {}", store::status_name(latest.status));
        println!("  Thread state:    {}", state.as_str());
        println!(
            "  Current node:    {}",
            latest.suspended_node.as_deref().unwrap_or("-")
        );
        for (at, identity) in store::graphs(latest) {
            match at {
                None => println!("  Graph:           {identity}"),
                Some(at) => println!("  {:<16} {identity}", format!("Graph at {at}:")),
            }
        }
        if let Some(lease) = &lease {
            println!("  Lease:           {}", store::lease_text(lease));
        }
        // A running thread says where its run is, or was when its process
        // died (design/33-Durable-Execution.md); any other, where it stopped.
        if let Some(running) = store::running_text(latest) {
            println!("  Running:         {running}");
        } else if let Some(path) = store::waiting_path(latest) {
            println!(
                "  {:<16} {path}",
                format!("{} at:", store::stopped_verb(latest.status))
            );
        }
        if let Some(recoveries) = store::recoveries(latest) {
            println!("  Recoveries:      {recoveries}");
        }
        if let Some(reason) = store::cancel_reason(latest).or_else(|| store::end_reason(latest)) {
            println!("  Reason:          {reason}");
        }
        if let Some(ended_by) = store::ended_by(latest) {
            println!("  Ended by:        {ended_by}");
        }
        if let Some(hops) = store::hops(latest) {
            println!("  Hops:            {hops}");
        }
        if state == ThreadState::Interrupted
            && let Some(wait) = latest.innermost_wait()
        {
            println!("  Waiting for:     {}", store::wait_subject(wait));
            if let Some(line) = store::wake_line(wait) {
                println!("  {line}");
            }
        }
        for decision in store::decisions(latest) {
            println!("  Decision:        {decision}");
        }
        let metadata = store::caller_metadata(latest);
        if !metadata.is_empty() {
            let pairs: Vec<String> = metadata
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect();
            println!("  Metadata:        {}", pairs.join(", "));
        }
        println!(
            "  Next node:       {}",
            latest.next_node.as_deref().unwrap_or("-")
        );
        println!("  Checkpoint count: {}", log.len());
        println!("  Created at:      {}", latest.created_at);
        println!(
            "  Resumed at:      {}",
            latest.resumed_at.as_deref().unwrap_or("-")
        );
        println!("  State:");
        println!(
            "{}",
            serde_json::to_string_pretty(&latest.frozen_state).unwrap()
        );
    }

    ExitCode::SUCCESS
}
