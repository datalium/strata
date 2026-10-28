//! `strata` CLI library crate — a thin `main.rs` binary wraps [`run`] so
//! integration tests (`tests/`) can seed a checkpoint log through `store`
//! directly instead of only being able to drive the CLI as a black-box
//! subprocess (see `tests/checkpoint_replay_interop.rs`).

pub mod commands;
pub mod store;

use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "strata", about = "Datalium Strata CLI", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Inspect and validate agent graph definitions.
    Graph {
        #[command(subcommand)]
        command: GraphCommand,
    },
    /// List and inspect conversation threads.
    Thread {
        #[command(subcommand)]
        command: ThreadCommand,
    },
    /// Inspect the checkpoint history of a thread.
    Checkpoint {
        #[command(subcommand)]
        command: CheckpointCommand,
    },
}

#[derive(Subcommand)]
pub enum GraphCommand {
    /// Render a visual representation of a graph definition.
    View {
        #[arg(long)]
        path: String,
        #[arg(long)]
        json: bool,
    },
    /// Run compile-time structural validation (STRATA-SPEC-004) on a graph
    /// definition — or, given a directory, on every `*.json` file under it.
    ///
    /// Exit code: 0 all valid, 1 structural errors, 2 unreadable input.
    Validate {
        /// A graph definition file, or a directory of them.
        #[arg(long)]
        path: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
pub enum ThreadCommand {
    /// List threads and their current status.
    List {
        /// A SQLite file, or a `postgres://` or `redis://` URL (a Redis key
        /// prefix other than `strata` goes in `?prefix=<name>`).
        #[arg(long)]
        storage: String,
        /// Only threads in this state: interrupted, failed, cancelled,
        /// resumed, completed, ended or running. A resumed thread is no longer
        /// interrupted; a running one is running, or its process died
        /// (design/33-Durable-Execution.md).
        #[arg(long)]
        status: Option<String>,
        /// Only waiting threads whose timer or timeout is due now
        /// (design/29-Event-Waits-And-Timers.md).
        #[arg(long, conflicts_with = "due_by")]
        due: bool,
        /// Only waiting threads whose timer or timeout is due at this RFC 3339
        /// time.
        #[arg(long = "due-by", value_name = "TIME", value_parser = parse_time)]
        due_by: Option<chrono::DateTime<chrono::Utc>>,
        /// Only threads whose latest checkpoint has this metadata
        /// (`key=value`, the engine's keys included). Repeat it to require several.
        #[arg(long = "meta", value_name = "KEY=VALUE", value_parser = parse_meta)]
        meta: Vec<(String, String)>,
        /// Only threads written by the graph with this id
        /// (design/31-Graph-Identity.md).
        #[arg(long, value_name = "ID")]
        graph: Option<String>,
        /// Only threads written by this version of the `--graph` graph.
        #[arg(long = "graph-version", value_name = "VERSION", requires = "graph")]
        graph_version: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Show full detail for one thread's latest checkpoint.
    Inspect {
        thread_id: String,
        /// A SQLite file, or a `postgres://` or `redis://` URL (a Redis key
        /// prefix other than `strata` goes in `?prefix=<name>`).
        #[arg(long)]
        storage: String,
        /// The directory of a filesystem blob store that holds the offloaded
        /// values of the threads read (design/41-Checkpoint-Efficiency.md,
        /// "Offload"); without it, they are read from the store's own database.
        #[arg(long, value_name = "DIR")]
        blobs: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
pub enum CheckpointCommand {
    /// Print the historical timeline of checkpoints for a thread.
    Log {
        thread_id: String,
        /// A SQLite file, or a `postgres://` or `redis://` URL (a Redis key
        /// prefix other than `strata` goes in `?prefix=<name>`).
        #[arg(long)]
        storage: String,
        /// The directory of a filesystem blob store that holds the offloaded
        /// values of the threads read (design/41-Checkpoint-Efficiency.md,
        /// "Offload"); without it, they are read from the store's own database.
        #[arg(long, value_name = "DIR")]
        blobs: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Compare state between two checkpoint sequence IDs.
    Diff {
        thread_id: String,
        #[arg(long = "seq-a")]
        seq_a: i64,
        #[arg(long = "seq-b")]
        seq_b: i64,
        /// A SQLite file, or a `postgres://` or `redis://` URL (a Redis key
        /// prefix other than `strata` goes in `?prefix=<name>`).
        #[arg(long)]
        storage: String,
        /// The directory of a filesystem blob store that holds the offloaded
        /// values of the threads read (design/41-Checkpoint-Efficiency.md,
        /// "Offload"); without it, they are read from the store's own database.
        #[arg(long, value_name = "DIR")]
        blobs: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Load historical state from a checkpoint for local replay.
    Replay {
        thread_id: String,
        #[arg(long)]
        sequence: i64,
        /// A SQLite file, or a `postgres://` or `redis://` URL (a Redis key
        /// prefix other than `strata` goes in `?prefix=<name>`).
        #[arg(long)]
        storage: String,
        /// The directory of a filesystem blob store that holds the offloaded
        /// values of the threads read (design/41-Checkpoint-Efficiency.md,
        /// "Offload"); without it, they are read from the store's own database.
        #[arg(long, value_name = "DIR")]
        blobs: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

pub async fn run() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Command::Graph { command } => match command {
            GraphCommand::View { path, json } => commands::graph::view(&path, json),
            GraphCommand::Validate { path, json } => commands::graph::validate(&path, json),
        },
        Command::Thread { command } => match command {
            ThreadCommand::List {
                storage,
                status,
                due,
                due_by,
                meta,
                graph,
                graph_version,
                json,
            } => {
                let due_by = due_by.or_else(|| due.then(chrono::Utc::now));
                commands::thread::list(
                    &storage,
                    status.as_deref(),
                    &meta,
                    due_by,
                    graph.as_deref(),
                    graph_version.as_deref(),
                    json,
                )
                .await
            }
            ThreadCommand::Inspect {
                thread_id,
                storage,
                blobs,
                json,
            } => commands::thread::inspect(&thread_id, &storage, blobs.as_deref(), json).await,
        },
        Command::Checkpoint { command } => match command {
            CheckpointCommand::Log {
                thread_id,
                storage,
                blobs,
                json,
            } => commands::checkpoint::log(&thread_id, &storage, blobs.as_deref(), json).await,
            CheckpointCommand::Diff {
                thread_id,
                seq_a,
                seq_b,
                storage,
                blobs,
                json,
            } => {
                commands::checkpoint::diff(
                    &thread_id,
                    seq_a,
                    seq_b,
                    &storage,
                    blobs.as_deref(),
                    json,
                )
                .await
            }
            CheckpointCommand::Replay {
                thread_id,
                sequence,
                storage,
                blobs,
                json,
            } => {
                commands::checkpoint::replay(&thread_id, sequence, &storage, blobs.as_deref(), json)
                    .await
            }
        },
    }
}

/// Parses `thread list --meta key=value`.
fn parse_meta(pair: &str) -> Result<(String, String), String> {
    match pair.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key.to_string(), value.to_string())),
        _ => Err(format!("expected KEY=VALUE, got '{pair}'")),
    }
}

/// An RFC 3339 time for `--due-by`, e.g. `2026-10-06T12:00:00Z`.
fn parse_time(text: &str) -> Result<chrono::DateTime<chrono::Utc>, String> {
    datalium_strata::store::parse_timestamp(text).map_err(|err| err.0)
}
