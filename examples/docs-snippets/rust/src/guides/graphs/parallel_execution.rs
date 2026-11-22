//! Samples for <https://stratamesh.dev/guides/graphs/parallel-execution/>.

use std::sync::Arc;
use std::time::Duration;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult};
use datalium_strata::context::NodeContext;
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
// docs:begin guides/graphs/parallel-execution/branches
use datalium_strata::parallel::ParallelBranches;
// docs:hide ...
use datalium_strata::parallel::{BranchOutcomes, BranchStatus, Join, ParallelGroupOptions};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
// docs:show

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Trip {
    destination: String,
    flight: Option<String>,
    hotel: Option<String>,
    forecast: Option<String>,
    gaps: Vec<String>,
    summary: Option<String>,
}

// docs:hide
fn trip(destination: &str) -> Trip {
    Trip {
        destination: destination.to_string(),
        ..Trip::default()
    }
}

#[allow(clippy::let_and_return)]
fn lookups() -> ParallelBranches<Trip> {
    // docs:show
    let lookups = ParallelBranches::new()
        .async_branch("flights", |mut trip: Trip, _ctx: NodeContext| async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            trip.flight = Some("LH 401, $612".into());
            Ok(trip)
        })
        .async_branch("hotels", |mut trip: Trip, _ctx: NodeContext| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            trip.hotel = Some("Hotel Aurora, $148/night".into());
            Ok(trip)
        })
        .async_branch("weather", |mut trip: Trip, _ctx: NodeContext| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            trip.forecast = Some("sunny, 24C".into());
            Ok(trip)
        });
    // docs:end guides/graphs/parallel-execution/branches
    lookups
}

fn build_graph(lookups: ParallelBranches<Trip>) -> Result<CompiledGraph<Trip>, StrataError> {
    // docs:begin guides/graphs/parallel-execution/group
    let graph = StateGraph::<Trip>::new()
        .add_node("ParseRequest", |mut trip: Trip| {
            trip.destination = trip.destination.trim().to_string();
            trip
        })
        .add_parallel_group("GatherInfo", lookups, merge)
        .add_node("BuildItinerary", |mut trip: Trip| {
            trip.summary = Some(format!(
                "{}: {} | {} | {}",
                trip.destination,
                trip.flight.as_deref().unwrap_or("-"),
                trip.hotel.as_deref().unwrap_or("-"),
                trip.forecast.as_deref().unwrap_or("-"),
            ));
            trip
        })
        .add_edge("ParseRequest", "GatherInfo")
        .add_edge("GatherInfo", "BuildItinerary")
        .compile()?;

    // docs:hide
    Ok(graph)
}

// docs:show
// Each branch fills in one field. Keep the first value found.
fn merge(entry: &Trip, outputs: Vec<Trip>) -> Trip {
    let mut merged = entry.clone();
    for output in outputs {
        merged.flight = merged.flight.or(output.flight);
        merged.hotel = merged.hotel.or(output.hotel);
        merged.forecast = merged.forecast.or(output.forecast);
    }
    merged
}
// docs:end guides/graphs/parallel-execution/group

// The paused clock runs the waits at once, and measures them exactly.
#[tokio::test(start_paused = true)]
async fn run_the_graph() -> Result<(), StrataError> {
    let graph = build_graph(lookups())?;

    // docs:begin guides/graphs/parallel-execution/run
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let clock = Instant::now();
    if let GraphExecutionResult::Completed(trip) =
        graph.run("trip-1", trip("  Lisbon "), store.clone()).await
    {
        // docs:hide
        assert_eq!(
            trip.summary.as_deref(),
            Some("Lisbon: LH 401, $612 | Hotel Aurora, $148/night | sunny, 24C")
        );
        // The branches overlap: the group takes as long as the slowest one, not the 600 ms of all three.
        assert!(clock.elapsed() < Duration::from_millis(600));
        // docs:show
        println!(
            "{} ({} ms)",
            trip.summary.unwrap_or_default(),
            clock.elapsed().as_millis()
        );
        // docs:hide
        return Ok(());
        // docs:show
    }
    // docs:end guides/graphs/parallel-execution/run
    panic!("the run didn't complete");
}

#[tokio::test(start_paused = true)]
async fn handle_a_failing_branch() -> Result<(), StrataError> {
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let no_rooms = ParallelBranches::new()
        .async_branch("flights", |mut trip: Trip, _ctx: NodeContext| async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            trip.flight = Some("LH 401, $612".into());
            Ok(trip)
        })
        // docs:begin guides/graphs/parallel-execution/failing-branch
        .async_branch("hotels", |_trip: Trip, _ctx: NodeContext| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Err(NodeError::Fatal("no rooms left on these dates".into()))
        })
        // docs:hide ...
        .async_branch("weather", |mut trip: Trip, _ctx: NodeContext| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            trip.forecast = Some("sunny, 24C".into());
            Ok(trip)
        });
    // docs:show

    let failing = StateGraph::<Trip>::new()
        .add_parallel_group("GatherInfo", no_rooms, merge)
        .set_recovery_policy("GatherInfo", RecoveryPolicy::retry(2))
        .compile()?;

    if let GraphExecutionResult::Failed(StrataError::NodeFailed {
        node,
        attempts,
        message,
    }) = failing.run("trip-2", trip("Madeira"), store.clone()).await
    {
        // docs:hide
        // The error is fatal, so the policy doesn't retry.
        assert_eq!(node, "GatherInfo");
        assert_eq!(attempts, 1);
        assert_eq!(message, "branch 'hotels': no rooms left on these dates");
        // docs:show
        println!("{node} failed after {attempts} attempt(s): {message}");
        // docs:hide
        return Ok(());
        // docs:show
    }
    // docs:end guides/graphs/parallel-execution/failing-branch
    panic!("the group didn't fail");
}

#[tokio::test(start_paused = true)]
async fn keep_partial_results() -> Result<(), StrataError> {
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let slow = ParallelBranches::new()
        .async_branch("flights", |mut trip: Trip, _ctx: NodeContext| async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            trip.flight = Some("LH 401, $612".into());
            Ok(trip)
        })
        .async_branch("hotels", |_trip: Trip, _ctx: NodeContext| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Err(NodeError::Fatal("no rooms left on these dates".into()))
        })
        // docs:begin guides/graphs/parallel-execution/partial
        .async_branch("weather", |trip: Trip, _ctx: NodeContext| async move {
            std::future::pending::<()>().await; // never answers
            Ok(trip)
        })
        // docs:hide ...
        .set_metadata("weather", [("service", "forecast")]);
    // docs:show

    let options = ParallelGroupOptions::new()
        .with_max_concurrency(2)
        .with_branch_timeout(Duration::from_secs(1))
        .keep_succeeded();

    let partial = StateGraph::<Trip>::new()
        .add_parallel_group_with_outcomes(
            "GatherInfo",
            slow,
            |entry: &Trip, results: BranchOutcomes<Trip>| {
                let mut merged = merge(entry, results.succeeded().cloned().collect());
                for r in results.outcomes() {
                    if r.status != BranchStatus::Succeeded {
                        let error = r.error.as_ref().map(NodeError::message).unwrap_or_default();
                        merged
                            .gaps
                            .push(format!("{}: {:?} ({error})", r.name, r.status));
                    }
                }
                merged
            },
            options,
        )
        .compile()?;
    // docs:end guides/graphs/parallel-execution/partial

    match partial.run("trip-3", trip("Madeira"), store).await {
        GraphExecutionResult::Completed(trip) => {
            assert_eq!(trip.flight.as_deref(), Some("LH 401, $612"));
            assert_eq!(
                trip.gaps,
                [
                    "hotels: Failed (no rooms left on these dates)",
                    "weather: TimedOut (branch 'weather' timed out after 1000 ms)",
                ]
            );
        }
        _ => panic!("expected the group to keep what succeeded"),
    }
    Ok(())
}

fn forecasts() -> ParallelBranches<Trip> {
    ParallelBranches::new()
        .async_branch("primary", |mut trip: Trip, _ctx: NodeContext| async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            trip.forecast = Some("sunny, 24C".into());
            Ok(trip)
        })
        .async_branch("backup", |mut trip: Trip, _ctx: NodeContext| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            trip.forecast = Some("clear, 23C".into());
            Ok(trip)
        })
}

#[tokio::test(start_paused = true)]
async fn race_branches_with_a_join() -> Result<(), StrataError> {
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());

    // docs:begin guides/graphs/parallel-execution/race
    let race = StateGraph::<Trip>::new()
        .add_parallel_group_with_outcomes(
            "Forecast",
            forecasts(), // "primary" answers after 300 ms, "backup" after 100 ms
            |entry: &Trip, results: BranchOutcomes<Trip>| {
                let statuses: Vec<String> = results
                    .outcomes()
                    .iter()
                    .map(|r| format!("{} {:?}", r.name, r.status))
                    .collect();
                println!(
                    "Won: {}; {}",
                    results.deciding().join(", "),
                    statuses.join(", ")
                );
                let winner = &results.deciding()[0];
                results
                    .get(winner)
                    .and_then(|r| r.state.clone())
                    .unwrap_or_else(|| entry.clone())
            },
            ParallelGroupOptions::new().with_join(Join::first_success()),
        )
        .compile()?;

    // The other joins:
    let first_sunny = ParallelGroupOptions::new().with_join(
        Join::first_matching(|t: &Trip| {
            t.forecast
                .as_deref()
                .is_some_and(|f| f.starts_with("sunny"))
        })
        .described("a sunny forecast"),
    );
    let two_answers = ParallelGroupOptions::new().with_join(Join::quorum(2));
    // docs:end guides/graphs/parallel-execution/race

    // The first answer wins, and the other branch is cancelled.
    match race.run("trip-4", trip("Lisbon"), store.clone()).await {
        GraphExecutionResult::Completed(trip) => {
            assert_eq!(trip.forecast.as_deref(), Some("clear, 23C"));
        }
        _ => panic!("expected the race to complete"),
    }

    // The first sunny forecast wins, though it comes second.
    let sunny = StateGraph::<Trip>::new()
        .add_parallel_group_with("Forecast", forecasts(), merge, first_sunny)
        .compile()?;
    match sunny.run("trip-5", trip("Lisbon"), store.clone()).await {
        GraphExecutionResult::Completed(trip) => {
            assert_eq!(trip.forecast.as_deref(), Some("sunny, 24C"));
        }
        _ => panic!("expected the first sunny forecast"),
    }

    // A quorum of 2 waits for both.
    let quorum = StateGraph::<Trip>::new()
        .add_parallel_group_with_outcomes(
            "Forecast",
            forecasts(),
            |entry: &Trip, results: BranchOutcomes<Trip>| {
                assert_eq!(results.deciding(), ["backup", "primary"]);
                merge(entry, results.succeeded().cloned().collect())
            },
            two_answers,
        )
        .compile()?;
    assert!(matches!(
        quorum.run("trip-6", trip("Lisbon"), store).await,
        GraphExecutionResult::Completed(_)
    ));
    Ok(())
}
