//! Whether a checkpoint store answers (`design/35-Hosting-Integration.md`,
//! "Health"), for whatever readiness endpoint the application has.

use std::time::Duration;

use datalium_strata::checkpoint::CheckpointStore;

/// The thread id [`check_store`] loads. Nobody's thread: the answer is
/// "none", and getting it shows the store is reachable and its tables are
/// current. The .NET health check loads the same id.
pub const PROBE_THREAD_ID: &str = "strata.health";

/// What [`check_store`] found.
#[derive(Debug, Clone, PartialEq)]
pub enum StoreHealth {
    /// The store answered, in this long.
    Healthy { latency: Duration },
    /// The store failed, or didn't answer in time. `error` is the store's own
    /// message, which can hold a connection string: don't put it on a public
    /// endpoint.
    Unhealthy { error: String },
}

impl StoreHealth {
    pub fn is_healthy(&self) -> bool {
        matches!(self, StoreHealth::Healthy { .. })
    }
}

/// Loads the latest checkpoint of [`PROBE_THREAD_ID`] from `store`. An
/// answer is healthy; a failure, or none within `timeout`, is unhealthy.
pub async fn check_store(store: &dyn CheckpointStore, timeout: Duration) -> StoreHealth {
    let started = tokio::time::Instant::now();
    match tokio::time::timeout(timeout, store.load_latest(PROBE_THREAD_ID)).await {
        Ok(Ok(_)) => StoreHealth::Healthy {
            latency: started.elapsed(),
        },
        Ok(Err(error)) => StoreHealth::Unhealthy { error: error.0 },
        Err(_) => StoreHealth::Unhealthy {
            error: format!("no answer within {timeout:?}"),
        },
    }
}
