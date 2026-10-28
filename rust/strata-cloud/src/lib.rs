//! The Strata Cloud checkpoint store, decisions client and decision worker
//! (`design/30-Strata-Cloud-Store.md`, SG-18).
//!
//! - [`StrataCloudCheckpointStore`] is a drop-in
//!   [`CheckpointStore`](datalium_strata::store::CheckpointStore) over Strata
//!   Cloud's SDK wire contract v1:
//!   saves, loads, retries as the contract says, and the conditional save
//!   when Cloud reports sequences.
//! - [`Decisions`] reads the feed of decisions reviewers made in the Strata
//!   Cloud console, each an engine
//!   [`HumanSignal`](datalium_strata::checkpoint::HumanSignal).
//! - [`DecisionWorker`] resumes the decided threads, one at a time.
//!
//! HTTP and TLS live in this crate (`reqwest` with `rustls`), so they reach
//! only the hosts that use Strata Cloud. The .NET adapter is
//! `Datalium.Strata.Cloud`, under strict parity.
//!
//! Telemetry: a `strata.cloud.request` span per HTTP attempt (target
//! `datalium_strata_cloud`), a `strata.cloud.retries` counter and a
//! `strata.cloud.request.duration` histogram through the `metrics` facade. No
//! key, state, metadata value or override value is ever recorded.

mod decisions;
mod equivalence;
mod error;
mod options;
mod store;
mod transport;
pub mod wire;
mod worker;

pub use decisions::{CloudDecision, DecisionPage, DecisionQuery, Decisions};
pub use equivalence::equivalent;
pub use error::{CloudError, is_entitlement_exceeded, is_unauthorized, is_unavailable};
pub use options::{RetryPolicy, Sleep, StrataCloudOptions, tokio_sleep};
pub use store::{SaveOutcome, StrataCloudCheckpointStore};
pub use worker::{DecisionOutcome, DecisionWorker, HandledDecision};
