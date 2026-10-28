//! The names on Strata Cloud's SDK wire (contract v1,
//! not public) that the adapter depends on, in one place. Mirrors the .NET adapter's
//! `StrataCloudWire`.

/// The response header on `GET /v1/sdk/checkpoints/latest` carrying the
/// latest checkpoint's sequence (`design/30-Strata-Cloud-Store.md`, "The
/// conditional save"). A Cloud without it leaves the store's loads without a
/// sequence, and its saves unconditional.
pub const SEQUENCE_HEADER: &str = "strata-sequence";

/// The request header on `POST /v1/sdk/checkpoints` naming the sequence the
/// thread's latest checkpoint must still have; a mismatch is `412`, with
/// nothing stored.
pub const IF_SEQUENCE_HEADER: &str = "strata-if-sequence";

/// The request header on `POST /v1/sdk/checkpoints` that makes it a save
/// under a lease (`design/43-Cloud-Leases-And-Listing.md`): Cloud appends
/// only if the token is still the thread's latest, and answers `423` with
/// [`LEASE_LOST_CODE`] otherwise, nothing stored.
pub const LEASE_TOKEN_HEADER: &str = "strata-lease-token";

/// The request header on `POST /v1/sdk/leases` that makes a retried
/// acquisition answer the lease its first attempt took: one key per call,
/// the same on each of its attempts.
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

pub const LEASE_LOST_CODE: &str = "STRATA_CLOUD_API_LEASE_LOST";

pub const ENTITLEMENT_EXCEEDED_CODE: &str = "STRATA_CLOUD_API_ENTITLEMENT_EXCEEDED";

pub const THREAD_NOT_FOUND_CODE: &str = "STRATA_CLOUD_API_THREAD_NOT_FOUND";

pub const PRECONDITION_FAILED_CODE: &str = "STRATA_CLOUD_API_PRECONDITION_FAILED";

pub(crate) const SAVE_PATH: &str = "v1/sdk/checkpoints";
pub(crate) const LOAD_PATH: &str = "v1/sdk/checkpoints/latest";
pub(crate) const DECISIONS_PATH: &str = "v1/sdk/decisions";
pub(crate) const LEASES_PATH: &str = "v1/sdk/leases";
pub(crate) const THREADS_PATH: &str = "v1/sdk/threads";
/// `GET` on the save's path reads a thread's log.
pub(crate) const LOG_PATH: &str = "v1/sdk/checkpoints";

pub(crate) const SAVE_ROUTE: &str = "checkpoints.save";
pub(crate) const LOAD_ROUTE: &str = "checkpoints.load";
pub(crate) const DECISIONS_ROUTE: &str = "decisions";
pub(crate) const ACQUIRE_ROUTE: &str = "leases.acquire";
pub(crate) const RENEW_ROUTE: &str = "leases.renew";
pub(crate) const RELEASE_ROUTE: &str = "leases.release";
pub(crate) const GET_LEASE_ROUTE: &str = "leases.get";
pub(crate) const THREADS_ROUTE: &str = "threads.list";
pub(crate) const LOG_ROUTE: &str = "checkpoints.log";
