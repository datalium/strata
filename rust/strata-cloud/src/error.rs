//! What went wrong talking to Strata Cloud (`design/30-Strata-Cloud-Store.md`,
//! "Status mapping").

use std::fmt;
use std::time::Duration;

use datalium_strata::StrataError;

use crate::wire;

/// A request to Strata Cloud failed. Carries the HTTP status and the error
/// envelope's `code`, never anything else from the body, and never the key.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloudError {
    /// `401`: no key, a malformed, revoked or expired one. Fix the
    /// credential; never retried.
    Unauthorized { code: Option<String>, attempts: u32 },
    /// `403 STRATA_CLOUD_API_ENTITLEMENT_EXCEEDED`: the save would start a new
    /// thread and the plan's limit of threads this month is reached. Refuse
    /// new executions; running ones carry on. Never retried.
    EntitlementExceeded { code: Option<String>, attempts: u32 },
    /// Any other `4xx` (`400`, `413`, `422`, …): the request will never
    /// succeed. Never retried.
    Rejected {
        status: u16,
        code: Option<String>,
        attempts: u32,
    },
    /// Not now: a network error, a timeout, `429` or a `5xx`, after the
    /// call's retries (the decisions client makes one attempt). Asking again
    /// later may succeed.
    Unavailable {
        /// The last response's status; `None` for a network error or a
        /// timeout.
        status: Option<u16>,
        code: Option<String>,
        /// The last response's `Retry-After`, when it had one.
        retry_after: Option<Duration>,
        attempts: u32,
        /// What the last attempt ran into, when it got no response.
        cause: Option<String>,
    },
    /// A success whose body isn't what the wire contract says.
    InvalidResponse { status: u16, message: String },
    /// The options can't be used (a base URL that isn't one, no key).
    Config(String),
}

impl CloudError {
    /// Whether asking again later may succeed ([`CloudError::Unavailable`]).
    pub fn is_transient(&self) -> bool {
        matches!(self, CloudError::Unavailable { .. })
    }

    /// The HTTP status of the answer that failed the call, if there was one.
    pub fn status(&self) -> Option<u16> {
        match self {
            CloudError::Unauthorized { .. } => Some(401),
            CloudError::EntitlementExceeded { .. } => Some(403),
            CloudError::Rejected { status, .. } | CloudError::InvalidResponse { status, .. } => {
                Some(*status)
            }
            CloudError::Unavailable { status, .. } => *status,
            CloudError::Config(_) => None,
        }
    }

    /// The error envelope's `code`, e.g. `STRATA_CLOUD_API_RATE_LIMITED`.
    pub fn code(&self) -> Option<&str> {
        match self {
            CloudError::Unauthorized { code, .. }
            | CloudError::EntitlementExceeded { code, .. }
            | CloudError::Rejected { code, .. }
            | CloudError::Unavailable { code, .. } => code.as_deref(),
            _ => None,
        }
    }

    /// The last response's `Retry-After`, when it had one.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            CloudError::Unavailable { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    /// How many attempts the call made.
    pub fn attempts(&self) -> u32 {
        match self {
            CloudError::Unauthorized { attempts, .. }
            | CloudError::EntitlementExceeded { attempts, .. }
            | CloudError::Rejected { attempts, .. }
            | CloudError::Unavailable { attempts, .. } => *attempts,
            CloudError::InvalidResponse { .. } | CloudError::Config(_) => 1,
        }
    }
}

fn detail(status: u16, code: &Option<String>) -> String {
    match code {
        Some(code) => format!("(HTTP {status} {code})"),
        None => format!("(HTTP {status})"),
    }
}

impl fmt::Display for CloudError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CloudError::Unauthorized { code, .. } => write!(
                f,
                "strata cloud: unauthorized {}: check the API key",
                detail(401, code)
            ),
            CloudError::EntitlementExceeded { code, .. } => write!(
                f,
                "strata cloud: refused {}: the plan's limit of new threads this month is reached",
                detail(403, code)
            ),
            CloudError::Rejected { status, code, .. } => {
                write!(f, "strata cloud: refused {}", detail(*status, code))
            }
            CloudError::Unavailable {
                status,
                code,
                attempts,
                cause,
                ..
            } => {
                let times = if *attempts == 1 {
                    "1 attempt".to_string()
                } else {
                    format!("{attempts} attempts")
                };
                write!(f, "strata cloud: unavailable after {times}")?;
                if let Some(status) = status {
                    write!(f, " {}", detail(*status, code))?;
                }
                if let Some(cause) = cause {
                    write!(f, ": {cause}")?;
                }
                Ok(())
            }
            CloudError::InvalidResponse { status, message } => write!(
                f,
                "strata cloud: a response the wire contract doesn't allow (HTTP {status}): {message}"
            ),
            CloudError::Config(message) => write!(f, "strata cloud: {message}"),
        }
    }
}

impl std::error::Error for CloudError {}

/// Whether a run's failure is a Cloud save refused for the plan's thread
/// limit (`403 STRATA_CLOUD_API_ENTITLEMENT_EXCEEDED`). The store port's
/// error is a string, so a store error reaches a run's result as
/// `Failed(ExecutionAborted(message))`; the message carries the code, and
/// this matches it. A host refuses new executions on it and lets running
/// ones carry on.
pub fn is_entitlement_exceeded(error: &StrataError) -> bool {
    matches!(error, StrataError::ExecutionAborted(message)
        if message.starts_with("strata cloud: refused (HTTP 403 ")
            && message.contains(wire::ENTITLEMENT_EXCEEDED_CODE))
}

/// Whether a run's failure is a Cloud store call that may succeed later
/// ([`CloudError::Unavailable`] after its retries), through the engine's
/// string store error.
pub fn is_unavailable(error: &StrataError) -> bool {
    matches!(error, StrataError::ExecutionAborted(message)
        if message.starts_with("strata cloud: unavailable after "))
}

/// Whether a run's failure is a Cloud store call refused for its key (`401`).
pub fn is_unauthorized(error: &StrataError) -> bool {
    matches!(error, StrataError::ExecutionAborted(message)
        if message.starts_with("strata cloud: unauthorized "))
}
