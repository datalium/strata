//! The client for Strata Cloud's decision feed, `GET /v1/sdk/decisions`
//! (`design/30-Strata-Cloud-Store.md`, "Decisions").

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use datalium_strata::checkpoint::HumanSignal;
use reqwest::Method;
use serde::Deserialize;
use serde_json::Value;

use crate::error::CloudError;
use crate::options::StrataCloudOptions;
use crate::transport::{self, Request, Transport};
use crate::wire;

const LONG_POLL_MARGIN: Duration = Duration::from_secs(10);

/// A decision a reviewer made in the Strata Cloud console, waiting for the
/// runtime: the `signal` to resume `thread_id` with. `sequence` is the
/// interrupted checkpoint it answers, the thread's latest when Cloud offered
/// it.
///
/// `signal.reviewer_token` is the reviewer's own choice, not who decided: an
/// application that lets only some people approve checks `decided_by` or
/// `decided_by_email`, which the console verified (contract §5,
/// `design/30-Strata-Cloud-Store.md`).
#[derive(Debug, Clone)]
pub struct CloudDecision {
    pub approval_id: String,
    pub thread_id: String,
    pub sequence: i64,
    pub decided_at: DateTime<Utc>,
    pub signal: HumanSignal,
    /// Who decided, as the console verified it: the IdP user id of a person,
    /// `idp-apikey:<key id>`, `idp-system:<account id>`, or `dev-console`
    /// for the development token. `None` from a Cloud that doesn't send it.
    pub decided_by: Option<String>,
    /// The decider's email as the IdP vouched for it when they decided,
    /// exactly as sent (compare it case-insensitively). `None` for an API
    /// key, a system account, the development token, a person the IdP sent
    /// none for, and from a Cloud that doesn't send it.
    pub decided_by_email: Option<String>,
    /// The reviewer's notes, or `None` when they wrote none.
    pub notes: Option<String>,
}

/// A page of the decision feed. `next_cursor` is `None` on the last page.
#[derive(Debug, Clone)]
pub struct DecisionPage {
    pub items: Vec<CloudDecision>,
    pub next_cursor: Option<String>,
}

/// What [`Decisions::get`] asks for.
#[derive(Debug, Clone, Default)]
pub struct DecisionQuery {
    /// Only this thread (prefer it when you know the thread).
    pub thread_id: Option<String>,
    /// At most this many; `None` for Cloud's default (25).
    pub limit: Option<u32>,
    /// The previous page's `next_cursor`.
    pub cursor: Option<String>,
    /// How long Cloud may hold the request while nothing is waiting (whole
    /// seconds; Cloud holds at most 25). Zero answers at once.
    pub wait: Duration,
}

impl DecisionQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_thread_id(mut self, thread_id: impl Into<String>) -> Self {
        self.thread_id = Some(thread_id.into());
        self
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    pub fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }
}

/// The client for Strata Cloud's decision feed. A decision is offered,
/// oldest first, until something is saved to its thread: the resume's own
/// save is the acknowledgement. Most hosts run a
/// [`DecisionWorker`](crate::DecisionWorker) over it rather than calling it
/// themselves.
pub struct Decisions {
    transport: Transport,
}

impl Decisions {
    pub fn new(options: StrataCloudOptions) -> Result<Self, CloudError> {
        Ok(Self {
            transport: Transport::new(options)?,
        })
    }

    pub(crate) fn options(&self) -> &StrataCloudOptions {
        &self.transport.options
    }

    /// One request for the decisions waiting now (or within `query.wait`; an
    /// empty page is then the normal answer: ask again). The read timeout is
    /// the wait plus 10 s.
    ///
    /// Not retried: a `429`, a `5xx`, a network error or a timeout is
    /// [`CloudError::Unavailable`], with `retry_after` when Cloud said how
    /// long to wait.
    pub async fn get(&self, query: &DecisionQuery) -> Result<DecisionPage, CloudError> {
        let wait_seconds = wait_seconds(query.wait);
        let mut params = Vec::new();
        if let Some(thread_id) = &query.thread_id {
            params.push(("thread_id".into(), thread_id.clone()));
        }
        if let Some(limit) = query.limit {
            params.push(("limit".into(), limit.to_string()));
        }
        if let Some(cursor) = &query.cursor {
            params.push(("cursor".into(), cursor.clone()));
        }
        if wait_seconds > 0 {
            params.push(("wait".into(), wait_seconds.to_string()));
        }
        let request = Request {
            route: wire::DECISIONS_ROUTE,
            method: Method::GET,
            path: wire::DECISIONS_PATH,
            query: params,
            body: None,
            if_sequence: None,
            lease_token: None,
            idempotency_key: None,
            thread_id: query.thread_id.as_deref(),
            defer_span: true,
        };
        let timeout = if wait_seconds > 0 {
            Duration::from_secs(wait_seconds) + LONG_POLL_MARGIN
        } else {
            self.transport.options.request_timeout
        };
        let started = Instant::now();
        let response = match self.transport.send(&request, |_| false, 1, timeout).await {
            Ok(response) => response,
            Err(err) => {
                let error_type = if err.status().is_none() {
                    Some("network")
                } else {
                    None
                };
                transport::record_deferred(&request, started, err.status(), error_type, None);
                return Err(err);
            }
        };
        if response.status != 200 {
            transport::record_deferred(
                &request,
                response.started,
                Some(response.status),
                None,
                None,
            );
            return Err(transport::failure(&response));
        }
        let page = match parse(&response.body) {
            Ok(page) => page,
            Err(message) => {
                transport::record_deferred(
                    &request,
                    response.started,
                    Some(response.status),
                    Some("invalid_response"),
                    None,
                );
                return Err(transport::invalid(&response, message));
            }
        };
        // Idle long-polls are the normal case: only a poll that brought work
        // gets a span.
        if !page.items.is_empty() {
            transport::record_deferred(
                &request,
                response.started,
                Some(response.status),
                None,
                Some(page.items.len()),
            );
        }
        Ok(page)
    }
}

/// Whole seconds, rounded up, at most 999 (Cloud's limit for the parameter).
fn wait_seconds(wait: Duration) -> u64 {
    if wait.is_zero() {
        return 0;
    }
    let seconds = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
    seconds.min(999)
}

#[derive(Deserialize)]
struct WirePage {
    items: Vec<WireDecision>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct WireDecision {
    approval_id: String,
    thread_id: String,
    sequence: i64,
    decided_at: String,
    signal: WireSignal,
    #[serde(default)]
    decided_by: Option<String>,
    #[serde(default)]
    decided_by_email: Option<String>,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Deserialize)]
struct WireSignal {
    is_approved: bool,
    #[serde(default)]
    reviewer_token: Option<String>,
    #[serde(default)]
    field_overrides: Option<HashMap<String, String>>,
    #[serde(default)]
    overrides: Option<HashMap<String, Value>>,
}

fn parse(body: &[u8]) -> Result<DecisionPage, String> {
    let page: WirePage = serde_json::from_slice(body).map_err(|err| err.to_string())?;
    let items = page
        .items
        .into_iter()
        .map(|item| {
            let decided_at = DateTime::parse_from_rfc3339(&item.decided_at)
                .map_err(|err| format!("decided_at: {err}"))?
                .with_timezone(&Utc);
            Ok(CloudDecision {
                approval_id: item.approval_id,
                thread_id: item.thread_id,
                sequence: item.sequence,
                decided_at,
                signal: HumanSignal {
                    is_approved: item.signal.is_approved,
                    reviewer_token: item.signal.reviewer_token.unwrap_or_default(),
                    field_overrides: item.signal.field_overrides.unwrap_or_default(),
                    overrides: item.signal.overrides.unwrap_or_default(),
                },
                decided_by: item.decided_by,
                decided_by_email: item.decided_by_email,
                notes: item.notes,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(DecisionPage {
        items,
        next_cursor: page.next_cursor,
    })
}
