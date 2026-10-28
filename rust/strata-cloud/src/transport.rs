//! The HTTP plumbing the store and the decisions client share
//! (`design/30-Strata-Cloud-Store.md`, "The wire"): authentication, gzip,
//! attempt timeouts, retries with backoff and `Retry-After`, the error
//! mapping, and a span per attempt. Mirrors the .NET adapter's `CloudTransport`.

use std::borrow::Cow;
use std::io::Write;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use reqwest::header::{self, HeaderValue};
use reqwest::{Method, Url};
use tracing::Instrument;
use tracing::field::Empty;

use crate::error::CloudError;
use crate::options::StrataCloudOptions;
use crate::wire;

const USER_AGENT: &str = concat!("datalium-strata-cloud/", env!("CARGO_PKG_VERSION"));

/// One request, as the transport sends it on every attempt.
pub(crate) struct Request<'a> {
    pub route: &'static str,
    pub method: Method,
    pub path: &'static str,
    pub query: Vec<(Cow<'static, str>, String)>,
    pub body: Option<Vec<u8>>,
    pub if_sequence: Option<i64>,
    /// A save under a lease: sent as `Strata-Lease-Token`
    /// (`design/43-Cloud-Leases-And-Listing.md`).
    pub lease_token: Option<i64>,
    /// Sent as `Idempotency-Key`, the same on every attempt of the call.
    pub idempotency_key: Option<String>,
    pub thread_id: Option<&'a str>,
    /// A decisions poll: its span is recorded afterwards, and only when it
    /// brought work or failed.
    pub defer_span: bool,
}

impl<'a> Request<'a> {
    /// A request with no query, body or headers of its own, for one thread
    /// when `thread_id` is set (the span's `strata.thread.id`).
    pub fn new(
        route: &'static str,
        method: Method,
        path: &'static str,
        thread_id: Option<&'a str>,
    ) -> Self {
        Self {
            route,
            method,
            path,
            query: Vec::new(),
            body: None,
            if_sequence: None,
            lease_token: None,
            idempotency_key: None,
            thread_id,
            defer_span: false,
        }
    }

    /// Adds `key=value` to the query string.
    pub fn with_query(
        mut self,
        key: impl Into<Cow<'static, str>>,
        value: impl Into<String>,
    ) -> Self {
        self.query.push((key.into(), value.into()));
        self
    }

    /// A JSON body.
    pub fn with_json(mut self, body: &serde_json::Value) -> Self {
        self.body = Some(body.to_string().into_bytes());
        self
    }
}

/// The final answer to a [`Request`]: its status, the headers the adapter
/// reads, the body, whether an earlier attempt failed in a way that may have
/// committed it, and when the last attempt began.
pub(crate) struct Response {
    pub status: u16,
    pub retry_after: Option<Duration>,
    pub sequence: Option<i64>,
    pub body: Vec<u8>,
    pub ambiguous: bool,
    pub attempts: u32,
    pub started: Instant,
}

/// What one attempt came to.
struct Attempt {
    status: Option<u16>,
    retry_after: Option<Duration>,
    sequence: Option<i64>,
    body: Vec<u8>,
    error_type: Option<&'static str>,
    cause: Option<String>,
}

pub(crate) struct Transport {
    client: reqwest::Client,
    base: Url,
    pub options: StrataCloudOptions,
}

impl Transport {
    pub fn new(options: StrataCloudOptions) -> Result<Self, CloudError> {
        options.validate().map_err(CloudError::Config)?;
        let mut root = options.base_url.clone();
        if !root.ends_with('/') {
            root.push('/');
        }
        let base = Url::parse(&root).map_err(|err| CloudError::Config(err.to_string()))?;
        let client = match &options.client {
            Some(client) => client.clone(),
            None => reqwest::Client::builder()
                .connect_timeout(options.connect_timeout)
                .gzip(true)
                .user_agent(USER_AGENT)
                .build()
                .map_err(|err| CloudError::Config(format!("can't build the HTTP client: {err}")))?,
        };
        Ok(Self {
            client,
            base,
            options,
        })
    }

    /// Sends `request` up to `max_attempts` times while the answer is a
    /// network error, a timeout or a status `retryable` accepts, waiting
    /// between attempts as the retry policy says. Returns the first answer
    /// that isn't retried (success or not); [`CloudError::Unavailable`] when
    /// the attempts run out.
    pub async fn send(
        &self,
        request: &Request<'_>,
        retryable: fn(u16) -> bool,
        max_attempts: u32,
        timeout: Duration,
    ) -> Result<Response, CloudError> {
        let policy = self.options.retry;
        let body = request.body.as_ref().map(|body| {
            if body.len() > self.options.gzip_threshold {
                (gzip(body), true)
            } else {
                (body.clone(), false)
            }
        });
        let mut ambiguous = false;
        let mut attempt_number = 1;
        loop {
            let started = Instant::now();
            let span = if request.defer_span {
                tracing::Span::none()
            } else {
                request_span(request, attempt_number)
            };
            let attempt = self
                .attempt(request, body.as_ref(), timeout)
                .instrument(span.clone())
                .await;
            record(
                request,
                &span,
                attempt.status,
                attempt.error_type,
                started.elapsed(),
            );

            if let Some(status) = attempt.status.filter(|status| !retryable(*status)) {
                return Ok(Response {
                    status,
                    retry_after: attempt.retry_after,
                    sequence: attempt.sequence,
                    body: attempt.body,
                    ambiguous,
                    attempts: attempt_number,
                    started,
                });
            }

            // A network error, a timeout or a gateway's 5xx may have reached
            // Cloud and committed; a 429 or a 503 never did (contract §6, §7).
            ambiguous |= matches!(attempt.status, None | Some(408 | 500 | 502 | 504));
            let reason = match attempt.status {
                Some(status) => format!("status_{status}"),
                None => attempt.error_type.unwrap_or("network").to_string(),
            };
            let too_long = attempt
                .retry_after
                .is_some_and(|after| after > policy.max_retry_after);
            if attempt_number >= max_attempts || too_long {
                return Err(CloudError::Unavailable {
                    status: attempt.status,
                    code: read_code(&attempt.body),
                    retry_after: attempt.retry_after,
                    attempts: attempt_number,
                    cause: attempt.cause,
                });
            }

            metrics::counter!("strata.cloud.retries", "route" => request.route, "reason" => reason)
                .increment(1);
            let delay = policy.delay_before(attempt_number, attempt.retry_after);
            (self.options.sleep)(delay).await;
            attempt_number += 1;
        }
    }

    async fn attempt(
        &self,
        request: &Request<'_>,
        body: Option<&(Vec<u8>, bool)>,
        timeout: Duration,
    ) -> Attempt {
        let mut url = self
            .base
            .join(request.path)
            .expect("a route joins the base");
        if !request.query.is_empty() {
            let query = request
                .query
                .iter()
                .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
                .collect::<Vec<_>>()
                .join("&");
            url.set_query(Some(&query));
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {}", self.options.api_key))
            .unwrap_or_else(|_| HeaderValue::from_static("Bearer invalid"));
        authorization.set_sensitive(true);
        let mut builder = self
            .client
            .request(request.method.clone(), url)
            .timeout(timeout)
            .header(header::AUTHORIZATION, authorization)
            .header(header::ACCEPT, "application/json")
            .header(header::USER_AGENT, USER_AGENT);
        if let Some(expected) = request.if_sequence {
            builder = builder.header(wire::IF_SEQUENCE_HEADER, expected.to_string());
        }
        if let Some(token) = request.lease_token {
            builder = builder.header(wire::LEASE_TOKEN_HEADER, token.to_string());
        }
        if let Some(key) = &request.idempotency_key {
            builder = builder.header(wire::IDEMPOTENCY_KEY_HEADER, key);
        }
        if let Some((bytes, gzipped)) = body {
            builder = builder
                .header(header::CONTENT_TYPE, "application/json")
                .body(bytes.clone());
            if *gzipped {
                builder = builder.header(header::CONTENT_ENCODING, "gzip");
            }
        }

        let failed = |err: reqwest::Error| {
            let timed_out = err.is_timeout();
            Attempt {
                status: None,
                retry_after: None,
                sequence: None,
                body: Vec::new(),
                error_type: Some(if timed_out { "timeout" } else { "network" }),
                cause: Some(if timed_out {
                    format!("timed out after {} s", timeout.as_secs_f64())
                } else {
                    format!("network error: {}", without_url(err))
                }),
            }
        };
        let response = match builder.send().await {
            Ok(response) => response,
            Err(err) => return failed(err),
        };
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        match response.bytes().await {
            Ok(bytes) => Attempt {
                status: Some(status),
                retry_after: retry_after(&headers),
                sequence: headers
                    .get(wire::SEQUENCE_HEADER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.trim().parse::<i64>().ok())
                    .filter(|sequence| *sequence >= 0),
                body: bytes.to_vec(),
                error_type: None,
                cause: None,
            },
            Err(err) => failed(err),
        }
    }
}

/// The error for a final answer that isn't a success the caller handles.
pub(crate) fn failure(response: &Response) -> CloudError {
    let code = read_code(&response.body);
    let attempts = response.attempts;
    match response.status {
        401 => CloudError::Unauthorized { code, attempts },
        403 if code.as_deref() == Some(wire::ENTITLEMENT_EXCEEDED_CODE) => {
            CloudError::EntitlementExceeded { code, attempts }
        }
        status @ (408 | 429 | 500..) => CloudError::Unavailable {
            status: Some(status),
            code,
            retry_after: response.retry_after,
            attempts,
            cause: None,
        },
        status => CloudError::Rejected {
            status,
            code,
            attempts,
        },
    }
}

/// A success whose body the adapter can't read.
pub(crate) fn invalid(response: &Response, message: impl Into<String>) -> CloudError {
    CloudError::InvalidResponse {
        status: response.status,
        message: message.into(),
    }
}

/// Records the span of a deferred request (a decisions poll), with how long
/// it took, since a `tracing` span can't be backdated.
pub(crate) fn record_deferred(
    request: &Request<'_>,
    started: Instant,
    status: Option<u16>,
    error_type: Option<&str>,
    items: Option<usize>,
) {
    let span = request_span(request, 1);
    span.record(
        "strata.cloud.duration_ms",
        started.elapsed().as_millis() as u64,
    );
    if let Some(items) = items {
        span.record("strata.cloud.items", items as u64);
    }
    tag(&span, status, error_type);
}

fn request_span(request: &Request<'_>, attempt: u32) -> tracing::Span {
    let span = tracing::info_span!(
        target: "datalium_strata_cloud",
        "strata.cloud.request",
        strata.cloud.route = request.route,
        http.request.method = %request.method,
        http.response.status_code = Empty,
        strata.cloud.attempt = attempt,
        strata.thread.id = Empty,
        strata.cloud.items = Empty,
        strata.cloud.duration_ms = Empty,
        error.type = Empty,
        otel.status_code = Empty,
    );
    if let Some(thread_id) = request.thread_id {
        span.record("strata.thread.id", thread_id);
    }
    span
}

fn record(
    request: &Request<'_>,
    span: &tracing::Span,
    status: Option<u16>,
    error_type: Option<&'static str>,
    elapsed: Duration,
) {
    tag(span, status, error_type);
    let status = match (status, error_type) {
        (Some(status), _) => status.to_string(),
        (None, Some(error_type)) => error_type.to_string(),
        (None, None) => "network".to_string(),
    };
    metrics::histogram!("strata.cloud.request.duration", "route" => request.route, "status" => status)
        .record(elapsed.as_secs_f64());
}

fn tag(span: &tracing::Span, status: Option<u16>, error_type: Option<&str>) {
    match status {
        Some(status) => {
            span.record("http.response.status_code", status as u64);
            if status >= 400 {
                span.record("error.type", status.to_string());
                span.record("otel.status_code", "ERROR");
            }
        }
        None => {
            if let Some(error_type) = error_type {
                span.record("error.type", error_type);
                span.record("otel.status_code", "ERROR");
            }
        }
    }
}

/// The error envelope's `code`, if the body is one. Nothing else is read
/// from it.
pub(crate) fn read_code(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()?
        .get("code")?
        .as_str()
        .map(str::to_string)
}

fn retry_after(headers: &header::HeaderMap) -> Option<Duration> {
    let value = headers.get(header::RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = DateTime::parse_from_rfc2822(value).ok()?;
    Some(
        (date.with_timezone(&Utc) - Utc::now())
            .to_std()
            .unwrap_or_default(),
    )
}

fn gzip(body: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(body).expect("gzip into memory");
    encoder.finish().expect("gzip into memory")
}

/// Percent-encodes a query component: everything but the unreserved
/// characters, so a space is `%20`, as .NET's `Uri.EscapeDataString` writes it.
fn encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

/// A transport error's message and causes, without its URL (the URL is in
/// the span).
fn without_url(err: reqwest::Error) -> String {
    let err = err.without_url();
    let mut message = err.to_string();
    let mut source = std::error::Error::source(&err);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}
