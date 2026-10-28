//! Which provider answers are worth retrying, and the provider's retry hint
//! (`design/44-LLM-Integration.md`, "Faults"). Mirrors .NET's `ModelFaults` and
//! `RetryAfter`.

use std::time::Duration;

use chrono::{DateTime, Utc};

/// Whether an HTTP status is worth retrying: 408, 409, 429 and every 5xx
/// (Anthropic's 529, overloaded, included). Every other status is the
/// request's fault, and a retry would get the same answer.
pub fn is_transient_status(status: u16) -> bool {
    matches!(status, 408 | 409 | 429 | 500..=599)
}

/// The wait a provider's headers ask for: `retry-after-ms` (OpenAI's), else
/// `Retry-After` as seconds or an HTTP date. `None` when they ask for none or
/// can't be read; a date in the past is no wait at all.
pub fn parse_retry_after(
    retry_after_ms: Option<&str>,
    retry_after: Option<&str>,
    now: DateTime<Utc>,
) -> Option<Duration> {
    if let Some(ms) = retry_after_ms.and_then(|ms| ms.trim().parse::<f64>().ok())
        && ms >= 0.0
        && ms.is_finite()
    {
        return Some(Duration::from_secs_f64(ms / 1000.0));
    }
    let retry_after = retry_after?.trim();
    if retry_after.is_empty() {
        return None;
    }
    if let Ok(seconds) = retry_after.parse::<f64>() {
        return (seconds >= 0.0 && seconds.is_finite()).then(|| Duration::from_secs_f64(seconds));
    }
    let at = DateTime::parse_from_rfc2822(retry_after).ok()?;
    Some(
        (at.with_timezone(&Utc) - now)
            .to_std()
            .unwrap_or(Duration::ZERO),
    )
}

/// The hint on an HTTP response's headers.
pub(crate) fn retry_after_of(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let header = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    parse_retry_after(header("retry-after-ms"), header("retry-after"), Utc::now())
}
