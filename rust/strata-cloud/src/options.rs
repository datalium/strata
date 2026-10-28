//! Where Strata Cloud is and how to talk to it (`design/30-Strata-Cloud-Store.md`).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::graph::{Backoff, Jitter};

/// What the client and the worker wait with between attempts and rounds:
/// tokio's sleep by default. Tests pass one that records each wait and returns
/// at once (tokio's paused clock would also fire the attempt timeouts of a
/// request in flight to a real socket).
pub type Sleep = Arc<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>;

/// tokio's sleep, as a [`Sleep`].
pub fn tokio_sleep() -> Sleep {
    Arc::new(|duration| Box::pin(tokio::time::sleep(duration)))
}

/// Where Strata Cloud is and how to talk to it. Shared by
/// [`StrataCloudCheckpointStore`](crate::StrataCloudCheckpointStore) and
/// [`Decisions`](crate::Decisions).
///
/// Its `Debug` prints the key as `<redacted>`: keep the key in a secret
/// store, never in a graph's state or a run's metadata.
#[derive(Clone)]
pub struct StrataCloudOptions {
    pub(crate) base_url: String,
    pub(crate) api_key: String,
    pub(crate) connect_timeout: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) retry: RetryPolicy,
    pub(crate) gzip_threshold: usize,
    pub(crate) client: Option<reqwest::Client>,
    pub(crate) sleep: Sleep,
}

impl StrataCloudOptions {
    /// `base_url` is the service's address without the `/v1`
    /// (`https://api.strata.datalium.com`, or `http://localhost:9820` for the
    /// local stack); routes are `/v1/sdk/…` under its path. `api_key` is the
    /// `strata_live_…` key, sent as `Authorization: Bearer`.
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(30),
            retry: RetryPolicy::default(),
            gzip_threshold: 64 * 1024,
            client: None,
            sleep: tokio_sleep(),
        }
    }

    /// How long a connection may take to open, on the client the store or
    /// the decisions client builds (a caller's own client sets its own).
    /// Default 3 s.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// How long one attempt of a save or a load may take, response read
    /// included. Default 30 s.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// How saves and loads are retried. Default [`RetryPolicy::default`]
    /// (contract §8).
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// A request body larger than this many bytes is sent gzipped. Default
    /// 64 KiB.
    pub fn with_gzip_threshold(mut self, bytes: usize) -> Self {
        self.gzip_threshold = bytes;
        self
    }

    /// The client to send with. Without one, a client is built with the
    /// connect timeout, `rustls` and gzip decompression.
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// What every wait between attempts, and every wait of a
    /// [`DecisionWorker`](crate::DecisionWorker), sleeps with.
    pub fn with_sleep(mut self, sleep: Sleep) -> Self {
        self.sleep = sleep;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        let url = reqwest::Url::parse(&self.base_url)
            .map_err(|err| format!("the base URL is not a URL: {err}"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err("the base URL must be http or https".to_string());
        }
        if self.api_key.trim().is_empty() {
            return Err("the API key is required".to_string());
        }
        if self.connect_timeout.is_zero() || self.request_timeout.is_zero() {
            return Err("timeouts must be above zero".to_string());
        }
        if self.retry.max_attempts == 0 {
            return Err("a retry policy makes at least one attempt".to_string());
        }
        Ok(())
    }
}

impl fmt::Debug for StrataCloudOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StrataCloudOptions")
            .field("base_url", &self.base_url)
            .field("api_key", &"<redacted>")
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("retry", &self.retry)
            .field("gzip_threshold", &self.gzip_threshold)
            .finish_non_exhaustive()
    }
}

/// How a save or a load is retried (`design/30-Strata-Cloud-Store.md`,
/// "Retries"): on a network error, a timeout, `408`, `429`, `500`, `502`,
/// `503` and `504`, and for a save `409`. Never on any other `4xx`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// How many attempts a call makes at most, the first included. At least 1.
    pub max_attempts: u32,
    /// The wait before each retry, before `Retry-After` is applied.
    pub backoff: Backoff,
    /// The longest `Retry-After` a call waits for. A longer one fails the
    /// call at once with [`CloudError::Unavailable`](crate::CloudError),
    /// carrying it, rather than holding a save that long.
    pub max_retry_after: Duration,
}

impl Default for RetryPolicy {
    /// Contract §8: 4 attempts, backoff from 200 ms doubling to 5 s with full
    /// jitter, never shorter than `Retry-After`, which may be at most 30 s.
    fn default() -> Self {
        Self {
            max_attempts: 4,
            backoff: Backoff {
                initial: Duration::from_millis(200),
                max: Duration::from_secs(5),
                jitter: Jitter::Full,
            },
            max_retry_after: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// The wait before the `retry`-th retry (1-based), given the response's
    /// `Retry-After`.
    pub(crate) fn delay_before(&self, retry: u32, retry_after: Option<Duration>) -> Duration {
        let computed = backoff_delay(&self.backoff, retry);
        match retry_after {
            Some(after) if after > computed => after,
            _ => computed,
        }
    }
}

/// `initial × 2^(n−1)`, capped at `max`, jittered.
pub(crate) fn backoff_delay(backoff: &Backoff, retry: u32) -> Duration {
    let mut ceiling = backoff.initial;
    let mut doubled = 1;
    while doubled < retry && ceiling < backoff.max {
        ceiling = ceiling.checked_mul(2).unwrap_or(backoff.max);
        doubled += 1;
    }
    let ceiling = ceiling.min(backoff.max);
    match backoff.jitter {
        Jitter::Full => ceiling.mul_f64(jitter_sample()),
        Jitter::Equal => ceiling / 2 + (ceiling / 2).mul_f64(jitter_sample()),
        Jitter::None => ceiling,
    }
}

/// A sample uniform in `[0, 1)`: std's per-hasher random keys, as the engine
/// draws its backoff jitter, so the crate needs no RNG.
fn jitter_sample() -> f64 {
    use std::hash::{BuildHasher, Hasher, RandomState};
    let bits = RandomState::new().build_hasher().finish();
    (bits >> 11) as f64 / (1u64 << 53) as f64
}
