//! What the two HTTP clients share: sending a request and reading a failure,
//! and server-sent events (`design/44-LLM-Integration.md`).

use std::time::Duration;

use serde_json::Value;

use crate::client::ChatError;
use crate::faults::retry_after_of;

/// The longest part of an error body a [`ChatError`] message keeps.
const MESSAGE_LIMIT: usize = 512;

/// The HTTP client both providers build: rustls, a connect timeout, and a
/// read timeout (no total timeout, which would cut a long stream off).
pub(crate) fn http_client(
    connect_timeout: Duration,
    read_timeout: Duration,
) -> Result<reqwest::Client, ChatError> {
    reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .read_timeout(read_timeout)
        .build()
        .map_err(|error| ChatError::fatal("client", error.to_string()))
}

/// Sends `request` and returns the response when its status is a success;
/// otherwise the [`ChatError`] its status, retry hint and body make.
pub(crate) async fn send(request: reqwest::RequestBuilder) -> Result<reqwest::Response, ChatError> {
    let response = request.send().await.map_err(transport)?;
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let retry_after = retry_after_of(response.headers());
    let body = response.text().await.unwrap_or_default();
    Err(ChatError::status(
        status.as_u16(),
        retry_after,
        describe(status, &body),
    ))
}

/// A transport failure: a timeout, or a connection that failed or was lost.
pub(crate) fn transport(error: reqwest::Error) -> ChatError {
    if error.is_timeout() {
        ChatError::transient("timeout", error.to_string())
    } else if error.is_decode() {
        ChatError::fatal("invalid_response", error.to_string())
    } else {
        ChatError::transient("connection", error.to_string())
    }
}

/// `"429 Too Many Requests: <the provider's error message>"`.
fn describe(status: reqwest::StatusCode, body: &str) -> String {
    let reason = status.canonical_reason().unwrap_or("");
    let detail = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| body.trim().to_string());
    let mut detail: String = detail.chars().take(MESSAGE_LIMIT).collect();
    if !detail.is_empty() {
        detail.insert_str(0, ": ");
    }
    format!("{} {reason}{detail}", status.as_u16())
}

/// The body of a successful, non-streamed response as JSON.
pub(crate) async fn json(response: reqwest::Response) -> Result<Value, ChatError> {
    let text = response.text().await.map_err(transport)?;
    serde_json::from_str(&text)
        .map_err(|error| ChatError::fatal("invalid_response", format!("not JSON: {error}")))
}

/// One server-sent event: its `event:` name, if any, and its `data:` lines
/// joined with newlines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Event {
    pub event: Option<String>,
    pub data: String,
}

/// Splits a byte stream into server-sent events, however the bytes arrive.
#[derive(Default)]
pub(crate) struct EventParser {
    buffer: Vec<u8>,
}

impl EventParser {
    /// The events `bytes` completes, in order.
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<Event> {
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some((end, skip)) = boundary(&self.buffer) {
            let block: Vec<u8> = self.buffer.drain(..end + skip).take(end).collect();
            if let Some(event) = parse_block(&String::from_utf8_lossy(&block)) {
                events.push(event);
            }
        }
        events
    }
}

/// Where the first blank line is: the block's end and the separator's length.
fn boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    (0..buffer.len()).find_map(|i| {
        if buffer[i..].starts_with(b"\r\n\r\n") {
            Some((i, 4))
        } else if buffer[i..].starts_with(b"\n\n") || buffer[i..].starts_with(b"\r\r") {
            Some((i, 2))
        } else {
            None
        }
    })
}

fn parse_block(block: &str) -> Option<Event> {
    let mut event = None;
    let mut data: Vec<&str> = Vec::new();
    for line in block.lines() {
        if line.starts_with(':') {
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => event = Some(value.to_string()),
            "data" => data.push(value),
            _ => {}
        }
    }
    (!data.is_empty()).then(|| Event {
        event,
        data: data.join("\n"),
    })
}

/// The events of a streamed response, as a stream of `T`s that `decode` makes
/// from each event (none, one or several), ending when the body ends or
/// `decode` says the answer is done.
pub(crate) fn events<T, D>(
    response: reqwest::Response,
    decode: D,
) -> impl futures_core::Stream<Item = Result<T, ChatError>> + Send
where
    T: Send + 'static,
    D: FnMut(Event) -> Result<Decoded<T>, ChatError> + Send + 'static,
{
    struct State<D, T> {
        response: reqwest::Response,
        parser: EventParser,
        decode: D,
        pending: std::collections::VecDeque<Result<T, ChatError>>,
        done: bool,
    }
    let state = State {
        response,
        parser: EventParser::default(),
        decode,
        pending: std::collections::VecDeque::new(),
        done: false,
    };
    futures_util::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(item) = state.pending.pop_front() {
                return Some((item, state));
            }
            if state.done {
                return None;
            }
            match state.response.chunk().await {
                Ok(Some(bytes)) => {
                    for event in state.parser.feed(&bytes) {
                        match (state.decode)(event) {
                            Ok(Decoded::Items(items)) => {
                                state.pending.extend(items.into_iter().map(Ok));
                            }
                            Ok(Decoded::Done(items)) => {
                                state.pending.extend(items.into_iter().map(Ok));
                                state.done = true;
                                break;
                            }
                            Err(error) => {
                                state.pending.push_back(Err(error));
                                state.done = true;
                                break;
                            }
                        }
                    }
                }
                Ok(None) => {
                    state.done = true;
                    state.pending.push_back(Err(ChatError::transient(
                        "connection",
                        "the stream ended before the answer did",
                    )));
                }
                Err(error) => {
                    state.done = true;
                    state.pending.push_back(Err(transport(error)));
                }
            }
        }
    })
}

/// What one event decodes to.
pub(crate) enum Decoded<T> {
    /// More of the answer.
    Items(Vec<T>),
    /// The last of it.
    Done(Vec<T>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_are_split_however_the_bytes_arrive() {
        let mut parser = EventParser::default();
        let mut events = parser.feed(b"event: message_start\ndata: {\"a\"");
        assert!(events.is_empty());
        events
            .extend(parser.feed(b":1}\n\n: comment\n\ndata: x\r\ndata: y\r\n\r\ndata: [DONE]\n\n"));
        assert_eq!(
            events,
            vec![
                Event {
                    event: Some("message_start".into()),
                    data: "{\"a\":1}".into()
                },
                Event {
                    event: None,
                    data: "x\ny".into()
                },
                Event {
                    event: None,
                    data: "[DONE]".into()
                },
            ]
        );
    }
}
