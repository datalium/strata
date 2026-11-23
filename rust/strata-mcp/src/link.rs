//! A link carries JSON-RPC messages to one server and its answers back. The
//! pipe link frames them one per line over any stream pair: a child's stdin
//! and stdout, a socket, an in-memory pipe (`design/50-MCP-Client.md`,
//! "stdio").

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::McpError;
use crate::jsonrpc::{Answer, Incoming, answer_server_request};
use crate::options::Log;

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub(crate) type Reader = Box<dyn AsyncRead + Send + Unpin>;
pub(crate) type Writer = Box<dyn AsyncWrite + Send + Unpin>;

/// One request on its way out.
pub(crate) struct Outgoing {
    pub id: u64,
    pub body: Value,
    /// HTTP only: `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name`, `Mcp-Param-*`.
    #[cfg_attr(not(feature = "http"), allow(dead_code))]
    pub headers: Vec<(String, String)>,
    /// Sent when the request is abandoned (its token fired, it timed out,
    /// its future was dropped) before the answer: `notifications/cancelled`.
    pub cancel: Option<Value>,
}

pub(crate) trait Link: Send + Sync {
    /// Sends the request and waits for its answer. Dropping the future
    /// abandons the request.
    fn request(&self, outgoing: Outgoing) -> BoxFuture<'_, Result<Answer, McpError>>;
    fn notify(
        &self,
        body: Value,
        headers: Vec<(String, String)>,
    ) -> BoxFuture<'_, Result<(), McpError>>;
    /// Ends the link: stdio closes the server's stdin, legacy HTTP deletes
    /// its session.
    fn close(&self) -> BoxFuture<'_, ()>;
    /// Why the link can't carry requests any more, when it can't.
    fn ended(&self) -> Option<McpError>;
    /// The legacy HTTP session, for spans.
    fn session_id(&self) -> Option<String> {
        None
    }
}

struct Waiting {
    answers: HashMap<u64, oneshot::Sender<Result<Answer, McpError>>>,
    ended: Option<McpError>,
}

struct Shared {
    writer: tokio::sync::Mutex<Option<Writer>>,
    waiting: Mutex<Waiting>,
    log: Log,
}

impl Shared {
    async fn write(&self, message: &Value) -> Result<(), McpError> {
        let mut line = serde_json::to_vec(message).expect("a JSON value serializes");
        line.push(b'\n');
        let mut writer = self.writer.lock().await;
        let Some(writer) = writer.as_mut() else {
            return Err(self.ended().unwrap_or(McpError::Closed));
        };
        let written = async {
            writer.write_all(&line).await?;
            writer.flush().await
        };
        written
            .await
            .map_err(|error| McpError::Transport(format!("couldn't write to the server: {error}")))
    }

    fn ended(&self) -> Option<McpError> {
        self.waiting.lock().unwrap().ended.clone()
    }

    /// Fails every waiting request with `error`, and every later one.
    fn end(&self, error: McpError) {
        let answers = {
            let mut waiting = self.waiting.lock().unwrap();
            if waiting.ended.is_none() {
                waiting.ended = Some(error.clone());
            }
            std::mem::take(&mut waiting.answers)
        };
        for (_, answer) in answers {
            let _ = answer.send(Err(error.clone()));
        }
    }
}

/// Frames messages one per line over a stream pair.
pub(crate) struct PipeLink {
    shared: Arc<Shared>,
    reader: JoinHandle<()>,
}

impl PipeLink {
    /// Starts reading `from_server`. `exited` is what a request fails with
    /// when the stream ends.
    pub(crate) fn start(
        from_server: Reader,
        to_server: Writer,
        max_message_bytes: usize,
        log: Log,
        exited: &'static str,
    ) -> Self {
        let shared = Arc::new(Shared {
            writer: tokio::sync::Mutex::new(Some(to_server)),
            waiting: Mutex::new(Waiting {
                answers: HashMap::new(),
                ended: None,
            }),
            log,
        });
        let reader = tokio::spawn(read(
            shared.clone(),
            BufReader::new(from_server),
            max_message_bytes,
            exited,
        ));
        Self { shared, reader }
    }
}

impl Drop for PipeLink {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

async fn read(
    shared: Arc<Shared>,
    mut from_server: BufReader<Reader>,
    max: usize,
    exited: &'static str,
) {
    let failure = loop {
        let line = match read_line(&mut from_server, max).await {
            Ok(Some(line)) => line,
            Ok(None) => break McpError::Transport(exited.to_string()),
            Err(LineError::TooBig) => {
                break McpError::Transport(format!(
                    "the server sent a message bigger than {max} bytes"
                ));
            }
            Err(LineError::Io(error)) => {
                break McpError::Transport(format!("couldn't read from the server: {error}"));
            }
        };
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let Ok(message) = serde_json::from_slice::<Value>(&line) else {
            // On stdio a server must write nothing but messages to stdout,
            // and the stream can't be resynchronized.
            break McpError::Transport("the server wrote a line that isn't JSON".to_string());
        };
        match Incoming::parse(&message) {
            Incoming::Response {
                id: Some(id),
                answer,
            } => {
                let waiting = shared.waiting.lock().unwrap().answers.remove(&id);
                match waiting {
                    Some(waiting) => {
                        let _ = waiting.send(Ok(answer));
                    }
                    None => shared.log.debug(|| {
                        format!("ignored an answer to request {id}, which nobody waits for")
                    }),
                }
            }
            Incoming::Request { id, method } => {
                shared
                    .log
                    .debug(|| format!("answered the server's request {method}"));
                let _ = shared.write(&answer_server_request(&id, &method)).await;
            }
            Incoming::Notification { method } => {
                shared
                    .log
                    .debug(|| format!("ignored the notification {method}"));
            }
            Incoming::Response { id: None, .. } | Incoming::Invalid => {
                shared.log.debug(|| {
                    "ignored a message that isn't a JSON-RPC answer to a request".to_string()
                });
            }
        }
    };
    shared.end(failure);
}

pub(crate) enum LineError {
    TooBig,
    Io(std::io::Error),
}

/// One line, without its `\n` (or `\r\n`); `None` at the end of the stream.
/// A last line without a newline counts.
pub(crate) async fn read_line<R: AsyncBufRead + Unpin>(
    from: &mut R,
    max: usize,
) -> Result<Option<Vec<u8>>, LineError> {
    let mut line = Vec::new();
    loop {
        let buffer = from.fill_buf().await.map_err(LineError::Io)?;
        if buffer.is_empty() {
            return Ok((!line.is_empty()).then_some(line));
        }
        let (taken, ended) = match buffer.iter().position(|&b| b == b'\n') {
            Some(end) => (end, true),
            None => (buffer.len(), false),
        };
        if line.len() + taken > max {
            return Err(LineError::TooBig);
        }
        line.extend_from_slice(&buffer[..taken]);
        from.consume(taken + usize::from(ended));
        if ended {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(Some(line));
        }
    }
}

/// Abandons a request whose future ends before its answer: stops waiting
/// for it and, when it has one, sends its `notifications/cancelled`.
struct Abandon {
    shared: Arc<Shared>,
    id: u64,
    cancel: Option<Value>,
    answered: bool,
}

impl Drop for Abandon {
    fn drop(&mut self) {
        if self.answered {
            return;
        }
        self.shared.waiting.lock().unwrap().answers.remove(&self.id);
        if let (Some(cancel), Ok(runtime)) =
            (self.cancel.take(), tokio::runtime::Handle::try_current())
        {
            let shared = self.shared.clone();
            runtime.spawn(async move {
                let _ = shared.write(&cancel).await;
            });
        }
    }
}

impl Link for PipeLink {
    fn request(&self, outgoing: Outgoing) -> BoxFuture<'_, Result<Answer, McpError>> {
        Box::pin(async move {
            let (sender, answer) = oneshot::channel();
            {
                let mut waiting = self.shared.waiting.lock().unwrap();
                if let Some(ended) = &waiting.ended {
                    return Err(ended.clone());
                }
                waiting.answers.insert(outgoing.id, sender);
            }
            let mut abandon = Abandon {
                shared: self.shared.clone(),
                id: outgoing.id,
                cancel: outgoing.cancel,
                answered: false,
            };
            if let Err(error) = self.shared.write(&outgoing.body).await {
                abandon.cancel = None;
                return Err(error);
            }
            let answer = answer.await;
            abandon.answered = true;
            answer.unwrap_or_else(|_| Err(self.shared.ended().unwrap_or(McpError::Closed)))
        })
    }

    fn notify(
        &self,
        body: Value,
        _headers: Vec<(String, String)>,
    ) -> BoxFuture<'_, Result<(), McpError>> {
        Box::pin(async move { self.shared.write(&body).await })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let writer = self.shared.writer.lock().await.take();
            if let Some(mut writer) = writer {
                let _ = writer.shutdown().await;
            }
            self.shared.end(McpError::Closed);
        })
    }

    fn ended(&self) -> Option<McpError> {
        self.shared.ended()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lines_are_read_with_a_limit() {
        let mut input: &[u8] = b"{\"a\":1}\r\nsecond\nlast";
        let mut reader = BufReader::new(&mut input);
        assert_eq!(
            read_line(&mut reader, 64).await.ok().flatten().unwrap(),
            b"{\"a\":1}"
        );
        assert_eq!(
            read_line(&mut reader, 64).await.ok().flatten().unwrap(),
            b"second"
        );
        assert_eq!(
            read_line(&mut reader, 64).await.ok().flatten().unwrap(),
            b"last"
        );
        assert!(read_line(&mut reader, 64).await.ok().unwrap().is_none());

        let mut input: &[u8] = b"0123456789\n";
        let mut reader = BufReader::new(&mut input);
        assert!(matches!(
            read_line(&mut reader, 4).await,
            Err(LineError::TooBig)
        ));
    }
}
