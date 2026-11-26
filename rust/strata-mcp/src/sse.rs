//! A `text/event-stream` parser (the WHATWG "server-sent events" rules), for
//! Streamable HTTP responses (`design/50-MCP-Client.md`, "Streamable HTTP";
//! `schema/fixtures/mcp/sse.json`). Fed bytes as they arrive.

/// One dispatched event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Event {
    pub kind: String,
    pub data: String,
    pub id: Option<String>,
}

#[derive(Default)]
pub(crate) struct Parser {
    /// Bytes of a line not yet ended.
    line: Vec<u8>,
    /// A `\r` ended the last line: a `\n` right after it belongs to it.
    after_cr: bool,
    data: String,
    kind: Option<String>,
    last_id: Option<String>,
    /// The size of the event being built, bounded by `limit`.
    size: usize,
    limit: usize,
}

/// An event's data grew past the limit.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TooBig;

impl Parser {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            ..Self::default()
        }
    }

    /// Feeds `bytes`, returning the events they completed.
    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Result<Vec<Event>, TooBig> {
        let mut events = Vec::new();
        for &byte in bytes {
            if self.after_cr {
                self.after_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            match byte {
                b'\n' | b'\r' => {
                    self.after_cr = byte == b'\r';
                    let line = std::mem::take(&mut self.line);
                    if let Some(event) = self.line_ended(&line) {
                        events.push(event);
                    }
                }
                _ => {
                    self.line.push(byte);
                    self.size += 1;
                    if self.size > self.limit {
                        return Err(TooBig);
                    }
                }
            }
        }
        Ok(events)
    }

    fn line_ended(&mut self, line: &[u8]) -> Option<Event> {
        if line.is_empty() {
            return self.dispatch();
        }
        if line[0] == b':' {
            return None;
        }
        let line = String::from_utf8_lossy(line);
        let (field, value) = match line.find(':') {
            Some(colon) => {
                let value = &line[colon + 1..];
                (&line[..colon], value.strip_prefix(' ').unwrap_or(value))
            }
            None => (&line[..], ""),
        };
        match field {
            "data" => {
                self.data.push_str(value);
                self.data.push('\n');
            }
            "event" => self.kind = Some(value.to_string()),
            "id" if !value.contains('\0') => self.last_id = Some(value.to_string()),
            _ => {}
        }
        None
    }

    fn dispatch(&mut self) -> Option<Event> {
        self.size = 0;
        let kind = self.kind.take();
        if self.data.is_empty() {
            return None;
        }
        let mut data = std::mem::take(&mut self.data);
        data.pop();
        Some(Event {
            kind: kind
                .filter(|kind| !kind.is_empty())
                .unwrap_or_else(|| "message".to_string()),
            data,
            id: self.last_id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures;

    fn events_of(case: &serde_json::Value) -> Vec<Event> {
        case["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| Event {
                kind: e["type"].as_str().unwrap().to_string(),
                data: e["data"].as_str().unwrap().to_string(),
                id: e["id"].as_str().map(str::to_string),
            })
            .collect()
    }

    #[test]
    fn the_shared_sse_fixture_at_once_and_byte_by_byte() {
        let fixture = fixtures::load("sse.json");
        for case in fixture["cases"].as_array().unwrap() {
            let input = case["input"].as_str().unwrap().as_bytes();
            let expected = events_of(case);

            let mut whole = Parser::new(1 << 20);
            assert_eq!(whole.feed(input).unwrap(), expected, "{}", case["name"]);

            let mut split = Parser::new(1 << 20);
            let mut events = Vec::new();
            for byte in input {
                events.extend(split.feed(std::slice::from_ref(byte)).unwrap());
            }
            assert_eq!(events, expected, "{} byte by byte", case["name"]);
        }
    }

    #[test]
    fn an_event_past_the_limit_is_refused() {
        let mut parser = Parser::new(8);
        assert_eq!(parser.feed(b"data: 0123456789\n\n"), Err(TooBig));
    }
}
