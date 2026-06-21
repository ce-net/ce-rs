//! Server-Sent Events (SSE) helper: a line parser plus typed async streams over the node's
//! four push endpoints (`/blocks/stream`, `/transactions/stream`, `/signals/stream`,
//! `/mesh/messages/stream`).
//!
//! The node emits `text/event-stream` frames (`data: <JSON>\n\n`, keep-alive comments every
//! 15s). [`CeClient`](crate::CeClient)'s `*_stream` methods open such an endpoint with
//! `reqwest`'s `bytes_stream`, feed the bytes through [`SseDecoder`] (which correctly handles
//! events split across read boundaries — the #1 SSE bug — plus `\n`/`\r`/`\r\n` line endings,
//! multi-line `data:`, and comments), and yield one typed domain event per frame.
//!
//! The event types mirror the `@ce-net/sdk` (TS) decoders 1:1: [`BlockEvent`], [`TxEvent`],
//! [`Signal`], [`AppMessage`].

use crate::amount::Amount;
use anyhow::{anyhow, Result};
use futures_core::Stream;
use serde::Deserialize;

/// A single parsed SSE frame: the `event:` type (default `"message"`), the accumulated `data:`
/// payload (lines joined by `\n`), and the optional `id:`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SseEvent {
    /// The `event:` field, or `"message"` when none was sent.
    pub event: String,
    /// The accumulated `data:` payload (multiple `data:` lines joined by `\n`).
    pub data: String,
    /// The `id:` field, if any (persists across frames per the SSE spec).
    pub id: Option<String>,
}

/// Incremental SSE frame decoder. Push raw bytes with [`push`](Self::push); it returns every
/// complete frame parsed so far. Bytes that don't yet form a complete line are buffered, so it
/// is safe to feed arbitrary chunk boundaries (the classic SSE failure mode).
#[derive(Debug, Default)]
pub struct SseDecoder {
    buffer: String,
    data_lines: Vec<String>,
    event_type: Option<String>,
    last_event_id: Option<String>,
    saw_data: bool,
}

impl SseDecoder {
    /// A fresh decoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed a chunk of UTF-8 bytes; returns every complete frame newly available. Invalid UTF-8
    /// is lossily decoded (the node always sends UTF-8 JSON, so this is defensive only).
    pub fn push(&mut self, bytes: &[u8]) -> Vec<SseEvent> {
        self.buffer.push_str(&String::from_utf8_lossy(bytes));
        let mut out = Vec::new();
        while let Some((line, rest)) = take_line(&self.buffer) {
            self.buffer = rest;
            if let Some(ev) = self.feed_line(&line) {
                out.push(ev);
            }
        }
        out
    }

    /// Flush a trailing frame when the stream ends without a final blank line.
    pub fn finish(&mut self) -> Option<SseEvent> {
        // Any remaining buffered text with no terminator is, per spec, an incomplete line and
        // is discarded; but a fully-formed pending frame (data with no blank line) is dispatched.
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            self.feed_line(&line);
        }
        self.flush()
    }

    fn feed_line(&mut self, line: &str) -> Option<SseEvent> {
        if line.is_empty() {
            return self.flush();
        }
        if line.starts_with(':') {
            // Comment / keep-alive.
            return None;
        }
        let (field, mut value) = match line.split_once(':') {
            Some((f, v)) => (f, v),
            None => (line, ""),
        };
        value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "data" => {
                self.data_lines.push(value.to_string());
                self.saw_data = true;
            }
            "event" => self.event_type = Some(value.to_string()),
            "id" => {
                if !value.contains('\0') {
                    self.last_event_id = Some(value.to_string());
                }
            }
            _ => {} // "retry" and unknown fields are ignored.
        }
        None
    }

    /// Dispatch the buffered frame on a blank line. Per the HTML SSE algorithm, a frame is only
    /// dispatched when its data buffer is non-empty.
    fn flush(&mut self) -> Option<SseEvent> {
        if !self.saw_data {
            self.data_lines.clear();
            self.event_type = None;
            return None;
        }
        let ev = SseEvent {
            event: self.event_type.take().unwrap_or_else(|| "message".to_string()),
            data: self.data_lines.join("\n"),
            id: self.last_event_id.clone(),
        };
        self.data_lines.clear();
        self.saw_data = false;
        Some(ev)
    }
}

/// Split off the first complete line (terminated by `\n`, `\r`, or `\r\n`), returning
/// `(line_without_terminator, remainder)`. `None` if no terminator is present yet.
fn take_line(s: &str) -> Option<(String, String)> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\n' || c == b'\r' {
            let line = s[..i].to_string();
            // Collapse \r\n.
            let mut next = i + 1;
            if c == b'\r' && bytes.get(i + 1) == Some(&b'\n') {
                next = i + 2;
            }
            return Some((line, s[next..].to_string()));
        }
        i += 1;
    }
    None
}

// ----- typed stream event types (mirror the TS SDK decoders) -----

/// A newly-accepted block, from `/blocks/stream`.
#[derive(Debug, Clone, Deserialize)]
pub struct BlockEvent {
    pub index: u64,
    pub hash: String,
    pub prev_hash: String,
    pub timestamp: u64,
    pub miner: String,
    pub tx_count: u64,
    pub nonce: u64,
}

/// A verified transaction, from `/transactions/stream`. `amount` is `Amount::ZERO` for kinds
/// that carry no value.
#[derive(Debug, Clone, Deserialize)]
pub struct TxEvent {
    pub id: String,
    pub origin: String,
    pub kind: String,
    #[serde(default)]
    pub amount: Amount,
}

/// A validated CEP-1 signal, from `/signals/stream`.
#[derive(Debug, Clone, Deserialize)]
pub struct Signal {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub payload_hex: String,
    #[serde(default)]
    pub burn_proof: serde_json::Value,
    pub nonce: u64,
    pub id: String,
}

impl Signal {
    /// Decode the hex payload bytes.
    pub fn payload(&self) -> Result<Vec<u8>> {
        hex::decode(&self.payload_hex).map_err(|e| anyhow!("bad signal payload hex: {e}"))
    }
}

/// Decode a [`reqwest::Response`] body (an SSE byte stream) into typed `T` events.
///
/// Yields `Result<T>` so a single malformed frame surfaces as an error item without tearing
/// down the whole stream. The stream ends when the connection closes.
pub(crate) fn decode_stream<T>(resp: reqwest::Response) -> impl Stream<Item = Result<T>>
where
    T: for<'de> Deserialize<'de>,
{
    use futures_util::StreamExt;

    let bytes = resp.bytes_stream();
    futures_util::stream::unfold(
        (bytes, SseDecoder::new(), std::collections::VecDeque::<SseEvent>::new(), false),
        |(mut bytes, mut decoder, mut pending, mut done)| async move {
            loop {
                // Drain already-parsed frames first.
                if let Some(ev) = pending.pop_front() {
                    let item = serde_json::from_str::<T>(&ev.data)
                        .map_err(|e| anyhow!("decode SSE frame: {e}: {}", ev.data));
                    return Some((item, (bytes, decoder, pending, done)));
                }
                if done {
                    return None;
                }
                match bytes.next().await {
                    Some(Ok(chunk)) => {
                        for ev in decoder.push(&chunk) {
                            pending.push_back(ev);
                        }
                    }
                    Some(Err(e)) => {
                        done = true;
                        return Some((
                            Err(anyhow!("SSE transport error: {e}")),
                            (bytes, decoder, pending, done),
                        ));
                    }
                    None => {
                        done = true;
                        if let Some(ev) = decoder.finish() {
                            pending.push_back(ev);
                        }
                        // Loop once more to drain any final frame, then end.
                    }
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(chunks: &[&str]) -> Vec<SseEvent> {
        let mut d = SseDecoder::new();
        let mut out = Vec::new();
        for c in chunks {
            out.extend(d.push(c.as_bytes()));
        }
        if let Some(ev) = d.finish() {
            out.push(ev);
        }
        out
    }

    #[test]
    fn parses_a_simple_data_event() {
        let evs = collect(&["data: {\"index\":1}\n\n"]);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data, "{\"index\":1}");
        assert_eq!(evs[0].event, "message");
    }

    #[test]
    fn handles_events_split_across_read_boundaries() {
        // The #1 SSE bug: a frame straddling chunk boundaries.
        let evs = collect(&["data: hel", "lo\n", "\ndata: wor", "ld\n\n"]);
        let datas: Vec<_> = evs.iter().map(|e| e.data.clone()).collect();
        assert_eq!(datas, vec!["hello", "world"]);
    }

    #[test]
    fn supports_multiline_data_event_type_and_id() {
        let evs = collect(&["event: tx\nid: 42\ndata: a\ndata: b\n\n"]);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].event, "tx");
        assert_eq!(evs[0].id.as_deref(), Some("42"));
        assert_eq!(evs[0].data, "a\nb");
    }

    #[test]
    fn ignores_keep_alive_comments() {
        let evs = collect(&[": keep-alive\n\n", "data: x\n\n"]);
        let datas: Vec<_> = evs.iter().map(|e| e.data.clone()).collect();
        assert_eq!(datas, vec!["x"]);
    }

    #[test]
    fn handles_crlf_line_endings() {
        let evs = collect(&["data: x\r\n\r\n"]);
        let datas: Vec<_> = evs.iter().map(|e| e.data.clone()).collect();
        assert_eq!(datas, vec!["x"]);
    }

    #[test]
    fn emits_trailing_event_without_final_blank_line() {
        let evs = collect(&["data: last\n"]);
        let datas: Vec<_> = evs.iter().map(|e| e.data.clone()).collect();
        assert_eq!(datas, vec!["last"]);
    }

    #[test]
    fn a_lone_id_with_no_data_emits_nothing_but_persists() {
        // id without data dispatches nothing; the following data frame carries the id forward.
        let evs = collect(&["id: 7\n\n", "data: hi\n\n"]);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data, "hi");
        assert_eq!(evs[0].id.as_deref(), Some("7"));
    }

    #[test]
    fn decodes_typed_block_and_tx_frames() {
        let mut d = SseDecoder::new();
        let frame = "data: {\"index\":3,\"hash\":\"aa\",\"prev_hash\":\"bb\",\"timestamp\":1,\"miner\":\"m\",\"tx_count\":2,\"nonce\":9}\n\n";
        let evs = d.push(frame.as_bytes());
        assert_eq!(evs.len(), 1);
        let blk: BlockEvent = serde_json::from_str(&evs[0].data).unwrap();
        assert_eq!(blk.index, 3);
        assert_eq!(blk.tx_count, 2);

        let mut d2 = SseDecoder::new();
        let evs2 =
            d2.push(b"data: {\"id\":\"t1\",\"origin\":\"o\",\"kind\":\"Transfer\",\"amount\":\"2500000000000000000\"}\n\n");
        let tx: TxEvent = serde_json::from_str(&evs2[0].data).unwrap();
        assert_eq!(tx.kind, "Transfer");
        assert_eq!(tx.amount.credits(), "2.5");
    }
}
