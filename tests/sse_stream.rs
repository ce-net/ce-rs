//! End-to-end SSE stream tests over the mock server: typed decoding through the substrate
//! `*_stream` methods, truncated frames, transport drops, and malformed frames yielding error
//! items (not stream teardown).
//!
//! The chain streams (`blocks_stream`/`transactions_stream`) moved to the economy ceapp's SDK; the
//! generic SSE decoder they used is substrate and stays, so these tests exercise its mechanics
//! through `signals_stream()` (a substrate `/signals/stream` stream) instead.
//!
//! The `*_stream` methods return `impl Stream` that (under Rust 2024 capture rules) borrows the
//! client, so each test binds the client to a `let` first, then `Box::pin`s the stream to poll it
//! with `.next()`.

mod common;
use ce_rs::CeClient;
use common::{MockServer, Reply};
use futures_util::StreamExt;

fn client_for(server: &MockServer) -> CeClient {
    CeClient::with_token(server.base_url(), Some("t".into()))
}

#[tokio::test]
async fn signals_stream_decodes_and_payload() {
    let payload_hex = hex::encode(b"signal-bytes");
    let body = format!(
        "data: {{\"from\":\"f\",\"to\":\"t\",\"nonce\":1,\"id\":\"s1\",\"payload_hex\":\"{payload_hex}\"}}\n\n"
    );
    let server = MockServer::new()
        .route("GET", "/signals/stream", Reply::sse(body, None))
        .start()
        .await;
    let ce = client_for(&server);
    let mut s = Box::pin(ce.signals_stream().await.unwrap());
    let sig = s.next().await.unwrap().unwrap();
    assert_eq!(sig.id, "s1");
    assert_eq!(sig.payload().unwrap(), b"signal-bytes");
    assert!(sig.capabilities.is_empty()); // default
}

#[tokio::test]
async fn messages_stream_typed() {
    let ph = hex::encode(b"msg");
    let body = format!("data: {{\"from\":\"f\",\"topic\":\"t\",\"payload_hex\":\"{ph}\",\"received_at\":1}}\n\n");
    let server = MockServer::new()
        .route("GET", "/mesh/messages/stream", Reply::sse(body, None))
        .start()
        .await;
    let ce = client_for(&server);
    let mut s = Box::pin(ce.messages_stream().await.unwrap());
    let m = s.next().await.unwrap().unwrap();
    assert_eq!(m.from, "f");
    assert_eq!(m.payload().unwrap(), b"msg");
}

#[tokio::test]
async fn sse_open_on_non_2xx_is_an_error() {
    let server = MockServer::new()
        .route("GET", "/signals/stream", Reply::text(500, "stream down"))
        .start()
        .await;
    let ce = client_for(&server);
    let err = ce.signals_stream().await.err().unwrap().to_string();
    assert!(err.contains("SSE open failed"), "{err}");
}

#[tokio::test]
async fn malformed_frame_surfaces_as_error_item_then_stream_continues() {
    // First frame has a wrong-typed field (nonce is a string) -> decode error item; the second
    // frame is a valid Signal. The error must NOT tear down the whole stream.
    let body = "data: {\"nonce\":\"not-a-number\"}\n\n\
                data: {\"from\":\"f\",\"to\":\"t\",\"nonce\":7,\"id\":\"s7\"}\n\n";
    let server = MockServer::new()
        .route("GET", "/signals/stream", Reply::sse(body, None))
        .start()
        .await;
    let ce = client_for(&server);
    let mut s = Box::pin(ce.signals_stream().await.unwrap());
    let first = s.next().await.unwrap();
    assert!(first.is_err(), "malformed frame should be an Err item");
    let second = s.next().await.unwrap().unwrap();
    assert_eq!(second.id, "s7");
}

#[tokio::test]
async fn truncated_final_frame_without_blank_line_is_still_emitted() {
    // The body ends mid-stream (connection drops) with a complete data line but no trailing blank
    // line. `decode_stream` flushes the pending frame on EOF.
    let body = "data: {\"from\":\"f\",\"to\":\"t\",\"nonce\":3,\"id\":\"s3\"}\n";
    let server = MockServer::new()
        .route("GET", "/signals/stream", Reply::sse(body, None))
        .start()
        .await;
    let ce = client_for(&server);
    let mut s = Box::pin(ce.signals_stream().await.unwrap());
    let sig = s.next().await.unwrap().unwrap();
    assert_eq!(sig.id, "s3");
    assert!(s.next().await.is_none());
}

#[tokio::test]
async fn truncated_incomplete_line_is_handled_gracefully() {
    // The connection drops in the middle of a data line (no terminator). On EOF, `finish()` treats
    // the leftover buffer as a final line; here that line is `data: {<partial JSON>` so the frame's
    // JSON fails to decode. The contract: the SDK must surface this as an Err *item* (or end the
    // stream) — never panic, and never hang. We assert it does not yield a successful Signal.
    let full = "data: {\"from\":\"f\",\"to\":\"t\",\"nonce\":9,\"id\":\"s9\"}\n\n";
    // Truncate to half the body, mid-line (mid-JSON, no terminator).
    let cut = full.len() / 2;
    let server = MockServer::new()
        .route("GET", "/signals/stream", Reply::sse(full, Some(cut)))
        .start()
        .await;
    let ce = client_for(&server);
    let mut s = Box::pin(ce.signals_stream().await.unwrap());
    // Drain the stream: every item must be an Err (malformed/partial), never an Ok event.
    let mut saw_ok = false;
    while let Some(item) = s.next().await {
        if item.is_ok() {
            saw_ok = true;
        }
    }
    assert!(!saw_ok, "a truncated mid-JSON frame must not decode to a valid event");
}

#[tokio::test]
async fn keepalive_comments_are_skipped_in_a_real_stream() {
    let body = ": keep-alive\n\n\
                : another\n\n\
                data: {\"from\":\"f\",\"to\":\"t\",\"nonce\":1,\"id\":\"s1\"}\n\n";
    let server = MockServer::new()
        .route("GET", "/signals/stream", Reply::sse(body, None))
        .start()
        .await;
    let ce = client_for(&server);
    let mut s = Box::pin(ce.signals_stream().await.unwrap());
    let sig = s.next().await.unwrap().unwrap();
    assert_eq!(sig.id, "s1");
}
