//! End-to-end SSE stream tests over the mock server: typed decoding through `*_stream` methods,
//! truncated frames, transport drops, malformed frames yielding error items (not stream teardown),
//! and the wallet's enriched `transactions_stream`.
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
async fn blocks_stream_yields_typed_events() {
    let body = "data: {\"index\":1,\"hash\":\"a\",\"prev_hash\":\"z\",\"timestamp\":10,\"miner\":\"m\",\"tx_count\":0,\"nonce\":5}\n\n\
                data: {\"index\":2,\"hash\":\"b\",\"prev_hash\":\"a\",\"timestamp\":20,\"miner\":\"m\",\"tx_count\":3,\"nonce\":6}\n\n";
    let server = MockServer::new()
        .route("GET", "/blocks/stream", Reply::sse(body, None))
        .start()
        .await;
    let ce = client_for(&server);
    let mut s = Box::pin(ce.blocks_stream().await.unwrap());
    let b1 = s.next().await.unwrap().unwrap();
    assert_eq!(b1.index, 1);
    let b2 = s.next().await.unwrap().unwrap();
    assert_eq!(b2.index, 2);
    assert_eq!(b2.tx_count, 3);
    assert!(s.next().await.is_none()); // stream closed
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
async fn transactions_stream_events_typed() {
    let body = "data: {\"id\":\"t1\",\"origin\":\"o\",\"kind\":\"Transfer\",\"amount\":\"1000000000000000000\"}\n\n";
    let server = MockServer::new()
        .route("GET", "/transactions/stream", Reply::sse(body, None))
        .start()
        .await;
    let ce = client_for(&server);
    let mut s = Box::pin(ce.transactions_stream_events().await.unwrap());
    let ev = s.next().await.unwrap().unwrap();
    assert_eq!(ev.kind, "Transfer");
    assert_eq!(ev.amount.credits(), "1");
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
        .route("GET", "/blocks/stream", Reply::text(500, "stream down"))
        .start()
        .await;
    let ce = client_for(&server);
    let err = ce.blocks_stream().await.err().unwrap().to_string();
    assert!(err.contains("SSE open failed"), "{err}");
}

#[tokio::test]
async fn malformed_frame_surfaces_as_error_item_then_stream_continues() {
    // First frame is valid JSON for a different shape (missing required fields) -> decode error
    // item; the second frame is valid. The error must NOT tear down the whole stream.
    let body = "data: {\"index\":\"not-a-number\"}\n\n\
                data: {\"index\":7,\"hash\":\"h\",\"prev_hash\":\"p\",\"timestamp\":1,\"miner\":\"m\",\"tx_count\":0,\"nonce\":0}\n\n";
    let server = MockServer::new()
        .route("GET", "/blocks/stream", Reply::sse(body, None))
        .start()
        .await;
    let ce = client_for(&server);
    let mut s = Box::pin(ce.blocks_stream().await.unwrap());
    let first = s.next().await.unwrap();
    assert!(first.is_err(), "malformed frame should be an Err item");
    let second = s.next().await.unwrap().unwrap();
    assert_eq!(second.index, 7);
}

#[tokio::test]
async fn truncated_final_frame_without_blank_line_is_still_emitted() {
    // The body ends mid-stream (connection drops) with a complete data line but no trailing blank
    // line. `decode_stream` flushes the pending frame on EOF.
    let body = "data: {\"index\":3,\"hash\":\"h\",\"prev_hash\":\"p\",\"timestamp\":1,\"miner\":\"m\",\"tx_count\":0,\"nonce\":0}\n";
    let server = MockServer::new()
        .route("GET", "/blocks/stream", Reply::sse(body, None))
        .start()
        .await;
    let ce = client_for(&server);
    let mut s = Box::pin(ce.blocks_stream().await.unwrap());
    let b = s.next().await.unwrap().unwrap();
    assert_eq!(b.index, 3);
    assert!(s.next().await.is_none());
}

#[tokio::test]
async fn truncated_incomplete_line_is_handled_gracefully() {
    // The connection drops in the middle of a data line (no terminator). On EOF, `finish()` treats
    // the leftover buffer as a final line; here that line is `data: {<partial JSON>` so the frame's
    // JSON fails to decode. The contract: the SDK must surface this as an Err *item* (or end the
    // stream) — never panic, and never hang. We assert it does not yield a successful BlockEvent.
    let full = "data: {\"index\":9,\"hash\":\"h\",\"prev_hash\":\"p\",\"timestamp\":1,\"miner\":\"m\",\"tx_count\":0,\"nonce\":0}\n\n";
    // Truncate to half the body, mid-line (mid-JSON, no terminator).
    let cut = full.len() / 2;
    let server = MockServer::new()
        .route("GET", "/blocks/stream", Reply::sse(full, Some(cut)))
        .start()
        .await;
    let ce = client_for(&server);
    let mut s = Box::pin(ce.blocks_stream().await.unwrap());
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
                data: {\"index\":1,\"hash\":\"h\",\"prev_hash\":\"p\",\"timestamp\":1,\"miner\":\"m\",\"tx_count\":0,\"nonce\":0}\n\n";
    let server = MockServer::new()
        .route("GET", "/blocks/stream", Reply::sse(body, None))
        .start()
        .await;
    let ce = client_for(&server);
    let mut s = Box::pin(ce.blocks_stream().await.unwrap());
    let b = s.next().await.unwrap().unwrap();
    assert_eq!(b.index, 1);
}

#[tokio::test]
async fn wallet_transactions_stream_enriches_direction() {
    let body = "data: {\"id\":\"t1\",\"origin\":\"me\",\"kind\":\"Transfer\",\"amount\":\"1000000000000000000\"}\n\n\
                data: {\"id\":\"t2\",\"origin\":\"other\",\"kind\":\"Transfer\",\"amount\":\"2000000000000000000\"}\n\n";
    let server = MockServer::new()
        .route("GET", "/transactions/stream", Reply::sse(body, None))
        .start()
        .await;
    let ce = client_for(&server);
    let wallet = ce.wallet();
    let mut s = Box::pin(wallet.transactions_stream("me").await.unwrap());
    let r1 = s.next().await.unwrap().unwrap();
    assert_eq!(r1.direction, ce_rs::Direction::Out);
    assert!(r1.counterparty.is_none());
    let r2 = s.next().await.unwrap().unwrap();
    assert_eq!(r2.direction, ce_rs::Direction::In);
    assert_eq!(r2.counterparty.as_deref(), Some("other"));
}
