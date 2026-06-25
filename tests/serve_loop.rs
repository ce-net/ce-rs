//! Coverage of the reusable mesh-app serve loop (`ce_rs::serve`) against the mock HTTP node.
//!
//! `serve` subscribes to its topics (`POST /mesh/subscribe`), reads inbound requests from the
//! node's SSE inbox (`GET /mesh/messages/stream`), runs the app's [`Handler`], and replies over the
//! mesh (`POST /mesh/reply`). These tests drive that whole loop with no real node: the
//! [`common::MockServer`] serves a canned SSE body of requests and captures the replies the SDK
//! posts back, so we assert handler dispatch, topic filtering, reply-token de-duplication, the
//! reconnect-after-stream-end behavior, and clean shutdown — all without libp2p.
//!
//! The mock server writes the SSE body then closes the connection (`Connection: close`), so each
//! `messages_stream()` ends after one batch; `serve` then reconnects. We exploit that to exercise
//! the reconnect path, and resolve the `shutdown` future once enough replies have arrived so the
//! loop terminates deterministically instead of reconnecting forever.
//!
//! This file requires the `serve` feature (the loop under test lives behind it); a plain
//! `cargo test` compiles it away, so run `cargo test --features serve` to exercise it.
#![cfg(feature = "serve")]

mod common;

use ce_rs::serve::{Handler, Request, serve, serve_where};
use ce_rs::CeClient;
use common::{CapturedRequest, MockServer, Reply};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn client_for(server: &MockServer) -> CeClient {
    CeClient::with_token(server.base_url(), Some("t".into()))
}

/// One SSE `data:` frame carrying an [`ce_rs::AppMessage`] JSON for a request (has a reply_token).
fn request_frame(from: &str, topic: &str, payload: &[u8], reply_token: u64) -> String {
    let ph = hex::encode(payload);
    format!(
        "data: {{\"from\":\"{from}\",\"topic\":\"{topic}\",\"payload_hex\":\"{ph}\",\"received_at\":1,\"reply_token\":{reply_token}}}\n\n"
    )
}

/// One SSE frame for a fire-and-forget message (no reply_token): the loop must ignore it.
fn fire_and_forget_frame(from: &str, topic: &str, payload: &[u8]) -> String {
    let ph = hex::encode(payload);
    format!("data: {{\"from\":\"{from}\",\"topic\":\"{topic}\",\"payload_hex\":\"{ph}\",\"received_at\":1}}\n\n")
}

/// A handler that records every request it sees and echoes a transformed reply. Cheap to clone.
#[derive(Clone, Default)]
struct RecordingHandler {
    seen: Arc<Mutex<Vec<Request>>>,
}
impl Handler for RecordingHandler {
    async fn handle(&self, req: Request) -> Vec<u8> {
        let reply = [b"reply:".as_slice(), &req.payload].concat();
        self.seen.lock().unwrap().push(req);
        reply
    }
}

/// Collects the `{token, payload}` pairs posted to `POST /mesh/reply`, and lets a test await until a
/// target count has arrived (so `shutdown` can fire deterministically without sleeping arbitrarily).
#[derive(Clone, Default)]
struct ReplySink {
    replies: Arc<Mutex<Vec<(u64, Vec<u8>)>>>,
}
impl ReplySink {
    fn record(&self, req: &CapturedRequest) -> Reply {
        let body = req.body_json();
        let token = body["token"].as_u64().unwrap_or_default();
        let payload = hex::decode(body["payload_hex"].as_str().unwrap_or_default()).unwrap_or_default();
        self.replies.lock().unwrap().push((token, payload));
        Reply::json(200, "{}")
    }
    fn len(&self) -> usize {
        self.replies.lock().unwrap().len()
    }
    async fn wait_for(&self, n: usize) {
        for _ in 0..400 {
            if self.len() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {n} replies; got {}", self.len());
    }
    fn snapshot(&self) -> Vec<(u64, Vec<u8>)> {
        self.replies.lock().unwrap().clone()
    }
}

/// Build a mock node that serves `sse_body` on the message stream, succeeds on subscribe, and feeds
/// every `/mesh/reply` into `sink`. Returns the started server.
async fn node_serving(sse_body: String, sink: ReplySink) -> MockServer {
    let s = sink.clone();
    MockServer::new()
        .route("POST", "/mesh/subscribe", Reply::json(200, "{}"))
        .route("GET", "/mesh/messages/stream", Reply::sse(sse_body, None))
        .route_fn("POST", "/mesh/reply", move |req| s.record(req))
        .start()
        .await
}

/// Drive `serve` against `server` for `handler`, shutting down once `sink` has `expect` replies.
///
/// `serve` is awaited directly on this task (not `tokio::spawn`ed): its inbound stream and the
/// `shutdown` future are polled concurrently inside the loop, and the handler's future is not
/// required to be `Send` — matching how a real app runs `serve` on its main task. A hard timeout
/// guards against a regression that never replies hanging CI.
async fn run_until_replies<H: Handler>(
    server: &MockServer,
    topics: Vec<String>,
    handler: &H,
    sink: ReplySink,
    expect: usize,
) {
    let ce = client_for(server);
    let topic_refs: Vec<&str> = topics.iter().map(|t| t.as_str()).collect();
    let s = sink.clone();
    let fut = serve(&ce, &topic_refs, handler, async move {
        s.wait_for(expect).await;
    });
    let res = tokio::time::timeout(Duration::from_secs(15), fut)
        .await
        .expect("serve loop did not finish within 15s (possible hang)");
    res.expect("serve returned an error");
}

// ---------------------------------------------------------------------------
// Handler dispatch + reply routing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatches_requests_and_replies_with_matching_token() {
    let body = format!(
        "{}{}",
        request_frame("alice", "app/rpc", b"one", 11),
        request_frame("bob", "app/rpc", b"two", 22),
    );
    let sink = ReplySink::default();
    let server = node_serving(body, sink.clone()).await;
    let handler = RecordingHandler::default();

    run_until_replies(&server, vec!["app/rpc".into()], &handler, sink.clone(), 2).await;

    // Both requests reached the handler with their authenticated sender + payload.
    let seen = handler.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].from, "alice");
    assert_eq!(seen[0].topic, "app/rpc");
    assert_eq!(seen[0].payload, b"one");
    assert_eq!(seen[1].from, "bob");

    // Each reply went back on the right token with the handler's transformed payload.
    let replies = sink.snapshot();
    assert!(replies.contains(&(11u64, b"reply:one".to_vec())));
    assert!(replies.contains(&(22u64, b"reply:two".to_vec())));
}

#[tokio::test]
async fn subscribes_to_every_served_topic_before_serving() {
    let body = request_frame("alice", "a/rpc", b"x", 1);
    let sink = ReplySink::default();
    let server = MockServer::new()
        .route("POST", "/mesh/subscribe", Reply::json(200, "{}"))
        .route("GET", "/mesh/messages/stream", Reply::sse(body, None))
        .route_fn("POST", "/mesh/reply", {
            let s = sink.clone();
            move |req| s.record(req)
        })
        .start()
        .await;

    run_until_replies(&server, vec!["a/rpc".into(), "b/rpc".into()], &RecordingHandler::default(), sink, 1)
        .await;

    // Both topics were subscribed (POST /mesh/subscribe carries the topic in the body).
    let subscribed: Vec<String> = server
        .requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path_only() == "/mesh/subscribe")
        .map(|r| r.body_json()["topic"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(subscribed.contains(&"a/rpc".to_string()), "got {subscribed:?}");
    assert!(subscribed.contains(&"b/rpc".to_string()), "got {subscribed:?}");
}

// ---------------------------------------------------------------------------
// Topic filtering + fire-and-forget handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ignores_messages_on_unserved_topics() {
    // A request on an *unserved* topic must be dropped (never handled, never replied to). We
    // interleave a served request so the loop has something to reply to and can then shut down.
    let body = format!(
        "{}{}",
        request_frame("eve", "other/rpc", b"nope", 99),
        request_frame("alice", "app/rpc", b"yes", 1),
    );
    let sink = ReplySink::default();
    let server = node_serving(body, sink.clone()).await;
    let handler = RecordingHandler::default();

    run_until_replies(&server, vec!["app/rpc".into()], &handler, sink.clone(), 1).await;

    let seen = handler.seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "only the served-topic request should be handled");
    assert_eq!(seen[0].topic, "app/rpc");
    // The unserved token 99 must never have been replied to.
    assert!(sink.snapshot().iter().all(|(t, _)| *t != 99));
}

#[tokio::test]
async fn ignores_fire_and_forget_messages_without_a_reply_token() {
    let body = format!(
        "{}{}",
        fire_and_forget_frame("pub", "app/rpc", b"broadcast"),
        request_frame("alice", "app/rpc", b"req", 7),
    );
    let sink = ReplySink::default();
    let server = node_serving(body, sink.clone()).await;
    let handler = RecordingHandler::default();

    run_until_replies(&server, vec!["app/rpc".into()], &handler, sink.clone(), 1).await;

    // Only the request (token 7) was handled; the fire-and-forget message was ignored.
    let seen = handler.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].payload, b"req");
    let replies = sink.snapshot();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].0, 7);
}

// ---------------------------------------------------------------------------
// De-duplication of redelivered requests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deduplicates_a_request_redelivered_within_one_stream() {
    // The same reply_token (5) appears three times (e.g. redelivered after a node-side retry). The
    // handler must run once and the SDK must reply once.
    let dup = request_frame("alice", "app/rpc", b"dup", 5);
    let body = format!("{dup}{dup}{dup}{}", request_frame("alice", "app/rpc", b"other", 6));
    let sink = ReplySink::default();
    let server = node_serving(body, sink.clone()).await;
    let handler = RecordingHandler::default();

    run_until_replies(&server, vec!["app/rpc".into()], &handler, sink.clone(), 2).await;

    let seen = handler.seen.lock().unwrap();
    let token5_count = seen.iter().filter(|r| r.payload == b"dup").count();
    assert_eq!(token5_count, 1, "duplicate reply_token must be handled exactly once");
    let token5_replies = sink.snapshot().into_iter().filter(|(t, _)| *t == 5).count();
    assert_eq!(token5_replies, 1, "duplicate reply_token must be replied to exactly once");
}

#[tokio::test]
async fn deduplicates_across_a_reconnect() {
    // The mock closes the stream after the body, so `serve` reconnects and re-reads the SAME body
    // (token 1) every time. Across reconnects the request must be answered at most once: we wait for
    // the (only) reply, shut down, and assert exactly one reply was posted despite multiple reads.
    let body = request_frame("alice", "app/rpc", b"redelivered", 1);
    let sink = ReplySink::default();
    let server = node_serving(body, sink.clone()).await;
    let handler = RecordingHandler::default();

    run_until_replies(&server, vec!["app/rpc".into()], &handler, sink.clone(), 1).await;

    // Give the loop a beat to (incorrectly) re-reply if dedup across reconnect were broken.
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(sink.len(), 1, "a redelivered request must be answered once across reconnects");
    assert_eq!(handler.seen.lock().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Reconnect after stream errors / open failures
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reconnects_after_the_stream_ends() {
    // First open of the message stream returns an empty body (immediate end). The loop must reconnect
    // and, on a later open, find the real request. We model "later" by always serving the request:
    // the FIRST connection still yields it, but the point is the loop survives stream end + reopen.
    // To prove reconnect specifically, the first response is empty and subsequent ones carry the
    // request — implemented with a counter in route_fn.
    let opens = Arc::new(AtomicUsize::new(0));
    let sink = ReplySink::default();
    let body = request_frame("alice", "app/rpc", b"after-reconnect", 3);
    let o = opens.clone();
    let server = MockServer::new()
        .route("POST", "/mesh/subscribe", Reply::json(200, "{}"))
        .route_fn("GET", "/mesh/messages/stream", move |_req| {
            let n = o.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // First open: empty stream, ends immediately -> forces a reconnect.
                Reply::sse(String::new(), None)
            } else {
                Reply::sse(body.clone(), None)
            }
        })
        .route_fn("POST", "/mesh/reply", {
            let s = sink.clone();
            move |req| s.record(req)
        })
        .start()
        .await;

    run_until_replies(&server, vec!["app/rpc".into()], &RecordingHandler::default(), sink.clone(), 1)
        .await;

    assert!(opens.load(Ordering::SeqCst) >= 2, "loop should have reopened the stream after it ended");
    assert_eq!(sink.snapshot()[0].0, 3);
}

#[tokio::test]
async fn reconnects_after_stream_open_failure_with_backoff() {
    // The message stream endpoint fails (500) on the first open, then succeeds. `serve` must back off
    // and retry rather than returning an error, eventually serving the request.
    let opens = Arc::new(AtomicUsize::new(0));
    let sink = ReplySink::default();
    let body = request_frame("alice", "app/rpc", b"recovered", 8);
    let o = opens.clone();
    let server = MockServer::new()
        .route("POST", "/mesh/subscribe", Reply::json(200, "{}"))
        .route_fn("GET", "/mesh/messages/stream", move |_req| {
            let n = o.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                Reply::text(500, "stream down") // open fails -> backoff path
            } else {
                Reply::sse(body.clone(), None)
            }
        })
        .route_fn("POST", "/mesh/reply", {
            let s = sink.clone();
            move |req| s.record(req)
        })
        .start()
        .await;

    run_until_replies(&server, vec!["app/rpc".into()], &RecordingHandler::default(), sink.clone(), 1)
        .await;

    assert!(opens.load(Ordering::SeqCst) >= 2, "loop should retry the stream open after a failure");
    assert_eq!(sink.snapshot()[0].0, 8);
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shutdown_returns_before_any_request_arrives() {
    // An already-resolved shutdown future must end the loop promptly with Ok(()), even though the
    // stream would keep delivering. (We serve an empty stream so nothing is handled.)
    let server = MockServer::new()
        .route("POST", "/mesh/subscribe", Reply::json(200, "{}"))
        .route("GET", "/mesh/messages/stream", Reply::sse(String::new(), None))
        .start()
        .await;
    let ce = client_for(&server);
    let handler = RecordingHandler::default();

    let res = tokio::time::timeout(
        Duration::from_secs(5),
        serve(&ce, &["app/rpc"], &handler, async {}),
    )
    .await
    .expect("immediate shutdown should not hang");
    assert!(res.is_ok());
    assert!(handler.seen.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// serve_where: accept-predicate topic families
// ---------------------------------------------------------------------------

#[tokio::test]
async fn serve_where_accepts_a_topic_family_by_predicate() {
    // A service handling any `app/` prefix: a request on `app/sub/deep` is accepted even though it
    // was never an explicit served topic, while `other/x` is rejected.
    let body = format!(
        "{}{}",
        request_frame("eve", "other/x", b"reject", 50),
        request_frame("alice", "app/sub/deep", b"accept", 51),
    );
    let sink = ReplySink::default();
    let s = sink.clone();
    let server = MockServer::new()
        .route("POST", "/mesh/subscribe", Reply::json(200, "{}"))
        .route("GET", "/mesh/messages/stream", Reply::sse(body, None))
        .route_fn("POST", "/mesh/reply", move |req| s.record(req))
        .start()
        .await;
    let ce = client_for(&server);
    let handler = RecordingHandler::default();
    let sink2 = sink.clone();

    // Awaited on this task (no spawn): the handler future need not be `Send`.
    let fut = serve_where(&ce, &[], |t| t.starts_with("app/"), &handler, async move {
        sink2.wait_for(1).await;
    });
    let res = tokio::time::timeout(Duration::from_secs(15), fut).await;
    res.expect("serve_where hung").expect("serve_where errored");

    let seen = handler.seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "only the app/ prefixed request should be accepted");
    assert_eq!(seen[0].topic, "app/sub/deep");
    assert!(sink.snapshot().iter().all(|(t, _)| *t != 50), "rejected topic must not be replied to");
}
