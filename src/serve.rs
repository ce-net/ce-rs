//! Reusable mesh-app serving — the shared loop for building a **real mesh service**.
//!
//! A mesh app answers requests over the CE mesh (libp2p `request`/`reply` on `/ce/rpc/1`), reached
//! by NodeId with relay/NAT traversal — never over a stored ip:port or a side HTTP channel. This
//! module is the one correct implementation of that serve loop: open the node's inbound message
//! stream, subscribe to the request topics, and answer each request via a [`Handler`], reconnecting
//! with backoff and de-duplicating redelivered requests. It codifies the loop that `ce-fn` and
//! `rdev` previously hand-rolled so every mesh app shares it.
//!
//! The node keeps `/mesh/subscribe` state in memory only, so a node restart silently wipes it.
//! The loop therefore re-subscribes every served topic on EVERY stream (re)connect — never only
//! once at startup — so a service survives its node restarting instead of going permanently deaf.
//! Ordering per connection is stream-open first, then subscribe, then process: the node confirms
//! `/mesh/subscribe` synchronously, so once it returns, every later message flows into the
//! already-open stream (no subscribed-but-unstreamed window).
//!
//! ## Authorization is the app's job
//!
//! The handler receives the **authenticated** sender NodeId (the local node verified it) plus the
//! request payload. The app enforces its own policy — typically a `ce-cap` capability chain, since
//! abilities are app-defined opaque strings — before acting. This module deliberately does not
//! depend on `ce-cap`: it is pure mesh transport, and authorization is layered on top.
//!
//! ## Example
//!
//! ```no_run
//! # async fn run(ce: ce_rs::CeClient) -> anyhow::Result<()> {
//! use ce_rs::serve::{serve, Handler, Request};
//!
//! struct Echo;
//! impl Handler for Echo {
//!     async fn handle(&self, req: Request) -> Vec<u8> {
//!         // authorize `req.from` here (e.g. verify a ce-cap chain) before acting
//!         req.payload // echo it straight back
//!     }
//! }
//!
//! // Serve until the shutdown future resolves (here: until the process is stopped).
//! serve(&ce, &["my-app/rpc"], &Echo, std::future::pending::<()>()).await
//! # }
//! ```

use crate::{AppMessage, CeClient};
use anyhow::Result;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// An incoming mesh request delivered to a [`Handler`].
#[derive(Debug, Clone)]
pub struct Request {
    /// Authenticated sender NodeId (hex) — the local node verified the sender's signature.
    pub from: String,
    /// The topic the request arrived on (one of the served topics).
    pub topic: String,
    /// The request payload bytes.
    pub payload: Vec<u8>,
}

/// A mesh request handler: given an authenticated [`Request`], produce the reply bytes. Decoding and
/// authorization (e.g. `ce-cap`) are the handler's responsibility. A handler should always return a
/// reply (even an encoded error), so the requester's [`CeClient::request`] never blocks to timeout.
///
/// The returned future is required to be `Send` so [`serve`] / [`serve_where`] can be driven from a
/// spawned task on a multi-threaded runtime (`tokio::spawn`), not only on the current thread. An
/// `async fn handle(&self, req: Request) -> Vec<u8>` impl satisfies this as long as its body is
/// `Send` (it holds no `!Send` value across an `.await`).
pub trait Handler: Send + Sync {
    /// Handle one request and return the reply payload.
    fn handle(&self, req: Request) -> impl std::future::Future<Output = Vec<u8>> + Send;
}

/// Serve an explicit set of `topics` until `shutdown` resolves: answer every incoming request from
/// the node's inbound message stream via `handler`, replying over the mesh. Each topic is
/// (re-)subscribed on every stream (re)connect, so the service survives its node restarting.
///
/// Reconnects to the message stream with exponential backoff (capped at 10s), rides out transient
/// subscribe failures the same way, and de-duplicates by reply token so a request redelivered after
/// a reconnect is answered at most once. Non-request messages (no `reply_token`) and messages on
/// other topics are ignored.
pub async fn serve<H: Handler>(
    ce: &CeClient,
    topics: &[&str],
    handler: &H,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    let set: HashSet<String> = topics.iter().map(|t| t.to_string()).collect();
    // An exact topic list, so the node can filter at the source: this app is never woken for
    // another app's traffic. `serve_where` cannot do this — its predicate may accept topics
    // nobody enumerated (families, dynamic sub-topics) — so it keeps the full stream.
    let declared: Vec<String> = set.iter().cloned().collect();
    serve_where_signal(ce, topics, move |t| set.contains(t), handler, shutdown, None, &declared).await
}

/// The general serve loop: answer every inbound request whose topic satisfies `accept`. Use this
/// when topics are a family rather than a fixed set — e.g. a service that handles `app/rpc/*` or any
/// `app/` prefix with dynamic sub-topics. `subscribe` lists the pub/sub topics to subscribe to
/// (often empty for purely directed request/reply services, where requests arrive regardless).
///
/// Reconnects with capped exponential backoff and de-duplicates by reply token. Authorization stays
/// the handler's job.
pub async fn serve_where<H, F>(
    ce: &CeClient,
    subscribe: &[&str],
    accept: F,
    handler: &H,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()>
where
    H: Handler,
    F: Fn(&str) -> bool,
{
    serve_where_signal(ce, subscribe, accept, handler, shutdown, None, &[]).await
}

/// [`serve_where`] plus an optional readiness flag, set to `true` the first time a stream is open
/// AND every subscribed topic has been CONFIRMED by the node (`/mesh/subscribe` returns
/// synchronously — that is the confirmation). `capability::provide` gates its first DHT advertise
/// on this, so a caller can never locate an instance whose node would still drop its requests.
pub(crate) async fn serve_where_signal<H, F>(
    ce: &CeClient,
    subscribe: &[&str],
    accept: F,
    handler: &H,
    shutdown: impl std::future::Future<Output = ()>,
    subscribed: Option<Arc<AtomicBool>>,
    /// Topics to ask the node to restrict this stream to. EMPTY = everything, which is the only
    /// safe default when `accept` may match topics that were never enumerated.
    stream_topics: &[String],
) -> Result<()>
where
    H: Handler,
    F: Fn(&str) -> bool,
{
    use futures_util::StreamExt as _;

    let mut seen: HashSet<u64> = HashSet::new();
    let mut backoff_ms = 250u64;
    tokio::pin!(shutdown);

    loop {
        // Open the inbound stream FIRST. Subscribing after guarantees no message can arrive
        // subscribed-but-unstreamed: the node confirms /mesh/subscribe synchronously, so every
        // message routed after that confirmation lands in this already-open stream.
        let stream = match ce.messages_stream_for(stream_topics).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "serve: messages_stream open failed; backing off");
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                }
                backoff_ms = (backoff_ms * 2).min(10_000);
                continue;
            }
        };
        tokio::pin!(stream);

        // (Re-)subscribe every topic on EVERY (re)connect: the node holds subscribe state in
        // memory only, so a node restart wipes it — a loop that only subscribed at startup would
        // reattach its stream to a node that no longer routes the topics and go permanently deaf.
        // A subscribe failure is transient node trouble (e.g. still starting up): drop this
        // connection and retry the whole connect with backoff rather than erroring out.
        let mut sub_failed = false;
        for t in subscribe {
            if let Err(e) = ce.subscribe(t).await {
                tracing::warn!(topic = %t, error = %e, "serve: subscribe failed; reconnecting");
                sub_failed = true;
                break;
            }
        }
        if sub_failed {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
            }
            backoff_ms = (backoff_ms * 2).min(10_000);
            continue;
        }
        if let Some(flag) = &subscribed {
            flag.store(true, Ordering::Release);
        }
        backoff_ms = 250;

        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                item = stream.next() => match item {
                    Some(Ok(m)) => answer_one(ce, handler, &accept, &mut seen, m).await,
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "serve: stream error; reconnecting");
                        break;
                    }
                    None => break,
                }
            }
        }
    }
}

/// Decode one inbound message and, if it is a request on an accepted topic we have not answered yet,
/// run the handler and reply over the mesh.
async fn answer_one<H, F>(
    ce: &CeClient,
    handler: &H,
    accept: &F,
    seen: &mut HashSet<u64>,
    m: AppMessage,
) where
    H: Handler,
    F: Fn(&str) -> bool,
{
    if !accept(&m.topic) {
        return;
    }
    let Some(token) = m.reply_token else {
        return; // fire-and-forget message, not a request: nothing to reply to
    };
    if !seen.insert(token) {
        return; // already answered this request
    }
    // Bound the de-dup set so a long-lived server never grows it without limit.
    if seen.len() > 100_000 {
        seen.clear();
        seen.insert(token);
    }

    let payload = match m.payload() {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "serve: dropping request with undecodable payload");
            return;
        }
    };
    let reply = handler.handle(Request { from: m.from, topic: m.topic, payload }).await;
    if let Err(e) = ce.reply(token, &reply).await {
        tracing::warn!(error = %e, "serve: reply failed");
    }
}
