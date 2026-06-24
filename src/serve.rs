//! Reusable mesh-app serving — the shared loop for building a **real mesh service**.
//!
//! A mesh app answers requests over the CE mesh (libp2p `request`/`reply` on `/ce/rpc/1`), reached
//! by NodeId with relay/NAT traversal — never over a stored ip:port or a side HTTP channel. This
//! module is the one correct implementation of that serve loop: subscribe to the request topics,
//! read the node's inbound message stream, and answer each request via a [`Handler`], reconnecting
//! with backoff and de-duplicating redelivered requests. It codifies the loop that `ce-fn` and
//! `rdev` previously hand-rolled so every mesh app shares it.
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
//! // Serve forever; ctrl_c shuts it down cleanly.
//! serve(&ce, &["my-app/rpc"], &Echo, async {
//!     let _ = tokio::signal::ctrl_c().await;
//! }).await
//! # }
//! ```

use crate::{AppMessage, CeClient};
use anyhow::Result;
use std::collections::HashSet;
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
#[allow(async_fn_in_trait)]
pub trait Handler: Send + Sync {
    /// Handle one request and return the reply payload.
    async fn handle(&self, req: Request) -> Vec<u8>;
}

/// Serve `topics` until `shutdown` resolves: subscribe to each, then answer every incoming request
/// from the node's inbound message stream via `handler`, replying over the mesh.
///
/// Reconnects to the message stream with exponential backoff (capped at 10s), and de-duplicates by
/// reply token so a request redelivered after a reconnect is answered at most once. Non-request
/// messages (no `reply_token`) and messages on other topics are ignored.
pub async fn serve<H: Handler>(
    ce: &CeClient,
    topics: &[&str],
    handler: &H,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    use futures_util::StreamExt as _;

    let topic_set: HashSet<String> = topics.iter().map(|t| t.to_string()).collect();
    for t in topics {
        ce.subscribe(t).await?;
    }

    let mut seen: HashSet<u64> = HashSet::new();
    let mut backoff_ms = 250u64;
    tokio::pin!(shutdown);

    loop {
        let stream = match ce.messages_stream().await {
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
        backoff_ms = 250;
        tokio::pin!(stream);

        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                item = stream.next() => match item {
                    Some(Ok(m)) => answer_one(ce, handler, &topic_set, &mut seen, m).await,
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

/// Decode one inbound message and, if it is a request on a served topic we have not answered yet,
/// run the handler and reply over the mesh.
async fn answer_one<H: Handler>(
    ce: &CeClient,
    handler: &H,
    topics: &HashSet<String>,
    seen: &mut HashSet<u64>,
    m: AppMessage,
) {
    if !topics.contains(&m.topic) {
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
