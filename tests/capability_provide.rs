//! Startup-race coverage for `ce_rs::capability::provide`: a capability must not become
//! discoverable before its node has CONFIRMED the topic subscription.
//!
//! `provide` runs two loops: the serve loop (answers requests) and the register loop (advertises
//! the service on the DHT so callers can locate it). If the first advertise wins the race against
//! the subscribe, a caller can locate the instance and fire a request into a node that is not yet
//! subscribed — the request is dropped and the caller 504s ("peer app did not reply in time").
//! The node answers `POST /mesh/subscribe` synchronously (that IS the confirmation), so the
//! register loop must hold the first `POST /discovery/advertise` until a subscribe has succeeded.
//!
//! Requires the `capability` feature; run with `cargo test --features capability`.
#![cfg(feature = "capability")]

mod common;

use ce_rs::capability::{provide, Caller, Capability};
use ce_rs::CeClient;
use common::{MockServer, Reply};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Req {
    n: u32,
}
#[derive(Serialize, Deserialize)]
struct Resp {
    n: u32,
}
struct Gate;
impl Capability for Gate {
    const NAME: &'static str = "test.provide.gate";
    type Req = Req;
    type Resp = Resp;
}

#[tokio::test]
async fn advertise_waits_for_a_confirmed_subscribe() {
    // The node is "still coming up": the first two /mesh/subscribe attempts fail, then the node
    // accepts. The FIRST /discovery/advertise must come after that first successful subscribe —
    // never while subscription is unconfirmed.
    let sub_calls = Arc::new(AtomicUsize::new(0));
    let sc = sub_calls.clone();
    let server = MockServer::new()
        .route_fn("POST", "/mesh/subscribe", move |_req| {
            if sc.fetch_add(1, Ordering::SeqCst) < 2 {
                Reply::text(500, "node starting up")
            } else {
                Reply::json(200, "{\"status\":\"subscribed\"}")
            }
        })
        .route("POST", "/discovery/advertise", Reply::json(200, "{}"))
        .route("GET", "/mesh/messages/stream", Reply::sse(String::new(), None))
        .start()
        .await;
    let ce = CeClient::with_token(server.base_url(), Some("t".into()));

    // Shut down once an advertise has been observed (bounded so a regression can't hang CI).
    let srv = server.clone();
    let shutdown = async move {
        for _ in 0..1_000 {
            if srv.requests().iter().any(|r| r.path_only() == "/discovery/advertise") {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };

    let res = tokio::time::timeout(
        Duration::from_secs(20),
        provide::<Gate, _>(&ce, |req: Req, _c: Caller| async move { Ok(Resp { n: req.n }) }, shutdown),
    )
    .await
    .expect("provide hung");
    assert!(res.is_ok(), "provide must ride out transient subscribe failures: {:?}", res.err());

    // Ordering: the first advertise must come after the first SUCCESSFUL subscribe. The first two
    // subscribe attempts fail by construction, so the earliest successful one is the third
    // captured /mesh/subscribe request.
    let reqs = server.requests();
    let sub_idx: Vec<usize> = reqs
        .iter()
        .enumerate()
        .filter(|(_, r)| r.method == "POST" && r.path_only() == "/mesh/subscribe")
        .map(|(i, _)| i)
        .collect();
    let first_ad = reqs
        .iter()
        .position(|r| r.method == "POST" && r.path_only() == "/discovery/advertise")
        .expect("an advertise must eventually happen");
    assert!(
        sub_idx.len() >= 3,
        "a subscribe must have SUCCEEDED before advertising (needs >=3 attempts here; got {}); \
         advertise at capture index {first_ad}",
        sub_idx.len()
    );
    assert!(
        sub_idx[2] < first_ad,
        "advertised before the first successful subscribe: success at {}, advertise at {first_ad}",
        sub_idx[2]
    );
}
