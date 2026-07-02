//! `ce_rs::adapt` — the typed adapter runtime (membrane Keystone 2). An adapter is an ordinary
//! app that the HOST interposes between another app and the node (or the next adapter): it sees
//! every call the app makes, can meter/transform/deny it, and forwards using its own authority.
//! The intercepted app never references the adapter — its `CeClient` API is byte-identical
//! whether an adapter is in the middle or not. This is the mesh analogue of an embedded HAL /
//! network-driver trait: the app codes to an abstract node channel, the host slots the driver.
//!
//! Implement [`Adapter`] and hand it to [`run_adapter`]; the runtime registers with the node's
//! lane socket and services each interposed flow the node hands it. Runtime-agnostic: it runs on
//! std threads (the ring's producer/consumer), so any executor — or none — works.
//!
//! Scope note: v1 exposes `Forward`/`Deny`. `Forward(bytes)` may transform the request; `Deny`
//! tears the flow down (the app's call fails / falls back), which needs no protocol knowledge.
//! Answering a call directly from the adapter (a cached/synthesized reply) needs the adapter to
//! speak the app's protocol and is a later addition.

#![cfg(unix)]

use anyhow::Result;
use ce_lane::Endpoint;
use ce_lane::bind::{ServeFlow, register_adapter};
use std::path::Path;
use std::sync::Arc;

/// Context for one intercepted call: which app made it and its node-asserted identity.
#[derive(Debug, Clone)]
pub struct CallCtx {
    /// The app name the host resolved this adapter chain for.
    pub app: String,
    /// Node-asserted caller identity (raw NodeId bytes). The host node id for a token-bound
    /// operator app; the presenting cap's audience for a cap-bound app. The app cannot spoof it.
    pub caller: [u8; 32],
}

/// What the adapter decides for one intercepted request.
pub enum Action {
    /// Forward these bytes upstream (verbatim, or transformed). The upstream response is relayed
    /// back to the app unchanged.
    Forward(Vec<u8>),
    /// Refuse: tear the flow down. The app's in-flight call fails and (for a ce-rs client) falls
    /// back to HTTP or errors, exactly as if the node were unreachable.
    Deny(String),
}

/// A membrane adapter: interpose one request at a time. Called once per request frame the app
/// sends; must be cheap and non-blocking relative to the data path (metering, policy checks).
pub trait Adapter: Send + Sync {
    fn intercept(&self, ctx: &CallCtx, request: &[u8]) -> Action;
}

/// Register as adapter `name` at the node's `lane.sock` and service interposed flows forever.
/// Blocks; run it on its own thread. `token` is the node api token (local-operator trust).
pub fn run_adapter(
    socket_path: &Path,
    token: &str,
    name: &str,
    adapter: Arc<dyn Adapter>,
) -> Result<()> {
    let control = register_adapter(socket_path, token, name)?;
    loop {
        // Blocks until the node hands over the next flow to interpose.
        let (meta, down, up) = control.recv_flow()?;
        let adapter = adapter.clone();
        std::thread::Builder::new()
            .name("ce-adapt-flow".into())
            .spawn(move || serve_flow(meta, down, up, adapter))
            .ok();
    }
}

/// Service one interposed flow: relay app->node through `intercept`, and node->app verbatim,
/// until either side closes.
fn serve_flow(meta: ServeFlow, down: Endpoint, up: Endpoint, adapter: Arc<dyn Adapter>) {
    let down = Arc::new(down);
    let up = Arc::new(up);
    let ctx = CallCtx { app: meta.app, caller: meta.caller };

    // Upstream -> app: verbatim (responses, pushed events). Its own thread since recv blocks.
    let up_r = up.clone();
    let down_w = down.clone();
    let relay = std::thread::Builder::new()
        .name("ce-adapt-up".into())
        .spawn(move || {
            let mut buf = Vec::new();
            while up_r.recv_into(&mut buf, None).is_ok() {
                if down_w.send(&buf).is_err() {
                    break;
                }
            }
            down_w.close();
            up_r.close();
        });

    // App -> upstream: through the adapter's decision.
    let mut buf = Vec::new();
    loop {
        match down.recv_into(&mut buf, None) {
            Ok(()) => match adapter.intercept(&ctx, &buf) {
                Action::Forward(bytes) => {
                    if up.send(&bytes).is_err() {
                        break;
                    }
                }
                Action::Deny(_) => break,
            },
            Err(_) => break, // app gone / flow revoked
        }
    }
    down.close();
    up.close();
    if let Ok(h) = relay {
        let _ = h.join();
    }
}
