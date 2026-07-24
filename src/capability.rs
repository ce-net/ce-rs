//! # Typed capabilities — the mesh as one computer's standard library
//!
//! A **capability** is a named request/reply function exposed on the CE mesh. This module makes a
//! capability a *typed, shared contract*: you define it once as a [`Capability`] type, and both the
//! provider and every caller use that same type. The provider writes a plain async function
//! (`Req -> Result<Resp>`); a caller invokes it with [`call`] and gets a `Resp` back — with
//! discovery, transport, NAT traversal, serialization, and reply-error propagation handled for you.
//!
//! This is the "functions and SDKs" layer over the [`serve`](crate::serve) +
//! [`locate`](crate::locate) primitives: it turns an untyped request/reply service into a typed
//! capability that composes. Every capability-providing app you install adds a function to the
//! mesh's growing standard library, callable from anywhere as if it were local — the same on a
//! laptop, the relay, a browser, or an embedded board, because the transport is abstracted away.
//!
//! ## Authorization stays the provider's job
//!
//! A provider receives the **authenticated** [`Caller`] (the local node verified the sender's
//! signature). Sensitive capabilities verify a `ce-cap` chain against `caller.from` before acting;
//! this module deliberately does not depend on `ce-cap` — it is pure typed transport, and
//! authorization layers on top exactly as it does for [`serve`](crate::serve).
//!
//! ## Example
//!
//! ```no_run
//! use ce_rs::{CeClient, capability::{Capability, call, provide, Caller}};
//! use serde::{Serialize, Deserialize};
//!
//! // 1. Define the capability ONCE — the provider and every caller share this type.
//! #[derive(Serialize, Deserialize)] pub struct GreetReq { pub name: String }
//! #[derive(Serialize, Deserialize)] pub struct GreetResp { pub text: String }
//! pub struct Greet;
//! impl Capability for Greet {
//!     const NAME: &'static str = "demo.greet";
//!     type Req = GreetReq;
//!     type Resp = GreetResp;
//! }
//!
//! // 2. Provider — a plain async fn. Authorize `caller.from` first if the op is sensitive.
//! # async fn run_provider(ce: CeClient) -> anyhow::Result<()> {
//! provide::<Greet, _>(&ce, |req: GreetReq, _caller: Caller| async move {
//!     Ok(GreetResp { text: format!("hello {}", req.name) })
//! }, std::future::pending::<()>()).await // serve until the process is stopped
//! # }
//!
//! // 3. Caller — typed, transport-agnostic, one line.
//! # async fn run_caller(ce: CeClient) -> anyhow::Result<()> {
//! let resp = call::<Greet>(&ce, &GreetReq { name: "world".into() }).await?;
//! println!("{}", resp.text);
//! # Ok(()) }
//! ```

use crate::locate::{self, LocateOpts};
use crate::serve;
use crate::CeClient;
use anyhow::{anyhow, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::marker::PhantomData;
use std::time::Duration;

/// Default per-call timeout (ms) for [`call`] — generous enough for a cross-NAT relay hop.
pub const DEFAULT_TIMEOUT_MS: u64 = 15_000;
/// Default re-advertise interval for a provider's discoverability heartbeat.
pub const DEFAULT_ADVERTISE_SECS: u64 = 30;

/// A typed mesh capability: a named request/reply function. Define it once; the provider and every
/// caller share the same type, so a mesh call is as type-safe as a local function call.
pub trait Capability: 'static {
    /// The service name — used for discovery ([`locate`](crate::locate)) and advertisement.
    /// Convention: dotted, lowercase, stable (e.g. `camera.frame`, `demo.greet`). This is the
    /// capability's identity in the mesh's standard library, so keep it stable across versions.
    const NAME: &'static str;
    /// The request type callers send.
    type Req: Serialize + DeserializeOwned + Send + 'static;
    /// The response type the provider returns.
    type Resp: Serialize + DeserializeOwned + Send + 'static;
}

/// The mesh topic a capability's request/reply travels on. Derived from the name so a provider and
/// caller always agree without a second shared constant.
pub fn topic<C: Capability>() -> String {
    format!("cap/{}", C::NAME)
}

/// Who is calling — the authenticated sender NodeId (hex), verified by the local node. A provider
/// authorizes against this (e.g. verifying a `ce-cap` chain) before acting on a sensitive op.
#[derive(Debug, Clone)]
pub struct Caller {
    /// Authenticated caller NodeId (hex).
    pub from: String,
}

/// The wire envelope. A provider ALWAYS replies with a decodable result, so a caller's request
/// never blocks to timeout on an application error — a provider error travels back as `Err(msg)`.
/// Internal to the capability contract; callers see a typed `Result<Resp>` from [`call`].
#[derive(Serialize, Deserialize)]
enum Wire<T> {
    Ok(T),
    Err(String),
}

// ---- Caller side ----------------------------------------------------------------------------

/// Call a capability with default options (best single live instance, [`DEFAULT_TIMEOUT_MS`]).
///
/// Locates a live provider of `C`, sends the typed request over the mesh, and returns the typed
/// response. A provider-side error surfaces here as an `Err`. Returns an error if no live provider
/// is found.
pub async fn call<C: Capability>(ce: &CeClient, req: &C::Req) -> Result<C::Resp> {
    call_with::<C>(ce, req, &LocateOpts::default(), DEFAULT_TIMEOUT_MS).await
}

/// Call a capability, controlling instance selection ([`LocateOpts`]) and timeout. Fails over to
/// another live instance on transport error before giving up.
pub async fn call_with<C: Capability>(
    ce: &CeClient,
    req: &C::Req,
    opts: &LocateOpts,
    timeout_ms: u64,
) -> Result<C::Resp> {
    let payload =
        serde_json::to_vec(req).map_err(|e| anyhow!("encoding request for '{}': {e}", C::NAME))?;
    let reply = locate::call(ce, C::NAME, &topic::<C>(), &payload, opts, timeout_ms).await?;
    decode_reply::<C>(&reply)
}

/// Decode a wire reply into the typed response, mapping a provider `Err` back to an `anyhow` error.
fn decode_reply<C: Capability>(bytes: &[u8]) -> Result<C::Resp> {
    match serde_json::from_slice::<Wire<C::Resp>>(bytes)
        .map_err(|e| anyhow!("decoding reply from '{}': {e}", C::NAME))?
    {
        Wire::Ok(resp) => Ok(resp),
        Wire::Err(msg) => Err(anyhow!("capability '{}' returned error: {msg}", C::NAME)),
    }
}

// ---- Provider side --------------------------------------------------------------------------

/// A capability provider: given a typed request and the authenticated [`Caller`], produce a typed
/// response (or an error, propagated to the caller). A plain closure
/// `Fn(C::Req, Caller) -> impl Future<Output = Result<C::Resp>>` implements this via the blanket
/// impl below — so most providers are just an async closure passed to [`provide`].
pub trait Provider<C: Capability>: Send + Sync {
    /// Handle one typed request.
    fn handle(
        &self,
        req: C::Req,
        caller: Caller,
    ) -> impl std::future::Future<Output = Result<C::Resp>> + Send;
}

impl<C, F, Fut> Provider<C> for F
where
    C: Capability,
    F: Fn(C::Req, Caller) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<C::Resp>> + Send,
{
    fn handle(
        &self,
        req: C::Req,
        caller: Caller,
    ) -> impl std::future::Future<Output = Result<C::Resp>> + Send {
        (self)(req, caller)
    }
}

/// Adapts a typed [`Provider`] into the untyped [`serve::Handler`]: decode the request, run the
/// provider, and encode the [`Wire`] reply. It never panics and always returns a decodable reply
/// (bad request bytes or a provider error both come back as `Wire::Err`).
struct CapHandler<C: Capability, P: Provider<C>> {
    provider: P,
    _pd: PhantomData<fn() -> C>,
}

impl<C: Capability, P: Provider<C>> serve::Handler for CapHandler<C, P> {
    async fn handle(&self, req: serve::Request) -> Vec<u8> {
        let caller = Caller { from: req.from };
        let reply: Wire<C::Resp> = match serde_json::from_slice::<C::Req>(&req.payload) {
            Ok(typed) => match self.provider.handle(typed, caller).await {
                Ok(resp) => Wire::Ok(resp),
                Err(e) => Wire::Err(e.to_string()),
            },
            Err(e) => Wire::Err(format!("bad request: {e}")),
        };
        serde_json::to_vec(&reply).unwrap_or_else(|_| {
            serde_json::to_vec(&Wire::<()>::Err("reply encode failed".into())).unwrap_or_default()
        })
    }
}

/// Provide a capability on the mesh until `shutdown` resolves: serve typed requests via `provider`
/// AND keep this node discoverable (re-advertising the service on the DHT so callers can
/// [`locate`](crate::locate) it). One call makes your app a live instance of the capability that
/// any node can [`call`].
pub async fn provide<C, P>(
    ce: &CeClient,
    provider: P,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()>
where
    C: Capability,
    P: Provider<C>,
{
    provide_with::<C, P>(ce, provider, Duration::from_secs(DEFAULT_ADVERTISE_SECS), shutdown).await
}

/// Like [`provide`], with a custom re-advertise `interval` (the discoverability heartbeat cadence).
pub async fn provide_with<C, P>(
    ce: &CeClient,
    provider: P,
    advertise_interval: Duration,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()>
where
    C: Capability,
    P: Provider<C>,
{
    use futures_util::future::FutureExt as _;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    // Fan the single shutdown into both loops so they stop together, without needing a tokio
    // runtime handle (`tokio::spawn`) — keeps the SDK's tokio feature footprint to time+macros.
    let shutdown = shutdown.shared();

    // Keep this node discoverable as a live instance of the capability, concurrently with serving —
    // but hold the FIRST advertise until the serve loop has a CONFIRMED topic subscription.
    // Advertising earlier would let a caller locate this instance and fire a request into a node
    // that is not yet routing the topic; the node drops it and the caller times out. The flag is an
    // AtomicBool polled on the existing tokio timer (no tokio `sync` feature needed).
    let ready = Arc::new(AtomicBool::new(false));
    let reg_ce = ce.clone();
    let name = C::NAME.to_string();
    let reg_shutdown = shutdown.clone();
    let reg_ready = ready.clone();
    let register = async move {
        while !reg_ready.load(Ordering::Acquire) {
            tokio::select! {
                () = reg_shutdown.clone() => return, // shut down before ever becoming ready
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }
        let _ = locate::register(&reg_ce, &name, advertise_interval, reg_shutdown).await;
    };

    let handler = CapHandler::<C, P> { provider, _pd: PhantomData };
    let topic = topic::<C>();
    let topics = [topic.as_str()];
    let set: std::collections::HashSet<String> = topics.iter().map(|t| t.to_string()).collect();
    let serve = serve::serve_where_signal(
        ce,
        &topics,
        move |t| set.contains(t),
        &handler,
        shutdown,
        Some(ready),
    );

    // Run both to completion; each returns when the shared shutdown fires.
    let (serve_res, ()) = futures_util::future::join(serve, register).await;
    serve_res
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Req {
        n: u32,
    }
    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Resp {
        s: String,
    }
    struct Cap;
    impl Capability for Cap {
        const NAME: &'static str = "test.cap";
        type Req = Req;
        type Resp = Resp;
    }

    #[test]
    fn topic_is_derived_from_name() {
        // Provider and caller must agree on the topic from the name alone.
        assert_eq!(topic::<Cap>(), "cap/test.cap");
    }

    #[test]
    fn ok_reply_round_trips_to_typed_response() {
        let bytes = serde_json::to_vec(&Wire::<Resp>::Ok(Resp { s: "hi".into() })).unwrap();
        assert_eq!(decode_reply::<Cap>(&bytes).unwrap(), Resp { s: "hi".into() });
    }

    #[test]
    fn err_reply_surfaces_as_caller_error() {
        let bytes = serde_json::to_vec(&Wire::<Resp>::Err("boom".into())).unwrap();
        let err = decode_reply::<Cap>(&bytes).unwrap_err();
        assert!(err.to_string().contains("boom"), "got: {err}");
    }

    #[test]
    fn garbage_reply_is_an_error_not_a_panic() {
        let err = decode_reply::<Cap>(b"not json at all").unwrap_err();
        assert!(err.to_string().contains("decoding reply"), "got: {err}");
    }

    #[tokio::test]
    async fn handler_maps_provider_error_to_err_envelope() {
        // A provider that always errors must come back as a decodable Err, never a timeout/panic.
        let h = CapHandler::<Cap, _> {
            provider: |_req: Req, _c: Caller| async { Err(anyhow!("nope")) },
            _pd: std::marker::PhantomData,
        };
        let payload = serde_json::to_vec(&Req { n: 1 }).unwrap();
        let reply = serve::Handler::handle(
            &h,
            serve::Request { from: "abc".into(), topic: topic::<Cap>(), payload },
        )
        .await;
        let err = decode_reply::<Cap>(&reply).unwrap_err();
        assert!(err.to_string().contains("nope"), "got: {err}");
    }

    #[tokio::test]
    async fn handler_maps_bad_request_bytes_to_err_envelope() {
        let h = CapHandler::<Cap, _> {
            provider: |req: Req, _c: Caller| async move { Ok(Resp { s: req.n.to_string() }) },
            _pd: std::marker::PhantomData,
        };
        let reply = serve::Handler::handle(
            &h,
            serve::Request { from: "x".into(), topic: "t".into(), payload: b"garbage".to_vec() },
        )
        .await;
        let err = decode_reply::<Cap>(&reply).unwrap_err();
        assert!(err.to_string().contains("bad request"), "got: {err}");
    }

    #[tokio::test]
    async fn handler_round_trips_a_good_request() {
        let h = CapHandler::<Cap, _> {
            provider: |req: Req, c: Caller| async move {
                assert_eq!(c.from, "peer1");
                Ok(Resp { s: format!("n={}", req.n) })
            },
            _pd: std::marker::PhantomData,
        };
        let payload = serde_json::to_vec(&Req { n: 7 }).unwrap();
        let reply = serve::Handler::handle(
            &h,
            serve::Request { from: "peer1".into(), topic: topic::<Cap>(), payload },
        )
        .await;
        assert_eq!(decode_reply::<Cap>(&reply).unwrap(), Resp { s: "n=7".into() });
    }
}
