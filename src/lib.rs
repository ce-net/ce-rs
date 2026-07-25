//! # ce-rs — Rust SDK for CE
//!
//! A typed, async client for talking to a **local CE node's HTTP API**. This is the SUBSTRATE
//! SDK: identity/status, the capacity atlas, content-addressed blobs/objects, mesh app-messaging
//! (publish/subscribe/send/request/reply/serve), signals, tunnels, and the transport hatch
//! (`get_json`/`post_json`/`post_void`/`post_bytes`/`sse_stream`) that adapter-ceapp SDKs ride.
//!
//! Domain surfaces live in their OWN ceapp SDKs, not here (the modularity law): MONEY —
//! `Amount`, balances, transfers, payment channels, the paid job market, paid data, chain
//! streams — is `ce_economy` (the economy ceapp). REPUTATION — `NodeHistory`/`history` — is
//! `ce_ratio` (the trust ceapp). See PLAN/ce-economy-sdk-extraction.md.
//!
//! ```no_run
//! use ce_rs::CeClient;
//! # async fn demo() -> anyhow::Result<()> {
//! let ce = CeClient::local(); // http://127.0.0.1:8844
//! let status = ce.status().await?;
//! println!("node {} peer {} economy {}", status.node_id, status.peer_id, status.economy_enabled());
//!
//! // Find a GPU host from the capacity atlas (placement is an adapter-SDK concern).
//! let hosts = ce.atlas().await?;
//! if let Some(h) = hosts.iter().find(|h| h.tags.iter().any(|t| t == "gpu")) {
//!     println!("gpu host available: {}", h.node_id);
//! }
//! # Ok(()) }
//! ```

// `Amount`/`CREDIT` (the money scalar) moved to the economy ceapp's SDK (`ce_economy::Amount`).
// The core substrate SDK carries no money type.

#[cfg(unix)]
mod lane;

#[cfg(unix)]
pub mod adapt;

pub mod data;
pub use data::{cid, Manifest};

pub mod sse;
pub use sse::{Signal, SseDecoder, SseEvent};

#[cfg(feature = "serve")]
pub mod serve;

#[cfg(feature = "locate")]
pub mod locate;

#[cfg(feature = "capability")]
pub mod capability;

// The wallet (Balance/Direction/TxQuery/TxRecord/Wallet — the money view) moved to the economy
// ceapp's SDK (`ce_economy`).

pub mod tags;
pub use tags::TagAdvertiser;

pub mod app;
pub use app::AppInstalled;

use anyhow::{anyhow, Result};
use futures_core::Stream;
use serde::{Deserialize, Serialize};

/// Default local CE node HTTP API base URL.
pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:8844";

/// How many object chunks to upload/download concurrently in [`CeClient::put_object`] /
/// [`CeClient::get_object`]. Bounded so a large object can't open thousands of sockets at once.
pub const OBJECT_CHUNK_CONCURRENCY: usize = 8;

/// Build a reqwest client that sends `Authorization: Bearer <token>` on every request.
fn build_http(token: Option<&str>) -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(t) = token.map(str::trim).filter(|t| !t.is_empty()) {
        if let Ok(v) = reqwest::header::HeaderValue::from_str(&format!("Bearer {t}")) {
            headers.insert(reqwest::header::AUTHORIZATION, v);
        }
    }
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

/// Discover the node's API token: `$CE_API_TOKEN` first (covers custom `--data-dir` and tests),
/// else `<default data dir>/api.token` written by a locally-running node.
pub fn discover_api_token() -> Option<String> {
    if let Ok(t) = std::env::var("CE_API_TOKEN") {
        let t = t.trim().to_string();
        if !t.is_empty() {
            return Some(t);
        }
    }
    let dir = directories::ProjectDirs::from("", "", "ce")?.data_dir().to_path_buf();
    std::fs::read_to_string(dir.join("api.token"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Percent-encode a query-string value (topics are app-chosen strings; `/` and `.` are common).
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b',' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Async client for a CE node. Speaks the HTTP API, and — targeting the LOCAL node on unix,
/// when the node exposes `<data_dir>/lane.sock` — transparently rides the ce-lane shared-memory
/// transport for the hot app-messaging paths (publish/send/request/reply/subscribe/stream),
/// falling back to HTTP silently on any lane absence or failure. Same node-side `AppBus`
/// either way, so apps observe identical behavior.
#[derive(Debug, Clone)]
pub struct CeClient {
    base: String,
    http: reqwest::Client,
    #[cfg(unix)]
    token: Option<String>,
    /// Lazily-bound lane transport, shared across clones. `None` inside = tried and unavailable.
    #[cfg(unix)]
    lane_cell: std::sync::Arc<std::sync::OnceLock<Option<std::sync::Arc<lane::LaneClient>>>>,
    /// Whether this client MAY use the lane (local target, not disabled via `$CE_NO_LANE`).
    #[cfg(unix)]
    lane_candidate: bool,
}

impl CeClient {
    /// Client for a node at `base_url` (e.g. `http://127.0.0.1:8844`).
    ///
    /// The node's HTTP API token is attached to every request (so mutating calls pass the node's
    /// auth middleware). It is discovered from `$CE_API_TOKEN`, else `<default data dir>/api.token`.
    /// For a node started with a custom `--data-dir`, set `$CE_API_TOKEN` (or use [`with_token`]).
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_token(base_url, discover_api_token())
    }

    /// Client for `base_url` with an explicit API token (or `None` for read-only access).
    pub fn with_token(base_url: impl Into<String>, token: Option<String>) -> Self {
        let base = base_url.into().trim_end_matches('/').to_string();
        let http = build_http(token.as_deref());
        #[cfg(unix)]
        {
            // The lane is same-host only: consider it exactly when this client targets the
            // local node (and the operator hasn't disabled it for debugging).
            let local = base.starts_with("http://127.0.0.1") || base.starts_with("http://localhost");
            let disabled = std::env::var("CE_NO_LANE").map(|v| !v.trim().is_empty()).unwrap_or(false);
            CeClient {
                base,
                http,
                token,
                lane_cell: std::sync::Arc::new(std::sync::OnceLock::new()),
                lane_candidate: local && !disabled,
            }
        }
        #[cfg(not(unix))]
        {
            CeClient { base, http }
        }
    }

    /// The lane transport when this client may and can use it; `None` = stay on HTTP. Bound
    /// once, shared across clones; a lane that later dies filters out here so every subsequent
    /// call falls back to HTTP without the app noticing.
    #[cfg(unix)]
    fn lane(&self) -> Option<std::sync::Arc<lane::LaneClient>> {
        if !self.lane_candidate {
            return None;
        }
        self.lane_cell
            .get_or_init(|| {
                // An ENDOWED endpoint first: the host pre-bound this app's node channel at
                // spawn (membrane policy applied there) and passed its fds — no socket, no
                // token. A present-but-broken endowment does NOT fall through to a socket
                // dial: the host wired this channel deliberately; dialing around it would
                // bypass whatever it composed.
                if let Some(endowed) = ce_lane::claim_endowed() {
                    return match endowed {
                        Ok(ep) => Some(std::sync::Arc::new(lane::LaneClient::from_endpoint(ep))),
                        Err(_) => None,
                    };
                }
                let path = lane::socket_path()?;
                let token = self.token.clone()?;
                lane::LaneClient::bind(&path, &token).ok().map(std::sync::Arc::new)
            })
            .clone()
            .filter(|l| !l.is_dead())
    }

    /// Parse a 64-hex NodeId for the lane's binary wire form.
    #[cfg(unix)]
    fn parse_node_id(hex_id: &str) -> Option<[u8; 32]> {
        let b = hex::decode(hex_id.trim()).ok()?;
        b.try_into().ok()
    }

    /// Client for the local node on the default port (8844).
    pub fn local() -> Self {
        Self::new(DEFAULT_BASE_URL)
    }

    /// The node API base URL this client targets (no trailing slash), e.g.
    /// `http://127.0.0.1:8844`. Useful for logging, or for hitting an endpoint the SDK does not
    /// yet wrap.
    pub fn base_url(&self) -> &str {
        &self.base
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Transport hatch: GET a path and decode its JSON body, with the shared error handling and
    /// auth token attached. This is the SUBSTRATE transport primitive that adapter SDKs (the
    /// economy ceapp's `ce-economy`, the trust ceapp's `ce-ratio`, …) ride to speak to their
    /// domain endpoints WITHOUT re-implementing auth/url/error handling. The core SDK owns the
    /// transport; each adapter SDK owns its own typed domain surface on top. Path may include a
    /// query string.
    pub async fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        json(self.http.get(self.url(path)).send().await?).await
    }

    /// Transport hatch: POST `body` as JSON to `path` and decode the JSON response. See
    /// [`get_json`](Self::get_json) — the same substrate primitive for adapter SDKs, for mutating
    /// calls. Retries are the caller's concern (money endpoints must not auto-retry).
    pub async fn post_json<B: Serialize, T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        json(self.http.post(self.url(path)).json(body).send().await?).await
    }

    /// Transport hatch: POST `body` as JSON to `path`, expecting an empty success response.
    pub async fn post_void<B: Serialize>(&self, path: &str, body: &B) -> Result<()> {
        ok(self.http.post(self.url(path)).json(body).send().await?).await
    }

    /// Transport hatch: POST `body` as JSON to `path` and return the raw response BYTES (for
    /// endpoints that answer with an octet-stream, not JSON — e.g. the economy ceapp's paid
    /// data fetch). Authenticated like every other call.
    pub async fn post_bytes<B: Serialize>(&self, path: &str, body: &B) -> Result<Vec<u8>> {
        let resp = self.http.post(self.url(path)).json(body).send().await?;
        if !resp.status().is_success() {
            return Err(anyhow!("CE API {}: {path}", resp.status()));
        }
        Ok(resp.bytes().await?.to_vec())
    }

    /// Transport hatch: open an SSE endpoint and decode its frames into typed `T` events. Rides
    /// the shared [`sse`] decoder (the generic SSE machinery stays substrate; adapter SDKs bring
    /// their own event types, e.g. the economy ceapp's `BlockEvent`/`TxEvent`).
    pub async fn sse_stream<T>(&self, path: &str) -> Result<impl Stream<Item = Result<T>>>
    where
        T: for<'de> Deserialize<'de>,
    {
        Ok(sse::decode_stream::<T>(self.open_sse(path).await?))
    }

    /// Internal: open an SSE endpoint, returning the raw streaming response. Callers feed the
    /// body through [`sse::decode_stream`]. Errors if the endpoint is not a successful stream.
    pub(crate) async fn open_sse(&self, path: &str) -> Result<reqwest::Response> {
        let resp = self
            .http
            .get(self.url(path))
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .header(reqwest::header::CACHE_CONTROL, "no-cache")
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(anyhow!("CE API {}: SSE open failed for {path}", resp.status()));
        }
        Ok(resp)
    }

    // ----- SSE streams -----
    // NOTE: the chain streams (`/blocks/stream`, `/transactions/stream` → BlockEvent/TxEvent)
    // moved to the economy ceapp's SDK (`ce_economy::EconomyClient::{blocks_stream,
    // transactions_stream}`). The core SDK keeps only the substrate streams below.

    /// Stream validated CEP-1 signals (`GET /signals/stream`).
    pub async fn signals_stream(&self) -> Result<impl Stream<Item = Result<Signal>>> {
        Ok(sse::decode_stream::<Signal>(self.open_sse("/signals/stream").await?))
    }

    /// Stream inbound app messages (`GET /mesh/messages/stream`) — the push counterpart to
    /// polling [`messages`](Self::messages). Every topic; see
    /// [`messages_stream_for`](Self::messages_stream_for) to declare what you actually want.
    pub async fn messages_stream(&self) -> Result<impl Stream<Item = Result<AppMessage>>> {
        self.messages_stream_for(&[]).await
    }

    /// Stream ONLY the topics this app serves. The node honors the list at the source: traffic
    /// belonging to other apps is never encoded and never sent here, so a node's cost scales
    /// with what each app asked for instead of with how many apps are installed. An empty list
    /// means "everything" (the historic behaviour).
    pub async fn messages_stream_for(
        &self,
        topics: &[String],
    ) -> Result<impl Stream<Item = Result<AppMessage>>> {
        use futures_util::StreamExt;
        // Same-host fast path: the node PUSHES inbound messages over the lane (Watch/Event) —
        // identical content to the SSE stream, no HTTP, no polling. Falls back silently.
        #[cfg(unix)]
        if let Some(l) = self.lane() {
            if let Ok(rx) = l.watch_stream().await {
                return Ok(rx
                    .map(|m| {
                        Ok(AppMessage {
                            from: hex::encode(m.from),
                            topic: m.topic,
                            payload_hex: hex::encode(&m.payload),
                            received_at: m.received_at,
                            reply_token: m.reply_token,
                        })
                    })
                    .boxed());
            }
        }
        let path = if topics.is_empty() {
            "/mesh/messages/stream".to_string()
        } else {
            format!("/mesh/messages/stream?topics={}", urlencode(&topics.join(",")))
        };
        Ok(sse::decode_stream::<AppMessage>(self.open_sse(&path).await?).boxed())
    }

    // ----- read -----

    /// The on-chain revoked `(issuer_hex, nonce)` capability set (`GET /capabilities/revoked`).
    /// Apps consult this to deny revoked capability chains.
    pub async fn revoked(&self) -> Result<Vec<(String, u64)>> {
        #[derive(serde::Deserialize)]
        struct R {
            issuer: String,
            nonce: u64,
        }
        let v: Vec<R> = json(self.http.get(self.url("/capabilities/revoked")).send().await?).await?;
        Ok(v.into_iter().map(|r| (r.issuer, r.nonce)).collect())
    }

    /// Liveness check (`GET /health`).
    pub async fn health(&self) -> Result<bool> {
        Ok(self.http.get(self.url("/health")).send().await?.status().is_success())
    }

    /// Node id, chain height, difficulty, balance (`GET /status`).
    pub async fn status(&self) -> Result<NodeStatus> {
        json(self.http.get(self.url("/status")).send().await?).await
    }

    /// Capacity atlas — every peer's latest capacity + capability self-tags (`GET /atlas`).
    pub async fn atlas(&self) -> Result<Vec<AtlasEntry>> {
        json(self.http.get(self.url("/atlas")).send().await?).await
    }

    /// Live ce-lane flows with their shared-ring counters (`GET /lanes`) — the same-host
    /// shared-memory transport's metering readout. This is what an economy adapter polls to
    /// bill traffic it never sat in the path of (approve-then-splice). Empty on non-unix nodes
    /// (the lane is unix-only). Read-only, no token.
    pub async fn lanes(&self) -> Result<Vec<LaneInfo>> {
        json(self.http.get(self.url("/lanes")).send().await?).await
    }

    /// Verifiable public randomness from the PoW tip (`GET /beacon`). Seed reproducible,
    /// auditable host selection from `beacon.hash`.
    pub async fn beacon(&self) -> Result<Beacon> {
        json(self.http.get(self.url("/beacon")).send().await?).await
    }

    /// Raw per-peer latency edges measured by the local node (`GET /netgraph`): smoothed RTT to each
    /// connected libp2p peer. The transport-intrinsic latency primitive that graph/placement SDKs
    /// build on. Peers are keyed by libp2p PeerId (hex), not NodeId.
    pub async fn netgraph(&self) -> Result<Vec<NetEdge>> {
        json(self.http.get(self.url("/netgraph")).send().await?).await
    }

    // ----- moved to adapter ceapps -----
    // The paid job market (jobs/job/bid/mesh_deploy/mesh_deploy_wasm, BidSpec/Job/Deployment) and
    // `transfer` moved to the economy ceapp's SDK (`ce_economy::EconomyClient`). `history()` /
    // NodeHistory (the reputation substrate) moved to the trust ceapp (`ce_ratio::history`). The
    // core SDK is substrate only. Remote exec / file sync were earlier moved to the `rdev` app.

    /// Upload bytes to the content-addressed blob store; returns the sha256 hash (`POST /blobs`).
    /// Use it to publish a WASM module before deploying it by hash.
    pub async fn put_blob(&self, bytes: Vec<u8>) -> Result<String> {
        let v: serde_json::Value = json(self.http.post(self.url("/blobs")).body(bytes).send().await?).await?;
        Ok(v["hash"].as_str().unwrap_or_default().to_string())
    }

    /// Fetch a blob by its content hash (`GET /blobs/:hash`).
    pub async fn get_blob(&self, hash: &str) -> Result<Vec<u8>> {
        let resp = self.http.get(self.url(&format!("/blobs/{hash}"))).send().await?;
        if !resp.status().is_success() {
            return Err(anyhow!("CE API {}: blob not found", resp.status()));
        }
        Ok(resp.bytes().await?.to_vec())
    }

    /// Upload an object of any size: split it into content-addressed chunks, store each via the
    /// blob store, then store the manifest. Returns the **object CID** (the manifest's hash) —
    /// pass it to [`get_object`](Self::get_object) to fetch the whole object back. Chunking is
    /// client-side (see [`data`]); the node just stores opaque blobs.
    pub async fn put_object(&self, bytes: &[u8]) -> Result<String> {
        use futures_util::{StreamExt, TryStreamExt, stream};
        let (manifest, chunks) = data::chunk_object(bytes, data::DEFAULT_CHUNK_SIZE);
        // Upload chunks concurrently (order-independent — the manifest records the order). Bounded so
        // a huge object can't open thousands of sockets at once. This is the dominant write cost, so
        // parallelism here is what makes object/file writes fast.
        stream::iter(chunks.into_iter().map(|(chunk_cid, chunk)| async move {
            let stored = self.put_blob(chunk).await?;
            if stored != chunk_cid {
                return Err(anyhow!(
                    "blob store returned hash {stored} for chunk {chunk_cid} (hashing mismatch)"
                ));
            }
            Ok::<(), anyhow::Error>(())
        }))
        .buffer_unordered(OBJECT_CHUNK_CONCURRENCY)
        .try_collect::<()>()
        .await?;
        let manifest_bytes = serde_json::to_vec(&manifest)?;
        self.put_blob(manifest_bytes).await
    }

    /// Fetch an object by its CID: resolve the manifest, pull each chunk from the blob store, and
    /// verify every chunk against its CID before reassembling (content addressing makes this
    /// trustless). Chunks are fetched concurrently (bounded) and reassembled in manifest order.
    pub async fn get_object(&self, object_cid: &str) -> Result<Vec<u8>> {
        use futures_util::{StreamExt, TryStreamExt, stream};
        let manifest_bytes = self.get_blob(object_cid).await?;
        let manifest: data::Manifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|e| anyhow!("{object_cid} is not a v1 object manifest: {e}"))?;
        if !manifest.is_v1() {
            return Err(anyhow!("unsupported manifest kind: {}", manifest.kind));
        }
        // `buffered` keeps results in manifest order while fetching up to N chunks at once. Own the
        // cids (don't borrow `manifest` inside the futures) so the returned future borrows only
        // `&self` — borrowing `manifest` here entangles an extra lifetime into this async fn's opaque
        // return type and breaks HRTB inference for callers that box it (e.g. ce-coord's loader).
        let cids: Vec<String> = manifest.chunks.clone();
        let chunks: Vec<Vec<u8>> = stream::iter(cids.into_iter().map(|chunk_cid| async move {
            let chunk = self.get_blob(&chunk_cid).await?;
            let got = data::cid(&chunk);
            if got != chunk_cid {
                return Err(anyhow!(
                    "chunk verification failed: expected {chunk_cid}, got {got}"
                ));
            }
            Ok::<Vec<u8>, anyhow::Error>(chunk)
        }))
        .buffered(OBJECT_CHUNK_CONCURRENCY)
        .try_collect()
        .await?;
        let mut out = Vec::with_capacity(manifest.total_size as usize);
        for chunk in chunks {
            out.extend_from_slice(&chunk);
        }
        if out.len() as u64 != manifest.total_size {
            return Err(anyhow!(
                "reassembled size {} != manifest total_size {}",
                out.len(),
                manifest.total_size
            ));
        }
        Ok(out)
    }

    // Paid chunk fetch (`fetch_chunk_paid`, `/data/fetch`) moved to the economy ceapp's SDK
    // (`ce_economy::EconomyClient::fetch_chunk_paid`). Free content-addressed blob/object fetch
    // (`get_blob`/`get_object`) stays here — it is substrate.

    // ----- app messaging (docs/app-messaging.md) -----

    /// Send a directed application message to a node over the mesh (`POST /mesh/send`). `topic` is
    /// an app-chosen namespace; `payload` is opaque bytes. The recipient authenticates the sender
    /// (it sees your verified NodeId) and enqueues it for its app. Returns once the node confirms
    /// delivery. Subscribe to incoming messages on `GET /mesh/messages/stream`, or poll
    /// [`messages`](Self::messages).
    pub async fn send_message(&self, to: &str, topic: &str, payload: &[u8]) -> Result<()> {
        #[cfg(unix)]
        if let (Some(l), Some(to_id)) = (self.lane(), Self::parse_node_id(to)) {
            use ce_lane::proto::{NodeOk, NodeOp};
            match l
                .op(NodeOp::Send { to: to_id, topic: topic.into(), payload: payload.to_vec() })
                .await
            {
                Ok(Ok(NodeOk::Done)) => return Ok(()),
                Ok(Ok(_)) => return Err(anyhow!("CE lane: unexpected response")),
                Ok(Err(e)) => return Err(anyhow!("CE node: {e}")),
                Err(_) => {} // lane gone mid-flight: fall back to HTTP below
            }
        }
        let body = serde_json::json!({
            "to": to,
            "topic": topic,
            "payload_hex": hex::encode(payload),
        });
        ok(self.http.post(self.url("/mesh/send")).json(&body).send().await?).await
    }

    /// Snapshot of recently-received app messages (`GET /mesh/messages`). For reliable delivery
    /// subscribe to the SSE stream instead; this ring is best-effort and capped. Received pub/sub
    /// messages and incoming requests appear here too (the latter carry a `reply_token`).
    pub async fn messages(&self) -> Result<Vec<AppMessage>> {
        json(self.http.get(self.url("/mesh/messages")).send().await?).await
    }

    /// Subscribe to an app pub/sub topic so this node receives its messages (`POST
    /// /mesh/subscribe`). Idempotent; lasts for the node's lifetime.
    pub async fn subscribe(&self, topic: &str) -> Result<()> {
        #[cfg(unix)]
        if let Some(l) = self.lane() {
            use ce_lane::proto::{NodeOk, NodeOp};
            match l.op(NodeOp::Subscribe { topic: topic.into() }).await {
                Ok(Ok(NodeOk::Done)) => return Ok(()),
                Ok(Ok(_)) => return Err(anyhow!("CE lane: unexpected response")),
                Ok(Err(e)) => return Err(anyhow!("CE node: {e}")),
                Err(_) => {}
            }
        }
        let body = serde_json::json!({ "topic": topic });
        ok(self.http.post(self.url("/mesh/subscribe")).json(&body).send().await?).await
    }

    /// Publish a signed message to an app pub/sub topic (`POST /mesh/publish`). The node signs it
    /// (subscribers verify authorship) and broadcasts it to everyone subscribed to `topic`.
    pub async fn publish(&self, topic: &str, payload: &[u8]) -> Result<()> {
        #[cfg(unix)]
        if let Some(l) = self.lane() {
            use ce_lane::proto::{NodeOk, NodeOp};
            match l.op(NodeOp::Publish { topic: topic.into(), payload: payload.to_vec() }).await {
                Ok(Ok(NodeOk::Done)) => return Ok(()),
                Ok(Ok(_)) => return Err(anyhow!("CE lane: unexpected response")),
                Ok(Err(e)) => return Err(anyhow!("CE node: {e}")),
                Err(_) => {}
            }
        }
        let body = serde_json::json!({ "topic": topic, "payload_hex": hex::encode(payload) });
        ok(self.http.post(self.url("/mesh/publish")).json(&body).send().await?).await
    }

    /// Send a request to a node and wait for its app's reply (`POST /mesh/request`). The peer's app
    /// answers via [`reply`](Self::reply); this returns the reply payload, or errors on timeout.
    pub async fn request(
        &self,
        to: &str,
        topic: &str,
        payload: &[u8],
        timeout_ms: u64,
    ) -> Result<Vec<u8>> {
        #[cfg(unix)]
        if let (Some(l), Some(to_id)) = (self.lane(), Self::parse_node_id(to)) {
            use ce_lane::proto::{NodeOk, NodeOp};
            match l
                .op(NodeOp::Request {
                    to: to_id,
                    topic: topic.into(),
                    payload: payload.to_vec(),
                    timeout_ms,
                })
                .await
            {
                Ok(Ok(NodeOk::Reply { payload })) => return Ok(payload),
                Ok(Ok(_)) => return Err(anyhow!("CE lane: unexpected response")),
                Ok(Err(e)) => return Err(anyhow!("CE node: {e}")),
                Err(_) => {}
            }
        }
        let body = serde_json::json!({
            "to": to,
            "topic": topic,
            "payload_hex": hex::encode(payload),
            "timeout_ms": timeout_ms,
        });
        let v: serde_json::Value =
            json(self.http.post(self.url("/mesh/request")).json(&body).send().await?).await?;
        let hexs = v["payload_hex"].as_str().unwrap_or_default();
        hex::decode(hexs).map_err(|e| anyhow!("bad reply payload hex: {e}"))
    }

    /// Answer an incoming request, identified by the `reply_token` on its [`AppMessage`] (`POST
    /// /mesh/reply`). The reply is routed back to the original requester's [`request`](Self::request).
    pub async fn reply(&self, token: u64, payload: &[u8]) -> Result<()> {
        #[cfg(unix)]
        if let Some(l) = self.lane() {
            use ce_lane::proto::{NodeOk, NodeOp};
            match l.op(NodeOp::Reply { token, payload: payload.to_vec() }).await {
                Ok(Ok(NodeOk::Done)) => return Ok(()),
                Ok(Ok(_)) => return Err(anyhow!("CE lane: unexpected response")),
                Ok(Err(e)) => return Err(anyhow!("CE node: {e}")),
                Err(_) => {}
            }
        }
        let body = serde_json::json!({ "token": token, "payload_hex": hex::encode(payload) });
        ok(self.http.post(self.url("/mesh/reply")).json(&body).send().await?).await
    }

    // ----- naming + discovery -----

    /// Claim a unique human-readable name for this node (`POST /names/claim`). Takes effect once
    /// mined; first claim wins. Name: 3–32 chars, lowercase `a-z` / `0-9` / hyphen.
    pub async fn claim_name(&self, name: &str) -> Result<()> {
        let body = serde_json::json!({ "name": name });
        ok(self.http.post(self.url("/names/claim")).json(&body).send().await?).await
    }

    /// Resolve a claimed name to its owner's NodeId hex (`GET /names/:name`); `None` if unclaimed.
    pub async fn resolve_name(&self, name: &str) -> Result<Option<String>> {
        let resp = self.http.get(self.url(&format!("/names/{name}"))).send().await?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(anyhow!("CE API {}: resolve failed", resp.status()));
        }
        let v: serde_json::Value = resp.json().await?;
        Ok(v["node_id"].as_str().map(|s| s.to_string()))
    }

    /// Advertise that this node provides a named service, discoverable via the DHT (`POST
    /// /discovery/advertise`). Re-call periodically — provider records expire.
    pub async fn advertise_service(&self, service: &str) -> Result<()> {
        let body = serde_json::json!({ "service": service });
        ok(self.http.post(self.url("/discovery/advertise")).json(&body).send().await?).await
    }

    /// Find the NodeId hexes of nodes advertising a named service (`GET /discovery/find/:service`).
    /// The name is percent-encoded: the route is a single path segment, so an unencoded `/` in a
    /// service name (the common `app/role` convention) would 404.
    pub async fn find_service(&self, service: &str) -> Result<Vec<String>> {
        let v: serde_json::Value = json(
            self.http.get(self.url(&format!("/discovery/find/{}", encode_path_segment(service)))).send().await?,
        )
        .await?;
        Ok(v["providers"]
            .as_array()
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default())
    }

    // `pay_relay` (`/relay/pay`) moved to the economy ceapp's SDK
    // (`ce_economy::EconomyClient::pay_relay`).

    /// Stop a job on a specific remote host (`POST /mesh-kill`).
    pub async fn mesh_kill(&self, node_id: &str, job_id: &str, grant: Option<&str>) -> Result<()> {
        let body = serde_json::json!({ "node_id": node_id, "job_id": job_id, "grant": grant });
        ok(self.http.post(self.url("/mesh-kill")).json(&body).send().await?).await
    }

    /// Force-stop a local job by id (`DELETE /jobs/:id`).
    pub async fn kill(&self, job_id: &str) -> Result<()> {
        ok(self.http.delete(self.url(&format!("/jobs/{job_id}"))).send().await?).await
    }

    // Payment channels (channel_open/sign_receipt/channel_close/channel_expire/channels,
    // Channel/Receipt) moved to the economy ceapp's SDK (`ce_economy::EconomyClient`).
}

/// Deserialize a successful JSON response, or surface an error with status + body.
async fn json<T: for<'de> Deserialize<'de>>(resp: reqwest::Response) -> Result<T> {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("CE API {status}: {body}"));
    }
    serde_json::from_str(&body).map_err(|e| anyhow!("decode {status} body: {e}: {body}"))
}

/// Expect a successful empty response.
async fn ok(resp: reqwest::Response) -> Result<()> {
    let status = resp.status();
    if status.is_success() {
        Ok(())
    } else {
        Err(anyhow!("CE API {status}: {}", resp.text().await.unwrap_or_default()))
    }
}

/// Percent-encode a value used as ONE path segment (RFC 3986 unreserved chars pass through).
/// Needed for user-chosen names in path-routed endpoints — e.g. service names like
/// `ce-hetzner/control`, where a raw `/` would add a path segment and 404.
fn encode_path_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ----- wire types (mirror the node's JSON; amounts are base-unit strings) -----

/// The core (substrate) node status from `GET /status`. The core node is chain-free: height,
/// balance, and the rest of the ledger are NOT substrate concepts — they belong to the economy
/// adapter and its own typed SDK, not this client. `economy` reports whether an economy adapter is
/// attached.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeStatus {
    pub node_id: String,
    /// libp2p peer id (core nodes report this; older economy nodes omit it → empty).
    #[serde(default)]
    pub peer_id: String,
    /// P2P listen port (core nodes report this; older nodes omit it → 0).
    #[serde(default)]
    pub listen_port: u16,
    /// Whether an economy adapter runs on this node. `Some(false)` = core/personal-mesh (the ledger
    /// and economic endpoints answer 503). `None` on older nodes predating the field — treat as
    /// `true` (economy on), the historical default.
    #[serde(default)]
    pub economy: Option<bool>,
}

impl NodeStatus {
    /// Whether an economy adapter is attached (a `None` flag means an older economy node).
    pub fn economy_enabled(&self) -> bool {
        self.economy.unwrap_or(true)
    }
}

/// One live ce-lane flow as reported by `GET /lanes` — the metering readout an economy adapter
/// bills from. Mirrors the node's JSON; kept a standalone type here (not the unix-only
/// `ce-lane` crate types) so the accessor works on every platform.
#[derive(Debug, Clone, Deserialize)]
pub struct LaneInfo {
    pub flow_id: u64,
    pub target: String,
    pub created_at: u64,
    pub counters: FlowCounters,
    /// Capability links `(issuer_hex, nonce)` this flow was bound to; revoking any severs it.
    #[serde(default)]
    pub links: Vec<(String, u64)>,
}

/// Combined per-direction counters of a flow (read live from the shared ring headers).
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct FlowCounters {
    pub a_to_b: RingCounters,
    pub b_to_a: RingCounters,
}

/// One direction's counters. `msgs`/`bytes` are cumulative and monotonic — the substrate for
/// metering deltas; `queued` is in-flight; `closed` marks a torn-down direction.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct RingCounters {
    pub msgs: u64,
    pub bytes: u64,
    pub queued: u64,
    pub closed: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NetEdge {
    /// The connected peer, keyed by libp2p PeerId (hex).
    pub peer: String,
    /// Smoothed round-trip time to the peer, in milliseconds (EWMA).
    pub rtt_ms: f64,
    /// Number of samples folded into the estimate.
    #[serde(default)]
    pub samples: u64,
    /// Unix seconds of the most recent sample.
    #[serde(default)]
    pub last_seen_secs: u64,
}

/// A capacity-atlas entry: one peer's advertised resources and self-tags.
#[derive(Debug, Clone, Deserialize)]
pub struct AtlasEntry {
    pub node_id: String,
    pub cpu_cores: u32,
    pub mem_mb: u32,
    pub running_jobs: u32,
    pub last_seen_secs: u64,
    /// Capability self-tags advertised by the node (`gpu`, `docker`, `linux`, ...).
    #[serde(default)]
    pub tags: Vec<String>,
}

impl AtlasEntry {
    /// True if this host advertises the given capability self-tag.
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }
}

// BidSpec / Job (the paid job market) moved to the economy ceapp's SDK (`ce_economy`).

/// Result of a one-shot [`CeClient::mesh_exec`].
#[derive(Debug, Clone, Deserialize)]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i64,
}

impl ExecResult {
    pub fn ok(&self) -> bool {
        self.exit_code == 0
    }
}

// Channel / Receipt (payment channels) and Deployment (paid deploy result) moved to the economy
// ceapp's SDK (`ce_economy`).

/// A directed application message received from a mesh peer. `from` is the cryptographically
/// authenticated sender NodeId — trust it to decide what to honor. Use [`payload`](Self::payload)
/// to decode the opaque bytes.
#[derive(Debug, Clone, Deserialize)]
pub struct AppMessage {
    /// Authenticated sender NodeId (hex).
    pub from: String,
    /// App-chosen topic namespace.
    pub topic: String,
    /// Opaque payload, hex-encoded.
    pub payload_hex: String,
    /// Unix seconds when the local node received it.
    pub received_at: u64,
    /// Set when this is a request expecting a reply: pass it to [`CeClient::reply`](crate::CeClient::reply).
    #[serde(default)]
    pub reply_token: Option<u64>,
}

impl AppMessage {
    /// Decode the payload bytes.
    pub fn payload(&self) -> Result<Vec<u8>> {
        hex::decode(&self.payload_hex).map_err(|e| anyhow!("bad payload hex: {e}"))
    }
}

/// Verifiable public randomness from the PoW chain tip.
#[derive(Debug, Clone, Deserialize)]
pub struct Beacon {
    pub height: u64,
    /// Tip block hash, 64 hex chars — unpredictable and globally agreed.
    pub hash: String,
}

// NodeHistory (the reputation substrate) moved to the TRUST ceapp's SDK (`ce_ratio::NodeHistory`
// + `ce_ratio::history::history`). The core SDK is substrate only.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_url_is_trimmed_and_accessible() {
        let c = CeClient::with_token("http://example.com:8844/", None);
        assert_eq!(c.base_url(), "http://example.com:8844");
        // url() composes correctly with no double slash.
        assert_eq!(c.url("/status"), "http://example.com:8844/status");
    }

    #[test]
    fn local_uses_default_base() {
        assert_eq!(CeClient::local().base_url(), DEFAULT_BASE_URL);
    }

    #[test]
    fn path_segment_encoding_protects_service_names() {
        // A `/` in a service name must not become a path separator (it 404'd find_service for the
        // common `app/role` naming convention, e.g. ce-hetzner/control).
        assert_eq!(encode_path_segment("ce-hetzner/control"), "ce-hetzner%2Fcontrol");
        assert_eq!(encode_path_segment("plain-name_1.0~x"), "plain-name_1.0~x");
        assert_eq!(encode_path_segment("a b%c"), "a%20b%25c");
    }

    #[test]
    fn discover_api_token_reads_env_first() {
        // Set the env var and confirm discovery returns it (covers the custom --data-dir path).
        // SAFETY: single-threaded test; we set then immediately read and clear.
        unsafe {
            std::env::set_var("CE_API_TOKEN", "  env-token  ");
        }
        assert_eq!(discover_api_token().as_deref(), Some("env-token"));
        unsafe {
            std::env::set_var("CE_API_TOKEN", "");
        }
        // Empty env var should not be returned (falls through to file, which likely is absent).
        let after = discover_api_token();
        assert_ne!(after.as_deref(), Some(""));
        unsafe {
            std::env::remove_var("CE_API_TOKEN");
        }
    }

    #[test]
    fn build_http_with_invalid_token_does_not_panic() {
        // A header value with control chars can't be a HeaderValue; build_http must still produce a
        // working client (it silently drops the bad header).
        let c = CeClient::with_token("http://127.0.0.1:8844", Some("bad\nvalue".into()));
        assert_eq!(c.base_url(), "http://127.0.0.1:8844");
    }

    #[test]
    fn node_status_decodes_core_substrate_shape() {
        // A core node returns only node_id/peer_id/listen_port/economy; chain fields are ignored.
        let s: NodeStatus = serde_json::from_str(
            r#"{"node_id":"n","peer_id":"p","listen_port":4001,"economy":false}"#,
        )
        .unwrap();
        assert_eq!(s.node_id, "n");
        assert_eq!(s.peer_id, "p");
        assert_eq!(s.listen_port, 4001);
        assert!(!s.economy_enabled());
        // And it still decodes an economy node's richer /status (extra fields ignored).
        let e: NodeStatus =
            serde_json::from_str(r#"{"node_id":"n","height":1,"balance":"0","economy":true}"#).unwrap();
        assert!(e.economy_enabled());
    }

    #[test]
    fn atlas_entry_has_tag() {
        let e: AtlasEntry = serde_json::from_str(
            r#"{"node_id":"n","cpu_cores":4,"mem_mb":8000,"running_jobs":1,"last_seen_secs":3,"tags":["gpu"]}"#,
        )
        .unwrap();
        assert!(e.has_tag("gpu"));
        assert!(!e.has_tag("cpu"));
    }

    #[test]
    fn exec_result_ok() {
        let ok = ExecResult { stdout: "".into(), stderr: "".into(), exit_code: 0 };
        assert!(ok.ok());
        let bad = ExecResult { stdout: "".into(), stderr: "boom".into(), exit_code: 1 };
        assert!(!bad.ok());
    }

    #[test]
    fn app_message_payload_decodes_hex() {
        let m: AppMessage = serde_json::from_str(
            r#"{"from":"f","topic":"t","payload_hex":"68656c6c6f","received_at":1}"#,
        )
        .unwrap();
        assert_eq!(m.payload().unwrap(), b"hello");
        assert!(m.reply_token.is_none());
    }
}
