//! # ce-rs — Rust SDK for CE
//!
//! A typed, async client for talking to a **local CE node's HTTP API**. Apps (schedulers,
//! dashboards, bots) use this instead of hand-rolling JSON: you get `Amount`, `NodeStatus`,
//! `AtlasEntry`, `Job` and methods that mirror the node's endpoints.
//!
//! ```no_run
//! use ce_rs::{CeClient, BidSpec, Amount};
//! # async fn demo() -> anyhow::Result<()> {
//! let ce = CeClient::local(); // http://127.0.0.1:8844
//! let status = ce.status().await?;
//! println!("height {} balance {}", status.height, status.balance);
//!
//! // Find a GPU host and place a job on it directly (mesh-routed).
//! let hosts = ce.atlas().await?;
//! if let Some(h) = hosts.iter().find(|h| h.tags.iter().any(|t| t == "gpu")) {
//!     let spec = BidSpec { image: "alpine:latest".into(), cmd: vec!["echo".into(), "hi".into()],
//!                          cpu_cores: 1, mem_mb: 128, duration_secs: 60, bid: Amount::from_credits(10) };
//!     let job_id = ce.mesh_deploy(&h.node_id, &spec, None).await?;
//!     println!("deployed {job_id} on {}", h.node_id);
//! }
//! # Ok(()) }
//! ```
//!
//! v0 targets the unauthenticated local-node API (status, atlas, jobs, transfer,
//! mesh-deploy/kill, signal send). CE-auth signing for direct-to-remote `/exec`,`/sync`
//! and SSE subscriptions are planned follow-ups.

mod amount;
pub use amount::{Amount, CREDIT};

pub mod data;
pub use data::{cid, Manifest};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

/// Default local CE node HTTP API base URL.
pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:8844";

/// Async client for a CE node's HTTP API.
#[derive(Debug, Clone)]
pub struct CeClient {
    base: String,
    http: reqwest::Client,
}

impl CeClient {
    /// Client for a node at `base_url` (e.g. `http://127.0.0.1:8844`).
    pub fn new(base_url: impl Into<String>) -> Self {
        let base = base_url.into().trim_end_matches('/').to_string();
        CeClient { base, http: reqwest::Client::new() }
    }

    /// Client for the local node on the default port (8844).
    pub fn local() -> Self {
        Self::new(DEFAULT_BASE_URL)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    // ----- read -----

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

    /// Verifiable public randomness from the PoW tip (`GET /beacon`). Seed reproducible,
    /// auditable host selection from `beacon.hash`.
    pub async fn beacon(&self) -> Result<Beacon> {
        json(self.http.get(self.url("/beacon")).send().await?).await
    }

    /// All jobs known to this node (`GET /jobs`).
    pub async fn jobs(&self) -> Result<Vec<Job>> {
        json(self.http.get(self.url("/jobs")).send().await?).await
    }

    /// One job's status (`GET /jobs/:id`).
    pub async fn job(&self, job_id: &str) -> Result<Job> {
        json(self.http.get(self.url(&format!("/jobs/{job_id}"))).send().await?).await
    }

    /// A node's interaction history — the reputation substrate (`GET /history/:node_id`).
    /// CE reports immutable facts (jobs hosted, heartbeats, earned/spent); the caller derives
    /// its own per-relationship trust. Query an archive node for complete history.
    pub async fn history(&self, node_id: &str) -> Result<NodeHistory> {
        json(self.http.get(self.url(&format!("/history/{node_id}"))).send().await?).await
    }

    // ----- economy -----

    /// Transfer credits to another node; returns the tx id (`POST /transfer`).
    pub async fn transfer(&self, to: &str, amount: Amount) -> Result<String> {
        let resp = self
            .http
            .post(self.url("/transfer"))
            .json(&serde_json::json!({ "to": to, "amount": amount }))
            .send()
            .await?;
        let v: serde_json::Value = json(resp).await?;
        Ok(v["tx_id"].as_str().unwrap_or_default().to_string())
    }

    // ----- placement -----

    /// Broadcast a bid; any host with capacity may accept it. Returns the job id
    /// (`POST /jobs/bid`). For directed placement use [`mesh_deploy`](Self::mesh_deploy).
    pub async fn bid(&self, spec: &BidSpec) -> Result<String> {
        let resp = self.http.post(self.url("/jobs/bid")).json(spec).send().await?;
        let v: serde_json::Value = json(resp).await?;
        Ok(v["job_id"].as_str().unwrap_or_default().to_string())
    }

    /// Directed placement: deploy a cell on a **specific** host over the mesh.
    /// Returns the host-assigned job id (`POST /mesh-deploy`).
    pub async fn mesh_deploy(
        &self,
        node_id: &str,
        spec: &BidSpec,
        grant: Option<&str>,
    ) -> Result<String> {
        let body = serde_json::json!({
            "node_id": node_id,
            "image": spec.image,
            "cmd": spec.cmd,
            "cpu_cores": spec.cpu_cores,
            "mem_mb": spec.mem_mb,
            "duration_secs": spec.duration_secs,
            "bid": spec.bid,
            "grant": grant,
        });
        let resp = self.http.post(self.url("/mesh-deploy")).json(&body).send().await?;
        let v: serde_json::Value = json(resp).await?;
        Ok(v["job_id"].as_str().unwrap_or_default().to_string())
    }

    // Remote exec and file sync/delete used to be SDK methods here (POST /mesh-exec,
    // PUT/DELETE /mesh-sync). They moved out of CE into the `rdev` app (built on AppRequest +
    // ce-cap); the SDK no longer wraps them. Apps drive them via `request`/`reply`.

    /// Deploy a **WASM** workload on a specific host over the mesh — the module is referenced by
    /// its content hash (upload it first with [`put_blob`](Self::put_blob)). `inputs` are
    /// content-addressed CIDs the host stages from the data layer before launch (Stage 4); pass
    /// `&[]` for none. (`POST /mesh-deploy`)
    #[allow(clippy::too_many_arguments)]
    pub async fn mesh_deploy_wasm(
        &self,
        node_id: &str,
        module_hash: &str,
        entry: &str,
        cpu_cores: u32,
        mem_mb: u64,
        duration_secs: u64,
        bid: Amount,
        grant: Option<&str>,
        inputs: &[&str],
    ) -> Result<Deployment> {
        let body = serde_json::json!({
            "node_id": node_id,
            "wasm_module": module_hash,
            "wasm_entry": entry,
            "cpu_cores": cpu_cores,
            "mem_mb": mem_mb,
            "duration_secs": duration_secs,
            "bid": bid,
            "grant": grant,
            "inputs": inputs,
        });
        let v: serde_json::Value = json(self.http.post(self.url("/mesh-deploy")).json(&body).send().await?).await?;
        Ok(Deployment {
            job_id: v["job_id"].as_str().unwrap_or_default().to_string(),
            output: v["output"].as_str().map(|s| s.to_string()),
        })
    }

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
        let (manifest, chunks) = data::chunk_object(bytes, data::DEFAULT_CHUNK_SIZE);
        for (chunk_cid, chunk) in chunks {
            let stored = self.put_blob(chunk).await?;
            if stored != chunk_cid {
                return Err(anyhow!(
                    "blob store returned hash {stored} for chunk {chunk_cid} (hashing mismatch)"
                ));
            }
        }
        let manifest_bytes = serde_json::to_vec(&manifest)?;
        self.put_blob(manifest_bytes).await
    }

    /// Fetch an object by its CID: resolve the manifest, pull each chunk from the blob store, and
    /// verify every chunk against its CID before reassembling (content addressing makes this
    /// trustless). Chunks are fetched sequentially in Stage 1; parallel multi-provider fetch is
    /// the Stage 2 mesh refinement.
    pub async fn get_object(&self, object_cid: &str) -> Result<Vec<u8>> {
        let manifest_bytes = self.get_blob(object_cid).await?;
        let manifest: data::Manifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|e| anyhow!("{object_cid} is not a v1 object manifest: {e}"))?;
        if !manifest.is_v1() {
            return Err(anyhow!("unsupported manifest kind: {}", manifest.kind));
        }
        let mut out = Vec::with_capacity(manifest.total_size as usize);
        for chunk_cid in &manifest.chunks {
            let chunk = self.get_blob(chunk_cid).await?;
            let got = data::cid(&chunk);
            if got != *chunk_cid {
                return Err(anyhow!(
                    "chunk verification failed: expected {chunk_cid}, got {got}"
                ));
            }
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

    /// Paid chunk fetch (data layer Stage 3): authorise `provider` to redeem `cumulative` on
    /// `channel_id` and pull the chunk `cid` from it over the mesh, paying as we go. `cumulative`
    /// is the monotonic total for the channel and must cover the running cost of every chunk
    /// fetched on it so far — the caller tracks it across fetches. Requires an open channel with
    /// the provider. The returned bytes are verified against `cid`. (`POST /data/fetch`)
    pub async fn fetch_chunk_paid(
        &self,
        provider: &str,
        cid: &str,
        channel_id: &str,
        cumulative: Amount,
    ) -> Result<Vec<u8>> {
        let body = serde_json::json!({
            "provider": provider,
            "cid": cid,
            "channel_id": channel_id,
            "cumulative": cumulative,
        });
        let resp = self.http.post(self.url("/data/fetch")).json(&body).send().await?;
        if !resp.status().is_success() {
            return Err(anyhow!("CE API {}: paid fetch failed", resp.status()));
        }
        Ok(resp.bytes().await?.to_vec())
    }

    // ----- app messaging (docs/app-messaging.md) -----

    /// Send a directed application message to a node over the mesh (`POST /mesh/send`). `topic` is
    /// an app-chosen namespace; `payload` is opaque bytes. The recipient authenticates the sender
    /// (it sees your verified NodeId) and enqueues it for its app. Returns once the node confirms
    /// delivery. Subscribe to incoming messages on `GET /mesh/messages/stream`, or poll
    /// [`messages`](Self::messages).
    pub async fn send_message(&self, to: &str, topic: &str, payload: &[u8]) -> Result<()> {
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
        let body = serde_json::json!({ "topic": topic });
        ok(self.http.post(self.url("/mesh/subscribe")).json(&body).send().await?).await
    }

    /// Publish a signed message to an app pub/sub topic (`POST /mesh/publish`). The node signs it
    /// (subscribers verify authorship) and broadcasts it to everyone subscribed to `topic`.
    pub async fn publish(&self, topic: &str, payload: &[u8]) -> Result<()> {
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
    pub async fn find_service(&self, service: &str) -> Result<Vec<String>> {
        let v: serde_json::Value =
            json(self.http.get(self.url(&format!("/discovery/find/{service}"))).send().await?).await?;
        Ok(v["providers"]
            .as_array()
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default())
    }

    /// Pay a relay for relay service over the mesh (`POST /relay/pay`). Signs a payment-channel
    /// receipt authorising `cumulative` total to the relay (the channel host) and sends it; the
    /// relay verifies it against the channel and its price. Requires an open channel with the
    /// relay; re-call periodically with a rising `cumulative` to keep paying for ongoing relaying.
    pub async fn pay_relay(&self, relay: &str, channel_id: &str, cumulative: Amount) -> Result<()> {
        let body = serde_json::json!({
            "relay": relay,
            "channel_id": channel_id,
            "cumulative": cumulative,
        });
        ok(self.http.post(self.url("/relay/pay")).json(&body).send().await?).await
    }

    /// Stop a job on a specific remote host (`POST /mesh-kill`).
    pub async fn mesh_kill(&self, node_id: &str, job_id: &str, grant: Option<&str>) -> Result<()> {
        let body = serde_json::json!({ "node_id": node_id, "job_id": job_id, "grant": grant });
        ok(self.http.post(self.url("/mesh-kill")).json(&body).send().await?).await
    }

    /// Force-stop a local job by id (`DELETE /jobs/:id`).
    pub async fn kill(&self, job_id: &str) -> Result<()> {
        ok(self.http.delete(self.url(&format!("/jobs/{job_id}"))).send().await?).await
    }

    // ----- payment channels (docs/payment-channels.md) -----

    /// Open an off-chain payment channel paying `host`, locking `capacity` (`POST /channels/open`).
    /// Returns the channel id. `expiry_height` 0 uses the node's default lifetime.
    pub async fn channel_open(&self, host: &str, capacity: Amount, expiry_height: u64) -> Result<String> {
        let body = serde_json::json!({ "host": host, "capacity": capacity, "expiry_height": expiry_height });
        let v: serde_json::Value = json(self.http.post(self.url("/channels/open")).json(&body).send().await?).await?;
        Ok(v["channel_id"].as_str().unwrap_or_default().to_string())
    }

    /// Sign an off-chain receipt as the payer for `cumulative` total paid (`POST /channels/receipt`).
    /// Hand the returned receipt to the host; they redeem the highest one to settle.
    pub async fn sign_receipt(&self, channel_id: &str, host: &str, cumulative: Amount) -> Result<Receipt> {
        let body = serde_json::json!({ "channel_id": channel_id, "host": host, "cumulative": cumulative });
        json(self.http.post(self.url("/channels/receipt")).json(&body).send().await?).await
    }

    /// Redeem a receipt to close a channel (call on the host node) (`POST /channels/:id/close`).
    pub async fn channel_close(&self, channel_id: &str, cumulative: Amount, payer_sig: &str) -> Result<()> {
        let body = serde_json::json!({ "cumulative": cumulative, "payer_sig": payer_sig });
        ok(self.http.post(self.url(&format!("/channels/{channel_id}/close"))).json(&body).send().await?).await
    }

    /// Reclaim a channel after expiry (call on the payer node) (`POST /channels/:id/expire`).
    pub async fn channel_expire(&self, channel_id: &str) -> Result<()> {
        ok(self.http.post(self.url(&format!("/channels/{channel_id}/expire"))).send().await?).await
    }

    /// List open payment channels (`GET /channels`).
    pub async fn channels(&self) -> Result<Vec<Channel>> {
        json(self.http.get(self.url("/channels")).send().await?).await
    }
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

// ----- wire types (mirror the node's JSON; amounts are base-unit strings) -----

#[derive(Debug, Clone, Deserialize)]
pub struct NodeStatus {
    pub node_id: String,
    pub height: u64,
    pub difficulty: u8,
    pub balance: Amount,
}

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

/// A bid / deploy spec. Used by [`CeClient::bid`] and [`CeClient::mesh_deploy`].
#[derive(Debug, Clone, Serialize)]
pub struct BidSpec {
    pub image: String,
    #[serde(default)]
    pub cmd: Vec<String>,
    pub cpu_cores: u32,
    pub mem_mb: u64,
    pub duration_secs: u64,
    /// Funding committed for the job.
    pub bid: Amount,
}

/// A job record. Fields present depend on the endpoint (`/jobs` vs `/jobs/:id`).
#[derive(Debug, Clone, Deserialize)]
pub struct Job {
    pub job_id: String,
    pub status: String,
    #[serde(default)]
    pub payer: Option<String>,
    #[serde(default)]
    pub container_id: Option<String>,
    #[serde(default)]
    pub cost: Option<Amount>,
    #[serde(default)]
    pub bid: Option<Amount>,
}

impl Job {
    pub fn is_running(&self) -> bool {
        self.status == "running"
    }
}

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

/// An open payment channel.
#[derive(Debug, Clone, Deserialize)]
pub struct Channel {
    pub channel_id: String,
    pub payer: String,
    pub host: String,
    pub capacity: Amount,
    pub expiry_height: u64,
}

/// A signed off-chain payment receipt (the payer authorizes `cumulative` total to the host).
#[derive(Debug, Clone, Deserialize)]
pub struct Receipt {
    pub channel_id: String,
    pub cumulative: Amount,
    /// Payer's signature (128 hex), redeemed by the host via `channel_close`.
    pub payer_sig: String,
}

/// The result of a mesh deploy: the job id, plus an output CID when the workload ran to completion
/// and produced a captured artifact (a WASI command's stdout — fetch it with [`CeClient::get_object`]
/// or `get_blob`). `output` is `None` for detached/streaming cells.
#[derive(Debug, Clone, Deserialize)]
pub struct Deployment {
    pub job_id: String,
    #[serde(default)]
    pub output: Option<String>,
}

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

/// A node's interaction history — the reputation substrate. Immutable facts from the chain;
/// derive your own trust from them.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeHistory {
    pub node_id: String,
    pub jobs_hosted: u64,
    pub jobs_paid: u64,
    pub heartbeats_hosted: u64,
    pub heartbeats_paid: u64,
    pub expiries: u64,
    pub earned: Amount,
    pub spent: Amount,
    pub first_height: u64,
    pub last_height: u64,
}

impl NodeHistory {
    /// A node with no recorded interactions — a stranger (starts at the bottom of the
    /// trust gradient; only watchable/redundant work).
    pub fn is_newcomer(&self) -> bool {
        self.first_height == 0
    }

    /// A simple default trust heuristic: total work this host delivered and was paid for
    /// (settled jobs + heartbeats received). Higher = more proven. Apps may define their own.
    pub fn delivered_work(&self) -> u64 {
        self.jobs_hosted + self.heartbeats_hosted
    }
}
