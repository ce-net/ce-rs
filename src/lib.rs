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

    /// Run a one-shot command in a sandboxed container on a **specific** host over the mesh
    /// and return its output synchronously (`POST /mesh-exec`). This is the scatter/gather
    /// primitive: fan a command out across hosts and collect each result.
    ///
    /// (v0 targets admin-trusted hosts; grant forwarding through the proxy is a pending
    /// node-side enhancement, so this takes no grant yet.)
    pub async fn mesh_exec(&self, node_id: &str, image: &str, cmd: &[String]) -> Result<ExecResult> {
        let body = serde_json::json!({ "node_id": node_id, "image": image, "cmd": cmd });
        json(self.http.post(self.url("/mesh-exec")).json(&body).send().await?).await
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
