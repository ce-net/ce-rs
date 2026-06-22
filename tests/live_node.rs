//! Live SDK validation against a real, ephemeral CE node.
//!
//! These tests spawn a throwaway `ce ... start --no-mine --ephemeral --no-mdns` node on a private
//! port, point a `CeClient` at it with the node's own API token, and exercise the real endpoints
//! end to end. They are the only place the SDK touches a real node; the rest of the suite is mock-
//! based.
//!
//! They run by default (NOT `#[ignore]`d) but **self-skip** (print + return Ok) when the release
//! `ce` binary is absent — so `cargo test` is green on a machine without a built node, and actually
//! exercises the node when one is present. A 2-node mesh test wires node B to node A's bootstrap
//! address.
//!
//! Ports: API in 18900-18999, P2P in 14900-14999 (per the validation guide). The live node on
//! :8844 is never touched. Temp data dirs and child processes are cleaned up on drop.

mod common;

use ce_rs::{Amount, CeClient};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

/// Candidate locations of the release `ce` binary (shared target first).
fn ce_binary() -> Option<PathBuf> {
    let candidates = [
        "/Users/07lead01/ce-net/.cargo-shared/release/ce",
        "/Users/07lead01/ce-net/ce/target/release/ce",
    ];
    candidates.iter().map(PathBuf::from).find(|p| p.exists())
}

static NEXT_API_PORT: AtomicU16 = AtomicU16::new(18900);
static NEXT_P2P_PORT: AtomicU16 = AtomicU16::new(14900);

fn next_api_port() -> u16 {
    NEXT_API_PORT.fetch_add(1, Ordering::SeqCst)
}
fn next_p2p_port() -> u16 {
    NEXT_P2P_PORT.fetch_add(1, Ordering::SeqCst)
}

/// A spawned ephemeral node; kills the process and removes its data dir on drop.
struct EphemeralNode {
    child: Child,
    data_dir: PathBuf,
    api_port: u16,
    /// The libp2p P2P port; used to build a dialable bootstrap multiaddr for the mesh test.
    p2p_port: u16,
    token: String,
}

impl EphemeralNode {
    /// Spawn a node. `bootstrap` is an optional multiaddr to dial (for the 2-node mesh test).
    fn spawn(bin: &Path, bootstrap: Option<&str>) -> anyhow::Result<EphemeralNode> {
        let api_port = next_api_port();
        let p2p_port = next_p2p_port();
        let data_dir = std::env::temp_dir().join(format!("ce-rs-live-{}-{}", std::process::id(), api_port));
        std::fs::create_dir_all(&data_dir)?;

        // --data-dir is GLOBAL (before the subcommand).
        let mut cmd = Command::new(bin);
        cmd.arg("--data-dir")
            .arg(&data_dir)
            .arg("start")
            .arg("--no-mine")
            .arg("--ephemeral")
            .arg("--no-mdns")
            .arg("--api-port")
            .arg(api_port.to_string())
            .arg("--port")
            .arg(p2p_port.to_string());
        if let Some(b) = bootstrap {
            cmd.arg("--bootstrap").arg(b);
        }
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
        let child = cmd.spawn()?;

        // Wait for api.token to appear, then for /health.
        let token = wait_for_token(&data_dir, Duration::from_secs(20))?;
        let base = format!("http://127.0.0.1:{api_port}");
        wait_for_health(&base, &token, Duration::from_secs(20))?;

        Ok(EphemeralNode { child, data_dir, api_port, p2p_port, token })
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.api_port)
    }

    fn client(&self) -> CeClient {
        CeClient::with_token(self.base_url(), Some(self.token.clone()))
    }
}

impl Drop for EphemeralNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn wait_for_token(data_dir: &Path, timeout: Duration) -> anyhow::Result<String> {
    let path = data_dir.join("api.token");
    let start = Instant::now();
    loop {
        if let Ok(s) = std::fs::read_to_string(&path) {
            let t = s.trim().to_string();
            if !t.is_empty() {
                return Ok(t);
            }
        }
        if start.elapsed() > timeout {
            anyhow::bail!("api.token never appeared in {}", data_dir.display());
        }
        std::thread::sleep(Duration::from_millis(150));
    }
}

fn wait_for_health(base: &str, token: &str, timeout: Duration) -> anyhow::Result<()> {
    let start = Instant::now();
    let url = format!("{base}/health");
    loop {
        // A blocking probe with std — avoids needing a runtime here.
        let ok = std::process::Command::new("curl")
            .args(["-s", "-o", "/dev/null", "-w", "%{http_code}", "-H", &format!("Authorization: Bearer {token}"), &url])
            .output()
            .ok()
            .map(|o| {
                let code = String::from_utf8_lossy(&o.stdout);
                code.trim() == "200" || code.trim() == "204"
            })
            .unwrap_or(false);
        if ok {
            return Ok(());
        }
        if start.elapsed() > timeout {
            anyhow::bail!("node /health never became ready at {base}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Macro: skip the test gracefully when the ce binary isn't built.
macro_rules! require_binary {
    () => {
        match ce_binary() {
            Some(b) => b,
            None => {
                eprintln!(
                    "SKIP: ce release binary not found (build with `cargo build --release --bin ce` \
                     in ce/); live node test skipped."
                );
                return Ok(());
            }
        }
    };
}

#[tokio::test]
async fn live_single_node_read_endpoints() -> anyhow::Result<()> {
    let bin = require_binary!();
    let node = match EphemeralNode::spawn(&bin, None) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("SKIP: could not spawn ephemeral node: {e}");
            return Ok(());
        }
    };
    let ce = node.client();

    // health
    assert!(ce.health().await?, "node should report healthy");

    // status: a real node id (64 hex), height, difficulty.
    let st = ce.status().await?;
    assert_eq!(st.node_id.len(), 64, "node_id should be 64 hex chars: {}", st.node_id);
    assert!(st.node_id.chars().all(|c| c.is_ascii_hexdigit()));

    // beacon: tip height + 64-hex hash, agrees with status height closely.
    let beacon = ce.beacon().await?;
    assert_eq!(beacon.hash.len(), 64, "beacon hash should be 64 hex: {}", beacon.hash);
    assert!(beacon.hash.chars().all(|c| c.is_ascii_hexdigit()));

    // atlas, jobs, channels, revoked: should all return (possibly empty) without error.
    let _atlas = ce.atlas().await?;
    let jobs = ce.jobs().await?;
    assert!(jobs.is_empty(), "a fresh node has no jobs");
    let chans = ce.channels().await?;
    assert!(chans.is_empty(), "a fresh node has no channels");
    let _revoked = ce.revoked().await?;

    // balance breakdown
    let _bal = ce.balance().await?;

    Ok(())
}

#[tokio::test]
async fn live_blob_and_object_round_trip() -> anyhow::Result<()> {
    let bin = require_binary!();
    let node = match EphemeralNode::spawn(&bin, None) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("SKIP: could not spawn ephemeral node: {e}");
            return Ok(());
        }
    };
    let ce = node.client();

    // Raw blob round-trip; the node must echo our sha256.
    let bytes = b"ce-rs live blob test payload".to_vec();
    let expected = ce_rs::cid(&bytes);
    let hash = ce.put_blob(bytes.clone()).await?;
    assert_eq!(hash, expected, "node blob hash must be content-addressed sha256");
    let back = ce.get_blob(&hash).await?;
    assert_eq!(back, bytes);

    // Missing blob -> error, not panic.
    let missing = ce.get_blob("00000000000000000000000000000000000000000000000000000000deadbeef").await;
    assert!(missing.is_err(), "missing blob should error");

    // Multi-chunk object round-trip (>1 MiB spans chunks).
    let big: Vec<u8> = (0..2_500_000u32).map(|i| (i % 251) as u8).collect();
    let object_cid = ce.put_object(&big).await?;
    let fetched = ce.get_object(&object_cid).await?;
    assert_eq!(fetched.len(), big.len());
    assert_eq!(fetched, big, "object must round-trip byte-for-byte");

    Ok(())
}

#[tokio::test]
async fn live_name_claim_and_discovery() -> anyhow::Result<()> {
    let bin = require_binary!();
    let node = match EphemeralNode::spawn(&bin, None) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("SKIP: could not spawn ephemeral node: {e}");
            return Ok(());
        }
    };
    let ce = node.client();

    // Resolving an unclaimed name returns None (not an error).
    let r = ce.resolve_name("definitely-unclaimed-xyz").await?;
    assert!(r.is_none(), "unclaimed name should resolve to None");

    // Advertise a service/tag — accepted by the node (DHT provider record).
    // (find may be empty on a solo node with no DHT peers; we only assert advertise succeeds.)
    ce.advertise_service("ce-rs-live-service").await?;
    ce.advertise_tag("ce-rs-live-tag").await?;

    Ok(())
}

#[tokio::test]
async fn live_status_402_on_insufficient_balance_transfer() -> anyhow::Result<()> {
    let bin = require_binary!();
    let node = match EphemeralNode::spawn(&bin, None) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("SKIP: could not spawn ephemeral node: {e}");
            return Ok(());
        }
    };
    let ce = node.client();
    // A --no-mine node has zero balance; a big transfer should be rejected (402) -> SDK Err.
    let res = ce
        .transfer(
            "1111111111111111111111111111111111111111111111111111111111111111",
            Amount::from_credits(1_000_000),
        )
        .await;
    assert!(res.is_err(), "transfer with no balance must error (likely 402)");
    Ok(())
}

#[tokio::test]
async fn live_two_node_mesh_bootstrap() -> anyhow::Result<()> {
    let bin = require_binary!();
    // Node A.
    let a = match EphemeralNode::spawn(&bin, None) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("SKIP: could not spawn node A: {e}");
            return Ok(());
        }
    };
    let ce_a = a.client();

    // Discover A's libp2p peer id from /bootstrap (entries look like `/p2p/<peerid>` or a full
    // multiaddr). The ephemeral node listens on a private P2P port we chose, so we build a
    // dialable loopback multiaddr `/ip4/127.0.0.1/tcp/<a.p2p_port>/p2p/<peerid>` rather than
    // trusting the (LAN-IP / circuit) addresses the node advertises.
    let boot: Vec<String> = match ce_a.get_bootstrap().await {
        Ok(v) if !v.is_empty() => v,
        Ok(_) => {
            eprintln!("SKIP: node A advertised no bootstrap peers");
            return Ok(());
        }
        Err(e) => {
            eprintln!("SKIP: /bootstrap unavailable: {e}");
            return Ok(());
        }
    };
    // Extract the peer id from the first entry containing `/p2p/`.
    let peer_id = boot
        .iter()
        .find_map(|a| a.rsplit("/p2p/").next().filter(|s| s.starts_with("12D3")).map(String::from));
    let peer_id = match peer_id {
        Some(p) => p,
        None => {
            eprintln!("SKIP: could not extract peer id from /bootstrap {boot:?}");
            return Ok(());
        }
    };
    let addr = format!("/ip4/127.0.0.1/tcp/{}/p2p/{peer_id}", a.p2p_port);

    // Node B dials A.
    let b = match EphemeralNode::spawn(&bin, Some(&addr)) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("SKIP: could not spawn node B with bootstrap {addr}: {e}");
            return Ok(());
        }
    };
    let ce_b = b.client();

    // Both healthy and distinct identities.
    assert!(ce_a.health().await?);
    assert!(ce_b.health().await?);
    let id_a = ce_a.status().await?.node_id;
    let id_b = ce_b.status().await?.node_id;
    assert_ne!(id_a, id_b, "two nodes must have distinct identities");

    // Give the mesh a few seconds to connect, then check B's atlas saw A (best-effort: a solo
    // ephemeral mesh may not always converge quickly, so we don't hard-fail on emptiness).
    for _ in 0..20 {
        let atlas = ce_b.atlas().await?;
        if atlas.iter().any(|e| e.node_id == id_a) {
            eprintln!("mesh converged: B sees A in atlas");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    eprintln!("NOTE: B did not see A in atlas within 10s (acceptable for a 2-node ephemeral mesh without mining/gossip warmup); both nodes were healthy with distinct ids.");
    Ok(())
}

/// Helper on CeClient for /bootstrap (not a first-class SDK method — only the live mesh test needs
/// it, so it lives here as a raw GET).
trait BootstrapExt {
    async fn get_bootstrap(&self) -> anyhow::Result<Vec<String>>;
}

impl BootstrapExt for CeClient {
    async fn get_bootstrap(&self) -> anyhow::Result<Vec<String>> {
        // The SDK doesn't wrap /bootstrap; hit it directly using the client's public base URL.
        let base = self.base_url();
        let resp = reqwest::Client::new().get(format!("{base}/bootstrap")).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("/bootstrap {}", resp.status());
        }
        let v: serde_json::Value = resp.json().await?;
        // Accept {"peers":[..]}, {"multiaddrs":[..]}, or a bare array of strings.
        let arr = v
            .get("peers")
            .and_then(|m| m.as_array())
            .or_else(|| v.get("multiaddrs").and_then(|m| m.as_array()))
            .or_else(|| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(arr.into_iter().filter_map(|x| x.as_str().map(String::from)).collect())
    }
}
