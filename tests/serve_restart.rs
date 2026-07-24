//! Live reproduction of the serve deafness bug: a `ce_rs::serve` loop must survive a restart of
//! its local ce node.
//!
//! The node keeps `/mesh/subscribe` state in memory only. When the node restarts, the serve loop's
//! SSE reconnect (which always worked) reattaches `/mesh/messages/stream` — but without
//! re-subscribing, the node never routes requests to the app again, so every request to the
//! service times out ("peer app did not reply in time") forever. Observed live three times on
//! 2026-07-24. This test is the permanent reproduction: it spawns a REAL throwaway node, serves a
//! topic, proves a round-trip, SIGKILLs the node, restarts it on the same data-dir/ports, and
//! asserts the service answers again within a generous window.
//!
//! `#[ignore]`d because it spawns a real node process; run with:
//! `cargo test --features serve --test serve_restart -- --ignored`
//!
//! The throwaway node uses a pid-derived private port pair and a temp data dir; it never touches
//! the machine's real node (default ports/data dir) and cleans up on drop.
#![cfg(feature = "serve")]

use ce_rs::serve::{serve, Handler, Request};
use ce_rs::CeClient;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Locate the `ce` binary: `CE_BIN` overrides; otherwise the installed CLI, then `$PATH`.
fn ce_binary() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CE_BIN") {
        let p = PathBuf::from(p);
        return p.exists().then_some(p);
    }
    if let Ok(home) = std::env::var("HOME") {
        let p = PathBuf::from(home).join(".local/bin/ce");
        if p.exists() {
            return Some(p);
        }
    }
    let out = Command::new("which").arg("ce").output().ok()?;
    let p = PathBuf::from(String::from_utf8_lossy(&out.stdout).trim());
    (out.status.success() && p.exists()).then_some(p)
}

/// Kills the node process and removes the data dir even when an assertion panics mid-test.
struct NodeGuard {
    child: Option<Child>,
    data_dir: PathBuf,
}

impl NodeGuard {
    fn kill_node(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl Drop for NodeGuard {
    fn drop(&mut self) {
        self.kill_node();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

/// Spawn a throwaway node on private ports. `--foreground` keeps it a plain child process (no
/// service install); `--no-mdns` isolates it from any real node on the LAN.
fn spawn_node(bin: &Path, data_dir: &Path, api_port: u16, p2p_port: u16) -> anyhow::Result<Child> {
    let child = Command::new(bin)
        .arg("--data-dir")
        .arg(data_dir)
        .arg("start")
        .arg("--foreground")
        .arg("--no-mdns")
        .arg("--api-port")
        .arg(api_port.to_string())
        .arg("--port")
        .arg(p2p_port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    Ok(child)
}

async fn wait_for_token(data_dir: &Path, timeout: Duration) -> anyhow::Result<String> {
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
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

async fn wait_healthy(base: &str, timeout: Duration) -> anyhow::Result<()> {
    let start = Instant::now();
    let http = reqwest::Client::new();
    loop {
        if let Ok(resp) = http.get(format!("{base}/health")).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        if start.elapsed() > timeout {
            anyhow::bail!("node never became healthy at {base}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll a directed request to the served topic until it round-trips "pong" or the deadline passes.
/// Polling (rather than one fixed-timeout shot) also absorbs the serve loop's startup: the test
/// asserts "answers within the window", the invariant that actually matters to callers.
async fn request_until_pong(
    ce: &CeClient,
    node_id: &str,
    topic: &str,
    deadline: Duration,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let mut last_err = String::from("never attempted");
    while start.elapsed() < deadline {
        match ce.request(node_id, topic, b"ping", 3_000).await {
            Ok(reply) if reply == b"pong" => return Ok(()),
            Ok(other) => last_err = format!("unexpected reply: {other:?}"),
            Err(e) => last_err = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("no pong within {deadline:?}; last error: {last_err}")
}

struct Pong;
impl Handler for Pong {
    async fn handle(&self, _req: Request) -> Vec<u8> {
        b"pong".to_vec()
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns a real throwaway ce node; run with -- --ignored"]
async fn serve_survives_node_restart() -> anyhow::Result<()> {
    // Force the HTTP path: without this, a client targeting 127.0.0.1 would try the DEFAULT data
    // dir's lane socket — the machine's REAL node — which this test must never touch.
    // SAFETY: set at test entry, before this test creates any client; this is the only test in
    // this binary, so no concurrent test races the environment.
    unsafe { std::env::set_var("CE_NO_LANE", "1") };

    let Some(bin) = ce_binary() else {
        eprintln!("SKIP: no ce binary found (install to ~/.local/bin/ce); live restart test skipped.");
        return Ok(());
    };

    // Pid-derived private ports so parallel runs never clash (and never hit real node ports).
    let pid = std::process::id();
    let api_port: u16 = 19_600 + (pid % 300) as u16;
    let p2p_port: u16 = 26_600 + (pid % 300) as u16;
    let base = format!("http://127.0.0.1:{api_port}");
    let topic = "serve-restart-test/rpc";

    let data_dir = std::env::temp_dir().join(format!("ce-rs-serve-restart-{pid}"));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir)?;

    let mut guard =
        NodeGuard { child: Some(spawn_node(&bin, &data_dir, api_port, p2p_port)?), data_dir: data_dir.clone() };
    let token = wait_for_token(&data_dir, Duration::from_secs(30)).await?;
    wait_healthy(&base, Duration::from_secs(30)).await?;

    let ce = CeClient::with_token(base.clone(), Some(token.clone()));
    let node_id = ce.status().await?.node_id;

    // The service under test: a real serve loop on this node, answering "pong".
    let serve_ce = ce.clone();
    let server_task = tokio::spawn(async move {
        let h = Pong;
        let _ = serve(&serve_ce, &[topic], &h, std::future::pending::<()>()).await;
    });

    // 1. Baseline: the service answers.
    request_until_pong(&ce, &node_id, topic, Duration::from_secs(30))
        .await
        .expect("baseline round-trip to a freshly served topic failed");

    // 2. Kill the node. Its in-memory /mesh/subscribe state dies with it.
    guard.kill_node();

    // 3. Restart on the SAME data dir and ports (same identity, same token).
    guard.child = Some(spawn_node(&bin, &data_dir, api_port, p2p_port)?);
    let token2 = wait_for_token(&data_dir, Duration::from_secs(30)).await?;
    assert_eq!(
        token2, token,
        "node rotated api.token across restart; the serve loop's held client would be unauthorized \
         and this test's premise (restart with stable auth) is broken"
    );
    wait_healthy(&base, Duration::from_secs(30)).await?;

    // 4. THE bug: without re-subscribing on stream reconnect, the service is permanently deaf now.
    let second = request_until_pong(&ce, &node_id, topic, Duration::from_secs(30)).await;
    assert!(
        second.is_ok(),
        "service went permanently deaf after its node restarted (serve never re-subscribed): {:?}",
        second.err()
    );

    server_task.abort();
    Ok(())
}
