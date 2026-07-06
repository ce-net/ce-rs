//! Lane probe — drive mesh-app ops through a local node under an app name the host may
//! interpose. The SDK rides the node's shared-memory lane transparently when
//! `<data_dir>/lane.sock` exists; `$CE_LANE_APP` names the app so the host's
//! `membrane-policy.toml` can compose an adapter chain (e.g. `economy-adapter`) onto its node
//! channel. The probe itself cannot tell whether it was interposed — that is the transparency
//! invariant — so a gate reads the ADAPTER's evidence (its settled usage record, and the node's
//! `via [...]` bind log), not this program's output.
//!
//! ```text
//! CE_LANE_APP=lane-probe cargo run --example lane_probe    # beside a running node
//! PROBE_MSGS=64 CE_LANE_APP=lane-probe cargo run --example lane_probe
//! ```

use ce_rs::CeClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ce = CeClient::local();
    let n: u32 = std::env::var("PROBE_MSGS").ok().and_then(|v| v.parse().ok()).unwrap_or(16);

    let status = ce.status().await?;
    println!(
        "node {} (economy: {})",
        &status.node_id[..8.min(status.node_id.len())],
        status.economy_enabled()
    );

    for i in 0..n {
        ce.publish("lane-probe", format!("probe message {i}").as_bytes()).await?;
    }
    println!(
        "published {n} messages on topic 'lane-probe' as app '{}'",
        std::env::var("CE_LANE_APP").unwrap_or_else(|_| "(unnamed)".into())
    );
    Ok(())
}
