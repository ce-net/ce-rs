//! Buy interposed lane access from a host's economy adapter — the payer side of the mesh
//! economy. Opens (or reuses) a payment channel to the HOST NODE, signs a receipt raising its
//! cumulative by the budget wanted, sends an `admit` request to the host's `economy-adapter`
//! mesh service, and prints the capability to present on a lane bind.
//!
//! ```text
//! HOST=<host node id hex> BUDGET=50 cargo run --example buy_lane
//! # then, on the host, run the paid app with the printed CE_LANE_CAPS
//! ```
//!
//! Env: `HOST` (required, 64 hex); `BUDGET` (base units of new credit to buy, default 50);
//! `CHANNEL_ID` + `CUMULATIVE` to reuse an open channel at a known prior cumulative (default:
//! open a fresh channel of `CAPACITY`, default 10x budget, and pay from zero).

use ce_rs::{Amount, CeClient};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ce = CeClient::local();
    let host = std::env::var("HOST").map_err(|_| anyhow::anyhow!("set HOST=<host node id hex>"))?;
    let budget: u128 =
        std::env::var("BUDGET").ok().and_then(|v| v.parse().ok()).unwrap_or(50);

    // The paying channel: reuse one (CHANNEL_ID + the cumulative already signed away on it), or
    // open a fresh one and wait for it to appear in the chain view.
    let (channel_id, prior): (String, u128) = match std::env::var("CHANNEL_ID") {
        Ok(id) => {
            let prior = std::env::var("CUMULATIVE").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
            (id, prior)
        }
        Err(_) => {
            let capacity: u128 = std::env::var("CAPACITY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(budget.saturating_mul(10));
            let id = ce
                .channel_open(&host, Amount::from_base(capacity.min(i128::MAX as u128) as i128), 0)
                .await?;
            println!("opened channel {id} (capacity {capacity} base units), waiting for inclusion...");
            // The open is a transaction; wait until our own chain view lists it (the host's
            // view follows within a sync round).
            for _ in 0..60 {
                if ce.channels().await?.iter().any(|c| c.channel_id == id) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            (id, 0)
        }
    };

    // Sign the receipt: cumulative = everything this channel has ever paid + the new budget.
    let cumulative = prior.saturating_add(budget);
    let receipt = ce
        .sign_receipt(&channel_id, &host, Amount::from_base(cumulative.min(i128::MAX as u128) as i128))
        .await?;

    // Ask the host's economy adapter to admit us for that receipt.
    let req = serde_json::json!({
        "op": "admit",
        "channel_id": channel_id,
        "cumulative": cumulative.to_string(),
        "payer_sig": receipt.payer_sig,
        "min_credit": budget.to_string(),
    });
    let reply = ce
        .request(&host, "economy-adapter/rpc", &serde_json::to_vec(&req)?, 20_000)
        .await?;
    let v: serde_json::Value = serde_json::from_slice(&reply)?;
    if v["ok"] != true {
        anyhow::bail!("admission refused: {}", v["error"].as_str().unwrap_or("unknown"));
    }
    println!(
        "admitted: budget {} base units, cap nonce {} (issuer {}), expires {}",
        v["budget"].as_str().unwrap_or("?"),
        v["nonce"],
        v["issuer"].as_str().unwrap_or("?"),
        v["expires"]
    );
    println!("\nrun the paid app on the host with:");
    println!("  CE_LANE_CAPS={}", v["cap"].as_str().unwrap_or("?"));
    println!("\n(reuse this channel next time: CHANNEL_ID={channel_id} CUMULATIVE={cumulative})");
    Ok(())
}
