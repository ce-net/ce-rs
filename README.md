# ce-rs

Rust SDK for [CE](https://github.com/ce-net/ce) — a typed, async client for talking to a local
CE node's HTTP API. Build apps (schedulers, dashboards, bots) on the CE compute mesh without
hand-rolling JSON.

```toml
[dependencies]
ce-rs = { git = "https://github.com/ce-net/ce-rs" }
```

```rust
use ce_rs::{CeClient, BidSpec, Amount};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ce = CeClient::local(); // http://127.0.0.1:8844

    let status = ce.status().await?;
    println!("node {} at height {} — balance {}", status.node_id, status.height, status.balance);

    // Discover a GPU host and place a job on it directly (mesh-routed).
    for host in ce.atlas().await? {
        if host.has_tag("gpu") {
            let spec = BidSpec {
                image: "alpine:latest".into(),
                cmd: vec!["echo".into(), "hello".into()],
                cpu_cores: 1, mem_mb: 128, duration_secs: 60,
                bid: Amount::from_credits(10),
            };
            let job_id = ce.mesh_deploy(&host.node_id, &spec, None).await?;
            println!("deployed {job_id} on {}", host.node_id);
            break;
        }
    }
    Ok(())
}
```

## What it covers

A typed async client over the full node HTTP + SSE API:

- Status / reads: `status`, `health`, `atlas`, `beacon`, `netgraph`, `history`, `bootstrap`
- Money: `transfer`, `wallet` module (`Wallet`, `Balance`, `TxRecord`, `TxQuery`, `Direction`)
- Jobs: `bid` (broadcast placement), `jobs`, `job`, `kill`, `mesh_deploy` / `mesh_kill`
  (directed placement on a specific host over the mesh), `mesh_deploy_wasm`
- App messaging: `send_message`, `messages`, `subscribe`, `publish`, `request`, `reply`
- Payment channels: `channel_open`, `sign_receipt`, `channel_close`, `channel_expire`, `channels`
- Naming / discovery: `claim_name`, `resolve_name`, `advertise_service`, `find_service`
- Blobs / objects: `put_blob`, `get_blob`, `put_object`, `get_object`, `fetch_chunk_paid`
- Capabilities: `revoked`; relay: `pay_relay`
- SSE streams: `blocks_stream`, `transactions_stream_events`, `signals_stream`, `messages_stream`

**Feature flags.** `serve` adds the mesh-app request/reply loop (`serve` / `serve_where` +
`Handler` / `Request` in `src/serve.rs`). `locate` adds service discovery (`locate` / `call` /
`register` + `Instance` / `LocateOpts` in `src/locate.rs`).

**Money is integer base units.** `Amount` wraps `i128` base units (`1 credit = 10^18`), never
floats, and (de)serializes as a decimal string (amounts exceed JSON's 2^53 limit). Use
`Amount::from_credits(n)`, `Amount::parse_credits("1.5")`, and `.credits()` for display.

Note: remote `exec`/`sync` are deliberately NOT in this SDK — they moved to the `rdev` app
(built on CE primitives). The SDK holds no key material and reaches peers only through the
local node's mesh HTTP endpoints.

## License

AGPL-3.0-only © Leif Rydenfalk. A commercial license is also available — see
[`LICENSING.md`](./LICENSING.md) and [`COMMERCIAL-LICENSE.md`](./COMMERCIAL-LICENSE.md).
