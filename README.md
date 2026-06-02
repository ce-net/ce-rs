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

## What it covers (v0)

The unauthenticated local-node API:

- `status`, `health`, `atlas`
- `transfer`
- `bid` (broadcast placement), `jobs`, `job`, `kill`
- `mesh_deploy` / `mesh_kill` (directed placement on a specific host over the mesh)

**Money is integer base units.** `Amount` wraps `i128` base units (`1 credit = 10^18`), never
floats, and (de)serializes as a decimal string (amounts exceed JSON's 2^53 limit). Use
`Amount::from_credits(n)`, `Amount::parse_credits("1.5")`, and `.credits()` for display.

## Planned

CE-auth request signing (for direct-to-remote `/exec`,`/sync`), SSE subscriptions
(`/blocks/stream`, `/signals/stream`, `/transactions/stream`), and grant issuing helpers.

## License

MIT © Leif Rydenfalk
