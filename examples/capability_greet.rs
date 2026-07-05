//! Worked example of a typed capability: define once, `provide` it, `call` it.
//!
//! This is the "functions and SDKs" layer in action — the caller invokes a mesh function with full
//! type safety and never touches transport, discovery, or serialization.
//!
//! Run against a live local node (`ce start`), two terminals:
//!
//! ```text
//! # Terminal 1 — become a live provider of the `demo.greet` capability:
//! cargo run --example capability_greet --features capability -- provide
//!
//! # Terminal 2 — call it (locates the provider over the mesh, typed round-trip):
//! cargo run --example capability_greet --features capability -- call Ada
//! # -> hello Ada (from node <hex>)
//! ```
//!
//! Both sides share the ONE `Greet` capability type below — that shared contract is the whole point.

use ce_rs::capability::{call, provide, Caller, Capability};
use ce_rs::CeClient;
use serde::{Deserialize, Serialize};

// The capability, defined ONCE. Provider and caller both use this type.
#[derive(Serialize, Deserialize)]
pub struct GreetReq {
    pub name: String,
}
#[derive(Serialize, Deserialize)]
pub struct GreetResp {
    pub text: String,
}
pub struct Greet;
impl Capability for Greet {
    const NAME: &'static str = "demo.greet";
    type Req = GreetReq;
    type Resp = GreetResp;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ce = CeClient::local();
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("provide") => {
            let me = ce.status().await?.node_id;
            eprintln!("providing capability '{}' as node {me}; ctrl-c to stop", Greet::NAME);
            provide::<Greet, _>(
                &ce,
                move |req: GreetReq, caller: Caller| {
                    let me = me.clone();
                    async move {
                        // A sensitive capability would verify a ce-cap chain against caller.from here.
                        eprintln!("  greet request from {}", caller.from);
                        Ok(GreetResp { text: format!("hello {} (from node {me})", req.name) })
                    }
                },
                // Serve until the process is stopped (ctrl-c / SIGINT).
                std::future::pending::<()>(),
            )
            .await?;
        }
        Some("call") => {
            let name = args.next().unwrap_or_else(|| "world".to_string());
            let resp = call::<Greet>(&ce, &GreetReq { name }).await?;
            println!("{}", resp.text);
        }
        _ => {
            eprintln!("usage: capability_greet <provide | call [name]>");
            std::process::exit(2);
        }
    }
    Ok(())
}
