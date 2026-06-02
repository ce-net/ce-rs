//! Upload an object to the data layer and read it back by CID.
//!
//! Run against a local CE node:
//!
//! ```text
//! ce start &                       # node listening on :8844
//! cargo run --example object_roundtrip
//! ```
//!
//! `put_object` chunks the bytes (1 MiB), stores each chunk + a manifest in the content-addressed
//! blob store, and returns the object CID (the manifest hash). `get_object` resolves the manifest,
//! pulls every chunk, verifies each against its CID, and reassembles — trustless by construction.

use ce_rs::CeClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ce = CeClient::local();

    // A 3 MiB payload spanning several chunks.
    let original: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();

    let object_cid = ce.put_object(&original).await?;
    println!("stored {} bytes as {object_cid}", original.len());

    let fetched = ce.get_object(&object_cid).await?;
    assert_eq!(fetched, original, "round-trip mismatch");
    println!("fetched {} bytes back, verified identical", fetched.len());

    Ok(())
}
