//! Content-addressed, chunked objects — the data layer's client-side core (Stage 1).
//!
//! An **object** (a file / dataset / model / WASM module) is split into fixed-size **chunks**,
//! each addressed by `cid = sha256(bytes)`. A **manifest** lists the chunk CIDs in order plus the
//! sizes; the manifest is itself stored as a blob, and the object's CID *is* the manifest's hash.
//! That makes a 1-level Merkle DAG: the manifest hash anchors every chunk hash, so any chunk can
//! be fetched independently and verified against its CID. Identical chunks share a CID (dedup).
//!
//! This module is pure (no network): [`chunk_object`] splits, [`reassemble`] verifies and rejoins.
//! [`CeClient::put_object`](crate::CeClient::put_object) /
//! [`get_object`](crate::CeClient::get_object) layer the `/blobs` HTTP store over it. The chunk
//! store, the manifest format, and verification are the substrate Stage 2 (mesh fetch-by-hash) and
//! Stage 4 (job I/O by CID) build on; see `ce/docs/data-layer.md`.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Default chunk size: 1 MiB. Large enough to amortise per-chunk overhead, small enough that a
/// failed or unpaid chunk costs little (the fair-exchange bound, see the design doc).
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

/// The content id of a byte slice: lowercase-hex sha256. Matches the node's `/blobs` keying, so a
/// CID computed here equals the hash the blob store returns for the same bytes.
pub fn cid(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// An object's chunk list. Stored as a blob; its own hash is the object's CID. Versioned by a
/// `kind` tag so future manifest shapes (trees, encrypted, content-defined chunking) can coexist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Discriminator for the manifest format. Always `"ce-object-v1"` in Stage 1.
    pub kind: String,
    /// Chunk size used when splitting (the last chunk may be smaller).
    pub chunk_size: u64,
    /// Total object size in bytes (sum of all chunks).
    pub total_size: u64,
    /// Ordered chunk CIDs (hex sha256).
    pub chunks: Vec<String>,
}

/// Manifest `kind` tag for the Stage 1 format.
pub const MANIFEST_KIND_V1: &str = "ce-object-v1";

impl Manifest {
    /// Is this a manifest this client understands?
    pub fn is_v1(&self) -> bool {
        self.kind == MANIFEST_KIND_V1
    }
}

/// Split `bytes` into `chunk_size` chunks, returning the manifest plus each `(cid, chunk_bytes)`.
/// The manifest is *not* included in the returned chunks — store it separately; its hash is the
/// object CID. An empty object yields an empty chunk list (and a zero-size manifest).
pub fn chunk_object(bytes: &[u8], chunk_size: usize) -> (Manifest, Vec<(String, Vec<u8>)>) {
    assert!(chunk_size > 0, "chunk_size must be positive");
    let mut chunks = Vec::new();
    let mut cids = Vec::new();
    for slice in bytes.chunks(chunk_size) {
        let id = cid(slice);
        cids.push(id.clone());
        chunks.push((id, slice.to_vec()));
    }
    let manifest = Manifest {
        kind: MANIFEST_KIND_V1.to_string(),
        chunk_size: chunk_size as u64,
        total_size: bytes.len() as u64,
        chunks: cids,
    };
    (manifest, chunks)
}

/// Reassemble an object from its manifest, pulling each chunk via `fetch` and verifying that every
/// chunk's bytes hash to its CID. A tampered or wrong-length result is an error — content
/// addressing makes the transfer trustless: you cannot be handed bytes you didn't ask for.
pub fn reassemble(
    manifest: &Manifest,
    mut fetch: impl FnMut(&str) -> Result<Vec<u8>>,
) -> Result<Vec<u8>> {
    if !manifest.is_v1() {
        bail!("unsupported manifest kind: {}", manifest.kind);
    }
    let mut out = Vec::with_capacity(manifest.total_size as usize);
    for chunk_cid in &manifest.chunks {
        let bytes = fetch(chunk_cid)?;
        let got = cid(&bytes);
        if got != *chunk_cid {
            bail!("chunk verification failed: expected {chunk_cid}, got {got}");
        }
        out.extend_from_slice(&bytes);
    }
    if out.len() as u64 != manifest.total_size {
        bail!(
            "reassembled size {} != manifest total_size {}",
            out.len(),
            manifest.total_size
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// chunk -> reassemble round-trips, and the chunk count matches the ceil-division.
    #[test]
    fn roundtrip_multichunk() {
        let data: Vec<u8> = (0..2_500_000u32).map(|i| (i % 251) as u8).collect();
        let (manifest, chunks) = chunk_object(&data, DEFAULT_CHUNK_SIZE);

        assert_eq!(manifest.total_size, 2_500_000);
        assert_eq!(manifest.chunk_size, DEFAULT_CHUNK_SIZE as u64);
        // 2_500_000 / 1_048_576 -> 3 chunks (1 MiB + 1 MiB + remainder).
        assert_eq!(manifest.chunks.len(), 3);
        assert!(manifest.is_v1());

        let store: HashMap<String, Vec<u8>> = chunks.into_iter().collect();
        let back = reassemble(&manifest, |c| {
            store.get(c).cloned().ok_or_else(|| anyhow::anyhow!("missing {c}"))
        })
        .unwrap();
        assert_eq!(back, data);
    }

    /// Identical chunks share a CID (dedup) — a file of all-zero bytes references one CID N times.
    #[test]
    fn identical_chunks_dedup() {
        let data = vec![0u8; DEFAULT_CHUNK_SIZE * 3];
        let (manifest, chunks) = chunk_object(&data, DEFAULT_CHUNK_SIZE);
        assert_eq!(manifest.chunks.len(), 3);
        // All three chunk CIDs are identical.
        assert_eq!(manifest.chunks[0], manifest.chunks[1]);
        assert_eq!(manifest.chunks[1], manifest.chunks[2]);
        // The unique set is size 1.
        let uniq: std::collections::HashSet<_> = chunks.iter().map(|(c, _)| c).collect();
        assert_eq!(uniq.len(), 1);
    }

    /// A small object is a single chunk; a manifest still wraps it (uniform path).
    #[test]
    fn small_object_single_chunk() {
        let data = b"hello world".to_vec();
        let (manifest, chunks) = chunk_object(&data, DEFAULT_CHUNK_SIZE);
        assert_eq!(manifest.chunks.len(), 1);
        assert_eq!(manifest.total_size, 11);
        let store: HashMap<String, Vec<u8>> = chunks.into_iter().collect();
        assert_eq!(reassemble(&manifest, |c| Ok(store[c].clone())).unwrap(), data);
    }

    /// An empty object has zero chunks and reassembles to empty.
    #[test]
    fn empty_object() {
        let (manifest, chunks) = chunk_object(&[], DEFAULT_CHUNK_SIZE);
        assert!(manifest.chunks.is_empty());
        assert_eq!(manifest.total_size, 0);
        assert!(chunks.is_empty());
        assert_eq!(reassemble(&manifest, |_| unreachable!()).unwrap(), Vec::<u8>::new());
    }

    /// A tampered chunk is rejected: its bytes no longer hash to the CID in the manifest.
    #[test]
    fn tampered_chunk_rejected() {
        let data: Vec<u8> = (0..1_500_000u32).map(|i| i as u8).collect();
        let (manifest, chunks) = chunk_object(&data, DEFAULT_CHUNK_SIZE);
        let mut store: HashMap<String, Vec<u8>> = chunks.into_iter().collect();
        // Corrupt the first chunk's bytes (keep the same CID key).
        let first = manifest.chunks[0].clone();
        store.get_mut(&first).unwrap()[0] ^= 0xff;
        let err = reassemble(&manifest, |c| Ok(store[c].clone())).unwrap_err();
        assert!(err.to_string().contains("verification failed"), "{err}");
    }

    /// CIDs match a known sha256 (cross-checks the keying against the node's blob store).
    #[test]
    fn cid_is_sha256_hex() {
        // sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assert_eq!(
            cid(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
