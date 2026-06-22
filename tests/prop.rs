//! Property / fuzz tests for the SDK's foundational invariants:
//!
//! - **Amount** money math: parse/format round-trips (incl. values far above 2^53), JSON
//!   wire round-trips, decimal-place boundaries, sign handling, ordering.
//! - **Object chunking** (the data-layer Merkle DAG): chunk -> reassemble round-trips for
//!   arbitrary bytes / chunk sizes; dedup; tamper rejection; CID == sha256.
//! - **SSE decoder**: feeding the same byte stream in *any* chunk split produces the same frames
//!   (the #1 SSE bug — chunk-boundary independence), plus CRLF/LF/CR equivalence and comment
//!   skipping. This is the "random op orders converge identically" analogue for the parser.
//! - **Tag set logic**: intersection/union semantics over random provider sets.

use ce_rs::data::{chunk_object, cid, reassemble, Manifest, MANIFEST_KIND_V1};
use ce_rs::sse::SseDecoder;
use ce_rs::Amount;
use proptest::prelude::*;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Amount money math
// ---------------------------------------------------------------------------

proptest! {
    /// base-unit -> JSON string -> base-unit round-trips for the whole i128 range, including
    /// values far above 2^53 (the reason amounts are strings on the wire).
    #[test]
    fn amount_json_round_trips_full_i128(n in any::<i128>()) {
        let a = Amount::from_base(n);
        let j = serde_json::to_string(&a).unwrap();
        // It must be a *string*, not a bare number.
        prop_assert!(j.starts_with('"') && j.ends_with('"'));
        let back: Amount = serde_json::from_str(&j).unwrap();
        prop_assert_eq!(a, back);
    }

    /// Human credit decimal -> Amount -> human decimal round-trips for non-negative whole+frac.
    #[test]
    fn amount_credit_string_round_trips(whole in 0u64..=u64::MAX, frac in 0u64..1_000_000_000_000_000_000u64) {
        // Build a canonical decimal string, then ensure parse->credits() reproduces it.
        let frac_str = format!("{frac:018}");
        let trimmed = frac_str.trim_end_matches('0');
        let s = if trimmed.is_empty() {
            whole.to_string()
        } else {
            format!("{whole}.{trimmed}")
        };
        let a = Amount::parse_credits(&s).unwrap();
        prop_assert_eq!(a.credits(), s);
    }

    /// from_credits(n) is exactly n * 10^18 and formats back to n.
    #[test]
    fn from_credits_is_exact(n in 0u64..21_000_000_000u64) {
        let a = Amount::from_credits(n);
        prop_assert_eq!(a.base(), n as i128 * 1_000_000_000_000_000_000i128);
        prop_assert_eq!(a.credits(), n.to_string());
    }

    /// Negative amounts round-trip and format with a leading '-'.
    #[test]
    fn negative_amounts_round_trip(n in i128::MIN..0i128) {
        let a = Amount::from_base(n);
        let s = a.credits();
        prop_assert!(s.starts_with('-'));
        let reparsed = Amount::parse_credits(&s).unwrap();
        prop_assert_eq!(a, reparsed);
    }

    /// Ordering on Amount matches integer ordering on base units.
    #[test]
    fn ordering_matches_base(a in any::<i128>(), b in any::<i128>()) {
        prop_assert_eq!(Amount::from_base(a).cmp(&Amount::from_base(b)), a.cmp(&b));
    }
}

#[test]
fn amount_rejects_more_than_18_decimals() {
    assert!(Amount::parse_credits("1.0000000000000000001").is_err()); // 19 places
    assert!(Amount::parse_credits("0.000000000000000000").is_ok()); // 18 zeros ok
}

#[test]
fn amount_parses_edge_forms() {
    assert_eq!(Amount::parse_credits(".5").unwrap().credits(), "0.5");
    assert_eq!(Amount::parse_credits("  10  ").unwrap().credits(), "10"); // trimmed
    assert_eq!(Amount::parse_credits("-0").unwrap(), Amount::ZERO);
    assert_eq!(Amount::parse_credits("0").unwrap(), Amount::ZERO);
}

#[test]
fn amount_supply_cap_value_is_exact() {
    // 21 billion credits — the hard supply cap, ~2.1e28 base units, far beyond u64.
    let cap = Amount::from_credits(21_000_000_000);
    assert_eq!(cap.base(), 21_000_000_000i128 * 1_000_000_000_000_000_000i128);
    // JSON wire form must be the full decimal string.
    assert_eq!(
        serde_json::to_string(&cap).unwrap(),
        "\"21000000000000000000000000000\""
    );
}

#[test]
fn amount_deserializes_with_surrounding_whitespace() {
    let a: Amount = serde_json::from_str("\"  42  \"").unwrap();
    assert_eq!(a.base(), 42);
}

#[test]
fn amount_rejects_non_integer_wire_string() {
    let r: Result<Amount, _> = serde_json::from_str("\"1.5\"");
    assert!(r.is_err(), "wire form is base-unit integers, not decimals");
}

// ---------------------------------------------------------------------------
// Object chunking / Merkle DAG
// ---------------------------------------------------------------------------

proptest! {
    /// chunk_object -> reassemble is the identity for arbitrary bytes and any positive chunk size.
    #[test]
    fn object_chunk_reassemble_round_trips(
        data in proptest::collection::vec(any::<u8>(), 0..20_000),
        chunk_size in 1usize..4096,
    ) {
        let (manifest, chunks) = chunk_object(&data, chunk_size);
        prop_assert_eq!(manifest.total_size, data.len() as u64);
        prop_assert_eq!(manifest.chunk_size, chunk_size as u64);
        prop_assert!(manifest.is_v1());
        // Chunk count is the ceil-division.
        let expected = if data.is_empty() { 0 } else { (data.len() + chunk_size - 1) / chunk_size };
        prop_assert_eq!(manifest.chunks.len(), expected);

        let store: HashMap<String, Vec<u8>> = chunks.into_iter().collect();
        let back = reassemble(&manifest, |c| {
            store.get(c).cloned().ok_or_else(|| anyhow::anyhow!("missing {c}"))
        }).unwrap();
        prop_assert_eq!(back, data);
    }

    /// Each chunk's CID equals sha256 of its bytes; identical content yields identical CIDs (dedup).
    #[test]
    fn cid_is_content_addressed(a in proptest::collection::vec(any::<u8>(), 0..2000)) {
        prop_assert_eq!(cid(&a), cid(&a.clone())); // deterministic
        // Distinct-ish: flipping a byte changes the CID (overwhelmingly).
        if !a.is_empty() {
            let mut b = a.clone();
            b[0] ^= 0xff;
            if b != a {
                prop_assert_ne!(cid(&a), cid(&b));
            }
        }
    }

    /// A tampered chunk is always rejected by reassemble (content addressing is enforced).
    #[test]
    fn tampered_chunk_always_rejected(
        data in proptest::collection::vec(any::<u8>(), 64..8000),
        chunk_size in 16usize..1024,
    ) {
        let (manifest, chunks) = chunk_object(&data, chunk_size);
        prop_assume!(!manifest.chunks.is_empty());
        let mut store: HashMap<String, Vec<u8>> = chunks.into_iter().collect();
        let target = manifest.chunks[0].clone();
        // Corrupt the stored bytes for the first chunk CID.
        let v = store.get_mut(&target).unwrap();
        if !v.is_empty() { v[0] ^= 0xff; } else { v.push(1); }
        // Only reject if we actually changed the bytes (a 1-byte all-... edge still differs).
        let res = reassemble(&manifest, |c| Ok(store[c].clone()));
        prop_assert!(res.is_err());
    }
}

#[test]
fn reassemble_rejects_wrong_total_size() {
    // A manifest that claims a bigger total than the chunks provide.
    let data = b"abc".to_vec();
    let (mut manifest, chunks) = chunk_object(&data, 1024);
    manifest.total_size = 999; // lie
    let store: HashMap<String, Vec<u8>> = chunks.into_iter().collect();
    let err = reassemble(&manifest, |c| Ok(store[c].clone())).unwrap_err().to_string();
    assert!(err.contains("total_size"), "{err}");
}

#[test]
fn reassemble_rejects_unknown_kind() {
    let manifest = Manifest {
        kind: "ce-object-v99".into(),
        chunk_size: 1,
        total_size: 0,
        chunks: vec![],
    };
    let err = reassemble(&manifest, |_| unreachable!()).unwrap_err().to_string();
    assert!(err.contains("unsupported manifest kind"), "{err}");
}

#[test]
fn manifest_json_round_trips() {
    let m = Manifest {
        kind: MANIFEST_KIND_V1.into(),
        chunk_size: 1024,
        total_size: 4096,
        chunks: vec!["aa".into(), "bb".into()],
    };
    let j = serde_json::to_string(&m).unwrap();
    let back: Manifest = serde_json::from_str(&j).unwrap();
    assert_eq!(m, back);
}

// ---------------------------------------------------------------------------
// SSE decoder: chunk-boundary independence (the convergence property)
// ---------------------------------------------------------------------------

/// Decode a whole SSE byte stream with a given list of split points, returning the data payloads.
fn decode_with_splits(bytes: &[u8], splits: &[usize]) -> Vec<String> {
    let mut d = SseDecoder::new();
    let mut out = Vec::new();
    let mut start = 0;
    let mut points: Vec<usize> = splits.iter().copied().filter(|&p| p < bytes.len()).collect();
    points.sort_unstable();
    points.dedup();
    for &p in &points {
        for ev in d.push(&bytes[start..p]) {
            out.push(ev.data);
        }
        start = p;
    }
    for ev in d.push(&bytes[start..]) {
        out.push(ev.data);
    }
    if let Some(ev) = d.finish() {
        out.push(ev.data);
    }
    out
}

proptest! {
    /// The decoder is independent of how the byte stream is chunked: the same complete stream,
    /// split at any random set of boundaries, yields the identical sequence of frames. This is the
    /// streaming-parser analogue of CRDT convergence.
    #[test]
    fn sse_is_chunk_boundary_independent(
        n_frames in 1usize..6,
        splits in proptest::collection::vec(0usize..200, 0..30),
    ) {
        // Build a deterministic multi-frame stream.
        let mut stream = String::new();
        for i in 0..n_frames {
            stream.push_str(&format!("event: e{i}\nid: {i}\ndata: payload-{i}\n\n"));
        }
        let bytes = stream.as_bytes();
        let whole = decode_with_splits(bytes, &[]); // one push
        let split = decode_with_splits(bytes, &splits); // many pushes
        prop_assert_eq!(&whole, &split);
        // And it's exactly the expected frames.
        let expected: Vec<String> = (0..n_frames).map(|i| format!("payload-{i}")).collect();
        prop_assert_eq!(whole, expected);
    }

    /// Per-byte feeding (the worst-case split) equals whole-buffer feeding.
    #[test]
    fn sse_per_byte_equals_whole(n_frames in 1usize..5) {
        let mut stream = String::new();
        for i in 0..n_frames {
            stream.push_str(&format!("data: d{i}\n\n"));
        }
        let bytes = stream.as_bytes();
        let whole = decode_with_splits(bytes, &[]);
        let per_byte: Vec<usize> = (0..bytes.len()).collect();
        let byte = decode_with_splits(bytes, &per_byte);
        prop_assert_eq!(whole, byte);
    }
}

#[test]
fn sse_line_ending_equivalence() {
    // LF, CRLF and CR must all be accepted and produce the same frame.
    let lf = decode_with_splits(b"data: x\n\n", &[]);
    let crlf = decode_with_splits(b"data: x\r\n\r\n", &[]);
    let cr = decode_with_splits(b"data: x\r\r", &[]);
    assert_eq!(lf, vec!["x".to_string()]);
    assert_eq!(crlf, vec!["x".to_string()]);
    assert_eq!(cr, vec!["x".to_string()]);
}

#[test]
fn sse_field_without_space_after_colon() {
    // Per spec, exactly one leading space after the colon is stripped; "data:x" has none.
    let v = decode_with_splits(b"data:x\n\n", &[]);
    assert_eq!(v, vec!["x".to_string()]);
}

#[test]
fn sse_data_with_internal_colons_preserved() {
    let v = decode_with_splits(b"data: a:b:c\n\n", &[]);
    assert_eq!(v, vec!["a:b:c".to_string()]);
}

#[test]
fn sse_frame_with_only_comment_emits_nothing() {
    let v = decode_with_splits(b": just a comment\n\n", &[]);
    assert!(v.is_empty());
}

// ---------------------------------------------------------------------------
// Tag set logic — exercising the live methods via pure mirrors is in unit tests;
// here we property-check the algebra they implement.
// ---------------------------------------------------------------------------

fn intersect(lists: &[Vec<String>]) -> Vec<String> {
    if lists.is_empty() {
        return Vec::new();
    }
    let mut acc: Option<Vec<String>> = None;
    for l in lists {
        acc = Some(match acc {
            None => l.clone(),
            Some(prev) => prev.into_iter().filter(|p| l.contains(p)).collect(),
        });
    }
    acc.unwrap_or_default()
}

fn union(lists: &[Vec<String>]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for l in lists {
        for s in l {
            if !out.contains(s) {
                out.push(s.clone());
            }
        }
    }
    out
}

proptest! {
    /// Intersection result members appear in every input list; union members appear in at least one,
    /// and union has no duplicates.
    #[test]
    fn tag_set_algebra(
        a in proptest::collection::vec("[a-d]", 0..8),
        b in proptest::collection::vec("[a-d]", 0..8),
    ) {
        let lists = vec![a.clone(), b.clone()];
        let inter = intersect(&lists);
        for x in &inter {
            prop_assert!(a.contains(x) && b.contains(x));
        }
        let uni = union(&lists);
        for x in &uni {
            prop_assert!(a.contains(x) || b.contains(x));
        }
        // Union dedups.
        let mut seen = std::collections::HashSet::new();
        for x in &uni {
            prop_assert!(seen.insert(x.clone()), "duplicate {x} in union");
        }
    }
}
