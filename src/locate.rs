//! Mesh-native service location + selection — the other half of building a real mesh app.
//!
//! [`crate::serve`] lets an app *be* a service (answer requests on a topic). This module lets a
//! client *find and pick* a live instance of a service over the mesh, so SDKs never hardcode a
//! NodeId or talk to a central HTTP endpoint. It is the primitive every higher SDK builds on to
//! answer "which running instance of service X do I talk to?".
//!
//! It composes only existing node primitives — DHT service discovery
//! ([`CeClient::find_service`](crate::CeClient::find_service)), the capacity atlas
//! ([`CeClient::atlas`](crate::CeClient::atlas)), per-node reputation
//! ([`CeClient::history`](crate::CeClient::history)), and the verifiable randomness beacon
//! ([`CeClient::beacon`](crate::CeClient::beacon)) — so there are no new node RPCs and nothing is
//! routed off the mesh.
//!
//! ## Selection
//!
//! [`locate`] discovers the instances advertising a service, then ranks the live ones by **trust**
//! (on-chain delivered-and-paid work), **capacity** (free cores/memory), and **recency**, with a
//! **beacon-seeded** deterministic tiebreak so the choice is reproducible and unsteerable. When more
//! than one instance is requested for redundancy, candidates are **spread across distinct fault
//! domains** (region / zone / ASN tags) so one datacenter or operator loss does not take them all.
//!
//! Latency-aware ranking (RTT from `/netgraph`) is a planned refinement: `/netgraph` is keyed by
//! libp2p PeerId, which needs a PeerId<->NodeId correlation not yet exposed; [`CeClient::netgraph`]
//! ships the raw primitive in the meantime.
//!
//! ## Example
//!
//! ```no_run
//! # async fn run(ce: ce_rs::CeClient) -> anyhow::Result<()> {
//! use ce_rs::locate::{call, LocateOpts};
//! // Find a live instance of "ce-db" and send it a request over the mesh, failing over to the next
//! // best instance if one is unreachable.
//! let reply = call(&ce, "ce-db", "ce-db/rpc", b"get:user:42", &LocateOpts::default(), 5_000).await?;
//! # let _ = reply; Ok(())
//! # }
//! ```

use crate::CeClient;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;

/// A located, live instance of a service, with the signals used to rank it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    /// The instance's NodeId (hex).
    pub node_id: String,
    /// Composite selection score (higher is better).
    pub score: f64,
    /// Advertised CPU cores.
    pub cores: u32,
    /// Advertised memory (MiB).
    pub mem_mb: u32,
    /// The node's capability self-tags.
    pub tags: Vec<String>,
    /// Unix seconds since this node was last seen in the atlas.
    pub last_seen_secs: u64,
    /// The fault domain (region:/zone:/asn: tag) used for redundancy spread, if any.
    pub fault_domain: Option<String>,
}

/// How to select instances.
#[derive(Debug, Clone)]
pub struct LocateOpts {
    /// How many instances to return (redundancy). Default 1.
    pub want: usize,
    /// Only consider instances whose atlas tags include all of these.
    pub require_tags: Vec<String>,
    /// Consider an instance live only if seen within this many seconds. Default 120.
    pub max_stale_secs: u64,
    /// When `want > 1`, spread the chosen instances across distinct fault domains. Default true.
    pub spread_domains: bool,
}

impl Default for LocateOpts {
    fn default() -> Self {
        LocateOpts { want: 1, require_tags: Vec::new(), max_stale_secs: 120, spread_domains: true }
    }
}

/// Discover and rank live instances of `service`, best first.
///
/// Returns at most `opts.want` instances. An empty vec means the service is advertised by no live
/// instance matching the constraints (the caller decides whether to start one).
pub async fn locate(ce: &CeClient, service: &str, opts: &LocateOpts) -> Result<Vec<Instance>> {
    let ids = ce.find_service(service).await?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let atlas = ce.atlas().await?;
    // Beacon seeds a deterministic, unsteerable tiebreak; tolerate its absence.
    let beacon_hash = ce.beacon().await.ok().map(|b| b.hash).unwrap_or_default();
    let now = unix_now();

    // The local node is a valid candidate for any service it advertises — always live, zero network
    // latency — but it never appears in its OWN atlas (the atlas is remote-peer capacity). Look it
    // up explicitly so a co-located service (serve + locate on one node) is reachable.
    let local_id = ce.status().await.ok().map(|s| s.node_id);

    // Index the atlas by node id for O(1) lookup.
    let mut by_id = std::collections::HashMap::new();
    for e in &atlas {
        by_id.insert(e.node_id.clone(), e);
    }

    let mut scored: Vec<Instance> = Vec::new();
    for id in ids {
        // Co-located self: always live and zero-latency, so prefer it for a service we host. (We
        // can't introspect self's capability tags here, so skip self when tags are required.)
        if Some(&id) == local_id.as_ref() {
            if !opts.require_tags.is_empty() {
                continue;
            }
            scored.push(Instance {
                node_id: id.clone(),
                score: 1.0,
                cores: 0,
                mem_mb: 0,
                tags: Vec::new(),
                last_seen_secs: now,
                fault_domain: String::new(),
            });
            continue;
        }
        let Some(entry) = by_id.get(&id) else { continue }; // not in atlas -> unknown/dead
        // Liveness: drop stale advertisements.
        let age = now.saturating_sub(entry.last_seen_secs);
        if age > opts.max_stale_secs {
            continue;
        }
        // Required capability tags.
        if !opts.require_tags.iter().all(|t| entry.has_tag(t)) {
            continue;
        }

        // Trust: on-chain delivered-and-paid work, log-saturated; a stranger scores 0. Best-effort
        // (a history read failure degrades this instance's trust to 0 rather than dropping it).
        let trust = match ce.history(&id).await {
            Ok(h) if !h.is_newcomer() => {
                let delivered = (h.jobs_paid + h.heartbeats_paid) as f64;
                (1.0 + delivered).ln()
            }
            _ => 0.0,
        };
        let trust_norm = (trust / 10.0).min(1.0); // ~22k delivered units saturates

        // Capacity headroom (rough): free cores (total minus running jobs) + memory, normalized to
        // soft ceilings.
        let free_cores = entry.cpu_cores.saturating_sub(entry.running_jobs);
        let cap = ((free_cores as f64) / 16.0).min(1.0) * 0.5
            + ((entry.mem_mb as f64) / 32_768.0).min(1.0) * 0.5;

        // Recency: 1.0 when just seen, decaying to 0 across the staleness window.
        let recency = 1.0 - (age as f64 / opts.max_stale_secs.max(1) as f64);

        // Deterministic, beacon-seeded jitter for reproducible tiebreaks nobody can steer.
        let jitter = beacon_jitter(&id, &beacon_hash);

        let score = 0.5 * trust_norm + 0.3 * cap + 0.2 * recency + 0.001 * jitter;
        scored.push(Instance {
            node_id: id.clone(),
            score,
            cores: entry.cpu_cores,
            mem_mb: entry.mem_mb,
            tags: entry.tags.clone(),
            last_seen_secs: entry.last_seen_secs,
            fault_domain: fault_domain(&entry.tags),
        });
    }

    scored.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

    let want = opts.want.max(1);
    if want == 1 || !opts.spread_domains {
        scored.truncate(want);
        return Ok(scored);
    }
    Ok(spread(scored, want))
}

/// Locate the best instance(s) of `service` and send `payload` to one over the mesh on `topic`,
/// failing over to the next-best instance if a request errors. Returns the first successful reply.
pub async fn call(
    ce: &CeClient,
    service: &str,
    topic: &str,
    payload: &[u8],
    opts: &LocateOpts,
    timeout_ms: u64,
) -> Result<Vec<u8>> {
    // Locate a few candidates so we can fail over, regardless of how many the caller ultimately
    // wants returned.
    let mut o = opts.clone();
    o.want = o.want.max(3);
    let instances = locate(ce, service, &o).await?;
    if instances.is_empty() {
        return Err(anyhow!("no live instance of service '{service}' found"));
    }
    let mut last_err: Option<anyhow::Error> = None;
    for inst in instances {
        match ce.request(&inst.node_id, topic, payload, timeout_ms).await {
            Ok(reply) => return Ok(reply),
            Err(e) => {
                tracing::warn!(service, node = %inst.node_id, error = %e, "locate::call: instance failed, failing over");
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("all instances of '{service}' failed")))
}

/// Keep this node discoverable as an instance of `service`: re-advertise on the DHT every `interval`
/// until `shutdown` resolves. A service built with [`crate::serve`] calls this so clients can
/// [`locate`] it. DHT provider records expire, so periodic re-advertisement is the liveness signal.
pub async fn register(
    ce: &CeClient,
    service: &str,
    interval: Duration,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    tokio::pin!(shutdown);
    loop {
        if let Err(e) = ce.advertise_service(service).await {
            tracing::warn!(service, error = %e, "register: advertise failed; will retry");
        }
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

/// The fault domain for redundancy spread: the first of region:/zone:/asn: tags, if present.
fn fault_domain(tags: &[String]) -> Option<String> {
    for prefix in ["region:", "zone:", "asn:"] {
        if let Some(t) = tags.iter().find(|t| t.starts_with(prefix)) {
            return Some(t.clone());
        }
    }
    None
}

/// Pick `want` instances spreading across distinct fault domains first (one best-scored per domain,
/// round-robin), then fill any remainder by score. Instances with an unknown domain are each their
/// own bucket so they are never blindly collapsed together.
fn spread(scored: Vec<Instance>, want: usize) -> Vec<Instance> {
    use std::collections::BTreeMap;
    // Group by domain, preserving score order within each group.
    let mut groups: BTreeMap<String, Vec<Instance>> = BTreeMap::new();
    for (i, inst) in scored.into_iter().enumerate() {
        // Unknown domain -> a unique bucket per instance (keyed by its index).
        let key = inst.fault_domain.clone().unwrap_or_else(|| format!("~{i}"));
        groups.entry(key).or_default().push(inst);
    }
    let mut buckets: Vec<Vec<Instance>> = groups.into_values().collect();
    // Order buckets by their best instance's score so we round-robin best-first.
    buckets.sort_by(|a, b| {
        let sa = a.first().map(|x| x.score).unwrap_or(f64::MIN);
        let sb = b.first().map(|x| x.score).unwrap_or(f64::MIN);
        sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut out = Vec::with_capacity(want);
    let mut round = 0usize;
    loop {
        let mut took_any = false;
        for bucket in buckets.iter_mut() {
            if out.len() >= want {
                return out;
            }
            if round < bucket.len() {
                out.push(bucket[round].clone());
                took_any = true;
            }
        }
        if !took_any {
            return out; // exhausted all buckets
        }
        round += 1;
    }
}

/// A deterministic [0,1) value derived from the node id and the beacon hash — a tiebreak no party
/// can predict before the beacon is fixed or steer afterward.
fn beacon_jitter(node_id: &str, beacon_hash: &str) -> f64 {
    let mut h = Sha256::new();
    h.update(node_id.as_bytes());
    h.update(b"|");
    h.update(beacon_hash.as_bytes());
    let d = h.finalize();
    let mut n = [0u8; 8];
    n.copy_from_slice(&d[..8]);
    (u64::from_be_bytes(n) as f64) / (u64::MAX as f64)
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inst(id: &str, score: f64, domain: Option<&str>) -> Instance {
        Instance {
            node_id: id.into(),
            score,
            cores: 4,
            mem_mb: 4096,
            tags: domain.map(|d| vec![d.to_string()]).unwrap_or_default(),
            last_seen_secs: 0,
            fault_domain: domain.map(|d| d.to_string()),
        }
    }

    #[test]
    fn spread_prefers_distinct_domains() {
        // Two eu instances (high score) + one us (lower). want=2 must take one eu + the us, not both eu.
        let scored = vec![
            inst("a", 0.9, Some("region:eu")),
            inst("b", 0.8, Some("region:eu")),
            inst("c", 0.7, Some("region:us")),
        ];
        let out = spread(scored, 2);
        let domains: Vec<_> = out.iter().filter_map(|i| i.fault_domain.clone()).collect();
        assert_eq!(out.len(), 2);
        assert!(domains.contains(&"region:eu".to_string()));
        assert!(domains.contains(&"region:us".to_string()));
    }

    #[test]
    fn spread_fills_from_same_domain_when_no_alternative() {
        let scored = vec![
            inst("a", 0.9, Some("region:eu")),
            inst("b", 0.8, Some("region:eu")),
        ];
        let out = spread(scored, 2);
        assert_eq!(out.len(), 2); // falls back to same domain rather than under-filling
    }

    #[test]
    fn unknown_domains_are_distinct_buckets() {
        let scored = vec![inst("a", 0.9, None), inst("b", 0.8, None)];
        let out = spread(scored, 2);
        assert_eq!(out.len(), 2); // both taken; never collapsed into one bucket
    }

    #[test]
    fn fault_domain_precedence() {
        assert_eq!(fault_domain(&["zone:z1".into(), "region:eu".into()]), Some("region:eu".into()));
        assert_eq!(fault_domain(&["asn:64500".into()]), Some("asn:64500".into()));
        assert_eq!(fault_domain(&["gpu".into()]), None);
    }

    #[test]
    fn beacon_jitter_is_deterministic_and_bounded() {
        let a = beacon_jitter("node1", "deadbeef");
        let b = beacon_jitter("node1", "deadbeef");
        assert_eq!(a, b);
        assert!((0.0..1.0).contains(&a));
        assert_ne!(beacon_jitter("node1", "deadbeef"), beacon_jitter("node2", "deadbeef"));
    }
}
