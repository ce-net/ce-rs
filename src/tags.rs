//! Atlas-style self-tagging over the node's existing discovery DHT — **no node change required**.
//!
//! The node exposes generic service discovery: `POST /discovery/advertise { service }` and
//! `GET /discovery/find/:service → { providers: [node_id] }`. By treating a tag as a service
//! string (`"infer"`, `"gpu"`, `"tier:hi"`, `"model:llama-3-8b"`), apps can self-advertise
//! capability/capacity tags and find peers by tag today, with the SDK's existing primitives.
//!
//! ## Relationship to `/atlas`
//!
//! This **complements** [`CeClient::atlas`](crate::CeClient::atlas). The atlas carries
//! **node-published** capability self-tags (`linux`, `docker`, `gpu`, ...) that the node derives
//! and broadcasts via CEP-1 capacity signals — authoritative, but a fixed vocabulary the node
//! controls. Discovery tags here are **app-published**: any app can advertise an arbitrary tag
//! string and discover peers by it, without waiting for a node release. Use the atlas for
//! hardware truth; use discovery tags for app-level routing (model availability, service class).
//!
//! ## Future node-side improvement
//!
//! A future node-side `set-tags` (letting an app push tags into the node's own atlas entry, so
//! they appear in `/atlas` alongside hardware tags and propagate via capacity signals) would make
//! tags first-class and mesh-replicated rather than DHT-provider-record-scoped. Until then,
//! re-advertise periodically (provider records expire) — see [`TagAdvertiser`].

use crate::CeClient;
use anyhow::Result;

/// Namespace prefix used to keep app tags from colliding with bare service names in the DHT.
/// A tag `gpu` is advertised under the service `tag:gpu`. Callers pass the bare tag; the SDK
/// applies the prefix on both advertise and find so the two always agree.
const TAG_PREFIX: &str = "tag:";

/// Map a bare tag (`"gpu"`, `"model:llama-3-8b"`) to its discovery service string.
fn tag_service(tag: &str) -> String {
    format!("{TAG_PREFIX}{tag}")
}

impl CeClient {
    /// Advertise that this node carries `tag` (e.g. `"infer"`, `"gpu"`, `"tier:hi"`,
    /// `"model:llama-3-8b"`), discoverable by [`find_tag`](Self::find_tag). Backed by
    /// `POST /discovery/advertise`; provider records expire, so re-advertise periodically (see
    /// [`TagAdvertiser`]). Complements `/atlas` — see the [module docs](crate::tags).
    pub async fn advertise_tag(&self, tag: &str) -> Result<()> {
        self.advertise_service(&tag_service(tag)).await
    }

    /// Advertise several tags at once. Best-effort: stops at the first failure.
    pub async fn advertise_tags(&self, tags: &[&str]) -> Result<()> {
        for t in tags {
            self.advertise_tag(t).await?;
        }
        Ok(())
    }

    /// Find the NodeId hexes of peers advertising `tag` (`GET /discovery/find/:service`).
    pub async fn find_tag(&self, tag: &str) -> Result<Vec<String>> {
        self.find_service(&tag_service(tag)).await
    }

    /// Find peers advertising **all** of `tags` (set intersection of each tag's providers).
    /// Returns NodeId hexes present in every tag's provider list. An empty `tags` yields empty.
    pub async fn find_tags_all(&self, tags: &[&str]) -> Result<Vec<String>> {
        if tags.is_empty() {
            return Ok(Vec::new());
        }
        let mut acc: Option<Vec<String>> = None;
        for t in tags {
            let providers = self.find_tag(t).await?;
            acc = Some(match acc {
                None => providers,
                Some(prev) => prev.into_iter().filter(|p| providers.contains(p)).collect(),
            });
        }
        Ok(acc.unwrap_or_default())
    }

    /// Find peers advertising **any** of `tags` (set union, de-duplicated).
    pub async fn find_tags_any(&self, tags: &[&str]) -> Result<Vec<String>> {
        let mut out: Vec<String> = Vec::new();
        for t in tags {
            for p in self.find_tag(t).await? {
                if !out.contains(&p) {
                    out.push(p);
                }
            }
        }
        Ok(out)
    }
}

/// Keeps a set of tags advertised by re-advertising them. DHT provider records expire, so a tag
/// must be re-advertised periodically to stay discoverable. Construct one with the tags to keep
/// alive and call [`refresh`](Self::refresh) on an interval (the node's records typically live
/// for tens of minutes; refreshing every few minutes is safe).
#[derive(Debug, Clone)]
pub struct TagAdvertiser {
    client: CeClient,
    tags: Vec<String>,
}

impl TagAdvertiser {
    /// Advertise and keep `tags` alive via this client.
    pub fn new(client: CeClient, tags: impl IntoIterator<Item = String>) -> Self {
        TagAdvertiser { client, tags: tags.into_iter().collect() }
    }

    /// The tags this advertiser keeps alive.
    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    /// Re-advertise every tag once. Call on an interval to keep records from expiring.
    pub async fn refresh(&self) -> Result<()> {
        for t in &self.tags {
            self.client.advertise_tag(t).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_service_namespacing() {
        assert_eq!(tag_service("gpu"), "tag:gpu");
        assert_eq!(tag_service("model:llama-3-8b"), "tag:model:llama-3-8b");
        assert_eq!(tag_service("tier:hi"), "tag:tier:hi");
    }

    #[test]
    fn advertiser_tracks_tags() {
        let c = CeClient::with_token("http://127.0.0.1:8844", None);
        let a = TagAdvertiser::new(c, ["gpu".to_string(), "infer".to_string()]);
        assert_eq!(a.tags(), &["gpu".to_string(), "infer".to_string()]);
    }

    // Pure set-logic mirroring `find_tags_all`/`find_tags_any` so we can assert the intersection
    // and union semantics without a live node.
    fn intersect(lists: &[Vec<&str>]) -> Vec<String> {
        if lists.is_empty() {
            return Vec::new();
        }
        let mut acc: Option<Vec<String>> = None;
        for l in lists {
            let cur: Vec<String> = l.iter().map(|s| s.to_string()).collect();
            acc = Some(match acc {
                None => cur,
                Some(prev) => prev.into_iter().filter(|p| cur.contains(p)).collect(),
            });
        }
        acc.unwrap_or_default()
    }

    fn union(lists: &[Vec<&str>]) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for l in lists {
            for s in l {
                if !out.iter().any(|x| x == s) {
                    out.push(s.to_string());
                }
            }
        }
        out
    }

    #[test]
    fn all_is_intersection() {
        let gpu = vec!["n1", "n2", "n3"];
        let infer = vec!["n2", "n3", "n4"];
        assert_eq!(intersect(&[gpu, infer]), vec!["n2".to_string(), "n3".to_string()]);
    }

    #[test]
    fn any_is_deduped_union() {
        let gpu = vec!["n1", "n2"];
        let infer = vec!["n2", "n4"];
        assert_eq!(
            union(&[gpu, infer]),
            vec!["n1".to_string(), "n2".to_string(), "n4".to_string()]
        );
    }

    #[test]
    fn empty_tags_intersection_is_empty() {
        let lists: Vec<Vec<&str>> = vec![];
        assert!(intersect(&lists).is_empty());
    }
}
