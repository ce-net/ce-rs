//! App-lifecycle operations — the SAME cap-gated install/spawn the `ce app` CLI performs, exposed on
//! the SDK so that ANY app (with clearance) can drive it, not only a human at a terminal. The CLI is
//! just the human skin over these calls; an app holding the right capability spawns/installs apps on
//! the fleet exactly as `ce app install --on …` does. This is how installing a capability-providing
//! ceapp "expands the functionality of the mesh": every app can compose the mesh's own app system.
//!
//! The node enforces authorization — each call carries the caller's capability (`grant`) and the node
//! verifies it before forwarding the RPC to the target, which runs its local appmgr flow. These are
//! thin, faithful wrappers over the node HTTP endpoints (`/mesh-app-install`, …), matching the CLI.
//!
//! Kept in its own module (not `lib.rs`) purely to compose cleanly with concurrent edits to the core
//! client; it is ordinary substrate SDK surface, sibling to `mesh_deploy`/`mesh_kill`.

use anyhow::Result;

use crate::{json, CeClient};

/// The outcome of a successful [`CeClient::mesh_app_install`]: the installed app slug + its version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppInstalled {
    /// The app slug the target reports as installed.
    pub app: String,
    /// The version the target resolved and installed.
    pub version: String,
}

impl CeClient {
    /// Install a published ceapp on a specific remote node over the mesh (`POST /mesh-app-install`) —
    /// the programmatic form of `ce app install <app> --on node=<id>`.
    ///
    /// The local node packages an `AppInstall` RPC carrying `grant` (the capability authorizing the
    /// install) and sends it to `node_id`; the target verifies the capability and runs its local
    /// appmgr install flow, resolving the manifest + artifacts from `registry` (a blob/registry
    /// origin). Returns the installed [`AppInstalled`] (app slug + version).
    ///
    /// Authorization is the node's, not the SDK's: a caller without a capability granting the install
    /// on the target is rejected by the node — identical to the CLI. `grant` is a serialized capability
    /// chain (as the wallet stores it); pass `None` to rely on a capability the node already holds for
    /// the target.
    pub async fn mesh_app_install(
        &self,
        node_id: &str,
        app: &str,
        registry: &str,
        grant: Option<&str>,
    ) -> Result<AppInstalled> {
        let body = serde_json::json!({
            "node_id": node_id,
            "app": app,
            "registry": registry,
            "grant": grant,
        });
        let v: serde_json::Value =
            json(self.http.post(self.url("/mesh-app-install")).json(&body).send().await?).await?;
        Ok(AppInstalled {
            app: v["app"].as_str().unwrap_or_default().to_string(),
            version: v["version"].as_str().unwrap_or_default().to_string(),
        })
    }
}
