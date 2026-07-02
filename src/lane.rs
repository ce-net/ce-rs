//! Transparent lane transport — the SDK half of ce-lane's `node` router target.
//!
//! When the client targets the LOCAL node and `<data_dir>/lane.sock` exists, the hot-path app
//! primitives (publish / send / request / reply / subscribe / messages_stream) ride a
//! shared-memory ring instead of local HTTP — same node, same `AppBus`, byte-identical
//! semantics, ~an order of magnitude less latency. Everything else (and every failure to bind
//! or a mid-flight lane death) falls back to HTTP silently. Apps notice nothing; that is the
//! contract.
//!
//! Runtime-agnostic like the rest of the base SDK: the mux runs on two std threads (reader =
//! the ring's single consumer, writer = its single producer) and async callers wait on
//! futures-channel primitives, so any executor works.

use ce_lane::proto::{Msg, NodeOk, NodeOp, NodeReq, NodeResp, TARGET_NODE};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Transport failure: the lane is gone (never bound, or died mid-flight). The caller falls
/// back to HTTP. Distinct from a NODE answer of `Err(reason)`, which is authoritative and
/// must NOT be retried over HTTP (the node already acted).
pub(crate) struct LaneGone;

type Pending = Arc<Mutex<HashMap<u64, futures_channel::oneshot::Sender<Result<NodeOk, String>>>>>;
type Subscribers = Arc<Mutex<Vec<futures_channel::mpsc::UnboundedSender<Msg>>>>;

pub(crate) struct LaneClient {
    req_tx: std::sync::mpsc::Sender<Vec<u8>>,
    pending: Pending,
    subscribers: Subscribers,
    next_id: AtomicU64,
    watch_started: OnceLock<()>,
    dead: Arc<AtomicBool>,
}

impl std::fmt::Debug for LaneClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneClient").field("dead", &self.dead.load(Ordering::Relaxed)).finish()
    }
}

/// Where the local node's lane socket lives: `$CE_LANE_SOCK` (custom `--data-dir` and tests),
/// else `<default data dir>/lane.sock`. `None` when absent — the caller stays on HTTP.
pub(crate) fn socket_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CE_LANE_SOCK") {
        let p = PathBuf::from(p.trim());
        return p.exists().then_some(p);
    }
    let dir = directories::ProjectDirs::from("", "", "ce")?.data_dir().to_path_buf();
    let p = dir.join("lane.sock");
    p.exists().then_some(p)
}

impl LaneClient {
    /// Bind the `node` target at `socket` and start the mux. Local + sub-millisecond; called
    /// once per client, lazily.
    pub(crate) fn bind(socket: &std::path::Path, token: &str) -> anyhow::Result<Self> {
        let req = ce_lane::bind::BindRequest {
            token: token.to_string(),
            target: TARGET_NODE.to_string(),
            // If the host launched this app under a name (`$CE_LANE_APP`, set at spawn for a
            // sandboxed/foreign app), pass it so the node resolves its membrane adapter chain
            // and interposes transparently. Absent = the anonymous operator app = direct.
            app: std::env::var("CE_LANE_APP").ok().filter(|s| !s.trim().is_empty()),
            caps: None,
            slot_size: 64 * 1024,
            n_slots: 64,
        };
        let (ep, _ack) = ce_lane::bind::bind(socket, &req)?;
        Ok(Self::from_endpoint(ep))
    }

    /// Start the mux over an already-bound endpoint (also the unit-test seam: tests hand in
    /// one half of an in-process flow and play the node on the other).
    pub(crate) fn from_endpoint(ep: ce_lane::Endpoint) -> Self {
        let ep = Arc::new(ep);
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let subscribers: Subscribers = Arc::new(Mutex::new(Vec::new()));
        let dead = Arc::new(AtomicBool::new(false));
        let (req_tx, req_rx) = std::sync::mpsc::channel::<Vec<u8>>();

        // Writer: the ring's single producer.
        {
            let ep = ep.clone();
            let dead = dead.clone();
            std::thread::Builder::new()
                .name("ce-rs-lane-writer".into())
                .spawn(move || {
                    while let Ok(frame) = req_rx.recv() {
                        if ep.send(&frame).is_err() {
                            break;
                        }
                    }
                    dead.store(true, Ordering::Release);
                    ep.close();
                })
                .ok();
        }

        // Reader: the ring's single consumer. Routes Event frames to stream subscribers and
        // everything else to the pending-op map; on flow death it drains both so no caller
        // waits forever.
        {
            let ep = ep.clone();
            let pending = pending.clone();
            let subscribers = subscribers.clone();
            let dead = dead.clone();
            std::thread::Builder::new()
                .name("ce-rs-lane-reader".into())
                .spawn(move || {
                    let mut buf = Vec::new();
                    loop {
                        match ep.recv_into(&mut buf, None) {
                            Ok(()) => {
                                let Ok(resp) = NodeResp::decode(&buf) else { break };
                                match resp.result {
                                    Ok(NodeOk::Event(msg)) => {
                                        if let Ok(mut subs) = subscribers.lock() {
                                            subs.retain(|s| s.unbounded_send(msg.clone()).is_ok());
                                        }
                                    }
                                    other => {
                                        let waiter = pending
                                            .lock()
                                            .ok()
                                            .and_then(|mut p| p.remove(&resp.id));
                                        if let Some(tx) = waiter {
                                            let _ = tx.send(other);
                                        }
                                    }
                                }
                            }
                            Err(_) => break, // Closed (node gone / restarted / flow revoked)
                        }
                    }
                    dead.store(true, Ordering::Release);
                    ep.close();
                    // Wake every waiter and end every stream: fall back to HTTP, don't hang.
                    if let Ok(mut p) = pending.lock() {
                        p.clear(); // dropping the senders errors the awaiting oneshots
                    }
                    if let Ok(mut subs) = subscribers.lock() {
                        subs.clear(); // dropping the senders ends the streams
                    }
                })
                .ok();
        }

        Self {
            req_tx,
            pending,
            subscribers,
            next_id: AtomicU64::new(1),
            watch_started: OnceLock::new(),
            dead,
        }
    }

    pub(crate) fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }

    /// One pipelined op. `Ok(inner)` is the NODE's answer (authoritative either way);
    /// `Err(LaneGone)` is a transport failure — the only case the caller may retry over HTTP.
    pub(crate) async fn op(&self, op: NodeOp) -> Result<Result<NodeOk, String>, LaneGone> {
        if self.is_dead() {
            return Err(LaneGone);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = futures_channel::oneshot::channel();
        if let Ok(mut p) = self.pending.lock() {
            p.insert(id, tx);
        } else {
            return Err(LaneGone);
        }
        if self.req_tx.send(NodeReq { id, op }.encode()).is_err() {
            if let Ok(mut p) = self.pending.lock() {
                p.remove(&id);
            }
            return Err(LaneGone);
        }
        // Sender dropped (reader drained on death) => LaneGone; the node never answered.
        rx.await.map_err(|_| LaneGone)
    }

    /// Subscribe to pushed inbound messages, starting the flow's Watch on first use. The
    /// receiver IS a `Stream<Item = Msg>`; it ends when the lane dies.
    pub(crate) async fn watch_stream(
        &self,
    ) -> Result<futures_channel::mpsc::UnboundedReceiver<Msg>, LaneGone> {
        if self.watch_started.get().is_none() {
            match self.op(NodeOp::Watch).await? {
                Ok(_) => {
                    let _ = self.watch_started.set(());
                }
                Err(e) => {
                    // The node refused the watch — treat as transport-unusable for streams.
                    let _ = e;
                    return Err(LaneGone);
                }
            }
        }
        let (tx, rx) = futures_channel::mpsc::unbounded();
        match self.subscribers.lock() {
            Ok(mut subs) => subs.push(tx),
            Err(_) => return Err(LaneGone),
        }
        if self.is_dead() {
            return Err(LaneGone); // raced a death; the stream would never yield
        }
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    /// A fake node on the other half of an in-process flow: acks ops, pushes one Event per
    /// Publish (so the watch path is exercised without a running node).
    fn fake_node(ep: ce_lane::Endpoint) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let mut buf = Vec::new();
            while ep.recv_into(&mut buf, Some(std::time::Duration::from_secs(10))).is_ok() {
                let req = NodeReq::decode(&buf).unwrap();
                let resp = match &req.op {
                    NodeOp::Status => NodeResp {
                        id: req.id,
                        result: Ok(NodeOk::Status { node_id: [7u8; 32], height: 42 }),
                    },
                    NodeOp::Publish { topic, payload } => {
                        // Echo the publish back as a pushed Event (self-delivery semantics).
                        let ev = NodeResp {
                            id: 0,
                            result: Ok(NodeOk::Event(Msg {
                                from: [7u8; 32],
                                topic: topic.clone(),
                                payload: payload.clone(),
                                received_at: 1,
                                reply_token: None,
                            })),
                        };
                        ep.send(&ev.encode()).unwrap();
                        NodeResp { id: req.id, result: Ok(NodeOk::Done) }
                    }
                    NodeOp::Request { payload, .. } => NodeResp {
                        id: req.id,
                        result: Ok(NodeOk::Reply { payload: payload.clone() }),
                    },
                    _ => NodeResp { id: req.id, result: Ok(NodeOk::Done) },
                };
                if ep.send(&resp.encode()).is_err() {
                    break;
                }
            }
        })
    }

    #[tokio::test]
    async fn ops_pipeline_and_streams_deliver() {
        let (client_ep, node_ep, _view) =
            ce_lane::flow(ce_lane::LaneConfig { slot_size: 4096, n_slots: 16 }).unwrap();
        let node = fake_node(node_ep);
        let lane = LaneClient::from_endpoint(client_ep);

        // Status roundtrip.
        let ok = lane.op(NodeOp::Status).await.ok().unwrap().unwrap();
        assert!(matches!(ok, NodeOk::Status { height: 42, .. }));

        // Watch, then a publish whose self-echo arrives as a pushed event.
        let mut stream = lane.watch_stream().await.ok().unwrap();
        let ok = lane
            .op(NodeOp::Publish { topic: "t".into(), payload: b"hello".to_vec() })
            .await
            .ok()
            .unwrap()
            .unwrap();
        assert!(matches!(ok, NodeOk::Done));
        let ev = stream.next().await.expect("event pushed");
        assert_eq!(ev.topic, "t");
        assert_eq!(ev.payload, b"hello");

        // Request echoes its payload via the fake node.
        let ok = lane
            .op(NodeOp::Request {
                to: [7u8; 32],
                topic: "svc".into(),
                payload: b"ping".to_vec(),
                timeout_ms: 1000,
            })
            .await
            .ok()
            .unwrap()
            .unwrap();
        assert!(matches!(ok, NodeOk::Reply { payload } if payload == b"ping"));

        drop(lane);
        node.join().unwrap();
    }

    #[tokio::test]
    async fn lane_death_errs_pending_ops_and_ends_streams() {
        let (client_ep, node_ep, _view) =
            ce_lane::flow(ce_lane::LaneConfig { slot_size: 4096, n_slots: 16 }).unwrap();
        let lane = LaneClient::from_endpoint(client_ep);
        // Fake just the watch ack so the stream opens; the thread hands the endpoint BACK so
        // the flow's death is deterministic (only after the stream is registered).
        let ack = std::thread::spawn(move || {
            let mut buf = Vec::new();
            node_ep.recv_into(&mut buf, Some(std::time::Duration::from_secs(5))).unwrap();
            let req = NodeReq::decode(&buf).unwrap();
            node_ep.send(&NodeResp { id: req.id, result: Ok(NodeOk::Done) }.encode()).unwrap();
            node_ep
        });
        let mut stream = lane.watch_stream().await.ok().unwrap();
        let node_ep = ack.join().unwrap();
        drop(node_ep); // closes the flow: the client's reader sees Closed

        // The stream ends rather than hanging.
        assert!(stream.next().await.is_none(), "stream must end when the lane dies");
        // New ops fail as LaneGone so the caller falls back to HTTP.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match lane.op(NodeOp::Status).await {
                Err(LaneGone) => break,
                Ok(_) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Ok(_) => panic!("ops must fail once the lane is dead"),
            }
        }
        assert!(lane.is_dead());
    }
}
