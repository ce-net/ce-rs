//! Wallet abstraction over the node's existing money endpoints.
//!
//! A [`Wallet`] is a thin, read-and-spend view of a single node's credits, composing endpoints
//! that already exist on [`CeClient`](crate::CeClient): a balance breakdown from `/status`,
//! itemized transaction history from `/transactions/:node_id` (paginated), transfers, payment
//! channels, and a live tx-history stream over `/transactions/stream`.
//!
//! It holds **no key material**. Operations that need a payer signature (job settlement, channel
//! receipts) are signed by the node behind the API; co-signatures are passed through as opaque
//! hex strings (matching CE's no-key-in-SDK rule). Issuing capabilities is likewise out of scope.

use crate::amount::Amount;
use crate::sse::{decode_stream, TxEvent};
use crate::{Channel, CeClient, NodeStatus, Receipt};
use anyhow::Result;
use futures_core::Stream;
use serde::Deserialize;

/// A node's credit balance, split into spendable and locked buckets. Invariant (when the node
/// is fully synced): `free + locked_channels + locked_bond == total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Balance {
    /// Total balance (may be negative on a fresh node before sync).
    pub total: Amount,
    /// Spendable balance: `total` minus all locks, clamped at zero by the node.
    pub free: Amount,
    /// Credits locked in this node's open payment channels.
    pub locked_channels: Amount,
    /// Credits locked in this node's active host bond.
    pub locked_bond: Amount,
    /// This node's active host bond (equals `locked_bond`).
    pub bond: Amount,
}

/// Value direction of a transaction relative to the queried node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    /// Credits flowed in to the queried node.
    In,
    /// Credits flowed out of the queried node.
    Out,
    /// A self-referential tx (no counterparty movement, e.g. a name claim).
    #[serde(rename = "self")]
    SelfTx,
}

/// One itemized transaction touching a node, from `GET /transactions/:node_id`.
#[derive(Debug, Clone, Deserialize)]
pub struct TxRecord {
    /// Content-addressed transaction id (64 hex).
    pub tx_id: String,
    /// Block height the tx was confirmed at.
    pub height: u64,
    /// Tx kind label, e.g. `"Transfer"`, `"JobSettle"`, `"UptimeReward"`, `"ChannelOpen"`.
    pub kind: String,
    /// Amount moved by this tx; `Amount::ZERO` for amount-less kinds.
    #[serde(default)]
    pub amount: Amount,
    /// The other party (64 hex) when there is one.
    #[serde(default)]
    pub counterparty: Option<String>,
    /// Value direction relative to the queried node.
    pub direction: Direction,
    /// Whether the tx is mined/confirmed (always `true` for history items; the node only
    /// returns confirmed txs here). Defaults to `true` for forward-compat.
    #[serde(default = "default_true")]
    pub confirmed: bool,
}

fn default_true() -> bool {
    true
}

/// Pagination cursor for [`Wallet::transactions`]. Walks newest-first; page older by passing the
/// oldest returned record's `height` as `before_height`.
#[derive(Debug, Clone, Copy, Default)]
pub struct TxQuery {
    /// Max items (the node defaults to 100 and caps at 500).
    pub limit: Option<u32>,
    /// Exclude txs at block height `>= before_height` — the cursor for the next (older) page.
    pub before_height: Option<u64>,
}

/// A wallet bound to one node's HTTP API. Cheap to clone (wraps a [`CeClient`]).
#[derive(Debug, Clone)]
pub struct Wallet {
    client: CeClient,
}

impl Wallet {
    /// Wrap an existing client.
    pub fn new(client: CeClient) -> Self {
        Wallet { client }
    }

    /// The underlying client (for endpoints not surfaced on the wallet).
    pub fn client(&self) -> &CeClient {
        &self.client
    }

    /// The node's balance breakdown (`GET /status`). Reads `free`/`locked_channels`/`locked_bond`/
    /// `bond` when the node reports them; on older nodes that only return `balance`, the locked
    /// buckets are zero and `free == total`.
    pub async fn balance(&self) -> Result<Balance> {
        let s: NodeStatus = self.client.status().await?;
        let total = s.balance;
        let locked_channels = s.locked_channels.unwrap_or(Amount::ZERO);
        let locked_bond = s.locked_bond.unwrap_or(Amount::ZERO);
        let bond = s.bond.unwrap_or(locked_bond);
        let free = s.free.unwrap_or_else(|| {
            // Derive a spendable estimate if the node didn't send `free`.
            let f = total.base() - locked_channels.base() - locked_bond.base();
            Amount::from_base(f.max(0))
        });
        Ok(Balance { total, free, locked_channels, locked_bond, bond })
    }

    /// Itemized transaction history for `node_id`, newest first (`GET /transactions/:node_id`).
    /// On a light node only post-checkpoint history is available.
    pub async fn transactions(&self, node_id: &str, q: TxQuery) -> Result<Vec<TxRecord>> {
        let mut path = format!("/transactions/{node_id}");
        let mut params = Vec::new();
        if let Some(limit) = q.limit {
            params.push(format!("limit={limit}"));
        }
        if let Some(before) = q.before_height {
            params.push(format!("before={before}"));
        }
        if !params.is_empty() {
            path.push('?');
            path.push_str(&params.join("&"));
        }
        self.client.get_json(&path).await
    }

    /// Live tail of confirmed transactions over `/transactions/stream`, mapped to a [`TxRecord`]
    /// relative to `self_node_id` (so `direction` and `counterparty` are filled in client-side).
    /// The node's stream frames are `{ id, origin, kind, amount }`; we enrich them here.
    pub async fn transactions_stream(
        &self,
        self_node_id: &str,
    ) -> Result<impl Stream<Item = Result<TxRecord>>> {
        let self_id = self_node_id.to_string();
        let inner = self.client.transactions_stream().await?;
        use futures_util::StreamExt;
        Ok(inner.map(move |item| item.map(|ev| tx_event_to_record(&ev, &self_id))))
    }

    // ----- spend (pass-through to existing endpoints) -----

    /// Transfer credits to another node; returns the tx id (`POST /transfer`).
    pub async fn transfer(&self, to: &str, amount: Amount) -> Result<String> {
        self.client.transfer(to, amount).await
    }

    /// Open an off-chain payment channel paying `host`, locking `capacity` (`POST /channels/open`).
    pub async fn open_channel(&self, host: &str, capacity: Amount, expiry_height: u64) -> Result<String> {
        self.client.channel_open(host, capacity, expiry_height).await
    }

    /// Sign an off-chain receipt as the payer for `cumulative` total (`POST /channels/receipt`).
    /// The node signs with its own key; the returned `payer_sig` is an opaque pass-through.
    pub async fn sign_receipt(&self, channel_id: &str, host: &str, cumulative: Amount) -> Result<Receipt> {
        self.client.sign_receipt(channel_id, host, cumulative).await
    }

    /// Redeem a receipt to close a channel, on the host node (`POST /channels/:id/close`). The
    /// payer's co-signature is passed through as opaque hex — the wallet never produces it.
    pub async fn close_channel(&self, channel_id: &str, cumulative: Amount, payer_sig: &str) -> Result<()> {
        self.client.channel_close(channel_id, cumulative, payer_sig).await
    }

    /// Reclaim a channel after expiry, on the payer node (`POST /channels/:id/expire`).
    pub async fn expire_channel(&self, channel_id: &str) -> Result<()> {
        self.client.channel_expire(channel_id).await
    }

    /// List open payment channels (`GET /channels`).
    pub async fn channels(&self) -> Result<Vec<Channel>> {
        self.client.channels().await
    }
}

/// Map a raw `/transactions/stream` event to a [`TxRecord`] relative to `self_id`. The stream
/// only carries `{ id, origin, kind, amount }`, so `direction`/`counterparty` are derived from
/// who originated the tx; `height` is unknown for the live tail (0).
fn tx_event_to_record(ev: &TxEvent, self_id: &str) -> TxRecord {
    // `origin` is the tx signer. If we originated it, it's outbound; otherwise inbound to us.
    let direction = if ev.origin == self_id { Direction::Out } else { Direction::In };
    let counterparty = if ev.origin == self_id { None } else { Some(ev.origin.clone()) };
    TxRecord {
        tx_id: ev.id.clone(),
        height: 0,
        kind: ev.kind.clone(),
        amount: ev.amount,
        counterparty,
        direction,
        confirmed: true,
    }
}

impl CeClient {
    /// A [`Wallet`] view over this client.
    pub fn wallet(&self) -> Wallet {
        Wallet::new(self.clone())
    }

    /// Balance breakdown convenience (`GET /status`). Equivalent to `self.wallet().balance()`.
    pub async fn balance(&self) -> Result<Balance> {
        self.wallet().balance().await
    }

    /// Itemized transaction history (`GET /transactions/:node_id`). Equivalent to
    /// `self.wallet().transactions(node_id, q)`.
    pub async fn transactions(&self, node_id: &str, q: TxQuery) -> Result<Vec<TxRecord>> {
        self.wallet().transactions(node_id, q).await
    }
}

// Used by `Wallet::transactions_stream` to avoid leaking the SSE internals into wallet.rs.
impl CeClient {
    pub(crate) async fn transactions_stream(&self) -> Result<impl Stream<Item = Result<TxEvent>>> {
        let resp = self.open_sse("/transactions/stream").await?;
        Ok(decode_stream::<TxEvent>(resp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balance_invariant_holds() {
        // free + locked_channels + locked_bond == total, the documented /status invariant.
        let total = Amount::from_credits(12_345);
        let locked_channels = Amount::from_credits(1_000);
        let locked_bond = Amount::from_credits(345);
        let free = Amount::from_base(
            total.base() - locked_channels.base() - locked_bond.base(),
        );
        let b = Balance { total, free, locked_channels, locked_bond, bond: locked_bond };
        assert_eq!(
            b.free.base() + b.locked_channels.base() + b.locked_bond.base(),
            b.total.base()
        );
        assert_eq!(b.free.credits(), "11000");
    }

    #[test]
    fn tx_record_decodes_from_node_json() {
        let json = r#"{
            "tx_id": "aa",
            "height": 12,
            "kind": "Transfer",
            "amount": "2500000000000000000",
            "counterparty": "peer",
            "direction": "out"
        }"#;
        let r: TxRecord = serde_json::from_str(json).unwrap();
        assert_eq!(r.tx_id, "aa");
        assert_eq!(r.height, 12);
        assert_eq!(r.amount.credits(), "2.5");
        assert_eq!(r.counterparty.as_deref(), Some("peer"));
        assert_eq!(r.direction, Direction::Out);
        assert!(r.confirmed); // defaulted
    }

    #[test]
    fn tx_record_handles_amountless_self_tx() {
        let json = r#"{
            "tx_id": "bb",
            "height": 3,
            "kind": "NameClaim",
            "amount": "0",
            "counterparty": null,
            "direction": "self"
        }"#;
        let r: TxRecord = serde_json::from_str(json).unwrap();
        assert!(r.amount.is_zero());
        assert!(r.counterparty.is_none());
        assert_eq!(r.direction, Direction::SelfTx);
    }

    #[test]
    fn stream_event_maps_to_outbound_when_self_originates() {
        let ev = TxEvent {
            id: "t1".into(),
            origin: "me".into(),
            kind: "Transfer".into(),
            amount: Amount::from_credits(5),
        };
        let out = tx_event_to_record(&ev, "me");
        assert_eq!(out.direction, Direction::Out);
        assert!(out.counterparty.is_none());

        let inbound = tx_event_to_record(&ev, "someone-else");
        assert_eq!(inbound.direction, Direction::In);
        assert_eq!(inbound.counterparty.as_deref(), Some("me"));
    }

    #[test]
    fn tx_query_builds_pagination_params() {
        // Indirect: build the path the way `transactions` does and assert the query string.
        let q = TxQuery { limit: Some(50), before_height: Some(13) };
        let mut path = String::from("/transactions/node");
        let mut params = Vec::new();
        if let Some(l) = q.limit {
            params.push(format!("limit={l}"));
        }
        if let Some(b) = q.before_height {
            params.push(format!("before={b}"));
        }
        if !params.is_empty() {
            path.push('?');
            path.push_str(&params.join("&"));
        }
        assert_eq!(path, "/transactions/node?limit=50&before=13");
    }
}
