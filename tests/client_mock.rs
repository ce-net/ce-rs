//! Mock-HTTP coverage of the substrate `CeClient` methods: the happy path (correct request shape
//! sent, response parsed), and node-error paths (404/500, malformed bodies, missing fields).
//!
//! These run with no node — a hand-rolled mock server ([`common::MockServer`]) returns canned
//! responses and captures what the SDK sent, so we assert both directions of the contract.
//!
//! Money (transfer/bid/deploy/channels/paid-fetch) and reputation (history) moved to the economy
//! and trust ceapp SDKs (`ce_economy`, `ce_ratio`); their mock tests live there now, not here.

mod common;
use ce_rs::CeClient;
use common::{MockServer, Reply};

fn client_for(server: &MockServer) -> CeClient {
    CeClient::with_token(server.base_url(), Some("test-token".into()))
}

// ---------------------------------------------------------------------------
// Auth header plumbing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn token_is_sent_as_bearer_on_every_request() {
    let server = MockServer::new()
        .route("GET", "/status", Reply::json(200, status_json()))
        .start()
        .await;
    let ce = client_for(&server);
    ce.status().await.unwrap();
    let req = server.last_request().unwrap();
    assert_eq!(req.authorization.as_deref(), Some("Bearer test-token"));
}

#[tokio::test]
async fn no_token_sends_no_auth_header() {
    let server = MockServer::new()
        .route("GET", "/status", Reply::json(200, status_json()))
        .start()
        .await;
    let ce = CeClient::with_token(server.base_url(), None);
    ce.status().await.unwrap();
    assert_eq!(server.last_request().unwrap().authorization, None);
}

#[tokio::test]
async fn empty_and_whitespace_token_send_no_auth_header() {
    for tok in ["", "   ", "\n\t "] {
        let server = MockServer::new()
            .route("GET", "/status", Reply::json(200, status_json()))
            .start()
            .await;
        let ce = CeClient::with_token(server.base_url(), Some(tok.into()));
        ce.status().await.unwrap();
        assert_eq!(
            server.last_request().unwrap().authorization,
            None,
            "token {tok:?} should not produce an auth header"
        );
    }
}

#[tokio::test]
async fn trailing_slash_in_base_url_is_trimmed() {
    let server = MockServer::new()
        .route("GET", "/health", Reply::empty(200))
        .start()
        .await;
    let ce = CeClient::with_token(format!("{}/", server.base_url()), None);
    assert!(ce.health().await.unwrap());
    // The path must be exactly "/health", not "//health".
    assert_eq!(server.last_request().unwrap().path_only(), "/health");
}

// ---------------------------------------------------------------------------
// Read endpoints
// ---------------------------------------------------------------------------

fn status_json() -> String {
    r#"{"node_id":"abc","peer_id":"p","listen_port":4001,"economy":false}"#.into()
}

#[tokio::test]
async fn status_parses_core_substrate_shape() {
    let server = MockServer::new()
        .route("GET", "/status", Reply::json(200, status_json()))
        .start()
        .await;
    let ce = client_for(&server);
    let s = ce.status().await.unwrap();
    assert_eq!(s.node_id, "abc");
    assert_eq!(s.peer_id, "p");
    assert_eq!(s.listen_port, 4001);
    assert!(!s.economy_enabled());

    // An economy node's richer /status still decodes (extra chain fields ignored); a missing
    // `economy` flag is treated as economy-on (the historical default).
    let full = r#"{"node_id":"n","height":1,"difficulty":1,"balance":"10000000000000000000"}"#;
    let server2 = MockServer::new()
        .route("GET", "/status", Reply::json(200, full))
        .start()
        .await;
    let s2 = client_for(&server2).status().await.unwrap();
    assert_eq!(s2.node_id, "n");
    assert!(s2.economy_enabled());
}

#[tokio::test]
async fn status_500_surfaces_status_and_body() {
    let server = MockServer::new()
        .route("GET", "/status", Reply::text(500, "boom"))
        .start()
        .await;
    let err = client_for(&server).status().await.unwrap_err().to_string();
    assert!(err.contains("500"), "{err}");
    assert!(err.contains("boom"), "{err}");
}

#[tokio::test]
async fn status_malformed_json_is_a_decode_error_not_a_panic() {
    let server = MockServer::new()
        .route("GET", "/status", Reply::json(200, "{not json"))
        .start()
        .await;
    let err = client_for(&server).status().await.unwrap_err().to_string();
    assert!(err.contains("decode"), "{err}");
}

#[tokio::test]
async fn health_true_on_2xx_false_on_error_status() {
    let ok = MockServer::new().route("GET", "/health", Reply::empty(204)).start().await;
    assert!(client_for(&ok).health().await.unwrap());

    let down = MockServer::new().route("GET", "/health", Reply::empty(503)).start().await;
    assert!(!client_for(&down).health().await.unwrap());
}

#[tokio::test]
async fn health_connection_refused_is_err() {
    // Nothing listening on this port.
    let ce = CeClient::with_token("http://127.0.0.1:1", None);
    assert!(ce.health().await.is_err());
}

#[tokio::test]
async fn atlas_parses_entries_and_has_tag() {
    let body = r#"[
        {"node_id":"a","cpu_cores":8,"mem_mb":16000,"running_jobs":2,"last_seen_secs":5,"tags":["gpu","linux"]},
        {"node_id":"b","cpu_cores":4,"mem_mb":8000,"running_jobs":0,"last_seen_secs":1}
    ]"#;
    let server = MockServer::new().route("GET", "/atlas", Reply::json(200, body)).start().await;
    let atlas = client_for(&server).atlas().await.unwrap();
    assert_eq!(atlas.len(), 2);
    assert!(atlas[0].has_tag("gpu"));
    assert!(!atlas[0].has_tag("tpu"));
    assert!(atlas[1].tags.is_empty()); // default
}

#[tokio::test]
async fn beacon_parses() {
    let server = MockServer::new()
        .route("GET", "/beacon", Reply::json(200, r#"{"height":99,"hash":"deadbeef"}"#))
        .start()
        .await;
    let b = client_for(&server).beacon().await.unwrap();
    assert_eq!(b.height, 99);
    assert_eq!(b.hash, "deadbeef");
}

#[tokio::test]
async fn revoked_maps_pairs() {
    let body = r#"[{"issuer":"iss1","nonce":7},{"issuer":"iss2","nonce":9}]"#;
    let server = MockServer::new()
        .route("GET", "/capabilities/revoked", Reply::json(200, body))
        .start()
        .await;
    let revoked = client_for(&server).revoked().await.unwrap();
    assert_eq!(revoked, vec![("iss1".to_string(), 7), ("iss2".to_string(), 9)]);
}

#[tokio::test]
async fn revoked_empty() {
    let server = MockServer::new()
        .route("GET", "/capabilities/revoked", Reply::json(200, "[]"))
        .start()
        .await;
    assert!(client_for(&server).revoked().await.unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Kill (directed mesh kill + local kill)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mesh_kill_and_local_kill() {
    let server = MockServer::new()
        .route_fn("POST", "/mesh-kill", |req| {
            let v = req.body_json();
            assert_eq!(v["node_id"], "h");
            assert_eq!(v["job_id"], "j");
            Reply::empty(200)
        })
        .route_prefix("DELETE", "/jobs/", Reply::empty(204))
        .start()
        .await;
    let ce = client_for(&server);
    ce.mesh_kill("h", "j", None).await.unwrap();
    ce.kill("local-job").await.unwrap();
}

#[tokio::test]
async fn kill_404_is_error() {
    let server = MockServer::new()
        .route_prefix("DELETE", "/jobs/", Reply::text(404, "gone"))
        .start()
        .await;
    assert!(client_for(&server).kill("x").await.is_err());
}

// ---------------------------------------------------------------------------
// Blobs / objects
// ---------------------------------------------------------------------------

#[tokio::test]
async fn put_and_get_blob() {
    let server = MockServer::new()
        .route_fn("POST", "/blobs", |req| {
            // Body is the raw bytes.
            assert_eq!(req.body, b"hello");
            Reply::json(200, r#"{"hash":"abc123"}"#)
        })
        .route("GET", "/blobs/abc123", Reply::bytes(200, "application/octet-stream", b"hello".to_vec()))
        .route("GET", "/blobs/missing", Reply::text(404, "not found"))
        .start()
        .await;
    let ce = client_for(&server);
    assert_eq!(ce.put_blob(b"hello".to_vec()).await.unwrap(), "abc123");
    assert_eq!(ce.get_blob("abc123").await.unwrap(), b"hello");
    let err = ce.get_blob("missing").await.unwrap_err().to_string();
    assert!(err.contains("blob not found"), "{err}");
}

#[tokio::test]
async fn put_object_uploads_chunks_then_manifest() {
    use ce_rs::cid;
    // Use a small payload (single chunk). The SDK posts the chunk, then the manifest.
    let data = b"some object bytes".to_vec();
    let chunk_cid = cid(&data);
    let server = MockServer::new()
        .route_fn("POST", "/blobs", move |req| {
            // The node echoes back sha256 of whatever it received (content addressing).
            let h = ce_rs::cid(&req.body);
            Reply::json(200, format!(r#"{{"hash":"{h}"}}"#))
        })
        .start()
        .await;
    let object_cid = client_for(&server).put_object(&data).await.unwrap();
    // Two POSTs: the chunk, then the manifest.
    let reqs = server.requests();
    assert_eq!(reqs.len(), 2);
    assert_eq!(ce_rs::cid(&reqs[0].body), chunk_cid);
    // Object CID is the manifest hash = sha256 of the second body.
    assert_eq!(object_cid, ce_rs::cid(&reqs[1].body));
}

#[tokio::test]
async fn put_object_detects_hash_mismatch() {
    // The node lies about the chunk hash -> SDK must reject (it verifies the echoed hash).
    let server = MockServer::new()
        .route("POST", "/blobs", Reply::json(200, r#"{"hash":"wrong-hash"}"#))
        .start()
        .await;
    let err = client_for(&server).put_object(b"data").await.unwrap_err().to_string();
    assert!(err.contains("mismatch"), "{err}");
}

#[tokio::test]
async fn get_object_round_trips_and_verifies() {
    use ce_rs::cid;
    let data = b"round trip object".to_vec();
    let chunk_cid = cid(&data);
    let manifest = serde_json::json!({
        "kind": "ce-object-v1",
        "chunk_size": 1048576u64,
        "total_size": data.len() as u64,
        "chunks": [chunk_cid],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_cid = cid(&manifest_bytes);
    let mcid = manifest_cid.clone();
    let data2 = data.clone();
    let cc = chunk_cid.clone();
    let server = MockServer::new()
        .route_prefix_fn("GET", "/blobs/", move |req| {
            let h = req.path_only().trim_start_matches("/blobs/");
            if h == mcid {
                Reply::bytes(200, "application/octet-stream", manifest_bytes.clone())
            } else if h == cc {
                Reply::bytes(200, "application/octet-stream", data2.clone())
            } else {
                Reply::text(404, "nope")
            }
        })
        .start()
        .await;
    let got = client_for(&server).get_object(&manifest_cid).await.unwrap();
    assert_eq!(got, data);
}

#[tokio::test]
async fn get_object_rejects_a_tampered_chunk() {
    use ce_rs::cid;
    let data = b"original".to_vec();
    let chunk_cid = cid(&data); // claimed CID
    let manifest = serde_json::json!({
        "kind":"ce-object-v1","chunk_size":1048576u64,"total_size":data.len() as u64,"chunks":[chunk_cid]
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_cid = cid(&manifest_bytes);
    let mcid = manifest_cid.clone();
    let cc = chunk_cid.clone();
    let server = MockServer::new()
        .route_prefix_fn("GET", "/blobs/", move |req| {
            let h = req.path_only().trim_start_matches("/blobs/");
            if h == mcid {
                Reply::bytes(200, "application/octet-stream", manifest_bytes.clone())
            } else if h == cc {
                // Hand back the wrong bytes for this CID.
                Reply::bytes(200, "application/octet-stream", b"TAMPERED".to_vec())
            } else {
                Reply::text(404, "nope")
            }
        })
        .start()
        .await;
    let err = client_for(&server).get_object(&manifest_cid).await.unwrap_err().to_string();
    assert!(err.contains("verification failed"), "{err}");
}

#[tokio::test]
async fn get_object_rejects_non_manifest_blob() {
    use ce_rs::cid;
    let junk = b"not a manifest at all".to_vec();
    let junk_cid = cid(&junk);
    let server = MockServer::new()
        .route_prefix("GET", "/blobs/", Reply::bytes(200, "application/octet-stream", junk.clone()))
        .start()
        .await;
    let err = client_for(&server).get_object(&junk_cid).await.unwrap_err().to_string();
    assert!(err.contains("not a v1 object manifest"), "{err}");
}

#[tokio::test]
async fn get_object_rejects_unsupported_manifest_kind() {
    use ce_rs::cid;
    let manifest = serde_json::json!({"kind":"ce-object-v2","chunk_size":1u64,"total_size":0u64,"chunks":[]});
    let mb = serde_json::to_vec(&manifest).unwrap();
    let mcid = cid(&mb);
    let server = MockServer::new()
        .route_prefix("GET", "/blobs/", Reply::bytes(200, "application/octet-stream", mb))
        .start()
        .await;
    let err = client_for(&server).get_object(&mcid).await.unwrap_err().to_string();
    assert!(err.contains("unsupported manifest kind"), "{err}");
}

// ---------------------------------------------------------------------------
// App messaging
// ---------------------------------------------------------------------------

#[tokio::test]
async fn send_message_hex_encodes_payload() {
    let server = MockServer::new()
        .route_fn("POST", "/mesh/send", |req| {
            let v = req.body_json();
            assert_eq!(v["to"], "peer");
            assert_eq!(v["topic"], "chat");
            assert_eq!(v["payload_hex"], hex::encode(b"hi there"));
            Reply::empty(200)
        })
        .start()
        .await;
    client_for(&server).send_message("peer", "chat", b"hi there").await.unwrap();
}

#[tokio::test]
async fn messages_parses_and_decodes_payload() {
    let body = format!(
        r#"[{{"from":"f","topic":"t","payload_hex":"{}","received_at":111,"reply_token":7}}]"#,
        hex::encode(b"payload")
    );
    let server = MockServer::new()
        .route("GET", "/mesh/messages", Reply::json(200, body))
        .start()
        .await;
    let msgs = client_for(&server).messages().await.unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].from, "f");
    assert_eq!(msgs[0].reply_token, Some(7));
    assert_eq!(msgs[0].payload().unwrap(), b"payload");
}

#[tokio::test]
async fn app_message_bad_hex_payload_errors_gracefully() {
    let body = r#"[{"from":"f","topic":"t","payload_hex":"zzzz","received_at":1}]"#;
    let server = MockServer::new()
        .route("GET", "/mesh/messages", Reply::json(200, body))
        .start()
        .await;
    let msgs = client_for(&server).messages().await.unwrap();
    assert!(msgs[0].payload().is_err());
    assert!(msgs[0].reply_token.is_none()); // default
}

#[tokio::test]
async fn subscribe_publish_request_reply() {
    let server = MockServer::new()
        .route_fn("POST", "/mesh/subscribe", |req| {
            assert_eq!(req.body_json()["topic"], "t");
            Reply::empty(200)
        })
        .route_fn("POST", "/mesh/publish", |req| {
            assert_eq!(req.body_json()["payload_hex"], hex::encode(b"pub"));
            Reply::empty(200)
        })
        .route_fn("POST", "/mesh/request", |req| {
            assert_eq!(req.body_json()["timeout_ms"], 5000);
            Reply::json(200, format!(r#"{{"payload_hex":"{}"}}"#, hex::encode(b"reply!")))
        })
        .route_fn("POST", "/mesh/reply", |req| {
            let v = req.body_json();
            assert_eq!(v["token"], 42);
            assert_eq!(v["payload_hex"], hex::encode(b"answer"));
            Reply::empty(200)
        })
        .start()
        .await;
    let ce = client_for(&server);
    ce.subscribe("t").await.unwrap();
    ce.publish("t", b"pub").await.unwrap();
    let r = ce.request("peer", "t", b"q", 5000).await.unwrap();
    assert_eq!(r, b"reply!");
    ce.reply(42, b"answer").await.unwrap();
}

#[tokio::test]
async fn request_bad_reply_hex_is_error() {
    let server = MockServer::new()
        .route("POST", "/mesh/request", Reply::json(200, r#"{"payload_hex":"nothex"}"#))
        .start()
        .await;
    let err = client_for(&server).request("p", "t", b"q", 100).await.unwrap_err().to_string();
    assert!(err.contains("bad reply payload hex"), "{err}");
}

// ---------------------------------------------------------------------------
// Naming + discovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn claim_name_and_advertise() {
    let server = MockServer::new()
        .route_fn("POST", "/names/claim", |req| {
            assert_eq!(req.body_json()["name"], "alice");
            Reply::empty(200)
        })
        .route_fn("POST", "/discovery/advertise", |req| {
            assert_eq!(req.body_json()["service"], "myservice");
            Reply::empty(200)
        })
        .start()
        .await;
    let ce = client_for(&server);
    ce.claim_name("alice").await.unwrap();
    ce.advertise_service("myservice").await.unwrap();
}

#[tokio::test]
async fn resolve_name_found_unclaimed_and_error() {
    let server = MockServer::new()
        .route("GET", "/names/alice", Reply::json(200, r#"{"node_id":"abc"}"#))
        .route("GET", "/names/nobody", Reply::text(404, "unclaimed"))
        .route("GET", "/names/broken", Reply::text(500, "oops"))
        .start()
        .await;
    let ce = client_for(&server);
    assert_eq!(ce.resolve_name("alice").await.unwrap().as_deref(), Some("abc"));
    assert_eq!(ce.resolve_name("nobody").await.unwrap(), None);
    assert!(ce.resolve_name("broken").await.is_err());
}

#[tokio::test]
async fn find_service_extracts_providers_and_handles_missing() {
    let server = MockServer::new()
        .route("GET", "/discovery/find/s", Reply::json(200, r#"{"providers":["a","b","c"]}"#))
        .route("GET", "/discovery/find/empty", Reply::json(200, "{}"))
        .start()
        .await;
    let ce = client_for(&server);
    assert_eq!(ce.find_service("s").await.unwrap(), vec!["a", "b", "c"]);
    assert!(ce.find_service("empty").await.unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Tags (atlas self-tagging) — verifies the tag: prefix is applied on both sides
// ---------------------------------------------------------------------------

#[tokio::test]
async fn advertise_tag_applies_prefix() {
    let server = MockServer::new()
        .route_fn("POST", "/discovery/advertise", |req| {
            assert_eq!(req.body_json()["service"], "tag:gpu");
            Reply::empty(200)
        })
        .start()
        .await;
    client_for(&server).advertise_tag("gpu").await.unwrap();
}

// The tag routes below are registered percent-encoded (`tag%3Agpu`): the client encodes path
// segments (see encode_path_segment) exactly as a real node's router expects — axum decodes
// them back — but this mock matches the raw request path, so it must hold the wire form.
#[tokio::test]
async fn find_tag_applies_prefix() {
    let server = MockServer::new()
        .route("GET", "/discovery/find/tag%3Agpu", Reply::json(200, r#"{"providers":["n1","n2"]}"#))
        .start()
        .await;
    let got = client_for(&server).find_tag("gpu").await.unwrap();
    assert_eq!(got, vec!["n1", "n2"]);
}

#[tokio::test]
async fn find_tags_all_intersects_live() {
    let server = MockServer::new()
        .route("GET", "/discovery/find/tag%3Agpu", Reply::json(200, r#"{"providers":["n1","n2","n3"]}"#))
        .route("GET", "/discovery/find/tag%3Ainfer", Reply::json(200, r#"{"providers":["n2","n3","n4"]}"#))
        .start()
        .await;
    let got = client_for(&server).find_tags_all(&["gpu", "infer"]).await.unwrap();
    assert_eq!(got, vec!["n2", "n3"]);
}

#[tokio::test]
async fn find_tags_all_empty_input_is_empty() {
    let server = MockServer::new().start().await;
    assert!(client_for(&server).find_tags_all(&[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn find_tags_any_dedups_union_live() {
    let server = MockServer::new()
        .route("GET", "/discovery/find/tag%3Aa", Reply::json(200, r#"{"providers":["n1","n2"]}"#))
        .route("GET", "/discovery/find/tag%3Ab", Reply::json(200, r#"{"providers":["n2","n4"]}"#))
        .start()
        .await;
    let got = client_for(&server).find_tags_any(&["a", "b"]).await.unwrap();
    assert_eq!(got, vec!["n1", "n2", "n4"]);
}

#[tokio::test]
async fn advertise_tags_stops_at_first_failure() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    CALLS.store(0, Ordering::SeqCst);
    let server = MockServer::new()
        .route_fn("POST", "/discovery/advertise", |req| {
            CALLS.fetch_add(1, Ordering::SeqCst);
            // Fail on the "bad" tag.
            if req.body_json()["service"] == "tag:bad" {
                Reply::text(500, "no")
            } else {
                Reply::empty(200)
            }
        })
        .start()
        .await;
    let err = client_for(&server).advertise_tags(&["ok", "bad", "never"]).await;
    assert!(err.is_err());
    // It stopped at "bad" — "never" was not attempted.
    assert_eq!(CALLS.load(Ordering::SeqCst), 2);
}
