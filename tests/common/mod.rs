//! A tiny, dependency-free mock HTTP server for exercising `CeClient` against canned and
//! programmable responses — happy paths, node errors (402/404/500), malformed bodies, and
//! truncated SSE frames — without a real node.
//!
//! It speaks just enough HTTP/1.1 for `reqwest`: it reads the request line + headers (and a body
//! sized by `Content-Length`), matches `METHOD PATH` against a route table, and writes back a
//! fixed status + body. Routes can be exact (`POST /transfer`) or prefix (`GET /jobs/`).
//!
//! This is a shared test module included by the integration test files via `mod mock_server;`.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// What a route returns, plus how to write it (so we can simulate truncation / connection drops).
#[derive(Clone)]
pub struct Reply {
    pub status: u16,
    pub reason: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Drop the connection after writing `truncate_after` bytes of the body (simulate a torn
    /// SSE frame / network failure). `None` = write the whole body normally.
    pub truncate_after: Option<usize>,
    /// Don't send Content-Length / send a streaming body (used for SSE).
    pub streaming: bool,
}

impl Reply {
    pub fn json(status: u16, body: impl Into<String>) -> Reply {
        Reply {
            status,
            reason: reason_for(status),
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: body.into().into_bytes(),
            truncate_after: None,
            streaming: false,
        }
    }

    pub fn text(status: u16, body: impl Into<String>) -> Reply {
        Reply {
            status,
            reason: reason_for(status),
            headers: vec![("Content-Type".into(), "text/plain".into())],
            body: body.into().into_bytes(),
            truncate_after: None,
            streaming: false,
        }
    }

    pub fn bytes(status: u16, content_type: &str, body: Vec<u8>) -> Reply {
        Reply {
            status,
            reason: reason_for(status),
            headers: vec![("Content-Type".into(), content_type.into())],
            body,
            truncate_after: None,
            streaming: false,
        }
    }

    /// An SSE body: declared as a stream, optionally truncated mid-frame.
    pub fn sse(body: impl Into<String>, truncate_after: Option<usize>) -> Reply {
        Reply {
            status: 200,
            reason: "OK".into(),
            headers: vec![("Content-Type".into(), "text/event-stream".into())],
            body: body.into().into_bytes(),
            truncate_after,
            streaming: true,
        }
    }

    pub fn empty(status: u16) -> Reply {
        Reply {
            status,
            reason: reason_for(status),
            headers: vec![],
            body: Vec::new(),
            truncate_after: None,
            streaming: false,
        }
    }
}

fn reason_for(status: u16) -> String {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Status",
    }
    .to_string()
}

/// A captured request, so tests can assert what the SDK actually sent (auth header, body, etc.).
#[derive(Clone, Debug)]
pub struct CapturedRequest {
    pub method: String,
    pub path: String,
    pub authorization: Option<String>,
    pub accept: Option<String>,
    pub body: Vec<u8>,
}

impl CapturedRequest {
    pub fn body_str(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }
    pub fn body_json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }
}

type Handler = Arc<dyn Fn(&CapturedRequest) -> Reply + Send + Sync>;

/// A programmable mock server. Build with routes, then `start()` it and point a `CeClient` at
/// `base_url()`.
#[derive(Clone)]
pub struct MockServer {
    exact: Arc<Mutex<HashMap<String, Handler>>>,
    prefix: Arc<Mutex<Vec<(String, String, Handler)>>>, // (method, path-prefix, handler)
    default: Handler,
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    base: Arc<Mutex<String>>,
}

impl MockServer {
    pub fn new() -> Self {
        MockServer {
            exact: Arc::new(Mutex::new(HashMap::new())),
            prefix: Arc::new(Mutex::new(Vec::new())),
            default: Arc::new(|req| {
                Reply::text(404, format!("no route for {} {}", req.method, req.path))
            }),
            captured: Arc::new(Mutex::new(Vec::new())),
            base: Arc::new(Mutex::new(String::new())),
        }
    }

    /// Register an exact `METHOD PATH` route returning a fixed reply.
    pub fn route(self, method: &str, path: &str, reply: Reply) -> Self {
        let key = format!("{} {}", method.to_uppercase(), path);
        self.exact
            .lock()
            .unwrap()
            .insert(key, Arc::new(move |_req| reply.clone()));
        self
    }

    /// Register an exact route with a closure that inspects the request.
    pub fn route_fn(
        self,
        method: &str,
        path: &str,
        f: impl Fn(&CapturedRequest) -> Reply + Send + Sync + 'static,
    ) -> Self {
        let key = format!("{} {}", method.to_uppercase(), path);
        self.exact.lock().unwrap().insert(key, Arc::new(f));
        self
    }

    /// Register a prefix route (e.g. `GET /jobs/` matches `/jobs/abc`).
    pub fn route_prefix(self, method: &str, prefix: &str, reply: Reply) -> Self {
        self.prefix.lock().unwrap().push((
            method.to_uppercase(),
            prefix.to_string(),
            Arc::new(move |_req| reply.clone()),
        ));
        self
    }

    pub fn route_prefix_fn(
        self,
        method: &str,
        prefix: &str,
        f: impl Fn(&CapturedRequest) -> Reply + Send + Sync + 'static,
    ) -> Self {
        self.prefix
            .lock()
            .unwrap()
            .push((method.to_uppercase(), prefix.to_string(), Arc::new(f)));
        self
    }

    pub fn base_url(&self) -> String {
        self.base.lock().unwrap().clone()
    }

    pub fn requests(&self) -> Vec<CapturedRequest> {
        self.captured.lock().unwrap().clone()
    }

    pub fn last_request(&self) -> Option<CapturedRequest> {
        self.captured.lock().unwrap().last().cloned()
    }

    fn resolve(&self, req: &CapturedRequest) -> Reply {
        let key = format!("{} {}", req.method, req.path_only());
        if let Some(h) = self.exact.lock().unwrap().get(&key) {
            return h(req);
        }
        for (m, pfx, h) in self.prefix.lock().unwrap().iter() {
            if *m == req.method && req.path_only().starts_with(pfx.as_str()) {
                return h(req);
            }
        }
        (self.default)(req)
    }

    /// Bind to an ephemeral port and start serving in the background. Returns immediately with the
    /// server (its `base_url()` is now set). The accept loop runs until the test ends.
    pub async fn start(self) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        *self.base.lock().unwrap() = format!("http://{addr}");
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(p) => p,
                    Err(_) => break,
                };
                let me2 = me.clone();
                tokio::spawn(async move {
                    let _ = me2.handle_conn(&mut sock).await;
                });
            }
        });
        self
    }

    async fn handle_conn(&self, sock: &mut tokio::net::TcpStream) -> std::io::Result<()> {
        // Read until we have headers (\r\n\r\n), then the Content-Length body.
        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];
        let header_end = loop {
            if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                break pos;
            }
            let n = sock.read(&mut tmp).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.len() > 64 * 1024 * 1024 {
                return Ok(());
            }
        };
        let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
        let mut lines = head.split("\r\n");
        let request_line = lines.next().unwrap_or("");
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let path = parts.next().unwrap_or("").to_string();
        let mut authorization = None;
        let mut accept = None;
        let mut content_length = 0usize;
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                let k = k.trim().to_ascii_lowercase();
                let v = v.trim().to_string();
                match k.as_str() {
                    "authorization" => authorization = Some(v),
                    "accept" => accept = Some(v),
                    "content-length" => content_length = v.parse().unwrap_or(0),
                    _ => {}
                }
            }
        }
        let mut body = buf[header_end + 4..].to_vec();
        while body.len() < content_length {
            let n = sock.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }
        body.truncate(content_length);

        let req = CapturedRequest { method, path, authorization, accept, body };
        self.captured.lock().unwrap().push(req.clone());
        let reply = self.resolve(&req);

        // Write status line + headers.
        let mut out = format!("HTTP/1.1 {} {}\r\n", reply.status, reply.reason);
        for (k, v) in &reply.headers {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
        let to_write = match reply.truncate_after {
            Some(n) => n.min(reply.body.len()),
            None => reply.body.len(),
        };
        if reply.streaming {
            // No Content-Length; close-delimited body (reqwest reads until EOF).
            out.push_str("Connection: close\r\n\r\n");
            sock.write_all(out.as_bytes()).await?;
            sock.write_all(&reply.body[..to_write]).await?;
            // For truncation we simply close before the full body — already done.
        } else {
            // Always advertise the *real* length; if we truncate, the client sees a short read.
            out.push_str(&format!("Content-Length: {}\r\n", reply.body.len()));
            out.push_str("Connection: close\r\n\r\n");
            sock.write_all(out.as_bytes()).await?;
            sock.write_all(&reply.body[..to_write]).await?;
        }
        let _ = sock.flush().await;
        Ok(())
    }
}

impl CapturedRequest {
    /// The path without any query string.
    pub fn path_only(&self) -> &str {
        self.path.split('?').next().unwrap_or(&self.path)
    }
    /// The query string (after `?`), if any.
    pub fn query(&self) -> Option<&str> {
        self.path.split_once('?').map(|(_, q)| q)
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}
