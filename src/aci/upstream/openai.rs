//! Plain OpenAI-compatible upstream backend.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::Value;

use super::tls::{pinned_spki_client, response_headers};
use super::{
    PreparedUpstreamRequest, UpstreamBackend, UpstreamError, UpstreamRequest, UpstreamResponse,
    UpstreamStreamResponse, DEFAULT_UPSTREAM_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_UPSTREAM_READ_TIMEOUT_SECONDS,
};
use crate::aci::receipt::{ChannelBinding, UpstreamVerifiedEvent};

/// Version header required by the native Anthropic API on every request.
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// How the configured credential is attached to upstream requests.
enum UpstreamAuth {
    /// `Authorization: Bearer <token>` (OpenAI-compatible APIs).
    Bearer(String),
    /// `x-api-key: <key>` plus the required `anthropic-version` header
    /// (native Anthropic API; it rejects API keys sent as Bearer).
    AnthropicApiKey(String),
}

/// The minimal forwarder.
///
/// Sends `req.body` as the request body to `base_url + path`. Attaches
/// the configured credential as either a Bearer token or an Anthropic
/// `x-api-key` header.
///
/// This backend does *not* do upstream attestation. An aggregator
/// that relies on it MUST run an attested per-upstream verifier
/// elsewhere and emit `upstream.verified` with its result; this
/// object is the forwarding plumbing only.
pub struct OpenAICompatibleBackend {
    name: String,
    base_url: String,
    path: String,
    auth: Option<UpstreamAuth>,
    client: reqwest::Client,
    pinned_client: PinnedClientCache,
    connect_timeout_seconds: u64,
    read_timeout_seconds: u64,
}

struct CachedPinnedClient {
    accepted_spkis: Vec<String>,
    client: reqwest::Client,
}

/// The backend has one immutable origin and timeout policy, so retaining only
/// its current verified binding generation keeps the cache strictly bounded.
#[derive(Default)]
struct PinnedClientCache {
    current: Mutex<Option<CachedPinnedClient>>,
}

impl PinnedClientCache {
    fn get_or_build(
        &self,
        accepted_spkis: Vec<String>,
        build: impl FnOnce(&[String]) -> Result<reqwest::Client, UpstreamError>,
    ) -> Result<reqwest::Client, UpstreamError> {
        // Build while holding the lock so concurrent misses for the same
        // generation converge on one connection pool.
        let mut current = self.current.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(cached) = current.as_ref() {
            if cached.accepted_spkis == accepted_spkis {
                return Ok(cached.client.clone());
            }
        }

        let client = build(&accepted_spkis)?;
        *current = Some(CachedPinnedClient {
            accepted_spkis,
            client: client.clone(),
        });
        Ok(client)
    }
}

impl OpenAICompatibleBackend {
    pub fn new(base_url: impl Into<String>) -> Result<Self, UpstreamError> {
        Self::new_with_timeouts(
            base_url,
            DEFAULT_UPSTREAM_CONNECT_TIMEOUT_SECONDS,
            DEFAULT_UPSTREAM_READ_TIMEOUT_SECONDS,
        )
    }

    pub fn new_with_timeouts(
        base_url: impl Into<String>,
        connect_timeout_seconds: u64,
        read_timeout_seconds: u64,
    ) -> Result<Self, UpstreamError> {
        let mut base = base_url.into();
        while base.ends_with('/') {
            base.pop();
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(connect_timeout_seconds))
            .read_timeout(Duration::from_secs(read_timeout_seconds))
            .build()
            .map_err(|e| UpstreamError::Transport(e.to_string()))?;
        Ok(Self {
            name: "openai-compatible".to_string(),
            base_url: base,
            path: "/v1/chat/completions".to_string(),
            auth: None,
            client,
            pinned_client: PinnedClientCache::default(),
            connect_timeout_seconds,
            read_timeout_seconds,
        })
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        let mut p = path.into();
        if !p.starts_with('/') {
            p.insert(0, '/');
        }
        self.path = p;
        self
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.auth = Some(UpstreamAuth::Bearer(token.into()));
        self
    }

    pub fn with_anthropic_api_key(mut self, key: impl Into<String>) -> Self {
        self.auth = Some(UpstreamAuth::AnthropicApiKey(key.into()));
        self
    }

    fn apply_auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            Some(UpstreamAuth::Bearer(token)) => {
                builder.header("authorization", format!("Bearer {token}"))
            }
            Some(UpstreamAuth::AnthropicApiKey(key)) => builder
                .header("x-api-key", key.as_str())
                .header("anthropic-version", ANTHROPIC_VERSION),
            None => builder,
        }
    }
}

pub(super) fn request_model_id(body: &[u8]) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    let parsed: Value = serde_json::from_slice(body).ok()?;
    parsed.get("model")?.as_str().map(str::to_string)
}

pub(super) fn rewrite_request_model(
    body: &[u8],
    upstream_model_id: &str,
) -> Result<Vec<u8>, UpstreamError> {
    let mut parsed: Value = serde_json::from_slice(body)
        .map_err(|e| UpstreamError::Routing(format!("invalid JSON request body: {e}")))?;
    let Some(obj) = parsed.as_object_mut() else {
        return Err(UpstreamError::Routing(
            "request body must be a JSON object".to_string(),
        ));
    };
    match obj.get_mut("model") {
        Some(model) if model.is_string() => {
            *model = Value::String(upstream_model_id.to_string());
        }
        _ => {
            return Err(UpstreamError::Routing(
                "request body must contain a string model field".to_string(),
            ));
        }
    }
    serde_json::to_vec(&parsed).map_err(|e| UpstreamError::Routing(e.to_string()))
}

#[async_trait]
impl UpstreamBackend for OpenAICompatibleBackend {
    fn name(&self) -> &str {
        &self.name
    }

    fn url_origin(&self) -> Option<&str> {
        Some(&self.base_url)
    }

    async fn forward(&self, req: UpstreamRequest) -> Result<UpstreamResponse, UpstreamError> {
        let resp = self
            .request_builder(&self.client, &req, "application/json")
            .body(req.body)
            .send()
            .await
            .map_err(|e| UpstreamError::Transport(e.to_string()))?;
        let status = resp.status().as_u16();
        let headers = response_headers(&resp);
        let body = resp
            .bytes()
            .await
            .map_err(|e| UpstreamError::Transport(e.to_string()))?
            .to_vec();
        Ok(UpstreamResponse {
            status_code: status,
            body,
            headers,
            served_instance_id: None,
        })
    }

    async fn forward_stream(
        &self,
        req: UpstreamRequest,
    ) -> Result<UpstreamStreamResponse, UpstreamError> {
        let resp = self
            .request_builder(&self.client, &req, "text/event-stream")
            .body(req.body)
            .send()
            .await
            .map_err(|e| UpstreamError::Transport(e.to_string()))?;
        let status = resp.status().as_u16();
        let headers = response_headers(&resp);
        let body = resp
            .bytes_stream()
            .map(|chunk| chunk.map_err(|e| UpstreamError::Transport(e.to_string())));
        Ok(UpstreamStreamResponse {
            status_code: status,
            headers,
            body: Box::pin(body),
            served_instance_id: None,
        })
    }

    async fn models(&self) -> Result<UpstreamResponse, UpstreamError> {
        self.get("/v1/models", "application/json").await
    }

    async fn forward_verified_prepared(
        &self,
        req: PreparedUpstreamRequest,
        event: &UpstreamVerifiedEvent,
    ) -> Result<UpstreamResponse, UpstreamError> {
        let client = self.client_for_event(event)?;
        let resp = self
            .request_builder(&client, &req.request, "application/json")
            .body(req.request.body)
            .send()
            .await
            .map_err(|e| UpstreamError::Transport(e.to_string()))?;
        let status = resp.status().as_u16();
        let headers = response_headers(&resp);
        let body = resp
            .bytes()
            .await
            .map_err(|e| UpstreamError::Transport(e.to_string()))?
            .to_vec();
        Ok(UpstreamResponse {
            status_code: status,
            body,
            headers,
            served_instance_id: None,
        })
    }

    async fn forward_stream_verified_prepared(
        &self,
        req: PreparedUpstreamRequest,
        event: &UpstreamVerifiedEvent,
    ) -> Result<UpstreamStreamResponse, UpstreamError> {
        let client = self.client_for_event(event)?;
        let resp = self
            .request_builder(&client, &req.request, "text/event-stream")
            .body(req.request.body)
            .send()
            .await
            .map_err(|e| UpstreamError::Transport(e.to_string()))?;
        let status = resp.status().as_u16();
        let headers = response_headers(&resp);
        let body = resp
            .bytes_stream()
            .map(|chunk| chunk.map_err(|e| UpstreamError::Transport(e.to_string())));
        Ok(UpstreamStreamResponse {
            status_code: status,
            headers,
            body: Box::pin(body),
            served_instance_id: None,
        })
    }
}

impl OpenAICompatibleBackend {
    fn client_for_event(
        &self,
        event: &UpstreamVerifiedEvent,
    ) -> Result<reqwest::Client, UpstreamError> {
        if event.channel_bindings.is_empty() {
            return Ok(self.client.clone());
        }
        let mut accepted_spkis = Vec::new();
        for binding in &event.channel_bindings {
            match binding {
                ChannelBinding::TlsSpkiSha256 {
                    origin,
                    spki_sha256,
                } if origin == &self.base_url => accepted_spkis.push(spki_sha256.clone()),
                ChannelBinding::TlsSpkiSha256 { origin, .. } => {
                    return Err(UpstreamError::Transport(format!(
                        "verified TLS SPKI binding origin {origin:?} does not match upstream {:?}",
                        self.base_url
                    )));
                }
                ChannelBinding::E2eePublicKeySha256 {
                    provider,
                    algorithm,
                    ..
                } => {
                    return Err(UpstreamError::Transport(format!(
                        "backend {} cannot enforce {provider} E2EE binding {algorithm:?}",
                        self.name
                    )));
                }
            }
        }
        if !self.base_url.starts_with("https://") {
            return Err(UpstreamError::Transport(
                "TLS channel binding requires an https upstream".to_string(),
            ));
        }
        accepted_spkis.sort_unstable();
        accepted_spkis.dedup();
        self.pinned_client
            .get_or_build(accepted_spkis, |accepted_spkis| {
                pinned_spki_client(
                    accepted_spkis.to_vec(),
                    self.connect_timeout_seconds,
                    self.read_timeout_seconds,
                )
            })
    }

    async fn get(
        &self,
        path: &str,
        accept: &'static str,
    ) -> Result<UpstreamResponse, UpstreamError> {
        let resp = self
            .get_builder(path, accept)
            .send()
            .await
            .map_err(|e| UpstreamError::Transport(e.to_string()))?;
        let status = resp.status().as_u16();
        let headers = response_headers(&resp);
        let body = resp
            .bytes()
            .await
            .map_err(|e| UpstreamError::Transport(e.to_string()))?
            .to_vec();
        Ok(UpstreamResponse {
            status_code: status,
            body,
            headers,
            served_instance_id: None,
        })
    }

    fn request_builder(
        &self,
        client: &reqwest::Client,
        req: &UpstreamRequest,
        accept: &'static str,
    ) -> reqwest::RequestBuilder {
        let path = req.path.as_deref().unwrap_or(&self.path);
        let url = format!("{}{}", self.base_url, path);
        let mut builder = client
            .post(&url)
            .header("content-type", "application/json")
            .header("accept", accept);
        for (k, v) in req.headers.iter() {
            builder = builder.header(k, v);
        }
        self.apply_auth(builder)
    }

    fn get_builder(&self, path: &str, accept: &'static str) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.base_url, path);
        let builder = self.client.get(&url).header("accept", accept);
        self.apply_auth(builder)
    }

    #[cfg(test)]
    fn pinned_client_cache_len(&self) -> usize {
        usize::from(self.pinned_client.current.lock().unwrap().is_some())
    }

    #[cfg(test)]
    fn pinned_client_accepted_spkis(&self) -> Vec<String> {
        self.pinned_client
            .current
            .lock()
            .unwrap()
            .as_ref()
            .map(|cached| cached.accepted_spkis.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    use super::*;
    use crate::aci::receipt::VerificationResult;

    #[test]
    fn anthropic_auth_sends_x_api_key_not_bearer() {
        let backend = OpenAICompatibleBackend::new("https://api.anthropic.com")
            .unwrap()
            .with_anthropic_api_key("sk-test");
        let req = backend
            .get_builder("/v1/models", "application/json")
            .build()
            .unwrap();
        assert_eq!(req.headers().get("x-api-key").unwrap(), "sk-test");
        assert_eq!(
            req.headers().get("anthropic-version").unwrap(),
            ANTHROPIC_VERSION
        );
        assert!(req.headers().get("authorization").is_none());

        let bearer = OpenAICompatibleBackend::new("https://example.com")
            .unwrap()
            .with_bearer_token("tok");
        let req = bearer
            .get_builder("/v1/models", "application/json")
            .build()
            .unwrap();
        assert_eq!(req.headers().get("authorization").unwrap(), "Bearer tok");
        assert!(req.headers().get("x-api-key").is_none());
    }

    #[test]
    fn pinned_tls_client_cache_reuses_same_binding() {
        let backend = OpenAICompatibleBackend::new("https://upstream.example").unwrap();
        let event = tls_spki_event(
            "https://upstream.example",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );

        backend.client_for_event(&event).unwrap();
        backend.client_for_event(&event).unwrap();

        assert_eq!(backend.pinned_client_cache_len(), 1);
    }

    #[test]
    fn client_for_event_canonicalizes_pin_sets() {
        let backend = OpenAICompatibleBackend::new("https://upstream.example").unwrap();
        let first = tls_spki_event_with_two_pins(
            "https://upstream.example",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        );
        let second = tls_spki_event_with_two_pins(
            "https://upstream.example",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );

        backend.client_for_event(&first).unwrap();
        backend.client_for_event(&second).unwrap();

        assert_eq!(backend.pinned_client_cache_len(), 1);
        assert_eq!(
            backend.pinned_client_accepted_spkis(),
            vec![
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            ]
        );
    }

    #[test]
    fn pinned_tls_client_cache_rotates_on_binding_change() {
        let backend = OpenAICompatibleBackend::new("https://upstream.example").unwrap();

        backend
            .client_for_event(&tls_spki_event(
                "https://upstream.example",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ))
            .unwrap();
        backend
            .client_for_event(&tls_spki_event(
                "https://upstream.example",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ))
            .unwrap();

        assert_eq!(backend.pinned_client_cache_len(), 1);
        assert_eq!(
            backend.pinned_client_accepted_spkis(),
            vec!["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()]
        );
    }

    #[test]
    fn pinned_client_cache_converges_concurrent_misses_and_rotates() {
        const THREADS: usize = 8;

        let cache = Arc::new(PinnedClientCache::default());
        let builds = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles = (0..THREADS)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let builds = Arc::clone(&builds);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    cache
                        .get_or_build(vec!["a".to_string()], |_| {
                            builds.fetch_add(1, Ordering::SeqCst);
                            Ok(reqwest::Client::new())
                        })
                        .unwrap();
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(builds.load(Ordering::SeqCst), 1);

        for _ in 0..2 {
            cache
                .get_or_build(vec!["b".to_string()], |_| {
                    builds.fetch_add(1, Ordering::SeqCst);
                    Ok(reqwest::Client::new())
                })
                .unwrap();
        }
        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn pinned_tls_client_cache_rejects_origin_mismatch_without_caching() {
        let backend = OpenAICompatibleBackend::new("https://upstream.example").unwrap();
        let event = tls_spki_event(
            "https://other.example",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );

        let err = backend.client_for_event(&event).unwrap_err();

        assert!(err
            .to_string()
            .contains("does not match upstream \"https://upstream.example\""));
        assert_eq!(backend.pinned_client_cache_len(), 0);
    }

    #[test]
    fn no_channel_binding_uses_default_client_without_pinned_cache() {
        let backend = OpenAICompatibleBackend::new("https://upstream.example").unwrap();
        let event = UpstreamVerifiedEvent {
            result: VerificationResult::Verified,
            ..Default::default()
        };

        backend.client_for_event(&event).unwrap();

        assert_eq!(backend.pinned_client_cache_len(), 0);
    }

    fn tls_spki_event(origin: &str, spki_sha256: &str) -> UpstreamVerifiedEvent {
        UpstreamVerifiedEvent {
            result: VerificationResult::Verified,
            channel_bindings: vec![ChannelBinding::TlsSpkiSha256 {
                origin: origin.to_string(),
                spki_sha256: spki_sha256.to_string(),
            }],
            ..Default::default()
        }
    }

    fn tls_spki_event_with_two_pins(
        origin: &str,
        first_spki_sha256: &str,
        second_spki_sha256: &str,
    ) -> UpstreamVerifiedEvent {
        UpstreamVerifiedEvent {
            result: VerificationResult::Verified,
            channel_bindings: vec![
                ChannelBinding::TlsSpkiSha256 {
                    origin: origin.to_string(),
                    spki_sha256: first_spki_sha256.to_string(),
                },
                ChannelBinding::TlsSpkiSha256 {
                    origin: origin.to_string(),
                    spki_sha256: second_spki_sha256.to_string(),
                },
            ],
            ..Default::default()
        }
    }
}
