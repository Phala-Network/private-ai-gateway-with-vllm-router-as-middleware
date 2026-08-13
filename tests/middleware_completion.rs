//! Router middleware completion tests.
//!
//! The fork supports one in-process middleware shape: one public model routed
//! across multiple configured upstreams. These tests keep the seam strict:
//! router middleware chooses ordered candidates, while `AciService` still owns
//! upstream verification, forwarding, and receipt finalization.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod common;

use async_trait::async_trait;
use axum::body::{to_bytes, Body, Bytes};
use axum::http::{header::CONTENT_TYPE, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::{routing::post, Json, Router};
use futures_util::{stream, StreamExt};
use private_ai_gateway::aci::digest::sha256_hex;
use private_ai_gateway::aci::receipt::{
    UpstreamVerifiedEvent, VerificationResult, EVENT_MIDDLEWARE_FORWARDED, EVENT_REQUEST_FORWARDED,
    EVENT_REQUEST_RECEIVED, EVENT_ROUTE_SELECTED,
};
use private_ai_gateway::aci::upstream::{
    PreparedUpstreamRequest, UpstreamBackend, UpstreamError, UpstreamRequest, UpstreamResponse,
};
use private_ai_gateway::aggregator::service::{
    AciService, AciServiceConfig, FixedClock, InMemoryReceiptStore, UpstreamVerificationRequest,
    UpstreamVerifier,
};
use private_ai_gateway::aggregator::upstream_config::{
    UpstreamConfig, UpstreamConfigManager, UpstreamProvider, UpstreamRuntimeOptions,
    UpstreamVerifierMode,
};
use private_ai_gateway::middleware::errors::Surface;
use private_ai_gateway::middleware::types::Endpoint;
use private_ai_gateway::middleware::{CompletionInput, Middleware, MiddlewareConfig};
use serde_json::{json, Value};
use tokio::net::TcpListener;

use common::{event_from_request, StaticKeyProvider, StubQuoter};

#[derive(Default)]
struct CapturedCalls {
    bodies: Mutex<Vec<Value>>,
    headers: Mutex<Vec<HashMap<String, String>>>,
}

struct MockUpstream {
    status: u16,
    body: Vec<u8>,
}

#[async_trait]
impl UpstreamBackend for MockUpstream {
    fn name(&self) -> &str {
        "mock-upstream"
    }

    fn url_origin(&self) -> Option<&str> {
        Some("https://mock-upstream.example")
    }

    fn prepare(&self, req: UpstreamRequest) -> Result<PreparedUpstreamRequest, UpstreamError> {
        let route_id = req.target_route_id.clone();
        let model_id = serde_json::from_slice::<Value>(&req.body)
            .ok()
            .and_then(|value| {
                value
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_default();
        Ok(PreparedUpstreamRequest {
            request: req,
            upstream_name: self.name().to_string(),
            url_origin: self.url_origin().map(str::to_string),
            model_id,
            route_id,
            is_tee: Some(true),
        })
    }

    async fn forward(&self, _req: UpstreamRequest) -> Result<UpstreamResponse, UpstreamError> {
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        Ok(UpstreamResponse {
            status_code: self.status,
            body: self.body.clone(),
            headers,
            served_instance_id: None,
        })
    }
}

struct FailVerifier;

#[async_trait]
impl UpstreamVerifier for FailVerifier {
    async fn verify(&self, request: UpstreamVerificationRequest) -> UpstreamVerifiedEvent {
        let mut event = event_from_request(&request, VerificationResult::Failed);
        event.reason = Some("fixture verification failed".to_string());
        event
    }
}

struct RecordingFailVerifier {
    request: Arc<Mutex<Option<UpstreamVerificationRequest>>>,
}

#[async_trait]
impl UpstreamVerifier for RecordingFailVerifier {
    async fn verify(&self, request: UpstreamVerificationRequest) -> UpstreamVerifiedEvent {
        *self.request.lock().unwrap() = Some(request.clone());
        let mut event = event_from_request(&request, VerificationResult::Failed);
        event.reason = Some("fixture verification failed".to_string());
        event
    }
}

fn temp_config_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "private-ai-gateway-{name}-{}-{}.json",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn runtime_options() -> UpstreamRuntimeOptions {
    UpstreamRuntimeOptions {
        verifier_mode: UpstreamVerifierMode::None,
        accepted_subjects: Vec::new(),
        accepted_image_digests: Vec::new(),
        accepted_dstack_kms_root_public_keys: Vec::new(),
        pccs_url: None,
        verifier_cache_seconds: 300,
        connect_timeout_seconds: 10,
        read_timeout_seconds: 600,
        verifier_request_timeout_seconds: 60,
    }
}

fn upstream_config(
    name: &str,
    base_url: &str,
    public_model: &str,
    upstream_model: &str,
) -> UpstreamConfig {
    UpstreamConfig {
        name: name.to_string(),
        enabled: true,
        provider: UpstreamProvider::OpenAiCompatible,
        base_url: base_url.to_string(),
        path: None,
        models: BTreeMap::from([(public_model.to_string(), upstream_model.to_string())]),
        bearer_token: None,
        basic_auth: false,
        accepted_subjects: None,
        accepted_image_digests: None,
        accepted_dstack_kms_root_public_keys: None,
        pccs_url: None,
        verifier_cache_seconds: None,
        connect_timeout_seconds: None,
        read_timeout_seconds: None,
        verifier_request_timeout_seconds: None,
        verification_refresh_seconds: None,
        session_refresh_seconds: None,
        chutes_e2ee_api_base: None,
        chutes_chute_ids: None,
        chutes_e2ee_discovery_rounds: None,
        chutes_e2ee_discovery_interval_seconds: None,
    }
}

fn upstream_manager(config: Vec<UpstreamConfig>) -> Arc<UpstreamConfigManager> {
    let path = temp_config_path("middleware-upstreams");
    let manager = UpstreamConfigManager::load(&path, runtime_options()).unwrap();
    manager.replace(config).unwrap();
    Arc::new(manager)
}

fn service_from_manager(manager: &Arc<UpstreamConfigManager>) -> Arc<AciService> {
    Arc::new(
        AciService::new_with_upstream_verifier(
            Arc::new(StaticKeyProvider::default()),
            Arc::new(StubQuoter::default()),
            manager.backend(),
            manager.verifier(),
            Arc::new(InMemoryReceiptStore::default()),
            AciServiceConfig::for_test(),
            Arc::new(FixedClock(1_700_000_000)),
        )
        .unwrap(),
    )
}

fn service_from_manager_with_verifier(
    manager: &Arc<UpstreamConfigManager>,
    verifier: Arc<dyn UpstreamVerifier>,
) -> Arc<AciService> {
    Arc::new(
        AciService::new_with_upstream_verifier(
            Arc::new(StaticKeyProvider::default()),
            Arc::new(StubQuoter::default()),
            manager.backend(),
            verifier,
            Arc::new(InMemoryReceiptStore::default()),
            AciServiceConfig::for_test(),
            Arc::new(FixedClock(1_700_000_000)),
        )
        .unwrap(),
    )
}

fn service_failing_verify() -> Arc<AciService> {
    Arc::new(
        AciService::new_with_upstream_verifier(
            Arc::new(StaticKeyProvider::default()),
            Arc::new(StubQuoter::default()),
            Arc::new(MockUpstream {
                status: 200,
                body: br#"{"id":"chat-ok","object":"chat.completion","choices":[]}"#.to_vec(),
            }),
            Arc::new(FailVerifier),
            Arc::new(InMemoryReceiptStore::default()),
            AciServiceConfig::for_test(),
            Arc::new(FixedClock(1_700_000_000)),
        )
        .unwrap(),
    )
}

async fn spawn_openai_upstream(
    id: &'static str,
    status: u16,
    body: Value,
    calls: Arc<CapturedCalls>,
) -> String {
    let response_body = Arc::new(body);
    let app = Router::new().route(
        "/v1/chat/completions",
        post(move |headers: HeaderMap, raw: Bytes| {
            let response_body = response_body.clone();
            let calls = calls.clone();
            async move {
                let parsed = serde_json::from_slice::<Value>(&raw).unwrap_or(Value::Null);
                calls.bodies.lock().unwrap().push(parsed);
                calls
                    .headers
                    .lock()
                    .unwrap()
                    .push(capture_headers(&headers));
                let status = StatusCode::from_u16(status).unwrap();
                let mut body = (*response_body).clone();
                if let Some(obj) = body.as_object_mut() {
                    obj.entry("id".to_string()).or_insert_with(|| json!(id));
                }
                (status, Json(body)).into_response()
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn spawn_openai_streaming_upstream(id: &'static str, calls: Arc<CapturedCalls>) -> String {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(move |headers: HeaderMap, raw: Bytes| {
            let calls = calls.clone();
            async move {
                let parsed = serde_json::from_slice::<Value>(&raw).unwrap_or(Value::Null);
                calls.bodies.lock().unwrap().push(parsed);
                calls
                    .headers
                    .lock()
                    .unwrap()
                    .push(capture_headers(&headers));
                let first = format!(
                    "data: {{\"id\":\"{id}\",\"object\":\"chat.completion.chunk\",\
                     \"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"hi\"}}}}]}}\n\n"
                );
                let chunks = stream::once(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<Bytes, std::io::Error>(Bytes::from(first))
                })
                .chain(stream::once(async {
                    Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data: [DONE]\n\n"))
                }));
                (
                    StatusCode::OK,
                    [(CONTENT_TYPE, "text/event-stream")],
                    Body::from_stream(chunks),
                )
                    .into_response()
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn spawn_pig_upstream(
    metrics: &'static str,
    status: StatusCode,
    body: Value,
    calls: Arc<CapturedCalls>,
) -> String {
    let response_body = Arc::new(body);
    let app = Router::new()
        .route(
            "/v1/metrics",
            axum::routing::get(move || async move { (StatusCode::OK, metrics) }),
        )
        .route(
            "/v1/chat/completions",
            post(move |headers: HeaderMap, raw: Bytes| {
                let calls = calls.clone();
                let response_body = response_body.clone();
                async move {
                    let parsed = serde_json::from_slice::<Value>(&raw).unwrap_or(Value::Null);
                    calls.bodies.lock().unwrap().push(parsed);
                    calls
                        .headers
                        .lock()
                        .unwrap()
                        .push(capture_headers(&headers));
                    (status, Json((*response_body).clone()))
                }
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn spawn_pressured_pig_upstream(calls: Arc<CapturedCalls>) -> String {
    spawn_pig_upstream(
        concat!(
            "pig_dynamic_observed_running 10\n",
            "pig_dynamic_observed_waiting 0\n",
            "pig_dynamic_global_limit 10\n",
            "pig_tier_basic_limit 9\n",
            "pig_tier_inflight{tier=\"basic\"} 9\n",
        ),
        StatusCode::TOO_MANY_REQUESTS,
        json!({
            "error": {
                "message": "PIG says capacity is full",
                "type": "rate_limit_error",
                "code": "pig_capacity_full"
            }
        }),
        calls,
    )
    .await
}

fn capture_headers(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_ascii_lowercase(), value.to_string()))
        })
        .collect()
}

fn middleware(manager: Arc<UpstreamConfigManager>, config: MiddlewareConfig) -> Middleware {
    Middleware::new(&config, manager).unwrap()
}

fn chat_input(model: &str, content: &str) -> CompletionInput {
    let params = json!({
        "model": model,
        "messages": [{ "role": "user", "content": content }]
    });
    CompletionInput {
        endpoint: Endpoint::ChatComplete,
        endpoint_path: "/v1/chat/completions",
        surface: Surface::Openai,
        received_body: serde_json::to_vec(&params).unwrap(),
        params,
        requester: None,
        aci_required: false,
        aci_session_ids: Vec::new(),
        request_id: "req-1".to_string(),
        user_model: Some(model.to_string()),
        user_tier: None,
        stream: false,
    }
}

async fn response_parts(response: axum::response::Response) -> (u16, axum::http::HeaderMap, Value) {
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, headers, body)
}

fn route_running(snapshot: &Value, route_id: &str) -> u64 {
    snapshot["routes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|route| route["route_id"] == json!(route_id))
        .unwrap_or_else(|| panic!("route {route_id} not found in snapshot: {snapshot}"))["running"]
        .as_u64()
        .unwrap()
}

fn route_snapshot<'a>(snapshot: &'a Value, route_id: &str) -> &'a Value {
    snapshot["routes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|route| route["route_id"] == json!(route_id))
        .unwrap_or_else(|| panic!("route {route_id} not found in snapshot: {snapshot}"))
}

fn receipt_event<'a>(payload: &'a Value, event_type: &str) -> &'a Value {
    payload["event_log"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["type"].as_str() == Some(event_type))
        .unwrap_or_else(|| panic!("event {event_type} not found in receipt: {payload}"))
}

#[tokio::test]
async fn tee_only_domain_policy_matches_request_host_exactly() {
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_openai_upstream("up-a", 200, json!({}), calls).await;
    let manager = upstream_manager(vec![upstream_config(
        "gpu-a", &upstream, "gpt-test", "up-a",
    )]);
    let mw = middleware(
        manager,
        MiddlewareConfig {
            tee_only_domains: vec!["gemma4-31b-it.use2.phala.com".to_string()],
            ..Default::default()
        },
    );

    assert!(mw.is_tee_only_domain(Some("gemma4-31b-it.use2.phala.com")));
    assert!(!mw.is_tee_only_domain(Some("api.gemma4-31b-it.use2.phala.com")));
    assert!(!mw.is_tee_only_domain(Some("evil.example")));
    assert!(!mw.is_tee_only_domain(None));
}

#[tokio::test]
async fn catalog_derives_single_public_model() {
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_openai_upstream("up-a", 200, json!({}), calls).await;
    let manager = upstream_manager(vec![
        upstream_config("gpu-a", &upstream, "gpt-test", "up-a"),
        upstream_config("gpu-b", &upstream, "gpt-test", "up-b"),
    ]);
    let mw = middleware(manager, MiddlewareConfig::default());

    let (status, _, body) = response_parts(mw.handle_catalog("/v1/models").await).await;

    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0]["id"], json!("gpt-test"));
}

#[tokio::test]
async fn multiple_public_models_without_static_selection_fail_closed() {
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_openai_upstream("up-a", 200, json!({}), calls).await;
    let manager = upstream_manager(vec![
        upstream_config("gpu-a", &upstream, "gpt-a", "up-a"),
        upstream_config("gpu-b", &upstream, "gpt-b", "up-b"),
    ]);
    let mw = middleware(manager, MiddlewareConfig::default());

    let (status, _, body) = response_parts(mw.handle_catalog("/v1/models").await).await;

    assert_eq!(status, 503);
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("requires exactly one public model"));
}

#[tokio::test]
async fn configured_public_model_filters_catalog_and_requests() {
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_openai_upstream("up-a", 200, json!({}), calls).await;
    let manager = upstream_manager(vec![
        upstream_config("gpu-a", &upstream, "gpt-a", "up-a"),
        upstream_config("gpu-b", &upstream, "gpt-b", "up-b"),
    ]);
    let mw = middleware(
        manager,
        MiddlewareConfig {
            public_model: Some("gpt-b".to_string()),
            ..Default::default()
        },
    );

    let (status, _, body) = response_parts(mw.handle_catalog("/v1/models").await).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0]["id"], json!("gpt-b"));
}

#[tokio::test]
async fn wrong_model_returns_model_not_found_without_forwarding() {
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_openai_upstream("up-a", 200, json!({}), calls.clone()).await;
    let manager = upstream_manager(vec![upstream_config(
        "gpu-a", &upstream, "gpt-test", "up-a",
    )]);
    let service = service_from_manager(&manager);
    let mw = middleware(manager, MiddlewareConfig::default());

    let (status, _, body) = response_parts(
        mw.handle_completion(&service, chat_input("wrong-model", "hello"))
            .await,
    )
    .await;

    assert_eq!(status, 404);
    assert_eq!(body["error"]["type"], json!("model_not_found"));
    assert!(calls.bodies.lock().unwrap().is_empty());
}

#[tokio::test]
async fn configured_model_with_no_enabled_upstreams_returns_rate_limit() {
    let manager = upstream_manager(Vec::new());
    let service = service_from_manager(&manager);
    let mw = middleware(
        manager,
        MiddlewareConfig {
            public_model: Some("gpt-test".to_string()),
            ..Default::default()
        },
    );

    let (status, headers, body) = response_parts(
        mw.handle_completion(&service, chat_input("gpt-test", "hello"))
            .await,
    )
    .await;

    assert_eq!(status, 429);
    assert_eq!(body["error"]["type"], json!("rate_limit_error"));
    assert_eq!(body["error"]["code"], json!("rate_limit_exceeded"));
    assert!(headers.get("retry-after").is_some());
}

#[tokio::test]
async fn unconfigured_model_with_no_upstreams_returns_rate_limit() {
    let manager = upstream_manager(Vec::new());
    let service = service_from_manager(&manager);
    let mw = middleware(manager, MiddlewareConfig::default());

    let (status, headers, body) = response_parts(
        mw.handle_completion(&service, chat_input("gpt-test", "hello"))
            .await,
    )
    .await;

    assert_eq!(status, 429);
    assert_eq!(body["error"]["type"], json!("rate_limit_error"));
    assert_eq!(body["error"]["code"], json!("rate_limit_exceeded"));
    assert!(headers.get("retry-after").is_some());
}

#[tokio::test]
async fn disabled_only_upstreams_return_rate_limit_without_forwarding() {
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_openai_upstream("up-a", 200, json!({}), calls.clone()).await;
    let mut disabled = upstream_config("gpu-a", &upstream, "gpt-test", "up-a");
    disabled.enabled = false;
    let manager = upstream_manager(vec![disabled]);
    let service = service_from_manager(&manager);
    let mw = middleware(manager, MiddlewareConfig::default());

    let (status, headers, body) = response_parts(
        mw.handle_completion(&service, chat_input("gpt-test", "hello"))
            .await,
    )
    .await;

    assert_eq!(status, 429);
    assert_eq!(body["error"]["type"], json!("rate_limit_error"));
    assert_eq!(body["error"]["code"], json!("rate_limit_exceeded"));
    assert!(headers.get("retry-after").is_some());
    assert!(calls.bodies.lock().unwrap().is_empty());
}

#[tokio::test]
async fn forwarding_uses_selected_route_and_finalizes_receipt() {
    let calls_a = Arc::new(CapturedCalls::default());
    let calls_b = Arc::new(CapturedCalls::default());
    let upstream_a = spawn_openai_upstream(
        "chat-a",
        200,
        json!({"object":"chat.completion","model":"up-a","choices":[]}),
        calls_a.clone(),
    )
    .await;
    let upstream_b = spawn_openai_upstream(
        "chat-b",
        200,
        json!({"object":"chat.completion","model":"up-b","choices":[]}),
        calls_b.clone(),
    )
    .await;
    let manager = upstream_manager(vec![
        upstream_config("gpu-a", &upstream_a, "gpt-test", "up-a"),
        upstream_config("gpu-b", &upstream_b, "gpt-test", "up-b"),
    ]);
    let service = service_from_manager(&manager);
    let mw = middleware(manager, MiddlewareConfig::default());
    let raw_body = br#"{
  "messages": [
    { "content": "stable prefix one", "role": "user" }
  ],
  "metadata": { "z": 1, "a": ["kept", "ordered"] },
  "stream_options": { "continuous_usage_stats": true, "include_usage": true },
  "model": "gpt-test"
}"#
    .to_vec();
    let input = CompletionInput {
        endpoint: Endpoint::ChatComplete,
        endpoint_path: "/v1/chat/completions",
        surface: Surface::Openai,
        params: serde_json::from_slice(&raw_body).unwrap(),
        received_body: raw_body.clone(),
        requester: None,
        aci_required: false,
        aci_session_ids: Vec::new(),
        request_id: "req-raw-body".to_string(),
        user_model: Some("gpt-test".to_string()),
        user_tier: None,
        stream: false,
    };

    let (status, headers, body) = response_parts(mw.handle_completion(&service, input).await).await;

    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("req-raw-body"));
    assert_eq!(body["model"], json!("gpt-test"));
    assert!(headers.get("x-receipt-id").is_some());
    assert!(headers.get("x-e2ee-applied").is_none());
    assert!(headers.get("x-e2ee-version").is_none());
    assert!(headers.get("x-e2ee-algo").is_none());
    assert_eq!(calls_a.bodies.lock().unwrap().len(), 1);
    assert_eq!(calls_b.bodies.lock().unwrap().len(), 0);
    assert_eq!(
        calls_a.bodies.lock().unwrap()[0]["model"],
        json!("gpt-test"),
        "selected route must forward the request model unchanged"
    );

    let receipt_id = headers
        .get("x-receipt-id")
        .expect("receipt id header")
        .to_str()
        .expect("receipt id is ascii");
    let receipt = service
        .get_receipt_by_receipt_id(receipt_id)
        .expect("receipt is stored");
    let payload = receipt.document_json().expect("receipt json parses");
    assert_eq!(
        receipt_event(&payload, EVENT_ROUTE_SELECTED)["target_route_id"],
        json!("gpu-a:gpt-test")
    );
    let received_hash = receipt_event(&payload, EVENT_REQUEST_RECEIVED)["body_hash"].clone();
    assert_eq!(received_hash, json!(sha256_hex(&raw_body)));
    assert_eq!(
        receipt_event(&payload, EVENT_MIDDLEWARE_FORWARDED)["body_hash"],
        received_hash,
        "middleware-selected route must not rewrite body before candidate forwarding"
    );
    assert_eq!(
        receipt_event(&payload, EVENT_REQUEST_FORWARDED)["body_hash"],
        received_hash,
        "middleware-selected route must not rewrite body in backend prepare"
    );
}

#[tokio::test]
async fn disabled_upstream_is_visible_but_not_routed() {
    let calls_disabled = Arc::new(CapturedCalls::default());
    let calls_active = Arc::new(CapturedCalls::default());
    let disabled_url = spawn_openai_upstream(
        "chat-disabled",
        200,
        json!({"object":"chat.completion","model":"up-disabled","choices":[]}),
        calls_disabled.clone(),
    )
    .await;
    let active_url = spawn_openai_upstream(
        "chat-active",
        200,
        json!({"object":"chat.completion","model":"up-active","choices":[]}),
        calls_active.clone(),
    )
    .await;
    let mut disabled = upstream_config("gpu-a", &disabled_url, "gpt-test", "up-disabled");
    disabled.enabled = false;
    let manager = upstream_manager(vec![
        disabled,
        upstream_config("gpu-b", &active_url, "gpt-test", "up-active"),
    ]);
    let service = service_from_manager(&manager);
    let mw = middleware(manager, MiddlewareConfig::default());

    let (status, _, body) = response_parts(
        mw.handle_completion(&service, chat_input("gpt-test", "hello"))
            .await,
    )
    .await;

    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("req-1"));
    assert_eq!(body["model"], json!("gpt-test"));
    assert_eq!(calls_disabled.bodies.lock().unwrap().len(), 0);
    assert_eq!(calls_active.bodies.lock().unwrap().len(), 1);

    let snapshot = mw.admin_snapshot().unwrap();
    let disabled_route = snapshot["routes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|route| route["route_id"] == json!("gpu-a:gpt-test"))
        .unwrap();
    assert_eq!(disabled_route["enabled"], json!(false));
}

#[tokio::test]
async fn pressured_pig_route_is_forwarded_instead_of_router_prerejected() {
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_pressured_pig_upstream(calls.clone()).await;
    let manager = upstream_manager(vec![upstream_config(
        "gpu-a", &upstream, "gpt-test", "up-a",
    )]);
    let service = service_from_manager(&manager);
    let mw = middleware(
        manager,
        MiddlewareConfig {
            metrics_poll_ms: 10,
            metrics_stale_ms: 10_000,
            ..Default::default()
        },
    );
    tokio::time::sleep(Duration::from_millis(80)).await;

    let (status, _, _body) = response_parts(
        mw.handle_completion(&service, chat_input("gpt-test", "hello"))
            .await,
    )
    .await;

    assert_eq!(status, 429);
    assert_eq!(calls.bodies.lock().unwrap().len(), 1);

    let snapshot = mw.admin_snapshot().unwrap();
    let route = route_snapshot(&snapshot, "gpu-a:gpt-test");
    assert_eq!(route["selectable"], json!(false));
    assert_eq!(route["pressure_passthrough"], json!(1));
}

#[tokio::test]
async fn pressured_pig_passthrough_failovers_after_first_429() {
    let calls_a = Arc::new(CapturedCalls::default());
    let calls_b = Arc::new(CapturedCalls::default());
    let calls_c = Arc::new(CapturedCalls::default());
    let upstream_a = spawn_pig_upstream(
        concat!(
            "pig_dynamic_observed_running 1\n",
            "pig_dynamic_observed_waiting 1\n",
            "pig_dynamic_global_limit 10\n",
            "pig_tier_basic_limit 9\n",
            "pig_tier_inflight{tier=\"basic\"} 1\n",
        ),
        StatusCode::OK,
        json!({"object":"chat.completion","model":"up-a","choices":[]}),
        calls_a.clone(),
    )
    .await;
    let upstream_b = spawn_pig_upstream(
        concat!(
            "pig_dynamic_observed_running 11\n",
            "pig_dynamic_observed_waiting 0\n",
            "pig_dynamic_global_limit 10\n",
            "pig_tier_basic_limit 9\n",
            "pig_tier_inflight{tier=\"basic\"} 9\n",
        ),
        StatusCode::TOO_MANY_REQUESTS,
        json!({"error":{"type":"rate_limit_error","code":"pig_b_full"}}),
        calls_b.clone(),
    )
    .await;
    let upstream_c = spawn_pig_upstream(
        concat!(
            "pig_dynamic_observed_running 10\n",
            "pig_dynamic_observed_waiting 0\n",
            "pig_dynamic_global_limit 10\n",
            "pig_tier_basic_limit 9\n",
            "pig_tier_inflight{tier=\"basic\"} 9\n",
        ),
        StatusCode::TOO_MANY_REQUESTS,
        json!({"error":{"type":"rate_limit_error","code":"pig_c_full"}}),
        calls_c.clone(),
    )
    .await;
    let manager = upstream_manager(vec![
        upstream_config("gpu-a", &upstream_a, "gpt-test", "up-a"),
        upstream_config("gpu-b", &upstream_b, "gpt-test", "up-b"),
        upstream_config("gpu-c", &upstream_c, "gpt-test", "up-c"),
    ]);
    let service = service_from_manager(&manager);
    let mw = middleware(
        manager,
        MiddlewareConfig {
            metrics_poll_ms: 10,
            metrics_stale_ms: 10_000,
            ..Default::default()
        },
    );
    tokio::time::sleep(Duration::from_millis(80)).await;

    let (status, headers, body) = response_parts(
        mw.handle_completion(&service, chat_input("gpt-test", "hello"))
            .await,
    )
    .await;

    assert_eq!(status, 200);
    assert_eq!(body["model"], json!("gpt-test"));
    assert_eq!(calls_c.bodies.lock().unwrap().len(), 1);
    assert_eq!(calls_b.bodies.lock().unwrap().len(), 1);
    assert_eq!(calls_a.bodies.lock().unwrap().len(), 1);

    let snapshot = mw.admin_snapshot().unwrap();
    assert_eq!(
        route_snapshot(&snapshot, "gpu-c:gpt-test")["pressure_passthrough"],
        json!(1)
    );

    let receipt_id = headers
        .get("x-receipt-id")
        .expect("receipt id header")
        .to_str()
        .expect("receipt id is ascii");
    let receipt = service
        .get_receipt_by_receipt_id(receipt_id)
        .expect("receipt is stored");
    let payload = receipt.document_json().expect("receipt json parses");
    assert_eq!(
        receipt_event(&payload, EVENT_ROUTE_SELECTED)["target_route_id"],
        json!("gpu-a:gpt-test")
    );
}

#[tokio::test]
async fn pressured_pig_passthrough_tries_only_first_three_candidates() {
    let calls_a = Arc::new(CapturedCalls::default());
    let calls_b = Arc::new(CapturedCalls::default());
    let calls_c = Arc::new(CapturedCalls::default());
    let calls_d = Arc::new(CapturedCalls::default());
    let full_metrics = concat!(
        "pig_dynamic_observed_running 10\n",
        "pig_dynamic_observed_waiting 0\n",
        "pig_dynamic_global_limit 10\n",
        "pig_tier_basic_limit 9\n",
        "pig_tier_inflight{tier=\"basic\"} 9\n",
    );
    let upstream_a = spawn_pig_upstream(
        full_metrics,
        StatusCode::TOO_MANY_REQUESTS,
        json!({"error":{"type":"rate_limit_error","code":"pig_a_full"}}),
        calls_a.clone(),
    )
    .await;
    let upstream_b = spawn_pig_upstream(
        full_metrics,
        StatusCode::TOO_MANY_REQUESTS,
        json!({"error":{"type":"rate_limit_error","code":"pig_b_full"}}),
        calls_b.clone(),
    )
    .await;
    let upstream_c = spawn_pig_upstream(
        full_metrics,
        StatusCode::TOO_MANY_REQUESTS,
        json!({"error":{"type":"rate_limit_error","code":"pig_c_full"}}),
        calls_c.clone(),
    )
    .await;
    let upstream_d = spawn_pig_upstream(
        full_metrics,
        StatusCode::OK,
        json!({"object":"chat.completion","model":"up-d","choices":[]}),
        calls_d.clone(),
    )
    .await;
    let manager = upstream_manager(vec![
        upstream_config("gpu-a", &upstream_a, "gpt-test", "up-a"),
        upstream_config("gpu-b", &upstream_b, "gpt-test", "up-b"),
        upstream_config("gpu-c", &upstream_c, "gpt-test", "up-c"),
        upstream_config("gpu-d", &upstream_d, "gpt-test", "up-d"),
    ]);
    let service = service_from_manager(&manager);
    let mw = middleware(
        manager,
        MiddlewareConfig {
            metrics_poll_ms: 10,
            metrics_stale_ms: 10_000,
            ..Default::default()
        },
    );
    tokio::time::sleep(Duration::from_millis(80)).await;

    let (status, _headers, body) = response_parts(
        mw.handle_completion(&service, chat_input("gpt-test", "hello"))
            .await,
    )
    .await;

    assert_eq!(status, 429);
    assert_eq!(body["error"]["type"], json!("rate_limit_error"));
    assert_eq!(calls_a.bodies.lock().unwrap().len(), 1);
    assert_eq!(calls_b.bodies.lock().unwrap().len(), 1);
    assert_eq!(calls_c.bodies.lock().unwrap().len(), 1);
    assert_eq!(calls_d.bodies.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn untrusted_user_tier_header_is_not_forwarded_by_default() {
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_openai_upstream(
        "chat-a",
        200,
        json!({"object":"chat.completion","model":"up-a","choices":[]}),
        calls.clone(),
    )
    .await;
    let manager = upstream_manager(vec![upstream_config(
        "gpu-a", &upstream, "gpt-test", "up-a",
    )]);
    let service = service_from_manager(&manager);
    let mw = middleware(manager, MiddlewareConfig::default());
    let mut input = chat_input("gpt-test", "hello");
    input.user_tier = Some("premium".to_string());

    let (status, _, _) = response_parts(mw.handle_completion(&service, input).await).await;

    assert_eq!(status, 200);
    let headers = calls.headers.lock().unwrap();
    assert_eq!(headers.len(), 1);
    assert!(
        !headers[0].contains_key("x-user-tier"),
        "untrusted public x-user-tier must not reach PIG"
    );
}

#[tokio::test]
async fn trusted_user_tier_header_is_forwarded_when_enabled() {
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_openai_upstream(
        "chat-a",
        200,
        json!({"object":"chat.completion","model":"up-a","choices":[]}),
        calls.clone(),
    )
    .await;
    let manager = upstream_manager(vec![upstream_config(
        "gpu-a", &upstream, "gpt-test", "up-a",
    )]);
    let service = service_from_manager(&manager);
    let mw = middleware(
        manager,
        MiddlewareConfig {
            trusted_user_tier_header: true,
            ..Default::default()
        },
    );
    let mut input = chat_input("gpt-test", "hello");
    input.user_tier = Some("premium".to_string());

    let (status, _, _) = response_parts(mw.handle_completion(&service, input).await).await;

    assert_eq!(status, 200);
    let headers = calls.headers.lock().unwrap();
    assert_eq!(headers.len(), 1);
    assert_eq!(
        headers[0].get("x-user-tier").map(String::as_str),
        Some("premium")
    );
}

#[tokio::test]
async fn cache_aware_selection_keeps_similar_prefix_on_same_route() {
    let calls_a = Arc::new(CapturedCalls::default());
    let calls_b = Arc::new(CapturedCalls::default());
    let upstream_a = spawn_openai_upstream(
        "chat-a",
        200,
        json!({"object":"chat.completion","model":"up-a","choices":[]}),
        calls_a.clone(),
    )
    .await;
    let upstream_b = spawn_openai_upstream(
        "chat-b",
        200,
        json!({"object":"chat.completion","model":"up-b","choices":[]}),
        calls_b.clone(),
    )
    .await;
    let manager = upstream_manager(vec![
        upstream_config("gpu-a", &upstream_a, "gpt-test", "up-a"),
        upstream_config("gpu-b", &upstream_b, "gpt-test", "up-b"),
    ]);
    let service = service_from_manager(&manager);
    let mw = middleware(
        manager,
        MiddlewareConfig {
            cache_threshold: 0.25,
            ..Default::default()
        },
    );

    let (first_status, _, first_body) = response_parts(
        mw.handle_completion(&service, chat_input("gpt-test", "shared prefix aaa"))
            .await,
    )
    .await;
    let (second_status, _, second_body) = response_parts(
        mw.handle_completion(&service, chat_input("gpt-test", "shared prefix bbb"))
            .await,
    )
    .await;

    assert_eq!(first_status, 200);
    assert_eq!(second_status, 200);
    assert_eq!(first_body["id"], json!("req-1"));
    assert_eq!(second_body["id"], json!("req-1"));
    assert_eq!(calls_a.bodies.lock().unwrap().len(), 2);
    assert_eq!(calls_b.bodies.lock().unwrap().len(), 0);

    let snapshot = mw.admin_snapshot().unwrap();
    assert_eq!(snapshot["cache_index"]["type"], json!("radix_tree"));
    assert_eq!(snapshot["cache_index"]["models"], json!(1));
    assert_eq!(snapshot["cache_index"]["records"], json!(2));
    assert_eq!(
        route_snapshot(&snapshot, "gpu-a:gpt-test")["cache_records"],
        json!(2)
    );
    assert_eq!(
        route_snapshot(&snapshot, "gpu-a:gpt-test")["selected_by_cache"],
        json!(1)
    );
}

#[tokio::test]
async fn disabled_upstream_cache_is_removed_before_selection() {
    let calls_a = Arc::new(CapturedCalls::default());
    let calls_b = Arc::new(CapturedCalls::default());
    let upstream_a = spawn_openai_upstream(
        "chat-a",
        200,
        json!({"object":"chat.completion","model":"up-a","choices":[]}),
        calls_a.clone(),
    )
    .await;
    let upstream_b = spawn_openai_upstream(
        "chat-b",
        200,
        json!({"object":"chat.completion","model":"up-b","choices":[]}),
        calls_b.clone(),
    )
    .await;
    let manager = upstream_manager(vec![
        upstream_config("gpu-a", &upstream_a, "gpt-test", "up-a"),
        upstream_config("gpu-b", &upstream_b, "gpt-test", "up-b"),
    ]);
    let service = service_from_manager(&manager);
    let mw = middleware(
        manager.clone(),
        MiddlewareConfig {
            cache_threshold: 0.25,
            ..Default::default()
        },
    );

    let (first_status, _, first_body) = response_parts(
        mw.handle_completion(&service, chat_input("gpt-test", "alpha stable prefix one"))
            .await,
    )
    .await;
    assert_eq!(first_status, 200);
    assert_eq!(first_body["id"], json!("req-1"));

    manager.set_enabled("gpu-a", false).unwrap();

    let (second_status, _, second_body) = response_parts(
        mw.handle_completion(&service, chat_input("gpt-test", "alpha stable prefix two"))
            .await,
    )
    .await;

    assert_eq!(second_status, 200);
    assert_eq!(second_body["id"], json!("req-1"));
    assert_eq!(calls_a.bodies.lock().unwrap().len(), 1);
    assert_eq!(calls_b.bodies.lock().unwrap().len(), 1);

    let snapshot = mw.admin_snapshot().unwrap();
    assert_eq!(
        route_snapshot(&snapshot, "gpu-a:gpt-test")["enabled"],
        json!(false)
    );
    assert_eq!(
        route_snapshot(&snapshot, "gpu-a:gpt-test")["cache_records"],
        json!(0)
    );
    assert_eq!(snapshot["cache_index"]["removed_routes_total"], json!(1));
}

#[tokio::test]
async fn streaming_running_count_stays_until_body_is_consumed() {
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_openai_streaming_upstream("chat-stream", calls.clone()).await;
    let manager = upstream_manager(vec![upstream_config(
        "gpu-a", &upstream, "gpt-test", "up-a",
    )]);
    let service = service_from_manager(&manager);
    let mw = middleware(manager, MiddlewareConfig::default());
    let raw_body = br#"{
  "stream": true,
  "messages": [
    { "role": "user", "content": "streaming request" }
  ],
  "stream_options": { "include_usage": true, "continuous_usage_stats": true },
  "model": "gpt-test"
}"#
    .to_vec();
    let input = CompletionInput {
        endpoint: Endpoint::ChatComplete,
        endpoint_path: "/v1/chat/completions",
        surface: Surface::Openai,
        params: serde_json::from_slice(&raw_body).unwrap(),
        received_body: raw_body.clone(),
        requester: None,
        aci_required: false,
        aci_session_ids: Vec::new(),
        request_id: "req-stream-raw-body".to_string(),
        user_model: Some("gpt-test".to_string()),
        user_tier: None,
        stream: true,
    };

    let response = mw.handle_completion(&service, input).await;

    assert_eq!(response.status(), StatusCode::OK);
    let receipt_id = response
        .headers()
        .get("x-receipt-id")
        .expect("streaming receipt id header")
        .to_str()
        .expect("streaming receipt id is ascii")
        .to_string();
    assert_eq!(
        route_running(&mw.admin_snapshot().unwrap(), "gpu-a:gpt-test"),
        1
    );

    let _ = to_bytes(response.into_body(), usize::MAX).await.unwrap();

    assert_eq!(
        route_running(&mw.admin_snapshot().unwrap(), "gpu-a:gpt-test"),
        0
    );
    assert_eq!(calls.bodies.lock().unwrap()[0]["model"], json!("gpt-test"));

    let receipt = service
        .get_receipt_by_receipt_id(&receipt_id)
        .expect("streaming receipt is stored after body is consumed");
    let payload = receipt.document_json().expect("streaming receipt parses");
    assert_eq!(
        receipt_event(&payload, EVENT_ROUTE_SELECTED)["target_route_id"],
        json!("gpu-a:gpt-test")
    );
    let received_hash = receipt_event(&payload, EVENT_REQUEST_RECEIVED)["body_hash"].clone();
    assert_eq!(received_hash, json!(sha256_hex(&raw_body)));
    assert_eq!(
        receipt_event(&payload, EVENT_MIDDLEWARE_FORWARDED)["body_hash"],
        received_hash,
        "streaming middleware candidate must preserve request body bytes"
    );
    assert_eq!(
        receipt_event(&payload, EVENT_REQUEST_FORWARDED)["body_hash"],
        received_hash,
        "streaming backend prepare must preserve request body bytes"
    );
}

#[tokio::test]
async fn upstream_verification_failure_fails_closed_before_forwarding() {
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_openai_upstream("up-a", 200, json!({}), calls.clone()).await;
    let manager = upstream_manager(vec![upstream_config(
        "gpu-a", &upstream, "gpt-test", "up-a",
    )]);
    let mw = middleware(manager, MiddlewareConfig::default());
    let mut input = chat_input("gpt-test", "hello");
    input.aci_required = true;

    let (status, _, body) =
        response_parts(mw.handle_completion(&service_failing_verify(), input).await).await;

    assert_eq!(status, 503);
    assert_eq!(body["error"]["type"], json!("service_unavailable"));
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("fixture verification failed"));
    assert!(calls.bodies.lock().unwrap().is_empty());
}

#[tokio::test]
async fn upstream_verification_uses_passthrough_body_hash() {
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_openai_upstream("up-a", 200, json!({}), calls.clone()).await;
    let mut cfg = upstream_config("gpu-a", &upstream, "gpt-test", "up-a");
    cfg.provider = UpstreamProvider::PhalaDirect;
    let manager = upstream_manager(vec![cfg]);
    let captured = Arc::new(Mutex::new(None));
    let service = service_from_manager_with_verifier(
        &manager,
        Arc::new(RecordingFailVerifier {
            request: captured.clone(),
        }),
    );
    let mw = middleware(manager, MiddlewareConfig::default());
    let raw_body = br#"{
  "messages": [
    { "content": "verification hash", "role": "user" }
  ],
  "provider": { "aci_verified": true },
  "stream_options": { "include_usage": true, "continuous_usage_stats": true },
  "model": "gpt-test"
}"#
    .to_vec();
    let input = CompletionInput {
        endpoint: Endpoint::ChatComplete,
        endpoint_path: "/v1/chat/completions",
        surface: Surface::Openai,
        params: serde_json::from_slice(&raw_body).unwrap(),
        received_body: raw_body.clone(),
        requester: None,
        aci_required: true,
        aci_session_ids: Vec::new(),
        request_id: "req-verify-hash".to_string(),
        user_model: Some("gpt-test".to_string()),
        user_tier: None,
        stream: false,
    };

    let (status, _, _) = response_parts(mw.handle_completion(&service, input).await).await;

    assert_eq!(status, 503);
    assert!(calls.bodies.lock().unwrap().is_empty());
    let request = captured
        .lock()
        .unwrap()
        .clone()
        .expect("verification request should be recorded before fail-closed response");
    assert_eq!(request.upstream_name, "gpu-a");
    assert_eq!(request.model_id, "up-a");
    assert_eq!(request.forwarded_body_hash, sha256_hex(&raw_body));
    assert!(request.required);
}

#[tokio::test]
async fn image_fetch_5xx_becomes_client_400() {
    let url = "https://halleonard.example/wl/02116757-wl.jpg";
    let calls = Arc::new(CapturedCalls::default());
    let upstream = spawn_openai_upstream(
        "bad-image",
        500,
        json!({
            "error": {
                "message": format!("403, message='Forbidden', url='{url}'"),
                "type": "InternalServerError",
                "code": 500
            }
        }),
        calls,
    )
    .await;
    let manager = upstream_manager(vec![upstream_config(
        "gpu-a", &upstream, "gpt-test", "up-a",
    )]);
    let service = service_from_manager(&manager);
    let mw = middleware(manager, MiddlewareConfig::default());
    let mut input = chat_input("gpt-test", "describe");
    input.params = json!({
        "model": "gpt-test",
        "messages": [{
            "role": "user",
            "content": [
                { "type": "text", "text": "describe" },
                { "type": "image_url", "image_url": { "url": url } }
            ]
        }]
    });
    input.received_body = serde_json::to_vec(&input.params).unwrap();

    let (status, _, body) = response_parts(mw.handle_completion(&service, input).await).await;

    assert_eq!(status, 400);
    assert_eq!(body["error"]["type"], json!("invalid_request_error"));
    assert!(body["error"]["message"].as_str().unwrap().contains(url));
}
