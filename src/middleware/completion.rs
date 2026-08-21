//! Completion forwarding for the single-model router middleware.
//!
//! The router chooses ordered candidates. `AciService` still validates the
//! route, verifies the upstream, enforces channel binding, forwards the request,
//! and finalizes receipts.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::{
    body::Body,
    http::{header::CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{Stream, StreamExt};
use serde_json::Value;

use crate::aci::upstream::UpstreamError;
use crate::aggregator::service::{
    AciService, ChatCompletionRequest, ForwardCandidate, GatewayRequestContext,
    MiddlewareAttemptObserver, MiddlewareForwardResult, MiddlewareReceiptJournal, ReceiptOwner,
    ServiceError, ServiceResponseStream,
};

use super::control::ControlClient;
use super::errors::{self, Surface};
use super::router::RouteInFlight;
use super::sse::{KeepAliveStream, MeterStream, StreamReport};
use super::stream_transform::SseTransformStream;
use super::types::{Endpoint, ProviderFormat, RouteCandidate};
use super::{response_transform, stream_transform};

/// Everything the completion path needs, computed by the HTTP handler after
/// request validation. `params` is a read-only routing view; `received_body` is
/// what the middleware-selected forwarding path sends upstream.
pub struct CompletionInput {
    pub endpoint: Endpoint,
    pub endpoint_path: &'static str,
    pub surface: Surface,
    /// Parsed request body used only for routing decisions.
    pub params: Value,
    /// Exact cleartext bytes the service observed (recorded into the receipt).
    pub received_body: Vec<u8>,
    pub requester: Option<ReceiptOwner>,
    pub aci_required: bool,
    pub aci_session_ids: Vec<String>,
    pub request_id: String,
    pub user_model: Option<String>,
    pub user_tier: Option<String>,
    pub stream: bool,
}

pub(super) fn rate_limited_by_router(
    service: &AciService,
    input: &CompletionInput,
    message: &str,
) -> Response {
    let reset_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64 + 1)
        .unwrap_or(1);
    let body =
        errors::rate_limit_envelope_bytes(input.surface, message, Some(input.request_id.as_str()));
    let headers = errors::rate_limit_headers(0, reset_at);
    finalize_generated(
        input.surface,
        service,
        input.endpoint_path,
        429,
        body,
        &headers,
    )
}

const MAX_LOG_DETAIL_CHARS: usize = 240;

#[derive(Clone, Copy)]
struct OutcomeCtx<'a> {
    request_id: &'a str,
    model: &'a str,
    started: Instant,
}

pub(super) fn should_log_failure(status: u16) -> bool {
    status != 429
}

const STANDARD_FINISH_REASONS: &[&str] = &[
    "stop",
    "length",
    "tool_calls",
    "function_call",
    "content_filter",
    "end_turn",
    "max_tokens",
    "stop_sequence",
    "tool_use",
    "pause_turn",
    "refusal",
    "model_context_window_exceeded",
];

pub(super) fn finish_reasons_anomalous<'a, I: IntoIterator<Item = &'a str>>(reasons: I) -> bool {
    reasons
        .into_iter()
        .any(|reason| !STANDARD_FINISH_REASONS.contains(&reason))
}

fn sanitize_log_value(value: &str, max_chars: usize) -> String {
    value
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(max_chars)
        .collect()
}

pub(super) fn sanitize_identifier(value: &str) -> String {
    sanitize_log_value(value, 128)
}

pub(super) fn sanitize_reason(reason: &str) -> String {
    sanitize_log_value(reason, 32)
}

fn detail_snippet_bytes(raw: &[u8]) -> String {
    let capped = &raw[..raw.len().min(4 * MAX_LOG_DETAIL_CHARS)];
    String::from_utf8_lossy(capped)
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(MAX_LOG_DETAIL_CHARS)
        .collect()
}

fn detail_snippet_text(raw: &str) -> String {
    sanitize_log_value(raw, MAX_LOG_DETAIL_CHARS)
}

pub(super) fn detail_snippet(raw: &[u8]) -> String {
    detail_snippet_bytes(raw)
}

pub(super) fn debug_gated_detail(detail: &str) -> &str {
    if tracing::enabled!(target: "request_outcome", tracing::Level::DEBUG) {
        detail
    } else {
        ""
    }
}

fn log_generated_outcome(
    ctx: OutcomeCtx<'_>,
    phase: &'static str,
    status: u16,
    upstream_status: u16,
    route: &str,
    attempt: u32,
    detail: &str,
) {
    if !should_log_failure(status) {
        return;
    }
    tracing::info!(
        target: "request_outcome",
        request_id = %ctx.request_id,
        model = %sanitize_log_value(ctx.model, 128),
        route = %sanitize_log_value(route, 128),
        attempt,
        status,
        upstream_status,
        phase,
        duration_ms = ctx.started.elapsed().as_millis() as u64,
        detail = %debug_gated_detail(detail),
        "request outcome"
    );
}

fn log_failed_attempts(ctx: OutcomeCtx<'_>, attempts: &[(String, u16)], is_streaming: bool) {
    for (index, (route, status)) in attempts.iter().enumerate() {
        if !should_log_failure(*status) {
            continue;
        }
        tracing::info!(
            target: "request_outcome",
            request_id = %ctx.request_id,
            model = %sanitize_log_value(ctx.model, 128),
            route = %sanitize_log_value(route, 128),
            attempt = index as u32,
            status = *status,
            upstream_status = *status,
            phase = "attempt_failed",
            is_streaming,
            duration_ms = ctx.started.elapsed().as_millis() as u64,
            "middleware candidate failed"
        );
    }
}

fn passthrough_forward_candidates(
    received_body: &[u8],
    candidates: &[RouteCandidate],
) -> Vec<ForwardCandidate> {
    candidates
        .iter()
        .map(|candidate| ForwardCandidate {
            route_id: candidate.route_id.clone(),
            body: received_body.to_vec(),
        })
        .collect()
}

fn response_usage(value: &Value) -> Option<&Value> {
    value
        .get("usage")
        .filter(|usage| !usage.is_null())
        .or_else(|| {
            value
                .get("response")
                .and_then(|response| response.get("usage"))
                .filter(|usage| !usage.is_null())
        })
}

fn cache_skip_reason_for_status(status: u16) -> &'static str {
    match status {
        429 => "upstream_429",
        500..=599 => "upstream_5xx",
        _ => "upstream_non_2xx",
    }
}

fn cache_skip_reason_for_error(err: &ServiceError) -> &'static str {
    match err {
        ServiceError::Upstream(UpstreamError::Upstream { status, .. }) => {
            cache_skip_reason_for_status(*status)
        }
        ServiceError::Upstream(UpstreamError::Transport(_)) => "transport_error",
        ServiceError::UpstreamVerification(_) => "verification_error",
        _ => "gateway_error",
    }
}

pub(super) async fn run(
    service: &AciService,
    sse_keepalive_ms: Option<u64>,
    control: Option<ControlClient>,
    pricing: Option<Value>,
    input: CompletionInput,
    candidates: Vec<RouteCandidate>,
    mut route_in_flight: Option<RouteInFlight>,
) -> Response {
    let started = Instant::now();
    let CompletionInput {
        endpoint,
        endpoint_path,
        surface,
        params,
        received_body,
        requester,
        aci_required,
        aci_session_ids,
        request_id,
        user_model,
        user_tier,
        stream,
    } = input;

    let model = params.get("model").and_then(Value::as_str);
    let outcome_ctx = OutcomeCtx {
        request_id: &request_id,
        model: model.unwrap_or(""),
        started,
    };
    let identity = Arc::new(response_transform::ResponseIdentity {
        request_id: request_id.clone(),
        user_model: user_model.clone(),
    });
    if candidates.is_empty() {
        // Not found, not malformed: model_not_found is an OpenAI-compatible
        // 404. Capacity exhaustion is handled earlier by the router wrapper.
        let message = format!("no route available for model {}", model.unwrap_or("(none)"));
        log_generated_outcome(outcome_ctx, "routing", 404, 0, "", 0, &message);
        let body = errors::envelope_bytes(surface, "model_not_found", &message, Some(&request_id));
        return finalize_generated(surface, service, endpoint_path, 404, body, &[]);
    }

    let forward_candidates = passthrough_forward_candidates(&received_body, &candidates);

    let context = GatewayRequestContext {
        request_id: request_id.clone(),
        user_model,
        target_route_id: None,
        user_tier,
    };

    let journal = MiddlewareReceiptJournal::default();
    let result = service
        .forward_chat_completion_for_middleware_observed(
            ChatCompletionRequest {
                context,
                endpoint_path,
                received_body: &received_body,
                forwarded_body: None,
                aci_required,
                aci_session_ids,
                upstream_verification_event: None,
                requester: requester.clone(),
                e2ee: None,
            },
            forward_candidates,
            stream,
            journal.clone(),
            route_in_flight
                .as_mut()
                .map(|observer| observer as &mut dyn MiddlewareAttemptObserver),
        )
        .await;

    match result {
        Ok(MiddlewareForwardResult::Forwarded(forward)) => {
            if let Some(in_flight) = route_in_flight.as_mut() {
                in_flight.retarget(&forward.selected_route);
            }
            let upstream_status = forward.upstream_status;
            let selected_format = candidates
                .iter()
                .find(|c| c.route_id == forward.selected_route)
                .or_else(|| candidates.first())
                .map(|c| c.format)
                .unwrap_or(ProviderFormat::Openai);

            let (client_status, final_body) = if (200..300).contains(&upstream_status) {
                let upstream_json: Value = match serde_json::from_slice(&forward.upstream_body) {
                    Ok(value) => value,
                    Err(_) => {
                        if let Some(in_flight) = route_in_flight.as_mut() {
                            in_flight.skip_cache("malformed_success");
                        }
                        let message = "upstream returned a malformed success body";
                        log_generated_outcome(
                            outcome_ctx,
                            "buffered_transform",
                            502,
                            upstream_status,
                            &forward.selected_route,
                            forward.failed_attempts.len() as u32,
                            message,
                        );
                        let body = errors::envelope_bytes(
                            surface,
                            errors::error_type(surface, 502),
                            message,
                            Some(&request_id),
                        );
                        return finalize_generated(surface, service, endpoint_path, 502, body, &[]);
                    }
                };
                if let Some(in_flight) = route_in_flight.as_mut() {
                    in_flight.commit_cache(response_usage(&upstream_json));
                }
                let mut transformed = response_transform::transform_response(
                    selected_format,
                    endpoint,
                    upstream_json,
                );
                response_transform::rewrite_identity(&mut transformed, &identity);
                response_transform::canonicalize(&mut transformed, endpoint);
                (
                    upstream_status,
                    serde_json::to_vec(&transformed).unwrap_or_default(),
                )
            } else {
                if let Some(in_flight) = route_in_flight.as_mut() {
                    in_flight.skip_cache(cache_skip_reason_for_status(upstream_status));
                }
                errors::normalize_upstream_error_parts(
                    surface,
                    upstream_status,
                    &forward.upstream_body,
                    &received_body,
                    Some(&request_id),
                )
            };
            if client_status >= 400 {
                log_failed_attempts(outcome_ctx, &forward.failed_attempts, false);
                let detail = detail_snippet_bytes(&forward.upstream_body);
                log_generated_outcome(
                    outcome_ctx,
                    "buffered_upstream",
                    client_status,
                    upstream_status,
                    &forward.selected_route,
                    forward.failed_attempts.len() as u32,
                    &detail,
                );
            }

            match service.finalize_middleware_receipt(
                forward.receipt,
                &final_body,
                Some("application/json"),
                requester,
                None,
            ) {
                Ok(finalized) => {
                    let status =
                        StatusCode::from_u16(client_status).unwrap_or(StatusCode::BAD_GATEWAY);
                    let mut headers = gateway_owned_headers("application/json");
                    insert_header(&mut headers, "x-receipt-id", &finalized.receipt.receipt_id);
                    (status, headers, finalized.wire_body).into_response()
                }
                Err(err) => {
                    let status = forward_error_status(&err);
                    let detail = detail_snippet_text(&err.to_string());
                    log_generated_outcome(
                        outcome_ctx,
                        "finalize_buffered",
                        status,
                        upstream_status,
                        &forward.selected_route,
                        forward.failed_attempts.len() as u32,
                        &detail,
                    );
                    service_error_response(surface, endpoint_path, service, &request_id, err)
                }
            }
        }
        Ok(MiddlewareForwardResult::Stream(forward)) => {
            if let Some(in_flight) = route_in_flight.as_mut() {
                in_flight.retarget(&forward.selected_route);
            }
            let content_type = forward
                .upstream_headers
                .get("content-type")
                .map(|value| value.split(';').next().unwrap_or("").trim())
                .filter(|base| !base.is_empty())
                .unwrap_or("text/event-stream")
                .to_string();
            let upstream_status = forward.upstream_status;
            let attempt_index = forward.failed_attempts.len() as u32;
            let selected_format = candidates
                .iter()
                .find(|c| c.route_id == forward.selected_route)
                .or_else(|| candidates.first())
                .map(|c| c.format)
                .unwrap_or(ProviderFormat::Openai);
            let transformed: ServiceResponseStream =
                match stream_transform::select_stream_transform(selected_format, endpoint) {
                    Some(transform) => Box::pin(SseTransformStream::new(forward.body, transform)),
                    None => forward.body,
                };
            let visible: ServiceResponseStream = transformed;
            let sanitized: ServiceResponseStream = Box::pin(SseTransformStream::new(
                visible,
                stream_transform::StreamTransform::SanitizeResponse(identity.clone(), endpoint),
            ));
            let downstream_abort = Arc::new(AtomicBool::new(false));
            let meter_settled = Arc::new(AtomicBool::new(false));
            let cache_observation = route_in_flight
                .as_mut()
                .and_then(RouteInFlight::take_cache_observation);
            let stream_report = StreamReport {
                control: control.clone(),
                request_id: request_id.clone(),
                endpoint: endpoint_path.to_string(),
                request_model: model.unwrap_or("").to_string(),
                pricing: pricing.clone(),
                spend_mode: None,
                user_id: None,
                virtual_key_id: None,
                selected_route_id: Some(forward.selected_route.clone()),
                attempt_index,
                upstream_status,
                started,
                downstream_abort: downstream_abort.clone(),
                settled: meter_settled.clone(),
                cache_observation,
            };
            let metered: ServiceResponseStream = Box::pin(MeterStream::new(
                sanitized,
                stream_report,
                crate::sse_protocol::sse_protocol(endpoint_path),
            ));
            let keepalive = match sse_keepalive_ms.unwrap_or(10_000) {
                0 => None,
                ms => Some(Duration::from_millis(ms)),
            };
            let kept: ServiceResponseStream = Box::pin(KeepAliveStream::new(metered, keepalive));

            let receipt_id = journal.peek_receipt_id();
            match service.finalize_middleware_response_stream(
                journal,
                kept,
                endpoint_path,
                Some(&content_type),
                requester,
                None,
                Some(request_id.clone()),
            ) {
                Ok(finalized) => {
                    let status =
                        StatusCode::from_u16(upstream_status).unwrap_or(StatusCode::BAD_GATEWAY);
                    let mut headers = gateway_owned_headers(&content_type);
                    if let Some(receipt_id) = &receipt_id {
                        insert_header(&mut headers, "x-receipt-id", receipt_id);
                    }
                    headers.insert(
                        HeaderName::from_static("x-accel-buffering"),
                        HeaderValue::from_static("no"),
                    );
                    headers.insert(
                        HeaderName::from_static("cache-control"),
                        HeaderValue::from_static("no-cache"),
                    );
                    let guarded: ServiceResponseStream = Box::pin(InFlightStream {
                        inner: finalized.body,
                        _route_in_flight: route_in_flight.take(),
                    });
                    let stream_request_id = request_id.clone();
                    let stream_model = model.unwrap_or("").to_string();
                    let stream_route = forward.selected_route.clone();
                    let stream_started = started;
                    let body = Body::from_stream(guarded.scan((), move |_, chunk| {
                        std::future::ready(match chunk {
                            Ok(bytes) => Some(Ok::<_, std::io::Error>(bytes)),
                            Err(err) => {
                                downstream_abort.store(true, Ordering::Relaxed);
                                tracing::warn!(
                                    target: "stream_abort",
                                    request_id = %stream_request_id,
                                    error = %err,
                                    "response stream error; ending body gracefully"
                                );
                                if meter_settled.load(Ordering::Relaxed) {
                                    let stream_ctx = OutcomeCtx {
                                        request_id: &stream_request_id,
                                        model: &stream_model,
                                        started: stream_started,
                                    };
                                    log_generated_outcome(
                                        stream_ctx,
                                        "finalize_error",
                                        502,
                                        upstream_status,
                                        &stream_route,
                                        attempt_index,
                                        &detail_snippet_text(&err.to_string()),
                                    );
                                }
                                None
                            }
                        })
                    }));
                    (status, headers, body).into_response()
                }
                Err(err) => {
                    let status = forward_error_status(&err);
                    let detail = detail_snippet_text(&err.to_string());
                    log_generated_outcome(
                        outcome_ctx,
                        "finalize_stream",
                        status,
                        upstream_status,
                        &forward.selected_route,
                        forward.failed_attempts.len() as u32,
                        &detail,
                    );
                    service_error_response(surface, endpoint_path, service, &request_id, err)
                }
            }
        }
        Ok(MiddlewareForwardResult::UpstreamError(forward)) => {
            if let Some(in_flight) = route_in_flight.as_mut() {
                in_flight.retarget(&forward.selected_route);
                in_flight.skip_cache(cache_skip_reason_for_status(forward.error.upstream_status));
            }
            let (status, body) = errors::normalize_upstream_error_parts(
                surface,
                forward.error.upstream_status,
                &forward.error.upstream_body,
                &received_body,
                Some(&request_id),
            );
            log_failed_attempts(outcome_ctx, &forward.failed_attempts, true);
            let detail = detail_snippet_bytes(&forward.error.upstream_body);
            log_generated_outcome(
                outcome_ctx,
                "stream_upstream",
                status,
                forward.error.upstream_status,
                &forward.selected_route,
                forward.failed_attempts.len() as u32,
                &detail,
            );
            finalize_generated(surface, service, endpoint_path, status, body, &[])
        }
        Ok(MiddlewareForwardResult::AllFailed(forward)) => {
            if let Some(in_flight) = route_in_flight.as_mut() {
                in_flight.skip_cache(cache_skip_reason_for_error(&forward.error));
            }
            log_failed_attempts(outcome_ctx, &forward.failed_attempts, stream);
            let status = forward_error_status(&forward.error);
            let detail = detail_snippet_text(&forward.error.to_string());
            log_generated_outcome(
                outcome_ctx,
                "all_candidates_failed",
                status,
                0,
                "",
                forward.failed_attempts.len() as u32,
                &detail,
            );
            service_error_response(surface, endpoint_path, service, &request_id, forward.error)
        }
        Err(err) => {
            if let Some(in_flight) = route_in_flight.as_mut() {
                in_flight.skip_cache(cache_skip_reason_for_error(&err));
            }
            let status = forward_error_status(&err);
            let detail = detail_snippet_text(&err.to_string());
            log_generated_outcome(outcome_ctx, "forward_error", status, 0, "", 0, &detail);
            service_error_response(surface, endpoint_path, service, &request_id, err)
        }
    }
}

struct InFlightStream {
    inner: ServiceResponseStream,
    _route_in_flight: Option<RouteInFlight>,
}

impl Unpin for InFlightStream {}

impl Stream for InFlightStream {
    type Item = Result<bytes::Bytes, ServiceError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

fn forward_error_status(err: &ServiceError) -> u16 {
    match err {
        ServiceError::E2ee(_) => 400,
        ServiceError::UpstreamVerification(_) => 503,
        ServiceError::Upstream(UpstreamError::Routing(_)) => 404,
        _ => 502,
    }
}

fn service_error_response(
    surface: Surface,
    endpoint_path: &str,
    service: &AciService,
    request_id: &str,
    err: ServiceError,
) -> Response {
    let status = forward_error_status(&err);
    let body = errors::envelope_bytes(
        surface,
        errors::error_type(surface, status),
        &err.to_string(),
        Some(request_id),
    );
    finalize_generated(surface, service, endpoint_path, status, body, &[])
}

fn finalize_generated(
    _surface: Surface,
    _service: &AciService,
    _endpoint_path: &str,
    status: u16,
    body: Vec<u8>,
    extra_headers: &[(&'static str, String)],
) -> Response {
    let status_code = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    for (name, value) in extra_headers {
        insert_header(&mut headers, name, value);
    }
    (status_code, headers, body).into_response()
}

fn gateway_owned_headers(content_type: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Ok(value) = HeaderValue::from_str(content_type) {
        headers.insert(CONTENT_TYPE, value);
    }
    headers
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) {
    if let (Ok(name), Ok(value)) = (
        HeaderName::from_bytes(name.as_bytes()),
        HeaderValue::from_str(value),
    ) {
        headers.insert(name, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::types::Engine;

    #[test]
    fn passthrough_candidates_preserve_received_body_bytes() {
        let received = br#"{
  "model": "gemma4-31b-it",
  "messages": [{"role": "user", "content": "hi"}],
  "stream_options": {"include_usage": true, "continuous_usage_stats": true}
}"#;
        let candidates = vec![
            RouteCandidate {
                route_id: "use2-a:gemma4-31b-it".to_string(),
                format: ProviderFormat::Openai,
                engine: Some(Engine::Vllm),
            },
            RouteCandidate {
                route_id: "use2-b:gemma4-31b-it".to_string(),
                format: ProviderFormat::Openai,
                engine: Some(Engine::Sglang),
            },
        ];

        let forward = passthrough_forward_candidates(received, &candidates);

        assert_eq!(forward.len(), candidates.len());
        assert_eq!(forward[0].route_id, "use2-a:gemma4-31b-it");
        assert_eq!(forward[1].route_id, "use2-b:gemma4-31b-it");
        assert!(forward.iter().all(|candidate| candidate.body == received));
    }
}
