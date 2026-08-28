//! Completion forwarding for the single-model router middleware.
//!
//! The router chooses ordered candidates. `AciService` still validates the
//! route, verifies the upstream, enforces channel binding, forwards the request,
//! and finalizes receipts.

use std::collections::{HashMap, HashSet};
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
use super::router::{RetryPlanner, RouteInFlight};
use super::sse::{KeepAliveStream, MeterStream, StreamReport};
use super::stream_transform::SseTransformStream;
use super::types::{Endpoint, ProviderFormat, RouteCandidate};

pub(super) struct CompletionRoutePlan {
    pub(super) candidates: Vec<RouteCandidate>,
    pub(super) retry_planner: Option<RetryPlanner>,
    pub(super) route_in_flight: Option<RouteInFlight>,
}
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
    rate_limited_generated(
        input.surface,
        service,
        input.endpoint_path,
        &input.request_id,
        message,
    )
}

pub(super) fn model_not_found(
    service: &AciService,
    input: &CompletionInput,
    model: Option<&str>,
) -> Response {
    let message = format!("no route available for model {}", model.unwrap_or("(none)"));
    let body = errors::envelope_bytes(
        input.surface,
        "model_not_found",
        &message,
        Some(&input.request_id),
    );
    finalize_generated(input.surface, service, input.endpoint_path, 404, body, &[])
}

fn rate_limited_generated(
    surface: Surface,
    service: &AciService,
    endpoint_path: &str,
    request_id: &str,
    message: &str,
) -> Response {
    let (body, headers) = rate_limit_parts(surface, request_id, message);
    finalize_generated(surface, service, endpoint_path, 429, body, &headers)
}

fn rate_limit_parts(
    surface: Surface,
    request_id: &str,
    message: &str,
) -> (Vec<u8>, Vec<(&'static str, String)>) {
    let reset_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64 + 1)
        .unwrap_or(1);
    let body = errors::rate_limit_envelope_bytes(surface, message, Some(request_id));
    let headers = errors::rate_limit_headers(0, reset_at);
    (body, headers)
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
    let body: Arc<[u8]> = Arc::from(received_body);
    candidates
        .iter()
        .map(|candidate| ForwardCandidate {
            route_id: candidate.route_id.clone(),
            body: body.clone(),
        })
        .collect()
}

fn retryable_capacity_attempts(result: &MiddlewareForwardResult) -> Option<Vec<(String, u16)>> {
    let (status, selected_route, failed_attempts) = match result {
        MiddlewareForwardResult::Forwarded(forward) => (
            forward.upstream_status,
            &forward.selected_route,
            &forward.failed_attempts,
        ),
        MiddlewareForwardResult::UpstreamError(error) => (
            error.error.upstream_status,
            &error.selected_route,
            &error.failed_attempts,
        ),
        MiddlewareForwardResult::Stream(_) | MiddlewareForwardResult::AllFailed(_) => return None,
    };
    if status != 429
        || failed_attempts
            .iter()
            .any(|(_, failed_status)| *failed_status != 429)
    {
        return None;
    }
    let mut attempts = failed_attempts.clone();
    attempts.push((selected_route.clone(), 429));
    Some(attempts)
}

fn prepend_failed_attempts(result: &mut MiddlewareForwardResult, mut previous: Vec<(String, u16)>) {
    let current = match result {
        MiddlewareForwardResult::Forwarded(result) => &mut result.failed_attempts,
        MiddlewareForwardResult::Stream(result) => &mut result.failed_attempts,
        MiddlewareForwardResult::UpstreamError(result) => &mut result.failed_attempts,
        MiddlewareForwardResult::AllFailed(result) => &mut result.failed_attempts,
    };
    previous.append(current);
    *current = previous;
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
    route_plan: CompletionRoutePlan,
) -> Response {
    let CompletionRoutePlan {
        candidates,
        retry_planner,
        mut route_in_flight,
    } = route_plan;
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
        // The Router resolves model identity before entering this function.
        // An empty candidate set here is therefore a transient routing/capacity
        // condition, including an upstream-config race, never model absence.
        let message = "Rate limit exceeded. Please retry after some time.";
        log_generated_outcome(outcome_ctx, "routing_exhausted", 429, 0, "", 0, message);
        return rate_limited_generated(surface, service, endpoint_path, &request_id, message);
    }

    let mut candidate_formats = candidates
        .iter()
        .map(|candidate| (candidate.route_id.clone(), candidate.format))
        .collect::<HashMap<_, _>>();
    let forward_candidates = passthrough_forward_candidates(&received_body, &candidates);

    let context = GatewayRequestContext {
        request_id: request_id.clone(),
        user_model: user_model.clone(),
        target_route_id: None,
        user_tier: user_tier.clone(),
    };

    let journal = MiddlewareReceiptJournal::default();
    let mut result = service
        .forward_chat_completion_for_middleware_observed(
            ChatCompletionRequest {
                context,
                endpoint_path,
                received_body: &received_body,
                forwarded_body: None,
                aci_required,
                aci_session_ids: aci_session_ids.clone(),
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

    if let (Ok(forwarded), Some(planner), Some(in_flight)) = (
        result.as_ref(),
        retry_planner.as_ref(),
        route_in_flight.as_mut(),
    ) {
        if let Some(previous_attempts) = retryable_capacity_attempts(forwarded) {
            let attempted_route_ids = previous_attempts
                .iter()
                .map(|(route_id, _)| route_id.clone())
                .collect::<HashSet<_>>();
            let retry_candidates = planner.next_round(&attempted_route_ids, in_flight).await;
            if !retry_candidates.is_empty() {
                candidate_formats.extend(
                    retry_candidates
                        .iter()
                        .map(|candidate| (candidate.route_id.clone(), candidate.format)),
                );
                let forward_candidates =
                    passthrough_forward_candidates(&received_body, &retry_candidates);
                let retry_context = GatewayRequestContext {
                    request_id: request_id.clone(),
                    user_model: user_model.clone(),
                    target_route_id: None,
                    user_tier: user_tier.clone(),
                };
                let mut retry_result = service
                    .forward_chat_completion_for_middleware_observed(
                        ChatCompletionRequest {
                            context: retry_context,
                            endpoint_path,
                            received_body: &received_body,
                            forwarded_body: None,
                            aci_required,
                            aci_session_ids: aci_session_ids.clone(),
                            upstream_verification_event: None,
                            requester: requester.clone(),
                            e2ee: None,
                        },
                        forward_candidates,
                        stream,
                        journal.clone(),
                        Some(in_flight as &mut dyn MiddlewareAttemptObserver),
                    )
                    .await;
                let retry_outcome = match retry_result.as_ref() {
                    Ok(forwarded) if retryable_capacity_attempts(forwarded).is_some() => {
                        "exhausted_429"
                    }
                    Ok(_) => "completed",
                    Err(_) => "error",
                };
                planner.record_outcome(retry_outcome);
                if let Ok(retry_forwarded) = retry_result.as_mut() {
                    prepend_failed_attempts(retry_forwarded, previous_attempts);
                }
                result = retry_result;
            }
        }
    }

    match result {
        Ok(MiddlewareForwardResult::Forwarded(forward)) => {
            if let Some(in_flight) = route_in_flight.as_mut() {
                in_flight.retarget(&forward.selected_route);
            }
            let upstream_status = forward.upstream_status;
            let selected_format = candidate_formats
                .get(&forward.selected_route)
                .copied()
                .or_else(|| candidates.first().map(|candidate| candidate.format))
                .unwrap_or(ProviderFormat::Openai);

            let (client_status, mut final_body) = if (200..300).contains(&upstream_status) {
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
            let rate_limit_headers = if client_status == 429 {
                let (body, headers) = rate_limit_parts(
                    surface,
                    &request_id,
                    "Rate limit exceeded. Please retry after some time.",
                );
                final_body = body;
                headers
            } else {
                Vec::new()
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
                    for (name, value) in &rate_limit_headers {
                        insert_header(&mut headers, name, value);
                    }
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
            let selected_format = candidate_formats
                .get(&forward.selected_route)
                .copied()
                .or_else(|| candidates.first().map(|candidate| candidate.format))
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
            if status == 429 {
                return rate_limited_generated(
                    surface,
                    service,
                    endpoint_path,
                    &request_id,
                    "Rate limit exceeded. Please retry after some time.",
                );
            }
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
        // At this point the Router already accepted the public model. A routing
        // error means the selected route disappeared or every candidate was
        // exhausted; exposing it as model_not_found incorrectly turns a
        // temporary capacity event into a 404.
        ServiceError::Upstream(UpstreamError::Routing(_)) => 429,
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
    if status == 429 {
        return rate_limited_generated(
            surface,
            service,
            endpoint_path,
            request_id,
            "Rate limit exceeded. Please retry after some time.",
        );
    }
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
        assert!(forward
            .iter()
            .all(|candidate| candidate.body.as_ref() == received));
        assert!(Arc::ptr_eq(&forward[0].body, &forward[1].body));
    }

    #[test]
    fn routing_exhaustion_maps_to_rate_limit() {
        let error = ServiceError::Upstream(UpstreamError::Routing(
            "all candidate routes disappeared".to_string(),
        ));

        assert_eq!(forward_error_status(&error), 429);
    }
}
