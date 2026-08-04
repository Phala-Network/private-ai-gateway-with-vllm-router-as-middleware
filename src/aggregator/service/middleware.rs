//! The middleware seam: forwarding a request on behalf of the
//! middleware and finalizing the receipt/response it returns.
//!

use super::e2ee_crypto::{encrypt_e2ee_final_response, is_sse_content_type};
use super::forward::{cite_served_session, ReverifyOutcome};
use super::helpers::{
    accepted_response_model, collect_upstream_body, extract_chat_id, generate_receipt_id,
};
use super::streaming::{
    E2eeSseTransformer, MiddlewareProviderResponseDraftingStream,
    MiddlewareResponseFinalizingStream, SseChatIdParser,
};
use super::{
    AciService, ChatCompletionRequest, E2eeError, E2eeRequestContext, E2eeResponseInfo,
    ForwardCandidate, MiddlewareAllFailed, MiddlewareForwardResult, MiddlewareForwarded,
    MiddlewareGeneratedFinalization, MiddlewareReceiptDraft, MiddlewareReceiptFinalization,
    MiddlewareReceiptJournal, MiddlewareStreamFinalization, MiddlewareStreamingForwarded,
    MiddlewareUpstreamError, ReceiptOwner, ServiceError, ServiceResponseStream,
    StreamingUpstreamError,
};
use crate::aci::receipt::{ReceiptBuilder, TransparencyEventKind, UpstreamVerifiedEvent};
use crate::aci::upstream::{UpstreamError, UpstreamRequest, UpstreamResponse};
use crate::aggregator::metrics::{RequestMode, StreamErrorKind};
use crate::aggregator::session::SessionClaims;
use crate::middleware::errors::is_upstream_capacity_signal;
use std::collections::HashMap;
use std::time::{Duration, Instant};

// Provider statuses that make this candidate worth abandoning for the next one.
// Beyond the transient 429/5xx signals, an auth/account failure specific to this
// provider - 401 (invalid key), 402 (out of credit), 403 (key lacks access) -
// can still be served by a sibling candidate on a different account. Provider
// 404s are catalog misses and can succeed on siblings; request-level 400/422
// stay terminal because they should fail identically on every candidate.
fn is_retryable_provider_status(status: u16) -> bool {
    matches!(status, 401 | 402 | 403 | 404 | 429 | 500 | 502 | 503 | 504)
}

// One delayed second pass for requests whose whole candidate chain died on
// capacity. Basic traffic is preemptible and keeps the fast 429.
const CAPACITY_RETRY_DELAY_MS: u64 = 2_000;
const CAPACITY_RETRY_MAX_ELAPSED: Duration = Duration::from_secs(10);
const PREEMPTIBLE_TIER: &str = "basic";

fn capacity_retry_eligible(done: bool, user_tier: Option<&str>, started: Instant) -> bool {
    !done && user_tier != Some(PREEMPTIBLE_TIER) && started.elapsed() <= CAPACITY_RETRY_MAX_ELAPSED
}

// Whether to abandon this candidate and try the next. The status must be a
// provider-specific/transient failure AND the error must not be the client's own
// fault: a fetch failure on a client-supplied image URL fails identically on every
// candidate, so it is terminal (committed and surfaced as a 400) rather than retried.
fn should_fail_over(status: u16, received_body: &[u8], upstream_body: &[u8]) -> bool {
    (is_retryable_provider_status(status) || is_upstream_capacity_signal(status, upstream_body))
        && crate::middleware::errors::classify_image_input_error(
            received_body,
            status,
            upstream_body,
        )
        .is_none()
}

/// Track the highest-priority failover error so that, when every candidate
/// fails, the returned error reflects the most informative failure.
/// Priority order: verification (3), then transport (2), then routing (1).
fn upgrade_err(slot: &mut Option<(u8, ServiceError)>, priority: u8, err: ServiceError) {
    if slot.as_ref().map(|(p, _)| priority >= *p).unwrap_or(true) {
        *slot = Some((priority, err));
    }
}

/// A candidate's real upstream response, held back while the failover walk
/// keeps looking for a 2xx. If nothing better follows, this response is
/// committed rather than synthesizing a less-informative aggregate error.
enum RetainedResponse {
    Streaming {
        error: StreamingUpstreamError,
        route_id: String,
        attempt_slot: usize,
    },
    Buffered {
        inputs: Box<BufferedCommit>,
        attempt_slot: usize,
    },
}

impl RetainedResponse {
    fn attempt_slot(&self) -> usize {
        match self {
            Self::Streaming { attempt_slot, .. } | Self::Buffered { attempt_slot, .. } => {
                *attempt_slot
            }
        }
    }
}

pub(super) struct BufferedCommit {
    pub response: UpstreamResponse,
    pub response_model: Option<String>,
    pub recorded_event: UpstreamVerifiedEvent,
    pub route_id: String,
    pub middleware_forwarded_body: Vec<u8>,
    pub forwarded_body: Vec<u8>,
}

/// The request/response context observed for one forwarded candidate,
/// captured inside the TEE. Grouped so
/// [`AciService::build_middleware_receipt_prefix`] reads by field name rather
/// than ten positional arguments.
pub(super) struct MiddlewareReceiptInputs<'a> {
    pub receipt_id: &'a str,
    pub chat_id: Option<String>,
    /// The user-requested model (received request's top-level `model`), recorded
    /// as the receipt's top-level `model`; `None` when the request carried none.
    pub model: Option<String>,
    pub served_at: u64,
    pub endpoint_path: &'a str,
    pub received_body: &'a [u8],
    pub middleware_forwarded_body: &'a [u8],
    pub selected_route_id: &'a str,
    pub forwarded_body: &'a [u8],
    pub recorded_event: UpstreamVerifiedEvent,
    pub recorded: Option<(String, SessionClaims)>,
}

impl AciService {
    pub(super) fn build_middleware_receipt_prefix(
        &self,
        inputs: MiddlewareReceiptInputs<'_>,
    ) -> Result<ReceiptBuilder, ServiceError> {
        let MiddlewareReceiptInputs {
            receipt_id,
            chat_id,
            model,
            served_at,
            endpoint_path,
            received_body,
            middleware_forwarded_body,
            selected_route_id,
            forwarded_body,
            recorded_event,
            recorded,
        } = inputs;
        let mut builder = ReceiptBuilder::new(
            receipt_id.to_string(),
            chat_id,
            model,
            self.workload_id.clone(),
            self.workload_keyset_digest.clone(),
            endpoint_path.to_string(),
            "POST".to_string(),
            served_at,
        );
        builder.add_request_received(received_body)?;
        builder.add_middleware_forwarded(middleware_forwarded_body)?;
        builder.add_route_selected(selected_route_id)?;
        builder.add_request_forwarded(forwarded_body)?;
        if received_body != forwarded_body {
            builder.add_transparency_event(TransparencyEventKind::RequestModified)?;
        }
        Self::append_upstream_verified(&mut builder, recorded_event, recorded)?;
        Ok(builder)
    }

    fn commit_buffered_response(
        &self,
        commit: BufferedCommit,
        failed_attempts: Vec<(String, u16)>,
        endpoint_path: &str,
        received_body: &[u8],
        user_model: Option<String>,
    ) -> Result<MiddlewareForwardResult, ServiceError> {
        let BufferedCommit {
            response,
            response_model,
            recorded_event,
            route_id,
            middleware_forwarded_body,
            forwarded_body,
        } = commit;
        let status = response.status_code;

        let receipt_id = generate_receipt_id();
        let served_at = self.clock.now_secs();
        let chat_id = extract_chat_id(&response.body);
        let sealed = self.record_attested_upstream_session(&recorded_event)?;
        let recorded = cite_served_session(&sealed, response.served_instance_id.as_deref());
        let session_id = recorded.as_ref().map(|(id, _)| id.clone());
        let mut builder = self.build_middleware_receipt_prefix(MiddlewareReceiptInputs {
            receipt_id: &receipt_id,
            chat_id,
            model: user_model,
            served_at,
            endpoint_path,
            received_body,
            middleware_forwarded_body: &middleware_forwarded_body,
            selected_route_id: &route_id,
            forwarded_body: &forwarded_body,
            recorded_event,
            recorded,
        })?;
        builder.set_upstream_verified_model_id(response_model.clone());
        let provider_response_hash = builder.add_response_received(&response.body)?;

        Ok(MiddlewareForwardResult::Forwarded(Box::new(
            MiddlewareForwarded {
                receipt_id: receipt_id.clone(),
                receipt: MiddlewareReceiptDraft {
                    receipt_id: receipt_id.clone(),
                    builder,
                    provider_response_hash,
                    endpoint_path: endpoint_path.to_string(),
                    request_mode: RequestMode::Buffered,
                    response_model,
                },
                upstream_status: status,
                upstream_body: response.body,
                upstream_headers: response.headers,
                selected_route: route_id,
                failed_attempts,
                session_id,
            },
        )))
    }

    pub async fn forward_chat_completion_for_middleware(
        &self,
        req: ChatCompletionRequest<'_>,
        candidates: Vec<ForwardCandidate>,
        stream: bool,
        receipt_journal: MiddlewareReceiptJournal,
    ) -> Result<MiddlewareForwardResult, ServiceError> {
        let received_body = req.received_body;
        let endpoint_path = req.endpoint_path;
        // The user-requested model, recorded as the receipt's top-level `model`.
        let user_model = req.context.user_model.clone();
        let mode = if stream {
            RequestMode::Streaming
        } else {
            RequestMode::Buffered
        };
        self.metrics
            .record_request(endpoint_path, mode, req.e2ee.as_ref().is_some());

        if candidates.is_empty() {
            return Err(ServiceError::Upstream(UpstreamError::Routing(
                "no candidate routes supplied".to_string(),
            )));
        }
        // A caller-supplied verifier event only applies to a single
        // explicit candidate (non-failover). With an ordered list the
        // backend always computes per-candidate events.
        let caller_supplied_upstream_event =
            req.upstream_verification_event.is_some() && candidates.len() == 1;
        let single_caller_event = if caller_supplied_upstream_event {
            req.upstream_verification_event.clone()
        } else {
            None
        };
        let candidate_route_ids: Vec<String> =
            candidates.iter().map(|c| c.route_id.clone()).collect();

        // Optional trusted x-user-tier passed through to every upstream attempt.
        let mut upstream_headers: HashMap<String, String> = HashMap::new();
        if let Some(tier) = req.context.user_tier.as_deref() {
            upstream_headers.insert("x-user-tier".to_string(), tier.to_string());
        }

        // Highest-priority error across exhausted candidates, returned if
        // no candidate succeeds.
        //
        // The number of candidates attempted (`index + 1` when one succeeds)
        // is surfaced via a response header for the caller's metrics. Failover
        // is internal to this forwarder and is NOT recorded in the user-facing
        // receipt; the receipt attests only the served request (route.selected
        // + upstream.verified + hashes).
        let mut aggregated_err: Option<(u8, ServiceError)> = None;

        // Candidates that failed and were failed over, as (route_id, status),
        // in the order tried. The committed route is carried separately via
        // `selected_route`; these are surfaced to the caller so every attempt
        // is observable, not just the one that served the response. Non-HTTP
        // failures (prepare/verification/transport) record 502.
        let mut failed_attempts: Vec<(String, u16)> = Vec::new();

        let mut retained: Option<RetainedResponse> = None;
        let forward_started = Instant::now();
        let mut capacity_retry_done = false;
        let mut current: Vec<ForwardCandidate> = candidates;
        let mut capacity_indices: Vec<usize> = Vec::new();

        loop {
            capacity_indices.clear();
            let last_index = current.len() - 1;
            for (index, candidate) in current.iter().enumerate() {
                let route_id = candidate.route_id.clone();
                let is_last = index == last_index;

                let prepared = match self.upstream.prepare(UpstreamRequest {
                    body: candidate.body.clone(),
                    headers: upstream_headers.clone(),
                    path: Some(endpoint_path.to_string()),
                    target_route_id: Some(route_id.clone()),
                }) {
                    Ok(prepared) => prepared,
                    Err(UpstreamError::Routing(message)) => {
                        failed_attempts.push((route_id.clone(), 502));
                        upgrade_err(
                            &mut aggregated_err,
                            1,
                            ServiceError::Upstream(UpstreamError::Routing(message)),
                        );
                        continue;
                    }
                    Err(err) => {
                        failed_attempts.push((route_id.clone(), 502));
                        upgrade_err(&mut aggregated_err, 2, err.into());
                        continue;
                    }
                };

                // Per-route fail-closed mode: explicitly non-TEE routes never
                // fail closed; TEE and unclassified routes honour the
                // request-level `upstream_required` flag.
                let non_tee = prepared.is_tee == Some(false);
                let candidate_required = if non_tee {
                    Some(false)
                } else {
                    req.upstream_required
                };

                let mut recorded_event = match self
                    .recorded_upstream_event(
                        &prepared,
                        candidate_required,
                        single_caller_event.clone(),
                    )
                    .await
                {
                    Ok(event) => event,
                    Err(ServiceError::UpstreamVerification(uv)) => {
                        failed_attempts.push((route_id.clone(), 502));
                        upgrade_err(
                            &mut aggregated_err,
                            3,
                            ServiceError::UpstreamVerification(uv),
                        );
                        continue;
                    }
                    Err(err) => return Err(err),
                };

                let forwarded_body = prepared.request.body.clone();

                if stream {
                    let upstream_response = match self
                        .forward_with_binding_reverify(
                            &prepared,
                            &mut recorded_event,
                            candidate_required,
                            caller_supplied_upstream_event,
                            // Failover path: flush a possibly-stale binding on any
                            // terminal mismatch so the next candidate/request re-verifies.
                            true,
                            |prepared, event| async move {
                                self.upstream
                                    .forward_stream_verified_prepared(prepared, &event)
                                    .await
                            },
                        )
                        .await
                    {
                        ReverifyOutcome::Forwarded(response) => Some(response),
                        ReverifyOutcome::RefreshFailed(err) => {
                            let priority = if matches!(err, ServiceError::UpstreamVerification(_)) {
                                3
                            } else {
                                2
                            };
                            upgrade_err(&mut aggregated_err, priority, err);
                            None
                        }
                        ReverifyOutcome::Failed(err) => {
                            // Terminal binding mismatch and transport errors
                            // intentionally share failover priority 2 (a failed
                            // reverify outranks them at 3).
                            upgrade_err(&mut aggregated_err, 2, err.into());
                            None
                        }
                    };
                    let Some(upstream_response) = upstream_response else {
                        failed_attempts.push((route_id.clone(), 502));
                        continue;
                    };

                    let status = upstream_response.status_code;
                    if status != 200 {
                        self.metrics.record_upstream_response(
                            endpoint_path,
                            RequestMode::Streaming,
                            status,
                            None,
                        );
                        // Collect the (small) error body up front so the failover
                        // decision can inspect it. A truncated/unreadable error body
                        // must not abort the remaining candidates, so it degrades to
                        // empty — the caller's normalizer emits its generic message.
                        let upstream_headers = upstream_response.headers;
                        let upstream_body = collect_upstream_body(upstream_response.body)
                            .await
                            .unwrap_or_default();
                        let is_capacity = is_upstream_capacity_signal(status, &upstream_body);
                        let last_may_retry = (is_capacity || !capacity_indices.is_empty())
                            && capacity_retry_eligible(
                                capacity_retry_done,
                                req.context.user_tier.as_deref(),
                                forward_started,
                            );
                        if (!is_last || last_may_retry)
                            && should_fail_over(status, received_body, &upstream_body)
                        {
                            if is_capacity {
                                capacity_indices.push(index);
                            }
                            retained = Some(RetainedResponse::Streaming {
                                error: StreamingUpstreamError {
                                    upstream_status: status,
                                    upstream_headers,
                                    upstream_body,
                                },
                                route_id: route_id.clone(),
                                attempt_slot: failed_attempts.len(),
                            });
                            failed_attempts.push((route_id.clone(), status));
                            continue;
                        }
                        self.metrics
                            .record_stream_error(endpoint_path, StreamErrorKind::UpstreamNon2xx);
                        return Ok(MiddlewareForwardResult::UpstreamError(Box::new(
                            MiddlewareUpstreamError {
                                error: StreamingUpstreamError {
                                    upstream_status: status,
                                    upstream_headers,
                                    upstream_body,
                                },
                                selected_route: route_id.clone(),
                                failed_attempts: std::mem::take(&mut failed_attempts),
                            },
                        )));
                    }

                    // Commit this candidate.
                    let upstream_headers = upstream_response.headers;
                    let receipt_id = generate_receipt_id();
                    let served_at = self.clock.now_secs();
                    let sealed = self.record_attested_upstream_session(&recorded_event)?;
                    let recorded = cite_served_session(
                        &sealed,
                        upstream_response.served_instance_id.as_deref(),
                    );
                    let session_id = recorded.as_ref().map(|(id, _)| id.clone());
                    let builder =
                        self.build_middleware_receipt_prefix(MiddlewareReceiptInputs {
                            receipt_id: &receipt_id,
                            chat_id: None,
                            model: user_model.clone(),
                            served_at,
                            endpoint_path,
                            received_body,
                            middleware_forwarded_body: &candidate.body,
                            selected_route_id: &route_id,
                            forwarded_body: &forwarded_body,
                            recorded_event,
                            recorded,
                        })?;
                    receipt_journal.reserve_receipt_id(receipt_id.clone());

                    let body = MiddlewareProviderResponseDraftingStream::new(
                        upstream_response.body,
                        builder,
                        receipt_journal,
                        receipt_id.clone(),
                        endpoint_path.to_string(),
                        self.metrics.clone(),
                        status,
                    );

                    return Ok(MiddlewareForwardResult::Stream(Box::new(
                        MiddlewareStreamingForwarded {
                            receipt_id: receipt_id.clone(),
                            upstream_status: status,
                            upstream_headers,
                            body: Box::pin(body),
                            selected_route: route_id.clone(),
                            failed_attempts: std::mem::take(&mut failed_attempts),
                            session_id,
                        },
                    )));
                }

                // Buffered forward.
                let upstream_response = match self
                    .forward_with_binding_reverify(
                        &prepared,
                        &mut recorded_event,
                        candidate_required,
                        caller_supplied_upstream_event,
                        // Failover path: flush a possibly-stale binding on any
                        // terminal mismatch so the next candidate/request re-verifies.
                        true,
                        |prepared, event| async move {
                            self.upstream
                                .forward_verified_prepared(prepared, &event)
                                .await
                        },
                    )
                    .await
                {
                    ReverifyOutcome::Forwarded(response) => Some(response),
                    ReverifyOutcome::RefreshFailed(err) => {
                        let priority = if matches!(err, ServiceError::UpstreamVerification(_)) {
                            3
                        } else {
                            2
                        };
                        upgrade_err(&mut aggregated_err, priority, err);
                        None
                    }
                    ReverifyOutcome::Failed(err) => {
                        // Terminal binding mismatch and transport errors
                        // intentionally share failover priority 2 (a failed
                        // reverify outranks them at 3).
                        upgrade_err(&mut aggregated_err, 2, err.into());
                        None
                    }
                };
                let Some(upstream_response) = upstream_response else {
                    failed_attempts.push((route_id.clone(), 502));
                    continue;
                };

                let status = upstream_response.status_code;
                let response_model = accepted_response_model(status, &upstream_response.body);
                self.metrics.record_upstream_response(
                    endpoint_path,
                    RequestMode::Buffered,
                    status,
                    response_model.as_deref(),
                );
                let is_capacity = is_upstream_capacity_signal(status, &upstream_response.body);
                let last_may_retry = (is_capacity || !capacity_indices.is_empty())
                    && capacity_retry_eligible(
                        capacity_retry_done,
                        req.context.user_tier.as_deref(),
                        forward_started,
                    );
                if (!is_last || last_may_retry)
                    && should_fail_over(status, received_body, &upstream_response.body)
                {
                    if is_capacity {
                        capacity_indices.push(index);
                    }
                    retained = Some(RetainedResponse::Buffered {
                        inputs: Box::new(BufferedCommit {
                            response: upstream_response,
                            response_model,
                            recorded_event,
                            route_id: route_id.clone(),
                            middleware_forwarded_body: candidate.body.clone(),
                            forwarded_body,
                        }),
                        attempt_slot: failed_attempts.len(),
                    });
                    failed_attempts.push((route_id.clone(), status));
                    continue;
                }

                return self.commit_buffered_response(
                    BufferedCommit {
                        response: upstream_response,
                        response_model,
                        recorded_event,
                        route_id,
                        middleware_forwarded_body: candidate.body.clone(),
                        forwarded_body,
                    },
                    std::mem::take(&mut failed_attempts),
                    endpoint_path,
                    received_body,
                    user_model.clone(),
                );
            }

            if !capacity_indices.is_empty()
                && capacity_retry_eligible(
                    capacity_retry_done,
                    req.context.user_tier.as_deref(),
                    forward_started,
                )
            {
                capacity_retry_done = true;
                let jitter = rand::random::<u64>() % CAPACITY_RETRY_DELAY_MS;
                tokio::time::sleep(Duration::from_millis(CAPACITY_RETRY_DELAY_MS + jitter)).await;
                current = capacity_indices
                    .iter()
                    .map(|&index| current[index].clone())
                    .collect();
                continue;
            }
            break;
        }

        if let Some(retained) = retained {
            let mut failed_attempts = failed_attempts;
            let attempt_slot = retained.attempt_slot();
            if attempt_slot < failed_attempts.len() {
                failed_attempts.remove(attempt_slot);
            }
            return match retained {
                RetainedResponse::Streaming {
                    error, route_id, ..
                } => Ok(MiddlewareForwardResult::UpstreamError(Box::new(
                    MiddlewareUpstreamError {
                        error,
                        selected_route: route_id,
                        failed_attempts,
                    },
                ))),
                RetainedResponse::Buffered { inputs, .. } => self.commit_buffered_response(
                    *inputs,
                    failed_attempts,
                    endpoint_path,
                    received_body,
                    user_model,
                ),
            };
        }

        // No candidate succeeded. Return the highest-priority failure together
        // with every attempt's outcome; otherwise the caller can only report
        // the aggregate error and loses which candidates were exhausted.
        let error = aggregated_err.map(|(_, err)| err).unwrap_or_else(|| {
            ServiceError::Upstream(UpstreamError::Routing(format!(
                "all upstream routes failed (attempted: {})",
                candidate_route_ids.join(", ")
            )))
        });
        Ok(MiddlewareForwardResult::AllFailed(Box::new(
            MiddlewareAllFailed {
                failed_attempts,
                error,
            },
        )))
    }

    /// Start a streaming chat completion. The response stream hashes
    /// every byte in order and stores the receipt only after the
    /// upstream stream completes.
    pub fn finalize_middleware_receipt(
        &self,
        mut draft: MiddlewareReceiptDraft,
        final_cleartext_body: &[u8],
        content_type: Option<&str>,
        requester: Option<ReceiptOwner>,
        e2ee: Option<E2eeRequestContext>,
    ) -> Result<MiddlewareReceiptFinalization, ServiceError> {
        let is_sse = is_sse_content_type(content_type);
        if is_sse {
            let mut parser = SseChatIdParser::default();
            parser.observe(final_cleartext_body);
            if parser.chat_id.is_some() {
                draft.builder.set_chat_id(parser.chat_id);
            }
        } else if let Some(chat_id) = extract_chat_id(final_cleartext_body) {
            draft.builder.set_chat_id(Some(chat_id));
        }

        let wire_body = match e2ee.as_ref() {
            Some(ctx) => encrypt_e2ee_final_response(
                final_cleartext_body,
                ctx,
                &draft.endpoint_path,
                is_sse,
            )?,
            None => final_cleartext_body.to_vec(),
        };
        let e2ee_response = e2ee.as_ref().map(|ctx| E2eeResponseInfo {
            version: ctx.version.clone(),
            algo: ctx.algo.clone(),
        });

        let final_cleartext_hash = crate::aci::canonical::sha256_hex(final_cleartext_body);
        if draft.provider_response_hash != final_cleartext_hash || wire_body != final_cleartext_body
        {
            draft
                .builder
                .add_transparency_event(TransparencyEventKind::ResponseModified)?;
        }
        draft
            .builder
            .add_response_returned(final_cleartext_body, &wire_body)?;
        let receipt = draft
            .builder
            .finalize(self.keys.as_ref(), &self.default_receipt_key_id)?;
        self.store_receipt(receipt.clone(), requester);
        self.metrics.record_receipt_issued(
            &draft.endpoint_path,
            draft.request_mode,
            draft.response_model.as_deref(),
        );

        Ok(MiddlewareReceiptFinalization {
            receipt,
            wire_body,
            e2ee: e2ee_response,
        })
    }

    pub fn finalize_middleware_generated_response(
        &self,
        endpoint_path: &str,
        cleartext_body: &[u8],
        content_type: Option<&str>,
        e2ee: Option<E2eeRequestContext>,
    ) -> Result<MiddlewareGeneratedFinalization, ServiceError> {
        let is_sse = is_sse_content_type(content_type);
        let wire_body = match e2ee.as_ref() {
            Some(ctx) => encrypt_e2ee_final_response(cleartext_body, ctx, endpoint_path, is_sse)?,
            None => cleartext_body.to_vec(),
        };
        let e2ee_response = e2ee.as_ref().map(|ctx| E2eeResponseInfo {
            version: ctx.version.clone(),
            algo: ctx.algo.clone(),
        });
        Ok(MiddlewareGeneratedFinalization {
            wire_body,
            e2ee: e2ee_response,
        })
    }

    pub fn finalize_middleware_response_stream(
        &self,
        journal: MiddlewareReceiptJournal,
        cleartext_stream: ServiceResponseStream,
        endpoint_path: &str,
        content_type: Option<&str>,
        requester: Option<ReceiptOwner>,
        e2ee: Option<E2eeRequestContext>,
    ) -> Result<MiddlewareStreamFinalization, ServiceError> {
        let is_sse = is_sse_content_type(content_type);
        if e2ee.is_some() && !is_sse {
            return Err(E2eeError::EncryptionFailed.into());
        }
        let e2ee_response = e2ee.as_ref().map(|ctx| E2eeResponseInfo {
            version: ctx.version.clone(),
            algo: ctx.algo.clone(),
        });
        let e2ee_transformer = e2ee
            .clone()
            .map(|ctx| E2eeSseTransformer::new(ctx, endpoint_path.to_string()));
        let body = MiddlewareResponseFinalizingStream::new(
            self,
            cleartext_stream,
            journal,
            requester,
            endpoint_path.to_string(),
            e2ee_transformer,
            e2ee_response.is_some(),
        );
        Ok(MiddlewareStreamFinalization {
            body: Box::pin(body),
            e2ee: e2ee_response,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::is_retryable_provider_status;

    #[test]
    fn retryable_covers_transient_and_account_specific_statuses() {
        // Transient provider trouble (429/5xx), auth/account failures, and
        // provider catalog misses fail over to sibling candidates.
        for status in [401, 402, 403, 404, 429, 500, 502, 503, 504] {
            assert!(
                is_retryable_provider_status(status),
                "{status} should retry"
            );
        }
        // Request-level errors would fail identically on every candidate.
        for status in [400, 422] {
            assert!(
                !is_retryable_provider_status(status),
                "{status} should not retry"
            );
        }
    }
}
