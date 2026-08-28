//! In-process cache-aware router middleware for one public model.
//!
//! PAG still performs the verified upstream forward and receipt finalization.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::{
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};
use serde_json::{json, Value};
use tokio::sync::Notify;

use crate::aggregator::service::{AciService, MiddlewareAttemptFailure, MiddlewareAttemptObserver};
use crate::aggregator::upstream_config::{
    PublicUpstreamConfig, UpstreamConfigManager, UpstreamConfigSnapshot, UpstreamMetricsTarget,
    UpstreamProvider,
};

use super::cache_index::CacheIndex;
use super::completion::{self, CompletionInput};
use super::config::MiddlewareConfig;
use super::control::ControlClient;
use super::errors::{self, Surface};
use super::runtime_config::{RouterConfigPatch, RouterConfigStore, RouterConfigUpdateError};
use super::types::{Endpoint, ProviderFormat, RouteCandidate};

const MAX_ROUTING_HISTORY_CHARS: usize = 16_384;
const UPSTREAM_STATUS_GREEN: u8 = 0;
const UPSTREAM_STATUS_YELLOW: u8 = 1;
const UPSTREAM_STATUS_RED: u8 = 2;
const PIG_PRESSURE_PASSTHROUGH_REASON: &str = "pig_pressure_passthrough";
const ROUTER_FORWARD_ROUND_LIMIT: usize = 3;
const ROUTE_CIRCUIT_FAILURE_THRESHOLD: u32 = 2;
const ROUTE_CIRCUIT_OPEN_DURATION: Duration = Duration::from_secs(5);
const METRICS_CIRCUIT_FAILURE_THRESHOLD: u32 = 3;
const METRICS_POLL_CONCURRENCY_LIMIT: usize = 4;

#[derive(Clone)]
struct RouterRoute {
    route_id: String,
    upstream_name: String,
    candidate: RouteCandidate,
}

#[derive(Default, Clone)]
struct RouteStats {
    running: usize,
    processed: u64,
    upstream_attempts: u64,
    upstream_429: u64,
    selected_by_cache: u64,
    selected_by_load: u64,
    selected_by_order: u64,
    cache_rejected_by_pressure: u64,
    pressure_passthrough: u64,
    last_capacity_rejection_epoch: Option<u64>,
}

#[derive(Default)]
struct RouterState {
    stats: HashMap<String, RouteStats>,
    cache_index: CacheIndex,
    upstream_metrics: HashMap<String, UpstreamMetrics>,
    dispatch_ledgers: HashMap<String, DispatchLedger>,
    route_circuits: HashMap<String, RouteCircuit>,
    metrics_epoch: u64,
}

#[derive(Default, Clone, Copy)]
struct RouteCircuit {
    consecutive_failures: u32,
    open_until: Option<Instant>,
    opens: u64,
}

impl RouteCircuit {
    fn record_failure(&mut self, now: Instant) -> bool {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures < ROUTE_CIRCUIT_FAILURE_THRESHOLD {
            return false;
        }
        let newly_opened = !self.is_open(now);
        if newly_opened {
            self.opens = self.opens.saturating_add(1);
        }
        self.open_until = Some(now + ROUTE_CIRCUIT_OPEN_DURATION);
        newly_opened
    }

    fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.open_until = None;
    }

    fn is_open(self, now: Instant) -> bool {
        self.open_until.is_some_and(|until| now < until)
    }

    fn remaining_ms(self, now: Instant) -> u64 {
        self.open_until
            .and_then(|until| until.checked_duration_since(now))
            .map_or(0, |remaining| remaining.as_millis() as u64)
    }
}

#[derive(Default, Clone, Copy)]
struct DispatchLedger {
    started: u64,
    reconciled: u64,
    pending_poll_watermark: Option<u64>,
    reservations: usize,
}

impl DispatchLedger {
    fn reserve(&mut self) {
        self.reservations = self.reservations.saturating_add(1);
    }

    fn release_reservation(&mut self) {
        self.reservations = self.reservations.saturating_sub(1);
    }

    fn record_dispatch(&mut self) {
        self.started = self.started.saturating_add(1);
    }

    fn observe_successful_poll(&mut self, started_before_poll: u64) {
        if let Some(previous) = self.pending_poll_watermark.replace(started_before_poll) {
            self.reconciled = self.reconciled.max(previous.min(self.started));
        }
    }

    fn unreconciled(self) -> usize {
        usize::try_from(self.started.saturating_sub(self.reconciled)).unwrap_or(usize::MAX)
    }
}

#[derive(Default, Clone)]
struct UpstreamMetrics {
    ok: bool,
    error: Option<String>,
    updated_at: Option<Instant>,
    last_error_at: Option<Instant>,
    consecutive_errors: u32,
    raw_observed_running: Option<f64>,
    raw_observed_waiting: Option<f64>,
    observed_running: Option<f64>,
    observed_waiting: Option<f64>,
    raw_global_limit: Option<f64>,
    global_limit: Option<f64>,
    predictive_admission_enforce: Option<bool>,
    router_backpressure_active: Option<bool>,
    router_backpressure_applied: Option<bool>,
    router_inspect_capacity: Option<f64>,
    basic_limit: Option<f64>,
    basic_inflight: Option<f64>,
    premium_inflight: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UserTier {
    Basic,
    Premium,
}

impl UserTier {
    fn from_header(value: Option<&str>) -> Self {
        match value.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("premium") => Self::Premium,
            _ => Self::Basic,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Basic => "basic",
            Self::Premium => "premium",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RoutePressure {
    blocked: bool,
    circuit_open: bool,
    capacity_rejected: bool,
    metrics_circuit_open: bool,
    metrics_missing: bool,
    metrics_error: bool,
    waiting: u64,
    fullness_milli: u64,
    effective_running: usize,
    pending_reservations: usize,
    unreconciled_dispatches: usize,
    processed: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LoadOrder {
    CapacityNormalized,
    Running,
}

type RouteOrderKey = (u8, u8, u8, u8, u8, u64, u64, u64, u64, String);

pub(super) struct RouterBackend {
    upstream_config: Arc<UpstreamConfigManager>,
    config: Arc<RouterConfigStore>,
    state: Arc<Mutex<RouterState>>,
    metrics: RouterMetrics,
    metrics_notify: Arc<Notify>,
}

pub(super) struct RetryPlanner {
    upstream_config: Arc<UpstreamConfigManager>,
    config: Arc<RouterConfigStore>,
    state: Arc<Mutex<RouterState>>,
    metrics: RouterMetrics,
    metrics_notify: Arc<Notify>,
    public_model: String,
    tier: UserTier,
}

#[derive(Debug, Clone)]
struct RouteSelection {
    route_id: String,
    reason: &'static str,
    cache_match_rate: f32,
    running_at_select: usize,
}

pub(super) struct RouteInFlight {
    route_id: Option<String>,
    pending_dispatch: bool,
    state: Arc<Mutex<RouterState>>,
    metrics: RouterMetrics,
    candidates_considered: usize,
    cache_observation: Option<CacheObservation>,
}

#[derive(Clone)]
struct RouterMetrics {
    registry: Registry,
    affinity_considered_total: IntCounterVec,
    affinity_selected_total: IntCounterVec,
    affinity_success_total: IntCounterVec,
    affinity_retarget_total: IntCounterVec,
    affinity_rejected_total: IntCounterVec,
    cache_record_committed_total: IntCounterVec,
    cache_record_skipped_total: IntCounterVec,
    cache_usage_skipped_total: IntCounterVec,
    cache_match_rate: HistogramVec,
    cache_match_chars: HistogramVec,
    cache_prompt_tokens_total: IntCounterVec,
    cache_cached_tokens_total: IntCounterVec,
    forward_candidate_count: Histogram,
    forward_attempt_count: Histogram,
    capacity_retry_total: IntCounterVec,
    circuit_open_total: IntCounterVec,
    process_open_fds: IntGauge,
    process_max_fds: IntGauge,
    metrics_error_upstreams: IntGauge,
    open_circuits: IntGauge,
}

pub(super) struct CacheObservation {
    state: Arc<Mutex<RouterState>>,
    metrics: RouterMetrics,
    model: String,
    routing_text: String,
    max_records: usize,
    initial_route: String,
    actual_route: String,
    selection_reason: &'static str,
    record_resolved: bool,
    record_committed: bool,
    usage_resolved: bool,
}

impl RouterMetrics {
    fn new() -> Result<Self, String> {
        let registry = Registry::new();
        let affinity_considered_total = IntCounterVec::new(
            Opts::new(
                "router_cache_affinity_considered_total",
                "Prefix-cache affinity candidates considered by route.",
            ),
            &["route"],
        )
        .map_err(|err| err.to_string())?;
        let affinity_selected_total = IntCounterVec::new(
            Opts::new(
                "router_cache_affinity_selected_total",
                "Requests whose initial route was selected by prefix-cache affinity.",
            ),
            &["route"],
        )
        .map_err(|err| err.to_string())?;
        let affinity_success_total = IntCounterVec::new(
            Opts::new(
                "router_cache_affinity_success_total",
                "Cache-selected requests committed after a successful upstream response.",
            ),
            &["route"],
        )
        .map_err(|err| err.to_string())?;
        let affinity_retarget_total = IntCounterVec::new(
            Opts::new(
                "router_cache_affinity_retarget_total",
                "Cache-selected requests ultimately served by a different route.",
            ),
            &["from_route", "to_route"],
        )
        .map_err(|err| err.to_string())?;
        let affinity_rejected_total = IntCounterVec::new(
            Opts::new(
                "router_cache_affinity_rejected_total",
                "Prefix-cache affinity candidates rejected by a bounded reason.",
            ),
            &["reason"],
        )
        .map_err(|err| err.to_string())?;
        let cache_record_committed_total = IntCounterVec::new(
            Opts::new(
                "router_cache_record_committed_total",
                "Cache-index records committed after confirmed upstream success.",
            ),
            &["route"],
        )
        .map_err(|err| err.to_string())?;
        let cache_record_skipped_total = IntCounterVec::new(
            Opts::new(
                "router_cache_record_skipped_total",
                "Cache-index records skipped before confirmed upstream success.",
            ),
            &["reason"],
        )
        .map_err(|err| err.to_string())?;
        let cache_usage_skipped_total = IntCounterVec::new(
            Opts::new(
                "router_cache_usage_skipped_total",
                "Successful cache observations without usable token-level cache telemetry.",
            ),
            &["reason"],
        )
        .map_err(|err| err.to_string())?;
        let cache_match_rate = HistogramVec::new(
            HistogramOpts::new(
                "router_cache_match_rate",
                "Character prefix match ratio for considered affinity candidates.",
            )
            .buckets(vec![0.0, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9, 1.0]),
            &["route"],
        )
        .map_err(|err| err.to_string())?;
        let cache_match_chars = HistogramVec::new(
            HistogramOpts::new(
                "router_cache_match_chars",
                "Matched routing characters for considered affinity candidates.",
            )
            .buckets(vec![
                64.0, 256.0, 1_024.0, 2_048.0, 4_096.0, 8_192.0, 16_384.0,
            ]),
            &["route"],
        )
        .map_err(|err| err.to_string())?;
        let cache_prompt_tokens_total = IntCounterVec::new(
            Opts::new(
                "router_cache_prompt_tokens_total",
                "Prompt tokens reported for successful routed requests.",
            ),
            &["route", "selection_reason"],
        )
        .map_err(|err| err.to_string())?;
        let cache_cached_tokens_total = IntCounterVec::new(
            Opts::new(
                "router_cache_cached_tokens_total",
                "Cached prompt tokens reported for successful routed requests.",
            ),
            &["route", "selection_reason"],
        )
        .map_err(|err| err.to_string())?;
        let forward_candidate_count = Histogram::with_opts(
            HistogramOpts::new(
                "router_forward_candidate_count",
                "Ordered candidates offered to one routed request.",
            )
            .buckets(vec![1.0, 2.0, 3.0]),
        )
        .map_err(|err| err.to_string())?;
        let forward_attempt_count = Histogram::with_opts(
            HistogramOpts::new(
                "router_forward_attempt_count",
                "Candidates considered by the forwarding walk for one request.",
            )
            .buckets(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
        )
        .map_err(|err| err.to_string())?;
        let capacity_retry_total = IntCounterVec::new(
            Opts::new(
                "router_capacity_retry_total",
                "Second-window capacity retries by bounded outcome.",
            ),
            &["outcome"],
        )
        .map_err(|err| err.to_string())?;
        let circuit_open_total = IntCounterVec::new(
            Opts::new(
                "router_circuit_open_total",
                "Route circuit openings by route and bounded failure class.",
            ),
            &["route", "reason"],
        )
        .map_err(|err| err.to_string())?;
        let process_open_fds = IntGauge::new(
            "router_process_open_fds",
            "Open file descriptors held by the Router process on Linux.",
        )
        .map_err(|err| err.to_string())?;
        let process_max_fds = IntGauge::new(
            "router_process_max_fds",
            "Soft file descriptor limit of the Router process on Linux.",
        )
        .map_err(|err| err.to_string())?;
        let metrics_error_upstreams = IntGauge::new(
            "router_metrics_error_upstreams",
            "Configured upstreams whose most recent metrics poll failed.",
        )
        .map_err(|err| err.to_string())?;
        let open_circuits = IntGauge::new(
            "router_open_circuits",
            "Open request-side route circuits plus metrics-side upstream circuits.",
        )
        .map_err(|err| err.to_string())?;

        for collector in [
            &affinity_considered_total,
            &affinity_selected_total,
            &affinity_success_total,
            &affinity_retarget_total,
            &affinity_rejected_total,
            &cache_record_committed_total,
            &cache_record_skipped_total,
            &cache_usage_skipped_total,
            &cache_prompt_tokens_total,
            &cache_cached_tokens_total,
            &circuit_open_total,
            &capacity_retry_total,
        ] {
            registry
                .register(Box::new(collector.clone()))
                .map_err(|err| err.to_string())?;
        }
        registry
            .register(Box::new(cache_match_rate.clone()))
            .map_err(|err| err.to_string())?;
        registry
            .register(Box::new(cache_match_chars.clone()))
            .map_err(|err| err.to_string())?;
        for collector in [&forward_candidate_count, &forward_attempt_count] {
            registry
                .register(Box::new(collector.clone()))
                .map_err(|err| err.to_string())?;
        }
        for collector in [
            &process_open_fds,
            &process_max_fds,
            &metrics_error_upstreams,
            &open_circuits,
        ] {
            registry
                .register(Box::new(collector.clone()))
                .map_err(|err| err.to_string())?;
        }

        Ok(Self {
            registry,
            affinity_considered_total,
            affinity_selected_total,
            affinity_success_total,
            affinity_retarget_total,
            affinity_rejected_total,
            cache_record_committed_total,
            cache_record_skipped_total,
            cache_usage_skipped_total,
            cache_match_rate,
            cache_match_chars,
            cache_prompt_tokens_total,
            cache_cached_tokens_total,
            forward_candidate_count,
            forward_attempt_count,
            capacity_retry_total,
            circuit_open_total,
            process_open_fds,
            process_max_fds,
            metrics_error_upstreams,
            open_circuits,
        })
    }

    fn render(&self) -> Result<Vec<u8>, prometheus::Error> {
        let encoder = TextEncoder::new();
        let mut body = Vec::new();
        encoder.encode(&self.registry.gather(), &mut body)?;
        Ok(body)
    }

    fn record_forward_candidate_count(&self, count: usize) {
        self.forward_candidate_count.observe(count as f64);
    }

    fn record_forward_attempt_count(&self, count: usize) {
        self.forward_attempt_count.observe(count as f64);
    }

    fn record_capacity_retry(&self, outcome: &'static str) {
        self.capacity_retry_total
            .with_label_values(&[outcome])
            .inc();
    }

    fn record_circuit_open(&self, route: &str, reason: &'static str) {
        self.circuit_open_total
            .with_label_values(&[route, reason])
            .inc();
    }

    fn refresh_runtime(&self, state: &RouterState) {
        let now = Instant::now();
        let metrics_errors = state
            .upstream_metrics
            .values()
            .filter(|metrics| !metrics.ok)
            .count();
        let open_routes = state
            .route_circuits
            .values()
            .filter(|circuit| circuit.is_open(now))
            .count()
            + state
                .upstream_metrics
                .values()
                .filter(|metrics| metrics.metrics_circuit_open())
                .count();
        self.metrics_error_upstreams.set(metrics_errors as i64);
        self.open_circuits.set(open_routes as i64);
        if let Some(open) = linux_open_fd_count() {
            self.process_open_fds.set(open as i64);
        }
        if let Some(limit) = linux_soft_fd_limit() {
            self.process_max_fds.set(limit as i64);
        }
    }

    fn record_affinity_considered(&self, route: &str, rate: f32, matched_chars: usize) {
        self.affinity_considered_total
            .with_label_values(&[route])
            .inc();
        self.cache_match_rate
            .with_label_values(&[route])
            .observe(f64::from(rate));
        self.cache_match_chars
            .with_label_values(&[route])
            .observe(matched_chars as f64);
    }

    fn record_affinity_selected(&self, route: &str) {
        self.affinity_selected_total
            .with_label_values(&[route])
            .inc();
    }

    fn record_affinity_rejected(&self, reason: &'static str) {
        self.affinity_rejected_total
            .with_label_values(&[reason])
            .inc();
    }

    fn record_cache_committed(&self, route: &str) {
        self.cache_record_committed_total
            .with_label_values(&[route])
            .inc();
    }

    fn record_cache_skipped(&self, reason: &'static str) {
        self.cache_record_skipped_total
            .with_label_values(&[reason])
            .inc();
    }

    fn record_usage_skipped(&self, reason: &'static str) {
        self.cache_usage_skipped_total
            .with_label_values(&[reason])
            .inc();
    }
}

impl CacheObservation {
    fn new(
        state: Arc<Mutex<RouterState>>,
        metrics: RouterMetrics,
        model: String,
        routing_text: String,
        max_records: usize,
        selection: &RouteSelection,
    ) -> Option<Self> {
        if routing_text.is_empty() || max_records == 0 {
            return None;
        }
        Some(Self {
            state,
            metrics,
            model,
            routing_text,
            max_records,
            initial_route: selection.route_id.clone(),
            actual_route: selection.route_id.clone(),
            selection_reason: selection.reason,
            record_resolved: false,
            record_committed: false,
            usage_resolved: false,
        })
    }

    fn retarget(&mut self, route_id: &str) {
        self.actual_route.clear();
        self.actual_route.push_str(route_id);
    }

    pub(super) fn commit(&mut self) {
        if self.record_resolved {
            return;
        }
        {
            let mut state = self.state.lock().expect("router state poisoned");
            state.record_cache(
                &self.model,
                &self.actual_route,
                &self.routing_text,
                self.max_records,
            );
        }
        self.metrics.record_cache_committed(&self.actual_route);
        if self.selection_reason == "cache" {
            self.metrics
                .affinity_success_total
                .with_label_values(&[&self.actual_route])
                .inc();
            if self.initial_route != self.actual_route {
                self.metrics
                    .affinity_retarget_total
                    .with_label_values(&[&self.initial_route, &self.actual_route])
                    .inc();
            }
        }
        self.record_resolved = true;
        self.record_committed = true;
    }

    pub(super) fn skip(&mut self, reason: &'static str) {
        if self.record_resolved {
            return;
        }
        self.metrics.record_cache_skipped(reason);
        self.record_resolved = true;
    }

    pub(super) fn is_committed(&self) -> bool {
        self.record_committed
    }

    pub(super) fn record_usage(&mut self, usage: Option<&Value>) {
        if self.usage_resolved {
            return;
        }
        self.usage_resolved = true;
        let Some(usage) = usage else {
            self.metrics.record_usage_skipped("no_usage");
            return;
        };
        let Some(prompt_tokens) = usage_prompt_tokens(usage) else {
            self.metrics.record_usage_skipped("prompt_tokens_missing");
            return;
        };
        self.metrics
            .cache_prompt_tokens_total
            .with_label_values(&[&self.actual_route, self.selection_reason])
            .inc_by(prompt_tokens);
        let Some(cached_tokens) = usage_cached_tokens(usage) else {
            self.metrics.record_usage_skipped("cached_tokens_missing");
            return;
        };
        self.metrics
            .cache_cached_tokens_total
            .with_label_values(&[&self.actual_route, self.selection_reason])
            .inc_by(cached_tokens);
    }
}

impl Drop for CacheObservation {
    fn drop(&mut self) {
        if !self.record_resolved {
            self.metrics.record_cache_skipped("request_dropped");
            self.record_resolved = true;
        }
        if !self.usage_resolved && self.record_committed {
            self.metrics.record_usage_skipped("no_usage");
            self.usage_resolved = true;
        }
    }
}

impl RouterBackend {
    pub fn new(
        config: &MiddlewareConfig,
        upstream_config: Arc<UpstreamConfigManager>,
        runtime_config_path: Option<PathBuf>,
    ) -> Result<Self, String> {
        if config
            .public_model
            .as_deref()
            .is_some_and(|model| model.trim().is_empty())
        {
            return Err("middleware.public_model must not be empty".to_string());
        }
        let config = Arc::new(RouterConfigStore::load(config, runtime_config_path)?);
        let state = Arc::new(Mutex::new(RouterState::default()));
        let metrics = RouterMetrics::new()?;
        let metrics_notify = Arc::new(Notify::new());
        spawn_metrics_poller(
            config.clone(),
            upstream_config.clone(),
            state.clone(),
            metrics_notify.clone(),
        );
        Ok(Self {
            upstream_config,
            config,
            state,
            metrics,
            metrics_notify,
        })
    }

    pub(super) fn patch_config(
        &self,
        patch: RouterConfigPatch,
    ) -> Result<(), RouterConfigUpdateError> {
        let config = self.config.patch(patch)?;
        tracing::info!(
            balance_abs_threshold = config.balance_abs_threshold,
            max_forward_candidates = config.max_forward_candidates,
            metrics_poll_ms = config.metrics_poll_ms,
            metrics_path = %config.metrics_path,
            "router runtime config updated"
        );
        self.metrics_notify.notify_waiters();
        Ok(())
    }

    fn public_model(
        &self,
        config: &MiddlewareConfig,
        snapshot: &UpstreamConfigSnapshot,
    ) -> Result<Option<String>, String> {
        if let Some(model) = config.public_model.as_deref() {
            return Ok(Some(model.trim().to_string()));
        }
        let mut models = BTreeSet::new();
        for upstream in &snapshot.upstreams {
            if !upstream.enabled {
                continue;
            }
            for public_model in upstream.models.keys() {
                models.insert(public_model.clone());
            }
        }
        match models.len() {
            0 => Ok(None),
            1 => Ok(models.into_iter().next()),
            _ => Err(
                "router middleware requires exactly one public model; set middleware.public_model \
                 or remove extra public models from upstream config"
                    .to_string(),
            ),
        }
    }

    fn model_routes(&self, config: &MiddlewareConfig, model: &str) -> Vec<RouterRoute> {
        let snapshot = self.upstream_config.snapshot();
        let mut routes = Vec::new();
        for upstream in snapshot.upstreams {
            if upstream.enabled && upstream.models.contains_key(model) {
                routes.push(route_from_upstream(&upstream, model, config));
            }
        }
        routes
    }

    fn ordered_routes(
        &self,
        config: &MiddlewareConfig,
        public_model: &str,
        input: &CompletionInput,
    ) -> (Vec<RouterRoute>, Option<RouteSelection>, usize, String) {
        let tier = Self::request_tier(config, input);
        let requested_model = input.params.get("model").and_then(Value::as_str);
        if requested_model != Some(public_model) {
            return (Vec::new(), None, 0, String::new());
        }

        let mut routes = self.model_routes(config, public_model);
        let configured_count = routes.len();
        let routing_text = bounded_routing_text(&input.params, input.endpoint);
        let selection = {
            let mut state = self.state.lock().expect("router state poisoned");
            let selected = state.select_observed(
                public_model,
                &routing_text,
                &routes,
                config,
                tier,
                &self.metrics,
            );
            selected.map(|selected| {
                // Selection and the local reservation are one transaction. A
                // second dispatcher must observe this request before it can
                // consume the same PIG capacity snapshot.
                // Capacity-full routes remain bounded fallbacks. This lets PIG
                // observe real demand and prevents one stale "selectable"
                // snapshot from hiding a sibling that can still admit.
                let candidate_route_ids = routes
                    .iter()
                    .filter(|route| !state.route_pressure(route, config, tier).circuit_open)
                    .map(|route| route.route_id.clone())
                    .collect::<HashSet<_>>();
                let load_order = state.load_order(&routes, config, tier);
                let pressure_order_keys = routes
                    .iter()
                    .filter(|route| candidate_route_ids.contains(&route.route_id))
                    .map(|route| {
                        (
                            route.route_id.clone(),
                            state.route_order_key(route, config, tier, load_order),
                        )
                    })
                    .collect::<HashMap<_, _>>();
                state.mark_started(&selected);
                (selected, candidate_route_ids, pressure_order_keys)
            })
        };
        let Some((selected, candidate_route_ids, pressure_order_keys)) = selection else {
            return (Vec::new(), None, configured_count, routing_text);
        };
        routes.retain(|route| candidate_route_ids.contains(&route.route_id));
        routes.sort_by(|a, b| {
            if a.route_id == selected.route_id {
                return std::cmp::Ordering::Less;
            }
            if b.route_id == selected.route_id {
                return std::cmp::Ordering::Greater;
            }
            if let (Some(a_key), Some(b_key)) = (
                pressure_order_keys.get(&a.route_id),
                pressure_order_keys.get(&b.route_id),
            ) {
                return a_key.cmp(b_key);
            }
            a.route_id.cmp(&b.route_id)
        });
        limit_forward_routes(
            &mut routes,
            config
                .max_forward_candidates
                .min(ROUTER_FORWARD_ROUND_LIMIT),
        );
        (routes, Some(selected), configured_count, routing_text)
    }

    pub(super) fn metrics_body(&self) -> Result<Vec<u8>, prometheus::Error> {
        let state = self.state.lock().expect("router state poisoned");
        self.metrics.refresh_runtime(&state);
        drop(state);
        self.metrics.render()
    }

    pub(super) fn admin_snapshot_value(&self) -> Value {
        let config = self.config.snapshot();
        let upstream_snapshot = self.upstream_config.snapshot();
        let public_model = self.public_model(&config, &upstream_snapshot);
        let state = self.state.lock().expect("router state poisoned");
        let mut routes = Vec::new();
        for upstream in &upstream_snapshot.upstreams {
            for model in upstream.models.keys() {
                let route_id = format!("{}:{model}", upstream.name);
                let stats = state.stats.get(&route_id).cloned().unwrap_or_default();
                let cache_stats = state.cache_index.route_stats(model, &route_id);
                let route = route_from_upstream(upstream, model, &config);
                let pressure = state.route_pressure(&route, &config, UserTier::Basic);
                let circuit = state.route_circuit_admin_json(&route_id);
                routes.push(json!({
                    "route_id": route_id,
                    "enabled": upstream.enabled,
                    "upstream_name": upstream.name,
                    "public_model": model,
                    "provider": provider_name(upstream.provider),
                    "running": stats.running,
                    "processed": stats.processed,
                    "upstream_attempts": stats.upstream_attempts,
                    "upstream_429": stats.upstream_429,
                    "selected_by_cache": stats.selected_by_cache,
                    "selected_by_load": stats.selected_by_load,
                    "selected_by_order": stats.selected_by_order,
                    "cache_rejected_by_pressure": stats.cache_rejected_by_pressure,
                    "pressure_passthrough": stats.pressure_passthrough,
                    "cache_records": cache_stats.records,
                    "cache_chars": cache_stats.chars,
                    "selectable": !pressure.blocked
                        && !pressure.capacity_rejected
                        && !pressure.circuit_open,
                    "pressure_passthrough_eligible": (pressure.blocked
                        || pressure.capacity_rejected)
                        && !pressure.circuit_open,
                    "capacity_rejected_this_epoch": pressure.capacity_rejected,
                    "circuit_open": pressure.circuit_open,
                    "metrics_circuit_open": pressure.metrics_circuit_open,
                    "circuit": circuit,
                    "effective_running": pressure.effective_running,
                    "pending_reservations": pressure.pending_reservations,
                    "unreconciled_dispatches": pressure.unreconciled_dispatches,
                    "fullness_milli": pressure.fullness_milli,
                    "bearer_token_configured": upstream.bearer_token_configured,
                    "pig_metrics": state.metrics_admin_json(&upstream.name, &config),
                }));
            }
        }
        routes.sort_by(|a, b| {
            a.get("route_id")
                .and_then(Value::as_str)
                .cmp(&b.get("route_id").and_then(Value::as_str))
        });
        json!({
            "mode": "router",
            "purpose": "single_model_multi_backend_cache_and_load_aware_routing",
            "config": config.as_ref(),
            "runtime_config": self.config.metadata_json(),
            "metrics_epoch": state.metrics_epoch,
            "routing_text_max_chars": MAX_ROUTING_HISTORY_CHARS,
            "cache_index": state.cache_index_admin_json(),
            "public_model": public_model.as_ref().ok().and_then(Clone::clone),
            "config_error": public_model.err(),
            "upstream_config_digest": upstream_snapshot.config_digest,
            "routes": routes,
        })
    }

    pub(super) fn upstream_status_code(&self) -> u8 {
        let config = self.config.snapshot();
        let upstream_snapshot = self.upstream_config.snapshot();
        let public_model = match self.public_model(&config, &upstream_snapshot) {
            Ok(Some(model)) => model,
            Ok(None) | Err(_) => return UPSTREAM_STATUS_RED,
        };
        let routes = upstream_snapshot
            .upstreams
            .iter()
            .filter(|upstream| upstream.enabled && upstream.models.contains_key(&public_model))
            .map(|upstream| route_from_upstream(upstream, &public_model, &config))
            .collect::<Vec<_>>();
        let state = self.state.lock().expect("router state poisoned");
        state.upstream_status_code(&routes, &config, UserTier::Basic)
    }

    pub async fn handle_catalog(&self, v1_path: &str) -> Response {
        if v1_path != "/v1/models" {
            return errors::error_response(
                Surface::Openai,
                404,
                "not_found",
                "router middleware only serves /v1/models catalog",
                None,
            );
        }
        let config = self.config.snapshot();
        let snapshot = self.upstream_config.snapshot();
        let public_model = match self.public_model(&config, &snapshot) {
            Ok(model) => model,
            Err(err) => {
                return errors::error_response(
                    Surface::Openai,
                    503,
                    "service_unavailable",
                    &err,
                    None,
                );
            }
        };
        let data = public_model
            .into_iter()
            .map(|id| {
                json!({
                    "id": id,
                    "object": "model",
                    "owned_by": "phala",
                })
            })
            .collect::<Vec<_>>();
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        (
            StatusCode::OK,
            headers,
            serde_json::to_vec(&json!({"object": "list", "data": data})).unwrap_or_default(),
        )
            .into_response()
    }

    pub async fn handle_completion(
        &self,
        service: &AciService,
        control: Option<ControlClient>,
        input: CompletionInput,
    ) -> Response {
        let config = self.config.snapshot();
        let snapshot = self.upstream_config.snapshot();
        let requested_model = input
            .params
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_string);
        let (public_model, requested_public_model) = match self.public_model(&config, &snapshot) {
            Ok(Some(model)) => {
                let requested_public_model = requested_model.as_deref() == Some(model.as_str());
                (model, requested_public_model)
            }
            // With no enabled upstream, a single-model Router cannot derive the
            // model catalog. Treat the requested model as temporarily
            // unavailable so clients see the same capacity 429 they would get
            // from PIG, instead of a malformed/unroutable request error.
            Ok(None) => (
                requested_model.clone().unwrap_or_default(),
                requested_model.is_some(),
            ),
            Err(err) => {
                return errors::error_response(
                    input.surface,
                    503,
                    "service_unavailable",
                    &err,
                    Some(&input.request_id),
                );
            }
        };
        let mut input = input;
        if !requested_public_model {
            return completion::model_not_found(service, &input, requested_model.as_deref());
        }
        let (routes, selected, configured_count, routing_text) =
            self.ordered_routes(&config, &public_model, &input);
        let user_tier = Self::request_tier(&config, &input);
        if !config.trusted_user_tier_header {
            input.user_tier = None;
        }
        if selected.is_none() {
            if !routing_text.is_empty() && config.max_history_per_route > 0 {
                self.metrics.record_cache_skipped("router_reject");
            }
            tracing::info!(
                public_model,
                user_tier = user_tier.as_str(),
                configured_count,
                "router middleware rejected request because no observable upstream has capacity"
            );
            return completion::rate_limited_by_router(
                service,
                &input,
                "Rate limit exceeded. Please retry after some time.",
            );
        }
        let route_in_flight = selected.as_ref().map(|selection| {
            RouteInFlight::from_reserved(
                self.state.clone(),
                self.metrics.clone(),
                selection,
                public_model.clone(),
                routing_text,
                config.max_history_per_route,
            )
        });
        let retry_planner = selected.as_ref().map(|_| RetryPlanner {
            upstream_config: self.upstream_config.clone(),
            config: self.config.clone(),
            state: self.state.clone(),
            metrics: self.metrics.clone(),
            metrics_notify: self.metrics_notify.clone(),
            public_model: public_model.clone(),
            tier: user_tier,
        });
        if let Some(selection) = selected.as_ref() {
            tracing::debug!(
                public_model,
                user_tier = user_tier.as_str(),
                selected_route = %selection.route_id,
                reason = selection.reason,
                cache_match_rate = selection.cache_match_rate,
                running_at_select = selection.running_at_select,
                candidate_count = routes.len(),
                "router middleware selected upstream route"
            );
        } else {
            tracing::debug!(public_model, "router middleware found no route");
        }

        self.metrics.record_forward_candidate_count(routes.len());

        completion::run(
            service,
            config.sse_keepalive_ms,
            control,
            config.pricing.clone(),
            input,
            completion::CompletionRoutePlan {
                candidates: routes.into_iter().map(|route| route.candidate).collect(),
                retry_planner,
                route_in_flight,
            },
        )
        .await
    }

    fn request_tier(config: &MiddlewareConfig, input: &CompletionInput) -> UserTier {
        if config.trusted_user_tier_header {
            UserTier::from_header(input.user_tier.as_deref())
        } else {
            UserTier::Basic
        }
    }
}

impl RetryPlanner {
    pub(super) async fn next_round(
        &self,
        attempted_route_ids: &HashSet<String>,
        route_in_flight: &mut RouteInFlight,
    ) -> Vec<RouteCandidate> {
        let initial_config = self.config.snapshot();
        if initial_config.metrics_poll_ms == 0
            || attempted_route_ids.len() >= initial_config.max_forward_candidates
        {
            return Vec::new();
        }
        let initial_epoch = self
            .state
            .lock()
            .expect("router state poisoned")
            .metrics_epoch;
        let wait = Duration::from_millis(
            initial_config
                .metrics_poll_ms
                .saturating_add(100)
                .clamp(100, 1_200),
        );
        let deadline = Instant::now() + wait;
        loop {
            let notified = self.metrics_notify.notified();
            if self
                .state
                .lock()
                .expect("router state poisoned")
                .metrics_epoch
                > initial_epoch
            {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, notified).await.is_err() {
                self.metrics.record_capacity_retry("epoch_timeout");
                tracing::info!(
                    initial_metrics_epoch = initial_epoch,
                    wait_ms = wait.as_millis() as u64,
                    "router capacity retry ended before a fresh metrics window"
                );
                return Vec::new();
            }
        }

        let config = self.config.snapshot();
        let remaining_budget = config
            .max_forward_candidates
            .saturating_sub(attempted_route_ids.len())
            .min(ROUTER_FORWARD_ROUND_LIMIT);
        if remaining_budget == 0 {
            return Vec::new();
        }
        let snapshot = self.upstream_config.snapshot();
        let routes = snapshot
            .upstreams
            .iter()
            .filter(|upstream| upstream.enabled && upstream.models.contains_key(&self.public_model))
            .map(|upstream| route_from_upstream(upstream, &self.public_model, &config))
            .collect::<Vec<_>>();
        let mut state = self.state.lock().expect("router state poisoned");
        let routes = state.retry_routes(
            &routes,
            &config,
            self.tier,
            attempted_route_ids,
            remaining_budget,
        );
        if routes.is_empty() {
            self.metrics.record_capacity_retry("no_candidate");
            tracing::info!(
                attempted_count = attempted_route_ids.len(),
                metrics_epoch = state.metrics_epoch,
                "router capacity retry found no hard-eligible candidate"
            );
            return Vec::new();
        }
        if let Some(first) = routes.first() {
            state.record_retry_selection(&first.route_id);
            route_in_flight.move_reservation_locked(&first.route_id, false, &mut state);
        }
        self.metrics.record_capacity_retry("started");
        self.metrics.record_forward_candidate_count(routes.len());
        tracing::info!(
            candidate_count = routes.len(),
            attempted_count = attempted_route_ids.len(),
            metrics_epoch = state.metrics_epoch,
            "router retrying capacity rejection after a fresh metrics window"
        );
        routes.into_iter().map(|route| route.candidate).collect()
    }

    pub(super) fn record_outcome(&self, outcome: &'static str) {
        self.metrics.record_capacity_retry(outcome);
    }
}

impl RouteInFlight {
    fn from_reserved(
        state: Arc<Mutex<RouterState>>,
        metrics: RouterMetrics,
        selection: &RouteSelection,
        model: String,
        routing_text: String,
        max_records: usize,
    ) -> Self {
        let cache_observation = CacheObservation::new(
            state.clone(),
            metrics.clone(),
            model,
            routing_text,
            max_records,
            selection,
        );
        Self {
            route_id: Some(selection.route_id.clone()),
            pending_dispatch: true,
            state,
            metrics,
            candidates_considered: 0,
            cache_observation,
        }
    }

    pub(super) fn retarget(&mut self, route_id: &str) {
        self.move_reservation(route_id, false);
        if let Some(observation) = self.cache_observation.as_mut() {
            observation.retarget(route_id);
        }
    }

    pub(super) fn commit_cache(&mut self, usage: Option<&Value>) {
        if let Some(observation) = self.cache_observation.as_mut() {
            observation.commit();
            observation.record_usage(usage);
        }
    }

    pub(super) fn skip_cache(&mut self, reason: &'static str) {
        if let Some(observation) = self.cache_observation.as_mut() {
            observation.skip(reason);
        }
    }

    pub(super) fn take_cache_observation(&mut self) -> Option<CacheObservation> {
        self.cache_observation.take()
    }

    fn move_reservation(&mut self, route_id: &str, record_dispatch: bool) {
        let state = self.state.clone();
        let mut state = state.lock().expect("router state poisoned");
        self.move_reservation_locked(route_id, record_dispatch, &mut state);
    }

    fn move_reservation_locked(
        &mut self,
        route_id: &str,
        record_dispatch: bool,
        state: &mut RouterState,
    ) {
        if self.route_id.as_deref() == Some(route_id) {
            if record_dispatch {
                if self.pending_dispatch {
                    state.release_reservation_for_route(route_id);
                    self.pending_dispatch = false;
                }
                state.record_dispatch_for_route(route_id);
            }
            return;
        }
        let move_pending_dispatch = self.pending_dispatch;
        if let Some(previous) = self.route_id.replace(route_id.to_string()) {
            state.decrement_running(&previous);
            if move_pending_dispatch {
                state.release_reservation_for_route(&previous);
                self.pending_dispatch = false;
            }
        }
        let stats = state.stats.entry(route_id.to_string()).or_default();
        stats.running = stats.running.saturating_add(1);
        if record_dispatch {
            state.record_dispatch_for_route(route_id);
        } else if move_pending_dispatch {
            state.reserve_for_route(route_id);
            self.pending_dispatch = true;
        }
    }
}

impl MiddlewareAttemptObserver for RouteInFlight {
    fn candidate_considered(&mut self, _route_id: &str) {
        self.candidates_considered = self.candidates_considered.saturating_add(1);
    }

    fn attempt_started(&mut self, route_id: &str) {
        self.move_reservation(route_id, true);
    }

    fn attempt_response(&mut self, route_id: &str, status: u16, route_failure: bool) {
        let recorded = {
            let mut state = self.state.lock().expect("router state poisoned");
            state.record_attempt_response(route_id, status, route_failure, &self.metrics)
        };
        if let Some((upstream_429_total, unreconciled_dispatches)) = recorded {
            tracing::info!(
                route = route_id,
                upstream_status = status,
                upstream_429_total,
                unreconciled_dispatches,
                "router middleware observed upstream capacity rejection"
            );
        }
    }

    fn attempt_failed(&mut self, route_id: &str, failure: MiddlewareAttemptFailure) {
        let mut state = self.state.lock().expect("router state poisoned");
        state.record_attempt_failure(route_id, failure, &self.metrics);
    }
}

impl Drop for RouteInFlight {
    fn drop(&mut self) {
        self.metrics
            .record_forward_attempt_count(self.candidates_considered);
        if let Some(route_id) = self.route_id.take() {
            let mut state = self.state.lock().expect("router state poisoned");
            state.decrement_running(&route_id);
            if self.pending_dispatch {
                state.release_reservation_for_route(&route_id);
                self.pending_dispatch = false;
            }
        }
    }
}

impl RouterState {
    fn retry_routes(
        &self,
        routes: &[RouterRoute],
        config: &MiddlewareConfig,
        tier: UserTier,
        attempted_route_ids: &HashSet<String>,
        limit: usize,
    ) -> Vec<RouterRoute> {
        let hard_eligible = routes
            .iter()
            .filter(|route| !self.route_pressure(route, config, tier).circuit_open)
            .cloned()
            .collect::<Vec<_>>();
        let normally_selectable = hard_eligible
            .iter()
            .filter(|route| self.route_selectable(route, config, tier))
            .cloned()
            .collect::<Vec<_>>();
        let mut candidates = if normally_selectable.is_empty() {
            hard_eligible
        } else {
            normally_selectable
        };
        let load_order = self.load_order(&candidates, config, tier);
        candidates.sort_by(|a, b| {
            let a_pressure = self.route_pressure(a, config, tier);
            let b_pressure = self.route_pressure(b, config, tier);
            let a_class = (
                u8::from(a_pressure.blocked || a_pressure.capacity_rejected),
                u8::from(a_pressure.metrics_error || a_pressure.metrics_circuit_open),
                u8::from(a_pressure.metrics_missing),
            );
            let b_class = (
                u8::from(b_pressure.blocked || b_pressure.capacity_rejected),
                u8::from(b_pressure.metrics_error || b_pressure.metrics_circuit_open),
                u8::from(b_pressure.metrics_missing),
            );
            a_class
                .cmp(&b_class)
                .then_with(|| {
                    attempted_route_ids
                        .contains(&a.route_id)
                        .cmp(&attempted_route_ids.contains(&b.route_id))
                })
                .then_with(|| {
                    self.route_order_key(a, config, tier, load_order)
                        .cmp(&self.route_order_key(b, config, tier, load_order))
                })
        });
        candidates.truncate(limit.max(1));
        candidates
    }

    fn record_retry_selection(&mut self, route_id: &str) {
        let stats = self.stats.entry(route_id.to_string()).or_default();
        stats.processed = stats.processed.saturating_add(1);
        stats.selected_by_load = stats.selected_by_load.saturating_add(1);
    }

    fn mark_started(&mut self, selection: &RouteSelection) {
        let stats = self.stats.entry(selection.route_id.clone()).or_default();
        stats.running = stats.running.saturating_add(1);
        stats.processed = stats.processed.saturating_add(1);
        match selection.reason {
            "cache" => {
                stats.selected_by_cache = stats.selected_by_cache.saturating_add(1);
            }
            "single" => {
                stats.selected_by_order = stats.selected_by_order.saturating_add(1);
            }
            PIG_PRESSURE_PASSTHROUGH_REASON => {
                stats.pressure_passthrough = stats.pressure_passthrough.saturating_add(1);
            }
            _ => {
                stats.selected_by_load = stats.selected_by_load.saturating_add(1);
            }
        }
        self.reserve_for_route(&selection.route_id);
    }

    fn reserve_for_route(&mut self, route_id: &str) {
        if let Some((upstream_name, _)) = route_id.split_once(':') {
            self.dispatch_ledgers
                .entry(upstream_name.to_string())
                .or_default()
                .reserve();
        }
    }

    fn release_reservation_for_route(&mut self, route_id: &str) {
        if let Some((upstream_name, _)) = route_id.split_once(':') {
            self.dispatch_ledgers
                .entry(upstream_name.to_string())
                .or_default()
                .release_reservation();
        }
    }

    fn record_dispatch_for_route(&mut self, route_id: &str) {
        let stats = self.stats.entry(route_id.to_string()).or_default();
        stats.upstream_attempts = stats.upstream_attempts.saturating_add(1);
        if let Some((upstream_name, _)) = route_id.split_once(':') {
            self.dispatch_ledgers
                .entry(upstream_name.to_string())
                .or_default()
                .record_dispatch();
        }
    }

    fn record_attempt_response(
        &mut self,
        route_id: &str,
        status: u16,
        route_failure: bool,
        metrics: &RouterMetrics,
    ) -> Option<(u64, usize)> {
        if route_failure {
            self.record_route_failure(route_id, "upstream_status", metrics);
        } else {
            self.route_circuits
                .entry(route_id.to_string())
                .or_default()
                .record_success();
        }
        if status != 429 {
            self.stats
                .entry(route_id.to_string())
                .or_default()
                .last_capacity_rejection_epoch = None;
            return None;
        }
        let upstream_429_total = {
            let stats = self.stats.entry(route_id.to_string()).or_default();
            stats.upstream_429 = stats.upstream_429.saturating_add(1);
            stats.last_capacity_rejection_epoch = Some(self.metrics_epoch);
            stats.upstream_429
        };
        let unreconciled = route_id
            .split_once(':')
            .and_then(|(upstream_name, _)| self.dispatch_ledgers.get(upstream_name))
            .copied()
            .unwrap_or_default()
            .unreconciled();
        Some((upstream_429_total, unreconciled))
    }

    fn record_attempt_failure(
        &mut self,
        route_id: &str,
        failure: MiddlewareAttemptFailure,
        metrics: &RouterMetrics,
    ) {
        let reason = match failure {
            MiddlewareAttemptFailure::Routing => "routing",
            MiddlewareAttemptFailure::Verification => "verification",
            MiddlewareAttemptFailure::Transport => "transport",
        };
        self.record_route_failure(route_id, reason, metrics);
    }

    fn record_route_failure(
        &mut self,
        route_id: &str,
        reason: &'static str,
        metrics: &RouterMetrics,
    ) {
        let circuit = self.route_circuits.entry(route_id.to_string()).or_default();
        if circuit.record_failure(Instant::now()) {
            metrics.record_circuit_open(route_id, reason);
            tracing::warn!(
                route = route_id,
                reason,
                failures = circuit.consecutive_failures,
                open_ms = ROUTE_CIRCUIT_OPEN_DURATION.as_millis() as u64,
                "router middleware opened route circuit"
            );
        }
    }

    fn route_circuit_open(&self, route_id: &str) -> bool {
        self.route_circuits
            .get(route_id)
            .is_some_and(|circuit| circuit.is_open(Instant::now()))
    }

    fn route_circuit_admin_json(&self, route_id: &str) -> Value {
        let now = Instant::now();
        let circuit = self
            .route_circuits
            .get(route_id)
            .copied()
            .unwrap_or_default();
        json!({
            "open": circuit.is_open(now),
            "remaining_ms": circuit.remaining_ms(now),
            "consecutive_failures": circuit.consecutive_failures,
            "opens": circuit.opens,
        })
    }

    fn decrement_running(&mut self, route_id: &str) {
        if let Some(stats) = self.stats.get_mut(route_id) {
            stats.running = stats.running.saturating_sub(1);
        }
    }

    fn least_loaded<'a>(
        &self,
        routes: &'a [RouterRoute],
        config: &MiddlewareConfig,
        tier: UserTier,
    ) -> Option<&'a RouterRoute> {
        let load_order = self.load_order(routes, config, tier);
        routes.iter().min_by(|a, b| {
            self.route_order_key(a, config, tier, load_order)
                .cmp(&self.route_order_key(b, config, tier, load_order))
        })
    }

    fn load_order(
        &self,
        routes: &[RouterRoute],
        config: &MiddlewareConfig,
        tier: UserTier,
    ) -> LoadOrder {
        if routes.iter().any(|route| {
            self.upstream_metrics
                .get(&route.upstream_name)
                .is_some_and(|metrics| {
                    !metrics.is_stale(config)
                        && metrics.ok
                        && metrics.request_aware_capacity_is_open()
                        && self.route_selectable(route, config, tier)
                })
        }) {
            // A request-aware PIG deliberately publishes global_limit=0 while
            // open: the scalar limit is neutral, not "zero percent full". In
            // a rolling mixed-version pool, compare live request counts first
            // so that this neutral sentinel cannot attract all traffic.
            LoadOrder::Running
        } else {
            LoadOrder::CapacityNormalized
        }
    }

    fn route_order_key(
        &self,
        route: &RouterRoute,
        config: &MiddlewareConfig,
        tier: UserTier,
        load_order: LoadOrder,
    ) -> RouteOrderKey {
        let pressure = self.route_pressure(route, config, tier);
        let capacity_unavailable = pressure.blocked || pressure.capacity_rejected;
        let (primary_load, secondary_load) = match load_order {
            LoadOrder::CapacityNormalized => {
                (pressure.fullness_milli, pressure.effective_running as u64)
            }
            LoadOrder::Running => (pressure.effective_running as u64, pressure.fullness_milli),
        };
        (
            u8::from(pressure.circuit_open),
            u8::from(capacity_unavailable),
            u8::from(pressure.capacity_rejected),
            u8::from(pressure.metrics_error || pressure.metrics_circuit_open),
            u8::from(pressure.metrics_missing),
            pressure.waiting,
            primary_load,
            secondary_load,
            pressure.processed,
            route.route_id.clone(),
        )
    }

    fn route_selectable(
        &self,
        route: &RouterRoute,
        config: &MiddlewareConfig,
        tier: UserTier,
    ) -> bool {
        let pressure = self.route_pressure(route, config, tier);
        !pressure.blocked && !pressure.capacity_rejected && !pressure.circuit_open
    }

    fn route_pressure(
        &self,
        route: &RouterRoute,
        config: &MiddlewareConfig,
        tier: UserTier,
    ) -> RoutePressure {
        let stats = self.stats.get(&route.route_id).cloned().unwrap_or_default();
        let local_running = stats.running;
        let ledger = self
            .dispatch_ledgers
            .get(&route.upstream_name)
            .copied()
            .unwrap_or_default();
        let pending_reservations = ledger.reservations;
        let unreconciled_dispatches = ledger.unreconciled();
        let local_projection = pending_reservations.saturating_add(unreconciled_dispatches);
        let fallback_effective_running = local_running.max(local_projection);
        let request_circuit_open = self.route_circuit_open(&route.route_id);
        let capacity_rejected = stats.last_capacity_rejection_epoch == Some(self.metrics_epoch);
        let Some(metrics) = self.upstream_metrics.get(&route.upstream_name) else {
            return RoutePressure {
                blocked: false,
                circuit_open: request_circuit_open,
                capacity_rejected,
                metrics_circuit_open: false,
                metrics_missing: true,
                metrics_error: false,
                waiting: 0,
                fullness_milli: 0,
                effective_running: fallback_effective_running,
                pending_reservations,
                unreconciled_dispatches,
                processed: stats.processed,
            };
        };
        let circuit_open = request_circuit_open;
        let metrics_circuit_open = metrics.metrics_circuit_open();
        if metrics.is_stale(config) {
            return RoutePressure {
                blocked: false,
                circuit_open,
                capacity_rejected,
                metrics_circuit_open,
                metrics_missing: true,
                metrics_error: !metrics.ok,
                waiting: 0,
                fullness_milli: 0,
                effective_running: fallback_effective_running,
                pending_reservations,
                unreconciled_dispatches,
                processed: stats.processed,
            };
        }
        if metrics.updated_at.is_none() {
            return RoutePressure {
                blocked: false,
                circuit_open,
                capacity_rejected,
                metrics_circuit_open,
                metrics_missing: true,
                metrics_error: !metrics.ok,
                waiting: 0,
                fullness_milli: 0,
                effective_running: fallback_effective_running,
                pending_reservations,
                unreconciled_dispatches,
                processed: stats.processed,
            };
        }

        let observed_running = metrics.observed_running.unwrap_or(0.0).max(0.0);
        if metrics.request_aware_projection_is_inconsistent() {
            return RoutePressure {
                blocked: false,
                circuit_open,
                capacity_rejected,
                metrics_circuit_open,
                metrics_missing: true,
                metrics_error: true,
                waiting: 0,
                fullness_milli: 0,
                effective_running: fallback_effective_running.max(observed_running.ceil() as usize),
                pending_reservations,
                unreconciled_dispatches,
                processed: stats.processed,
            };
        }
        let observed_waiting = metrics.observed_waiting.unwrap_or(0.0).max(0.0);
        let projected_running = (observed_running.ceil() as usize)
            .saturating_add(pending_reservations)
            .saturating_add(unreconciled_dispatches);
        let effective_running = local_running.max(projected_running);
        let unreconciled_local = (local_running as f64 - observed_running)
            .max(pending_reservations.saturating_add(unreconciled_dispatches) as f64)
            .max(0.0);
        let global_fullness = ratio_milli(effective_running as f64, metrics.global_limit);
        let tier_fullness = match (metrics.uses_request_aware_capacity(), tier) {
            // Request-aware PIG owns per-request admission and deliberately no
            // longer exposes tier capacity as Router authority.
            (true, _) | (false, UserTier::Premium) => global_fullness,
            (false, UserTier::Basic) => global_fullness.max(ratio_milli(
                metrics.basic_inflight.unwrap_or(0.0).max(0.0) + unreconciled_local,
                metrics.basic_limit,
            )),
        };
        RoutePressure {
            blocked: observed_waiting > 0.0 || tier_fullness >= 1_000,
            circuit_open,
            capacity_rejected,
            metrics_circuit_open,
            metrics_missing: false,
            metrics_error: !metrics.ok,
            waiting: observed_waiting.ceil() as u64,
            fullness_milli: tier_fullness,
            effective_running,
            pending_reservations,
            unreconciled_dispatches,
            processed: stats.processed,
        }
    }

    fn upstream_status_code(
        &self,
        routes: &[RouterRoute],
        config: &MiddlewareConfig,
        tier: UserTier,
    ) -> u8 {
        if routes.is_empty() {
            return UPSTREAM_STATUS_RED;
        }
        let mut saw_yellow = false;
        for route in routes {
            match self.route_status_code(route, config, tier) {
                UPSTREAM_STATUS_GREEN => return UPSTREAM_STATUS_GREEN,
                UPSTREAM_STATUS_YELLOW => saw_yellow = true,
                _ => {}
            }
        }
        if saw_yellow {
            UPSTREAM_STATUS_YELLOW
        } else {
            UPSTREAM_STATUS_RED
        }
    }

    fn route_status_code(
        &self,
        route: &RouterRoute,
        config: &MiddlewareConfig,
        tier: UserTier,
    ) -> u8 {
        let pressure = self.route_pressure(route, config, tier);
        if pressure.circuit_open {
            return UPSTREAM_STATUS_RED;
        }
        if pressure.metrics_error || pressure.metrics_circuit_open {
            return UPSTREAM_STATUS_YELLOW;
        }
        if pressure.blocked
            || pressure.capacity_rejected
            || pressure.waiting > 0
            || pressure.fullness_milli >= 1_000
        {
            return UPSTREAM_STATUS_YELLOW;
        }
        if pressure.metrics_missing || pressure.fullness_milli >= 850 {
            return UPSTREAM_STATUS_YELLOW;
        }
        let Some(metrics) = self.upstream_metrics.get(&route.upstream_name) else {
            return UPSTREAM_STATUS_YELLOW;
        };
        let request_aware = metrics.uses_request_aware_capacity();
        let global_limit_known =
            request_aware || metrics.global_limit.is_some_and(|limit| limit > 0.0);
        let tier_limit_known = match tier {
            UserTier::Premium => true,
            UserTier::Basic => {
                request_aware || metrics.basic_limit.is_some_and(|limit| limit > 0.0)
            }
        };
        if !global_limit_known || !tier_limit_known {
            return UPSTREAM_STATUS_YELLOW;
        }
        UPSTREAM_STATUS_GREEN
    }

    fn select_observed(
        &mut self,
        model: &str,
        text: &str,
        routes: &[RouterRoute],
        config: &MiddlewareConfig,
        tier: UserTier,
        metrics: &RouterMetrics,
    ) -> Option<RouteSelection> {
        if routes.is_empty() {
            return None;
        }
        for route in routes {
            self.stats.entry(route.route_id.clone()).or_default();
        }
        let active_routes = routes
            .iter()
            .map(|route| route.route_id.clone())
            .collect::<HashSet<_>>();
        self.cache_index.retain_model_routes(model, &active_routes);
        let selectable_routes = routes
            .iter()
            .filter(|route| self.route_selectable(route, config, tier))
            .cloned()
            .collect::<Vec<_>>();
        if selectable_routes.is_empty() {
            return self.select_pressure_passthrough(routes, config, tier);
        }
        if selectable_routes.len() == 1 {
            let route_id = selectable_routes[0].route_id.clone();
            let running_at_select = self.stats.get(&route_id).map_or(0, |s| s.running);
            if active_routes.len() > 1 && !text.is_empty() {
                if let Some(matched) = self.cache_index.match_prefix(model, text) {
                    let input_chars = matched.input_chars.max(1);
                    let rate = matched.matched_chars as f32 / input_chars as f32;
                    metrics.record_affinity_considered(
                        &matched.route_id,
                        rate,
                        matched.matched_chars,
                    );
                    if rate > config.cache_threshold
                        && matched.route_id != route_id
                        && active_routes.contains(&matched.route_id)
                    {
                        self.stats
                            .entry(matched.route_id)
                            .or_default()
                            .cache_rejected_by_pressure += 1;
                        metrics.record_affinity_rejected("pressure");
                    } else if rate <= config.cache_threshold {
                        metrics.record_affinity_rejected("below_threshold");
                    }
                }
            }
            return Some(RouteSelection {
                route_id,
                reason: if active_routes.len() == 1 {
                    "single"
                } else {
                    "least_running"
                },
                cache_match_rate: 0.0,
                running_at_select,
            });
        }

        let selected = if text.is_empty() {
            self.least_loaded(&selectable_routes, config, tier)
                .map(|route| {
                    let pressure = self.route_pressure(route, config, tier);
                    RouteSelection {
                        route_id: route.route_id.clone(),
                        reason: "no_text",
                        cache_match_rate: 0.0,
                        running_at_select: pressure.effective_running,
                    }
                })
        } else {
            self.select_cache_aware(model, text, &selectable_routes, config, tier, metrics)
        }?;
        Some(selected)
    }

    #[cfg(test)]
    fn select(
        &mut self,
        model: &str,
        text: &str,
        routes: &[RouterRoute],
        config: &MiddlewareConfig,
        tier: UserTier,
    ) -> Option<RouteSelection> {
        let metrics = RouterMetrics::new().expect("test router metrics");
        self.select_observed(model, text, routes, config, tier, &metrics)
    }

    fn select_pressure_passthrough(
        &self,
        routes: &[RouterRoute],
        config: &MiddlewareConfig,
        tier: UserTier,
    ) -> Option<RouteSelection> {
        let available = routes
            .iter()
            .filter(|route| !self.route_pressure(route, config, tier).circuit_open)
            .cloned()
            .collect::<Vec<_>>();
        let selected = self.least_loaded(&available, config, tier)?;
        let pressure = self.route_pressure(selected, config, tier);
        Some(RouteSelection {
            route_id: selected.route_id.clone(),
            reason: PIG_PRESSURE_PASSTHROUGH_REASON,
            cache_match_rate: 0.0,
            running_at_select: pressure.effective_running,
        })
    }

    fn select_cache_aware(
        &mut self,
        model: &str,
        text: &str,
        routes: &[RouterRoute],
        config: &MiddlewareConfig,
        tier: UserTier,
        metrics: &RouterMetrics,
    ) -> Option<RouteSelection> {
        let matched = self.cache_index.match_prefix(model, text);
        let input_chars = matched.as_ref().map_or_else(
            || text.chars().count().max(1),
            |matched| matched.input_chars,
        );
        let rate = matched.as_ref().map_or(0.0, |matched| {
            matched.matched_chars as f32 / input_chars as f32
        });
        let least = self.least_loaded(routes, config, tier)?;
        if let Some(matched) = matched {
            metrics.record_affinity_considered(&matched.route_id, rate, matched.matched_chars);
            if let Some(cache_route) = routes
                .iter()
                .find(|route| route.route_id == matched.route_id)
            {
                if rate > config.cache_threshold
                    && self.cache_route_is_acceptable(cache_route, least, config, tier)
                {
                    metrics.record_affinity_selected(&cache_route.route_id);
                    let pressure = self.route_pressure(cache_route, config, tier);
                    return Some(RouteSelection {
                        route_id: cache_route.route_id.clone(),
                        reason: "cache",
                        cache_match_rate: rate,
                        running_at_select: pressure.effective_running,
                    });
                }
                if rate > config.cache_threshold {
                    self.stats
                        .entry(cache_route.route_id.clone())
                        .or_default()
                        .cache_rejected_by_pressure += 1;
                    metrics.record_affinity_rejected("pressure");
                } else {
                    metrics.record_affinity_rejected("below_threshold");
                }
            } else if rate > config.cache_threshold {
                metrics.record_affinity_rejected("route_unavailable");
            } else {
                metrics.record_affinity_rejected("below_threshold");
            }
        }
        let pressure = self.route_pressure(least, config, tier);
        Some(RouteSelection {
            route_id: least.route_id.clone(),
            reason: "least_running",
            cache_match_rate: rate,
            running_at_select: pressure.effective_running,
        })
    }

    fn cache_route_is_acceptable(
        &self,
        cache_route: &RouterRoute,
        least_route: &RouterRoute,
        config: &MiddlewareConfig,
        tier: UserTier,
    ) -> bool {
        let cache = self.route_pressure(cache_route, config, tier);
        let least = self.route_pressure(least_route, config, tier);
        if cache.blocked && !least.blocked {
            return false;
        }
        if cache.metrics_missing && !least.metrics_missing {
            return false;
        }
        if cache.waiting > 0 && least.waiting == 0 {
            return false;
        }
        if cache.fullness_milli > least.fullness_milli.saturating_add(250) {
            return false;
        }
        let gap = cache
            .effective_running
            .saturating_sub(least.effective_running);
        if gap > config.balance_abs_threshold
            && (cache.effective_running as f32)
                > (least.effective_running as f32 * config.balance_rel_threshold)
        {
            return false;
        }
        true
    }

    fn record_cache(&mut self, model: &str, route_id: &str, text: &str, max_records: usize) {
        if text.is_empty() || max_records == 0 {
            return;
        }
        let text = limit_chars(text, MAX_ROUTING_HISTORY_CHARS);
        self.cache_index.record(model, route_id, &text, max_records);
    }

    fn update_upstream_metrics(&mut self, upstream_name: String, metrics: UpstreamMetrics) {
        self.upstream_metrics.insert(upstream_name, metrics);
    }

    fn update_upstream_metrics_from_poll(
        &mut self,
        upstream_name: String,
        mut metrics: UpstreamMetrics,
        dispatch_watermark: u64,
    ) {
        if metrics.ok {
            self.dispatch_ledgers
                .entry(upstream_name.clone())
                .or_default()
                .observe_successful_poll(dispatch_watermark);
            self.update_upstream_metrics(upstream_name, metrics);
            return;
        }
        if let Some(previous) = self.upstream_metrics.get_mut(&upstream_name) {
            previous.ok = false;
            previous.error = metrics.error.take();
            previous.last_error_at = metrics.last_error_at.or(Some(Instant::now()));
            previous.consecutive_errors = previous.consecutive_errors.saturating_add(1);
        } else {
            metrics.consecutive_errors = metrics.consecutive_errors.max(1);
            self.update_upstream_metrics(upstream_name, metrics);
        }
    }

    fn dispatch_watermark(&self, upstream_name: &str) -> u64 {
        self.dispatch_ledgers
            .get(upstream_name)
            .map_or(0, |ledger| ledger.started)
    }

    fn retain_upstream_metrics(&mut self, upstream_names: &HashSet<String>) {
        self.upstream_metrics
            .retain(|name, _| upstream_names.contains(name));
        self.dispatch_ledgers
            .retain(|name, _| upstream_names.contains(name));
        self.route_circuits.retain(|route_id, _| {
            route_id
                .split_once(':')
                .is_some_and(|(name, _)| upstream_names.contains(name))
        });
    }

    fn metrics_admin_json(&self, upstream_name: &str, config: &MiddlewareConfig) -> Value {
        let Some(metrics) = self.upstream_metrics.get(upstream_name) else {
            return json!({
                "ok": false,
                "stale": true,
                "error": "not_collected",
            });
        };
        json!({
            "ok": metrics.ok,
            "stale": metrics.is_stale(config),
            "error": metrics.error,
            "consecutive_errors": metrics.consecutive_errors,
            "last_error_age_ms": metrics.last_error_age_ms(),
            "metrics_circuit_open": metrics.metrics_circuit_open(),
            "capacity_protocol": metrics.capacity_protocol_name(),
            "raw_observed_running": metrics.raw_observed_running,
            "raw_observed_waiting": metrics.raw_observed_waiting,
            "observed_running": metrics.observed_running,
            "observed_waiting": metrics.observed_waiting,
            "raw_global_limit": metrics.raw_global_limit,
            "global_limit": metrics.global_limit,
            "predictive_admission_enforce": metrics.predictive_admission_enforce,
            "router_backpressure_active": metrics.router_backpressure_active,
            "router_backpressure_applied": metrics.router_backpressure_applied,
            "router_inspect_capacity": metrics.router_inspect_capacity,
            "basic_limit": metrics.basic_limit,
            "basic_inflight": metrics.basic_inflight,
            "premium_inflight": metrics.premium_inflight,
            "age_ms": metrics.age_ms(),
        })
    }

    fn cache_index_admin_json(&self) -> Value {
        let stats = self.cache_index.stats();
        json!({
            "type": "radix_tree",
            "models": stats.models,
            "routes": stats.routes,
            "records": stats.records,
            "chars": stats.chars,
            "nodes": stats.nodes,
            "evictions_total": stats.evictions_total,
            "removed_routes_total": stats.removed_routes_total,
        })
    }
}

impl UpstreamMetrics {
    fn collected_error(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(message.into()),
            last_error_at: Some(Instant::now()),
            consecutive_errors: 1,
            ..Default::default()
        }
    }

    fn is_stale(&self, config: &MiddlewareConfig) -> bool {
        let Some(updated_at) = self.updated_at else {
            return true;
        };
        updated_at.elapsed() > Duration::from_millis(config.metrics_stale_ms)
    }

    fn age_ms(&self) -> Option<u64> {
        self.updated_at
            .map(|updated_at| updated_at.elapsed().as_millis() as u64)
    }

    fn last_error_age_ms(&self) -> Option<u64> {
        self.last_error_at
            .map(|last_error_at| last_error_at.elapsed().as_millis() as u64)
    }

    fn metrics_circuit_open(&self) -> bool {
        !self.ok && self.consecutive_errors >= METRICS_CIRCUIT_FAILURE_THRESHOLD
    }

    fn request_aware_capacity_is_open(&self) -> bool {
        self.uses_request_aware_capacity()
            && self.router_backpressure_applied == Some(false)
            && self.global_limit.is_some_and(|limit| limit == 0.0)
    }

    fn uses_request_aware_capacity(&self) -> bool {
        self.predictive_admission_enforce == Some(true)
    }

    fn request_aware_projection_is_inconsistent(&self) -> bool {
        if !self.uses_request_aware_capacity() {
            return false;
        }
        match self.router_backpressure_applied {
            Some(false) => !self.global_limit.is_some_and(|limit| limit == 0.0),
            Some(true) => !self
                .global_limit
                .is_some_and(|limit| limit.is_finite() && limit > 0.0),
            None => true,
        }
    }

    fn capacity_protocol_name(&self) -> &'static str {
        if !self.uses_request_aware_capacity() {
            return "legacy";
        }
        if self.request_aware_projection_is_inconsistent() {
            return "request_aware_invalid";
        }
        match self.router_backpressure_applied {
            Some(true) => "request_aware_protected",
            Some(false) => "request_aware_open",
            None => "request_aware_invalid",
        }
    }
}

fn ratio_milli(value: f64, limit: Option<f64>) -> u64 {
    let Some(limit) = limit else {
        return 0;
    };
    if limit <= 0.0 {
        return 0;
    }
    ((value / limit) * 1_000.0).max(0.0).round() as u64
}

fn linux_open_fd_count() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir("/proc/self/fd")
            .ok()
            .map(|entries| entries.count())
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

fn linux_soft_fd_limit() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let limits = std::fs::read_to_string("/proc/self/limits").ok()?;
        let line = limits
            .lines()
            .find(|line| line.starts_with("Max open files"))?;
        line.split_whitespace().nth(3)?.parse().ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

fn usage_prompt_tokens(usage: &Value) -> Option<u64> {
    usage
        .get("prompt_tokens")
        .or_else(|| usage.get("input_tokens"))
        .and_then(Value::as_u64)
}

fn usage_cached_tokens(usage: &Value) -> Option<u64> {
    usage
        .get("prompt_tokens_details")
        .and_then(|details| details.get("cached_tokens"))
        .or_else(|| {
            usage
                .get("input_tokens_details")
                .and_then(|details| details.get("cached_tokens"))
        })
        .or_else(|| usage.get("cache_read_input_tokens"))
        .and_then(Value::as_u64)
}

fn spawn_metrics_poller(
    config_store: Arc<RouterConfigStore>,
    upstream_config: Arc<UpstreamConfigManager>,
    state: Arc<Mutex<RouterState>>,
    notify: Arc<Notify>,
) {
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    tokio::spawn(async move {
        let mut client = None;
        let mut client_timeout_ms = None;
        let mut next_poll = Instant::now();
        loop {
            let config = config_store.snapshot();
            if config.metrics_poll_ms == 0 {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {},
                    _ = notify.notified() => {},
                }
                next_poll = Instant::now();
                continue;
            }
            let now = Instant::now();
            if now < next_poll {
                tokio::select! {
                    _ = tokio::time::sleep(next_poll.duration_since(now).min(Duration::from_millis(250))) => {},
                    _ = notify.notified() => {},
                }
                continue;
            }
            if client_timeout_ms != Some(config.metrics_timeout_ms) {
                match reqwest::Client::builder()
                    .connect_timeout(Duration::from_millis(config.metrics_timeout_ms))
                    .timeout(Duration::from_millis(config.metrics_timeout_ms))
                    .build()
                {
                    Ok(next) => {
                        client = Some(next);
                        client_timeout_ms = Some(config.metrics_timeout_ms);
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "router middleware could not build metrics client");
                        next_poll = Instant::now() + Duration::from_millis(config.metrics_poll_ms);
                        continue;
                    }
                }
            }
            let poll_started = Instant::now();
            let targets = upstream_config.metrics_targets();
            let live_names = targets
                .iter()
                .map(|target| target.upstream_name.clone())
                .collect::<HashSet<_>>();
            let dispatch_watermarks = {
                let state = state.lock().expect("router state poisoned");
                targets
                    .iter()
                    .map(|target| {
                        (
                            target.upstream_name.clone(),
                            state.dispatch_watermark(&target.upstream_name),
                        )
                    })
                    .collect::<HashMap<_, _>>()
            };
            let client = client.as_ref().expect("metrics client initialized");
            let mut fetched = futures_util::stream::iter(targets.into_iter().map(|target| {
                let upstream_name = target.upstream_name.clone();
                let dispatch_watermark = dispatch_watermarks
                    .get(&upstream_name)
                    .copied()
                    .unwrap_or(0);
                let config = config.as_ref();
                async move {
                    let metrics = fetch_upstream_metrics(client, config, target).await;
                    (upstream_name, dispatch_watermark, metrics)
                }
            }))
            .buffer_unordered(METRICS_POLL_CONCURRENCY_LIMIT);
            while let Some((upstream_name, dispatch_watermark, metrics)) = fetched.next().await {
                let mut state = state.lock().expect("router state poisoned");
                state.update_upstream_metrics_from_poll(upstream_name, metrics, dispatch_watermark);
            }
            {
                let mut state = state.lock().expect("router state poisoned");
                state.retain_upstream_metrics(&live_names);
                state.metrics_epoch = state.metrics_epoch.saturating_add(1);
            }
            notify.notify_waiters();
            next_poll = poll_started + Duration::from_millis(config.metrics_poll_ms);
        }
    });
}

async fn fetch_upstream_metrics(
    client: &reqwest::Client,
    config: &MiddlewareConfig,
    target: UpstreamMetricsTarget,
) -> UpstreamMetrics {
    let path = if config.metrics_path.starts_with('/') {
        config.metrics_path.as_str()
    } else {
        "/v1/metrics"
    };
    let url = format!("{}{}", target.base_url.trim_end_matches('/'), path);
    let mut req = client.get(url).header("accept", "text/plain");
    if let Some(token) = target.bearer_token.as_deref() {
        req = req.bearer_auth(token);
    }
    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            if !status.is_success() {
                return UpstreamMetrics::collected_error(format!("http_{status}"));
            }
            match resp.text().await {
                Ok(body) => parse_upstream_metrics(&body),
                Err(err) => UpstreamMetrics::collected_error(format!("read_error: {err}")),
            }
        }
        Err(err) => UpstreamMetrics::collected_error(format!("fetch_error: {err}")),
    }
}

fn parse_upstream_metrics(text: &str) -> UpstreamMetrics {
    let mut metrics = UpstreamMetrics {
        ok: true,
        updated_at: Some(Instant::now()),
        ..Default::default()
    };
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name_and_labels, value)) = parse_prometheus_sample(line) else {
            continue;
        };
        let (name, labels) = split_metric_name_labels(name_and_labels);
        match name {
            "pig_dynamic_observed_running_raw" => metrics.raw_observed_running = Some(value),
            "pig_dynamic_observed_waiting_raw" => metrics.raw_observed_waiting = Some(value),
            "pig_dynamic_observed_running" => metrics.observed_running = Some(value),
            "pig_dynamic_observed_waiting" => metrics.observed_waiting = Some(value),
            "pig_dynamic_global_limit_raw" => metrics.raw_global_limit = Some(value),
            "pig_dynamic_global_limit" => metrics.global_limit = Some(value),
            "pig_predictive_admission_enforce" => {
                metrics.predictive_admission_enforce = Some(value >= 0.5)
            }
            "pig_dynamic_router_backpressure_active"
            | "pig_predictive_router_backpressure_active" => {
                metrics.router_backpressure_active = Some(value >= 0.5)
            }
            "pig_dynamic_router_backpressure_applied"
            | "pig_predictive_router_backpressure_applied" => {
                metrics.router_backpressure_applied = Some(value >= 0.5)
            }
            "pig_predictive_router_inspect_capacity" => {
                metrics.router_inspect_capacity = Some(value.max(0.0))
            }
            "pig_tier_basic_limit" => metrics.basic_limit = Some(value),
            "pig_tier_inflight" => match labels.get("tier").map(String::as_str) {
                Some("basic") => metrics.basic_inflight = Some(value),
                Some("premium") => metrics.premium_inflight = Some(value),
                _ => {}
            },
            _ => {}
        }
    }
    if metrics.observed_running.is_none()
        && metrics.observed_waiting.is_none()
        && metrics.global_limit.is_none()
        && metrics.basic_limit.is_none()
    {
        return UpstreamMetrics::collected_error("pig_metrics_missing");
    }
    metrics
}

fn parse_prometheus_sample(line: &str) -> Option<(&str, f64)> {
    let split_at = line.rfind(|c: char| c.is_whitespace())?;
    let (left, right) = line.split_at(split_at);
    right
        .trim()
        .parse::<f64>()
        .ok()
        .map(|value| (left.trim(), value))
}

fn split_metric_name_labels(input: &str) -> (&str, HashMap<String, String>) {
    let Some(open) = input.find('{') else {
        return (input, HashMap::new());
    };
    let name = &input[..open];
    let labels_text = input[open + 1..].trim_end_matches('}');
    let mut labels = HashMap::new();
    for part in labels_text.split(',') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        labels.insert(
            key.trim().to_string(),
            value.trim().trim_matches('"').to_string(),
        );
    }
    (name, labels)
}

fn route_from_upstream(
    upstream: &PublicUpstreamConfig,
    public_model: &str,
    config: &MiddlewareConfig,
) -> RouterRoute {
    let route_id = format!("{}:{public_model}", upstream.name);
    RouterRoute {
        route_id: route_id.clone(),
        upstream_name: upstream.name.clone(),
        candidate: RouteCandidate {
            route_id,
            format: provider_format(upstream.provider),
            engine: config.default_engine,
        },
    }
}

fn limit_forward_routes(routes: &mut Vec<RouterRoute>, limit: usize) {
    routes.truncate(limit.max(1));
}

fn provider_format(provider: UpstreamProvider) -> ProviderFormat {
    match provider {
        UpstreamProvider::Anthropic => ProviderFormat::Anthropic,
        _ => ProviderFormat::Openai,
    }
}

fn provider_name(provider: UpstreamProvider) -> &'static str {
    match provider {
        UpstreamProvider::OpenAiCompatible => "openai-compatible",
        UpstreamProvider::Anthropic => "anthropic",
        UpstreamProvider::AciService => "aci-service",
        UpstreamProvider::Chutes => "chutes",
        UpstreamProvider::Tinfoil => "tinfoil",
        UpstreamProvider::NearAi => "near-ai",
        UpstreamProvider::SecretAi => "secret-ai",
        UpstreamProvider::PhalaDirect => "phala-direct",
    }
}

fn bounded_routing_text(params: &Value, endpoint: Endpoint) -> String {
    let mut builder = BoundedText::new(MAX_ROUTING_HISTORY_CHARS);
    match endpoint {
        Endpoint::Complete => append_prompt(&mut builder, params.get("prompt")),
        Endpoint::Embed => append_prompt(&mut builder, params.get("input")),
        Endpoint::Messages | Endpoint::ChatComplete => {
            append_messages(&mut builder, params.get("messages"))
        }
        Endpoint::CreateModelResponse => append_prompt(&mut builder, params.get("input")),
    }
    builder.finish()
}

fn limit_chars(text: &str, max_chars: usize) -> String {
    let mut builder = BoundedText::new(max_chars);
    builder.push(text);
    builder.finish()
}

struct BoundedText {
    out: String,
    remaining: usize,
}

impl BoundedText {
    fn new(max_chars: usize) -> Self {
        Self {
            out: String::new(),
            remaining: max_chars,
        }
    }

    fn is_full(&self) -> bool {
        self.remaining == 0
    }

    fn push(&mut self, text: &str) {
        if self.remaining == 0 || text.is_empty() {
            return;
        }
        for ch in text.chars().take(self.remaining) {
            self.out.push(ch);
            self.remaining -= 1;
        }
    }

    fn separator(&mut self, text: &str) {
        if !self.out.is_empty() {
            self.push(text);
        }
    }

    fn finish(self) -> String {
        self.out
    }
}

fn append_prompt(out: &mut BoundedText, value: Option<&Value>) {
    let Some(value) = value else {
        return;
    };
    append_prompt_value(out, value);
}

fn append_prompt_value(out: &mut BoundedText, value: &Value) {
    if out.is_full() {
        return;
    }
    match value {
        Value::String(s) => out.push(s),
        Value::Number(n) => out.push(&n.to_string()),
        Value::Array(items) => {
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.separator(" ");
                }
                append_prompt_value(out, item);
                if out.is_full() {
                    break;
                }
            }
        }
        Value::Object(obj) => {
            if let Some(value) = obj.get("text").or_else(|| obj.get("content")) {
                append_prompt_value(out, value);
            }
        }
        _ => {}
    }
}

fn append_messages(out: &mut BoundedText, value: Option<&Value>) {
    let Some(messages) = value.and_then(Value::as_array) else {
        return;
    };
    for msg in messages {
        out.separator("\n");
        let role = msg.get("role").and_then(Value::as_str).unwrap_or("");
        out.push(role);
        out.push(":");
        append_prompt(out, msg.get("content"));
        if out.is_full() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Barrier;

    #[test]
    fn routing_text_uses_chat_messages_not_only_session_id() {
        let text = bounded_routing_text(
            &json!({
                "messages": [
                    {"role": "system", "content": "stable prefix"},
                    {"role": "user", "content": "hello"}
                ]
            }),
            Endpoint::ChatComplete,
        );
        assert!(text.contains("stable prefix"));
        assert!(text.contains("hello"));
    }

    #[test]
    fn cache_prefers_previous_prefix_when_balanced() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig {
            cache_threshold: 0.25,
            balance_abs_threshold: 64,
            balance_rel_threshold: 1.5,
            max_history_per_route: 16,
            metrics_poll_ms: 0,
            ..Default::default()
        };
        let routes = vec![test_route("a:m"), test_route("b:m")];
        assert_eq!(
            state
                .select("m", "hello world", &routes, &config, UserTier::Basic)
                .map(|s| s.route_id)
                .as_deref(),
            Some("a:m")
        );
        assert_eq!(
            state
                .select("m", "hello there", &routes, &config, UserTier::Basic)
                .map(|s| s.route_id)
                .as_deref(),
            Some("a:m")
        );
    }

    #[test]
    fn no_cache_tie_uses_processed_count_to_spread_idle_routes() {
        let state = Arc::new(Mutex::new(RouterState::default()));
        let config = MiddlewareConfig {
            cache_threshold: 0.9,
            balance_abs_threshold: 64,
            balance_rel_threshold: 1.5,
            max_history_per_route: 16,
            metrics_poll_ms: 0,
            ..Default::default()
        };
        let routes = vec![test_route("a:m"), test_route("b:m")];
        let first = {
            let mut locked = state.lock().unwrap();
            locked
                .select("m", "aaaa", &routes, &config, UserTier::Basic)
                .unwrap()
        };
        assert_eq!(first.route_id, "a:m");
        state.lock().unwrap().mark_started(&first);
        drop(test_in_flight(state.clone(), &first, "aaaa"));

        let second = {
            let mut locked = state.lock().unwrap();
            locked
                .select("m", "zzzz", &routes, &config, UserTier::Basic)
                .unwrap()
        };
        assert_eq!(second.route_id, "b:m");
    }

    #[test]
    fn request_aware_open_capacity_uses_running_before_legacy_fullness() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("new:m"), test_route("legacy:m")];
        state.update_upstream_metrics(
            "new".to_string(),
            request_aware_metrics(20.0, 0.0, false, 0.0),
        );
        state.update_upstream_metrics(
            "legacy".to_string(),
            test_metrics(4.0, 0.0, 24.0, 23.0, 4.0),
        );

        assert_eq!(
            state.load_order(&routes, &config, UserTier::Basic),
            LoadOrder::Running
        );
        let selected = state
            .select("m", "cold request", &routes, &config, UserTier::Basic)
            .unwrap();
        assert_eq!(selected.route_id, "legacy:m");
    }

    #[test]
    fn live_mixed_pool_shape_does_not_treat_neutral_limit_as_empty() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![
            test_route("use1-cb:m"),
            test_route("use1-19:m"),
            test_route("use1-9b:m"),
        ];
        state.update_upstream_metrics(
            "use1-cb".to_string(),
            request_aware_metrics(20.0, 0.0, false, 0.0),
        );
        state.update_upstream_metrics(
            "use1-19".to_string(),
            test_metrics(5.0, 0.0, 30.0, 29.0, 5.0),
        );
        state.update_upstream_metrics(
            "use1-9b".to_string(),
            test_metrics(4.0, 0.0, 24.0, 23.0, 4.0),
        );

        let selected = state
            .select("m", "cold request", &routes, &config, UserTier::Basic)
            .unwrap();

        assert_eq!(
            state.load_order(&routes, &config, UserTier::Basic),
            LoadOrder::Running
        );
        assert_eq!(selected.route_id, "use1-9b:m");
    }

    #[test]
    fn blocked_request_aware_route_does_not_change_legacy_pool_ordering() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![
            test_route("new:m"),
            test_route("wide:m"),
            test_route("narrow:m"),
        ];
        state.update_upstream_metrics(
            "new".to_string(),
            request_aware_metrics(1.0, 1.0, false, 0.0),
        );
        state.update_upstream_metrics("wide".to_string(), test_metrics(5.0, 0.0, 100.0, 90.0, 5.0));
        state.update_upstream_metrics("narrow".to_string(), test_metrics(2.0, 0.0, 10.0, 9.0, 2.0));

        assert_eq!(
            state.load_order(&routes, &config, UserTier::Basic),
            LoadOrder::CapacityNormalized
        );
        let selected = state
            .select("m", "cold request", &routes, &config, UserTier::Basic)
            .unwrap();
        assert_eq!(selected.route_id, "wide:m");
    }

    #[test]
    fn predictive_shadow_metrics_keep_legacy_capacity_semantics() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("shadow:m"), test_route("legacy:m")];
        let mut shadow = test_metrics(20.0, 0.0, 100.0, 100.0, 20.0);
        shadow.predictive_admission_enforce = Some(false);
        shadow.router_backpressure_applied = Some(false);
        state.update_upstream_metrics("shadow".to_string(), shadow);
        state.update_upstream_metrics(
            "legacy".to_string(),
            test_metrics(4.0, 0.0, 10.0, 10.0, 4.0),
        );

        assert_eq!(
            state.load_order(&routes, &config, UserTier::Basic),
            LoadOrder::CapacityNormalized
        );
        let selected = state
            .select("m", "cold request", &routes, &config, UserTier::Basic)
            .unwrap();
        assert_eq!(selected.route_id, "shadow:m");
        assert_eq!(
            state.upstream_metrics["shadow"].capacity_protocol_name(),
            "legacy"
        );
    }

    #[test]
    fn malformed_request_aware_projection_degrades_to_unknown_capacity() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = [test_route("new:m")];
        let mut missing_applied = request_aware_metrics(2.0, 0.0, false, 0.0);
        missing_applied.router_backpressure_applied = None;
        state.update_upstream_metrics("new".to_string(), missing_applied);

        let pressure = state.route_pressure(&routes[0], &config, UserTier::Basic);
        assert!(!pressure.blocked);
        assert!(pressure.metrics_error);
        assert!(pressure.metrics_missing);
        assert!(state.route_selectable(&routes[0], &config, UserTier::Basic));
        assert_eq!(
            state.upstream_metrics["new"].capacity_protocol_name(),
            "request_aware_invalid"
        );

        let mut invalid_open = request_aware_metrics(2.0, 0.0, false, 3.0);
        invalid_open.router_backpressure_active = Some(false);
        state.update_upstream_metrics("new".to_string(), invalid_open);
        let pressure = state.route_pressure(&routes[0], &config, UserTier::Basic);
        assert!(!pressure.blocked);
        assert!(pressure.metrics_error);
        assert!(pressure.metrics_missing);
    }

    #[test]
    fn legacy_only_pool_keeps_capacity_normalized_ordering() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("wide:m"), test_route("narrow:m")];
        state.update_upstream_metrics("wide".to_string(), test_metrics(5.0, 0.0, 100.0, 90.0, 5.0));
        state.update_upstream_metrics("narrow".to_string(), test_metrics(2.0, 0.0, 10.0, 9.0, 2.0));

        assert_eq!(
            state.load_order(&routes, &config, UserTier::Basic),
            LoadOrder::CapacityNormalized
        );
        let selected = state
            .select("m", "cold request", &routes, &config, UserTier::Basic)
            .unwrap();
        assert_eq!(selected.route_id, "wide:m");
    }

    #[test]
    fn local_reservation_exhaustion_passthroughs_to_pig() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("new:m")];
        state.update_upstream_metrics(
            "new".to_string(),
            request_aware_metrics(2.0, 0.0, true, 3.0),
        );

        let selected = state
            .select("m", "first", &routes, &config, UserTier::Basic)
            .expect("one inspect slot should be selectable");
        state.mark_started(&selected);

        let pressure = state.route_pressure(&routes[0], &config, UserTier::Basic);
        assert!(pressure.blocked);
        assert_eq!(pressure.effective_running, 3);
        assert_eq!(pressure.fullness_milli, 1_000);
        let second = state
            .select("m", "second", &routes, &config, UserTier::Basic)
            .expect("soft PIG pressure should passthrough to PIG");
        assert_eq!(second.route_id, "new:m");
        assert_eq!(second.reason, PIG_PRESSURE_PASSTHROUGH_REASON);
    }

    #[test]
    fn concurrent_burst_passthroughs_after_request_aware_slot_is_spent() {
        const DISPATCHERS: usize = 16;
        let state = Arc::new(Mutex::new(RouterState::default()));
        let config = Arc::new(MiddlewareConfig::default());
        let routes = Arc::new(vec![test_route("new:m")]);
        state.lock().unwrap().update_upstream_metrics(
            "new".to_string(),
            request_aware_metrics(2.0, 0.0, true, 3.0),
        );
        let barrier = Arc::new(Barrier::new(DISPATCHERS));

        let handles = (0..DISPATCHERS)
            .map(|index| {
                let state = state.clone();
                let config = config.clone();
                let routes = routes.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let mut state = state.lock().unwrap();
                    let selected = state.select(
                        "m",
                        &format!("burst-{index}"),
                        &routes,
                        &config,
                        UserTier::Basic,
                    );
                    let selected = selected.expect("soft PIG pressure should not pre-reject");
                    let passthrough =
                        usize::from(selected.reason == PIG_PRESSURE_PASSTHROUGH_REASON);
                    state.mark_started(&selected);
                    passthrough
                })
            })
            .collect::<Vec<_>>();
        let passthroughs = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .sum::<usize>();

        assert_eq!(passthroughs, DISPATCHERS - 1);
    }

    #[test]
    fn fresh_request_aware_open_snapshot_recovers_without_sticky_lock() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("new:m")];
        state.update_upstream_metrics(
            "new".to_string(),
            request_aware_metrics(3.0, 0.0, true, 3.0),
        );
        let pressured = state
            .select("m", "blocked", &routes, &config, UserTier::Basic)
            .unwrap();
        assert_eq!(pressured.reason, PIG_PRESSURE_PASSTHROUGH_REASON);

        state.update_upstream_metrics(
            "new".to_string(),
            request_aware_metrics(0.0, 0.0, false, 0.0),
        );
        assert!(
            state
                .select("m", "recovered", &routes, &config, UserTier::Basic)
                .is_some(),
            "fresh open metrics did not clear request-aware protection"
        );
    }

    #[test]
    fn cancelled_before_forward_releases_reservation_without_dispatch_debt() {
        let state = Arc::new(Mutex::new(RouterState::default()));
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("new:m")];
        state.lock().unwrap().update_upstream_metrics(
            "new".to_string(),
            request_aware_metrics(0.0, 0.0, false, 0.0),
        );
        let selected = state
            .lock()
            .unwrap()
            .select("m", "first", &routes, &config, UserTier::Basic)
            .unwrap();
        state.lock().unwrap().mark_started(&selected);

        drop(test_in_flight(state.clone(), &selected, "first"));

        let mut state = state.lock().unwrap();
        assert_eq!(state.stats["new:m"].running, 0);
        assert_eq!(state.dispatch_watermark("new"), 0);
        assert!(
            state
                .select("m", "second", &routes, &config, UserTier::Basic)
                .is_some(),
            "a request cancelled before forwarding left the idle route locked"
        );
    }

    #[test]
    fn request_aware_open_is_healthy_without_legacy_tier_capacity() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("new:m")];
        let mut metrics = request_aware_metrics(2.0, 0.0, false, 0.0);
        metrics.basic_limit = None;
        metrics.basic_inflight = Some(10_000.0);
        state.update_upstream_metrics("new".to_string(), metrics);

        let pressure = state.route_pressure(&routes[0], &config, UserTier::Basic);
        assert!(!pressure.blocked);
        assert_eq!(pressure.fullness_milli, 0);
        assert_eq!(
            state.upstream_status_code(&routes, &config, UserTier::Basic),
            UPSTREAM_STATUS_GREEN
        );
    }

    #[test]
    fn dispatch_debt_requires_a_later_successful_poll_to_reconcile() {
        let mut state = RouterState::default();
        state.record_dispatch_for_route("new:m");
        let watermark = state.dispatch_watermark("new");
        assert_eq!(watermark, 1);

        state.update_upstream_metrics_from_poll(
            "new".to_string(),
            request_aware_metrics(0.0, 0.0, false, 0.0),
            watermark,
        );
        assert_eq!(state.dispatch_ledgers["new"].unreconciled(), 1);

        state.update_upstream_metrics_from_poll(
            "new".to_string(),
            UpstreamMetrics::collected_error("fetch_error"),
            watermark,
        );
        assert_eq!(state.dispatch_ledgers["new"].unreconciled(), 1);

        state.update_upstream_metrics_from_poll(
            "new".to_string(),
            request_aware_metrics(0.0, 0.0, false, 0.0),
            watermark,
        );
        assert_eq!(state.dispatch_ledgers["new"].unreconciled(), 0);
    }

    #[test]
    fn request_scoped_429_demotes_but_does_not_lock_the_only_route() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("new:m")];
        state.update_upstream_metrics(
            "new".to_string(),
            request_aware_metrics(0.0, 0.0, false, 0.0),
        );
        state.record_dispatch_for_route("new:m");
        state.record_attempt_response(
            "new:m",
            429,
            false,
            &RouterMetrics::new().expect("test router metrics"),
        );

        let pressure = state.route_pressure(&routes[0], &config, UserTier::Basic);
        assert!(!pressure.blocked);
        assert!(pressure.capacity_rejected);
        assert_eq!(pressure.unreconciled_dispatches, 1);
        let selected = state
            .select("m", "small follow-up", &routes, &config, UserTier::Basic)
            .expect("capacity rejection must remain a least-bad probe, not a hard lock");
        assert_eq!(selected.reason, PIG_PRESSURE_PASSTHROUGH_REASON);
    }

    #[test]
    fn capacity_rejection_deprioritizes_route_until_the_next_metrics_epoch() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("a:m"), test_route("b:m")];
        state.update_upstream_metrics("a".to_string(), test_metrics(1.0, 0.0, 10.0, 9.0, 1.0));
        state.update_upstream_metrics("b".to_string(), test_metrics(7.0, 0.0, 10.0, 9.0, 7.0));
        let metrics = RouterMetrics::new().expect("test router metrics");

        assert_eq!(
            state
                .select("m", "cold-a", &routes, &config, UserTier::Basic)
                .unwrap()
                .route_id,
            "a:m"
        );
        state.record_attempt_response("a:m", 429, false, &metrics);
        assert_eq!(
            state
                .select("m", "avoid-a", &routes, &config, UserTier::Basic)
                .unwrap()
                .route_id,
            "b:m",
            "a same-epoch 429 must beat stale low-pressure metrics"
        );

        state.metrics_epoch = state.metrics_epoch.saturating_add(1);
        assert_eq!(
            state
                .select("m", "probe-a-again", &routes, &config, UserTier::Basic)
                .unwrap()
                .route_id,
            "a:m",
            "a fresh metrics epoch must release the short capacity penalty"
        );
    }

    #[test]
    fn removing_an_upstream_clears_its_dispatch_debt() {
        let mut state = RouterState::default();
        state.record_dispatch_for_route("removed:m");
        state.record_dispatch_for_route("kept:m");

        state.retain_upstream_metrics(&HashSet::from(["kept".to_string()]));

        assert!(!state.dispatch_ledgers.contains_key("removed"));
        assert_eq!(state.dispatch_ledgers["kept"].unreconciled(), 1);
    }

    #[test]
    fn parser_reads_request_aware_capacity_projection() {
        let metrics = parse_upstream_metrics(
            "pig_dynamic_observed_running_raw 7\n\
             pig_dynamic_observed_running 8\n\
             pig_dynamic_observed_waiting_raw 1\n\
             pig_dynamic_observed_waiting 0\n\
             pig_dynamic_global_limit_raw 9\n\
             pig_dynamic_global_limit 8\n\
             pig_predictive_admission_enforce 1\n\
             pig_dynamic_router_backpressure_active 1\n\
             pig_dynamic_router_backpressure_applied 1\n\
             pig_predictive_router_inspect_capacity 0\n",
        );

        assert!(metrics.ok);
        assert_eq!(metrics.raw_observed_running, Some(7.0));
        assert_eq!(metrics.observed_running, Some(8.0));
        assert_eq!(metrics.raw_observed_waiting, Some(1.0));
        assert_eq!(metrics.observed_waiting, Some(0.0));
        assert_eq!(metrics.raw_global_limit, Some(9.0));
        assert_eq!(metrics.global_limit, Some(8.0));
        assert_eq!(metrics.predictive_admission_enforce, Some(true));
        assert_eq!(metrics.router_backpressure_active, Some(true));
        assert_eq!(metrics.router_backpressure_applied, Some(true));
        assert_eq!(metrics.router_inspect_capacity, Some(0.0));
        assert_eq!(metrics.capacity_protocol_name(), "request_aware_protected");
    }

    #[test]
    fn middleware_metrics_poll_default_is_one_second() {
        assert_eq!(MiddlewareConfig::default().metrics_poll_ms, 1_000);
    }

    #[test]
    fn pig_pressure_overrides_cache_affinity() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig {
            cache_threshold: 0.25,
            balance_abs_threshold: 64,
            balance_rel_threshold: 1.5,
            max_history_per_route: 16,
            ..Default::default()
        };
        let routes = vec![test_route("a:m"), test_route("b:m")];
        state.update_upstream_metrics("a".to_string(), test_metrics(1.0, 0.0, 10.0, 9.0, 1.0));
        state.update_upstream_metrics("b".to_string(), test_metrics(1.0, 0.0, 10.0, 9.0, 1.0));
        assert_eq!(
            state
                .select("m", "stable prefix one", &routes, &config, UserTier::Basic)
                .map(|s| s.route_id)
                .as_deref(),
            Some("a:m")
        );
        // Selection no longer mutates the cache index. Model a confirmed first
        // response before exercising pressure rejection of that affinity.
        state.record_cache("m", "a:m", "stable prefix one", 16);
        state.update_upstream_metrics(
            "a".to_string(),
            UpstreamMetrics {
                ok: true,
                updated_at: Some(Instant::now()),
                observed_running: Some(10.0),
                observed_waiting: Some(1.0),
                global_limit: Some(10.0),
                basic_limit: Some(9.0),
                basic_inflight: Some(9.0),
                premium_inflight: Some(0.0),
                error: None,
                ..Default::default()
            },
        );
        state.update_upstream_metrics(
            "b".to_string(),
            UpstreamMetrics {
                ok: true,
                updated_at: Some(Instant::now()),
                observed_running: Some(1.0),
                observed_waiting: Some(0.0),
                global_limit: Some(10.0),
                basic_limit: Some(9.0),
                basic_inflight: Some(1.0),
                premium_inflight: Some(0.0),
                error: None,
                ..Default::default()
            },
        );

        let selected = state
            .select("m", "stable prefix two", &routes, &config, UserTier::Basic)
            .unwrap();
        assert_eq!(selected.route_id, "b:m");
        assert_eq!(selected.reason, "least_running");
        assert_eq!(state.stats["a:m"].cache_rejected_by_pressure, 1);
    }

    #[test]
    fn cache_match_is_checked_before_unrelated_global_imbalance() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig {
            cache_threshold: 0.25,
            balance_abs_threshold: 64,
            balance_rel_threshold: 1.5,
            max_history_per_route: 16,
            ..Default::default()
        };
        let routes = vec![test_route("a:m"), test_route("b:m"), test_route("c:m")];
        state.record_cache("m", "b:m", "stable prefix one", 16);
        state.update_upstream_metrics("a".to_string(), test_metrics(20.0, 0.0, 200.0, 180.0, 20.0));
        state.update_upstream_metrics("b".to_string(), test_metrics(1.0, 0.0, 200.0, 180.0, 1.0));
        state.update_upstream_metrics(
            "c".to_string(),
            test_metrics(100.0, 0.0, 200.0, 180.0, 100.0),
        );

        let selected = state
            .select("m", "stable prefix two", &routes, &config, UserTier::Basic)
            .unwrap();

        assert_eq!(selected.route_id, "b:m");
        assert_eq!(selected.reason, "cache");
        assert!(selected.cache_match_rate > config.cache_threshold);
    }

    #[test]
    fn cache_match_falls_back_to_load_when_matched_route_is_too_loaded() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig {
            cache_threshold: 0.25,
            balance_abs_threshold: 64,
            balance_rel_threshold: 1.5,
            max_history_per_route: 16,
            ..Default::default()
        };
        let routes = vec![test_route("a:m"), test_route("b:m")];
        state.record_cache("m", "a:m", "stable prefix one", 16);
        state.update_upstream_metrics(
            "a".to_string(),
            test_metrics(100.0, 0.0, 200.0, 180.0, 100.0),
        );
        state.update_upstream_metrics("b".to_string(), test_metrics(10.0, 0.0, 200.0, 180.0, 10.0));

        let selected = state
            .select("m", "stable prefix two", &routes, &config, UserTier::Basic)
            .unwrap();

        assert_eq!(selected.route_id, "b:m");
        assert_eq!(selected.reason, "least_running");
        assert!(selected.cache_match_rate > config.cache_threshold);
        assert_eq!(state.stats["a:m"].cache_rejected_by_pressure, 1);
    }

    #[test]
    fn premium_does_not_treat_basic_full_as_blocked() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig {
            cache_threshold: 0.25,
            balance_abs_threshold: 64,
            balance_rel_threshold: 1.5,
            max_history_per_route: 16,
            ..Default::default()
        };
        let routes = vec![test_route("a:m"), test_route("b:m")];
        state.update_upstream_metrics(
            "a".to_string(),
            UpstreamMetrics {
                ok: true,
                updated_at: Some(Instant::now()),
                observed_running: Some(4.0),
                observed_waiting: Some(0.0),
                global_limit: Some(10.0),
                basic_limit: Some(4.0),
                basic_inflight: Some(4.0),
                premium_inflight: Some(0.0),
                error: None,
                ..Default::default()
            },
        );
        state.update_upstream_metrics(
            "b".to_string(),
            UpstreamMetrics {
                ok: true,
                updated_at: Some(Instant::now()),
                observed_running: Some(5.0),
                observed_waiting: Some(0.0),
                global_limit: Some(10.0),
                basic_limit: Some(8.0),
                basic_inflight: Some(2.0),
                premium_inflight: Some(0.0),
                error: None,
                ..Default::default()
            },
        );

        let basic = state
            .select("m", "cold-basic", &routes, &config, UserTier::Basic)
            .unwrap();
        let mut premium_state = RouterState::default();
        premium_state.update_upstream_metrics(
            "a".to_string(),
            UpstreamMetrics {
                ok: true,
                updated_at: Some(Instant::now()),
                observed_running: Some(4.0),
                observed_waiting: Some(0.0),
                global_limit: Some(10.0),
                basic_limit: Some(4.0),
                basic_inflight: Some(4.0),
                premium_inflight: Some(0.0),
                error: None,
                ..Default::default()
            },
        );
        premium_state.update_upstream_metrics(
            "b".to_string(),
            UpstreamMetrics {
                ok: true,
                updated_at: Some(Instant::now()),
                observed_running: Some(5.0),
                observed_waiting: Some(0.0),
                global_limit: Some(10.0),
                basic_limit: Some(8.0),
                basic_inflight: Some(2.0),
                premium_inflight: Some(0.0),
                error: None,
                ..Default::default()
            },
        );
        let premium = premium_state
            .select("m", "cold-premium", &routes, &config, UserTier::Premium)
            .unwrap();

        assert_eq!(basic.route_id, "b:m");
        assert_eq!(premium.route_id, "a:m");
    }

    #[test]
    fn metrics_error_route_is_selectable_when_every_measured_route_is_blocked() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("a:m"), test_route("b:m")];
        state.update_upstream_metrics("a".to_string(), test_metrics(10.0, 0.0, 10.0, 9.0, 9.0));
        state.update_upstream_metrics(
            "b".to_string(),
            UpstreamMetrics::collected_error("fetch_error"),
        );

        let selected = state
            .select("m", "cold-basic", &routes, &config, UserTier::Basic)
            .unwrap();
        assert_eq!(selected.route_id, "b:m");
    }

    #[test]
    fn metrics_error_route_is_ignored_when_healthy_route_has_capacity() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("a:m"), test_route("b:m")];
        state.update_upstream_metrics("a".to_string(), test_metrics(2.0, 0.0, 10.0, 9.0, 2.0));
        state.update_upstream_metrics(
            "b".to_string(),
            UpstreamMetrics::collected_error("fetch_error"),
        );

        let selected = state
            .select("m", "cold-basic", &routes, &config, UserTier::Basic)
            .unwrap();
        assert_eq!(selected.route_id, "a:m");
    }

    #[test]
    fn failed_metrics_poll_preserves_last_good_capacity() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let route = test_route("a:m");
        state.update_upstream_metrics_from_poll(
            "a".to_string(),
            test_metrics(7.0, 0.0, 10.0, 9.0, 7.0),
            0,
        );

        state.update_upstream_metrics_from_poll(
            "a".to_string(),
            UpstreamMetrics::collected_error("fetch_error"),
            0,
        );

        let pressure = state.route_pressure(&route, &config, UserTier::Basic);
        assert!(pressure.metrics_error);
        assert!(!pressure.metrics_missing);
        assert!(!pressure.circuit_open);
        assert_eq!(pressure.effective_running, 7);
        assert_eq!(pressure.fullness_milli, 778);
        assert_eq!(state.upstream_metrics["a"].observed_running, Some(7.0));
        assert_eq!(state.upstream_metrics["a"].consecutive_errors, 1);
    }

    #[test]
    fn repeated_metrics_failures_deprioritize_but_do_not_remove_inference_route() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("a:m")];
        state.update_upstream_metrics_from_poll(
            "a".to_string(),
            test_metrics(1.0, 0.0, 10.0, 9.0, 1.0),
            0,
        );
        for _ in 0..METRICS_CIRCUIT_FAILURE_THRESHOLD {
            state.update_upstream_metrics_from_poll(
                "a".to_string(),
                UpstreamMetrics::collected_error("fetch_error"),
                0,
            );
        }

        let pressure = state.route_pressure(&routes[0], &config, UserTier::Basic);
        assert!(!pressure.circuit_open);
        assert!(pressure.metrics_circuit_open);
        assert!(state
            .select("m", "metrics-unknown", &routes, &config, UserTier::Basic)
            .is_some());

        state.update_upstream_metrics_from_poll(
            "a".to_string(),
            test_metrics(1.0, 0.0, 10.0, 9.0, 1.0),
            0,
        );
        assert!(state
            .select("m", "recovered", &routes, &config, UserTier::Basic)
            .is_some());
    }

    #[test]
    fn request_failures_open_route_circuit_and_http_success_closes_it() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("a:m")];
        let metrics = RouterMetrics::new().expect("test router metrics");
        state.update_upstream_metrics("a".to_string(), test_metrics(1.0, 0.0, 10.0, 9.0, 1.0));

        for _ in 0..ROUTE_CIRCUIT_FAILURE_THRESHOLD {
            state.record_attempt_failure("a:m", MiddlewareAttemptFailure::Transport, &metrics);
        }
        assert!(
            state
                .route_pressure(&routes[0], &config, UserTier::Basic)
                .circuit_open
        );
        assert!(state
            .select("m", "blocked", &routes, &config, UserTier::Basic)
            .is_none());

        state.record_attempt_response("a:m", 200, false, &metrics);
        assert!(state
            .select("m", "recovered", &routes, &config, UserTier::Basic)
            .is_some());
    }

    #[test]
    fn client_errors_and_capacity_rejections_do_not_open_route_circuit() {
        let metrics = RouterMetrics::new().expect("test router metrics");
        for status in [200, 400, 422, 429, 500] {
            let mut state = RouterState::default();
            for _ in 0..ROUTE_CIRCUIT_FAILURE_THRESHOLD {
                state.record_attempt_failure("a:m", MiddlewareAttemptFailure::Transport, &metrics);
            }

            state.record_attempt_response("a:m", status, false, &metrics);

            let circuit = state.route_circuits["a:m"];
            assert!(!circuit.is_open(Instant::now()), "status {status}");
            assert_eq!(circuit.consecutive_failures, 0, "status {status}");
        }
    }

    #[test]
    fn route_circuit_reopens_only_for_bounded_duration() {
        let now = Instant::now();
        let mut circuit = RouteCircuit::default();
        for _ in 0..ROUTE_CIRCUIT_FAILURE_THRESHOLD {
            circuit.record_failure(now);
        }
        assert!(circuit.is_open(now + Duration::from_secs(1)));
        assert!(!circuit.is_open(now + ROUTE_CIRCUIT_OPEN_DURATION));
    }

    #[test]
    fn forwarding_candidate_list_uses_per_round_limit() {
        let mut routes = (0..10)
            .map(|index| test_route(&format!("gpu-{index}:m")))
            .collect::<Vec<_>>();

        limit_forward_routes(&mut routes, ROUTER_FORWARD_ROUND_LIMIT);

        assert_eq!(routes.len(), ROUTER_FORWARD_ROUND_LIMIT);
        assert_eq!(routes[0].route_id, "gpu-0:m");
        assert_eq!(routes[2].route_id, "gpu-2:m");
    }

    #[test]
    fn retry_round_prefers_untried_routes_and_is_bounded_to_three() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = (0..8)
            .map(|index| test_route(&format!("gpu-{index}:m")))
            .collect::<Vec<_>>();
        for (index, route) in routes.iter().enumerate() {
            state.update_upstream_metrics(
                route.upstream_name.clone(),
                test_metrics(index as f64, 0.0, 20.0, 19.0, index as f64),
            );
        }
        let attempted = HashSet::from([
            "gpu-0:m".to_string(),
            "gpu-1:m".to_string(),
            "gpu-2:m".to_string(),
        ]);

        let retry = state.retry_routes(
            &routes,
            &config,
            UserTier::Basic,
            &attempted,
            ROUTER_FORWARD_ROUND_LIMIT,
        );

        assert_eq!(retry.len(), ROUTER_FORWARD_ROUND_LIMIT);
        assert_eq!(
            retry
                .iter()
                .map(|route| route.route_id.as_str())
                .collect::<Vec<_>>(),
            vec!["gpu-3:m", "gpu-4:m", "gpu-5:m"]
        );
    }

    #[test]
    fn runtime_metrics_expose_fd_and_breaker_signals() {
        let metrics = RouterMetrics::new().expect("test router metrics");
        metrics.record_capacity_retry("started");
        metrics.refresh_runtime(&RouterState::default());
        let body = String::from_utf8(metrics.render().unwrap()).unwrap();

        assert!(body.contains("router_process_open_fds"));
        assert!(body.contains("router_process_max_fds"));
        assert!(body.contains("router_metrics_error_upstreams"));
        assert!(body.contains("router_open_circuits"));
        assert!(body.contains("router_capacity_retry_total"));
    }

    #[test]
    fn upstream_status_returns_green_when_any_route_has_capacity() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("a:m"), test_route("b:m")];
        state.update_upstream_metrics("a".to_string(), test_metrics(10.0, 1.0, 10.0, 9.0, 9.0));
        state.update_upstream_metrics("b".to_string(), test_metrics(2.0, 0.0, 10.0, 9.0, 2.0));

        assert_eq!(
            state.upstream_status_code(&routes, &config, UserTier::Basic),
            UPSTREAM_STATUS_GREEN
        );
    }

    #[test]
    fn upstream_status_returns_yellow_when_all_metrics_are_missing() {
        let state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("a:m")];

        assert_eq!(
            state.upstream_status_code(&routes, &config, UserTier::Basic),
            UPSTREAM_STATUS_YELLOW
        );
    }

    #[test]
    fn upstream_status_returns_yellow_for_near_full_metrics() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("a:m")];

        state.update_upstream_metrics("a".to_string(), test_metrics(8.6, 0.0, 10.0, 9.0, 7.0));
        assert_eq!(
            state.upstream_status_code(&routes, &config, UserTier::Basic),
            UPSTREAM_STATUS_YELLOW
        );
    }

    #[test]
    fn upstream_status_returns_yellow_when_all_routes_are_soft_blocked() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("a:m"), test_route("b:m")];
        state.update_upstream_metrics("a".to_string(), test_metrics(10.0, 0.0, 10.0, 9.0, 9.0));
        state.update_upstream_metrics("b".to_string(), test_metrics(2.0, 1.0, 10.0, 9.0, 2.0));

        assert_eq!(
            state.upstream_status_code(&routes, &config, UserTier::Basic),
            UPSTREAM_STATUS_YELLOW
        );
    }

    #[test]
    fn upstream_status_treats_basic_full_as_yellow() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("a:m")];
        state.update_upstream_metrics("a".to_string(), test_metrics(4.0, 0.0, 10.0, 4.0, 4.0));

        assert_eq!(
            state.upstream_status_code(&routes, &config, UserTier::Basic),
            UPSTREAM_STATUS_YELLOW
        );
        assert_eq!(
            state.upstream_status_code(&routes, &config, UserTier::Premium),
            UPSTREAM_STATUS_GREEN
        );
    }

    #[test]
    fn in_flight_guard_can_retarget_and_drops_running_count() {
        let state = Arc::new(Mutex::new(RouterState::default()));
        let selection = RouteSelection {
            route_id: "a:m".to_string(),
            reason: "least_running",
            cache_match_rate: 0.0,
            running_at_select: 0,
        };

        state.lock().unwrap().mark_started(&selection);
        let mut guard = test_in_flight(state.clone(), &selection, "test");
        assert_eq!(state.lock().unwrap().stats["a:m"].running, 1);

        guard.retarget("b:m");
        {
            let locked = state.lock().unwrap();
            assert_eq!(locked.stats["a:m"].running, 0);
            assert_eq!(locked.stats["b:m"].running, 1);
        }

        drop(guard);
        assert_eq!(state.lock().unwrap().stats["b:m"].running, 0);
    }

    #[test]
    fn attempt_observer_accounts_initial_and_fallback_dispatches_and_429() {
        let state = Arc::new(Mutex::new(RouterState::default()));
        let selection = RouteSelection {
            route_id: "a:m".to_string(),
            reason: "least_running",
            cache_match_rate: 0.0,
            running_at_select: 0,
        };
        state.lock().unwrap().mark_started(&selection);
        let mut guard = test_in_flight(state.clone(), &selection, "test");

        assert_eq!(state.lock().unwrap().dispatch_watermark("a"), 0);
        MiddlewareAttemptObserver::attempt_started(&mut guard, "a:m");
        MiddlewareAttemptObserver::attempt_response(&mut guard, "a:m", 429, false);
        MiddlewareAttemptObserver::attempt_started(&mut guard, "b:m");

        {
            let locked = state.lock().unwrap();
            assert_eq!(locked.stats["a:m"].running, 0);
            assert_eq!(locked.stats["a:m"].upstream_attempts, 1);
            assert_eq!(locked.stats["a:m"].upstream_429, 1);
            assert_eq!(locked.stats["b:m"].running, 1);
            assert_eq!(locked.stats["b:m"].upstream_attempts, 1);
            assert_eq!(locked.dispatch_ledgers["a"].unreconciled(), 1);
            assert_eq!(locked.dispatch_ledgers["b"].unreconciled(), 1);
        }

        drop(guard);
        assert_eq!(state.lock().unwrap().stats["b:m"].running, 0);
    }

    #[test]
    fn routing_cache_records_are_bounded() {
        let mut state = RouterState::default();
        let long = "x".repeat(MAX_ROUTING_HISTORY_CHARS + 10);

        state.record_cache("m", "a:m", &long, 16);

        let stats = state.cache_index.route_stats("m", "a:m");
        assert_eq!(stats.records, 1);
        assert_eq!(stats.chars, MAX_ROUTING_HISTORY_CHARS);
    }

    #[test]
    fn selection_does_not_record_cache_before_upstream_success() {
        let state = Arc::new(Mutex::new(RouterState::default()));
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("a:m"), test_route("b:m")];
        let selected = state
            .lock()
            .unwrap()
            .select("m", "shared prefix", &routes, &config, UserTier::Basic)
            .unwrap();

        assert_eq!(state.lock().unwrap().cache_index.stats().records, 0);

        state.lock().unwrap().mark_started(&selected);
        let mut in_flight = test_in_flight(state.clone(), &selected, "shared prefix");
        in_flight.commit_cache(Some(&json!({
            "prompt_tokens": 12,
            "prompt_tokens_details": {"cached_tokens": 0}
        })));

        let locked = state.lock().unwrap();
        assert_eq!(locked.cache_index.stats().records, 1);
        assert_eq!(
            locked
                .cache_index
                .match_prefix("m", "shared prefix continuation")
                .unwrap()
                .route_id,
            "a:m"
        );
    }

    #[test]
    fn failover_commits_only_the_actual_success_route() {
        let state = Arc::new(Mutex::new(RouterState::default()));
        let selection = RouteSelection {
            route_id: "a:m".to_string(),
            reason: "cache",
            cache_match_rate: 0.75,
            running_at_select: 0,
        };
        state.lock().unwrap().mark_started(&selection);
        let metrics = RouterMetrics::new().unwrap();
        let mut in_flight = RouteInFlight::from_reserved(
            state.clone(),
            metrics.clone(),
            &selection,
            "m".to_string(),
            "shared failover prefix".to_string(),
            16,
        );

        in_flight.retarget("b:m");
        in_flight.commit_cache(Some(&json!({
            "prompt_tokens": 20,
            "prompt_tokens_details": {"cached_tokens": 8}
        })));

        let locked = state.lock().unwrap();
        assert_eq!(locked.cache_index.route_stats("m", "a:m").records, 0);
        assert_eq!(locked.cache_index.route_stats("m", "b:m").records, 1);
        drop(locked);
        let rendered = String::from_utf8(metrics.render().unwrap()).unwrap();
        assert!(rendered.contains(
            "router_cache_affinity_retarget_total{from_route=\"a:m\",to_route=\"b:m\"} 1"
        ));
        assert!(rendered.contains(
            "router_cache_cached_tokens_total{route=\"b:m\",selection_reason=\"cache\"} 8"
        ));
    }

    #[test]
    fn failed_request_skips_cache_record() {
        let state = Arc::new(Mutex::new(RouterState::default()));
        let selection = RouteSelection {
            route_id: "a:m".to_string(),
            reason: "least_running",
            cache_match_rate: 0.0,
            running_at_select: 0,
        };
        state.lock().unwrap().mark_started(&selection);
        let metrics = RouterMetrics::new().unwrap();
        let mut in_flight = RouteInFlight::from_reserved(
            state.clone(),
            metrics.clone(),
            &selection,
            "m".to_string(),
            "failed prefix".to_string(),
            16,
        );

        in_flight.skip_cache("upstream_429");
        drop(in_flight);

        assert_eq!(state.lock().unwrap().cache_index.stats().records, 0);
        let rendered = String::from_utf8(metrics.render().unwrap()).unwrap();
        assert!(rendered.contains("router_cache_record_skipped_total{reason=\"upstream_429\"} 1"));
    }

    #[test]
    fn bounded_routing_text_caps_extracted_prompt_text() {
        let long = "x".repeat(MAX_ROUTING_HISTORY_CHARS + 10);
        let text = bounded_routing_text(
            &json!({
                "messages": [
                    {"role": "system", "content": long},
                    {"role": "user", "content": "must not be reached"}
                ]
            }),
            Endpoint::ChatComplete,
        );

        assert_eq!(text.chars().count(), MAX_ROUTING_HISTORY_CHARS);
        assert!(text.starts_with("system:"));
        assert!(!text.contains("must not be reached"));
    }

    fn test_route(route_id: &str) -> RouterRoute {
        let upstream_name = route_id.split(':').next().unwrap_or(route_id).to_string();
        RouterRoute {
            route_id: route_id.to_string(),
            upstream_name,
            candidate: RouteCandidate {
                route_id: route_id.to_string(),
                format: ProviderFormat::Openai,
                engine: None,
            },
        }
    }

    fn test_in_flight(
        state: Arc<Mutex<RouterState>>,
        selection: &RouteSelection,
        routing_text: &str,
    ) -> RouteInFlight {
        RouteInFlight::from_reserved(
            state,
            RouterMetrics::new().expect("test router metrics"),
            selection,
            "m".to_string(),
            routing_text.to_string(),
            16,
        )
    }

    fn test_metrics(
        observed_running: f64,
        observed_waiting: f64,
        global_limit: f64,
        basic_limit: f64,
        basic_inflight: f64,
    ) -> UpstreamMetrics {
        UpstreamMetrics {
            ok: true,
            updated_at: Some(Instant::now()),
            observed_running: Some(observed_running),
            observed_waiting: Some(observed_waiting),
            global_limit: Some(global_limit),
            basic_limit: Some(basic_limit),
            basic_inflight: Some(basic_inflight),
            premium_inflight: Some(0.0),
            error: None,
            ..Default::default()
        }
    }

    fn request_aware_metrics(
        observed_running: f64,
        observed_waiting: f64,
        backpressure_applied: bool,
        global_limit: f64,
    ) -> UpstreamMetrics {
        UpstreamMetrics {
            ok: true,
            updated_at: Some(Instant::now()),
            raw_observed_running: Some(observed_running),
            raw_observed_waiting: Some(observed_waiting),
            observed_running: Some(observed_running),
            observed_waiting: Some(observed_waiting),
            raw_global_limit: Some(1.0),
            global_limit: Some(global_limit),
            predictive_admission_enforce: Some(true),
            router_backpressure_active: Some(backpressure_applied),
            router_backpressure_applied: Some(backpressure_applied),
            router_inspect_capacity: Some(if backpressure_applied {
                (global_limit - observed_running).max(0.0)
            } else {
                0.0
            }),
            basic_limit: Some(511.0),
            basic_inflight: Some(0.0),
            premium_inflight: Some(0.0),
            error: None,
            ..Default::default()
        }
    }
}
