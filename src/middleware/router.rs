//! In-process cache-aware router middleware for one public model.
//!
//! PAG still performs the verified upstream forward and receipt finalization.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::{
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{json, Value};

use crate::aggregator::service::{AciService, MiddlewareAttemptObserver};
use crate::aggregator::upstream_config::{
    PublicUpstreamConfig, UpstreamConfigManager, UpstreamConfigSnapshot, UpstreamMetricsTarget,
    UpstreamProvider,
};

use super::cache_index::CacheIndex;
use super::completion::{self, CompletionInput};
use super::config::MiddlewareConfig;
use super::control::ControlClient;
use super::errors::{self, Surface};
use super::types::{Endpoint, ProviderFormat, RouteCandidate};

const MAX_ROUTING_HISTORY_CHARS: usize = 16_384;
const UPSTREAM_STATUS_GREEN: u8 = 0;
const UPSTREAM_STATUS_YELLOW: u8 = 1;
const UPSTREAM_STATUS_RED: u8 = 2;
const PIG_PRESSURE_PASSTHROUGH_REASON: &str = "pig_pressure_passthrough";

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
}

#[derive(Default)]
struct RouterState {
    stats: HashMap<String, RouteStats>,
    cache_index: CacheIndex,
    upstream_metrics: HashMap<String, UpstreamMetrics>,
    dispatch_ledgers: HashMap<String, DispatchLedger>,
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

type RouteOrderKey = (u8, u8, u8, u64, u64, u64, u64, String);

pub(super) struct RouterBackend {
    upstream_config: Arc<UpstreamConfigManager>,
    config: MiddlewareConfig,
    state: Arc<Mutex<RouterState>>,
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
}

impl RouterBackend {
    pub fn new(
        config: &MiddlewareConfig,
        upstream_config: Arc<UpstreamConfigManager>,
    ) -> Result<Self, String> {
        if config
            .public_model
            .as_deref()
            .is_some_and(|model| model.trim().is_empty())
        {
            return Err("middleware.public_model must not be empty".to_string());
        }
        let state = Arc::new(Mutex::new(RouterState::default()));
        spawn_metrics_poller(config.clone(), upstream_config.clone(), state.clone());
        Ok(Self {
            upstream_config,
            config: config.clone(),
            state,
        })
    }

    fn public_model(&self, snapshot: &UpstreamConfigSnapshot) -> Result<Option<String>, String> {
        if let Some(model) = self.config.public_model.as_deref() {
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

    fn model_routes(&self, model: &str) -> Vec<RouterRoute> {
        let snapshot = self.upstream_config.snapshot();
        let mut routes = Vec::new();
        for upstream in snapshot.upstreams {
            if upstream.enabled && upstream.models.contains_key(model) {
                routes.push(route_from_upstream(&upstream, model, &self.config));
            }
        }
        routes
    }

    fn ordered_routes(
        &self,
        public_model: &str,
        input: &CompletionInput,
    ) -> (Vec<RouterRoute>, Option<RouteSelection>, usize) {
        let tier = self.request_tier(input);
        let requested_model = input.params.get("model").and_then(Value::as_str);
        if requested_model != Some(public_model) {
            return (Vec::new(), None, 0);
        }

        let mut routes = self.model_routes(public_model);
        let configured_count = routes.len();
        let routing_text = bounded_routing_text(&input.params, input.endpoint);
        let selection = {
            let mut state = self.state.lock().expect("router state poisoned");
            let selected = state.select(public_model, &routing_text, &routes, &self.config, tier);
            selected.map(|selected| {
                // Selection and the local reservation are one transaction. A
                // second dispatcher must observe this request before it can
                // consume the same PIG capacity snapshot.
                let selectable = state
                    .selectable_route_ids(&routes, &self.config, tier)
                    .collect::<HashSet<_>>();
                let selected_is_normally_selectable = selectable.contains(&selected.route_id);
                let candidate_route_ids = if selected_is_normally_selectable {
                    selectable
                } else {
                    routes
                        .iter()
                        .map(|route| route.route_id.clone())
                        .collect::<HashSet<_>>()
                };
                let pressure_order_keys = if selected.reason == PIG_PRESSURE_PASSTHROUGH_REASON {
                    let load_order = state.load_order(&routes, &self.config, tier);
                    routes
                        .iter()
                        .filter(|route| candidate_route_ids.contains(&route.route_id))
                        .map(|route| {
                            (
                                route.route_id.clone(),
                                state.route_order_key(route, &self.config, tier, load_order),
                            )
                        })
                        .collect::<HashMap<_, _>>()
                } else {
                    HashMap::new()
                };
                let loads = routes
                    .iter()
                    .filter(|route| candidate_route_ids.contains(&route.route_id))
                    .map(|route| {
                        (
                            route.route_id.clone(),
                            state
                                .route_pressure(route, &self.config, tier)
                                .effective_running,
                        )
                    })
                    .collect::<HashMap<_, _>>();
                state.mark_started(&selected);
                (selected, candidate_route_ids, loads, pressure_order_keys)
            })
        };
        let Some((selected, candidate_route_ids, loads, pressure_order_keys)) = selection else {
            return (Vec::new(), None, configured_count);
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
            let a_load = loads.get(&a.route_id).copied().unwrap_or(0);
            let b_load = loads.get(&b.route_id).copied().unwrap_or(0);
            a_load
                .cmp(&b_load)
                .then_with(|| a.route_id.cmp(&b.route_id))
        });
        (routes, Some(selected), configured_count)
    }

    pub(super) fn admin_snapshot_value(&self) -> Value {
        let upstream_snapshot = self.upstream_config.snapshot();
        let public_model = self.public_model(&upstream_snapshot);
        let state = self.state.lock().expect("router state poisoned");
        let mut routes = Vec::new();
        for upstream in &upstream_snapshot.upstreams {
            for model in upstream.models.keys() {
                let route_id = format!("{}:{model}", upstream.name);
                let stats = state.stats.get(&route_id).cloned().unwrap_or_default();
                let cache_stats = state.cache_index.route_stats(model, &route_id);
                let route = route_from_upstream(upstream, model, &self.config);
                let pressure = state.route_pressure(&route, &self.config, UserTier::Basic);
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
                    "selectable": !pressure.blocked,
                    "pressure_passthrough_eligible": pressure.blocked,
                    "effective_running": pressure.effective_running,
                    "pending_reservations": pressure.pending_reservations,
                    "unreconciled_dispatches": pressure.unreconciled_dispatches,
                    "fullness_milli": pressure.fullness_milli,
                    "bearer_token_configured": upstream.bearer_token_configured,
                    "pig_metrics": state.metrics_admin_json(&upstream.name, &self.config),
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
            "config": self.config,
            "routing_text_max_chars": MAX_ROUTING_HISTORY_CHARS,
            "cache_index": state.cache_index_admin_json(),
            "public_model": public_model.as_ref().ok().and_then(Clone::clone),
            "config_error": public_model.err(),
            "upstream_config_digest": upstream_snapshot.config_digest,
            "routes": routes,
        })
    }

    pub(super) fn upstream_status_code(&self) -> u8 {
        let upstream_snapshot = self.upstream_config.snapshot();
        let public_model = match self.public_model(&upstream_snapshot) {
            Ok(Some(model)) => model,
            Ok(None) | Err(_) => return UPSTREAM_STATUS_RED,
        };
        let routes = upstream_snapshot
            .upstreams
            .iter()
            .filter(|upstream| upstream.enabled && upstream.models.contains_key(&public_model))
            .map(|upstream| route_from_upstream(upstream, &public_model, &self.config))
            .collect::<Vec<_>>();
        let state = self.state.lock().expect("router state poisoned");
        state.upstream_status_code(&routes, &self.config, UserTier::Basic)
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
        let snapshot = self.upstream_config.snapshot();
        let public_model = match self.public_model(&snapshot) {
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
        let snapshot = self.upstream_config.snapshot();
        let requested_model = input
            .params
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_string);
        let public_model = match self.public_model(&snapshot) {
            Ok(Some(model)) => model,
            // With no enabled upstream, a single-model Router cannot derive the
            // model catalog. Treat the requested model as temporarily
            // unavailable so clients see the same capacity 429 they would get
            // from PIG, instead of a malformed/unroutable request error.
            Ok(None) => requested_model.clone().unwrap_or_default(),
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
        let requested_public_model = !public_model.is_empty()
            && requested_model
                .as_deref()
                .is_some_and(|model| model == public_model);
        let (routes, selected, configured_count) = self.ordered_routes(&public_model, &input);
        let user_tier = self.request_tier(&input);
        if !self.config.trusted_user_tier_header {
            input.user_tier = None;
        }
        if requested_public_model && selected.is_none() {
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
        let route_in_flight = selected
            .as_ref()
            .map(|selection| RouteInFlight::from_reserved(self.state.clone(), selection));
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

        completion::run(
            service,
            self.config.sse_keepalive_ms,
            control,
            self.config.pricing.clone(),
            input,
            routes.into_iter().map(|route| route.candidate).collect(),
            route_in_flight,
        )
        .await
    }

    fn request_tier(&self, input: &CompletionInput) -> UserTier {
        if self.config.trusted_user_tier_header {
            UserTier::from_header(input.user_tier.as_deref())
        } else {
            UserTier::Basic
        }
    }
}

impl RouteInFlight {
    fn from_reserved(state: Arc<Mutex<RouterState>>, selection: &RouteSelection) -> Self {
        Self {
            route_id: Some(selection.route_id.clone()),
            pending_dispatch: true,
            state,
        }
    }

    pub(super) fn retarget(&mut self, route_id: &str) {
        self.move_reservation(route_id, false);
    }

    fn move_reservation(&mut self, route_id: &str, record_dispatch: bool) {
        if self.route_id.as_deref() == Some(route_id) {
            if record_dispatch {
                let mut state = self.state.lock().expect("router state poisoned");
                if self.pending_dispatch {
                    state.release_reservation_for_route(route_id);
                    self.pending_dispatch = false;
                }
                state.record_dispatch_for_route(route_id);
            }
            return;
        }
        let move_pending_dispatch = self.pending_dispatch;
        let mut state = self.state.lock().expect("router state poisoned");
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
    fn attempt_started(&mut self, route_id: &str) {
        self.move_reservation(route_id, true);
    }

    fn attempt_response(&mut self, route_id: &str, status: u16) {
        let recorded = {
            let mut state = self.state.lock().expect("router state poisoned");
            state.record_attempt_response(route_id, status)
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
}

impl Drop for RouteInFlight {
    fn drop(&mut self) {
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

    fn record_attempt_response(&mut self, route_id: &str, status: u16) -> Option<(u64, usize)> {
        if status != 429 {
            return None;
        }
        let upstream_429_total = {
            let stats = self.stats.entry(route_id.to_string()).or_default();
            stats.upstream_429 = stats.upstream_429.saturating_add(1);
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
        let (primary_load, secondary_load) = match load_order {
            LoadOrder::CapacityNormalized => {
                (pressure.fullness_milli, pressure.effective_running as u64)
            }
            LoadOrder::Running => (pressure.effective_running as u64, pressure.fullness_milli),
        };
        (
            u8::from(pressure.blocked),
            u8::from(pressure.metrics_error),
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
        !pressure.blocked
    }

    fn selectable_route_ids<'a>(
        &'a self,
        routes: &'a [RouterRoute],
        config: &'a MiddlewareConfig,
        tier: UserTier,
    ) -> impl Iterator<Item = String> + 'a {
        routes
            .iter()
            .filter(move |route| self.route_selectable(route, config, tier))
            .map(|route| route.route_id.clone())
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
        let Some(metrics) = self.upstream_metrics.get(&route.upstream_name) else {
            return RoutePressure {
                blocked: false,
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
        if metrics.is_stale(config) {
            return RoutePressure {
                blocked: false,
                metrics_missing: true,
                metrics_error: false,
                waiting: 0,
                fullness_milli: 0,
                effective_running: fallback_effective_running,
                pending_reservations,
                unreconciled_dispatches,
                processed: stats.processed,
            };
        }
        if !metrics.ok {
            return RoutePressure {
                blocked: false,
                metrics_missing: true,
                metrics_error: true,
                waiting: 0,
                fullness_milli: 0,
                effective_running: fallback_effective_running,
                pending_reservations,
                unreconciled_dispatches,
                processed: stats.processed,
            };
        }

        let observed_running = metrics.observed_running.unwrap_or(0.0).max(0.0);
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
        let inconsistent_request_aware_projection =
            metrics.request_aware_projection_is_inconsistent();
        RoutePressure {
            blocked: inconsistent_request_aware_projection
                || observed_waiting > 0.0
                || tier_fullness >= 1_000,
            metrics_missing: false,
            metrics_error: false,
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
        if pressure.blocked || pressure.waiting > 0 || pressure.fullness_milli >= 1_000 {
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

    fn select(
        &mut self,
        model: &str,
        text: &str,
        routes: &[RouterRoute],
        config: &MiddlewareConfig,
        tier: UserTier,
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
            return self.select_pressure_passthrough(model, text, routes, config, tier);
        }
        if selectable_routes.len() == 1 {
            let route_id = selectable_routes[0].route_id.clone();
            let running_at_select = self.stats.get(&route_id).map_or(0, |s| s.running);
            if active_routes.len() > 1 && !text.is_empty() {
                if let Some(matched) = self.cache_index.match_prefix(model, text) {
                    let input_chars = matched.input_chars.max(1);
                    let rate = matched.matched_chars as f32 / input_chars as f32;
                    if rate > config.cache_threshold
                        && matched.route_id != route_id
                        && active_routes.contains(&matched.route_id)
                    {
                        self.stats
                            .entry(matched.route_id)
                            .or_default()
                            .cache_rejected_by_pressure += 1;
                    }
                }
            }
            self.record_cache(model, &route_id, text, config.max_history_per_route);
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
            self.select_cache_aware(model, text, &selectable_routes, config, tier)
        }?;

        self.record_cache(
            model,
            &selected.route_id,
            text,
            config.max_history_per_route,
        );
        Some(selected)
    }

    fn select_pressure_passthrough(
        &mut self,
        model: &str,
        text: &str,
        routes: &[RouterRoute],
        config: &MiddlewareConfig,
        tier: UserTier,
    ) -> Option<RouteSelection> {
        let selected = self.least_loaded(routes, config, tier)?;
        let pressure = self.route_pressure(selected, config, tier);
        if !text.is_empty() {
            self.record_cache(
                model,
                &selected.route_id,
                text,
                config.max_history_per_route,
            );
        }
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
            if let Some(cache_route) = routes
                .iter()
                .find(|route| route.route_id == matched.route_id)
            {
                if rate > config.cache_threshold
                    && self.cache_route_is_acceptable(cache_route, least, config, tier)
                {
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
                }
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
        metrics: UpstreamMetrics,
        dispatch_watermark: u64,
    ) {
        if metrics.ok {
            self.dispatch_ledgers
                .entry(upstream_name.clone())
                .or_default()
                .observe_successful_poll(dispatch_watermark);
        }
        self.update_upstream_metrics(upstream_name, metrics);
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
            updated_at: Some(Instant::now()),
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

fn spawn_metrics_poller(
    config: MiddlewareConfig,
    upstream_config: Arc<UpstreamConfigManager>,
    state: Arc<Mutex<RouterState>>,
) {
    if config.metrics_poll_ms == 0 {
        return;
    }
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(config.metrics_timeout_ms))
            .timeout(Duration::from_millis(config.metrics_timeout_ms))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                tracing::warn!(error = %err, "router middleware could not build metrics client");
                return;
            }
        };
        let poll = Duration::from_millis(config.metrics_poll_ms);
        let mut ticker = tokio::time::interval(poll);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
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
            let fetched = futures_util::future::join_all(targets.into_iter().map(|target| {
                let upstream_name = target.upstream_name.clone();
                let dispatch_watermark = dispatch_watermarks
                    .get(&upstream_name)
                    .copied()
                    .unwrap_or(0);
                let client = &client;
                let config = &config;
                async move {
                    let metrics = fetch_upstream_metrics(client, config, target).await;
                    (upstream_name, dispatch_watermark, metrics)
                }
            }))
            .await;
            for (upstream_name, dispatch_watermark, metrics) in fetched {
                let mut state = state.lock().expect("router state poisoned");
                state.update_upstream_metrics_from_poll(upstream_name, metrics, dispatch_watermark);
            }
            {
                let mut state = state.lock().expect("router state poisoned");
                state.retain_upstream_metrics(&live_names);
            }
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
        drop(RouteInFlight::from_reserved(state.clone(), &first));

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
    fn malformed_request_aware_projection_fails_closed() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = [test_route("new:m")];
        let mut missing_applied = request_aware_metrics(2.0, 0.0, false, 0.0);
        missing_applied.router_backpressure_applied = None;
        state.update_upstream_metrics("new".to_string(), missing_applied);

        let pressure = state.route_pressure(&routes[0], &config, UserTier::Basic);
        assert!(pressure.blocked);
        assert_eq!(
            state.upstream_metrics["new"].capacity_protocol_name(),
            "request_aware_invalid"
        );

        let mut invalid_open = request_aware_metrics(2.0, 0.0, false, 3.0);
        invalid_open.router_backpressure_active = Some(false);
        state.update_upstream_metrics("new".to_string(), invalid_open);
        assert!(
            state
                .route_pressure(&routes[0], &config, UserTier::Basic)
                .blocked
        );
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

        drop(RouteInFlight::from_reserved(state.clone(), &selected));

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
    fn request_scoped_429_debt_does_not_lock_an_open_idle_route() {
        let mut state = RouterState::default();
        let config = MiddlewareConfig::default();
        let routes = vec![test_route("new:m")];
        state.update_upstream_metrics(
            "new".to_string(),
            request_aware_metrics(0.0, 0.0, false, 0.0),
        );
        state.record_dispatch_for_route("new:m");
        state.record_attempt_response("new:m", 429);

        let pressure = state.route_pressure(&routes[0], &config, UserTier::Basic);
        assert!(!pressure.blocked);
        assert_eq!(pressure.unreconciled_dispatches, 1);
        assert!(
            state
                .select("m", "small follow-up", &routes, &config, UserTier::Basic)
                .is_some(),
            "request-scoped protection became an upstream-wide low-flow lock"
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
        let mut guard = RouteInFlight::from_reserved(state.clone(), &selection);
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
        let mut guard = RouteInFlight::from_reserved(state.clone(), &selection);

        assert_eq!(state.lock().unwrap().dispatch_watermark("a"), 0);
        MiddlewareAttemptObserver::attempt_started(&mut guard, "a:m");
        MiddlewareAttemptObserver::attempt_response(&mut guard, "a:m", 429);
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
        }
    }
}
