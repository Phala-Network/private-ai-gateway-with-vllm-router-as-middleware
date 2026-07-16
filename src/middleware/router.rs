//! In-process cache-aware router middleware for one public model.
//!
//! PAG still performs the verified upstream forward and receipt finalization.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use axum::{
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{json, Value};

use crate::aggregator::service::AciService;
use crate::aggregator::upstream_config::{
    PublicUpstreamConfig, UpstreamConfigManager, UpstreamConfigSnapshot, UpstreamProvider,
};

use super::completion::{self, CompletionInput};
use super::config::MiddlewareConfig;
use super::errors::{self, Surface};
use super::request_transform::Endpoint;
use super::types::{ProviderFormat, RouteCandidate};

const MAX_ROUTING_HISTORY_CHARS: usize = 16_384;

#[derive(Clone)]
struct RouterRoute {
    route_id: String,
    candidate: RouteCandidate,
}

#[derive(Default, Clone)]
struct RouteStats {
    running: usize,
    processed: u64,
    selected_by_cache: u64,
    selected_by_load: u64,
    selected_by_order: u64,
}

#[derive(Default)]
struct RouterState {
    stats: HashMap<String, RouteStats>,
    history: HashMap<String, HashMap<String, Vec<String>>>,
}

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
        Ok(Self {
            upstream_config,
            config: config.clone(),
            state: Arc::new(Mutex::new(RouterState::default())),
        })
    }

    fn public_model(&self, snapshot: &UpstreamConfigSnapshot) -> Result<Option<String>, String> {
        if let Some(model) = self.config.public_model.as_deref() {
            return Ok(Some(model.trim().to_string()));
        }
        let mut models = BTreeSet::new();
        for upstream in &snapshot.upstreams {
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
            if upstream.models.contains_key(model) {
                routes.push(route_from_upstream(&upstream, model, &self.config));
            }
        }
        routes
    }

    fn ordered_routes(
        &self,
        public_model: &str,
        input: &CompletionInput,
    ) -> (Vec<RouterRoute>, Option<RouteSelection>) {
        let requested_model = input.params.get("model").and_then(Value::as_str);
        if requested_model != Some(public_model) {
            return (Vec::new(), None);
        }

        let mut routes = self.model_routes(public_model);
        let routing_text = bounded_routing_text(&input.params, input.endpoint);
        let selected = {
            let mut state = self.state.lock().expect("router state poisoned");
            state.select(public_model, &routing_text, &routes, &self.config)
        };
        let Some(selected) = selected.clone() else {
            return (routes, None);
        };

        let loads = {
            let state = self.state.lock().expect("router state poisoned");
            routes
                .iter()
                .map(|route| {
                    (
                        route.route_id.clone(),
                        state.stats.get(&route.route_id).map_or(0, |s| s.running),
                    )
                })
                .collect::<HashMap<_, _>>()
        };
        routes.sort_by(|a, b| {
            if a.route_id == selected.route_id {
                return std::cmp::Ordering::Less;
            }
            if b.route_id == selected.route_id {
                return std::cmp::Ordering::Greater;
            }
            let a_load = loads.get(&a.route_id).copied().unwrap_or(0);
            let b_load = loads.get(&b.route_id).copied().unwrap_or(0);
            a_load
                .cmp(&b_load)
                .then_with(|| a.route_id.cmp(&b.route_id))
        });
        (routes, Some(selected))
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
                let history_samples = state
                    .history
                    .get(model)
                    .and_then(|by_route| by_route.get(&route_id))
                    .map_or(0, Vec::len);
                routes.push(json!({
                    "route_id": route_id,
                    "upstream_name": upstream.name,
                    "public_model": model,
                    "provider": provider_name(upstream.provider),
                    "running": stats.running,
                    "processed": stats.processed,
                    "selected_by_cache": stats.selected_by_cache,
                    "selected_by_load": stats.selected_by_load,
                    "selected_by_order": stats.selected_by_order,
                    "history_samples": history_samples,
                    "bearer_token_configured": upstream.bearer_token_configured,
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
            "public_model": public_model.as_ref().ok().and_then(Clone::clone),
            "config_error": public_model.err(),
            "upstream_config_digest": upstream_snapshot.config_digest,
            "routes": routes,
        })
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
        input: CompletionInput,
    ) -> Response {
        let snapshot = self.upstream_config.snapshot();
        let public_model = match self.public_model(&snapshot) {
            Ok(Some(model)) => model,
            Ok(None) => String::new(),
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
        let (routes, selected) = self.ordered_routes(&public_model, &input);
        let route_in_flight = selected
            .as_ref()
            .map(|selection| RouteInFlight::start(self.state.clone(), selection));
        if let Some(selection) = selected.as_ref() {
            tracing::debug!(
                public_model,
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
            input,
            routes.into_iter().map(|route| route.candidate).collect(),
            route_in_flight,
        )
        .await
    }
}

impl RouteInFlight {
    fn start(state: Arc<Mutex<RouterState>>, selection: &RouteSelection) -> Self {
        {
            let mut state_guard = state.lock().expect("router state poisoned");
            state_guard.mark_started(selection);
        }
        Self {
            route_id: Some(selection.route_id.clone()),
            state,
        }
    }

    pub(super) fn retarget(&mut self, route_id: &str) {
        if self.route_id.as_deref() == Some(route_id) {
            return;
        }
        let mut state = self.state.lock().expect("router state poisoned");
        if let Some(previous) = self.route_id.replace(route_id.to_string()) {
            state.decrement_running(&previous);
        }
        state.stats.entry(route_id.to_string()).or_default().running += 1;
    }
}

impl Drop for RouteInFlight {
    fn drop(&mut self) {
        if let Some(route_id) = self.route_id.take() {
            let mut state = self.state.lock().expect("router state poisoned");
            state.decrement_running(&route_id);
        }
    }
}

impl RouterState {
    fn mark_started(&mut self, selection: &RouteSelection) {
        let stats = self.stats.entry(selection.route_id.clone()).or_default();
        stats.running += 1;
        stats.processed += 1;
        match selection.reason {
            "cache" => stats.selected_by_cache += 1,
            "single" => stats.selected_by_order += 1,
            _ => stats.selected_by_load += 1,
        }
    }

    fn decrement_running(&mut self, route_id: &str) {
        if let Some(stats) = self.stats.get_mut(route_id) {
            stats.running = stats.running.saturating_sub(1);
        }
    }

    fn least_loaded<'a>(&self, routes: &'a [RouterRoute]) -> Option<&'a RouterRoute> {
        routes.iter().min_by(|a, b| {
            self.route_order_key(&a.route_id)
                .cmp(&self.route_order_key(&b.route_id))
        })
    }

    fn route_order_key(&self, route_id: &str) -> (usize, u64, String) {
        let stats = self.stats.get(route_id).cloned().unwrap_or_default();
        (stats.running, stats.processed, route_id.to_string())
    }

    fn select(
        &mut self,
        model: &str,
        text: &str,
        routes: &[RouterRoute],
        config: &MiddlewareConfig,
    ) -> Option<RouteSelection> {
        if routes.is_empty() {
            return None;
        }
        for route in routes {
            self.stats.entry(route.route_id.clone()).or_default();
            self.history
                .entry(model.to_string())
                .or_default()
                .entry(route.route_id.clone())
                .or_default();
        }
        if routes.len() == 1 {
            let route_id = routes[0].route_id.clone();
            let running_at_select = self.stats.get(&route_id).map_or(0, |s| s.running);
            self.record_history(model, &route_id, text, config.max_history_per_route);
            return Some(RouteSelection {
                route_id,
                reason: "single",
                cache_match_rate: 0.0,
                running_at_select,
            });
        }

        let (min_load, max_load) = routes
            .iter()
            .fold((usize::MAX, 0usize), |(min, max), route| {
                let load = self.stats.get(&route.route_id).map_or(0, |s| s.running);
                (min.min(load), max.max(load))
            });
        let min_load = if min_load == usize::MAX { 0 } else { min_load };
        let imbalanced = max_load.saturating_sub(min_load) > config.balance_abs_threshold
            && (max_load as f32) > (min_load as f32 * config.balance_rel_threshold);

        let selected = if imbalanced || text.is_empty() {
            self.least_loaded(routes).map(|route| {
                let running_at_select = self.stats.get(&route.route_id).map_or(0, |s| s.running);
                RouteSelection {
                    route_id: route.route_id.clone(),
                    reason: if imbalanced {
                        "load_imbalance"
                    } else {
                        "no_text"
                    },
                    cache_match_rate: 0.0,
                    running_at_select,
                }
            })
        } else {
            self.select_cache_aware(model, text, routes, config)
        }?;

        self.record_history(
            model,
            &selected.route_id,
            text,
            config.max_history_per_route,
        );
        Some(selected)
    }

    fn select_cache_aware(
        &self,
        model: &str,
        text: &str,
        routes: &[RouterRoute],
        config: &MiddlewareConfig,
    ) -> Option<RouteSelection> {
        let input_chars = text.chars().count().max(1);
        let history = self.history.get(model);
        let mut best: Option<(&str, f32, (usize, u64, String))> = None;
        for route in routes {
            let matched = history
                .and_then(|by_route| by_route.get(&route.route_id))
                .map(|samples| {
                    samples
                        .iter()
                        .map(|sample| common_prefix_chars(sample, text))
                        .max()
                        .unwrap_or(0)
                })
                .unwrap_or(0);
            let rate = matched as f32 / input_chars as f32;
            let order_key = self.route_order_key(&route.route_id);
            let replace = match best.as_ref() {
                None => true,
                Some((_, best_rate, best_key)) => {
                    rate > *best_rate || (rate == *best_rate && order_key < *best_key)
                }
            };
            if replace {
                best = Some((&route.route_id, rate, order_key));
            }
        }
        let (route_id, rate, _) = best?;
        if rate > config.cache_threshold {
            let running_at_select = self.stats.get(route_id).map_or(0, |s| s.running);
            Some(RouteSelection {
                route_id: route_id.to_string(),
                reason: "cache",
                cache_match_rate: rate,
                running_at_select,
            })
        } else {
            self.least_loaded(routes).map(|route| {
                let running_at_select = self.stats.get(&route.route_id).map_or(0, |s| s.running);
                RouteSelection {
                    route_id: route.route_id.clone(),
                    reason: "least_running",
                    cache_match_rate: rate,
                    running_at_select,
                }
            })
        }
    }

    fn record_history(&mut self, model: &str, route_id: &str, text: &str, max_samples: usize) {
        if text.is_empty() || max_samples == 0 {
            return;
        }
        let text = limit_chars(text, MAX_ROUTING_HISTORY_CHARS);
        let samples = self
            .history
            .entry(model.to_string())
            .or_default()
            .entry(route_id.to_string())
            .or_default();
        samples.push(text);
        if samples.len() > max_samples {
            let overflow = samples.len() - max_samples;
            samples.drain(0..overflow);
        }
    }
}

fn route_from_upstream(
    upstream: &PublicUpstreamConfig,
    public_model: &str,
    config: &MiddlewareConfig,
) -> RouterRoute {
    let route_id = format!("{}:{public_model}", upstream.name);
    RouterRoute {
        route_id: route_id.clone(),
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

fn common_prefix_chars(a: &str, b: &str) -> usize {
    a.chars().zip(b.chars()).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
            ..Default::default()
        };
        let routes = vec![test_route("a:m"), test_route("b:m")];
        assert_eq!(
            state
                .select("m", "hello world", &routes, &config)
                .map(|s| s.route_id)
                .as_deref(),
            Some("a:m")
        );
        assert_eq!(
            state
                .select("m", "hello there", &routes, &config)
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
            ..Default::default()
        };
        let routes = vec![test_route("a:m"), test_route("b:m")];
        let first = {
            let mut locked = state.lock().unwrap();
            locked.select("m", "aaaa", &routes, &config).unwrap()
        };
        assert_eq!(first.route_id, "a:m");
        drop(RouteInFlight::start(state.clone(), &first));

        let second = {
            let mut locked = state.lock().unwrap();
            locked.select("m", "zzzz", &routes, &config).unwrap()
        };
        assert_eq!(second.route_id, "b:m");
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

        let mut guard = RouteInFlight::start(state.clone(), &selection);
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
    fn routing_history_is_bounded() {
        let mut state = RouterState::default();
        let long = "x".repeat(MAX_ROUTING_HISTORY_CHARS + 10);

        state.record_history("m", "a:m", &long, 16);

        let len = state.history["m"]["a:m"][0].chars().count();
        assert_eq!(len, MAX_ROUTING_HISTORY_CHARS);
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
        RouterRoute {
            route_id: route_id.to_string(),
            candidate: RouteCandidate {
                route_id: route_id.to_string(),
                format: ProviderFormat::Openai,
                engine: None,
            },
        }
    }
}
