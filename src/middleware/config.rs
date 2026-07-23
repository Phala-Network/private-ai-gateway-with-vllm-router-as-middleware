//! Configuration for the middleware.
//!
//! Selected through the gateway's optional `middleware` config section. This
//! fork intentionally supports one middleware shape: one public model routed
//! across multiple configured upstreams with cache-aware and PIG-aware ordering.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::types::Engine;

/// Single-model router middleware settings.
///
/// If `public_model` is unset, the router derives it from the live upstream
/// config and requires exactly one unique public model.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MiddlewareConfig {
    pub public_model: Option<String>,
    pub cache_threshold: f32,
    pub balance_abs_threshold: usize,
    pub balance_rel_threshold: f32,
    pub max_history_per_route: usize,
    /// Background upstream `/v1/metrics` polling interval. `0` disables metric
    /// polling and falls back to gateway-local in-flight routing only.
    pub metrics_poll_ms: u64,
    pub metrics_timeout_ms: u64,
    pub metrics_stale_ms: u64,
    pub metrics_path: String,
    /// Trust inbound `x-user-tier` for routing and upstream forwarding. Keep
    /// disabled unless a trusted front door strips or sets the header.
    pub trusted_user_tier_header: bool,
    pub default_engine: Option<Engine>,
    /// Optional control-plane URL used only for post-request usage reports.
    /// Routing remains fully local to this middleware.
    pub control_url: Option<String>,
    /// Optional bearer token for post-request usage reports.
    pub control_token: Option<String>,
    /// Timeout for the best-effort post-request usage report. Defaults to
    /// 10_000 ms.
    pub control_post_timeout_ms: Option<u64>,
    /// Optional static pricing block used to inject `usage.cost` into client
    /// responses and echoed in `/consult/post` reports.
    pub pricing: Option<Value>,
    /// SSE keep-alive interval for streaming responses. Defaults to 10_000 ms;
    /// `0` disables the heartbeat.
    pub sse_keepalive_ms: Option<u64>,
}

impl Default for MiddlewareConfig {
    fn default() -> Self {
        Self {
            public_model: None,
            cache_threshold: 0.30,
            balance_abs_threshold: 64,
            balance_rel_threshold: 1.50,
            max_history_per_route: 256,
            metrics_poll_ms: 1_000,
            metrics_timeout_ms: 800,
            metrics_stale_ms: 3_000,
            metrics_path: "/v1/metrics".to_string(),
            trusted_user_tier_header: false,
            default_engine: None,
            control_url: None,
            control_token: None,
            control_post_timeout_ms: None,
            pricing: None,
            sse_keepalive_ms: None,
        }
    }
}
