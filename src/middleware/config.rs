//! Configuration for the middleware.
//!
//! Selected through the gateway's optional `middleware` config section. This
//! fork intentionally supports one middleware shape: one public model routed
//! across multiple configured upstreams with cache-aware and load-aware ordering.

use serde::{Deserialize, Serialize};

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
    pub default_engine: Option<Engine>,
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
            default_engine: None,
            sse_keepalive_ms: None,
        }
    }
}
