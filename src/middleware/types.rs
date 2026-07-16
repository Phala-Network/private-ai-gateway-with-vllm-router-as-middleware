//! Router middleware request-shaping types.

use serde::{Deserialize, Serialize};

/// Which API format shapes a candidate's request and parses its response.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProviderFormat {
    Openai,
    Anthropic,
}

/// Serving engine of a self-hosted OpenAI-compatible upstream. Selects
/// engine-specific request shaping; absent for managed third-party APIs.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    Sglang,
    Vllm,
}

/// One ordered failover candidate: a backend route id plus the upstream format.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RouteCandidate {
    /// `<upstream name>:<public model id>`, aligned with the backend's upstreams.
    pub route_id: String,
    /// API format that shapes the request and parses the response.
    pub format: ProviderFormat,
    /// Serving engine when this upstream is a self-hosted OpenAI-compatible
    /// server. Absent for managed APIs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<Engine>,
}
