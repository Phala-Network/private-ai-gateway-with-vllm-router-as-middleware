//! Router middleware data types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Request endpoint, used for route text extraction and response handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    ChatComplete,
    Complete,
    Embed,
    Messages,
    CreateModelResponse,
}

/// Opaque pricing block. Carried verbatim until cost computation.
pub type PricingConfig = Value;

/// Which API format parses a candidate's response.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProviderFormat {
    Openai,
    Anthropic,
}

/// Serving engine hint retained for config compatibility.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    Sglang,
    Vllm,
}

/// Billing mode, carried into the optional post-request usage report.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SpendMode {
    Regular,
    Subscription,
    SubscriptionOverflow,
}

/// One ordered failover candidate: a backend route id plus response format.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RouteCandidate {
    /// `<upstream name>:<public model id>`, aligned with the backend's upstreams.
    pub route_id: String,
    /// API format used only when response adaptation is required.
    pub format: ProviderFormat,
    /// Retained for config compatibility; transparent routing does not shape
    /// request bodies from this hint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<Engine>,
}

/// Which component a gateway-synthesized failure is attributed to in reports.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ErrorSource {
    Control,
    Upstream,
    Gateway,
}

/// Post-request usage report. Fire-and-forget; mirrors PAG control mode.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostReport {
    pub request_id: String,
    pub endpoint: String,
    pub status: u16,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttft_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_streaming: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_index: Option<u32>,
    pub selected_route_id: Option<String>,
    pub request_model: String,
    pub usage: Option<Value>,
    pub pricing: Option<PricingConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spend_mode: Option<SpendMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub virtual_key_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_source: Option<ErrorSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}
