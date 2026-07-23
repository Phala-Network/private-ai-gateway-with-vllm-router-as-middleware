//! Optional post-request usage reporter for router middleware.
//!
//! The router does not use PAG's pre-consult control-plane mode for routing, but
//! it can still send the same `/consult/post` report shape when a control URL is
//! configured. Reporting is best-effort and never fails an already-served
//! response.

use std::time::Duration;

use super::config::MiddlewareConfig;
use super::types::PostReport;

const DEFAULT_CONTROL_POST_TIMEOUT_MS: u64 = 10_000;

#[derive(Clone)]
pub struct ControlClient {
    client: reqwest::Client,
    base_url: String,
    token: Option<String>,
    post_timeout: Duration,
}

impl ControlClient {
    pub fn from_config(config: &MiddlewareConfig) -> Result<Option<Self>, String> {
        let Some(control_url) = config
            .control_url
            .as_deref()
            .map(str::trim)
            .filter(|url| !url.is_empty())
        else {
            return Ok(None);
        };
        let client = reqwest::Client::builder()
            .build()
            .map_err(|err| format!("failed to build control HTTP client: {err}"))?;
        Ok(Some(Self {
            client,
            base_url: control_url.trim_end_matches('/').to_string(),
            token: config
                .control_token
                .as_ref()
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty()),
            post_timeout: Duration::from_millis(
                config
                    .control_post_timeout_ms
                    .unwrap_or(DEFAULT_CONTROL_POST_TIMEOUT_MS),
            ),
        }))
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn authorize(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(token) => builder.header("authorization", format!("Bearer {token}")),
            None => builder,
        }
    }

    async fn send_idempotent(
        &self,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> reqwest::Result<reqwest::Response> {
        match build().send().await {
            Ok(response) => Ok(response),
            Err(err) if !err.is_timeout() && !err.is_connect() => {
                tracing::debug!(
                    error = %err,
                    "control post failed on a pooled connection; retrying once"
                );
                build().send().await
            }
            Err(err) => Err(err),
        }
    }

    pub async fn consult_post(&self, report: &PostReport) {
        let build = || {
            self.authorize(self.client.post(self.url("/consult/post")))
                .timeout(self.post_timeout)
                .json(report)
        };
        if let Err(err) = self.send_idempotent(build).await {
            tracing::error!(error = %err, "consult_post request failed");
        }
    }
}
