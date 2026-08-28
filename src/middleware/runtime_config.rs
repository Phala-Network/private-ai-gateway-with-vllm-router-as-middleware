use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::config::MiddlewareConfig;

const MAX_HISTORY_PER_ROUTE: usize = 65_536;
const MAX_FORWARD_CANDIDATES: usize = 6;
const MAX_METRICS_PATH_BYTES: usize = 128;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct RouterConfigPatch {
    /// Restore the immutable startup values and remove the persisted override.
    pub reset: bool,
    pub cache_threshold: Option<f32>,
    pub balance_abs_threshold: Option<usize>,
    pub balance_rel_threshold: Option<f32>,
    pub max_history_per_route: Option<usize>,
    pub max_forward_candidates: Option<usize>,
    pub metrics_poll_ms: Option<u64>,
    pub metrics_timeout_ms: Option<u64>,
    pub metrics_stale_ms: Option<u64>,
    pub metrics_path: Option<String>,
    pub sse_keepalive_ms: Option<u64>,
}

impl RouterConfigPatch {
    fn has_updates(&self) -> bool {
        self.cache_threshold.is_some()
            || self.balance_abs_threshold.is_some()
            || self.balance_rel_threshold.is_some()
            || self.max_history_per_route.is_some()
            || self.max_forward_candidates.is_some()
            || self.metrics_poll_ms.is_some()
            || self.metrics_timeout_ms.is_some()
            || self.metrics_stale_ms.is_some()
            || self.metrics_path.is_some()
            || self.sse_keepalive_ms.is_some()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct RouterTuningConfig {
    cache_threshold: f32,
    balance_abs_threshold: usize,
    balance_rel_threshold: f32,
    max_history_per_route: usize,
    max_forward_candidates: usize,
    metrics_poll_ms: u64,
    metrics_timeout_ms: u64,
    metrics_stale_ms: u64,
    metrics_path: String,
    sse_keepalive_ms: u64,
}

impl RouterTuningConfig {
    fn from_config(config: &MiddlewareConfig) -> Self {
        Self {
            cache_threshold: config.cache_threshold,
            balance_abs_threshold: config.balance_abs_threshold,
            balance_rel_threshold: config.balance_rel_threshold,
            max_history_per_route: config.max_history_per_route,
            max_forward_candidates: config.max_forward_candidates,
            metrics_poll_ms: config.metrics_poll_ms,
            metrics_timeout_ms: config.metrics_timeout_ms,
            metrics_stale_ms: config.metrics_stale_ms,
            metrics_path: config.metrics_path.clone(),
            sse_keepalive_ms: config.sse_keepalive_ms.unwrap_or(10_000),
        }
    }

    fn apply_patch(&mut self, patch: &RouterConfigPatch) {
        if let Some(value) = patch.cache_threshold {
            self.cache_threshold = value;
        }
        if let Some(value) = patch.balance_abs_threshold {
            self.balance_abs_threshold = value;
        }
        if let Some(value) = patch.balance_rel_threshold {
            self.balance_rel_threshold = value;
        }
        if let Some(value) = patch.max_history_per_route {
            self.max_history_per_route = value;
        }
        if let Some(value) = patch.max_forward_candidates {
            self.max_forward_candidates = value;
        }
        if let Some(value) = patch.metrics_poll_ms {
            self.metrics_poll_ms = value;
        }
        if let Some(value) = patch.metrics_timeout_ms {
            self.metrics_timeout_ms = value;
        }
        if let Some(value) = patch.metrics_stale_ms {
            self.metrics_stale_ms = value;
        }
        if let Some(value) = patch.metrics_path.as_ref() {
            self.metrics_path = value.clone();
        }
        if let Some(value) = patch.sse_keepalive_ms {
            self.sse_keepalive_ms = value;
        }
    }

    fn apply_to(&self, config: &mut MiddlewareConfig) {
        config.cache_threshold = self.cache_threshold;
        config.balance_abs_threshold = self.balance_abs_threshold;
        config.balance_rel_threshold = self.balance_rel_threshold;
        config.max_history_per_route = self.max_history_per_route;
        config.max_forward_candidates = self.max_forward_candidates;
        config.metrics_poll_ms = self.metrics_poll_ms;
        config.metrics_timeout_ms = self.metrics_timeout_ms;
        config.metrics_stale_ms = self.metrics_stale_ms;
        config.metrics_path.clone_from(&self.metrics_path);
        config.sse_keepalive_ms = Some(self.sse_keepalive_ms);
    }

    fn validate(&self) -> Result<(), String> {
        if !self.cache_threshold.is_finite() || !(0.0..=1.0).contains(&self.cache_threshold) {
            return Err("cache_threshold must be finite and between 0 and 1".to_string());
        }
        if !self.balance_rel_threshold.is_finite()
            || !(1.0..=100.0).contains(&self.balance_rel_threshold)
        {
            return Err("balance_rel_threshold must be finite and between 1 and 100".to_string());
        }
        if self.max_history_per_route > MAX_HISTORY_PER_ROUTE {
            return Err(format!(
                "max_history_per_route must not exceed {MAX_HISTORY_PER_ROUTE}"
            ));
        }
        if !(1..=MAX_FORWARD_CANDIDATES).contains(&self.max_forward_candidates) {
            return Err(format!(
                "max_forward_candidates must be between 1 and {MAX_FORWARD_CANDIDATES}"
            ));
        }
        if self.metrics_poll_ms != 0 && !(100..=60_000).contains(&self.metrics_poll_ms) {
            return Err("metrics_poll_ms must be 0 or between 100 and 60000".to_string());
        }
        if !(50..=30_000).contains(&self.metrics_timeout_ms) {
            return Err("metrics_timeout_ms must be between 50 and 30000".to_string());
        }
        if !(100..=300_000).contains(&self.metrics_stale_ms) {
            return Err("metrics_stale_ms must be between 100 and 300000".to_string());
        }
        if self.metrics_poll_ms != 0 && self.metrics_stale_ms < self.metrics_timeout_ms {
            return Err("metrics_stale_ms must be at least metrics_timeout_ms".to_string());
        }
        validate_metrics_path(&self.metrics_path)?;
        if self.sse_keepalive_ms != 0 && !(1_000..=300_000).contains(&self.sse_keepalive_ms) {
            return Err("sse_keepalive_ms must be 0 or between 1000 and 300000".to_string());
        }
        Ok(())
    }
}

fn validate_metrics_path(path: &str) -> Result<(), String> {
    if path.is_empty()
        || !path.starts_with('/')
        || path.len() > MAX_METRICS_PATH_BYTES
        || path.contains("//")
        || path.contains('?')
        || path.contains('#')
        || path.contains("..")
        || path.chars().any(char::is_whitespace)
    {
        return Err(
            "metrics_path must be a short absolute path without query, fragment, traversal, or whitespace"
                .to_string(),
        );
    }
    Ok(())
}

#[derive(Debug)]
pub enum RouterConfigUpdateError {
    Invalid(String),
    Persist(String),
}

struct ActiveConfig {
    config: Arc<MiddlewareConfig>,
    tuning: RouterTuningConfig,
    revision: u64,
    source: &'static str,
}

pub(super) struct RouterConfigStore {
    baseline: MiddlewareConfig,
    override_path: Option<PathBuf>,
    active: RwLock<ActiveConfig>,
}

impl RouterConfigStore {
    pub(super) fn load(
        baseline: &MiddlewareConfig,
        override_path: Option<PathBuf>,
    ) -> Result<Self, String> {
        let baseline_tuning = RouterTuningConfig::from_config(baseline);
        baseline_tuning.validate()?;
        let (tuning, source) = match override_path.as_deref() {
            Some(path) if path.exists() => {
                let body = fs::read(path).map_err(|err| {
                    format!(
                        "failed to read router runtime config {}: {err}",
                        path.display()
                    )
                })?;
                let tuning: RouterTuningConfig = serde_json::from_slice(&body).map_err(|err| {
                    format!(
                        "failed to parse router runtime config {}: {err}",
                        path.display()
                    )
                })?;
                tuning.validate()?;
                (tuning, "runtime_override")
            }
            _ => (baseline_tuning, "startup"),
        };
        let config = Arc::new(config_with_tuning(baseline, &tuning));
        Ok(Self {
            baseline: baseline.clone(),
            override_path,
            active: RwLock::new(ActiveConfig {
                config,
                tuning,
                revision: 0,
                source,
            }),
        })
    }

    pub(super) fn snapshot(&self) -> Arc<MiddlewareConfig> {
        self.active
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .config
            .clone()
    }

    pub(super) fn metadata_json(&self) -> Value {
        let active = self
            .active
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        json!({
            "source": active.source,
            "revision": active.revision,
            "persistent": self.override_path.is_some(),
        })
    }

    pub(super) fn patch(
        &self,
        patch: RouterConfigPatch,
    ) -> Result<Arc<MiddlewareConfig>, RouterConfigUpdateError> {
        if patch.reset && patch.has_updates() {
            return Err(RouterConfigUpdateError::Invalid(
                "reset cannot be combined with parameter updates".to_string(),
            ));
        }
        if !patch.reset && !patch.has_updates() {
            return Err(RouterConfigUpdateError::Invalid(
                "at least one router tuning parameter is required".to_string(),
            ));
        }

        let mut active = self
            .active
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let next_tuning = if patch.reset {
            RouterTuningConfig::from_config(&self.baseline)
        } else {
            let mut tuning = active.tuning.clone();
            tuning.apply_patch(&patch);
            tuning
        };
        next_tuning
            .validate()
            .map_err(RouterConfigUpdateError::Invalid)?;

        if let Some(path) = self.override_path.as_deref() {
            if patch.reset {
                remove_override(path).map_err(RouterConfigUpdateError::Persist)?;
            } else {
                persist_override(path, &next_tuning).map_err(RouterConfigUpdateError::Persist)?;
            }
        }

        let next = Arc::new(config_with_tuning(&self.baseline, &next_tuning));
        active.config = next.clone();
        active.tuning = next_tuning;
        active.revision = active.revision.saturating_add(1);
        active.source = if patch.reset {
            "startup"
        } else {
            "runtime_override"
        };
        Ok(next)
    }
}

fn config_with_tuning(
    baseline: &MiddlewareConfig,
    tuning: &RouterTuningConfig,
) -> MiddlewareConfig {
    let mut config = baseline.clone();
    tuning.apply_to(&mut config);
    config
}

fn persist_override(path: &Path, tuning: &RouterTuningConfig) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create {}: {err}", parent.display()))?;
    }
    let body = serde_json::to_vec_pretty(tuning)
        .map_err(|err| format!("failed to serialize router runtime config: {err}"))?;
    let tmp = path.with_extension("json.tmp");
    let mut file =
        File::create(&tmp).map_err(|err| format!("failed to create {}: {err}", tmp.display()))?;
    file.write_all(&body)
        .and_then(|_| file.sync_all())
        .map_err(|err| format!("failed to write {}: {err}", tmp.display()))?;
    #[cfg(windows)]
    if path.exists() {
        fs::remove_file(path)
            .map_err(|err| format!("failed to replace {}: {err}", path.display()))?;
    }
    fs::rename(&tmp, path).map_err(|err| format!("failed to install {}: {err}", path.display()))
}

fn remove_override(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("failed to remove {}: {err}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "pag-router-runtime-{name}-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn patch_is_atomic_persistent_and_resettable() {
        let path = temp_path("patch");
        let baseline = MiddlewareConfig::default();
        let store = RouterConfigStore::load(&baseline, Some(path.clone())).unwrap();
        store
            .patch(RouterConfigPatch {
                balance_abs_threshold: Some(16),
                max_forward_candidates: Some(6),
                metrics_path: Some("/pig/metrics".to_string()),
                ..Default::default()
            })
            .unwrap();
        let active = store.snapshot();
        assert_eq!(active.balance_abs_threshold, 16);
        assert_eq!(active.max_forward_candidates, 6);
        assert_eq!(active.metrics_path, "/pig/metrics");

        let reloaded = RouterConfigStore::load(&baseline, Some(path.clone())).unwrap();
        assert_eq!(reloaded.snapshot().balance_abs_threshold, 16);
        reloaded
            .patch(RouterConfigPatch {
                reset: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(reloaded.snapshot().balance_abs_threshold, 64);
        assert!(!path.exists());
    }

    #[test]
    fn patch_rejects_unsafe_or_ambiguous_values() {
        let store = RouterConfigStore::load(&MiddlewareConfig::default(), None).unwrap();
        for patch in [
            RouterConfigPatch {
                max_forward_candidates: Some(0),
                ..Default::default()
            },
            RouterConfigPatch {
                metrics_path: Some("https://evil.example/metrics".to_string()),
                ..Default::default()
            },
            RouterConfigPatch {
                reset: true,
                balance_abs_threshold: Some(1),
                ..Default::default()
            },
        ] {
            assert!(matches!(
                store.patch(patch),
                Err(RouterConfigUpdateError::Invalid(_))
            ));
        }
    }
}
