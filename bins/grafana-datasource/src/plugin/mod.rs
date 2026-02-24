mod query;
mod error;
mod frame;
mod data_service;
mod stream_service;
mod resource_service;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use grafana_plugin_sdk::backend::{self, async_trait};
use grafana_plugin_sdk::prelude::*;

use query::StreamQuery;

// ═══════════════════════════════════════════════════════════════
//  Plugin struct
// ═══════════════════════════════════════════════════════════════

#[derive(Clone, Debug, GrafanaPlugin)]
#[grafana_plugin(plugin_type = "datasource")]
pub struct QuotesPlugin {
    queries: Arc<RwLock<HashMap<String, StreamQuery>>>,
}

impl QuotesPlugin {
    pub fn new() -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

// ═══════════════════════════════════════════════════════════════
//  DiagnosticsService
// ═══════════════════════════════════════════════════════════════

#[async_trait]
impl backend::DiagnosticsService for QuotesPlugin {
    type CheckHealthError = std::convert::Infallible;
    type CollectMetricsError = std::convert::Infallible;

    async fn check_health(
        &self,
        request: backend::CheckHealthRequest<Self>,
    ) -> Result<backend::CheckHealthResponse, Self::CheckHealthError> {
        let base_url = extract_url(request.plugin_context.instance_settings.as_ref());
        let url = format!("{base_url}/api/topics");

        let http = reqwest::Client::new();
        match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                Ok(backend::CheckHealthResponse::ok(format!(
                    "Connected to gauss-server at {base_url}",
                )))
            }
            Ok(resp) => Ok(backend::CheckHealthResponse::error(format!(
                "gauss-server returned HTTP {}: {}",
                resp.status(),
                base_url,
            ))),
            Err(e) => Ok(backend::CheckHealthResponse::error(format!(
                "Failed to connect to gauss-server at {base_url}: {e}",
            ))),
        }
    }

    async fn collect_metrics(
        &self,
        _request: backend::CollectMetricsRequest<Self>,
    ) -> Result<backend::CollectMetricsResponse, Self::CollectMetricsError> {
        Ok(backend::CollectMetricsResponse::new(None))
    }
}

// ═══════════════════════════════════════════════════════════════
//  Shared helper
// ═══════════════════════════════════════════════════════════════

fn extract_url(
    settings: Option<
        &backend::DataSourceInstanceSettings<serde_json::Value, serde_json::Value>,
    >,
) -> String {
    settings
        .and_then(|s| s.json_data.get("url"))
        .and_then(|v| v.as_str())
        .unwrap_or("http://gauss-server:9200")
        .to_string()
}
