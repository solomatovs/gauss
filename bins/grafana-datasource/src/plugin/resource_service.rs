use std::collections::HashMap;

use bytes::Bytes;
use grafana_plugin_sdk::backend::{self, async_trait};
use http::Response;

use super::error::ResourceError;
use super::frame::flatten_json_paths;
use super::query::TopicRecord;
use super::{QuotesPlugin, extract_url};

// ═══════════════════════════════════════════════════════════════
//  ResourceService — /fields endpoint for field auto-discovery
// ═══════════════════════════════════════════════════════════════

#[async_trait]
impl backend::ResourceService for QuotesPlugin {
    type Error = ResourceError;
    type InitialResponse = http::Response<Bytes>;
    type Stream = backend::BoxResourceStream<Self::Error>;

    async fn call_resource(
        &self,
        request: backend::CallResourceRequest<Self>,
    ) -> Result<(Self::InitialResponse, Self::Stream), Self::Error> {
        match request.request.uri().path() {
            "/fields" => {
                let base_url = extract_url(request.plugin_context.instance_settings.as_ref());
                let query_str = request.request.uri().query().unwrap_or("");
                let params = parse_query_string(query_str);
                let topic = params.get("topic").cloned().unwrap_or_default();
                let key = params.get("key").cloned().unwrap_or_default();

                if topic.is_empty() {
                    return Err(ResourceError::Http("topic parameter is required".into()));
                }

                let url = if key.is_empty() {
                    format!("{base_url}/api/topics/{topic}?limit=1")
                } else {
                    format!("{base_url}/api/topics/{topic}?key={key}&limit=1")
                };

                let http = reqwest::Client::new();
                let resp = http.get(&url).send().await
                    .map_err(|e| ResourceError::Http(format!("fetch: {e}")))?;

                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(ResourceError::Http(format!("HTTP {status}: {body}")));
                }

                let body = resp.text().await
                    .map_err(|e| ResourceError::Http(format!("read body: {e}")))?;
                let records: Vec<TopicRecord> = serde_json::from_str(&body)
                    .map_err(|e| ResourceError::Http(format!("parse JSON: {e}")))?;

                let fields = if let Some(record) = records.first() {
                    let full = serde_json::json!({
                        "ts_ms": record.ts_ms,
                        "key": &record.key,
                        "value": &record.value,
                    });
                    flatten_json_paths(&full, "")
                } else {
                    Vec::new()
                };

                let json = serde_json::to_vec(&fields)
                    .map_err(|e| ResourceError::Http(format!("serialize: {e}")))?;

                let response = Response::builder()
                    .status(200)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Bytes::from(json))
                    .map_err(|e| ResourceError::Http(e.to_string()))?;

                Ok((response, Box::pin(futures::stream::empty()) as Self::Stream))
            }
            other => Err(ResourceError::NotFound(format!("unknown resource: {other}"))),
        }
    }
}

/// Parse query string into key-value pairs.
fn parse_query_string(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter(|s| !s.is_empty())
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next().unwrap_or("");
            Some((
                urlencoding::decode(key).unwrap_or_default().to_string(),
                urlencoding::decode(value).unwrap_or_default().to_string(),
            ))
        })
        .collect()
}
