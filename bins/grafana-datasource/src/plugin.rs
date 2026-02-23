use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::FuturesOrdered;
use grafana_plugin_sdk::backend::{self, async_trait, BoxDataResponseStream, DataResponse};
use grafana_plugin_sdk::data::Frame;
use grafana_plugin_sdk::live;
use grafana_plugin_sdk::prelude::*;
use http::Response;
use serde::Deserialize;

// ═══════════════════════════════════════════════════════════════
//  TopicRecord — matches the server's API response format
// ═══════════════════════════════════════════════════════════════

#[derive(Debug, Deserialize)]
struct TopicRecord {
    ts_ms: i64,
    #[allow(dead_code)]
    key: String,
    data: RecordDataView,
}

/// Client-side view of RecordData. Handles both new format-aware
/// and legacy (bare JSON value) representations.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RecordDataView {
    /// New format: {"format": "json", "data": {...}} or {"format": "protobuf", "data": "base64..."}
    Formatted {
        #[allow(dead_code)]
        format: String,
        data: serde_json::Value,
    },
    /// Legacy: bare serde_json::Value without format metadata
    Legacy(serde_json::Value),
}

impl RecordDataView {
    /// Get the inner data as a JSON value for field extraction.
    /// For JSON format or legacy: returns the data directly.
    /// For binary formats: returns the base64 string as Value::String.
    fn as_json_value(&self) -> &serde_json::Value {
        match self {
            RecordDataView::Formatted { data, .. } => data,
            RecordDataView::Legacy(v) => v,
        }
    }
}

// ═══════════════════════════════════════════════════════════════
//  Query model — format-aware with per-field configuration
// ═══════════════════════════════════════════════════════════════

/// Supported data formats. Phase 1: only Json is implemented.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    #[default]
    Json,
    Csv,
    Protobuf,
    Avro,
}

/// Connection mode. Http = polling, Tcp = real-time streaming via WS.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionMode {
    #[default]
    Http,
    Tcp,
}

/// Field value type — determines extraction logic and Grafana field type.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum FieldValueType {
    #[default]
    Number,
    String,
    Boolean,
    Time,
}

/// Per-field extraction configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldConfig {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub alias: String,
    #[serde(default)]
    pub r#type: FieldValueType,
}

impl FieldConfig {
    /// Display name: alias if set, otherwise path.
    fn display_name(&self) -> &str {
        if self.alias.is_empty() {
            &self.path
        } else {
            &self.alias
        }
    }
}

/// The query model sent from Grafana frontend.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotesQuery {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    #[allow(dead_code)]
    pub format: DataFormat,
    #[serde(default)]
    pub connection_mode: ConnectionMode,
    #[serde(default)]
    pub field_configs: Vec<FieldConfig>,
    /// @deprecated — legacy comma-separated field names.
    #[serde(default)]
    pub fields: Option<String>,
}

// ═══════════════════════════════════════════════════════════════
//  Error types
// ═══════════════════════════════════════════════════════════════

/// Grafana plugin error for individual queries.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct QueryError {
    pub ref_id: String,
    pub message: String,
}

impl backend::DataQueryError for QueryError {
    fn ref_id(self) -> String {
        self.ref_id
    }
}

/// Error type for streaming operations.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("query not found for path: {0}")]
    QueryNotFound(String),
    #[error("websocket: {0}")]
    WebSocket(String),
    #[error("frame: {0}")]
    Frame(String),
    #[error("json: {0}")]
    Json(String),
}

// ═══════════════════════════════════════════════════════════════
//  Plugin struct with shared state
// ═══════════════════════════════════════════════════════════════

/// Saved query for a streaming channel.
#[derive(Clone, Debug)]
struct StreamQuery {
    base_url: String,
    topic: String,
    key: String,
    field_configs: Vec<FieldConfig>,
}

/// The main plugin struct.
#[derive(Clone, Debug, GrafanaPlugin)]
#[grafana_plugin(plugin_type = "datasource")]
pub struct QuotesPlugin {
    /// Channel path → stream query parameters.
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
//  DataService
// ═══════════════════════════════════════════════════════════════

#[async_trait]
impl backend::DataService for QuotesPlugin {
    type Query = QuotesQuery;
    type QueryError = QueryError;
    type Stream = BoxDataResponseStream<Self::QueryError>;

    async fn query_data(
        &self,
        request: backend::QueryDataRequest<Self::Query, Self>,
    ) -> Self::Stream {
        let base_url = extract_url(request.plugin_context.instance_settings.as_ref());
        let ds_uid = request
            .plugin_context
            .instance_settings
            .as_ref()
            .map(|s| s.uid.clone())
            .unwrap_or_default();
        let http = reqwest::Client::new();
        let queries_map = self.queries.clone();

        Box::pin(
            request
                .queries
                .into_iter()
                .map(|q| {
                    let http = http.clone();
                    let base_url = base_url.clone();
                    let ds_uid = ds_uid.clone();
                    let queries_map = queries_map.clone();
                    async move {
                        match q.query.connection_mode {
                            ConnectionMode::Http => {
                                handle_query(&http, &base_url, &q).await
                            }
                            ConnectionMode::Tcp => {
                                handle_stream_query(&base_url, &ds_uid, &queries_map, &q)
                            }
                        }
                    }
                })
                .collect::<FuturesOrdered<_>>(),
        )
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
//  StreamService — real-time streaming via Grafana Live
// ═══════════════════════════════════════════════════════════════

#[async_trait]
impl backend::StreamService for QuotesPlugin {
    type JsonValue = ();
    type Error = StreamError;
    type Stream = backend::BoxRunStream<Self::Error>;

    async fn subscribe_stream(
        &self,
        request: backend::SubscribeStreamRequest<Self>,
    ) -> Result<backend::SubscribeStreamResponse, Self::Error> {
        let path = request.path.as_str();
        let exists = self.queries.read().unwrap().contains_key(path);
        Ok(if exists {
            backend::SubscribeStreamResponse::ok(None)
        } else {
            backend::SubscribeStreamResponse::not_found()
        })
    }

    async fn run_stream(
        &self,
        request: backend::RunStreamRequest<Self>,
    ) -> Result<Self::Stream, Self::Error> {
        let path = request.path.as_str().to_string();
        let sq = self
            .queries
            .read()
            .unwrap()
            .get(&path)
            .cloned()
            .ok_or_else(|| StreamError::QueryNotFound(path))?;

        // Build WebSocket URL from base HTTP URL
        let ws_url = sq
            .base_url
            .replace("http://", "ws://")
            .replace("https://", "wss://");
        let ws_url = format!("{ws_url}/ws");

        let field_configs = sq.field_configs;
        let topic = sq.topic;
        let key = sq.key;

        // Connect to WebSocket
        let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
            .await
            .map_err(|e| StreamError::WebSocket(e.to_string()))?;

        let (mut write, mut read) = futures_util::StreamExt::split(ws_stream);

        // Send subscribe message
        let sub_msg = serde_json::json!({
            "action": "subscribe",
            "topic": &topic,
            "key": &key,
            "history": 100
        });
        use futures_util::SinkExt;
        write
            .send(tokio_tungstenite::tungstenite::Message::Text(
                sub_msg.to_string().into(),
            ))
            .await
            .map_err(|e| StreamError::WebSocket(e.to_string()))?;

        // Return async stream that yields StreamPackets
        Ok(Box::pin(async_stream::try_stream! {
            use futures_util::StreamExt as _;
            while let Some(msg) = read.next().await {
                let msg = msg.map_err(|e| StreamError::WebSocket(e.to_string()))?;
                let text: &str = match &msg {
                    tokio_tungstenite::tungstenite::Message::Text(t) => t,
                    _ => continue,
                };

                let value: serde_json::Value = serde_json::from_str(text)
                    .map_err(|e| StreamError::Json(e.to_string()))?;

                let msg_type = value.get("type").and_then(|t| t.as_str()).unwrap_or("");

                match msg_type {
                    "snapshot" => {
                        if let Some(records) = value.get("records").and_then(|r| r.as_array()) {
                            if records.is_empty() {
                                continue;
                            }
                            let frame = build_frame_from_ws_records(
                                &topic, &key, records, &field_configs,
                            ).map_err(StreamError::Frame)?;
                            let packet = backend::StreamPacket::from_frame(
                                frame.check().map_err(|e| StreamError::Frame(e.to_string()))?
                            ).map_err(|e| StreamError::Frame(e.to_string()))?;
                            yield packet;
                        }
                    }
                    "record" => {
                        if let Some(record) = value.get("record") {
                            let records = [record.clone()];
                            let frame = build_frame_from_ws_records(
                                &topic, &key, &records, &field_configs,
                            ).map_err(StreamError::Frame)?;
                            let packet = backend::StreamPacket::from_frame(
                                frame.check().map_err(|e| StreamError::Frame(e.to_string()))?
                            ).map_err(|e| StreamError::Frame(e.to_string()))?;
                            yield packet;
                        }
                    }
                    _ => continue,
                }
            }
        }))
    }

    async fn publish_stream(
        &self,
        _request: backend::PublishStreamRequest<Self>,
    ) -> Result<backend::PublishStreamResponse, Self::Error> {
        Ok(backend::PublishStreamResponse::ok(
            serde_json::Value::Null,
        ))
    }
}

// ═══════════════════════════════════════════════════════════════
//  ResourceService — /fields endpoint for field auto-discovery
// ═══════════════════════════════════════════════════════════════

#[derive(Debug, thiserror::Error)]
pub enum ResourceError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("Not found: {0}")]
    NotFound(String),
}

impl backend::ErrIntoHttpResponse for ResourceError {
    fn into_http_response(self) -> Result<http::Response<Bytes>, Box<dyn std::error::Error>> {
        let status = match &self {
            Self::Http(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
            Self::NotFound(_) => http::StatusCode::NOT_FOUND,
        };
        Ok(Response::builder()
            .status(status)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Bytes::from(serde_json::to_vec(
                &serde_json::json!({"error": self.to_string()}),
            ).unwrap_or_default()))?)
    }
}

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
                    // Build the full record JSON so paths show from root
                    let data_value = record.data.as_json_value();
                    let full = serde_json::json!({
                        "ts_ms": record.ts_ms,
                        "key": &record.key,
                        "data": data_value,
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

/// Flatten a JSON value into dot-notation paths with inferred types.
fn flatten_json_paths(value: &serde_json::Value, prefix: &str) -> Vec<serde_json::Value> {
    let mut result = Vec::new();
    if let serde_json::Value::Object(map) = value {
        for (key, val) in map {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            match val {
                serde_json::Value::Object(_) => {
                    result.extend(flatten_json_paths(val, &path));
                }
                serde_json::Value::Number(_) => {
                    result.push(serde_json::json!({"path": path, "type": "number"}));
                }
                serde_json::Value::String(_) => {
                    result.push(serde_json::json!({"path": path, "type": "string"}));
                }
                serde_json::Value::Bool(_) => {
                    result.push(serde_json::json!({"path": path, "type": "boolean"}));
                }
                serde_json::Value::Array(arr) => {
                    // Include the array path itself
                    result.push(serde_json::json!({"path": path, "type": "string"}));
                    // Also flatten first element if it's an object
                    if let Some(first) = arr.first() {
                        if first.is_object() {
                            result.extend(flatten_json_paths(first, &format!("{path}.0")));
                        }
                    }
                }
                serde_json::Value::Null => {
                    result.push(serde_json::json!({"path": path, "type": "string"}));
                }
            }
        }
    }
    result
}

// ═══════════════════════════════════════════════════════════════
//  Streaming query handler — returns frame with channel metadata
// ═══════════════════════════════════════════════════════════════

fn handle_stream_query(
    base_url: &str,
    ds_uid: &str,
    queries_map: &Arc<RwLock<HashMap<String, StreamQuery>>>,
    query: &backend::DataQuery<QuotesQuery>,
) -> Result<DataResponse, QueryError> {
    let ref_id = query.ref_id.clone();
    let topic = &query.query.topic;
    let key = &query.query.key;

    if topic.is_empty() {
        return Err(QueryError {
            ref_id,
            message: "topic is required".into(),
        });
    }

    let field_configs = resolve_field_configs(&query.query);
    if field_configs.is_empty() {
        return Err(QueryError {
            ref_id,
            message: "at least one field is required".into(),
        });
    }

    // Generate deterministic channel path
    let path_str = channel_path(topic, key, &field_configs);

    // Register query for the stream
    queries_map.write().unwrap().insert(
        path_str.clone(),
        StreamQuery {
            base_url: base_url.to_string(),
            topic: topic.clone(),
            key: key.clone(),
            field_configs: field_configs.clone(),
        },
    );

    // Build channel: ds/<datasource_uid>/<path>
    let namespace = live::Namespace::new(ds_uid.to_string()).map_err(|e| QueryError {
        ref_id: ref_id.clone(),
        message: format!("invalid channel namespace: {e}"),
    })?;
    let path = live::Path::new(path_str).map_err(|e| QueryError {
        ref_id: ref_id.clone(),
        message: format!("invalid channel path: {e}"),
    })?;
    let channel = live::Channel::new(live::Scope::Datasource, namespace, path);

    // Return empty frame with channel — frontend will auto-subscribe via Grafana Live
    let frame_name = if key.is_empty() {
        topic.clone()
    } else {
        format!("{key} {topic}")
    };

    let mut frame = Frame::new(frame_name);
    frame.set_channel(channel);

    let checked = frame.check().map_err(|e| QueryError {
        ref_id: ref_id.clone(),
        message: format!("frame error: {e}"),
    })?;

    Ok(DataResponse::new(ref_id, vec![checked]))
}

// ═══════════════════════════════════════════════════════════════
//  HTTP query handler (unchanged from Phase 1)
// ═══════════════════════════════════════════════════════════════

async fn handle_query(
    http: &reqwest::Client,
    base_url: &str,
    query: &backend::DataQuery<QuotesQuery>,
) -> Result<DataResponse, QueryError> {
    let ref_id = query.ref_id.clone();
    let topic = &query.query.topic;

    if topic.is_empty() {
        return Err(QueryError {
            ref_id,
            message: "topic is required".into(),
        });
    }

    // Resolve field configs (with legacy migration)
    let field_configs = resolve_field_configs(&query.query);

    if field_configs.is_empty() {
        return Err(QueryError {
            ref_id,
            message: "at least one field is required".into(),
        });
    }

    // Build URL
    let from_ms = query.time_range.from.timestamp_millis();
    let to_ms = query.time_range.to.timestamp_millis();
    let key = &query.query.key;
    let url = if key.is_empty() {
        format!(
            "{base_url}/api/topics/{topic}?from={from_ms}&to={to_ms}",
        )
    } else {
        format!(
            "{base_url}/api/topics/{topic}?key={key}&from={from_ms}&to={to_ms}",
        )
    };

    // Fetch records from API
    let body = fetch(http, &url, &ref_id).await?;
    let records: Vec<TopicRecord> = serde_json::from_str(&body).map_err(|e| QueryError {
        ref_id: ref_id.clone(),
        message: format!("parse JSON: {e}"),
    })?;

    let frame = build_frame_from_topic_records(topic, key, &records, &field_configs);

    let checked = frame.check().map_err(|e| QueryError {
        ref_id: ref_id.clone(),
        message: format!("frame error: {e}"),
    })?;

    Ok(DataResponse::new(ref_id, vec![checked]))
}

// ═══════════════════════════════════════════════════════════════
//  Frame builders
// ═══════════════════════════════════════════════════════════════

/// Build a Grafana Frame from TopicRecord structs (HTTP API response).
fn build_frame_from_topic_records(
    topic: &str,
    key: &str,
    records: &[TopicRecord],
    field_configs: &[FieldConfig],
) -> Frame {
    let len = records.len();
    let mut timestamps: Vec<DateTime<Utc>> = Vec::with_capacity(len);

    let mut num_cols: Vec<(usize, String, Vec<f64>)> = Vec::new();
    let mut str_cols: Vec<(usize, String, Vec<String>)> = Vec::new();

    for (i, fc) in field_configs.iter().enumerate() {
        let name = fc.display_name().to_string();
        match fc.r#type {
            FieldValueType::Number | FieldValueType::Time => {
                num_cols.push((i, name, Vec::with_capacity(len)));
            }
            FieldValueType::String | FieldValueType::Boolean => {
                str_cols.push((i, name, Vec::with_capacity(len)));
            }
        }
    }

    for record in records {
        let dt = match DateTime::from_timestamp_millis(record.ts_ms) {
            Some(dt) => dt,
            None => continue,
        };
        timestamps.push(dt);

        // Full record JSON for resolving paths like "data.bid", "ts_ms", "key"
        let data_value = record.data.as_json_value();
        let full = serde_json::json!({
            "ts_ms": record.ts_ms,
            "key": &record.key,
            "data": data_value,
        });

        for (idx, _, col) in &mut num_cols {
            let fc = &field_configs[*idx];
            // Try full path first ("data.bid"), fallback to data-relative ("bid")
            col.push(
                extract_number(&full, &fc.path)
                    .or_else(|| extract_number(data_value, &fc.path))
                    .unwrap_or(f64::NAN),
            );
        }

        for (idx, _, col) in &mut str_cols {
            let fc = &field_configs[*idx];
            let val = match fc.r#type {
                FieldValueType::Boolean => extract_bool(&full, &fc.path)
                    .or_else(|| extract_bool(data_value, &fc.path))
                    .map(|b| b.to_string())
                    .unwrap_or_default(),
                _ => extract_string(&full, &fc.path)
                    .or_else(|| extract_string(data_value, &fc.path))
                    .unwrap_or_default(),
            };
            col.push(val);
        }
    }

    let frame_name = if key.is_empty() {
        topic.to_string()
    } else {
        format!("{key} {topic}")
    };

    let mut frame = Frame::new(frame_name).with_field(timestamps.into_field("time"));

    for (_, name, col) in num_cols {
        frame = frame.with_field(col.into_field(name));
    }

    for (_, name, col) in str_cols {
        frame = frame.with_field(col.into_field(name));
    }

    frame
}

/// Build a Grafana Frame from WebSocket JSON records.
/// Each record is a JSON object: {"ts_ms": i64, "key": "...", "data": {...}}.
fn build_frame_from_ws_records(
    topic: &str,
    key: &str,
    records: &[serde_json::Value],
    field_configs: &[FieldConfig],
) -> Result<Frame, String> {
    let len = records.len();
    let mut timestamps: Vec<DateTime<Utc>> = Vec::with_capacity(len);

    let mut num_cols: Vec<(usize, String, Vec<f64>)> = Vec::new();
    let mut str_cols: Vec<(usize, String, Vec<String>)> = Vec::new();

    for (i, fc) in field_configs.iter().enumerate() {
        let name = fc.display_name().to_string();
        match fc.r#type {
            FieldValueType::Number | FieldValueType::Time => {
                num_cols.push((i, name, Vec::with_capacity(len)));
            }
            FieldValueType::String | FieldValueType::Boolean => {
                str_cols.push((i, name, Vec::with_capacity(len)));
            }
        }
    }

    for record in records {
        // Parse ts_ms from the record envelope
        let ts_ms = record
            .get("ts_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let dt = match DateTime::from_timestamp_millis(ts_ms) {
            Some(dt) => dt,
            None => continue,
        };
        timestamps.push(dt);

        // Full record for resolving paths like "data.bid", "ts_ms", "key"
        // Fallback to record.data for legacy paths like "bid"
        let data = record.get("data").unwrap_or(record);

        for (idx, _, col) in &mut num_cols {
            let fc = &field_configs[*idx];
            col.push(
                extract_number(record, &fc.path)
                    .or_else(|| extract_number(data, &fc.path))
                    .unwrap_or(f64::NAN),
            );
        }

        for (idx, _, col) in &mut str_cols {
            let fc = &field_configs[*idx];
            let val = match fc.r#type {
                FieldValueType::Boolean => extract_bool(record, &fc.path)
                    .or_else(|| extract_bool(data, &fc.path))
                    .map(|b| b.to_string())
                    .unwrap_or_default(),
                _ => extract_string(record, &fc.path)
                    .or_else(|| extract_string(data, &fc.path))
                    .unwrap_or_default(),
            };
            col.push(val);
        }
    }

    let frame_name = if key.is_empty() {
        topic.to_string()
    } else {
        format!("{key} {topic}")
    };

    let mut frame = Frame::new(frame_name).with_field(timestamps.into_field("time"));

    for (_, name, col) in num_cols {
        frame = frame.with_field(col.into_field(name));
    }

    for (_, name, col) in str_cols {
        frame = frame.with_field(col.into_field(name));
    }

    Ok(frame)
}

// ═══════════════════════════════════════════════════════════════
//  Channel path — deterministic hash of query parameters
// ═══════════════════════════════════════════════════════════════

/// Generate a deterministic channel path from query parameters.
/// Same query → same path → reuses existing stream.
fn channel_path(topic: &str, key: &str, field_configs: &[FieldConfig]) -> String {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    topic.hash(&mut h);
    key.hash(&mut h);
    for fc in field_configs {
        fc.path.hash(&mut h);
        fc.alias.hash(&mut h);
        let type_tag = match fc.r#type {
            FieldValueType::Number => "n",
            FieldValueType::String => "s",
            FieldValueType::Boolean => "b",
            FieldValueType::Time => "t",
        };
        type_tag.hash(&mut h);
    }
    format!("{:016x}", h.finish())
}

// ═══════════════════════════════════════════════════════════════
//  Field config resolution (with legacy migration)
// ═══════════════════════════════════════════════════════════════

/// Resolve field configs. If `field_configs` is populated, use it directly.
/// Otherwise, fall back to legacy comma-separated `fields` string.
fn resolve_field_configs(query: &QuotesQuery) -> Vec<FieldConfig> {
    if !query.field_configs.is_empty() {
        return query.field_configs.clone();
    }
    // Legacy migration
    if let Some(ref fields) = query.fields {
        return fields
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| FieldConfig {
                path: s.to_string(),
                alias: String::new(),
                r#type: FieldValueType::Number,
            })
            .collect();
    }
    Vec::new()
}

// ═══════════════════════════════════════════════════════════════
//  JSON dot-path extraction
// ═══════════════════════════════════════════════════════════════

/// Resolve a dot-notation path against a JSON value.
/// E.g., "nested.price.value" → data["nested"]["price"]["value"].
/// Also supports array index segments: "items.0.price".
fn resolve_path<'a>(data: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = data;
    for segment in path.split('.') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        // Try object key first, then array index
        match current.get(segment) {
            Some(next) => current = next,
            None => {
                if let Ok(idx) = segment.parse::<usize>() {
                    current = current.get(idx)?;
                } else {
                    return None;
                }
            }
        }
    }
    Some(current)
}

/// Extract a numeric value via dot-path, coercing to f64.
fn extract_number(data: &serde_json::Value, path: &str) -> Option<f64> {
    let v = resolve_path(data, path)?;
    v.as_f64()
        .or_else(|| v.as_i64().map(|i| i as f64))
        .or_else(|| v.as_u64().map(|u| u as f64))
        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

/// Extract a string value via dot-path.
fn extract_string(data: &serde_json::Value, path: &str) -> Option<String> {
    let v = resolve_path(data, path)?;
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Null => None,
        other => Some(other.to_string()),
    }
}

/// Extract a boolean value via dot-path.
fn extract_bool(data: &serde_json::Value, path: &str) -> Option<bool> {
    let v = resolve_path(data, path)?;
    v.as_bool()
        .or_else(|| v.as_str().and_then(|s| s.parse::<bool>().ok()))
}

// ═══════════════════════════════════════════════════════════════
//  HTTP helpers
// ═══════════════════════════════════════════════════════════════

async fn fetch(
    http: &reqwest::Client,
    url: &str,
    ref_id: &str,
) -> Result<String, QueryError> {
    let resp = http.get(url).send().await.map_err(|e| QueryError {
        ref_id: ref_id.to_string(),
        message: format!("HTTP request failed: {e}"),
    })?;

    let status = resp.status();
    let body = resp.text().await.map_err(|e| QueryError {
        ref_id: ref_id.to_string(),
        message: format!("read response body: {e}"),
    })?;

    if !status.is_success() {
        return Err(QueryError {
            ref_id: ref_id.to_string(),
            message: format!("gauss-server HTTP {status}: {body}"),
        });
    }

    Ok(body)
}

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
