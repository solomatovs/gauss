use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use futures::stream::FuturesOrdered;
use grafana_plugin_sdk::backend::{self, async_trait, BoxDataResponseStream, DataResponse};
use grafana_plugin_sdk::data::Frame;
use grafana_plugin_sdk::live;

use super::error::QueryError;
use super::frame::{build_frame_from_topic_records, channel_path};
use super::query::{ConnectionMode, QuotesQuery, StreamQuery, resolve_field_configs};
use super::{QuotesPlugin, extract_url};

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

    let path_str = channel_path(topic, key, &field_configs);

    queries_map.write().unwrap().insert(
        path_str.clone(),
        StreamQuery {
            base_url: base_url.to_string(),
            topic: topic.clone(),
            key: key.clone(),
            field_configs: field_configs.clone(),
        },
    );

    let namespace = live::Namespace::new(ds_uid.to_string()).map_err(|e| QueryError {
        ref_id: ref_id.clone(),
        message: format!("invalid channel namespace: {e}"),
    })?;
    let path = live::Path::new(path_str).map_err(|e| QueryError {
        ref_id: ref_id.clone(),
        message: format!("invalid channel path: {e}"),
    })?;
    let channel = live::Channel::new(live::Scope::Datasource, namespace, path);

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
//  HTTP query handler
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

    let field_configs = resolve_field_configs(&query.query);

    if field_configs.is_empty() {
        return Err(QueryError {
            ref_id,
            message: "at least one field is required".into(),
        });
    }

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

    let body = fetch(http, &url, &ref_id).await?;
    let records: Vec<super::query::TopicRecord> = serde_json::from_str(&body).map_err(|e| QueryError {
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
