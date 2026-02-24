use bytes::Bytes;
use grafana_plugin_sdk::backend;
use http::Response;

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

/// Error type for resource operations.
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
