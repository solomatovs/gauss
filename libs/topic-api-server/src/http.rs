use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;

use server_api::TopicQuery;

use super::AppState;

// ═══════════════════════════════════════════════════════════════
//  REST: GET /api/topics
// ═══════════════════════════════════════════════════════════════

pub(crate) async fn handle_list_topics(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let names = state.inspector.topics();
    axum::Json(names).into_response()
}

// ═══════════════════════════════════════════════════════════════
//  REST: GET /api/topics/{name}?key=X&from=&to=&limit=
// ═══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub(crate) struct TopicQueryParams {
    key: Option<String>,
    from: Option<i64>,
    to: Option<i64>,
    limit: Option<usize>,
    offset: Option<usize>,
    order: Option<String>,
}

pub(crate) async fn handle_query_topic(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<TopicQueryParams>,
) -> impl IntoResponse {
    let order = match params.order.as_deref() {
        Some("desc" | "DESC") => server_api::SortOrder::Desc,
        _ => server_api::SortOrder::Asc,
    };
    let query = TopicQuery {
        key: params.key,
        from_ms: params.from,
        to_ms: params.to,
        limit: params.limit,
        offset: params.offset,
        order,
    };

    match state.inspector.query(&name, &query).await {
        Ok(records) => axum::Json(records).into_response(),
        Err(e) => (
            axum::http::StatusCode::NOT_FOUND,
            format!("error: {e}"),
        )
            .into_response(),
    }
}
