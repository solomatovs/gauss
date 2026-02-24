use std::collections::{HashMap, HashSet};

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use server_api::{TopicQuery, TopicRecord};

use super::AppState;

// ═══════════════════════════════════════════════════════════════
//  WebSocket: /ws
// ═══════════════════════════════════════════════════════════════

pub(crate) async fn handle_ws(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_connection(socket, state))
}

// ═══════════════════════════════════════════════════════════════
//  Protocol types
// ═══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
struct WsAction {
    action: String,
    #[serde(default)]
    topic: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    history: Option<usize>,
    #[serde(default)]
    from: Option<i64>,
    #[serde(default)]
    to: Option<i64>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(serde::Serialize)]
struct WsSnapshot {
    r#type: &'static str,
    topic: String,
    records: Vec<TopicRecord>,
}

#[derive(serde::Serialize)]
struct WsRecord<'a> {
    r#type: &'static str,
    topic: &'a str,
    record: &'a TopicRecord,
}

/// Сообщение из subscription task → WS writer.
struct SubRecord {
    topic: String,
    record: TopicRecord,
}

// ═══════════════════════════════════════════════════════════════
//  Connection handler
// ═══════════════════════════════════════════════════════════════

async fn ws_connection(mut socket: WebSocket, state: AppState) {
    let (mux_tx, mut mux_rx) = mpsc::channel::<SubRecord>(state.ws_buffer);

    let mut subs: HashMap<String, (HashSet<String>, JoinHandle<()>)> = HashMap::new();

    loop {
        tokio::select! {
            biased;

            msg = socket.recv() => {
                let msg = match msg {
                    Some(Ok(msg)) => msg,
                    _ => break,
                };

                let text = match msg {
                    Message::Text(t) => t,
                    Message::Close(_) => break,
                    _ => continue,
                };

                let action: WsAction = match serde_json::from_str(&text) {
                    Ok(a) => a,
                    Err(e) => {
                        let _ = socket.send(Message::Text(
                            format!(r#"{{"error":"parse: {e}"}}"#).into(),
                        )).await;
                        continue;
                    }
                };

                match action.action.as_str() {
                    "subscribe" => {
                        handle_ws_subscribe(&state, &mut socket, &mut subs, &mux_tx, &action).await;
                    }
                    "unsubscribe" => {
                        if subs.contains_key(&action.topic) {
                            if action.key.is_empty() {
                                let (_, handle) = subs.remove(&action.topic).unwrap();
                                handle.abort();
                            } else {
                                subs.get_mut(&action.topic).unwrap().0.remove(&action.key);
                            }
                        }
                    }
                    "query" => {
                        handle_ws_query(&state, &mut socket, &action).await;
                    }
                    _ => {
                        let _ = socket.send(Message::Text(
                            r#"{"error":"unknown action"}"#.into(),
                        )).await;
                    }
                }
            }

            sub_record = mux_rx.recv() => {
                match sub_record {
                    Some(SubRecord { topic, record }) => {
                        if let Some((key_filter, _)) = subs.get(&topic) {
                            if !key_filter.is_empty() && !key_filter.contains(&record.key) {
                                continue;
                            }
                        }

                        let msg = WsRecord {
                            r#type: "record",
                            topic: &topic,
                            record: &record,
                        };
                        if let Ok(json) = serde_json::to_string(&msg) {
                            if socket.send(Message::Text(json.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    None => break,
                }
            }
        }
    }

    for (_, (_, handle)) in subs {
        handle.abort();
    }
}

async fn handle_ws_subscribe(
    state: &AppState,
    socket: &mut WebSocket,
    subs: &mut HashMap<String, (HashSet<String>, JoinHandle<()>)>,
    mux_tx: &mpsc::Sender<SubRecord>,
    action: &WsAction,
) {
    if !subs.contains_key(&action.topic) {
        let subscription = match state.subscriber.subscribe(
            &action.topic,
            state.ws_buffer,
            state.ws_overflow,
        ).await {
            Ok(sub) => sub,
            Err(e) => {
                let _ = socket
                    .send(Message::Text(
                        format!(r#"{{"error":"{e}"}}"#).into(),
                    ))
                    .await;
                return;
            }
        };

        let topic_name = action.topic.clone();
        let tx = mux_tx.clone();
        let handle = tokio::spawn(async move {
            let mut sub = subscription;
            while let Some(record) = sub.recv().await {
                if tx.send(SubRecord {
                    topic: topic_name.clone(),
                    record,
                }).await.is_err() {
                    break;
                }
            }
        });

        subs.insert(action.topic.clone(), (HashSet::new(), handle));
    }

    if !action.key.is_empty() {
        subs.get_mut(&action.topic)
            .unwrap()
            .0
            .insert(action.key.clone());
    }

    if let Some(limit) = action.history {
        let query = TopicQuery {
            key: if action.key.is_empty() {
                None
            } else {
                Some(action.key.clone())
            },
            from_ms: None,
            to_ms: None,
            limit: Some(limit),
            ..Default::default()
        };
        match state.inspector.query(&action.topic, &query).await {
            Ok(records) => {
                let snap = WsSnapshot {
                    r#type: "snapshot",
                    topic: action.topic.clone(),
                    records,
                };
                if let Ok(json) = serde_json::to_string(&snap) {
                    let _ = socket.send(Message::Text(json.into())).await;
                }
            }
            Err(e) => {
                let _ = socket
                    .send(Message::Text(
                        format!(r#"{{"error":"query: {e}"}}"#).into(),
                    ))
                    .await;
            }
        }
    }
}

async fn handle_ws_query(
    state: &AppState,
    socket: &mut WebSocket,
    action: &WsAction,
) {
    let query = TopicQuery {
        key: if action.key.is_empty() {
            None
        } else {
            Some(action.key.clone())
        },
        from_ms: action.from,
        to_ms: action.to,
        limit: action.limit,
        ..Default::default()
    };

    match state.inspector.query(&action.topic, &query).await {
        Ok(records) => {
            let snap = WsSnapshot {
                r#type: "query_result",
                topic: action.topic.clone(),
                records,
            };
            if let Ok(json) = serde_json::to_string(&snap) {
                let _ = socket.send(Message::Text(json.into())).await;
            }
        }
        Err(e) => {
            let _ = socket
                .send(Message::Text(
                    format!(r#"{{"error":"query: {e}"}}"#).into(),
                ))
                .await;
        }
    }
}
