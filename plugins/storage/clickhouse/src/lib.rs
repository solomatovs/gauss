use std::future::Future;
use std::pin::Pin;

use server_api::{
    parse_plugin_config, plugin_err, plugin_ok,
    TopicRecord, TopicQuery, TopicStorage, PluginCreateResult,
    RecordData, DataFormat, PluginError,
};
use base64::Engine;

const SQL_CREATE: &str = include_str!("../sql/create_ohlc.sql");

/// Escape a string value for safe use inside a ClickHouse single-quoted literal.
/// Prevents SQL injection by escaping `\` and `'`.
fn escape_ch_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// ClickHouse TopicStorage backend.
///
/// Использует ReplacingMergeTree для upsert-семантики:
/// INSERT с тем же (key, ts_ms) заменяет старую строку.
///
/// Столбцы: key (partition key), ts_ms (Unix ms), data (JSON string).
pub struct ClickHouseStorage {
    http: reqwest::Client,
    base_url: String,
    user: String,
    password: String,
    database: String,
    table: String,
}

impl ClickHouseStorage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
        tls: bool,
        accept_invalid_certs: bool,
        table: &str,
    ) -> Result<Self, PluginError> {
        let scheme = if tls { "https" } else { "http" };
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(accept_invalid_certs)
            .build()
            .map_err(|e| PluginError::config(format!("HTTP client: {e}")))?;
        Ok(Self {
            http,
            base_url: format!("{scheme}://{host}:{port}"),
            user: user.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            table: table.to_string(),
        })
    }

    async fn exec(&self, sql: &str) -> Result<String, PluginError> {
        let resp = self
            .http
            .post(&self.base_url)
            .query(&[
                ("user", self.user.as_str()),
                ("password", self.password.as_str()),
                ("database", self.database.as_str()),
            ])
            .body(sql.to_owned())
            .send()
            .await
            .map_err(|e| PluginError::io(format!("CH request: {e}")))?;

        let status = resp.status();
        let body = resp.text().await.map_err(|e| PluginError::io(format!("CH read: {e}")))?;

        if status.is_success() {
            Ok(body)
        } else {
            Err(PluginError::io(body))
        }
    }
}

impl TopicStorage for ClickHouseStorage {
    fn init(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        Box::pin(async {
            let sql = SQL_CREATE.replace("{table}", &self.table);
            self.exec(&sql).await?;
            Ok(())
        })
    }

    fn save(
        &self,
        records: &[TopicRecord],
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        let records = records.to_vec();
        Box::pin(async move {
            if records.is_empty() {
                return Ok(());
            }

            let values: Vec<String> = records
                .iter()
                .map(|r| {
                    let format_str = r.data.format().to_string();
                    let data_str = match r.data.format() {
                        DataFormat::Json => {
                            String::from_utf8_lossy(r.data.as_bytes()).to_string()
                        }
                        _ => {
                            base64::engine::general_purpose::STANDARD.encode(r.data.as_bytes())
                        }
                    };
                    format!(
                        "('{}', {}, '{}', '{}')",
                        escape_ch_string(&r.key),
                        r.ts_ms,
                        escape_ch_string(&format_str),
                        escape_ch_string(&data_str),
                    )
                })
                .collect();

            let sql = format!(
                "INSERT INTO {} (key, ts_ms, format, data) VALUES {}",
                self.table,
                values.join(","),
            );
            self.exec(&sql).await?;
            Ok(())
        })
    }

    fn query(
        &self,
        query: &TopicQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRecord>, PluginError>> + Send + '_>> {
        let query = query.clone();
        Box::pin(async move {
            let mut conditions: Vec<String> = Vec::new();

            if let Some(ref key) = query.key {
                conditions.push(format!("key = '{}'", escape_ch_string(key)));
            }
            if let Some(from) = query.from_ms {
                conditions.push(format!("ts_ms >= {from}"));
            }
            if let Some(to) = query.to_ms {
                conditions.push(format!("ts_ms < {to}"));
            }

            let where_clause = if conditions.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", conditions.join(" AND "))
            };

            let limit_clause = match query.limit {
                Some(n) => format!("LIMIT {n}"),
                None => String::new(),
            };

            let sql = format!(
                "SELECT key, ts_ms, format, data \
                 FROM {} FINAL \
                 {} \
                 ORDER BY ts_ms \
                 {} \
                 FORMAT JSONEachRow",
                self.table,
                where_clause,
                limit_clause,
            );

            let body = self.exec(&sql).await?;

            let mut result = Vec::new();
            for line in body.lines() {
                if line.is_empty() {
                    continue;
                }
                let row: ChRow =
                    serde_json::from_str(line).map_err(|e| PluginError::format_err(format!("parse CH row: {e}")))?;

                let format_str = row.format.as_deref().unwrap_or("json");
                let format: DataFormat = serde_json::from_value(
                    serde_json::Value::String(format_str.to_string())
                ).unwrap_or(DataFormat::Json);

                let record_data = match format {
                    DataFormat::Json => {
                        RecordData::new(row.data.into_bytes(), DataFormat::Json)
                    }
                    _ => {
                        let bytes = base64::engine::general_purpose::STANDARD
                            .decode(&row.data)
                            .map_err(|e| PluginError::format_err(format!("base64 decode: {e}")))?;
                        RecordData::new(bytes, format)
                    }
                };

                result.push(TopicRecord {
                    ts_ms: row.ts_ms,
                    key: row.key,
                    data: record_data,
                });
            }

            Ok(result)
        })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(serde::Deserialize)]
struct ChRow {
    key: String,
    ts_ms: i64,
    #[serde(default)]
    format: Option<String>,
    data: String,
}

// ---- Plugin FFI entry points ----

#[derive(serde::Deserialize)]
struct ClickHousePluginConfig {
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default = "default_user")]
    user: String,
    #[serde(default)]
    password: String,
    #[serde(default = "default_database")]
    database: String,
    #[serde(default)]
    tls: bool,
    #[serde(default)]
    accept_invalid_certs: bool,
    #[serde(default = "default_table")]
    table: String,
}

fn default_port() -> u16 {
    8123
}
fn default_user() -> String {
    "default".into()
}
fn default_database() -> String {
    "default".into()
}
fn default_table() -> String {
    "topic_data".into()
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_topic_storage(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: ClickHousePluginConfig = match unsafe { parse_plugin_config(config_json_ptr, config_json_len) } {
        Ok(c) => c,
        Err(e) => return plugin_err(e.to_string()),
    };

    let storage = match ClickHouseStorage::new(
        &cfg.host,
        cfg.port,
        &cfg.user,
        &cfg.password,
        &cfg.database,
        cfg.tls,
        cfg.accept_invalid_certs,
        &cfg.table,
    ) {
        Ok(s) => s,
        Err(e) => return plugin_err(e.to_string()),
    };
    plugin_ok(Box::new(storage) as Box<dyn TopicStorage>)
}

server_api::qs_destroy_fn!(qs_destroy_topic_storage, TopicStorage);
server_api::qs_abi_version_fn!();
