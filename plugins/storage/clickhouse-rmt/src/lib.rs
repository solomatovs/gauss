use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

use server_api::{
    parse_plugin_config, plugin_err, plugin_ok,
    TopicRecord, TopicQuery, TopicStorage, PluginCreateResult,
    PluginError, Field, FieldType, ScalarType, RecordSchema, SortOrder,
};

// ════════════════════════════════════════════════════════════════
//  Identifier validation
// ════════════════════════════════════════════════════════════════

/// Validate a ClickHouse identifier (table name, column name).
/// Allowed: `^[a-zA-Z_][a-zA-Z0-9_.]*$`.
fn validate_identifier(name: &str, context: &str) -> Result<(), PluginError> {
    if name.is_empty() {
        return Err(PluginError::config(format!("{context}: identifier is empty")));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(PluginError::config(format!(
            "{context}: invalid identifier '{name}' — must start with a letter or underscore"
        )));
    }
    for ch in chars {
        if !ch.is_ascii_alphanumeric() && ch != '_' && ch != '.' {
            return Err(PluginError::config(format!(
                "{context}: invalid character '{ch}' in identifier '{name}'"
            )));
        }
    }
    Ok(())
}

/// Escape a string value for safe use inside a ClickHouse single-quoted literal.
/// Used only in WHERE clauses for key filtering.
fn escape_ch_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

// ════════════════════════════════════════════════════════════════
//  Columnar schema (cached after init)
// ════════════════════════════════════════════════════════════════

/// Cached schema for columnar mode.
/// Created in init() when RecordSchema is provided.
struct ColumnarSchema {
    fields: Vec<Field>,
    /// "key, ts_ms, open, high, ..."
    insert_columns: String,
    select_columns: String,
}

// ════════════════════════════════════════════════════════════════
//  ChRmtStorage
// ════════════════════════════════════════════════════════════════

/// ClickHouse ReplacingMergeTree TopicStorage backend.
///
/// Uses `ENGINE = ReplacingMergeTree()` — upsert semantics where
/// INSERT with the same (key, ts_ms) replaces the old row.
///
/// Two operating modes:
/// - **Columnar** (when RecordSchema is provided):
///   each data field → a separate typed CH column.
/// - **Fallback** (no schema): all data → `data String` (JSON blob).
///
/// Works directly with `serde_json::Value` from TopicRecord — no
/// FormatSerializer round-trips.
pub struct ChRmtStorage {
    http: reqwest::Client,
    base_url: String,
    user: String,
    password: String,
    database: String,
    table: String,
    // DDL config
    partition_by: Option<String>,
    order_by: String,
    ttl: Option<String>,
    table_settings: Option<HashMap<String, String>>,
    // Runtime behavior
    use_final: bool,
    insert_batch_size: usize,
    async_insert: bool,
    // Initialized after init()
    /// None = fallback (data String), Some = columnar mode.
    columnar: OnceLock<Option<ColumnarSchema>>,
}

impl ChRmtStorage {
    fn from_config(cfg: &ClickHousePluginConfig) -> Result<Self, PluginError> {
        let scheme = if cfg.tls { "https" } else { "http" };
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(cfg.accept_invalid_certs)
            .timeout(Duration::from_secs(cfg.request_timeout_secs))
            .build()
            .map_err(|e| PluginError::config(format!("HTTP client: {e}")))?;
        Ok(Self {
            http,
            base_url: format!("{scheme}://{}:{}", cfg.host, cfg.port),
            user: cfg.user.clone(),
            password: cfg.password.clone(),
            database: cfg.database.clone(),
            table: cfg.table.clone(),
            partition_by: cfg.partition_by.clone(),
            order_by: cfg.order_by.clone(),
            ttl: cfg.ttl.clone(),
            table_settings: cfg.table_settings.clone(),
            use_final: cfg.use_final,
            insert_batch_size: cfg.insert_batch_size,
            async_insert: cfg.async_insert,
            columnar: OnceLock::new(),
        })
    }

    fn columnar(&self) -> Option<&ColumnarSchema> {
        self.columnar.get().expect("init() must be called before save/query").as_ref()
    }

    /// Execute a SQL statement (DDL / SELECT). Body = SQL text.
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

    /// Execute an INSERT using FORMAT JSONEachRow.
    /// The INSERT statement goes in the `query` URL parameter;
    /// the NDJSON body goes as the POST body.
    async fn exec_insert(&self, insert_sql: &str, ndjson_body: String) -> Result<(), PluginError> {
        let mut params: Vec<(&str, &str)> = vec![
            ("user", self.user.as_str()),
            ("password", self.password.as_str()),
            ("database", self.database.as_str()),
            ("query", insert_sql),
        ];
        if self.async_insert {
            params.push(("async_insert", "1"));
            params.push(("wait_for_async_insert", "1"));
        }

        let resp = self
            .http
            .post(&self.base_url)
            .query(&params)
            .body(ndjson_body)
            .send()
            .await
            .map_err(|e| PluginError::io(format!("CH insert: {e}")))?;

        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = resp.text().await.map_err(|e| PluginError::io(format!("CH read: {e}")))?;
            Err(PluginError::io(body))
        }
    }

    // ── Save helpers ──

    /// Fallback: single `data String` column (JSON blob).
    async fn save_fallback(&self, records: &[TopicRecord]) -> Result<(), PluginError> {
        let insert_sql = format!(
            "INSERT INTO {} (key, ts_ms, data) FORMAT JSONEachRow",
            self.table,
        );

        let mut body = String::new();
        for r in records {
            let data_json = serde_json::to_string(&r.value)
                .map_err(|e| PluginError::format_err(format!("json: {e}")))?;
            let row = serde_json::json!({
                "key": r.key,
                "ts_ms": r.ts_ms,
                "data": data_json,
            });
            let row_str = serde_json::to_string(&row)
                .map_err(|e| PluginError::format_err(format!("json: {e}")))?;
            body.push_str(&row_str);
            body.push('\n');
        }

        self.exec_insert(&insert_sql, body).await
    }

    /// Columnar: each field → a separate column.
    async fn save_columnar(
        &self,
        records: &[TopicRecord],
        schema: &ColumnarSchema,
    ) -> Result<(), PluginError> {
        let insert_sql = format!(
            "INSERT INTO {} ({}) FORMAT JSONEachRow",
            self.table,
            schema.insert_columns,
        );

        let mut body = String::new();
        for r in records {
            let obj = r.value.as_object().ok_or_else(|| {
                PluginError::format_err("expected JSON object for columnar insert")
            })?;

            let mut row = serde_json::Map::with_capacity(schema.fields.len() + 2);
            row.insert("key".into(), serde_json::Value::String(r.key.clone()));
            row.insert("ts_ms".into(), serde_json::Value::Number(r.ts_ms.into()));

            for field in &schema.fields {
                let val = obj.get(&field.name).cloned().unwrap_or(serde_json::Value::Null);
                let ch_val = coerce_value_to_ch(&val, &field.field_type);
                row.insert(field.name.clone(), ch_val);
            }

            let row_str = serde_json::to_string(&serde_json::Value::Object(row))
                .map_err(|e| PluginError::format_err(format!("json: {e}")))?;
            body.push_str(&row_str);
            body.push('\n');
        }

        self.exec_insert(&insert_sql, body).await
    }

    // ── Query helpers ──

    /// Fallback: read from `data` column.
    async fn query_fallback(&self, query: &TopicQuery) -> Result<Vec<TopicRecord>, PluginError> {
        let sql = build_select_sql(
            "key, ts_ms, data",
            &self.table,
            self.use_final,
            query,
        );

        let body = self.exec(&sql).await?;
        let mut result = Vec::new();
        for line in body.lines() {
            if line.is_empty() { continue; }
            let row: FallbackRow = serde_json::from_str(line)
                .map_err(|e| PluginError::format_err(format!("parse CH row: {e}")))?;

            let value: serde_json::Value = serde_json::from_str(&row.data)
                .map_err(|e| PluginError::format_err(format!("parse data json: {e}")))?;

            result.push(TopicRecord {
                ts_ms: row.ts_ms,
                key: row.key,
                value,
                raw: None,
            });
        }
        Ok(result)
    }

    /// Columnar: reconstruct Value from individual column values.
    async fn query_columnar(
        &self,
        query: &TopicQuery,
        schema: &ColumnarSchema,
    ) -> Result<Vec<TopicRecord>, PluginError> {
        let sql = build_select_sql(
            &schema.select_columns,
            &self.table,
            self.use_final,
            query,
        );

        let body = self.exec(&sql).await?;
        let mut result = Vec::new();
        for line in body.lines() {
            if line.is_empty() { continue; }
            let row: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| PluginError::format_err(format!("parse CH row: {e}")))?;
            let row_obj = row.as_object()
                .ok_or_else(|| PluginError::format_err("CH row is not an object"))?;

            let key = row_obj.get("key")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ts_ms = row_obj.get("ts_ms")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            let mut data_obj = serde_json::Map::new();
            for field in &schema.fields {
                if let Some(val) = row_obj.get(&field.name) {
                    data_obj.insert(
                        field.name.clone(),
                        coerce_ch_to_value(val, &field.field_type),
                    );
                }
            }

            result.push(TopicRecord {
                ts_ms,
                key,
                value: serde_json::Value::Object(data_obj),
                raw: None,
            });
        }
        Ok(result)
    }
}

// ════════════════════════════════════════════════════════════════
//  TopicStorage impl
// ════════════════════════════════════════════════════════════════

impl TopicStorage for ChRmtStorage {
    fn init(&self, schema: Option<RecordSchema>) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        Box::pin(async move {
            let columns_sql = match schema {
                Some(ref s) if !s.fields.is_empty() => {
                    for field in &s.fields {
                        validate_identifier(&field.name, "column")?;
                    }
                    build_columnar_columns(&s.fields)
                }
                _ => build_fallback_columns().to_string(),
            };

            let create_sql = generate_create_table(
                &self.table,
                &columns_sql,
                self.partition_by.as_deref(),
                &self.order_by,
                self.ttl.as_deref(),
                self.table_settings.as_ref(),
            );
            self.exec(&create_sql).await?;

            match schema {
                Some(s) if !s.fields.is_empty() => {
                    let col_names: Vec<&str> = s.fields.iter().map(|f| f.name.as_str()).collect();
                    let cols_csv = col_names.join(", ");
                    let _ = self.columnar.set(Some(ColumnarSchema {
                        fields: s.fields,
                        insert_columns: format!("key, ts_ms, {cols_csv}"),
                        select_columns: format!("key, ts_ms, {cols_csv}"),
                    }));
                }
                _ => {
                    let _ = self.columnar.set(None);
                }
            }
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
            match self.columnar() {
                Some(schema) => {
                    for chunk in records.chunks(self.insert_batch_size) {
                        self.save_columnar(chunk, schema).await?;
                    }
                }
                None => {
                    for chunk in records.chunks(self.insert_batch_size) {
                        self.save_fallback(chunk).await?;
                    }
                }
            }
            Ok(())
        })
    }

    fn query(
        &self,
        query: &TopicQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRecord>, PluginError>> + Send + '_>> {
        let query = query.clone();
        Box::pin(async move {
            match self.columnar() {
                Some(schema) => self.query_columnar(&query, schema).await,
                None => self.query_fallback(&query).await,
            }
        })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// ════════════════════════════════════════════════════════════════
//  SQL generation helpers
// ════════════════════════════════════════════════════════════════

/// Map ScalarType to ClickHouse column type.
fn scalar_type_to_ch(st: &ScalarType) -> String {
    match st {
        ScalarType::Bool       => "UInt8".into(),
        ScalarType::Int32      => "Int32".into(),
        ScalarType::Int64      => "Int64".into(),
        ScalarType::Float32    => "Float32".into(),
        ScalarType::Float64    => "Float64".into(),
        ScalarType::Decimal { precision, scale } => format!("Decimal({precision},{scale})"),
        ScalarType::String     => "String".into(),
        ScalarType::Bytes      => "String".into(),
        ScalarType::Timestamp  => "DateTime64(6)".into(),
        ScalarType::Date       => "Date".into(),
        ScalarType::Uuid       => "UUID".into(),
        ScalarType::Json       => "String".into(),
    }
}

/// Map FieldType to ClickHouse column type string.
fn field_type_to_ch(ft: &FieldType, nullable: bool) -> String {
    let base = match ft {
        FieldType::Scalar(s) => scalar_type_to_ch(s),
        FieldType::Array(s) => format!("Array({})", scalar_type_to_ch(s)),
    };
    if nullable {
        format!("Nullable({base})")
    } else {
        base
    }
}

/// Build columnar DDL column definitions.
fn build_columnar_columns(fields: &[Field]) -> String {
    let mut cols = std::string::String::from("    key    LowCardinality(String),\n    ts_ms  Int64");
    for field in fields {
        cols.push_str(",\n    ");
        cols.push_str(&field.name);
        cols.push(' ');
        cols.push_str(&field_type_to_ch(&field.field_type, field.nullable));
    }
    cols
}

/// Build fallback DDL column definitions (key, ts_ms, data).
fn build_fallback_columns() -> &'static str {
    "    key    LowCardinality(String),\n    ts_ms  Int64,\n    data   String"
}

/// Generate CREATE TABLE DDL with ReplacingMergeTree engine.
fn generate_create_table(
    table: &str,
    columns_sql: &str,
    partition_by: Option<&str>,
    order_by: &str,
    ttl: Option<&str>,
    table_settings: Option<&HashMap<String, String>>,
) -> String {
    let mut ddl = format!(
        "CREATE TABLE IF NOT EXISTS {table} (\n{columns_sql}\n) ENGINE = ReplacingMergeTree()\n"
    );
    if let Some(pb) = partition_by {
        ddl.push_str(&format!("PARTITION BY {pb}\n"));
    }
    ddl.push_str(&format!("ORDER BY {order_by}"));
    if let Some(ttl_expr) = ttl {
        ddl.push_str(&format!("\nTTL {ttl_expr}"));
    }
    if let Some(settings) = table_settings {
        if !settings.is_empty() {
            let pairs: Vec<String> = settings.iter()
                .map(|(k, v)| format!("{k} = {v}"))
                .collect();
            ddl.push_str(&format!("\nSETTINGS {}", pairs.join(", ")));
        }
    }
    ddl
}

/// Coerce a JSON value for CH INSERT (Value → CH-compatible JSON).
fn coerce_value_to_ch(val: &serde_json::Value, ft: &FieldType) -> serde_json::Value {
    match ft {
        // CH stores Bool as UInt8: true→1, false→0
        FieldType::Scalar(ScalarType::Bool) => match val.as_bool() {
            Some(true) => serde_json::Value::Number(1.into()),
            _ => serde_json::Value::Number(0.into()),
        },
        _ => val.clone(),
    }
}

/// Coerce a ClickHouse JSONEachRow value back to expected JSON type.
fn coerce_ch_to_value(val: &serde_json::Value, ft: &FieldType) -> serde_json::Value {
    match ft {
        // ClickHouse UInt8 → 0/1 in JSON, convert back to bool
        FieldType::Scalar(ScalarType::Bool) => serde_json::Value::Bool(val.as_u64().unwrap_or(0) != 0),
        _ => val.clone(),
    }
}

/// Build a full SELECT statement from TopicQuery.
fn build_select_sql(
    columns: &str,
    table: &str,
    use_final: bool,
    query: &TopicQuery,
) -> String {
    let final_kw = if use_final { " FINAL" } else { "" };

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

    let order_dir = match query.order {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    };

    let mut sql = format!(
        "SELECT {columns} FROM {table}{final_kw} {where_clause} ORDER BY ts_ms {order_dir}",
    );

    if let Some(limit) = query.limit {
        if let Some(offset) = query.offset {
            sql.push_str(&format!(" LIMIT {offset}, {limit}"));
        } else {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
    } else if let Some(offset) = query.offset {
        // LIMIT with large number + OFFSET
        sql.push_str(&format!(" LIMIT {offset}, 18446744073709551615"));
    }

    sql.push_str(" FORMAT JSONEachRow");
    sql
}

// ════════════════════════════════════════════════════════════════
//  Deserialization structs
// ════════════════════════════════════════════════════════════════

/// Row for fallback mode (data String column).
#[derive(serde::Deserialize)]
struct FallbackRow {
    key: String,
    ts_ms: i64,
    data: String,
}

// ════════════════════════════════════════════════════════════════
//  Plugin FFI entry points
// ════════════════════════════════════════════════════════════════

#[derive(serde::Deserialize)]
struct ClickHousePluginConfig {
    // ── Connection ──
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

    // ── Table ──
    #[serde(default = "default_table")]
    table: String,
    /// PARTITION BY expression. None = omitted from DDL.
    #[serde(default)]
    partition_by: Option<String>,
    /// ORDER BY expression.
    #[serde(default = "default_order_by")]
    order_by: String,
    /// TTL expression. None = omitted from DDL.
    #[serde(default)]
    ttl: Option<String>,
    /// SETTINGS key=value pairs for CREATE TABLE.
    #[serde(default)]
    table_settings: Option<HashMap<String, String>>,

    // ── Query tuning ──
    /// Whether to use FINAL keyword in SELECT queries (deduplication on read).
    #[serde(default = "default_use_final")]
    use_final: bool,

    // ── Insert tuning ──
    /// Maximum records per INSERT batch.
    #[serde(default = "default_insert_batch_size")]
    insert_batch_size: usize,
    /// Enable ClickHouse async_insert mode.
    #[serde(default)]
    async_insert: bool,

    // ── Timeouts ──
    /// HTTP request timeout in seconds.
    #[serde(default = "default_request_timeout_secs")]
    request_timeout_secs: u64,
}

fn default_port() -> u16 { 8123 }
fn default_user() -> String { "default".into() }
fn default_database() -> String { "default".into() }
fn default_table() -> String { "topic_data".into() }
fn default_order_by() -> String { "(key, ts_ms)".into() }
fn default_use_final() -> bool { true }
fn default_insert_batch_size() -> usize { 10_000 }
fn default_request_timeout_secs() -> u64 { 30 }

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

    if let Err(e) = validate_identifier(&cfg.table, "table") {
        return plugin_err(e.to_string());
    }

    let storage = match ChRmtStorage::from_config(&cfg) {
        Ok(s) => s,
        Err(e) => return plugin_err(e.to_string()),
    };
    plugin_ok(Box::new(storage) as Box<dyn TopicStorage>)
}

server_api::qs_destroy_fn!(qs_destroy_topic_storage, TopicStorage);
server_api::qs_abi_version_fn!();
