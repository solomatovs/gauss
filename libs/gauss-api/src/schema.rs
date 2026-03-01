use std::collections::HashMap;

/// Structured data type. Used everywhere: source schema, target schema,
/// Rhai script input/output.
///
/// Type name and attributes are arbitrary — the engine does not interpret them.
///
/// Examples:
/// - `{ name: "decimal", attrs: { precision: 18, scale: 8 } }`
/// - `{ name: "DateTime64", attrs: { precision: 3 } }`
/// - `{ name: "varchar", attrs: { length: 255 } }`
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldType {
    pub name: String,
    #[serde(default)]
    pub attrs: HashMap<String, serde_json::Value>,
}

/// A single field in a schema.
///
/// - For flat formats (protobuf, Arrow): `name` = field name (`"symbol"`, `"bid"`)
/// - For hierarchical formats (JSON): `name` = path (`"$.order.id"`)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    /// Field-level properties. For target: `default`, `materialized`, `codec`, etc.
    /// For source: usually empty.
    #[serde(default)]
    pub props: HashMap<String, serde_json::Value>,
}

/// Unified schema description. Used both as source (from format plugin)
/// and as target (for storage plugin DDL).
///
/// Field position in `fields` determines its index — position in `Row(Vec<Value>)`
/// after `deserialize()`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Schema {
    pub fields: Vec<Field>,
    /// Schema-level attributes.
    /// - Source: metadata (`package`, `message`, etc.)
    /// - Target: DDL properties (`table`, `engine`, `order_by`, etc.)
    #[serde(default)]
    pub attrs: HashMap<String, serde_json::Value>,
}
