use std::sync::Arc;

use crate::converter::FieldConverter;
use crate::schema::{Field, Schema};

/// Reference to a source field (position in `source.fields` and in `Row`).
pub struct FieldRef {
    /// Position in `source.fields` and `Row.0`.
    pub index: usize,
    /// Source field name (for observability — logs, metrics).
    pub name: String,
}

/// Conversion strategy for a single field.
pub enum Converter {
    /// Value as-is, no conversion needed.
    Passthrough,
    /// Loaded converter plugin (.so).
    Plugin(Arc<dyn FieldConverter>),
    /// No data from Row — DB handles it (DEFAULT, MATERIALIZED).
    Computed,
    /// Field excluded from target.
    Excluded,
}

/// One field — symmetric source↔target link + converter.
pub struct FieldMap {
    /// `None` → computed (default/materialized).
    pub source: Option<FieldRef>,
    /// `None` → excluded.
    pub target: Option<Field>,
    pub converter: Converter,
}

/// Unified source → target transformation map.
///
/// Built at startup by the engine from: format schema + Rhai script + converters.
/// Used for both DDL generation and per-record data pipeline.
///
/// - DDL: all `target.is_some()` → generate CREATE TABLE
/// - Data pipeline: all `source.is_some() && target.is_some()` → convert per record
pub struct MapSchema {
    /// Full source schema (preserved).
    pub source: Schema,
    /// Final target schema (for DDL).
    pub target: Schema,
    /// Ordered field map (order from Rhai script).
    pub fields: Vec<FieldMap>,
}
