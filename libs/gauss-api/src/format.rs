use std::sync::Arc;

use crate::schema::Schema;
use crate::value::Row;

/// Runtime serializer — performs `bytes ↔ Row`.
///
/// - `deserialize()` — parses raw bytes into `Row`. Uses `Cow::Borrowed`
///   for strings/bytes (zero-copy references into source buffer).
/// - `serialize()` — assembles `Row` back into format bytes.
pub trait FormatSerializer: Send + Sync {
    fn deserialize<'a>(&self, bytes: &'a [u8]) -> Row<'a>;
    fn serialize(&self, row: &Row<'_>) -> Vec<u8>;
}

/// Format plugin — factory. Creates serializer and provides schema.
///
/// - `serializer()` — creates `FormatSerializer` with internal knowledge of
///   schema and format config.
/// - `schema()` — returns `Schema` for Schema Mapping Pipeline (source schema).
///   Returns `None` if the format has no fixed schema and isn't configured with `fields`.
///   If `schema_map` is specified but `schema()` returns `None` — startup error.
pub trait FormatPlugin: Send + Sync {
    fn serializer(&self) -> Arc<dyn FormatSerializer>;
    fn schema(&self) -> Option<Schema>;
}
