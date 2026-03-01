use crate::value::Value;

/// Field-level value converter plugin.
///
/// Solves one task: convert a value from one type/format to another.
/// Each converter is a small binary plugin (.so).
///
/// Principle: generic passthrough always works, converter optimizes a specific pair.
/// If types are compatible (both int64, both UTF-8 strings) — no converter needed.
/// If wire format differs — user connects a converter explicitly in the Rhai script.
pub trait FieldConverter: Send + Sync {
    fn convert<'a>(&self, value: &Value<'a>) -> Value<'a>;
}
