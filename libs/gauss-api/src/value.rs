use std::borrow::Cow;

/// Canonical value representation.
///
/// Strategy by type:
/// - Scalars (Int64, Float64, Bool): eager parse, cost ~0
/// - Decimal, Timestamp: eager parse, binary layout incompatible between formats
/// - String, Bytes: `Cow` (zero-copy when possible)
/// - Array, Map, Tuple: recursive eager parse
pub enum Value<'a> {
    Int64(i64),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    /// `(value, scale)` — eager, layout incompatible between formats.
    Decimal(i128, u8),
    /// `(micros, precision)` — eager.
    Timestamp(i64, u8),

    /// Raw bytes, not necessarily UTF-8. Source encoding may vary.
    String(Cow<'a, [u8]>),
    /// Opaque binary data (UUID, IP, JSONB, etc.).
    Bytes(Cow<'a, [u8]>),

    /// Recursive — elements parsed individually.
    Array(Vec<Value<'a>>),
    Map(Vec<(Value<'a>, Value<'a>)>),
    Tuple(Vec<Value<'a>>),

    Null,
}

/// Positional array of values. Order matches `Schema.fields`.
///
/// Maximally lightweight — values only, no names or types.
/// All metadata (names, types, converters) lives in `MapSchema`.
pub struct Row<'a>(pub Vec<Value<'a>>);
