use std::fmt;

/// Error kind for plugin errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Config,
    Io,
    Format,
    Schema,
    Logic,
}

/// Plugin error — returned by all plugin trait methods.
#[derive(Debug)]
pub struct PluginError {
    pub kind: ErrorKind,
    pub message: String,
}

impl PluginError {
    pub fn config(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Config, message: msg.into() }
    }

    pub fn io(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Io, message: msg.into() }
    }

    pub fn format(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Format, message: msg.into() }
    }

    pub fn schema(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Schema, message: msg.into() }
    }

    pub fn logic(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Logic, message: msg.into() }
    }

    /// Add context to the error, preserving the original ErrorKind.
    ///
    /// Produces: `"context: original message"`.
    pub fn with_context(self, ctx: impl fmt::Display) -> Self {
        Self {
            kind: self.kind,
            message: format!("{ctx}: {}", self.message),
        }
    }
}

impl fmt::Display for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for PluginError {}

// ---------------------------------------------------------------------------
// From impls: standard error types → PluginError with correct ErrorKind
// ---------------------------------------------------------------------------

impl From<std::io::Error> for PluginError {
    fn from(e: std::io::Error) -> Self {
        Self::io(e.to_string())
    }
}

impl From<serde_json::Error> for PluginError {
    fn from(e: serde_json::Error) -> Self {
        Self::format(e.to_string())
    }
}

impl From<std::str::Utf8Error> for PluginError {
    fn from(e: std::str::Utf8Error) -> Self {
        Self::format(e.to_string())
    }
}

impl From<std::string::FromUtf8Error> for PluginError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Self::format(e.to_string())
    }
}
