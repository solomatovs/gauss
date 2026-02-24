/// Category of a plugin error. Allows the host to make intelligent
/// decisions about error handling (skip, retry, fail fast).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Invalid configuration — permanent, fail at startup.
    Config,
    /// I/O or network error — transient, may retry/reconnect.
    Io,
    /// Data format/parse error — bad input, skip record.
    Format,
    /// Logical error (not found, invalid state, generic).
    Logic,
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Config => f.write_str("config"),
            ErrorKind::Io => f.write_str("io"),
            ErrorKind::Format => f.write_str("format"),
            ErrorKind::Logic => f.write_str("logic"),
        }
    }
}

/// Unified error type for all plugin trait methods.
///
/// Carries an `ErrorKind` for categorization and a human-readable message.
/// `From` impls assign the appropriate kind automatically and allow
/// ergonomic `?` in plugin implementations.
#[derive(Clone)]
pub struct PluginError {
    kind: ErrorKind,
    message: String,
}

impl PluginError {
    /// Generic logic error (default kind).
    pub fn new(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Logic, message: msg.into() }
    }

    /// Configuration error — permanent, fail at startup.
    pub fn config(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Config, message: msg.into() }
    }

    /// I/O error — transient, may retry/reconnect.
    pub fn io(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Io, message: msg.into() }
    }

    /// Format/parse error — bad input, skip record.
    pub fn format_err(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Format, message: msg.into() }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Debug for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.kind, self.message)
    }
}

impl std::fmt::Display for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PluginError {}

impl From<String> for PluginError {
    fn from(s: String) -> Self { Self { kind: ErrorKind::Logic, message: s } }
}

impl From<&str> for PluginError {
    fn from(s: &str) -> Self { Self { kind: ErrorKind::Logic, message: s.to_string() } }
}

impl From<std::io::Error> for PluginError {
    fn from(e: std::io::Error) -> Self { Self { kind: ErrorKind::Io, message: e.to_string() } }
}

impl From<serde_json::Error> for PluginError {
    fn from(e: serde_json::Error) -> Self { Self { kind: ErrorKind::Format, message: e.to_string() } }
}

impl From<std::str::Utf8Error> for PluginError {
    fn from(e: std::str::Utf8Error) -> Self { Self { kind: ErrorKind::Format, message: e.to_string() } }
}

impl From<std::string::FromUtf8Error> for PluginError {
    fn from(e: std::string::FromUtf8Error) -> Self { Self { kind: ErrorKind::Format, message: e.to_string() } }
}
