/// Parameter type for plugin configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamType {
    Bool,
    I64,
    U64,
    F64,
    Str,
}

/// Context determines when a parameter can be changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamContext {
    /// Set at startup only. Change requires full restart.
    Postmaster,
    /// Can be changed at runtime (SIGHUP / API).
    Sighup,
}

/// Declaration of a single config parameter.
///
/// Plugins export these via `qs_config_params()`.
/// Engine uses them to validate config values BEFORE creating the plugin.
#[derive(Debug, Clone)]
pub struct ConfigParam {
    pub name: String,
    pub param_type: ParamType,
    pub context: ParamContext,
    pub required: bool,
    pub default: Option<ParamValue>,
    pub description: String,
}

/// Typed config value.
#[derive(Debug, Clone, PartialEq)]
pub enum ParamValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(String),
}

/// Validated config values, passed to plugin at creation time.
///
/// Engine builds this from the config source (TOML, YAML, env, ...)
/// after validating against plugin's `ConfigParam` declarations.
/// Plugin reads values via typed getters — no parsing needed.
#[derive(Debug, Clone, Default)]
pub struct ConfigValues {
    entries: Vec<(String, ParamValue)>,
}

impl ConfigValues {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn set(&mut self, name: impl Into<String>, value: ParamValue) {
        let name = name.into();
        if let Some(entry) = self.entries.iter_mut().find(|(k, _)| k == &name) {
            entry.1 = value;
        } else {
            self.entries.push((name, value));
        }
    }

    pub fn get(&self, name: &str) -> Option<&ParamValue> {
        self.entries.iter().find(|(k, _)| k == name).map(|(_, v)| v)
    }

    pub fn get_bool(&self, name: &str) -> Option<bool> {
        match self.get(name) {
            Some(ParamValue::Bool(v)) => Some(*v),
            _ => None,
        }
    }

    pub fn get_i64(&self, name: &str) -> Option<i64> {
        match self.get(name) {
            Some(ParamValue::I64(v)) => Some(*v),
            _ => None,
        }
    }

    pub fn get_u64(&self, name: &str) -> Option<u64> {
        match self.get(name) {
            Some(ParamValue::U64(v)) => Some(*v),
            // Most config formats lack unsigned integers — accept non-negative i64.
            Some(ParamValue::I64(v)) if *v >= 0 => Some(*v as u64),
            _ => None,
        }
    }

    pub fn get_f64(&self, name: &str) -> Option<f64> {
        match self.get(name) {
            Some(ParamValue::F64(v)) => Some(*v),
            _ => None,
        }
    }

    pub fn get_str(&self, name: &str) -> Option<&str> {
        match self.get(name) {
            Some(ParamValue::Str(v)) => Some(v),
            _ => None,
        }
    }
}
