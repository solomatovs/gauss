use server_api::{
    plugin_ok, plugin_err, parse_plugin_config_opt,
    PluginCreateResult, PluginError, Framing,
};

pub struct LinesFraming {
    max_length: usize,
}

impl Framing for LinesFraming {
    fn decode(&self, buf: &[u8]) -> Result<Option<(Vec<u8>, usize)>, PluginError> {
        // Ищем первый \n в буфере
        let pos = match buf.iter().position(|&b| b == b'\n') {
            Some(p) => p,
            None => {
                // Нет полной строки — проверяем max_length
                if self.max_length > 0 && buf.len() > self.max_length {
                    return Err(PluginError::format_err(format!(
                        "line too long: {} bytes (max {}) and no newline found",
                        buf.len(),
                        self.max_length
                    )));
                }
                return Ok(None);
            }
        };

        let consumed = pos + 1; // включая \n

        // Извлекаем содержимое строки (до \n), обрезаем \r
        let line = &buf[..pos];
        let line = if line.last() == Some(&b'\r') {
            &line[..line.len() - 1]
        } else {
            line
        };

        if self.max_length > 0 && line.len() > self.max_length {
            return Err(PluginError::format_err(format!(
                "line too long: {} bytes (max {})",
                line.len(),
                self.max_length
            )));
        }

        Ok(Some((line.to_vec(), consumed)))
    }

    fn encode(&self, data: &[u8], buf: &mut Vec<u8>) -> Result<(), PluginError> {
        buf.extend_from_slice(data);
        buf.push(b'\n');
        Ok(())
    }
}

// ---- FFI ----

#[derive(Default, serde::Deserialize)]
struct LinesConfig {
    /// Максимальная длина строки в байтах (0 = без ограничения).
    #[serde(default)]
    max_length: usize,
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_framing(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: LinesConfig = match unsafe { parse_plugin_config_opt(config_json_ptr, config_json_len) } {
        Ok(c) => c,
        Err(e) => return plugin_err(e.to_string()),
    };

    plugin_ok(Box::new(LinesFraming { max_length: cfg.max_length }) as Box<dyn Framing>)
}

server_api::qs_destroy_fn!(qs_destroy_framing, Framing);
server_api::qs_abi_version_fn!();
