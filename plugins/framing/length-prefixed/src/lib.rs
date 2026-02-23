use server_api::{
    plugin_ok, plugin_err, parse_plugin_config_opt,
    PluginCreateResult, PluginError, Framing,
};

#[derive(Clone, Copy)]
enum ByteOrder {
    Big,
    Little,
}

pub struct LengthPrefixedFraming {
    length_bytes: usize,
    byte_order: ByteOrder,
    max_payload: usize,
}

impl Framing for LengthPrefixedFraming {
    fn decode(&self, buf: &[u8]) -> Result<Option<(Vec<u8>, usize)>, PluginError> {
        // Нужно минимум length_bytes для заголовка
        if buf.len() < self.length_bytes {
            return Ok(None);
        }

        // Декодируем длину payload из заголовка
        let n = self.length_bytes;
        let len = match (n, self.byte_order) {
            (1, _) => buf[0] as usize,
            (2, ByteOrder::Big) => u16::from_be_bytes([buf[0], buf[1]]) as usize,
            (2, ByteOrder::Little) => u16::from_le_bytes([buf[0], buf[1]]) as usize,
            (4, ByteOrder::Big) => u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize,
            (4, ByteOrder::Little) => u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize,
            _ => return Err(PluginError::config(format!("unsupported length_bytes: {n}"))),
        };

        if self.max_payload > 0 && len > self.max_payload {
            return Err(PluginError::format_err(format!(
                "payload too large: {len} bytes (max {})",
                self.max_payload
            )));
        }

        let total = self.length_bytes + len;
        if buf.len() < total {
            return Ok(None); // Не хватает данных для полного фрейма
        }

        let payload = buf[self.length_bytes..total].to_vec();
        Ok(Some((payload, total)))
    }

    fn encode(&self, data: &[u8], buf: &mut Vec<u8>) -> Result<(), PluginError> {
        let len = data.len();
        let n = self.length_bytes;
        match (n, self.byte_order) {
            (1, _) => {
                if len > u8::MAX as usize {
                    return Err(PluginError::format_err(format!("payload too large for 1-byte header: {len}")));
                }
                buf.push(len as u8);
            }
            (2, ByteOrder::Big) => {
                if len > u16::MAX as usize {
                    return Err(PluginError::format_err(format!("payload too large for 2-byte header: {len}")));
                }
                buf.extend_from_slice(&(len as u16).to_be_bytes());
            }
            (2, ByteOrder::Little) => {
                if len > u16::MAX as usize {
                    return Err(PluginError::format_err(format!("payload too large for 2-byte header: {len}")));
                }
                buf.extend_from_slice(&(len as u16).to_le_bytes());
            }
            (4, ByteOrder::Big) => {
                buf.extend_from_slice(&(len as u32).to_be_bytes());
            }
            (4, ByteOrder::Little) => {
                buf.extend_from_slice(&(len as u32).to_le_bytes());
            }
            _ => return Err(PluginError::config(format!("unsupported length_bytes: {n}"))),
        }
        buf.extend_from_slice(data);
        Ok(())
    }
}

// ---- FFI ----

#[derive(serde::Deserialize)]
#[serde(default)]
struct LengthPrefixedConfig {
    /// Размер заголовка длины в байтах: 1, 2 или 4 (по умолчанию 4).
    length_bytes: usize,

    /// Порядок байтов: "big" (по умолчанию) или "little".
    byte_order: String,

    /// Максимальный размер payload в байтах (0 = без ограничения).
    max_payload: usize,
}

impl Default for LengthPrefixedConfig {
    fn default() -> Self {
        Self {
            length_bytes: 4,
            byte_order: "big".to_string(),
            max_payload: 0,
        }
    }
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_framing(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: LengthPrefixedConfig = match unsafe { parse_plugin_config_opt(config_json_ptr, config_json_len) } {
        Ok(c) => c,
        Err(e) => return plugin_err(e.to_string()),
    };

    if !matches!(cfg.length_bytes, 1 | 2 | 4) {
        return plugin_err(format!(
            "length_bytes must be 1, 2, or 4, got {}",
            cfg.length_bytes
        ));
    }

    let byte_order = match cfg.byte_order.as_str() {
        "big" | "be" => ByteOrder::Big,
        "little" | "le" => ByteOrder::Little,
        other => {
            return plugin_err(format!(
                "byte_order must be \"big\" or \"little\", got {other:?}"
            ));
        }
    };

    plugin_ok(Box::new(LengthPrefixedFraming {
        length_bytes: cfg.length_bytes,
        byte_order,
        max_payload: cfg.max_payload,
    }) as Box<dyn Framing>)
}

server_api::qs_destroy_fn!(qs_destroy_framing, Framing);
server_api::qs_abi_version_fn!();
