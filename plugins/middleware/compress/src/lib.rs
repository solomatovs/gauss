use std::io::Read;

use server_api::{
    parse_plugin_config, plugin_err, plugin_ok,
    Middleware, PluginCreateResult, PluginError,
};

// ---- Config ----

#[derive(serde::Deserialize)]
struct CompressConfig {
    /// Алгоритм сжатия: "gzip" или "lz4".
    algorithm: String,
    /// Уровень сжатия для gzip (1-9, default 6). Для lz4 игнорируется.
    #[serde(default = "default_level")]
    level: u32,
}

fn default_level() -> u32 {
    6
}

// ---- Algorithm ----

enum Algorithm {
    Gzip(flate2::Compression),
    Lz4,
}

// ---- CompressMiddleware ----

struct CompressMiddleware {
    algorithm: Algorithm,
}

impl Middleware for CompressMiddleware {
    fn decode(&self, data: Vec<u8>) -> Result<Vec<u8>, PluginError> {
        match &self.algorithm {
            Algorithm::Gzip(_) => {
                let mut decoder = flate2::read::GzDecoder::new(&data[..]);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| PluginError::new(format!("gzip decompress: {e}")))?;
                Ok(decompressed)
            }
            Algorithm::Lz4 => {
                lz4_flex::decompress_size_prepended(&data)
                    .map_err(|e| PluginError::new(format!("lz4 decompress: {e}")))
            }
        }
    }

    fn encode(&self, data: Vec<u8>) -> Result<Vec<u8>, PluginError> {
        match &self.algorithm {
            Algorithm::Gzip(level) => {
                let mut encoder = flate2::read::GzEncoder::new(&data[..], *level);
                let mut compressed = Vec::new();
                encoder
                    .read_to_end(&mut compressed)
                    .map_err(|e| PluginError::new(format!("gzip compress: {e}")))?;
                Ok(compressed)
            }
            Algorithm::Lz4 => {
                Ok(lz4_flex::compress_prepend_size(&data))
            }
        }
    }
}

// ---- FFI ----

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_middleware(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: CompressConfig =
        match unsafe { parse_plugin_config(config_json_ptr, config_json_len) } {
            Ok(c) => c,
            Err(e) => return plugin_err(e.to_string()),
        };

    let algorithm = match cfg.algorithm.as_str() {
        "gzip" => {
            let level = cfg.level.clamp(1, 9);
            Algorithm::Gzip(flate2::Compression::new(level))
        }
        "lz4" => Algorithm::Lz4,
        other => {
            return plugin_err(format!(
                "compress: unknown algorithm '{other}'. Supported: gzip, lz4"
            ))
        }
    };

    plugin_ok(Box::new(CompressMiddleware { algorithm }) as Box<dyn Middleware>)
}

server_api::qs_destroy_fn!(qs_destroy_middleware, Middleware);
server_api::qs_abi_version_fn!();
