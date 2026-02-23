use std::net::TcpStream;

use server_api::{
    parse_plugin_config, plugin_err, plugin_ok,
    TransportStream, Transport, PluginCreateResult, PluginError,
};

pub struct TcpClientTransport {
    addr: String,
    connected: bool,
}

impl Transport for TcpClientTransport {
    fn start(&mut self) -> Result<(), PluginError> {
        Ok(())
    }

    fn next_connection(&mut self) -> Result<Option<Box<dyn TransportStream>>, PluginError> {
        if self.connected {
            // Single-connection transport: after connecting once, signal "no more".
            return Ok(None);
        }
        let stream = TcpStream::connect(&self.addr)
            .map_err(|e| PluginError::io(format!("TCP connect to {}: {e}", self.addr)))?;
        self.connected = true;
        tracing::info!(addr = %self.addr, "tcp-client connected");
        Ok(Some(Box::new(stream)))
    }

    fn stop(&mut self) -> Result<(), PluginError> {
        Ok(())
    }
}

// ---- FFI ----

#[derive(serde::Deserialize)]
struct TcpClientConfig {
    host: String,
    port: u16,
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_transport(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: TcpClientConfig = match unsafe { parse_plugin_config(config_json_ptr, config_json_len) } {
        Ok(c) => c,
        Err(e) => return plugin_err(e.to_string()),
    };

    let addr = format!("{}:{}", cfg.host, cfg.port);
    plugin_ok(Box::new(TcpClientTransport { addr, connected: false }) as Box<dyn Transport>)
}

server_api::qs_destroy_fn!(qs_destroy_transport, Transport);
server_api::qs_abi_version_fn!();
