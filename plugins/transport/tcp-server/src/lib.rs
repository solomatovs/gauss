use std::net::TcpListener;

use server_api::{
    parse_plugin_config, plugin_err, plugin_ok,
    TransportStream, Transport, PluginCreateResult, PluginError,
};

pub struct TcpServerTransport {
    addr: String,
    listener: Option<TcpListener>,
}

impl Transport for TcpServerTransport {
    fn start(&mut self) -> Result<(), PluginError> {
        let listener = TcpListener::bind(&self.addr)
            .map_err(|e| PluginError::io(format!("bind {}: {e}", self.addr)))?;
        tracing::info!(addr = %self.addr, "tcp-server listening");
        self.listener = Some(listener);
        Ok(())
    }

    fn next_connection(&mut self) -> Result<Option<Box<dyn TransportStream>>, PluginError> {
        let listener = self.listener.as_ref().ok_or(PluginError::io("transport not started"))?;
        match listener.accept() {
            Ok((stream, addr)) => {
                tracing::info!(peer = %addr, "tcp-server client connected");
                Ok(Some(Box::new(stream)))
            }
            Err(e) => Err(PluginError::io(format!("accept error: {e}"))),
        }
    }

    fn stop(&mut self) -> Result<(), PluginError> {
        self.listener = None;
        Ok(())
    }
}

// ---- FFI ----

#[derive(serde::Deserialize)]
struct TcpServerConfig {
    port: u16,
    #[serde(default)]
    host: Option<String>,
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_transport(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: TcpServerConfig = match unsafe { parse_plugin_config(config_json_ptr, config_json_len) } {
        Ok(c) => c,
        Err(e) => return plugin_err(e.to_string()),
    };

    let host = cfg.host.as_deref().unwrap_or("0.0.0.0");
    let addr = format!("{host}:{}", cfg.port);

    plugin_ok(Box::new(TcpServerTransport {
        addr,
        listener: None,
    }) as Box<dyn Transport>)
}

server_api::qs_destroy_fn!(qs_destroy_transport, Transport);
server_api::qs_abi_version_fn!();
