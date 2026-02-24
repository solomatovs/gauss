use std::io::Write;

use serde::{Deserialize, Serialize};
use server_api::{PluginError, TopicRecord, now_ms};

use pipeline::Endpoint;

use super::config::SinkConfig;
use super::error::QuotesGenError;

// ═══════════════════════════════════════════════════════════════
//  Quote type (for generation only)
// ═══════════════════════════════════════════════════════════════

#[derive(Debug, Serialize, Deserialize)]
pub struct Quote {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_ms: Option<i64>,
}

pub fn quote_to_record(quote: &Quote) -> Result<TopicRecord, PluginError> {
    let value = serde_json::to_value(quote)?;
    Ok(TopicRecord {
        ts_ms: quote.ts_ms.unwrap_or_else(now_ms),
        key: quote.symbol.clone(),
        value,
        raw: None,
    })
}

// ═══════════════════════════════════════════════════════════════
//  Sink pipeline (Endpoint wrapper with connection management)
// ═══════════════════════════════════════════════════════════════

pub struct SinkPipeline {
    pub endpoint: Endpoint,
    pub stream: Option<Box<dyn server_api::TransportStream>>,
    pub buf: Vec<u8>,
}

impl SinkPipeline {
    pub fn name(&self) -> &str {
        &self.endpoint.name
    }

    pub fn ensure_connected(&mut self) -> Result<(), PluginError> {
        if self.stream.is_none() {
            let stream = self
                .endpoint
                .transport
                .next_connection()?
                .ok_or_else(|| PluginError::io(format!("[{}] transport returned None", self.endpoint.name)))?;
            self.stream = Some(stream);
        }
        Ok(())
    }

    pub fn send_quote(&mut self, quote: &Quote) -> Result<(), PluginError> {
        let record = quote_to_record(quote)?;
        self.buf.clear();
        self.endpoint.encode_to_wire(&record, &mut self.buf)?;
        self.ensure_connected()?;
        self.stream
            .as_mut()
            .unwrap()
            .write_all(&self.buf)?;
        Ok(())
    }

    pub fn send_raw(&mut self, raw: &[u8]) -> Result<(), PluginError> {
        self.buf.clear();
        self.endpoint.framing.encode(raw, &mut self.buf)?;
        self.ensure_connected()?;
        self.stream
            .as_mut()
            .unwrap()
            .write_all(&self.buf)?;
        Ok(())
    }

    pub fn send_quote_reconnect(&mut self, quote: &Quote) -> Result<(), PluginError> {
        match self.send_quote(quote) {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::warn!(sink = %self.endpoint.name, error = ?e, "send error, reconnecting");
                self.stream = None;
                self.send_quote(quote)
            }
        }
    }

    pub fn send_raw_reconnect(&mut self, raw: &[u8]) -> Result<(), PluginError> {
        match self.send_raw(raw) {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::warn!(sink = %self.endpoint.name, error = ?e, "send error, reconnecting");
                self.stream = None;
                self.send_raw(raw)
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════
//  Load sink pipelines from config
// ═══════════════════════════════════════════════════════════════

pub fn load_pipelines(sinks: &[SinkConfig]) -> Result<Vec<SinkPipeline>, QuotesGenError> {
    let mut pipelines = Vec::new();

    for sink_cfg in sinks {
        let endpoint = Endpoint::load(
            &sink_cfg.name,
            &sink_cfg.transport,
            &sink_cfg.transport_config,
            &sink_cfg.framing,
            &sink_cfg.framing_config,
            &sink_cfg.middleware,
            &sink_cfg.codec,
            &sink_cfg.codec_config,
        )?;

        pipelines.push(SinkPipeline {
            endpoint,
            stream: None,
            buf: Vec::with_capacity(8192),
        });
    }

    Ok(pipelines)
}
