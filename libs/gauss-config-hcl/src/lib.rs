use gauss_engine::config::{ConfigParser, GaussConfig};
use gauss_engine::error::EngineError;

pub struct HclParser;

impl ConfigParser for HclParser {
    fn extensions(&self) -> &[&str] {
        &["hcl"]
    }

    fn parse(&self, content: &str) -> Result<GaussConfig, EngineError> {
        hcl::from_str(content).map_err(|e| EngineError::Config(e.to_string()))
    }
}
