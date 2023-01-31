use crate::constants::APP_NAME;

use anyhow::Result;
use config::Config;

#[derive(serde::Deserialize, Clone)]
pub struct BasinConfig {
    pub name: String,
    pub waterwheel_url: String,
}

pub fn init(file: &str) -> Result<BasinConfig> {
    Ok(Config::builder()
        .add_source(config::File::with_name(file))
        .add_source(config::Environment::with_prefix(APP_NAME))
        .build()?
        .try_deserialize::<BasinConfig>()?)
}
