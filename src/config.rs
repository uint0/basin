use crate::constants::APP_NAME;

use anyhow::Result;
use aws_config::SdkConfig;
use config::Config;
use serde::Deserialize;

pub struct BasinConfig {
    pub name: String,
    // FIXME: just use the WaterwheelConf struct
    pub waterwheel_username: String,
    pub waterwheel_password: String,
    pub waterwheel_project: String,
    pub waterwheel_url: String,
    pub event_sqs_url: String,
    pub redis_url: String,
    pub aws_creds: SdkConfig,
}

#[derive(Deserialize, Clone)]
struct ConfFileSettings {
    name: String,
    waterwheel: WaterwheelConf,
    event_sqs_url: String,
    redis_url: String,
}

#[derive(Deserialize, Clone)]
struct WaterwheelConf {
    username: String,
    password: String,
    project: String,
    url: String,
}

pub async fn init(file: &str) -> Result<BasinConfig> {
    let conf_file_settings = Config::builder()
        .add_source(config::File::with_name(file))
        .add_source(config::Environment::with_prefix(APP_NAME).separator("__"))
        .build()?
        .try_deserialize::<ConfFileSettings>()?;

    Ok(BasinConfig {
        name: conf_file_settings.name,
        redis_url: conf_file_settings.redis_url,
        event_sqs_url: conf_file_settings.event_sqs_url,
        waterwheel_username: conf_file_settings.waterwheel.username,
        waterwheel_password: conf_file_settings.waterwheel.password,
        waterwheel_project: conf_file_settings.waterwheel.project,
        waterwheel_url: conf_file_settings.waterwheel.url,
        aws_creds: aws_config::load_from_env().await,
    })
}
