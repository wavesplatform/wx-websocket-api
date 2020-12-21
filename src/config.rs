use crate::error::Error;
use crate::repo;
use serde::Deserialize;

fn default_port() -> u16 {
    8080
}

pub struct AppConfig {
    pub port: u16,
}

#[derive(Deserialize)]
struct FlatAppConfig {
    #[serde(default = "default_port")]
    pub port: u16,
}

#[derive(Deserialize)]
struct FlatRepoConfig {
    pub host: String,
    pub subscriptions_key: String,
}

pub fn load_repo() -> Result<repo::Config, Error> {
    let flat_config = envy::prefixed("REPO__").from_env::<FlatRepoConfig>()?;

    Ok(repo::Config {
        host: flat_config.host,
        subscriptions_key: flat_config.subscriptions_key,
    })
}

pub fn load_app() -> Result<AppConfig, Error> {
    let flat_config = envy::from_env::<FlatAppConfig>()?;

    Ok(AppConfig {
        port: flat_config.port,
    })
}
