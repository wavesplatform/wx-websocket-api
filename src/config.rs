use crate::error::Error;
use crate::repo;
use crate::server;
use serde::Deserialize;

fn default_port() -> u16 {
    8080
}

fn default_repo_port() -> u16 {
    6379
}

fn default_client_ping_interval_in_secs() -> u64 {
    30
}

fn default_client_ping_failures_threshold() -> u16 {
    3
}

#[derive(Deserialize)]
struct FlatServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_client_ping_interval_in_secs")]
    pub client_ping_interval_in_secs: u64,
    #[serde(default = "default_client_ping_failures_threshold")]
    pub client_ping_failures_threshold: u16,
}

#[derive(Deserialize)]
struct FlatRepoConfig {
    pub host: String,
    #[serde(default = "default_repo_port")]
    pub port: u16,
    pub username: String,
    pub password: String,
    pub subscriptions_key: String,
}

pub fn load_repo() -> Result<repo::Config, Error> {
    let flat_config = envy::prefixed("REPO__").from_env::<FlatRepoConfig>()?;

    Ok(repo::Config {
        host: flat_config.host,
        port: flat_config.port,
        username: flat_config.username,
        password: flat_config.password,
        subscriptions_key: flat_config.subscriptions_key,
    })
}

pub fn load_server() -> Result<server::ServerConfig, Error> {
    let flat_config = envy::from_env::<FlatServerConfig>()?;

    Ok(server::ServerConfig {
        port: flat_config.port,
        client_ping_interval: flat_config.client_ping_interval_in_secs,
        client_ping_failures_threshold: flat_config.client_ping_failures_threshold,
    })
}
