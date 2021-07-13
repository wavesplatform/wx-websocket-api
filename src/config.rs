use crate::error::Error;
use crate::repo;
use crate::server;
use serde::Deserialize;
use std::time::Duration;

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

fn default_ttl() -> u64 {
    60
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
    #[serde(default = "default_ttl")]
    pub ttl: u64,
}

pub mod app {
    #[derive(Debug, serde::Deserialize)]
    pub struct Config {
        pub updater_timeout: Option<std::time::Duration>,
    }

    pub fn load() -> Result<Config, crate::error::Error> {
        let config = envy::from_env::<Config>()?;
        Ok(config)
    }
}

pub fn load_repo() -> Result<repo::Config, Error> {
    let flat_config = envy::prefixed("REPO__").from_env::<FlatRepoConfig>()?;

    Ok(repo::Config {
        host: flat_config.host,
        port: flat_config.port,
        username: flat_config.username,
        password: flat_config.password,
        ttl: Duration::from_secs(flat_config.ttl),
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
