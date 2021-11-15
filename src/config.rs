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

fn default_key_ttl() -> u64 {
    60
}

fn default_max_pool_size() -> u32 {
    10
}

fn default_refresh_threads() -> u16 {
    1
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
    #[serde(default = "default_key_ttl")]
    pub key_ttl: u64,
    #[serde(default = "default_max_pool_size")]
    pub max_pool_size: u32,
    #[serde(default = "default_refresh_threads")]
    pub refresh_threads: u16,
}

pub mod app {
    use crate::error::Error;

    #[derive(Debug, serde::Deserialize)]
    struct FlatConfig {
        pub updater_timeout_in_secs: Option<i64>,
    }

    #[derive(Debug)]
    pub struct Config {
        pub updater_timeout: Option<std::time::Duration>,
    }

    pub fn load() -> Result<Config, Error> {
        let flat_config = envy::from_env::<FlatConfig>()?;

        let updater_timeout_in_secs = match flat_config.updater_timeout_in_secs {
            Some(updater_timeout_in_secs) if updater_timeout_in_secs > 0 => Ok(Some(
                std::time::Duration::from_secs(updater_timeout_in_secs as u64),
            )),
            Some(updater_timeout_in_secs) if updater_timeout_in_secs < 0 => Ok(None),
            None => Ok(None),
            _ => Err(Error::ConfigLoadError(envy::Error::Custom(
                "Updater timeout cannot be zero".to_string(),
            ))),
        }?;

        Ok(Config {
            updater_timeout: updater_timeout_in_secs,
        })
    }
}

pub fn load_repo() -> Result<repo::Config, Error> {
    let flat_config = envy::prefixed("REPO__").from_env::<FlatRepoConfig>()?;

    if flat_config.refresh_threads as u32 >= flat_config.max_pool_size {
        return Err(Error::ConfigValidationError(format!(
            "Value of REFRESH_THREADS ({}) must be less that value of MAX_POOL_SIZE ({})",
            flat_config.refresh_threads, flat_config.max_pool_size
        )));
    }

    Ok(repo::Config {
        host: flat_config.host,
        port: flat_config.port,
        username: flat_config.username,
        password: flat_config.password,
        key_ttl: Duration::from_secs(flat_config.key_ttl),
        max_pool_size: flat_config.max_pool_size,
        refresh_threads: flat_config.refresh_threads,
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
