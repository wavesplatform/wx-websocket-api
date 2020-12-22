mod config;
mod error;
mod messages;
mod models;
mod repo;
mod server;
mod updater;

use bb8_redis::{bb8, RedisConnectionManager};
use crossfire::mpsc;
use error::Error;
use messages::PreOutcomeMessage;
use repo::RepoImpl;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use wavesexchange_log::{error, info};
use tracing_subscriber;

pub type ConnectionId = usize;

pub type Connection = Arc<mpsc::TxUnbounded<PreOutcomeMessage>>;
pub type Connections = Arc<RwLock<HashMap<ConnectionId, Connection>>>;

pub type Subscribtions = Arc<RwLock<HashMap<ConnectionId, HashSet<String>>>>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let app_config = config::load_app()?;
    let repo_config = config::load_repo()?;

    let redis_connection_url = format!(
        "redis://{}:{}@{}:{}/",
        repo_config.username, repo_config.password, repo_config.host, repo_config.port
    );

    let manager = RedisConnectionManager::new(redis_connection_url.clone())?;
    let pool = bb8::Pool::builder().build(manager).await?;
    let repo = RepoImpl::new(pool.clone(), repo_config.subscriptions_key);

    let connections: Connections = Connections::default();
    let subscriptions: Subscribtions = Subscribtions::default();

    let redis_conn = redis::Client::open(redis_connection_url)?;
    let updates_handle = tokio::task::spawn({
        info!("updater started");
        updater::run(connections.clone(), subscriptions.clone(), redis_conn)
    });

    server::start(app_config.port, repo, connections, subscriptions).await;

    if let Err(e) = tokio::try_join!(updates_handle) {
        let err = Error::from(e);
        error!("subscriptions error: {}", err);
        return Err(err);
    };

    Ok(())
}
