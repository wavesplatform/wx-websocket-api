mod config;
mod error;
mod messages;
mod models;
mod repo;
mod server;
mod updater;

use bb8_redis::{bb8, RedisConnectionManager};
use error::Error;
use repo::RepoImpl;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use wavesexchange_log::{error, info};

pub type ConnectionId = usize;

pub type Connections = Arc<
    RwLock<HashMap<ConnectionId, Arc<RwLock<mpsc::UnboundedSender<messages::PreOutcomeMessage>>>>>,
>;

pub type Subscribtions = Arc<RwLock<HashMap<ConnectionId, HashSet<String>>>>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let app_config = config::load_app()?;
    let repo_config = config::load_repo()?;

    let manager = RedisConnectionManager::new(repo_config.host).unwrap();
    let pool = bb8::Pool::builder().build(manager).await.unwrap();
    let repo = RepoImpl::new(pool.clone(), repo_config.subscriptions_counter_key);

    let connections: Connections = Connections::default();
    let subscriptions: Subscribtions = Subscribtions::default();

    let updates_handle = tokio::task::spawn({
        info!("updater started");
        updater::run(connections.clone(), subscriptions.clone(), pool.clone())
    });

    server::start(app_config.port, repo, connections, subscriptions).await;

    if let Err(e) = tokio::try_join!(updates_handle) {
        let err = Error::from(e);
        error!("subscriptions error: {}", err);
        return Err(err);
    };

    Ok(())
}
