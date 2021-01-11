mod config;
mod error;
mod messages;
mod models;
mod repo;
mod server;
mod updater;
mod websocket;

use bb8_redis::{bb8, RedisConnectionManager};
use error::Error;
use repo::RepoImpl;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use updater::UpdateResource;
use wavesexchange_log::{error, info};

pub type ClientId = usize;

#[derive(Clone, Debug)]
pub struct Client {
    sender: tokio::sync::mpsc::UnboundedSender<Result<warp::ws::Message, warp::Error>>,
    subscriptions: HashSet<String>,
}

pub type Clients = Arc<RwLock<HashMap<ClientId, Client>>>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let app_config = config::load_app()?;
    let repo_config = config::load_repo()?;

    let redis_connection_url = format!(
        "redis://{}:{}@{}:{}/",
        repo_config.username, repo_config.password, repo_config.host, repo_config.port
    );

    let manager = RedisConnectionManager::new(redis_connection_url.clone())?;
    let pool = bb8::Pool::builder().build(manager).await?;
    let repo = RepoImpl::new(pool.clone(), repo_config.subscriptions_key);
    let repo = Arc::new(repo);

    let clients: Clients = Clients::default();

    let (updates_sender, updates_receiver) =
        tokio::sync::mpsc::unbounded_channel::<UpdateResource>();

    let updates_handler_handle = tokio::task::spawn({
        info!("updates handler started");
        websocket::updates_handler(updates_receiver, repo.clone(), clients.clone())
    });

    let redis_conn = redis::Client::open(redis_connection_url)?;
    let updates_handle = tokio::task::spawn_blocking(move || {
        info!("updater started");
        updater::run(redis_conn, updates_sender)
    });

    server::start(app_config.port, repo, clients).await;

    if let Err(e) = tokio::try_join!(updates_handler_handle, updates_handle) {
        let err = Error::from(e);
        error!("{}", err);
        return Err(err);
    };

    Ok(())
}
