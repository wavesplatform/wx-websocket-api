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
use messages::OutcomeMessage;
use models::Topic;
use repo::RepoImpl;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use wavesexchange_log::{error, info};

pub type ClientId = usize;

#[derive(Debug)]
pub struct Client {
    sender: tokio::sync::mpsc::UnboundedSender<Result<warp::ws::Message, warp::Error>>,
    subscriptions: HashSet<String>,
    message_counter: i64,
    pings: Vec<i64>,
}

impl Client {
    fn send(&mut self, message: OutcomeMessage) -> Result<(), Error> {
        self.message_counter += 1;
        self.sender.send(Ok(warp::ws::Message::from(message)))?;
        Ok(())
    }
}

pub type Clients = Arc<RwLock<HashMap<ClientId, Client>>>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let repo_config = config::load_repo()?;
    let server_config = config::load_server()?;

    let redis_connection_url = format!(
        "redis://{}:{}@{}:{}/",
        repo_config.username, repo_config.password, repo_config.host, repo_config.port
    );

    let manager = RedisConnectionManager::new(redis_connection_url.clone())?;
    let pool = bb8::Pool::builder().build(manager).await?;
    let repo = RepoImpl::new(pool.clone(), repo_config.subscriptions_key);
    let repo = Arc::new(repo);

    let clients: Clients = Clients::default();

    let (updates_sender, updates_receiver) = tokio::sync::mpsc::unbounded_channel::<Topic>();

    let updates_handler_handle = tokio::task::spawn({
        info!("updates handler started");
        websocket::updates_handler(updates_receiver, repo.clone(), clients.clone())
    });

    let redis_conn = redis::Client::open(redis_connection_url)?;
    let updates_handle = tokio::task::spawn_blocking(move || {
        info!("updater started");
        updater::run(redis_conn, updates_sender)
    });

    let server_options = server::ServerOptions {
        client_ping_interval: tokio::time::Duration::from_secs(server_config.client_ping_interval),
        client_ping_failures_threshold: server_config.client_ping_failures_threshold,
    };
    server::start(server_config.port, repo, clients, server_options).await;

    if let Err(e) = tokio::try_join!(updates_handler_handle, updates_handle) {
        let err = Error::from(e);
        error!("{}", err);
        return Err(err);
    };

    Ok(())
}
