mod client;
mod config;
mod error;
mod messages;
mod metrics;
mod models;
mod repo;
mod server;
mod shard;
mod updater;
mod websocket;

use bb8_redis::{bb8, RedisConnectionManager};
use error::Error;
use repo::{Refresher, RepoImpl};
use std::sync::Arc;
use wavesexchange_log::{error, info};

fn main() -> Result<(), Error> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt.block_on(tokio_main());
    rt.shutdown_timeout(std::time::Duration::from_millis(1));
    result
}

async fn tokio_main() -> Result<(), Error> {
    let repo_config = config::load_repo()?;
    let server_config = config::load_server()?;

    metrics::register_metrics();

    let redis_connection_url = format!(
        "redis://{}:{}@{}:{}/",
        repo_config.username, repo_config.password, repo_config.host, repo_config.port
    );

    let clients = Arc::new(shard::Sharded::<client::Clients>::new(20));
    let topics = Arc::new(shard::Sharded::<client::Topics>::new(20));

    let manager = RedisConnectionManager::new(redis_connection_url.clone())?;
    let pool = bb8::Pool::builder().build(manager).await?;
    let repo = Arc::new(RepoImpl::new(pool.clone(), repo_config.ttl));

    let refresher = Refresher::new(repo.clone(), repo_config.ttl, topics.clone());
    let refresher_handle = tokio::spawn(async move { refresher.run().await });

    let (updates_sender, updates_receiver) = tokio::sync::mpsc::unbounded_channel();

    let updates_handler_handle = tokio::task::spawn({
        info!("updates handler started");
        websocket::updates_handler(updates_receiver, clients.clone(), topics.clone())
    });

    let redis_conn = redis::Client::open(redis_connection_url)?;
    let conn = redis_conn.clone();
    let updates_handle = tokio::task::spawn_blocking(move || {
        info!("updater started");
        updater::run(conn, updates_sender)
    });

    let server_options = server::ServerOptions {
        client_ping_interval: tokio::time::Duration::from_secs(server_config.client_ping_interval),
        client_ping_failures_threshold: server_config.client_ping_failures_threshold,
    };
    let (server_stop_tx, server) =
        server::start(server_config.port, repo, clients, topics, server_options);
    let server_handler = tokio::spawn(server);

    let updates_future = async {
        if let Err(e) = tokio::try_join!(updates_handler_handle, updates_handle, refresher_handle,)
        {
            let err = Error::from(e);
            error!("{}", err);
            return Err(err);
        };
        Ok(())
    };
    let signal = tokio::signal::ctrl_c();
    tokio::select! {
        _ = signal => {},
        _ = updates_future => {}
    }
    let _ = server_stop_tx.send(());
    server_handler.await?;
    Ok(())
}
