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
use wavesexchange_log::{debug, error, info};

fn main() -> Result<(), Error> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt.block_on(tokio_main());
    rt.shutdown_timeout(std::time::Duration::from_millis(1));
    result
}

async fn tokio_main() -> Result<(), Error> {
    let app_config = config::app::load()?;
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

    let websocket_updates_handler_handle = tokio::spawn({
        info!("websocket updates handler started");
        websocket::updates_handler(updates_receiver, clients.clone(), topics.clone())
    });

    let redis_conn = redis::Client::open(redis_connection_url)?;
    let conn = redis_conn.clone();

    let updater_handle = tokio::task::spawn_blocking(move || {
        info!("updater started");
        let err = updater::run(conn, app_config.updater_timeout, updates_sender);
        error!("updater returned an err: {:?}", err);
        err
    });

    let server_options = server::ServerOptions {
        client_ping_interval: tokio::time::Duration::from_secs(server_config.client_ping_interval),
        client_ping_failures_threshold: server_config.client_ping_failures_threshold,
    };
    let (shutdown_signal_tx, mut shutdown_signal_rx) = tokio::sync::mpsc::channel(1);
    let (server_stop_tx, server) = server::start(
        server_config.port,
        repo,
        clients,
        topics,
        server_options,
        shutdown_signal_tx,
    );
    let server_handler = tokio::spawn(server);

    let updates_future = async {
        if let Err(e) = tokio::try_join!(websocket_updates_handler_handle,) {
            let err = Error::from(e);
            error!("got an error: {}", err);
            return Err(err);
        };
        Ok(())
    };

    tokio::select! {
        _ =
        tokio::signal::ctrl_c() => {
            debug!("got sigint");
        },
        _ = refresher_handle => {
            debug!("refresher finished");
        },
        _ = updater_handle => {
            debug!("updater finished");
        },
        _ = updates_future => {
            debug!("updates finished");
        }
    }

    // graceful shutdown for websocket connections
    shutdown_signal_rx.close();

    let _ = server_stop_tx.send(());
    server_handler.await?;
    Ok(())
}
