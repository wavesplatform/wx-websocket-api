mod client;
mod config;
mod error;
mod messages;
mod metrics;
mod models;
mod refresher;
mod repo;
mod server;
mod shard;
mod updater;
mod websocket;

use bb8_redis::{bb8, RedisConnectionManager};
use error::Error;
use refresher::KeysRefresher;
use repo::RepoImpl;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use wavesexchange_log::{debug, error};

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
    let repo = Arc::new(RepoImpl::new(pool.clone(), repo_config.key_ttl));

    let keys_refresher = KeysRefresher::new(repo.clone(), repo_config.key_ttl, topics.clone());
    let keys_refresher_handle = tokio::spawn(async move { keys_refresher.run().await });

    let (updates_sender, updates_receiver) = tokio::sync::mpsc::unbounded_channel();

    let websocket_updates_handler_handle = {
        let clients = clients.clone();
        let topics = topics.clone();
        tokio::task::spawn(websocket::updates_handler(
            updates_receiver,
            clients,
            topics,
        ))
    };

    let redis_conn = redis::Client::open(redis_connection_url)?;
    let conn = redis_conn.clone();

    let updater_handle = tokio::task::spawn_blocking(move || {
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
    let server_handle = tokio::spawn(server);

    let mut sigterm_stream =
        signal(SignalKind::terminate()).expect("error occurred while creating sigterm stream");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            debug!("got sigint");
        },
        _ = sigterm_stream.recv() => {
            debug!("got sigterm");
        },
        r = keys_refresher_handle => {
            error!("keys_refresher finished: {:?}", r);
        },
        r = updater_handle => {
            error!("updater finished: {:?}", r);
        },
        r = websocket_updates_handler_handle => {
            error!("websocket updates handler finished: {:?}", r);
        }
    }

    // graceful shutdown for websocket connections
    shutdown_signal_rx.close();

    let _ = server_stop_tx.send(());
    server_handle.await?;
    Ok(())
}
