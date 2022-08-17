extern crate wavesexchange_log as log;

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
mod topic;
mod updater;
mod websocket;

use bb8_redis::{bb8, RedisConnectionManager};
use error::Error;
use refresher::KeysRefresher;
use repo::RepoImpl;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};

fn main() -> Result<(), Error> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt.block_on(tokio_main());
    rt.shutdown_timeout(Duration::from_millis(1));
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
    let topics = Arc::new(client::Topics::default());

    let manager = RedisConnectionManager::new(redis_connection_url.clone())?;
    let pool = bb8::Pool::builder()
        .max_size(repo_config.max_pool_size)
        .build(manager)
        .await?;
    let repo = Arc::new(RepoImpl::new(
        pool.clone(),
        repo_config.key_ttl,
        repo_config.refresh_threads as usize,
    ));

    let keys_refresher = KeysRefresher::new(repo.clone(), repo_config.key_ttl, topics.clone());
    let mut keys_refresher_handle = tokio::spawn(async move { keys_refresher.run().await });

    let (updates_sender, updates_receiver) = tokio::sync::mpsc::unbounded_channel();

    let mut websocket_updates_handler_handle = {
        let clients = clients.clone();
        let topics = topics.clone();
        tokio::task::spawn(websocket::updates_handler(
            updates_receiver,
            clients,
            topics,
            repo.clone(),
        ))
    };

    let redis_conn = redis::Client::open(redis_connection_url)?;
    let conn = redis_conn.clone();

    let mut updater_handle = tokio::task::spawn_blocking(move || {
        let err = updater::run(conn, app_config.updater_timeout, updates_sender);
        log::error!("updater returned an err: {:?}", err);
        err
    });

    let server_options = server::ServerOptions {
        client_ping_interval: Duration::from_secs(server_config.client_ping_interval),
        client_ping_failures_threshold: server_config.client_ping_failures_threshold,
    };
    let (shutdown_signal_tx, mut shutdown_signal_rx) = tokio::sync::mpsc::channel(1);
    let (server_stop_tx, server) = server::start(
        server_config.port,
        repo,
        clients.clone(),
        topics,
        server_options,
        shutdown_signal_tx,
    );
    let server_handle = tokio::spawn(server);

    let (shutdown_start_tx, shutdown_start_rx) = tokio::sync::oneshot::channel();
    let mut shutdown_handle = tokio::spawn(async move {
        if shutdown_start_rx.await.is_ok() {
            log::info!("Graceful shutdown started");
            let mut clients_to_kill = Vec::new();
            for clients_shard in clients.as_ref().into_iter() {
                let clients_shard = clients_shard.read().await;
                for client in clients_shard.values() {
                    clients_to_kill.push(client.clone());
                }
            }
            let client_count = clients_to_kill.len() as u32;
            let sleep_interval = server_config.graceful_shutdown_duration / client_count;
            log::debug!(
                "Client kill interval: {:?} ({} clients)",
                sleep_interval,
                client_count,
            );
            for client in clients_to_kill {
                tokio::time::sleep(sleep_interval).await;
                let mut client = client.lock().await;
                client.graceful_kill();
            }
        }
    });
    let mut shutdown_start_tx = Some(shutdown_start_tx);

    let mut sigterm_stream =
        signal(SignalKind::terminate()).expect("error occurred while creating sigterm stream");

    loop {
        tokio::select! {
            _ = sigterm_stream.recv() => {
                log::debug!("got SIGTERM");
                shutdown_start_tx.take().map(|tx| tx.send(()));
            },
            _ = tokio::signal::ctrl_c() => {
                log::debug!("got SIGINT");
                break;
            },
            _ = &mut shutdown_handle => {
                log::debug!("Graceful shutdown finished");
                break;
            }
            r = &mut keys_refresher_handle => {
                log::error!("keys_refresher finished: {:?}", r);
                break;
            },
            r = &mut updater_handle => {
                log::error!("updater finished: {:?}", r);
                break;
            },
            r = &mut websocket_updates_handler_handle => {
                log::error!("websocket updates handler finished: {:?}", r);
                break;
            }
        }
    }

    // graceful shutdown for websocket connections
    shutdown_signal_rx.close();

    let _ = server_stop_tx.send(());
    server_handle.await?;
    Ok(())
}
