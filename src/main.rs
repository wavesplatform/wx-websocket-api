mod client;
mod config;
mod error;
mod messages;
mod models;
mod repo;
mod server;
mod updater;
mod websocket;

use bb8_redis::{bb8, RedisConnectionManager};
use client::ClientsTrait;
use error::Error;
use models::Topic;
use repo::RepoImpl;
use std::sync::Arc;
use wavesexchange_log::{error, info};

fn main() -> Result<(), Error> {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(tokio_main())?;
    Ok(rt.shutdown_timeout(std::time::Duration::from_millis(1)))
}

async fn tokio_main() -> Result<(), Error> {
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

    let clients = client::Clients::default();

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
    let (tx, server) = server::start(
        server_config.port,
        repo.clone(),
        clients.clone(),
        server_options,
    );
    let server_handler = tokio::spawn(server);

    let updates_future = async {
        if let Err(e) = tokio::try_join!(updates_handler_handle, updates_handle) {
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
    let _ = tx.send(());
    server_handler.await?;
    clients.clean(repo).await;

    Ok(())
}
