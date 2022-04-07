use futures::future::FutureExt;
use prometheus::{Encoder, TextEncoder};
use std::sync::Arc;
use std::time::Duration;
use warp::{Filter, Rejection, Reply};
use wavesexchange_log::info;
use wavesexchange_warp::log::access;

use crate::client::{Clients, Topics};
use crate::metrics::REGISTRY;
use crate::repo::Repo;
use crate::shard::Sharded;
use crate::websocket;

pub struct ServerConfig {
    pub port: u16,
    pub client_ping_interval: u64,
    pub client_ping_failures_threshold: u16,
    pub graceful_shutdown_duration: Duration,
}

pub struct ServerOptions {
    pub client_ping_interval: tokio::time::Duration,
    pub client_ping_failures_threshold: u16,
}

pub fn start<R: Repo + 'static>(
    server_port: u16,
    repo: Arc<R>,
    clients: Arc<Sharded<Clients>>,
    topics: Arc<Topics>,
    options: ServerOptions,
    shutdown_signal: tokio::sync::mpsc::Sender<()>,
) -> (
    tokio::sync::oneshot::Sender<()>,
    impl futures::Future<Output = ()>,
) {
    let handle_connection_opts = websocket::HandleConnectionOptions {
        ping_interval: options.client_ping_interval,
        ping_failures_threshold: options.client_ping_failures_threshold as usize,
    };

    let ws = warp::path("ws")
        .and(warp::path::end())
        .and(warp::ws())
        .and(warp::any().map(move || repo.clone()))
        .and(warp::any().map(move || clients.clone()))
        .and(warp::any().map(move || topics.clone()))
        .and(warp::any().map(move || handle_connection_opts.clone()))
        .and(warp::header::optional::<String>("x-request-id"))
        .and(warp::any().map(move || shutdown_signal.clone()))
        .map(
            |ws: warp::ws::Ws, repo: Arc<R>, clients, topics, opts, req_id, shutdown_signal| {
                ws.on_upgrade(move |socket| {
                    websocket::handle_connection(
                        socket,
                        clients,
                        topics,
                        repo,
                        opts,
                        req_id,
                        shutdown_signal,
                    )
                    .map(|result| result.expect("Cannot handle ws connection"))
                })
            },
        )
        .with(warp::log::custom(access));

    let metrics = warp::path!("metrics").and_then(metrics_handler);

    info!("websocket server listening on 0.0.0.0:{}", server_port);

    let (tx, rx) = tokio::sync::oneshot::channel();
    let (_addr, server) = warp::serve(ws.or(metrics)).bind_with_graceful_shutdown(
        ([0, 0, 0, 0], server_port),
        async {
            let _ = rx.await;
        },
    );
    (tx, server)
}

async fn metrics_handler() -> Result<impl Reply, Rejection> {
    let encoder = TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
        eprintln!("could not encode custom metrics: {}", e);
    };
    let mut res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("custom metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        eprintln!("could not encode prometheus metrics: {}", e);
    };
    let res_custom = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("prometheus metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    res.push_str(&res_custom);
    Ok(res)
}
