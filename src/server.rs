use futures::future::FutureExt;
use std::sync::Arc;
use std::time::Duration;
use warp::Filter;
use wavesexchange_warp::log::access;
use wavesexchange_warp::MetricsWarpBuilder;

use crate::client::{Clients, Topics};
use crate::metrics::*;
use crate::repo::Repo;
use crate::shard::Sharded;
use crate::websocket;

pub struct ServerConfig {
    pub port: u16,
    pub metrics_port: u16,
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
    metrics_port: u16,
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

    log::info!("websocket server listening on 0.0.0.0:{}", server_port);

    let (tx, rx) = tokio::sync::oneshot::channel();

    let servers = MetricsWarpBuilder::new()
        .with_main_routes(ws)
        .with_main_routes_port(server_port)
        .with_metrics_port(metrics_port)
        .with_metric(&*CLIENTS)
        .with_metric(&*CLIENT_CONNECT)
        .with_metric(&*CLIENT_DISCONNECT)
        .with_metric(&*TOPICS)
        .with_metric(&*TOPIC_SUBSCRIBED)
        .with_metric(&*TOPIC_UNSUBSCRIBED)
        .with_metric(&*MESSAGES)
        .with_metric(&*SUBSCRIBED_MESSAGE_LATENCIES)
        .with_metric(&*REDIS_INPUT_QUEUE_SIZE)
        .with_metric(&*TOPICS_HASHMAP_SIZE)
        .with_metric(&*TOPICS_HASHMAP_CAPACITY)
        .with_graceful_shutdown(async {
            let _ = rx.await;
        })
        .run_async();

    (tx, servers)
}
