use crate::client::{Clients, Topics};
use crate::repo::Repo;
use crate::websocket;
use futures::future::FutureExt;
use std::sync::Arc;
use warp::Filter;
use wavesexchange_log::info;

pub struct ServerConfig {
    pub port: u16,
    pub client_ping_interval: u64,
    pub client_ping_failures_threshold: u16,
}

pub struct ServerOptions {
    pub client_ping_interval: tokio::time::Duration,
    pub client_ping_failures_threshold: u16,
}

pub fn start<R: Repo + 'static>(
    server_port: u16,
    repo: Arc<R>,
    clients: Clients,
    topics: Topics,
    options: ServerOptions,
) -> (
    tokio::sync::oneshot::Sender<()>,
    impl futures::Future<Output = ()>,
) {
    let handle_connection_opts = websocket::HandleConnectionOptions {
        ping_interval: options.client_ping_interval,
        ping_failures_threshold: options.client_ping_failures_threshold,
    };

    let routes = warp::path("ws")
        .and(warp::path::end())
        .and(warp::ws())
        .and(warp::any().map(move || repo.clone()))
        .and(warp::any().map(move || clients.clone()))
        .and(warp::any().map(move || topics.clone()))
        .and(warp::any().map(move || handle_connection_opts.clone()))
        .and(warp::header::optional::<String>("x-request-id"))
        .map(
            |ws: warp::ws::Ws, repo: Arc<R>, clients, topics, opts, req_id| {
                ws.on_upgrade(move |socket| {
                    websocket::handle_connection(socket, clients, topics, repo, opts, req_id)
                        .map(|result| result.expect("Cannot handle ws connection"))
                })
            },
        )
        .with(warp::log::custom(access));

    info!("websocket server listening on :{}", server_port);

    let (tx, rx) = tokio::sync::oneshot::channel();
    let (_addr, server) =
        warp::serve(routes).bind_with_graceful_shutdown(([0, 0, 0, 0], server_port), async {
            let _ = rx.await;
        });
    (tx, server)
}

fn access(info: warp::log::Info) {
    let req_id = info
        .request_headers()
        .get("x-request-id")
        .map(|h| h.to_str().unwrap_or(&""));

    info!(
        "access";
        "path" => info.path(),
        "method" => info.method().to_string(),
        "status" => info.status().as_u16(),
        "ua" => info.user_agent(),
        "latency" => info.elapsed().as_millis(),
        "req_id" => req_id,
        "ip" => info.remote_addr().map(|a| format!("{}", a.ip())),
        "protocol" => format!("{:?}", info.version()),
    );
}
