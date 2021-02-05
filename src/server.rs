use crate::repo::Repo;
use crate::websocket;
use crate::Clients;
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

pub async fn start<R: Repo + Sync + Send + 'static>(
    server_port: u16,
    repo: Arc<R>,
    clients: Clients,
    options: ServerOptions,
) {
    let with_repo = warp::any().map(move || repo.clone());
    let with_clients = warp::any().map(move || clients.clone());

    let handle_connection_opts = websocket::HandleConnectionOptions {
        ping_interval: options.client_ping_interval,
        ping_failures_threshold: options.client_ping_failures_threshold,
    };
    let with_opts = warp::any().map(move || handle_connection_opts.clone());

    let routes = warp::path("ws")
        .and(warp::path::end())
        .and(warp::ws())
        .and(with_repo.clone())
        .and(with_clients.clone())
        .and(with_opts.clone())
        .map(|ws: warp::ws::Ws, repo: Arc<R>, clients, opts| {
            ws.on_upgrade(move |socket| {
                websocket::handle_connection(socket, clients, repo, opts)
                    .map(|result| result.expect("Cannot handle ws connection"))
            })
        });

    info!("websocket server listening on :{}", server_port);

    warp::serve(routes.with(warp::log::custom(access)))
        .run(([0, 0, 0, 0], server_port))
        .await;
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
