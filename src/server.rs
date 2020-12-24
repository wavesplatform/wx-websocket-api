use crate::repo::Repo;
use crate::websocket;
use crate::Connections;
use futures::future::FutureExt;
use std::sync::Arc;
use warp::Filter;
use wavesexchange_log::info;

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
        "connection" => format!("{:?}", info.request_headers().get("connection").map(|h| h.to_str().unwrap_or(&"")))
    );
}

pub async fn start<R: Repo + Sync + Send + 'static>(
    server_port: u16,
    repo: Arc<R>,
    connections: Connections,
) {
    let with_repo = warp::any().map(move || repo.clone());
    let with_connections = warp::any().map(move || connections.clone());

    let routes = warp::path("ws")
        .and(warp::path::end())
        .and(warp::ws())
        .and(with_repo.clone())
        .and(with_connections.clone())
        .map(|ws: warp::ws::Ws, repo: Arc<R>, connections| {
            ws.on_upgrade(move |socket| {
                websocket::handle_connection(socket, connections, repo)
                    .map(|result| result.expect("Cannot handle ws connection"))
            })
        });

    info!("websocket server listening on :{}", server_port);

    warp::serve(routes.with(warp::log::custom(access)))
        .run(([0, 0, 0, 0], server_port))
        .await;
}
