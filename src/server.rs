use crate::{error::Error, updater::UpdateResource};
use crate::{
    messages::{IncomeMessage, OutcomeMessage, PreOutcomeMessage, SubscribeMessage},
    ConnectionId,
};
use crate::{repo::Repo, Connection};
use crate::{Connections, Subscribtions};
use futures::{future::try_join_all, FutureExt, SinkExt, StreamExt};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::{ws, Filter};
use wavesexchange_log::{debug, error, info};
use wavesexchange_warp::log;

pub async fn start<R: Repo + Sync + Send + 'static>(
    server_port: u16,
    repo: R,
    connections: Connections,
    subscriptions: Subscribtions,
) {
    let with_connections = warp::any().map(move || connections.clone());
    let with_subscriptions = warp::any().map(move || subscriptions.clone());

    let with_repo = {
        let repo = Arc::new(repo);
        warp::any().map(move || repo.clone())
    };

    let routes = warp::path("ws")
        .and(warp::path::end())
        .and(warp::ws())
        .and(with_repo.clone())
        .and(with_connections.clone())
        .and(with_subscriptions.clone())
        .map(
            |ws: warp::ws::Ws, repo: Arc<R>, connections, subscriptions| {
                info!("new web-socket connection");
                ws.on_upgrade(move |socket| {
                    handle_connection(socket, connections, subscriptions, repo)
                        .map(|result| result.unwrap())
                })
            },
        );

    info!("websocket server listening on :{}", server_port);

    warp::serve(routes.with(warp::log::custom(log::access)))
        .run(([0, 0, 0, 0], server_port))
        .await;
}

async fn handle_connection<R: Repo + Sync + Send + 'static>(
    socket: ws::WebSocket,
    connections: Connections,
    subscriptions: Subscribtions,
    repo: Arc<R>,
) -> Result<(), Error> {
    let connection_id = repo.get_connection_id().await.map_err(|e| Error::from(e))?;

    let (ws_tx, mut ws_rx) = socket.split();
    let (tx, rx) = crossfire::mpsc::unbounded_future::<PreOutcomeMessage>();

    let ws_tx = Arc::new(RwLock::new(ws_tx));
    let tx = Arc::new(tx);

    let repo2 = repo.clone();
    let ws_tx2 = ws_tx.clone();
    // forward messages to ws connection
    let forward_messages_handle = tokio::task::spawn(async move {
        info!("starting new task for messages forwarding");
        while let Ok(msg) = rx.recv().await {
            let msg = on_pre_outcome_message(repo2.clone(), msg).await.unwrap();

            let mut ws_tx_write_guard = ws_tx2.write().await;
            if let Err(err) = ws_tx_write_guard.send(msg).await {
                error!(
                    "error occurred while sending message to ws client: {:?}",
                    err
                );
            }
        }
    });
    info!("add new connection {:?}", connection_id);
    connections.write().await.insert(connection_id, tx.clone());
    info!("new connection saved");
    // ws connection messages processing
    while let Some(next_msg_result) = ws_rx.next().await {
        info!("got next message from ws client");
        let msg = match next_msg_result {
            Ok(msg) => msg,
            Err(_disconnected) => {
                break;
            }
        };

        if msg.is_close() {
            debug!("connection({}) was closed", connection_id);
            break;
        }

        on_message(
            repo.clone(),
            tx.clone(),
            subscriptions.clone(),
            connection_id,
            msg,
        )
        .await?;
    }

    // handle closed connection
    on_disconnect(repo, &connection_id, connections, subscriptions).await?;

    if let Err(err) = tokio::try_join!(forward_messages_handle) {
        error!("forward messages error: {}", err);
    }

    Ok(())
}

async fn on_pre_outcome_message<R: Repo>(
    repo: Arc<R>,
    message: PreOutcomeMessage,
) -> Result<ws::Message, warp::Error> {
    match message {
        PreOutcomeMessage::Pong => Ok(ws::Message::from(OutcomeMessage::Pong)),
        PreOutcomeMessage::Update(update) => {
            let update_key = String::from(&update);

            let value = repo.get_by_key(update_key.as_ref()).await.unwrap();

            let res = ws::Message::from(OutcomeMessage::Update {
                resource: update,
                value: value,
            });

            Ok(res)
        }
    }
}

async fn on_message<R: Repo>(
    repo: Arc<R>,
    tx: Connection,
    subscriptions: Subscribtions,
    connection_id: ConnectionId,
    msg: ws::Message,
) -> Result<(), Error> {
    let msg = IncomeMessage::try_from(msg)?;
    info!("on_message {:?}", msg);
    match msg {
        IncomeMessage::Ping => match tx.send(PreOutcomeMessage::Pong) {
            Ok(()) => (),
            Err(_disconnected) => {}
        },
        IncomeMessage::Subscribe(SubscribeMessage::Config {
            options: config_options,
        }) => {
            let mut subscriptions_write_guard = subscriptions.write().await;
            let connection_subscriptions = subscriptions_write_guard
                .entry(connection_id)
                .or_insert(HashSet::new());

            if let Some(file) = &config_options.file {
                let update_resource = UpdateResource::Config(file.to_owned());
                let subscription_key = String::from(&update_resource);

                if !connection_subscriptions.contains(&subscription_key) {
                    repo.subscribe(subscription_key.clone()).await?;

                    connection_subscriptions.insert(subscription_key);
                }
            } else if let Some(files) = &config_options.files {
                let fs = files
                    .iter()
                    .filter_map(|file| {
                        let update_resource = UpdateResource::Config(file.to_owned());
                        let subscription_key = String::from(&update_resource);

                        if connection_subscriptions.contains(&subscription_key) {
                            None
                        } else {
                            Some(subscription_key)
                        }
                    })
                    .map(|subscription_key| {
                        repo.subscribe(subscription_key.clone())
                            .map(|res| res.map(|_| subscription_key))
                    });

                let subscribe_result = futures::future::join_all(fs)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<String>, Error>>()?;

                subscribe_result.into_iter().for_each(|subscription_key| {
                    connection_subscriptions.insert(subscription_key);
                });
            }
        }
        IncomeMessage::Unsubscribe(crate::messages::UnsubscribeMessage::Config {
            options: config_options,
        }) => {
            let mut subscriptions_write_guard = subscriptions.write().await;
            let connection_subscriptions = subscriptions_write_guard
                .entry(connection_id)
                .or_insert(HashSet::new());

            if let Some(file) = &config_options.file {
                let update_resource = UpdateResource::Config(file.to_owned());
                let subscription_key = String::from(&update_resource);

                if connection_subscriptions.contains(&subscription_key) {
                    repo.unsubscribe(subscription_key.clone()).await?;
                    connection_subscriptions.remove(&subscription_key);
                }
            } else if let Some(files) = &config_options.files {
                let fs = files
                    .iter()
                    .filter_map(|file| {
                        let update_resource = UpdateResource::Config(file.to_owned());
                        let subscription_key = String::from(&update_resource);

                        if connection_subscriptions.contains(&subscription_key) {
                            Some(subscription_key)
                        } else {
                            None
                        }
                    })
                    .map(|subscription_key| {
                        repo.unsubscribe(subscription_key.clone())
                            .map(|res| res.map(|_| subscription_key))
                    });

                let subscribe_result = futures::future::join_all(fs)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<String>, Error>>()?;

                subscribe_result.into_iter().for_each(|subscription_key| {
                    connection_subscriptions.remove(&subscription_key);
                });
            }
        }
    }

    Ok(())
}

async fn on_disconnect<R: Repo>(
    repo: Arc<R>,
    connection_id: &ConnectionId,
    connections: Connections,
    subscriptions: Subscribtions,
) -> Result<(), Error> {
    let connection_subscriptions = subscriptions
        .read()
        .await
        .get(connection_id)
        .unwrap()
        .to_owned();

    let fs = connection_subscriptions
        .iter()
        .map(|subscription_key| repo.unsubscribe(subscription_key));

    try_join_all(fs).await?;

    connections.write().await.remove(connection_id);

    Ok(())
}
