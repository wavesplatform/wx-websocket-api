use crate::Connections;
use crate::{error::Error, updater::UpdateResource};
use crate::{
    messages::{IncomeMessage, OutcomeMessage, SubscribeMessage},
    ConnectionId,
};
use crate::{repo::Repo, Connection};
use futures::{future::try_join_all, FutureExt, StreamExt};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;
use warp::ws;
use wavesexchange_log::{debug, error};

pub async fn handle_connection<R: Repo + Sync + Send + 'static>(
    socket: ws::WebSocket,
    connections: Connections,
    repo: Arc<R>,
) -> Result<(), Error> {
    let connection_id = repo.get_connection_id().await.map_err(|e| Error::from(e))?;

    let (ws_tx, mut ws_rx) = socket.split();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let tx = Arc::new(tx);

    let mut connection = Connection {
        sender: tx,
        subscriptions: HashSet::new(),
    };

    // forward messages to ws connection
    tokio::task::spawn(rx.forward(ws_tx).map(|result| {
        if let Err(err) = result {
            error!(
                "error occurred while sending message to ws client: {:?}",
                err
            )
        }
    }));

    connections
        .write()
        .await
        .insert(connection_id, connection.clone());

    // ws connection messages processing
    while let Some(next_msg_result) = ws_rx.next().await {
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

        if let Err(_) = on_message(repo.clone(), &mut connection, msg).await {
            debug!("error occured while processing message");
            break;
        }
    }

    // handle closed connection
    on_disconnect(repo, &connection_id, connections).await?;

    Ok(())
}

async fn on_message<R: Repo>(
    repo: Arc<R>,
    connection: &mut Connection,
    msg: ws::Message,
) -> Result<(), Error> {
    let msg = IncomeMessage::try_from(msg)?;

    match msg {
        IncomeMessage::Ping => connection
            .sender
            .send(Ok(ws::Message::from(OutcomeMessage::Pong)))
            .map_err(|err| Error::from(err)),
        IncomeMessage::Subscribe(SubscribeMessage::Config {
            options: config_options,
        }) => {
            if let Some(file) = &config_options.file {
                let update_resource = UpdateResource::Config(file.to_owned());
                let subscription_key = String::from(&update_resource);

                if !connection.subscriptions.contains(&subscription_key) {
                    repo.subscribe(subscription_key.clone()).await?;

                    connection.subscriptions.insert(subscription_key);

                    connection.sender.send(Ok(ws::Message::from(
                        OutcomeMessage::SubscribeSuccess {
                            resources: vec![update_resource],
                        },
                    )))?;
                }

                Ok(())
            } else if let Some(files) = &config_options.files {
                let update_resources: Vec<UpdateResource> = files
                    .iter()
                    .filter_map(|file| {
                        let update_resource = UpdateResource::Config(file.to_owned());

                        let subscription_key = String::from(&update_resource);

                        if connection.subscriptions.contains(&subscription_key) {
                            None
                        } else {
                            Some(update_resource)
                        }
                    })
                    .collect();

                let fs = update_resources.clone().into_iter().map(|update_resource| {
                    let subscription_key = update_resource.to_string();
                    repo.subscribe(subscription_key.clone())
                        .map(|res| res.map(|_| subscription_key))
                });

                let subscribe_result = futures::future::join_all(fs)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<String>, Error>>()?;

                subscribe_result.into_iter().for_each(|subscription_key| {
                    connection.subscriptions.insert(subscription_key);
                });

                connection.sender.send(Ok(ws::Message::from(
                    OutcomeMessage::SubscribeSuccess {
                        resources: update_resources,
                    },
                )))?;

                Ok(())
            } else {
                Err(Error::InvalidSubscribeMessage)
            }
        }
        IncomeMessage::Unsubscribe(crate::messages::UnsubscribeMessage::Config {
            options: config_options,
        }) => {
            if let Some(file) = &config_options.file {
                let update_resource = UpdateResource::Config(file.to_owned());
                let subscription_key = String::from(&update_resource);

                if connection.subscriptions.contains(&subscription_key) {
                    repo.unsubscribe(subscription_key.clone()).await?;
                    connection.subscriptions.remove(&subscription_key);
                }

                connection.sender.send(Ok(ws::Message::from(
                    OutcomeMessage::UnsubscribeSuccess {
                        resources: vec![update_resource],
                    },
                )))?;

                Ok(())
            } else if let Some(files) = &config_options.files {
                let update_resources: Vec<UpdateResource> = files
                    .iter()
                    .filter_map(|file| {
                        let update_resource = UpdateResource::Config(file.to_owned());

                        let subscription_key = String::from(&update_resource);

                        if connection.subscriptions.contains(&subscription_key) {
                            Some(update_resource)
                        } else {
                            None
                        }
                    })
                    .collect();

                let fs = update_resources.clone().into_iter().map(|update_resource| {
                    let subscription_key = update_resource.to_string();
                    repo.unsubscribe(subscription_key.clone())
                        .map(|res| res.map(|_| subscription_key))
                });

                let subscribe_result = futures::future::join_all(fs)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<String>, Error>>()?;

                subscribe_result.into_iter().for_each(|subscription_key| {
                    connection.subscriptions.remove(&subscription_key);
                });

                connection.sender.send(Ok(ws::Message::from(
                    OutcomeMessage::UnsubscribeSuccess {
                        resources: update_resources,
                    },
                )))?;

                Ok(())
            } else {
                Err(Error::InvalidUnsubscribeMessage)
            }
        }
    }
}

async fn on_disconnect<R: Repo>(
    repo: Arc<R>,
    connection_id: &ConnectionId,
    connections: Connections,
) -> Result<(), Error> {
    if let Some(connection) = connections.read().await.get(connection_id) {
        let fs = connection
            .subscriptions
            .iter()
            .map(|subscription_key| repo.unsubscribe(subscription_key));

        try_join_all(fs).await?;
    }

    connections.write().await.remove(connection_id);

    Ok(())
}

pub async fn updates_handler<R: Repo>(
    updates_receiver: tokio::sync::mpsc::UnboundedReceiver<UpdateResource>,
    repo: Arc<R>,
    connections: Connections,
) -> Result<(), Error> {
    let mut updates_receiver = updates_receiver;
    while let Some(update) = updates_receiver.next().await {
        let subscription_key = update.to_string();

        let value = repo
            .get_by_key(subscription_key.as_ref())
            .await
            .expect(&format!("Cannot get value by key {}", subscription_key));

        let reply = ws::Message::from(OutcomeMessage::Update {
            resource: update,
            value: value,
        });

        for (_, connection) in connections.read().await.iter() {
            if connection.subscriptions.contains(&subscription_key) {
                if let Err(_disconnected) = connection.sender.send(Ok(reply.clone())) {}
            }
        }
    }

    Ok(())
}
