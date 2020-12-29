use crate::messages::{IncomeMessage, OutcomeMessage, SubscribeMessage};
use crate::repo::Repo;
use crate::{error::Error, updater::UpdateResource};
use crate::{Client, ClientId, Clients};
use futures::{future::try_join_all, FutureExt, StreamExt};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;
use warp::ws;
use wavesexchange_log::{debug, error};

pub async fn handle_connection<R: Repo + Sync + Send + 'static>(
    socket: ws::WebSocket,
    clients: Clients,
    repo: Arc<R>,
) -> Result<(), Error> {
    let client_id = repo.get_connection_id().await.map_err(|e| Error::from(e))?;

    let (ws_tx, mut ws_rx) = socket.split();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    // forward messages to ws connection
    tokio::task::spawn(rx.forward(ws_tx).map(|result| {
        if let Err(err) = result {
            error!(
                "error occurred while sending message to ws client: {:?}",
                err
            )
        }
    }));

    let tx = Arc::new(tx);

    let client = Client {
        sender: tx,
        subscriptions: HashSet::new(),
    };

    clients.write().await.insert(client_id, client);

    // ws connection messages processing
    while let Some(next_msg_result) = ws_rx.next().await {
        let msg = match next_msg_result {
            Ok(msg) => msg,
            Err(_disconnected) => {
                break;
            }
        };

        if msg.is_close() {
            debug!("connection({}) was closed", client_id);
            break;
        }

        if let Err(err) = on_message(repo.clone(), &clients, &client_id, msg).await {
            error!("error occured while processing message: {:?}", err);
            break;
        }
    }

    // handle closed connection
    on_disconnect(repo, &client_id, clients).await?;

    Ok(())
}

async fn on_message<R: Repo>(
    repo: Arc<R>,
    clients: &Clients,
    client_id: &ClientId,
    msg: ws::Message,
) -> Result<(), Error> {
    let msg = IncomeMessage::try_from(msg)?;

    let mut clients_lock = clients.write().await;
    let client = clients_lock
        .get_mut(client_id)
        .expect(&format!("Unknown client_id: {}", client_id));

    match msg {
        IncomeMessage::Ping => client
            .sender
            .send(Ok(ws::Message::from(OutcomeMessage::Pong)))
            .map_err(|err| Error::from(err)),
        IncomeMessage::Subscribe(SubscribeMessage::Config {
            options: config_options,
        }) => {
            if let Some(file) = &config_options.file {
                let update_resource = UpdateResource::Config(file.to_owned());
                let subscription_key = String::from(&update_resource);

                if !client.subscriptions.contains(&subscription_key) {
                    repo.subscribe(subscription_key.clone()).await?;

                    client.subscriptions.insert(subscription_key);

                    client.sender.send(Ok(ws::Message::from(
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

                        if client.subscriptions.contains(&subscription_key) {
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
                    client.subscriptions.insert(subscription_key);
                });

                client
                    .sender
                    .send(Ok(ws::Message::from(OutcomeMessage::SubscribeSuccess {
                        resources: update_resources,
                    })))?;

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

                if client.subscriptions.contains(&subscription_key) {
                    repo.unsubscribe(subscription_key.clone()).await?;
                    client.subscriptions.remove(&subscription_key);
                }

                client
                    .sender
                    .send(Ok(ws::Message::from(OutcomeMessage::UnsubscribeSuccess {
                        resources: vec![update_resource],
                    })))?;

                Ok(())
            } else if let Some(files) = &config_options.files {
                let update_resources: Vec<UpdateResource> = files
                    .iter()
                    .filter_map(|file| {
                        let update_resource = UpdateResource::Config(file.to_owned());

                        let subscription_key = String::from(&update_resource);

                        if client.subscriptions.contains(&subscription_key) {
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
                    client.subscriptions.remove(&subscription_key);
                });

                client
                    .sender
                    .send(Ok(ws::Message::from(OutcomeMessage::UnsubscribeSuccess {
                        resources: update_resources,
                    })))?;

                Ok(())
            } else {
                Err(Error::InvalidUnsubscribeMessage)
            }
        }
    }
}

async fn on_disconnect<R: Repo>(
    repo: Arc<R>,
    client_id: &ClientId,
    clients: Clients,
) -> Result<(), Error> {
    if let Some(client) = clients.read().await.get(client_id) {
        let fs = client
            .subscriptions
            .iter()
            .map(|subscription_key| repo.unsubscribe(subscription_key));

        try_join_all(fs).await?;
    }

    clients.write().await.remove(client_id);

    Ok(())
}

pub async fn updates_handler<R: Repo>(
    updates_receiver: tokio::sync::mpsc::UnboundedReceiver<UpdateResource>,
    repo: Arc<R>,
    clients: Clients,
) -> Result<(), Error> {
    let mut updates_receiver = updates_receiver;
    while let Some(update) = updates_receiver.recv().await {
        let subscription_key = update.to_string();

        let value = repo
            .get_by_key(subscription_key.as_ref())
            .await
            .expect(&format!("Cannot get value by key {}", subscription_key));

        let reply = ws::Message::from(OutcomeMessage::Update {
            resource: update,
            value: value,
        });

        for (_, client) in clients.read().await.iter() {
            if client.subscriptions.contains(&subscription_key) {
                if let Err(err) = client.sender.send(Ok(reply.clone())) {
                    println!("error occured while sending message: {:?}", err)
                }
            }
        }
    }

    Ok(())
}
