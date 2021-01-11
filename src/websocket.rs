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
    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();
    let (client_disconnect_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    let mut client_disconnect_rx = client_disconnect_tx.subscribe();
    // forward messages to ws connection
    let _forwarder_handle = tokio::task::spawn(async move {
        tokio::select! {
            _ = client_rx.forward(ws_tx).map(|result| {
                if let Err(err) = result {
                    error!(
                        "error occurred while sending message to ws client: {:?}",
                        err
                    )
                }
            }) => (),
            _ = client_disconnect_rx.recv() => {
                debug!("client was disconnected: stop the forwarder task");
                ()
            }
        }
    });

    let client_tx2 = client_tx.clone();
    let mut client_disconnect_rx = client_disconnect_tx.subscribe();
    let _pinger_handle = tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = interval.tick() => {

                    client_tx2
                        .send(Ok(ws::Message::from(OutcomeMessage::Ping)))
                        .expect("error occured while sending message to client");
                },
                _ = client_disconnect_rx.recv() => {
                    debug!("client was disconnected: stop the pinger task");
                    break;
                }
            }
        }
    });

    let client = Client {
        sender: client_tx,
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
            debug!("the client #{} connection was closed", client_id);
            break;
        }

        // todo: do processing invalid messages without closing the connection
        // but sending the error
        if let Err(err) = on_message(repo.clone(), &clients, &client_id, msg).await {
            error!("error occured while processing message: {:?}", err);
            break;
        }
    }

    // handle closed connection
    on_disconnect(repo, &client_id, clients).await?;

    client_disconnect_tx
        .send(())
        .expect("error occured while client disconnecting");

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
        IncomeMessage::Pong => todo!("update client alive status"),
        IncomeMessage::Subscribe(SubscribeMessage::Config {
            parameters: config_parameters,
        }) => {
            let update_resource = UpdateResource::Config(config_parameters);
            let subscription_key = String::from(&update_resource);

            if !client.subscriptions.contains(&subscription_key) {
                repo.subscribe(subscription_key.clone()).await?;

                client.subscriptions.insert(subscription_key);

                client
                    .sender
                    .send(Ok(ws::Message::from(OutcomeMessage::SubscribeSuccess {
                        resource: update_resource,
                    })))?;
            }

            Ok(())
        }
        IncomeMessage::Unsubscribe(crate::messages::UnsubscribeMessage::Config {
            parameters: config_parameters,
        }) => {
            let update_resource = UpdateResource::Config(config_parameters);
            let subscription_key = String::from(&update_resource);

            if client.subscriptions.contains(&subscription_key) {
                repo.unsubscribe(subscription_key.clone()).await?;
                client.subscriptions.remove(&subscription_key);
            }

            client
                .sender
                .send(Ok(ws::Message::from(OutcomeMessage::UnsubscribeSuccess {
                    resource: update_resource,
                })))?;

            Ok(())
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
                    debug!("error occured while sending message: {:?}", err)
                }
            }
        }
    }

    Ok(())
}
