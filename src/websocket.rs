use crate::error::Error;
use crate::messages::{IncomeMessage, OutcomeMessage};
use crate::models::Topic;
use crate::repo::Repo;
use crate::{Client, ClientId, Clients};
use futures::{future::try_join_all, FutureExt, StreamExt};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use warp::ws;
use wavesexchange_log::{debug, error};

const INVALID_MESSAGE_ERROR_CODE: u16 = 1;
const ALREADY_SUBSCRIBED_ERROR_CODE: u16 = 2;

#[derive(Clone, Debug)]
pub struct HandleConnectionOptions {
    pub ping_interval: tokio::time::Duration,
    pub ping_failures_threshold: u16,
}

pub async fn handle_connection<R: Repo + Sync + Send + 'static>(
    socket: ws::WebSocket,
    clients: Clients,
    repo: Arc<R>,
    options: HandleConnectionOptions,
) -> Result<(), Error> {
    let client_id = repo.get_connection_id().await.map_err(|e| Error::from(e))?;

    let (ws_tx, mut ws_rx) = socket.split();
    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();

    let client = Client {
        sender: client_tx.clone(),
        subscriptions: HashSet::new(),
        message_counter: 0,
        pings: vec![],
    };

    clients.write().await.insert(client_id, client);

    let (client_disconnect_signal_sender, _) = tokio::sync::broadcast::channel::<()>(1);
    let mut client_disconnect_signal_receiver = client_disconnect_signal_sender.subscribe();
    // forward messages to ws connection
    tokio::task::spawn(async move {
        tokio::select! {
            _ = client_rx.forward(ws_tx).map(|result| {
                if let Err(err) = result {
                    error!(
                        "error occurred while sending message to ws client: {:?}",
                        err
                    )
                }
            }) => (),
            _ = client_disconnect_signal_receiver.recv() => {
                debug!("got the client disconnect signal: stop the forwarder task");
                ()
            }
        }
    });

    let pinger_failure_signal_sender = client_disconnect_signal_sender.clone();
    let mut pinger_failure_signal_receiver = pinger_failure_signal_sender.subscribe();
    {
        let clients = clients.clone();
        let client_id = client_id.clone();
        let client_tx = client_tx.clone();
        let mut client_disconnect_signal_receiver = client_disconnect_signal_sender.subscribe();
        // pinging
        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(options.ping_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mut clients_lock = clients.write().await;
                        let client = clients_lock
                            .get_mut(&client_id)
                            .expect(&format!("Unknown client_id: {}", client_id));

                        if client.pings.len() >= options.ping_failures_threshold as usize {
                            debug!("client did not answer for {} consequent ping messages", options.ping_failures_threshold);

                            pinger_failure_signal_sender.send(())
                                .expect("error occured while client disconnecting, because of ping failure");
                            break;
                        } else {
                            client.message_counter += 1;
                            client.pings.push(client.message_counter);

                            client_tx
                                .send(Ok(ws::Message::from(OutcomeMessage::Ping { message_number: client.message_counter })))
                                .expect("error occured while sending message to client");
                        }
                    },
                    _ = client_disconnect_signal_receiver.recv() => {
                        debug!("got the client disconnect signal: stop the pinger task");
                        break;
                    }
                }
            }
        });
    }

    // ws connection messages processing
    let mut client_disconnect_signal_receiver = client_disconnect_signal_sender.subscribe();
    loop {
        tokio::select! {
            next_message = ws_rx.next() => {
                if let Some(next_msg_result) = next_message {
                    let msg = match next_msg_result {
                        Ok(msg) => msg,
                        Err(_disconnected) => {
                            debug!("client #{} connection was unexpectedly closed", client_id);
                            break;
                        }
                    };

                    if msg.is_close() {
                        debug!("client #{} connection was closed", client_id);
                        break;
                    }

                    if let Err(err) = on_message(repo.clone(), &clients, &client_id, msg).await {
                        match err {
                            crate::error::Error::UnknownIncomeMessage(error_message) => {
                                let mut clients_lock = clients.write().await;
                                let client = clients_lock
                                    .get_mut(&client_id)
                                    .expect(&format!("Unknown client_id: {}", client_id));

                                client.message_counter += 1;

                                let mut error_details = std::collections::HashMap::new();
                                error_details.insert("reason".to_string(), error_message);
                                client.sender
                                    .send(Ok(ws::Message::from(OutcomeMessage::Error { message_number: client.message_counter, code: INVALID_MESSAGE_ERROR_CODE, message: "Invalid message".to_string(), details: Some(error_details) })))
                                    .expect("error occured while sending message to client");
                            },
                            _ => {
                                error!("error occured while processing message: {:?}", err);
                                break;
                            }
                        }
                    }
                }
            },
            _ = client_disconnect_signal_receiver.recv() => {
                debug!("got the client disconnect signal: stop the messages processing");
                break;
            }
            _ = pinger_failure_signal_receiver.recv() => {
                debug!("got the ping failure signal: stop the messages processing");
                client_disconnect_signal_sender
                    .send(())
                    .expect("error occured while sending ping failure signal");
                break;
            }

        }
    }

    // handle connection close
    on_disconnect(repo, &client_id, clients, client_disconnect_signal_sender).await?;

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
        IncomeMessage::Pong(pong) => {
            if client.pings.contains(&pong.message_number) {
                client.pings.clear();
                Ok(())
            } else {
                // client sent invalid pong message
                Err(Error::InvalidPongMessage)
            }
        }
        IncomeMessage::Subscribe {
            topic: subscription_key,
        } => {
            // just for subscription key validation
            let _topic = Topic::try_from(subscription_key.as_str())?;

            if client.subscriptions.contains(&subscription_key) {
                client.message_counter += 1;

                client
                    .sender
                    .send(Ok(ws::Message::from(OutcomeMessage::Error {
                        code: ALREADY_SUBSCRIBED_ERROR_CODE,
                        message: "You are already subscribed for the specified topic".to_string(),
                        details: None,
                        message_number: client.message_counter,
                    })))?;
            } else {
                repo.subscribe(subscription_key.clone()).await?;

                client.subscriptions.insert(subscription_key.clone());
                client.message_counter += 1;

                client
                    .sender
                    .send(Ok(ws::Message::from(OutcomeMessage::Subscribed {
                        message_number: client.message_counter,
                        topic: subscription_key.clone(),
                    })))?;

                if let Ok(value) = repo.get_by_key(&subscription_key).await {
                    client.message_counter += 1;
                    client
                        .sender
                        .send(Ok(ws::Message::from(OutcomeMessage::Update {
                            message_number: client.message_counter,
                            topic: subscription_key,
                            value,
                        })))?;
                }
            }

            Ok(())
        }
        IncomeMessage::Unsubscribe {
            topic: subscription_key,
        } => {
            // just for subscription key validation
            let _topic = Topic::try_from(subscription_key.as_str())?;

            if client.subscriptions.contains(&subscription_key) {
                repo.unsubscribe(subscription_key.clone()).await?;
                client.subscriptions.remove(&subscription_key);
            }

            client.message_counter += 1;

            client
                .sender
                .send(Ok(ws::Message::from(OutcomeMessage::Unsubscribed {
                    message_number: client.message_counter,
                    topic: subscription_key,
                })))?;

            Ok(())
        }
    }
}

async fn on_disconnect<R: Repo>(
    repo: Arc<R>,
    client_id: &ClientId,
    clients: Clients,
    client_disconnect_signal_sender: Sender<()>,
) -> Result<(), Error> {
    if let Some(client) = clients.read().await.get(client_id) {
        let fs = client
            .subscriptions
            .iter()
            .map(|subscription_key| repo.unsubscribe(subscription_key));

        try_join_all(fs).await?;

        debug!(
            "client #{} disconnected; he got {} messages",
            client_id, client.message_counter
        );
    }

    clients.write().await.remove(client_id);

    client_disconnect_signal_sender
        .send(())
        .expect("error occured while client disconnecting");

    Ok(())
}

pub async fn updates_handler<R: Repo>(
    updates_receiver: tokio::sync::mpsc::UnboundedReceiver<Topic>,
    repo: Arc<R>,
    clients: Clients,
) -> Result<(), Error> {
    let mut updates_receiver = updates_receiver;
    while let Some(topic) = updates_receiver.recv().await {
        let topic_encoded = topic.to_string();

        let value = repo
            .get_by_key(topic_encoded.as_ref())
            .await
            .expect(&format!("Cannot get value by key {}", topic_encoded));

        for (_, client) in clients.write().await.iter_mut() {
            let subscribtion_key = topic_encoded.clone();
            if client.subscriptions.contains(&subscribtion_key) {
                client.message_counter += 1;

                let reply = ws::Message::from(OutcomeMessage::Update {
                    message_number: client.message_counter,
                    topic: subscribtion_key,
                    value: value.clone(),
                });

                if let Err(err) = client.sender.send(Ok(reply)) {
                    debug!("error occured while sending message: {:?}", err)
                }
            }
        }
    }

    Ok(())
}
