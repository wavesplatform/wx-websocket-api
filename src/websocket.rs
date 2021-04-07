use crate::client::{Client, ClientId, Clients};
use crate::error::Error;
use crate::messages::{IncomeMessage, OutcomeMessage};
use crate::models::Topic;
use crate::repo::Repo;
use futures::{stream, SinkExt, StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use warp::ws;
use wavesexchange_log::{error, info};

const INVALID_MESSAGE_ERROR_CODE: u16 = 1;
const ALREADY_SUBSCRIBED_ERROR_CODE: u16 = 2;
const INVALID_TOPIC_ERROR_CODE: u16 = 3;

#[derive(Clone, Debug)]
pub struct HandleConnectionOptions {
    pub ping_interval: tokio::time::Duration,
    pub ping_failures_threshold: u16,
}

pub async fn handle_connection<R: Repo>(
    socket: ws::WebSocket,
    clients: Clients,
    repo: Arc<R>,
    options: HandleConnectionOptions,
    request_id: Option<String>,
) -> Result<(), Error> {
    let client_id = repo.get_connection_id().await.map_err(|e| Error::from(e))?;
    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();

    let client = Client {
        sender: client_tx.clone(),
        subscriptions: HashMap::new(),
        new_subscriptions: HashSet::new(),
        message_counter: 1,
        request_id: request_id.clone(),
        pings: vec![],
    };

    clients.write().await.insert(client_id, client);

    let (client_disconnect_signal_sender, _) = tokio::sync::broadcast::channel::<()>(1);

    let pinger_failure_signal_sender = client_disconnect_signal_sender.clone();
    {
        let clients = clients.clone();
        let client_id = client_id.clone();
        let client_disconnect_signal_receiver = client_disconnect_signal_sender.subscribe();
        // pinging
        tokio::task::spawn(async move {
            pinging(
                clients,
                client_id,
                client_disconnect_signal_receiver,
                pinger_failure_signal_sender,
                options,
            )
            .await
        });
    }

    // ws connection messages processing
    let client_disconnect_signal_receiver = client_disconnect_signal_sender.subscribe();
    messages_processing(
        socket,
        clients.clone(),
        client_id,
        repo.clone(),
        client_disconnect_signal_receiver,
        client_rx,
    )
    .await;

    // handle connection close
    on_disconnect(repo, &client_id, clients, client_disconnect_signal_sender).await?;

    Ok(())
}

async fn pinging(
    clients: Clients,
    client_id: ClientId,
    mut client_disconnect_signal_receiver: Receiver<()>,
    pinger_failure_signal_sender: Sender<()>,
    options: HandleConnectionOptions,
) {
    let mut interval = tokio::time::interval(options.ping_interval);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let mut clients_lock = clients.write().await;
                let client = clients_lock
                    .get_mut(&client_id)
                    .expect(&format!("Unknown client_id: {}", client_id));

                if client.pings.len() >= options.ping_failures_threshold as usize {
                    info!("client did not answer for {} consequent ping messages", options.ping_failures_threshold);

                    pinger_failure_signal_sender.send(())
                        .expect("error occured while client disconnecting, because of ping failure");
                    break;
                } else {
                    client.pings.push(client.message_counter);
                    let message = OutcomeMessage::Ping { message_number: client.message_counter };
                    client.send(message).expect("error occured while sending message to client");
                }
            },
            _ = client_disconnect_signal_receiver.recv() => {
                info!("got the client disconnect signal: stop the pinger task");
                break;
            }
        }
    }
}

async fn messages_processing<R: Repo>(
    mut socket: warp::ws::WebSocket,
    clients: Clients,
    client_id: ClientId,
    repo: Arc<R>,
    mut client_disconnect_signal_receiver: Receiver<()>,
    mut client_rx: tokio::sync::mpsc::UnboundedReceiver<warp::ws::Message>,
) {
    loop {
        tokio::select! {
            next_message = socket.next() => {
                if let Some(next_msg_result) = next_message {
                    let msg = match next_msg_result {
                        Ok(msg) => msg,
                        Err(disconnected) => {
                            let request_id = clients.read().await.get(&client_id).expect(&format!("Unknown client_id: {}", client_id)).request_id.clone();
                            info!("client #{} connection was unexpectedly closed: {}", client_id, disconnected; "req_id" => request_id);
                            break;
                        }
                    };

                    if msg.is_close() {
                        let request_id = clients.read().await.get(&client_id).expect(&format!("Unknown client_id: {}", client_id)).request_id.clone();
                        info!("client #{} connection was closed", client_id; "req_id" => request_id);
                        break;
                    }

                    match on_message(repo.clone(), &clients, &client_id, msg).await {
                        Err(Error::UnknownIncomeMessage(error)) => send_error(error, "Invalid message", INVALID_MESSAGE_ERROR_CODE, &clients, &client_id).await,
                        Err(Error::InvalidTopic(error)) => {
                            let error = format!("Invalid topic: {}", error);
                            send_error(error, "Invalid topic", INVALID_TOPIC_ERROR_CODE, &clients, &client_id).await
                        }
                        Err(Error::UrlParseError(error)) => {
                            let error = format!("Invalid topic format: {:?}", error);
                            send_error(error, "Invalid topic", INVALID_TOPIC_ERROR_CODE, &clients, &client_id).await
                        }
                        Err(Error::InvalidPongMessage) => {
                            // nothing to do
                            // just close the connection
                            break;
                        }
                        Err(err) => {
                            error!("error occured while processing message: {:?}", err);
                            break;
                        }
                        _ => ()
                    }
                }
            },
            msg = client_rx.recv() => {
                match msg {
                    Some(message) => {
                        if let Err(err) = socket.send(message).await {
                            let request_id = clients.read().await.get(&client_id).expect(&format!("Unknown client_id: {}", client_id)).request_id.clone();
                            error!("error occurred while sending message to ws client: {:?}", err; "req_id" => request_id);
                            break;
                        }
                    }
                    None => break
                }
            }
            _ = client_disconnect_signal_receiver.recv() => {
                info!("got the client disconnect signal: stop the messages processing");
                break;
            }

        }
    }
}

async fn on_message<R: Repo>(
    repo: Arc<R>,
    clients: &Clients,
    client_id: &ClientId,
    raw_msg: ws::Message,
) -> Result<(), Error> {
    let msg = IncomeMessage::try_from(raw_msg.clone())?;

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
                info!("got invalid pong message: {:?}", raw_msg);
                Err(Error::InvalidPongMessage)
            }
        }
        IncomeMessage::Subscribe {
            topic: client_subscription_key,
        } => {
            // just for subscription key validation
            let topic = Topic::try_from(&client_subscription_key)?;
            let subscription_key = topic.to_string();

            if client.subscriptions.contains_key(&topic) {
                let message = OutcomeMessage::Error {
                    code: ALREADY_SUBSCRIBED_ERROR_CODE,
                    message: "You are already subscribed for the specified topic".to_string(),
                    details: None,
                    message_number: client.message_counter,
                };
                client.send(message)?;
            } else {
                repo.subscribe(subscription_key.clone()).await?;
                client
                    .subscriptions
                    .insert(topic.clone(), client_subscription_key.clone());
                if let Some(value) = repo.get_by_key(&subscription_key).await? {
                    let message = OutcomeMessage::Subscribed {
                        message_number: client.message_counter,
                        topic: client_subscription_key,
                        value,
                    };
                    client.send(message)?;
                } else {
                    client.new_subscriptions.insert(topic);
                }
            }

            Ok(())
        }
        IncomeMessage::Unsubscribe {
            topic: client_subscription_key,
        } => {
            // just for subscription key validation
            let topic = Topic::try_from(&client_subscription_key)?;
            let subscription_key = topic.to_string();

            if client.subscriptions.contains_key(&topic) {
                repo.unsubscribe(subscription_key.clone()).await?;
                client.subscriptions.remove(&topic);
            }

            let message = OutcomeMessage::Unsubscribed {
                message_number: client.message_counter,
                topic: client_subscription_key,
            };
            client.send(message)?;

            Ok(())
        }
    }
}

async fn send_error(
    error: impl Into<String>,
    message: impl Into<String>,
    code: u16,
    clients: &Clients,
    client_id: &ClientId,
) {
    let mut clients_lock = clients.write().await;
    let client = clients_lock
        .get_mut(client_id)
        .expect(&format!("Unknown client_id: {}", client_id));

    let mut error_details = std::collections::HashMap::new();
    error_details.insert("reason".to_string(), error.into());
    let message = OutcomeMessage::Error {
        message_number: client.message_counter,
        code,
        message: message.into(),
        details: Some(error_details),
    };
    client
        .send(message)
        .expect("error occured while sending message to client");
}

async fn on_disconnect<R: Repo>(
    repo: Arc<R>,
    client_id: &ClientId,
    clients: Clients,
    client_disconnect_signal_sender: Sender<()>,
) -> Result<(), Error> {
    if let Some(client) = clients.read().await.get(client_id) {
        stream::iter(client.subscriptions.iter())
            .map(|(topic, _)| Ok((repo.clone(), topic)))
            .try_for_each_concurrent(10, |(repo, topic)| async move {
                repo.unsubscribe(topic.to_string()).await
            })
            .await?;

        info!(
            "client #{} disconnected; he got {} messages",
            client_id,
            client.message_counter - 1;
            "req_id" => client.request_id.clone()
        );
    }

    clients.write().await.remove(client_id);

    let _ = client_disconnect_signal_sender.send(());

    Ok(())
}

pub async fn updates_handler<R: Repo>(
    mut updates_receiver: tokio::sync::mpsc::UnboundedReceiver<Topic>,
    repo: Arc<R>,
    clients: Clients,
) -> Result<(), Error> {
    while let Some(topic) = updates_receiver.recv().await {
        let subscription_key = topic.to_string();

        if let Some(value) = repo
            .get_by_key(subscription_key.as_ref())
            .await
            .expect(&format!("Cannot get value by key {}", subscription_key))
        {
            for (_, client) in clients.write().await.iter_mut() {
                if let Some(subscription_string) = client.subscriptions.get(&topic) {
                    let message = if client.new_subscriptions.remove(&topic) {
                        OutcomeMessage::Subscribed {
                            message_number: client.message_counter,
                            topic: subscription_string.clone(),
                            value: value.clone(),
                        }
                    } else {
                        OutcomeMessage::Update {
                            message_number: client.message_counter,
                            topic: subscription_string.clone(),
                            value: value.clone(),
                        }
                    };
                    if let Err(err) = client.send(message) {
                        info!("error occured while sending message: {:?}", err)
                    }
                }
            }
        }
    }

    Ok(())
}
