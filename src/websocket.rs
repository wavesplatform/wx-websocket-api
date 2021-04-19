use crate::client::{Client, ClientId, Clients, Topics};
use crate::error::Error;
use crate::messages::{IncomeMessage, OutcomeMessage};
use crate::models::Topic;
use crate::repo::Repo;
use futures::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::Mutex;
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
    topics: Topics,
    repo: Arc<R>,
    options: HandleConnectionOptions,
    request_id: Option<String>,
) -> Result<(), Error> {
    let client_id = repo.get_connection_id().await.map_err(|e| Error::from(e))?;
    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();

    let client = Arc::new(Mutex::new(Client {
        sender: client_tx.clone(),
        subscriptions: HashMap::new(),
        new_subscriptions: HashSet::new(),
        message_counter: 1,
        request_id: request_id.clone(),
        pings: vec![],
    }));

    clients.write().await.insert(client_id, client.clone());

    let (client_disconnect_signal_sender, _) = tokio::sync::broadcast::channel::<()>(1);

    let pinger_failure_signal_sender = client_disconnect_signal_sender.clone();
    {
        let client = client.clone();
        let client_disconnect_signal_receiver = client_disconnect_signal_sender.subscribe();
        // pinging
        tokio::task::spawn(async move {
            pinging(
                client,
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
        client,
        &client_id,
        &topics,
        &repo,
        client_disconnect_signal_receiver,
        client_rx,
    )
    .await;

    // handle connection close
    on_disconnect(
        repo,
        &client_id,
        clients,
        topics,
        client_disconnect_signal_sender,
    )
    .await?;

    Ok(())
}

async fn pinging(
    client: Arc<Mutex<Client>>,
    mut client_disconnect_signal_receiver: Receiver<()>,
    pinger_failure_signal_sender: Sender<()>,
    options: HandleConnectionOptions,
) {
    let mut interval = tokio::time::interval(options.ping_interval);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let mut client_lock = client.lock().await;

                if client_lock.pings.len() >= options.ping_failures_threshold as usize {
                    info!("client did not answer for {} consequent ping messages", options.ping_failures_threshold);

                    pinger_failure_signal_sender.send(())
                        .expect("error occured while client disconnecting, because of ping failure");
                    break;
                } else {
                    let message_counter = client_lock.message_counter;
                    client_lock.pings.push(message_counter);
                    let message = OutcomeMessage::Ping { message_number: client_lock.message_counter };
                    client_lock.send(message).expect("error occured while sending message to client");
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
    client: Arc<Mutex<Client>>,
    client_id: &ClientId,
    topics: &Topics,
    repo: &Arc<R>,
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
                            let request_id = client.lock().await.request_id.clone();
                            info!("client #{} connection was unexpectedly closed: {}", client_id, disconnected; "req_id" => request_id);
                            break;
                        }
                    };

                    if msg.is_close() {
                        let request_id = client.lock().await.request_id.clone();
                        info!("client #{} connection was closed", client_id; "req_id" => request_id);
                        break;
                    }

                    match on_message(&repo, &client, &client_id, &topics, msg).await {
                        Err(Error::UnknownIncomeMessage(error)) => send_error(error, "Invalid message", INVALID_MESSAGE_ERROR_CODE, &client).await,
                        Err(Error::InvalidTopic(error)) => {
                            let error = format!("Invalid topic: {}", error);
                            send_error(error, "Invalid topic", INVALID_TOPIC_ERROR_CODE, &client).await
                        }
                        Err(Error::UrlParseError(error)) => {
                            let error = format!("Invalid topic format: {:?}", error);
                            send_error(error, "Invalid topic", INVALID_TOPIC_ERROR_CODE, &client).await
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
                            let request_id = client.lock().await.request_id.clone();
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
    repo: &Arc<R>,
    client: &Arc<Mutex<Client>>,
    client_id: &ClientId,
    topics: &Topics,
    raw_msg: ws::Message,
) -> Result<(), Error> {
    let msg = IncomeMessage::try_from(raw_msg.clone())?;
    let mut client_lock = client.lock().await;

    match msg {
        IncomeMessage::Pong(pong) => {
            if client_lock.pings.contains(&pong.message_number) {
                client_lock.pings.clear();
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

            if client_lock.subscriptions.contains_key(&topic) {
                let message = OutcomeMessage::Error {
                    code: ALREADY_SUBSCRIBED_ERROR_CODE,
                    message: "You are already subscribed for the specified topic".to_string(),
                    details: None,
                    message_number: client_lock.message_counter,
                };
                client_lock.send(message)?;
            } else {
                repo.subscribe(subscription_key.clone()).await?;
                client_lock
                    .subscriptions
                    .insert(topic.clone(), client_subscription_key.clone());
                if let Some(value) = repo.get_by_key(&subscription_key).await? {
                    let message = OutcomeMessage::Subscribed {
                        message_number: client_lock.message_counter,
                        topic: client_subscription_key,
                        value,
                    };
                    client_lock.send(message)?;
                } else {
                    client_lock.new_subscriptions.insert(topic.clone());
                }
                let mut topic_lock = topics.write().await;
                if let Some(clients) = topic_lock.get_mut(&topic) {
                    clients.insert(*client_id);
                } else {
                    let mut v = HashSet::new();
                    v.insert(*client_id);
                    topic_lock.insert(topic, v);
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

            if client_lock.subscriptions.contains_key(&topic) {
                repo.unsubscribe(subscription_key.clone()).await?;
                client_lock.subscriptions.remove(&topic);
            }

            let message = OutcomeMessage::Unsubscribed {
                message_number: client_lock.message_counter,
                topic: client_subscription_key,
            };
            client_lock.send(message)?;

            Ok(())
        }
    }
}

async fn send_error(
    error: impl Into<String>,
    message: impl Into<String>,
    code: u16,
    client: &Arc<Mutex<Client>>,
) {
    let mut client_lock = client.lock().await;

    let mut error_details = std::collections::HashMap::new();
    error_details.insert("reason".to_string(), error.into());
    let message = OutcomeMessage::Error {
        message_number: client_lock.message_counter,
        code,
        message: message.into(),
        details: Some(error_details),
    };
    client_lock
        .send(message)
        .expect("error occured while sending message to client");
}

async fn on_disconnect<R: Repo>(
    repo: Arc<R>,
    client_id: &ClientId,
    clients: Clients,
    topics: Topics,
    client_disconnect_signal_sender: Sender<()>,
) -> Result<(), Error> {
    if let Some(client) = clients.read().await.get(client_id) {
        let client_lock = client.lock().await;
        let mut topics_lock = topics.write().await;
        for (topic, _subscription_key) in client_lock.subscriptions.iter() {
            repo.unsubscribe(topic.to_string()).await?;
            if let Some(client_ids) = topics_lock.get_mut(&topic) {
                client_ids.remove(client_id);
                if client_ids.len() == 0 {
                    topics_lock.remove(&topic);
                }
            };
        };

        info!(
            "client #{} disconnected; he got {} messages",
            client_id,
            client_lock.message_counter - 1;
            "req_id" => client_lock.request_id.clone()
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
    topics: Topics,
) -> Result<(), Error> {
    while let Some(topic) = updates_receiver.recv().await {
        let subscription_key = topic.to_string();

        if let Some(value) = repo
            .get_by_key(subscription_key.as_ref())
            .await
            .expect(&format!("Cannot get value by key {}", subscription_key))
        {
            handle_update(topic, value, &clients, &topics).await?
        }
    }

    Ok(())
}

pub async fn transactions_updates_handler(
    mut transaction_updates_receiver: tokio::sync::mpsc::UnboundedReceiver<(Topic, String)>,
    clients: Clients,
    topics: Topics,
) -> Result<(), Error> {
    while let Some((topic, value)) = transaction_updates_receiver.recv().await {
        handle_update(topic, value, &clients, &topics).await?
    }

    Ok(())
}

async fn handle_update(
    topic: Topic,
    value: String,
    clients: &Clients,
    topics: &Topics,
) -> Result<(), Error> {
    if let Some(client_ids) = topics.read().await.get(&topic) {
        for client_id in client_ids {
            if let Some(client) = clients.read().await.get(client_id) {
                let mut client_lock = client.lock().await;
                if let Some(subscription_string) = client_lock.subscriptions.get(&topic).cloned() {
                    let message = if client_lock.new_subscriptions.remove(&topic) {
                        OutcomeMessage::Subscribed {
                            message_number: client_lock.message_counter,
                            topic: subscription_string.clone(),
                            value: value.clone(),
                        }
                    } else {
                        OutcomeMessage::Update {
                            message_number: client_lock.message_counter,
                            topic: subscription_string.clone(),
                            value: value.clone(),
                        }
                    };
                    if let Err(err) = client_lock.send(message) {
                        info!("error occured while sending message: {:?}", err)
                    }
                }
            }
        }
    }
    Ok(())
}
