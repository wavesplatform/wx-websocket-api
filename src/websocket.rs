use crate::client::{Client, ClientId, Clients, Topics};
use crate::error::Error;
use crate::messages::IncomeMessage;
use crate::models::Topic;
use crate::repo::Repo;
use crate::shard::Sharded;
use futures::{stream, SinkExt, StreamExt, TryStreamExt};
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::sync::Mutex;
use warp::ws;
use wavesexchange_log::{error, info};

const INVALID_MESSAGE_ERROR_CODE: u16 = 1;
const ALREADY_SUBSCRIBED_ERROR_CODE: u16 = 2;
const INVALID_TOPIC_ERROR_CODE: u16 = 3;

#[derive(Clone, Debug)]
pub struct HandleConnectionOptions {
    pub ping_interval: tokio::time::Duration,
    pub ping_failures_threshold: usize,
}

pub async fn handle_connection<R: Repo>(
    socket: ws::WebSocket,
    clients: Arc<Sharded<Clients>>,
    topics: Arc<Sharded<Topics>>,
    repo: Arc<R>,
    options: HandleConnectionOptions,
    request_id: Option<String>,
) -> Result<(), Error> {
    let client_id = repo.get_connection_id().await?;
    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();

    let client = Arc::new(Mutex::new(Client::new(
        client_tx.clone(),
        request_id.clone(),
    )));

    clients
        .get(&client_id)
        .write()
        .await
        .insert(client_id, client.clone());

    // ws connection messages processing
    run(
        socket, &client, &client_id, &topics, &repo, options, client_rx,
    )
    .await;

    // handle connection close
    on_disconnect(repo, client, client_id, clients, topics).await?;

    Ok(())
}

async fn run<R: Repo>(
    mut socket: ws::WebSocket,
    client: &Arc<Mutex<Client>>,
    client_id: &ClientId,
    topics: &Arc<Sharded<Topics>>,
    repo: &Arc<R>,
    options: HandleConnectionOptions,
    mut client_rx: tokio::sync::mpsc::UnboundedReceiver<ws::Message>,
) {
    let mut interval = tokio::time::interval(options.ping_interval);
    loop {
        tokio::select! {
            // income message (from ws)
            next_message = socket.next() => {
                if let Some(next_msg_result) = next_message {
                    let msg = match next_msg_result {
                        Ok(msg) => msg,
                        Err(disconnected) => {
                            let request_id = client.lock().await.get_request_id().clone();
                            info!("client #{} connection was unexpectedly closed: {}", client_id, disconnected; "req_id" => request_id);
                            break;
                        }
                    };

                    if msg.is_close() {
                        let request_id = client.lock().await.get_request_id().clone();
                        info!("client #{} connection was closed", client_id; "req_id" => request_id);
                        break;
                    }

                    if match handle_message(repo, client, client_id, topics, &msg).await {
                        Err(Error::UnknownIncomeMessage(error)) => send_error(error, "Invalid message", INVALID_MESSAGE_ERROR_CODE, client).await,
                        Err(Error::InvalidTopic(error)) => {
                            let error = format!("Invalid topic: {:?} – {}", msg, error);
                            send_error(error, "Invalid topic", INVALID_TOPIC_ERROR_CODE, client).await
                        }
                        Err(Error::UrlParseError(error)) => {
                            let error = format!("Invalid topic format: {:?} – {:?}", msg, error);
                            send_error(error, "Invalid topic", INVALID_TOPIC_ERROR_CODE, client).await
                        }
                        Err(Error::InvalidPongMessage) => {
                            // nothing to do
                            // just close the connection
                            break;
                        }
                        Err(err) => {
                            error!("error occured while processing client message: {:?} – {:?}", msg, err);
                            break;
                        }
                        _ => Ok(())
                    }.is_err() {
                        error!("error occured while sending message to client");
                        break;
                    }
                }
            },
            // outcome message (to ws)
            msg = client_rx.recv() => {
                if let Some(message) = msg {
                    if let Err(err) = socket.send(message).await {
                        let request_id = client.lock().await.get_request_id().clone();
                        error!("error occurred while sending message to ws client: {:?}", err; "req_id" => request_id);
                        break;
                    }
                } else {
                    break;
                }
            }
            // ping
            _ = interval.tick() => {
                let mut client_lock = client.lock().await;

                if client_lock.pings_len() >= options.ping_failures_threshold {
                    info!("client did not answer for {} consequent ping messages", options.ping_failures_threshold);
                    break;
                }
                if let Err(error) = client_lock.send_ping() {
                    error!("error occured while sending ping message to client: {:?}", error);
                    break;
                }
            },
        }
    }
}

async fn handle_message<R: Repo>(
    repo: &Arc<R>,
    client: &Arc<Mutex<Client>>,
    client_id: &ClientId,
    topics: &Arc<Sharded<Topics>>,
    raw_msg: &ws::Message,
) -> Result<(), Error> {
    let msg = IncomeMessage::try_from(raw_msg)?;

    match msg {
        IncomeMessage::Pong(pong) => client.lock().await.handle_pong(pong.message_number),
        IncomeMessage::Subscribe {
            topic: client_subscription_key,
        } => {
            let topic = Topic::try_from(&client_subscription_key)?;
            let subscription_key = topic.to_string();
            let mut client_lock = client.lock().await;

            if client_lock.contains_subscription(&topic) {
                let message = "You are already subscribed for the specified topic".to_string();
                client_lock.send_error(ALREADY_SUBSCRIBED_ERROR_CODE, message, None)?;
            } else {
                let mut topics_lock = topics.get(&topic).write().await;
                repo.subscribe(subscription_key.clone()).await?;
                client_lock.add_subscription(topic.clone(), client_subscription_key.clone());
                if let Some(value) = repo.get_by_key(&subscription_key).await? {
                    client_lock.send_subscribed(client_subscription_key, value)?;
                } else {
                    client_lock.add_new_subscription(topic.clone());
                }
                topics_lock.add_subscription(topic, *client_id);
            }

            Ok(())
        }
        IncomeMessage::Unsubscribe {
            topic: client_subscription_key,
        } => {
            let topic = Topic::try_from(&client_subscription_key)?;
            let subscription_key = topic.to_string();
            let mut client_lock = client.lock().await;

            if client_lock.contains_subscription(&topic) {
                repo.unsubscribe(subscription_key.clone()).await?;
                client_lock.remove_subscription(&topic);
                topics
                    .get(&topic)
                    .write()
                    .await
                    .remove_subscription(&topic, client_id);
            }

            client_lock.send_unsubscribed(client_subscription_key)?;

            Ok(())
        }
    }
}

async fn send_error(
    error: impl Into<String>,
    message: impl Into<String>,
    code: u16,
    client: &Arc<Mutex<Client>>,
) -> Result<(), Error> {
    let mut error_details = std::collections::HashMap::new();
    error_details.insert("reason".to_string(), error.into());
    client
        .lock()
        .await
        .send_error(code, message.into(), Some(error_details))
}

async fn on_disconnect<R: Repo>(
    repo: Arc<R>,
    client: Arc<Mutex<Client>>,
    client_id: ClientId,
    clients: Arc<Sharded<Clients>>,
    topics: Arc<Sharded<Topics>>,
) -> Result<(), Error> {
    let client_lock = client.lock().await;
    // remove topics from Topics
    stream::iter(client_lock.subscriptions_iter())
        .map(|(topic, _)| (topic, &topics))
        .for_each_concurrent(20, |(topic, topics)| async move {
            topics
                .get(&topic)
                .write()
                .await
                .remove_subscription(topic, &client_id);
        })
        .await;
    // unsubscribing all topics from repo
    stream::iter(client_lock.subscriptions_iter())
        .map(|(topic, _subscription_key)| Ok::<_, Error>((topic, &repo)))
        .try_for_each_concurrent(10, |(topic, repo)| async move {
            repo.unsubscribe(topic.to_string()).await?;
            Ok(())
        })
        .await?;

    info!(
        "client #{} disconnected; he got {} messages",
        client_id,
        client_lock.messages_count();
        "req_id" => client_lock.get_request_id().clone()
    );

    clients.get(&client_id).write().await.remove(&client_id);

    Ok(())
}

pub async fn updates_handler<R: Repo>(
    mut updates_receiver: tokio::sync::mpsc::UnboundedReceiver<Topic>,
    repo: Arc<R>,
    clients: Arc<Sharded<Clients>>,
    topics: Arc<Sharded<Topics>>,
) -> Result<(), Error> {
    while let Some(topic) = updates_receiver.recv().await {
        let subscription_key = topic.to_string();

        if let Some(value) = repo
            .get_by_key(subscription_key.as_ref())
            .await
            .unwrap_or_else(|_| panic!("Cannot get value by key {}", subscription_key))
        {
            handle_update(topic, value, &clients, &topics).await?
        }
    }

    Ok(())
}

pub async fn transactions_updates_handler(
    mut transaction_updates_receiver: tokio::sync::mpsc::UnboundedReceiver<(Topic, String)>,
    clients: Arc<Sharded<Clients>>,
    topics: Arc<Sharded<Topics>>,
) -> Result<(), Error> {
    while let Some((topic, value)) = transaction_updates_receiver.recv().await {
        handle_update(topic, value, &clients, &topics).await?
    }

    Ok(())
}

async fn handle_update(
    topic: Topic,
    value: String,
    clients: &Arc<Sharded<Clients>>,
    topics: &Arc<Sharded<Topics>>,
) -> Result<(), Error> {
    let maybe_client_ids = topics
        .get(&topic)
        .read()
        .await
        .get_client_ids(&topic)
        .cloned();
    if let Some(client_ids) = maybe_client_ids {
        for client_id in client_ids {
            if let Some(client) = clients.get(&client_id).read().await.get(&client_id) {
                client
                    .lock()
                    .await
                    .send_update(&topic, value.to_owned())
                    .expect("error occured while sending message")
            }
        }
    }
    Ok(())
}
