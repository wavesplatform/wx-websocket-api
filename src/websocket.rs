use futures::{stream, SinkExt, StreamExt};
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use warp::ws;
use wavesexchange_log::{debug, error, info};
use wavesexchange_topic::Topic;

use crate::client::{Client, ClientId, Clients, Topics};
use crate::error::Error;
use crate::messages::IncomeMessage;
use crate::metrics::CLIENTS;
use crate::repo::Repo;
use crate::shard::Sharded;

const INVALID_MESSAGE_ERROR_CODE: u16 = 1;
const ALREADY_SUBSCRIBED_ERROR_CODE: u16 = 2;
const INVALID_TOPIC_ERROR_CODE: u16 = 3;

// different duration to avoid starvation
const LOCKED_CLIENT_ON_PING_SENDING_SLEEP_DURATION_IN_MS: u64 = 10;
const LOCKED_CLIENT_ON_ERROR_SENDING_SLEEP_DURATION_IN_MS: u64 = 50;
const LOCKED_CLIENT_ON_INCOME_MESSAGE_HANDLING_SLEEP_DURATION_IN_MS: u64 = 100;
const LOCKED_CLIENT_ON_UPDATE_SLEEP_DURATION_IN_MS: u64 = 200;
const LOCKED_CLIENT_ON_DISCONNECTING_SLEEP_DURATION_IN_MS: u64 = 300;

#[derive(Clone, Debug)]
pub struct HandleConnectionOptions {
    pub ping_interval: tokio::time::Duration,
    pub ping_failures_threshold: usize,
}

pub async fn handle_connection<R: Repo>(
    mut socket: ws::WebSocket,
    clients: Arc<Sharded<Clients>>,
    topics: Arc<Sharded<Topics>>,
    repo: Arc<R>,
    options: HandleConnectionOptions,
    request_id: Option<String>,
    shutdown_signal: tokio::sync::mpsc::Sender<()>,
) -> Result<(), Error> {
    let client_id = repo.get_connection_id().await?;
    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();

    let client = Arc::new(Mutex::new(Client::new(
        client_tx.clone(),
        request_id.clone(),
    )));

    CLIENTS.inc();

    clients
        .get(&client_id)
        .write()
        .await
        .insert(client_id, client.clone());

    // ws connection messages processing
    let run_handler = run(
        &mut socket,
        &client,
        &client_id,
        request_id,
        &topics,
        repo,
        options,
        client_rx,
    );

    tokio::select! {
        _ = run_handler => {},
        _ = shutdown_signal.closed() => {
            debug!("shutdown signal handled");
        }
    }

    // handle connection close
    on_disconnect(socket, client, client_id, clients, topics).await;

    CLIENTS.dec();

    Ok(())
}

async fn run<R: Repo>(
    socket: &mut ws::WebSocket,
    client: &Arc<Mutex<Client>>,
    client_id: &ClientId,
    request_id: Option<String>,
    topics: &Arc<Sharded<Topics>>,
    repo: Arc<R>,
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
                            debug!("client #{} connection was unexpectedly closed: {}", client_id, disconnected; "req_id" => request_id);
                            break;
                        }
                    };

                    if msg.is_close() {
                        debug!("client #{} connection was closed", client_id; "req_id" => request_id);
                        break;
                    }

                    if msg.is_ping() || msg.is_pong() {
                        continue
                    }

                    if match handle_income_message(&repo, client, client_id, topics, &msg).await {
                        Err(Error::UnknownIncomeMessage(error)) => send_error(error, "Invalid message", INVALID_MESSAGE_ERROR_CODE, client, client_id).await,
                        Err(Error::InvalidTopic(error)) => {
                            let error = format!("Invalid topic: {:?} – {}", msg, error);
                            send_error(error, "Invalid topic", INVALID_TOPIC_ERROR_CODE, client, client_id).await
                        }
                        Err(Error::UrlParseError(error)) => {
                            let error = format!("Invalid topic format: {:?} – {:?}", msg, error);
                            send_error(error, "Invalid topic", INVALID_TOPIC_ERROR_CODE, client, client_id).await
                        }
                        Err(Error::InvalidPongMessage) => {
                            // nothing to do
                            // just close the connection
                            break;
                        }
                        Err(err) => {
                            error!("error occured while processing client #{} message: {:?} – {:?}", client_id, msg, err);
                            break;
                        }
                        _ => Ok(())
                    }.is_err() {
                        error!("error occured while sending message to client #{}", client_id);
                        break;
                    }
                }
            },
            // outcome message (to ws)
            msg = client_rx.recv() => {
                if let Some(message) = msg {
                    debug!("send message to the client#{:?}", client_id);
                    if let Err(err) = socket.send(message).await {
                        error!("error occurred while sending message to ws client: {:?}", err; "req_id" => request_id);
                        break;
                    }
                } else {
                    break;
                }
            }
            // ping
            _ = interval.tick() => {
                loop {
                    match client.try_lock() {
                        Ok(mut client_lock) => {
                            if client_lock.pings_len() >= options.ping_failures_threshold {
                                debug!("client #{} did not answer for {} consequent ping messages", client_id, options.ping_failures_threshold);
                                break;
                            }
                            if let Err(error) = client_lock.send_ping() {
                                error!("error occured while sending ping message to client #{}: {:?}", client_id, error);
                                break;
                            }
                            break;
                        }
                        Err(_) => {
                            debug!(
                                "client #{} is locked while ping sending, try to wait",
                                client_id
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(
                                LOCKED_CLIENT_ON_PING_SENDING_SLEEP_DURATION_IN_MS,
                            ))
                            .await;
                        }
                    }
                }
            },
        }
    }
}

async fn handle_income_message<R: Repo>(
    repo: &Arc<R>,
    client: &Arc<Mutex<Client>>,
    client_id: &ClientId,
    topics: &Arc<Sharded<Topics>>,
    raw_msg: &ws::Message,
) -> Result<(), Error> {
    let msg = IncomeMessage::try_from(raw_msg)?;

    loop {
        match client.try_lock() {
            Ok(mut client_lock) => {
                match msg {
                    IncomeMessage::Pong(pong) => {
                        client_lock.handle_pong(pong.message_number)?;
                    }
                    IncomeMessage::Subscribe {
                        topic: client_subscription_key,
                    } => {
                        let topic = Topic::try_from(&client_subscription_key)?;
                        let subscription_key = String::from(topic.clone());

                        if client_lock.contains_subscription(&topic) {
                            let message =
                                "You are already subscribed for the specified topic".to_string();
                            client_lock.send_error(ALREADY_SUBSCRIBED_ERROR_CODE, message, None)?;
                        } else {
                            let mut topics_lock = topics.get(&topic).write().await;
                            repo.subscribe(subscription_key.clone()).await?;
                            client_lock
                                .add_subscription(topic.clone(), client_subscription_key.clone());
                            if let Some(value) = repo.get_by_key(&subscription_key).await? {
                                client_lock.send_subscribed(&topic, value)?;
                            } else {
                                client_lock.add_new_subscription(topic.clone());
                            }
                            topics_lock.add_subscription(topic, *client_id);
                        }
                    }
                    IncomeMessage::Unsubscribe {
                        topic: client_subscription_key,
                    } => {
                        let topic = Topic::try_from(&client_subscription_key)?;

                        if client_lock.contains_subscription(&topic) {
                            client_lock.remove_subscription(&topic);
                            topics
                                .get(&topic)
                                .write()
                                .await
                                .remove_subscription(&topic, client_id);
                        }

                        client_lock.send_unsubscribed(client_subscription_key)?;
                    }
                }
                break;
            }
            Err(_) => {
                debug!(
                    "client #{} is locked while income message handling, try to wait",
                    client_id
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    LOCKED_CLIENT_ON_INCOME_MESSAGE_HANDLING_SLEEP_DURATION_IN_MS,
                ))
                .await;
            }
        }
    }

    Ok(())
}

async fn send_error(
    error: impl Into<String>,
    message: impl Into<String>,
    code: u16,
    client: &Arc<Mutex<Client>>,
    client_id: &ClientId,
) -> Result<(), Error> {
    let mut error_details = std::collections::HashMap::new();
    error_details.insert("reason".to_string(), error.into());

    loop {
        match client.try_lock() {
            Ok(mut client_lock) => {
                client_lock.send_error(code, message.into(), Some(error_details))?;
                break;
            }
            Err(_) => {
                debug!(
                    "client #{} is locked while error sending, try to wait",
                    client_id
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    LOCKED_CLIENT_ON_ERROR_SENDING_SLEEP_DURATION_IN_MS,
                ))
                .await;
            }
        }
    }

    Ok(())
}

async fn on_disconnect(
    mut socket: ws::WebSocket,
    client: Arc<Mutex<Client>>,
    client_id: ClientId,
    clients: Arc<Sharded<Clients>>,
    topics: Arc<Sharded<Topics>>,
) -> () {
    loop {
        match client.try_lock() {
            Ok(client_lock) => {
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

                info!(
                    "client #{} disconnected; he got {} messages",
                    client_id,
                    client_lock.messages_count();
                    "req_id" => client_lock.get_request_id().clone()
                );

                break;
            }
            Err(_) => {
                debug!(
                    "client #{} is locked while disconnecting, try to wait",
                    client_id
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    LOCKED_CLIENT_ON_DISCONNECTING_SLEEP_DURATION_IN_MS,
                ))
                .await;
            }
        }
    }

    debug!(
        "client#{} subscribtions cleared; remove him from clients",
        client_id
    );

    clients.get(&client_id).write().await.remove(&client_id);

    debug!("client#{} removed from clients", client_id);

    // 1) errors will be only if socket already closed so just mute it
    // 2) client.sender sink closed, so send message using socket
    // todo: send real reason?
    let _ = socket.send(ws::Message::close_with(1000u16, "")).await;
    let _ = socket.close().await;
}

pub async fn updates_handler(
    mut updates_receiver: tokio::sync::mpsc::UnboundedReceiver<(Topic, String)>,
    clients: Arc<Sharded<Clients>>,
    topics: Arc<Sharded<Topics>>,
) {
    info!("websocket updates handler started");

    while let Some((topic, value)) = updates_receiver.recv().await {
        debug!("handled new update {:?}", topic);
        let maybe_client_ids = topics
            .get(&topic)
            .read()
            .await
            .get_client_ids(&topic)
            .cloned();

        if let Some(client_ids) = maybe_client_ids {
            let broadcasting_start = Instant::now();

            // NB: 1st implementation iterate over client_ids -> find shard for client_id -> acquire shard read lock -> get client lock -> send update
            // but it sometimes leads to deadlock|livelock|star (https://docs.rs/tokio/1.8.1/tokio/sync/struct.RwLock.html#method.read)
            // 2st implementation iterate over shards -> acquire shard read lock -> iterate over clients, filtered for update -> try to lock client -> send update
            for shard in clients.into_iter() {
                for (client_id, client) in
                    shard.read().await.iter().filter_map(|(client_id, client)| {
                        if client_ids.contains(&client_id) {
                            Some((client_id, client))
                        } else {
                            None
                        }
                    })
                {
                    debug!("send update to the client#{:?} {:?}", client_id, topic);
                    loop {
                        match client.try_lock() {
                            Ok(mut client_lock) => {
                                client_lock
                                    .send_update(&topic, value.to_owned())
                                    .expect("error occured while sending message");
                                break;
                            }
                            Err(_) => {
                                debug!(
                                    "client #{} is locked while sending update, try to wait",
                                    client_id
                                );
                                tokio::time::sleep(tokio::time::Duration::from_millis(
                                    LOCKED_CLIENT_ON_UPDATE_SLEEP_DURATION_IN_MS,
                                ))
                                .await;
                            }
                        }
                    }
                }
            }

            let broadcasting_end = Instant::now();
            debug!(
                "update successfully sent to {} clients for {} ms",
                client_ids.iter().count(),
                broadcasting_end
                    .duration_since(broadcasting_start)
                    .as_millis()
            );
        } else {
            debug!("there are not any clients to send update");
        }
    }
}
