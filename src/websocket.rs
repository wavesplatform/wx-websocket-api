use futures::{stream, SinkExt, StreamExt};
use serde::Serialize;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use warp::ws;
use wavesexchange_log::{debug, error, info};
use wavesexchange_topic::{State, StateSingle, Topic};

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
    topics: Arc<Topics>,
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
    topics: &Arc<Topics>,
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
                            error!("error occurred while processing client #{} message: {:?} – {:?}", client_id, msg, err);
                            break;
                        }
                        _ => Ok(())
                    }.is_err() {
                        error!("error occurred while sending message to client #{}", client_id);
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
                                error!("error occurred while sending ping message to client #{}: {:?}", client_id, error);
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
    topics: &Arc<Topics>,
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
                            let mut topics_lock = topics.write().await;
                            repo.subscribe(subscription_key.clone()).await?;
                            {
                                let topic = topic.clone();
                                let key = client_subscription_key.clone();
                                client_lock.add_direct_subscription(topic, key);
                            }
                            let value = repo.get_by_key(&subscription_key).await?;
                            let (value, subtopics) = if let (Some(value), true) =
                                (value.as_ref(), topic.is_multi_topic())
                            {
                                let subtopics_vec = parse_multitopic_value::<Vec<_>>(value)?;
                                let subtopics = HashSet::from_iter(subtopics_vec.iter().cloned());
                                let value =
                                    prepare_multitopic_response(repo, subtopics_vec, Vec::new())
                                        .await?;
                                (Some(value), Some(subtopics))
                            } else {
                                (value, None)
                            };
                            if let Some(value) = value {
                                client_lock.send_subscribed(&topic, value)?;
                            } else {
                                client_lock.mark_subscription_as_new(topic.clone());
                            }
                            let key = client_subscription_key.clone();
                            topics_lock.add_subscription(topic.clone(), *client_id, key);
                            if let Some(subtopics) = subtopics {
                                let update =
                                    topics_lock.update_multitopic_info(topic.clone(), subtopics);
                                for subtopic in update.added_subtopics.iter().cloned() {
                                    let key = client_subscription_key.clone();
                                    client_lock.add_indirect_subscription(
                                        subtopic,
                                        topic.clone(),
                                        key,
                                    );
                                }
                                for subtopic in update.removed_subtopics.iter() {
                                    client_lock.remove_indirect_subscription(subtopic, &topic);
                                }
                                topics_lock.update_indirect_subscriptions(topic, update, client_id);
                            }
                        }
                    }
                    IncomeMessage::Unsubscribe {
                        topic: client_subscription_key,
                    } => {
                        let topic = Topic::try_from(&client_subscription_key)?;

                        if client_lock.contains_subscription(&topic) {
                            client_lock.remove_direct_subscription(&topic);
                            let mut topics_lock = topics.write().await;
                            if topic.is_multi_topic() {
                                let removed =
                                    topics_lock.remove_indirect_subscriptions(&topic, client_id);
                                for subtopic in removed {
                                    client_lock.remove_indirect_subscription(&subtopic, &topic);
                                }
                            }
                            topics_lock.remove_subscription(&topic, client_id);
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
    topics: Arc<Topics>,
) -> () {
    loop {
        match client.try_lock() {
            Ok(client_lock) => {
                // remove topics from Topics
                stream::iter(client_lock.subscription_topics_iter())
                    .map(|topic| (topic, &topics))
                    .for_each_concurrent(20, |(topic, topics)| async move {
                        let mut topics_lock = topics.write().await;
                        if topic.is_multi_topic() {
                            let _ = topics_lock.remove_indirect_subscriptions(topic, &client_id);
                            // Since we are dropping the disconnected client anyway,
                            // don't bother removing individual indirect subscriptions
                        }
                        topics_lock.remove_subscription(topic, &client_id);
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
        "client#{} subscriptions cleared; remove him from clients",
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

pub async fn updates_handler<R: Repo>(
    mut updates_receiver: tokio::sync::mpsc::UnboundedReceiver<(Topic, String)>,
    clients: Arc<Sharded<Clients>>,
    topics: Arc<Topics>,
    repo: Arc<R>,
) {
    info!("websocket updates handler started");

    while let Some((topic, value)) = updates_receiver.recv().await {
        debug!("handled new update {:?}", topic);
        let subscribed_clients = topics.read().await.get_subscribed_clients(&topic);
        let has_subscribed_clients = !subscribed_clients.is_empty();

        let multiupdate = if topic.is_multi_topic() {
            let subtopics = match parse_multitopic_value(&value) {
                Ok(v) => v,
                Err(e) => {
                    error!(
                        "multitopic {} has unrecognized value {}; error = {}",
                        String::from(topic),
                        value,
                        e
                    );
                    continue;
                }
            };
            let mut topics_lock = topics.write().await;
            let multitopic_update = topics_lock.update_multitopic_info(topic.clone(), subtopics);
            if has_subscribed_clients {
                for client_id in subscribed_clients.keys() {
                    topics_lock.update_indirect_subscriptions(
                        topic.clone(),
                        multitopic_update.clone(),
                        client_id,
                    );
                }
                let multitopic_response = {
                    let update = multitopic_update.clone();
                    prepare_multitopic_response(
                        &repo,
                        update.added_subtopics,
                        update.removed_subtopics,
                    )
                    .await
                };
                match multitopic_response {
                    Ok(v) => Some((multitopic_update, v)),
                    Err(e) => {
                        error!(
                            "failed to build update for multitopic {}; error = {}",
                            String::from(topic),
                            e
                        );
                        continue;
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        if has_subscribed_clients {
            let broadcasting_start = Instant::now();

            // NB: 1st implementation iterate over subscribed_clients.keys() -> find shard for client_id -> acquire shard read lock -> get client lock -> send update
            // but it sometimes leads to deadlock|livelock|star (https://docs.rs/tokio/1.8.1/tokio/sync/struct.RwLock.html#method.read)
            // 2nd implementation iterate over shards -> acquire shard read lock -> iterate over clients, filtered for update -> try to lock client -> send update
            for clients_shard in clients.into_iter() {
                let clients_shard_lock = clients_shard.read().await;
                let matching_clients_iter =
                    clients_shard_lock.iter().filter_map(|(client_id, client)| {
                        if let Some(direct_subscription_key) = subscribed_clients.get(client_id) {
                            Some((client_id, client, direct_subscription_key))
                        } else {
                            None
                        }
                    });
                for (client_id, client, direct_subscription_key) in matching_clients_iter {
                    debug!("send update to the client#{:?} {:?}", client_id, topic);
                    let (value, multitopic_update) = {
                        if let Some((multitopic_update, update_value)) = multiupdate.as_ref() {
                            (update_value.clone(), Some(multitopic_update))
                        } else {
                            (value.clone(), None)
                        }
                    };
                    loop {
                        match client.try_lock() {
                            Ok(mut client_lock) => {
                                if let Some(multitopic_update) = multitopic_update {
                                    let (added_topics, removed_topics) = (
                                        multitopic_update.added_subtopics.iter().cloned(),
                                        multitopic_update.removed_subtopics.iter(),
                                    );
                                    for subtopic in added_topics {
                                        let parent_subscription_key =
                                            direct_subscription_key.clone().unwrap_or_default();
                                        client_lock.add_indirect_subscription(
                                            subtopic,
                                            topic.clone(),
                                            parent_subscription_key,
                                        );
                                    }
                                    for subtopic in removed_topics {
                                        client_lock.remove_indirect_subscription(subtopic, &topic);
                                    }
                                }
                                client_lock
                                    .send_update(&topic, value)
                                    .expect("error occurred while sending message");
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
                subscribed_clients.len(),
                broadcasting_end
                    .duration_since(broadcasting_start)
                    .as_millis()
            );
        } else {
            debug!("there are not any clients to send update");
        }
    }
}

fn parse_multitopic_value<T>(topics_json: &str) -> Result<T, Error>
where
    T: FromIterator<Topic>,
{
    let topics = serde_json::from_str::<Vec<&str>>(topics_json)?;
    topics
        .into_iter()
        .map(|topic_url| {
            Topic::try_from(topic_url).map_err(|_| Error::InvalidTopic(topic_url.to_owned()))
        })
        .collect()
}

async fn prepare_multitopic_response<R, T1, T2>(
    repo: &Arc<R>,
    updated_topics: T1,
    removed_topics: T2,
) -> Result<String, Error>
where
    R: Repo,
    T1: IntoIterator<Item = Topic>,
    T2: IntoIterator<Item = Topic>,
    T1::IntoIter: Clone,
{
    let updated_topics = updated_topics.into_iter();
    let removed_topics = removed_topics.into_iter();

    let topics: Vec<String> = updated_topics
        .clone()
        .map(|topic| String::from(topic))
        .collect();
    let topics_len = topics.len();
    let values = repo.get_by_keys(topics).await?;
    debug_assert_eq!(topics_len, values.len()); // Redis' MGET guarantees this
    let values = values
        .into_iter()
        .map(|maybe_topic| maybe_topic.unwrap_or_default());
    let updated = updated_topics
        .zip(values)
        .map(|(topic, value)| match topic {
            Topic::State(State::Single(StateSingle { address, key })) => MultiValueResponseItem {
                address,
                key,
                value,
            },
            _ => panic!("internal error: updated_topics contains unexpected topics"),
        });
    let removed = removed_topics.map(|topic| match topic {
        Topic::State(State::Single(StateSingle { address, key })) => MultiValueResponseItem {
            address,
            key,
            value: "".to_string(),
        },
        _ => panic!("internal error: updated_topics contains unexpected topics"),
    });
    let res_list = updated.chain(removed).collect::<Vec<_>>();
    let response_json_str = serde_json::to_string(&res_list)?;
    Ok(response_json_str)
}

#[derive(Debug, Serialize)]
struct MultiValueResponseItem {
    address: String,
    key: String,
    value: String,
}
