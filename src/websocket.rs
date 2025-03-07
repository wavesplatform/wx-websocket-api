use futures::{stream, SinkExt, StreamExt};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use warp::ws;
use wx_topic::{StateSingle, Topic};

use crate::client::{Client, ClientId, Clients, MultitopicUpdate, Subscribed, Topics};
use crate::error::Error;
use crate::messages::IncomeMessage;
use crate::metrics::{
    CLIENTS, CLIENT_CONNECT, CLIENT_DISCONNECT, REDIS_INPUT_QUEUE_SIZE,
    SUBSCRIBED_MESSAGE_LATENCIES,
};
use crate::repo::Repo;
use crate::shard::Sharded;

use self::values::{DataEntry, TopicValue, TopicValues};

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

    log::info!("Client#{} connected", client_id);

    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();

    let (kill_tx, kill_rx) = tokio::sync::oneshot::channel();

    let client = Arc::new(Mutex::new(Client::new(
        client_id,
        client_tx,
        kill_tx,
        request_id.clone(),
    )));

    CLIENTS.inc();
    CLIENT_CONNECT.inc();

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
            log::debug!("shutdown signal handled by Client#{}", client_id);
        }
        _ = kill_rx => {
            log::debug!("graceful kill handled by Client#{}", client_id);
        }
    }

    // handle connection close
    on_disconnect(socket, client, client_id, clients, topics).await;

    CLIENTS.dec();
    CLIENT_DISCONNECT.inc();

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
                            log::debug!("Connection to Client#{} was unexpectedly closed: {}", client_id, disconnected; "req_id" => request_id);
                            break;
                        }
                    };

                    if msg.is_close() {
                        log::debug!("Connection to Client#{} was closed", client_id; "req_id" => request_id);
                        break;
                    }

                    if msg.is_ping() || msg.is_pong() {
                        continue
                    }

                    if match handle_income_message(&repo, client, client_id, topics, &msg).await {
                        Err(Error::UnknownIncomeMessage(error)) => send_error(error, "Invalid message", INVALID_MESSAGE_ERROR_CODE, client, client_id).await,
                        Err(Error::InvalidTopicFromClient(topic)) => {
                            log::debug!(
                                "Bad request: Client#{} has sent invalid topic '{}' in message '{}'",
                                client_id,
                                topic,
                                msg.to_str().unwrap_or("<BINARY>"),
                            );
                            let error = format!("Invalid topic: {}", topic);
                            send_error(error, "Invalid topic", INVALID_TOPIC_ERROR_CODE, client, client_id).await
                        }
                        Err(Error::InvalidPongMessage) => {
                            // nothing to do
                            // just close the connection
                            break;
                        }
                        Err(err) => {
                            log::error!("Error occurred while processing Client#{} message: {:?} – {:?}", client_id, msg, err);
                            break;
                        }
                        _ => Ok(())
                    }.is_err() {
                        log::error!("Error occurred while sending message to Client#{}", client_id);
                        break;
                    }
                }
            },
            // outcome message (to ws)
            msg = client_rx.recv() => {
                if let Some(message) = msg {
                    log::debug!("Sending message to Client#{:?}", client_id);
                    if let Err(err) = socket.send(message).await {
                        log::error!("error occurred while sending message to ws client: {:?}", err; "req_id" => request_id);
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
                                log::debug!("Client#{} did not answer for {} consequent ping messages", client_id, options.ping_failures_threshold);
                                break;
                            }
                            if let Err(error) = client_lock.send_ping() {
                                log::error!("Error occurred while sending ping message to Client#{}: {:?}", client_id, error);
                                break;
                            }
                            break;
                        }
                        Err(_) => {
                            log::debug!(
                                "Client#{} is locked while ping sending, try to wait",
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
                        log::debug!(
                            "Handling Client#{} subscription to key {:?}",
                            client_id,
                            client_subscription_key,
                        );

                        let latency_timer = SUBSCRIBED_MESSAGE_LATENCIES.start_timer();

                        let topic = client_subscription_key.try_as_topic()?;
                        let subscription_key = topic.to_string();

                        // When topic URI is parsed there may some character substitutions occur,
                        // like non-ASCII characters replaced with their percent-encoding.
                        // For potential problems solving we better log it if this happens.
                        if subscription_key.as_str() != client_subscription_key.as_str() {
                            log::debug!(
                                "Subscription key parsed with non-ASCII characters: '{}' -> '{}'",
                                client_subscription_key.as_str(),
                                subscription_key.as_str(),
                            );
                        }

                        if client_lock.contains_subscription(&topic) {
                            const ALREADY_SUBSCRIBED_ERROR_MESSAGE: &'static str =
                                "You are already subscribed for the specified topic";
                            client_lock.send_error(
                                ALREADY_SUBSCRIBED_ERROR_CODE,
                                ALREADY_SUBSCRIBED_ERROR_MESSAGE.to_string(),
                                None,
                            )?;
                        } else {
                            let mut topics_lock = topics.write().await;
                            {
                                let topic = topic.clone();
                                let key = client_subscription_key.clone();
                                client_lock.add_direct_subscription(topic, key);
                            }
                            let value = repo.get_by_key(&subscription_key).await?;
                            log::debug!("Current value in Redis: {:?}", value; "topic" => format!("{:?}", topic));
                            let subtopics;
                            if let Some(value) = value {
                                let send_value;
                                if topic.is_multi_topic() {
                                    let subtopics_vec = parse_subtopic_list::<Vec<_>>(&value)?;
                                    log::debug!(
                                        "Subtopics ({}): {:?}",
                                        subtopics_vec.len(),
                                        subtopics_vec;
                                        "topic" => format!("{:?}", topic)
                                    );
                                    subtopics = Some(HashSet::<Topic>::from_iter(
                                        subtopics_vec.iter().cloned(),
                                    ));
                                    let mut subtopic_values =
                                        fetch_subtopic_values(repo, subtopics_vec, Vec::new())
                                            .await?;
                                    log::debug!(
                                        "Subtopic values ({}): {:?}",
                                        subtopic_values.0.len(),
                                        subtopic_values;
                                        "topic" => format!("{:?}", topic)
                                    );
                                    subtopic_values.filter_raw_null();
                                    send_value = subtopic_values.as_json_string();
                                } else {
                                    send_value = value;
                                    subtopics = None;
                                }
                                client_lock.send_subscribed(&topic, send_value)?;
                                let latency = latency_timer.stop_and_record();
                                log::debug!(
                                    "Latency {}ms (immediate send), topic {:?}",
                                    latency * 1_000_f64,
                                    topic
                                );
                            } else {
                                subtopics = None;
                                client_lock.mark_subscription_as_new(topic.clone(), latency_timer);
                            }
                            let key = client_subscription_key.clone();
                            topics_lock.add_subscription(topic.clone(), *client_id, key);
                            if let Some(subtopics) = subtopics {
                                log::debug!("Subtopics: {:?}", subtopics; "topic" => format!("{:?}", topic));
                                let _ = topics_lock
                                    .update_multitopic_info(topic.clone(), subtopics.clone());
                                for subtopic in subtopics.iter().cloned() {
                                    log::trace!("    Subtopic: {:?}", subtopic);
                                    client_lock.add_indirect_subscription(
                                        subtopic,
                                        topic.clone(),
                                        client_subscription_key.clone(),
                                    );
                                }
                                let update = MultitopicUpdate {
                                    added_subtopics: subtopics.iter().cloned().collect(),
                                    removed_subtopics: vec![],
                                };
                                topics_lock.update_indirect_subscriptions(topic, update, client_id);
                            }
                            repo.subscribe(subscription_key).await?;
                        }
                    }
                    IncomeMessage::Unsubscribe {
                        topic: client_subscription_key,
                    } => {
                        let topic = client_subscription_key.try_as_topic()?;

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
                log::debug!(
                    "Client#{} is locked while income message handling, try to wait",
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
                log::debug!(
                    "Client#{} is locked while error sending, try to wait",
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
            Ok(mut client_lock) => {
                client_lock.on_disconnect();
                // remove topics from Topics
                stream::iter(client_lock.subscription_topics_iter())
                    .map(|(topic, is_direct, is_indirect)| (topic, is_direct, is_indirect, &topics))
                    .for_each_concurrent(20, |(topic, is_direct, is_indirect, topics)| async move {
                        let mut topics_lock = topics.write().await;
                        if topic.is_multi_topic() {
                            let _ = topics_lock.remove_indirect_subscriptions(topic, &client_id);
                            // Since we are dropping the disconnected client anyway,
                            // don't bother removing individual indirect subscriptions
                        }
                        if is_direct {
                            topics_lock.remove_subscription(topic, &client_id);
                        }
                        if is_indirect {
                            // Will be deleted when the corresponding multitopic is deleted
                        }
                    })
                    .await;

                log::info!(
                    "Client#{} disconnected; he got {} messages",
                    client_id,
                    client_lock.messages_count();
                    "req_id" => client_lock.get_request_id().clone()
                );

                break;
            }
            Err(_) => {
                log::debug!(
                    "Client#{} is locked while disconnecting, try to wait",
                    client_id
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    LOCKED_CLIENT_ON_DISCONNECTING_SLEEP_DURATION_IN_MS,
                ))
                .await;
            }
        }
    }

    log::debug!(
        "Client#{} subscriptions cleared; remove him from clients",
        client_id
    );

    clients.get(&client_id).write().await.remove(&client_id);

    log::debug!("Client#{} removed from clients", client_id);

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
    log::info!("websocket updates handler started");

    enum Update {
        Ignore,
        Single {
            topic: Topic,
            value: String,
        },
        Multi {
            topic: Topic,
            value: String,
            multitopic_update: MultitopicUpdate,
        },
    }

    while let Some((topic, value)) = updates_receiver.recv().await {
        REDIS_INPUT_QUEUE_SIZE.dec();
        let subscribed_clients = topics.read().await.get_subscribed_clients(&topic);
        let has_subscribed_clients = !subscribed_clients.is_empty();
        log::debug!(
            "Topic updated in Redis: {:?}\n\tValue: {:?}\n\tSubscribed clients ({}): {:?}",
            topic,
            value,
            subscribed_clients.len(),
            subscribed_clients,
        );

        let update = {
            if has_subscribed_clients {
                if topic.is_multi_topic() {
                    match parse_subtopic_list::<HashSet<_>>(&value) {
                        Ok(subtopics) => {
                            let multitopic_is_empty = subtopics.is_empty();
                            log::debug!("Subtopics (from Redis): {:?}", subtopics; "topic" => format!("{:?}", topic));
                            let mut topics_lock = topics.write().await;
                            let multitopic_update = {
                                let topic = topic.clone();
                                topics_lock.update_multitopic_info(topic, subtopics)
                            };
                            log::debug!("Multitopic update: {:?}", multitopic_update; "topic" => format!("{:?}", topic));
                            for client_id in subscribed_clients.keys() {
                                topics_lock.update_indirect_subscriptions(
                                    topic.clone(),
                                    multitopic_update.clone(),
                                    client_id,
                                );
                            }
                            let subtopic_values = {
                                let update = multitopic_update.clone();
                                fetch_subtopic_values(
                                    &repo,
                                    update.added_subtopics,
                                    update.removed_subtopics,
                                )
                            };
                            match subtopic_values.await {
                                Ok(mut subtopic_values) => {
                                    log::debug!("Subtopic values: {:?}", subtopic_values; "topic" => format!("{:?}", topic));
                                    subtopic_values.filter_raw_null();
                                    let subtopic_values_available = !subtopic_values.is_empty();
                                    if multitopic_is_empty {
                                        log::debug!("Update accepted: multitopic is empty"; "topic" => format!("{:?}", topic), "value" => subtopic_values.as_json_string());
                                    }
                                    if subtopic_values_available {
                                        log::debug!("Update accepted: subtopic values available"; "topic" => format!("{:?}", topic), "value" => subtopic_values.as_json_string());
                                    }
                                    if multitopic_is_empty || subtopic_values_available {
                                        Update::Multi {
                                            topic,
                                            value: subtopic_values.as_json_string(),
                                            multitopic_update,
                                        }
                                    } else {
                                        log::debug!("Update ignored: has subtopics but values not available yet"; "topic" => format!("{:?}", topic));
                                        // Values of the new topics not yet available,
                                        // so just ignore the update,
                                        // it will be received later again as individual
                                        // subtopic update anyway
                                        Update::Ignore
                                    }
                                }
                                Err(err) => {
                                    log::error!(
                                        "failed to build update for multitopic {}; error = {}",
                                        format!("{:?}", topic),
                                        err; "topic" => format!("{:?}", topic)
                                    );
                                    Update::Ignore
                                }
                            }
                        }
                        Err(err) => {
                            log::error!(
                                "Multitopic {:?} has unrecognized value '{}' in Redis; error = {}",
                                topic,
                                value,
                                err; "topic" => format!("{}", topic)
                            );
                            Update::Ignore
                        }
                    }
                } else {
                    Update::Single { topic, value }
                }
            } else {
                log::debug!("there are no clients to send update to"; "topic" => format!("{:?}", topic));
                Update::Ignore
            }
        };

        if matches!(update, Update::Ignore) {
            continue;
        }

        let broadcasting_start = Instant::now();

        // NB: 1st implementation iterate over subscribed_clients.keys() -> find shard for client_id -> acquire shard read lock -> get client lock -> send update
        // but it sometimes leads to deadlock|livelock|star (https://docs.rs/tokio/1.8.1/tokio/sync/struct.RwLock.html#method.read)
        // 2nd implementation iterate over shards -> acquire shard read lock -> iterate over clients, filtered for update -> try to lock client -> send update
        for clients_shard in clients.into_iter() {
            let clients_shard_lock = clients_shard.read().await;
            let matching_clients_iter =
                clients_shard_lock.iter().filter_map(|(client_id, client)| {
                    if let Some(subscribed) = subscribed_clients.get(client_id) {
                        Some((client_id, client, subscribed))
                    } else {
                        None
                    }
                });
            for (client_id, client, subscribed) in matching_clients_iter {
                log::debug!("Sending update to Client#{:?}", client_id);
                loop {
                    match client.try_lock() {
                        Ok(mut client_lock) => {
                            let (topic, value) = match &update {
                                Update::Single { topic, value } => (topic, value.clone()),
                                Update::Multi {
                                    topic,
                                    value,
                                    multitopic_update,
                                } => {
                                    let (added_topics, removed_topics) = (
                                        multitopic_update.added_subtopics.iter().cloned(),
                                        multitopic_update.removed_subtopics.iter(),
                                    );
                                    for subtopic in added_topics {
                                        let parent_subscription_key = match subscribed {
                                            Subscribed::DirectlyWithKey(ref key) => key.clone(),
                                            Subscribed::Indirectly => Default::default(),
                                        };
                                        client_lock.add_indirect_subscription(
                                            subtopic,
                                            topic.clone(),
                                            parent_subscription_key,
                                        );
                                    }
                                    for subtopic in removed_topics {
                                        client_lock.remove_indirect_subscription(subtopic, topic);
                                    }
                                    (topic, value.clone())
                                }
                                Update::Ignore => unreachable!(),
                            };
                            client_lock
                                .send_update(topic, value)
                                .expect("error occurred while sending message");
                            break;
                        }
                        Err(_) => {
                            log::debug!(
                                "Client#{} is locked while sending update, try to wait",
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
        log::debug!(
            "update successfully sent to {} clients for {} ms",
            subscribed_clients.len(),
            broadcasting_end
                .duration_since(broadcasting_start)
                .as_millis()
        );
    }
}

fn parse_subtopic_list<T>(topics_json: &str) -> Result<T, Error>
where
    T: FromIterator<Topic>,
{
    let topics = serde_json::from_str::<Vec<&str>>(topics_json)?;
    topics
        .into_iter()
        .map(|topic_url| {
            match Topic::parse_str(topic_url) {
                Ok(topic) => Ok(topic),
                Err(e) => {
                    // This is a bad data coming from Redis,
                    // it is either a programming error or manually-tampered data.
                    Err(Error::InvalidTopicInRedis(e))
                }
            }
        })
        .collect()
}

async fn fetch_subtopic_values<R, T>(
    repo: &Arc<R>,
    updated_topics: T,
    removed_topics: T,
) -> Result<TopicValues, Error>
where
    R: Repo,
    T: IntoIterator<Item = Topic>,
{
    let updated_topics = updated_topics.into_iter();
    let removed_topics = removed_topics.into_iter();

    let topics = updated_topics
        .map(|topic| topic.to_string())
        .collect::<Vec<_>>();
    let topics_len = topics.len();
    let values = repo.get_by_keys(topics).await?;
    debug_assert_eq!(topics_len, values.len()); // Redis' MGET guarantees this
    let updated = values
        .into_iter()
        .filter_map(|maybe_value| maybe_value.map(|value| TopicValue::Raw(value)));
    let removed = removed_topics.map(|topic| {
        let topic_data = topic.data();
        let StateSingle { address, key } = topic_data
            .as_state_single()
            .expect("internal error: updated_topics contains unexpected topics");
        let entry = DataEntry::deleted(address.clone(), key.clone());
        TopicValue::DataEntry(entry)
    });
    let res_list = updated.chain(removed).collect::<Vec<_>>();
    Ok(TopicValues(res_list))
}

mod values {
    use serde::{Serialize, Serializer};

    #[derive(Debug)]
    pub struct TopicValues(pub Vec<TopicValue>);

    #[derive(Clone, Debug, Serialize)]
    #[serde(untagged)]
    pub enum TopicValue {
        Raw(String),
        #[serde(serialize_with = "data_entry_serialize")]
        DataEntry(DataEntry),
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct DataEntry {
        address: String,
        key: String,
        value: String,
    }

    impl DataEntry {
        pub fn deleted(address: String, key: String) -> Self {
            DataEntry {
                address,
                key,
                value: "".to_string(),
            }
        }

        fn as_json_string(&self) -> String {
            // Serialization of this struct can't fail.
            // If it happens - something is fundamentally broken, so panic is ok here.
            serde_json::to_string(self).expect("DataEntry serialize")
        }
    }

    impl TopicValues {
        pub fn is_empty(&self) -> bool {
            self.0.is_empty()
        }

        pub fn as_json_string(&self) -> String {
            let TopicValues(values) = self;
            // Serialization of this struct can't fail.
            // If it happens - something is fundamentally broken, so panic is ok here.
            serde_json::to_string(values).expect("TopicValues serialize")
        }

        pub fn filter_raw_null(&mut self) {
            self.0
                .retain(|e| !matches!(e, TopicValue::Raw(s) if s == "null"))
        }
    }

    fn data_entry_serialize<S: Serializer>(entry: &DataEntry, s: S) -> Result<S::Ok, S::Error> {
        let str = entry.as_json_string();
        s.serialize_str(&str)
    }

    #[test]
    fn topic_values_as_json() {
        assert_eq!(TopicValues(vec![]).as_json_string(), r#"[]"#);

        let values = TopicValues(vec![
            TopicValue::Raw("raw value".to_string()),
            TopicValue::DataEntry(DataEntry::deleted("addr".to_string(), "key".to_string())),
        ]);
        assert_eq!(
            values.as_json_string(),
            r#"["raw value","{\"address\":\"addr\",\"key\":\"key\",\"value\":\"\"}"]"#
        )
    }
}
