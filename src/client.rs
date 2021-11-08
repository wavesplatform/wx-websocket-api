use crate::error::Error;
use crate::messages::OutcomeMessage;
use crate::metrics::MESSAGES;
use prometheus::HistogramTimer;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock};
use warp::ws::Message;
use wavesexchange_log::{debug, warn};
use wavesexchange_topic::Topic;

pub type ClientId = usize;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClientSubscriptionKey(pub String);

#[derive(Debug)]
pub struct Client {
    client_id: ClientId,
    sender: ClientSender,
    subscriptions: HashMap<Topic, ClientSubscriptionData>,
    request_id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Subscribed {
    DirectlyWithKey(ClientSubscriptionKey),
    Indirectly,
}

#[derive(Debug)]
struct ClientSender {
    sender: tokio::sync::mpsc::UnboundedSender<Message>,
    message_counter: i64,
    pings: Vec<i64>,
}

#[derive(Default, Debug)]
struct ClientSubscriptionData {
    /// Subscription key for the topic as received from the client.
    /// Can refer to either concrete topic or multitopic.
    subscription_key: ClientSubscriptionKey,
    /// Multitopics that indirectly added this concrete topic
    /// to the client subscriptions.
    indirect_subscription_sources: HashMap<Topic, IndirectSubscriptionData>,
    /// Intermediate state - awaiting of the initial values
    /// so that we can send 'subscribed' message.
    is_new: bool,
    /// This topic was directly subscribed to.
    is_direct: bool,
    /// This topic was indirectly subscribed to by one or more multitopics.
    is_indirect: bool,
    /// Last value of indirect balance (to allow computation of delta value).
    leasing_balance_last_value: Option<LeasingBalance>,
    /// Timer that counts latency between 'Subscribe' and 'Subscribed' messages
    latency_timer: Option<HistogramTimer>,
}

#[derive(Default, Debug)]
struct IndirectSubscriptionData {
    /// Subscription key for the parent multitopic as received from the client.
    subscription_key: ClientSubscriptionKey,
}

#[derive(Serialize, Deserialize, Debug)]
struct LeasingBalance {
    address: String,
    #[serde(rename = "in")]
    balance_in: i64,
    #[serde(rename = "out")]
    balance_out: i64,
}

fn leasing_balance_diff(old_value: &LeasingBalance, new_value: &LeasingBalance) -> String {
    if old_value.balance_in == new_value.balance_in {
        if old_value.balance_out == new_value.balance_out {
            serde_json::to_string(&new_value).unwrap()
        } else {
            let v = serde_json::json!({"address": new_value.address, "out": new_value.balance_out});
            serde_json::to_string(&v).unwrap()
        }
    } else if old_value.balance_out == new_value.balance_out {
        let v = serde_json::json!({"address": new_value.address, "in": new_value.balance_in});
        serde_json::to_string(&v).unwrap()
    } else {
        serde_json::to_string(&new_value).unwrap()
    }
}

impl Client {
    pub fn new(
        client_id: ClientId,
        sender: tokio::sync::mpsc::UnboundedSender<Message>,
        request_id: Option<String>,
    ) -> Self {
        Client {
            client_id,
            sender: ClientSender {
                sender,
                message_counter: 1,
                pings: vec![],
            },
            request_id,
            subscriptions: HashMap::new(),
        }
    }

    pub fn get_request_id(&self) -> &Option<String> {
        &self.request_id
    }

    pub fn contains_subscription(&self, topic: &Topic) -> bool {
        self.subscriptions.contains_key(topic)
    }

    pub fn add_direct_subscription(
        &mut self,
        topic: Topic,
        client_subscription_key: ClientSubscriptionKey,
    ) {
        debug!(
            "[Client] Client#{} directly subscribed to {:?}\n\tSubscription key: {:?}",
            self.client_id, topic, client_subscription_key
        );
        let subscription_data = self.subscriptions.entry(topic).or_default();
        subscription_data.subscription_key = client_subscription_key;
        subscription_data.is_direct = true;
    }

    pub fn add_indirect_subscription(
        &mut self,
        topic: Topic,
        parent_multitopic: Topic,
        client_subscription_key: ClientSubscriptionKey,
    ) {
        debug!(
            "[Client] Client#{} indirectly subscribed to {:?}\n\tParent multitopic {:?}\n\tSubscription key: {:?}",
            self.client_id, topic, parent_multitopic, client_subscription_key
        );
        let subscription_data = self.subscriptions.entry(topic).or_default();
        subscription_data.indirect_subscription_sources.insert(
            parent_multitopic,
            IndirectSubscriptionData {
                subscription_key: client_subscription_key,
            },
        );
        subscription_data.is_indirect = true;
    }

    pub fn mark_subscription_as_new(&mut self, topic: Topic, timer: HistogramTimer) {
        self.subscriptions
            .entry(topic)
            .and_modify(|subscription_data| {
                subscription_data.is_new = true;
                subscription_data.latency_timer = Some(timer);
            });
    }

    pub fn remove_direct_subscription(&mut self, topic: &Topic) {
        debug!(
            "[Client] Client#{} directly unsubscribed from {:?}",
            self.client_id, topic
        );
        if let Some(subscription_data) = self.subscriptions.get_mut(topic) {
            subscription_data.is_direct = false;
            let still_subscribed = subscription_data.is_direct || subscription_data.is_indirect;
            if !still_subscribed {
                self.subscriptions.remove(topic);
            }
        }
    }

    pub fn remove_indirect_subscription(&mut self, topic: &Topic, parent_multitopic: &Topic) {
        if let Some(subscription_data) = self.subscriptions.get_mut(topic) {
            debug!(
                "[Client] Client#{} indirectly unsubscribed from {:?}\n\tParent multitopic {:?}",
                self.client_id, topic, parent_multitopic
            );
            subscription_data
                .indirect_subscription_sources
                .remove(parent_multitopic);
            if subscription_data.indirect_subscription_sources.is_empty() {
                subscription_data.is_indirect = false;
            }
            let still_subscribed = subscription_data.is_direct || subscription_data.is_indirect;
            if !still_subscribed {
                self.subscriptions.remove(topic);
            }
        }
    }

    pub fn handle_pong(&mut self, message_number: i64) -> Result<(), Error> {
        let valid_pong = self.sender.handle_pong(message_number).is_ok();
        if valid_pong {
            Ok(())
        } else {
            // client sent invalid pong message
            warn!("got invalid pong message");
            Err(Error::InvalidPongMessage)
        }
    }

    pub fn pings_len(&self) -> usize {
        self.sender.pings.len()
    }

    pub fn send_ping(&mut self) -> Result<(), Error> {
        self.sender.send_ping()
    }

    pub fn send_subscribed(&mut self, topic: &Topic, value: String) -> Result<(), Error> {
        if let Some(subscription_data) = self.subscriptions.get_mut(topic) {
            if let Ok(Some(lb)) = serde_json::from_str(&value) {
                subscription_data.leasing_balance_last_value = lb;
            }
            let subscription_key = subscription_data.subscription_key.clone();
            self.sender.send_subscribed(subscription_key, value)?;
        }
        Ok(())
    }

    pub fn send_update(&mut self, topic: &Topic, mut value: String) -> Result<(), Error> {
        if let Some(subscription_data) = self.subscriptions.get_mut(topic) {
            if let Ok(Some(lb)) = serde_json::from_str(&value) {
                if let Some(ref old_value) = subscription_data.leasing_balance_last_value {
                    value = leasing_balance_diff(old_value, &lb);
                }
                subscription_data.leasing_balance_last_value = Some(lb);
            }

            if subscription_data.is_direct {
                let subscription_key = subscription_data.subscription_key.clone();
                let value = value.clone();
                if subscription_data.is_new {
                    subscription_data.is_new = false;
                    self.sender.send_subscribed(subscription_key, value)?;
                } else {
                    self.sender.send_update(subscription_key, value)?;
                }
            }

            if subscription_data.is_indirect {
                for sub in subscription_data.indirect_subscription_sources.values() {
                    let subscription_key = sub.subscription_key.clone();
                    let value = value.clone();
                    self.sender.send_update(subscription_key, value)?;
                }
            }

            if let Some(latency_timer) = subscription_data.latency_timer.take() {
                let latency = latency_timer.stop_and_record();
                debug!(
                    "Latency {}ms (delayed send), topic {:?}",
                    latency * 1_000_f64,
                    topic
                );
            }
        }
        Ok(())
    }

    pub fn send_unsubscribed(
        &mut self,
        subscription_key: ClientSubscriptionKey,
    ) -> Result<(), Error> {
        self.sender.send_unsubscribed(subscription_key)
    }

    pub fn send_error(
        &mut self,
        code: u16,
        message: String,
        details: Option<HashMap<String, String>>,
    ) -> Result<(), Error> {
        self.sender.send_error(code, message, details)
    }

    pub fn on_disconnect(&mut self) {
        for (topic, subscription_data) in self.subscriptions.iter_mut() {
            if let Some(latency_timer) = subscription_data.latency_timer.take() {
                let latency = latency_timer.stop_and_discard();
                debug!(
                    "Subscription aborted after {}ms waiting, topic {:?}",
                    latency * 1_000_f64,
                    topic
                );
            }
        }
    }

    pub fn messages_count(&self) -> i64 {
        self.sender.message_counter - 1
    }

    pub fn subscription_topics_iter(&self) -> impl Iterator<Item = (&Topic, bool, bool)> {
        self.subscriptions
            .iter()
            .map(|(topic, data)| (topic, data.is_direct, data.is_indirect))
    }
}

impl ClientSender {
    fn handle_pong(&mut self, message_number: i64) -> Result<(), ()> {
        if self.pings.contains(&message_number) {
            self.pings = self
                .pings
                .iter()
                .filter(|&&x| x > message_number)
                .cloned()
                .collect();
            Ok(())
        } else {
            Err(())
        }
    }

    fn send_ping(&mut self) -> Result<(), Error> {
        self.pings.push(self.message_counter);
        let message_number = self.message_counter;
        let message = OutcomeMessage::Ping { message_number };
        self.send(message)
    }

    fn send_subscribed(
        &mut self,
        subscription_key: ClientSubscriptionKey,
        value: String,
    ) -> Result<(), Error> {
        let message = OutcomeMessage::Subscribed {
            message_number: self.message_counter,
            topic: subscription_key,
            value,
        };
        self.send(message)?;
        MESSAGES.inc();
        Ok(())
    }

    fn send_update(
        &mut self,
        subscription_key: ClientSubscriptionKey,
        value: String,
    ) -> Result<(), Error> {
        let message = OutcomeMessage::Update {
            message_number: self.message_counter,
            topic: subscription_key,
            value,
        };
        self.send(message)?;
        MESSAGES.inc();
        Ok(())
    }

    fn send_unsubscribed(&mut self, subscription_key: ClientSubscriptionKey) -> Result<(), Error> {
        let message = OutcomeMessage::Unsubscribed {
            message_number: self.message_counter,
            topic: subscription_key,
        };
        self.send(message)
    }

    fn send_error(
        &mut self,
        code: u16,
        message: String,
        details: Option<HashMap<String, String>>,
    ) -> Result<(), Error> {
        let message = OutcomeMessage::Error {
            code,
            message,
            details,
            message_number: self.message_counter,
        };
        self.send(message)
    }

    fn send(&mut self, message: OutcomeMessage) -> Result<(), Error> {
        if !self.sender.is_closed() {
            self.message_counter += 1;
            self.sender.send(Message::from(message))?;
        }
        Ok(())
    }
}

pub type Clients = RwLock<HashMap<ClientId, Arc<Mutex<Client>>>>;
pub type Topics = RwLock<ClientIdsByTopics>;

#[derive(Default, Debug)]
pub struct ClientIdsByTopics(HashMap<Topic, KeyInfo>);

#[derive(Debug)]
pub struct KeyInfo {
    /// List of clients directly subscribed to this topic
    clients: HashMap<ClientId, ClientSubscriptionKey>,
    /// List of clients receiving updates to this topic indirectly from multi-topics
    indirect_clients: HashMap<ClientId, HashSet<Topic>>,
    /// List of subtopics for this multi-topic (empty otherwise)
    subtopics: HashSet<Topic>,
    last_refresh_time: Instant,
}

impl KeyInfo {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
            indirect_clients: HashMap::new(),
            subtopics: HashSet::new(),
            last_refresh_time: Instant::now(),
        }
    }

    pub fn is_expiring(&self, expire_time: Instant) -> bool {
        self.last_refresh_time < expire_time
    }

    pub fn refresh(&mut self, refresh_time: Instant) {
        if self.last_refresh_time < refresh_time {
            self.last_refresh_time = refresh_time
        }
    }
}

#[derive(Clone, Debug)]
pub struct MultitopicUpdate {
    pub added_subtopics: Vec<Topic>,
    pub removed_subtopics: Vec<Topic>,
}

impl MultitopicUpdate {
    pub fn is_empty(&self) -> bool {
        self.added_subtopics.is_empty() && self.removed_subtopics.is_empty()
    }
}

impl ClientIdsByTopics {
    pub fn add_subscription(
        &mut self,
        topic: Topic,
        client_id: ClientId,
        subscription_key: ClientSubscriptionKey,
    ) {
        debug!(
            "[ClientIdsByTopics] Client#{} directly subscribed to {:?}",
            client_id, subscription_key
        );
        self.0
            .entry(topic)
            .or_insert_with(KeyInfo::new)
            .clients
            .insert(client_id, subscription_key);
    }

    pub fn remove_subscription(&mut self, topic: &Topic, client_id: &ClientId) {
        if let Some(key_info) = self.0.get_mut(topic) {
            debug!(
                "[ClientIdsByTopics] Client#{} directly unsubscribed from {:?}",
                client_id, topic
            );
            key_info.clients.remove(client_id);
            if key_info.clients.is_empty() && key_info.indirect_clients.is_empty() {
                self.0.remove(topic);
            }
        }
    }

    pub fn update_multitopic_info(
        &mut self,
        multitopic: Topic,
        subtopics: HashSet<Topic>,
    ) -> MultitopicUpdate {
        let key_info = self.0.entry(multitopic).or_insert_with(KeyInfo::new);

        let added_subtopics = subtopics
            .difference(&key_info.subtopics)
            .cloned()
            .collect::<Vec<_>>();

        let removed_subtopics = key_info
            .subtopics
            .difference(&subtopics)
            .cloned()
            .collect::<Vec<_>>();

        if key_info.subtopics != subtopics {
            key_info.subtopics = subtopics;
        }

        MultitopicUpdate {
            added_subtopics,
            removed_subtopics,
        }
    }

    pub fn update_indirect_subscriptions(
        &mut self,
        multitopic: Topic,
        update: MultitopicUpdate,
        client_id: &ClientId,
    ) {
        for topic in update.added_subtopics {
            debug!(
                "[ClientIdsByTopics] Client#{} indirectly subscribed to {:?}",
                client_id, topic
            );
            self.0
                .entry(topic)
                .or_insert_with(KeyInfo::new)
                .indirect_clients
                .entry(client_id.clone())
                .or_insert_with(HashSet::new)
                .insert(multitopic.clone());
        }

        for topic in update.removed_subtopics {
            if let Some(key_info) = self.0.get_mut(&topic) {
                if let Some(multitopics) = key_info.indirect_clients.get_mut(client_id) {
                    debug!(
                        "[ClientIdsByTopics] Client#{} indirectly unsubscribed from {:?}",
                        client_id, topic
                    );
                    multitopics.remove(&multitopic);
                    if multitopics.is_empty() {
                        key_info.indirect_clients.remove(client_id);
                    }
                }
                if key_info.clients.is_empty() && key_info.indirect_clients.is_empty() {
                    self.0.remove(&topic);
                }
            }
        }
    }

    pub fn remove_indirect_subscriptions(
        &mut self,
        multitopic: &Topic,
        client_id: &ClientId,
    ) -> Vec<Topic> {
        if let Some(multitopic_key_info) = self.0.get(multitopic) {
            let subtopics = multitopic_key_info
                .subtopics
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            for topic in subtopics.iter() {
                if let Some(key_info) = self.0.get_mut(topic) {
                    if let Some(multitopics) = key_info.indirect_clients.get_mut(client_id) {
                        debug!(
                            "[ClientIdsByTopics] Client#{} indirectly unsubscribed from {:?}",
                            client_id, topic
                        );
                        multitopics.remove(multitopic);
                        if multitopics.is_empty() {
                            key_info.indirect_clients.remove(client_id);
                        }
                    }
                    if key_info.clients.is_empty() && key_info.indirect_clients.is_empty() {
                        self.0.remove(topic);
                    }
                }
            }
            subtopics
        } else {
            vec![]
        }
    }

    pub fn get_subscribed_clients(&self, topic: &Topic) -> HashMap<ClientId, Subscribed> {
        self.0
            .get(topic)
            .map(|key_info| {
                let direct_clients =
                    key_info
                        .clients
                        .iter()
                        .map(|(client_id, subscription_key)| {
                            (
                                client_id.clone(),
                                Subscribed::DirectlyWithKey(subscription_key.clone()),
                            )
                        });

                let indirect_clients = key_info
                    .indirect_clients
                    .keys()
                    .map(|client_id| (client_id.clone(), Subscribed::Indirectly));

                direct_clients.chain(indirect_clients).collect()
            })
            .unwrap_or_default()
    }

    pub fn topics_iter(&self) -> impl Iterator<Item = (&Topic, &KeyInfo)> {
        self.0.iter()
    }

    pub fn refresh_topic(&mut self, topic: Topic, refresh_time: Instant) {
        if let Some(key_info) = self.0.get_mut(&topic) {
            key_info.refresh(refresh_time)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, convert::TryFrom};
    use wavesexchange_topic::Topic;

    use super::ClientIdsByTopics;
    use crate::client::{leasing_balance_diff, ClientSubscriptionKey, LeasingBalance};

    #[test]
    fn should_correctly_update_multitopic_info() -> anyhow::Result<()> {
        let multitopic_subscription_key = "topic://state?address__in[]=3PPNhHYkkEy13gRWDCaruQyhNbX2GrjYSyV&key__match_any[]=%s%s%s__staked__3PNVVvuvWqpTnHPgWDTtESJhsBTYdGc4eQ8__*";
        let multitopic = Topic::try_from(multitopic_subscription_key)?;

        let subtopic_subscription_key = "topic://state/3PPNhHYkkEy13gRWDCaruQyhNbX2GrjYSyV/%25s%25s%25s__staked__3PNVVvuvWqpTnHPgWDTtESJhsBTYdGc4eQ8__7KZbJrVopwJhkdwbe1eFDBbex4dkY63MxjTNjqXtrzj1";
        let subtopic = Topic::try_from(subtopic_subscription_key)?;

        let mut target = ClientIdsByTopics::default();
        let mut subtopics = HashSet::new();
        subtopics.insert(subtopic.clone());
        let multitopic_update =
            target.update_multitopic_info(multitopic.clone(), subtopics.clone());
        assert_eq!(multitopic_update.added_subtopics, vec![subtopic.clone()]);

        let multitopic_update =
            target.update_multitopic_info(multitopic.clone(), subtopics.clone());
        assert_eq!(multitopic_update.added_subtopics, vec![]);

        // Check whether adding subscriptions before update multitopic info is valid
        let mut target = ClientIdsByTopics::default();

        let subscribtion_key = ClientSubscriptionKey(multitopic_subscription_key.to_owned());
        target.add_subscription(multitopic.clone(), 1, subscribtion_key);
        let multitopic_update =
            target.update_multitopic_info(multitopic.clone(), subtopics.clone());

        assert_eq!(multitopic_update.added_subtopics, vec![subtopic]);

        Ok(())
    }

    #[test]
    fn leasing_balance_diff_test() {
        // changed both fields
        let lb1 = LeasingBalance {
            address: "address".to_string(),
            balance_in: 7,
            balance_out: 2,
        };
        let lb2 = LeasingBalance {
            address: "address".to_string(),
            balance_in: 5,
            balance_out: 8,
        };
        let diff = leasing_balance_diff(&lb1, &lb2);
        assert_eq!("{\"address\":\"address\",\"in\":5,\"out\":8}", &diff);
        let diff = leasing_balance_diff(&lb2, &lb1);
        assert_eq!("{\"address\":\"address\",\"in\":7,\"out\":2}", &diff);
        // changed only out
        let lb1 = LeasingBalance {
            address: "address".to_string(),
            balance_in: 7,
            balance_out: 2,
        };
        let lb2 = LeasingBalance {
            address: "address".to_string(),
            balance_in: 7,
            balance_out: 8,
        };
        let diff = leasing_balance_diff(&lb1, &lb2);
        assert_eq!("{\"address\":\"address\",\"out\":8}", &diff);
        let diff = leasing_balance_diff(&lb2, &lb1);
        assert_eq!("{\"address\":\"address\",\"out\":2}", &diff);
        // changed only in
        let lb1 = LeasingBalance {
            address: "address".to_string(),
            balance_in: 7,
            balance_out: 2,
        };
        let lb2 = LeasingBalance {
            address: "address".to_string(),
            balance_in: 5,
            balance_out: 2,
        };
        let diff = leasing_balance_diff(&lb1, &lb2);
        assert_eq!("{\"address\":\"address\",\"in\":5}", &diff);
        let diff = leasing_balance_diff(&lb2, &lb1);
        assert_eq!("{\"address\":\"address\",\"in\":7}", &diff);
        // no changes
        let lb1 = LeasingBalance {
            address: "address".to_string(),
            balance_in: 7,
            balance_out: 2,
        };
        let lb2 = LeasingBalance {
            address: "address".to_string(),
            balance_in: 7,
            balance_out: 2,
        };
        let diff = leasing_balance_diff(&lb1, &lb2);
        assert_eq!("{\"address\":\"address\",\"in\":7,\"out\":2}", &diff);
    }
}
