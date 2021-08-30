use crate::error::Error;
use crate::messages::OutcomeMessage;
use crate::metrics::MESSAGES;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock};
use warp::ws::Message;
use wavesexchange_log::warn;
use wavesexchange_topic::Topic;

pub type ClientId = usize;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClientSubscriptionKey(pub String);

#[derive(Debug)]
pub struct Client {
    sender: tokio::sync::mpsc::UnboundedSender<Message>,
    subscriptions: HashMap<Topic, ClientSubscriptionData>,
    message_counter: i64,
    pings: Vec<i64>,
    request_id: Option<String>,
}

#[derive(Default, Debug)]
struct ClientSubscriptionData {
    subscription_key: ClientSubscriptionKey,
    indirect_keys: HashSet<Topic>,
    is_new: bool,
    is_direct: bool,
    is_indirect: bool,
    leasing_balance_last_value: Option<LeasingBalance>,
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
        sender: tokio::sync::mpsc::UnboundedSender<Message>,
        request_id: Option<String>,
    ) -> Self {
        Client {
            sender,
            request_id,
            subscriptions: HashMap::new(),
            message_counter: 1,
            pings: vec![],
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
        let subscription_data = self.subscriptions.entry(topic).or_default();
        subscription_data.subscription_key = client_subscription_key;
        subscription_data.is_direct = true;
    }

    pub fn add_indirect_subscription(&mut self, topic: Topic, parent_multitopic: Topic) {
        let subscription_data = self.subscriptions.entry(topic).or_default();
        subscription_data.indirect_keys.insert(parent_multitopic);
        subscription_data.is_indirect = true;
    }

    pub fn mark_subscription_as_new(&mut self, topic: Topic) {
        self.subscriptions
            .entry(topic)
            .and_modify(|subscription_data| subscription_data.is_new = true);
    }

    pub fn remove_direct_subscription(&mut self, topic: &Topic) {
        if let Some(subscription_data) = self.subscriptions.get_mut(topic) {
            subscription_data.is_direct = false;
            let still_subscribed = subscription_data.is_direct | subscription_data.is_indirect;
            if !still_subscribed {
                self.subscriptions.remove(topic);
            }
        }
    }

    pub fn remove_indirect_subscription(&mut self, topic: &Topic, parent_multitopic: &Topic) {
        if let Some(subscription_data) = self.subscriptions.get_mut(topic) {
            subscription_data.indirect_keys.remove(parent_multitopic);
            if subscription_data.indirect_keys.is_empty() {
                subscription_data.is_indirect = false;
            }
            let still_subscribed = subscription_data.is_direct | subscription_data.is_indirect;
            if !still_subscribed {
                self.subscriptions.remove(topic);
            }
        }
    }

    pub fn handle_pong(&mut self, message_number: i64) -> Result<(), Error> {
        if self.pings.contains(&message_number) {
            self.pings = self
                .pings
                .iter()
                .filter(|&&x| x > message_number)
                .cloned()
                .collect();
            Ok(())
        } else {
            // client sent invalid pong message
            warn!("got invalid pong message");
            Err(Error::InvalidPongMessage)
        }
    }

    pub fn pings_len(&self) -> usize {
        self.pings.len()
    }

    pub fn send_ping(&mut self) -> Result<(), Error> {
        self.pings.push(self.message_counter);
        let message_number = self.message_counter;
        let message = OutcomeMessage::Ping { message_number };
        self.send(message)
    }

    pub fn send_subscribed(&mut self, topic: &Topic, value: String) -> Result<(), Error> {
        if let Some(subscription_data) = self.subscriptions.get_mut(topic) {
            if let Ok(Some(lb)) = serde_json::from_str(&value) {
                subscription_data.leasing_balance_last_value = lb;
            }
            let message = OutcomeMessage::Subscribed {
                message_number: self.message_counter,
                topic: subscription_data.subscription_key.clone(),
                value,
            };
            self.send(message)?;
            MESSAGES.inc();
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
            let message = if subscription_data.is_new {
                subscription_data.is_new = false;
                OutcomeMessage::Subscribed {
                    message_number: self.message_counter,
                    topic: subscription_data.subscription_key.clone(),
                    value,
                }
            } else {
                OutcomeMessage::Update {
                    message_number: self.message_counter,
                    topic: subscription_data.subscription_key.clone(),
                    value,
                }
            };
            self.send(message)?;
            MESSAGES.inc();
        }
        Ok(())
    }

    pub fn send_unsubscribed(
        &mut self,
        subscription_key: ClientSubscriptionKey,
    ) -> Result<(), Error> {
        let message = OutcomeMessage::Unsubscribed {
            message_number: self.message_counter,
            topic: subscription_key,
        };
        self.send(message)
    }

    pub fn send_error(
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

    pub fn messages_count(&self) -> i64 {
        self.message_counter - 1
    }

    pub fn subscription_topics_iter(&self) -> impl Iterator<Item = &Topic> {
        self.subscriptions.iter().map(|(topic, _)| topic)
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
    clients: HashSet<ClientId>,
    /// List of clients receiving updates to this topic indirectly from multi-topics
    indirect_clients: HashMap<ClientId, HashSet<Topic>>,
    /// List of subtopics for this multi-topic (empty otherwise)
    subtopics: HashSet<Topic>,
    last_refresh_time: Instant,
}

impl KeyInfo {
    pub fn new() -> Self {
        Self {
            clients: HashSet::new(),
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

#[derive(Clone)]
pub struct MultitopicUpdate {
    pub added_subtopics: Vec<Topic>,
    pub removed_subtopics: Vec<Topic>,
}

impl ClientIdsByTopics {
    pub fn add_subscription(&mut self, topic: Topic, client_id: ClientId) {
        self.0
            .entry(topic)
            .or_insert_with(KeyInfo::new)
            .clients
            .insert(client_id);
    }

    pub fn remove_subscription(&mut self, topic: &Topic, client_id: &ClientId) {
        if let Some(key_info) = self.0.get_mut(topic) {
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

    pub fn get_client_ids(&self, topic: &Topic) -> Option<HashSet<ClientId>> {
        self.0.get(topic).map(|key_info| {
            if key_info.indirect_clients.is_empty() {
                key_info.clients.clone()
            } else {
                let total_clients = key_info.clients.len() + key_info.indirect_clients.len();
                let mut res = HashSet::with_capacity(total_clients);
                res.extend(key_info.clients.iter());
                res.extend(key_info.indirect_clients.keys());
                res
            }
        })
    }

    pub fn topics_iter(&self) -> std::collections::hash_map::Iter<'_, Topic, KeyInfo> {
        self.0.iter()
    }

    pub fn refresh_topic(&mut self, topic: Topic, refresh_time: Instant) {
        if let Some(key_info) = self.0.get_mut(&topic) {
            key_info.refresh(refresh_time)
        }
    }
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
