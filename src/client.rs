use crate::error::Error;
use crate::messages::OutcomeMessage;
use crate::repo::Repo;
use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use warp::ws::Message;
use wavesexchange_log::{error, info};
use wavesexchange_topic::Topic;

pub type ClientId = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientSubscriptionKey(pub String);

#[derive(Debug)]
pub struct Client {
    sender: tokio::sync::mpsc::UnboundedSender<Message>,
    subscriptions: HashMap<Topic, ClientSubscriptionKey>,
    message_counter: i64,
    pings: Vec<i64>,
    request_id: Option<String>,
    new_subscriptions: HashSet<Topic>,
    leasing_balance_last_values: HashMap<Topic, LeasingBalance>,
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
    } else {
        if old_value.balance_out == new_value.balance_out {
            let v = serde_json::json!({"address": new_value.address, "in": new_value.balance_in});
            serde_json::to_string(&v).unwrap()
        } else {
            serde_json::to_string(&new_value).unwrap()
        }
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
            new_subscriptions: HashSet::new(),
            message_counter: 1,
            pings: vec![],
            leasing_balance_last_values: HashMap::new(),
        }
    }

    pub fn get_request_id(&self) -> &Option<String> {
        &self.request_id
    }

    pub fn contains_subscription(&self, topic: &Topic) -> bool {
        self.subscriptions.contains_key(topic)
    }

    pub fn add_subscription(
        &mut self,
        topic: Topic,
        client_subscription_key: ClientSubscriptionKey,
    ) {
        self.subscriptions.insert(topic, client_subscription_key);
    }

    pub fn add_new_subscription(&mut self, topic: Topic) {
        self.new_subscriptions.insert(topic);
    }

    pub fn remove_subscription(&mut self, topic: &Topic) {
        self.subscriptions.remove(topic);
        self.new_subscriptions.remove(topic);
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
            info!("got invalid pong message");
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
        if let Some(subscription_key) = self.subscriptions.get(topic) {
            if let Ok(Some(lb)) = serde_json::from_str(&value) {
                self.leasing_balance_last_values.insert(topic.clone(), lb);
            }
            let message = OutcomeMessage::Subscribed {
                message_number: self.message_counter,
                topic: subscription_key.clone(),
                value,
            };
            self.send(message)?;
        }
        Ok(())
    }

    pub fn send_update(&mut self, topic: &Topic, mut value: String) -> Result<(), Error> {
        if let Some(client_subscription_key) = self.subscriptions.get(topic) {
            if let Ok(Some(lb)) = serde_json::from_str(&value) {
                if let Some(old_value) = self.leasing_balance_last_values.get(topic) {
                    value = leasing_balance_diff(old_value, &lb);
                }
                self.leasing_balance_last_values.insert(topic.clone(), lb);
            }
            let message = if self.new_subscriptions.remove(topic) {
                OutcomeMessage::Subscribed {
                    message_number: self.message_counter,
                    topic: client_subscription_key.clone(),
                    value,
                }
            } else {
                OutcomeMessage::Update {
                    message_number: self.message_counter,
                    topic: client_subscription_key.clone(),
                    value,
                }
            };
            self.send(message)?;
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

    pub fn subscriptions_iter(
        &self,
    ) -> std::collections::hash_map::Iter<'_, Topic, ClientSubscriptionKey> {
        self.subscriptions.iter()
    }

    fn send(&mut self, message: OutcomeMessage) -> Result<(), Error> {
        self.message_counter += 1;
        self.sender.send(Message::from(message))?;
        Ok(())
    }
}

pub type Clients = RwLock<HashMap<ClientId, Arc<Mutex<Client>>>>;
pub type Topics = RwLock<ClientIdsByTopics>;

#[derive(Default, Debug)]
pub struct ClientIdsByTopics(HashMap<Topic, HashSet<ClientId>>);

impl ClientIdsByTopics {
    pub fn add_subscription(&mut self, topic: Topic, client_id: ClientId) {
        if let Some(clients) = self.0.get_mut(&topic) {
            clients.insert(client_id);
        } else {
            let mut v = HashSet::new();
            v.insert(client_id);
            self.0.insert(topic, v);
        }
    }

    pub fn remove_subscription(&mut self, topic: &Topic, client_id: &ClientId) {
        if let Some(client_ids) = self.0.get_mut(&topic) {
            client_ids.remove(client_id);
            if client_ids.is_empty() {
                self.0.remove(&topic);
            }
        };
    }

    pub fn get_client_ids(&self, topic: &Topic) -> Option<&HashSet<ClientId>> {
        self.0.get(topic)
    }
}

#[async_trait]
pub trait ClientsTrait {
    async fn clean<R: Repo>(&self, repo: &Arc<R>);
}

#[async_trait]
impl ClientsTrait for Clients {
    async fn clean<R: Repo>(&self, repo: &Arc<R>) {
        if let Err(error) = stream::iter(self.write().await.iter_mut())
            .map(|(_client_id, client)| Ok::<_, Error>((client, &repo)))
            .try_for_each_concurrent(10, |(client, repo)| async move {
                stream::iter(client.lock().await.subscriptions_iter())
                    .map(|(topic, _subscription_key)| Ok::<_, Error>((topic, repo)))
                    .try_for_each_concurrent(10, |(topic, repo)| async move {
                        let subscription_key = String::from(topic.to_owned());
                        repo.unsubscribe(subscription_key).await?;
                        Ok(())
                    })
                    .await?;
                Ok(())
            })
            .await
        {
            error!("error on cleaning clients: {:?}", error)
        };
    }
}
