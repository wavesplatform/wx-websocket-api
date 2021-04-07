use crate::error::Error;
use crate::messages::OutcomeMessage;
use crate::models::Topic;
use crate::repo::Repo;
use async_trait::async_trait;
use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

pub type ClientId = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientSubscriptionKey(pub String);

#[derive(Debug)]
pub struct Client {
    pub sender: tokio::sync::mpsc::UnboundedSender<warp::ws::Message>,
    pub subscriptions: HashMap<Topic, ClientSubscriptionKey>,
    pub message_counter: i64,
    pub pings: Vec<i64>,
    pub request_id: Option<String>,
    pub new_subscriptions: HashSet<Topic>,
}

impl Client {
    pub fn send(&mut self, message: OutcomeMessage) -> Result<(), Error> {
        self.message_counter += 1;
        self.sender.send(warp::ws::Message::from(message))?;
        Ok(())
    }
}

pub type Clients = Arc<RwLock<HashMap<ClientId, Client>>>;

#[async_trait]
pub trait ClientsTrait {
    async fn clean<R: Repo>(&self, repo: Arc<R>);
}

#[async_trait]
impl ClientsTrait for Clients {
    async fn clean<R: Repo>(&self, repo: Arc<R>) {
        for (_client_id, client) in self.write().await.iter_mut() {
            let fs = client
                .subscriptions
                .iter()
                .map(|(topic, _subscription_string)| {
                    let subscription_key = topic.to_string();
                    repo.unsubscribe(subscription_key)
                });

            let _ = try_join_all(fs).await;
        }
    }
}
