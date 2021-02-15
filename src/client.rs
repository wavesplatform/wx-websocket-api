use crate::error::Error;
use crate::messages::OutcomeMessage;
use crate::repo::Repo;
use async_trait::async_trait;
use futures::future::try_join_all;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

pub type ClientId = usize;

#[derive(Debug)]
pub struct Client {
    pub sender: tokio::sync::mpsc::UnboundedSender<Result<warp::ws::Message, warp::Error>>,
    pub subscriptions: HashSet<String>,
    pub message_counter: i64,
    pub pings: Vec<i64>,
    pub new_subscriptions: HashSet<String>,
}

impl Client {
    pub fn send(&mut self, message: OutcomeMessage) -> Result<(), Error> {
        self.message_counter += 1;
        self.sender.send(Ok(warp::ws::Message::from(message)))?;
        Ok(())
    }
}

pub type Clients = Arc<RwLock<HashMap<ClientId, Client>>>;

#[async_trait]
pub trait ClientsTrait {
    async fn clean<R: Repo + Send + Sync>(&self, repo: Arc<R>);
}

#[async_trait]
impl ClientsTrait for Clients {
    async fn clean<R: Repo + Send + Sync>(&self, repo: Arc<R>) {
        for (_client_id, client) in self.write().await.iter_mut() {
            let fs = client
                .subscriptions
                .iter()
                .map(|subscription_key| repo.unsubscribe(subscription_key));

            let _ = try_join_all(fs).await;
        }
    }
}
