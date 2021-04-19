use crate::error::Error;
use crate::messages::OutcomeMessage;
use crate::models::Topic;
use crate::repo::Repo;
use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use wavesexchange_log::error;

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

pub type Clients = Arc<RwLock<HashMap<ClientId, Arc<Mutex<Client>>>>>;
pub type Topics = Arc<RwLock<HashMap<Topic, HashSet<ClientId>>>>;

#[async_trait]
pub trait ClientsTrait {
    async fn clean<R: Repo>(&self, repo: Arc<R>);
}

#[async_trait]
impl ClientsTrait for Clients {
    async fn clean<R: Repo>(&self, repo: Arc<R>) {
        if let Err(error) = stream::iter(self.write().await.iter_mut())
            .map(|(_client_id, client)| Ok((client, repo.clone())))
            .try_for_each_concurrent(10, |(client, repo)| async move {
                stream::iter(client.lock().await.subscriptions.iter())
                    .map(|(topic, _subscription_string)| Ok::<_, Error>((topic, &repo)))
                    .try_for_each(|(topic, repo)| async move {
                        let subscription_key = topic.to_string();
                        repo.unsubscribe(subscription_key).await?;
                        Ok(())
                    })
                    .await
            })
            .await
        {
            error!("error on cleaning clients: {:?}", error)
        };
    }
}
