use futures::stream::{self, StreamExt};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::client::Topics;
use crate::error::Error;
use crate::repo::Repo;

pub struct KeysRefresher<R: Repo> {
    key_ttl: Duration,
    repo: Arc<R>,
    topics: Arc<Topics>,
}

impl<R: Repo> KeysRefresher<R> {
    pub fn new(repo: Arc<R>, key_ttl: Duration, topics: Arc<Topics>) -> Self {
        Self {
            key_ttl,
            repo,
            topics,
        }
    }

    pub async fn run(&self) -> Result<(), Error> {
        let refresh_time = self.key_ttl / 4;
        loop {
            tokio::time::sleep(refresh_time).await;
            let mut topics_to_update = vec![];

            let expiry_time = Instant::now() - self.key_ttl / 2;
            for (topic, key_info) in self.topics.read().await.topics_iter() {
                if key_info.is_expiring(expiry_time) {
                    topics_to_update.push(topic.to_owned())
                }
            }

            if !topics_to_update.is_empty() {
                let updated_topics = self.repo.refresh(topics_to_update).await?;
                stream::iter(updated_topics)
                    .for_each_concurrent(10, |(topic, update_time)| async move {
                        self.topics.write().await.refresh_topic(topic, update_time)
                    })
                    .await;
            }
        }
    }
}
