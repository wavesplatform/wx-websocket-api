use futures::stream::{self, StreamExt};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::select;
use wavesexchange_log::{debug, timer, warn};

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

            timer!("Refresh loop iteration", level = debug);

            let topics_to_update = {
                let mut topics_to_update = Vec::new();
                let expiry_time = Instant::now() - self.key_ttl / 2;

                let get_read_guard_time = refresh_time / 16;

                select! {
                    read_guard = self.topics.read() => {
                        for (topic, key_info) in read_guard.topics_iter() {
                            if key_info.is_expiring(expiry_time) {
                                topics_to_update.push(topic.to_owned())
                            }
                        }
                    }
                    _ = tokio::time::sleep(get_read_guard_time) => {
                        warn!("Refresh: cannot acquire read lock in {:?}, skip current refresh iteration", get_read_guard_time);
                        continue;
                    }
                }

                topics_to_update
            };

            if !topics_to_update.is_empty() {
                let updated_topics = self.repo.refresh(topics_to_update).await?;
                timer!("Refresh: store update timestamps", level = debug, verbose);
                debug!("Refresh: storing {} timestamps", updated_topics.len());
                stream::iter(updated_topics)
                    .for_each_concurrent(10, |(topic, update_time)| async move {
                        self.topics.write().await.refresh_topic(topic, update_time)
                    })
                    .await;
            }
        }
    }
}
