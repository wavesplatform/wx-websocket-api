use crate::client::ClientId;
use crate::client::Topics;
use crate::error::Error;
use crate::shard::Sharded;
use async_trait::async_trait;
use bb8_redis::{bb8, redis::AsyncCommands, RedisConnectionManager};
use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use wavesexchange_topic::Topic;

const CONNECTION_ID_KEY: &str = "NEXT_CONNECTION_ID";

pub struct Config {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub ttl: Duration,
}

#[async_trait]
pub trait Repo: Send + Sync {
    async fn get_connection_id(&self) -> Result<ClientId, Error>;

    async fn subscribe<S: Into<String> + Send + Sync>(&self, key: S) -> Result<(), Error>;

    async fn get_by_key(&self, key: &str) -> Result<Option<String>, Error>;

    async fn refresh(&self, topics: Vec<Topic>) -> Result<HashMap<Topic, Instant>, Error>;
}

pub struct RepoImpl {
    pool: bb8::Pool<RedisConnectionManager>,
    ttl: Duration,
}

impl RepoImpl {
    pub fn new(pool: bb8::Pool<RedisConnectionManager>, ttl: Duration) -> RepoImpl {
        RepoImpl { pool, ttl }
    }
}

#[async_trait]
impl Repo for RepoImpl {
    async fn get_connection_id(&self) -> Result<usize, Error> {
        let mut con = self.pool.get().await?;
        let next_user_id: usize = con.incr(CONNECTION_ID_KEY, 1).await?;
        return Ok(next_user_id);
    }

    async fn subscribe<S: Into<String> + Send + Sync>(&self, key: S) -> Result<(), Error> {
        let key = "sub:".to_string() + &key.into();
        let mut con = self.pool.get().await?;
        let ttl = self.ttl.as_secs() as usize;
        if con.exists(key.clone()).await? {
            con.expire(key, ttl).await?;
        } else {
            con.set_ex(key, 0, ttl).await?;
        }

        Ok(())
    }

    async fn get_by_key(&self, key: &str) -> Result<Option<String>, Error> {
        let mut con = self.pool.get().await?;
        Ok(con.get(key).await?)
    }

    async fn refresh(&self, topics: Vec<Topic>) -> Result<HashMap<Topic, Instant>, Error> {
        let mut con = self.pool.get().await?;
        let ttl = self.ttl.as_secs() as usize;
        let mut result = HashMap::new();
        for topic in topics {
            let key = "sub:".to_string() + &String::from(topic.clone());
            let update_time = Instant::now();
            con.expire(key, ttl).await?;
            result.insert(topic, update_time);
        }
        Ok(result)
    }
}

pub struct Refresher<R: Repo> {
    ttl: Duration,
    repo: Arc<R>,
    topics: Arc<Sharded<Topics>>,
}

impl<R: Repo> Refresher<R> {
    pub fn new(repo: Arc<R>, ttl: Duration, topics: Arc<Sharded<Topics>>) -> Self {
        Self { ttl, repo, topics }
    }

    pub async fn run(&self) -> Result<(), Error> {
        let refresh_time = self.ttl / 4;
        loop {
            tokio::time::sleep(refresh_time).await;
            let mut topics_to_update = vec![];
            for clients_topics in &*self.topics {
                let dying_time = Instant::now() - self.ttl / 2;
                for (topic, key_info) in clients_topics.read().await.topics_iter() {
                    if key_info.dying_soon(dying_time) {
                        topics_to_update.push(topic.to_owned())
                    }
                }
            }
            if !topics_to_update.is_empty() {
                let updated_topics = self.repo.refresh(topics_to_update).await?;
                stream::iter(updated_topics)
                    .for_each_concurrent(10, |(topic, update_time)| async move {
                        self.topics
                            .get(&topic)
                            .write()
                            .await
                            .refresh_topic(topic, update_time)
                    })
                    .await;
            }
        }
    }
}
