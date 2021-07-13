use crate::client::ClientId;
use crate::error::Error;
use async_trait::async_trait;
use bb8_redis::{bb8, redis::AsyncCommands, RedisConnectionManager};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use wavesexchange_topic::Topic;

const CONNECTION_ID_KEY: &str = "NEXT_CONNECTION_ID";

pub struct Config {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub key_ttl: Duration,
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
    key_ttl: Duration,
}

impl RepoImpl {
    pub fn new(pool: bb8::Pool<RedisConnectionManager>, key_ttl: Duration) -> RepoImpl {
        RepoImpl { pool, key_ttl }
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
        let key_ttl = self.key_ttl.as_secs() as usize;
        if con.exists(key.clone()).await? {
            con.expire(key, key_ttl).await?;
        } else {
            con.set_ex(key, 0, key_ttl).await?;
        }

        Ok(())
    }

    async fn get_by_key(&self, key: &str) -> Result<Option<String>, Error> {
        let mut con = self.pool.get().await?;
        Ok(con.get(key).await?)
    }

    async fn refresh(&self, topics: Vec<Topic>) -> Result<HashMap<Topic, Instant>, Error> {
        let mut con = self.pool.get().await?;
        let key_ttl = self.key_ttl.as_secs() as usize;
        let mut result = HashMap::new();
        for topic in topics {
            let key = "sub:".to_string() + &String::from(topic.clone());
            let update_time = Instant::now();
            con.expire(key, key_ttl).await?;
            result.insert(topic, update_time);
        }
        Ok(result)
    }
}
