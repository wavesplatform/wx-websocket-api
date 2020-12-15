use crate::{error::Error, ConnectionId};
use async_trait::async_trait;
use bb8_redis::{bb8, redis::AsyncCommands, RedisConnectionManager};

const CONNECTION_ID_KEY: &str = "NEXT_CONNECTION_ID";

pub struct Config {
    pub host: String,
    pub subscriptions_counter_key: String,
}

#[async_trait]
pub trait Repo {
    async fn get_connection_id(&self) -> Result<ConnectionId, Error>;

    // HEXISTS REDIS_SUBSCRIPTIONS_COUNTER_KEY <key>?
    // Y: HINCRBY REDIS_SUBSCRIPTIONS_COUNTER_KEY <key> 1
    // N: HSET REDIS_SUBSCRIPTIONS_COUNTER_KEY <key> 1
    // PSUBSCRIBE __keyspace*__:<key>
    async fn subscribe<S: Into<String> + Send + Sync>(&self, key: S) -> Result<(), Error>;

    // HINCRBY REDIS_SUBSCRIPTIONS_COUNTER_KEY <key> -1
    async fn unsubscribe(&self, key: &str) -> Result<(), Error>;

    // GET <key>
    async fn get_by_key(&self, key: &str) -> Result<String, Error>;
}

pub struct RepoImpl {
    pool: bb8::Pool<RedisConnectionManager>,
    subscriptions_counter_key: String,
}

impl RepoImpl {
    pub fn new(
        pool: bb8::Pool<RedisConnectionManager>,
        subscriptions_counter_key: impl AsRef<str>,
    ) -> RepoImpl {
        RepoImpl {
            pool,
            subscriptions_counter_key: subscriptions_counter_key.as_ref().to_owned(),
        }
    }
}

#[async_trait]
impl Repo for RepoImpl {
    async fn get_connection_id(&self) -> Result<usize, Error> {
        let mut con = self.pool.get().await.map_err(|e| Error::from(e))?;
        let next_user_id = con.incr(CONNECTION_ID_KEY, 1).await.map(|v: usize| v)?;
        return Ok(next_user_id);
    }

    async fn subscribe<S: Into<String> + Send + Sync>(&self, key: S) -> Result<(), Error> {
        let key = key.into();

        let mut con = self.pool.get().await.map_err(|e| Error::from(e))?;

        let exists = con
            .hexists(&self.subscriptions_counter_key, key.clone())
            .await?;

        if exists {
            con.hincr(&self.subscriptions_counter_key, key, 1)
                .await
                .map_err(|e| Error::from(e))?;
        } else {
            con.hset(&self.subscriptions_counter_key, key, 1)
                .await
                .map_err(|e| Error::from(e))?;
        }

        Ok(())
    }

    async fn unsubscribe(&self, key: &str) -> Result<(), Error> {
        let mut con = self.pool.get().await.map_err(|e| Error::from(e))?;

        con.hincr(&self.subscriptions_counter_key, key, -1)
            .await
            .map_err(|e| Error::from(e))
    }

    async fn get_by_key(&self, key: &str) -> Result<String, Error> {
        let mut con = self.pool.get().await.map_err(|e| Error::from(e))?;

        con.get(key).await.map_err(|e| Error::from(e))
    }
}
