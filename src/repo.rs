use crate::client::ClientId;
use crate::error::Error;
use async_trait::async_trait;
use bb8_redis::{bb8, redis::AsyncCommands, RedisConnectionManager};

const CONNECTION_ID_KEY: &str = "NEXT_CONNECTION_ID";

pub struct Config {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub subscriptions_key: String,
}

#[async_trait]
pub trait Repo: Send + Sync {
    async fn get_connection_id(&self) -> Result<ClientId, Error>;

    // HEXISTS REDIS_SUBSCRIPTIONS_KEY <key>?
    // Y: HINCRBY REDIS_SUBSCRIPTIONS_KEY <key> 1
    // N: HSET REDIS_SUBSCRIPTIONS_KEY <key> 1
    async fn subscribe<S: Into<String> + Send + Sync>(&self, key: S) -> Result<(), Error>;

    // HINCRBY REDIS_SUBSCRIPTIONS_KEY <key> -1
    async fn unsubscribe<S: Into<String> + Send + Sync>(&self, key: S) -> Result<(), Error>;

    // GET <key>
    async fn get_by_key(&self, key: &str) -> Result<Option<String>, Error>;
}

pub struct RepoImpl {
    pool: bb8::Pool<RedisConnectionManager>,
    subscriptions_key: String,
}

impl RepoImpl {
    pub fn new(
        pool: bb8::Pool<RedisConnectionManager>,
        subscriptions_key: impl AsRef<str>,
    ) -> RepoImpl {
        RepoImpl {
            pool,
            subscriptions_key: subscriptions_key.as_ref().to_owned(),
        }
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
        let key = key.into();
        let mut con = self.pool.get().await?;
        if con.hexists(&self.subscriptions_key, key.clone()).await? {
            con.hincr(&self.subscriptions_key, key, 1).await?;
        } else {
            con.hset(&self.subscriptions_key, key, 1).await?;
        }

        Ok(())
    }

    async fn unsubscribe<S: Into<String> + Send + Sync>(&self, key: S) -> Result<(), Error> {
        let key = key.into();
        let mut con = self.pool.get().await?;
        con.hincr(&self.subscriptions_key, key, -1).await?;
        Ok(())
    }

    async fn get_by_key(&self, key: &str) -> Result<Option<String>, Error> {
        let mut con = self.pool.get().await?;
        Ok(con.get(key).await?)
    }
}
