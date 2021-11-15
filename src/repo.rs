use crate::client::ClientId;
use crate::error::Error;
use async_trait::async_trait;
use bb8_redis::{bb8, redis::AsyncCommands, RedisConnectionManager};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use wavesexchange_log::{debug, timer};
use wavesexchange_topic::Topic;

use self::counter::VersionCounter;

const CONNECTION_ID_KEY: &str = "NEXT_CONNECTION_ID";

pub struct Config {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub key_ttl: Duration,
    pub max_pool_size: u32,
}

#[async_trait]
pub trait Repo: Send + Sync {
    async fn get_connection_id(&self) -> Result<ClientId, Error>;

    async fn subscribe<S: Into<String> + Send + Sync>(&self, key: S) -> Result<(), Error>;

    async fn get_by_key(&self, key: &str) -> Result<Option<String>, Error>;

    async fn get_by_keys(&self, keys: Vec<String>) -> Result<Vec<Option<String>>, Error>;

    async fn refresh(&self, topics: Vec<Topic>) -> Result<HashMap<Topic, Instant>, Error>;
}

pub struct RepoImpl {
    pool: bb8::Pool<RedisConnectionManager>,
    key_ttl: Duration,
    state_version: VersionCounter,
}

impl RepoImpl {
    pub fn new(pool: bb8::Pool<RedisConnectionManager>, key_ttl: Duration) -> RepoImpl {
        RepoImpl {
            pool,
            key_ttl,
            state_version: Default::default(),
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
        let key = "sub:".to_string() + &key.into();
        let mut con = self.pool.get().await?;
        let key_ttl = self.key_ttl.as_secs() as usize;
        let state_version = self.state_version.next();
        con.set_ex(key, state_version, key_ttl).await?;

        Ok(())
    }

    async fn get_by_key(&self, key: &str) -> Result<Option<String>, Error> {
        let mut con = self.pool.get().await?;
        Ok(con.get(key).await?)
    }

    async fn get_by_keys(&self, keys: Vec<String>) -> Result<Vec<Option<String>>, Error> {
        let mut con = self.pool.get().await?;
        // Need to explicitly handle case with keys.len() == 1
        // due to the issue https://github.com/mitsuhiko/redis-rs/issues/336
        match keys.len() {
            0 => Ok(vec![]),
            1 => Ok(vec![con.get(keys).await?]),
            _ => Ok(con.get(keys).await?),
        }
    }

    async fn refresh(&self, topics: Vec<Topic>) -> Result<HashMap<Topic, Instant>, Error> {
        timer!("Refresh: update TTLs in Redis", level = debug, verbose);
        debug!("Refresh: updating TTLs of {} keys in Redis", topics.len());
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

mod counter {
    use std::sync::atomic::{AtomicI32, Ordering};

    #[derive(Default, Debug)]
    pub(super) struct VersionCounter(AtomicI32);

    impl VersionCounter {
        pub(super) fn next(&self) -> i32 {
            let Self(counter) = self;
            counter.fetch_add(1, Ordering::SeqCst)
        }
    }
}
