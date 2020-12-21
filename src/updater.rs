use crate::error::Error;
use crate::messages::PreOutcomeMessage;
use crate::models::ConfigFile;
use crate::{Connections, Subscribtions};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateResource {
    Config(ConfigFile),
}

impl TryFrom<&str> for UpdateResource {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = s.splitn(2, ":").collect();
        if parts.len() < 2 {
            return Err(Error::InvalidUpdateResource(s.to_owned()));
        }

        let resource = parts.first().unwrap().to_owned();
        let metadata = parts.last().unwrap().to_owned();

        match resource {
            "config" => {
                let config_file = ConfigFile::try_from(metadata)?;
                Ok(UpdateResource::Config(config_file))
            }
            _ => Err(Error::InvalidUpdateResource(s.to_owned())),
        }
    }
}

impl From<&UpdateResource> for String {
    fn from(um: &UpdateResource) -> Self {
        match um {
            UpdateResource::Config(cf) => format!("config:{}", String::from(cf)),
        }
    }
}

// NB: redis server have to be configured to publish keyspace notifications:
// https://redis.io/topics/notifications
pub async fn run(
    connections: Connections,
    subscriptions: Subscribtions,
    redis_client: redis::Client,
) -> Result<(), Error> {
    let mut conn = redis_client.get_connection()?;
    let mut pubsub = conn.as_pubsub();

    pubsub.psubscribe("__keyevent*__:*")?;

    while let Ok(msg) = pubsub.get_message() {
        let update: String = msg.get_payload::<String>()?;

        if let Ok(update) = UpdateResource::try_from(update.as_ref()) {
            let subscription_key = String::from(&update);
            let message = PreOutcomeMessage::Update(update);

            for (&connection_id, tx) in connections.read().await.iter() {
                if let Some(connection_subscriptions) =
                    subscriptions.read().await.get(&connection_id)
                {
                    if connection_subscriptions.contains(&subscription_key) {
                        if let Err(_disconnected) = tx.send(message.clone()) {}
                    }
                }
            }
        }
    }

    Ok(())
}
