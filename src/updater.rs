use crate::error::Error;
use crate::models::ConfigFile;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use wavesexchange_log::error;

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

impl ToString for UpdateResource {
    fn to_string(&self) -> String {
        self.into()
    }
}

// NB: redis server have to be configured to publish keyspace notifications:
// https://redis.io/topics/notifications
pub fn run(
    redis_client: redis::Client,
    updates_sender: tokio::sync::mpsc::UnboundedSender<UpdateResource>,
) -> Result<(), Error> {
    let mut conn = redis_client.get_connection()?;
    let mut pubsub = conn.as_pubsub();

    pubsub.psubscribe("__keyevent*__:*")?;

    while let Ok(msg) = pubsub.get_message() {
        let update: String = msg.get_payload::<String>()?;

        if let Ok(update_resource) = UpdateResource::try_from(update.as_ref()) {
            if let Err(err) = updates_sender.send(update_resource) {
                error!("error occured while sending resource update: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
