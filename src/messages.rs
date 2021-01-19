use crate::models::ConfigFile;
use crate::{error::Error, models::ConfigParameters};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use url::Url;
use warp::ws;

type ErrorCode = u16;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IncomeMessage {
    Pong(PongMessage),
    Subscribe { topic: String },
    Unsubscribe { topic: String },
}

impl TryFrom<ws::Message> for IncomeMessage {
    type Error = crate::error::Error;

    fn try_from(value: ws::Message) -> Result<Self, Self::Error> {
        serde_json::from_slice(value.into_bytes().as_slice()).map_err(|e| match e.classify() {
            serde_json::error::Category::Data => Error::UnknownIncomeMessage(e.to_string()),
            _ => Error::SerdeJsonError(e),
        })
    }
}

impl From<IncomeMessage> for ws::Message {
    fn from(m: IncomeMessage) -> Self {
        ws::Message::text(serde_json::to_string(&m).unwrap())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PongMessage {
    pub message_number: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Topic {
    Config(ConfigParameters),
}

impl TryFrom<&str> for Topic {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(s)?;

        match url.scheme() {
            "topic" => match url.host_str() {
                Some("config") => {
                    let config_file = ConfigFile::try_from(url)?;
                    Ok(Topic::Config(ConfigParameters { file: config_file }))
                }
                _ => Err(Error::InvalidTopic(s.to_owned())),
            },
            _ => Err(Error::InvalidTopic(s.to_owned())),
        }
    }
}

impl ToString for Topic {
    fn to_string(&self) -> String {
        let mut url = Url::parse("topic://").unwrap();
        match self {
            Topic::Config(cf) => {
                url.set_host(Some("config")).unwrap();
                url.set_path(&cf.file.path);
                url.as_str().to_owned()
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutcomeMessage {
    Ping {
        message_number: i64,
    },
    Update {
        message_number: i64,
        topic: String,
        value: String,
    },
    Subscribed {
        message_number: i64,
        topic: String,
    },
    Unsubscribed {
        message_number: i64,
        topic: String,
    },
    Error {
        message_number: i64,
        code: ErrorCode,
        message: String,
        details: HashMap<String, String>,
    },
}

impl From<OutcomeMessage> for ws::Message {
    fn from(om: OutcomeMessage) -> Self {
        let json = serde_json::to_string(&om).unwrap();
        ws::Message::text(&json)
    }
}
