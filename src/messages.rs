use crate::client::ClientSubscriptionKey;
use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use warp::ws;

type ErrorCode = u16;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IncomeMessage {
    Pong(PongMessage),
    Subscribe { topic: ClientSubscriptionKey },
    Unsubscribe { topic: ClientSubscriptionKey },
}

impl TryFrom<&ws::Message> for IncomeMessage {
    type Error = crate::error::Error;

    fn try_from(value: &ws::Message) -> Result<Self, Self::Error> {
        serde_json::from_slice(value.as_bytes()).map_err(|e| match e.classify() {
            serde_json::error::Category::Data => Error::UnknownIncomeMessage(e.to_string()),
            _ => Error::SerdeJsonError(e),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PongMessage {
    pub message_number: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutcomeMessage {
    Ping {
        message_number: i64,
    },
    Update {
        message_number: i64,
        topic: ClientSubscriptionKey,
        value: String,
    },
    Subscribed {
        message_number: i64,
        topic: ClientSubscriptionKey,
        value: String,
    },
    Unsubscribed {
        message_number: i64,
        topic: ClientSubscriptionKey,
    },
    Error {
        message_number: i64,
        code: ErrorCode,
        message: String,
        details: Option<HashMap<String, String>>,
    },
}

impl From<OutcomeMessage> for ws::Message {
    fn from(om: OutcomeMessage) -> Self {
        let json = serde_json::to_string(&om).unwrap();
        ws::Message::text(&json)
    }
}
