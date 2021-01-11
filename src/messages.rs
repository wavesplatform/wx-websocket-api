use crate::models::ConfigParameters;
use crate::updater::UpdateResource;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use warp::ws;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IncomeMessage {
    Pong,
    Subscribe(SubscribeMessage),
    Unsubscribe(UnsubscribeMessage),
}

impl TryFrom<ws::Message> for IncomeMessage {
    type Error = crate::error::Error;

    fn try_from(value: ws::Message) -> Result<Self, Self::Error> {
        serde_json::from_slice(value.into_bytes().as_slice()).map_err(|e| e.into())
    }
}

impl From<IncomeMessage> for ws::Message {
    fn from(m: IncomeMessage) -> Self {
        ws::Message::text(serde_json::to_string(&m).unwrap())
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "resource", rename_all = "snake_case")]
pub enum SubscribeMessage {
    Config { parameters: ConfigParameters },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "resource", rename_all = "snake_case")]
pub enum UnsubscribeMessage {
    Config { parameters: ConfigParameters },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutcomeMessage {
    Ping,
    Update {
        resource: UpdateResource,
        value: String,
    },
    SubscribeSuccess {
        resource: UpdateResource,
    },
    UnsubscribeSuccess {
        resource: UpdateResource,
    },
}

impl From<OutcomeMessage> for ws::Message {
    fn from(om: OutcomeMessage) -> Self {
        let json = serde_json::to_string(&om).unwrap();
        ws::Message::text(&json)
    }
}
