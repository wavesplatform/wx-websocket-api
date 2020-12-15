use crate::models::{ConfigOptions, StateOptions};
use crate::updater::UpdateResource;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use warp::ws;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IncomeMessage {
    Ping,
    Subscribe(SubscribeMessage),
    Unsubscribe(UnsubscribeMessage),
}

impl TryFrom<warp::ws::Message> for IncomeMessage {
    type Error = crate::error::Error;

    fn try_from(value: warp::ws::Message) -> Result<Self, Self::Error> {
        serde_json::from_slice(value.into_bytes().as_slice()).map_err(|e| e.into())
    }
}

impl From<IncomeMessage> for warp::ws::Message {
    fn from(m: IncomeMessage) -> Self {
        warp::ws::Message::text(serde_json::to_string(&m).unwrap())
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "resource", rename_all = "snake_case")]
pub enum SubscribeMessage {
    Config { options: ConfigOptions },
    State { options: StateOptions },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "resource", rename_all = "snake_case")]
pub enum UnsubscribeMessage {
    Config { options: ConfigOptions },
    State { options: StateOptions },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutcomeMessage {
    Pong,
    Update {
        resource: UpdateResource,
        value: String,
    },
}

impl From<OutcomeMessage> for ws::Message {
    fn from(om: OutcomeMessage) -> Self {
        let json = serde_json::to_string(&om).unwrap();
        ws::Message::text(&json)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PreOutcomeMessage {
    Pong,
    Update(UpdateResource),
}
