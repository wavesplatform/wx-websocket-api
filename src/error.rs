use crate::messages::PreOutcomeMessage;
use std::sync::mpsc::RecvError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ConfigLoadError: {0}")]
    ConfigLoadError(#[from] envy::Error),
    #[error("JoinError: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("WarpError: {0}")]
    WarpError(#[from] warp::Error),
    #[error("SerdeJsonError: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("RedisPoolError: {0}")]
    RedisPoolError(#[from] bb8_redis::redis::RedisError),
    #[error("RunRedisError: {0}")]
    RunRedisError(#[from] bb8::RunError<bb8_redis::redis::RedisError>),
    #[error("RedisError: {0}")]
    RedisPool(#[from] redis::RedisError),
    #[error("CrossbeamSendError: {0}")]
    CrossbeamSendError(#[from] crossbeam::channel::SendError<PreOutcomeMessage>),
    #[error("InvalidUpdateResource: {0}")]
    InvalidUpdateResource(String),
    #[error("InvalidConfigPath: {0}")]
    InvalidConfigPath(String),
    #[error("RecvError: {0}")]
    RecvError(#[from] RecvError),
    #[error("InvalidSubscribeMessage")]
    InvalidSubscribeMessage,
    #[error("InvalidUnsubscribeMessage")]
    InvalidUnsubscribeMessage,
    #[error("UnknownConnectionId")]
    UnknownConnectionId,
}
