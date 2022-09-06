use wx_topic::{Topic, TopicParseError};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ConfigLoadError: {0}")]
    ConfigLoadError(#[from] envy::Error),
    #[error("ConfigValidationError: {0}")]
    ConfigValidationError(String),
    #[error("JoinError: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("WarpError: {0}")]
    WarpError(#[from] warp::Error),
    #[error("SerdeJsonError: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("RunRedisError: {0}")]
    RunRedisError(#[from] bb8::RunError<bb8_redis::redis::RedisError>),
    #[error("RedisError: {0}")]
    RedisPool(#[from] redis::RedisError),
    #[error("SendUpdateResourceError: {0}")]
    SendUpdateResourceError(#[from] tokio::sync::mpsc::error::SendError<Topic>),
    #[error("SendMessageError: {0}")]
    SendMessageError(#[from] tokio::sync::mpsc::error::SendError<warp::ws::Message>),
    #[error("InvalidPongMessage")]
    InvalidPongMessage,
    #[error("UnknownIncomeMessage: {0}")]
    UnknownIncomeMessage(String),
    #[error("InvalidTopicFromClient: {0}")]
    InvalidTopicFromClient(String),
    #[error("InvalidTopicInRedis: {0}")]
    InvalidTopicInRedis(TopicParseError),
}
