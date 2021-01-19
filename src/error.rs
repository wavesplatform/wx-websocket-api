use crate::messages::Topic;

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
    #[error("SendUpdateResourceError: {0}")]
    SendUpdateResourceError(#[from] tokio::sync::mpsc::error::SendError<Topic>),
    #[error("SendMessageError: {0}")]
    SendMessageError(
        #[from] tokio::sync::mpsc::error::SendError<Result<warp::ws::Message, warp::Error>>,
    ),
    #[error("InvalidUpdateResource: {0}")]
    InvalidUpdateResource(String),
    #[error("InvalidConfigPath: {0}")]
    InvalidConfigPath(String),
    #[error("InvalidSubscribeMessage")]
    InvalidSubscribeMessage,
    #[error("InvalidUnsubscribeMessage")]
    InvalidUnsubscribeMessage,
    #[error("UrlParseError: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("InvalidPongMessage")]
    InvalidPongMessage,
    #[error("UnknownIncomeMessage: {0}")]
    UnknownIncomeMessage(String),
    #[error("InvalidTopic: {0}")]
    InvalidTopic(String),
}
