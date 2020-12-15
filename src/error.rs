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
    #[error("InvalidResourceName: {0}")]
    InvalidResourceName(String),
    #[error("EmptyFilename: {0}")]
    EmptyFilename(String),
    #[error("InvalidFilename: {0}")]
    InvalidFilename(String),
    #[error("InvalidUpdateResource: {0}")]
    InvalidUpdateResource(String),
    #[error("InvalidStateEntry: {0}")]
    InvalidStateEntry(String),
    #[error("RunRedisError: {0}")]
    RunRedisError(#[from] bb8::RunError<bb8_redis::redis::RedisError>),
    #[error("ParseError: {0}")]
    ParseError(#[from] url::ParseError),
    #[error("InvalidPath: {0}")]
    InvalidPath(String),
}
