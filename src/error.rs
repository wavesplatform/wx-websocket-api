use wavesexchange_topic::Topic;

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
    #[error("InvalidUpdateResource: {0}")]
    InvalidUpdateResource(String),
    #[error("InvalidConfigPath: {0}")]
    InvalidConfigPath(String),
    #[error("InvalidStatePath: {0}")]
    InvalidStatePath(String),
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
    #[error("InvalidTransactionType: {0}")]
    InvalidTransactionType(String),
    #[error("InvalidTransactionPath: {0}")]
    InvalidTransactionPath(String),
    #[error("InvalidTransactionQuery: {0}")]
    InvalidTransactionQuery(ErrorQuery),
    #[error("InvalidLeasingPath: {0}")]
    InvalidLeasingPath(String),
    #[error("TopicError: {0}")]
    TopicError(wavesexchange_topic::error::Error),
}

#[derive(Debug)]
pub struct ErrorQuery(pub Option<String>);

impl std::fmt::Display for ErrorQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.as_ref() {
            None => write!(f, "None"),
            Some(s) => write!(f, "{}", s.to_owned()),
        }
    }
}
