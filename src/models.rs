use crate::client::ClientSubscriptionKey;
use crate::error::Error;
use std::convert::TryFrom;
use wavesexchange_topic::Topic;

impl TryFrom<&ClientSubscriptionKey> for Topic {
    type Error = Error;

    fn try_from(v: &ClientSubscriptionKey) -> Result<Self, Self::Error> {
        Topic::try_from(v.0.as_str()).map_err(Error::TopicError)
    }
}
