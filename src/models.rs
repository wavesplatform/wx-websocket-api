use crate::client::ClientSubscriptionKey;
use crate::error::Error;
use crate::topic::Topic;
use std::convert::TryFrom;

impl TryFrom<&ClientSubscriptionKey> for Topic {
    type Error = Error;

    fn try_from(v: &ClientSubscriptionKey) -> Result<Self, Self::Error> {
        let topic = &v.0;
        Topic::try_from(topic.as_str()).map_err(|_| Error::InvalidTopic(topic.clone()))
    }
}
