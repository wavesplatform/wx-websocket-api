use crate::error::Error;
use std::convert::TryFrom;
use wavesexchange_log::info;
use wavesexchange_topic::Topic;

// NB: redis server has to be configured to publish keyspace notifications:
// https://redis.io/topics/notifications
pub fn run(
    redis_client: redis::Client,
    updates_sender: tokio::sync::mpsc::UnboundedSender<Topic>,
) -> Result<(), Error> {
    loop {
        let mut conn = redis_client.get_connection()?;
        let mut pubsub = conn.as_pubsub();
        pubsub.set_read_timeout(Some(std::time::Duration::from_secs(60)))?;
        pubsub.psubscribe("__keyevent*__:*")?;
        loop {
            match pubsub.get_message() {
                Ok(msg) => {
                    if let Ok(update) = msg.get_payload::<String>() {
                        if let Ok(topic) = Topic::try_from(update.as_str()) {
                            if let Topic::Transaction(_) = topic {
                                continue;
                            }
                            updates_sender
                                .send(topic)
                                .expect("error occured while sending resource update")
                        }
                    }
                }
                Err(error) => {
                    // error when socket don't response in time
                    if error.to_string().contains("os error 11") {
                        info!("updater don't get new events, reopen connection");
                        break;
                    }
                    return Err(error.into());
                }
            }
        }
    }
}
