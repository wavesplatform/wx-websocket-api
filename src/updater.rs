use crate::{error::Error, models::Topic};
use std::convert::TryFrom;
use wavesexchange_log::error;

// NB: redis server has to be configured to publish keyspace notifications:
// https://redis.io/topics/notifications
pub fn run(
    redis_client: redis::Client,
    updates_sender: tokio::sync::mpsc::UnboundedSender<Topic>,
) -> Result<(), Error> {
    let mut conn = redis_client.get_connection()?;
    let mut pubsub = conn.as_pubsub();
    pubsub.set_read_timeout(Some(std::time::Duration::from_secs(240)))?;

    loop {
        pubsub.psubscribe("__keyevent*__:*")?;
        match pubsub.get_message() {
            Ok(msg) => {
                if let Ok(update) = msg.get_payload::<String>() {
                    if let Ok(topic) = Topic::try_from(update.as_ref()) {
                        if let Err(err) = updates_sender.send(topic) {
                            error!("error occured while sending resource update: {:?}", err);
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                // error when socket don't response in time
                if error.to_string().contains("os error 11") {
                    continue;
                }
                return Err(error.into());
            }
        }
    }

    Ok(())
}
