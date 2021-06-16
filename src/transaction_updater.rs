use crate::error::Error;
use std::convert::TryFrom;
use wavesexchange_log::info;
use wavesexchange_topic::Topic;

// NB: redis server has to be configured to publish keyspace notifications:
// https://redis.io/topics/notifications
pub fn run(
    redis_client: redis::Client,
    transaction_updates_sender: tokio::sync::mpsc::UnboundedSender<(Topic, String)>,
) -> Result<(), Error> {
    loop {
        let mut conn = redis_client.get_connection()?;
        let mut pubsub = conn.as_pubsub();
        pubsub.set_read_timeout(Some(std::time::Duration::from_secs(60)))?;
        pubsub.psubscribe("topic://transactions*")?;
        loop {
            match pubsub.get_message() {
                Ok(msg) => {
                    if let Ok(topic @ Topic::Transaction(_)) =
                        Topic::try_from(msg.get_channel_name())
                    {
                        let value = msg.get_payload::<String>()?;
                        transaction_updates_sender
                            .send((topic, value))
                            .expect("error occured while sending resource transaction update");
                    }
                }
                Err(error) => {
                    // error when socket don't response in time
                    if error.to_string().contains("os error 11") {
                        info!("transaction_updater don't get new events, reopen connection");
                        break;
                    }
                    return Err(error.into());
                }
            }
        }
    }
}
