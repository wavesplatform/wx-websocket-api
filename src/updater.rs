use crate::error::Error;
use std::convert::TryFrom;
use wavesexchange_log::{debug, info};
use wavesexchange_topic::Topic;

pub fn run(
    redis_client: redis::Client,
    updater_timeout: Option<std::time::Duration>,
    updates_sender: tokio::sync::mpsc::UnboundedSender<(Topic, String)>,
) -> Result<(), Error> {
    loop {
        debug!("get new redis connection");

        let mut conn = redis_client.get_connection()?;
        let mut pubsub = conn.as_pubsub();
        pubsub.set_read_timeout(updater_timeout)?;
        pubsub.psubscribe("topic://*")?;
        loop {
            match pubsub.get_message() {
                Ok(msg) => {
                    debug!("got msg from redis: {:?}", msg);

                    if let Ok(topic) = Topic::try_from(msg.get_channel_name()) {
                        let value = msg.get_payload::<String>()?;
                        updates_sender
                            .send((topic, value))
                            .expect("error occured while sending resource update");
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
