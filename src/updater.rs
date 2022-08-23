use crate::error::Error;
use crate::metrics::REDIS_INPUT_QUEUE_SIZE;
use crate::topic::Topic;

pub fn run(
    redis_client: redis::Client,
    updater_timeout: Option<std::time::Duration>,
    updates_sender: tokio::sync::mpsc::UnboundedSender<(Topic, String)>,
) -> Result<(), Error> {
    log::info!("updater started");

    loop {
        log::debug!("get new redis connection");

        let mut conn = redis_client.get_connection()?;
        let mut pubsub = conn.as_pubsub();
        pubsub.set_read_timeout(updater_timeout)?;
        pubsub.psubscribe("topic://*")?;
        loop {
            match pubsub.get_message() {
                Ok(msg) => {
                    let topic_str = msg.get_channel_name();
                    match Topic::parse_str(topic_str) {
                        Ok(topic) => {
                            let value = msg.get_payload::<String>()?;
                            REDIS_INPUT_QUEUE_SIZE.inc();
                            updates_sender
                                .send((topic, value))
                                .expect("error occurred while sending resource update");
                        }
                        Err(e) => {
                            // This really should never happen, because every topic that gets into Redis
                            // is parsed and validated, so if we get something weird here it is either
                            // a programming error or some manually-tampered data in Redis.
                            log::warn!("Ignoring bad topic from Redis: '{}' ({:?})", topic_str, e);
                        }
                    }
                }
                Err(error) => {
                    // error when socket don't respond on time
                    if error.to_string().contains("os error 11") {
                        log::info!("updater don't get new events, reopen connection");
                        break;
                    }

                    return Err(error.into());
                }
            }
        }
    }
}
