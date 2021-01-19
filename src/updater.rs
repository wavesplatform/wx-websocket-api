use crate::{error::Error, messages::Topic};
use std::convert::TryFrom;
use wavesexchange_log::error;

// #[derive(Clone, Debug, Serialize, Deserialize)]
// #[serde(rename_all = "snake_case")]
// pub enum UpdateResource {
//     Config(ConfigParameters),
// }

// impl TryFrom<&str> for UpdateResource {
//     type Error = Error;

//     fn try_from(s: &str) -> Result<Self, Self::Error> {
//         let url = Url::parse(s)?;

//         match url.scheme() {
//             "topic" => match url.host_str() {
//                 Some("config") => {
//                     let config_parameters = ConfigParameters::try_from(url)?;
//                     Ok(UpdateResource::Config(config_parameters))
//                 }
//                 _ => Err(Error::InvalidUpdateResource(s.to_owned())),
//             },
//             _ => Err(Error::InvalidUpdateResource(s.to_owned())),
//         }
//     }
// }

// impl From<&UpdateResource> for String {
//     fn from(um: &UpdateResource) -> Self {
//         let mut url = Url::parse("topic://").unwrap();
//         match um {
//             UpdateResource::Config(cp) => {
//                 url.set_host(Some("config")).unwrap();
//                 url.set_path(&cp.file.to_string());
//                 url.as_str().to_owned()
//             }
//         }
//     }
// }

// impl ToString for UpdateResource {
//     fn to_string(&self) -> String {
//         self.into()
//     }
// }

// NB: redis server has to be configured to publish keyspace notifications:
// https://redis.io/topics/notifications
pub fn run(
    redis_client: redis::Client,
    updates_sender: tokio::sync::mpsc::UnboundedSender<Topic>,
) -> Result<(), Error> {
    let mut conn = redis_client.get_connection()?;
    let mut pubsub = conn.as_pubsub();

    pubsub.psubscribe("__keyevent*__:*")?;

    while let Ok(msg) = pubsub.get_message() {
        let update: String = msg.get_payload::<String>()?;

        if let Ok(topic) = Topic::try_from(update.as_ref()) {
            if let Err(err) = updates_sender.send(topic) {
                error!("error occured while sending resource update: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
