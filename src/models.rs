use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Topic {
    Config(ConfigParameters),
    TestResource(TestResource),
}

impl TryFrom<&str> for Topic {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(s)?;

        match url.scheme() {
            "topic" => match url.host_str() {
                Some("config") => {
                    let config_file = ConfigFile::try_from(url)?;
                    Ok(Topic::Config(ConfigParameters { file: config_file }))
                }
                Some("test.resource") => {
                    let ps = TestResource::try_from(&url)?;
                    Ok(Topic::TestResource(ps))
                }
                _ => Err(Error::InvalidTopic(s.to_owned())),
            },
            _ => Err(Error::InvalidTopic(s.to_owned())),
        }
    }
}

impl ToString for Topic {
    fn to_string(&self) -> String {
        let mut url = Url::parse("topic://").unwrap();
        match self {
            Topic::Config(cf) => {
                url.set_host(Some("config")).unwrap();
                url.set_path(&cf.file.path);
                url.as_str().to_owned()
            }
            Topic::TestResource(ps) => {
                url.set_host(Some("test.resource")).unwrap();
                url.set_path(&ps.path);
                if let Some(query) = ps.query.clone() {
                    url.set_query(Some(query.as_str()));
                }
                url.as_str().to_owned()
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigFile {
    pub path: String,
}

impl From<ConfigFile> for String {
    fn from(c: ConfigFile) -> Self {
        c.path
    }
}

impl From<&ConfigFile> for String {
    fn from(c: &ConfigFile) -> Self {
        c.path.to_owned()
    }
}

impl TryFrom<Url> for ConfigFile {
    type Error = Error;

    fn try_from(u: Url) -> Result<Self, Self::Error> {
        Ok(ConfigFile {
            path: u.path().to_owned(),
        })
    }
}

impl ToString for ConfigFile {
    fn to_string(&self) -> String {
        self.into()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigParameters {
    pub file: ConfigFile,
}

impl From<&ConfigParameters> for String {
    fn from(cp: &ConfigParameters) -> Self {
        cp.clone().file.into()
    }
}

impl ToString for ConfigParameters {
    fn to_string(&self) -> String {
        self.into()
    }
}

impl TryFrom<Url> for ConfigParameters {
    type Error = Error;

    fn try_from(value: Url) -> Result<Self, Self::Error> {
        let config_file = ConfigFile::try_from(value)?;
        Ok(Self { file: config_file })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TestResource {
    pub path: String,
    pub query: Option<String>,
}

impl ToString for TestResource {
    fn to_string(&self) -> String {
        let mut s = self.path.clone();
        if let Some(query) = self.query.clone() {
            s = format!("{}?{}", s, query).to_string();
        }
        s
    }
}

impl TryFrom<&url::Url> for TestResource {
    type Error = Error;

    fn try_from(u: &url::Url) -> Result<Self, Self::Error> {
        Ok(Self {
            path: u.path().to_string(),
            query: u.query().map(|q| q.to_owned()),
        })
    }
}
