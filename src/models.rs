use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigFile {
    path: String,
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

impl TryFrom<&str> for ConfigFile {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let p = std::path::Path::new(s)
            .to_str()
            .ok_or(Error::InvalidConfigPath(s.to_owned()))?;

        Ok(ConfigFile { path: p.to_owned() })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigOptions {
    pub file: Option<ConfigFile>,
    pub files: Option<Vec<ConfigFile>>,
}
