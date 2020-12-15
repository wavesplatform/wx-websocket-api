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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateEntry {
    address: String,
    key: String,
}

impl From<StateEntry> for String {
    fn from(se: StateEntry) -> Self {
        format!("{}:{}", se.address, se.key)
    }
}

impl From<&StateEntry> for String {
    fn from(se: &StateEntry) -> Self {
        format!("{}:{}", se.address, se.key)
    }
}

impl TryFrom<&str> for StateEntry {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = s.split(":").collect();
        if parts.len() < 2 {
            return Err(Error::InvalidStateEntry(s.to_owned()));
        }

        let address = parts.first().unwrap().to_owned();
        let key = parts.last().unwrap().to_owned();

        Ok(StateEntry {
            address: address.to_owned(),
            key: key.to_owned(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StateOptions {
    pub entry: Option<StateEntry>,
    pub entries: Option<Vec<StateEntry>>,
}
