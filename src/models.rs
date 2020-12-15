use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigFile {
    name: String,
    path: Option<String>,
}

impl From<ConfigFile> for String {
    fn from(c: ConfigFile) -> Self {
        format!("{}{}", c.path.unwrap_or("/".to_string()), c.name)
    }
}

impl From<&ConfigFile> for String {
    fn from(c: &ConfigFile) -> Self {
        format!("{}{}", c.path.as_ref().unwrap_or(&"/".to_string()), c.name)
    }
}

impl TryFrom<&str> for ConfigFile {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let p = std::path::Path::new(s);

        let name = p
            .file_name()
            .ok_or(Error::EmptyFilename(s.to_owned()))
            .and_then(|n| n.to_str().ok_or(Error::InvalidFilename(s.to_owned())))?;
        let path = p
            .parent()
            .and_then(|path| path.to_str())
            .map(|s| s.to_owned());

        Ok(ConfigFile {
            name: name.to_owned(),
            path: path,
        })
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
