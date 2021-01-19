use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use url::Url;

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
