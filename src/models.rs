use crate::error::{self, Error};
use std::{convert::TryFrom, str::FromStr};
use url::Url;

#[derive(Clone, Debug)]
pub enum Topic {
    Config(ConfigParameters),
    State(State),
    TestResource(TestResource),
    BlockchainHeight,
    Transaction(TransactionByAddress),
}

impl TryFrom<&str> for Topic {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(s)?;

        match url.host_str() {
            Some("config") => {
                let config_file = ConfigFile::try_from(url)?;
                Ok(Topic::Config(ConfigParameters { file: config_file }))
            }
            Some("state") => {
                let state = State::try_from(url)?;
                Ok(Topic::State(state))
            }
            Some("test.resource") => {
                let ps = TestResource::try_from(url)?;
                Ok(Topic::TestResource(ps))
            }
            Some("blockchain_height") => Ok(Topic::BlockchainHeight),
            Some("transaction") => {
                let transaction = TransactionByAddress::try_from(url)?;
                Ok(Topic::Transaction(transaction))
            }
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
            Topic::State(state) => {
                url.set_host(Some("state")).unwrap();
                let path = state.to_string();
                url.set_path(&path);
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
            Topic::BlockchainHeight => {
                url.set_host(Some("blockchain_height")).unwrap();
                url.as_str().to_owned()
            }
            Topic::Transaction(transaction) => {
                url.set_host(Some("transaction")).unwrap();
                url.set_path(&transaction.tx_type.to_string());
                url.set_query(Some(format!("address={}", &transaction.address).as_str()));
                url.as_str().to_owned()
            }
        }
    }
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct State {
    pub address: String,
    pub key: String,
}

impl From<&State> for String {
    fn from(s: &State) -> Self {
        format!("{}/{}", s.address.clone(), s.key.clone())
    }
}

impl ToString for State {
    fn to_string(&self) -> String {
        self.into()
    }
}

impl TryFrom<Url> for State {
    type Error = Error;

    fn try_from(value: Url) -> Result<Self, Self::Error> {
        let params = value
            .path_segments()
            .ok_or_else(|| Error::InvalidStatePath(value.path().to_string()))?
            .take(2)
            .collect::<Vec<_>>();
        if params.len() == 2 {
            let address = params[0].to_string();
            let key = params[1].to_string();
            Ok(Self { address, key })
        } else {
            Err(Error::InvalidStatePath(value.path().to_string()))
        }
    }
}

#[test]
fn topic_state_test() {
    let url = Url::parse("topic://state/some_address/some_key").unwrap();
    let state = State::try_from(url).unwrap();
    assert_eq!(state.address, "some_address".to_string());
    assert_eq!(state.key, "some_key".to_string());
    let url = Url::parse("topic://state/some_address/some_key/some_other_part_of_path").unwrap();
    let state = State::try_from(url).unwrap();
    assert_eq!(state.address, "some_address".to_string());
    assert_eq!(state.key, "some_key".to_string());
    let state_string = state.to_string();
    assert_eq!("some_address/some_key".to_string(), state_string);
}

#[derive(Clone, Debug)]
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

impl TryFrom<Url> for TestResource {
    type Error = Error;

    fn try_from(u: Url) -> Result<Self, Self::Error> {
        Ok(Self {
            path: u.path().to_string(),
            query: u.query().map(|q| q.to_owned()),
        })
    }
}

pub struct BlockchainHeight {}

impl TryFrom<Url> for BlockchainHeight {
    type Error = Error;

    fn try_from(_value: Url) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}

#[derive(Clone, Debug)]
pub struct TransactionByAddress {
    pub tx_type: TransactionType,
    pub address: String,
}

impl TryFrom<Url> for TransactionByAddress {
    type Error = Error;

    fn try_from(value: Url) -> Result<Self, Self::Error> {
        let tx_type = FromStr::from_str(
            &value
                .path_segments()
                .ok_or_else(|| Error::InvalidTransactionType(value.path().to_string()))?
                .next()
                .ok_or_else(|| Error::InvalidTransactionType(value.path().to_string()))?,
        )?;
        let address = get_address(&value)?;
        Ok(Self { address, tx_type })
    }
}

fn get_address(value: &Url) -> Result<String, Error> {
    for (k, v) in value.query_pairs() {
        if k.to_string() == "address".to_string() {
            return Ok(v.to_string());
        }
    }
    return Err(Error::InvalidTransactionQuery(error::ErrorQuery(
        value.query().map(ToString::to_string),
    )));
}

#[test]
fn transaction_topic_test() {
    let url = Url::parse("topic://transaction/all?address=some_address").unwrap();
    let transaction = TransactionByAddress::try_from(url).unwrap();
    assert_eq!(transaction.tx_type.to_string(), "all".to_string());
    assert_eq!(transaction.address, "some_address".to_string());
    assert_eq!(
        "topic://transaction/all?address=some_address".to_string(),
        Topic::Transaction(transaction).to_string()
    );
    let url = Url::parse("topic://transaction/exchange?address=some_other_address").unwrap();
    let transaction = TransactionByAddress::try_from(url).unwrap();
    assert_eq!(transaction.tx_type.to_string(), "exchange".to_string());
    assert_eq!(transaction.address, "some_other_address".to_string());
    assert_eq!(
        "topic://transaction/exchange?address=some_other_address".to_string(),
        Topic::Transaction(transaction).to_string()
    );
    let url = Url::parse("topic://transaction/exchange").unwrap();
    let error = TransactionByAddress::try_from(url);
    assert!(error.is_err());
    assert_eq!(
        format!("{}", error.unwrap_err()),
        "InvalidTransactionQuery: None".to_string()
    );
}

#[derive(Clone, Debug)]
pub enum TransactionType {
    All,
    Genesis,
    Payment,
    Issue,
    Transfer,
    Reissue,
    Burn,
    Exchange,
    Lease,
    LeaseCancel,
    CreateAlias,
    MassTransfer,
    DataTransaction,
    SetScript,
    SponsorFee,
    SetAssetScript,
    InvokeScript,
    UpdateAssetInfo,
}

impl ToString for TransactionType {
    fn to_string(&self) -> String {
        let s = match self {
            Self::All => "all",
            Self::Genesis => "genesis",
            Self::Payment => "payment",
            Self::Issue => "issue",
            Self::Transfer => "transfer",
            Self::Reissue => "reissue",
            Self::Burn => "burn",
            Self::Exchange => "exchange",
            Self::Lease => "lease",
            Self::LeaseCancel => "lease_cancel",
            Self::CreateAlias => "create_alias",
            Self::MassTransfer => "mass_transfer",
            Self::DataTransaction => "data_transaction",
            Self::SetScript => "set_script",
            Self::SponsorFee => "sponsor_fee",
            Self::SetAssetScript => "set_asset_script",
            Self::InvokeScript => "invoke_script",
            Self::UpdateAssetInfo => "update_asset_info",
        };
        s.to_string()
    }
}

impl FromStr for TransactionType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let transaction_type = match s {
            "all" => Self::All,
            "genesis" => Self::Genesis,
            "payment" => Self::Payment,
            "issue" => Self::Issue,
            "transfer" => Self::Transfer,
            "reissue" => Self::Reissue,
            "burn" => Self::Burn,
            "exchange" => Self::Exchange,
            "lease" => Self::Lease,
            "lease_cancel" => Self::LeaseCancel,
            "create_alias" => Self::CreateAlias,
            "mass_transfer" => Self::MassTransfer,
            "data_transaction" => Self::DataTransaction,
            "set_script" => Self::SetScript,
            "sponsor_fee" => Self::SponsorFee,
            "set_asset_script" => Self::SetAssetScript,
            "invoke_script" => Self::InvokeScript,
            "update_asset_info" => Self::UpdateAssetInfo,
            _ => return Err(Error::InvalidTransactionType(s.to_string())),
        };
        Ok(transaction_type)
    }
}
