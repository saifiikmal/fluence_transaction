use marine_rs_sdk::marine;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DataTypeClone {
    pub origin_data_key: String,
    pub origin_public_key: String,
    pub origin_alias: String,
}

#[marine]
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct DataTypeFork {
    pub data: String,
    pub token_id: String,
    pub address: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct SerdeDataTypeFork {
  pub data: Option<String>,
  pub to: Option<String>,
  #[serde(rename = "tokenId")]
  pub token_id: Option<u64>,
}

impl From<SerdeDataTypeFork> for DataTypeFork {
  fn from(fork_data: SerdeDataTypeFork) -> Self {
    Self {
      data: fork_data.data.unwrap_or_default(),
      token_id: fork_data.token_id.unwrap_or_default().to_string(),
      address: fork_data.to.unwrap_or_default(), 
    }
  }
}