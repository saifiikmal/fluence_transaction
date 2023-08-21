use crate::defaults::STATUS_PENDING;
use marine_rs_sdk::marine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[marine]
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub hash: String,
    pub method: String,
    pub meta_contract_id: String,
    pub data_key: String,
    pub token_key: String,
    pub data: String,
    pub public_key: String,
    pub alias: String,
    pub timestamp: u64,
    pub chain_id: String,
    pub token_address: String,
    pub token_id: String,
    pub version: String,
    pub status: i64,
    pub mcdata: String,
}

#[marine]
#[derive(Debug, Default)]
pub struct TransactionRequest {
  pub meta_contract_id: String,
  pub alias: String,
  pub public_key: String,
  pub signature: String,
  pub data: String,
  pub method: String,
  pub chain_id: String,
  pub token_address: String,
  pub token_id: String,
  pub version: String,
  pub mcdata: String,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionSubset {
    pub hash: String,
    pub timestamp: u64,
    pub meta_contract_id: String,
    pub method: String,
    pub value: String,
}

#[marine]
#[derive(Debug)]
pub struct TransactionQuery {
  pub column: String,
  pub query: String,
  pub op: String,
}

#[marine]
#[derive(Debug)]
pub struct TransactionOrdering {
  pub column: String,
  pub sort: String,
}

#[marine]
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TransactionReceipt {
    pub hash: String,
    pub meta_contract_id: String,
    pub status: i64,
    pub timestamp: u64,
    pub error_text: String,
    pub data: String,
}

impl Transaction {
    pub fn new(
        meta_contract_id: String,
        token_key: String,
        data_key: String,
        data: String,
        public_key: String,
        alias: String,
        timestamp: u64,
        method: String,
        chain_id: String,
        token_address: String,
        token_id: String,
        version: String,
        mcdata: String,
        status: i64,
        previous_data: String,
    ) -> Self {
      let hash = Self::generate_hash(
          meta_contract_id.clone(),
          token_key.clone(),
          data_key.clone(),
          data.clone(),
          public_key.clone(),
          alias.clone(),
          method.clone(),
          version.clone(),
          mcdata.clone(),
          previous_data,
      );

      Self {
          hash,
          method,
          meta_contract_id,
          data_key,
          token_key,
          data,
          public_key,
          alias,
          timestamp,
          chain_id,
          token_address,
          token_id,
          version,
          status,
          mcdata,
      }
    }

    pub fn generate_hash(
        meta_contract_id: String,
        token_key: String,
        data_key: String,
        data: String,
        public_key: String,
        alias: String,
        method: String,
        version: String,
        mcdata: String,
        previous_content: String,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(
            format!(
                "{}{}{}{}{}{}{}{}{}{}",
                meta_contract_id,
                token_key,
                data_key,
                data,
                public_key,
                alias,
                method,
                version,
                mcdata,
                previous_content
            )
            .as_bytes(),
        );
        bs58::encode(hasher.finalize()).into_string()
    }
}
