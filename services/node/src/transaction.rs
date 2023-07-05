use crate::defaults::STATUS_PENDING;
use marine_rs_sdk::marine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[marine]
#[derive(Debug, Default, Clone, Serialize)]
pub struct Transaction {
    pub hash: String,
    pub token_key: String,
    pub data_key: String,
    pub nonce: i64,
    pub from_peer_id: String,
    pub host_id: String,
    pub status: i64,
    pub data: String,
    pub public_key: String,
    pub alias: String,
    pub timestamp: u64,
    pub meta_contract_id: String,
    pub method: String,
    pub error_text: String,
    pub token_id: String,
}

#[marine]
#[derive(Debug, Default)]
pub struct TransactionRequest {
  pub data_key: String,
  pub token_key: String,
  pub token_id: String,
  pub alias: String,
  pub public_key: String,
  pub signature: String,
  pub data: String,
  pub method: String,
  pub nonce: i64,
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
}

#[marine]
#[derive(Debug)]
pub struct TransactionOrdering {
  pub column: String,
  pub sort: String,
}

impl Transaction {
    pub fn new(
        token_key: String,
        from_peer_id: String,
        host_id: String,
        data_key: String,
        nonce: i64,
        data: String,
        public_key: String,
        alias: String,
        timestamp: u64,
        meta_contract_id: String,
        method: String,
        token_id: String,
    ) -> Self {
        let hash = Self::generate_hash(
            token_key.clone(),
            from_peer_id.clone(),
            host_id.clone(),
            data_key.clone(),
            data.clone(),
            nonce.clone(),
            public_key.clone(),
            alias.clone(),
            meta_contract_id.clone(),
            method.clone(),
            token_id.clone(),
        );

        Self {
            hash,
            token_key,
            from_peer_id,
            host_id,
            status: STATUS_PENDING,
            data_key,
            nonce,
            data,
            public_key,
            alias,
            timestamp,
            meta_contract_id,
            method,
            error_text: "".to_string(),
            token_id,
        }
    }

    pub fn generate_hash(
        token_key: String,
        from: String,
        host_id: String,
        data_key: String,
        data: String,
        nonce: i64,
        public_key: String,
        alias: String,
        meta_contract_id: String,
        method: String,
        token_id: String,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(
            format!(
                "{}{}{}{}{}{}{}{}{}{}{}",
                token_key,
                from,
                host_id,
                data_key,
                nonce,
                data,
                public_key,
                alias,
                meta_contract_id,
                method,
                token_id
            )
            .as_bytes(),
        );
        bs58::encode(hasher.finalize()).into_string()
    }
}
