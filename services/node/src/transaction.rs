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
    pub encryption_type: String,
    pub service_id: String,
    pub method: String,
    pub error_text: String,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionSubset {
    pub hash: String,
    pub timestamp: u64,
    pub meta_contract_id: String,
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
        encryption_type: String,
        service_id: String,
        method: String,
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
            encryption_type.clone(),
            service_id.clone(),
            method.clone(),
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
            encryption_type,
            service_id,
            method,
            error_text: "".to_string(),
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
        encryption_type: String,
        service_id: String,
        method: String,
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
                encryption_type,
                service_id,
                method
            )
            .as_bytes(),
        );
        bs58::encode(hasher.finalize()).into_string()
    }
}
