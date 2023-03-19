use crate::defaults::STATUS_PENDING;
use marine_rs_sdk::marine;
use sha2::{Digest, Sha256};

#[marine]
#[derive(Debug, Default, Clone)]
pub struct Transaction {
    pub hash: String,
    pub from_peer_id: String,
    pub host_id: String,
    pub status: i64,
    pub data_key: String,
    pub metadata: String,
    pub public_key: String,
    pub alias: String,
    pub timestamp: u64,
    pub encryption_type: String,
    pub metadata_cid: String,
    pub method: String,
    pub error_text: String,
}

impl Transaction {
    pub fn new(
        from_peer_id: String,
        host_id: String,
        data_key: String,
        metadata: String,
        public_key: String,
        alias: String,
        timestamp: u64,
        encryption_type: String,
        metadata_cid: String,
        method: String,
    ) -> Self {
        let hash = Self::generate_hash(
            from_peer_id.clone(),
            host_id.clone(),
            data_key.clone(),
            metadata.clone(),
            public_key.clone(),
            alias.clone(),
            encryption_type.clone(),
            method.clone(),
        );

        Self {
            hash,
            from_peer_id,
            host_id,
            status: STATUS_PENDING,
            data_key,
            metadata,
            public_key,
            alias,
            timestamp,
            encryption_type,
            metadata_cid,
            method,
            error_text: "".to_string(),
        }
    }

    pub fn generate_hash(
        from: String,
        host_id: String,
        data_key: String,
        metadata: String,
        public_key: String,
        alias: String,
        encryption_type: String,
        method: String,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(
            format!(
                "{}{}{}{}{}{}{}{}",
                from, host_id, data_key, metadata, public_key, alias, encryption_type, method
            )
            .as_bytes(),
        );
        bs58::encode(hasher.finalize()).into_string()
    }
}
