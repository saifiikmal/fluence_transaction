use crate::defaults::STATUS_PENDING;
use marine_rs_sdk::marine;
use sha2::{Digest, Sha256};

#[marine]
#[derive(Debug, Default, Clone)]
pub struct Transaction {
    pub hash: String,
    pub token_address: String,
    pub token_id: String,
    pub chain_id: String,
    pub version: String,
    pub from_peer_id: String,
    pub host_id: String,
    pub status: i64,
    pub data_key: String,
    pub metadata: String,
    pub public_key: String,
    pub alias: String,
    pub timestamp: u64,
    pub encryption_type: String,
    pub service_id: String,
    pub method: String,
    pub error_text: String,
}

impl Transaction {
    pub fn new(
        token_address: String,
        token_id: String,
        chain_id: String,
        version: String,
        from_peer_id: String,
        host_id: String,
        data_key: String,
        metadata: String,
        public_key: String,
        alias: String,
        timestamp: u64,
        encryption_type: String,
        service_id: String,
        method: String,
    ) -> Self {
        let hash = Self::generate_hash(
            token_address.clone(),
            token_id.clone(),
            chain_id.clone(),
            version.clone(),
            from_peer_id.clone(),
            host_id.clone(),
            data_key.clone(),
            metadata.clone(),
            public_key.clone(),
            alias.clone(),
            encryption_type.clone(),
            service_id.clone(),
            method.clone(),
        );

        Self {
            hash,
            token_address,
            token_id,
            chain_id,
            version,
            from_peer_id,
            host_id,
            status: STATUS_PENDING,
            data_key,
            metadata,
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
        token_address: String,
        token_id: String,
        chain_id: String,
        version: String,
        from: String,
        host_id: String,
        data_key: String,
        metadata: String,
        public_key: String,
        alias: String,
        encryption_type: String,
        service_id: String,
        method: String,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(
            format!(
                "{}{}{}{}{}{}{}{}{}{}{}{}{}",
                token_address,
                token_id,
                chain_id,
                version,
                from,
                host_id,
                data_key,
                metadata,
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
