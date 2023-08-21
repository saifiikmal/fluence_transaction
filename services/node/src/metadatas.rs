use marine_rs_sdk::marine;
use sha2::{Digest, Sha256};
use serde::{Serialize, Deserialize};
#[marine]
#[derive(Debug, Default, Clone, Deserialize)]
pub struct Metadata {
    pub hash: String,
    pub token_key: String,
    pub data_key: String,
    pub meta_contract_id: String,
    pub alias: String,
    pub cid: String,
    pub public_key: String,
    pub version: String,
    pub loose: i64,
}

impl Metadata {
    pub fn new(data_key: String, token_key: String, meta_contract_id: String, alias: String, cid: String, 
      public_key: String, version: String, loose: i64) -> Self {
        let hash = Self::generate_hash(
          data_key.clone(), 
          meta_contract_id.clone(), 
          alias.clone(),
          public_key.clone(),
          version.clone(),
      );

        Self {
            hash,
            token_key,
            data_key,
            meta_contract_id,
            alias,
            cid,
            public_key,
            version,
            loose,
        }
    }
    pub fn generate_hash(
      data_key: String,
      meta_contract_id: String,
      alias: String,
      public_key: String,
      version: String,
  ) -> String {
      let mut hasher = Sha256::new();
      hasher.update(
          format!(
              "{}{}{}{}{}",
              data_key,
              meta_contract_id,
              alias,
              public_key,
              version,
          )
          .as_bytes(),
      );
      bs58::encode(hasher.finalize()).into_string()
  }

    pub fn generate_token_key(
        chain_id: String,
        token_address: String,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(
            format!(
                "{}{}",
                chain_id, token_address,
            )
            .as_bytes(),
        );
        bs58::encode(hasher.finalize()).into_string()
    }

    pub fn generate_data_key(
        chain_id: String,
        token_address: String,
        token_id: String,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(
            format!(
                "{}{}{}",
                chain_id, token_address, token_id,
            )
            .as_bytes(),
        );
        bs58::encode(hasher.finalize()).into_string()
    }
}

#[marine]
#[derive(Debug, Clone)]
pub struct FinalMetadata {
    pub public_key: String,
    pub alias: String,
    pub content: String,
    pub loose: i64,
}

#[marine]
#[derive(Debug)]
pub struct MetadataQuery {
  pub column: String,
  pub query: String,
  pub op: String,
}

#[marine]
#[derive(Debug)]
pub struct MetadataOrdering {
  pub column: String,
  pub sort: String,
}

#[marine]
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SerdeMetadata {
  pub loose: i64,
}
