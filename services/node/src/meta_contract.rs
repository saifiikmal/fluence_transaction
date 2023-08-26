use marine_rs_sdk::marine;
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[marine]
#[derive(Debug, Default, Clone, Deserialize)]
pub struct MetaContract {
    pub hash: String,
    pub token_key: String,
    pub meta_contract_id: String,
    pub public_key: String,
    pub cid: String,
}

impl MetaContract {
  pub fn new(token_key: String, meta_contract_id: String, public_key: String, cid: String) -> Self {
      let hash = Self::generate_hash(
        token_key.clone(), 
        meta_contract_id.clone(), 
        public_key.clone(),
    );

      Self {
          hash,
          token_key,
          meta_contract_id,
          public_key,
          cid,
      }
  }
  pub fn generate_hash(
      token_key: String,
      meta_contract_id: String,
      public_key: String,
  ) -> String {
      let mut hasher = Sha256::new();
      hasher.update(
          format!(
              "{}{}{}",
              token_key,
              meta_contract_id,
              public_key,
          )
          .as_bytes(),
      );
      bs58::encode(hasher.finalize()).into_string()
  }

}
