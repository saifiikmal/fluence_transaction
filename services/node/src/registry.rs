use marine_rs_sdk::marine;
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[marine]
#[derive(Debug, Default, Clone, Deserialize)]
pub struct Registry {
    pub registry_id: String,
    pub registry_name: String,
    pub meta_contract_id: String,
    pub public_key: String,
}

impl Registry {
  pub fn new(registry_id: String, registry_name: String, meta_contract_id: String, public_key: String) -> Self {

      Self {
          registry_id,
          registry_name,
          meta_contract_id,
          public_key,
      }
  }

}
