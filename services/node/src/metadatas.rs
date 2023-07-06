use marine_rs_sdk::marine;
use sha2::{Digest, Sha256};
#[marine]
#[derive(Debug, Default, Clone)]
pub struct Metadata {
    pub hash: String,
    pub data_key: String,
    pub alias: String,
    pub cid: String,
    pub public_key: String,
}

impl Metadata {
    pub fn new(data_key: String, alias: String, cid: String, public_key: String) -> Self {
        let hash = Self::generate_hash(data_key.clone(), alias.clone(), public_key.clone());

        Self {
            hash,
            data_key,
            alias,
            cid,
            public_key,
        }
    }
    pub fn generate_hash(
      data_key: String,
      alias: String,
      public_key: String,
  ) -> String {
      let mut hasher = Sha256::new();
      hasher.update(
          format!(
              "{}{}{}",
              data_key,
              alias,
              public_key,
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
