use marine_rs_sdk::marine;

#[marine]
#[derive(Debug, Default, Clone)]
pub struct Metadata {
    pub data_key: String,
    pub alias: String,
    pub cid: String,
    pub public_key: String,
}

impl Metadata {
    pub fn new(data_key: String, alias: String, cid: String, public_key: String) -> Self {
        Self {
            data_key,
            alias,
            cid,
            public_key,
        }
    }
}

#[marine]
#[derive(Debug, Clone)]
pub struct FinalMetadata {
    pub public_key: String,
    pub alias: String,
    pub content: String,
}
