use marine_rs_sdk::marine;

#[marine]
#[derive(Debug)]
pub struct Ed25519KeyPair {
    pub pk: String,
    pub sk: String,
}
