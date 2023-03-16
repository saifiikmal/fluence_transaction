use marine_rs_sdk::marine;

#[marine]
#[derive(Default)]
pub struct KeyPair {
    pub pk: String,
    pub sk: String,
}
