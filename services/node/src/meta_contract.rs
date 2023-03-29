use marine_rs_sdk::marine;

#[marine]
#[derive(Debug, Default, Clone)]
pub struct MetaContract {
    pub token_key: String,
    pub meta_contract_id: String,
    pub public_key: String,
}
