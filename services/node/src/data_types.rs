use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DataTypeClone {
    pub origin_data_key: String,
    pub origin_public_key: String,
    pub origin_alias: String,
}
