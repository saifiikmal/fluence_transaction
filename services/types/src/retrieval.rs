use marine_rs_sdk::marine;

use crate::block::Block;

#[marine]
#[derive(Debug, Clone, Default)]
pub struct FdbRetrieval {
    pub key: String,
    pub public_key: String,
    pub cid: String,
    pub block: Block,
}
