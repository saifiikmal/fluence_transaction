use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct Block {
    pub timestamp: u64,
    pub content: Value,
    pub previous: Value,
    pub transaction: Value,
}
