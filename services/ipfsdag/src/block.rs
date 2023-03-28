use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Block {
    pub timestamp: u64,
    pub content: Value,
    pub previous: Value,
    pub transaction: Value,
}

pub fn serialize(content: String, previous_cid: String, transaction: String) -> Block {
    let now = SystemTime::now();
    let timestamp = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

    let milliseconds = timestamp.as_millis();

    let obj = serde_json::from_str(&content).unwrap();
    let tx = serde_json::from_str(&transaction).unwrap();
    log::info!("{}", obj);

    let data = Block {
        timestamp: milliseconds as u64,
        content: obj,
        previous: serde_json::json!({ "/": previous_cid }),
        transaction: tx,
    };

    data
}

pub fn deserialize(json: &String) -> Block {
    serde_json::from_str(json).unwrap()
}
