use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize)]
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

    let content_value = match serde_json::from_str(&content) {
        Ok(value) => value,
        Err(_) => Value::String(content.clone()),
    };

    let tx = serde_json::from_str(&transaction).unwrap();

    let data = Block {
        timestamp: milliseconds as u64,
        content: content_value,
        previous: serde_json::json!({ "/": previous_cid }),
        transaction: tx,
    };

    data
}

pub fn deserialize(json: &str) -> Block {
    serde_json::from_str(json).unwrap()
}
