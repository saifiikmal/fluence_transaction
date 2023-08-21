use std::collections::HashMap;

use crate::cron::Cron;
use crate::cron_tx::CronTx;
use crate::curl;
use crate::defaults::{SQL_EXECUTE, SQL_QUERY};
use crate::error::ServiceError;
use crate::meta_contract::MetaContract;
use crate::metadatas::Metadata;
use crate::transaction::{Transaction, TransactionReceipt};
use eyre::Result;
use marine_rs_sdk::MountedBinaryResult;
use marine_sqlite_connector::Value;
use serde::{Deserialize};
use serde_json::{Value as SerdeValue};

pub struct Storage {}

#[derive(Debug, Deserialize)]
pub struct RQLiteResponse {
    results: Vec<RQLiteResult>,
}
#[derive(Debug, Deserialize)]
pub struct RQLiteResult {
    last_insert_id: Option<i64>,
    rows_affected: Option<i64>,
    error: Option<String>,
    types: Option<HashMap<String, SerdeValue>>,
    pub rows: Option<Vec<Row>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Row {
    Metadata(Metadata),
    MetaContract(MetaContract),
    Transaction(Transaction),
    TransactionReceipt(TransactionReceipt),
    Cron(Cron),
    CronTx(CronTx),
}

#[inline]
pub fn get_storage() -> Storage {
    Storage {}
}

impl Storage {
    pub fn execute(query: String) -> Result<RQLiteResult, ServiceError> {
        let statement = vec![
            "-s".to_string(),
            "-XPOST".to_string(),
            SQL_EXECUTE.to_string(),
            "-H".to_string(),
            "Content-Type: application/json".to_string(),
            "-d".to_string(),
            format!(r#"["{}"]"#, query.replace("\n", "")),
        ];

        // log::info!("execute: {:?}", statement);

        let result = curl(statement);
        Self::unwrap_mounted_binary_result(result)
    }

    pub fn read(query: String) -> Result<RQLiteResult, ServiceError> {
        let args = vec![
            "-s".to_string(),
            "-XPOST".to_string(),
            SQL_QUERY.to_string(),
            "-H".to_string(),
            "Content-Type: application/json".to_string(),
            "-d".to_string(),
            format!(r#"["{}"]"#, query.replace("\n", "")),
        ];

        log::info!("read: {:?}", args);

        let result = curl(args);

        Self::unwrap_mounted_binary_result(result)
    }

    pub fn unwrap_mounted_binary_result(
        result: MountedBinaryResult,
    ) -> Result<RQLiteResult, ServiceError> {
        let response: RQLiteResponse = serde_json::from_slice(&result.stdout).expect("Failed to parse JSON");

        if let Some(result) = response.results.into_iter().next() {
            if let Some(error) = result.error {
                return Err(ServiceError::InternalError(error));
            } else {
                return Ok(result);
            }
        }

        Err(ServiceError::InternalError("Invalid response".to_string()))
    }

    pub fn trimmer(input: String) -> String {
      input[1..input.len()-1].to_string()
    }
}
