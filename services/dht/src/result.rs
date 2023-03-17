use eyre::Result;
use marine_rs_sdk::marine;

use crate::{error::ServiceError, transaction::Transaction};

#[marine]
#[derive(Debug)]
pub struct FdbResult {
    pub success: bool,
    pub err_msg: String,
}

impl From<Result<(), ServiceError>> for FdbResult {
    fn from(result: Result<(), ServiceError>) -> Self {
        match result {
            Ok(_) => Self {
                success: true,
                err_msg: "".to_string(),
            },
            Err(err) => Self {
                success: false,
                err_msg: err.to_string(),
            },
        }
    }
}

#[marine]
#[derive(Debug)]
pub struct FdbTransactionsResult {
    pub success: bool,
    pub err_msg: String,
    pub transactions: Vec<Transaction>,
}

impl From<Result<Vec<Transaction>, ServiceError>> for FdbTransactionsResult {
    fn from(result: Result<Vec<Transaction>, ServiceError>) -> Self {
        match result {
            Ok(transactions) => Self {
                success: true,
                err_msg: "".to_string(),
                transactions,
            },
            Err(err) => Self {
                success: false,
                err_msg: err.to_string(),
                transactions: Vec::new(),
            },
        }
    }
}
