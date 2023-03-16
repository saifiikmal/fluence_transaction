use eyre::Result;
use marine_rs_sdk::marine;

use crate::FdbRetrieval;

#[marine]
#[derive(Debug)]
pub struct FdbGetResult {
    pub success: bool,
    pub error: String,
    pub data: String,
}

impl From<Result<String>> for FdbGetResult {
    fn from(result: Result<String>) -> Self {
        match result {
            Ok(data) => Self {
                success: true,
                error: "".to_string(),
                data,
            },
            Err(err) => Self {
                success: false,
                error: err.to_string(),
                data: "".to_string(),
            },
        }
    }
}

#[marine]
pub struct FdbGetResults {
    pub success: bool,
    pub error: String,
    pub datas: Vec<FdbRetrieval>,
}

impl From<Result<Vec<FdbRetrieval>>> for FdbGetResults {
    fn from(result: Result<Vec<FdbRetrieval>>) -> Self {
        match result {
            Ok(datas) => Self {
                success: true,
                error: "".to_string(),
                datas,
            },
            Err(err) => Self {
                success: false,
                error: err.to_string(),
                datas: Vec::new(),
            },
        }
    }
}
