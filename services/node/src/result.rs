use eyre::Result;
use marine_rs_sdk::marine;

use crate::{
    cron::Cron, error::ServiceError, meta_contract::MetaContract, metadatas::Metadata,
    transaction::Transaction,
};

#[marine]
#[derive(Debug)]
pub struct FdbResult {
    pub transaction_hash: String,
}

#[marine]
#[derive(Debug)]
pub struct FdbTransactionResult {
    pub success: bool,
    pub err_msg: String,
    pub transaction: Transaction,
}

impl From<Result<Transaction, ServiceError>> for FdbTransactionResult {
    fn from(result: Result<Transaction, ServiceError>) -> Self {
        match result {
            Ok(transaction) => Self {
                success: true,
                err_msg: "".to_string(),
                transaction,
            },
            Err(err) => Self {
                success: false,
                err_msg: err.to_string(),
                transaction: Transaction::default(),
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

#[marine]
#[derive(Debug)]
pub struct FdbMetadataResult {
    pub success: bool,
    pub err_msg: String,
    pub metadata: Metadata,
}

impl From<Result<Metadata, ServiceError>> for FdbMetadataResult {
    fn from(result: Result<Metadata, ServiceError>) -> Self {
        match result {
            Ok(metadata) => Self {
                success: true,
                err_msg: "".to_string(),
                metadata,
            },
            Err(err) => Self {
                success: false,
                err_msg: err.to_string(),
                metadata: Metadata::default(),
            },
        }
    }
}

#[marine]
#[derive(Debug)]
pub struct FdbMetadatasResult {
    pub success: bool,
    pub err_msg: String,
    pub metadatas: Vec<Metadata>,
}

impl From<Result<Vec<Metadata>, ServiceError>> for FdbMetadatasResult {
    fn from(result: Result<Vec<Metadata>, ServiceError>) -> Self {
        match result {
            Ok(metadatas) => Self {
                success: true,
                err_msg: "".to_string(),
                metadatas,
            },
            Err(err) => Self {
                success: false,
                err_msg: err.to_string(),
                metadatas: Vec::new(),
            },
        }
    }
}

#[marine]
#[derive(Debug)]
pub struct FdbMetadataHistoryResult {
    pub success: bool,
    pub err_msg: String,
    pub metadata: String,
    pub history: Vec<String>,
}

impl From<Result<Vec<String>, ServiceError>> for FdbMetadataHistoryResult {
    fn from(result: Result<Vec<String>, ServiceError>) -> Self {
        match result {
            Ok(metadatas) => Self {
                success: true,
                err_msg: "".to_string(),
                metadata: metadatas[0].clone(),
                history: metadatas[1..].to_vec(),
            },
            Err(err) => Self {
                success: false,
                err_msg: err.to_string(),
                metadata: "{}".to_string(),
                history: Vec::new(),
            },
        }
    }
}

#[marine]
#[derive(Debug)]
pub struct FdbMetaContractResult {
    pub success: bool,
    pub err_msg: String,
    pub meta: MetaContract,
}

impl From<Result<MetaContract, ServiceError>> for FdbMetaContractResult {
    fn from(result: Result<MetaContract, ServiceError>) -> Self {
        match result {
            Ok(meta) => Self {
                success: true,
                err_msg: "".to_string(),
                meta,
            },
            Err(err) => Self {
                success: false,
                err_msg: err.to_string(),
                meta: MetaContract::default(),
            },
        }
    }
}

#[marine]
#[derive(Debug)]
pub struct FdbCronsResult {
    pub success: bool,
    pub err_msg: String,
    pub crons: Vec<Cron>,
}

impl From<Result<Vec<Cron>, ServiceError>> for FdbCronsResult {
    fn from(result: Result<Vec<Cron>, ServiceError>) -> Self {
        match result {
            Ok(crons) => Self {
                success: true,
                err_msg: "".to_string(),
                crons,
            },
            Err(err) => Self {
                success: false,
                err_msg: err.to_string(),
                crons: Vec::new(),
            },
        }
    }
}
