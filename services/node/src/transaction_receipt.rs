use crate::storage_impl::{RQLiteResult, Row, Storage};
use crate::transaction::TransactionReceipt;
use crate::{defaults::TRANSACTION_RECEIPT_TABLE_NAME, error::ServiceError};

impl Storage {
    pub fn create_transaction_receipt_table(&self) {
        let table_schema = format!(
            "
          CREATE TABLE IF NOT EXISTS {} (
          hash varchar(32) PRIMARY KEY UNIQUE,
          meta_contract_id varchar(32) not null,
          status INTEGER not null,
          timestamp INTEGER not null,
          error_text INTEGER not null,
          data text null
          )",
            TRANSACTION_RECEIPT_TABLE_NAME
        );

        Storage::execute(table_schema);
    }

    pub fn write_transaction_receipt(
        &self,
        receipt: TransactionReceipt,
    ) -> Result<String, ServiceError> {
        let s = format!(
          "insert or replace into {} (hash, meta_contract_id, status, timestamp, error_text, data) values ('{}', '{}', '{}', '{}', '{}', '{}')",
          TRANSACTION_RECEIPT_TABLE_NAME,
          receipt.hash,
          receipt.meta_contract_id,
          receipt.status,
          receipt.timestamp,
          receipt.error_text,
          receipt.data
      );

        let result = Storage::execute(s);
        Ok(receipt.hash)
    }

    pub fn get_transaction_receipt(
        &self,
        hash: String,
    ) -> Result<TransactionReceipt, ServiceError> {
        let statement = format!(
            "SELECT * FROM {} WHERE hash = '{}'",
            TRANSACTION_RECEIPT_TABLE_NAME,
            hash.clone()
        );
        let result = Storage::read(statement)?;
        match read(result) {
            Ok(metas) => metas
                .first()
                .cloned()
                .ok_or_else(|| ServiceError::RecordNotFound("No record found".to_string())),
            Err(e) => Err(e),
        }
    }
}

pub fn read(result: RQLiteResult) -> Result<Vec<TransactionReceipt>, ServiceError> {
    let mut receipts = Vec::new();

    if result.rows.is_some() {
      for row in result.rows.unwrap() {
          match row {
              Row::TransactionReceipt(receipt) => receipts.push(receipt),
              _ => {
                  return Err(ServiceError::InternalError(format!(
                      "Invalid data format: {}",
                      TRANSACTION_RECEIPT_TABLE_NAME
                  )))
              }
          }
      }
    }

    Ok(receipts)
}
