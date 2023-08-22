use crate::defaults::{STATUS_PENDING, TRANSACTIONS_TABLE_NAME, STATUS_DONE};
use crate::error::ServiceError;
use crate::error::ServiceError::InternalError;
use crate::storage_impl::{RQLiteResult, Row, Storage};
use crate::transaction::{Transaction, TransactionQuery, TransactionOrdering};
use marine_sqlite_connector::{State, Statement, Value};

impl Storage {
    pub fn create_transactions_table(&self) {
        let table_schema = format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
                hash TEXT PRIMARY KEY UNIQUE,
                method TEXT NOT NULL,
                meta_contract_id TEXT,
                token_key TEXT,
                data_key TEXT,
                data TEXT NULL,
                public_key TEXT NOT NULL,
                alias TEXT,
                timestamp INTEGER NOT NULL,
                chain_id TEXT,
                token_address TEXT,
                token_id TEXT,
                version varchar(32) NOT NULL,
                mcdata TEXT NULL,
                status INTEGER NOT NULL
            )",
            TRANSACTIONS_TABLE_NAME
        );

        let result = Storage::execute(table_schema);

        if let Err(error) = result {
            println!("create_transactions_table error: {}", error);
        }
    }

    pub fn write_transaction(&self, transaction: Transaction) -> Result<String, ServiceError> {
        let s = format!(
            "insert into {} (hash, method, meta_contract_id, token_key, data_key, data, public_key, alias, timestamp, chain_id, token_address, token_id, version, mcdata, status) 
            values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
            TRANSACTIONS_TABLE_NAME,
            transaction.hash,
            transaction.method,
            transaction.meta_contract_id,
            transaction.token_key,
            transaction.data_key,
            Storage::trimmer(serde_json::to_string(&transaction.data).unwrap()),
            transaction.public_key,
            transaction.alias,
            transaction.timestamp,
            transaction.chain_id,
            transaction.token_address,
            transaction.token_id,
            transaction.version,
            Storage::trimmer(serde_json::to_string(&transaction.mcdata).unwrap()),
            transaction.status,
        );

        let result = Storage::execute(s);

        match result {
            Ok(_) => Ok(transaction.hash),
            Err(e) => {
                log::info!("{}", e.to_string());
                Err(InternalError(e.to_string()))
            }
        }
    }

    pub fn update_transaction_status(
        &self,
        hash: String,
        status: i64,
    ) -> Result<(), ServiceError> {
        Storage::execute(format!(
            "
          update {}
          set status = '{}'
          where hash = '{}';
          ",
            TRANSACTIONS_TABLE_NAME, status, hash
        ))?;

        Ok(())
    }

    pub fn get_transaction(&self, hash: String) -> Result<Transaction, ServiceError> {
      let statement = format!(
          "SELECT * FROM {} WHERE hash = '{}'",
          TRANSACTIONS_TABLE_NAME,
          hash.clone()
      );
      let result = Storage::read(statement)?;
      // log::info!("get tx: {:?}", result);
      match read(result) {
          Ok(metas) => metas
              .first()
              .cloned()
              .ok_or_else(|| ServiceError::RecordNotFound("No record found".to_string())),
          Err(e) => Err(e),
      }
    }

    pub fn get_pending_transactions(&self) -> Result<Vec<Transaction>, ServiceError> {
      let statement = format!(
          "SELECT * FROM {} WHERE status = {}",
          TRANSACTIONS_TABLE_NAME,
          STATUS_PENDING,
      );
      let result = Storage::read(statement)?;
      match read(result) {
          Ok(metas) => Ok(metas),
          Err(e) => Err(e),
      }
    }

    pub fn get_transactions(&self, query: Vec<TransactionQuery>, ordering: Vec<TransactionOrdering>, from: u32, to: u32) -> Result<Vec<Transaction>, ServiceError> {
      
      let mut query_str = "".to_string();
      let mut ordering_str = "".to_string();
      let mut limit_str = "".to_string();

      if query.len() > 0 {
        let queries: Vec<String> = query.into_iter().map(|param| format!("{} {} '{}'", param.column, param.op, param.query)).collect();

        query_str = format!("WHERE {}",queries.join(" AND "));
      }

      if ordering.len() > 0 {
        let orders: Vec<String> = ordering.into_iter().map(|param| format!("{} {}", param.column, param.sort)).collect();
      
        ordering_str = format!("ORDER BY {}",orders.join(", "));
      } else {
        ordering_str = format!("ORDER BY timestamp DESC");
      }
      if to > 0 {
        limit_str = format!("LIMIT {},{}", from, to);
      }

      
      let s = format!("SELECT * FROM {} {} {} {}", TRANSACTIONS_TABLE_NAME, query_str, ordering_str, limit_str);


      let result = Storage::read(s)?;

      match read(result) {
          Ok(metas) => Ok(metas),
          Err(e) => Err(e),
      }
    }

    pub fn get_complete_transactions(
        &self,
        from: i64,
        to: i64,
    ) -> Result<Vec<Transaction>, ServiceError> {
        let mut s = format!(
            "SELECT * FROM {} WHERE status = {} AND timestamp BETWEEN {} AND {}",
            TRANSACTIONS_TABLE_NAME,
            STATUS_DONE,
            from,
            to,
        );

        let result = Storage::read(s)?;
        match read(result) {
            Ok(metas) => Ok(metas),
            Err(e) => Err(e),
        }
    }
}

pub fn read(result: RQLiteResult) -> Result<Vec<Transaction>, ServiceError> {
  let mut txs = Vec::new();

  if result.rows.is_some() {
    for row in result.rows.unwrap() {
        match row {
            Row::Transaction(metadata) => txs.push(metadata),
            _ => {
                return Err(ServiceError::InternalError(format!(
                    "Invalid data format: {}",
                    TRANSACTIONS_TABLE_NAME
                )))
            }
        }
    }
  }

  Ok(txs)
}
