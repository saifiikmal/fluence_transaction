use crate::defaults::STATUS_PENDING;
use crate::storage_impl::{RQLiteResult, Row};
use crate::{defaults::CRON_TX_TABLE_NAME, storage_impl::Storage};
use marine_rs_sdk::marine;
use marine_sqlite_connector::{State, Statement, Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use crate::{error::ServiceError, error::ServiceError::InternalError};

#[marine]
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CronTx {
    pub hash: String,
    pub address: String,
    pub topic: String,
    pub token_type: String,
    pub chain: String,
    pub meta_contract_id: String,
    pub timestamp: u64,
    pub tx_block_number: u64,
    pub tx_hash: String,
    pub status: i64,
    pub data: String,
    pub error_text: String,
    pub token_id: String,
    pub data_key: String,
    pub token_key: String,
}

impl CronTx {
    pub fn new(
      address: String,
      topic: String,
      token_type: String,
      chain: String,
      meta_contract_id: String,
      timestamp: u64,
      tx_block_number: u64,
      tx_hash: String,
      status: i64,
      data: String,
      error_text: String,
      token_id: String,
      data_key: String,
      token_key: String,
    ) -> Self {
      let hash = Self::generate_hash(
        address.clone(),
        topic.clone(),
        token_type.clone(),
        chain.clone(),
        tx_block_number.clone(),
        tx_hash.clone(),
        token_id.clone(),
      );
      Self {
        hash,
        address,
        topic,
        token_type,
        chain,
        meta_contract_id,
        timestamp,
        tx_block_number,
        tx_hash,
        status,
        data,
        error_text,
        token_id,
        data_key,
        token_key,
      }
    }

    pub fn generate_hash(
        address: String,
        topic: String,
        token_type: String,
        chain: String,
        tx_block_number: u64,
        tx_hash: String,
        token_id: String,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(
            format!(
                "{}{}{}{}{}{}{}",
                address,
                topic,
                token_type,
                chain,
                tx_block_number,
                tx_hash,
                token_id
            )
            .as_bytes(),
        );
        bs58::encode(hasher.finalize()).into_string()
    }
}

impl Storage {
  pub fn create_cron_tx_table(&self) {
      let table_schema = format!(
          "
      CREATE TABLE IF NOT EXISTS {} (
          hash TEXT PRIMARY KEY UNIQUE,
          address varchar(255) not null,
          token_type varchar(255) not null,
          chain varchar(255) not null,
          topic TEXT null,
          meta_contract_id varchar(255) null,
          timestamp INTEGER NOT NULL,
          tx_block_number INTEGER NOT NULL default(0),
          tx_hash varchar(255) null,
          status INTEGER NOT NULL,
          data TEXT NULL,
          error_text TEXT NULL,
          token_id TEXT NULL,
          data_key TEXT NULL,
          token_key TEXT NULL,
          UNIQUE(address, chain, topic, tx_hash)
      )",
          CRON_TX_TABLE_NAME
      );

      let result = Storage::execute(table_schema);

      if let Err(error) = result {
          println!("create_cron_tx_table error: {}", error);
      }
  }

  /**
   * Creation of cron log
   */
  pub fn write_cron_tx(&self, cron: CronTx) -> Result<(), ServiceError> {
    let s = format!(
        "insert into {} (
          hash,
          address, 
          token_type, 
          chain, 
          topic, 
          meta_contract_id,
          timestamp,
          tx_block_number,
          tx_hash,
          status,
          data,
          error_text,
          token_id,
          data_key,
          token_key
        ) values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
        CRON_TX_TABLE_NAME,
        cron.hash,
        cron.address,
        cron.token_type,
        cron.chain,
        cron.topic,
        cron.meta_contract_id,
        cron.timestamp,
        cron.tx_block_number,
        cron.tx_hash,
        cron.status,
        Storage::trimmer(serde_json::to_string(&cron.data).unwrap()),
        cron.error_text,
        cron.token_id,
        cron.data_key,
        cron.token_key,
    );

    let result = Storage::execute(s);

    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            log::info!("{}", e.to_string());
            Err(InternalError(e.to_string()))
        }
    }
  }

  pub fn get_cron_tx_by_tx_hash(&self, 
    transaction_hash: String, 
    address: String, 
    chain: String,
    topic: String, ) -> Result<CronTx, ServiceError> {
    let statement = format!("SELECT * FROM {} WHERE 
        tx_hash = '{}' and address = '{}' and chain = '{}' and topic = '{}'",
        CRON_TX_TABLE_NAME, transaction_hash, address, chain, topic
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

  pub fn get_cron_tx_latest_block(&self, 
    address: String, 
    chain: String,
    topic: String, ) -> Result<CronTx, ServiceError> {
    let statement = format!("SELECT * FROM {} WHERE address = '{}' and chain = '{}' and topic = '{}' order by tx_block_number desc",
        CRON_TX_TABLE_NAME,
        address,
        chain,
        topic,
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

  pub fn get_all_cron_txs(&self) -> Result<Vec<CronTx>, ServiceError> {
    let statement = format!("SELECT * FROM {} order by timestamp desc", CRON_TX_TABLE_NAME);

    let result = Storage::read(statement)?;
    match read(result) {
        Ok(metas) => Ok(metas),
        Err(e) => Err(e),
    }
  }

  pub fn search_cron_tx(
      &self,
      address: String,
      chain: String,
      topic: String,
  ) -> Result<Vec<CronTx>, ServiceError> {
      let statement = format!(
        "SELECT * FROM {} WHERE address = '{}' AND chain = '{}' AND topic = '{}'",
        CRON_TX_TABLE_NAME,
        address,
        chain,
        topic,
      );

      let result = Storage::read(statement)?;
      match read(result) {
          Ok(metas) => Ok(metas),
          Err(e) => Err(e),
      }
  }

}

pub fn read(result: RQLiteResult) -> Result<Vec<CronTx>, ServiceError> {
  let mut txs = Vec::new();

  if result.rows.is_some() {
    for row in result.rows.unwrap() {
        match row {
            Row::CronTx(metadata) => txs.push(metadata),
            _ => {
                return Err(ServiceError::InternalError(format!(
                    "Invalid data format: {}",
                    CRON_TX_TABLE_NAME
                )))
            }
        }
    }
  }

  Ok(txs)
}
