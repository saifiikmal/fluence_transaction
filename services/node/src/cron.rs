use marine_rs_sdk::marine;
use marine_sqlite_connector::{State, Statement, Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::defaults::{CRON_STATUS_DISABLE, CRON_STATUS_ENABLE};
use crate::storage_impl::{RQLiteResult, Row};
use crate::{defaults::CRON_TABLE_NAME, storage_impl::Storage};
use crate::{error::ServiceError, error::ServiceError::InternalError};

#[marine]
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Cron {
    pub hash: String,
    pub token_key: String,
    pub address: String,
    pub topic: String,
    pub token_type: String,
    pub chain: String,
    pub status: i64,
    pub meta_contract_id: String,
    pub node_url: String,
    pub public_key: String,
}

impl Cron {
    pub fn new(token_key: String, address: String, topic: String, token_type: String, chain: String, status: i64, meta_contract_id: String, node_url: String, public_key: String) -> Self {
      let hash = Self::generate_hash(address.clone(), topic.clone(), chain.clone());

      Self {
          hash,
          token_key,
          address,
          topic,
          token_type,
          chain,
          status,
          meta_contract_id,
          node_url,
          public_key,
      }
    }

    pub fn generate_hash(
        address: String,
        topic: String,
        chain: String,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(
            format!(
                "{}{}{}",
                chain,
                address,
                topic,
            )
            .as_bytes(),
        );
        bs58::encode(hasher.finalize()).into_string()
    }
}

#[marine]
#[derive(Debug, Default, Clone, Serialize)]
pub struct CronResult {
    pub hash: String,
    pub token_key: String,
    pub address: String,
    pub topic: String,
    pub token_type: String,
    pub chain: String,
    pub status: i64,
    pub meta_contract_id: String,
    pub node_url: String,
    pub public_key: String,
}

#[derive(Deserialize)]
pub struct SerdeCron {
    pub action: String,
    pub hash: String,
    pub address: String,
    pub topic: String,
    pub token_type: String,
    pub chain: String,
    pub status: i64,
    // pub meta_contract_id: String,
    pub node_url: String,
}

impl Storage {
    pub fn create_cron_table(&self) {
        let table_schema = format!(
            "
        CREATE TABLE IF NOT EXISTS {} (
            hash TEXT PRIMARY KEY UNIQUE,
            token_key varchar(255) not null,
            address varchar(255) not null,
            token_type varchar(255) not null,
            chain varchar(255) not null,
            topic TEXT null,
            status INTEGER not null,
            meta_contract_id varchar(255) null,
            node_url text null,
            last_processed_block integer not null default(0),
            public_key TEXT not null
        )",
            CRON_TABLE_NAME
        );

        let result = Storage::execute(table_schema);

        if let Err(error) = result {
            println!("create_meta_contract_table error: {}", error);
        }
    }

    /**
     * Creation of cron record
     */
    pub fn write_cron(&self, cron: Cron) -> Result<(), ServiceError> {
        let s = format!(
            "insert into {} (hash, token_key, address, token_type, chain, topic, status, last_processed_block, meta_contract_id, node_url, public_key) values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
            CRON_TABLE_NAME,
            cron.hash,
            cron.token_key,
            cron.address,
            cron.token_type,
            cron.chain,
            cron.topic,
            cron.status,
            0,
            cron.meta_contract_id,
            cron.node_url,
            cron.public_key,
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
    pub fn cron_disable(&self, meta_contract_id: String) -> Result<(), ServiceError> {
        let s = format!(
            "
          update {}
          set status = '{}'
          where meta_contract_id = '{}';
          ",
            CRON_TABLE_NAME, CRON_STATUS_DISABLE, meta_contract_id
        );

        Storage::execute(s);

        Ok(())
    }

    pub fn cron_enable(&self, meta_contract_id: String) -> Result<(), ServiceError> {
        let s = format!(
            "
          update {}
          set status = '{}'
          where meta_contract_id = '{}';
          ",
            CRON_TABLE_NAME, CRON_STATUS_ENABLE, meta_contract_id
        );

        Storage::execute(s);

        Ok(())
    }


    pub fn update_cron(&self, hash: String, cron: Cron) -> Result<(), ServiceError> {
      let statement = format!(
        "
          update {}
          set meta_contract_id = '{}',
          node_url = '{}'
          where hash = '{}';
          ",
            CRON_TABLE_NAME, cron.meta_contract_id, cron.node_url, hash
        );

        let result = Storage::execute(statement);

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                log::info!("{}", e.to_string());
                Err(InternalError(e.to_string()))
            }
        }
    }


    pub fn update_cron_status(&self, hash: String, status: i64) -> Result<(), ServiceError> {
        let statement = format!(
          "
            update {}
            set status = '{}'
            where hash = '{}';
            ",
              CRON_TABLE_NAME, status, hash
          );

          let result = Storage::execute(statement);

          match result {
              Ok(_) => Ok(()),
              Err(e) => {
                  log::info!("{}", e.to_string());
                  Err(InternalError(e.to_string()))
              }
          }
    }

    pub fn get_cron_by_hash(&self, hash: String) -> Result<Cron, ServiceError> {
        let statement = format!("SELECT * FROM {} WHERE hash = '{}'",CRON_TABLE_NAME, hash);

        let result = Storage::read(statement)?;
        match read(result) {
            Ok(metas) => metas
                .first()
                .cloned()
                .ok_or_else(|| ServiceError::RecordNotFound("No record found".to_string())),
            Err(e) => Err(e),
        }
    }

    pub fn search_cron(
        &self,
        address: String,
        chain: String,
        topic: String,
    ) -> Result<Cron, ServiceError> {
        let statement = format!(
            "SELECT * FROM {} WHERE address = '{}' AND chain = '{}' AND topic = '{}'",
            CRON_TABLE_NAME, address, chain, topic
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

    pub fn get_cron_by_meta_contract_id(&self, meta_contract_id: String) -> Result<Cron, ServiceError> {
        let statement = f!("SELECT * FROM {CRON_TABLE_NAME} WHERE meta_contract_id = '{meta_contract_id}'");
        let result = Storage::read(statement)?;
        match read(result) {
            Ok(metas) => metas
                .first()
                .cloned()
                .ok_or_else(|| ServiceError::RecordNotFound("No record found".to_string())),
            Err(e) => Err(e),
        }
    }

    pub fn get_enabled_crons(&self) -> Result<Vec<Cron>, ServiceError> {
        let statement = format!("SELECT * FROM {} WHERE status = {}", CRON_TABLE_NAME, CRON_STATUS_ENABLE);

        let result = Storage::read(statement)?;
        match read(result) {
            Ok(metas) => Ok(metas),
            Err(e) => Err(e),
        }
    }

    pub fn get_all_crons(&self) -> Result<Vec<Cron>, ServiceError> {
        let statement = format!("SELECT * FROM {}", CRON_TABLE_NAME);

        let result = Storage::read(statement)?;
        match read(result) {
            Ok(metas) => Ok(metas),
            Err(e) => Err(e),
        }
    }
}

pub fn read(result: RQLiteResult) -> Result<Vec<Cron>, ServiceError> {
  let mut txs = Vec::new();

  if result.rows.is_some() {
    for row in result.rows.unwrap() {
        match row {
            Row::Cron(metadata) => txs.push(metadata),
            _ => {
                return Err(ServiceError::InternalError(format!(
                    "Invalid data format: {}",
                    CRON_TABLE_NAME
                )))
            }
        }
    }
  }

  Ok(txs)
}

