use marine_rs_sdk::marine;
use marine_sqlite_connector::{State, Statement, Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::defaults::CRON_STATUS_ACTIVE;
use crate::{defaults::CRON_TABLE_NAME, storage_impl::Storage};
use crate::{error::ServiceError, error::ServiceError::InternalError};

#[derive(Debug, Default, Clone, Serialize)]
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
                address,
                topic,
                chain,
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
    pub meta_contract_id: String,
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
        );",
            CRON_TABLE_NAME
        );

        let result = self.connection.execute(table_schema);

        if let Err(error) = result {
            println!("create_meta_contract_table error: {}", error);
        }
    }

    /**
     * Creation of cron record
     */
    pub fn write_cron(&self, cron: Cron) -> Result<(), ServiceError> {
        let s = format!(
            "insert into {} (hash, token_key, address, token_type, chain, topic, status, last_processed_block, meta_contract_id, node_url, public_key) values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');",
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

        let result = self.connection.execute(s);

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                log::info!("{}", e.to_string());
                Err(InternalError(e.to_string()))
            }
        }
    }

    pub fn update_cron(&self, hash: String, cron: Cron) -> Result<(), ServiceError> {
      self.connection.execute(format!(
          "
        update {}
        set meta_contract_id = '{}',
        node_url = '{}'
        where hash = '{}';
        ",
          CRON_TABLE_NAME, cron.meta_contract_id, cron.node_url, hash
      ))?;

      Ok(())
  }

    pub fn update_cron_status(&self, hash: String, status: i64) -> Result<(), ServiceError> {
        self.connection.execute(format!(
            "
          update {}
          set status = '{}'
          where hash = '{}';
          ",
            CRON_TABLE_NAME, status, hash
        ))?;

        Ok(())
    }

    pub fn get_cron_by_hash(&self, hash: String) -> Result<CronResult, ServiceError> {
        let mut statement = self
            .connection
            .prepare(f!("SELECT * FROM {CRON_TABLE_NAME} WHERE hash = ?"))?;

        statement.bind(1, &Value::String(hash.clone()))?;

        if let State::Row = statement.next()? {
            read(&statement)
        } else {
            Err(ServiceError::RecordNotFound(f!(
                "cron not found - hash: {hash}"
            )))
        }
    }

    pub fn search_cron(
        &self,
        address: String,
        chain: String,
        topic: String,
    ) -> Result<CronResult, ServiceError> {
        let mut statement = self.connection.prepare(f!(
            "SELECT * FROM {CRON_TABLE_NAME} WHERE address = ? AND chain = ? AND topic = ?"
        ))?;

        statement.bind(1, &Value::String(address.clone()))?;
        statement.bind(2, &Value::String(chain.clone()))?;
        statement.bind(3, &Value::String(topic.clone()))?;

        if let State::Row = statement.next()? {
            read(&statement)
        } else {
            Err(ServiceError::RecordNotFound(f!(
                "cron not found - address: {address}, chain: {chain}, topic: {topic}"
            )))
        }
    }

    pub fn get_active_crons(&self) -> Result<Vec<CronResult>, ServiceError> {
        let mut statement = self
            .connection
            .prepare(f!("SELECT * FROM {CRON_TABLE_NAME} WHERE status = ?"))?;

        statement.bind(1, &Value::Integer(CRON_STATUS_ACTIVE))?;

        let mut crons = Vec::new();

        while let State::Row = statement.next()? {
            crons.push(read(&statement)?);
        }

        Ok(crons)
    }

    pub fn get_all_crons(&self) -> Result<Vec<CronResult>, ServiceError> {
        let mut statement = self
            .connection
            .prepare(f!("SELECT * FROM {CRON_TABLE_NAME}"))?;

        let mut crons = Vec::new();

        while let State::Row = statement.next()? {
            crons.push(read(&statement)?);
        }

        Ok(crons)
    }
}

pub fn read(statement: &Statement) -> Result<CronResult, ServiceError> {
    Ok(CronResult {
        hash: statement.read::<String>(0)?,
        token_key: statement.read::<String>(1)?,
        address: statement.read::<String>(2)?,
        token_type: statement.read::<String>(3)?,
        chain: statement.read::<String>(4)?,
        topic: statement.read::<String>(5)?,
        status: statement.read::<i64>(6)?,
        meta_contract_id: statement.read::<String>(7)?,
        node_url: statement.read::<String>(8)?,
        public_key: statement.read::<String>(9)?,
    })
}
