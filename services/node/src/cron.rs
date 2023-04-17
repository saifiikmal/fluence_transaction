use marine_rs_sdk::marine;
use marine_sqlite_connector::{State, Statement, Value};
use serde::{Deserialize, Serialize};

use crate::defaults::CRON_STATUS_ACTIVE;
use crate::{defaults::CRON_TABLE_NAME, storage_impl::Storage};
use crate::{error::ServiceError, error::ServiceError::InternalError};

#[derive(Debug, Default, Clone, Serialize)]
pub struct Cron {
    pub address: String,
    pub topic: String,
    pub token_type: String,
    pub chain: String,
    pub status: i64,
    pub meta_contract_id: String,
    pub node_url: String,
}

impl Cron {
    pub fn new(address: String, topic: String, token_type: String, chain: String, status: i64, meta_contract_id: String, node_url: String) -> Self {
        Self {
            address,
            topic,
            token_type,
            chain,
            status,
            meta_contract_id,
            node_url,
        }
    }
}

#[marine]
#[derive(Debug, Default, Clone, Serialize)]
pub struct CronResult {
    pub cron_id: i64,
    pub address: String,
    pub topic: String,
    pub token_type: String,
    pub chain: String,
    pub status: i64,
    pub meta_contract_id: String,
    pub node_url: String,
}

#[derive(Deserialize)]
pub struct SerdeCron {
    pub action: String,
    pub cron_id: i64,
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
          	cron_id INTEGER PRIMARY KEY AUTOINCREMENT,
            address varchar(255) not null,
            token_type varchar(255) not null,
            chain varchar(255) not null,
            topic TEXT null,
			status INTEGER not null,
      meta_contract_id varchar(255) null,
      node_url text null,
      last_processed_block integer not null default(0),
			UNIQUE(address, chain, topic)
        );",
            CRON_TABLE_NAME
        );

        let current_table_schema = self
            .get_table_schema(CRON_TABLE_NAME.to_string())
            .expect(f!("failed to get {CRON_TABLE_NAME} table schema").as_str());
        if !current_table_schema.is_empty() && table_schema != current_table_schema {
            self.delete_table(CRON_TABLE_NAME.to_string())
                .expect(f!("failed to delete {CRON_TABLE_NAME} table").as_str())
        }

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
            "insert into {} (address, token_type, chain, topic, status, last_processed_block, meta_contract_id, node_url) values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');",
            CRON_TABLE_NAME,
            cron.address,
            cron.token_type,
            cron.chain,
            cron.topic,
            cron.status,
            0,
            cron.meta_contract_id,
            cron.node_url,
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

    pub fn update_cron_status(&self, cron_id: i64, status: i64) -> Result<(), ServiceError> {
        self.connection.execute(format!(
            "
          update {}
          set status = '{}'
          where cron_id = '{}';
          ",
            CRON_TABLE_NAME, status, cron_id
        ))?;

        Ok(())
    }

    pub fn get_cron_by_id(&self, cron_id: i64) -> Result<CronResult, ServiceError> {
        let mut statement = self
            .connection
            .prepare(f!("SELECT * FROM {CRON_TABLE_NAME} WHERE cron_id = ?"))?;

        statement.bind(1, &Value::Integer(cron_id.clone()))?;

        if let State::Row = statement.next()? {
            read(&statement)
        } else {
            Err(ServiceError::RecordNotFound(f!(
                "cron not found - cron_id: {cron_id}"
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
        cron_id: statement.read::<i64>(0)?,
        address: statement.read::<String>(1)?,
        token_type: statement.read::<String>(2)?,
        chain: statement.read::<String>(3)?,
        topic: statement.read::<String>(4)?,
        status: statement.read::<i64>(5)?,
        meta_contract_id: statement.read::<String>(6)?,
        node_url: statement.read::<String>(7)?,
    })
}
