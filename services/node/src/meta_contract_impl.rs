use crate::error::ServiceError;
use crate::error::ServiceError::RecordNotFound;
use crate::storage_impl::Storage;
use crate::{defaults::META_CONTRACT_TABLE_NAME, meta_contract::MetaContract};
use marine_sqlite_connector::{State, Statement, Value};

impl Storage {
    pub fn create_meta_contract_table(&self) {
        let table_schema = format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
                token_key varchar(255) not null primary key,
                meta_contract_id varchar(255) null,
                public_key varchar(255) null
            );",
            META_CONTRACT_TABLE_NAME
        );

        let current_table_schema = self
            .get_table_schema(META_CONTRACT_TABLE_NAME.to_string())
            .expect(f!("failed to get {META_CONTRACT_TABLE_NAME} table schema").as_str());
        if !current_table_schema.is_empty() && table_schema != current_table_schema {
            self.delete_table(META_CONTRACT_TABLE_NAME.to_string())
                .expect(f!("failed to delete {META_CONTRACT_TABLE_NAME} table").as_str())
        }

        let result = self.connection.execute(table_schema);

        if let Err(error) = result {
            println!("create_meta_contract_table error: {}", error);
        }
    }

    /**
     * Upon creation of metadata record, it doesnt write metadata CID to the record.
     * Its focusing on creating schema
     */
    pub fn write_meta_contract(&self, contract: MetaContract) -> Result<(), ServiceError> {
        let s = format!(
            "insert into {} (token_key, meta_contract_id, public_key) values ('{}', '{}', '{}');",
            META_CONTRACT_TABLE_NAME,
            contract.token_key,
            contract.meta_contract_id,
            contract.public_key
        );

        self.connection.execute(s)?;

        Ok(())
    }

    pub fn rebind_meta_contract(
        &self,
        token_key: String,
        meta_contract_id: String,
    ) -> Result<(), ServiceError> {
        self.connection.execute(format!(
            "
          update {}
          set meta_contract_id = '{}'
          where token_key = '{}';
          ",
            META_CONTRACT_TABLE_NAME, meta_contract_id, token_key
        ))?;

        Ok(())
    }

    pub fn get_meta_contract(&self, token_key: String) -> Result<MetaContract, ServiceError> {
        let mut statement = self.connection.prepare(f!(
            "SELECT * FROM {META_CONTRACT_TABLE_NAME} WHERE token_key = ?"
        ))?;

        statement.bind(1, &Value::String(token_key.clone()))?;

        if let State::Row = statement.next()? {
            read(&statement)
        } else {
            Err(RecordNotFound(f!("{token_key}")))
        }
    }
}

pub fn read(statement: &Statement) -> Result<MetaContract, ServiceError> {
    Ok(MetaContract {
        token_key: statement.read::<String>(0)?,
        meta_contract_id: statement.read::<String>(1)?,
        public_key: statement.read::<String>(2)?,
    })
}
