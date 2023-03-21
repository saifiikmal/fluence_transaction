use crate::defaults::METADATAS_TABLE_NAME;
use crate::error::ServiceError;
use crate::error::ServiceError::RecordNotFound;
use crate::metadatas::Metadata;
use crate::storage_impl::Storage;
use marine_sqlite_connector::{State, Statement, Value};

impl Storage {
    pub fn create_metadatas_tables(&self) {
        let table_schema = format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
                data_key TEXT not null primary key unique,
                alias varchar(255) not null,
                cid TEXT null,
                public_key TEXT not null,
                service_id varchar(255) null
            );",
            METADATAS_TABLE_NAME
        );

        let current_table_schema = self
            .get_table_schema(METADATAS_TABLE_NAME.to_string())
            .expect(f!("failed to get {METADATAS_TABLE_NAME} table schema").as_str());
        if !current_table_schema.is_empty() && table_schema != current_table_schema {
            self.delete_table(METADATAS_TABLE_NAME.to_string())
                .expect(f!("failed to delete {METADATAS_TABLE_NAME} table").as_str())
        }

        let result = self.connection.execute(table_schema);

        if let Err(error) = result {
            println!("create_transactions_table error: {}", error);
        }
    }

    /**
     * Upon creation of metadata record, it doesnt write metadata CID to the record.
     * Its focusing on creating schema
     */
    pub fn write_metadata(&self, metadata: Metadata) -> Result<(), ServiceError> {
        let s = format!(
            "insert into {} (data_key, alias, public_key, enc) values ('{}', '{}', '{}', '{}');",
            METADATAS_TABLE_NAME,
            metadata.data_key,
            metadata.alias,
            metadata.public_key,
            metadata.enc
        );

        self.connection.execute(s)?;

        Ok(())
    }

    pub fn update_cid(&self, data_key: String, cid: i32) -> Result<(), ServiceError> {
        self.connection.execute(format!(
            "
          update {}
          set cid = '{}'
          where data_key = '{}';
          ",
            METADATAS_TABLE_NAME, cid, data_key
        ))?;

        Ok(())
    }

    pub fn update_service_id(&self, data_key: String, service_id: i32) -> Result<(), ServiceError> {
        self.connection.execute(format!(
            "
          update {}
          set service_id = '{}'
          where data_key = '{}';
          ",
            METADATAS_TABLE_NAME, service_id, data_key
        ))?;

        Ok(())
    }

    pub fn get_metadata(&self, data_key: String) -> Result<Metadata, ServiceError> {
        let mut statement = self.connection.prepare(f!(
            "SELECT * FROM {METADATAS_TABLE_NAME} WHERE data_key = ?"
        ))?;

        statement.bind(1, &Value::String(data_key.clone()))?;

        if let State::Row = statement.next()? {
            read(&statement)
        } else {
            Err(RecordNotFound(f!("{data_key}")))
        }
    }

    // pub fn get_transactions_by_datakey(&self, data_key: String) -> Result<Vec<Transaction>, ServiceError> {
    //     let mut statement =
    //         self.connection
    //             .prepare(f!("SELECT * FROM {METADATAS_TABLE_NAME} WHERE data_key = ?"))?;

    //     statement.bind(1, &Value::String(data_key))?;

    //     let mut result = vec![];

    //     while let State::Row = statement.next()? {
    //         result.push(read(&statement)?)
    //     }

    //     Ok(result)
    // }
}

pub fn read(statement: &Statement) -> Result<Metadata, ServiceError> {
    Ok(Metadata {
        data_key: statement.read::<String>(0)?,
        alias: statement.read::<String>(1)?,
        cid: statement.read::<String>(2)?,
        public_key: statement.read::<String>(3)?,
        enc: statement.read::<String>(4)?,
        service_id: statement.read::<String>(5)?,
    })
}
