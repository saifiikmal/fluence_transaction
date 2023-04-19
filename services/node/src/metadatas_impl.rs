use crate::defaults::METADATAS_TABLE_NAME;
use crate::error::ServiceError;
use crate::error::ServiceError::RecordNotFound;
use crate::metadatas::Metadata;
use crate::storage_impl::Storage;
use marine_sqlite_connector::{State, Statement, Value};

impl Storage {
    pub fn create_metadatas_table(&self) {
        let table_schema = format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_key TEXT not null,
                alias varchar(255),
                cid TEXT null,
                public_key TEXT not null,
                UNIQUE(data_key, public_key, alias)
            );",
            METADATAS_TABLE_NAME
        );

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
            "insert into {} (data_key, alias, cid, public_key) values ('{}', '{}', '{}', '{}');",
            METADATAS_TABLE_NAME,
            metadata.data_key,
            metadata.alias,
            metadata.cid,
            metadata.public_key
        );

        log::info!("{}", s);

        let result = self.connection.execute(s);
        match result {
            Ok(_) => return Ok(()),
            Err(error) => {
                log::info!("{:?}", error);
                return Ok(());
            }
        }
    }

    pub fn update_cid(
        &self,
        data_key: String,
        public_key: String,
        cid: String,
    ) -> Result<(), ServiceError> {
        self.connection.execute(format!(
            "
          update {}
          set cid = '{}'
          where data_key = '{}' AND public_key = '{}';
          ",
            METADATAS_TABLE_NAME, cid, data_key, public_key
        ))?;

        Ok(())
    }

    pub fn get_owner_metadata_by_datakey_and_alias(
        &self,
        data_key: String,
        public_key: String,
        alias: String,
    ) -> Result<Metadata, ServiceError> {
        let mut statement = self
            .connection
            .prepare(format!(
                "SELECT * FROM {} WHERE data_key = '{}' AND public_key = '{}' AND alias = '{}'",
                METADATAS_TABLE_NAME, data_key, public_key, alias
            ))
            .unwrap();

        if let State::Row = statement.next()? {
            read(&statement)
        } else {
            Err(RecordNotFound(f!("{data_key}#{public_key}#{alias}")))
        }
    }

    pub fn get_metadata_by_datakey(&self, data_key: String) -> Result<Vec<Metadata>, ServiceError> {
        let mut statement = self.connection.prepare(f!(
            "SELECT * FROM {METADATAS_TABLE_NAME} WHERE data_key = ?"
        ))?;

        statement.bind(1, &Value::String(data_key.clone()))?;

        let mut metadatas = vec![];

        while let State::Row = statement.next()? {
            metadatas.push(read(&statement)?);
        }

        Ok(metadatas)
    }
}

pub fn read(statement: &Statement) -> Result<Metadata, ServiceError> {
    Ok(Metadata {
        data_key: statement.read::<String>(1)?,
        alias: statement.read::<String>(2)?,
        cid: statement.read::<String>(3)?,
        public_key: statement.read::<String>(4)?,
    })
}
