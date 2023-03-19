use crate::defaults::{METHOD_CREATE, METHOD_UPDATE, TRANSACTIONS_TABLE_NAME};
use crate::error::ServiceError;
use crate::error::ServiceError::{InternalError, InvalidMethod};
use crate::storage_impl::Storage;
use crate::transaction::Transaction;
use marine_sqlite_connector::{State, Statement, Value};

impl Storage {
    pub fn create_transactions_tables(&self) {
        let table_schema = format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
                hash TEXT PRIMARY KEY,
                token_address TEXT NOT NULL,
                token_id TEXT NOT NULL,
                chain_id TEXT NOT NULL,
                version TEXT NOT NULL,
                from_peer_id TEXT NOT NULL,
                host_id TEXT NOT NULL,
                status INTEGER NOT NULL,
                data_key TEXT NOT NULL,
                metadata TEXT NOT NULL,
                public_key TEXT NOT NULL,
                alias TEXT,
                timestamp INTEGER NOT NULL,
                error_text TEXT NULL,
                encryption_type TEXT NOT NULL,
                service_id TEXT,
                method TEXT NOT NULL
            );",
            TRANSACTIONS_TABLE_NAME
        );

        let current_table_schema = self
            .get_table_schema(TRANSACTIONS_TABLE_NAME.to_string())
            .expect(f!("failed to get {TRANSACTIONS_TABLE_NAME} table schema").as_str());
        if !current_table_schema.is_empty() && table_schema != current_table_schema {
            self.delete_table(TRANSACTIONS_TABLE_NAME.to_string())
                .expect(f!("failed to delete {TRANSACTIONS_TABLE_NAME} table").as_str())
        }

        let result = self.connection.execute(table_schema);

        if let Err(error) = result {
            println!("create_transactions_table error: {}", error);
        }
    }

    pub fn write_transaction(&self, transaction: Transaction) -> Result<String, ServiceError> {
        let s = format!(
            "insert into {} (hash, token_address, token_id, chain_id, version, from_peer_id, host_id, status, data_key, metadata, public_key, alias, timestamp, encryption_type, service_id, method, error_text) values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');",
            TRANSACTIONS_TABLE_NAME,
            transaction.hash,
            transaction.token_address,
            transaction.token_id,
            transaction.chain_id,
            transaction.version,
            transaction.from_peer_id,
            transaction.host_id,
            transaction.status,
            transaction.data_key,
            transaction.metadata,
            transaction.public_key,
            transaction.alias,
            transaction.timestamp,
            transaction.encryption_type,
            transaction.service_id,
            transaction.method,
            transaction.error_text
        );

        self.connection.execute(s)?;

        Ok(transaction.hash)
    }

    pub fn update_transaction_status(
        &self,
        hash: String,
        status: i32,
        error_text: String,
    ) -> Result<(), ServiceError> {
        self.connection.execute(format!(
            "
          update {}
          set status = '{}', error_text = '{}'
          where hash = '{}';
          ",
            TRANSACTIONS_TABLE_NAME, status, error_text, hash
        ))?;

        Ok(())
    }

    pub fn get_transaction(&self, hash: String) -> Result<Transaction, ServiceError> {
        let mut statement = self
            .connection
            .prepare(f!("SELECT * FROM {TRANSACTIONS_TABLE_NAME} WHERE hash = ?"))?;

        statement.bind(1, &Value::String(hash.clone()))?;

        if let State::Row = statement.next()? {
            read(&statement)
        } else {
            Err(InternalError(f!(
                "not found non-host records for given key_hash: {hash}"
            )))
        }
    }
}

pub fn read(statement: &Statement) -> Result<Transaction, ServiceError> {
    Ok(Transaction {
        hash: statement.read::<String>(0)?,
        token_address: statement.read::<String>(1)?,
        token_id: statement.read::<String>(2)?,
        chain_id: statement.read::<String>(3)?,
        version: statement.read::<String>(4)?,
        from_peer_id: statement.read::<String>(5)?,
        host_id: statement.read::<String>(6)?,
        status: statement.read::<i64>(7)? as i64,
        data_key: statement.read::<String>(8)?,
        metadata: statement.read::<String>(9)?,
        public_key: statement.read::<String>(10)?,
        alias: statement.read::<String>(11)?,
        timestamp: statement.read::<i64>(12)? as u64,
        error_text: statement.read::<String>(13)?,
        encryption_type: statement.read::<String>(14)?,
        service_id: statement.read::<String>(15)?,
        method: statement.read::<String>(16)?,
    })
}
