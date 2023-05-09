use crate::defaults::{STATUS_PENDING, STATUS_SUCCESS, TRANSACTIONS_TABLE_NAME};
use crate::error::ServiceError;
use crate::error::ServiceError::InternalError;
use crate::storage_impl::Storage;
use crate::transaction::Transaction;
use marine_sqlite_connector::{State, Statement, Value};

impl Storage {
    pub fn create_transactions_table(&self) {
        let table_schema = format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
                hash TEXT PRIMARY KEY UNIQUE,
                token_key TEXT NOT NULL,
                data_key TEXT NOT NULL,
                from_peer_id TEXT NOT NULL,
                host_id TEXT NOT NULL,
                status INTEGER NOT NULL,
                data TEXT NOT NULL,
                public_key TEXT NOT NULL,
                alias TEXT,
                timestamp INTEGER NOT NULL,
                error_text TEXT NULL,
                meta_contract_id TEXT,
                method TEXT NOT NULL,
                nonce INTEGER NOT NULL,
                token_id TEXT
            );",
            TRANSACTIONS_TABLE_NAME
        );

        let result = self.connection.execute(table_schema);

        if let Err(error) = result {
            println!("create_transactions_table error: {}", error);
        }
    }

    pub fn write_transaction(&self, transaction: Transaction) -> Result<String, ServiceError> {
        let s = format!(
            "insert into {} (hash, token_key, token_id, from_peer_id, host_id, status, data_key, data, public_key, alias, timestamp,meta_contract_id, method, error_text, nonce) values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');",
            TRANSACTIONS_TABLE_NAME,
            transaction.hash,
            transaction.token_key,
            transaction.token_id,
            transaction.from_peer_id,
            transaction.host_id,
            transaction.status,
            transaction.data_key,
            transaction.data,
            transaction.public_key,
            transaction.alias,
            transaction.timestamp,
            transaction.meta_contract_id,
            transaction.method,
            transaction.error_text,
            transaction.nonce
        );

        let result = self.connection.execute(s);

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

    pub fn get_pending_transactions(&self) -> Result<Vec<Transaction>, ServiceError> {
        let mut statement = self.connection.prepare(f!(
            "SELECT * FROM {TRANSACTIONS_TABLE_NAME} WHERE status = ?"
        ))?;

        statement.bind(1, &Value::Integer(STATUS_PENDING))?;

        let mut transactions = Vec::new();

        while let State::Row = statement.next()? {
            transactions.push(read(&statement)?);
        }

        Ok(transactions)
    }

    pub fn get_success_transansactions(
        &self,
        from: i64,
        to: i64,
    ) -> Result<Vec<Transaction>, ServiceError> {
        let mut statement = self.connection.prepare(f!(
            "SELECT * FROM {TRANSACTIONS_TABLE_NAME} WHERE status = ? AND timestamp BETWEEN ? AND ?"
        ))?;

        statement.bind(1, &Value::Integer(STATUS_SUCCESS))?;
        statement.bind(2, &Value::Integer(from))?;
        statement.bind(3, &Value::Integer(to))?;

        let mut transactions = Vec::new();

        while let State::Row = statement.next()? {
            transactions.push(read(&statement)?);
        }

        Ok(transactions)
    }
}

pub fn read(statement: &Statement) -> Result<Transaction, ServiceError> {
    Ok(Transaction {
        hash: statement.read::<String>(0)?,
        token_key: statement.read::<String>(1)?,
        data_key: statement.read::<String>(2)?,
        from_peer_id: statement.read::<String>(3)?,
        host_id: statement.read::<String>(4)?,
        status: statement.read::<i64>(5)? as i64,
        data: statement.read::<String>(6)?,
        public_key: statement.read::<String>(7)?,
        alias: statement.read::<String>(8)?,
        timestamp: statement.read::<i64>(9)? as u64,
        error_text: statement.read::<String>(10)?,
        meta_contract_id: statement.read::<String>(11)?,
        method: statement.read::<String>(12)?,
        nonce: statement.read::<i64>(13)?,
        token_id: statement.read::<String>(14)?,
    })
}
