use crate::defaults::DB_PATH;
use crate::error::ServiceError;
use marine_sqlite_connector::{Connection, Result as SqliteResult, State, Value};

pub struct Storage {
    pub(crate) connection: Connection,
}

#[inline]
pub(crate) fn get_storage() -> SqliteResult<Storage> {
    marine_sqlite_connector::open(DB_PATH).map(|c| Storage { connection: c })
}

impl Storage {
    pub fn get_table_schema(&self, table_name: String) -> Result<String, ServiceError> {
        let mut statement = self
            .connection
            .prepare(f!("SELECT sql FROM sqlite_master WHERE name=?;"))?;

        statement.bind(1, &Value::String(table_name))?;

        if let State::Row = statement.next()? {
            let schema = statement.read::<String>(0)?;
            Ok(schema)
        } else {
            Ok("".to_string())
        }
    }

    pub fn delete_table(&self, table_name: String) -> Result<(), ServiceError> {
        self.connection
            .execute(f!("DROP TABLE IF EXISTS {table_name};"))?;
        Ok(())
    }
}
