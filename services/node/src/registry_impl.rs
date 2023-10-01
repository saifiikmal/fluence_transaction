use crate::error::ServiceError;
use crate::error::ServiceError::RecordNotFound;
use crate::storage_impl::{Storage, RQLiteResult, Row};
use crate::{defaults::REGISTRY_TABLE_NAME, registry::Registry};
use marine_sqlite_connector::{State, Statement, Value};

impl Storage {
    pub fn create_registry_table(&self) {
        let table_schema = format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
                registry_id varchar(100) PRIMARY KEY UNIQUE,
                registry_name varchar(255) null,
                meta_contract_id varchar(255) null,
                public_key varchar(255) null
            )",
            REGISTRY_TABLE_NAME
        );

        let result = Storage::execute(table_schema);

        if let Err(error) = result {
            println!("create_meta_contract_table error: {}", error);
        }
    }

    /**
     * Upon creation of metadata record, it doesnt write metadata CID to the record.
     * Its focusing on creating schema
     */
    pub fn write_registry(&self, registry: Registry) -> Result<(), ServiceError> {
        let s = format!(
            "insert into {} (registry_id, registry_name, meta_contract_id, public_key) values ('{}', '{}', '{}', '{}')",
            REGISTRY_TABLE_NAME,
            registry.registry_id,
            registry.registry_name,
            registry.meta_contract_id,
            registry.public_key,
        );

        Storage::execute(s)?;

        Ok(())
    }

    pub fn update_registry(
        &self,
        registry_id: String,
        meta_contract_id: String,
    ) -> Result<(), ServiceError> {
        let statement = format!(
          "
          update {}
          set meta_contract_id = '{}'
          where registry_id = '{}'
          ",
            REGISTRY_TABLE_NAME, meta_contract_id, registry_id,
        );
        let result = Storage::execute(statement)?;

        Ok(())
    }

    pub fn get_registry_by_meta_contract_id(&self, meta_contract_id: String) -> Result<Registry, ServiceError> {
        let statement = format!("SELECT * FROM {} WHERE meta_contract_id = '{}'",
          REGISTRY_TABLE_NAME, meta_contract_id,
        );

        let result = Storage::read(statement)?;
        match read(result) {
            Ok(regs) => regs
                .first()
                .cloned()
                .ok_or_else(|| ServiceError::RecordNotFound("No record found".to_string())),
            Err(e) => Err(e),
        }
    }

    pub fn get_registry_by_id(&self, registry_id: String) -> Result<Registry, ServiceError> {
        let statement = format!("SELECT * FROM {} WHERE registry_id = '{}'",
          REGISTRY_TABLE_NAME, registry_id,
        );

        let result = Storage::read(statement)?;
          match read(result) {
              Ok(regs) => regs
                  .first()
                  .cloned()
                  .ok_or_else(|| ServiceError::RecordNotFound("No record found".to_string())),
              Err(e) => Err(e),
          }
    }

}

pub fn read(result: RQLiteResult) -> Result<Vec<Registry>, ServiceError> {
  let mut registry = Vec::new();

  if result.rows.is_some() {
    for row in result.rows.unwrap() {
        match row {
            Row::Registry(reg) => registry.push(reg),
            _ => {
                return Err(ServiceError::InternalError(format!(
                    "Invalid data format: {}",
                    REGISTRY_TABLE_NAME
                )))
            }
        }
    }
  }

  Ok(registry)
}
