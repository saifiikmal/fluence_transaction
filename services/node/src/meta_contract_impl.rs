use crate::error::ServiceError;
use crate::error::ServiceError::RecordNotFound;
use crate::storage_impl::{Storage, RQLiteResult, Row};
use crate::{defaults::META_CONTRACT_TABLE_NAME, meta_contract::MetaContract};
use marine_sqlite_connector::{State, Statement, Value};

impl Storage {
    pub fn create_meta_contract_table(&self) {
        let table_schema = format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
                token_key varchar(255) not null primary key,
                meta_contract_id varchar(255) null,
                public_key varchar(255) null,
                cid varchar(255) null
            )",
            META_CONTRACT_TABLE_NAME
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
    pub fn write_meta_contract(&self, contract: MetaContract) -> Result<(), ServiceError> {
        let s = format!(
            "insert into {} (token_key, meta_contract_id, public_key, cid) values ('{}', '{}', '{}', '{}')",
            META_CONTRACT_TABLE_NAME,
            contract.token_key,
            contract.meta_contract_id,
            contract.public_key,
            contract.cid,
        );

        Storage::execute(s)?;

        Ok(())
    }

    pub fn rebind_meta_contract(
        &self,
        token_key: String,
        meta_contract_id: String,
        pk: String,
    ) -> Result<(), ServiceError> {
        let statement = format!(
          "
          update {}
          set token_key = '{}'
          where meta_contract_id = '{}'
          and public_key = '{}'
          ",
            META_CONTRACT_TABLE_NAME, token_key, meta_contract_id, pk,
        );
        let result = Storage::execute(statement)?;

        Ok(())
    }

    pub fn get_meta_contract_by_tokenkey(&self, token_key: String) -> Result<MetaContract, ServiceError> {
        let statement = format!("SELECT * FROM {} WHERE token_key = '{}'",
          META_CONTRACT_TABLE_NAME, token_key,
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

    pub fn get_meta_contract_by_id(&self, meta_contract_id: String) -> Result<MetaContract, ServiceError> {
        let statement = format!("SELECT * FROM {} WHERE meta_contract_id = '{}'",
          META_CONTRACT_TABLE_NAME, meta_contract_id,
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

    pub fn get_meta_contract_by_id_and_pk(&self, meta_contract_id: String, public_key: String) -> Result<MetaContract, ServiceError> {
        let statement = format!("SELECT * FROM {} WHERE meta_contract_id = '{}' and public_key = '{}'",
          META_CONTRACT_TABLE_NAME, meta_contract_id, public_key,
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
}

pub fn read(result: RQLiteResult) -> Result<Vec<MetaContract>, ServiceError> {
  let mut metas = Vec::new();

  if result.rows.is_some() {
    for row in result.rows.unwrap() {
        match row {
            Row::MetaContract(meta_contract) => metas.push(meta_contract),
            _ => {
                return Err(ServiceError::InternalError(format!(
                    "Invalid data format: {}",
                    META_CONTRACT_TABLE_NAME
                )))
            }
        }
    }
  }

  Ok(metas)
}
