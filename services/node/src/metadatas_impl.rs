use crate::defaults::METADATAS_TABLE_NAME;
use crate::error::ServiceError;
use crate::error::ServiceError::RecordNotFound;
use crate::metadatas::{Metadata, MetadataQuery, MetadataOrdering};
use crate::storage_impl::{Storage, RQLiteResult, Row};
use marine_sqlite_connector::{State, Statement, Value};

impl Storage {
    pub fn create_metadatas_table(&self) {
        let table_schema = format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
                hash TEXT PRIMARY KEY UNIQUE,
                token_key TEXT not null,
                data_key TEXT not null,
                meta_contract_id TEXT not null,
                token_id TEXT null,
                alias varchar(255),
                cid TEXT null,
                public_key TEXT not null,
                version varchar(255) null,
                loose INTEGER CHECK(loose IN (0, 1))
            )",
            METADATAS_TABLE_NAME
        );

        let result = Storage::execute(table_schema);

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
            "insert into {} (hash, token_key, data_key, meta_contract_id, token_id, alias, cid, public_key, version, loose) values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
            METADATAS_TABLE_NAME,
            metadata.hash,
            metadata.token_key,
            metadata.data_key,
            metadata.meta_contract_id,
            metadata.token_id,
            metadata.alias,
            metadata.cid,
            metadata.public_key,
            metadata.version,
            metadata.loose,
        );

        log::info!("{}", s);

        let result = Storage::execute(s);
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
        meta_contract_id: String,
        alias: String,
        public_key: String,
        cid: String,
        version: String,
    ) -> Result<(), ServiceError> {
        let s = format!(
          "
          update {}
          set cid = '{}'
          where data_key = '{}' AND version = '{}' AND meta_contract_id = '{}' AND alias = '{}' AND public_key = '{}';
          ",
            METADATAS_TABLE_NAME, cid, data_key, meta_contract_id, version, alias, public_key
        );

        let result = Storage::execute(s);
        Ok(())
    }

    pub fn get_owner_metadata(
        &self,
        data_key: String,
        meta_contract_id: String,
        public_key: String,
        alias: String,
        version: String,
    ) -> Result<Metadata, ServiceError> {
        let statement = format!(
            "SELECT * FROM {} WHERE data_key = '{}' AND version = '{}' AND meta_contract_id = '{}' AND public_key = '{}' AND alias = '{}'",
            METADATAS_TABLE_NAME, data_key, version, meta_contract_id, public_key, alias
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

    pub fn get_metadata_by_datakey_and_version(
        &self,
        data_key: String,
        version: String,
    ) -> Result<Vec<Metadata>, ServiceError> {
        let statement = format!(
            "SELECT * FROM {} WHERE data_key = '{}' AND version = '{}'",
            METADATAS_TABLE_NAME,
            data_key.clone(),
            version.clone()
        );

        let result = Storage::read(statement)?;
        match read(result) {
            Ok(metas) => Ok(metas),
            Err(e) => Err(e),
        }
    }

    pub fn get_metadata_by_tokenkey_and_tokenid(
        &self,
        token_key: String,
        token_id: String,
        version: String,
    ) -> Result<Vec<Metadata>, ServiceError> {
        let statement = format!(
            "SELECT * FROM {} WHERE token_key = '{}' AND token_id = '{}' AND version = '{}'",
            METADATAS_TABLE_NAME,
            token_key.clone(),
            token_id.clone(),
            version.clone(),
        );

        let result = Storage::read(statement)?;
        match read(result) {
            Ok(metas) => Ok(metas),
            Err(e) => Err(e),
        }
    }


    pub fn get_owner_metadata_by_datakey_and_alias(
        &self,
        data_key: String,
        version: String,
        public_key: String,
        alias: String,
    ) -> Result<Metadata, ServiceError> {
      let statement = format!(
          "SELECT * FROM {} WHERE data_key = '{}' AND version = '{}' AND public_key = '{}' AND alias = '{}'",
          METADATAS_TABLE_NAME,
          data_key.clone(),
          version.clone(),
          public_key.clone(),
          alias.clone(),
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

    // pub fn get_metadata_by_datakey(&self, data_key: String) -> Result<Vec<Metadata>, ServiceError> {
    //     let mut statement = self.connection.prepare(f!(
    //         "SELECT * FROM {METADATAS_TABLE_NAME} WHERE data_key = ?"
    //     ))?;

    //     statement.bind(1, &Value::String(data_key.clone()))?;

    //     let mut metadatas = vec![];

    //     while let State::Row = statement.next()? {
    //         metadatas.push(read(&statement)?);
    //     }

    //     Ok(metadatas)
    // }

    pub fn search_metadatas(&self, query: Vec<MetadataQuery>, ordering: Vec<MetadataOrdering>, from: u32, to: u32) -> Result<Vec<Metadata>, ServiceError> {
      
      let mut query_str = "".to_string();
      let mut ordering_str = "".to_string();
      let mut limit_str = "".to_string();

      if query.len() > 0 {
        let queries: Vec<String> = query.into_iter().map(|param| format!("{} {} '{}'", param.column, param.op, param.query)).collect();

        query_str = format!("WHERE {}",queries.join(" AND "));
      }

      if ordering.len() > 0 {
        let orders: Vec<String> = ordering.into_iter().map(|param| format!("{} {}", param.column, param.sort)).collect();
      
        ordering_str = format!("ORDER BY {}",orders.join(", "));
      }
      if to > 0 {
        limit_str = format!("LIMIT {},{}", from, to);
      }

      
      let s = format!("SELECT * FROM {} {} {} {}", METADATAS_TABLE_NAME, query_str, ordering_str, limit_str);

      log::info!("{}", s.clone());

      let result = Storage::read(s)?;

      match read(result) {
          Ok(metas) => Ok(metas),
          Err(e) => Err(e),
      }
    }
}

pub fn read(result: RQLiteResult) -> Result<Vec<Metadata>, ServiceError> {
  let mut metas = Vec::new();

  if result.rows.is_some() {
    for row in result.rows.unwrap() {
        // log::info!("row: {:?}", row);
        match row {
            Row::Metadata(metadata) => metas.push(metadata),
            _ => {
                return Err(ServiceError::InternalError(format!(
                    "Invalid data format: {}",
                    METADATAS_TABLE_NAME
                )))
            }
        }
    }
  }

  Ok(metas)
}

